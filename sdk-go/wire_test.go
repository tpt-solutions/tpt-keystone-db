package keystone

import (
	"bytes"
	"errors"
	"net"
	"testing"
)

// --- request (frontend) encoding -----------------------------------------

func TestEncodeParam(t *testing.T) {
	cases := []struct {
		in     any
		want   []byte
		isNull bool
	}{
		{nil, nil, true},
		{"hi", []byte("hi"), false},
		{[]byte("raw"), []byte("raw"), false},
		{true, []byte("t"), false},
		{false, []byte("f"), false},
		{int(7), []byte("7"), false},
		{int64(99), []byte("99"), false},
		{float64(1.5), []byte("1.5"), false},
	}
	for _, c := range cases {
		got, isNull := encodeParam(c.in)
		if isNull != c.isNull {
			t.Errorf("encodeParam(%v) isNull=%v, want %v", c.in, isNull, c.isNull)
		}
		if !bytes.Equal(got, c.want) {
			t.Errorf("encodeParam(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestWriteQueryEncoding(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		n, _ := server.Read(buf)
		got <- buf[:n]
	}()

	w := newWireConn(client)
	w.writeQuery("SELECT 1")
	if err := w.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	out := <-got

	if out[0] != 'Q' {
		t.Fatalf("expected tag 'Q', got %q", out[0])
	}
	// length prefix (4 + body), body is "SELECT 1\0"
	wantLen := int32(4 + len("SELECT 1\x00"))
	gotLen := int32(out[1])<<24 | int32(out[2])<<16 | int32(out[3])<<8 | int32(out[4])
	if gotLen != wantLen {
		t.Fatalf("length prefix = %d, want %d", gotLen, wantLen)
	}
	if string(out[5:]) != "SELECT 1\x00" {
		t.Fatalf("body = %q, want SELECT 1\\0", out[5:])
	}
}

func TestWriteBindEncodesNullsAsNegativeLength(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 128)
		n, _ := server.Read(buf)
		got <- buf[:n]
	}()

	w := newWireConn(client)
	p1 := []byte("a")
	w.writeBind("", "", []*[]byte{&p1, nil})
	if err := w.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	out := <-got
	if out[0] != 'B' {
		t.Fatalf("expected tag 'B', got %q", out[0])
	}
	// The NULL param must be encoded as a 4-byte -1 length.
	if !bytes.Contains(out, []byte{0xff, 0xff, 0xff, 0xff}) {
		t.Fatalf("NULL param not encoded as -1 length: %x", out)
	}
}

// --- response (backend) decoding -----------------------------------------

func mustDecode(t *testing.T, tag byte, body []byte) kindAndPayload {
	t.Helper()
	msg, err := decodeBody(tag, body)
	if err != nil {
		t.Fatalf("decodeBody(%q) unexpected error: %v", tag, err)
	}
	return msg
}

func TestDecodeRowDescription(t *testing.T) {
	var b []byte
	b = putI16(b, 1)
	b = putCStr(b, "id")
	b = append(b, 0, 0, 0, 0) // table oid
	b = append(b, 0, 0)       // col attr
	b = putI32(b, 23)         // type oid (int8)
	b = append(b, 0, 0)       // type size
	b = append(b, 0, 0, 0, 0) // type modifier
	b = append(b, 0, 0)       // format
	msg := mustDecode(t, 'T', b)
	if msg.Kind != msgRowDescription {
		t.Fatalf("kind = %v, want msgRowDescription", msg.Kind)
	}
	if len(msg.Fields) != 1 || msg.Fields[0].Name != "id" || msg.Fields[0].TypeOID != 23 {
		t.Fatalf("fields = %+v", msg.Fields)
	}
}

func TestDecodeDataRowWithNull(t *testing.T) {
	var b []byte
	b = putI16(b, 2)
	b = putI32(b, 3)
	b = append(b, "abc"...)
	b = putI32(b, -1) // NULL
	msg := mustDecode(t, 'D', b)
	if len(msg.Row) != 2 {
		t.Fatalf("got %d cols, want 2", len(msg.Row))
	}
	if string(*msg.Row[0]) != "abc" {
		t.Fatalf("col0 = %q, want abc", *msg.Row[0])
	}
	if msg.Row[1] != nil {
		t.Fatalf("col1 = %v, want nil", msg.Row[1])
	}
}

func TestDecodeTerminalMessages(t *testing.T) {
	if mustDecode(t, 'C', []byte("INSERT 0 1\x00")).Kind != msgCommandComplete {
		t.Fatal("CommandComplete")
	}
	if mustDecode(t, 'I', nil).Kind != msgEmptyQueryResponse {
		t.Fatal("EmptyQueryResponse")
	}
	if mustDecode(t, 'n', nil).Kind != msgNoData {
		t.Fatal("NoData")
	}
	if mustDecode(t, '1', nil).Kind != msgParseComplete {
		t.Fatal("ParseComplete")
	}
	rf := mustDecode(t, 'E', []byte{'M', 'b', 'o', 'o', 'm', 0, 0})
	if rf.Kind != msgErrorResponse || rf.ErrMsg != "boom" {
		t.Fatalf("ErrorResponse = %+v", rf)
	}
	rq := mustDecode(t, 'Z', []byte{'T'})
	if rq.Kind != msgReadyForQuery || rq.ReadyStatus != 'T' {
		t.Fatalf("ReadyForQuery = %+v", rq)
	}
}

func TestReadCStrAndParseErrorFields(t *testing.T) {
	buf := []byte("ab\x00cd")
	pos := 0
	s := readCStr(buf, &pos)
	if s != "ab" || pos != 3 {
		t.Fatalf("readCStr = (%q,%d), want (ab,3)", s, pos)
	}
	body := []byte{'S', 's', 'e', 'v', 0, 'M', 'b', 'o', 'o', 'm', 0, 0}
	if got := parseErrorFields(body); got != "boom" {
		t.Fatalf("parseErrorFields = %q, want boom", got)
	}
	if got := parseErrorFields([]byte{'M', 0, 0}); got != "unknown server error" {
		t.Fatalf("parseErrorFields = %q, want fallback", got)
	}
}

func TestSniffValueAndNormalizeBool(t *testing.T) {
	if v := sniffValue("t"); v != true {
		t.Fatalf("sniffValue(t) = %v", v)
	}
	if v := sniffValue("42"); v != int64(42) {
		t.Fatalf("sniffValue(42) = %v", v)
	}
	if v := sniffValue("3.5"); v != 3.5 {
		t.Fatalf("sniffValue(3.5) = %v", v)
	}
	if v := sniffValue("x"); v != "x" {
		t.Fatalf("sniffValue(x) = %v", v)
	}
	if normalizeBool("t") != "true" || normalizeBool("f") != "false" || normalizeBool("yes") != "yes" {
		t.Fatal("normalizeBool")
	}
}

func TestParseRowsAffected(t *testing.T) {
	cases := map[string]int64{
		"INSERT 0 5": 5,
		"DELETE 10":  10,
		"UPDATE 2":    2,
		"SELECT 3":    3,
		"":            0,
		"GARBAGE":     0,
	}
	for tag, want := range cases {
		if got := parseRowsAffected(tag); got != want {
			t.Errorf("parseRowsAffected(%q) = %d, want %d", tag, got, want)
		}
	}
}

// --- error mapping --------------------------------------------------------

func TestServerErrorFormatting(t *testing.T) {
	if got := (&ServerError{Message: "boom"}).Error(); got != "keystone: server error: boom" {
		t.Fatalf("ServerError.Error() = %q", got)
	}
}

func TestConnClosedAndErrConnClosedAreDistinct(t *testing.T) {
	// errConnClosed is the wire-layer closed-by-peer sentinel; ErrClosed is
	// the user-facing "this Conn can no longer be used" error.
	if errors.Is(ErrClosed, errConnClosed) {
		t.Fatal("ErrClosed and errConnClosed should be distinct types")
	}
}

// --- scan mapping ---------------------------------------------------------

func TestScanOneMapsTextToDestinations(t *testing.T) {
	hi := []byte("hi")
	var s string
	if err := scanOne(&hi, &s); err != nil || s != "hi" {
		t.Fatalf("scan *string: err=%v s=%q", err, s)
	}

	idBytes := []byte("123")
	var id int64
	if err := scanOne(&idBytes, &id); err != nil || id != 123 {
		t.Fatalf("scan *int64: err=%v id=%v", err, id)
	}

	tBytes := []byte("t")
	var ok bool
	if err := scanOne(&tBytes, &ok); err != nil || !ok {
		t.Fatalf("scan *bool: err=%v ok=%v", err, ok)
	}

	fortyTwo := []byte("42")
	var anyVal any
	if err := scanOne(&fortyTwo, &anyVal); err != nil {
		t.Fatalf("scan *any: %v", err)
	}
	if anyVal != int64(42) {
		t.Fatalf("scan *any = %v (%T), want int64(42)", anyVal, anyVal)
	}

	// NULL leaves the destination at its zero value (except *any, which
	// receives nil so callers can distinguish NULL).
	var ns string
	_ = scanOne(nil, &ns)
	if ns != "" {
		t.Fatalf("NULL *string should stay zero value, got %q", ns)
	}
	var anyNil any = "preset"
	_ = scanOne(nil, &anyNil)
	if anyNil != nil {
		t.Fatalf("NULL *any should become nil, got %v", anyNil)
	}
}
