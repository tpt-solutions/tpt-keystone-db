package keystone

// Hand-written client-side codec for the Postgres wire protocol v3 that
// tpt-keystone/src/wire implements server-side. This mirrors
// tpt-sdk/src/keystone/wire.rs (the Rust SDK's client codec) message-for-
// message: frontend (client -> server) messages are *encoded* here, backend
// (server -> client) messages are *decoded* here. Only the subset of the
// protocol this SDK needs is implemented: startup, the simple query loop,
// and the extended query subset (Parse/Bind/Describe/Execute/Sync) needed
// for parameterized and streaming queries. All formats are text (format
// code 0) — there is no binary-format support, matching the Rust SDK.
//
// No third-party Postgres driver (lib/pq, jackc/pgx, ...) is used anywhere
// in this module by design — see the project's CLAUDE.md.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// fieldDescription describes one result column, as sent in a RowDescription
// message.
type fieldDescription struct {
	Name    string
	TypeOID int32
}

// msgKind tags which variant a decoded backendMessage holds.
type msgKind int

const (
	msgAuthenticationOk msgKind = iota
	msgParameterStatus
	msgBackendKeyData
	msgReadyForQuery
	msgRowDescription
	msgDataRow
	msgCommandComplete
	msgErrorResponse
	msgNoticeResponse
	msgParseComplete
	msgBindComplete
	msgCloseComplete
	msgParameterDescription
	msgNoData
	msgPortalSuspended
	msgEmptyQueryResponse
	msgUnknown
)

// backendMessage is a decoded server->client message. Only the fields
// relevant to msgKind are populated; this plays the role of the Rust SDK's
// `enum BackendMessage`.
type backendMessage struct {
	kind kindAndPayload
}

// kindAndPayload avoids needing one struct field per variant scattered
// across call sites; fields are still named per-variant for clarity.
type kindAndPayload struct {
	Kind        msgKind
	ParamName   string
	ParamValue  string
	PID         int32
	Secret      int32
	ReadyStatus byte
	Fields      []fieldDescription
	Row         []*[]byte // nil element == SQL NULL
	CommandTag  string
	ErrMsg      string
	ParamTypes  []int32
	UnknownTag  byte
}

var errConnClosed = errors.New("keystone: connection closed by server")

// wireConn is the low-level framer: it knows how to write frontend messages
// into a buffer and decode backend messages from a bufio.Reader. It does
// not know about contexts; deadline/cancellation plumbing lives in Conn
// (conn.go), which wraps wireConn.
type wireConn struct {
	nc      net.Conn
	r       *bufio.Reader
	wbuf    []byte
	hdrbuf  [5]byte
	lenbuf  [4]byte
	readBuf []byte // reused scratch buffer for message bodies
}

func newWireConn(nc net.Conn) *wireConn {
	return &wireConn{nc: nc, r: bufio.NewReaderSize(nc, 16*1024)}
}

// --- frontend message encoding -------------------------------------------------

func (w *wireConn) resetWrite() {
	w.wbuf = w.wbuf[:0]
}

func putI32(buf []byte, v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return append(buf, b[:]...)
}

func putI16(buf []byte, v int16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	return append(buf, b[:]...)
}

func putCStr(buf []byte, s string) []byte {
	buf = append(buf, s...)
	buf = append(buf, 0)
	return buf
}

// writeStartup queues the StartupMessage (no auth type byte — it's the one
// message with no leading tag byte).
func (w *wireConn) writeStartup(params map[string]string) {
	var body []byte
	body = putI32(body, 196608) // protocol version 3.0
	for k, v := range params {
		body = putCStr(body, k)
		body = putCStr(body, v)
	}
	body = append(body, 0)
	w.wbuf = putI32(w.wbuf, int32(4+len(body)))
	w.wbuf = append(w.wbuf, body...)
}

func (w *wireConn) writeMsg(tag byte, body []byte) {
	w.wbuf = append(w.wbuf, tag)
	w.wbuf = putI32(w.wbuf, int32(4+len(body)))
	w.wbuf = append(w.wbuf, body...)
}

func (w *wireConn) writeQuery(sql string) {
	w.writeMsg('Q', putCStr(nil, sql))
}

func (w *wireConn) writeParse(name, sql string, paramTypes []int32) {
	var b []byte
	b = putCStr(b, name)
	b = putCStr(b, sql)
	b = putI16(b, int16(len(paramTypes)))
	for _, t := range paramTypes {
		b = putI32(b, t)
	}
	w.writeMsg('P', b)
}

func (w *wireConn) writeBind(portal, stmt string, params []*[]byte) {
	var b []byte
	b = putCStr(b, portal)
	b = putCStr(b, stmt)
	b = putI16(b, 1)
	b = putI16(b, 0) // all params text format
	b = putI16(b, int16(len(params)))
	for _, p := range params {
		if p == nil {
			b = putI32(b, -1)
			continue
		}
		b = putI32(b, int32(len(*p)))
		b = append(b, (*p)...)
	}
	b = putI16(b, 1)
	b = putI16(b, 0) // all results text format
	w.writeMsg('B', b)
}

func (w *wireConn) writeDescribePortal(name string) {
	b := []byte{'P'}
	b = putCStr(b, name)
	w.writeMsg('D', b)
}

func (w *wireConn) writeExecute(portal string, maxRows int32) {
	b := putCStr(nil, portal)
	b = putI32(b, maxRows)
	w.writeMsg('E', b)
}

func (w *wireConn) writeSync() {
	w.writeMsg('S', nil)
}

func (w *wireConn) writeTerminate() {
	w.writeMsg('X', nil)
}

// flush blocks writing the queued bytes; callers apply ctx deadlines before
// calling this (see conn.go).
func (w *wireConn) flush() error {
	if len(w.wbuf) == 0 {
		return nil
	}
	_, err := w.nc.Write(w.wbuf)
	w.resetWrite()
	return err
}

// --- backend message decoding -------------------------------------------------

func readCStr(data []byte, pos *int) string {
	start := *pos
	end := start
	for end < len(data) && data[end] != 0 {
		end++
	}
	s := string(data[start:end])
	if end < len(data) {
		end++ // skip the nul
	}
	*pos = end
	return s
}

func parseErrorFields(data []byte) string {
	pos := 0
	message := ""
	for pos < len(data) {
		code := data[pos]
		pos++
		if code == 0 {
			break
		}
		val := readCStr(data, &pos)
		if code == 'M' {
			message = val
		}
	}
	if message == "" {
		message = "unknown server error"
	}
	return message
}

// readMessage blocks reading exactly one backend message; callers apply ctx
// deadlines/cancellation before calling this (see conn.go's withDeadline).
func (w *wireConn) readMessage() (kindAndPayload, error) {
	if _, err := readFull(w.r, w.hdrbuf[:5]); err != nil {
		return kindAndPayload{}, err
	}
	tag := w.hdrbuf[0]
	length := int32(binary.BigEndian.Uint32(w.hdrbuf[1:5]))
	if length < 4 {
		return kindAndPayload{}, fmt.Errorf("keystone: invalid message length %d for tag %q", length, tag)
	}
	bodyLen := int(length) - 4
	if cap(w.readBuf) < bodyLen {
		w.readBuf = make([]byte, bodyLen)
	}
	body := w.readBuf[:bodyLen]
	if bodyLen > 0 {
		if _, err := readFull(w.r, body); err != nil {
			return kindAndPayload{}, err
		}
	}
	return decodeBody(tag, body)
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			if n == len(buf) {
				break
			}
			if errors.Is(err, io.EOF) && n == 0 {
				return n, errConnClosed
			}
			return n, err
		}
	}
	return n, nil
}

func decodeBody(tag byte, data []byte) (kindAndPayload, error) {
	pos := 0
	switch tag {
	case 'R':
		return kindAndPayload{Kind: msgAuthenticationOk}, nil
	case 'S':
		name := readCStr(data, &pos)
		value := readCStr(data, &pos)
		return kindAndPayload{Kind: msgParameterStatus, ParamName: name, ParamValue: value}, nil
	case 'K':
		pid := int32(binary.BigEndian.Uint32(data[0:4]))
		secret := int32(binary.BigEndian.Uint32(data[4:8]))
		return kindAndPayload{Kind: msgBackendKeyData, PID: pid, Secret: secret}, nil
	case 'Z':
		status := byte('I')
		if len(data) > 0 {
			status = data[0]
		}
		return kindAndPayload{Kind: msgReadyForQuery, ReadyStatus: status}, nil
	case 'T':
		n := int(binary.BigEndian.Uint16(data[0:2]))
		pos = 2
		fields := make([]fieldDescription, 0, n)
		for i := 0; i < n; i++ {
			name := readCStr(data, &pos)
			pos += 4 // table oid
			pos += 2 // col attr
			typeOID := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			pos += 2 // type size
			pos += 4 // type modifier
			pos += 2 // format code
			fields = append(fields, fieldDescription{Name: name, TypeOID: typeOID})
		}
		return kindAndPayload{Kind: msgRowDescription, Fields: fields}, nil
	case 'D':
		n := int(binary.BigEndian.Uint16(data[0:2]))
		pos = 2
		cols := make([]*[]byte, 0, n)
		for i := 0; i < n; i++ {
			l := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if l < 0 {
				cols = append(cols, nil)
				continue
			}
			take := int(l)
			if pos+take > len(data) {
				take = len(data) - pos
			}
			cell := make([]byte, take)
			copy(cell, data[pos:pos+take])
			pos += take
			cols = append(cols, &cell)
		}
		return kindAndPayload{Kind: msgDataRow, Row: cols}, nil
	case 'C':
		return kindAndPayload{Kind: msgCommandComplete, CommandTag: readCStr(data, &pos)}, nil
	case 'E':
		return kindAndPayload{Kind: msgErrorResponse, ErrMsg: parseErrorFields(data)}, nil
	case 'N':
		return kindAndPayload{Kind: msgNoticeResponse, ErrMsg: parseErrorFields(data)}, nil
	case '1':
		return kindAndPayload{Kind: msgParseComplete}, nil
	case '2':
		return kindAndPayload{Kind: msgBindComplete}, nil
	case '3':
		return kindAndPayload{Kind: msgCloseComplete}, nil
	case 't':
		n := int(binary.BigEndian.Uint16(data[0:2]))
		pos = 2
		types := make([]int32, 0, n)
		for i := 0; i < n; i++ {
			types = append(types, int32(binary.BigEndian.Uint32(data[pos:pos+4])))
			pos += 4
		}
		return kindAndPayload{Kind: msgParameterDescription, ParamTypes: types}, nil
	case 'n':
		return kindAndPayload{Kind: msgNoData}, nil
	case 's':
		return kindAndPayload{Kind: msgPortalSuspended}, nil
	case 'I':
		return kindAndPayload{Kind: msgEmptyQueryResponse}, nil
	default:
		return kindAndPayload{Kind: msgUnknown, UnknownTag: tag}, nil
	}
}
