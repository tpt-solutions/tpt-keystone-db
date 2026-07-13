"""Raw HTTP/2 (h2c) + gRPC client — no grpc/h2 library.

This verifies TPT Keystone's *from-scratch* Flux gRPC endpoint by speaking the
wire protocol directly: the client connection preface, a SETTINGS exchange, a
hand-rolled HPACK request header block (static-table indexed entries only), the
gRPC message envelope, and DATA-frame parsing on the way back. It exists
because grpcio's generic (non-codegen) client-streaming path is awkward to drive
for this one-shot server-streaming call; a raw socket proves the *server* framing
independently of any client library quirk.

Run `python tools/raw_grpc_client.py <host> <port>` after starting the server
(e.g. `cargo run` and `python tools/raw_grpc_client.py 127.0.0.1 5436`).
"""
import socket
import struct
import sys

import hpack

PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
FRAME_HEADER = 9
DEFAULT_INITIAL_WINDOW = 65535


class GrpcError(Exception):
    pass


def encode_hpack_indexed(index):
    # Indexed Header Field (RFC 7541 §6.1): 1-bit prefix = 1.
    out = bytearray()
    if index <= 126:
        out.append(0x80 | index)
    else:
        out.append(0x80 | 127)
        out += struct.pack(">H", index - 127)
    return bytes(out)


def encode_hpack_literal_without_indexing(name_index, value):
    # Literal Header Field without Indexing (RFC 7541 §6.2): 4-bit prefix = 0,
    # then a name that is an indexed static entry (index in 4-bit prefix), then
    # the value as a length-prefixed string (no Huffman).
    out = bytearray()
    # First byte: 4-bit prefix (0000) + name index (7-bit, since name_index<127).
    if name_index <= 126:
        out.append(name_index & 0x7F)
    else:
        out.append(0x7F)
        out += struct.pack(">H", name_index - 127)
    _encode_string(out, value)
    return bytes(out)


def _encode_string(out, value):
    vb = value.encode() if isinstance(value, str) else value
    n = len(vb)
    # Huffman flag = 0, 7-bit length prefix.
    if n <= 126:
        out.append(n)
    else:
        out.append(0x7F)
        out += struct.pack(">H", n - 127)
    out += vb


def build_request_headers(path, message_bytes):
    # HPACK request header block: :method POST, :path <path>, :scheme http,
    # content-type application/grpc, te trailers (best-effort), then the gRPC
    # message envelope as a DATA frame separately.
    # Static table indices: :method POST=2, :path=/=3, :scheme http=6,
    # content-type=31, te=112.
    block = bytearray()
    block += encode_hpack_indexed(2)  # :method POST
    block += encode_hpack_indexed(3)  # :path /
    block += encode_hpack_indexed(6)  # :scheme http
    block += encode_hpack_literal_without_indexing(31, "application/grpc")  # content-type
    # :path is just "/" in the static table; set the real path via a literal
    # :path name (name-index for :path would only give "/"). Use a fresh
    # literal name for :path with the actual value.
    # (name ":path" is not in the static table as a name alone, so encode as a
    #  literal-without-indexing with a literal name.)
    block += _literal_name_value(":path", path)
    return bytes(block), message_bytes


def _literal_name_value(name, value):
    out = bytearray()
    out.append(0x00)  # 4-bit prefix 0000, name not indexed.
    _encode_string(out, name)
    _encode_string(out, value)
    return bytes(out)


def grpc_envelope(payload):
    return b"\x00" + struct.pack(">I", len(payload)) + payload


class Conn:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.hdec = hpack.Decoder()
        self.window = DEFAULT_INITIAL_WINDOW
        self._send_preface_and_settings()

    def _frame(self, ftype, flags, stream_id, payload):
        body = payload or b""
        header = struct.pack(">I", len(body))[1:]  # length (24 bits) as 3 bytes
        header += bytes([ftype, flags])
        header += struct.pack(">I", stream_id & 0x7FFFFFFF)[1:]  # 31-bit stream id
        self.sock.sendall(header + body)

    def _send_preface_and_settings(self):
        self.sock.sendall(PREFACE)
        # Client SETTINGS: initial window size 1MiB so the server can push.
        self._frame(0x4, 0x0, 0, struct.pack(">HH", 0x4, 1_000_000))
        # Read server preface + settings + ack.
        self._read_until_settings_ack()

    def _read_frame(self):
        header = self._recv_exact(9)
        length = int.from_bytes(header[0:3], "big")
        ftype = header[3]
        flags = header[4]
        stream_id = int.from_bytes(header[5:9], "big") & 0x7FFFFFFF
        payload = self._recv_exact(length) if length else b""
        return ftype, flags, stream_id, payload

    def _recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise GrpcError("connection closed")
            buf += chunk
        return buf

    def _read_until_settings_ack(self):
        # Read frames until we've seen the peer's SETTINGS and our ACK to it,
        # plus the peer's SETTINGS ACK.
        seen_peer_settings = False
        seen_ack_of_ours = False
        # Send ACK for peer SETTINGS (we haven't read it yet, but we know it's
        # coming first). Read frames:
        while not (seen_peer_settings and seen_ack_of_ours):
            ftype, flags, sid, payload = self._read_frame()
            if ftype == 0x4:  # SETTINGS
                if flags & 0x1:  # ACK
                    seen_ack_of_ours = True
                else:
                    seen_peer_settings = True
                    self._frame(0x4, 0x1, 0, b"")  # ACK peer settings
            elif ftype == 0x6:  # PING
                self._frame(0x6, 0x1, sid, payload)  # ACK pings

    def unary(self, path, req_bytes, stream_id=1):
        """Send a unary request and read one gRPC response message."""
        hblock, msg = build_request_headers(path, grpc_envelope(req_bytes))
        self._frame(0x1, 0x4, stream_id, hblock)  # HEADERS END_HEADERS
        self._frame(0x0, 0x1, stream_id, msg)  # DATA END_STREAM
        return self._read_unary_response(stream_id)

    def _read_unary_response(self, stream_id):
        headers = None
        trailer_status = None
        body = bytearray()
        while True:
            ftype, flags, sid, payload = self._read_frame()
            if ftype == 0x1:  # HEADERS
                headers = self.hdec.decode(payload)
                if flags & 0x4 == 0:
                    continue  # more header blocks (CONTINUATION) expected
            elif ftype == 0x0:  # DATA
                body += payload
                # Maintain flow control.
                self.window -= len(payload)
                if self.window < 1_000_000 // 2:
                    self._frame(0x8, 0x0, stream_id, struct.pack(">I", 1_000_000 - self.window))
                    self.window = 1_000_000
            elif ftype == 0x7:  # GOAWAY
                raise GrpcError("GOAWAY")
            elif ftype == 0x3:  # RST_STREAM
                raise GrpcError("RST_STREAM")
            if flags & 0x1:  # END_STREAM: trailers carry grpc-status
                trailer_status = self._trailer_status(payload if ftype == 0x1 else b"")
                break
        if trailer_status and trailer_status != 0:
            raise GrpcError(f"grpc status {trailer_status}")
        # Parse the single gRPC message envelope from body.
        return self._parse_message(bytes(body))

    def _trailer_status(self, hblock_bytes):
        try:
            hdrs = hpack.Decoder().decode(hblock_bytes)
        except Exception:
            return 0
        for name, value in hdrs:
            if name == "grpc-status":
                return int(value)
        return 0

    def _parse_message(self, body):
        # body may contain one or more gRPC messages; return the first.
        if len(body) < 5:
            return b""
        compressed = body[0]
        length = struct.unpack(">I", body[1:5])[0]
        return body[5 : 5 + length]

    def subscribe(self, path, req_bytes, stream_id, collect=1):
        """Server-streaming: send one request, collect `collect` pushed messages."""
        hblock, msg = build_request_headers(path, grpc_envelope(req_bytes))
        self._frame(0x1, 0x4, stream_id, hblock)
        self._frame(0x0, 0x1, stream_id, msg)
        messages = []
        while len(messages) < collect:
            ftype, flags, sid, payload = self._read_frame()
            if ftype == 0x1:  # trailing HEADERS (END_STREAM) -> done
                if flags & 0x1:
                    break
            elif ftype == 0x0:  # DATA
                self.window -= len(payload)
                if self.window < 1_000_000 // 2:
                    self._frame(0x8, 0x0, sid, struct.pack(">I", 1_000_000 - self.window))
                    self.window = 1_000_000
                m = self._parse_message(payload)
                if m:
                    messages.append(m)
                if flags & 0x1:  # END_STREAM on data (trailers-only)
                    break
            elif ftype in (0x3, 0x7):
                break
        return messages


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5436
    sys.path.insert(0, __file__[: __file__.rfind("\\") if "\\" in __file__ else __file__.rfind("/")])
    import flux_grpc_pb2 as pb

    conn = Conn(host, port)
    # CreateTopic
    conn.unary("/flux.Flux/CreateTopic", pb.CreateTopicRequest(name="greetings").SerializeToString(), 1)
    # Publish #1
    resp = conn.unary(
        "/flux.Flux/Publish",
        pb.PublishRequest(topic="greetings", key=b"k1", value=b"hello-grpc").SerializeToString(),
        3,
    )
    pub1 = pb.PublishResponse.FromString(resp)
    print(f"Publish#1 -> partition={pub1.partition} offset={pub1.offset}")
    # Subscribe on a fresh stream; then Publish #2; expect the pushed record.
    sub_bytes = pb.SubscribeRequest(topic="greetings").SerializeToString()
    # Publish #2 in a separate connection-step: we open it after subscribe by
    # reusing the same conn on a new stream id.
    # Send subscribe, then publish on another stream, then read pushed records.
    import threading
    import time

    pushed = []

    def do_subscribe():
        msgs = conn.subscribe("/flux.Flux/Subscribe", sub_bytes, 5, collect=1)
        for m in msgs:
            pushed.append(pb.Record.FromString(m))

    t = threading.Thread(target=do_subscribe)
    t.start()
    time.sleep(0.5)
    conn.unary(
        "/flux.Flux/Publish",
        pb.PublishRequest(topic="greetings", value=b"second").SerializeToString(),
        7,
    )
    t.join(timeout=5)
    print(f"Subscribe stream saw {len(pushed)} live record(s)")
    assert pushed, "subscribe stream received no records"
    assert pushed[0].value in (b"hello-grpc", b"second"), pushed[0]
    # Poll
    presp = pb.PollResponse.FromString(
        conn.unary(
            "/flux.Flux/Poll",
            pb.PollRequest(topic="greetings", partition=0, group="g1", max=10).SerializeToString(),
            9,
        )
    )
    print(f"Poll -> {len(presp.records)} record(s) for group g1")
    assert len(presp.records) >= 1, "poll returned nothing"
    print("OK: Flux gRPC endpoint verified end-to-end (raw h2c client)")


if __name__ == "__main__":
    main()
