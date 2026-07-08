"""Client-side codec for the hand-written Postgres wire protocol v3 that
``tpt-keystone/src/wire/codec.rs`` implements server-side. This is a direct
Python port of the Rust SDK's ``tpt-sdk/src/keystone/wire.rs`` — read that
file's module doc first if you're comparing the two.

Only the subset of the protocol this SDK needs is implemented: the startup
handshake, the simple query protocol, and the extended query subset
(Parse/Bind/Describe/Execute/Sync) needed for parameterized queries. All
formats are text (format code 0) — there is no binary-format support, no
SASL/password auth (Keystone's startup handshake auto-approves any
``user`` parameter), and no SSL negotiation.

This module is deliberately dependency-free (stdlib ``asyncio`` only) — no
``psycopg2``/``asyncpg``/other Postgres driver. Pulling in a real Postgres
driver here would defeat the entire point of this project: every wire
protocol implementation (server *and* client) is hand-written from scratch.
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass, field
from typing import Optional


class WireError(Exception):
    """Raised for malformed frames or a connection that closed unexpectedly."""


class ServerError(Exception):
    """Raised when the server sends an ErrorResponse. Carries the
    human-readable message extracted from the 'M' field of the error
    fields (see ``parse_error_fields`` below); other fields (SQLSTATE code,
    severity, etc.) are not surfaced — matching the Rust SDK's scope cut.
    """


@dataclass
class FieldDescription:
    name: str
    type_oid: int


@dataclass
class RowDescriptionMsg:
    fields: list[FieldDescription]


@dataclass
class DataRowMsg:
    cells: list[Optional[bytes]]


@dataclass
class CommandCompleteMsg:
    tag: str


@dataclass
class ErrorResponseMsg:
    message: str


@dataclass
class NoticeResponseMsg:
    message: str


@dataclass
class ReadyForQueryMsg:
    status: int


@dataclass
class ParameterStatusMsg:
    name: str
    value: str


@dataclass
class BackendKeyDataMsg:
    pid: int
    secret: int


@dataclass
class ParameterDescriptionMsg:
    type_oids: list[int]


class AuthenticationOkMsg:
    pass


class ParseCompleteMsg:
    pass


class BindCompleteMsg:
    pass


class CloseCompleteMsg:
    pass


class NoDataMsg:
    pass


class PortalSuspendedMsg:
    pass


class EmptyQueryResponseMsg:
    pass


@dataclass
class UnknownMsg:
    tag: int


BackendMessage = object  # union of the *Msg classes above


def _read_cstr(buf: bytes, offset: int) -> tuple[str, int]:
    end = buf.index(b"\x00", offset)
    return buf[offset:end].decode("utf-8", errors="replace"), end + 1


def _parse_error_fields(buf: bytes, offset: int) -> str:
    """Error/notice fields are a sequence of (u8 field_code, cstr value)
    pairs terminated by a nul byte; we only surface the 'M' (message) field,
    matching the Rust SDK.
    """
    message: Optional[str] = None
    while offset < len(buf):
        code = buf[offset]
        offset += 1
        if code == 0:
            break
        value, offset = _read_cstr(buf, offset)
        if code == ord("M"):
            message = value
    return message if message is not None else "unknown server error"


class Conn:
    """One TCP connection to a Keystone node, speaking the wire protocol
    directly over ``asyncio`` streams (no connection pooling, no retries —
    a thin transport, same scope as the Rust SDK's ``Conn``).
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._write_buf = bytearray()

    @classmethod
    async def connect(cls, host: str, port: int, params: dict[str, str]) -> "Conn":
        reader, writer = await asyncio.open_connection(host, port)
        conn = cls(reader, writer)
        conn._write_startup(params)
        await conn.flush()

        while True:
            msg = await conn.read_message()
            if isinstance(msg, AuthenticationOkMsg):
                continue
            if isinstance(msg, ReadyForQueryMsg):
                break
            if isinstance(msg, ErrorResponseMsg):
                writer.close()
                raise ServerError(f"startup rejected: {msg.message}")
        return conn

    # -- message writers -------------------------------------------------

    def _write_startup(self, params: dict[str, str]) -> None:
        body = bytearray()
        body += struct.pack("!i", 196608)  # protocol version 3.0
        for k, v in params.items():
            body += k.encode("utf-8") + b"\x00"
            body += v.encode("utf-8") + b"\x00"
        body += b"\x00"
        self._write_buf += struct.pack("!i", 4 + len(body))
        self._write_buf += body

    def write_query(self, sql: str) -> None:
        def body(b: bytearray) -> None:
            b += sql.encode("utf-8") + b"\x00"

        self._write_msg(ord("Q"), body)

    def write_parse(self, name: str, sql: str, param_types: list[int]) -> None:
        def body(b: bytearray) -> None:
            b += name.encode("utf-8") + b"\x00"
            b += sql.encode("utf-8") + b"\x00"
            b += struct.pack("!h", len(param_types))
            for ty in param_types:
                b += struct.pack("!i", ty)

        self._write_msg(ord("P"), body)

    def write_bind(self, portal: str, stmt: str, params: list[Optional[bytes]]) -> None:
        def body(b: bytearray) -> None:
            b += portal.encode("utf-8") + b"\x00"
            b += stmt.encode("utf-8") + b"\x00"
            b += struct.pack("!h", 1)
            b += struct.pack("!h", 0)  # all params text format
            b += struct.pack("!h", len(params))
            for p in params:
                if p is None:
                    b += struct.pack("!i", -1)
                else:
                    b += struct.pack("!i", len(p))
                    b += p
            b += struct.pack("!h", 1)
            b += struct.pack("!h", 0)  # all results text format

        self._write_msg(ord("B"), body)

    def write_describe_portal(self, name: str) -> None:
        def body(b: bytearray) -> None:
            b += b"P"
            b += name.encode("utf-8") + b"\x00"

        self._write_msg(ord("D"), body)

    def write_execute(self, portal: str, max_rows: int) -> None:
        def body(b: bytearray) -> None:
            b += portal.encode("utf-8") + b"\x00"
            b += struct.pack("!i", max_rows)

        self._write_msg(ord("E"), body)

    def write_sync(self) -> None:
        self._write_msg(ord("S"), lambda b: None)

    def write_terminate(self) -> None:
        self._write_msg(ord("X"), lambda b: None)

    def _write_msg(self, tag: int, body_fn) -> None:
        b = bytearray()
        body_fn(b)
        self._write_buf.append(tag)
        self._write_buf += struct.pack("!i", 4 + len(b))
        self._write_buf += b

    async def flush(self) -> None:
        self._writer.write(bytes(self._write_buf))
        await self._writer.drain()
        self._write_buf.clear()

    # -- message reader ----------------------------------------------------

    async def read_message(self) -> BackendMessage:
        header = await self._readexactly(5)
        tag = header[0]
        (length,) = struct.unpack("!i", header[1:5])
        if length < 4:
            raise WireError(f"invalid message length {length} for tag {chr(tag)!r}")
        body = await self._readexactly(length - 4)

        offset = 0

        if tag == ord("R"):
            return AuthenticationOkMsg()
        if tag == ord("S"):
            name, offset = _read_cstr(body, offset)
            value, offset = _read_cstr(body, offset)
            return ParameterStatusMsg(name, value)
        if tag == ord("K"):
            pid, secret = struct.unpack("!ii", body[:8])
            return BackendKeyDataMsg(pid, secret)
        if tag == ord("Z"):
            status = body[0] if body else ord("I")
            return ReadyForQueryMsg(status)
        if tag == ord("T"):
            (n,) = struct.unpack("!h", body[offset : offset + 2])
            offset += 2
            fields = []
            for _ in range(n):
                name, offset = _read_cstr(body, offset)
                # table_oid(i32), col_attr(i16), type_oid(i32), type_size(i16),
                # type_modifier(i32), format(i16)
                _table_oid, _col_attr, type_oid, _type_size, _type_modifier, _format = struct.unpack(
                    "!ihihih", body[offset : offset + 18]
                )
                offset += 18
                fields.append(FieldDescription(name, type_oid))
            return RowDescriptionMsg(fields)
        if tag == ord("D"):
            (n,) = struct.unpack("!h", body[offset : offset + 2])
            offset += 2
            cells: list[Optional[bytes]] = []
            for _ in range(n):
                (clen,) = struct.unpack("!i", body[offset : offset + 4])
                offset += 4
                if clen < 0:
                    cells.append(None)
                else:
                    cells.append(body[offset : offset + clen])
                    offset += clen
            return DataRowMsg(cells)
        if tag == ord("C"):
            tag_str, offset = _read_cstr(body, offset)
            return CommandCompleteMsg(tag_str)
        if tag == ord("E"):
            return ErrorResponseMsg(_parse_error_fields(body, offset))
        if tag == ord("N"):
            return NoticeResponseMsg(_parse_error_fields(body, offset))
        if tag == ord("1"):
            return ParseCompleteMsg()
        if tag == ord("2"):
            return BindCompleteMsg()
        if tag == ord("3"):
            return CloseCompleteMsg()
        if tag == ord("t"):
            (n,) = struct.unpack("!h", body[offset : offset + 2])
            offset += 2
            types = []
            for _ in range(n):
                (ty,) = struct.unpack("!i", body[offset : offset + 4])
                offset += 4
                types.append(ty)
            return ParameterDescriptionMsg(types)
        if tag == ord("n"):
            return NoDataMsg()
        if tag == ord("s"):
            return PortalSuspendedMsg()
        if tag == ord("I"):
            return EmptyQueryResponseMsg()
        return UnknownMsg(tag)

    async def _readexactly(self, n: int) -> bytes:
        try:
            return await self._reader.readexactly(n)
        except asyncio.IncompleteReadError as exc:
            raise WireError("connection closed by server") from exc

    def close(self) -> None:
        self._writer.close()

    async def wait_closed(self) -> None:
        await self._writer.wait_closed()
