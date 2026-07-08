"""Async Keystone client — the Python counterpart of the Rust SDK's
``tpt-sdk/src/keystone/mod.rs`` (``KeystoneClient``). Talks the same
hand-written Postgres wire protocol v3 as ``tpt-keystone/src/wire`` (server
side) and ``tpt_sdk.wire`` (client-side codec, this package). Plain TCP via
``asyncio.open_connection`` — no ``psycopg2``/``asyncpg``.

Matches the example in ``9sdkspec.txt`` section 4:

    client = KeystoneClient(host="localhost", port=5432)
    await client.connect()
    result = await client.query("SELECT * FROM robots WHERE id = $1", params=[42])
    for row in result.rows:
        print(row.id)  # attribute access, per the spec's example

Scope cuts (same ethos as this repo's other SDKs — say what's not done):

- Text format only, no binary wire format.
- No SSL/TLS negotiation, no auth (Keystone's startup handshake
  auto-approves any ``user``; there's no server-side auth to build against).
- ``Value.from_text`` is a best-effort scalar sniff (bool/int/float/text),
  not a full type-OID-aware decode, exactly like the Rust SDK's ``Value``.
  ``QueryResult.to_pandas()`` instead does real catalog-driven coercion
  (see ``pandas_ext.py``) since a DataFrame benefits from real dtypes.
- No connection pooling, no automatic reconnect/retry.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Any, Iterator, Optional, Sequence, Union

from . import wire
from .wire import Conn

ParamValue = Union[None, bool, int, float, str, bytes]


class KeystoneError(Exception):
    """Base class for errors raised by this SDK."""


class ServerError(KeystoneError):
    """The server sent an ErrorResponse; ``args[0]`` is its message text."""


class ConnectionNotOpen(KeystoneError):
    """Raised when a client method is called before ``connect()`` or after ``close()``."""


def _encode_param(value: ParamValue) -> Optional[bytes]:
    """Encode a Python value as a wire text-format parameter, mirroring the
    Rust SDK's ``Value::to_param``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return b"t" if value else b"f"
    if isinstance(value, bytes):
        return value
    if isinstance(value, (int, float)):
        return str(value).encode("utf-8")
    return str(value).encode("utf-8")


def _decode_scalar(text: Optional[str]) -> Any:
    """Best-effort text -> Python scalar, mirroring the Rust SDK's
    ``Value::from_text``. No type-OID catalog lookup here (see module doc)."""
    if text is None:
        return None
    if text in ("t", "true"):
        return True
    if text in ("f", "false"):
        return False
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


@dataclass
class Row:
    """One decoded row. Supports both attribute access (``row.id``, per
    ``9sdkspec.txt``'s example) and ``row["id"]`` / positional ``row[0]``.
    """

    columns: list[str]
    values: list[Any]

    def __getattr__(self, name: str) -> Any:
        try:
            idx = object.__getattribute__(self, "columns").index(name)
        except ValueError as exc:
            raise AttributeError(name) from exc
        return object.__getattribute__(self, "values")[idx]

    def __getitem__(self, key: Union[int, str]) -> Any:
        if isinstance(key, int):
            return self.values[key]
        return self.values[self.columns.index(key)]

    def as_dict(self) -> dict[str, Any]:
        return dict(zip(self.columns, self.values))

    def __repr__(self) -> str:
        return f"Row({self.as_dict()!r})"


class QueryResult:
    """Result of a query: columns, decoded rows, and (for DML) the raw
    command tag (e.g. ``"INSERT 0 1"``).
    """

    def __init__(self, columns: list[str], raw_rows: list[list[Optional[bytes]]], command_tag: Optional[str]):
        self.columns = columns
        self.command_tag = command_tag
        self._raw_rows = raw_rows
        self._rows: Optional[list[Row]] = None

    @property
    def rows(self) -> list[Row]:
        if self._rows is None:
            decoded = []
            for raw in self._raw_rows:
                texts = [None if c is None else c.decode("utf-8", errors="replace") for c in raw]
                decoded.append(Row(self.columns, [_decode_scalar(t) for t in texts]))
            self._rows = decoded
        return self._rows

    def __len__(self) -> int:
        return len(self._raw_rows)

    def __iter__(self) -> Iterator[Row]:
        return iter(self.rows)

    def __repr__(self) -> str:
        return f"QueryResult(columns={self.columns!r}, rows={len(self._raw_rows)}, command_tag={self.command_tag!r})"

    def _repr_html_(self) -> str:
        """Jupyter notebook support: rendering a `QueryResult` as the last
        expression in a cell shows an HTML table instead of the `repr()`
        text. This is the concrete, testable slice of "Jupyter support" this
        SDK implements — there is no `%%tpt_sql` magic command (not built).
        """
        head = "".join(f"<th>{escape(c)}</th>" for c in self.columns)
        body_rows = []
        for row in self.rows:
            cells = "".join(
                f"<td>{escape(str(v))}</td>" if v is not None else "<td><i>null</i></td>" for v in row.values
            )
            body_rows.append(f"<tr>{cells}</tr>")
        tag = f"<p><code>{escape(self.command_tag)}</code></p>" if self.command_tag else ""
        return (
            f"{tag}<table border='1' cellpadding='4' style='border-collapse:collapse'>"
            f"<thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"
            f"<p>{len(self._raw_rows)} row(s)</p>"
        )

    def to_pandas(self, *, client: Optional["KeystoneClient"] = None, table_hint: Optional[str] = None):
        """Convert to a `pandas.DataFrame`.

        By default this just hands pandas the decoded Python-scalar columns
        (`rows` above), which pandas will dtype-infer itself. Pass `client`
        (and optionally `table_hint`, the source table name) to instead do
        real catalog-driven coercion: this method will query
        `information_schema.columns` via `client` for `table_hint`'s
        (or, if omitted, best-effort matched by column name across all
        tables) declared column types and cast each pandas Series to the
        matching dtype (`Int64`/`Float64`/`boolean`/`string`), the same
        approach `sdk-web`'s `queryTyped()` takes server-side instead of
        relying on pandas' own text-sniffing.

        Requires the `pandas` extra: `pip install tpt-sdk[pandas]`.
        """
        from . import pandas_ext

        return pandas_ext.to_pandas(self, client=client, table_hint=table_hint)

    async def to_pandas_async(self, *, client: Optional["KeystoneClient"] = None, table_hint: Optional[str] = None):
        """Async counterpart of `to_pandas()` — use this from inside an
        `async def` (e.g. right after `await client.query(...)`), since
        `asyncio.run()` (used internally by the sync `to_pandas()` for its
        catalog lookup) cannot be nested inside an already-running loop.
        """
        from . import pandas_ext

        return await pandas_ext.to_pandas_async(self, client=client, table_hint=table_hint)


class KeystoneClient:
    """Async client for one Keystone node. Not thread-safe; not safe to
    share across concurrent `query`/`query_params` calls on the same
    instance (there is exactly one in-flight request per connection, same
    as the Rust SDK)."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5432, user: str = "tpt_sdk"):
        self.host = host
        self.port = port
        self.user = user
        self._conn: Optional[Conn] = None

    async def connect(self) -> "KeystoneClient":
        self._conn = await Conn.connect(self.host, self.port, {"user": self.user})
        return self

    async def __aenter__(self) -> "KeystoneClient":
        return await self.connect()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _require_conn(self) -> Conn:
        if self._conn is None:
            raise ConnectionNotOpen("call `await client.connect()` first")
        return self._conn

    async def query(self, sql: str) -> QueryResult:
        """Run `sql` over the simple query protocol. Supports multi-statement
        SQL text but only the last statement's rows are returned (each
        statement gets its own CommandComplete; only the final
        ReadyForQuery ends the exchange) — same semantics as the Rust SDK.
        """
        conn = self._require_conn()
        conn.write_query(sql)
        await conn.flush()

        columns: list[str] = []
        raw_rows: list[list[Optional[bytes]]] = []
        command_tag: Optional[str] = None

        while True:
            msg = await conn.read_message()
            if isinstance(msg, wire.RowDescriptionMsg):
                columns = [f.name for f in msg.fields]
                raw_rows = []
            elif isinstance(msg, wire.DataRowMsg):
                raw_rows.append(msg.cells)
            elif isinstance(msg, wire.CommandCompleteMsg):
                command_tag = msg.tag
            elif isinstance(msg, wire.EmptyQueryResponseMsg):
                pass
            elif isinstance(msg, wire.ErrorResponseMsg):
                raise ServerError(msg.message)
            elif isinstance(msg, wire.NoticeResponseMsg):
                pass
            elif isinstance(msg, wire.ReadyForQueryMsg):
                break

        return QueryResult(columns, raw_rows, command_tag)

    async def query_params(self, sql: str, params: Sequence[ParamValue] = ()) -> QueryResult:
        """Run a parameterized query (`$1`, `$2`, ... placeholders) over the
        extended query protocol (Parse/Bind/Describe/Execute/Sync), all
        params/results in text format.
        """
        conn = self._require_conn()
        encoded = [_encode_param(p) for p in params]

        conn.write_parse("", sql, [])
        conn.write_bind("", "", encoded)
        conn.write_describe_portal("")
        conn.write_execute("", 0)
        conn.write_sync()
        await conn.flush()

        columns: list[str] = []
        raw_rows: list[list[Optional[bytes]]] = []
        command_tag: Optional[str] = None

        while True:
            msg = await conn.read_message()
            if isinstance(
                msg,
                (wire.ParseCompleteMsg, wire.BindCompleteMsg, wire.ParameterDescriptionMsg, wire.NoDataMsg),
            ):
                continue
            if isinstance(msg, wire.RowDescriptionMsg):
                columns = [f.name for f in msg.fields]
                continue
            if isinstance(msg, wire.DataRowMsg):
                raw_rows.append(msg.cells)
                continue
            if isinstance(msg, wire.CommandCompleteMsg):
                command_tag = msg.tag
                continue
            if isinstance(msg, wire.PortalSuspendedMsg):
                continue
            if isinstance(msg, wire.ErrorResponseMsg):
                # Drain to ReadyForQuery so the connection stays usable.
                while not isinstance(await conn.read_message(), wire.ReadyForQueryMsg):
                    pass
                raise ServerError(msg.message)
            if isinstance(msg, wire.ReadyForQueryMsg):
                break

        return QueryResult(columns, raw_rows, command_tag)

    async def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.write_terminate()
            await self._conn.flush()
        except Exception:
            pass
        self._conn.close()
        self._conn = None
