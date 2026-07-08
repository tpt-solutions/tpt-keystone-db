"""Optional pandas/NumPy integration for `QueryResult.to_pandas()`.

Kept in its own module so `import tpt_sdk` never requires pandas/numpy to
be installed — they're an extra (`pip install tpt-sdk[pandas]`). This module
is only imported lazily, from inside `QueryResult.to_pandas()`.

Type coercion strategy: rather than let pandas sniff dtypes from the
already-Python-scalar-decoded columns (which is what happens if no `client`
is passed), optionally query Keystone's own `information_schema.columns`
catalog for the real declared column types and cast accordingly — the same
approach `packages/sdk-web/src/client.ts`'s `queryTyped()` takes (it maps
`pg_tables`/`information_schema`-style type names to JS coercions;
`type_name()` in `tpt-keystone/src/executor/catalog.rs` is the source of
truth for the strings compared below: "bigint", "integer", "smallint",
"double precision", "real", "boolean", "timestamp without time zone",
"date", "json", "bytea", "text", "geometry").
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .client import KeystoneClient, QueryResult

_INT_TYPES = {"bigint", "integer", "smallint"}
_FLOAT_TYPES = {"double precision", "real"}
_BOOL_TYPES = {"boolean"}
_DATETIME_TYPES = {"timestamp without time zone", "date"}


async def _fetch_column_types(client: "KeystoneClient", table_hint: Optional[str]) -> dict[str, str]:
    if table_hint is not None:
        result = await client.query_params(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
            [table_hint],
        )
    else:
        result = await client.query("SELECT column_name, data_type FROM information_schema.columns")
    types: dict[str, str] = {}
    for row in result.rows:
        # First match wins if the same column name appears in multiple
        # tables and no table_hint was given to disambiguate — best-effort,
        # documented scope cut.
        types.setdefault(row.column_name, row.data_type)
    return types


def _build_dataframe(result: "QueryResult"):
    import pandas as pd

    columns = result.columns
    data = {col: [row.values[i] for row in result.rows] for i, col in enumerate(columns)}
    return pd.DataFrame(data, columns=columns)


def _apply_types(df, col_types: dict[str, str]):
    import pandas as pd

    for col in df.columns:
        dtype = col_types.get(col)
        if dtype is None:
            continue
        try:
            if dtype in _INT_TYPES:
                df[col] = df[col].astype("Int64")
            elif dtype in _FLOAT_TYPES:
                df[col] = df[col].astype("Float64")
            elif dtype in _BOOL_TYPES:
                df[col] = df[col].astype("boolean")
            elif dtype in _DATETIME_TYPES:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "text":
                df[col] = df[col].astype("string")
        except (ValueError, TypeError):
            # Leave the column as pandas' own inferred dtype if the catalog
            # type doesn't actually cast cleanly (e.g. NULLs mixed with
            # non-numeric text from a permissive column) rather than raise.
            continue
    return df


def to_pandas(result: "QueryResult", *, client: Optional["KeystoneClient"] = None, table_hint: Optional[str] = None):
    try:
        import pandas as pd  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "QueryResult.to_pandas() requires the 'pandas' extra: pip install tpt-sdk[pandas]"
        ) from exc

    df = _build_dataframe(result)
    if client is None:
        return df

    # client is an async KeystoneClient; to_pandas() itself is sync, so the
    # catalog lookup runs via asyncio.run() on its own throwaway event loop.
    # This raises RuntimeError if called from inside a running event loop
    # (e.g. `await`-ing code already inside `async def`) — in that case use
    # `await result.to_pandas_async(client, table_hint)` instead (see
    # `QueryResult.to_pandas_async`).
    import asyncio

    col_types = asyncio.run(_fetch_column_types(client, table_hint))
    return _apply_types(df, col_types)


async def to_pandas_async(
    result: "QueryResult", *, client: Optional["KeystoneClient"] = None, table_hint: Optional[str] = None
):
    """Async counterpart of `to_pandas()`, for callers already inside a
    running event loop (`asyncio.run()` can't be nested). Awaits the
    `information_schema.columns` lookup directly instead of spinning up a
    throwaway loop.
    """
    try:
        import pandas as pd  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "QueryResult.to_pandas_async() requires the 'pandas' extra: pip install tpt-sdk[pandas]"
        ) from exc

    df = _build_dataframe(result)
    if client is None:
        return df
    col_types = await _fetch_column_types(client, table_hint)
    return _apply_types(df, col_types)
