import pytest

from tpt_sdk.query_builder import QueryBuilder, TableDef

Users = TableDef(name="users", columns=["id", "name", "signed_up_at"])


def test_build_selects_every_column_with_no_filters():
    sql, params = QueryBuilder(Users).build()
    assert sql == "SELECT id, name, signed_up_at FROM users"
    assert params == []


def test_build_applies_select_filter_order_limit_offset():
    sql, params = (
        QueryBuilder(Users)
        .select(["id", "name"])
        .filter_eq("name", "Ada")
        .order_by("id", desc=True)
        .limit(10)
        .offset(5)
        .build()
    )
    assert sql == "SELECT id, name FROM users WHERE name = $1 ORDER BY id DESC LIMIT 10 OFFSET 5"
    assert params == ["Ada"]


def test_build_ands_multiple_filters_with_positional_params():
    sql, params = QueryBuilder(Users).filter_eq("id", 1).filter_eq("name", "Ada").build()
    assert sql == "SELECT id, name, signed_up_at FROM users WHERE id = $1 AND name = $2"
    assert params == [1, "Ada"]


class _FakeClient:
    def __init__(self):
        self.calls = []

    async def query_params(self, sql, params=()):
        self.calls.append((sql, list(params)))
        return "fake-result"


@pytest.mark.asyncio
async def test_fetch_delegates_to_query_params():
    client = _FakeClient()
    result = await QueryBuilder(Users).filter_eq("id", 1).fetch(client)
    assert result == "fake-result"
    assert len(client.calls) == 1
    assert "WHERE id = $1" in client.calls[0][0]
    assert client.calls[0][1] == [1]
