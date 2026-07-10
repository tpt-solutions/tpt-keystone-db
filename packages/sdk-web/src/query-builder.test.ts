import assert from "node:assert/strict";
import { test } from "node:test";

import { from, table, TypedQueryBuilder } from "./query-builder.js";

interface User {
  id: number;
  name: string;
  signed_up_at: number;
}

const Users = table<User>("users", ["id", "name", "signed_up_at"]);

test("build() selects every column with no filters", () => {
  const q = from(Users).build();
  assert.equal(q.sql, 'SELECT "id", "name", "signed_up_at" FROM "users"');
  assert.deepEqual(q.params, []);
});

test("build() applies select/whereEq/orderBy/limit/offset", () => {
  const q = from(Users)
    .select(["id", "name"])
    .whereEq("name", "Ada")
    .orderBy("id", "DESC")
    .limit(10)
    .offset(5)
    .build();
  assert.equal(q.sql, 'SELECT "id", "name" FROM "users" WHERE "name" = $1 ORDER BY "id" DESC LIMIT 10 OFFSET 5');
  assert.deepEqual(q.params, ["Ada"]);
});

test("build() AND-s multiple whereEq filters with positional params", () => {
  const q = from(Users).whereEq("id", 1).whereEq("name", "Ada").build();
  assert.equal(q.sql, 'SELECT "id", "name", "signed_up_at" FROM "users" WHERE "id" = $1 AND "name" = $2');
  assert.deepEqual(q.params, [1, "Ada"]);
});

test("fetch() builds then delegates to the client's query()", async () => {
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  const fakeClient = {
    async query(sql: string, params?: unknown[]) {
      calls.push({ sql, params });
      return { rows: [{ id: 1, name: "Ada", signed_up_at: 0 }] };
    },
  };
  const rows = await new TypedQueryBuilder(Users).whereEq("id", 1).fetch(fakeClient);
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /WHERE "id" = \$1/);
  assert.deepEqual(rows, [{ id: 1, name: "Ada", signed_up_at: 0 }]);
});
