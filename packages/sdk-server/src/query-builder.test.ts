import assert from "node:assert/strict";
import { test } from "node:test";

import { from, table, TypedQueryBuilder } from "./query-builder.js";

interface User {
  id: number;
  name: string;
}

const Users = table<User>("users", ["id", "name"]);

test("build() selects every column with no filters", () => {
  const q = from(Users).build();
  assert.equal(q.sql, 'SELECT "id", "name" FROM "users"');
  assert.deepEqual(q.params, []);
});

test("build() applies whereEq/orderBy/limit", () => {
  const q = from(Users).whereEq("name", "Ada").orderBy("id", "DESC").limit(5).build();
  assert.equal(q.sql, 'SELECT "id", "name" FROM "users" WHERE "name" = $1 ORDER BY "id" DESC LIMIT 5');
  assert.deepEqual(q.params, ["Ada"]);
});

test("fetch() delegates to queryParams and decodes rows via toObject()", async () => {
  const calls: Array<{ sql: string; params: unknown[] }> = [];
  const fakeClient = {
    async queryParams(sql: string, params: unknown[] = []) {
      calls.push({ sql, params });
      return {
        columns: ["id", "name"],
        commandTag: null,
        rows: [
          {
            cells: ["1", "Ada"],
            columns: ["id", "name"],
            get: () => undefined,
            toObject: () => ({ id: 1, name: "Ada" }),
          },
        ],
      };
    },
  };
  const rows = await new TypedQueryBuilder(Users).whereEq("id", 1).fetch(fakeClient as never);
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /WHERE "id" = \$1/);
  assert.deepEqual(rows, [{ id: 1, name: "Ada" }]);
});
