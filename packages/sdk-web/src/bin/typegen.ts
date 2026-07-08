#!/usr/bin/env node
// TypeScript codegen against a live Keystone node's `GET /schema`
// (`wire::http_query`), the JS-side sibling of `tpt-canvas`'s
// `src/bin/tsgen.rs` (same source endpoint, same output shape — kept in
// sync intentionally). Usage:
//
//   npx tpt-typegen http://localhost:5435 > schema.d.ts

import type { SchemaInfo } from "../client.js";

function pascalCase(name: string): string {
  return name
    .split(/[_-]/)
    .map((part) => (part.length ? part[0].toUpperCase() + part.slice(1) : ""))
    .join("");
}

function tsTypeFor(keystoneType: string): string {
  switch (keystoneType) {
    case "int2":
    case "int4":
    case "int8":
    case "float4":
    case "float8":
      return "number";
    case "bool":
      return "boolean";
    case "json":
      return "unknown";
    default:
      return "string";
  }
}

async function main(): Promise<void> {
  const url = (process.argv[2] ?? "http://localhost:5435").replace(/\/$/, "");
  const res = await fetch(`${url}/schema`);
  if (!res.ok) {
    throw new Error(`GET ${url}/schema failed with status ${res.status}`);
  }
  const schema = (await res.json()) as SchemaInfo;

  const output: string[] = [];
  for (const table of schema.tables ?? []) {
    output.push(`export interface ${pascalCase(table.name)} {`);
    for (const column of table.columns ?? []) {
      output.push(`  ${column.name}: ${tsTypeFor(column.type)} | null;`);
    }
    output.push("}\n");
  }
  process.stdout.write(output.join("\n"));
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
