# tpt-keystone-sdk

[![crates.io](https://img.shields.io/crates/v/tpt-keystone-sdk.svg)](https://crates.io/crates/tpt-keystone-sdk)
[![docs.rs](https://img.shields.io/docsrs/tpt-keystone-sdk)](https://docs.rs/tpt-keystone-sdk)
[![license](https://img.shields.io/crates/l/tpt-keystone-sdk.svg)](https://github.com/tpt-solutions/tpt-keystone-db/blob/master/tpt-keystone-sdk/LICENSE-MIT)

The native Rust SDK for [TPT Keystone](../tpt-keystone/README.md). It provides an async client and a
blocking client, both speaking the same hand-written Postgres wire protocol v3 that the server uses. It
also includes a typed query builder, zero-copy row views, a C ABI for C and C++ callers, and access to
the reactive core of [`tpt-keystone-canvas`](../tpt-keystone-canvas/README.md).

The library is named `tpt_sdk` in code, and the package is `tpt-keystone-sdk`:

```rust
use tpt_sdk::prelude::*;
```

## Features

| Feature | Default | What it enables |
|---|---|---|
| `keystone` | yes | Async `KeystoneClient`, the blocking `Client` wrapper, the query builder, and zero-copy row views |
| `canvas` | no | Re-exports `Signal`, `create_effect`, and `create_memo` from `tpt-keystone-canvas` |
| `ffi` | no | `extern "C"` functions for C and C++ (implies `keystone`) |

A second binary, `tpt-keystone-sdk-typegen`, generates `query_builder::Table` implementations from a
running server's schema.

## Requirements

- Rust **1.85** or newer
- A running `tpt-keystone` node to connect to (the Postgres wire port, `55432` by default)

## Install

```toml
[dependencies]
tpt-keystone-sdk = "0.1"
tokio = { version = "1", features = ["full"] }
```

Enable the blocking client and the query builder with the default features. Enable `canvas` or `ffi`
only if you need them.

## Async client

```rust
use tpt_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), KeystoneError> {
    let mut client = KeystoneClient::connect("127.0.0.1:55432").await?;

    let result = client.query("SELECT id, name FROM users LIMIT 10").await?;
    println!("columns: {:?}", result.columns);
    for row in &result.rows {
        println!("{:?}", row);
    }

    client.close().await
}
```

`connect_with_params` accepts connection parameters. `query_params` takes positional parameters
(`$1`, `$2`, ...) as `&[Value]`, which keeps user input out of the SQL text:

```rust
use tpt_sdk::keystone::Value;

let result = client
    .query_params("SELECT name FROM users WHERE age > $1", &[Value::Int(18)])
    .await?;
```

`copy_in` sends rows through `COPY ... FROM STDIN` for bulk ingest.

## Blocking client

For command-line tools and other code that should not start an async runtime:

```rust
use tpt_sdk::keystone::blocking::Client;

fn main() -> Result<(), tpt_sdk::keystone::KeystoneError> {
    let mut client = Client::connect("127.0.0.1:55432")?;
    let result = client.query("SELECT count(*) FROM events")?;
    println!("{:?}", result.rows);
    Ok(())
}
```

## Query builder

The query builder produces SQL and parameters. It never executes anything on its own. You pass the
result of `build()` to `query_params`:

```rust
use tpt_sdk::keystone::Value;
use tpt_sdk::query_builder::{Order, QueryBuilder, Table};

struct Users;

impl Table for Users {
    const NAME: &'static str = "users";
    const COLUMNS: &'static [&'static str] = &["id", "name", "age"];
}

let (sql, params) = QueryBuilder::<Users>::new()
    .select(&["id", "name"])
    .filter_eq("age", Value::Int(18))
    .order_by("name", Order::Asc)
    .limit(20)
    .build();

let result = client.query_params(&sql, &params).await?;
```

`Table` is hand-written here. `tpt-keystone-sdk-typegen` generates the same implementations from a live
server's `information_schema`. Filters are equality only. For anything more complex, write the SQL and
call `query_params` directly.

## Rows

`Row` owns its cells. You can read a cell as text, as a `Value`, or as raw bytes, by index or by column
name:

```rust
let first_name = row.get_str(1);          // Option<&str>
let by_name = row.get_by_name("name");     // Option<&[u8]>
let typed = row.get_value(2);              // Value
```

`zerocopy::RowView` borrows directly from the wire buffer, so reading a row does not allocate. It is
built per row batch, not per column, and is not an Arrow-style columnar view.

## C and C++ (`ffi`)

Enable the `ffi` feature to build the library as a `cdylib` and `staticlib`. It exports:

| Function | Purpose |
|---|---|
| `tpt_sdk_connect(addr)` | Opens a client handle |
| `tpt_sdk_query(client, sql)` | Runs a query and returns a result handle |
| `tpt_sdk_result_row_count` / `tpt_sdk_result_column_count` | Result dimensions |
| `tpt_sdk_result_cell(result, row, col)` | Reads one cell |
| `tpt_sdk_free_result` / `tpt_sdk_free_client` | Frees handles |
| `tpt_sdk_last_error()` | Returns the last error message |

The FFI covers request and response queries only. It has no streaming or async callbacks.

## Canvas reactive core (`canvas`)

With the `canvas` feature, the SDK re-exports the target-agnostic reactive primitives from
`tpt-keystone-canvas`: `Signal`, `create_effect`, and `create_memo`. Native code can use them to drive
state. They do not render anything. The `Canvas.*` components only render in a browser.

## Limitations

- No native GPU or WebGPU rendering. The `canvas` feature exposes reactive primitives only.
- FFI covers query execution only, not streaming.
- Zero-copy is per row batch, not columnar.
- The query builder supports equality filters only.

## Changelog

Release notes are in [`CHANGELOG.md`](CHANGELOG.md).

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE))
- MIT license ([`LICENSE-MIT`](LICENSE-MIT))

at your option. Copyright 2026 TPT Solutions.
