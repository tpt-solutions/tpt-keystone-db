# tpt-keystone-sdk

Native Rust SDK for TPT Keystone — async and blocking clients speaking the same hand-written Postgres
wire protocol v3 as `tpt-keystone` itself, plus a typed query builder, zero-copy row views, an FFI
surface for C/C++ interop, and optional access to `tpt-keystone-canvas`'s reactive core.

## Features (Cargo feature flags)

| Feature | Default | Description |
|---|---|---|
| `keystone` | yes | Async `KeystoneClient` + blocking wrapper, typed query builder |
| `canvas` | no | Re-exports `tpt-keystone-canvas`'s reactive primitives (`Signal`, `create_effect`, `create_memo`) |
| `ffi` | no | `extern "C"` bindings (`tpt_sdk_connect`/`tpt_sdk_query`/`tpt_sdk_free_result`) |

## Quick start

```toml
[dependencies]
tpt-keystone-sdk = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use tpt_sdk::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = KeystoneClient::connect("127.0.0.1:55432").await?;
    let rows = client.query("SELECT id, name FROM users LIMIT 10").await?;
    for row in &rows {
        println!("{:?}", row);
    }
    Ok(())
}
```

## Blocking client (for CLI tools)

```rust
use tpt_sdk::keystone::blocking::Client;

let client = Client::connect("127.0.0.1:55432")?;
let rows = client.query("SELECT count(*) FROM events")?;
```

## Typed query builder

```rust
use tpt_sdk::query_builder::{QueryBuilder, Order};

let sql = QueryBuilder::from("users")
    .filter("age > $1")
    .order_by("name", Order::Asc)
    .limit(20)
    .build();
let rows = client.query_params(&sql, &[&18i32]).await?;
```

## Zero-copy row views

`zerocopy::RowView` borrows directly from the wire read buffer (row-batch-level, not columnar/Arrow).

## Scope cuts

- No native GPU / WebGPU rendering — the `canvas` feature exposes reactive primitives only; `Canvas.*`
  components only render inside a browser (see `tpt-keystone-canvas`).
- FFI covers request/response query flows only; no streaming/async FFI callbacks.

## License

Apache-2.0 — Copyright 2026 TPT Solutions
