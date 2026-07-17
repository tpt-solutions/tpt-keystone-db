//! Automatic TypeScript type generation (Phase 13 checklist item), run as a
//! standalone CLI against a live `tpt-keystone` node's `GET /schema`
//! endpoint (`wire::http_query` server-side): `cargo run --bin tsgen --
//! 127.0.0.1:5435 > schema.d.ts`.
//!
//! Host-target only (`#[cfg(not(target_arch = "wasm32"))]` guards the real
//! `main`) — this never runs in the browser, so it hand-rolls a plain HTTP
//! GET over `std::net::TcpStream` rather than pulling in an HTTP client
//! crate, matching this project's from-scratch wire-protocol ethos even
//! though it's a client here rather than a server.

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5435".to_string());
    let body = http_get(&addr, "/schema")?;
    let schema: serde_json::Value = serde_json::from_str(&body).map_err(std::io::Error::other)?;

    let tables = schema["tables"].as_array().cloned().unwrap_or_default();
    for table in tables {
        let name = table["name"].as_str().unwrap_or("Unknown");
        println!("export interface {} {{", pascal_case(name));
        for column in table["columns"].as_array().cloned().unwrap_or_default() {
            let col_name = column["name"].as_str().unwrap_or("field");
            let ts_type = ts_type_for(column["type"].as_str().unwrap_or("text"));
            println!("  {col_name}: {ts_type};");
        }
        println!("}}\n");
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn ts_type_for(keystone_type: &str) -> &'static str {
    match keystone_type {
        "int2" | "int4" | "int8" | "float4" | "float8" => "number",
        "bool" => "boolean",
        "json" => "unknown",
        _ => "string",
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn pascal_case(s: &str) -> String {
    s.split(['_', '-']).map(|part| {
        let mut chars = part.chars();
        match chars.next() {
            Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
            None => String::new(),
        }
    }).collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn http_get(addr: &str, path: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let mut stream = TcpStream::connect(addr)?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let response = String::from_utf8_lossy(&response);
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("");
    Ok(body.to_string())
}
