//! Sandboxed execution of WASM user-defined functions via `wasmtime`.
//!
//! **Scalar types** (`int8`/`float8`/`bool`) are passed and returned as
//! native Wasm `i64`/`f64`/`i32` values — zero encoding decisions, same as
//! v1.
//!
//! **Array/bytea types** (`float8[]`/`bytea`) are marshaled through the
//! module's exported linear memory using a `(ptr, len)` pointer ABI, because
//! a value can't fit in a single Wasm register. Concretely:
//!
//! * Argument passing (engine → WASM): a `float8[]`/`bytea` argument becomes
//!   two `i32` params — a pointer into linear memory and an element/byte
//!   count. The engine allocates a buffer via the module's exported
//!   `alloc(len: i32) -> i32` allocator, writes the little-endian `f64`
//!   elements (or raw bytes for `bytea`), and passes the `(ptr, len)`.
//! * Return (WASM → engine): a `float8[]`/`bytea` return is a `(ptr, len)`
//!   pair; the engine reads the buffer back out of linear memory.
//!
//! So any UDF that uses `float8[]`/`bytea` must export `alloc(len: i32) -> i32`
//! returning a pointer to `len` bytes of (zeroed, non-overlapping) memory —
//! the classic bump-allocator contract. This is *verified at `CREATE
//! FUNCTION` time* by `validate_module`, so a module with the wrong ABI is a
//! creation-time error, not a surprise on first call.
//!
//! A UDF module gets zero host imports (an empty `Linker`) — it can only
//! compute, it has no I/O. Every invocation is bounded by a fuel budget
//! (execution steps) and a linear-memory cap, so a runaway or malicious
//! module can't hang a connection or exhaust host memory.

use wasmtime::{
    Config, Engine, ExternType, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder, Val,
    ValType,
};

use crate::executor::eval::Value;
use crate::storage::config::UdfConfig;
use crate::storage::{ColumnType, UserFunction};

/// `wasmtime::ValType` doesn't implement `PartialEq`, so compare by variant
/// kind directly (we only ever produce/expect `I32`/`I64`/`F64` here).
fn valtype_eq(a: &ValType, b: &ValType) -> bool {
    matches!(
        (a, b),
        (ValType::I32, ValType::I32)
            | (ValType::I64, ValType::I64)
            | (ValType::F32, ValType::F32)
            | (ValType::F64, ValType::F64)
    )
}

fn valtypes_eq(a: &[ValType], b: &[ValType]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| valtype_eq(x, y))
}

/// Does this type require the `(ptr, len)` linear-memory ABI rather than a
/// single scalar register?
fn is_memory_type(ty: &ColumnType) -> bool {
    matches!(ty, ColumnType::Float8Array | ColumnType::Bytea)
}

/// The Wasm *value types* a (possibly array) `ColumnType` appears as in a
/// function signature: one value for scalars, a `(i32, i32)` pair for
/// `float8[]`/`bytea`.
fn wasm_value_types(ty: &ColumnType) -> anyhow::Result<Vec<ValType>> {
    match ty {
        ColumnType::Int8 => Ok(vec![ValType::I64]),
        ColumnType::Float8 => Ok(vec![ValType::F64]),
        ColumnType::Bool => Ok(vec![ValType::I32]),
        ColumnType::Float8Array | ColumnType::Bytea => Ok(vec![ValType::I32, ValType::I32]),
        other => anyhow::bail!(
            "WASM UDFs only support int8/float8/bool/float8[]/bytea types, got {other:?}"
        ),
    }
}

/// Encode a `float8[]` as its little-endian `f64` byte representation.
fn encode_f64_vec(values: &[f64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Decode little-endian `f64` bytes back into a `float8[]`.
fn decode_f64_vec(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap_or([0u8; 8])))
        .collect()
}

/// Compile `wasm_bytes` and verify it exports a function named `name` whose
/// Wasm signature exactly matches `arg_types`/`return_type` under the
/// scalar-or-(ptr,len) ABI. Called once at `CREATE FUNCTION` time so a bad
/// module/signature is a creation-time error, not a surprise on first call.
///
/// For any `float8[]`/`bytea` type, also requires an exported
/// `alloc(len: i32) -> i32` allocator (see module docs).
pub fn validate_module(
    wasm_bytes: &[u8],
    name: &str,
    arg_types: &[ColumnType],
    return_type: &ColumnType,
    max_module_bytes: usize,
) -> anyhow::Result<()> {
    if wasm_bytes.len() > max_module_bytes {
        anyhow::bail!("WASM module for function \"{name}\" is {} bytes, exceeding the {max_module_bytes}-byte limit", wasm_bytes.len());
    }

    let engine = Engine::default();
    let module = Module::new(&engine, wasm_bytes)?;

    let export = module.exports().find(|e| e.name() == name).ok_or_else(|| {
        anyhow::anyhow!("WASM module does not export a function named \"{name}\"")
    })?;
    let ExternType::Func(func_ty) = export.ty() else {
        anyhow::bail!("export \"{name}\" is not a function");
    };

    let mut expected_params: Vec<ValType> = Vec::new();
    for ty in arg_types {
        expected_params.extend(wasm_value_types(ty)?);
    }
    let expected_results = wasm_value_types(return_type)?;

    let actual_params: Vec<ValType> = func_ty.params().collect();
    if !valtypes_eq(&actual_params, &expected_params) {
        anyhow::bail!(
            "function \"{name}\" declares argument types implying Wasm params {expected_params:?}, but the module's export takes {actual_params:?}"
        );
    }

    let actual_results: Vec<ValType> = func_ty.results().collect();
    if !valtypes_eq(&actual_results, &expected_results) {
        anyhow::bail!(
            "function \"{name}\" must return Wasm values {expected_results:?}, but the module's export returns {actual_results:?}"
        );
    }

    if arg_types.iter().any(is_memory_type) || is_memory_type(return_type) {
        let Some(ExternType::Func(alloc_ty)) = module
            .exports()
            .find(|e| e.name() == "alloc")
            .map(|e| e.ty())
        else {
            anyhow::bail!(
                "function \"{name}\" uses float8[]/bytea, so its module must export `alloc(len: i32) -> i32`"
            )
        };
        let alloc_params: Vec<ValType> = alloc_ty.params().collect();
        let alloc_results: Vec<ValType> = alloc_ty.results().collect();
        if !valtypes_eq(&alloc_params, &[ValType::I32]) || !valtypes_eq(&alloc_results, &[ValType::I32]) {
            anyhow::bail!("`alloc` must have the signature (i32) -> i32");
        }
    }

    Ok(())
}

/// Invoke a registered WASM UDF with already-evaluated argument `Value`s,
/// sandboxed per `cfg`. Scalars go through native Wasm registers; `float8[]`/
/// `bytea` go through the module's linear memory via its exported `alloc`.
pub fn call(cfg: UdfConfig, uf: &UserFunction, args: &[Value]) -> anyhow::Result<Value> {
    if args.len() != uf.arg_types.len() {
        anyhow::bail!(
            "function \"{}\" expects {} argument(s), got {}",
            uf.name,
            uf.arg_types.len(),
            args.len()
        );
    }

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config)?;
    let module = Module::new(&engine, &uf.wasm_bytes)?;

    let limits = StoreLimitsBuilder::new()
        .memory_size(cfg.memory_limit_bytes)
        .build();
    let mut store = Store::new(&engine, limits);
    store.limiter(|s| s);
    store.set_fuel(cfg.fuel_limit)?;

    let linker: Linker<StoreLimits> = Linker::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| anyhow::anyhow!("failed to instantiate WASM UDF \"{}\": {e}", uf.name))?;
    let func = instance.get_func(&mut store, &uf.name).ok_or_else(|| {
        anyhow::anyhow!(
            "function \"{}\" export not found in its own WASM module",
            uf.name
        )
    })?;

    let needs_memory = uf.arg_types.iter().any(is_memory_type) || is_memory_type(&uf.return_type);
    let (memory, alloc) = if needs_memory {
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow::anyhow!("UDF \"{}\" uses float8[]/bytea but exports no linear memory named \"memory\"", uf.name))?;
        let alloc = instance
            .get_func(&mut store, "alloc")
            .ok_or_else(|| anyhow::anyhow!("UDF \"{}\" uses float8[]/bytea but does not export `alloc`", uf.name))?;
        (Some(memory), Some(alloc))
    } else {
        (None, None)
    };

    let mut params: Vec<Val> = Vec::new();
    for (v, ty) in args.iter().zip(&uf.arg_types) {
        encode_arg(
            &mut store,
            memory,
            alloc,
            v,
            ty,
            &mut params,
        )?;
    }

    let result_count = wasm_value_types(&uf.return_type)?.len();
    let mut results = vec![Val::I32(0); result_count];
    func.call(&mut store, &params, &mut results)
        .map_err(|e| anyhow::anyhow!("WASM UDF \"{}\" failed: {e}", uf.name))?;

    decode_result(&mut store, memory, &results, &uf.return_type)
}

/// Append this argument's Wasm values to `out`, allocating linear-memory
/// buffers (via `alloc`) for `float8[]`/`bytea` arguments.
fn encode_arg(
    store: &mut Store<StoreLimits>,
    memory: Option<Memory>,
    alloc: Option<wasmtime::Func>,
    v: &Value,
    ty: &ColumnType,
    out: &mut Vec<Val>,
) -> anyhow::Result<()> {
    match ty {
        ColumnType::Int8 => out.push(to_scalar_val(v, ty)?),
        ColumnType::Float8 => out.push(to_scalar_val(v, ty)?),
        ColumnType::Bool => out.push(to_scalar_val(v, ty)?),
        ColumnType::Float8Array => {
            let arr = match v {
                Value::FloatArray(a) => a,
                other => anyhow::bail!("expected float8[] argument, got {}", other.type_name()),
            };
            let bytes = encode_f64_vec(arr);
            let ptr = alloc_bytes(store, memory.unwrap(), alloc.unwrap(), &bytes)?;
            out.push(Val::I32(ptr));
            out.push(Val::I32(arr.len() as i32));
        }
        ColumnType::Bytea => {
            let bytes = match v {
                Value::Bytea(b) => b,
                other => anyhow::bail!("expected bytea argument, got {}", other.type_name()),
            };
            let ptr = alloc_bytes(store, memory.unwrap(), alloc.unwrap(), bytes)?;
            out.push(Val::I32(ptr));
            out.push(Val::I32(bytes.len() as i32));
        }
        other => anyhow::bail!("unsupported UDF argument type {other:?}"),
    }
    Ok(())
}

/// Allocate `data.len()` bytes via the module's `alloc`, copy `data` into the
/// buffer, and return the pointer.
fn alloc_bytes(
    store: &mut Store<StoreLimits>,
    memory: Memory,
    alloc: wasmtime::Func,
    data: &[u8],
) -> anyhow::Result<i32> {
    let mut result = [Val::I32(0)];
    alloc
        .call(&mut *store, &[Val::I32(data.len() as i32)], &mut result)
        .map_err(|e| anyhow::anyhow!("UDF `alloc` failed: {e}"))?;
    let ptr = match result[0] {
        Val::I32(p) => p,
        _ => anyhow::bail!("UDF `alloc` returned a non-i32 value"),
    };
    memory
        .write(&mut *store, ptr as usize, data)
        .map_err(|e| anyhow::anyhow!("failed to write UDF argument into linear memory: {e}"))?;
    Ok(ptr)
}

/// Read a `float8[]`/`bytea` result out of the instance's linear memory from
/// the `(ptr, len)` pair in `results`.
fn decode_result(
    store: &mut Store<StoreLimits>,
    memory: Option<Memory>,
    results: &[Val],
    ty: &ColumnType,
) -> anyhow::Result<Value> {
    match ty {
        ColumnType::Int8 | ColumnType::Float8 | ColumnType::Bool => {
            from_scalar_val(&results[0], ty)
        }
        ColumnType::Float8Array => {
            let (ptr, len) = read_ptr_len(results)?;
            let memory = memory.ok_or_else(|| anyhow::anyhow!("UDF result memory unavailable"))?;
            let mut buf = vec![0u8; len * 8];
            memory
                .read(store, ptr as usize, &mut buf)
                .map_err(|e| anyhow::anyhow!("failed to read float8[] result: {e}"))?;
            Ok(Value::FloatArray(decode_f64_vec(&buf)))
        }
        ColumnType::Bytea => {
            let (ptr, len) = read_ptr_len(results)?;
            let memory = memory.ok_or_else(|| anyhow::anyhow!("UDF result memory unavailable"))?;
            let mut buf = vec![0u8; len];
            memory
                .read(store, ptr as usize, &mut buf)
                .map_err(|e| anyhow::anyhow!("failed to read bytea result: {e}"))?;
            Ok(Value::Bytea(buf))
        }
        other => anyhow::bail!("unsupported UDF return type {other:?}"),
    }
}

fn read_ptr_len(results: &[Val]) -> anyhow::Result<(i32, usize)> {
    let ptr = match results.first() {
        Some(Val::I32(p)) => *p,
        _ => anyhow::bail!("UDF result pointer was not an i32"),
    };
    let len = match results.get(1) {
        Some(Val::I32(l)) => *l,
        _ => anyhow::bail!("UDF result length was not an i32"),
    };
    Ok((ptr, len.max(0) as usize))
}

fn to_scalar_val(v: &Value, ty: &ColumnType) -> anyhow::Result<Val> {
    match (v, ty) {
        (Value::Int(n), ColumnType::Int8) => Ok(Val::I64(*n)),
        (Value::Float(f), ColumnType::Float8) => Ok(Val::F64(f.to_bits())),
        (Value::Bool(b), ColumnType::Bool) => Ok(Val::I32(if *b { 1 } else { 0 })),
        (Value::Null, _) => anyhow::bail!("NULL is not supported as a WASM UDF argument"),
        (other, ty) => anyhow::bail!(
            "cannot pass a {} value as a {ty:?} WASM UDF argument",
            other.type_name()
        ),
    }
}

fn from_scalar_val(v: &Val, ty: &ColumnType) -> anyhow::Result<Value> {
    match (v, ty) {
        (Val::I64(n), ColumnType::Int8) => Ok(Value::Int(*n)),
        (Val::F64(bits), ColumnType::Float8) => Ok(Value::Float(f64::from_bits(*bits))),
        (Val::I32(n), ColumnType::Bool) => Ok(Value::Bool(*n != 0)),
        _ => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::execute_query;
    use crate::storage::config::NodeRole;
    use crate::storage::database::Database;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use std::sync::Arc;
    use std::time::Duration;

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        test_db_with_udf_config(UdfConfig::default())
    }

    fn test_db_with_udf_config(
        udf_config: UdfConfig,
    ) -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(
            store.clone(),
            "db",
            "node-1".into(),
            Duration::from_secs(30),
        ));
        lease.try_acquire().unwrap();
        let db = Arc::new(
            Database::open(
                local.path(),
                store,
                lease.handle(),
                NodeRole::Writer,
                udf_config,
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    fn wat_base64(wat: &str) -> String {
        use base64::Engine as _;
        let bytes = wat::parse_str(wat).unwrap();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn create_function_and_call_it() {
        let (db, _b, _l) = test_db();
        let wasm_b64 = wat_base64(
            r#"(module (func (export "add_one") (param i64) (result i64)
                 (i64.add (local.get 0) (i64.const 1))))"#,
        );
        let sql =
            format!("CREATE FUNCTION add_one(n int8) RETURNS int8 LANGUAGE wasm AS '{wasm_b64}'");
        execute_query(&sql, db.clone()).unwrap();

        let result = execute_query("SELECT add_one(41)", db.clone()).unwrap();
        assert_eq!(result.rows[0][0].as_deref(), Some(b"42".as_slice()));
    }

    // NOTE: an automated test that actually forces a WASM trap (fuel
    // exhaustion, a genuine unconditional infinite loop, etc.) is
    // deliberately not included here. In this sandboxed dev environment,
    // wasmtime's trap machinery on Windows (`traphandlers::catch_traps`,
    // which relies on OS-level exception handling to convert a WASM trap
    // into a catchable `Err`) crashes the whole test process with
    // STATUS_STACK_BUFFER_OVERRUN instead of returning an error — verified
    // reproducible with both a real infinite loop and a fuel-exhausting
    // bounded loop, and confirmed to happen *inside* wasmtime's own
    // trap-handling frames (not in `executor::udf` code) via a full
    // backtrace. This looks specific to this sandbox's process/exception
    // handling restrictions rather than a bug in the fuel-limiting code
    // above, but it means fuel/memory-limit trap behavior should be
    // verified by hand (e.g. `cargo test` on a normal Linux CI runner, or
    // manually via `psql`) before relying on it in production.

    #[test]
    fn create_function_rejects_signature_mismatch() {
        let (db, _b, _l) = test_db();
        // Export returns i64, but the SQL declares RETURNS float8.
        let wasm_b64 = wat_base64(
            r#"(module (func (export "bad") (param i64) (result i64)
                 (local.get 0)))"#,
        );
        let sql =
            format!("CREATE FUNCTION bad(n int8) RETURNS float8 LANGUAGE wasm AS '{wasm_b64}'");
        assert!(execute_query(&sql, db.clone()).is_err());
    }

    #[test]
    fn create_function_rejects_unsupported_type() {
        let (db, _b, _l) = test_db();
        let wasm_b64 =
            wat_base64(r#"(module (func (export "f") (param i64) (result i64) (local.get 0)))"#);
        let sql = format!("CREATE FUNCTION f(n text) RETURNS int8 LANGUAGE wasm AS '{wasm_b64}'");
        assert!(execute_query(&sql, db.clone()).is_err());
    }

    #[test]
    fn create_function_rejects_oversized_module() {
        let (db, _b, _l) = test_db_with_udf_config(UdfConfig {
            max_module_bytes: 4,
            ..UdfConfig::default()
        });
        let wasm_b64 = wat_base64(
            r#"(module (func (export "add_one") (param i64) (result i64)
                 (i64.add (local.get 0) (i64.const 1))))"#,
        );
        let sql =
            format!("CREATE FUNCTION add_one(n int8) RETURNS int8 LANGUAGE wasm AS '{wasm_b64}'");
        let err = match execute_query(&sql, db.clone()) {
            Err(e) => e,
            Ok(_) => panic!("expected oversized module to be rejected"),
        };
        assert!(
            err.to_string().contains("exceeding the 4-byte limit"),
            "unexpected error: {err}"
        );
    }

    /// A `float8[] -> float8[]` UDF that receives a signal window in linear
    /// memory, computes the sum-of-squares magnitude, and returns it as a
    /// single-element float8[] — proving the in-DB array ABI end-to-end
    /// (the spec's "signal window for FFT-style work" promise). Non-trapping
    /// by construction, so it doesn't trip the wasmtime trap issue noted
    /// above.
    #[test]
    fn float_array_udf_receives_and_returns_window() {
        let wasm = r#"(module
            (memory (export "memory") 2)
            (global $bp (mut i32) (i32.const 1024))
            (func $alloc (export "alloc") (param $size i32) (result i32)
              (local $ptr i32)
              (local.set $ptr (global.get $bp))
              (global.set $bp (i32.add (global.get $bp) (local.get $size)))
              (local.get $ptr))
            (func (export "sqmag") (param $in_ptr i32) (param $in_len i32) (result i32 i32)
              (local $i i32)
              (local $sum f64)
              (local $v f64)
              (local $addr i32)
              (local $out i32)
              (local.set $i (i32.const 0))
              (block $done
                (loop $cont
                  (br_if $done (i32.ge_u (local.get $i) (local.get $in_len)))
                  (local.set $addr (i32.add (local.get $in_ptr) (i32.mul (local.get $i) (i32.const 8))))
                  (local.set $v (f64.load (local.get $addr)))
                  (local.set $sum (f64.add (local.get $sum) (f64.mul (local.get $v) (local.get $v))))
                  (local.set $i (i32.add (local.get $i) (i32.const 1)))
                  (br $cont)
                )
              )
              (local.set $out (call $alloc (i32.const 8)))
              (f64.store (local.get $out) (local.get $sum))
              (local.get $out)
              (i32.const 1)
            ))"#;
        let wasm_b64 = wat_base64(wasm);

        let uf = UserFunction {
            name: "sqmag".into(),
            arg_types: vec![ColumnType::Float8Array],
            return_type: ColumnType::Float8Array,
            wasm_bytes: wat::parse_str(wasm).unwrap(),
        };
        // Validation must require the `alloc` export and accept the (ptr,len)
        // signature.
        validate_module(
            &uf.wasm_bytes,
            &uf.name,
            &uf.arg_types,
            &uf.return_type,
            usize::MAX,
        )
        .unwrap();

        let result = call(
            UdfConfig::default(),
            &uf,
            &[Value::FloatArray(vec![3.0, 4.0])],
        )
        .unwrap();
        assert_eq!(result, Value::FloatArray(vec![25.0]));
    }

    /// `bytea -> bytea` round-trips opaque bytes through linear memory.
    #[test]
    fn bytea_udf_round_trips_bytes() {
        let wasm = r#"(module
            (memory (export "memory") 2)
            (global $bp (mut i32) (i32.const 1024))
            (func $alloc (export "alloc") (param $size i32) (result i32)
              (local $ptr i32)
              (local.set $ptr (global.get $bp))
              (global.set $bp (i32.add (global.get $bp) (local.get $size)))
              (local.get $ptr))
            (func (export "echo") (param $in_ptr i32) (param $in_len i32) (result i32 i32)
              (local $out i32)
              (local $i i32)
              (local.set $out (call $alloc (local.get $in_len)))
              (local.set $i (i32.const 0))
              (block $done
                (loop $cont
                  (br_if $done (i32.ge_u (local.get $i) (local.get $in_len)))
                  (i32.store
                    (i32.add (local.get $out) (local.get $i))
                    (i32.load8_u (i32.add (local.get $in_ptr) (local.get $i))))
                  (local.set $i (i32.add (local.get $i) (i32.const 1)))
                  (br $cont)))
              (local.get $out)
              (local.get $in_len)))"#;
        let wasm_bytes = wat::parse_str(wasm).unwrap();
        let uf = UserFunction {
            name: "echo".into(),
            arg_types: vec![ColumnType::Bytea],
            return_type: ColumnType::Bytea,
            wasm_bytes,
        };
        validate_module(&uf.wasm_bytes, &uf.name, &uf.arg_types, &uf.return_type, usize::MAX)
            .unwrap();
        let result = call(
            UdfConfig::default(),
            &uf,
            &[Value::Bytea(vec![1, 2, 3, 255])],
        )
        .unwrap();
        assert_eq!(result, Value::Bytea(vec![1, 2, 3, 255]));
    }
}
