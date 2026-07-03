//! Sandboxed execution of WASM user-defined functions via `wasmtime`.
//!
//! Scope cut (v1): only `int8`/`float8`/`bool` argument and return types are
//! supported. They map directly onto `Value::Int(i64)`/`Value::Float(f64)`/
//! `Value::Bool` and onto Wasm's native `i64`/`f64`/`i32` types with no
//! encoding decisions to make. `text`/`bytea` UDF arguments would require
//! the module to export linear memory plus an allocator convention — real
//! ABI design work deferred to a follow-up rather than a half-working
//! string marshaling scheme.
//!
//! A UDF module gets zero host imports (an empty `Linker`) — it can only
//! compute, it has no I/O. Every invocation is bounded by a fuel budget
//! (execution steps) and a linear-memory cap, so a runaway or malicious
//! module can't hang a connection or exhaust host memory.

use wasmtime::{Config, Engine, ExternType, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Val, ValType};

use crate::executor::eval::Value;
use crate::storage::config::UdfConfig;
use crate::storage::{ColumnType, UserFunction};

/// `wasmtime::ValType` doesn't implement `PartialEq`, so compare by variant
/// kind directly (we only ever produce/expect `I32`/`I64`/`F64` here).
fn valtype_eq(a: &ValType, b: &ValType) -> bool {
    matches!(
        (a, b),
        (ValType::I32, ValType::I32) | (ValType::I64, ValType::I64) | (ValType::F32, ValType::F32) | (ValType::F64, ValType::F64)
    )
}

fn wasm_type(ty: &ColumnType) -> anyhow::Result<ValType> {
    match ty {
        ColumnType::Int8 => Ok(ValType::I64),
        ColumnType::Float8 => Ok(ValType::F64),
        ColumnType::Bool => Ok(ValType::I32),
        other => anyhow::bail!("WASM UDFs only support int8/float8/bool argument and return types, got {other:?}"),
    }
}

/// Compile `wasm_bytes` and verify it exports a function named `name` whose
/// Wasm signature exactly matches `arg_types`/`return_type`. Called once at
/// `CREATE FUNCTION` time so a bad module/signature is a creation-time
/// error, not a surprise on first call.
pub fn validate_module(wasm_bytes: &[u8], name: &str, arg_types: &[ColumnType], return_type: &ColumnType) -> anyhow::Result<()> {
    let engine = Engine::default();
    let module = Module::new(&engine, wasm_bytes)?;

    let export = module
        .exports()
        .find(|e| e.name() == name)
        .ok_or_else(|| anyhow::anyhow!("WASM module does not export a function named \"{name}\""))?;
    let ExternType::Func(func_ty) = export.ty() else {
        anyhow::bail!("export \"{name}\" is not a function");
    };

    let expected_params: Vec<ValType> = arg_types.iter().map(wasm_type).collect::<anyhow::Result<_>>()?;
    let actual_params: Vec<ValType> = func_ty.params().collect();
    let params_match = actual_params.len() == expected_params.len()
        && actual_params.iter().zip(&expected_params).all(|(a, e)| valtype_eq(a, e));
    if !params_match {
        anyhow::bail!(
            "function \"{name}\" declares argument types implying Wasm params {expected_params:?}, but the module's export takes {actual_params:?}"
        );
    }

    let expected_result = wasm_type(return_type)?;
    let actual_results: Vec<ValType> = func_ty.results().collect();
    if actual_results.len() != 1 || !valtype_eq(&actual_results[0], &expected_result) {
        anyhow::bail!(
            "function \"{name}\" must return exactly one Wasm value of type {expected_result:?}, but the module's export returns {actual_results:?}"
        );
    }

    Ok(())
}

/// Invoke a registered WASM UDF with already-evaluated argument `Value`s,
/// sandboxed per `cfg`.
pub fn call(cfg: UdfConfig, uf: &UserFunction, args: &[Value]) -> anyhow::Result<Value> {
    if args.len() != uf.arg_types.len() {
        anyhow::bail!("function \"{}\" expects {} argument(s), got {}", uf.name, uf.arg_types.len(), args.len());
    }

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config)?;
    let module = Module::new(&engine, &uf.wasm_bytes)?;

    let limits = StoreLimitsBuilder::new().memory_size(cfg.memory_limit_bytes).build();
    let mut store = Store::new(&engine, limits);
    store.limiter(|s| s);
    store.set_fuel(cfg.fuel_limit)?;

    let linker: Linker<StoreLimits> = Linker::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| anyhow::anyhow!("failed to instantiate WASM UDF \"{}\": {e}", uf.name))?;
    let func = instance
        .get_func(&mut store, &uf.name)
        .ok_or_else(|| anyhow::anyhow!("function \"{}\" export not found in its own WASM module", uf.name))?;

    let params: Vec<Val> = args
        .iter()
        .zip(&uf.arg_types)
        .map(|(v, ty)| to_wasm_val(v, ty))
        .collect::<anyhow::Result<_>>()?;
    let mut results = vec![Val::I32(0)];
    func.call(&mut store, &params, &mut results)
        .map_err(|e| anyhow::anyhow!("WASM UDF \"{}\" failed: {e}", uf.name))?;

    Ok(from_wasm_val(&results[0], &uf.return_type))
}

fn to_wasm_val(v: &Value, ty: &ColumnType) -> anyhow::Result<Val> {
    match (v, ty) {
        (Value::Int(n), ColumnType::Int8) => Ok(Val::I64(*n)),
        (Value::Float(f), ColumnType::Float8) => Ok(Val::F64(f.to_bits())),
        (Value::Bool(b), ColumnType::Bool) => Ok(Val::I32(if *b { 1 } else { 0 })),
        (Value::Null, _) => anyhow::bail!("NULL is not supported as a WASM UDF argument"),
        (other, ty) => anyhow::bail!("cannot pass a {} value as a {ty:?} WASM UDF argument", other.type_name()),
    }
}

fn from_wasm_val(v: &Val, ty: &ColumnType) -> Value {
    match (v, ty) {
        (Val::I64(n), ColumnType::Int8) => Value::Int(*n),
        (Val::F64(bits), ColumnType::Float8) => Value::Float(f64::from_bits(*bits)),
        (Val::I32(n), ColumnType::Bool) => Value::Bool(*n != 0),
        _ => Value::Null,
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

    fn test_db_with_udf_config(udf_config: UdfConfig) -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
        let bucket = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
        let lease = Arc::new(LeaseManager::new(store.clone(), "db", "node-1".into(), Duration::from_secs(30)));
        lease.try_acquire().unwrap();
        let db = Arc::new(Database::open(local.path(), store, lease.handle(), NodeRole::Writer, udf_config).unwrap());
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
        let sql = format!("CREATE FUNCTION add_one(n int8) RETURNS int8 LANGUAGE wasm AS '{wasm_b64}'");
        execute_query(&sql, db.clone()).unwrap();

        let result = execute_query("SELECT add_one(41)", db.clone()).unwrap();
        assert_eq!(result.rows[0][0].as_deref(), Some(b"42".as_slice()));
    }

    #[test]
    fn create_function_rejects_signature_mismatch() {
        let (db, _b, _l) = test_db();
        // Export returns i64, but the SQL declares RETURNS float8.
        let wasm_b64 = wat_base64(
            r#"(module (func (export "bad") (param i64) (result i64)
                 (local.get 0)))"#,
        );
        let sql = format!("CREATE FUNCTION bad(n int8) RETURNS float8 LANGUAGE wasm AS '{wasm_b64}'");
        assert!(execute_query(&sql, db.clone()).is_err());
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
    fn create_function_rejects_unsupported_type() {
        let (db, _b, _l) = test_db();
        let wasm_b64 = wat_base64(r#"(module (func (export "f") (param i64) (result i64) (local.get 0)))"#);
        let sql = format!("CREATE FUNCTION f(n text) RETURNS int8 LANGUAGE wasm AS '{wasm_b64}'");
        assert!(execute_query(&sql, db.clone()).is_err());
    }
}
