//! Schema Translator — maps a source engine's introspected DDL onto a
//! source-agnostic IR ([`TableSchema`]/[`ColumnSchema`]), then renders that
//! IR as a target `CREATE TABLE` statement. Only the Postgres -> Keystone
//! direction is implemented (`from_postgres_type`/`to_keystone_ddl`); other
//! sources plug into the same IR once their connectors exist (see
//! `src/sources`).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    /// Source-native type name (e.g. Postgres's `pg_type.typname`), kept
    /// around for diagnostics even after translation.
    pub source_type: String,
    pub keystone_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    pub fn primary_key_columns(&self) -> Vec<&str> {
        self.columns.iter().filter(|c| c.is_primary_key).map(|c| c.name.as_str()).collect()
    }

    /// Render this table as a Keystone `CREATE TABLE` statement.
    pub fn to_keystone_ddl(&self) -> String {
        let mut cols = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let mut def = format!("{} {}", quote_ident(&c.name), c.keystone_type);
            if !c.nullable {
                def.push_str(" NOT NULL");
            }
            cols.push(def);
        }
        let pk = self.primary_key_columns();
        if !pk.is_empty() {
            cols.push(format!("PRIMARY KEY ({})", pk.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")));
        }
        format!("CREATE TABLE IF NOT EXISTS {} (\n  {}\n)", quote_ident(&self.name), cols.join(",\n  "))
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Map a Postgres `pg_type.typname` (as returned by
/// `format_type()`/`pg_catalog.pg_type` lookups during discovery) to the
/// closest Keystone SQL column type. Falls back to `TEXT` for anything
/// unrecognized (JSON/array/enum/domain types included) rather than
/// failing discovery — the Verification Engine's checksum pass is what
/// ultimately proves a chosen mapping preserved the data faithfully.
pub fn from_postgres_type(pg_type: &str) -> String {
    let t = pg_type.to_ascii_lowercase();
    match t.trim_start_matches('_') {
        "int2" | "smallint" => "SMALLINT".to_string(),
        "int4" | "integer" | "int" => "INTEGER".to_string(),
        "int8" | "bigint" => "BIGINT".to_string(),
        "float4" | "real" => "REAL".to_string(),
        "float8" | "double precision" | "double" => "DOUBLE PRECISION".to_string(),
        "numeric" | "decimal" => "NUMERIC".to_string(),
        "bool" | "boolean" => "BOOLEAN".to_string(),
        "varchar" | "character varying" => "TEXT".to_string(),
        "bpchar" | "char" | "character" => "TEXT".to_string(),
        "text" | "name" | "citext" => "TEXT".to_string(),
        "uuid" => "UUID".to_string(),
        "timestamp" | "timestamptz" | "timestamp with time zone" | "timestamp without time zone" => "TIMESTAMP".to_string(),
        "date" => "DATE".to_string(),
        "time" | "timetz" => "TIME".to_string(),
        "json" | "jsonb" => "JSONB".to_string(),
        "bytea" => "BYTEA".to_string(),
        _ => "TEXT".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_postgres_types() {
        assert_eq!(from_postgres_type("int4"), "INTEGER");
        assert_eq!(from_postgres_type("varchar"), "TEXT");
        assert_eq!(from_postgres_type("jsonb"), "JSONB");
        assert_eq!(from_postgres_type("some_enum_type"), "TEXT");
    }

    #[test]
    fn renders_create_table_with_primary_key() {
        let t = TableSchema {
            schema: "public".into(),
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), source_type: "int4".into(), keystone_type: "INTEGER".into(), nullable: false, is_primary_key: true },
                ColumnSchema { name: "email".into(), source_type: "text".into(), keystone_type: "TEXT".into(), nullable: true, is_primary_key: false },
            ],
        };
        let ddl = t.to_keystone_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"users\""));
        assert!(ddl.contains("\"id\" INTEGER NOT NULL"));
        assert!(ddl.contains("PRIMARY KEY (\"id\")"));
    }
}
