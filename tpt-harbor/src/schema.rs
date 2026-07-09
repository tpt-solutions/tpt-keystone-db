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

/// Map a PostGIS/Postgres type name to Keystone. Geometry and geography
/// columns become `GEOMETRY` (WKT text), everything else delegates to
/// `from_postgres_type`.
pub fn from_postgis_type(pg_type: &str) -> String {
    let t = pg_type.to_ascii_lowercase();
    match t.as_str() {
        "geometry" | "geography" | "geometry_dump" => "GEOMETRY".to_string(),
        _ => from_postgres_type(pg_type),
    }
}

/// Map a MySQL `COLUMN_TYPE` (e.g. `int(11)`, `varchar(255)`, `tinyint(1)`)
/// to Keystone. MySQL stores display widths in parentheses — strip them
/// before matching.
pub fn from_mysql_type(mysql_type: &str) -> String {
    // Strip display width / parameter: `int(11)` → `int`, `varchar(255)` → `varchar`
    let base = mysql_type
        .to_ascii_lowercase()
        .chars()
        .take_while(|c| *c != '(')
        .collect::<String>();
    let t = base.trim();
    // Handle unsigned prefix: `unsigned int` → `int`
    let t = t.strip_prefix("unsigned ").unwrap_or(t);
    match t {
        "tinyint" => "SMALLINT".to_string(),
        "smallint" | "mediumint" => "INTEGER".to_string(),
        "int" | "integer" => "INTEGER".to_string(),
        "bigint" => "BIGINT".to_string(),
        "float" => "REAL".to_string(),
        "double" => "DOUBLE PRECISION".to_string(),
        "decimal" | "numeric" => "NUMERIC".to_string(),
        "tinyint(1)" | "bool" | "boolean" => "BOOLEAN".to_string(),
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set" => "TEXT".to_string(),
        "json" => "JSONB".to_string(),
        "date" => "DATE".to_string(),
        "datetime" | "timestamp" => "TIMESTAMP".to_string(),
        "time" => "TIME".to_string(),
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => "BYTEA".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Map a SQL Server `DATA_TYPE` (from `information_schema.columns`) to
/// Keystone.
pub fn from_mssql_type(mssql_type: &str) -> String {
    match mssql_type.to_ascii_lowercase().as_str() {
        "tinyint" => "SMALLINT".to_string(),
        "smallint" => "SMALLINT".to_string(),
        "int" => "INTEGER".to_string(),
        "bigint" => "BIGINT".to_string(),
        "real" | "float" => "DOUBLE PRECISION".to_string(),
        "decimal" | "numeric" | "smallmoney" | "money" => "NUMERIC".to_string(),
        "bit" => "BOOLEAN".to_string(),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" => "TEXT".to_string(),
        "xml" | "json" => "JSONB".to_string(),
        "date" => "DATE".to_string(),
        "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset" => "TIMESTAMP".to_string(),
        "time" => "TIME".to_string(),
        "binary" | "varbinary" | "image" => "BYTEA".to_string(),
        "uniqueidentifier" => "UUID".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Map a MongoDB BSON type name to Keystone. MongoDB types are identified
/// by their wire-protocol type codes; this function takes the human-readable
/// name for clarity.
pub fn from_mongodb_type(bson_type: &str) -> String {
    match bson_type.to_ascii_lowercase().as_str() {
        "double" | "float" | "number_double" => "DOUBLE PRECISION".to_string(),
        "int32" | "int" | "number_int" => "INTEGER".to_string(),
        "int64" | "long" | "number_long" => "BIGINT".to_string(),
        "bool" | "boolean" | "bool" => "BOOLEAN".to_string(),
        "string" | "utf8" | "stringutf8" => "TEXT".to_string(),
        "date" | "timestamp" => "TIMESTAMP".to_string(),
        "objectid" => "TEXT".to_string(), // ObjectId hex string
        "binary" | "binData" => "BYTEA".to_string(),
        "object" | "document" | "objectobj" => "JSONB".to_string(),
        "array" => "JSONB".to_string(),
        "null" | "nulltype" => "TEXT".to_string(), // nullable column
        "decimal128" | "numberdecimal" => "NUMERIC".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Map a Neo4j property type to Keystone. Neo4j uses labels and property
/// types; this maps the Cypher type names.
pub fn from_neo4j_type(neo4j_type: &str) -> String {
    match neo4j_type.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "integer" | "int" | "long" => "BIGINT".to_string(),
        "float" | "double" => "DOUBLE PRECISION".to_string(),
        "string" | "text" | "varchar" => "TEXT".to_string(),
        "date" => "DATE".to_string(),
        "datetime" | "localdatetime" | "timestamp" => "TIMESTAMP".to_string(),
        "duration" | "point" | "cartesianpoint" | "geographicpoint" => "TEXT".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Map an InfluxDB field type to Keystone. InfluxDB uses float, integer,
/// boolean, and string field types.
pub fn from_influx_type(influx_type: &str) -> String {
    match influx_type.to_ascii_lowercase().as_str() {
        "float" | "double" => "DOUBLE PRECISION".to_string(),
        "integer" | "long" | "int" => "BIGINT".to_string(),
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "string" | "tag" | "text" => "TEXT".to_string(),
        "time" | "timestamp" | "datetime" => "TIMESTAMP".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Map an Elasticsearch field type to Keystone. ES types come from the
/// `_mapping` API.
pub fn from_es_type(es_type: &str) -> String {
    match es_type.to_ascii_lowercase().as_str() {
        "long" | "integer" => "BIGINT".to_string(),
        "short" | "byte" => "SMALLINT".to_string(),
        "double" | "float" | "half_float" => "DOUBLE PRECISION".to_string(),
        "scaled_float" => "DOUBLE PRECISION".to_string(),
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "text" | "keyword" | "match_only_text" => "TEXT".to_string(),
        "date" | "date_nanos" => "TIMESTAMP".to_string(),
        "object" | "nested" | "flattened" => "JSONB".to_string(),
        "binary" => "BYTEA".to_string(),
        "ip" | "geo_point" | "geo_shape" | "point" | "shape" => "TEXT".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Map an Oracle data-type name (from `user_tab_columns.data_type`) to
/// Keystone.
pub fn from_oracle_type(oracle_type: &str) -> String {
    match oracle_type.to_ascii_lowercase().as_str() {
        "number" | "float" | "binary_float" | "binary_double" => {
            // Oracle NUMBER can represent any precision — map to NUMERIC
            "NUMERIC".to_string()
        }
        "pls_integer" | "binary_integer" | "simple_integer" => "INTEGER".to_string(),
        "varchar2" | "nvarchar2" | "char" | "nchar" | "clob" | "nclob" | "long" => "TEXT".to_string(),
        "raw" | "long raw" | "blob" | "bfile" => "BYTEA".to_string(),
        "date" | "timestamp" | "timestamp with time zone" | "timestamp with local time zone" => "TIMESTAMP".to_string(),
        "interval year to month" | "interval day to second" => "TEXT".to_string(),
        _ => "TEXT".to_string(),
    }
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

    #[test]
    fn maps_postgis_geometry_types() {
        assert_eq!(from_postgis_type("geometry"), "GEOMETRY");
        assert_eq!(from_postgis_type("geography"), "GEOMETRY");
        assert_eq!(from_postgis_type("int4"), "INTEGER"); // non-spatial delegates to postgres
    }

    #[test]
    fn maps_mysql_types() {
        assert_eq!(from_mysql_type("int(11)"), "INTEGER");
        assert_eq!(from_mysql_type("varchar(255)"), "TEXT");
        assert_eq!(from_mysql_type("tinyint(1)"), "BOOLEAN");
        assert_eq!(from_mysql_type("bigint"), "BIGINT");
        assert_eq!(from_mysql_type("datetime"), "TIMESTAMP");
        assert_eq!(from_mysql_type("json"), "JSONB");
        assert_eq!(from_mysql_type("blob"), "BYTEA");
        assert_eq!(from_mysql_type("unsigned int"), "INTEGER");
    }

    #[test]
    fn maps_mssql_types() {
        assert_eq!(from_mssql_type("int"), "INTEGER");
        assert_eq!(from_mssql_type("bit"), "BOOLEAN");
        assert_eq!(from_mssql_type("nvarchar"), "TEXT");
        assert_eq!(from_mssql_type("datetime2"), "TIMESTAMP");
        assert_eq!(from_mssql_type("varbinary"), "BYTEA");
    }

    #[test]
    fn maps_mongodb_types() {
        assert_eq!(from_mongodb_type("double"), "DOUBLE PRECISION");
        assert_eq!(from_mongodb_type("int32"), "INTEGER");
        assert_eq!(from_mongodb_type("string"), "TEXT");
        assert_eq!(from_mongodb_type("object"), "JSONB");
        assert_eq!(from_mongodb_type("array"), "JSONB");
        assert_eq!(from_mongodb_type("date"), "TIMESTAMP");
    }

    #[test]
    fn maps_neo4j_types() {
        assert_eq!(from_neo4j_type("boolean"), "BOOLEAN");
        assert_eq!(from_neo4j_type("integer"), "BIGINT");
        assert_eq!(from_neo4j_type("string"), "TEXT");
        assert_eq!(from_neo4j_type("float"), "DOUBLE PRECISION");
    }

    #[test]
    fn maps_influx_types() {
        assert_eq!(from_influx_type("float"), "DOUBLE PRECISION");
        assert_eq!(from_influx_type("integer"), "BIGINT");
        assert_eq!(from_influx_type("string"), "TEXT");
        assert_eq!(from_influx_type("boolean"), "BOOLEAN");
    }

    #[test]
    fn maps_elasticsearch_types() {
        assert_eq!(from_es_type("long"), "BIGINT");
        assert_eq!(from_es_type("text"), "TEXT");
        assert_eq!(from_es_type("keyword"), "TEXT");
        assert_eq!(from_es_type("boolean"), "BOOLEAN");
        assert_eq!(from_es_type("date"), "TIMESTAMP");
        assert_eq!(from_es_type("object"), "JSONB");
        assert_eq!(from_es_type("geo_point"), "TEXT");
    }

    #[test]
    fn maps_oracle_types() {
        assert_eq!(from_oracle_type("number"), "NUMERIC");
        assert_eq!(from_oracle_type("varchar2"), "TEXT");
        assert_eq!(from_oracle_type("clob"), "TEXT");
        assert_eq!(from_oracle_type("date"), "TIMESTAMP");
        assert_eq!(from_oracle_type("blob"), "BYTEA");
    }
}
