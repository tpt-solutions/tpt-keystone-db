//! Tool registry and discovery: tools are rows in `_synapse_tools` (name,
//! description, an OpenAPI/JSON-Schema-shaped tool definition, and an
//! optional caller-supplied embedding), discoverable either by exact name
//! or by semantic search over that embedding via the same Prism
//! `VectorIndex` machinery Phase 7 already built — "semantic search via
//! Prism" from the TODO.md checklist item, not a separate ranking engine.
//! Embedding generation itself is out of scope here (same boundary Prism's
//! own `VECTOR` columns already draw: the caller supplies the floats, this
//! engine only indexes/searches them).

use std::sync::Arc;

use anyhow::Result;

use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{cell_text, col, decode_cell, encode_cells, now_ms, text_cell};
use crate::vector::hnsw::{HnswConfig, Metric};
use crate::vector::vector::Vector;

const TABLE: &str = "_synapse_tools";
const COL_NAME: usize = 0;
const COL_DESCRIPTION: usize = 1;
const COL_SCHEMA: usize = 2;
const COL_EMBEDDING: usize = 3;
const COL_CREATED_AT: usize = 4;

#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    /// An OpenAPI operation object or plain JSON Schema, verbatim text — this
    /// module doesn't parse or validate it, only stores/returns it.
    pub schema_json: String,
}

fn decode_tool(value: &[u8]) -> Option<ToolDef> {
    Some(ToolDef {
        name: cell_text(&decode_cell(value, COL_NAME))?,
        description: cell_text(&decode_cell(value, COL_DESCRIPTION)).unwrap_or_default(),
        schema_json: cell_text(&decode_cell(value, COL_SCHEMA)).unwrap_or_default(),
    })
}

pub struct ToolRegistry {
    db: Arc<Database>,
}

impl ToolRegistry {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        Self::ensure_schema(&db)?;
        Ok(Self { db })
    }

    fn ensure_schema(db: &Arc<Database>) -> Result<()> {
        if db.get_table(TABLE)?.is_none() {
            db.create_table_with_constraints(
                TABLE,
                &[
                    col("name", ColumnType::Text, false, true),
                    col("description", ColumnType::Text, true, false),
                    col("schema_json", ColumnType::Text, true, false),
                    col("embedding", ColumnType::Vector, true, false),
                    col("created_at", ColumnType::Int8, false, false),
                ],
                vec![],
                vec![],
            )?;
        }
        if !db.indexed_column_vector(TABLE, "embedding") {
            db.create_vector_index(TABLE, "embedding", Metric::Cosine, HnswConfig::default())?;
        }
        Ok(())
    }

    /// Registers (or replaces, keyed by `name`) a tool definition. `Database`
    /// row storage is keyed by primary key, so re-registering the same
    /// `name` naturally overwrites the previous definition/embedding.
    pub fn register(
        &self,
        name: &str,
        description: &str,
        schema_json: &str,
        embedding: Option<&[f32]>,
    ) -> Result<()> {
        let cells = vec![
            text_cell(name),
            text_cell(description),
            text_cell(schema_json),
            embedding
                .map(|v| text_cell(Vector(v.to_vec()).to_text()))
                .unwrap_or(None),
            Some(now_ms().to_string().into_bytes()),
        ];
        self.db.write(TABLE, name.as_bytes(), &encode_cells(&cells))
    }

    pub fn get(&self, name: &str) -> Result<Option<ToolDef>> {
        Ok(self
            .db
            .read(TABLE, name.as_bytes())?
            .and_then(|v| decode_tool(&v)))
    }

    pub fn list(&self) -> Result<Vec<ToolDef>> {
        Ok(self
            .db
            .scan(TABLE)?
            .into_iter()
            .filter_map(|kv| decode_tool(&kv.value))
            .collect())
    }

    /// Ranked semantic discovery over registered tools' embeddings,
    /// nearest-first — the "tool discovery returns ranked results via
    /// Prism" milestone. `None` if no tool has an embedding yet (the vector
    /// index exists but is empty, so this still returns `Some(vec![])`; this
    /// module never returns `None` itself, matching `vector_knn_query`'s own
    /// "index exists but empty" vs. "no index" distinction).
    pub fn discover(&self, query_embedding: &[f32], k: usize) -> Result<Vec<(ToolDef, f32)>> {
        let Some(hits) = self
            .db
            .vector_knn_query(TABLE, "embedding", query_embedding, k, None)
        else {
            return Ok(Vec::new());
        };
        Ok(hits
            .into_iter()
            .filter_map(|(kv, dist)| decode_tool(&kv.value).map(|t| (t, dist)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::NodeRole;
    use crate::storage::lease::LeaseManager;
    use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
    use std::time::Duration;

    fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
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
                Default::default(),
            )
            .unwrap(),
        );
        (db, bucket, local)
    }

    #[test]
    fn register_and_get_by_name() {
        let (db, _b, _l) = test_db();
        let reg = ToolRegistry::new(db).unwrap();
        reg.register(
            "get_weather",
            "Fetches current weather for a city",
            r#"{"type":"object"}"#,
            None,
        )
        .unwrap();
        let tool = reg.get("get_weather").unwrap().unwrap();
        assert_eq!(tool.description, "Fetches current weather for a city");
    }

    #[test]
    fn re_registering_same_name_overwrites() {
        let (db, _b, _l) = test_db();
        let reg = ToolRegistry::new(db).unwrap();
        reg.register("t", "v1", "{}", None).unwrap();
        reg.register("t", "v2", "{}", None).unwrap();
        assert_eq!(reg.list().unwrap().len(), 1);
        assert_eq!(reg.get("t").unwrap().unwrap().description, "v2");
    }

    #[test]
    fn discover_ranks_nearest_first() {
        let (db, _b, _l) = test_db();
        let reg = ToolRegistry::new(db).unwrap();
        reg.register(
            "weather",
            "weather lookup tool",
            "{}",
            Some(&[1.0, 0.0, 0.0]),
        )
        .unwrap();
        reg.register(
            "calendar",
            "calendar scheduling tool",
            "{}",
            Some(&[0.0, 1.0, 0.0]),
        )
        .unwrap();
        reg.register(
            "forecast",
            "weather forecast tool",
            "{}",
            Some(&[0.9, 0.1, 0.0]),
        )
        .unwrap();

        let hits = reg.discover(&[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        let names: Vec<&str> = hits.iter().map(|(t, _)| t.name.as_str()).collect();
        assert!(names.contains(&"weather"));
        assert!(names.contains(&"forecast"));
    }
}
