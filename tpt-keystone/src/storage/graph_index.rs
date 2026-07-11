//! Local (per-node, not object-store-replicated — same documented scope cut
//! as `storage::btree`/`storage::geo_index`/`storage::ts_index`) adjacency
//! index for Plexus graph traversal, built from `CREATE INDEX ... USING
//! GRAPH (from_col) WITH (to = 'to_col' [, type = 'rel_col'])` on an edge
//! table.
//!
//! Wraps `graph::AdjacencyGraph` (the in-memory dense adjacency-list
//! structure) with the same append-only record log + full-replay-on-open
//! persistence model `GeoIndex` uses: acceptable for a local secondary-index
//! accelerator that's cheap to rebuild from the table on first open if the
//! file is missing.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::graph::AdjacencyGraph;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EdgeRecord {
    from: Vec<u8>,
    to: Vec<u8>,
    rel_type: Option<String>,
}

pub struct GraphIndex {
    path: PathBuf,
    to_column: String,
    type_column: Option<String>,
    graph: AdjacencyGraph,
}

impl GraphIndex {
    /// Opens (replaying any existing records) or creates a fresh graph
    /// index file. `to_column`/`type_column` name the edge table's
    /// destination-vertex and (optional) relationship-type columns; they're
    /// stored in the file header so a later open (via `read_dir` in
    /// `Database::open`, which doesn't otherwise know this DDL-time config)
    /// recovers the same wiring.
    pub fn open(
        path: &Path,
        default_to_column: &str,
        default_type_column: Option<&str>,
    ) -> Result<Self> {
        if !path.exists() {
            let idx = Self {
                path: path.to_path_buf(),
                to_column: default_to_column.to_string(),
                type_column: default_type_column.map(|s| s.to_string()),
                graph: AdjacencyGraph::new(),
            };
            idx.write_header()?;
            return Ok(idx);
        }
        let mut file = BufReader::new(File::open(path)?);
        let to_column = read_string(&mut file)?;
        let has_type = read_u8(&mut file)? != 0;
        let type_column = if has_type {
            Some(read_string(&mut file)?)
        } else {
            None
        };

        let mut graph = AdjacencyGraph::new();
        let mut len_buf = [0u8; 4];
        loop {
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            let rec: EdgeRecord = bincode::deserialize(&buf)?;
            graph.add_edge(&rec.from, &rec.to, rec.rel_type);
        }
        Ok(Self {
            path: path.to_path_buf(),
            to_column,
            type_column,
            graph,
        })
    }

    fn write_header(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        write_string(&mut file, &self.to_column)?;
        match &self.type_column {
            Some(t) => {
                file.write_all(&[1])?;
                write_string(&mut file, t)?;
            }
            None => file.write_all(&[0])?,
        }
        Ok(())
    }

    pub fn to_column(&self) -> &str {
        &self.to_column
    }

    pub fn type_column(&self) -> Option<&str> {
        self.type_column.as_deref()
    }

    pub fn graph(&self) -> &AdjacencyGraph {
        &self.graph
    }

    /// Records one edge. Appends to the on-disk log and updates the
    /// in-memory adjacency graph.
    pub fn insert(&mut self, from: &[u8], to: &[u8], rel_type: Option<String>) -> Result<()> {
        let rec = EdgeRecord {
            from: from.to_vec(),
            to: to.to_vec(),
            rel_type: rel_type.clone(),
        };
        let encoded = bincode::serialize(&rec)?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        file.write_all(&encoded)?;
        self.graph.add_edge(from, to, rel_type);
        Ok(())
    }
}

fn write_string(file: &mut File, s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    file.write_all(&(bytes.len() as u32).to_be_bytes())?;
    file.write_all(bytes)?;
    Ok(())
}

fn read_string(file: &mut BufReader<File>) -> Result<String> {
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn read_u8(file: &mut BufReader<File>) -> Result<u8> {
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf)?;
    Ok(buf[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Direction;

    #[test]
    fn insert_and_query_neighbors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.graph");
        let mut idx = GraphIndex::open(&path, "to_id", Some("rel")).unwrap();
        idx.insert(b"1", b"2", Some("FOLLOWS".into())).unwrap();
        idx.insert(b"2", b"3", Some("FOLLOWS".into())).unwrap();

        let v1 = idx.graph().id_of(b"1").unwrap();
        let neighbors = idx.graph().neighbors(v1, Direction::Out);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(idx.graph().key_of(neighbors[0].0), Some(b"2".as_slice()));
    }

    #[test]
    fn reopen_replays_log_and_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.graph");
        {
            let mut idx = GraphIndex::open(&path, "to_id", Some("rel")).unwrap();
            idx.insert(b"1", b"2", Some("FOLLOWS".into())).unwrap();
        }
        let reopened = GraphIndex::open(&path, "wrong_default", None).unwrap();
        assert_eq!(reopened.to_column(), "to_id");
        assert_eq!(reopened.type_column(), Some("rel"));
        let v1 = reopened.graph().id_of(b"1").unwrap();
        assert_eq!(reopened.graph().neighbors(v1, Direction::Out).len(), 1);
    }
}
