use anyhow::Result;
use std::cmp::Ordering;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::debug;

/// The order of the B-Tree (max children per node).
const ORDER: usize = 64;

/// A B-Tree index for fast key lookups.
///
/// This is a persistent B-Tree stored on disk. It maps indexed column values
/// to primary keys, enabling fast lookups without scanning the LSM tree.
pub struct BTree {
    path: PathBuf,
    root: Option<u64>, // file offset of root node
}

#[derive(Debug, Clone)]
enum Node {
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<u64>, // file offsets
    },
    Leaf {
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>, // primary keys
    },
}

impl BTree {
    /// Open or create a B-Tree index at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        let root = if path.exists() {
            let mut file = OpenOptions::new().read(true).open(&path)?;
            let mut buf = [0u8; 8];
            if file.read_exact(&mut buf).is_ok() {
                let root_offset = u64::from_be_bytes(buf);
                if root_offset > 0 {
                    Some(root_offset)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { path, root })
    }

    /// Insert a key-value pair (indexed value → primary key).
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.path)?;

        match self.root {
            None => {
                // Reserve the 8-byte root-pointer header (bytes 0..8) before
                // writing the first node, so the node lands at offset 8+
                // instead of offset 0 — otherwise the `write_root` call
                // below would overwrite the node's own header with the root
                // pointer, corrupting the just-inserted entry.
                file.seek(SeekFrom::Start(0))?;
                file.write_all(&0u64.to_be_bytes())?;

                let leaf = Node::Leaf {
                    keys: vec![key.to_vec()],
                    values: vec![value.to_vec()],
                };
                let offset = self.write_node(&mut file, &leaf)?;
                self.root = Some(offset);
                self.write_root(&mut file, offset)?;
            }
            Some(root_offset) => {
                let result = self.insert_into_node(&mut file, root_offset, key, value)?;
                if let Some((promoted_key, left_offset, right_offset)) = result {
                    // Root was split — create a new root
                    let new_root = Node::Internal {
                        keys: vec![promoted_key],
                        children: vec![left_offset, right_offset],
                    };
                    let new_offset = self.write_node(&mut file, &new_root)?;
                    self.root = Some(new_offset);
                    self.write_root(&mut file, new_offset)?;
                }
            }
        }

        file.sync_all()?;
        Ok(())
    }

    /// Search for a key, returning the associated value (primary key) if found.
    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let root = match self.root {
            None => return Ok(None),
            Some(r) => r,
        };

        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        self.search_in_node(&mut file, root, key)
    }

    /// Scan all entries in the B-Tree.
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let root = match self.root {
            None => return Ok(Vec::new()),
            Some(r) => r,
        };

        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut results = Vec::new();
        self.scan_node(&mut file, root, &mut results)?;
        Ok(results)
    }

    fn insert_into_node(
        &self,
        file: &mut std::fs::File,
        node_offset: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, u64, u64)>> {
        let node = self.read_node(file, node_offset)?;

        match node {
            Node::Leaf {
                mut keys,
                mut values,
            } => {
                // Find insertion position
                let pos = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(p) => p,  // key exists — update
                    Err(p) => p, // insert
                };

                if pos < keys.len() && keys[pos] == key {
                    values[pos] = value.to_vec();
                } else {
                    keys.insert(pos, key.to_vec());
                    values.insert(pos, value.to_vec());
                }

                // Check if split needed
                if keys.len() >= ORDER {
                    let mid = keys.len() / 2;
                    let promoted_key = keys[mid].clone();

                    let right_keys = keys.split_off(mid);
                    let right_values = values.split_off(mid);

                    // Remove promoted key from right
                    let _promoted = right_keys[0].clone();
                    let _promoted_val = right_values[0].clone();

                    let right_leaf = Node::Leaf {
                        keys: right_keys[1..].to_vec(),
                        values: right_values[1..].to_vec(),
                    };
                    let right_offset = self.write_node(file, &right_leaf)?;

                    let left_leaf = Node::Leaf { keys, values };
                    let left_offset = self.write_node(file, &left_leaf)?;

                    Ok(Some((promoted_key, left_offset, right_offset)))
                } else {
                    let leaf = Node::Leaf { keys, values };
                    self.write_node_at(file, node_offset, &leaf)?;
                    Ok(None)
                }
            }
            Node::Internal {
                mut keys,
                mut children,
            } => {
                // Find the child to descend into
                let pos = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(p) => p + 1,
                    Err(p) => p,
                };

                let child_offset = children[pos];
                let result = self.insert_into_node(file, child_offset, key, value)?;

                if let Some((promoted_key, left_offset, right_offset)) = result {
                    // Insert promoted key into this internal node
                    let insert_pos =
                        match keys.binary_search_by(|k| k.as_slice().cmp(&promoted_key)) {
                            Ok(p) => p,
                            Err(p) => p,
                        };

                    keys.insert(insert_pos, promoted_key);
                    children[insert_pos] = left_offset;
                    children.insert(insert_pos + 1, right_offset);

                    // Check if this node needs splitting
                    if keys.len() >= ORDER {
                        let mid = keys.len() / 2;
                        let promoted = keys[mid].clone();

                        let right_keys = keys.split_off(mid);
                        let right_children = children.split_off(mid);

                        // Remove promoted key from right
                        let _ = right_keys[0].clone();

                        let right_node = Node::Internal {
                            keys: right_keys[1..].to_vec(),
                            children: right_children[1..].to_vec(),
                        };
                        let right_offset = self.write_node(file, &right_node)?;

                        let left_node = Node::Internal { keys, children };
                        let left_offset = self.write_node_at(file, node_offset, &left_node)?;

                        Ok(Some((promoted, left_offset, right_offset)))
                    } else {
                        let node = Node::Internal { keys, children };
                        self.write_node_at(file, node_offset, &node)?;
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn search_in_node(
        &self,
        file: &mut std::fs::File,
        node_offset: u64,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let node = self.read_node(file, node_offset)?;

        match node {
            Node::Leaf { keys, values } => match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(pos) => Ok(Some(values[pos].clone())),
                Err(_) => Ok(None),
            },
            Node::Internal { keys, children } => {
                let pos = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(p) => p + 1,
                    Err(p) => p,
                };
                self.search_in_node(file, children[pos], key)
            }
        }
    }

    fn scan_node(
        &self,
        file: &mut std::fs::File,
        node_offset: u64,
        results: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let node = self.read_node(file, node_offset)?;

        match node {
            Node::Leaf { keys, values } => {
                for (k, v) in keys.into_iter().zip(values.into_iter()) {
                    results.push((k, v));
                }
            }
            Node::Internal { keys: _, children } => {
                for child in children {
                    self.scan_node(file, child, results)?;
                }
            }
        }

        Ok(())
    }

    fn read_node(&self, file: &mut std::fs::File, offset: u64) -> Result<Node> {
        file.seek(SeekFrom::Start(offset))?;

        let mut tag_buf = [0u8; 1];
        file.read_exact(&mut tag_buf)?;
        let is_leaf = tag_buf[0] == 0;

        let mut count_buf = [0u8; 4];
        file.read_exact(&mut count_buf)?;
        let count = u32::from_be_bytes(count_buf) as usize;

        if is_leaf {
            let mut keys = Vec::with_capacity(count);
            let mut values = Vec::with_capacity(count);

            for _ in 0..count {
                let mut key_len_buf = [0u8; 4];
                file.read_exact(&mut key_len_buf)?;
                let key_len = u32::from_be_bytes(key_len_buf) as usize;
                let mut key = vec![0u8; key_len];
                file.read_exact(&mut key)?;
                keys.push(key);
            }

            for _ in 0..count {
                let mut val_len_buf = [0u8; 4];
                file.read_exact(&mut val_len_buf)?;
                let val_len = u32::from_be_bytes(val_len_buf) as usize;
                let mut value = vec![0u8; val_len];
                file.read_exact(&mut value)?;
                values.push(value);
            }

            Ok(Node::Leaf { keys, values })
        } else {
            let mut keys = Vec::with_capacity(count);
            let mut children = Vec::with_capacity(count + 1);

            for _ in 0..count {
                let mut key_len_buf = [0u8; 4];
                file.read_exact(&mut key_len_buf)?;
                let key_len = u32::from_be_bytes(key_len_buf) as usize;
                let mut key = vec![0u8; key_len];
                file.read_exact(&mut key)?;
                keys.push(key);
            }

            for _ in 0..=count {
                let mut child_buf = [0u8; 8];
                file.read_exact(&mut child_buf)?;
                children.push(u64::from_be_bytes(child_buf));
            }

            Ok(Node::Internal { keys, children })
        }
    }

    fn write_node(&self, file: &mut std::fs::File, node: &Node) -> Result<u64> {
        let offset = file.stream_position()?;
        self.write_node_at(file, offset, node)?;
        Ok(offset)
    }

    fn write_node_at(&self, file: &mut std::fs::File, offset: u64, node: &Node) -> Result<u64> {
        file.seek(SeekFrom::Start(offset))?;

        match node {
            Node::Leaf { keys, values } => {
                file.write_all(&[0u8])?; // tag: leaf
                file.write_all(&(keys.len() as u32).to_be_bytes())?;

                for key in keys {
                    file.write_all(&(key.len() as u32).to_be_bytes())?;
                    file.write_all(key)?;
                }
                for value in values {
                    file.write_all(&(value.len() as u32).to_be_bytes())?;
                    file.write_all(value)?;
                }
            }
            Node::Internal { keys, children } => {
                file.write_all(&[1u8])?; // tag: internal
                file.write_all(&(keys.len() as u32).to_be_bytes())?;

                for key in keys {
                    file.write_all(&(key.len() as u32).to_be_bytes())?;
                    file.write_all(key)?;
                }
                for child in children {
                    file.write_all(&child.to_be_bytes())?;
                }
            }
        }

        Ok(offset)
    }

    fn write_root(&self, file: &mut std::fs::File, offset: u64) -> Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&offset.to_be_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_insert_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.bt");
        {
            let mut bt = BTree::open(&path).unwrap();
            bt.insert(b"k1", b"v1").unwrap();
        }
        let bt = BTree::open(&path).unwrap();
        assert_eq!(bt.search(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(bt.scan().unwrap(), vec![(b"k1".to_vec(), b"v1".to_vec())]);
    }
}
