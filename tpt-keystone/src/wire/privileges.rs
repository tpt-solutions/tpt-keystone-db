//! `_tpt_privileges` — object-privilege grant catalog for RBAC (`TODO.md`
//! Phase 20). A row means "grantee role holds `privilege` on `object`".
//! Persisted as a plain Keystone table alongside `_tpt_roles` /
//! `_tpt_role_members`.

use std::sync::Arc;

use crate::sql::ast::{GrantObject, Privilege};
use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{col, decode_cell, encode_cells, text_cell};

const TABLE: &str = "_tpt_privileges";

/// A grantable object privilege, including the synthetic `ALL` used by
/// `GRANT/REVOKE ALL`. Stored as its lowercase name string in `_tpt_privileges`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivilegeRepr {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Usage,
    All,
}

impl PrivilegeRepr {
    pub fn as_str(&self) -> &'static str {
        match self {
            PrivilegeRepr::Select => "select",
            PrivilegeRepr::Insert => "insert",
            PrivilegeRepr::Update => "update",
            PrivilegeRepr::Delete => "delete",
            PrivilegeRepr::Create => "create",
            PrivilegeRepr::Drop => "drop",
            PrivilegeRepr::Usage => "usage",
            PrivilegeRepr::All => "all",
        }
    }

    pub fn from_ast(p: Privilege) -> Self {
        match p {
            Privilege::Select => PrivilegeRepr::Select,
            Privilege::Insert => PrivilegeRepr::Insert,
            Privilege::Update => PrivilegeRepr::Update,
            Privilege::Delete => PrivilegeRepr::Delete,
            Privilege::Create => PrivilegeRepr::Create,
            Privilege::Drop => PrivilegeRepr::Drop,
            Privilege::Usage => PrivilegeRepr::Usage,
        }
    }
}

/// A grant target: a single relation or the whole database. Stored as an
/// object-type tag (`TABLE`/`DATABASE`) plus the object name (empty for the
/// database).
#[derive(Debug, Clone)]
pub struct GrantObjectRepr {
    pub kind: &'static str,
    pub name: String,
}

impl GrantObjectRepr {
    pub fn table(name: &str) -> Self {
        GrantObjectRepr {
            kind: "TABLE",
            name: name.to_string(),
        }
    }

    pub fn database() -> Self {
        GrantObjectRepr {
            kind: "DATABASE",
            name: String::new(),
        }
    }

    pub fn from_ast(o: &GrantObject) -> Self {
        match o {
            GrantObject::Table(t) => GrantObjectRepr::table(t),
            GrantObject::Database => GrantObjectRepr::database(),
        }
    }

    /// `(object_type, object_name)` cells for row storage.
    pub fn cells(&self) -> (&str, &str) {
        (self.kind, &self.name)
    }

    pub fn label(&self) -> String {
        match self.kind {
            "DATABASE" => "DATABASE".to_string(),
            _ => format!("TABLE {}", self.name),
        }
    }
}

fn row_key(grantee: &str, priv_: &str, obj_type: &str, obj_name: &str) -> Vec<u8> {
    format!("{grantee}\u{0}{priv_}\u{0}{obj_type}\u{0}{obj_name}").into_bytes()
}

pub struct PrivilegeStore {
    db: Arc<Database>,
}

impl PrivilegeStore {
    pub fn ensure_schema(db: &Arc<Database>) -> anyhow::Result<()> {
        if db.get_table(TABLE)?.is_some() {
            return Ok(());
        }
        db.create_table_with_constraints(
            TABLE,
            &[
                col("id", ColumnType::Text, false, true),
                col("grantee", ColumnType::Text, false, false),
                col("privilege", ColumnType::Text, false, false),
                col("object_type", ColumnType::Text, false, false),
                col("object_name", ColumnType::Text, false, false),
            ],
            vec![],
            vec![],
        )
    }

    pub fn new(db: Arc<Database>) -> anyhow::Result<Self> {
        Self::ensure_schema(&db)?;
        Ok(Self { db })
    }

    /// Grant `grantee` `privilege` on `object`. Idempotent on the row key.
    pub fn grant(
        &self,
        grantee: &str,
        privilege: PrivilegeRepr,
        object: &GrantObjectRepr,
    ) -> anyhow::Result<()> {
        let (obj_type, obj_name) = object.cells();
        let key = row_key(grantee, &privilege.as_str(), obj_type, obj_name);
        let cells = vec![
            text_cell(String::from_utf8_lossy(&key).into_owned()),
            text_cell(grantee),
            text_cell(privilege.as_str()),
            text_cell(obj_type),
            text_cell(obj_name),
        ];
        self.db.write(TABLE, &key, &encode_cells(&cells))
    }

    /// Revoke `privilege` from `grantee` on `object`. When `privilege` is
    /// `ALL`, every privilege the grantee holds on that object is revoked.
    pub fn revoke(
        &self,
        grantee: &str,
        privilege: PrivilegeRepr,
        object: &GrantObjectRepr,
    ) -> anyhow::Result<()> {
        let (obj_type, obj_name) = object.cells();
        if privilege == PrivilegeRepr::All {
            let mut to_delete = Vec::new();
            for kv in self.db.scan(TABLE)? {
                if cell_str(&kv.value, 1)? == grantee
                    && cell_str(&kv.value, 3)? == obj_type
                    && cell_str(&kv.value, 4)? == obj_name
                {
                    to_delete.push(kv.key.clone());
                }
            }
            for key in to_delete {
                self.db.delete(TABLE, &key)?;
            }
            return Ok(());
        }
        self.db
            .delete(TABLE, &row_key(grantee, &privilege.as_str(), obj_type, obj_name))
    }

    /// Whether `role` (or any role it is a transitive member of) directly
    /// holds `privilege` on `object`.
    pub fn has_privilege(
        &self,
        db: &Arc<Database>,
        role: &str,
        privilege: PrivilegeRepr,
        object: &GrantObjectRepr,
    ) -> anyhow::Result<bool> {
        let (obj_type, obj_name) = object.cells();
        let members = crate::wire::role_members::RoleMemberStore::new(db.clone())?;
        let mut candidates = vec![role.to_string()];
        candidates.extend(members.all_memberships(role)?);
        for cand in candidates {
            for kv in self.db.scan(TABLE)? {
                if cell_str(&kv.value, 1)? != cand {
                    continue;
                }
                if cell_str(&kv.value, 3)? != obj_type {
                    continue;
                }
                if cell_str(&kv.value, 4)? != obj_name {
                    continue;
                }
                // An explicit ALL grant also satisfies any specific privilege.
                let stored = cell_str(&kv.value, 2)?;
                if stored == PrivilegeRepr::All.as_str() || stored == privilege.as_str() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

fn cell_str(value: &[u8], idx: usize) -> anyhow::Result<String> {
    decode_cell(value, idx)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| anyhow::anyhow!("missing/invalid _tpt_privileges cell {idx}"))
}
