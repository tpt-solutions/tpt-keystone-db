//! `_tpt_role_members` — role-membership catalog for RBAC (`TODO.md` Phase 20).
//!
//! A row means "role `member` is a direct member of role `group`" (the same
//! `member`/`roleid` direction Postgres's `pg_auth_members` uses, where
//! `member` is the role that *has* the membership and `roleid` is the group it
//! is a member *of*). Persisted as a plain Keystone table, following the same
//! precedent as `_tpt_roles`/`_tpt_privileges`.

use std::sync::Arc;

use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{col, decode_cell, encode_cells, text_cell};

const TABLE: &str = "_tpt_role_members";

/// Composite row key: `member\u{0}group`.
fn row_key(member: &str, group: &str) -> Vec<u8> {
    format!("{member}\u{0}{group}").into_bytes()
}

pub struct RoleMemberStore {
    db: Arc<Database>,
}

impl RoleMemberStore {
    pub fn ensure_schema(db: &Arc<Database>) -> anyhow::Result<()> {
        if db.get_table(TABLE)?.is_some() {
            return Ok(());
        }
        db.create_table_with_constraints(
            TABLE,
            &[
                col("id", ColumnType::Text, false, true),
                col("member", ColumnType::Text, false, false),
                col("group_role", ColumnType::Text, false, false),
            ],
            vec![],
            vec![],
        )
    }

    pub fn new(db: Arc<Database>) -> anyhow::Result<Self> {
        Self::ensure_schema(&db)?;
        Ok(Self { db })
    }

    /// Grant `member` direct membership in `group`. Rejects a self-membership
    /// and any grant that would introduce a cycle in the membership graph.
    pub fn grant_membership(&self, member: &str, group: &str) -> anyhow::Result<()> {
        if member == group {
            anyhow::bail!("role \"{member}\" cannot be a member of itself");
        }
        // A cycle exists if `group` can already reach `member` through
        // memberships (member -> group -> ... -> member) or vice-versa
        // (member -> ... -> group after we add member -> group would close
        // member -> ... -> member). Check both directions of the transitive
        // closure before adding the edge.
        if self
            .all_memberships(member)?
            .iter()
            .any(|g| g == group)
            || self.all_memberships(group)?.iter().any(|m| m == member)
        {
            anyhow::bail!("granting \"{member}\" membership in \"{group}\" would create a role-membership cycle");
        }
        let key = row_key(member, group);
        let cells = vec![
            text_cell(String::from_utf8_lossy(&key).into_owned()),
            text_cell(member),
            text_cell(group),
        ];
        self.db.write(TABLE, &key, &encode_cells(&cells))
    }

    /// Revoke `member`'s direct membership in `group`.
    pub fn revoke_membership(&self, member: &str, group: &str) -> anyhow::Result<()> {
        self.db.delete(TABLE, &row_key(member, group))
    }

    /// Revoke every membership edge touching `role` (as member or as group).
    /// Used by `DROP ROLE` so a removed role leaves no dangling edges.
    pub fn revoke_all(&self, role: &str) -> anyhow::Result<()> {
        let mut to_delete = Vec::new();
        for kv in self.db.scan(TABLE)? {
            let member = cell_str(&kv.value, 1)?;
            let group = cell_str(&kv.value, 2)?;
            if member == role || group == role {
                to_delete.push(kv.key.clone());
            }
        }
        for key in to_delete {
            self.db.delete(TABLE, &key)?;
        }
        Ok(())
    }

    /// Groups `role` is a *direct* (one-hop) member of.
    pub fn direct_memberships(&self, role: &str) -> anyhow::Result<Vec<String>> {
        let mut out = Vec::new();
        for kv in self.db.scan(TABLE)? {
            if cell_str(&kv.value, 1)? == role {
                out.push(cell_str(&kv.value, 2)?);
            }
        }
        Ok(out)
    }

    /// Full transitive (reflexive-agnostic) closure of groups `role` belongs
    /// to, via a breadth-first walk over `direct_memberships`. No cycle can
    /// exist (grant rejects them), so the walk always terminates.
    pub fn all_memberships(&self, role: &str) -> anyhow::Result<Vec<String>> {
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut frontier = vec![role.to_string()];
        while let Some(current) = frontier.pop() {
            for group in self.direct_memberships(&current)? {
                if seen.insert(group.clone()) {
                    result.push(group.clone());
                    frontier.push(group);
                }
            }
        }
        Ok(result)
    }
}

fn cell_str(value: &[u8], idx: usize) -> anyhow::Result<String> {
    decode_cell(value, idx)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| anyhow::anyhow!("missing/invalid _tpt_role_members cell {idx}"))
}
