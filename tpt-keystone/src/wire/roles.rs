//! `_tpt_roles` — the wire-auth credential catalog. Stores per-user SCRAM
//! `StoredKey`/`ServerKey` (never the plaintext password), following the
//! same "plain Keystone table, `CREATE TABLE IF NOT EXISTS` at open time"
//! precedent Synapse/Mirror's own system tables already establish
//! (`synapse::actor::AGENTS_TABLE`, `mirror::metrics`).
//!
//! Scope cut: no `CREATE ROLE`/`ALTER ROLE`/`DROP ROLE` SQL DDL yet — a role
//! is only ever created by `bootstrap_if_empty` (from `TPT_AUTH_BOOTSTRAP_USER`/
//! `_PASSWORD`), which exists to solve the chicken-and-egg problem of
//! creating a first credential with no SQL access yet. Wiring DDL up to this
//! same table is a natural, separate follow-up.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine};

use super::scram::{derive_credential, random_salt, ScramCredential};
use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{col, decode_cell, encode_cells, int_cell, now_ms, text_cell};

const TABLE: &str = "_tpt_roles";

pub struct RoleStore {
    db: Arc<Database>,
}

impl RoleStore {
    pub fn new(db: Arc<Database>) -> anyhow::Result<Self> {
        Self::ensure_schema(&db)?;
        Ok(Self { db })
    }

    fn ensure_schema(db: &Arc<Database>) -> anyhow::Result<()> {
        if db.get_table(TABLE)?.is_some() {
            return Ok(());
        }
        db.create_table_with_constraints(
            TABLE,
            &[
                col("rolname", ColumnType::Text, false, true),
                col("salt", ColumnType::Text, false, false),
                col("iterations", ColumnType::Int8, false, false),
                col("stored_key", ColumnType::Text, false, false),
                col("server_key", ColumnType::Text, false, false),
                col("created_at", ColumnType::Int8, false, false),
            ],
            vec![],
            vec![],
        )
    }

    /// Whether any role has ever been configured — while empty, wire auth is
    /// skipped entirely and `session::run` falls back to today's unconditional
    /// `AuthenticationOk`, so a zero-config node behaves exactly as before.
    pub fn is_empty(&self) -> anyhow::Result<bool> {
        Ok(self.db.scan(TABLE)?.is_empty())
    }

    pub fn find(&self, rolname: &str) -> anyhow::Result<Option<ScramCredential>> {
        let Some(value) = self.db.read(TABLE, rolname.as_bytes())? else {
            return Ok(None);
        };
        let salt_b64 = cell_str(&value, 1)?;
        let iterations = cell_str(&value, 2)?.parse::<u32>()?;
        let stored_key_b64 = cell_str(&value, 3)?;
        let server_key_b64 = cell_str(&value, 4)?;

        let salt = STANDARD.decode(salt_b64)?;
        let stored_key: [u8; 32] = STANDARD.decode(stored_key_b64)?.try_into()
            .map_err(|_| anyhow::anyhow!("corrupt stored_key for role \"{rolname}\""))?;
        let server_key: [u8; 32] = STANDARD.decode(server_key_b64)?.try_into()
            .map_err(|_| anyhow::anyhow!("corrupt server_key for role \"{rolname}\""))?;

        Ok(Some(ScramCredential { salt, iterations, stored_key, server_key }))
    }

    /// Creates or replaces a role's credential. Iteration count matches
    /// real Postgres's SCRAM default (4096).
    pub fn upsert(&self, rolname: &str, password: &str) -> anyhow::Result<()> {
        let salt = random_salt();
        let cred = derive_credential(password, &salt, 4096);
        let cells = vec![
            text_cell(rolname),
            text_cell(STANDARD.encode(&cred.salt)),
            int_cell(cred.iterations as i64),
            text_cell(STANDARD.encode(cred.stored_key)),
            text_cell(STANDARD.encode(cred.server_key)),
            int_cell(now_ms()),
        ];
        self.db.write(TABLE, rolname.as_bytes(), &encode_cells(&cells))
    }

    /// Seeds the very first role from env-configured bootstrap credentials,
    /// only if the catalog is still empty — never overwrites an
    /// operator-managed role on every restart.
    pub fn bootstrap_if_empty(&self, user: &str, password: &str) -> anyhow::Result<()> {
        if self.is_empty()? {
            self.upsert(user, password)?;
        }
        Ok(())
    }
}

fn cell_str(value: &[u8], idx: usize) -> anyhow::Result<String> {
    decode_cell(value, idx)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| anyhow::anyhow!("missing/invalid _tpt_roles cell {idx}"))
}
