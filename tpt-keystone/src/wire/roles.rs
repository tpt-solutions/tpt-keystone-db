//! `_tpt_roles` — the wire-auth credential + role-attribute catalog. Stores
//! per-user SCRAM `StoredKey`/`ServerKey` (never the plaintext password)
//! alongside role attributes (`rolsuper`/`rolcanlogin`), following the same
//! "plain Keystone table, `CREATE TABLE IF NOT EXISTS` at open time" precedent
//! Synapse/Mirror's own system tables already establish.
//!
//! Before Phase 20 this catalog only held SCRAM credentials and roles could
//! only be seeded via `TPT_AUTH_BOOTSTRAP_USER`/`_PASSWORD`. Phase 20 (RBAC,
//! `TODO.md`) adds `CREATE/ALTER/DROP ROLE` DDL over this same table, plus the
//! two companion catalogs `_tpt_role_members` (`wire/role_members.rs`) and
//! `_tpt_privileges` (`wire/privileges.rs`) that the executor's authorization
//! layer (`executor::rbac`) reads.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine};

use super::scram::{derive_credential, random_salt, ScramCredential};
use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{col, decode_cell, encode_cells, int_cell, now_ms, text_cell};

const TABLE: &str = "_tpt_roles";

/// Role attributes discoverable without the SCRAM credential material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleAttributes {
    pub superuser: bool,
    pub can_login: bool,
}

pub struct RoleStore {
    db: Arc<Database>,
}

impl RoleStore {
    pub fn new(db: Arc<Database>) -> anyhow::Result<Self> {
        Self::ensure_schema(&db)?;
        // The two companion RBAC catalogs are created alongside this one so a
        // fresh node is fully RBAC-ready even before any role is created.
        crate::wire::role_members::RoleMemberStore::ensure_schema(&db)?;
        crate::wire::privileges::PrivilegeStore::ensure_schema(&db)?;
        Ok(Self { db })
    }

    fn ensure_schema(db: &Arc<Database>) -> anyhow::Result<()> {
        if db.get_table(TABLE)?.is_some() {
            // Migration for legacy installs that predate the `rolsuper`/
            // `rolcanlogin` columns: any existing row (the bootstrap role) was
            // written without them, so normalize it to a superuser that can
            // log in — exactly the role `bootstrap_if_empty` would have
            // created. A legacy install has at most one role (CREATE ROLE did
            // not exist before this phase), so this assumption is safe.
            Self::migrate_legacy_rows(db)?;
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
                col("rolsuper", ColumnType::Text, false, false),
                col("rolcanlogin", ColumnType::Text, false, false),
            ],
            vec![],
            vec![],
        )
    }

    fn migrate_legacy_rows(db: &Arc<Database>) -> anyhow::Result<()> {
        for kv in db.scan(TABLE)? {
            // A non-legacy row already carries 8 cells; leave it untouched.
            if cell_str(&kv.value, 6).is_ok() {
                continue;
            }
            let rolname = decode_cell(&kv.value, 0)
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| anyhow::anyhow!("corrupt _tpt_roles row during migration"))?;
            let salt = cell_str(&kv.value, 1)?;
            let iterations = cell_str(&kv.value, 2)?;
            let stored_key = cell_str(&kv.value, 3)?;
            let server_key = cell_str(&kv.value, 4)?;
            let created_at = cell_str(&kv.value, 5).unwrap_or_else(|_| "0".to_string());
            let cells = vec![
                text_cell(&rolname),
                text_cell(salt),
                text_cell(iterations),
                text_cell(stored_key),
                text_cell(server_key),
                int_cell(created_at.parse().unwrap_or(0)),
                text_cell("t"), // legacy role is the bootstrap superuser
                text_cell("t"),
            ];
            db.write(TABLE, rolname.as_bytes(), &encode_cells(&cells))?;
        }
        Ok(())
    }

    /// Whether any role has ever been configured — while empty, wire auth is
    /// skipped entirely and `session::run` falls back to today's unconditional
    /// `AuthenticationOk`, so a zero-config node behaves exactly as before.
    pub fn is_empty(&self) -> anyhow::Result<bool> {
        Ok(self.db.scan(TABLE)?.is_empty())
    }

    /// Look up a role's attributes (superuser / login privilege) by name.
    pub fn find_role(&self, rolname: &str) -> anyhow::Result<Option<RoleAttributes>> {
        let Some(value) = self.db.read(TABLE, rolname.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(RoleAttributes {
            superuser: cell_str(&value, 6).map(|s| s == "t").unwrap_or(false),
            can_login: cell_str(&value, 7).map(|s| s == "t").unwrap_or(true),
        }))
    }

    pub fn find(&self, rolname: &str) -> anyhow::Result<Option<ScramCredential>> {
        let Some(value) = self.db.read(TABLE, rolname.as_bytes())? else {
            return Ok(None);
        };
        let salt_b64 = match cell_str(&value, 1) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let iterations = match cell_str(&value, 2)?.parse::<u32>() {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let stored_key_b64 = match cell_str(&value, 3) {
            Ok(s) if !s.is_empty() => s,
            _ => return Ok(None),
        };
        let server_key_b64 = match cell_str(&value, 4) {
            Ok(s) if !s.is_empty() => s,
            _ => return Ok(None),
        };

        let salt = match STANDARD.decode(salt_b64) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let stored_key: [u8; 32] = match STANDARD.decode(stored_key_b64)?.try_into() {
            Ok(k) => k,
            Err(_) => return Ok(None),
        };
        let server_key: [u8; 32] = match STANDARD.decode(server_key_b64)?.try_into() {
            Ok(k) => k,
            Err(_) => return Ok(None),
        };

        Ok(Some(ScramCredential {
            salt,
            iterations,
            stored_key,
            server_key,
        }))
    }

    /// Creates or replaces a role's credential with default attributes
    /// (`NOSUPERUSER LOGIN`). Iteration count matches real Postgres's SCRAM
    /// default (4096).
    pub fn upsert(&self, rolname: &str, password: &str) -> anyhow::Result<()> {
        self.upsert_with_attrs(rolname, Some(password), false, true)
    }

    /// Creates or replaces a role's credential together with its role
    /// attributes. A `None` password means a `NOLOGIN` role with no SCRAM
    /// credential (authentication will reject it before ever reaching the
    /// credential lookup).
    pub fn upsert_with_attrs(
        &self,
        rolname: &str,
        password: Option<&str>,
        superuser: bool,
        can_login: bool,
    ) -> anyhow::Result<()> {
        let (salt_b64, iterations, stored_key_b64, server_key_b64) = match password {
            Some(pw) => {
                let salt = random_salt();
                let cred = derive_credential(pw, &salt, 4096);
                (
                    STANDARD.encode(&cred.salt),
                    cred.iterations as i64,
                    STANDARD.encode(cred.stored_key),
                    STANDARD.encode(cred.server_key),
                )
            }
            None => ("".to_string(), 0, "".to_string(), "".to_string()),
        };
        let cells = vec![
            text_cell(rolname),
            text_cell(salt_b64),
            int_cell(iterations),
            text_cell(stored_key_b64),
            text_cell(server_key_b64),
            int_cell(now_ms()),
            text_cell(if superuser { "t" } else { "f" }),
            text_cell(if can_login { "t" } else { "f" }),
        ];
        self.db
            .write(TABLE, rolname.as_bytes(), &encode_cells(&cells))
    }

    /// `CREATE ROLE name [SUPERUSER|NOSUPERUSER] [LOGIN|NOLOGIN]
    ///   [PASSWORD '...'] [IN ROLE a, b ...]`.
    pub fn create_role(
        &self,
        name: &str,
        superuser: bool,
        can_login: bool,
        password: Option<&str>,
        in_role: &[String],
    ) -> anyhow::Result<()> {
        if self.find_role(name)?.is_some() {
            anyhow::bail!("role \"{name}\" already exists");
        }
        self.upsert_with_attrs(name, password, superuser, can_login)?;
        let members = crate::wire::role_members::RoleMemberStore::new(self.db.clone())?;
        for group in in_role {
            members.grant_membership(name, group)?;
        }
        Ok(())
    }

    /// `ALTER ROLE name [SUPERUSER|NOSUPERUSER] [LOGIN|NOLOGIN]
    ///   [PASSWORD '...']` — each clause optional, applied independently.
    pub fn alter_role(
        &self,
        name: &str,
        superuser: Option<bool>,
        can_login: Option<bool>,
        password: Option<&str>,
    ) -> anyhow::Result<()> {
        let Some(value) = self.db.read(TABLE, name.as_bytes())? else {
            anyhow::bail!("role \"{name}\" does not exist");
        };
        let current_super = cell_str(&value, 6).map(|s| s == "t").unwrap_or(false);
        let current_login = cell_str(&value, 7).map(|s| s == "t").unwrap_or(true);
        let new_super = superuser.unwrap_or(current_super);
        let new_login = can_login.unwrap_or(current_login);

        match password {
            Some(pw) => self.upsert_with_attrs(name, Some(pw), new_super, new_login)?,
            None => {
                let salt = cell_str(&value, 1).unwrap_or_default();
                let iterations = cell_str(&value, 2).unwrap_or_else(|_| "0".to_string());
                let stored_key = cell_str(&value, 3).unwrap_or_default();
                let server_key = cell_str(&value, 4).unwrap_or_default();
                let created_at = cell_str(&value, 5).unwrap_or_else(|_| "0".to_string());
                let cells = vec![
                    text_cell(name),
                    text_cell(salt),
                    text_cell(iterations),
                    text_cell(stored_key),
                    text_cell(server_key),
                    int_cell(created_at.parse().unwrap_or(0)),
                    text_cell(if new_super { "t" } else { "f" }),
                    text_cell(if new_login { "t" } else { "f" }),
                ];
                self.db
                    .write(TABLE, name.as_bytes(), &encode_cells(&cells))?;
            }
        }
        Ok(())
    }

    /// `DROP ROLE [IF EXISTS] name`.
    pub fn drop_role(&self, name: &str, if_exists: bool) -> anyhow::Result<()> {
        if self.find_role(name)?.is_none() {
            if if_exists {
                return Ok(());
            }
            anyhow::bail!("role \"{name}\" does not exist");
        }
        self.db.delete(TABLE, name.as_bytes())?;
        let members = crate::wire::role_members::RoleMemberStore::new(self.db.clone())?;
        members.revoke_all(name)?;
        Ok(())
    }

    /// Seeds the very first role from env-configured bootstrap credentials,
    /// only if the catalog is still empty — never overwrites an
    /// operator-managed role on every restart. The bootstrap role is always a
    /// superuser that can log in.
    pub fn bootstrap_if_empty(&self, user: &str, password: &str) -> anyhow::Result<()> {
        if self.is_empty()? {
            self.upsert_with_attrs(user, Some(password), true, true)?;
        }
        Ok(())
    }
}

fn cell_str(value: &[u8], idx: usize) -> anyhow::Result<String> {
    decode_cell(value, idx)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| anyhow::anyhow!("missing/invalid _tpt_roles cell {idx}"))
}
