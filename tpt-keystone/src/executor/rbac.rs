//! RBAC authorization layer (TODO.md Phase 20).
//!
//! A Postgres-style DAC layer over the existing `_tpt_roles` credential
//! catalog (`wire::roles`), the new `_tpt_role_membership` graph and
//! `_tpt_privileges` grants (`wire::role_members` / `wire::privileges`). This
//! module owns:
//!
//! - the per-connection `Actor` (role identity + superuser flag + transitive
//!   memberships), built once right after the SCRAM handshake in
//!   `wire::session::run`;
//! - `Actor::check`, the single authorization predicate consulted by the wire
//!   layer before every statement;
//! - the actual execution of the RBAC DDL statements (`CREATE/ALTER/DROP ROLE`,
//!   `GRANT`/`REVOKE`), which mutate the catalogs.
//!
//! Scope cuts (tracked in TODO.md): no column-level privileges, no `WITH GRANT
//! OPTION` re-delegation, no `ALTER DEFAULT PRIVILEGES`, no object-ownership
//! model (a non-superuser's own `CREATE TABLE` grants no implicit privileges —
//! it must be separately `GRANT`ed or the actor must be superuser), and no
//! `SET ROLE`/`SET SESSION AUTHORIZATION`.

use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;

use crate::sql::ast::{
    AlterRoleStmt, CreateRoleStmt, DropRoleStmt, GrantStmt, Privilege, RevokeStmt, Stmt, TableRef,
    TableWithJoins,
};
use crate::storage::database::Database;
use crate::wire::privileges::{GrantObjectRepr, PrivilegeRepr, PrivilegeStore};
use crate::wire::role_members::RoleMemberStore;
use crate::wire::roles::{RoleAttributes, RoleStore};

/// A permission-denied error. Carries no sensitive detail (the SQLSTATE is
/// enough for a client). The wire layer downcasts this to emit SQLSTATE
/// `42501` instead of the generic `42601` it uses for all other executor
/// errors.
#[derive(Debug)]
pub struct InsufficientPrivilege(pub String);

impl std::fmt::Display for InsufficientPrivilege {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for InsufficientPrivilege {}

/// The identity a connection runs as. Built once per connection (or once per
/// `execute_parsed_as` call in tests) from the catalogs.
#[derive(Debug, Clone)]
pub struct Actor {
    pub rolname: String,
    pub superuser: bool,
    /// `true` when `_tpt_roles` is empty — the zero-config quickstart path,
    /// where wire auth is skipped entirely and every statement is allowed.
    pub unrestricted: bool,
    /// Transitive (closure) groups this role is a member of.
    pub memberships: Vec<String>,
}

impl Actor {
    /// Zero-config mode: no roles configured, everything allowed.
    pub fn unrestricted() -> Self {
        Actor {
            rolname: String::new(),
            superuser: true,
            unrestricted: true,
            memberships: vec![],
        }
    }

    /// Build the actor for an authenticated role from the catalogs.
    pub fn for_role(db: &Arc<Database>, rolname: &str) -> Result<Self> {
        let roles = RoleStore::new(db.clone())?;
        let attrs: RoleAttributes = roles
            .find_role(rolname)?
            .ok_or_else(|| anyhow!("role \"{rolname}\" does not exist"))?;
        let members = RoleMemberStore::new(db.clone())?;
        let memberships = members.all_memberships(rolname)?;
        Ok(Actor {
            rolname: rolname.to_string(),
            superuser: attrs.superuser,
            unrestricted: false,
            memberships,
        })
    }

    /// Authorize `stmt` for this actor. Returns `InsufficientPrivilege`
    /// (SQLSTATE 42501 at the wire layer) on denial.
    pub fn check(
        &self,
        db: &Arc<Database>,
        stmt: &Stmt,
    ) -> std::result::Result<(), InsufficientPrivilege> {
        // Superuser bypass and zero-config bypass: no further checks.
        if self.unrestricted || self.superuser {
            return Ok(());
        }

        // System catalog protection: a non-superuser may never touch the
        // reserved `_tpt_*` catalogs directly (bypassing the RBAC DDL).
        if references_system_table(stmt) {
            return Err(InsufficientPrivilege(
                "permission denied: access to system catalog tables is restricted to superusers"
                    .to_string(),
            ));
        }

        match stmt {
            // These DDL statements are superuser-only.
            Stmt::CreateRole(_)
            | Stmt::AlterRole(_)
            | Stmt::DropRole(_)
            | Stmt::Grant(_)
            | Stmt::Revoke(_) => Err(InsufficientPrivilege(
                "permission denied: role and privilege administration requires superuser".to_string(),
            )),

            // DDL that creates objects needs CREATE on the database.
            Stmt::CreateTable(_)
            | Stmt::CreateIndex(_)
            | Stmt::CreateFunction(_)
            | Stmt::CreateSequence(_)
            | Stmt::CreateTopic(_)
            | Stmt::AlterTable(_)
            | Stmt::Analyze(_) => self.require(db, PrivilegeRepr::Create, &GrantObjectRepr::database()),

            // DDL that drops objects needs DROP on the database.
            Stmt::DropTable(_) => self.require(db, PrivilegeRepr::Drop, &GrantObjectRepr::database()),

            Stmt::Select(s) => {
                for table in select_tables(s) {
                    self.require(db, PrivilegeRepr::Select, &GrantObjectRepr::table(&table))?;
                }
                Ok(())
            }
            Stmt::Insert(i) => {
                self.require(db, PrivilegeRepr::Insert, &GrantObjectRepr::table(&i.table))
            }
            Stmt::Update(u) => {
                self.require(db, PrivilegeRepr::Update, &GrantObjectRepr::table(&u.table))
            }
            Stmt::Delete(d) => {
                self.require(db, PrivilegeRepr::Delete, &GrantObjectRepr::table(&d.table))
            }

            // Session / introspection / no-op statements are always allowed.
            Stmt::Begin
            | Stmt::Commit
            | Stmt::Rollback
            | Stmt::Set(_)
            | Stmt::Show(_)
            | Stmt::Notify(_, _)
            | Stmt::Listen(_)
            | Stmt::Unlisten(_)
            | Stmt::DeclareCursor(_)
            | Stmt::Fetch(_)
            | Stmt::MoveCursor(_)
            | Stmt::CloseCursor(_)
            | Stmt::CopyIn(_)
            | Stmt::CopyOut(_)
            | Stmt::Match(_) => Ok(()),
        }
    }

    fn require(
        &self,
        db: &Arc<Database>,
        privilege: PrivilegeRepr,
        object: &GrantObjectRepr,
    ) -> std::result::Result<(), InsufficientPrivilege> {
        let store = match PrivilegeStore::new(db.clone()) {
            Ok(s) => s,
            Err(_) => {
                return Err(InsufficientPrivilege(
                    "permission denied: could not load privilege catalog".to_string(),
                ))
            }
        };
        let ok = store
            .has_privilege(db, &self.rolname, privilege, object)
            .unwrap_or(false);
        if ok {
            Ok(())
        } else {
            Err(InsufficientPrivilege(format!(
                "permission denied: role \"{}\" lacks {} on {}",
                self.rolname,
                privilege.as_str(),
                object.label()
            )))
        }
    }
}

/// Collect the concrete table names a SELECT reads from (FROM + JOINs). CTEs
/// and derived tables are intentionally not walked — they cannot name a
/// `_tpt_*` catalog directly in the grammar, and a non-superuser reaching
/// system data through a CTE would already need a grant on the underlying
/// table, which is what we enforce at the leaf.
fn select_tables(s: &crate::sql::ast::SelectStmt) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(from) = &s.from {
        collect_from(&from, &mut out);
    }
    collect_select(s, &mut out);
    out
}

fn collect_select(s: &crate::sql::ast::SelectStmt, out: &mut Vec<String>) {
    if let Some(from) = &s.from {
        collect_from(from, out);
    }
    if let Some((_, rhs)) = &s.union {
        collect_select(rhs, out);
    }
    for cte in &s.ctes {
        collect_select(&cte.subquery, out);
    }
}

fn collect_from(twj: &TableWithJoins, out: &mut Vec<String>) {
    push_ref(&twj.primary, out);
    for j in &twj.joins {
        push_ref(&j.table, out);
    }
}

fn push_ref(r: &TableRef, out: &mut Vec<String>) {
    if r.subquery.is_none() && r.func_args.is_none() {
        let name = r.name.trim_start_matches("pg_catalog.");
        out.push(name.to_string());
    }
}

/// Whether `stmt` references a reserved `_tpt_*` system catalog table. Used to
/// block direct DML/DDL against the RBAC catalogs by non-superusers.
fn references_system_table(stmt: &Stmt) -> bool {
    let mut tables: Vec<String> = Vec::new();
    match stmt {
        Stmt::Select(s) => tables.extend(select_tables(s)),
        Stmt::Insert(i) => tables.push(i.table.clone()),
        Stmt::Update(u) => tables.push(u.table.clone()),
        Stmt::Delete(d) => tables.push(d.table.clone()),
        Stmt::CreateTable(c) => tables.push(c.table.clone()),
        Stmt::DropTable(d) => tables.push(d.table.clone()),
        Stmt::CreateIndex(c) => tables.push(c.table.clone()),
        _ => {}
    }
    tables.iter().any(|t| t.starts_with("_tpt_"))
}

/// `CREATE ROLE name [SUPERUSER|NOSUPERUSER] [LOGIN|NOLOGIN]
///   [PASSWORD '...'] [IN ROLE a, b, ...]`.
pub fn execute_create_role(db: &Arc<Database>, stmt: &CreateRoleStmt) -> Result<()> {
    let roles = RoleStore::new(db.clone())?;
    roles.create_role(
        &stmt.name,
        stmt.superuser,
        stmt.can_login,
        stmt.password.as_deref(),
        &stmt.in_role,
    )
}

/// `ALTER ROLE name [SUPERUSER|NOSUPERUSER] [LOGIN|NOLOGIN] [PASSWORD '...']`.
pub fn execute_alter_role(db: &Arc<Database>, stmt: &AlterRoleStmt) -> Result<()> {
    let roles = RoleStore::new(db.clone())?;
    roles.alter_role(
        &stmt.name,
        stmt.superuser,
        stmt.can_login,
        stmt.password.as_deref(),
    )
}

/// `DROP ROLE [IF EXISTS] name`.
pub fn execute_drop_role(db: &Arc<Database>, stmt: &DropRoleStmt) -> Result<()> {
    let roles = RoleStore::new(db.clone())?;
    roles.drop_role(&stmt.name, stmt.if_exists)
}

/// `GRANT ... TO ...` — membership and object-privilege variants.
pub fn execute_grant(db: &Arc<Database>, stmt: &GrantStmt) -> Result<()> {
    if stmt.is_role_grant {
        let members = RoleMemberStore::new(db.clone())?;
        for role in &stmt.roles {
            for grantee in &stmt.grantees {
                members.grant_membership(grantee, role)?;
            }
        }
        Ok(())
    } else {
        let store = PrivilegeStore::new(db.clone())?;
        let object = GrantObjectRepr::from_ast(&stmt.object);
        for p in &stmt.privileges {
            for grantee in &stmt.grantees {
                store.grant(grantee, PrivilegeRepr::from_ast(*p), &object)?;
            }
        }
        Ok(())
    }
}

/// `REVOKE ... FROM ...` — membership and object-privilege variants.
pub fn execute_revoke(db: &Arc<Database>, stmt: &RevokeStmt) -> Result<()> {
    if stmt.is_role_grant {
        let members = RoleMemberStore::new(db.clone())?;
        for role in &stmt.roles {
            for grantee in &stmt.grantees {
                members.revoke_membership(grantee, role)?;
            }
        }
        Ok(())
    } else {
        let store = PrivilegeStore::new(db.clone())?;
        let object = GrantObjectRepr::from_ast(&stmt.object);
        for p in &stmt.privileges {
            for grantee in &stmt.grantees {
                store.revoke(grantee, PrivilegeRepr::from_ast(*p), &object)?;
            }
        }
        Ok(())
    }
}
