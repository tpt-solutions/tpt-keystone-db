//! Authorization enforcement tests for the RBAC layer (`executor::rbac`,
//! TODO.md Phase 20). Exercises per-statement allow/deny, superuser bypass,
//! membership-inherited privileges, system-catalog write protection, the
//! `_tpt_roles`-empty no-op regression, and the downcastable `42501` error.

use std::sync::Arc;

use super::execute_parsed;
use super::execute_parsed_as;
use super::execute_query;
use super::rbac::{Actor, InsufficientPrivilege};
use crate::sql::ast::Stmt;
use crate::sql::parse;

/// Parse `sql` into a `Stmt`, panicking on a syntax error (tests only).
fn p(sql: &str) -> Stmt {
    parse(sql).unwrap()
}
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::wire::privileges::{GrantObjectRepr, PrivilegeRepr, PrivilegeStore};
use crate::wire::roles::RoleStore;

fn open_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "node-1".into(),
        std::time::Duration::from_secs(30),
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
fn empty_roles_catalog_is_unrestricted() {
    let (db, _b, _l) = open_db();
    let actor = Actor::unrestricted();
    // No roles configured: every statement is allowed (zero-config path).
    assert!(actor
        .check(&db, &p("SELECT 1"))
        .is_ok());
    assert!(actor
        .check(&db, &p("CREATE ROLE x"))
        .is_ok());
    assert!(actor
        .check(&db, &p("SELECT * FROM _tpt_roles"))
        .is_ok());
}

#[test]
fn superuser_bypasses_all_checks() {
    let (db, _b, _l) = open_db();
    RoleStore::new(db.clone())
        .unwrap()
        .create_role("admin", true, true, Some("pw"), &[])
        .unwrap();
    let admin = Actor::for_role(&db, "admin").unwrap();
    assert!(admin.superuser);
    assert!(admin.check(&db, &p("CREATE ROLE x")).is_ok());
    assert!(admin.check(&db, &p("DROP TABLE t")).is_ok());
    assert!(admin.check(&db, &p("SELECT * FROM _tpt_roles")).is_ok());
}

#[test]
fn non_superuser_denied_role_admin_ddl() {
    let (db, _b, _l) = open_db();
    let roles = RoleStore::new(db.clone()).unwrap();
    roles.create_role("admin", true, true, Some("pw"), &[]).unwrap();
    roles.create_role("bob", false, false, None, &[]).unwrap();
    let bob = Actor::for_role(&db, "bob").unwrap();

    assert!(bob.check(&db, &p("CREATE ROLE x")).is_err());
    assert!(bob.check(&db, &p("GRANT SELECT ON t TO bob")).is_err());
    assert!(bob.check(&db, &p("ALTER ROLE bob SUPERUSER")).is_err());
}

#[test]
fn privilege_grant_allows_then_deny_without() {
    let (db, _b, _l) = open_db();
    execute_parsed(p("CREATE TABLE t (id INT8 PRIMARY KEY, v INT8)"), db.clone(), &[], None)
        .unwrap();
    let roles = RoleStore::new(db.clone()).unwrap();
    roles.create_role("admin", true, true, Some("pw"), &[]).unwrap();
    roles.create_role("reader", false, false, None, &[]).unwrap();
    let reader = Actor::for_role(&db, "reader").unwrap();

    // No grant yet: SELECT is denied.
    let denied = execute_parsed_as(p("SELECT * FROM t"), db.clone(), &[], &reader, None);
    assert!(denied.is_err());
    assert!(denied
        .unwrap_err()
        .downcast_ref::<InsufficientPrivilege>()
        .is_some());

    // Grant SELECT on t, then it succeeds.
    PrivilegeStore::new(db.clone())
        .unwrap()
        .grant("reader", PrivilegeRepr::Select, &GrantObjectRepr::table("t"));
    assert!(execute_parsed_as(p("SELECT * FROM t"), db.clone(), &[], &reader, None).is_ok());

    // But INSERT is still denied.
    assert!(execute_parsed_as(p("INSERT INTO t VALUES (1, 2)"), db.clone(), &[], &reader, None).is_err());
}

#[test]
fn membership_inherits_table_privilege() {
    let (db, _b, _l) = open_db();
    execute_parsed(p("CREATE TABLE t (id INT8 PRIMARY KEY, v INT8)"), db.clone(), &[], None)
        .unwrap();
    let roles = RoleStore::new(db.clone()).unwrap();
    roles.create_role("admin", true, true, Some("pw"), &[]).unwrap();
    roles.create_role("readers", false, false, None, &[]).unwrap();
    roles.create_role("alice", false, false, None, &[]).unwrap();
    roles.create_role("readers2", false, false, None, &[]).unwrap();

    PrivilegeStore::new(db.clone())
        .unwrap()
        .grant("readers", PrivilegeRepr::Select, &GrantObjectRepr::table("t"));
    // alice is a member of readers, which is a member of readers2 (chain).
    crate::wire::role_members::RoleMemberStore::new(db.clone())
        .unwrap()
        .grant_membership("alice", "readers")
        .unwrap();

    let alice = Actor::for_role(&db, "alice").unwrap();
    assert!(alice.memberships.contains(&"readers".to_string()));
    assert!(execute_parsed_as(p("SELECT * FROM t"), db.clone(), &[], &alice, None).is_ok());

    // bob is not a member: denied.
    let roles2 = RoleStore::new(db.clone()).unwrap();
    roles2.create_role("bob", false, false, None, &[]).unwrap();
    let bob = Actor::for_role(&db, "bob").unwrap();
    assert!(execute_parsed_as(p("SELECT * FROM t"), db.clone(), &[], &bob, None).is_err());
}

#[test]
fn non_superuser_cannot_touch_system_catalog() {
    let (db, _b, _l) = open_db();
    let roles = RoleStore::new(db.clone()).unwrap();
    roles.create_role("admin", true, true, Some("pw"), &[]).unwrap();
    roles.create_role("reader", false, false, None, &[]).unwrap();

    // Grant SELECT on the catalog table directly — still must be denied for a
    // non-superuser, since system tables are superuser-only.
    PrivilegeStore::new(db.clone())
        .unwrap()
        .grant("reader", PrivilegeRepr::Select, &GrantObjectRepr::table("_tpt_roles"));
    let reader = Actor::for_role(&db, "reader").unwrap();
    assert!(reader
        .check(&db, &p("SELECT * FROM _tpt_roles"))
        .is_err());
    assert!(reader.check(&db, &p("DROP TABLE _tpt_roles")).is_err());
}

#[test]
fn database_level_create_required_for_ddl() {
    let (db, _b, _l) = open_db();
    let roles = RoleStore::new(db.clone()).unwrap();
    roles.create_role("admin", true, true, Some("pw"), &[]).unwrap();
    roles.create_role("dev", false, false, None, &[]).unwrap();
    let dev = Actor::for_role(&db, "dev").unwrap();

    // No CREATE on DATABASE: DDL denied.
    assert!(dev.check(&db, &p("CREATE TABLE t (id INT8 PRIMARY KEY)")).is_err());

    PrivilegeStore::new(db.clone())
        .unwrap()
        .grant("dev", PrivilegeRepr::Create, &GrantObjectRepr::database());
    assert!(dev.check(&db, &p("CREATE TABLE t (id INT8 PRIMARY KEY)")).is_ok());
    // DROP still denied (needs DROP privilege).
    assert!(dev.check(&db, &p("DROP TABLE t")).is_err());
}

