//! End-to-end tests for Synapse (Phase 16): the two SQL-level ranked-recall
//! table functions, plus the phase milestone itself — "a 3-agent workflow
//! completes with shared state; semantic memory persists and is recalled
//! across sessions; tool discovery returns ranked results via Prism" —
//! exercised against an in-process `Database`, mirroring `prism_tests.rs`'s
//! role for Phase 7.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};
use crate::synapse::actor::{AgentRegistry, AgentStatus};
use crate::synapse::coordination::Coordinator;
use crate::synapse::memory::MemoryStore;
use crate::synapse::tools::ToolRegistry;

fn open_db(bucket: &std::path::Path, local: &std::path::Path, node_id: &str) -> Arc<Database> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        node_id.into(),
        Duration::from_secs(30),
    ));
    lease.try_acquire().unwrap();
    Arc::new(
        Database::open(
            local,
            store,
            lease.handle(),
            NodeRole::Writer,
            Default::default(),
        )
        .unwrap(),
    )
}

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

#[test]
fn synapse_recall_semantic_sql_function_ranks_nearest_first() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let db = open_db(bucket.path(), local.path(), "node-1");

    let mem = MemoryStore::new(db.clone()).unwrap();
    mem.remember_semantic("agent1", "the sky is blue", &[1.0, 0.0, 0.0])
        .unwrap();
    mem.remember_semantic("agent1", "grass is green", &[0.0, 1.0, 0.0])
        .unwrap();

    let result = execute_query(
        "SELECT content FROM synapse_recall_semantic('agent1', '[1.0,0.0,0.0]', 1)",
        db.clone(),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(cell_text(&result.rows[0][0]), "the sky is blue");
}

#[test]
fn synapse_discover_tools_sql_function_ranks_nearest_first() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let db = open_db(bucket.path(), local.path(), "node-1");

    let reg = ToolRegistry::new(db.clone()).unwrap();
    reg.register("web_search", "search the web", "{}", Some(&[1.0, 0.0]))
        .unwrap();
    reg.register("calculator", "does arithmetic", "{}", Some(&[0.0, 1.0]))
        .unwrap();

    let result = execute_query(
        "SELECT name FROM synapse_discover_tools('[0.9,0.1]', 1)",
        db.clone(),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(cell_text(&result.rows[0][0]), "web_search");
}

/// The Phase 16 milestone, verified end-to-end: three agents complete a
/// shared workflow via Flux-backed task delegation and last-write-wins
/// shared state; semantic memory and tool registrations survive a real
/// close-and-reopen of the `Database` at the same on-disk location
/// (simulating a process restart, not just a second in-memory handle);
/// and ranked tool discovery is verified through the SQL-level
/// `synapse_discover_tools` function.
#[tokio::test]
async fn milestone_three_agent_workflow_with_shared_state_and_cross_session_recall() {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();

    // --- "Session 1": one workflow, three agents, task delegation, shared
    // state, and a semantic memory + tool registration. Everything here is
    // dropped at the end of the block, closing the underlying WAL/index
    // file handles, before a fresh `Database` reopens the same directory.
    {
        let db = open_db(bucket.path(), local.path(), "node-1");
        let registry = AgentRegistry::new(db.clone()).unwrap();
        let coord = Coordinator::new(db.clone()).unwrap();
        let mem = MemoryStore::new(db.clone()).unwrap();
        let tools = ToolRegistry::new(db.clone()).unwrap();

        let agents = ["planner", "researcher", "writer"];
        for name in agents {
            registry
                .spawn(name, name, Box::new(|_state, msg| format!("done:{msg}")))
                .unwrap();
        }

        coord.delegate_task("report", "gather sources").unwrap();
        coord.delegate_task("report", "draft outline").unwrap();
        coord.delegate_task("report", "write summary").unwrap();

        for name in agents {
            let (offset, task) = coord
                .claim_task("report", "workers")
                .unwrap()
                .expect("a task should be available for each of the 3 agents");
            let reply = registry.get(name).unwrap().send(&task).await.unwrap();
            assert_eq!(reply, format!("done:{task}"));
            coord.complete_task("report", "workers", offset).unwrap();
            coord
                .set_shared_state("report", name, "done", name)
                .unwrap();
        }
        assert!(
            coord.claim_task("report", "workers").unwrap().is_none(),
            "all 3 tasks should be claimed"
        );

        let state = coord.list_shared_state("report").unwrap();
        assert_eq!(state.len(), 3);
        assert!(
            state.iter().all(|(_, v)| v == "done"),
            "workflow completes with shared state"
        );

        mem.remember_semantic("planner", "the report deadline is Friday", &[1.0, 0.0, 0.0])
            .unwrap();

        tools
            .register(
                "web_search",
                "search the web for sources",
                "{}",
                Some(&[0.0, 1.0, 0.0]),
            )
            .unwrap();
        tools
            .register(
                "calendar",
                "look up calendar deadlines",
                "{}",
                Some(&[0.9, 0.1, 0.0]),
            )
            .unwrap();

        for name in agents {
            registry
                .set_status(name, AgentStatus::Terminated)
                .await
                .unwrap();
        }
    }

    // --- "Session 2": a fresh `Database` opens the same bucket + local
    // directory, simulating the process restarting.
    let db2 = open_db(bucket.path(), local.path(), "node-1");

    let registry2 = AgentRegistry::new(db2.clone()).unwrap();
    let persisted_agents = registry2.list_all().unwrap();
    assert_eq!(persisted_agents.len(), 3);
    assert!(persisted_agents
        .iter()
        .all(|(_, _, status, _)| status == "terminated"));

    let coord2 = Coordinator::new(db2.clone()).unwrap();
    assert_eq!(
        coord2.list_shared_state("report").unwrap().len(),
        3,
        "shared state survives a restart"
    );

    let mem2 = MemoryStore::new(db2.clone()).unwrap();
    let recalled = mem2
        .recall_semantic("planner", &[1.0, 0.0, 0.0], 1)
        .unwrap();
    assert_eq!(recalled.len(), 1);
    assert_eq!(
        recalled[0].0.content, "the report deadline is Friday",
        "semantic memory recalled across sessions"
    );

    // Ranked tool discovery via the SQL-level table function.
    let result = execute_query(
        "SELECT name FROM synapse_discover_tools('[1.0,0.0,0.0]', 1)",
        db2.clone(),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        cell_text(&result.rows[0][0]),
        "calendar",
        "tool discovery returns ranked results via Prism"
    );
}
