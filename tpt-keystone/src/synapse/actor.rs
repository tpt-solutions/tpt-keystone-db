//! Actor model runtime: one Tokio task per agent, `tokio::sync::mpsc`
//! mailboxes, coordinated through a shared `AgentRegistry`. This is the one
//! piece of Synapse that's genuinely new engine code (see `synapse` module
//! docs) — everything else composes existing Keystone/Chronos/Prism/Flux
//! primitives.
//!
//! What an agent actually *does* in response to a message is caller-supplied
//! (`StepFn`) — this module is infrastructure (mailbox, lifecycle,
//! checkpoint persistence), not an LLM agent framework. That mirrors the
//! boundary Flux already draws around a consumer's business logic: the
//! engine owns the queue/lifecycle machinery, not the payload semantics.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, oneshot};

use crate::storage::database::Database;
use crate::storage::{ColumnType, StorageEngine};
use crate::synapse::{
    cell_i64, cell_text, col, decode_cell, encode_cells, int_cell, now_ms, text_cell,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStatus {
    Running,
    Paused,
    Terminated,
}

impl AgentStatus {
    fn as_str(self) -> &'static str {
        match self {
            AgentStatus::Running => "running",
            AgentStatus::Paused => "paused",
            AgentStatus::Terminated => "terminated",
        }
    }
}

/// Mutable state an agent's `StepFn` operates on. `short_term` is the
/// "Keystone in-session" memory tier from the TODO.md checklist — plain
/// process memory here for zero-latency read/write during a message burst;
/// `AgentRegistry::checkpoint` is what actually persists to Keystone (a
/// `StepFn` that wants durable short-term recall should fold relevant
/// entries into the checkpoint string it returns).
pub struct AgentState {
    pub short_term: HashMap<String, String>,
}

/// Per-message behavior. Returns the reply payload sent back through
/// `AgentHandle::send`'s oneshot channel.
pub type StepFn = Box<dyn FnMut(&mut AgentState, &str) -> String + Send>;

enum AgentMessage {
    Deliver {
        payload: String,
        reply: oneshot::Sender<String>,
    },
    Pause,
    Resume,
    Checkpoint(oneshot::Sender<String>),
    Terminate,
}

async fn actor_loop(mut state: AgentState, mut step: StepFn, mut rx: mpsc::Receiver<AgentMessage>) {
    let mut status = AgentStatus::Running;
    let mut last_checkpoint = String::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            AgentMessage::Deliver { payload, reply } => {
                let out = if status == AgentStatus::Running {
                    step(&mut state, &payload)
                } else {
                    String::new()
                };
                let _ = reply.send(out);
            }
            AgentMessage::Pause => status = AgentStatus::Paused,
            AgentMessage::Resume => status = AgentStatus::Running,
            AgentMessage::Checkpoint(reply) => {
                // The checkpoint text is whatever the last `step` call left
                // in `short_term["__checkpoint"]`, or empty if the `StepFn`
                // never set one — a deliberately simple convention rather
                // than a second callback hook.
                last_checkpoint = state
                    .short_term
                    .get("__checkpoint")
                    .cloned()
                    .unwrap_or(last_checkpoint);
                let _ = reply.send(last_checkpoint.clone());
            }
            AgentMessage::Terminate => break,
        }
    }
}

/// A live handle to a spawned agent. Cloning shares the same mailbox (cheap
/// `mpsc::Sender` clone), same as any Tokio actor handle.
#[derive(Clone)]
pub struct AgentHandle {
    pub id: String,
    tx: mpsc::Sender<AgentMessage>,
}

impl AgentHandle {
    /// Delivers `payload` to the agent and awaits its reply. If the agent is
    /// paused or terminated the reply is an empty string (no error — a
    /// paused agent legitimately drops work rather than failing the sender).
    pub async fn send(&self, payload: &str) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(AgentMessage::Deliver {
                payload: payload.to_string(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow!("agent \"{}\" mailbox closed", self.id))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("agent \"{}\" dropped without replying", self.id))
    }

    pub async fn pause(&self) -> Result<()> {
        self.tx
            .send(AgentMessage::Pause)
            .await
            .map_err(|_| anyhow!("agent \"{}\" mailbox closed", self.id))
    }

    pub async fn resume(&self) -> Result<()> {
        self.tx
            .send(AgentMessage::Resume)
            .await
            .map_err(|_| anyhow!("agent \"{}\" mailbox closed", self.id))
    }

    pub async fn checkpoint(&self) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(AgentMessage::Checkpoint(reply_tx))
            .await
            .map_err(|_| anyhow!("agent \"{}\" mailbox closed", self.id))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("agent \"{}\" dropped without replying", self.id))
    }

    pub async fn terminate(&self) -> Result<()> {
        self.tx
            .send(AgentMessage::Terminate)
            .await
            .map_err(|_| anyhow!("agent \"{}\" mailbox closed", self.id))
    }
}

/// Spawns/tracks agents and persists their lifecycle/checkpoint state to
/// `_synapse_agents` — Keystone-durable, so a checkpoint survives a process
/// restart even though the live Tokio task doesn't (see module docs: no
/// auto-respawn-on-boot).
pub struct AgentRegistry {
    db: Arc<Database>,
    handles: Mutex<HashMap<String, AgentHandle>>,
}

const AGENTS_TABLE: &str = "_synapse_agents";

impl AgentRegistry {
    pub fn new(db: Arc<Database>) -> Result<Self> {
        Self::ensure_schema(&db)?;
        Ok(Self {
            db,
            handles: Mutex::new(HashMap::new()),
        })
    }

    fn ensure_schema(db: &Arc<Database>) -> Result<()> {
        if db.get_table(AGENTS_TABLE)?.is_some() {
            return Ok(());
        }
        db.create_table_with_constraints(
            AGENTS_TABLE,
            &[
                col("id", ColumnType::Text, false, true),
                col("name", ColumnType::Text, true, false),
                col("status", ColumnType::Text, false, false),
                col("checkpoint", ColumnType::Text, true, false),
                col("created_at", ColumnType::Int8, false, false),
                col("updated_at", ColumnType::Int8, false, false),
            ],
            vec![],
            vec![],
        )
    }

    fn persist(
        &self,
        id: &str,
        name: &str,
        status: AgentStatus,
        checkpoint: &str,
        created_at: i64,
    ) -> Result<()> {
        let cells = vec![
            text_cell(id),
            text_cell(name),
            text_cell(status.as_str()),
            text_cell(checkpoint),
            int_cell(created_at),
            int_cell(now_ms()),
        ];
        self.db
            .write(AGENTS_TABLE, id.as_bytes(), &encode_cells(&cells))
    }

    /// Spawns a fresh agent with empty short-term state, registers it, and
    /// persists an initial `_synapse_agents` row.
    pub fn spawn(&self, id: impl Into<String>, name: &str, step: StepFn) -> Result<AgentHandle> {
        let id = id.into();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(actor_loop(
            AgentState {
                short_term: HashMap::new(),
            },
            step,
            rx,
        ));
        let handle = AgentHandle { id: id.clone(), tx };
        self.persist(&id, name, AgentStatus::Running, "", now_ms())?;
        self.handles.lock().unwrap().insert(id, handle.clone());
        Ok(handle)
    }

    /// Spawns a fresh agent whose `AgentState::short_term["__checkpoint"]`
    /// is pre-loaded from `id`'s last persisted checkpoint (empty if the
    /// agent never checkpointed, or never existed). This is the "persistent
    /// session state across restarts" path: the checkpoint text survives in
    /// Keystone; the caller decides how their `StepFn` interprets it to
    /// rebuild application state.
    pub fn resume_from_checkpoint(
        &self,
        id: &str,
        name: &str,
        step: StepFn,
    ) -> Result<AgentHandle> {
        let checkpoint = self
            .db
            .read(AGENTS_TABLE, id.as_bytes())?
            .and_then(|v| decode_cell(&v, 3))
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        let mut initial = HashMap::new();
        if !checkpoint.is_empty() {
            initial.insert("__checkpoint".to_string(), checkpoint.clone());
        }
        tokio::spawn(actor_loop(
            AgentState {
                short_term: initial,
            },
            step,
            rx,
        ));
        let handle = AgentHandle {
            id: id.to_string(),
            tx,
        };
        self.persist(id, name, AgentStatus::Running, &checkpoint, now_ms())?;
        self.handles
            .lock()
            .unwrap()
            .insert(id.to_string(), handle.clone());
        Ok(handle)
    }

    pub fn get(&self, id: &str) -> Option<AgentHandle> {
        self.handles.lock().unwrap().get(id).cloned()
    }

    pub fn list_live(&self) -> Vec<String> {
        self.handles.lock().unwrap().keys().cloned().collect()
    }

    /// Sends `Pause`/`Resume`/`Terminate` to the live actor (if any) and
    /// updates its `_synapse_agents` row's `status` column — a caller that
    /// only wants the persisted status updated (e.g. after the process
    /// restarted and the live handle is gone) can call this even when
    /// `get(id)` returns `None`.
    pub async fn set_status(&self, id: &str, status: AgentStatus) -> Result<()> {
        if let Some(handle) = self.get(id) {
            match status {
                AgentStatus::Paused => handle.pause().await?,
                AgentStatus::Running => handle.resume().await?,
                AgentStatus::Terminated => {
                    handle.terminate().await?;
                    self.handles.lock().unwrap().remove(id);
                }
            }
        }
        let row = self
            .db
            .read(AGENTS_TABLE, id.as_bytes())?
            .ok_or_else(|| anyhow!("agent \"{id}\" not found"))?;
        let name = cell_text(&decode_cell(&row, 1)).unwrap_or_default();
        let checkpoint = cell_text(&decode_cell(&row, 3)).unwrap_or_default();
        let created_at = cell_i64(&decode_cell(&row, 4)).unwrap_or_else(now_ms);
        self.persist(id, &name, status, &checkpoint, created_at)
    }

    /// Checkpoints a live agent and persists the result; a no-op returning
    /// the last-persisted checkpoint if the agent isn't currently live.
    pub async fn checkpoint(&self, id: &str) -> Result<String> {
        let checkpoint = match self.get(id) {
            Some(handle) => handle.checkpoint().await?,
            None => {
                let row = self
                    .db
                    .read(AGENTS_TABLE, id.as_bytes())?
                    .ok_or_else(|| anyhow!("agent \"{id}\" not found"))?;
                return Ok(cell_text(&decode_cell(&row, 3)).unwrap_or_default());
            }
        };
        let row = self
            .db
            .read(AGENTS_TABLE, id.as_bytes())?
            .ok_or_else(|| anyhow!("agent \"{id}\" not found"))?;
        let name = cell_text(&decode_cell(&row, 1)).unwrap_or_default();
        let created_at = cell_i64(&decode_cell(&row, 4)).unwrap_or_else(now_ms);
        self.persist(id, &name, AgentStatus::Running, &checkpoint, created_at)?;
        Ok(checkpoint)
    }

    /// `(id, name, status, checkpoint)` for every agent ever persisted, live
    /// or not — `pg_catalog`-style introspection, mirrors `list_topics`.
    pub fn list_all(&self) -> Result<Vec<(String, String, String, String)>> {
        let rows = self.db.scan(AGENTS_TABLE)?;
        Ok(rows
            .into_iter()
            .map(|kv| {
                let id = cell_text(&decode_cell(&kv.value, 0)).unwrap_or_default();
                let name = cell_text(&decode_cell(&kv.value, 1)).unwrap_or_default();
                let status = cell_text(&decode_cell(&kv.value, 2)).unwrap_or_default();
                let checkpoint = cell_text(&decode_cell(&kv.value, 3)).unwrap_or_default();
                (id, name, status, checkpoint)
            })
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

    #[tokio::test]
    async fn spawn_send_and_receive_reply() {
        let (db, _b, _l) = test_db();
        let registry = AgentRegistry::new(db).unwrap();
        let handle = registry
            .spawn(
                "a1",
                "echo-agent",
                Box::new(|_state, msg| format!("echo:{msg}")),
            )
            .unwrap();
        let reply = handle.send("hello").await.unwrap();
        assert_eq!(reply, "echo:hello");
    }

    #[tokio::test]
    async fn short_term_state_persists_across_messages_within_a_session() {
        let (db, _b, _l) = test_db();
        let registry = AgentRegistry::new(db).unwrap();
        let handle = registry
            .spawn(
                "a1",
                "counter",
                Box::new(|state, _msg| {
                    let n: u64 = state
                        .short_term
                        .get("count")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0)
                        + 1;
                    state.short_term.insert("count".to_string(), n.to_string());
                    n.to_string()
                }),
            )
            .unwrap();
        assert_eq!(handle.send("tick").await.unwrap(), "1");
        assert_eq!(handle.send("tick").await.unwrap(), "2");
        assert_eq!(handle.send("tick").await.unwrap(), "3");
    }

    #[tokio::test]
    async fn pause_drops_messages_and_resume_processes_again() {
        let (db, _b, _l) = test_db();
        let registry = AgentRegistry::new(db).unwrap();
        let handle = registry
            .spawn("a1", "echo", Box::new(|_s, msg| msg.to_string()))
            .unwrap();
        handle.pause().await.unwrap();
        assert_eq!(handle.send("x").await.unwrap(), "");
        handle.resume().await.unwrap();
        assert_eq!(handle.send("x").await.unwrap(), "x");
    }

    #[tokio::test]
    async fn checkpoint_persists_and_resume_from_checkpoint_reloads_it() {
        let (db, _b, _l) = test_db();
        let registry = AgentRegistry::new(db).unwrap();
        let handle = registry
            .spawn(
                "a1",
                "saver",
                Box::new(|state, msg| {
                    state
                        .short_term
                        .insert("__checkpoint".to_string(), format!("saw:{msg}"));
                    "ok".to_string()
                }),
            )
            .unwrap();
        handle.send("hello").await.unwrap();
        let cp = registry.checkpoint("a1").await.unwrap();
        assert_eq!(cp, "saw:hello");

        // Simulate a restart: registry state (live handles) forgotten, but
        // the row in `_synapse_agents` survives (same in-process Database).
        let handle2 = registry
            .resume_from_checkpoint(
                "a1",
                "saver",
                Box::new(|state, _msg| {
                    state
                        .short_term
                        .get("__checkpoint")
                        .cloned()
                        .unwrap_or_default()
                }),
            )
            .unwrap();
        assert_eq!(handle2.send("ignored").await.unwrap(), "saw:hello");
    }

    #[tokio::test]
    async fn terminate_removes_from_live_registry() {
        let (db, _b, _l) = test_db();
        let registry = AgentRegistry::new(db).unwrap();
        registry
            .spawn("a1", "x", Box::new(|_s, m| m.to_string()))
            .unwrap();
        assert!(registry.get("a1").is_some());
        registry
            .set_status("a1", AgentStatus::Terminated)
            .await
            .unwrap();
        assert!(registry.get("a1").is_none());
        let all = registry.list_all().unwrap();
        assert_eq!(
            all.iter().find(|(id, ..)| id == "a1").unwrap().2,
            "terminated"
        );
    }
}
