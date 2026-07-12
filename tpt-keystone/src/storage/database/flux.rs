use anyhow::Result;
use serde_json::Value as JsonValue;
use tracing::info;

use super::Database;
use crate::storage::flux::{now_ms, FluxLog, FluxRecord, RetentionPolicy};

impl Database {
    /// Create a named Flux topic (`CREATE TOPIC`), failing if one with this
    /// name already exists — callers implementing `IF NOT EXISTS` check
    /// `list_topics` first (mirrors `create_sequence`).
    pub fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        retention_ms: Option<i64>,
        retention_bytes: Option<u64>,
    ) -> Result<()> {
        self.check_writable()?;
        let mut topics = self.topics.lock().unwrap();
        if topics.contains_key(name) {
            anyhow::bail!("topic \"{name}\" already exists");
        }
        let dir = self.flux_dir.join(name);
        let log = FluxLog::create(
            &dir,
            partitions,
            RetentionPolicy {
                retention_ms,
                retention_bytes,
            },
        )?;
        topics.insert(name.to_string(), log);
        info!(topic = name, partitions, "topic created");
        Ok(())
    }

    /// Creates `__cdc_<table>` (1 partition, unlimited retention) the first
    /// time a mutation happens on `table`, otherwise a no-op. Unlike
    /// `create_topic`, this never errors on "already exists" — it's called
    /// unconditionally on every insert/update/delete.
    fn ensure_cdc_topic(&self, table: &str) -> Result<String> {
        let topic = format!("__cdc_{table}");
        let mut topics = self.topics.lock().unwrap();
        if !topics.contains_key(&topic) {
            let dir = self.flux_dir.join(&topic);
            let log = FluxLog::create(&dir, 1, RetentionPolicy::default())?;
            topics.insert(topic.clone(), log);
        }
        Ok(topic)
    }

    /// Publish one record to `topic`. See `storage::flux` module docs for
    /// partition assignment when `partition` is `None`.
    pub fn flux_publish(
        &self,
        topic: &str,
        partition: Option<u32>,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<(u32, u64)> {
        self.check_writable()?;
        let mut topics = self.topics.lock().unwrap();
        let log = topics
            .get_mut(topic)
            .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
        let (p, offset) = log.publish(partition, key.clone(), value.clone())?;
        let _ = self.flux_bus.send((
            topic.to_string(),
            FluxRecord {
                offset,
                key,
                value,
                timestamp_ms: now_ms(),
            },
        ));
        Ok((p, offset))
    }

    /// Publish a native CDC event (see `executor::execute_insert`/`update`/
    /// `delete`) to `table`'s implicit `__cdc_<table>` topic, auto-creating
    /// it on first use.
    pub fn flux_publish_cdc(&self, table: &str, event: JsonValue) -> Result<()> {
        let topic = self.ensure_cdc_topic(table)?;
        let bytes = serde_json::to_vec(&event)?;
        self.flux_publish(&topic, None, None, bytes)?;
        Ok(())
    }

    /// Records at/after `group`'s tracked offset on `topic`'s `partition`,
    /// without advancing it.
    pub fn flux_poll(
        &self,
        topic: &str,
        partition: u32,
        group: &str,
        max: usize,
    ) -> Result<Vec<FluxRecord>> {
        let topics = self.topics.lock().unwrap();
        let log = topics
            .get(topic)
            .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
        log.poll(group, partition, max)
    }

    /// Advances `group`'s tracked offset on `topic`'s `partition`.
    pub fn flux_commit(&self, topic: &str, partition: u32, group: &str, offset: u64) -> Result<()> {
        let mut topics = self.topics.lock().unwrap();
        let log = topics
            .get_mut(topic)
            .ok_or_else(|| anyhow::anyhow!("topic \"{topic}\" does not exist"))?;
        log.commit(group, partition, offset)
    }

    /// Every record currently retained on `topic`'s `partition`, bypassing
    /// consumer-group tracking — used by the time-travel/windowing table
    /// functions, which replay the whole log rather than one consumer's
    /// unread tail.
    pub fn flux_all(&self, topic: &str, partition: u32) -> Option<Vec<FluxRecord>> {
        let topics = self.topics.lock().unwrap();
        topics.get(topic)?.all_records(partition)
    }

    /// Number of partitions on `topic`, or `None` if it doesn't exist.
    pub fn flux_num_partitions(&self, topic: &str) -> Option<u32> {
        self.topics
            .lock()
            .unwrap()
            .get(topic)
            .map(|t| t.num_partitions())
    }

    /// `(name, partition_count)` for every topic — `pg_catalog`-style
    /// introspection, mirrors `list_sequences`.
    pub fn list_topics(&self) -> Vec<(String, u32)> {
        self.topics
            .lock()
            .unwrap()
            .iter()
            .map(|(name, log)| (name.clone(), log.num_partitions()))
            .collect()
    }

    /// Subscribe to every Flux publish across every topic; the WebSocket
    /// endpoint filters by topic name itself (mirrors
    /// `subscribe_notifications`).
    pub fn subscribe_flux(&self) -> tokio::sync::broadcast::Receiver<(String, FluxRecord)> {
        self.flux_bus.subscribe()
    }
}
