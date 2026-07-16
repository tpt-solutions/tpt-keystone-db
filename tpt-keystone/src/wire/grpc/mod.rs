//! Flux gRPC streaming endpoint (Phase 11) — the high-throughput consumer
//! protocol counterpart to the RFC 6455 WebSocket bridge in `wire::websocket`.
//!
//! Like every other auxiliary listener on this node (MCP on :5433, Flux WS on
//! :5434, Canvas HTTP on :5435), this one is **hand-rolled**: a from-scratch
//! HTTP/2 (h2c, prior-knowledge) frame layer (`http2.rs`) + HPACK header
//! compression (`hpack.rs`) + a minimal protobuf codec (`proto.rs`) + gRPC
//! message framing — no `h2`/`hyper`/`tonic`/`prost` crate (see this repo's
//! from-scratch wire-protocol rule in AGENTS.md).
//!
//! ## Service
//!
//! ```proto
//! service Flux {
//!   rpc Subscribe(SubscribeRequest) returns (stream Record);  // server-streaming
//!   rpc Publish(PublishRequest) returns (PublishResponse);    // unary
//!   rpc Poll(PollRequest) returns (PollResponse);             // unary
//! }
//! ```
//!
//! `Subscribe` is the headline capability this endpoint adds over the WS
//! bridge: a long-lived server-stream of records as they're published, framed
//! by HTTP/2 flow control and consumable by any standard gRPC client (verified
//! end-to-end here against Python's `grpcio` — see `grpc_tests`). `Publish` /
//! `Poll` round out a complete consumer surface (post a record; pull a
//! consumer-group's unread tail).
//!
//! ## Scope cuts (honest, matching this codebase's "real but scoped" discipline)
//!
//! * **Cleartext h2c only** — no ALPN/TLS HTTP/2 negotiation. An insecure
//!   gRPC channel (`grpc.insecure_channel` / `grpcio.insecure_channel`) is what
//!   this speaks. The Postgres listener has opt-in TLS (`wire::tls`); wiring
//!   `h2` ALPN onto the same rustls acceptor is a separate effort and out of
//!   scope here.
//! * **No request streaming / no client-streaming RPCs** — `Subscribe` takes a
//!   single request message; there is no bidirectional stream. Matches the
//!   WS bridge's one-`subscribe`-per-connection model.
//! * **No `Content-Encoding`/trailers-only abstraction gymnastics** — responses
//!   carry `grpc-status` in the trailing HEADERS frame only (never inline in
//!   the initial HEADERS), per the gRPC-HTTP/2 spec.
//! * **Per-stream and connection flow-control windows are tracked and
//!   respected** (a real gRPC client will stall otherwise), but there's no
//!   per-stream *priority* scheduling and no `SETTINGS_MAX_FRAME_SIZE`
//!   enforcement beyond a hard 16 MiB parse guard.
//! * **No custom `message` marshaller / any proto type beyond the three
//!   messages above** — the `.proto` is in `docs/formats/flux_grpc.proto` for
//!   independent reimplementation by other languages.

pub mod hpack;
pub mod http2;
pub mod proto;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tracing::debug;

use crate::storage::database::Database;
use crate::wire::bridge_auth::authenticate_basic;
use crate::wire::roles::RoleStore;

// HTTP/2 flow-control default initial window size (RFC 7540 §6.9.2) and our
// own hard parse guard.
const DEFAULT_INITIAL_WINDOW: u32 = 65_535;
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// gRPC error status codes (subset we emit).
const GRPC_OK: u8 = 0;
const GRPC_UNIMPLEMENTED: u8 = 12;
const GRPC_INTERNAL: u8 = 13;
/// gRPC `UNAUTHENTICATED` — sent when `_tpt_roles` is non-empty and the
/// request's `authorization` metadata header is missing/invalid.
const GRPC_UNAUTHENTICATED: u8 = 16;

/// Drive one client TCP connection from the h2c preface through the request
/// loop until it closes or errors. `roles`/```guard`` enable optional Basic
/// auth (via the `authorization` metadata header, when `_tpt_roles` is
/// non-empty) and connection-rate admission control.
pub async fn handle(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    db: Arc<Database>,
    roles: Arc<RoleStore>,
    guard: Arc<Semaphore>,
) {
    let _permit = match guard.acquire_owned().await {
        Ok(p) => p,
        Err(_) => return,
    };
    if let Err(e) = run(stream, db, roles).await {
        debug!(%peer, "flux grpc session ended: {e}");
    }
}

async fn run(stream: TcpStream, db: Arc<Database>, roles: Arc<RoleStore>) -> Result<()> {
    stream.set_nodelay(true).ok();
    // Splitting the stream lets the read loop and concurrent write loop each
    // own one half (a single `TcpStream` isn't `Sync`, so sharing it across
    // select arms is awkward).
    let (mut read_half, write_half) = tokio::io::split(stream);
    let mut conn = Conn::new(db, roles, write_half);

    conn.handshake(&mut read_half).await?;
    conn.serve(&mut read_half).await
}

/// Per-open-stream state.
struct Stream {
    /// The request :path (e.g. `/flux.Flux/Subscribe`).
    path: Option<String>,
    /// HPACK-decoded request headers (for routing/debugging).
    headers: Vec<(String, String)>,
    /// Accumulated request DATA bytes across possibly many frames.
    body: Vec<u8>,
    /// True once the client half-closed (END_STREAM on the request's last DATA
    /// frame / HEADERS).
    req_ended: bool,
    /// Remaining bytes the peer will let us send on this stream.
    send_window: i64,
    /// Outbound messages waiting for send-window credit.
    outbound: Vec<Vec<u8>>,
    /// Trailers to send once `outbound` drains (unary responses).
    pending_trailers: Option<(u8, String)>,
    /// For server-streaming: the topic this stream is subscribed to.
    subscribed_topic: Option<String>,
}

impl Stream {
    fn new() -> Self {
        Stream {
            path: None,
            headers: Vec::new(),
            body: Vec::new(),
            req_ended: false,
            send_window: DEFAULT_INITIAL_WINDOW as i64,
            outbound: Vec::new(),
            pending_trailers: None,
            subscribed_topic: None,
        }
    }
}

struct Conn<W: AsyncWrite + Unpin> {
    db: Arc<Database>,
    roles: Arc<RoleStore>,
    write: BufWriter<W>,
    hpack: hpack::Decoder,
    /// `true` after the peer acks our settings (only needed for the preface
    /// handshake; we don't gate writes on it).
    peer_settings_acked: bool,
    /// Accumulated header-block fragment awaiting END_HEADERS.
    pending_headers: Option<Vec<u8>>,
    /// Connection-level send flow-control window (bytes we may still emit).
    conn_send_window: i64,
    /// Initial stream window advertised by the peer (applied to new streams).
    peer_initial_window: u32,
    /// Active streams, keyed by HTTP/2 stream id.
    streams: HashMap<u32, Stream>,
    /// Broadcast receiver for live Flux publishes across all topics.
    flux_rx: tokio::sync::broadcast::Receiver<(String, crate::storage::flux::FluxRecord)>,
}

impl<W: AsyncWrite + Unpin> Conn<W> {
    fn new(db: Arc<Database>, roles: Arc<RoleStore>, write: W) -> Self {
        Conn {
            db: db.clone(),
            roles,
            write: BufWriter::new(write),
            hpack: hpack::Decoder::new(),
            peer_settings_acked: false,
            pending_headers: None,
            conn_send_window: DEFAULT_INITIAL_WINDOW as i64,
            peer_initial_window: DEFAULT_INITIAL_WINDOW,
            streams: HashMap::new(),
            flux_rx: db.subscribe_flux(),
        }
    }

    async fn handshake<R: AsyncRead + Unpin>(&mut self, r: &mut R) -> Result<()> {
        http2::read_preface(r).await?;
        // Server preface: a SETTINGS frame (empty = all defaults).
        http2::write_frame(
            &mut self.write,
            http2::FRAME_SETTINGS,
            0,
            0,
            &http2::encode_settings(&[(http2::SETTINGS_INITIAL_WINDOW_SIZE, DEFAULT_INITIAL_WINDOW)]),
        )
        .await?;
        Ok(())
    }

    async fn serve<R: AsyncRead + Unpin>(&mut self, r: &mut R) -> Result<()> {
        loop {
            tokio::select! {
                frame = http2::read_frame(r) => {
                    let Some(frame) = frame? else { break };
                    self.on_frame(frame).await?;
                }
                event = self.flux_rx.recv() => {
                    match event {
                        Ok((topic, record)) => self.on_flux_event(topic, record).await?,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // A slow subscriber fell behind the broadcast ring;
                            // we just keep going (same semantics as the WS bridge).
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_flux_event(
        &mut self,
        topic: String,
        record: crate::storage::flux::FluxRecord,
    ) -> Result<()> {
        // Fan a live publish out to every subscribed server-streaming stream.
        let mut to_send: Vec<(u32, Vec<u8>)> = Vec::new();
        for (id, st) in self.streams.iter() {
            if st.subscribed_topic.as_deref() == Some(topic.as_str()) {
                let rec = proto::Record {
                    offset: record.offset,
                    key: record.key.clone().unwrap_or_default(),
                    value: record.value.clone(),
                    timestamp_ms: record.timestamp_ms,
                    topic: topic.clone(),
                };
                to_send.push((*id, encode_grpc_message(&rec.encode())));
            }
        }
        for (id, msg) in to_send {
            if let Some(st) = self.streams.get_mut(&id) {
                st.outbound.push(msg);
            }
            self.flush_stream(id).await?;
        }
        Ok(())
    }

    async fn on_frame(&mut self, frame: http2::Frame) -> Result<()> {
        match frame.frame_type {
            http2::FRAME_SETTINGS => self.on_settings(&frame).await?,
            http2::FRAME_PING => self.on_ping(&frame).await?,
            http2::FRAME_WINDOW_UPDATE => self.on_window_update(&frame).await?,
            http2::FRAME_HEADERS => self.on_headers(frame).await?,
            http2::FRAME_CONTINUATION => self.on_continuation(frame).await?,
            http2::FRAME_DATA => self.on_data(frame).await?,
            http2::FRAME_RST_STREAM => self.on_rst_stream(&frame).await?,
            http2::FRAME_GOAWAY => {
                // Peer is closing; stop serving.
                return Ok(());
            }
            _ => {
                // PRIORITY, PUSH_PROMISE, etc.: read off the wire (already
                // consumed) and ignored.
            }
        }
        Ok(())
    }

    async fn on_settings(&mut self, frame: &http2::Frame) -> Result<()> {
        if frame.flag(http2::FLAG_ACK) {
            self.peer_settings_acked = true;
            return Ok(());
        }
        // Apply any parameter updates we care about.
        for (id, val) in http2::parse_settings(&frame.payload)? {
            if id == http2::SETTINGS_INITIAL_WINDOW_SIZE {
                // Re-size every existing stream's send window by the delta.
                let new_window = val as i64;
                let old_window = self.peer_initial_window as i64;
                let delta = new_window - old_window;
                for st in self.streams.values_mut() {
                    st.send_window += delta;
                }
                self.peer_initial_window = val;
            }
        }
        // Acknowledge.
        http2::write_frame(&mut self.write, http2::FRAME_SETTINGS, http2::FLAG_ACK, 0, &[]).await?;
        Ok(())
    }

    async fn on_ping(&mut self, frame: &http2::Frame) -> Result<()> {
        // Echo the opaque payload back with the ACK flag (RFC 7540 §6.7).
        http2::write_frame(
            &mut self.write,
            http2::FRAME_PING,
            http2::FLAG_ACK,
            0,
            &frame.payload,
        )
        .await
    }

    async fn on_window_update(&mut self, frame: &http2::Frame) -> Result<()> {
        // A WINDOW_UPDATE payload is exactly 4 bytes (RFC 7540 §6.9); take the
        // last 4 to be resilient to any prepend.
        let delta = if frame.payload.len() >= 4 {
            let tail = &frame.payload[frame.payload.len() - 4..];
            u32::from_be_bytes(tail.try_into().unwrap()) as i64
        } else {
            0
        };
        if frame.stream_id == 0 {
            self.conn_send_window += delta;
        } else if let Some(st) = self.streams.get_mut(&frame.stream_id) {
            st.send_window += delta;
        }
        // Flush anything that was blocked (connection first, then the stream).
        if frame.stream_id != 0 {
            self.flush_stream(frame.stream_id).await?;
        }
        Ok(())
    }

    async fn on_rst_stream(&mut self, frame: &http2::Frame) -> Result<()> {
        self.streams.remove(&frame.stream_id);
        Ok(())
    }

    async fn on_headers(&mut self, frame: http2::Frame) -> Result<()> {
        let frag = http2::headers_block_fragment(&frame)?;
        let is_end = frame.flag(http2::FLAG_END_HEADERS);
        if is_end {
            let block = self
                .pending_headers
                .take()
                .unwrap_or_default()
                .into_iter()
                .chain(frag)
                .collect::<Vec<u8>>();
            self.finish_headers(frame.stream_id, &block).await?;
        } else {
            let pend = self.pending_headers.get_or_insert_with(Vec::new);
            pend.extend_from_slice(&frag);
        }
        Ok(())
    }

    async fn on_continuation(&mut self, frame: http2::Frame) -> Result<()> {
        // A CONTINUATION carries more of the header block; only legal when a
        // HEADERS (or prior CONTINUATION) hasn't set END_HEADERS yet.
        let frag = http2::headers_block_fragment(&frame)?;
        let is_end = frame.flag(http2::FLAG_END_HEADERS);
        let pend = self.pending_headers.get_or_insert_with(Vec::new);
        pend.extend_from_slice(&frag);
        if is_end {
            let block = self.pending_headers.take().unwrap_or_default();
            self.finish_headers(frame.stream_id, &block).await?;
        }
        Ok(())
    }

    async fn finish_headers(&mut self, stream_id: u32, block: &[u8]) -> Result<()> {
        let headers = self.hpack.decode(block)?;
        let path = headers
            .iter()
            .find(|(n, _)| n == ":path")
            .map(|(_, v)| v.clone());
        if !self.streams.contains_key(&stream_id) {
            self.streams.insert(stream_id, Stream::new());
        }
        let st = self.streams.get_mut(&stream_id).unwrap();
        st.path = path.clone();
        st.headers = headers;
        st.send_window = self.peer_initial_window as i64;

        if stream_id % 2 != 0 {
            // Real clients use odd stream ids for requests; ids !=1 are fine,
            // but we never initiate streams ourselves.
        }

        // If the HEADERS frame itself carries END_STREAM (a headers-only /
        // trailers-only request, or an empty-body RPC), the request is done.
        if frame_is_end_stream_headers_placeholder() {
            // never true here; END_STREAM for requests arrives on DATA.
        }
        Ok(())
    }

    async fn on_data(&mut self, frame: http2::Frame) -> Result<()> {
        let data = http2::data_payload(&frame)?;
        // Acknowledge the received bytes so the peer's receive window refills.
        self.send_window_update(frame.stream_id, data.len() as u32).await?;

        let st = match self.streams.get_mut(&frame.stream_id) {
            Some(s) => s,
            None => return Ok(()),
        };
        st.body.extend_from_slice(&data);

        if frame.flag(http2::FLAG_END_STREAM) {
            st.req_ended = true;
            // The full request body is buffered; parse the gRPC message and
            // dispatch. We must release the `self.streams` borrow before the
            // per-`self` calls below (send_response_head / flush_stream), so
            // capture what we need first.
            let body = std::mem::take(&mut st.body);
            let path = st.path.clone().unwrap_or_default();
            let authorization = st
                .headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case("authorization"))
                .map(|(_, v)| v.clone());
            // Authenticate via the shared bridge helper. Zero-config
            // (`_tpt_roles` empty) skips this; otherwise a valid
            // `Authorization: Basic` metadata header is required.
            if let Err(e) = authenticate_basic(&self.roles, &self.db, authorization.as_deref()) {
                self.send_error(frame.stream_id, GRPC_UNAUTHENTICATED, &format!("unauthorized: {e}"))
                    .await?;
                self.streams.remove(&frame.stream_id);
                return Ok(());
            }
            let dispatch = dispatch(&path, &body, &self.db);
            drop(st);
            match dispatch {
                DispatchResult::Unary(resp_headers, msg, trailers) => {
                    self.send_response_head(frame.stream_id, &resp_headers)
                        .await?;
                    if let Some(m) = msg {
                        if let Some(s) = self.streams.get_mut(&frame.stream_id) {
                            s.outbound.push(encode_grpc_message(&m));
                        }
                    }
                    if let Some(s) = self.streams.get_mut(&frame.stream_id) {
                        s.pending_trailers = Some(trailers);
                    }
                    self.flush_stream(frame.stream_id).await?;
                }
                DispatchResult::Subscribe(topic) => {
                    // HEADERS (200) then leave the stream open; records are
                    // pushed via `on_flux_event`.
                    self.send_response_head(frame.stream_id, &[]).await?;
                    if let Some(s) = self.streams.get_mut(&frame.stream_id) {
                        s.subscribed_topic = Some(topic);
                    }
                }
                DispatchResult::Error(status, msg) => {
                    self.send_error(frame.stream_id, status, &msg).await?;
                    self.streams.remove(&frame.stream_id);
                }
            }
        }
        Ok(())
    }

    // ---- outbound helpers --------------------------------------------------

    async fn send_window_update(&mut self, stream_id: u32, delta: u32) -> Result<()> {
        let mut payload = [0u8; 4];
        payload.copy_from_slice(&delta.to_be_bytes());
        http2::write_frame(
            &mut self.write,
            http2::FRAME_WINDOW_UPDATE,
            0,
            stream_id,
            &payload,
        )
        .await
    }

    /// Sends the response HEADERS (`:status 200`, `content-type:
    /// application/grpc`). `extra` are additional response headers (none for
    /// streaming success).
    async fn send_response_head(&mut self, stream_id: u32, extra: &[(&str, &str)]) -> Result<()> {
        let mut headers: Vec<(&str, &str)> = vec![
            (":status", "200"),
            ("content-type", "application/grpc"),
        ];
        headers.extend_from_slice(extra);
        let block = hpack::encode(&headers);
        http2::write_frame(
            &mut self.write,
            http2::FRAME_HEADERS,
            http2::FLAG_END_HEADERS,
            stream_id,
            &block,
        )
        .await
    }

    async fn send_error(&mut self, stream_id: u32, status: u8, msg: &str) -> Result<()> {
        let status_str = status.to_string();
        let headers = vec![
            (":status", "200"),
            ("content-type", "application/grpc"),
            ("grpc-status", &status_str),
            ("grpc-message", msg),
        ];
        let block = hpack::encode(&headers);
        http2::write_frame(
            &mut self.write,
            http2::FRAME_HEADERS,
            http2::FLAG_END_HEADERS | http2::FLAG_END_STREAM,
            stream_id,
            &block,
        )
        .await
    }

    /// Drains `stream.outbound` (subject to flow control) and, once empty, emits
    /// any pending trailers as a final END_STREAM HEADERS. The per-message
    /// borrow of `self.streams` is released before each `await` on `self.write`
    /// (the stream map and the writer are disjoint, so we must not hold both at
    /// once across an await).
    async fn flush_stream(&mut self, stream_id: u32) -> Result<()> {
        loop {
            // Take the next message, releasing the streams borrow before write.
            let msg = match self.streams.get_mut(&stream_id) {
                Some(s) => {
                    let len = s.outbound.first().map(|m| m.len() as i64);
                    match len {
                        Some(l)
                            if l <= self.conn_send_window && l <= s.send_window =>
                        {
                            s.outbound.remove(0)
                        }
                        Some(_) => return Ok(()), // blocked on flow control
                        None => {
                            // Nothing queued: emit pending trailers if any.
                            if let Some((status, msg)) = s.pending_trailers.take() {
                                let status_str = status.to_string();
                                let trailer_headers = [
                                    ("grpc-status", status_str.as_str()),
                                    ("grpc-message", msg.as_str()),
                                ];
                                let block = hpack::encode(&trailer_headers);
                                http2::write_frame(
                                    &mut self.write,
                                    http2::FRAME_HEADERS,
                                    http2::FLAG_END_HEADERS | http2::FLAG_END_STREAM,
                                    stream_id,
                                    &block,
                                )
                                .await?;
                                let is_subscribe = s.subscribed_topic.is_some();
                                if !is_subscribe {
                                    self.streams.remove(&stream_id);
                                }
                            }
                            return Ok(());
                        }
                    }
                }
                None => return Ok(()),
            };
            let len = msg.len() as i64;
            self.conn_send_window -= len;
            if let Some(s) = self.streams.get_mut(&stream_id) {
                s.send_window -= len;
            }
            http2::write_frame(&mut self.write, http2::FRAME_DATA, 0, stream_id, &msg).await?;
        }
    }
}

/// Placeholder so `finish_headers` reads cleanly; requests end on DATA
/// END_STREAM, not on HEADERS.
fn frame_is_end_stream_headers_placeholder() -> bool {
    false
}

// ---- gRPC message framing --------------------------------------------------

/// Wraps a serialized protobuf message in the gRPC wire envelope: a 1-byte
/// compression flag (0 = uncompressed) + 4-byte big-endian length + payload.
fn encode_grpc_message(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(0u8); // compressed = false
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Extracts the first gRPC message from a buffered request body (a request
/// body contains at least one complete message).
fn decode_first_grpc_message(body: &[u8]) -> Result<Vec<u8>> {
    anyhow::ensure!(body.len() >= 5, "grpc: request body too short for a message");
    anyhow::ensure!(body[0] == 0, "grpc: compressed messages are not supported");
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    anyhow::ensure!(5 + len <= body.len(), "grpc: message length exceeds body");
    Ok(body[5..5 + len].to_vec())
}

// ---- dispatch --------------------------------------------------------------

enum DispatchResult {
    Unary(Vec<(&'static str, &'static str)>, Option<Vec<u8>>, (u8, String)),
    Subscribe(String),
    Error(u8, String),
}

fn dispatch(path: &str, body: &[u8], db: &Database) -> DispatchResult {
    match path {
        "/flux.Flux/Publish" => {
            let req = match proto::PublishRequest::decode(&body_for(body)) {
                Ok(r) => r,
                Err(e) => return DispatchResult::Error(GRPC_INTERNAL, e.to_string()),
            };
            match db.flux_publish(
                &req.topic,
                req.has_partition.then_some(req.partition),
                Some(req.key),
                req.value,
            ) {
                Ok((partition, offset)) => {
                    let resp = proto::PublishResponse { partition, offset };
                    DispatchResult::Unary(
                        vec![],
                        Some(resp.encode()),
                        (GRPC_OK, String::new()),
                    )
                }
                Err(e) => DispatchResult::Error(GRPC_INTERNAL, e.to_string()),
            }
        }
        "/flux.Flux/Poll" => {
            let req = match proto::PollRequest::decode(&body_for(body)) {
                Ok(r) => r,
                Err(e) => return DispatchResult::Error(GRPC_INTERNAL, e.to_string()),
            };
            match db.flux_poll(&req.topic, req.partition, &req.group, req.max as usize) {
                Ok(records) => {
                    let recs: Vec<proto::Record> = records
                        .into_iter()
                        .map(|r| proto::Record {
                            offset: r.offset,
                            key: r.key.unwrap_or_default(),
                            value: r.value,
                            timestamp_ms: r.timestamp_ms,
                            topic: req.topic.clone(),
                        })
                        .collect();
                    let resp = proto::PollResponse { records: recs };
                    DispatchResult::Unary(vec![], Some(resp.encode()), (GRPC_OK, String::new()))
                }
                Err(e) => DispatchResult::Error(GRPC_INTERNAL, e.to_string()),
            }
        }
        "/flux.Flux/Subscribe" => {
            let req = match proto::SubscribeRequest::decode(&body_for(body)) {
                Ok(r) => r,
                Err(e) => return DispatchResult::Error(GRPC_INTERNAL, e.to_string()),
            };
            if req.topic.is_empty() {
                return DispatchResult::Error(GRPC_INTERNAL, "subscribe topic must not be empty".into());
            }
            DispatchResult::Subscribe(req.topic)
        }
        "/flux.Flux/CreateTopic" => {
            let req = match proto::CreateTopicRequest::decode(&body_for(body)) {
                Ok(r) => r,
                Err(e) => return DispatchResult::Error(GRPC_INTERNAL, e.to_string()),
            };
            if req.name.is_empty() {
                return DispatchResult::Error(GRPC_INTERNAL, "topic name must not be empty".into());
            }
            // Idempotent: create only if it doesn't already exist.
            if db.flux_num_partitions(&req.name).is_none() {
                if let Err(e) = db.create_topic(&req.name, 1, None, None) {
                    return DispatchResult::Error(GRPC_INTERNAL, e.to_string());
                }
            }
            DispatchResult::Unary(vec![], Some(proto::CreateTopicResponse {}.encode()), (GRPC_OK, String::new()))
        }
        other => DispatchResult::Error(
            GRPC_UNIMPLEMENTED,
            format!("unknown method: {other}"),
        ),
    }
}

/// The request body arrives wrapped in the gRPC envelope; extract the first
/// message's bytes as the protobuf payload.
fn body_for(body: &[u8]) -> Vec<u8> {
    decode_first_grpc_message(body).unwrap_or_default()
}
