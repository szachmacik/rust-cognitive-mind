//! CognitiveMind Rust Node v2 — Maximum Throughput
//!
//! Architektura:
//!   - tokio async runtime (zero-cost abstractions)
//!   - tokio-tungstenite dla WS (native-tls)
//!   - SIMD-friendly JSON via serde_json
//!   - Lock-free broadcast via tokio::broadcast
//!   - Upstash REST polling jako fallback pub/sub
//!   - Port 9001: lokalny WS serwer (Python/Node agenci)
//!   - Automatyczny reconnect z exponential backoff

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use tokio::sync::{RwLock, broadcast};
use tokio::time::interval;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, accept_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn, error, debug};

// ── Config ────────────────────────────────────────────────────────────────────
const CF_HUB_WS: &str = "wss://cognitive-mind.maciej-koziej01.workers.dev/ws";
const UPSTASH_URL: &str = "https://fresh-walleye-84119.upstash.io";
const BRAIN_ROUTER: &str = "https://brain-router.maciej-koziej01.workers.dev";
const LOCAL_PORT: u16 = 9001;
const NODE_ID: &str = "rust-ws-do-1";
const RECONNECT_BASE_MS: u64 = 1000;
const RECONNECT_MAX_MS: u64 = 30000;
const PING_INTERVAL_S: u64 = 20;
const INFRA_CHECK_S: u64 = 60;

// ── Messages ──────────────────────────────────────────────────────────────────
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Frame {
    #[serde(rename = "type")]
    pub t: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<u64>,
}

impl Frame {
    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }
    fn hello() -> String {
        serde_json::to_string(&Frame {
            t: "hello".into(),
            node_id: Some(NODE_ID.into()),
            node_type: Some("rust_ws".into()),
            payload: Some(json!({
                "local_port": LOCAL_PORT,
                "runtime": "tokio",
                "version": "2.0"
            })),
            topic: None, event: None, prompt: None,
            ts: Some(Self::now_ms()),
        }).unwrap_or_default()
    }
    fn publish(topic: &str, event: &str, payload: Value) -> String {
        serde_json::to_string(&json!({
            "type": "publish", "topic": topic, "event": event,
            "payload": payload, "source_node": NODE_ID,
            "ts": Self::now_ms()
        })).unwrap_or_default()
    }
    fn ping() -> String { r#"{"type":"ping"}"#.into() }
}

// ── Shared State ──────────────────────────────────────────────────────────────
#[derive(Default)]
struct CognState {
    knowledge: HashMap<String, Value>,
    peer_count: usize,
    last_sync: u64,
    msg_in: u64,
    msg_out: u64,
}

type State = Arc<RwLock<CognState>>;

// ── HTTP Client (pre-allocated) ───────────────────────────────────────────────
struct FastHttp {
    client: reqwest::Client,
    upstash_url: String,
    upstash_token: String,
    brain_router_token: String,
}

impl FastHttp {
    fn new() -> Self {
        FastHttp {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .tcp_keepalive(Duration::from_secs(30))
                .pool_idle_timeout(Duration::from_secs(90))
                .build().unwrap(),
            upstash_url: UPSTASH_URL.into(),
            upstash_token: std::env::var("UPSTASH_TOKEN").unwrap_or_default(),
            brain_router_token: std::env::var("BRAIN_ROUTER_TOKEN")
                .unwrap_or("holon-brain-router-2026".into()),
        }
    }

    async fn upstash_set(&self, key: &str, value: &str, ex: u64) -> bool {
        let url = format!("{}/set/{}/{}/EX/{}", self.upstash_url, key, 
            urlencoding::encode(value), ex);
        self.client.get(&url)
            .bearer_auth(&self.upstash_token)
            .send().await.map(|r| r.status().is_success()).unwrap_or(false)
    }

    async fn upstash_get(&self, key: &str) -> Option<String> {
        let url = format!("{}/get/{}", self.upstash_url, key);
        let r = self.client.get(&url).bearer_auth(&self.upstash_token).send().await.ok()?;
        let d: Value = r.json().await.ok()?;
        d["result"].as_str().map(String::from)
    }

    async fn groq_fast(&self, prompt: &str) -> Option<String> {
        let key = std::env::var("GROQ_API_KEY").unwrap_or_default();
        if key.is_empty() { return None; }
        let r = self.client.post("https://api.groq.com/openai/v1/chat/completions")
            .bearer_auth(&key)
            .json(&json!({
                "model": "llama-3.1-8b-instant",
                "messages": [{"role":"user","content":prompt}],
                "max_tokens": 256
            }))
            .send().await.ok()?;
        let d: Value = r.json().await.ok()?;
        d["choices"][0]["message"]["content"].as_str().map(String::from)
    }

    async fn push_to_hub(&self, frame: &str) -> bool {
        self.client.post(format!("{}/push", "https://cognitive-mind.maciej-koziej01.workers.dev"))
            .header("Content-Type", "application/json")
            .body(frame.to_string())
            .send().await.map(|r| r.status().is_success()).unwrap_or(false)
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════════════════════
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    dotenvy::dotenv().ok();

    info!("🧠 CognitiveMind Rust v2 starting");
    info!("   Hub: {}", CF_HUB_WS);
    info!("   Local WS: :{}", LOCAL_PORT);

    let state: State = Arc::new(RwLock::new(CognState::default()));
    let http = Arc::new(FastHttp::new());

    // Broadcast channel: DO → local agents (lock-free)
    let (tx, _) = broadcast::channel::<String>(512);
    let tx = Arc::new(tx);

    // Task 1: CF Durable Object connection (reconnecting)
    {
        let s = state.clone(); let t = tx.clone(); let h = http.clone();
        tokio::spawn(async move { cf_hub_loop(s, t, h).await; });
    }

    // Task 2: Local WS server (Python/Node agents connect here)
    {
        let s = state.clone(); let t = tx.clone(); let h = http.clone();
        tokio::spawn(async move {
            if let Err(e) = local_ws_server(s, t, h).await {
                error!("Local WS: {}", e);
            }
        });
    }

    // Task 3: Infrastructure monitor (Ollama, serwisy)
    {
        let t = tx.clone(); let h = http.clone(); let s = state.clone();
        tokio::spawn(async move { infra_monitor(t, h, s).await; });
    }

    // Task 4: Stats reporter (co 5 min do CF hub)
    {
        let s = state.clone(); let h = http.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(300));
            loop {
                tick.tick().await;
                let st = s.read().await;
                let frame = Frame::publish("rust-node-stats", "heartbeat", json!({
                    "node": NODE_ID,
                    "msg_in": st.msg_in,
                    "msg_out": st.msg_out,
                    "peers": st.peer_count,
                    "knowledge_topics": st.knowledge.len()
                }));
                h.push_to_hub(&frame).await;
            }
        });
    }

    tokio::signal::ctrl_c().await?;
    info!("Shutdown");
    Ok(())
}

// ── CF Hub WebSocket Loop (auto-reconnect) ────────────────────────────────────
async fn cf_hub_loop(state: State, tx: Arc<broadcast::Sender<String>>, http: Arc<FastHttp>) {
    let mut backoff_ms = RECONNECT_BASE_MS;
    loop {
        info!("Connecting to CF hub...");
        match cf_hub_session(state.clone(), tx.clone(), http.clone()).await {
            Ok(_)  => { info!("CF hub disconnected cleanly"); backoff_ms = RECONNECT_BASE_MS; }
            Err(e) => { warn!("CF hub error: {}. Retry in {}ms", e, backoff_ms); }
        }
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
    }
}

async fn cf_hub_session(
    state: State, tx: Arc<broadcast::Sender<String>>, _http: Arc<FastHttp>
) -> anyhow::Result<()> {
    let url = format!("{}?node={}&type=rust_ws", CF_HUB_WS, NODE_ID);
    let (ws, _) = connect_async(&url).await?;
    info!("✅ CF hub connected");

    let (mut write, mut read) = ws.split();
    let mut rx = tx.subscribe();
    let mut ping_tick = interval(Duration::from_secs(PING_INTERVAL_S));

    // Przedstaw się
    write.send(Message::Text(Frame::hello().into())).await?;

    loop {
        tokio::select! {
            // Odbieraj z CF hub → broadcast local
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let text_str = text.to_string();
                        state.write().await.msg_in += 1;
                        // Broadcast do lokalnych agentów
                        let _ = tx.send(text_str.clone());
                        // Aktualizuj knowledge cache
                        if let Ok(f) = serde_json::from_str::<Value>(&text_str) {
                            if f["type"] == "knowledge" {
                                if let (Some(topic), Some(payload)) = (f["topic"].as_str(), f.get("payload")) {
                                    state.write().await.knowledge.insert(topic.to_string(), payload.clone());
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("CF hub closed connection");
                        break;
                    }
                    _ => {}
                }
            }
            // Wysyłaj do CF hub (z lokalnych agentów)
            out = rx.recv() => {
                if let Ok(msg) = out {
                    // Tylko jeśli to wiadomość do CF (nie echo z CF)
                    if let Ok(f) = serde_json::from_str::<Value>(&msg) {
                        if f["type"] == "publish" || f["type"] == "groq" {
                            write.send(Message::Text(msg.into())).await?;
                            state.write().await.msg_out += 1;
                        }
                    }
                }
            }
            // Ping
            _ = ping_tick.tick() => {
                write.send(Message::Text(Frame::ping().into())).await?;
                debug!("Ping sent to CF hub");
            }
        }
    }
    Ok(())
}

// ── Local WS Server (port 9001) ───────────────────────────────────────────────
async fn local_ws_server(
    state: State, tx: Arc<broadcast::Sender<String>>, _http: Arc<FastHttp>
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", LOCAL_PORT)).await?;
    info!("🔌 Local WS server: ws://0.0.0.0:{}", LOCAL_PORT);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Local agent connected: {}", addr);
        let s = state.clone(); let t = tx.clone();
        tokio::spawn(async move {
            handle_local_agent(stream, s, t).await;
            info!("Local agent disconnected: {}", addr);
        });
    }
}

async fn handle_local_agent(
    stream: tokio::net::TcpStream,
    state: State,
    tx: Arc<broadcast::Sender<String>>,
) {
    let Ok(ws) = accept_async(stream).await else { return; };
    let (mut write, mut read) = ws.split();
    let mut rx = tx.subscribe();

    // Wyślij snapshot
    let snapshot = {
        let st = state.read().await;
        serde_json::to_string(&json!({
            "type": "snapshot",
            "knowledge": st.knowledge,
            "peers": st.peer_count,
            "ts": Frame::now_ms()
        })).unwrap_or_default()
    };
    let _ = write.send(Message::Text(snapshot.into())).await;

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Lokalny agent publikuje → do CF hub przez broadcast
                        state.write().await.msg_in += 1;
                        let _ = tx.send(text.to_string());
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            broadcast = rx.recv() => {
                if let Ok(msg) = broadcast {
                    if write.send(Message::Text(msg.into())).await.is_err() { break; }
                    state.write().await.msg_out += 1;
                }
            }
        }
    }
}

// ── Infrastructure Monitor ────────────────────────────────────────────────────
async fn infra_monitor(tx: Arc<broadcast::Sender<String>>, http: Arc<FastHttp>, state: State) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5)).build().unwrap();
    let mut tick = interval(Duration::from_secs(INFRA_CHECK_S));

    loop {
        tick.tick().await;
        let t0 = Instant::now();

        // Check Ollama
        let ollama = client.get("http://localhost:11434/api/tags")
            .send().await.map(|r| r.status().is_success()).unwrap_or(false);

        // Check brain-router
        let brain = client.get(format!("{}/health", BRAIN_ROUTER))
            .send().await.map(|r| r.status().is_success()).unwrap_or(false);

        let frame = Frame::publish("infra", "heartbeat", json!({
            "node": NODE_ID,
            "ollama": ollama,
            "brain_router": brain,
            "check_ms": t0.elapsed().as_millis(),
            "knowledge_topics": state.read().await.knowledge.len()
        }));

        let _ = tx.send(frame);
        info!("Infra: ollama={} brain-router={}", ollama, brain);
    }
}

trait FrameTs { fn now_ms() -> u64; }
impl FrameTs for Frame {
    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }
}
