use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr};
use iroh_tickets::endpoint::EndpointTicket;
use iroh_tickets::Ticket as _;
use n0_future::task;
use n0_future::time::{Duration, Instant};
use serde::Serialize;
use serde_json::Value;
use tracing::{debug, info};

use crate::proto::{self, FrameReader, FrameWriter, LOBBY_ALPN, SIGNAL_ALPN};

const ROSTER_SAMPLE: usize = 32;
const PEER_TTL: Duration = Duration::from_secs(150);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(8);
const SEARCH_INTERVAL: Duration = Duration::from_secs(1);
const MATCH_TIMEOUT: Duration = Duration::from_secs(12);
const LOBBY_TIMEOUT: Duration = Duration::from_secs(15);
const REMATCH_COOLDOWN: Duration = Duration::from_secs(90);
const DIAL_RETRY_COOLDOWN: Duration = Duration::from_secs(20);
const FANOUT: usize = 3;
const MAX_CHAT_CHARS: usize = 4000;

/// Refusals that mean "not right now" rather than "this peer is unreachable".
const SOFT_REFUSALS: &[&str] = &["in-chat", "not-searching", "dialing", "raced"];

/// Seed nodes. Any running Domegle node - browser, desktop or phone - answers
/// the lobby ALPN, so any of their tickets works here.
pub const DEFAULT_BOOTSTRAP: &[&str] = &[
    "endpointac5dh6m5rpgvkdi645lgtkbuaoaoxim55ka42yvicqoahu24s4ztwbibacwbcaab2gaqeaiavqjaaaorqebacafmcmaadumbaiaqbqfiaefndaicaeambkd2ahiycaq",
];

/// Everything the UI is told about.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Event {
    Status {
        status: String,
        endpoint_id: String,
        ticket: String,
        peers: usize,
        nickname: String,
    },
    Matched {
        peer_id: String,
        peer_nick: String,
        initiator: bool,
    },
    /// SDP or ICE from the stranger, bound for the local WebRTC stack.
    Signal {
        kind: String,
        payload: Value,
    },
    Chat {
        text: String,
    },
    Typing {
        on: bool,
    },
    Ended {
        reason: String,
    },
    Notice {
        text: String,
    },
}

struct Peer {
    ticket: String,
    last_seen: Instant,
}

struct Session {
    peer_id: String,
    peer_nick: String,
    initiator: bool,
    writer: tokio::sync::Mutex<FrameWriter>,
    connection: Connection,
    closed: AtomicBool,
}

impl Session {
    async fn send(&self, value: &Value) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.writer.lock().await.write(value).await
    }
}

#[derive(Default)]
struct State {
    directory: HashMap<String, Peer>,
    session: Option<Arc<Session>>,
    want_chat: bool,
    pending_dial: Option<String>,
    dial_cooldown: HashMap<String, Instant>,
    recent_partners: HashMap<String, Instant>,
    nickname: String,
    bootstrap: Vec<String>,
}

pub struct Core {
    endpoint: Endpoint,
    state: Mutex<State>,
    events: async_channel::Sender<Event>,
}

impl Core {
    // --- identity ------------------------------------------------------------

    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    pub fn ticket(&self) -> String {
        EndpointTicket::new(self.endpoint.addr()).to_string()
    }

    fn nickname(&self) -> String {
        self.state.lock().unwrap().nickname.clone()
    }

    fn peer_count(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        prune(&mut state.directory);
        state.directory.len()
    }

    fn status(&self) -> &'static str {
        let state = self.state.lock().unwrap();
        if state.session.is_some() {
            "matched"
        } else if state.want_chat {
            "searching"
        } else {
            "idle"
        }
    }

    pub fn emit_status(&self) {
        let event = Event::Status {
            status: self.status().to_string(),
            endpoint_id: self.endpoint_id(),
            ticket: self.ticket(),
            peers: self.peer_count(),
            nickname: self.nickname(),
        };
        self.emit(event);
    }

    fn emit(&self, event: Event) {
        // The channel is bounded; dropping a UI event is better than stalling
        // the network task that produced it.
        let _ = self.events.try_send(event);
    }

    fn notice(&self, text: impl Into<String>) {
        let text = text.into();
        info!("{text}");
        self.emit(Event::Notice { text });
    }

    // --- directory -----------------------------------------------------------

    /// Record a peer. Returns true if this is somebody new.
    fn merge_peer(&self, endpoint_id: &str, ticket: &str) -> bool {
        if endpoint_id.is_empty() || ticket.is_empty() || endpoint_id == self.endpoint_id() {
            return false;
        }
        // A ticket that does not belong to the claimed id is either a bug or an
        // attempt to poison the roster; drop it either way.
        match ticket_owner(ticket) {
            Some(owner) if owner == endpoint_id => {}
            _ => {
                debug!("rejecting roster entry for {endpoint_id} (ticket mismatch)");
                return false;
            }
        }
        let mut state = self.state.lock().unwrap();
        match state.directory.get_mut(endpoint_id) {
            Some(peer) => {
                peer.ticket = ticket.to_string();
                peer.last_seen = Instant::now();
                false
            }
            None => {
                state.directory.insert(
                    endpoint_id.to_string(),
                    Peer {
                        ticket: ticket.to_string(),
                        last_seen: Instant::now(),
                    },
                );
                true
            }
        }
    }

    fn merge_entries(&self, entries: Vec<(String, String)>) -> bool {
        let mut changed = false;
        for (id, ticket) in entries {
            if self.merge_peer(&id, &ticket) {
                changed = true;
            }
        }
        changed
    }

    fn sample(&self, count: usize, exclude: Option<&str>) -> Vec<(String, String)> {
        let mut state = self.state.lock().unwrap();
        prune(&mut state.directory);
        let mut peers: Vec<(String, String)> = state
            .directory
            .iter()
            .filter(|(id, _)| Some(id.as_str()) != exclude)
            .map(|(id, peer)| (id.clone(), peer.ticket.clone()))
            .collect();
        shuffle(&mut peers);
        peers.truncate(count);
        peers
    }

    fn roster_entries(&self, exclude: Option<&str>) -> Vec<Value> {
        self.sample(ROSTER_SAMPLE, exclude)
            .into_iter()
            .map(|(id, ticket)| proto::peer_entry(&id, &ticket))
            .collect()
    }

    pub fn add_bootstrap(&self, ticket: &str) -> Result<String> {
        let ticket = ticket.trim().to_string();
        let owner = ticket_owner(&ticket).ok_or_else(|| anyhow!("that ticket does not parse"))?;
        if owner == self.endpoint_id() {
            return Err(anyhow!("that ticket is this tab's own node"));
        }
        {
            let mut state = self.state.lock().unwrap();
            if !state.bootstrap.contains(&ticket) {
                state.bootstrap.push(ticket.clone());
            }
        }
        self.merge_peer(&owner, &ticket);
        self.notice(format!("added seed {}", short(&owner)));
        self.emit_status();
        Ok(owner)
    }

    // --- commands ------------------------------------------------------------

    pub fn set_nickname(&self, nickname: &str) {
        let cleaned = nickname.trim().chars().take(32).collect::<String>();
        let mut state = self.state.lock().unwrap();
        state.nickname = if cleaned.is_empty() {
            "stranger".to_string()
        } else {
            cleaned
        };
    }

    pub fn start_search(self: &Arc<Self>) {
        self.state.lock().unwrap().want_chat = true;
        self.emit_status();
        self.notice("looking for a stranger...");
    }

    pub async fn next(self: &Arc<Self>) {
        self.state.lock().unwrap().want_chat = true;
        self.end_session("next").await;
        self.emit_status();
    }

    pub async fn stop(self: &Arc<Self>) {
        self.state.lock().unwrap().want_chat = false;
        self.end_session("stopped").await;
        self.emit_status();
    }

    /// Hand a locally produced SDP or ICE payload to the stranger.
    pub async fn send_signal(&self, kind: &str, payload: Value) {
        let session = self.state.lock().unwrap().session.clone();
        let Some(session) = session else { return };
        let message = match kind {
            "sdp" => proto::sdp(payload),
            "ice" => proto::ice(payload),
            _ => return,
        };
        let _ = session.send(&message).await;
    }

    pub async fn send_chat(&self, text: &str) {
        let text: String = text.chars().take(MAX_CHAT_CHARS).collect();
        if text.trim().is_empty() {
            return;
        }
        let session = self.state.lock().unwrap().session.clone();
        if let Some(session) = session {
            let _ = session.send(&proto::chat(&text)).await;
        }
    }

    pub async fn send_typing(&self, on: bool) {
        let session = self.state.lock().unwrap().session.clone();
        if let Some(session) = session {
            let _ = session.send(&proto::typing(on)).await;
        }
    }

    pub async fn shutdown(&self) {
        self.state.lock().unwrap().want_chat = false;
        self.end_session("shutdown").await;
        self.endpoint.close().await;
    }

    // --- sessions ------------------------------------------------------------

    fn install_session(&self, session: Arc<Session>) {
        {
            let mut state = self.state.lock().unwrap();
            state
                .recent_partners
                .insert(session.peer_id.clone(), Instant::now() + REMATCH_COOLDOWN);
            state.session = Some(session.clone());
        }
        self.notice(format!(
            "paired with {} ({})",
            short(&session.peer_id),
            if session.initiator {
                "offering"
            } else {
                "answering"
            }
        ));
        self.emit(Event::Matched {
            peer_id: session.peer_id.clone(),
            peer_nick: session.peer_nick.clone(),
            initiator: session.initiator,
        });
        self.emit_status();
    }

    async fn end_session(&self, reason: &str) {
        let session = self.state.lock().unwrap().session.clone();
        if let Some(session) = session {
            close_session(self, &session, reason, true).await;
        }
    }

    /// Why we cannot take this incoming match right now; `None` means accept.
    fn refuse_reason(&self, peer_id: &str) -> Option<&'static str> {
        let state = self.state.lock().unwrap();
        let now = Instant::now();
        if state.session.is_some() {
            return Some("in-chat");
        }
        if !state.want_chat {
            return Some("not-searching");
        }
        if state
            .recent_partners
            .get(peer_id)
            .is_some_and(|until| *until > now)
        {
            return Some("just-chatted");
        }
        // Simultaneous dials: the endpoint with the smaller id keeps the dialer
        // role, so exactly one of the two attempts survives.
        if state.pending_dial.is_some() && peer_id > self.endpoint_id().as_str() {
            return Some("dialing");
        }
        None
    }

    /// How long to skip a peer that just turned us down.
    ///
    /// A refusal that only reflects a moment in time deserves a short,
    /// *randomised* pause. Without the randomness two nodes retry in lockstep
    /// and refuse each other forever; without the shortness one mistimed attempt
    /// costs both of them the full dial cooldown.
    fn refusal_backoff(reason: &str) -> Duration {
        if SOFT_REFUSALS.contains(&reason) {
            Duration::from_millis(2_000 + rand_u64() % 4_000)
        } else {
            let base = DIAL_RETRY_COOLDOWN.as_millis() as u64;
            Duration::from_millis(base * 7 / 10 + rand_u64() % (base * 6 / 10))
        }
    }

    fn cool(&self, peer_id: &str, backoff: Duration) {
        let mut state = self.state.lock().unwrap();
        state
            .dial_cooldown
            .insert(peer_id.to_string(), Instant::now() + backoff);
    }
}

// --- protocol handlers -------------------------------------------------------

#[derive(Clone)]
struct LobbyProtocol(Arc<Core>);

// `ProtocolHandler` requires `Debug`; `Core` holds an endpoint and a mutex, so
// spell out something short rather than deriving through all of it.
impl std::fmt::Debug for LobbyProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LobbyProtocol")
    }
}

impl ProtocolHandler for LobbyProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let core = self.0.clone();
        if let Err(err) = handle_lobby(&core, &connection).await {
            debug!("lobby session dropped: {err}");
        }
        connection.close(0u32.into(), b"bye");
        Ok(())
    }
}

async fn handle_lobby(core: &Arc<Core>, connection: &Connection) -> Result<()> {
    let (send, recv) = n0_future::time::timeout(LOBBY_TIMEOUT, connection.accept_bi()).await??;
    let mut writer = FrameWriter::new(send);
    let mut reader = FrameReader::new(recv);

    let message = n0_future::time::timeout(LOBBY_TIMEOUT, reader.read())
        .await??
        .ok_or_else(|| anyhow!("lobby stream closed before the announce"))?;
    if proto::kind(&message) != "announce" {
        return Ok(());
    }

    let peer_id = proto::text_field(&message, "node_id").to_string();
    let mut changed = core.merge_peer(&peer_id, proto::text_field(&message, "ticket"));
    changed |= core.merge_entries(proto::peer_entries(&message, "peers"));

    let roster = proto::roster(
        &core.endpoint_id(),
        &core.ticket(),
        core.roster_entries(Some(&peer_id)),
    );
    writer.write(&roster).await?;
    writer.finish().await;
    if changed {
        core.emit_status();
    }
    Ok(())
}

#[derive(Clone)]
struct SignalProtocol(Arc<Core>);

impl std::fmt::Debug for SignalProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SignalProtocol")
    }
}

impl ProtocolHandler for SignalProtocol {
    /// Runs for the whole conversation: the returned future owns the session.
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let core = self.0.clone();
        if let Err(err) = handle_signal(&core, connection).await {
            debug!("inbound match failed: {err}");
        }
        Ok(())
    }
}

async fn handle_signal(core: &Arc<Core>, connection: Connection) -> Result<()> {
    let peer_id = connection.remote_id().to_string();
    let (send, recv) =
        n0_future::time::timeout(MATCH_TIMEOUT, connection.accept_bi()).await??;
    let mut writer = FrameWriter::new(send);
    let mut reader = FrameReader::new(recv);

    let request = n0_future::time::timeout(MATCH_TIMEOUT, reader.read())
        .await??
        .ok_or_else(|| anyhow!("signal stream closed before the request"))?;
    if proto::kind(&request) != "match_req" {
        writer.write(&proto::match_busy("bad-request")).await?;
        writer.finish().await;
        connection.close(0u32.into(), b"bye");
        return Ok(());
    }

    if let Some(reason) = core.refuse_reason(&peer_id) {
        writer.write(&proto::match_busy(reason)).await?;
        writer.finish().await;
        connection.close(0u32.into(), b"bye");
        return Ok(());
    }

    writer.write(&proto::match_ok(&core.nickname())).await?;
    let session = Arc::new(Session {
        peer_id,
        peer_nick: proto::text_field(&request, "nick").to_string(),
        initiator: false,
        writer: tokio::sync::Mutex::new(writer),
        connection,
        closed: AtomicBool::new(false),
    });
    core.install_session(session.clone());
    run_session(core.clone(), session, reader).await;
    Ok(())
}

// --- session loop ------------------------------------------------------------

async fn run_session(core: Arc<Core>, session: Arc<Session>, mut reader: FrameReader) {
    loop {
        match reader.read().await {
            Ok(Some(message)) => {
                if !dispatch(&core, &session, message).await {
                    return;
                }
            }
            Ok(None) => {
                close_session(&core, &session, "peer-disconnected", false).await;
                return;
            }
            Err(err) => {
                debug!("session {} ended: {err}", short(&session.peer_id));
                close_session(&core, &session, "stream-error", false).await;
                return;
            }
        }
    }
}

/// Returns false once the session is over.
async fn dispatch(core: &Arc<Core>, session: &Arc<Session>, message: Value) -> bool {
    match proto::kind(&message) {
        "sdp" => {
            if let Some(payload) = message.get("sdp") {
                core.emit(Event::Signal {
                    kind: "sdp".to_string(),
                    payload: payload.clone(),
                });
            }
        }
        "ice" => {
            if let Some(payload) = message.get("candidate") {
                core.emit(Event::Signal {
                    kind: "ice".to_string(),
                    payload: payload.clone(),
                });
            }
        }
        "chat" => {
            let text: String = proto::text_field(&message, "text")
                .chars()
                .take(MAX_CHAT_CHARS)
                .collect();
            if !text.is_empty() {
                core.emit(Event::Chat { text });
            }
        }
        "typing" => {
            let on = message
                .get("on")
                .and_then(Value::as_bool)
                .unwrap_or_default();
            core.emit(Event::Typing { on });
        }
        "bye" => {
            let reason = proto::text_field(&message, "reason").to_string();
            let reason = if reason.is_empty() {
                "peer-left".to_string()
            } else {
                reason
            };
            close_session(core, session, &reason, false).await;
            return false;
        }
        other => debug!("ignoring unknown frame {other}"),
    }
    true
}

async fn close_session(core: &Core, session: &Arc<Session>, reason: &str, notify_peer: bool) {
    if session.closed.swap(true, Ordering::SeqCst) {
        return;
    }
    if notify_peer {
        let _ = n0_future::time::timeout(
            Duration::from_secs(3),
            session.writer.lock().await.write(&proto::bye(reason)),
        )
        .await;
    }
    session.writer.lock().await.finish().await;
    session.connection.close(0u32.into(), b"bye");

    let mut is_current = false;
    {
        let mut state = core.state.lock().unwrap();
        if state
            .session
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            state.session = None;
            is_current = true;
        }
        // Push the rematch window out from the end of the chat, not the start.
        state
            .recent_partners
            .insert(session.peer_id.clone(), Instant::now() + REMATCH_COOLDOWN);
    }
    if is_current {
        core.emit(Event::Ended {
            reason: reason.to_string(),
        });
        core.emit_status();
    }
}

// --- background loops --------------------------------------------------------

async fn announce_loop(core: Arc<Core>) {
    // A short random delay stops tabs opened together from hammering the same
    // seed in lockstep.
    n0_future::time::sleep(Duration::from_millis(200 + rand_u64() % 1_300)).await;
    loop {
        let targets = announce_targets(&core);
        for ticket in targets {
            let core = core.clone();
            task::spawn(async move {
                if let Err(err) = announce_to(&core, &ticket).await {
                    debug!("announce to {}... failed: {err}", &ticket[..16.min(ticket.len())]);
                }
            });
        }
        let base = ANNOUNCE_INTERVAL.as_millis() as u64;
        n0_future::time::sleep(Duration::from_millis(base * 8 / 10 + rand_u64() % (base * 4 / 10)))
            .await;
    }
}

/// Tickets to contact this round: known peers first, seeds as backup.
fn announce_targets(core: &Arc<Core>) -> Vec<String> {
    let known: Vec<String> = core
        .sample(FANOUT, None)
        .into_iter()
        .map(|(_, ticket)| ticket)
        .collect();
    let mut seeds: Vec<String> = {
        let state = core.state.lock().unwrap();
        state.bootstrap.clone()
    };
    let own = core.endpoint_id();
    seeds.retain(|ticket| ticket_owner(ticket).as_deref() != Some(own.as_str()));
    shuffle(&mut seeds);

    if known.is_empty() {
        seeds.truncate(FANOUT.max(1));
        seeds
    } else {
        // Always keep one seed in the mix so a partitioned tab can rejoin.
        let mut targets = known;
        targets.extend(seeds.into_iter().take(1));
        targets
    }
}

async fn announce_to(core: &Arc<Core>, ticket: &str) -> Result<()> {
    let addr = ticket_addr(ticket).ok_or_else(|| anyhow!("unparsable ticket"))?;
    let connection =
        n0_future::time::timeout(LOBBY_TIMEOUT, core.endpoint.connect(addr, LOBBY_ALPN)).await??;
    let (send, recv) = n0_future::time::timeout(LOBBY_TIMEOUT, connection.open_bi()).await??;
    let mut writer = FrameWriter::new(send);
    let mut reader = FrameReader::new(recv);

    let announce = proto::announce(
        &core.endpoint_id(),
        &core.ticket(),
        core.roster_entries(None),
    );
    writer.write(&announce).await?;
    writer.finish().await;

    let reply = n0_future::time::timeout(LOBBY_TIMEOUT, reader.read()).await??;
    if let Some(reply) = reply {
        if proto::kind(&reply) == "roster" {
            let mut changed = core.merge_peer(
                proto::text_field(&reply, "node_id"),
                proto::text_field(&reply, "ticket"),
            );
            changed |= core.merge_entries(proto::peer_entries(&reply, "peers"));
            if changed {
                core.emit_status();
            }
        }
    }
    connection.close(0u32.into(), b"bye");
    Ok(())
}

async fn search_loop(core: Arc<Core>) {
    loop {
        let base = SEARCH_INTERVAL.as_millis() as u64;
        n0_future::time::sleep(Duration::from_millis(base * 6 / 10 + rand_u64() % base)).await;

        let candidate = {
            let mut state = core.state.lock().unwrap();
            if !state.want_chat || state.session.is_some() || state.pending_dial.is_some() {
                None
            } else {
                prune(&mut state.directory);
                let now = Instant::now();
                let mut options: Vec<(String, String)> = state
                    .directory
                    .iter()
                    .filter(|(id, _)| {
                        state.dial_cooldown.get(*id).is_none_or(|until| *until <= now)
                            && state
                                .recent_partners
                                .get(*id)
                                .is_none_or(|until| *until <= now)
                    })
                    .map(|(id, peer)| (id.clone(), peer.ticket.clone()))
                    .collect();
                shuffle(&mut options);
                options.into_iter().next()
            }
        };

        let Some((peer_id, ticket)) = candidate else {
            continue;
        };
        try_match(&core, peer_id, ticket).await;
    }
}

async fn try_match(core: &Arc<Core>, peer_id: String, ticket: String) {
    {
        let mut state = core.state.lock().unwrap();
        if !state.want_chat || state.session.is_some() || state.pending_dial.is_some() {
            return;
        }
        state.pending_dial = Some(peer_id.clone());
    }

    let outcome = dial_match(core, &peer_id, &ticket).await;
    core.state.lock().unwrap().pending_dial = None;

    if let Err(err) = outcome {
        core.cool(&peer_id, Core::refusal_backoff("error"));
        debug!("dial to {} failed: {err}", short(&peer_id));
    }
}

async fn dial_match(core: &Arc<Core>, peer_id: &str, ticket: &str) -> Result<()> {
    let addr = ticket_addr(ticket).ok_or_else(|| anyhow!("unparsable ticket"))?;
    let connection =
        n0_future::time::timeout(MATCH_TIMEOUT, core.endpoint.connect(addr, SIGNAL_ALPN)).await??;
    let (send, recv) = n0_future::time::timeout(MATCH_TIMEOUT, connection.open_bi()).await??;
    let mut writer = FrameWriter::new(send);
    let mut reader = FrameReader::new(recv);

    writer
        .write(&proto::match_request(&core.endpoint_id(), &core.nickname()))
        .await?;

    let reply = n0_future::time::timeout(MATCH_TIMEOUT, reader.read())
        .await??
        .unwrap_or(Value::Null);
    if proto::kind(&reply) != "match_ok" {
        let reason = proto::text_field(&reply, "reason");
        let reason = if reason.is_empty() { "no-reply" } else { reason };
        core.cool(peer_id, Core::refusal_backoff(reason));
        debug!("peer {} declined: {reason}", short(peer_id));
        writer.finish().await;
        connection.close(0u32.into(), b"bye");
        return Ok(());
    }

    // An inbound match may have landed while we were dialing.
    {
        let state = core.state.lock().unwrap();
        if state.session.is_some() || !state.want_chat {
            drop(state);
            let _ = writer.write(&proto::bye("raced")).await;
            writer.finish().await;
            connection.close(0u32.into(), b"bye");
            return Ok(());
        }
    }

    let session = Arc::new(Session {
        peer_id: peer_id.to_string(),
        peer_nick: proto::text_field(&reply, "nick").to_string(),
        initiator: true,
        writer: tokio::sync::Mutex::new(writer),
        connection,
        closed: AtomicBool::new(false),
    });
    core.install_session(session.clone());
    let core = core.clone();
    task::spawn(async move { run_session(core, session, reader).await });
    Ok(())
}

// --- spawning ----------------------------------------------------------------

pub struct Node {
    pub core: Arc<Core>,
    _router: Router,
    pub events: async_channel::Receiver<Event>,
}

impl Node {
    pub async fn spawn(
        nickname: String,
        extra_seeds: Vec<String>,
        secret_key: Option<[u8; 32]>,
    ) -> Result<Self> {
        let mut builder = Endpoint::builder(iroh::endpoint::presets::N0)
            .alpns(vec![SIGNAL_ALPN.to_vec(), LOBBY_ALPN.to_vec()]);
        // A stable key keeps this browser's endpoint id - and therefore its
        // ticket - the same across reloads, matching the desktop and Android
        // nodes. The page keeps the key in localStorage.
        if let Some(secret) = secret_key {
            builder = builder.secret_key(iroh::SecretKey::from_bytes(&secret));
        }
        let endpoint = builder.bind().await?;

        let (sender, receiver) = async_channel::bounded(256);
        let mut bootstrap: Vec<String> = DEFAULT_BOOTSTRAP.iter().map(|s| s.to_string()).collect();
        for seed in extra_seeds {
            let seed = seed.trim().to_string();
            if !seed.is_empty() && !bootstrap.contains(&seed) {
                bootstrap.push(seed);
            }
        }

        let core = Arc::new(Core {
            endpoint: endpoint.clone(),
            state: Mutex::new(State {
                nickname: if nickname.trim().is_empty() {
                    "stranger".to_string()
                } else {
                    nickname
                },
                bootstrap,
                ..Default::default()
            }),
            events: sender,
        });

        let router = Router::builder(endpoint)
            .accept(SIGNAL_ALPN, SignalProtocol(core.clone()))
            .accept(LOBBY_ALPN, LobbyProtocol(core.clone()))
            .spawn();

        task::spawn(announce_loop(core.clone()));
        task::spawn(search_loop(core.clone()));
        task::spawn({
            let core = core.clone();
            async move {
                // The ticket is far more useful to other peers once a relay is
                // attached, so re-publish once that happens.
                core.endpoint.online().await;
                core.notice(format!("node {} online", short(&core.endpoint_id())));
                core.emit_status();
            }
        });

        core.emit_status();
        Ok(Self {
            core,
            _router: router,
            events: receiver,
        })
    }
}

// --- helpers -----------------------------------------------------------------

fn prune(directory: &mut HashMap<String, Peer>) {
    let now = Instant::now();
    directory.retain(|_, peer| now.duration_since(peer.last_seen) < PEER_TTL);
}

pub fn ticket_owner(ticket: &str) -> Option<String> {
    ticket_addr(ticket).map(|addr| addr.id.to_string())
}

fn ticket_addr(ticket: &str) -> Option<EndpointAddr> {
    EndpointTicket::decode_string(ticket.trim())
        .ok()
        .map(|ticket| ticket.endpoint_addr().clone())
}

fn short(endpoint_id: &str) -> &str {
    &endpoint_id[..8.min(endpoint_id.len())]
}

/// Randomness without pulling in a full RNG: `getrandom` is already a
/// dependency because iroh needs it, and this is only used for jitter and for
/// picking a stranger.
fn rand_u64() -> u64 {
    let mut bytes = [0u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        return 0;
    }
    u64::from_le_bytes(bytes)
}

fn shuffle<T>(items: &mut [T]) {
    if items.len() < 2 {
        return;
    }
    for i in (1..items.len()).rev() {
        let j = (rand_u64() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}
