use anyhow::{bail, Result};
use iroh::endpoint::{RecvStream, SendStream};
use serde_json::{json, Value};

/// WebRTC handshake, typing state and fallback chat.
pub const SIGNAL_ALPN: &[u8] = b"/domegle/signal/1";
/// Peer-exchange rendezvous.
pub const LOBBY_ALPN: &[u8] = b"/domegle/lobby/1";

pub const PROTOCOL_VERSION: u32 = 1;

const MAX_FRAME: usize = 1 << 20;
const READ_CHUNK: usize = 16 * 1024;

/// Reads JSON objects off an iroh recv stream.
pub struct FrameReader {
    recv: RecvStream,
    buf: Vec<u8>,
    eof: bool,
}

impl FrameReader {
    pub fn new(recv: RecvStream) -> Self {
        Self {
            recv,
            buf: Vec::new(),
            eof: false,
        }
    }

    /// The next message, or `None` at end of stream.
    pub async fn read(&mut self) -> Result<Option<Value>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let text = std::str::from_utf8(&line[..line.len() - 1])?.trim();
                if text.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(text)?;
                if !value.is_object() {
                    bail!("frame is not an object");
                }
                return Ok(Some(value));
            }

            if self.eof {
                if !self.buf.is_empty() {
                    bail!("truncated frame at end of stream");
                }
                return Ok(None);
            }

            let mut chunk = [0u8; READ_CHUNK];
            let read = match self.recv.read(&mut chunk).await {
                Ok(Some(n)) => n,
                // `None` is a clean end of stream; an error means the peer went
                // away mid-stream. Both end the read loop.
                Ok(None) | Err(_) => 0,
            };
            if read == 0 {
                self.eof = true;
                continue;
            }
            if self.buf.len() + read > MAX_FRAME {
                bail!("frame exceeds maximum size");
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }
}

/// Writes JSON objects to an iroh send stream.
pub struct FrameWriter {
    send: SendStream,
    finished: bool,
}

impl FrameWriter {
    pub fn new(send: SendStream) -> Self {
        Self {
            send,
            finished: false,
        }
    }

    pub async fn write(&mut self, value: &Value) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        let mut payload = serde_json::to_vec(value)?;
        if payload.len() + 1 > MAX_FRAME {
            bail!("frame exceeds maximum size");
        }
        payload.push(b'\n');
        self.send.write_all(&payload).await?;
        Ok(())
    }

    pub async fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let _ = self.send.finish();
    }
}

// message constructors 

pub fn match_request(endpoint_id: &str, nickname: &str) -> Value {
    json!({ "t": "match_req", "v": PROTOCOL_VERSION, "node_id": endpoint_id, "nick": nickname })
}

pub fn match_ok(nickname: &str) -> Value {
    json!({ "t": "match_ok", "nick": nickname })
}

pub fn match_busy(reason: &str) -> Value {
    json!({ "t": "match_busy", "reason": reason })
}

pub fn sdp(description: Value) -> Value {
    json!({ "t": "sdp", "sdp": description })
}

pub fn ice(candidate: Value) -> Value {
    json!({ "t": "ice", "candidate": candidate })
}

pub fn chat(text: &str) -> Value {
    json!({ "t": "chat", "text": text })
}

pub fn typing(on: bool) -> Value {
    json!({ "t": "typing", "on": on })
}

pub fn bye(reason: &str) -> Value {
    json!({ "t": "bye", "reason": reason })
}

pub fn announce(endpoint_id: &str, ticket: &str, peers: Vec<Value>) -> Value {
    json!({
        "t": "announce",
        "v": PROTOCOL_VERSION,
        "node_id": endpoint_id,
        "ticket": ticket,
        "peers": peers,
    })
}

pub fn roster(endpoint_id: &str, ticket: &str, peers: Vec<Value>) -> Value {
    json!({
        "t": "roster",
        "v": PROTOCOL_VERSION,
        "node_id": endpoint_id,
        "ticket": ticket,
        "peers": peers,
    })
}

pub fn peer_entry(endpoint_id: &str, ticket: &str) -> Value {
    json!({ "node_id": endpoint_id, "ticket": ticket })
}

//  accessors 

pub fn kind(value: &Value) -> &str {
    value.get("t").and_then(Value::as_str).unwrap_or_default()
}

pub fn text_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

pub fn peer_entries(value: &Value, key: &str) -> Vec<(String, String)> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    (
                        text_field(entry, "node_id").to_string(),
                        text_field(entry, "ticket").to_string(),
                    )
                })
                .filter(|(id, ticket)| !id.is_empty() && !ticket.is_empty())
                .collect()
        })
        .unwrap_or_default()
}
