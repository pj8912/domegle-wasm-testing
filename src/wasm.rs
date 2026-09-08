use tracing::level_filters::LevelFilter;
use tracing_subscriber_wasm::MakeConsoleWriter;
use wasm_bindgen::{prelude::wasm_bindgen, JsError, JsValue};
use wasm_streams::{readable::sys::ReadableStream as JsReadableStream, ReadableStream};

use crate::node::{self, Event};

#[wasm_bindgen(start)]
fn start() {
    console_error_panic_hook::set_once();

    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        // Keeps trace events from printing a JS backtrace for every line.
        .with_writer(MakeConsoleWriter::default().map_trace_level_to(tracing::Level::DEBUG))
        // Without this the browser build panics at runtime: no clock.
        .without_time()
        .with_ansi(false)
        .init();
}

#[wasm_bindgen]
pub struct DomegleNode {
    inner: node::Node,
}

#[wasm_bindgen]
impl DomegleNode {
    /// Bind an endpoint and start discovery. One per tab.
    ///
    /// `secret_key` is 32 bytes kept by the page; pass the same bytes on every
    /// load to keep a stable endpoint id, or `undefined` for a throwaway one.
    pub async fn spawn(
        nickname: String,
        seeds: Vec<String>,
        secret_key: Option<Vec<u8>>,
    ) -> Result<DomegleNode, JsError> {
        let secret = match secret_key {
            Some(bytes) => Some(
                <[u8; 32]>::try_from(bytes.as_slice())
                    .map_err(|_| JsError::new("secret key must be exactly 32 bytes"))?,
            ),
            None => None,
        };
        let inner = node::Node::spawn(nickname, seeds, secret)
            .await
            .map_err(to_js_err)?;
        Ok(Self { inner })
    }

    /// A stream of JSON strings: status, matched, signal, chat, typing, ended
    /// and notice events. Read it once, from a single loop.
    pub fn events(&self) -> JsReadableStream {
        let events = self.inner.events.clone();
        let stream = ReadableStream::from_stream(async_stream(events));
        stream.into_raw()
    }

    #[wasm_bindgen(js_name = endpointId)]
    pub fn endpoint_id(&self) -> String {
        self.inner.core.endpoint_id()
    }

    pub fn ticket(&self) -> String {
        self.inner.core.ticket()
    }

    /// Re-emit the current status; handy right after the page attaches.
    #[wasm_bindgen(js_name = refreshStatus)]
    pub fn refresh_status(&self) {
        self.inner.core.emit_status();
    }

    pub fn start(&self) {
        self.inner.core.start_search();
    }

    pub async fn next(&self) {
        self.inner.core.next().await;
    }

    pub async fn stop(&self) {
        self.inner.core.stop().await;
    }

    /// Relay a locally produced SDP (`kind = "sdp"`) or ICE candidate
    /// (`kind = "ice"`) to the stranger. The payload is JSON text - exactly what
    /// `JSON.stringify` produced - so nothing is reshaped in transit.
    #[wasm_bindgen(js_name = sendSignal)]
    pub async fn send_signal(&self, kind: String, payload_json: String) -> Result<(), JsError> {
        let payload: serde_json::Value =
            serde_json::from_str(&payload_json).map_err(|err| JsError::new(&err.to_string()))?;
        self.inner.core.send_signal(&kind, payload).await;
        Ok(())
    }

    /// Text chat over iroh - the fallback when the WebRTC data channel is not
    /// open.
    #[wasm_bindgen(js_name = sendChat)]
    pub async fn send_chat(&self, text: String) {
        self.inner.core.send_chat(&text).await;
    }

    #[wasm_bindgen(js_name = sendTyping)]
    pub async fn send_typing(&self, on: bool) {
        self.inner.core.send_typing(on).await;
    }

    #[wasm_bindgen(js_name = setNickname)]
    pub fn set_nickname(&self, nickname: String) {
        self.inner.core.set_nickname(&nickname);
        self.inner.core.emit_status();
    }

    #[wasm_bindgen(js_name = addBootstrap)]
    pub fn add_bootstrap(&self, ticket: String) -> Result<String, JsError> {
        self.inner.core.add_bootstrap(&ticket).map_err(to_js_err)
    }

    /// Close the endpoint. The tab guard calls this when another tab appears.
    pub async fn shutdown(&self) {
        self.inner.core.shutdown().await;
    }
}

fn async_stream(
    events: async_channel::Receiver<Event>,
) -> impl n0_future::Stream<Item = Result<JsValue, JsValue>> {
    n0_future::stream::StreamExt::map(events, |event| {
        let json = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
        Ok(JsValue::from_str(&json))
    })
}

fn to_js_err(err: impl Into<anyhow::Error>) -> JsError {
    let err: anyhow::Error = err.into();
    JsError::new(&err.to_string())
}
