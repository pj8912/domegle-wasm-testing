# Domegle for the browser (WebAssembly)

The whole node runs inside the tab. `iroh` is compiled to `wasm32-unknown-unknown`
with `wasm-bindgen`, so the page binds a real iroh endpoint, discovers strangers,
pairs with one, and relays the WebRTC handshake — with no signalling server
anywhere in the path.


```
 this tab                                             stranger
 ┌──────────────────────────┐                   ┌──────────────────────────┐
 │ page (WebRTC)            │                   │ page / app               │
 │   ↕ wasm-bindgen         │                   │                          │
 │ iroh endpoint (wasm) ────┼── QUIC over relay ┼──── iroh endpoint        │
 └──────────────────────────┘  SDP + ICE + chat └──────────────────────────┘
              ╰──────── media: WebRTC, peer to peer ────────╯
```

## One tab, enforced

A browser profile runs **one** node. A second tab would bind a second endpoint
on the same identity, announce itself into the same swarm and compete for the
same strangers, so the app refuses to run twice:

* open a second tab and **every** tab stops the running node shuts down, any
  chat ends, and both tabs show *"Domegle is open in another tab"*;
* close all but one and the survivor detects that it is alone and starts itself
  again. No reload, no manual step.

[`public/tabguard.js`](public/tabguard.js) does this with a `BroadcastChannel`
heartbeat rather than a lock, so a tab that crashed without saying goodbye ages
out after ~2 seconds instead of blocking the app forever.

## Build

Prerequisites (none of these were installed on this machine; all four are now):

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.122 --locked
winget install LLVM.LLVM     # clang; `ring` will not cross-compile without it
npm install                  # only for the dev server
```

`wasm-bindgen-cli` must match the `wasm-bindgen` version pinned in
`Cargo.toml` (`=0.2.122`) or the generated glue will not load.

```bash
npm run build      # debug
npm run serve      # http://127.0.0.1:7500
```

`npm run build:release` builds with `opt-level = "z"` + LTO into the same
`public/wasm/`. Install [binaryen](https://github.com/WebAssembly/binaryen) and
run `wasm-opt -Os` over the output if you want it smaller still.

Serve over `http://127.0.0.1` or HTTPS: `getUserMedia` needs a secure context.

## Constraints the browser imposes

Per [the iroh guide](https://docs.iroh.computer/languages/wasm-browser):

* **Every iroh connection is relayed.** A tab cannot send UDP, so there is no
  hole punching. Traffic stays end-to-end encrypted — the relay cannot read it —
  but expect relay latency on the signalling path. WebRTC media still goes
  peer-to-peer once ICE completes.
* **`default-features = false`** on the `iroh` dependency. The metrics feature
  does not build for wasm.
* **`getrandom` needs an explicit backend.** `.cargo/config.toml` sets
  `--cfg getrandom_backend="wasm_js"`; without it the build fails at link time.
* **`ring` needs clang** to cross-compile. On Windows that means installing LLVM
  separately — Visual Studio's `clang-format`/`clang-tidy` are not enough.

## Layout

| path | what |
| --- | --- |
| `src/proto.rs` | newline-JSON framing + the message vocabulary |
| `src/node.rs` | discovery, matchmaking, the paired session |
| `src/wasm.rs` | the `DomegleNode` class JavaScript sees |
| `public/app.js` | WebRTC, UI, and the glue to the wasm node |
| `public/tabguard.js` | single-tab enforcement |
| `public/index.html`, `public/styles.css` | the page |

The node keeps its 32-byte endpoint secret in `localStorage`, so the tab's
endpoint id and ticket survive reloads, matching the desktop and Android nodes.
Clearing site data gives you a new identity.



## Notes and limits

* No TURN server is configured; add one to `ICE_SERVERS` in `public/app.js` if
  you need media across symmetric NATs. Text always works regardless — it falls
  back to the iroh stream, which is already relayed.
* The `.wasm` is 17 MB in debug and **3.0 MB** in release (measured, before
  `wasm-opt` or transport compression). Serve the release build gzipped or
  brotli-compressed; it is still a heavy first paint.
* No moderation or reporting, and peers learn each other's IP addresses once
  WebRTC connects directly — the same exposure any p2p video chat has.
