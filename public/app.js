/*
 * Domegle in the browser.
 *
 *   this tab (wasm iroh node)  --QUIC over a relay-->  stranger's node
 *                              \__ SDP + ICE + fallback chat __/
 *              media and low-latency chat: WebRTC, peer to peer
 *
 * The iroh endpoint lives inside the WebAssembly module in this very tab. There
 * is no signalling server: the handshake rides the same encrypted iroh stream
 * that discovery uses. Browsers cannot send UDP, so iroh traffic is relayed -
 * the relay cannot read it.
 *
 * Exactly one tab may run this. See tabguard.js.
 */

import init, { DomegleNode } from './wasm/domegle_wasm.js';
import { TabGuard } from './tabguard.js';

const el = (id) => document.getElementById(id);
const ui = {
  dot: document.querySelector('.dot'),
  backendTag: el('backendTag'),
  peerCount: el('peerCount'),
  nodeId: el('nodeId'),
  stage: el('stage'),
  blocker: el('blocker'),
  blockerStatus: el('blockerStatus'),
  remoteVideo: el('remoteVideo'),
  localVideo: el('localVideo'),
  overlay: el('remoteOverlay'),
  overlayTitle: el('overlayTitle'),
  overlaySub: el('overlaySub'),
  spinner: el('spinner'),
  connBadge: el('connBadge'),
  startBtn: el('startBtn'),
  nextBtn: el('nextBtn'),
  stopBtn: el('stopBtn'),
  micBtn: el('micBtn'),
  camBtn: el('camBtn'),
  messages: el('messages'),
  typing: el('typing'),
  composer: el('composer'),
  input: el('input'),
  sendBtn: el('sendBtn'),
  peerName: el('peerName'),
  peerId: el('peerId'),
  settings: el('settings'),
  settingsBtn: el('settingsBtn'),
  nickInput: el('nickInput'),
  ticketOut: el('ticketOut'),
  copyTicket: el('copyTicket'),
  seedInput: el('seedInput'),
  addSeed: el('addSeed'),
  log: el('log'),
};

// stun3.l.google.com:19302,
// stun4.l.google.com:19302,

const ICE_SERVERS = [
  { urls: ['stun:stun.l.google.com:19302'] },
  { urls: ['stun:stun1.l.google.com:19302'] },
  { urls: ['stun:stun3.l.google.com:19302'] },
  { urls: ['stun:stun4.l.google.com:19302'] },
  
];

const state = {
  node: null,
  reader: null,
  wasmReady: false,
  status: 'offline',
  peer: null,
  initiator: false,
  pc: null,
  dc: null,
  localStream: null,
  pendingIce: [],
  haveRemoteDescription: false,
  micOn: true,
  camOn: true,
  typingSentAt: 0,
  typingTimer: null,
};

const settings = {
  get nickname() {
    return localStorage.getItem('domegle.nick') || 'stranger';
  },
  set nickname(value) {
    localStorage.setItem('domegle.nick', value);
  },
  get seeds() {
    try {
      return JSON.parse(localStorage.getItem('domegle.seeds') || '[]');
    } catch (err) {
      return [];
    }
  },
  addSeed(ticket) {
    const seeds = this.seeds;
    if (!seeds.includes(ticket)) {
      seeds.push(ticket);
      localStorage.setItem('domegle.seeds', JSON.stringify(seeds));
    }
  },
  /**
   * 32-byte endpoint secret, created once and reused. Without it every reload
   * would join the swarm as a brand new peer and every shared ticket would go
   * stale.
   */
  get secretKey() {
    const stored = localStorage.getItem('domegle.secret');
    if (stored && /^[0-9a-f]{64}$/.test(stored)) {
      return Uint8Array.from(stored.match(/../g).map((byte) => parseInt(byte, 16)));
    }
    const fresh = crypto.getRandomValues(new Uint8Array(32));
    const hex = [...fresh].map((b) => b.toString(16).padStart(2, '0')).join('');
    localStorage.setItem('domegle.secret', hex);
    return fresh;
  },
};

/* --tab guard -- */

const guard = new TabGuard(async (alone, others) => {
  if (alone) {
    hideBlocker();
    await startNode();
  } else {
    // The rule is one node per browser, so every tab stands down - including
    // the one that was already running.
    showBlocker(others);
    await stopNode();
  }
});

function showBlocker(others) {
  ui.blocker.hidden = false;
  ui.stage.classList.add('hidden');
  ui.blockerStatus.textContent =
    others === 1
      ? 'Waiting for the other tab to close...'
      : `Waiting for ${others} other tabs to close...`;
}

function hideBlocker() {
  ui.blocker.hidden = true;
  ui.stage.classList.remove('hidden');
}

/* ----- node -- */

async function startNode() {
  if (state.node) return;
  try {
    if (!state.wasmReady) {
      await init();
      state.wasmReady = true;
    }
    setOverlay('Starting up', 'Binding an iroh endpoint in this tab.', true);
    state.node = await DomegleNode.spawn(settings.nickname, settings.seeds, settings.secretKey);
    pumpEvents(state.node);
    refreshControls();
  } catch (err) {
    appendLog(`node failed to start: ${err}`, 'warn');
    setOverlay('Could not start', String(err), false);
  }
}

async function stopNode() {
  teardownPeer();
  const node = state.node;
  state.node = null;
  state.status = 'offline';
  if (state.reader) {
    try {
      await state.reader.cancel();
    } catch (err) {
      /* the stream is already gone */
    }
    state.reader = null;
  }
  if (node) {
    try {
      await node.shutdown();
    } catch (err) {
      appendLog(`shutdown: ${err}`, 'warn');
    }
  }
  refreshControls();
}

async function pumpEvents(node) {
  const reader = node.events().getReader();
  state.reader = reader;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      let event;
      try {
        event = JSON.parse(value);
      } catch (err) {
        continue;
      }
      handleEvent(event);
    }
  } catch (err) {
    /* cancelled on shutdown */
  }
}

function handleEvent(event) {
  switch (event.type) {
    case 'status':
      applyStatus(event);
      break;
    case 'matched':
      onMatched(event);
      break;
    case 'signal':
      onSignal(event.kind, event.payload);
      break;
    case 'chat':
      addMessage(event.text, 'them', 'iroh');
      break;
    case 'typing':
      ui.typing.hidden = !event.on;
      break;
    case 'ended':
      onEnded(event.reason);
      break;
    case 'notice':
      appendLog(event.text);
      break;
    default:
      break;
  }
}

function applyStatus(event) {
  state.status = event.status;
  ui.peerCount.textContent = event.peers;
  ui.nodeId.textContent = (event.endpointId || '').slice(0, 8) || '--------';
  ui.ticketOut.value = event.ticket || '';
  if (document.activeElement !== ui.nickInput) ui.nickInput.value = event.nickname || '';
  setDot(event.status === 'matched' ? 'busy' : 'online');
  refreshControls();
}

/* media  */

async function ensureMedia() {
  if (state.localStream) return state.localStream;
  try {
    state.localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: { width: { ideal: 1280 }, height: { ideal: 720 } },
    });
  } catch (err) {
    systemMessage(`Camera/microphone unavailable (${err.name}). Continuing in text-only mode.`);
    state.localStream = null;
    return null;
  }
  ui.localVideo.srcObject = state.localStream;
  applyTrackToggles();
  return state.localStream;
}

function applyTrackToggles() {
  if (!state.localStream) return;
  state.localStream.getAudioTracks().forEach((track) => (track.enabled = state.micOn));
  state.localStream.getVideoTracks().forEach((track) => (track.enabled = state.camOn));
  
  // ui.micBtn.textContent = state.micOn ? 'Mic on' : 'Mic off';
  // ui.camBtn.textContent = state.camOn ? 'Camera on' : 'Camera off';

  ui.micBtn.innerHTML = state.micOn ? '<i class="bi bi-mic"></i> Mic on' : '<i class="bi bi-mic-mute"></i> Mic off';
  ui.camBtn.innerHTML = state.camOn ? '<i class="bi bi-camera-video"></i> Camera on' : '<i class="bi bi-camera-video-off"></i> Camera off';
  ui.micBtn.setAttribute('aria-pressed', String(state.micOn));
  ui.camBtn.setAttribute('aria-pressed', String(state.camOn));
}

/* -- webrtc --- */

function createPeerConnection() {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS, bundlePolicy: 'max-bundle' });
  state.pc = pc;
  state.pendingIce = [];
  state.haveRemoteDescription = false;

  pc.onicecandidate = (event) => {
    if (event.candidate) sendSignal('ice', event.candidate.toJSON());
  };

  pc.ontrack = (event) => {
    if (ui.remoteVideo.srcObject !== event.streams[0]) {
      ui.remoteVideo.srcObject = event.streams[0];
    }
    hideOverlay();
  };

  pc.onconnectionstatechange = () => {
    setBadge(pc.connectionState);
    if (pc.connectionState === 'connected') {
      hideOverlay();
    } else if (pc.connectionState === 'failed') {
      systemMessage('Media connection failed - text chat still runs over iroh.');
    }
  };

  pc.ondatachannel = (event) => attachDataChannel(event.channel);

  pc.onnegotiationneeded = async () => {
    if (!state.initiator) return;
    try {
      await pc.setLocalDescription(await pc.createOffer());
      sendSignal('sdp', pc.localDescription.toJSON());
    } catch (err) {
      systemMessage(`Could not create an offer: ${err.message}`);
    }
  };

  return pc;
}

function sendSignal(kind, payload) {
  if (!state.node) return;
  state.node.sendSignal(kind, JSON.stringify(payload)).catch((err) => {
    appendLog(`signal send failed: ${err}`, 'warn');
  });
}

function addLocalTracks(pc) {
  if (state.localStream) {
    state.localStream.getTracks().forEach((track) => pc.addTrack(track, state.localStream));
  } else if (state.initiator) {
    // No camera here, but we still want the stranger's media.
    pc.addTransceiver('video', { direction: 'recvonly' });
    pc.addTransceiver('audio', { direction: 'recvonly' });
  }
}

function attachDataChannel(channel) {
  state.dc = channel;
  // channel.onopen = () => setBadge('p2p data channel open');
  channel.onclose = () => {
    if (state.dc === channel) state.dc = null;
  };
  channel.onmessage = (event) => {
    let message;
    try {
      message = JSON.parse(event.data);
    } catch (err) {
      return;
    }
    if (message.t === 'chat' && typeof message.text === 'string') {
      addMessage(message.text.slice(0, 4000), 'them');
    } else if (message.t === 'typing') {
      ui.typing.hidden = !message.on;
    }
  };
}

async function onMatched(event) {
  state.peer = event.peerId;
  state.initiator = !!event.initiator;
  state.status = 'matched';
  clearChat();
  ui.peerName.textContent = event.peerNick || 'stranger';
  ui.peerId.textContent = (event.peerId || '').slice(0, 8);
  systemMessage(`You are now chatting with a stranger (${(event.peerId || '').slice(0, 8)}).`);
  setOverlay('Connecting...', 'Exchanging WebRTC details over iroh.', true);
  refreshControls();

  await ensureMedia();
  const pc = createPeerConnection();
  if (state.initiator) {
    attachDataChannel(pc.createDataChannel('chat', { ordered: true }));
  }
  addLocalTracks(pc);
}

async function onSignal(kind, payload) {
  const pc = state.pc;
  if (!pc || !payload) return;
  try {
    if (kind === 'sdp') {
      const description = new RTCSessionDescription(payload);
      await pc.setRemoteDescription(description);
      state.haveRemoteDescription = true;
      await flushIce();
      if (description.type === 'offer') {
        await pc.setLocalDescription(await pc.createAnswer());
        sendSignal('sdp', pc.localDescription.toJSON());
      }
    } else if (kind === 'ice') {
      const candidate = new RTCIceCandidate(payload);
      if (state.haveRemoteDescription) {
        await pc.addIceCandidate(candidate);
      } else {
        state.pendingIce.push(candidate);
      }
    }
  } catch (err) {
    systemMessage(`Signalling error: ${err.message}`);
  }
}

async function flushIce() {
  const queued = state.pendingIce.splice(0);
  for (const candidate of queued) {
    try {
      await state.pc.addIceCandidate(candidate);
    } catch (err) {
      /* a stale candidate is not fatal */
    }
  }
}

function teardownPeer() {
  if (state.dc) {
    try { state.dc.close(); } catch (err) { /* ignore */ }
    state.dc = null;
  }
  if (state.pc) {
    state.pc.onicecandidate = null;
    state.pc.ontrack = null;
    state.pc.ondatachannel = null;
    state.pc.onnegotiationneeded = null;
    try { state.pc.close(); } catch (err) { /* ignore */ }
    state.pc = null;
  }
  state.pendingIce = [];
  state.haveRemoteDescription = false;
  state.peer = null;
  ui.remoteVideo.srcObject = null;
  ui.typing.hidden = true;
  setBadge('');
}

function onEnded(reason) {
  teardownPeer();
  // The node sends a fresh status right behind this event; it decides whether
  // we go back to searching or to idle.
  if (state.status === 'matched') state.status = 'searching';
  ui.peerName.textContent = 'No stranger yet';
  ui.peerId.textContent = '';
  systemMessage(`Stranger disconnected (${reason}).`);
  refreshControls();
}

/*  ui --- */

function setDot(kind) {
  ui.dot.className = `dot ${kind}`.trim();
}

function setBadge(text) {
  ui.connBadge.textContent = text || '';
  ui.connBadge.hidden = !text;
}

function setOverlay(title, sub, spinning) {
  ui.overlayTitle.textContent = title;
  ui.overlaySub.textContent = sub;
  ui.spinner.hidden = !spinning;
  ui.overlay.hidden = false;
}

function hideOverlay() {
  ui.overlay.hidden = true;
}

function refreshControls() {
  const online = !!state.node;
  const matched = state.status === 'matched';
  const searching = state.status === 'searching';
  ui.startBtn.disabled = !online || matched || searching;
  ui.nextBtn.disabled = !matched;
  ui.stopBtn.disabled = !(matched || searching);
  ui.input.disabled = !matched;
  ui.sendBtn.disabled = !matched;
  if (!online) {
    setOverlay('Node stopped', 'Domegle is not running in this tab.', false);
  } else if (searching) {
    setOverlay('Looking for someone...', 'Scanning the swarm for a stranger.', true);
  } else if (!matched) {
    setOverlay('Not connected', 'Press Start to meet a stranger.', false);
  }
}

function addMessage(text, who, via) {
  const node = document.createElement('div');
  node.className = `msg ${who}`;
  node.textContent = text;
  if (via) {
    const tag = document.createElement('span');
    tag.className = 'via';
    tag.textContent = `via ${via}`;
    node.appendChild(tag);
  }
  ui.messages.appendChild(node);
  ui.messages.scrollTop = ui.messages.scrollHeight;
}

function systemMessage(text) {
  addMessage(text, 'sys');
}

function clearChat() {
  ui.messages.replaceChildren();
}

function appendLog(text, level) {
  const line = document.createElement('div');
  if (level && level !== 'info') line.className = level;
  line.textContent = text;
  ui.log.appendChild(line);
  while (ui.log.childElementCount > 200) ui.log.removeChild(ui.log.firstChild);
  ui.log.scrollTop = ui.log.scrollHeight;
}

/*  actions --- */

async function start() {
  if (!state.node) return;
  await ensureMedia();
  state.node.start();
  state.status = 'searching';
  refreshControls();
}

async function next() {
  if (!state.node || state.status !== 'matched') return;
  teardownPeer();
  clearChat();
  state.status = 'searching';
  refreshControls();
  await state.node.next();
}

async function stop() {
  if (!state.node) return;
  teardownPeer();
  state.status = 'idle';
  refreshControls();
  await state.node.stop();
}

function sendChat(text) {
  const payload = text.trim().slice(0, 2000);
  if (!payload || state.status !== 'matched') return;
  // Prefer the data channel; the iroh stream is already NAT-traversed, so text
  // keeps working even when WebRTC never connects.
  if (state.dc && state.dc.readyState === 'open') {
    state.dc.send(JSON.stringify({ t: 'chat', text: payload }));
    addMessage(payload, 'me');
  } else if (state.node) {
    state.node.sendChat(payload);
    addMessage(payload, 'me', 'iroh');
  }
}

function sendTyping(on) {
  if (state.status !== 'matched') return;
  if (state.dc && state.dc.readyState === 'open') {
    state.dc.send(JSON.stringify({ t: 'typing', on }));
  } else if (state.node) {
    state.node.sendTyping(on);
  }
}

/* --------------------------------------------------------------- wiring --- */

ui.startBtn.addEventListener('click', start);
ui.nextBtn.addEventListener('click', next);
ui.stopBtn.addEventListener('click', stop);

ui.micBtn.addEventListener('click', () => {
  state.micOn = !state.micOn;
  applyTrackToggles();
});
ui.camBtn.addEventListener('click', () => {
  state.camOn = !state.camOn;
  applyTrackToggles();
});

ui.composer.addEventListener('submit', (event) => {
  event.preventDefault();
  sendChat(ui.input.value);
  ui.input.value = '';
  sendTyping(false);
});

ui.input.addEventListener('input', () => {
  const now = Date.now();
  if (now - state.typingSentAt > 1500) {
    state.typingSentAt = now;
    sendTyping(true);
  }
  clearTimeout(state.typingTimer);
  state.typingTimer = setTimeout(() => {
    state.typingSentAt = 0;
    sendTyping(false);
  }, 2500);
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    event.preventDefault();
    if (ui.settings.open) ui.settings.close();
    else if (state.status === 'matched') next();
  }
});

ui.settingsBtn.addEventListener('click', () => ui.settings.showModal());
ui.copyTicket.addEventListener('click', async () => {
  ui.ticketOut.select();
  try {
    await navigator.clipboard.writeText(ui.ticketOut.value);
    appendLog('ticket copied to clipboard');
  } catch (err) {
    document.execCommand('copy');
  }
});
ui.addSeed.addEventListener('click', () => {
  const ticket = ui.seedInput.value.trim();
  if (!ticket || !state.node) return;
  try {
    const owner = state.node.addBootstrap(ticket);
    settings.addSeed(ticket);
    appendLog(`added seed ${owner.slice(0, 8)}`);
    ui.seedInput.value = '';
  } catch (err) {
    appendLog(String(err), 'warn');
  }
});
ui.nickInput.addEventListener('change', () => {
  const nickname = ui.nickInput.value.trim() || 'stranger';
  settings.nickname = nickname;
  if (state.node) state.node.setNickname(nickname);
});

guard.start();
refreshControls();
