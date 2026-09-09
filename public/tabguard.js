/*
 * Single-tab enforcement.
 *
 * Domegle runs one iroh endpoint per browser profile: a second tab would bind a
 * second endpoint on the same identity, announce itself into the same swarm and
 * compete for the same strangers. So the rule is one tab, full stop.
 *
 * When a second tab appears, *every* tab stops the app and shows the same
 * "close one of these tabs" screen. Whichever tab survives detects that it is
 * alone again and starts the app itself - no reload needed.
 *
 * Detection is a BroadcastChannel heartbeat rather than a lock, because it has
 * to survive a tab that crashed without saying goodbye: peers that stop pinging
 * age out after PEER_TIMEOUT.
 */

const CHANNEL = 'domegle-tabs';
const PING_INTERVAL = 700;
const PEER_TIMEOUT = 2200;
const ELECTION_DELAY = 450;

export class TabGuard {
  /**
   * @param {(alone: boolean, otherTabs: number) => void} onChange called
   *   whenever this tab becomes the only one (true) or stops being alone.
   */
  constructor(onChange) {
    this.onChange = onChange;
    this.id = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    this.peers = new Map();
    this.alone = null;
    this.channel = null;
    this.timer = null;
  }

  start() {
    if (typeof BroadcastChannel === 'undefined') {
      // Nothing to coordinate with; treat this tab as the only one rather than
      // locking the user out of an app that would otherwise work.
      this.alone = true;
      this.onChange(true, 0);
      return;
    }

    this.channel = new BroadcastChannel(CHANNEL);
    this.channel.onmessage = (event) => this.receive(event.data);

    const leave = () => this.leave();
    window.addEventListener('pagehide', leave);
    window.addEventListener('beforeunload', leave);

    this.post({ t: 'hello' });
    this.timer = setInterval(() => this.tick(), PING_INTERVAL);

    // Give any existing tab a moment to answer before deciding we are alone.
    setTimeout(() => this.evaluate(), ELECTION_DELAY);
  }

  stop() {
    this.leave();
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.channel) this.channel.close();
    this.channel = null;
  }

  post(message) {
    if (!this.channel) return;
    try {
      this.channel.postMessage({ ...message, id: this.id });
    } catch (err) {
      /* the channel closes during unload; nothing to do */
    }
  }

  leave() {
    this.post({ t: 'bye' });
  }

  receive(message) {
    if (!message || message.id === this.id) return;
    switch (message.t) {
      case 'hello':
        // Answer immediately so the newcomer sees us before its election ends.
        this.peers.set(message.id, Date.now());
        this.post({ t: 'ping' });
        this.evaluate();
        break;
      case 'ping':
        this.peers.set(message.id, Date.now());
        this.evaluate();
        break;
      case 'bye':
        this.peers.delete(message.id);
        this.evaluate();
        break;
      default:
        break;
    }
  }

  tick() {
    this.post({ t: 'ping' });
    const cutoff = Date.now() - PEER_TIMEOUT;
    for (const [id, seen] of this.peers) {
      if (seen < cutoff) this.peers.delete(id);
    }
    this.evaluate();
  }

  evaluate() {
    const alone = this.peers.size === 0;
    if (alone === this.alone) return;
    this.alone = alone;
    this.onChange(alone, this.peers.size);
  }
}
