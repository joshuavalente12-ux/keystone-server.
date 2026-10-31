/* global io */
/**
 * GameNetwork — browser networking client for server.js
 *
 * Load order in your page:
 *   <script src="/socket.io/socket.io.js"></script>   (served automatically by the server)
 *   <script src="/client.js"></script>
 *
 * Usage:
 *   const net = new GameNetwork();
 *   net.on('ready', ({ self }) => { myPlayer.x = self.x; myPlayer.y = self.y; });
 *   net.on('correction', ({ x, y }) => { myPlayer.x = x; myPlayer.y = y; });
 *   await net.connect('Josh');
 *   // every frame:
 *   net.sendUpdate(myPlayer.x, myPlayer.y, myPlayer.dir, myPlayer.anim);
 *   for (const p of net.getRemotePlayers()) draw(p);
 *
 * Spectator (watch-only, e.g. the /monitor page):
 *   const net = new GameNetwork({ spectator: true });
 *
 * Events: ready, player_joined, player_left, chat, correction, batch,
 *         disconnected, reconnecting, connect_error, server_full
 */
(function (global) {
  'use strict';

  const OP = Object.freeze({ UPDATE: 0, JOIN: 1, LEAVE: 2, CHAT: 3 });
  const SNAPSHOT_HISTORY_MS = 1000;

  function lerp(a, b, t) {
    return a + (b - a) * t;
  }

  function lerpAngle(a, b, t) {
    const diff = ((b - a + 540) % 360) - 180; // shortest path, in degrees
    return (a + diff * t + 360) % 360;
  }

  function round1(value) {
    return Math.round(value * 10) / 10;
  }

  class GameNetwork {
    /**
     * @param {object} [options]
     * @param {string} [options.serverUrl]            Defaults to the page's own origin.
     * @param {number} [options.sendRateHz=15]        Outgoing update rate (server allows 20).
     * @param {number} [options.interpolationDelayMs] Render remote players this far in the past.
     */
    constructor(options = {}) {
      if (typeof io !== 'function') {
        throw new Error('Socket.IO client not loaded. Include /socket.io/socket.io.js before client.js.');
      }
      this.serverUrl = options.serverUrl || undefined;
      this.sendRateHz = options.sendRateHz || 15;
      this.interpolationDelayMs = options.interpolationDelayMs || null; // set from tickMs on init
      this.spectator = Boolean(options.spectator); // watch only: no player is created on the server

      this.socket = null;
      this.selfId = null;
      this.selfName = '';
      this.world = null;
      this.tickMs = 100;
      this.maxSpeed = 0;
      this.players = new Map(); // remote players only

      this._handlers = new Map();
      this._clockOffset = 0;
      this._outgoing = null;
      this._lastSentKey = '';
      this._sendTimer = null;
    }

    // ------------------------------------------------------------------ events
    on(eventName, handler) {
      if (!this._handlers.has(eventName)) this._handlers.set(eventName, new Set());
      this._handlers.get(eventName).add(handler);
      return () => this._handlers.get(eventName).delete(handler);
    }

    _fire(eventName, payload) {
      const handlers = this._handlers.get(eventName);
      if (!handlers) return;
      for (const handler of handlers) {
        try {
          handler(payload);
        } catch (err) {
          console.error(`[GameNetwork] "${eventName}" handler threw:`, err);
        }
      }
    }

    // -------------------------------------------------------------- connection
    get connected() {
      return Boolean(this.socket && this.socket.connected);
    }

    /** Resolves with your player id once the server sends the initial snapshot. */
    connect(name) {
      if (this.socket) return Promise.reject(new Error('Already connected or connecting.'));

      return new Promise((resolve, reject) => {
        const socket = io(this.serverUrl, {
          transports: ['websocket'],
          auth: { name: String(name || ''), spectator: this.spectator },
          reconnection: true,
          reconnectionDelay: 1000,
          reconnectionDelayMax: 5000,
        });
        this.socket = socket;
        let settled = false;

        const fail = (err) => {
          if (settled) return;
          settled = true;
          socket.close();
          this.socket = null;
          reject(err);
        };

        socket.on('init', (data) => {
          this._handleInit(data);
          if (!settled) {
            settled = true;
            resolve(this.selfId);
          }
        });

        socket.on('server_update_batch', (batch) => this._handleBatch(batch));

        socket.on('correction', (pos) => {
          if (Array.isArray(pos)) this._fire('correction', { x: pos[0], y: pos[1] });
        });

        socket.on('server_full', (info) => {
          this._fire('server_full', info);
          fail(new Error('Server is full.'));
        });

        socket.on('connect_error', (err) => {
          this._fire('connect_error', err);
          fail(err);
        });

        socket.on('disconnect', (reason) => {
          this._stopSendLoop();
          this._fire('disconnected', reason);
        });

        socket.io.on('reconnect_attempt', (attempt) => this._fire('reconnecting', attempt));
      });
    }

    disconnect() {
      this._stopSendLoop();
      if (this.socket) {
        this.socket.disconnect();
        this.socket = null;
      }
      this.players.clear();
      this.selfId = null;
    }

    // ---------------------------------------------------------------- outgoing
    /** Call as often as you like (e.g. every frame); it is sent at sendRateHz, only when changed. */
    sendUpdate(x, y, dir = 0, anim = 0) {
      this._outgoing = [round1(x), round1(y), Math.round(dir) % 360, anim | 0];
    }

    sendChat(text) {
      if (!this.connected) return;
      const clean = String(text || '').trim().slice(0, 120);
      if (clean) this.socket.emit('chat', clean);
    }

    _startSendLoop() {
      this._stopSendLoop();
      this._lastSentKey = '';
      this._sendTimer = setInterval(() => {
        if (!this._outgoing || !this.connected) return;
        const key = this._outgoing.join(',');
        if (key === this._lastSentKey) return;
        this._lastSentKey = key;
        this.socket.emit('player_update', this._outgoing);
      }, 1000 / this.sendRateHz);
    }

    _stopSendLoop() {
      if (this._sendTimer) clearInterval(this._sendTimer);
      this._sendTimer = null;
    }

    // ---------------------------------------------------------------- incoming
    serverNow() {
      return Date.now() + this._clockOffset;
    }

    _handleInit(data) {
      this.selfId = data.selfId;
      this.world = data.world;
      this.tickMs = data.tickMs || 100;
      this.maxSpeed = data.maxSpeed || 0;
      if (!this.interpolationDelayMs) this.interpolationDelayMs = this.tickMs * 2;
      this._clockOffset = data.serverTime - Date.now();
      this.players.clear();

      let self = null;
      for (const tuple of data.players) {
        if (tuple[0] === this.selfId) {
          self = { id: tuple[0], name: tuple[1], x: tuple[2], y: tuple[3], dir: tuple[4], anim: tuple[5], color: tuple[6] };
          this.selfName = tuple[1];
        } else {
          this._addOrRefreshPlayer(tuple, data.serverTime);
        }
      }

      this._outgoing = null;
      this._startSendLoop();
      this._fire('ready', {
        selfId: this.selfId,
        self,
        world: this.world,
        players: Array.from(this.players.values(), (p) => this._view(p)),
      });
    }

    /** Unpacks one 'server_update_batch' payload: { t, e: [entry, ...] }. */
    _handleBatch(batch) {
      if (!batch || !Array.isArray(batch.e)) return;
      const t = typeof batch.t === 'number' ? batch.t : this.serverNow();

      // Smooth clock offset so jitter doesn't make interpolation stutter.
      this._clockOffset = this._clockOffset * 0.9 + (t - Date.now()) * 0.1;

      for (const entry of batch.e) {
        if (!Array.isArray(entry)) continue;
        const id = entry[1];

        switch (entry[0]) {
          case OP.UPDATE: {
            if (id === this.selfId) break; // local player is client-authoritative/predicted
            const p = this.players.get(id);
            if (p) this._pushSnapshot(p, t, entry[2], entry[3], entry[4], entry[5]);
            break;
          }
          case OP.JOIN: {
            if (id === this.selfId) break;
            const isNew = !this.players.has(id);
            const p = this._addOrRefreshPlayer(entry.slice(1), t);
            if (isNew) this._fire('player_joined', this._view(p));
            break;
          }
          case OP.LEAVE: {
            const p = this.players.get(id);
            if (p) {
              this.players.delete(id);
              this._fire('player_left', this._view(p));
            }
            break;
          }
          case OP.CHAT: {
            const isSelf = id === this.selfId;
            const sender = isSelf ? null : this.players.get(id);
            this._fire('chat', {
              id,
              isSelf,
              name: isSelf ? this.selfName : sender ? sender.name : 'Unknown',
              text: String(entry[2]),
            });
            break;
          }
          default:
            break;
        }
      }

      this._fire('batch', { serverTime: t, entries: batch.e.length });
    }

    _addOrRefreshPlayer(tuple, t) {
      const [id, name, x, y, dir, anim, color] = tuple;
      let p = this.players.get(id);
      if (p) {
        p.name = name;
        p.color = color;
        return p;
      }
      p = { id, name, color, x, y, dir, anim, buffer: [{ t, x, y, dir, anim }] };
      this.players.set(id, p);
      return p;
    }

    _pushSnapshot(p, t, x, y, dir, anim) {
      const buf = p.buffer;
      const last = buf[buf.length - 1];
      // Player was idle: re-anchor so movement doesn't get smeared across the idle gap.
      if (last && t - last.t > this.tickMs * 2) {
        buf.push({ t: t - this.tickMs, x: last.x, y: last.y, dir: last.dir, anim: last.anim });
      }
      buf.push({ t, x, y, dir, anim });
      const cutoff = t - SNAPSHOT_HISTORY_MS;
      while (buf.length > 2 && buf[0].t < cutoff) buf.shift();
    }

    /** Smoothly interpolated remote players. Call once per render frame. */
    getRemotePlayers() {
      const renderTime = this.serverNow() - this.interpolationDelayMs;
      const out = [];

      for (const p of this.players.values()) {
        const buf = p.buffer;
        const first = buf[0];
        const last = buf[buf.length - 1];

        if (renderTime >= last.t) {
          p.x = last.x; p.y = last.y; p.dir = last.dir; p.anim = last.anim;
        } else if (renderTime <= first.t) {
          p.x = first.x; p.y = first.y; p.dir = first.dir; p.anim = first.anim;
        } else {
          for (let i = buf.length - 1; i > 0; i--) {
            const a = buf[i - 1];
            const b = buf[i];
            if (a.t <= renderTime && renderTime <= b.t) {
              const span = b.t - a.t || 1;
              const k = (renderTime - a.t) / span;
              p.x = lerp(a.x, b.x, k);
              p.y = lerp(a.y, b.y, k);
              p.dir = lerpAngle(a.dir, b.dir, k);
              p.anim = b.anim;
              break;
            }
          }
        }
        out.push(p);
      }
      return out;
    }

    _view(p) {
      return { id: p.id, name: p.name, color: p.color, x: p.x, y: p.y, dir: p.dir, anim: p.anim };
    }
  }

  global.GameNetwork = GameNetwork;
  if (typeof module !== 'undefined' && module.exports) module.exports = GameNetwork;
})(typeof window !== 'undefined' ? window : globalThis);
