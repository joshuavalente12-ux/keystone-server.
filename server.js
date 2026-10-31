'use strict';

/**
 * Single-room multiplayer server.
 *
 * Design:
 *  - Clients send compact position updates: ['player_update', [x, y, dir, anim]].
 *  - The server keeps only the LATEST state per player (a "dirty set"), so a client
 *    that sends 20 updates in one tick costs one entry in the next broadcast.
 *  - Join / leave / chat events go into an ordered pending queue.
 *  - Every TICK_MS (default 100ms) everything is flushed in ONE global emit:
 *        'server_update_batch' -> { t: serverTimeMs, e: [ entry, entry, ... ] }
 *    Socket.IO encodes a broadcast once and writes the same frame to every socket,
 *    so cost per tick is one JSON encode + N socket writes.
 *  - perMessageDeflate and HTTP compression are disabled to save CPU.
 *
 * Batch entry formats (arrays, to keep packets small):
 *    [0, id, x, y, dir, anim]                 UPDATE
 *    [1, id, name, x, y, dir, anim, color]    JOIN
 *    [2, id]                                  LEAVE
 *    [3, id, text]                            CHAT
 *
 * Live monitor: open http://localhost:3000/monitor
 *
 * Settings can be passed as command-line flags (works on Windows, Mac and Linux)
 * or environment variables:
 *    node server.js --port=3000 --tick=100 --max-players=60
 */

const http = require('node:http');
const path = require('node:path');
const { monitorEventLoopDelay } = require('node:perf_hooks');
const express = require('express');
const { Server } = require('socket.io');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const ARGS = Object.fromEntries(
  process.argv
    .slice(2)
    .filter((arg) => arg.startsWith('--'))
    .map((arg) => {
      const [key, value = 'true'] = arg.slice(2).split('=');
      return [key, value];
    }),
);

function setting(argName, envName, fallback) {
  const raw = ARGS[argName] !== undefined ? ARGS[argName] : process.env[envName];
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

const CONFIG = Object.freeze({
  port: setting('port', 'PORT', 3000),
  host: ARGS.host || process.env.HOST || '0.0.0.0',
  tickMs: setting('tick', 'TICK_MS', 100),
  maxPlayers: setting('max-players', 'MAX_PLAYERS', 60),
  maxSpectators: setting('max-spectators', 'MAX_SPECTATORS', 20),
  corsOrigin: process.env.CORS_ORIGIN || '*',
  worldWidth: setting('world-width', 'WORLD_WIDTH', 4000),
  worldHeight: setting('world-height', 'WORLD_HEIGHT', 4000),
  maxSpeed: setting('max-speed', 'MAX_SPEED', 600), // world units per second (anti-teleport)
  maxUpdatesPerSec: setting('max-updates', 'MAX_UPDATES_PER_SEC', 20),
  chatLimit: 5, // messages...
  chatWindowMs: 5000, // ...per this window
  maxNameLength: 16,
  maxChatLength: 120,
  correctionCooldownMs: 500,
});

const OP = Object.freeze({ UPDATE: 0, JOIN: 1, LEAVE: 2, CHAT: 3 });

// Bytes Socket.IO + WebSocket add around each batch: '42["server_update_batch",' ... ']' plus frame header.
const FRAME_OVERHEAD_BYTES = 30;

const COLORS = [
  '#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4',
  '#46f0f0', '#f032e6', '#bcf60c', '#fabebe', '#008080', '#e6beff',
  '#9a6324', '#fffac8', '#aaffc3', '#ffd8b1',
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const players = new Map(); // numeric id -> player object
const dirtyPlayers = new Set(); // players whose state changed since last flush
let pendingEvents = []; // ordered JOIN / LEAVE / CHAT entries
let nextPlayerId = 1;
let spectatorCount = 0;

const stats = {
  startedAt: Date.now(),
  batchesSent: 0,
  lastBatchEntries: 0,
  lastBatchBytes: 0,
  lastFlushMs: 0,
  peakPlayers: 0,
  rejectedMoves: 0,
  rateLimitedUpdates: 0,
};

// Per-second counters, rolled into `live` once a second.
const counters = { outBytes: 0, batches: 0, updatesIn: 0, entries: 0 };
const live = {
  uploadMbps: 0,
  perClientKBps: 0,
  batchesPerSec: 0,
  updatesInPerSec: 0,
  avgBatchEntries: 0,
  cpuPercent: 0,
  eventLoopLagMs: 0,
  eventLoopLagP99Ms: 0,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function clamp(value, min, max) {
  return value < min ? min : value > max ? max : value;
}

function round1(value) {
  return Math.round(value * 10) / 10;
}

function round2(value) {
  return Math.round(value * 100) / 100;
}

function sanitizeText(input, maxLength) {
  if (typeof input !== 'string') return '';
  return input
    .replace(/[\u0000-\u001F\u007F<>]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, maxLength);
}

function joinTuple(player) {
  return [player.id, player.name, player.x, player.y, player.dir, player.anim, player.color];
}

function snapshotAll() {
  const snapshot = [];
  for (const p of players.values()) snapshot.push(joinTuple(p));
  return snapshot;
}

function normalizeDir(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return ((Math.round(n) % 360) + 360) % 360;
}

function normalizeAnim(value) {
  const n = Number(value);
  return Number.isInteger(n) && n >= 0 && n <= 255 ? n : 0;
}

// ---------------------------------------------------------------------------
// HTTP + Socket.IO setup
// ---------------------------------------------------------------------------
const app = express();
app.disable('x-powered-by');
// Web files live next to server.js (flat layout, easy to upload from a phone).
// Only these files are served, so server.js and package.json stay private.
const PAGES = {
  '/': 'index.html',
  '/index.html': 'index.html',
  '/client.js': 'client.js',
  '/monitor': 'monitor.html',
  '/monitor.html': 'monitor.html',
};
for (const [route, file] of Object.entries(PAGES)) {
  app.get(route, (_req, res) => {
    res.set('Cache-Control', 'no-cache');
    res.sendFile(path.join(__dirname, file));
  });
}

app.get('/health', (_req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json({
    status: 'ok',
    players: players.size,
    spectators: spectatorCount,
    connections: io.engine.clientsCount,
    peakPlayers: stats.peakPlayers,
    maxPlayers: CONFIG.maxPlayers,
    tickMs: CONFIG.tickMs,
    batchesSent: stats.batchesSent,
    lastBatchEntries: stats.lastBatchEntries,
    lastBatchBytes: stats.lastBatchBytes,
    lastFlushMs: round2(stats.lastFlushMs),
    rejectedMoves: stats.rejectedMoves,
    rateLimitedUpdates: stats.rateLimitedUpdates,
    uptimeSec: Math.round((Date.now() - stats.startedAt) / 1000),
    memoryMB: Math.round(process.memoryUsage().rss / 1048576),
    live,
  });
});

const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  perMessageDeflate: false, // no per-frame compression: saves a lot of CPU at 500+ sockets
  httpCompression: false,
  transports: ['websocket'], // skip long-polling; far cheaper per connection
  maxHttpBufferSize: 10_000, // largest message a client may send (bytes)
  pingInterval: 25_000,
  pingTimeout: 20_000,
  connectTimeout: 10_000,
  cors: { origin: CONFIG.corsOrigin },
});

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------
function sendInit(socket, selfId) {
  socket.emit('init', {
    selfId,
    serverTime: Date.now(),
    tickMs: CONFIG.tickMs,
    maxSpeed: CONFIG.maxSpeed,
    world: { width: CONFIG.worldWidth, height: CONFIG.worldHeight },
    players: snapshotAll(),
  });
}

io.on('connection', (socket) => {
  const auth = socket.handshake.auth || {};

  // Spectators (the monitor page) receive every batch but never become a player.
  if (auth.spectator === true) {
    if (spectatorCount >= CONFIG.maxSpectators) {
      socket.emit('server_full', { maxSpectators: CONFIG.maxSpectators });
      socket.disconnect(true);
      return;
    }
    spectatorCount++;
    sendInit(socket, 0);
    socket.on('disconnect', () => {
      spectatorCount--;
    });
    return;
  }

  if (players.size >= CONFIG.maxPlayers) {
    socket.emit('server_full', { maxPlayers: CONFIG.maxPlayers });
    socket.disconnect(true);
    return;
  }

  const now = Date.now();
  const id = nextPlayerId++;
  const moveBudgetCap = CONFIG.maxSpeed * 0.5;

  const player = {
    id,
    name: sanitizeText(auth.name, CONFIG.maxNameLength) || `Player${id}`,
    color: COLORS[id % COLORS.length],
    x: round1(CONFIG.worldWidth / 2 + (Math.random() - 0.5) * 400),
    y: round1(CONFIG.worldHeight / 2 + (Math.random() - 0.5) * 400),
    dir: 0,
    anim: 0,
    // Rate limiting (token bucket)
    tokens: CONFIG.maxUpdatesPerSec,
    lastRefill: now,
    // Anti-teleport (movement budget refills at slightly above max speed)
    moveBudget: moveBudgetCap,
    lastBudgetAt: now,
    lastCorrectionAt: 0,
    chatTimes: [],
  };

  players.set(id, player);
  if (players.size > stats.peakPlayers) stats.peakPlayers = players.size;

  // Full snapshot for the newcomer only; everyone else learns about them next tick.
  sendInit(socket, id);
  pendingEvents.push([OP.JOIN, ...joinTuple(player)]);

  socket.on('player_update', (data) => {
    if (!Array.isArray(data) || data.length < 4) return;
    const t = Date.now();

    // Token bucket: max N updates per second per player.
    player.tokens = Math.min(
      CONFIG.maxUpdatesPerSec,
      player.tokens + ((t - player.lastRefill) * CONFIG.maxUpdatesPerSec) / 1000,
    );
    player.lastRefill = t;
    if (player.tokens < 1) {
      stats.rateLimitedUpdates++;
      return;
    }
    player.tokens -= 1;

    const rawX = Number(data[0]);
    const rawY = Number(data[1]);
    if (!Number.isFinite(rawX) || !Number.isFinite(rawY)) return;

    const x = clamp(rawX, 0, CONFIG.worldWidth);
    const y = clamp(rawY, 0, CONFIG.worldHeight);

    // Movement budget: tolerant of packets arriving in bursts, but blocks teleports.
    player.moveBudget = Math.min(
      moveBudgetCap,
      player.moveBudget + (CONFIG.maxSpeed * 1.25 * (t - player.lastBudgetAt)) / 1000,
    );
    player.lastBudgetAt = t;

    const distance = Math.hypot(x - player.x, y - player.y);
    if (distance > player.moveBudget + 5) {
      stats.rejectedMoves++;
      if (t - player.lastCorrectionAt >= CONFIG.correctionCooldownMs) {
        player.lastCorrectionAt = t;
        socket.emit('correction', [player.x, player.y]);
      }
      return;
    }
    player.moveBudget -= distance;

    player.x = round1(x);
    player.y = round1(y);
    player.dir = normalizeDir(data[2], player.dir);
    player.anim = normalizeAnim(data[3]);
    dirtyPlayers.add(player);
    counters.updatesIn++;
  });

  socket.on('chat', (text) => {
    const t = Date.now();
    player.chatTimes = player.chatTimes.filter((ts) => t - ts < CONFIG.chatWindowMs);
    if (player.chatTimes.length >= CONFIG.chatLimit) return;

    const clean = sanitizeText(text, CONFIG.maxChatLength);
    if (!clean) return;

    player.chatTimes.push(t);
    pendingEvents.push([OP.CHAT, id, clean]);
  });

  socket.on('disconnect', () => {
    players.delete(id);
    dirtyPlayers.delete(player);
    pendingEvents.push([OP.LEAVE, id]);
  });

  socket.on('error', (err) => {
    console.error(`[socket ${id}] error:`, err && err.message ? err.message : err);
  });
});

// ---------------------------------------------------------------------------
// Throttled broadcast loop
// ---------------------------------------------------------------------------
function flushBroadcastQueue() {
  if (pendingEvents.length === 0 && dirtyPlayers.size === 0) return;
  const started = process.hrtime.bigint();

  // Ordered events first (joins before that player's updates), then latest positions.
  const entries = pendingEvents;
  pendingEvents = [];

  for (const p of dirtyPlayers) {
    entries.push([OP.UPDATE, p.id, p.x, p.y, p.dir, p.anim]);
  }
  dirtyPlayers.clear();

  const payload = { t: Date.now(), e: entries };
  io.emit('server_update_batch', payload);
  stats.lastFlushMs = Number(process.hrtime.bigint() - started) / 1e6;

  // Measure the real outgoing size (for the monitor), outside the timed section.
  const frameBytes = Buffer.byteLength(JSON.stringify(payload)) + FRAME_OVERHEAD_BYTES;
  counters.outBytes += frameBytes * io.engine.clientsCount;
  counters.batches++;
  counters.entries += entries.length;

  stats.batchesSent++;
  stats.lastBatchEntries = entries.length;
  stats.lastBatchBytes = frameBytes;
}

const tickTimer = setInterval(() => {
  try {
    flushBroadcastQueue();
  } catch (err) {
    console.error('[tick] broadcast failed:', err);
  }
}, CONFIG.tickMs);

// ---------------------------------------------------------------------------
// Live stats (once per second)
// ---------------------------------------------------------------------------
const loopDelay = monitorEventLoopDelay({ resolution: 10 });
loopDelay.enable();
let lastCpu = process.cpuUsage();
let lastStatsAt = Date.now();

const statsTimer = setInterval(() => {
  const now = Date.now();
  const elapsedSec = Math.max((now - lastStatsAt) / 1000, 0.001);
  lastStatsAt = now;

  const cpu = process.cpuUsage(lastCpu);
  lastCpu = process.cpuUsage();

  const clients = io.engine.clientsCount;
  live.uploadMbps = round2((counters.outBytes * 8) / 1e6 / elapsedSec);
  live.perClientKBps = clients ? round2(counters.outBytes / clients / 1024 / elapsedSec) : 0;
  live.batchesPerSec = round1(counters.batches / elapsedSec);
  live.updatesInPerSec = Math.round(counters.updatesIn / elapsedSec);
  live.avgBatchEntries = counters.batches ? Math.round(counters.entries / counters.batches) : 0;
  live.cpuPercent = round1(((cpu.user + cpu.system) / 1000 / (elapsedSec * 1000)) * 100); // % of one core

  const mean = loopDelay.mean / 1e6;
  const p99 = loopDelay.percentile(99) / 1e6;
  live.eventLoopLagMs = Number.isFinite(mean) ? round2(mean) : 0;
  live.eventLoopLagP99Ms = Number.isFinite(p99) ? round2(p99) : 0;
  loopDelay.reset();

  counters.outBytes = 0;
  counters.batches = 0;
  counters.updatesIn = 0;
  counters.entries = 0;
}, 1000);

// ---------------------------------------------------------------------------
// Startup / shutdown
// ---------------------------------------------------------------------------
httpServer.listen(CONFIG.port, CONFIG.host, () => {
  console.log(
    `Server listening on http://localhost:${CONFIG.port} ` +
      `(tick ${CONFIG.tickMs}ms, max ${CONFIG.maxPlayers} players)`,
  );
  console.log(`Live monitor:  http://localhost:${CONFIG.port}/monitor`);
});

let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${signal} received, shutting down...`);
  clearInterval(tickTimer);
  clearInterval(statsTimer);
  setTimeout(() => process.exit(1), 5000).unref();
  io.close(() => process.exit(0)); // also closes the HTTP server
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', (reason) => console.error('Unhandled rejection:', reason));
