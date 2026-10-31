'use strict';

/**
 * Bot runner / load tester: connects N bots that walk around the world.
 *
 *   npm run demo                      -> starts the server AND 60 bots, runs until Ctrl+C
 *   npm run loadtest                  -> 60 bots against an already-running server, 60s
 *   node loadtest.js --bots=300 --forever
 *   node loadtest.js --bots=200 --with-server --forever --tick=200
 *
 * Flags: --bots=N  --ramp=N (bots/sec)  --send-hz=N  --duration=SEC  --forever
 *        --url=http://host:port  --with-server  --tick=MS (passed to the server)
 *
 * Watch it live at http://localhost:3000/monitor
 *
 * Note: all bots run in ONE Node process and each decodes every batch, so on very
 * large tests this process can become the bottleneck. Split big tests across
 * terminals or machines (e.g. 2 x --bots=250).
 */

const path = require('node:path');
const { fork } = require('node:child_process');
const { io } = require('socket.io-client');

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

const URL = ARGS.url || process.env.URL || 'http://localhost:3000';
const BOTS = setting('bots', 'BOTS', 60);
const RAMP_PER_SEC = setting('ramp', 'RAMP', 50);
const SEND_HZ = setting('send-hz', 'SEND_HZ', 10);
const FOREVER = ARGS.forever === 'true' || ARGS.duration === '0' || process.env.DURATION_SEC === '0';
const DURATION_SEC = setting('duration', 'DURATION_SEC', 60);
const WITH_SERVER = ARGS['with-server'] === 'true';
const BOT_SPEED = 200; // world units per second (server allows 600)
const REPORT_SEC = 5;

const bots = [];
const totals = { connected: 0, failed: 0, batches: 0, entries: 0 };
let serverProcess = null;
let rampTimer = null;
let reportTimer = null;

// ---------------------------------------------------------------------------
// Optional: start the server as a separate process (so bot CPU doesn't skew its stats)
// ---------------------------------------------------------------------------
function startServer() {
  const serverArgs = [];
  if (ARGS.tick) serverArgs.push(`--tick=${ARGS.tick}`);
  if (ARGS['max-players']) serverArgs.push(`--max-players=${ARGS['max-players']}`);
  const port = new globalThis.URL(URL).port;
  if (port) serverArgs.push(`--port=${port}`);

  serverProcess = fork(path.join(__dirname, 'server.js'), serverArgs, { stdio: 'inherit' });
  serverProcess.on('exit', (code) => {
    if (!stopping) {
      console.error(`Server exited unexpectedly (code ${code}).`);
      stop(1);
    }
  });
}

async function waitForServer(timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${URL}/health`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`Server did not respond at ${URL}/health within ${timeoutMs / 1000}s`);
}

// ---------------------------------------------------------------------------
// Bots
// ---------------------------------------------------------------------------
function startBot(index) {
  const socket = io(URL, {
    transports: ['websocket'],
    auth: { name: `bot${index}` },
    reconnection: false,
    forceNew: true,
  });

  const bot = { socket, x: 0, y: 0, heading: Math.random() * Math.PI * 2, world: null, timer: null };
  bots.push(bot);

  socket.on('init', (data) => {
    totals.connected++;
    const self = data.players.find((p) => p[0] === data.selfId);
    bot.x = self[2];
    bot.y = self[3];
    bot.world = data.world;

    const dt = 1 / SEND_HZ;
    bot.timer = setInterval(() => {
      // Wander: occasionally turn, bounce off the world edges.
      if (Math.random() < 0.05) bot.heading += (Math.random() - 0.5) * Math.PI;
      bot.x += Math.cos(bot.heading) * BOT_SPEED * dt;
      bot.y += Math.sin(bot.heading) * BOT_SPEED * dt;
      if (bot.x <= 0 || bot.x >= bot.world.width) bot.heading = Math.PI - bot.heading;
      if (bot.y <= 0 || bot.y >= bot.world.height) bot.heading = -bot.heading;
      bot.x = Math.max(0, Math.min(bot.world.width, bot.x));
      bot.y = Math.max(0, Math.min(bot.world.height, bot.y));

      const dir = ((Math.round((bot.heading * 180) / Math.PI) % 360) + 360) % 360;
      socket.emit('player_update', [Math.round(bot.x * 10) / 10, Math.round(bot.y * 10) / 10, dir, 1]);
    }, 1000 / SEND_HZ);
  });

  socket.on('server_update_batch', (batch) => {
    totals.batches++;
    totals.entries += batch.e.length;
  });

  socket.on('correction', (pos) => {
    bot.x = pos[0];
    bot.y = pos[1];
  });

  socket.on('connect_error', () => totals.failed++);
  socket.on('server_full', () => totals.failed++);
  socket.on('disconnect', () => clearInterval(bot.timer));
}

function startBots() {
  let started = 0;
  rampTimer = setInterval(() => {
    const perTick = Math.max(1, Math.round(RAMP_PER_SEC / 10));
    for (let i = 0; i < perTick && started < BOTS; i++) startBot(started++);
    if (started >= BOTS) clearInterval(rampTimer);
  }, 100);
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------
function startReporting() {
  reportTimer = setInterval(async () => {
    let line = `bots ${totals.connected}/${BOTS} (failed ${totals.failed})`;
    try {
      const res = await fetch(`${URL}/health`);
      const h = await res.json();
      line +=
        ` | players ${h.players} | upload ${h.live.uploadMbps} Mbps` +
        ` | per-player ${h.live.perClientKBps} KB/s | batch ${h.lastBatchEntries} entries / ${(h.lastBatchBytes / 1024).toFixed(1)} KB` +
        ` | CPU ${h.live.cpuPercent}% | lag p99 ${h.live.eventLoopLagP99Ms}ms | ${h.memoryMB}MB`;
    } catch {
      line += ' | server: /health unreachable';
    }
    console.log(line);
  }, REPORT_SEC * 1000);
}

// ---------------------------------------------------------------------------
// Start / stop
// ---------------------------------------------------------------------------
let stopping = false;
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  clearInterval(rampTimer);
  clearInterval(reportTimer);
  for (const bot of bots) {
    clearInterval(bot.timer);
    bot.socket.disconnect();
  }
  if (serverProcess && serverProcess.exitCode === null) serverProcess.kill('SIGINT');
  console.log('Bots stopped.');
  setTimeout(() => process.exit(code), 500);
}

process.on('SIGINT', () => stop(0));
process.on('SIGTERM', () => stop(0));

async function main() {
  if (WITH_SERVER) {
    startServer();
    await waitForServer();
  }

  console.log(
    `Starting ${BOTS} bots against ${URL} (${RAMP_PER_SEC}/sec ramp, ${SEND_HZ} updates/sec each, ` +
      `${FOREVER ? 'until Ctrl+C' : `${DURATION_SEC}s`})`,
  );
  console.log(`Watch live: ${URL}/monitor`);

  startBots();
  startReporting();
  if (!FOREVER) setTimeout(() => stop(0), DURATION_SEC * 1000);
}

main().catch((err) => {
  console.error(err.message);
  stop(1);
});
