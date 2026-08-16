const express = require('express');
const fs = require('fs');
const multer = require('multer');
const pino = require('pino');
const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers, DisconnectReason, delay } = require('@whiskeysockets/baileys');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// ---------- STATE ----------
let sock = null;
let saveCreds = null;
let isConnected = false;

// Bumped every time we create a brand new socket. Pairing codes are only
// valid for the socket session that generated them, so we tag cache
// entries with the generation that made them and reject anything stale.
let sockGeneration = 0;

// pairing code cache to avoid duplicate generation
// phone -> { code, timestamp, generation }
const pairingCache = new Map();

// How long we're willing to hand back the *same* code instead of asking
// Baileys for a new one. Kept short — WhatsApp pairing codes are short-lived,
// so caching for minutes just serves users an expired code.
const PAIRING_CACHE_MS = 45000;

// ---------- BULK SEND JOB STATE ----------
// Only one bulk job runs at a time to avoid overlapping sends / rate storms.
const bulkJob = {
  running: false,
  stopRequested: false,
  total: 0,
  sent: 0,
  failed: 0,
  currentNumber: null,
  log: [],       // last N entries: { number, status, error? }
  startedAt: null,
  finishedAt: null,
};

function bulkLog(entry) {
  bulkJob.log.push(entry);
  if (bulkJob.log.length > 200) bulkJob.log.shift(); // cap memory
}

const logger = pino({ level: 'silent' });

// ---------- KEEP THE SERVER ALIVE ----------
// Baileys/Node can throw from deep inside event callbacks or socket
// internals. Without these handlers, one bad event kills the whole process.
// Log and keep running instead of crashing.
process.on('uncaughtException', (err) => {
  console.error('🧯 Uncaught exception (server kept alive):', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('🧯 Unhandled rejection (server kept alive):', reason);
});

// ---------- HELPER: Normalize phone number ----------
function normalizePhone(input) {
  // remove all non-digits
  let digits = input.replace(/\D/g, '');
  // optional: add country code if missing? we assume user gives with country code
  if (digits.length < 10 || digits.length > 15) return null;
  return digits;
}

// ---------- HELPER: Parse a numbers list from uploaded text ----------
// Accepts one number per line, or comma/space separated. Skips blanks and
// invalid entries instead of failing the whole batch.
function parseNumbersFromText(text) {
  const raw = text.split(/[\n,;\s]+/).map(s => s.trim()).filter(Boolean);
  const seen = new Set();
  const numbers = [];
  for (const r of raw) {
    const n = normalizePhone(r);
    if (n && !seen.has(n)) {
      seen.add(n);
      numbers.push(n);
    }
  }
  return numbers;
}
// Calling requestPairingCode too early (right after makeWASocket, before the
// underlying WS handshake finishes) throws "Connection Closed". Poll briefly
// instead of assuming it's ready.
async function waitForSocketReady(targetSock, timeoutMs = 8000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (targetSock?.ws?.readyState === 1) return true;
    // socket got replaced or torn down while we were waiting
    if (targetSock !== sock) return false;
    await delay(200);
  }
  return targetSock?.ws?.readyState === 1;
}

// ---------- WHATSAPP CONNECTION ----------
async function connectToWhatsApp() {
  // Prevent duplicate socket creation/reconnect loops
  if (sock && (isConnected || sock.ws?.readyState === 1)) {
    console.log('⚠️ Socket already exists, skipping duplicate connect');
    return;
  }

  try {
    if (!fs.existsSync('./auth_info')) fs.mkdirSync('./auth_info', { recursive: true });
    const { state, saveCreds: save } = await useMultiFileAuthState('./auth_info');
    saveCreds = save;

    // fetchLatestBaileysVersion() hits the network. If that call is slow or
    // fails (common right after a cold start on free hosting), don't let it
    // block socket creation forever — fall back to a known-good version.
    let version;
    try {
      const versionTimeout = new Promise((_, reject) => setTimeout(() => reject(new Error('version fetch timeout')), 8000));
      const result = await Promise.race([fetchLatestBaileysVersion(), versionTimeout]);
      version = result.version;
    } catch (verErr) {
      console.error('⚠️ Could not fetch latest Baileys version, using fallback:', verErr.message);
      version = [2, 3000, 1023223821]; // reasonably recent fallback, updated periodically
    }

    sock = makeWASocket({
      logger,
      printQRInTerminal: false,          // QR disabled
      browser: Browsers.ubuntu('Chrome'),
      auth: {
        creds: state.creds,
        keys: state.keys,
      },
      version,
    });

    console.log('🔌 Socket created, waiting for connection...');
    sockGeneration += 1;
    // Any pairing codes we handed out belonged to the previous socket
    // session and are no longer valid — drop them so we don't re-serve them.
    pairingCache.clear();

    sock.ev.on('connection.update', (update) => {
     try {
      const { connection, lastDisconnect } = update;

      if (connection === 'open') {
        isConnected = true;
        console.log('✅ WhatsApp connected!');
        pairingCache.clear();
      }

      if (connection === 'close') {
        isConnected = false;
        // Stale codes/socket reference — clear both immediately so
        // /get-code doesn't try to reuse a dead session while we reconnect.
        pairingCache.clear();
        const deadSock = sock;
        sock = null;

        const statusCode = lastDisconnect?.error?.output?.statusCode;
        console.log(`🔌 Disconnected, statusCode: ${statusCode}`);

        if (statusCode === DisconnectReason.loggedOut) {
          console.log('🚫 Logged out – clearing session');
          fs.rmSync('./auth_info', { recursive: true, force: true });
          setTimeout(() => connectToWhatsApp(), 5000);
        } else if (statusCode === DisconnectReason.restartRequired || statusCode === 408 || statusCode === 515) {
          console.log('🔄 Restart required, reconnecting...');
          setTimeout(() => connectToWhatsApp(), 3000);
        } else {
          const delayMs = Math.min(60000, 2000 * Math.pow(2, (deadSock?.reconnectAttempts || 0)));
          console.log(`🔄 Reconnecting in ${delayMs / 1000}s`);
          setTimeout(() => connectToWhatsApp(), delayMs);
        }
      }
     } catch (err) {
       console.error('❌ Error in connection.update handler (kept alive):', err.message);
     }
    });

    sock.ev.on('creds.update', async () => {
      try {
        await saveCreds();
      } catch (err) {
        console.error('❌ Failed to save creds:', err.message);
      }
    });

  } catch (err) {
    console.error('❌ Connection error:', err.message);
    setTimeout(() => connectToWhatsApp(), 5000);
  }
}

// ---------- BULK SEND JOB RUNNER ----------
async function runBulkJob(numbers, messageText, delaySeconds) {
  bulkJob.running = true;
  bulkJob.stopRequested = false;
  bulkJob.total = numbers.length;
  bulkJob.sent = 0;
  bulkJob.failed = 0;
  bulkJob.currentNumber = null;
  bulkJob.log = [];
  bulkJob.startedAt = Date.now();
  bulkJob.finishedAt = null;

  const baseDelayMs = Math.max(1, delaySeconds) * 1000;

  for (const number of numbers) {
    if (bulkJob.stopRequested) {
      bulkLog({ number, status: 'skipped (stopped)' });
      continue;
    }

    // If connection drops mid-run, pause the loop rather than blasting
    // errors for every remaining number — wait for reconnect briefly.
    let waited = 0;
    while (!(isConnected && sock?.user) && waited < 30000 && !bulkJob.stopRequested) {
      await delay(1000);
      waited += 1000;
    }
    if (!(isConnected && sock?.user)) {
      bulkLog({ number, status: 'failed', error: 'WhatsApp disconnected' });
      bulkJob.failed++;
      continue;
    }

    bulkJob.currentNumber = number;
    const jid = number + '@s.whatsapp.net';

    try {
      let checkedJid = jid;
      try {
        const [result] = await sock.onWhatsApp(jid);
        if (result && result.exists === false) {
          bulkLog({ number, status: 'failed', error: 'Not on WhatsApp' });
          bulkJob.failed++;
          // still respect the delay so we don't hammer the lookup endpoint
          await delay(baseDelayMs + Math.floor(Math.random() * 1000));
          continue;
        }
        if (result?.jid) checkedJid = result.jid;
      } catch {
        // lookup failing shouldn't block the send attempt
      }

      await sock.sendMessage(checkedJid, { text: messageText });
      bulkJob.sent++;
      bulkLog({ number, status: 'sent' });
    } catch (err) {
      bulkJob.failed++;
      bulkLog({ number, status: 'failed', error: err.message });
    }

    // Random jitter (±30%) around the requested delay so sends don't look
    // like a bot firing at a perfectly fixed interval.
    const jitter = baseDelayMs * (0.7 + Math.random() * 0.6);
    await delay(jitter);
  }

  bulkJob.currentNumber = null;
  bulkJob.running = false;
  bulkJob.finishedAt = Date.now();
}

// ---------- BULK SEND PAGE ----------
app.get('/bulk', (req, res) => {
  res.send(`
    <html>
    <head>
      <title>Bulk Send - Muskan with Yanki</title>
      <style>
        body { background:#111; color:#0f0; font-family:monospace; padding:30px; max-width:700px; margin:0 auto; }
        h1 { color:#f0f; text-align:center; }
        label { display:block; margin-top:15px; color:#aaa; }
        input, textarea { width:100%; padding:10px; margin-top:5px; background:#222; border:1px solid #444; color:#fff; font-family:monospace; border-radius:5px; box-sizing:border-box; }
        button { background:#f0f; color:#fff; padding:12px 30px; border:none; border-radius:5px; cursor:pointer; font-size:1.1em; margin-top:20px; }
        .hint { color:#888; font-size:0.85em; }
        a { color:#0f0; }
      </style>
    </head>
    <body>
      <h1>📨 Bulk Send</h1>
      <form action="/bulk/start" method="post" enctype="multipart/form-data">
        <label>Target numbers file (.txt/.csv — one number per line, with country code)</label>
        <input type="file" name="targetFile" accept=".txt,.csv" required>
        <p class="hint">Or paste numbers directly below (used only if no file is chosen):</p>
        <textarea name="targetNumbers" rows="3" placeholder="919999999999&#10;919888888888"></textarea>

        <label>Message text</label>
        <textarea name="message" rows="4" placeholder="Type your message..."></textarea>
        <p class="hint">Or upload a message file (.txt) — used instead if provided:</p>
        <input type="file" name="messageFile" accept=".txt">

        <label>Delay between messages (seconds)</label>
        <input type="number" name="delaySeconds" min="1" value="5" required>
        <p class="hint">Higher delay = safer for your number. Don't go below a few seconds.</p>

        <button type="submit">▶ START</button>
      </form>
      <p style="margin-top:20px;"><a href="/bulk/status">📊 View progress</a> | <a href="/">🏠 Dashboard</a></p>
    </body>
    </html>
  `);
});

app.post('/bulk/start', upload.fields([{ name: 'targetFile', maxCount: 1 }, { name: 'messageFile', maxCount: 1 }]), async (req, res) => {
  if (bulkJob.running) {
    return res.send('<h2>❌ A bulk job is already running.</h2><a href="/bulk/status">VIEW PROGRESS</a>');
  }
  if (!isConnected || !sock?.user) {
    return res.send('<h2>❌ WhatsApp not connected. Please pair first.</h2><a href="/">BACK</a>');
  }

  const targetFile = req.files?.targetFile?.[0];
  const messageFile = req.files?.messageFile?.[0];

  const numbersSource = targetFile ? targetFile.buffer.toString('utf-8') : (req.body.targetNumbers || '');
  const numbers = parseNumbersFromText(numbersSource);

  const messageText = messageFile ? messageFile.buffer.toString('utf-8').trim() : (req.body.message || '').trim();

  const delaySeconds = Math.max(1, parseInt(req.body.delaySeconds, 10) || 5);

  if (numbers.length === 0) {
    return res.send('<h2>❌ No valid numbers found in file/input.</h2><a href="/bulk">BACK</a>');
  }
  if (!messageText) {
    return res.send('<h2>❌ Message is empty — provide text or a message file.</h2><a href="/bulk">BACK</a>');
  }

  // Fire and forget — the job runs in the background, progress via /bulk/status
  runBulkJob(numbers, messageText, delaySeconds).catch(err => {
    console.error('❌ Bulk job crashed (kept server alive):', err.message);
    bulkJob.running = false;
    bulkJob.finishedAt = Date.now();
  });

  res.redirect('/bulk/status');
});

app.post('/bulk/stop', (req, res) => {
  bulkJob.stopRequested = true;
  res.redirect('/bulk/status');
});

app.get('/bulk/status', (req, res) => {
  const logRows = bulkJob.log.slice(-50).reverse().map(l =>
    `<tr><td>${l.number}</td><td style="color:${l.status === 'sent' ? '#0f0' : '#f66'}">${l.status}</td><td>${l.error || ''}</td></tr>`
  ).join('');

  res.send(`
    <html>
    <head>
      <title>Bulk Progress</title>
      ${bulkJob.running ? '<meta http-equiv="refresh" content="3">' : ''}
      <style>
        body { background:#111; color:#0f0; font-family:monospace; padding:30px; max-width:800px; margin:0 auto; }
        h1 { color:#f0f; text-align:center; }
        .stats { display:flex; justify-content:space-around; background:#222; padding:15px; border-radius:8px; margin:20px 0; }
        table { width:100%; border-collapse:collapse; font-size:0.85em; }
        td, th { padding:6px; border-bottom:1px solid #333; text-align:left; }
        button { background:#f66; color:#fff; padding:10px 20px; border:none; border-radius:5px; cursor:pointer; }
        a { color:#0f0; }
      </style>
    </head>
    <body>
      <h1>📊 Bulk Send Progress</h1>
      <div class="stats">
        <div>Status: <b style="color:${bulkJob.running ? '#0f0' : '#aaa'}">${bulkJob.running ? 'RUNNING' : 'IDLE'}</b></div>
        <div>Total: <b>${bulkJob.total}</b></div>
        <div>Sent: <b style="color:#0f0">${bulkJob.sent}</b></div>
        <div>Failed: <b style="color:#f66">${bulkJob.failed}</b></div>
      </div>
      ${bulkJob.currentNumber ? `<p>Currently sending to: <b>${bulkJob.currentNumber}</b></p>` : ''}
      ${bulkJob.running ? '<form action="/bulk/stop" method="post"><button type="submit">⏹ STOP</button></form>' : ''}
      <table>
        <tr><th>Number</th><th>Status</th><th>Error</th></tr>
        ${logRows || '<tr><td colspan="3">No activity yet</td></tr>'}
      </table>
      <p style="margin-top:20px;"><a href="/bulk">🔁 New Job</a> | <a href="/">🏠 Dashboard</a></p>
    </body>
    </html>
  `);
});

// ---------- GROUP UID LIST ----------
app.get('/groups', async (req, res) => {
  if (!isConnected || !sock?.user) {
    return res.send('<h2>❌ WhatsApp not connected. Please pair first.</h2><a href="/">BACK</a>');
  }
  try {
    const groups = await sock.groupFetchAllParticipating();
    const rows = Object.values(groups).map(g =>
      `<tr><td>${g.subject || '(no name)'}</td><td style="font-size:0.8em;">${g.id}</td><td>${g.participants?.length ?? '-'}</td></tr>`
    ).join('');

    res.send(`
      <html>
      <head>
        <title>Groups</title>
        <style>
          body { background:#111; color:#0f0; font-family:monospace; padding:30px; max-width:800px; margin:0 auto; }
          h1 { color:#f0f; text-align:center; }
          table { width:100%; border-collapse:collapse; font-size:0.85em; }
          td, th { padding:8px; border-bottom:1px solid #333; text-align:left; }
          a { color:#0f0; }
        </style>
      </head>
      <body>
        <h1>👥 Your Groups</h1>
        <table>
          <tr><th>Name</th><th>Group UID</th><th>Members</th></tr>
          ${rows || '<tr><td colspan="3">No groups found</td></tr>'}
        </table>
        <p style="margin-top:20px;"><a href="/">🏠 Dashboard</a></p>
      </body>
      </html>
    `);
  } catch (err) {
    console.error('❌ Group fetch error:', err.message);
    res.send(`<h2>❌ Failed to fetch groups: ${err.message}</h2><a href="/">BACK</a>`);
  }
});

// ---------- DEBUG STATUS ----------
app.get('/status', (req, res) => {
  res.json({
    socketExists: !!sock,
    isConnected,
    hasUser: !!sock?.user,
    wsReadyState: sock?.ws?.readyState ?? null,
    sockGeneration,
    bulkJobRunning: bulkJob.running,
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

// ---------- PAIRING PAGE ----------
app.get('/pair', (req, res) => {
  res.send(`
    <html>
    <head>
      <title>Pair WhatsApp - Muskan with Yanki</title>
      <style>
        body { background:#111; color:#0f0; font-family:monospace; text-align:center; padding:40px; }
        h1 { color:#f0f; }
        input { padding:15px; font-size:1.2em; background:#222; border:2px solid #f0f; color:#fff; border-radius:5px; width:300px; margin:20px; }
        button { padding:15px 30px; background:#f0f; color:#fff; border:none; border-radius:5px; font-size:1.2em; cursor:pointer; }
        .steps { background:#222; padding:15px; border-radius:5px; margin:20px auto; max-width:500px; text-align:left; color:#aaa; }
        a { color:#0f0; }
      </style>
    </head>
    <body>
      <h1>📱 Pair WhatsApp</h1>
      <p>Enter your phone number with country code (no + or spaces)</p>
      <form action="/get-code" method="post">
        <input type="text" name="phone" placeholder="919999999999" required>
        <br>
        <button type="submit">GET PAIRING CODE</button>
      </form>
      <div class="steps">
        <strong>📌 How to link:</strong><br>
        1. Open WhatsApp on your phone<br>
        2. Go to Settings → Linked Devices<br>
        3. Tap "Link a Device"<br>
        4. Choose "Link with phone number instead?"<br>
        5. Enter the code shown here (within 2 minutes)
      </div>
      <p><a href="/">🏠 Dashboard</a></p>
    </body>
    </html>
  `);
});

// ---------- GET PAIRING CODE ----------
app.post('/get-code', async (req, res) => {
  const phoneRaw = req.body.phone || '';
  const phone = normalizePhone(phoneRaw);
  if (!phone) {
    return res.send('<h2>❌ Invalid phone number</h2><a href="/pair">BACK</a>');
  }

  // Ensure socket exists — poll briefly instead of failing instantly,
  // since right after a cold start the socket can take a few seconds to spin up.
  let waited = 0;
  while (!sock && waited < 15000) {
    await delay(500);
    waited += 500;
  }
  if (!sock) {
    return res.send('<h2>❌ WhatsApp not ready yet. Please wait a few seconds and try again.</h2><a href="/pair">BACK</a>');
  }

  // If already authenticated, no pairing needed
  if (isConnected && sock.user) {
    return res.send('<h2>✅ Already paired!</h2><a href="/">GO TO DASHBOARD</a>');
  }

  // Check for existing valid code (prevents duplicate generation) — but only
  // if it came from the socket session that's still alive.
  const existing = pairingCache.get(phone);
  if (existing && existing.generation === sockGeneration && (Date.now() - existing.timestamp < PAIRING_CACHE_MS)) {
    const formatted = existing.code.match(/.{1,4}/g)?.join('-') || existing.code;
    return res.send(`
      <html>
      <head><title>Pairing Code</title>
      <style>
        body { background:#111; color:#0f0; font-family:monospace; text-align:center; padding:40px; }
        .code-box { font-size:3em; color:#f0f; letter-spacing:5px; background:#000; padding:20px; border-radius:10px; display:inline-block; margin:20px; }
        a { color:#0f0; }
      </style>
      </head>
      <body>
        <h1>📱 Pairing Code</h1>
        <div class="code-box">${formatted}</div>
        <p style="color:#aaa;">(Re‑using same code – still valid)</p>
        <p><a href="/">🏠 Dashboard</a> | <a href="/pair">🔄 New Code</a></p>
      </body>
      </html>
    `);
  }

  const currentSock = sock;
  const currentGeneration = sockGeneration;

  try {
    const ready = await waitForSocketReady(currentSock);
    if (!ready || currentSock !== sock) {
      return res.send('<h2>❌ WhatsApp connection isn\'t ready yet. Please wait a few seconds and try again.</h2><a href="/pair">BACK</a>');
    }

    // Retry a couple of times — transient "Connection Closed" errors can
    // happen right as the WS finishes its handshake.
    let rawCode = null;
    let lastErr = null;
    for (let attempt = 0; attempt < 3 && !rawCode; attempt++) {
      try {
        rawCode = await currentSock.requestPairingCode(phone);
      } catch (err) {
        lastErr = err;
        if (attempt < 2) await delay(1000);
      }
    }

    if (!rawCode) throw lastErr || new Error('Unknown pairing error');

    const formatted = rawCode.match(/.{1,4}/g)?.join('-') || rawCode;
    pairingCache.set(phone, { code: rawCode, timestamp: Date.now(), generation: currentGeneration });

    res.send(`
      <html>
      <head>
        <title>Pairing Code</title>
        <style>
          body { background:#111; color:#0f0; font-family:monospace; text-align:center; padding:40px; }
          .code-box { font-size:3em; color:#f0f; letter-spacing:5px; background:#000; padding:20px; border-radius:10px; display:inline-block; margin:20px; }
          a { color:#0f0; }
        </style>
      </head>
      <body>
        <h1>📱 Pairing Code</h1>
        <div class="code-box">${formatted}</div>
        <p style="color:#aaa;">Enter this code in WhatsApp → Linked Devices → Link a Device (within ~1 minute)</p>
        <p><a href="/">🏠 Dashboard</a> | <a href="/pair">🔄 New Code</a></p>
      </body>
      </html>
    `);
  } catch (err) {
    console.error('❌ Pairing error:', err.message);
    res.send(`<h2>❌ Pairing failed: ${err.message}</h2><p>Try tapping "New Code" — if it keeps failing, the connection may need a moment to settle.</p><a href="/pair">BACK</a>`);
  }
});

// ---------- DASHBOARD ----------
app.get('/', (req, res) => {
  res.send(`
    <html>
    <head>
      <title>Muskan with Yanki</title>
      <style>
        body { background:#0a0a0f; color:#0f0; font-family:monospace; padding:20px; }
        h1 { color:#f0f; text-align:center; }
        .box { background:#111; padding:20px; border-radius:10px; max-width:800px; margin:20px auto; border:1px solid #333; }
        input, textarea { width:100%; padding:10px; margin:10px 0; background:#222; border:1px solid #444; color:#fff; font-family:monospace; border-radius:5px; }
        button { background:#f0f; color:#fff; padding:12px 30px; border:none; border-radius:5px; cursor:pointer; font-size:1.1em; }
        .status { text-align:center; font-size:1.5em; margin:20px 0; }
        a { color:#0f0; }
      </style>
    </head>
    <body>
      <h1>🔥 Muskan with Yanki</h1>
      <div class="status">
        Status: <span style="color:${isConnected ? '#0f0' : '#f00'};">${isConnected ? 'CONNECTED' : 'DISCONNECTED'}</span>
      </div>
      <div class="box">
        <h3>📱 Pair WhatsApp</h3>
        <a href="/pair">Go to Pairing Page</a>
      </div>
      <div class="box">
        <h3>📨 Bulk Send</h3>
        <a href="/bulk">Go to Bulk Send</a>
      </div>
      <div class="box">
        <h3>👥 Groups</h3>
        <a href="/groups">View Group UIDs</a>
      </div>
      <div class="box">
        <h3>📤 Send Message</h3>
        <form action="/send-test" method="post">
          <label>Recipient Phone Number (with country code, no +)</label>
          <input type="text" name="number" placeholder="919999999999" required>
          <label>Message (optional — leave blank for a default test message)</label>
          <textarea name="message" rows="3" placeholder="Type your message..."></textarea>
          <button type="submit">Send Message</button>
        </form>
      </div>
    </body>
    </html>
  `);
});

// ---------- SEND TEST MESSAGE ----------
app.post('/send-test', async (req, res) => {
  const phoneRaw = req.body.number || '';
  const phone = normalizePhone(phoneRaw);
  const messageText = (req.body.message || '').trim() || '✅ Test message from Muskan with Yanki — connection OK!';

  if (!phone) {
    return res.send('<h2>❌ Invalid phone number</h2><a href="/">BACK</a>');
  }
  if (!isConnected || !sock?.user) {
    return res.send('<h2>❌ WhatsApp not connected. Please pair first.</h2><a href="/">BACK</a>');
  }

  // Capture the socket reference now — if it gets replaced mid-request
  // (reconnect happening in parallel), we don't want to send on a dead one.
  const currentSock = sock;
  const jid = phone + '@s.whatsapp.net';

  try {
    // Confirm the number is actually on WhatsApp before sending — sending
    // to a non-WA number silently fails or errors confusingly otherwise.
    let checkedJid = jid;
    try {
      const [result] = await currentSock.onWhatsApp(jid);
      if (result && result.exists === false) {
        return res.send('<h2>❌ This number isn\'t on WhatsApp.</h2><a href="/">BACK</a>');
      }
      if (result?.jid) checkedJid = result.jid;
    } catch (checkErr) {
      // onWhatsApp lookup failing shouldn't block sending — some servers
      // restrict this; just fall through and try the direct send.
      console.error('⚠️ onWhatsApp lookup failed, sending anyway:', checkErr.message);
    }

    if (currentSock !== sock || !isConnected) {
      return res.send('<h2>❌ Connection changed while sending — please try again.</h2><a href="/">BACK</a>');
    }

    let sent = false;
    let lastErr = null;
    for (let attempt = 0; attempt < 3 && !sent; attempt++) {
      try {
        await currentSock.sendMessage(checkedJid, { text: messageText });
        sent = true;
      } catch (err) {
        lastErr = err;
        if (attempt < 2) await delay(1000);
      }
    }

    if (!sent) throw lastErr || new Error('Unknown send error');

    res.send('<h2>✅ Message sent successfully!</h2><a href="/">BACK TO DASHBOARD</a>');
  } catch (err) {
    console.error('❌ Send message error:', err.message);
    res.send(`<h2>❌ Failed to send message: ${err.message}</h2><a href="/">BACK</a>`);
  }
});

// ---------- START SERVER ----------
const PORT = 5000;
connectToWhatsApp();
app.listen(PORT, () => {
  console.log(`\n🚀 Muskan with Yanki running on http://localhost:${PORT}`);
  console.log('📱 Pairing page: http://localhost:' + PORT + '/pair');
  console.log('📤 Test message: http://localhost:' + PORT + '/send-test (POST)');
  console.log('💡 QR is disabled. Use pairing code only.\n');
});
