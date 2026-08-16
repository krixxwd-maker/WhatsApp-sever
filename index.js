const express = require('express');
const fs = require('fs');
const multer = require('multer');
const pino = require('pino');
const {
  makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers,
  DisconnectReason,
  delay
} = require('@whiskeysockets/baileys');

const app = express();
const upload = multer(); // not used for message body, kept for compatibility
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// ---------- STATE ----------
let sock = null;
let saveCreds = null;
let isConnected = false;
let isSocketReady = false;   // NEW: indicates the WebSocket is open for pairing
let reconnectAttempts = 0;   // NEW: for exponential backoff

// pairing code cache to avoid duplicate generation
const pairingCache = new Map(); // phone -> { code, timestamp }

const logger = pino({ level: 'silent' });

// ---------- HELPER: Normalize phone number ----------
function normalizePhone(input) {
  // remove all non-digits
  let digits = input.replace(/\D/g, '');
  // optional: add country code if missing? we assume user gives with country code
  if (digits.length < 10 || digits.length > 15) return null;
  return digits;
}

// ---------- WAIT FOR SOCKET READY ----------
// This function resolves when the WebSocket is open and we can request a pairing code.
// It listens for the 'connection.update' event and waits for a 'qr' or 'connecting' state.
// Since we disable QR, we still get the event - we just don't print it.
function waitForSocketReady(timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    if (isSocketReady) return resolve();
    if (!sock) return reject(new Error('Socket not created'));

    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error('Timeout waiting for socket to be ready'));
    }, timeoutMs);

    const onUpdate = (update) => {
      // When connection is 'connecting' or we receive a QR (even though we don't show it), socket is ready
      if (update.connection === 'connecting' || update.qr) {
        isSocketReady = true;
        cleanup();
        resolve();
      }
      // If connection closes before ready, reject
      if (update.connection === 'close') {
        cleanup();
        reject(new Error('Socket closed before ready'));
      }
    };

    sock.ev.on('connection.update', onUpdate);

    const cleanup = () => {
      clearTimeout(timeout);
      sock.ev.off('connection.update', onUpdate);
    };
  });
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
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      logger,
      printQRInTerminal: false,          // 1. QR disabled completely
      browser: Browsers.ubuntu('Chrome'),
      auth: {
        creds: state.creds,
        keys: state.keys,
      },
      version,
    });

    // Reset flags on new socket
    isSocketReady = false;

    sock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect } = update;

      // Mark socket as ready when we get a QR event (even if we don't show it) or when connecting
      if (update.qr || connection === 'connecting') {
        isSocketReady = true;
      }

      if (connection === 'open') {
        isConnected = true;
        reconnectAttempts = 0; // reset counter on successful connection
        console.log('✅ WhatsApp connected!');
        // clear pairing cache when authenticated
        pairingCache.clear();
      }

      if (connection === 'close') {
        isConnected = false;
        isSocketReady = false; // reset readiness
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        console.log(`🔌 Disconnected, statusCode: ${statusCode}`);

        // Handle different disconnect reasons
        if (statusCode === DisconnectReason.loggedOut) {
          // Logged out – clear session and restart fresh
          console.log('🚫 Logged out – clearing session');
          fs.rmSync('./auth_info', { recursive: true, force: true });
          reconnectAttempts = 0;
          setTimeout(() => connectToWhatsApp(), 5000);
        } else if (statusCode === DisconnectReason.restartRequired || statusCode === 408 || statusCode === 515) {
          // Restart required – reconnect quickly
          console.log('🔄 Restart required, reconnecting...');
          reconnectAttempts = 0;
          setTimeout(() => connectToWhatsApp(), 3000);
        } else {
          // Other failures – exponential backoff
          const delayMs = Math.min(60000, 2000 * Math.pow(2, reconnectAttempts));
          reconnectAttempts++;
          console.log(`🔄 Reconnecting in ${delayMs / 1000}s (attempt ${reconnectAttempts})`);
          setTimeout(() => connectToWhatsApp(), delayMs);
        }
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
    reconnectAttempts++;
    setTimeout(() => connectToWhatsApp(), 5000);
  }
}

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

  // Ensure socket exists
  if (!sock) {
    return res.send('<h2>❌ WhatsApp not ready yet. Please wait a few seconds and try again.</h2><a href="/pair">BACK</a>');
  }

  // If already authenticated, no pairing needed
  if (isConnected && sock.user) {
    return res.send('<h2>✅ Already paired!</h2><a href="/">GO TO DASHBOARD</a>');
  }

  // Wait for socket to be ready (WebSocket open) before requesting code
  try {
    await waitForSocketReady();
  } catch (err) {
    console.error('❌ Socket ready timeout:', err.message);
    return res.send('<h2>❌ Socket not ready. Please try again in a moment.</h2><a href="/pair">BACK</a>');
  }

  // Check for existing valid code (prevents duplicate generation)
  const existing = pairingCache.get(phone);
  if (existing && (Date.now() - existing.timestamp < 120000)) {
    // return the same code
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

  try {
    // 2 & 3. Call requestPairingCode after socket is ready (no QR condition)
    const rawCode = await sock.requestPairingCode(phone);
    const formatted = rawCode.match(/.{1,4}/g)?.join('-') || rawCode;

    // store in cache
    pairingCache.set(phone, { code: rawCode, timestamp: Date.now() });

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
        <p style="color:#aaa;">Enter this code in WhatsApp → Linked Devices → Link a Device</p>
        <p><a href="/">🏠 Dashboard</a> | <a href="/pair">🔄 New Code</a></p>
      </body>
      </html>
    `);
  } catch (err) {
    console.error('❌ Pairing error:', err.message);
    res.send(`<h2>❌ Pairing failed: ${err.message}</h2><a href="/pair">BACK</a>`);
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
        <h3>📤 Send Test Message</h3>
        <form action="/send-test" method="post">
          <label>Recipient Phone Number (with country code, no +)</label>
          <input type="text" name="number" placeholder="919999999999" required>
          <button type="submit">Send Test Message</button>
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
  if (!phone) {
    return res.send('<h2>❌ Invalid phone number</h2><a href="/">BACK</a>');
  }
  if (!isConnected || !sock?.user) {
    return res.send('<h2>❌ WhatsApp not connected. Please pair first.</h2><a href="/">BACK</a>');
  }

  const jid = phone + '@s.whatsapp.net';
  try {
    await sock.sendMessage(jid, { text: '✅ Test message from Muskan with Yanki — connection OK!' });
    res.send('<h2>✅ Test message sent successfully!</h2><a href="/">BACK TO DASHBOARD</a>');
  } catch (err) {
    console.error('❌ Send test error:', err.message);
    res.send(`<h2>❌ Failed to send test message: ${err.message}</h2><a href="/">BACK</a>`);
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
