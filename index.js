const express = require('express');
const fs = require('fs');
const multer = require('multer');
const pino = require('pino');
const { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, Browsers, DisconnectReason, delay } = require('@whiskeysockets/baileys');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Global variables
let sock = null;
let saveCreds = null;
let isConnected = false;
let loopRunning = false;
let loopInterval = null;

let targets = [];
let messages = [];
let haterName = 'krix';
let delaySeconds = 10;
let totalSent = 0;
let totalFailed = 0;
let currentMsgIndex = 0;
let currentTargetIndex = 0;

// Pairing readiness flag
let pairingReady = false;
let qrReceived = false;

const logger = pino({ level: 'silent' });

// ==================== WHATSAPP CONNECTION ====================
async function connectToWhatsApp() {
  try {
    if (!fs.existsSync('./auth_info')) fs.mkdirSync('./auth_info', { recursive: true });
    const { state, saveCreds: save } = await useMultiFileAuthState('./auth_info');
    saveCreds = save;
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      logger,
      printQRInTerminal: true, // Terminal me QR bhi dikhega
      browser: Browsers.ubuntu('Chrome'),
      auth: {
        creds: state.creds,
        keys: state.keys,
      },
      version,
    });

    sock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect, qr } = update;

      // QR code aaya to pairingReady true
      if (qr) {
        console.log('📱 QR code received! Pairing ready.');
        pairingReady = true;
        qrReceived = true;
      }

      if (connection === 'open') {
        isConnected = true;
        console.log('✅ WhatsApp connected!');
      }

      if (connection === 'close') {
        isConnected = false;
        pairingReady = false;
        qrReceived = false;
        const statusCode = lastDisconnect && lastDisconnect.error && lastDisconnect.error.output ? lastDisconnect.error.output.statusCode : null;
        console.log('❌ Disconnected, status:', statusCode);
        if (statusCode === DisconnectReason.loggedOut) {
          fs.rmSync('./auth_info', { recursive: true, force: true });
        }
        setTimeout(() => connectToWhatsApp(), 5000);
      }
    });

    sock.ev.on('creds.update', saveCreds);

  } catch (err) {
    console.error('Connection error:', err.message);
    setTimeout(() => connectToWhatsApp(), 5000);
  }
}

// ==================== PAIRING CODE ====================
app.get('/pair-page', (req, res) => {
  res.send(`
    <html>
    <head><title>Pair WhatsApp</title></head>
    <body style="background:#111;color:#0f0;font-family:monospace;text-align:center;padding:40px;">
      <h1 style="color:#f0f;">📱 Pair WhatsApp</h1>
      <form action="/pair" method="post">
        <input type="text" name="phone" placeholder="919999999999" required style="padding:15px;font-size:1.2em;background:#222;border:2px solid #f0f;color:#fff;border-radius:5px;width:300px;margin:20px;">
        <br>
        <button type="submit" style="padding:15px 30px;background:#f0f;color:#fff;border:none;border-radius:5px;font-size:1.2em;cursor:pointer;">GET CODE</button>
      </form>
      <p style="color:#aaa;">Country code ke sath number daalo (e.g. 91XXXXXXXXXX)</p>
      <a href="/" style="color:#0f0;">🏠 Dashboard</a>
    </body>
    </html>
  `);
});

app.post('/pair', async (req, res) => {
  const phone = (req.body.phone || '').replace(/[^0-9]/g, '');
  if (!phone || phone.length < 10 || phone.length > 15) {
    return res.send('<h2>❌ Invalid phone number</h2><a href="/pair-page">BACK</a>');
  }

  // Socket check
  if (!sock) {
    return res.send('<h2>❌ WhatsApp socket not ready. Try again in a few seconds.</h2><a href="/pair-page">BACK</a>');
  }

  // Already connected?
  if (isConnected && sock.user) {
    return res.send('<h2>✅ Already paired!</h2><a href="/">GO TO DASHBOARD</a>');
  }

  // Wait for pairingReady (QR generated) max 20 seconds
  let waited = 0;
  while (!pairingReady && waited < 20000) {
    await delay(500);
    waited += 500;
  }

  if (!pairingReady) {
    return res.send('<h2>❌ Pairing not ready. Please wait a few seconds and try again.</h2><a href="/pair-page">BACK</a>');
  }

  try {
    // Now request pairing code
    const code = await sock.requestPairingCode(phone);
    const formatted = code.match(/.{1,4}/g) ? code.match(/.{1,4}/g).join('-') : code;
    res.send(`
      <html>
      <head><title>Pairing Code</title></head>
      <body style="background:#111;color:#0f0;font-family:monospace;text-align:center;padding:40px;">
        <h1 style="color:#f0f;">📱 Pairing Code</h1>
        <div style="font-size:3em;color:#f0f;letter-spacing:5px;background:#000;padding:20px;border-radius:10px;display:inline-block;">
          ${formatted}
        </div>
        <p style="color:#aaa;">Is code ko WhatsApp → Linked Devices → Link a Device me enter karo.</p>
        <a href="/" style="color:#0f0;">🏠 Dashboard</a> | 
        <a href="/pair-page" style="color:#0f0;">🔄 New Code</a>
      </body>
      </html>
    `);
  } catch (err) {
    console.error('Pairing error:', err.message);
    res.send('<h2>❌ Pairing failed: ' + err.message + '</h2><a href="/pair-page">BACK</a>');
  }
});

// ==================== ATTACK LOOP ====================
async function sendLoop() {
  while (loopRunning) {
    if (!isConnected || !targets.length || !messages.length) {
      await delay(2000);
      continue;
    }
    const target = targets[currentTargetIndex % targets.length];
    const msg = haterName + ' ' + messages[currentMsgIndex % messages.length];
    try {
      await sock.sendMessage(target, { text: msg });
      totalSent++;
      console.log(`✅ Sent #${totalSent} to ${target}`);
    } catch (err) {
      totalFailed++;
      console.log(`❌ Failed to ${target}: ${err.message}`);
    }
    currentTargetIndex++;
    if (currentTargetIndex >= targets.length) {
      currentTargetIndex = 0;
      currentMsgIndex++;
    }
    await delay(delaySeconds * 1000);
  }
}

function startLoop() {
  if (loopRunning) return;
  loopRunning = true;
  console.log('🔥 Attack loop started');
  sendLoop().catch(err => {
    console.error('Loop error:', err);
    loopRunning = false;
  });
}

function stopLoop() {
  loopRunning = false;
  console.log('⛔ Attack loop stopped');
}

// ==================== DASHBOARD ====================
app.get('/', (req, res) => {
  res.send(`
    <html>
    <head>
      <title>Muskan with Yanki</title>
      <style>
        body { background:#0a0a0f; color:#0f0; font-family:monospace; padding:20px; }
        h1 { color:#f0f; text-align:center; }
        .box { background:#111; padding:20px; border-radius:10px; max-width:800px; margin:20px auto; border:1px solid #333; }
        textarea, input { width:100%; padding:10px; margin:10px 0; background:#222; border:1px solid #444; color:#fff; font-family:monospace; border-radius:5px; }
        button { background:#f0f; color:#fff; padding:12px 30px; border:none; border-radius:5px; cursor:pointer; font-size:1.1em; }
        .status { text-align:center; font-size:1.5em; margin:20px 0; }
      </style>
    </head>
    <body>
      <h1>🔥 Muskan with Yanki</h1>
      <div class="status">
        Status: <span style="color:${isConnected ? '#0f0' : '#f00'};">${isConnected ? 'CONNECTED' : 'DISCONNECTED'}</span>
        | Sent: ${totalSent} | Failed: ${totalFailed}
      </div>
      <div class="box">
        <h3>📱 Pair WhatsApp</h3>
        <a href="/pair-page" style="color:#0f0;">Go to Pairing Page</a>
      </div>
      <div class="box">
        <h3>⚡ Start Attack</h3>
        <form action="/attack" method="post" enctype="multipart/form-data">
          <label>📱 Target Numbers (one per line)</label>
          <textarea name="numbers" rows="4" placeholder="919999999999&#10;918888888888"></textarea>
          
          <label>👥 Group IDs (optional, one per line)</label>
          <textarea name="groups" rows="2" placeholder="123456789@g.us"></textarea>
          
          <label>👤 Hater Name</label>
          <input type="text" name="hater" value="krix" required>
          
          <label>📄 Message File (.txt)</label>
          <input type="file" name="msgFile" accept=".txt" required>
          
          <label>⏱️ Delay (seconds)</label>
          <input type="number" name="delay" value="10" min="1" required>
          
          <button type="submit">🚀 START ATTACK</button>
        </form>
      </div>
      <div class="box" style="text-align:center;">
        <form action="/stop" method="post" style="display:inline;">
          <button type="submit" style="background:#f00;">⛔ STOP ATTACK</button>
        </form>
      </div>
    </body>
    </html>
  `);
});

// ==================== ATTACK ENDPOINT ====================
app.post('/attack', upload.single('msgFile'), (req, res) => {
  try {
    stopLoop();

    totalSent = 0;
    totalFailed = 0;
    currentMsgIndex = 0;
    currentTargetIndex = 0;

    targets = [];
    if (req.body.numbers) {
      const lines = req.body.numbers.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      for (const line of lines) {
        const cleaned = line.replace(/[^0-9]/g, '');
        if (cleaned.length >= 10 && cleaned.length <= 15) {
          targets.push(cleaned + '@s.whatsapp.net');
        }
      }
    }
    if (req.body.groups) {
      const lines = req.body.groups.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      for (const line of lines) {
        if (line.includes('@g.us')) targets.push(line);
        else if (line.includes('-')) targets.push(line + '@g.us');
        else if (line.length > 15) targets.push(line + '@g.us');
      }
    }

    if (targets.length === 0) {
      return res.send('<h2>❌ No valid targets provided</h2><a href="/">BACK</a>');
    }

    if (!req.file) {
      return res.send('<h2>❌ Message file required</h2><a href="/">BACK</a>');
    }
    messages = req.file.buffer.toString('utf-8').split('\n').map(l => l.trim()).filter(l => l.length > 0);
    if (messages.length === 0) {
      return res.send('<h2>❌ Message file empty</h2><a href="/">BACK</a>');
    }

    haterName = req.body.hater || 'krix';
    delaySeconds = parseInt(req.body.delay) || 10;
    if (delaySeconds < 1) delaySeconds = 1;

    startLoop();

    res.redirect('/');
  } catch (err) {
    console.error('Attack error:', err);
    res.send('<h2>❌ Error: ' + err.message + '</h2><a href="/">BACK</a>');
  }
});

app.post('/stop', (req, res) => {
  stopLoop();
  res.redirect('/');
});

// ==================== START SERVER ====================
const PORT = 5000;
connectToWhatsApp();
app.listen(PORT, () => {
  console.log(`\n🚀 Muskan with Yanki running on http://localhost:${PORT}`);
  console.log('📱 Pairing page: http://localhost:' + PORT + '/pair-page');
  console.log('💡 QR code bhi terminal me dikhega (agar chahe to scan kar lo)\n');
});
