const express = require('express');
const fs = require('fs');
const multer = require('multer');
const pino = require('pino');
const { makeWASocket, useMultiFileAuthState, delay, DisconnectReason, fetchLatestBaileysVersion, Browsers } = require('@whiskeysockets/baileys');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

let sock = null;
let isConnected = false;
let loopActive = false;
let sendInterval = null;
let totalSent = 0;
let totalFailed = 0;
let currentMessageIndex = 0;
let messages = [];
let targets = [];
let haterName = 'krix';

const logger = pino({ level: 'silent' });

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('./auth_info');
  const { version } = await fetchLatestBaileysVersion();
  
  sock = makeWASocket({
    logger,
    printQRInTerminal: true, // Terminal me QR code aayega
    browser: Browsers.ubuntu('Chrome'),
    auth: {
      creds: state.creds,
      keys: state.keys
    },
    version,
  });

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'open') {
      isConnected = true;
      console.log('✅ WhatsApp connected!');
    }
    if (connection === 'close') {
      isConnected = false;
      const statusCode = lastDisconnect && lastDisconnect.error && lastDisconnect.error.output ? lastDisconnect.error.output.statusCode : null;
      console.log('❌ Disconnected. Status:', statusCode);
      if (statusCode === DisconnectReason.loggedOut) {
        fs.rmSync('./auth_info', { recursive: true, force: true });
      }
      setTimeout(() => connectToWhatsApp(), 5000);
    }
  });

  sock.ev.on('creds.update', saveCreds);
}

async function sendMessage(target, message) {
  if (!sock || !isConnected) {
    console.log('WhatsApp not connected');
    return false;
  }
  try {
    await sock.sendMessage(target, { text: message });
    totalSent++;
    console.log(`✅ Sent #${totalSent} to ${target}`);
    return true;
  } catch (err) {
    totalFailed++;
    console.log(`❌ Failed to ${target}: ${err.message}`);
    return false;
  }
}

function startLoop() {
  if (loopActive) return;
  loopActive = true;
  console.log('🔥 Loop started');
  sendInterval = setInterval(async () => {
    if (!isConnected || !targets.length || !messages.length) return;
    const target = targets[totalSent % targets.length];
    const msg = haterName + ' ' + messages[currentMessageIndex % messages.length];
    await sendMessage(target, msg);
    currentMessageIndex++;
  }, 10000); // 10 second delay
}

function stopLoop() {
  if (sendInterval) clearInterval(sendInterval);
  loopActive = false;
  console.log('⛔ Loop stopped');
}

app.get('/', (req, res) => {
  res.send(`
    <h1>Muskan with Yanki</h1>
    <p>Status: ${isConnected ? 'Connected' : 'Disconnected'}</p>
    <p>Sent: ${totalSent} | Failed: ${totalFailed}</p>
    <form action="/start" method="post" enctype="multipart/form-data">
      <textarea name="numbers" placeholder="Numbers (one per line) e.g. 919999999999" rows="5"></textarea><br>
      <input type="file" name="msgFile" accept=".txt" required><br>
      <input type="text" name="hater" placeholder="Your name" value="krix"><br>
      <button type="submit">START</button>
    </form>
    <form action="/stop" method="post"><button type="submit">STOP</button></form>
  `);
});

app.post('/start', upload.single('msgFile'), (req, res) => {
  const numbers = req.body.numbers.split('\n').map(n => n.trim()).filter(n => n.length >= 10);
  targets = numbers.map(n => n.includes('@') ? n : n + '@s.whatsapp.net');
  if (req.file) {
    messages = req.file.buffer.toString('utf-8').split('\n').map(m => m.trim()).filter(m => m.length > 0);
  }
  haterName = req.body.hater || 'krix';
  totalSent = 0;
  totalFailed = 0;
  currentMessageIndex = 0;
  stopLoop();
  startLoop();
  res.redirect('/');
});

app.post('/stop', (req, res) => {
  stopLoop();
  res.redirect('/');
});

const PORT = 5000;
connectToWhatsApp();
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
