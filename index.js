const express = require('express');
const fs = require('fs');
const pino = require('pino');
const multer = require('multer');
const {
    makeWASocket,
    useMultiFileAuthState,
    delay,
    DisconnectReason,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    Browsers
} = require('@whiskeysockets/baileys');

const app = express();
const port = process.env.PORT || 5000;

// ======================= GLOBAL STATE =======================
let MznKing = null;
let messages = null;
let targets = [];
let intervalTime = null;
let haterName = null;
let reconnectAttempts = 0;
let isConnecting = false;

let sessionRefreshInterval = null;
let infiniteLoopThread = null;
let reconnectTimer = null;

const loopController = {
    active: false,
    running: false,
    crashCount: 0,
    lastActivityTime: Date.now(),
    lastSendTime: Date.now(),
    forcedClearCount: 0,
    messageCount: 0
};

let totalSent = 0;
let totalFailed = 0;
let totalErrors = 0;
let sessionStart = null;

let consecutiveErrors = 0;
let isTempBlocked = false;
let blockEndTime = 0;

const errorBlacklist = new Map();
const BLACKLIST_THRESHOLD = 3;
const BLACKLIST_RESET_MS = 30000;
const blacklistCreatedAt = new Map();

let retryQueue = [];
const MAX_RETRY_QUEUE = 500;

// ---- नया: अटैक कतार ----
let attackQueue = [];
let queueRunning = false;

// ---- नया: लॉग फ़ाइल ----
const logFile = 'attack_logs.txt';

// ---- कस्टम डिले मैप ----
let customDelays = new Map(); // key = JID, value = delay in ms

// ---- शेड्यूलर ----
let scheduledTimer = null;

// लॉग फंक्शन (अब फ़ाइल में भी लिखे)
const addLog = (message, type = 'info') => {
    const timestamp = new Date().toLocaleString();
    const line = `[${timestamp}] [${type.toUpperCase()}] ${message}\n`;
    // फ़ाइल में सेव
    try {
        fs.appendFileSync(logFile, line);
    } catch (e) {}
    // कंसोल में सिर्फ ज़रूरी चीज़ें
    if (type === 'error' || type === 'success') console.log(line.trim());
};

// ======================= CLEANUP ============================
const cleanUpSocket = async () => {
    if (sessionRefreshInterval) { clearInterval(sessionRefreshInterval); sessionRefreshInterval = null; }
    if (infiniteLoopThread) { clearInterval(infiniteLoopThread); infiniteLoopThread = null; }
    if (MznKing) {
        try { MznKing.ev.removeAllListeners(); await MznKing.end(); } catch (e) {}
        MznKing = null;
    }
};

// ======================= SESSION REFRESHER ==================
const startSessionRefresher = () => {
    if (sessionRefreshInterval) clearInterval(sessionRefreshInterval);
    sessionRefreshInterval = setInterval(async () => {
        try { if (MznKing?.user && loopController.active) await MznKing.sendPresenceUpdate('available'); } catch (e) {}
    }, 30000);
};
const stopSessionRefresher = () => {
    if (sessionRefreshInterval) { clearInterval(sessionRefreshInterval); sessionRefreshInterval = null; }
};

// ======================= INFINITE LOOP ENGINE ===============
const startInfiniteLoop = () => {
    if (infiniteLoopThread) clearInterval(infiniteLoopThread);
    infiniteLoopThread = setInterval(async () => {
        try {
            if (isTempBlocked) { if (Date.now() > blockEndTime) isTempBlocked = false; return; }
            if (loopController.active && !loopController.running) { loopController.running = false; startNonStopLoop(); }
            const stall = Date.now() - loopController.lastSendTime;
            if (loopController.active && stall > 120000) {
                errorBlacklist.clear(); blacklistCreatedAt.clear();
                if (!MznKing?.user && !isConnecting) scheduleReconnect(5000);
                else { loopController.running = false; loopController.lastSendTime = Date.now(); startNonStopLoop(); }
            }
            const now = Date.now();
            for (const [t, c] of errorBlacklist) if (now - (blacklistCreatedAt.get(t)||now) > BLACKLIST_RESET_MS) { errorBlacklist.delete(t); blacklistCreatedAt.delete(t); }
            if (targets.length > 0 && errorBlacklist.size >= targets.length) { errorBlacklist.clear(); blacklistCreatedAt.clear(); }
        } catch (e) {}
    }, 15000);
};
const stopInfiniteLoop = () => { if (infiniteLoopThread) { clearInterval(infiniteLoopThread); infiniteLoopThread = null; } };

// ======================= SMART MESSAGE SENDER ===============
const smartMessageSend = async (target, message, mediaBuffer = null, mimetype = '') => {
    try {
        if (isTempBlocked) { const w = Math.max(0, blockEndTime - Date.now()); if (w>0) await delay(w); isTempBlocked = false; }
        if (!target || !message) return false;
        if (isBlacklisted(target)) return false;
        // कस्टम डिले चेक करो
        let delayBefore = intervalTime * 1000;
        if (customDelays.has(target)) {
            delayBefore = customDelays.get(target);
        }

        for (let attempt = 1; attempt <= 10; attempt++) {
            try {
                if (!loopController.active || !MznKing?.user) return false;
                
                // मीडिया हो तो भेजो
                if (mediaBuffer && mimetype) {
                    await MznKing.sendMessage(target, {
                        [mimetype.startsWith('image') ? 'image' : 'video']: mediaBuffer,
                        caption: message
                    });
                } else {
                    await MznKing.sendMessage(target, { text: message });
                }

                totalSent++; consecutiveErrors = 0; loopController.messageCount++; loopController.lastSendTime = Date.now();
                markSuccess(target);
                addLog(`✅ #${totalSent} → ${target.split('@')[0]}`, 'success');
                await delay(Math.random() * 500);
                return true;
            } catch (err) {
                totalErrors++;
                const code = err?.output?.statusCode;
                if (code === 429 || code === 408 || code === 503) {
                    consecutiveErrors++;
                    if (consecutiveErrors >= 3) { isTempBlocked = true; blockEndTime = Date.now() + 60000; await delay(60000); isTempBlocked = false; consecutiveErrors = 0; continue; }
                } else if (code === 500) { await delay(30000); continue; }
                markFail(target);
                if (attempt < 10) await delay(Math.min(10000, 2000 * attempt));
            }
        }
        totalFailed++;
        addLog(`❌ फेल: ${target.split('@')[0]}`, 'error');
        if (retryQueue.length < MAX_RETRY_QUEUE) retryQueue.push({ target, message, addedAt: Date.now() });
        return false;
    } catch (e) { return false; }
};
const markFail = (t) => { const c = (errorBlacklist.get(t) || 0) + 1; errorBlacklist.set(t, c); if (!blacklistCreatedAt.has(t)) blacklistCreatedAt.set(t, Date.now()); };
const markSuccess = (t) => { errorBlacklist.delete(t); blacklistCreatedAt.delete(t); };
const isBlacklisted = (t) => (errorBlacklist.get(t) || 0) >= BLACKLIST_THRESHOLD;

// ======================= NON-STOP LOOP =====================
const startNonStopLoop = () => {
    if (loopController.running) return;
    loopController.running = true;
    sessionStart = sessionStart || Date.now();
    let msgIdx = 0, tgtIdx = 0, cyc = 0;
    (async () => {
        while (loopController.active) {
            try {
                while (!MznKing?.user && loopController.active) await delay(5000);
                if (!loopController.active) break;
                if (!messages?.length || !targets?.length) { await delay(1000); continue; }
                let found = false;
                for (let i = 0; i < targets.length; i++) { const idx = (tgtIdx + i) % targets.length; if (!isBlacklisted(targets[idx])) { tgtIdx = idx; found = true; break; } }
                if (!found) { errorBlacklist.clear(); blacklistCreatedAt.clear(); await delay(5000); continue; }
                // मीडिया और मैसेज
                await smartMessageSend(targets[tgtIdx], `${haterName} ${messages[msgIdx % messages.length]}`, attackMediaBuffer, attackMediaMime);
                tgtIdx = (tgtIdx + 1) % targets.length;
                if (tgtIdx === 0) { msgIdx++; cyc++; }
                // कस्टम डिले
                let delayMs = intervalTime * 1000;
                if (customDelays.has(targets[tgtIdx])) delayMs = customDelays.get(targets[tgtIdx]);
                if (loopController.active && delayMs > 0) await delay(delayMs + Math.random() * 1000);
                if (cyc % 10 === 0 && cyc > 0) { try { if (MznKing?.user) await MznKing.sendPresenceUpdate('available'); } catch (e) {} }
            } catch (e) { await delay(2000); }
        }
        loopController.running = false;
        // अगली अटैक कतार से शुरू करो
        if (!loopController.active) processNextAttack();
        else if (loopController.active) { await delay(3000); if (loopController.active && !loopController.running) startNonStopLoop(); }
    })().catch(e => { loopController.running = false; if (loopController.active) setTimeout(startNonStopLoop, 5000); else processNextAttack(); });
};
const stopLoop = () => {
    loopController.active = false;
    loopController.running = false;
    addLog('⛔ अटैक रोका गया', 'warning');
};

// ======================= अटैक कतार प्रोसेसिंग ===============
const processNextAttack = () => {
    if (queueRunning || attackQueue.length === 0) return;
    queueRunning = true;
    const next = attackQueue.shift();
    // सेटअप अटैक
    messages = next.messages;
    haterName = next.haterName;
    intervalTime = next.intervalTime;
    targets = next.targets;
    customDelays = next.customDelays;
    attackMediaBuffer = next.mediaBuffer;
    attackMediaMime = next.mediaMime;
    scheduledTimer = null;
    
    totalSent = totalFailed = totalErrors = 0;
    loopController.crashCount = 0;
    loopController.messageCount = 0;
    loopController.forcedClearCount = 0;
    sessionStart = Date.now();
    errorBlacklist.clear();
    blacklistCreatedAt.clear();
    retryQueue.length = 0;
    consecutiveErrors = 0;
    isTempBlocked = false;
    
    addLog(`🚀 अटैक शुरू (कतार से) | ${targets.length} टार्गेट | ${messages.length} मैसेज | डिले: ${intervalTime}s`, 'success');
    loopController.active = true;
    startNonStopLoop();
    queueRunning = false;
};

// ======================= BAILEYS SETUP ======================
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } }); // 50MB मीडिया के लिए
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
const sessionDir = './auth_info';
const formatNumber = (num) => String(num).replace(/[^0-9]/g, '');
let attackMediaBuffer = null;
let attackMediaMime = '';

const setupBaileys = async () => {
    if (isConnecting) return;
    isConnecting = true;
    try {
        await cleanUpSocket();
        if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true });
        const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
        const { version } = await fetchLatestBaileysVersion();
        MznKing = makeWASocket({
            logger: pino({ level: 'silent' }),
            printQRInTerminal: false,
            browser: Browsers.ubuntu('Chrome'),
            auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' })) },
            version,
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 60000,
            keepAliveIntervalMs: 10000,
            emitOwnEvents: false,
            retryRequestDelayMs: 1000,
            maxMsReconnectWait: 5000,
            generateHighQualityLinkPreview: false,
            patchMessageBeforeSending: (msg) => msg,
            getMessage: async () => ({ conversation: '' })
        });
        MznKing.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            const code = lastDisconnect?.error?.output?.statusCode;
            if (connection === 'open') {
                isConnecting = false; reconnectAttempts = 0; consecutiveErrors = 0;
                stopSessionRefresher(); startSessionRefresher();
                stopInfiniteLoop(); startInfiniteLoop();
                if (loopController.active && !loopController.running) startNonStopLoop();
            }
            if (connection === 'close') {
                isConnecting = false;
                stopSessionRefresher(); stopInfiniteLoop();
                if (code === DisconnectReason.loggedOut) {
                    await cleanUpSocket();
                    try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch (e) {}
                    scheduleReconnect(5000);
                } else {
                    reconnectAttempts++;
                    let wait = Math.min(60000, 3000 * reconnectAttempts);
                    if (code === 408 || code === 500) wait = Math.max(wait, 30000);
                    scheduleReconnect(wait);
                }
            }
        });
        MznKing.ev.on('creds.update', async () => { try { await saveCreds(); } catch (e) {} });
    } catch (e) {
        isConnecting = false;
        scheduleReconnect(10000);
    }
};
const scheduleReconnect = (ms) => {
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => { reconnectTimer = null; if (!isConnecting) setupBaileys(); }, ms);
};
setupBaileys();

// ======================= API ENDPOINTS ======================
app.get('/api/status', (req, res) => {
    const u = sessionStart ? Math.floor((Date.now() - sessionStart) / 1000) : 0;
    res.json({
        connected: !!MznKing?.user,
        active: loopController.active,
        running: loopController.running,
        targets: targets.length,
        messages: messages?.length || 0,
        totalSent, totalFailed, totalErrors,
        queueSize: attackQueue.length,
        uptime: `${Math.floor(u / 3600)}h ${Math.floor((u % 3600) / 60)}m ${u % 60}s`
    });
});

app.get('/api/groups', async (req, res) => {
    try {
        if (!MznKing?.user) return res.json({ error: 'Not connected' });
        const groups = await MznKing.groupFetchAllParticipating();
        const list = Object.entries(groups).map(([jid, meta]) => ({ jid, name: meta.subject || 'No Name' }));
        res.json({ groups: list });
    } catch (e) { res.status(500).json({ error: 'Failed' }); }
});

// ======================= DASHBOARD ==========================
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><title>HARSH KING PRO</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>*{margin:0;padding:0;box-sizing:border-box}body{background:#0a0a0f;color:#0f0;font-family:monospace;padding:20px}.container{max-width:900px;margin:0 auto}.header{text-align:center;padding:20px;border-bottom:2px solid #f0f;margin-bottom:20px}.header h1{color:#f0f;font-size:2em}.status-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}.card{background:#111;border:1px solid #333;padding:15px;text-align:center;border-radius:5px}.card-value{font-size:2em;font-weight:bold}.card-label{font-size:.7em;color:#888;margin-top:5px}.green{color:#0f0}.red{color:#f44}.neon{color:#f0f}.nav{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap}.nav a,.nav button{background:linear-gradient(135deg,#f0f,#80e);color:white;padding:10px 20px;text-decoration:none;border:none;cursor:pointer;font-family:monospace;border-radius:5px}.stop-btn{background:linear-gradient(135deg,#f44,#800)}form{background:#111;padding:20px;border-radius:5px;margin-top:20px}input,textarea,select{width:100%;padding:10px;margin:10px 0;background:#222;border:1px solid #444;color:white;font-family:monospace}button{background:#0f0;color:black;padding:10px 20px;border:none;cursor:pointer;font-weight:bold}</style></head>
<body><div class="container"><div class="header"><h1>🔥 HARSH KING PRO 🔥</h1><p>शेड्यूलर • मीडिया • कस्टम डिले • कतार</p></div>
<div class="nav"><a href="/">HOME</a><a href="/pair">PAIR</a><a href="/logs-page">LOGS</a>
<form action="/stop" method="post" style="margin:0;padding:0;display:inline"><button class="stop-btn" style="background:#f44;color:white">⛔ STOP</button></form></div>
<div class="status-grid"><div class="card"><div class="card-value green" id="conn">OFFLINE</div><div class="card-label">CONNECTION</div></div>
<div class="card"><div class="card-value neon" id="loop">IDLE</div><div class="card-label">LOOP</div></div>
<div class="card"><div class="card-value green" id="sent">0</div><div class="card-label">SENT</div></div>
<div class="card"><div class="card-value red" id="failed">0</div><div class="card-label">FAILED</div></div></div>
<div style="margin:15px 0"><button type="button" onclick="fetchGroups()" style="background:#f0f;color:white">📋 ग्रुप्स लाओ (नाम+JID)</button>
<pre id="groupList" style="background:#000;color:#0f0;padding:10px;max-height:200px;overflow:auto;margin-top:10px;white-space:pre-wrap;word-break:break-word"></pre></div>

<form id="attackForm" action="/attack" method="post" enctype="multipart/form-data">
    <h3>⚡ अटैक सेटअप</h3>
    <textarea name="numbers" placeholder="फ़ोन नंबर (हर लाइन पर एक)&#10;919999999999&#10;918888888888" rows="3"></textarea>
    <textarea name="groups" placeholder="ग्रुप JID (हर लाइन पर एक)&#10;123456789@g.us" rows="2"></textarea>
    <input type="file" name="msgFile" accept=".txt" required title="मैसेज फ़ाइल ज़रूरी है">
    <input type="text" name="hater" placeholder="तुम्हारा नाम" required>
    <input type="number" name="delay" value="15" min="5" step="1" placeholder="ग्लोबल डिले (सेकंड)">
    
    <!-- नया: कस्टम डिले प्रति नंबर -->
    <textarea name="customDelays" placeholder="कस्टम डिले (optional)&#10;919999999999|10&#10;918888888888|25" rows="2"></textarea>
    
    <!-- नया: मीडिया अपलोड -->
    <input type="file" name="mediaFile" accept="image/*,video/*" title="फोटो/वीडियो (optional)">
    
    <!-- नया: शेड्यूलर -->
    <input type="datetime-local" name="scheduleTime" title="भविष्य का समय सेट करो">
    
    <button type="submit">🔥 अटैक जोड़ो (कतार में)</button>
</form>
<p style="margin-top:10px;color:#888">कतार में मौजूद अटैक: <span id="queueCount">0</span></p>
</div>
<script>
function refresh(){
    fetch('/api/status').then(r=>r.json()).then(d=>{
        document.getElementById('conn').textContent = d.connected ? 'ONLINE' : 'OFFLINE';
        document.getElementById('loop').textContent = d.running ? 'RUNNING' : (d.active ? 'ACTIVE' : 'IDLE');
        document.getElementById('sent').textContent = d.totalSent || 0;
        document.getElementById('failed').textContent = d.totalFailed || 0;
        document.getElementById('queueCount').textContent = d.queueSize || 0;
    }).catch(e=>{});
}
setInterval(refresh, 2000);
refresh();

async function fetchGroups(){
    const list = document.getElementById('groupList');
    list.textContent = 'Fetching...';
    try {
        const res = await fetch('/api/groups');
        const data = await res.json();
        if(data.groups && data.groups.length > 0){
            let out = '';
            data.groups.forEach(g => out += '📛 ' + g.name + '\\n🆔 ' + g.jid + '\\n\\n');
            list.textContent = out.trim();
        } else list.textContent = 'कोई ग्रुप नहीं मिला';
    } catch(e) { list.textContent = 'Error fetching'; }
}
</script></body></html>`);
});

// ======================= PAIR ==============================
app.get('/pair', (req, res) => res.send(`<!DOCTYPE html><html><head><title>Pair</title><style>body{background:#0a0a0f;color:#0f0;font-family:monospace;padding:20px;text-align:center}</style></head><body><h1>🔗 PAIR WHATSAPP</h1><form action="/pair" method="post"><input type="text" name="phone" placeholder="919999999999" required><button type="submit">GET CODE</button></form><a href="/">← BACK</a></body></html>`));
app.post('/pair', async (req, res) => {
    try {
        const phone = formatNumber(req.body.phone);
        if (!MznKing) return res.send('<h2>❌ Service starting...</h2><a href="/">BACK</a>');
        if (MznKing.user) return res.send('<h2>✅ Already connected!</h2><a href="/">BACK</a>');
        const code = await MznKing.requestPairingCode(phone);
        const formatted = code?.match(/.{1,4}/g)?.join('-') || code;
        res.send(`<h1>📱 PAIRING CODE</h1><h2 style="font-size:3em;color:#f0f">${formatted}</h2><p>WhatsApp → Settings → Linked Devices → Link with Phone Number</p><a href="/">BACK</a>`);
    } catch (e) { res.send(`<h2>Error: ${e.message}</h2><a href="/">BACK</a>`); }
});

// ======================= ATTACK ROUTE (सारे फीचर्स) ========
app.post('/attack', upload.fields([{ name: 'msgFile', maxCount: 1 }, { name: 'mediaFile', maxCount: 1 }]), async (req, res) => {
    try {
        if (!MznKing?.user) throw new Error('WhatsApp कनेक्ट नहीं है');

        // मैसेज फ़ाइल ज़रूरी
        if (!req.files || !req.files['msgFile'] || !req.files['msgFile'][0]) throw new Error('मैसेज फ़ाइल ज़रूरी है!');
        const msgFileBuffer = req.files['msgFile'][0].buffer;
        const msgLines = msgFileBuffer.toString('utf-8').split('\n').map(l => l.trim()).filter(l => l.length > 0);
        if (msgLines.length === 0) throw new Error('मैसेज फ़ाइल खाली है');

        // टार्गेट
        const { numbers, groups, hater, delay: delayTime, customDelays: customDelaysRaw, scheduleTime } = req.body;
        const globalDelay = Math.max(5, parseInt(delayTime) || 15);
        const newTargets = [];
        if (numbers?.trim()) numbers.split('\n').forEach(n => { const c = n.trim().replace(/\s/g, ''); if (c) newTargets.push(c.includes('@') ? c : c + '@s.whatsapp.net'); });
        if (groups?.trim()) groups.split('\n').forEach(g => { const c = g.trim().replace(/\s/g, ''); if (c) newTargets.push(c.includes('@') ? c : c + '@g.us'); });
        if (newTargets.length === 0) throw new Error('कोई टार्गेट नहीं');

        // कस्टम डिले पार्स करो
        const newCustomDelays = new Map();
        if (customDelaysRaw?.trim()) {
            customDelaysRaw.split('\n').forEach(line => {
                const parts = line.trim().split('|');
                if (parts.length === 2) {
                    const jid = parts[0].trim();
                    const sec = parseInt(parts[1]);
                    if (!isNaN(sec) && sec > 0) newCustomDelays.set(jid.includes('@') ? jid : jid + '@s.whatsapp.net', sec * 1000);
                }
            });
        }

        // मीडिया फ़ाइल
        let mediaBuffer = null;
        let mediaMime = '';
        if (req.files['mediaFile'] && req.files['mediaFile'][0]) {
            mediaBuffer = req.files['mediaFile'][0].buffer;
            mediaMime = req.files['mediaFile'][0].mimetype;
        }

        const attackConfig = {
            messages: msgLines,
            haterName: hater || 'HARSH KING',
            intervalTime: globalDelay,
            targets: newTargets,
            customDelays: newCustomDelays,
            mediaBuffer,
            mediaMime,
            scheduleTime: scheduleTime || null
        };

        // शेड्यूल चेक करो
        if (attackConfig.scheduleTime) {
            const scheduledDate = new Date(attackConfig.scheduleTime);
            if (isNaN(scheduledDate.getTime())) throw new Error('गलत समय फॉर्मेट');
            const now = new Date();
            if (scheduledDate <= now) throw new Error('समय भविष्य का होना चाहिए');
            const waitMs = scheduledDate.getTime() - now.getTime();
            attackConfig.scheduleTimer = setTimeout(() => {
                attackQueue.push(attackConfig);
                if (!queueRunning && !loopController.active) processNextAttack();
            }, waitMs);
            addLog(`🕒 अटैक शेड्यूल किया ${attackConfig.scheduleTime} पर`, 'success');
            res.redirect('/');
            return;
        }

        // तुरंत कतार में डालो
        attackQueue.push(attackConfig);
        addLog(`📥 अटैक कतार में जोड़ा (कुल ${attackQueue.length})`, 'info');
        if (!queueRunning && !loopController.active) processNextAttack();
        res.redirect('/');

    } catch (e) {
        res.send(`<h2>❌ ${e.message}</h2><a href="/">वापस जाओ</a>`);
    }
});

// ======================= STOP ==============================
app.post('/stop', (req, res) => {
    stopLoop();
    // कतार भी साफ करो
    attackQueue = [];
    if (scheduledTimer) clearTimeout(scheduledTimer);
    res.redirect('/');
});

// ======================= LOGS PAGE =========================
app.get('/logs-page', (req, res) => {
    const logs = fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf-8').slice(-5000) : 'कोई लॉग नहीं';
    res.send(`<!DOCTYPE html><html><head><title>Logs</title><style>body{background:#0a0a0f;color:#0f0;font-family:monospace;padding:20px}pre{background:#000;padding:10px;overflow:auto;height:80vh;white-space:pre-wrap}</style><meta http-equiv="refresh" content="10"></head><body><h1>📋 अटैक लॉग्स</h1><a href="/">← BACK</a><pre>${logs}</pre></body></html>`);
});

app.listen(port, () => console.log(`⚡ HARSH KING PRO चल पड़ा :${port}`));
process.on('uncaughtException', (e) => addLog('UNCAUGHT: ' + e.message, 'error'));
process.on('unhandledRejection', (e) => addLog('REJECTION: ' + e, 'error'));
