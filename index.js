const express = require('express');
const fs      = require('fs');
const pino    = require('pino');
const multer  = require('multer');
const {
    makeWASocket,
    useMultiFileAuthState,
    delay,
    DisconnectReason,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    Browsers
} = require('@whiskeysockets/baileys');

const app  = express();
const port = process.env.PORT || 20023;

// ════════════════════════════════════════════════════════════
//  GLOBAL STATE
// ════════════════════════════════════════════════════════════
let MznKing                 = null;
let messages                = null;
let targets                 = [];
let intervalTime            = null;
let haterName               = null;
let reconnectAttempts       = 0;
let isConnecting            = false;
let keepAliveInterval       = null;
let connectionCheckInterval = null;
let reconnectTimer          = null;
let infiniteLoopThread      = null;
let sessionRefreshInterval  = null;

const loopController = { 
    active: false, 
    running: false, 
    crashCount: 0, 
    lastActivityTime: Date.now(),
    lastSendTime: Date.now(),
    forcedClearCount: 0,
    messageCount: 0
};

let totalSent      = 0;
let totalFailed    = 0;
let totalRecovered = 0;
let totalErrors    = 0;
let sessionStart   = null;

// Rate limiting detection
let consecutiveErrors = 0;
let isTempBlocked = false;
let blockEndTime = 0;

const errorBlacklist      = new Map();
const BLACKLIST_THRESHOLD = 3;
const BLACKLIST_RESET_MS  = 30000;
const blacklistTimeouts   = new Map();
const blacklistCreatedAt  = new Map();

let retryQueue      = [];
const MAX_RETRY_QUEUE = 500;

let liveLogs    = [];
const MAX_LOGS  = 200;

const addLog = (message, type = 'info') => {
    try {
        const timestamp = new Date().toLocaleTimeString();
        liveLogs.unshift({ timestamp, message, type });
        if (liveLogs.length > MAX_LOGS) {
            liveLogs.length = MAX_LOGS;
        }
        console.log(`[${timestamp}] [${type.toUpperCase()}] ${message}`);
    } catch (e) {}
};

// ════════════════════════════════════════════════════════════
//  SESSION REFRESHER - Prevents session corruption
// ════════════════════════════════════════════════════════════
const startSessionRefresher = () => {
    if (sessionRefreshInterval) clearInterval(sessionRefreshInterval);
    sessionRefreshInterval = setInterval(async () => {
        try {
            if (MznKing?.user && loopController.active) {
                await MznKing.sendPresenceUpdate('available').catch(() => {});
                if (loopController.messageCount % 30 === 0 && loopController.messageCount > 0) {
                    await delay(1000).catch(() => {});
                }
            }
        } catch (e) {
            // ignore – session refresh fails silently
        }
    }, 30000);
};

// ════════════════════════════════════════════════════════════
//  INFINITE LOOP ENGINE - ULTRA AGGRESSIVE
// ════════════════════════════════════════════════════════════
const startInfiniteLoop = () => {
    if (infiniteLoopThread) clearInterval(infiniteLoopThread);
    
    addLog('🚀 ULTRA INFINITE ENGINE STARTED', 'success');
    
    infiniteLoopThread = setInterval(async () => {
        try {
            // Check for temporary block
            if (isTempBlocked) {
                if (Date.now() > blockEndTime) {
                    isTempBlocked = false;
                    addLog('[UNBLOCK] Temporary block lifted - resuming', 'success');
                }
                return;
            }
            
            // Force restart if loop should run but isn't
            if (loopController.active && !loopController.running) {
                addLog('[FORCE] Loop dead - restarting NOW', 'warning');
                loopController.running = false;
                startNonStopLoop();
            }
            
            // Check send stall
            const timeSinceLastSend = Date.now() - loopController.lastSendTime;
            if (loopController.active && timeSinceLastSend > 60000) {
                addLog(`[STALL] ${Math.floor(timeSinceLastSend/1000)}s stall - emergency reset`, 'error');
                
                // Emergency clear everything
                errorBlacklist.clear();
                blacklistCreatedAt.clear();
                blacklistTimeouts.forEach(t => clearTimeout(t));
                blacklistTimeouts.clear();
                
                // Reset connection if needed
                if (!MznKing?.user) {
                    scheduleReconnect(1000);
                }
                
                loopController.running = false;
                loopController.lastSendTime = Date.now();
                startNonStopLoop();
            }
            
            // Auto clear blacklisted targets
            const now = Date.now();
            for (const [target, count] of errorBlacklist) {
                const createdAt = blacklistCreatedAt.get(target) || now;
                if (now - createdAt > BLACKLIST_RESET_MS) {
                    errorBlacklist.delete(target);
                    blacklistCreatedAt.delete(target);
                    addLog(`[AUTO-CLEAR] ${target.split('@')[0]}`, 'info');
                }
            }
            
            // If all targets blacklisted, force clear all
            if (targets.length > 0 && errorBlacklist.size >= targets.length) {
                addLog(`[EMERGENCY] All ${targets.length} targets blacklisted - MASS CLEAR`, 'warning');
                errorBlacklist.clear();
                blacklistCreatedAt.clear();
                loopController.forcedClearCount++;
            }
            
        } catch (e) {
            addLog(`Engine error: ${e.message}`, 'error');
        }
    }, 10000);
};

// ════════════════════════════════════════════════════════════
//  WAITING MESSAGES - Keeps connection alive
// ════════════════════════════════════════════════════════════
const keepConnectionAlive = async () => {
    try {
        if (MznKing?.user) {
            await MznKing.sendPresenceUpdate('available').catch(() => {});
            await MznKing.readMessages([]).catch(() => {});
        }
    } catch (e) {}
};

// ════════════════════════════════════════════════════════════
//  SMART MESSAGE SENDER - With automatic throttling
// ════════════════════════════════════════════════════════════
const smartMessageSend = async (target, message, retryCount = 0) => {
    try {
        if (isTempBlocked) {
            const waitTime = Math.max(0, blockEndTime - Date.now());
            if (waitTime > 0) {
                addLog(`[BLOCKED] Waiting ${Math.ceil(waitTime/1000)}s...`, 'warning');
                await delay(waitTime).catch(() => {});
                isTempBlocked = false;
            }
        }
        
        if (!target || !message) return false;
        if (isBlacklisted(target)) return false;
        
        // Progressive delay based on error count
        let baseDelay = intervalTime * 1000;
        if (consecutiveErrors > 3) {
            baseDelay += Math.min(30000, consecutiveErrors * 2000);
        }
        
        const MAX_RETRIES = 10;
        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                if (!loopController.active) return false;
                if (!MznKing?.user) {
                    await delay(2000).catch(() => {});
                    continue;
                }
                
                await MznKing.sendMessage(target, { text: message }).catch(e => { throw e; });
                
                const display = target.includes('@g.us') ? `Group:${target.split('@')[0].slice(-8)}` : target.split('@')[0];
                totalSent++;
                consecutiveErrors = 0;
                loopController.messageCount++;
                loopController.lastActivityTime = Date.now();
                loopController.lastSendTime = Date.now();
                markSuccess(target);
                
                if (totalSent % 10 === 0) {
                    addLog(`📨 #${totalSent} → ${display}`, 'success');
                }
                
                await delay(Math.random() * 500).catch(() => {});
                return true;
                
            } catch (err) {
                totalErrors++;
                const errMsg = err?.message || String(err);
                
                // Detect rate limiting / temporary block
                if (errMsg.includes('429') || errMsg.includes('rate') || errMsg.includes('too many')) {
                    consecutiveErrors++;
                    if (consecutiveErrors >= 3) {
                        isTempBlocked = true;
                        blockEndTime = Date.now() + 60000; // 1 minute block
                        addLog(`⚠️ TEMPORARY BLOCK - 60s cooldown`, 'error');
                        await delay(60000).catch(() => {});
                        isTempBlocked = false;
                        consecutiveErrors = 0;
                        continue;
                    }
                }
                
                addLog(`❌ Attempt ${attempt}/${MAX_RETRIES}`, 'error');
                markFail(target);
                
                if (attempt < MAX_RETRIES) {
                    const backoff = Math.min(10000, 2000 * attempt);
                    await delay(backoff).catch(() => {});
                }
            }
        }
        
        totalFailed++;
        if (retryQueue.length < MAX_RETRY_QUEUE) {
            retryQueue.push({ target, message, addedAt: Date.now() });
        }
        return false;
        
    } catch (e) {
        addLog(`Send error: ${e.message}`, 'error');
        return false;
    }
};

const markFail = (target) => {
    const count = (errorBlacklist.get(target) || 0) + 1;
    errorBlacklist.set(target, count);
    if (!blacklistCreatedAt.has(target)) blacklistCreatedAt.set(target, Date.now());
};

const markSuccess = (t) => { 
    if (errorBlacklist.has(t)) {
        errorBlacklist.delete(t);
        blacklistCreatedAt.delete(t);
    }
};

const isBlacklisted = (t) => (errorBlacklist.get(t) || 0) >= BLACKLIST_THRESHOLD;

// ════════════════════════════════════════════════════════════
//  NON-STOP LOOP - SEAMLESS CONTINUOUS
// ════════════════════════════════════════════════════════════
const startNonStopLoop = () => {
    if (loopController.running) {
        addLog('[LOOP] Already running', 'info');
        return;
    }

    loopController.running = true;
    loopController.crashCount = 0;
    loopController.lastActivityTime = Date.now();
    sessionStart = sessionStart || Date.now();

    let msgIndex = 0;
    let targetIndex = 0;
    let cycleCount = 0;

    addLog('🔥 INFINITE LOOP ENGAGED 🔥', 'success');

    (async () => {
        while (loopController.active) {
            try {
                // Wait for connection
                while (!MznKing?.user && loopController.active) {
                    addLog('[WAIT] Connection...', 'info');
                    await delay(3000).catch(() => {});
                }
                
                if (!loopController.active) break;
                if (!messages?.length || !targets?.length) {
                    await delay(1000).catch(() => {});
                    continue;
                }
                
                // Find next non-blacklisted target (with wrap-around)
                let found = false;
                for (let i = 0; i < targets.length; i++) {
                    const idx = (targetIndex + i) % targets.length;
                    if (!isBlacklisted(targets[idx])) {
                        targetIndex = idx;
                        found = true;
                        break;
                    }
                }
                
                if (!found) {
                    addLog('[CLEAR] Removing all blacklists...', 'warning');
                    errorBlacklist.clear();
                    blacklistCreatedAt.clear();
                    await delay(5000).catch(() => {});
                    continue;
                }
                
                const fullMessage = `${haterName} ${messages[msgIndex % messages.length]}`;
                const target = targets[targetIndex];
                
                await smartMessageSend(target, fullMessage);
                
                targetIndex = (targetIndex + 1) % targets.length;
                if (targetIndex === 0) {
                    msgIndex++;
                    cycleCount++;
                    if (cycleCount % 5 === 0) {
                        addLog(`📊 Cycle ${cycleCount} | Sent:${totalSent} | BL:${errorBlacklist.size}`, 'info');
                    }
                }
                
                if (loopController.active && intervalTime > 0) {
                    const jitter = Math.random() * 1000;
                    await delay((intervalTime * 1000) + jitter).catch(() => {});
                }
                
                // Keep connection alive every cycle
                if (cycleCount % 10 === 0 && cycleCount > 0) {
                    await keepConnectionAlive().catch(() => {});
                }
                
            } catch (err) {
                loopController.crashCount++;
                addLog(`⚠️ Loop error #${loopController.crashCount}: ${err.message}`, 'error');
                await delay(2000).catch(() => {});
            }
        }
        
        loopController.running = false;
        addLog('[LOOP] Stopped', 'warning');
        
        if (loopController.active) {
            addLog('[RESTART] Auto restarting in 3s...', 'warning');
            await delay(3000).catch(() => {});
            if (loopController.active && !loopController.running) {
                startNonStopLoop();
            }
        }
    })().catch(e => {
        addLog(`Loop fatal: ${e.message}`, 'error');
        loopController.running = false;
        if (loopController.active) setTimeout(startNonStopLoop, 5000);
    });
};

const stopLoop = () => {
    loopController.active = false;
    loopController.running = false;
    addLog('⛔ Loop stopped by user', 'warning');
};

// ════════════════════════════════════════════════════════════
//  BAILEYS SETUP WITH AUTO-RECOVERY
// ════════════════════════════════════════════════════════════
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const sessionDir = './auth_info';
const formatNumber = (num) => String(num).replace(/[^0-9]/g, '');

const setupBaileys = async () => {
    if (isConnecting) return;
    isConnecting = true;
    addLog('📱 Connecting to WhatsApp...', 'info');

    try {
        if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true });
        
        const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
        const { version } = await fetchLatestBaileysVersion();

        if (MznKing) {
            try { MznKing.end(); } catch(e) {}
            MznKing = null;
        }

        MznKing = makeWASocket({
            logger: pino({ level: 'silent' }),
            printQRInTerminal: false,
            browser: Browsers.ubuntu('Chrome'),
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }))
            },
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
            
            if (connection === 'open') {
                isConnecting = false;
                reconnectAttempts = 0;
                consecutiveErrors = 0;
                addLog(`✅ CONNECTED!`, 'success');
                startSessionRefresher();
                startInfiniteLoop();
                
                if (loopController.active && !loopController.running) {
                    addLog('[RESUME] Starting loop...', 'success');
                    startNonStopLoop();
                }
            }
            
            if (connection === 'close') {
                isConnecting = false;
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                addLog(`🔌 Disconnected (${statusCode})`, 'warning');
                
                if (statusCode === DisconnectReason.loggedOut) {
                    addLog('🚫 Logged out - clearing session', 'error');
                    try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch(e) {}
                    scheduleReconnect(5000);
                } else {
                    reconnectAttempts++;
                    const waitMs = Math.min(30000, 3000 * reconnectAttempts);
                    scheduleReconnect(waitMs);
                }
            }
        });

        MznKing.ev.on('creds.update', async () => {
            try { await saveCreds(); } catch(e) {}
        });

    } catch (error) {
        isConnecting = false;
        addLog(`❌ Setup error: ${error.message}`, 'error');
        scheduleReconnect(10000);
    }
};

const scheduleReconnect = (waitMs) => {
    if (reconnectTimer) clearTimeout(reconnectTimer);
    addLog(`🔄 Reconnecting in ${Math.round(waitMs/1000)}s`, 'info');
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        if (!isConnecting) setupBaileys();
    }, waitMs);
};

setupBaileys();

// ════════════════════════════════════════════════════════════
//  API ENDPOINTS
// ════════════════════════════════════════════════════════════
app.get('/api/status', (req, res) => {
    const uptimeSec = sessionStart ? Math.floor((Date.now() - sessionStart) / 1000) : 0;
    res.json({
        connected: !!MznKing?.user,
        active: loopController.active,
        running: loopController.running,
        targets: targets.length,
        messages: messages?.length || 0,
        totalSent, totalFailed, totalErrors,
        blacklistCount: errorBlacklist.size,
        retryQueueSize: retryQueue.length,
        uptime: `${Math.floor(uptimeSec/3600)}h ${Math.floor((uptimeSec%3600)/60)}m ${uptimeSec%60}s`,
        blocked: isTempBlocked,
        forcedClears: loopController.forcedClearCount
    });
});

app.get('/api/logs', (req, res) => {
    res.json({ logs: liveLogs, connected: !!MznKing?.user, active: loopController.active });
});

// Simple dashboard
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html>
<head>
    <title>MUAKAN WITH YANKI - INFINITE</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { background: #0a0a0f; color: #00ff88; font-family: monospace; padding: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        .header { text-align: center; padding: 20px; border-bottom: 2px solid #ff00ff; margin-bottom: 20px; }
        .header h1 { color: #ff00ff; font-size: 2em; }
        .status-grid { display: grid; grid-template-columns: repeat(4,1fr); gap: 10px; margin-bottom: 20px; }
        .card { background: #111; border: 1px solid #333; padding: 15px; text-align: center; border-radius: 5px; }
        .card-value { font-size: 2em; font-weight: bold; }
        .card-label { font-size: 0.7em; color: #888; margin-top: 5px; }
        .green { color: #00ff88; }
        .red { color: #ff4444; }
        .neon { color: #ff00ff; }
        .nav { display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }
        .nav a, .nav button { background: linear-gradient(135deg,#ff00ff,#8800ee); color: white; padding: 10px 20px; text-decoration: none; border: none; cursor: pointer; font-family: monospace; border-radius: 5px; }
        .stop-btn { background: linear-gradient(135deg,#ff4444,#880000); }
        form { background: #111; padding: 20px; border-radius: 5px; margin-top: 20px; }
        input, textarea, select { width: 100%; padding: 10px; margin: 10px 0; background: #222; border: 1px solid #444; color: white; font-family: monospace; }
        button { background: #00ff88; color: black; padding: 10px 20px; border: none; cursor: pointer; font-weight: bold; }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🔥 MUSKAN WITH YANKI INFINITE 🔥</h1>
        <p>WILL NEVER STOP • AUTO-HEALING • SMART THROTTLING</p>
    </div>
    
    <div class="nav">
        <a href="/">DASHBOARD</a>
        <a href="/pair">PAIR</a>
        <a href="/attack-page">ATTACK</a>
        <a href="/logs-page">LOGS</a>
        <form action="/stop" method="post" style="margin:0;padding:0;display:inline">
            <button type="submit" class="stop-btn" style="background:#ff4444;color:white">⛔ STOP</button>
        </form>
    </div>
    
    <div class="status-grid">
        <div class="card"><div class="card-value green" id="conn">OFFLINE</div><div class="card-label">CONNECTION</div></div>
        <div class="card"><div class="card-value neon" id="loop">IDLE</div><div class="card-label">LOOP</div></div>
        <div class="card"><div class="card-value green" id="sent">0</div><div class="card-label">SENT</div></div>
        <div class="card"><div class="card-value red" id="failed">0</div><div class="card-label">FAILED</div></div>
    </div>
    
    <form action="/attack" method="post" enctype="multipart/form-data">
        <h3>⚡ START INFINITE ATTACK</h3>
        <textarea name="numbers" placeholder="Phone numbers (one per line)&#10;919999999999&#10;918888888888" rows="3"></textarea>
        <textarea name="groups" placeholder="Group IDs (one per line)&#10;123456789@g.us" rows="2"></textarea>
        <input type="file" name="msgFile" accept=".txt" required>
        <input type="text" name="hater" placeholder="Your Name" required>
        <input type="number" name="delay" value="15" min="5" step="1">
        <button type="submit">🔥 START INFINITE ATTACK 🔥</button>
    </form>
</div>
<script>
function refresh(){
    fetch('/api/status').then(r=>r.json()).then(d=>{
        document.getElementById('conn').textContent = d.connected ? 'ONLINE' : 'OFFLINE';
        document.getElementById('loop').textContent = d.running ? 'RUNNING' : (d.active ? 'ACTIVE' : 'IDLE');
        document.getElementById('sent').textContent = d.totalSent || 0;
        document.getElementById('failed').textContent = d.totalFailed || 0;
    }).catch(e=>{});
}
setInterval(refresh, 2000);
refresh();
</script>
</body>
</html>`);
});

app.get('/pair', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><title>Pair WhatsApp</title><style>body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px;text-align:center}</style></head><body><h1>🔗 PAIR WHATSAPP</h1><form action="/pair" method="post"><input type="text" name="phone" placeholder="919999999999" required><button type="submit">GET CODE</button></form><a href="/">← BACK</a></body></html>`);
});

app.post('/pair', async (req, res) => {
    try {
        const phone = formatNumber(req.body.phone);
        if (!MznKing) return res.send('<h2>❌ Service starting...</h2><a href="/">BACK</a>');
        if (MznKing.user) return res.send('<h2>✅ Already connected!</h2><a href="/">BACK</a>');
        const code = await MznKing.requestPairingCode(phone);
        const formatted = code?.match(/.{1,4}/g)?.join('-') || code;
        res.send(`<h1>📱 PAIRING CODE</h1><h2 style="font-size:3em;color:#ff00ff">${formatted}</h2><p>Open WhatsApp → Settings → Linked Devices → Link with Phone Number</p><a href="/">BACK</a>`);
    } catch(e) { res.send(`<h2>Error: ${e.message}</h2><a href="/">BACK</a>`); }
});

app.get('/attack-page', (req, res) => {
    res.redirect('/');
});

app.post('/attack', upload.single('msgFile'), async (req, res) => {
    try {
        if (!MznKing?.user) throw new Error('WhatsApp not connected!');
        const { numbers, groups, hater, delay: delayTime } = req.body;
        if (!req.file) throw new Error('No message file');
        
        messages = req.file.buffer.toString('utf-8').split('\n').map(l => l.trim()).filter(l => l.length > 0);
        haterName = hater || 'HARSH KING';
        intervalTime = Math.max(5, parseInt(delayTime) || 15);
        targets = [];
        
        if (numbers?.trim()) {
            numbers.split('\n').forEach(n => {
                const c = n.trim().replace(/\s/g,'');
                if (c) targets.push(c.includes('@') ? c : c + '@s.whatsapp.net');
            });
        }
        if (groups?.trim()) {
            groups.split('\n').forEach(g => {
                const c = g.trim().replace(/\s/g,'');
                if (c) targets.push(c.includes('@') ? c : c + '@g.us');
            });
        }
        
        if (!targets.length) throw new Error('No targets!');
        
        stopLoop();
        await delay(1000).catch(() => {});
        
        totalSent = totalFailed = totalErrors = 0;
        loopController.crashCount = 0;
        loopController.messageCount = 0;
        loopController.forcedClearCount = 0;
        sessionStart = Date.now();
        errorBlacklist.clear();
        blacklistCreatedAt.clear();
        blacklistTimeouts.forEach(t => clearTimeout(t));
        blacklistTimeouts.clear();
        retryQueue.length = 0;
        consecutiveErrors = 0;
        isTempBlocked = false;
        
        addLog(`🚀 ATTACK STARTED | ${targets.length} targets | ${messages.length} messages | ${intervalTime}s delay`, 'success');
        loopController.active = true;
        startNonStopLoop();
        res.redirect('/');
    } catch(e) {
        res.send(`<h2>❌ Error: ${e.message}</h2><a href="/">BACK</a>`);
    }
});

app.post('/stop', (req, res) => {
    stopLoop();
    res.redirect('/');
});

app.get('/logs-page', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><title>Live Logs</title><style>body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px}pre{background:#000;padding:10px;overflow:auto;height:80vh}</style><meta http-equiv="refresh" content="5"></head><body><h1>📋 LIVE LOGS</h1><a href="/">← BACK</a><pre id="logs">Loading...</pre><script>setInterval(()=>{fetch('/api/logs').then(r=>r.json()).then(d=>{document.getElementById('logs').innerHTML=d.logs.map(l=>'['+l.timestamp+'] '+l.message).join('\\n')})},2000);</script></body></html>`);
});

// ════════════════════════════════════════════════════════════
//  🔥 CRITICAL: Process-level error handlers (NON-STOP FIX)
// ════════════════════════════════════════════════════════════
process.on('uncaughtException', (err) => {
    addLog(`💀 UNCAUGHT EXCEPTION: ${err.message}`, 'error');
    addLog('🔄 Restarting process in 5 seconds...', 'warning');
    // Give time to flush logs, then exit so PM2/forever can restart
    setTimeout(() => process.exit(1), 5000);
});

process.on('unhandledRejection', (reason, promise) => {
    addLog(`💀 UNHANDLED REJECTION: ${reason?.message || reason}`, 'error');
    // Do NOT exit – just log and continue (critical to prevent crash)
});

// ════════════════════════════════════════════════════════════
//  START SERVER
// ════════════════════════════════════════════════════════════
app.listen(port, () => {
    console.log(`
╔══════════════════════════════════════════════════════════╗
║  🔥 MUSKAN WITH YANKI v7.0 - INFINITE 🔥                 ║
║  ═══════════════════════════════════════════════════════║
║  ✅ SMART RATE LIMIT DETECTION                          ║
║  ✅ AUTO THROTTLING & COOLDOWN                          ║
║  ✅ WILL NEVER STOP ONCE STARTED                        ║
║  ✅ AUTO-RECOVERY FROM ANY ERROR                        ║
╚══════════════════════════════════════════════════════════╝`);
});
