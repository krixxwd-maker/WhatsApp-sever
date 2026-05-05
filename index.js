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
const port = process.env.PORT || 5000;

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

// ── Loop Controller ────────────────────────────────────────
const loopController = { active: false, running: false };

// ── Counters ───────────────────────────────────────────────
let totalSent      = 0;
let totalFailed    = 0;
let totalRecovered = 0;
let totalErrors    = 0;
let sessionStart   = null;

// ── Error Blacklist ────────────────────────────────────────
const errorBlacklist      = new Map();
const BLACKLIST_THRESHOLD = 5;
const BLACKLIST_RESET_MS  = 300000;

// ── Rate-limit State ───────────────────────────────────────
let rateLimitActive = false;
let rateLimitUntil  = 0;

// ── Retry Queue ────────────────────────────────────────────
const retryQueue      = [];
const MAX_RETRY_QUEUE = 100;

// ════════════════════════════════════════════════════════════
//  LIVE LOG SYSTEM
// ════════════════════════════════════════════════════════════
let liveLogs    = [];
const MAX_LOGS  = 300;

const addLog = (message, type = 'info') => {
    const timestamp = new Date().toLocaleTimeString();
    liveLogs.unshift({ timestamp, message, type });
    if (liveLogs.length > MAX_LOGS) liveLogs.pop();
    console.log(`[${timestamp}] [${type.toUpperCase()}] ${message}`);
};

// ════════════════════════════════════════════════════════════
//  ERROR CLASSIFICATION
// ════════════════════════════════════════════════════════════
const ErrorType = {
    RATE_LIMIT   : 'RATE_LIMIT',
    SOCKET_CLOSED: 'SOCKET_CLOSED',
    NOT_ON_WA    : 'NOT_ON_WA',
    TIMEOUT      : 'TIMEOUT',
    BAD_SESSION  : 'BAD_SESSION',
    UNKNOWN      : 'UNKNOWN'
};

const classifyError = (errMsg = '') => {
    const m = errMsg.toLowerCase();
    if (m.includes('428') || m.includes('rate') || m.includes('spam') || m.includes('too many'))
        return ErrorType.RATE_LIMIT;
    if (m.includes('closed') || m.includes('not open') || m.includes('stream') || m.includes('end'))
        return ErrorType.SOCKET_CLOSED;
    if (m.includes('not-authorized') || m.includes('not a wa') || m.includes('no session'))
        return ErrorType.NOT_ON_WA;
    if (m.includes('timeout') || m.includes('timed out'))
        return ErrorType.TIMEOUT;
    if (m.includes('bad session') || m.includes('invalid session'))
        return ErrorType.BAD_SESSION;
    return ErrorType.UNKNOWN;
};

const errorRecovery = {
    [ErrorType.RATE_LIMIT]: async () => {
        if (rateLimitActive) return;
        rateLimitActive = true;
        const cooldown  = 90000;
        rateLimitUntil  = Date.now() + cooldown;
        addLog(`[RECOVERY] Rate limit — cooling ${cooldown / 1000}s`, 'warning');
        await delay(cooldown);
        rateLimitActive = false;
        rateLimitUntil  = 0;
        totalRecovered++;
        addLog('[RECOVERY] Rate limit done — resuming!', 'success');
    },
    [ErrorType.SOCKET_CLOSED]: async () => {
        addLog('[RECOVERY] Socket closed — waiting 15s', 'warning');
        await delay(15000);
        totalRecovered++;
    },
    [ErrorType.NOT_ON_WA]: async (target) => {
        addLog(`[RECOVERY] Not on WA: ${target.split('@')[0]} — blacklisting`, 'error');
        errorBlacklist.set(target, BLACKLIST_THRESHOLD + 1);
    },
    [ErrorType.TIMEOUT]: async () => {
        addLog('[RECOVERY] Timeout — retry in 8s', 'warning');
        await delay(8000);
        totalRecovered++;
    },
    [ErrorType.BAD_SESSION]: async () => {
        addLog('[RECOVERY] Bad session — reconnecting in 5s', 'warning');
        await delay(5000);
        scheduleReconnect(3000);
    },
    [ErrorType.UNKNOWN]: async () => {
        addLog('[RECOVERY] Unknown error — pause 5s', 'warning');
        await delay(5000);
    }
};

// ════════════════════════════════════════════════════════════
//  BLACKLIST HELPERS
// ════════════════════════════════════════════════════════════
const markFail = (target) => {
    const count = (errorBlacklist.get(target) || 0) + 1;
    errorBlacklist.set(target, count);
    if (count >= BLACKLIST_THRESHOLD) {
        addLog(`[BLACKLIST] ${target.split('@')[0]} blacklisted (${count} fails)`, 'error');
        setTimeout(() => {
            errorBlacklist.delete(target);
            addLog(`[BLACKLIST] ${target.split('@')[0]} cleared`, 'info');
        }, BLACKLIST_RESET_MS);
    }
};

const markSuccess    = (t) => { if (errorBlacklist.has(t)) errorBlacklist.delete(t); };
const isBlacklisted  = (t) => (errorBlacklist.get(t) || 0) >= BLACKLIST_THRESHOLD;

// ════════════════════════════════════════════════════════════
//  UPLOAD CONFIG
// ════════════════════════════════════════════════════════════
const upload = multer({
    storage: multer.memoryStorage(),
    limits : { fileSize: 10 * 1024 * 1024 }
});

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const sessionDir   = './auth_info';
const formatNumber = (num) => num.replace(/[^0-9]/g, '');

// ════════════════════════════════════════════════════════════
//  KEEP-ALIVE
// ════════════════════════════════════════════════════════════
const startKeepAlive = () => {
    if (keepAliveInterval) clearInterval(keepAliveInterval);
    keepAliveInterval = setInterval(async () => {
        if (MznKing?.user) {
            try   { await MznKing.sendPresenceUpdate('available'); }
            catch (err) { addLog(`Keep-alive failed: ${err.message}`, 'warning'); }
        }
    }, 25000);
};

// ════════════════════════════════════════════════════════════
//  CONNECTION MONITOR
// ════════════════════════════════════════════════════════════
const startConnectionMonitor = () => {
    if (connectionCheckInterval) clearInterval(connectionCheckInterval);
    connectionCheckInterval = setInterval(() => {
        if (!MznKing?.user && !isConnecting) {
            addLog('[Monitor] Dead connection — scheduling reconnect', 'warning');
            scheduleReconnect(5000);
        }
    }, 60000);
};

// ════════════════════════════════════════════════════════════
//  NON-RECURSIVE RECONNECT SCHEDULER
// ════════════════════════════════════════════════════════════
const scheduleReconnect = (waitMs) => {
    if (isConnecting) { addLog('Reconnect skipped — already connecting', 'info'); return; }
    if (reconnectTimer) clearTimeout(reconnectTimer);
    addLog(`Reconnect in ${Math.round(waitMs / 1000)}s`, 'info');
    reconnectTimer = setTimeout(() => { reconnectTimer = null; setupBaileys(); }, waitMs);
};

// ════════════════════════════════════════════════════════════
//  SAFE MESSAGE SENDER
// ════════════════════════════════════════════════════════════
const safeMessageSend = async (target, message) => {
    if (isBlacklisted(target)) {
        addLog(`[SKIP] Blacklisted: ${target.split('@')[0]}`, 'warning');
        return false;
    }

    if (rateLimitActive) {
        const waitLeft = Math.max(0, rateLimitUntil - Date.now());
        addLog(`[HOLD] Rate limit — waiting ${Math.round(waitLeft / 1000)}s`, 'warning');
        await delay(waitLeft + 1000);
    }

    const MAX_RETRIES = 10;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        // ── FIX: loop.active check sirf manual stop ke liye ──────
        if (!loopController.active) return false;

        if (!MznKing?.user) {
            addLog(`[WAIT] No connection — attempt ${attempt}, waiting 12s`, 'warning');
            await delay(12000);
            continue;
        }

        try {
            await MznKing.sendMessage(target, { text: message });
            const display = target.includes('@g.us')
                ? `Group:${target.split('@')[0]}`
                : target.split('@')[0];
            totalSent++;
            markSuccess(target);
            addLog(`[SENT] #${totalSent} -> ${display}`, 'success');
            return true;

        } catch (err) {
            totalErrors++;
            const errMsg  = err?.message || '';
            const errType = classifyError(errMsg);
            addLog(`[ERR] Attempt ${attempt}/${MAX_RETRIES} [${errType}]: ${errMsg.substring(0, 80)}`, 'error');

            if (errorRecovery[errType]) await errorRecovery[errType](target);

            markFail(target);
            if (isBlacklisted(target)) {
                addLog(`[ABORT] ${target.split('@')[0]} blacklisted`, 'error');
                totalFailed++;
                return false;
            }

            if (attempt < MAX_RETRIES && errType !== ErrorType.RATE_LIMIT) {
                const backoff = Math.min(30000, 3000 * attempt);
                await delay(backoff);
            }
        }
    }

    totalFailed++;
    if (retryQueue.length < MAX_RETRY_QUEUE) {
        retryQueue.push({ target, message, addedAt: Date.now() });
        addLog(`[QUEUE] Queued for retry. Size: ${retryQueue.length}`, 'warning');
    }
    return false;
};

// ════════════════════════════════════════════════════════════
//  RETRY QUEUE PROCESSOR
// ════════════════════════════════════════════════════════════
const processRetryQueue = async () => {
    while (true) {
        await delay(30000);
        if (!loopController.active || retryQueue.length === 0) continue;
        if (!MznKing?.user) continue;
        const item = retryQueue.shift();
        if (!item) continue;
        if (Date.now() - item.addedAt > 600000) {
            addLog('[QUEUE] Dropped stale item (>10min)', 'warning');
            continue;
        }
        addLog(`[QUEUE] Retrying to ${item.target.split('@')[0]}`, 'info');
        const ok = await safeMessageSend(item.target, item.message);
        if (ok) totalRecovered++;
    }
};
processRetryQueue();

// ════════════════════════════════════════════════════════════
//  UNLIMITED NON-STOP LOOP
// ════════════════════════════════════════════════════════════
const startNonStopLoop = () => {
    if (loopController.running) {
        addLog('Loop already running — skip', 'warning');
        return;
    }

    loopController.active  = true;
    loopController.running = true;
    sessionStart           = sessionStart || Date.now();

    let msgIndex    = 0;
    let targetIndex = 0;
    let cycleCount  = 0;

    addLog('NON-STOP UNLIMITED LOOP STARTED!', 'success');

    (async () => {
        while (loopController.active) {
            try {
                if (!MznKing?.user) {
                    addLog('[LOOP] Waiting for connection... 10s', 'warning');
                    await delay(10000);
                    continue;
                }

                if (!messages?.length || !targets?.length) {
                    addLog('[LOOP] No messages/targets — pause 5s', 'warning');
                    await delay(5000);
                    continue;
                }

                // Find non-blacklisted target
                let targetFound = false;
                for (let i = 0; i < targets.length; i++) {
                    const checkIdx = (targetIndex + i) % targets.length;
                    if (!isBlacklisted(targets[checkIdx])) {
                        targetIndex = checkIdx;
                        targetFound = true;
                        break;
                    }
                }

                if (!targetFound) {
                    addLog('[LOOP] All targets blacklisted — wait 60s for reset', 'warning');
                    await delay(60000);
                    continue;
                }

                const fullMessage = `${haterName} ${messages[msgIndex]}`;
                const target      = targets[targetIndex];

                await safeMessageSend(target, fullMessage);

                targetIndex = (targetIndex + 1) % targets.length;
                if (targetIndex === 0) {
                    msgIndex = (msgIndex + 1) % messages.length;
                    cycleCount++;
                    addLog(`[LOOP] Cycle #${cycleCount} done. Sent:${totalSent} Failed:${totalFailed}`, 'info');
                }

                if (loopController.active) await delay(intervalTime * 1000);

            } catch (err) {
                totalErrors++;
                addLog(`[LOOP] Exception (auto-recover): ${err.message}`, 'error');
                await delay(5000);
                // ── FIX: Exception pe loop NAHI rukta, continue karta hai ──
            }
        }

        loopController.running = false;
        addLog('Loop stopped (manual stop).', 'warning');
    })();
};

// ── Sirf manual /stop pe call hoga ────────────────────────
const stopLoop = () => {
    loopController.active = false;
    addLog('Loop stop signal sent (manual)', 'warning');
};

// ════════════════════════════════════════════════════════════
//  BAILEYS SETUP — MAIN FIX IS HERE
// ════════════════════════════════════════════════════════════
const setupBaileys = async () => {
    if (isConnecting) { addLog('Already connecting — skip', 'warning'); return; }
    isConnecting = true;
    addLog('Setting up WhatsApp connection...', 'info');

    if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true });

    try {
        const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
        const { version }          = await fetchLatestBaileysVersion();

        if (MznKing) {
            try { MznKing.end(); } catch (_) {}
            MznKing = null;
        }

        MznKing = makeWASocket({
            logger: pino({ level: 'silent' }),
            printQRInTerminal: false,
            browser: Browsers.ubuntu('Chrome'),
            auth: {
                creds: state.creds,
                keys : makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }))
            },
            version,
            connectTimeoutMs:      120000,
            defaultQueryTimeoutMs: 120000,
            keepAliveIntervalMs:   25000,
            emitOwnEvents:         false,
            retryRequestDelayMs:   2000,
            getMessage: async () => ({ conversation: '' })
        });

        MznKing.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;

            if (connection === 'connecting') addLog('Connecting to WhatsApp...', 'info');

            if (connection === 'open') {
                isConnecting      = false;
                reconnectAttempts = 0;
                addLog(`Connected! User: ${MznKing.user?.name || 'Unknown'}`, 'success');
                startKeepAlive();
                startConnectionMonitor();
                // ── FIX: Reconnect ke baad loop active ho to ZAROOR resume karo ──
                if (loopController.active && !loopController.running) {
                    addLog('[AUTO-RESUME] Loop resuming after reconnect!', 'success');
                    startNonStopLoop();
                }
            }

            if (connection === 'close') {
                isConnecting = false;
                MznKing      = null;
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                addLog(`Disconnected (code: ${statusCode})`, 'warning');

                // ════════════════════════════════════════════════
                // CRITICAL FIX: LoggedOut / BadSession pe bhi
                // loop STOP NAHI hoga — session clear karke
                // reconnect karega aur loop resume hoga!
                // ════════════════════════════════════════════════
                if (statusCode === DisconnectReason.loggedOut) {
                    addLog('[RECOVERY] Logged out — clearing session, reconnecting...', 'warning');
                    // ── FIX: stopLoop() NAHI call karte ──────────────────
                    try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch (_) {}
                    reconnectAttempts++;
                    scheduleReconnect(8000);
                    return;
                }

                if (statusCode === DisconnectReason.badSession) {
                    addLog('[RECOVERY] Bad session — clearing, reconnecting...', 'warning');
                    try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch (_) {}
                    reconnectAttempts++;
                    scheduleReconnect(5000);
                    return;
                }

                // Other disconnects: exponential backoff reconnect
                reconnectAttempts++;
                const waitMs = Math.min(120000, 3000 * reconnectAttempts);
                addLog(`Reconnect #${reconnectAttempts} in ${Math.round(waitMs / 1000)}s`, 'info');
                scheduleReconnect(waitMs);
            }
        });

        MznKing.ev.on('creds.update', saveCreds);

        MznKing.ev.on('error', (err) => {
            addLog(`Socket error (auto-handled): ${err?.message || err}`, 'error');
            // ── FIX: Error pe crash nahi, bas log karo ──
        });

    } catch (error) {
        isConnecting = false;
        addLog(`Setup error: ${error.message}`, 'error');
        scheduleReconnect(15000);
    }
};

setupBaileys();

// ════════════════════════════════════════════════════════════
//  API ENDPOINTS
// ════════════════════════════════════════════════════════════
app.get('/api/logs', (_req, res) => {
    res.json({
        logs:      liveLogs,
        active:    loopController.active,
        connected: !!(MznKing?.user)
    });
});

app.get('/api/status', (_req, res) => {
    const uptimeSec = sessionStart ? Math.floor((Date.now() - sessionStart) / 1000) : 0;
    const h = Math.floor(uptimeSec / 3600);
    const m = Math.floor((uptimeSec % 3600) / 60);
    const s = uptimeSec % 60;
    const uptime = sessionStart
        ? `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`
        : '--:--:--';

    res.json({
        connected:        !!(MznKing?.user),
        sendingActive:    loopController.active,
        loopRunning:      loopController.running,
        targets:          targets.length,
        messages:         messages?.length || 0,
        reconnectAttempts,
        totalSent,
        totalFailed,
        totalErrors,
        totalRecovered,
        retryQueueSize:   retryQueue.length,
        blacklistCount:   errorBlacklist.size,
        rateLimitActive,
        uptime,
        userName:         MznKing?.user?.name || ''
    });
});

app.get('/api/groups', async (_req, res) => {
    if (!MznKing?.user) return res.json({ error: 'Not connected', groups: [] });
    try {
        const groups    = await MznKing.groupFetchAllParticipating();
        const groupList = Object.entries(groups).map(([uid, data]) => ({
            uid, name: data.subject, participants: data.participants?.length || 0
        }));
        addLog(`Fetched ${groupList.length} groups`, 'info');
        res.json({ groups: groupList });
    } catch (e) {
        addLog(`Group fetch failed: ${e.message}`, 'error');
        res.json({ error: e.message, groups: [] });
    }
});

app.post('/clear-blacklist', (_req, res) => {
    const count = errorBlacklist.size;
    errorBlacklist.clear();
    addLog(`Blacklist cleared (${count} entries)`, 'success');
    res.json({ ok: true, cleared: count });
});

app.post('/clear-queue', (_req, res) => {
    const count       = retryQueue.length;
    retryQueue.length = 0;
    addLog(`Retry queue cleared (${count} items)`, 'success');
    res.json({ ok: true, cleared: count });
});

// ════════════════════════════════════════════════════════════
//  MAIN PAGE
// ════════════════════════════════════════════════════════════
app.get('/', (_req, res) => {
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>HARSH KING ULTRA v5</title>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700;900&family=Rajdhani:wght@400;600;700&display=swap" rel="stylesheet"/>
<style>
:root{--neon:#ff00ff;--cyan:#00ffff;--green:#00ff66;--red:#ff0033;--yellow:#ffcc00;--blue:#0099ff;--orange:#ff6600;--bg:#030305;--panel:#080810;--border:#16162a;--text:#c8c8d8;}
*{margin:0;padding:0;box-sizing:border-box;}
body{background:var(--bg);font-family:'Rajdhani',sans-serif;min-height:100vh;color:var(--text);overflow-x:hidden;}
body::before{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(255,0,255,0.025) 1px,transparent 1px),linear-gradient(90deg,rgba(255,0,255,0.025) 1px,transparent 1px);background-size:40px 40px;pointer-events:none;z-index:0;}
body::after{content:'';position:fixed;inset:0;background:radial-gradient(ellipse at 50% 0%,rgba(255,0,255,0.06) 0%,transparent 60%);pointer-events:none;z-index:0;}
.wrapper{position:relative;z-index:1;padding:14px;max-width:1700px;margin:0 auto;}
.header{text-align:center;padding:18px 0 14px;border-bottom:1px solid rgba(255,0,255,0.3);margin-bottom:14px;}
.header h1{font-family:'Orbitron',monospace;font-size:clamp(16px,3.5vw,36px);font-weight:900;letter-spacing:6px;color:var(--neon);text-shadow:0 0 40px var(--neon),0 0 80px rgba(255,0,255,0.3);animation:flicker 5s infinite;}
.header .ver{font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--yellow);letter-spacing:5px;margin-top:4px;opacity:0.8;}
.header .sub{font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--cyan);letter-spacing:3px;margin-top:3px;opacity:0.5;}
@keyframes flicker{0%,88%,100%{opacity:1}90%{opacity:0.7}93%{opacity:1}96%{opacity:0.5}98%{opacity:1}}
.status-row{display:grid;grid-template-columns:repeat(5,1fr);gap:7px;margin-bottom:10px;}
.status-row2{display:grid;grid-template-columns:repeat(5,1fr);gap:7px;margin-bottom:14px;}
.stat-card{background:var(--panel);border:1px solid var(--border);border-top:2px solid #222;border-radius:4px;padding:10px 6px;text-align:center;transition:border-top-color 0.3s;}
.stat-card .val{font-family:'Orbitron',monospace;font-size:clamp(13px,1.8vw,22px);font-weight:700;color:#444;transition:color 0.3s;}
.stat-card .lbl{font-family:'Share Tech Mono',monospace;font-size:8px;color:#333;letter-spacing:2px;margin-top:3px;}
.s-neon{border-top-color:var(--neon)!important;}.s-neon .val{color:var(--neon)!important;}
.s-green{border-top-color:var(--green)!important;}.s-green .val{color:var(--green)!important;}
.s-red{border-top-color:var(--red)!important;}.s-red .val{color:var(--red)!important;}
.s-cyan{border-top-color:var(--cyan)!important;}.s-cyan .val{color:var(--cyan)!important;}
.s-yellow{border-top-color:var(--yellow)!important;}.s-yellow .val{color:var(--yellow)!important;}
.s-blue{border-top-color:var(--blue)!important;}.s-blue .val{color:var(--blue)!important;}
.s-orange{border-top-color:var(--orange)!important;}.s-orange .val{color:var(--orange)!important;}
.main-grid{display:grid;grid-template-columns:320px 1fr 360px;gap:10px;align-items:start;}
@media(max-width:1100px){.main-grid{grid-template-columns:1fr 1fr;}.col-logs{grid-column:1/-1;}}
@media(max-width:680px){.main-grid{grid-template-columns:1fr;}.status-row,.status-row2{grid-template-columns:repeat(3,1fr);}}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:6px;overflow:hidden;margin-bottom:10px;}
.ph{background:rgba(255,0,255,0.05);border-bottom:1px solid var(--border);padding:9px 12px;display:flex;align-items:center;gap:8px;}
.ph-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;}
.ph>span:first-of-type{font-family:'Share Tech Mono',monospace;font-size:10px;letter-spacing:3px;color:var(--neon);font-weight:bold;}
.pb{padding:12px;}
.fg{margin-bottom:10px;}
.fg label{display:block;font-family:'Share Tech Mono',monospace;font-size:9px;color:#555;letter-spacing:2px;margin-bottom:4px;text-transform:uppercase;}
.fg input,.fg textarea{width:100%;padding:9px 11px;background:rgba(255,255,255,0.03);border:1px solid #1a1a1a;border-radius:3px;color:var(--text);font-family:'Share Tech Mono',monospace;font-size:11px;transition:border-color 0.2s;}
.fg input:focus,.fg textarea:focus{outline:none;border-color:var(--neon);box-shadow:0 0 8px rgba(255,0,255,0.15);}
.fg input[type=file]{padding:7px;cursor:pointer;}
.fg textarea{resize:vertical;min-height:65px;}
.btn{width:100%;padding:11px;border:none;border-radius:3px;font-family:'Orbitron',monospace;font-size:10px;font-weight:700;letter-spacing:2px;cursor:pointer;transition:all 0.2s;margin-bottom:7px;text-transform:uppercase;}
.btn:hover{transform:translateY(-1px);filter:brightness(1.15);}
.btn:active{transform:translateY(0);}
.btn-sm{padding:7px 14px;font-size:9px;width:auto;margin-bottom:0;}
.btn-pair{background:linear-gradient(135deg,#0055ee,#00bbff);color:#fff;box-shadow:0 2px 16px rgba(0,85,238,0.25);}
.btn-fetch{background:linear-gradient(135deg,#5500bb,#8800ee);color:#fff;}
.btn-start{background:linear-gradient(135deg,#009933,#00ee55);color:#000;box-shadow:0 2px 16px rgba(0,238,85,0.25);}
.btn-stop{background:linear-gradient(135deg,#aa0022,#ee0033);color:#fff;}
.btn-warn{background:linear-gradient(135deg,#885500,#ffaa00);color:#000;}
.btn-info{background:linear-gradient(135deg,#003366,#0077cc);color:#fff;}
.log-terminal{background:#000;font-family:'Share Tech Mono',monospace;font-size:10.5px;height:500px;overflow-y:auto;padding:8px;scrollbar-width:thin;scrollbar-color:var(--neon) #0a0a0a;}
.log-terminal::-webkit-scrollbar{width:3px;}
.log-terminal::-webkit-scrollbar-thumb{background:var(--neon);}
.log-entry{padding:3px 6px;margin:2px 0;border-left:2px solid;border-radius:0 2px 2px 0;animation:fadeIn 0.15s ease;}
@keyframes fadeIn{from{opacity:0;transform:translateX(-3px)}to{opacity:1;transform:translateX(0)}}
.ls{border-color:var(--green);color:var(--green);}
.le{border-color:var(--red);color:var(--red);}
.lw{border-color:var(--yellow);color:var(--yellow);}
.li{border-color:#333;color:#666;}
.log-ts{color:#2a2a2a;margin-right:5px;}
.group-list{max-height:360px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:#8800ee #0a0a0a;}
.group-item{border:1px solid var(--border);border-left:3px solid #8800ee;border-radius:3px;padding:9px;margin-bottom:7px;background:rgba(136,0,238,0.04);}
.group-item:hover{border-left-color:var(--neon);}
.g-name{font-weight:700;font-size:12px;color:#bb77ff;margin-bottom:3px;}
.g-pts{font-family:'Share Tech Mono',monospace;font-size:8px;color:#444;margin-bottom:5px;}
.uid-in{width:100%;padding:5px 7px;background:#030305;border:1px solid #1a1a2a;border-radius:2px;color:var(--cyan);font-family:'Share Tech Mono',monospace;font-size:10px;cursor:pointer;}
.c-hint{font-family:'Share Tech Mono',monospace;font-size:8px;color:#222;text-align:right;margin-top:2px;}
.no-items{text-align:center;padding:24px;font-family:'Share Tech Mono',monospace;font-size:10px;color:#2a2a2a;}
.divider{border:none;border-top:1px solid var(--border);margin:10px 0;}
.log-tb{display:flex;align-items:center;justify-content:space-between;padding:5px 12px;border-bottom:1px solid var(--border);background:rgba(0,0,0,0.4);}
.log-tb span{font-family:'Share Tech Mono',monospace;font-size:9px;color:#333;}
.scroll-btn{padding:3px 9px;background:transparent;border:1px solid #222;border-radius:2px;color:#444;font-family:'Share Tech Mono',monospace;font-size:8px;cursor:pointer;letter-spacing:1px;}
.scroll-btn.on{border-color:var(--green);color:var(--green);}
.spinner{display:inline-block;width:12px;height:12px;border:2px solid #222;border-top-color:var(--neon);border-radius:50%;animation:spin 0.7s linear infinite;vertical-align:middle;}
@keyframes spin{to{transform:rotate(360deg)}}
.info-box{margin-top:10px;padding:10px;background:rgba(255,0,255,0.04);border:1px solid rgba(255,0,255,0.08);border-radius:3px;}
.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;font-family:'Share Tech Mono',monospace;font-size:10px;margin-top:7px;}
.recovery-badge{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:2px;font-family:'Share Tech Mono',monospace;font-size:8px;letter-spacing:1px;}
.rb-active{background:rgba(255,204,0,0.1);border:1px solid var(--yellow);color:var(--yellow);}
.rb-ok{background:rgba(0,255,102,0.1);border:1px solid var(--green);color:var(--green);}
.rb-dot{width:5px;height:5px;border-radius:50%;animation:pulse 0.8s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.2}}
.action-row{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;}
.uptime-badge{font-family:'Share Tech Mono',monospace;font-size:9px;color:var(--cyan);background:rgba(0,255,255,0.06);border:1px solid rgba(0,255,255,0.15);border-radius:2px;padding:2px 8px;display:inline-block;}
</style>
</head>
<body>
<div class="wrapper">
<div class="header">
    <h1>HARSH KING ULTRA</h1>
    <div class="ver">VERSION 5.1 - UNLIMITED NON-STOP + NEVER-STOP AUTO RECOVERY</div>
    <div class="sub">FULLY FIXED - LOGGEDOUT / BADSESSION = AUTO RECONNECT, LOOP NEVER STOPS</div>
</div>

<div class="status-row">
    <div class="stat-card" id="connCard"><div class="val" id="connVal">--</div><div class="lbl">CONNECTION</div></div>
    <div class="stat-card" id="loopCard"><div class="val" id="loopVal">IDLE</div><div class="lbl">LOOP</div></div>
    <div class="stat-card s-green"><div class="val" id="sentCount">0</div><div class="lbl">TOTAL SENT</div></div>
    <div class="stat-card s-red"><div class="val" id="failCount">0</div><div class="lbl">FAILED</div></div>
    <div class="stat-card s-cyan"><div class="val" id="recovCount">0</div><div class="lbl">RECOVERED</div></div>
</div>

<div class="status-row2">
    <div class="stat-card s-yellow"><div class="val" id="errCount">0</div><div class="lbl">TOTAL ERRORS</div></div>
    <div class="stat-card s-cyan"><div class="val" id="tCount">0</div><div class="lbl">TARGETS</div></div>
    <div class="stat-card s-blue"><div class="val" id="mCount">0</div><div class="lbl">MESSAGES</div></div>
    <div class="stat-card s-orange"><div class="val" id="qCount">0</div><div class="lbl">RETRY QUEUE</div></div>
    <div class="stat-card s-red"><div class="val" id="blCount">0</div><div class="lbl">BLACKLISTED</div></div>
</div>

<div class="main-grid">
<div class="col-left">
    <div class="panel">
        <div class="ph">
            <div class="ph-dot" style="background:var(--cyan);box-shadow:0 0 5px var(--cyan);"></div>
            <span>WHATSAPP AUTH</span>
            <span id="userBadge" style="margin-left:auto;font-family:'Share Tech Mono',monospace;font-size:9px;color:#555;"></span>
        </div>
        <div class="pb">
            <form action="/pair" method="post">
                <div class="fg">
                    <label>Phone Number (with country code)</label>
                    <input type="text" name="phone" placeholder="919999999999" required/>
                </div>
                <button class="btn btn-pair" type="submit">GET PAIRING CODE</button>
            </form>
            <div style="display:flex;align-items:center;justify-content:space-between;margin-top:4px;">
                <span id="uptimeBadge" class="uptime-badge">UPTIME: --:--:--</span>
                <span id="rlBadge"></span>
            </div>
        </div>
    </div>
    <div class="panel">
        <div class="ph">
            <div class="ph-dot" style="background:#8800ee;box-shadow:0 0 5px #8800ee;"></div>
            <span>GROUP UID FETCHER</span>
            <span id="gBadge" style="margin-left:auto;font-family:'Share Tech Mono',monospace;font-size:9px;color:#555;"></span>
        </div>
        <div class="pb" style="padding-bottom:0;">
            <button class="btn btn-fetch" id="fetchBtn" type="button" onclick="fetchGroups()">FETCH ALL GROUPS</button>
            <div id="groupList" class="group-list" style="padding-bottom:12px;">
                <div class="no-items">Connect WhatsApp and click FETCH</div>
            </div>
        </div>
    </div>
</div>

<div class="col-middle">
    <div class="panel" style="margin-bottom:0;">
        <div class="ph">
            <div class="ph-dot" style="background:var(--red);box-shadow:0 0 5px var(--red);"></div>
            <span>ATTACK CONFIGURATION</span>
        </div>
        <div class="pb">
            <form action="/attack" method="post" enctype="multipart/form-data">
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                    <div class="fg">
                        <label>Target Numbers (comma separated)</label>
                        <textarea name="numbers" placeholder="919xxxxxxxxx, 918xxxxxxxxx"></textarea>
                    </div>
                    <div class="fg">
                        <label>Group UIDs (comma separated)</label>
                        <textarea name="groups" id="groupUIDInput" placeholder="123456789@g.us, ..."></textarea>
                    </div>
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
                    <div class="fg">
                        <label>Message File (.txt)</label>
                        <input type="file" name="msgFile" accept=".txt" required/>
                    </div>
                    <div class="fg">
                        <label>Hater / Prefix Name</label>
                        <input type="text" name="hater" placeholder="Name or prefix" required/>
                    </div>
                    <div class="fg">
                        <label>Delay (seconds, min 5)</label>
                        <input type="number" name="delay" min="5" value="10" required/>
                    </div>
                </div>
                <button class="btn btn-start" type="submit">START UNLIMITED NON-STOP ATTACK</button>
            </form>
            <hr class="divider"/>
            <form action="/stop" method="post" style="margin-bottom:7px;">
                <button class="btn btn-stop" type="submit">EMERGENCY STOP ALL</button>
            </form>
            <div class="action-row">
                <button class="btn btn-warn btn-sm" onclick="clearBlacklist()">CLEAR BLACKLIST</button>
                <button class="btn btn-info btn-sm" onclick="clearQueue()">CLEAR RETRY QUEUE</button>
            </div>
            <div class="info-box">
                <div style="font-family:'Share Tech Mono',monospace;font-size:9px;color:#444;letter-spacing:2px;">ENGINE v5.1 - NEVER STOP SYSTEM</div>
                <div class="info-grid">
                    <div>Loop: <span id="loopStat" style="color:#444;">STOPPED</span></div>
                    <div>User: <span id="userInfo" style="color:#aa66ff;">--</span></div>
                    <div>Rate Limit: <span id="rlStat" style="color:var(--green);">CLEAR</span></div>
                    <div>Reconnects: <span id="rCount" style="color:var(--cyan);">0</span></div>
                    <div>Blacklisted: <span id="blStat" style="color:#444;">0 targets</span></div>
                    <div>Retry Queue: <span id="qStat" style="color:#444;">0 items</span></div>
                    <div>Max Retries: <span style="color:var(--cyan);">10 per msg</span></div>
                    <div>Auto Resume: <span style="color:var(--green);">ENABLED</span></div>
                    <div>Never Stop: <span style="color:var(--green);">ENABLED</span></div>
                    <div>Keep-Alive: <span style="color:var(--green);">25s</span></div>
                </div>
            </div>
        </div>
    </div>
</div>

<div class="col-logs">
    <div class="panel" style="margin-bottom:0;">
        <div class="ph">
            <div class="ph-dot" style="background:var(--green);box-shadow:0 0 5px var(--green);"></div>
            <span>LIVE TERMINAL</span>
            <span id="logBadge" style="margin-left:auto;font-family:'Share Tech Mono',monospace;font-size:9px;color:#333;">0 ENTRIES</span>
        </div>
        <div class="log-tb">
            <span id="liveTs">-- LIVE --</span>
            <button class="scroll-btn on" id="scrollBtn" onclick="toggleScroll()">AUTO-SCROLL ON</button>
        </div>
        <div class="log-terminal" id="terminal">
            <div style="color:#1a1a1a;text-align:center;padding:18px;font-family:'Share Tech Mono',monospace;font-size:10px;">WAITING FOR LOGS...</div>
        </div>
    </div>
</div>
</div>
</div>

<script>
let autoScroll = true, lastCount = 0;
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const clsMap = { success:'ls', error:'le', warning:'lw', info:'li' };

function toggleScroll() {
    autoScroll = !autoScroll;
    const b = document.getElementById('scrollBtn');
    b.textContent = autoScroll ? 'AUTO-SCROLL ON' : 'AUTO-SCROLL OFF';
    b.className = 'scroll-btn' + (autoScroll ? ' on' : '');
}

async function clearBlacklist() {
    try { await fetch('/clear-blacklist', { method:'POST' }); } catch(e) {}
}
async function clearQueue() {
    try { await fetch('/clear-queue', { method:'POST' }); } catch(e) {}
}

async function fetchGroups() {
    const btn = document.getElementById('fetchBtn');
    const box = document.getElementById('groupList');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> FETCHING...';
    box.innerHTML = '<div class="no-items"><span class="spinner"></span> Loading...</div>';
    try {
        const r = await fetch('/api/groups');
        const d = await r.json();
        if (d.error) { box.innerHTML = '<div class="no-items" style="color:var(--red);">ERROR: ' + esc(d.error) + '</div>'; return; }
        document.getElementById('gBadge').textContent = d.groups.length + ' GROUPS';
        if (!d.groups.length) { box.innerHTML = '<div class="no-items">No groups found</div>'; return; }
        box.innerHTML = d.groups.map((g, i) =>
            '<div class="group-item">' +
            '<div class="g-name">' + (i+1) + '. ' + esc(g.name) + '</div>' +
            '<div class="g-pts">PARTICIPANTS: ' + g.participants + '</div>' +
            '<input class="uid-in" type="text" value="' + esc(g.uid) + '" readonly onclick="copyUID(this)"/>' +
            '<div class="c-hint">click to copy</div></div>'
        ).join('');
    } catch(e) {
        box.innerHTML = '<div class="no-items" style="color:var(--red);">Failed: ' + esc(e.message) + '</div>';
    } finally {
        btn.disabled = false;
        btn.textContent = 'FETCH ALL GROUPS';
    }
}

function copyUID(input) {
    input.select();
    navigator.clipboard.writeText(input.value).then(() => {
        const hint = input.nextElementSibling;
        hint.textContent = 'COPIED!'; hint.style.color = 'var(--green)';
        setTimeout(() => { hint.textContent = 'click to copy'; hint.style.color = ''; }, 2000);
        const ta = document.getElementById('groupUIDInput');
        const ex = ta.value.split(',').map(s=>s.trim()).filter(Boolean);
        if (!ex.includes(input.value)) ta.value = [...ex, input.value].join(', ');
    }).catch(() => { input.select(); document.execCommand('copy'); });
}

async function refresh() {
    try {
        const [sr, lr] = await Promise.all([fetch('/api/status'), fetch('/api/logs')]);
        const s = await sr.json();
        const l = await lr.json();

        const cc = document.getElementById('connCard'), cv = document.getElementById('connVal');
        cc.className = 'stat-card ' + (s.connected ? 's-green' : 's-red');
        cv.textContent = s.connected ? 'ONLINE' : 'OFFLINE';

        const lc = document.getElementById('loopCard'), lv = document.getElementById('loopVal');
        if (s.sendingActive && s.loopRunning) {
            lc.className = 'stat-card s-neon'; lv.textContent = 'FIRING';
        } else if (s.sendingActive && !s.loopRunning) {
            lc.className = 'stat-card s-yellow'; lv.textContent = 'RECONNECTING';
        } else {
            lc.className = 'stat-card'; lv.textContent = 'IDLE';
        }

        document.getElementById('sentCount').textContent  = s.totalSent       || 0;
        document.getElementById('failCount').textContent  = s.totalFailed      || 0;
        document.getElementById('recovCount').textContent = s.totalRecovered   || 0;
        document.getElementById('errCount').textContent   = s.totalErrors      || 0;
        document.getElementById('tCount').textContent     = s.targets          || 0;
        document.getElementById('mCount').textContent     = s.messages         || 0;
        document.getElementById('qCount').textContent     = s.retryQueueSize   || 0;
        document.getElementById('blCount').textContent    = s.blacklistCount   || 0;
        document.getElementById('rCount').textContent     = s.reconnectAttempts|| 0;

        const ls = document.getElementById('loopStat');
        ls.textContent = s.loopRunning ? 'RUNNING' : (s.sendingActive ? 'WAITING-RECONNECT' : 'STOPPED');
        ls.style.color = s.loopRunning ? 'var(--green)' : (s.sendingActive ? 'var(--yellow)' : '#444');
        document.getElementById('userInfo').textContent = s.userName || '--';
        if (s.userName) document.getElementById('userBadge').textContent = s.userName;
        document.getElementById('blStat').textContent = (s.blacklistCount || 0) + ' targets';
        document.getElementById('blStat').style.color = s.blacklistCount > 0 ? 'var(--red)' : '#444';
        document.getElementById('qStat').textContent  = (s.retryQueueSize || 0) + ' items';
        document.getElementById('qStat').style.color  = s.retryQueueSize  > 0 ? 'var(--yellow)' : '#444';

        const rlEl = document.getElementById('rlStat'), rlBadge = document.getElementById('rlBadge');
        if (s.rateLimitActive) {
            rlEl.textContent = 'COOLING'; rlEl.style.color = 'var(--red)';
            rlBadge.innerHTML = '<span class="recovery-badge rb-active"><span class="rb-dot" style="background:var(--yellow);"></span>RATE LIMIT</span>';
        } else {
            rlEl.textContent = 'CLEAR'; rlEl.style.color = 'var(--green)';
            rlBadge.innerHTML = '<span class="recovery-badge rb-ok"><span class="rb-dot" style="background:var(--green);"></span>CLEAR</span>';
        }

        document.getElementById('uptimeBadge').textContent = 'UPTIME: ' + (s.uptime || '--:--:--');

        if (l.logs && l.logs.length !== lastCount) {
            lastCount = l.logs.length;
            document.getElementById('logBadge').textContent = l.logs.length + ' ENTRIES';
            const t = document.getElementById('terminal');
            t.innerHTML = l.logs.slice(0, 200).map(e =>
                '<div class="log-entry ' + (clsMap[e.type] || 'li') + '">' +
                '<span class="log-ts">' + e.timestamp + '</span>' + esc(e.message) + '</div>'
            ).join('');
            if (autoScroll) t.scrollTop = 0;
        }
        document.getElementById('liveTs').textContent = new Date().toLocaleTimeString() + ' - LIVE';
    } catch(_) {}
}

setInterval(refresh, 1500);
refresh();
</script>
</body>
</html>`);
});

// ════════════════════════════════════════════════════════════
//  PAIRING
// ════════════════════════════════════════════════════════════
app.post('/pair', async (req, res) => {
    const phone = formatNumber(req.body.phone);
    if (!MznKing)     return res.send(errorPage('Service starting, wait and retry'));
    if (MznKing.user) return res.send(successPage('Already Connected', 'WhatsApp is already linked!'));
    try {
        await delay(2000);
        const code      = await MznKing.requestPairingCode(phone);
        const formatted = code?.match(/.{1,4}/g)?.join('-') || code;
        addLog(`Pairing code for ${phone}: ${formatted}`, 'success');
        res.send(`<!DOCTYPE html><html><head>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@700;900&family=Share+Tech+Mono&display=swap" rel="stylesheet"/>
</head><body style="background:#030305;color:#fff;display:flex;justify-content:center;align-items:center;min-height:100vh;">
<div style="text-align:center;border:2px solid #ff00ff;padding:50px 60px;border-radius:8px;background:#080810;box-shadow:0 0 60px rgba(255,0,255,0.15);">
<div style="font-family:'Orbitron',monospace;color:#ff00ff;font-size:13px;letter-spacing:4px;margin-bottom:18px;">PAIRING CODE</div>
<div style="font-family:'Orbitron',monospace;font-size:clamp(28px,8vw,54px);color:#00ff66;letter-spacing:12px;font-weight:900;text-shadow:0 0 30px #00ff66;margin:20px 0;">${formatted}</div>
<p style="color:#555;font-family:'Share Tech Mono',monospace;font-size:10px;letter-spacing:2px;">WHATSAPP > LINKED DEVICES > LINK WITH PHONE NUMBER</p>
<a href="/" style="display:inline-block;margin-top:28px;padding:11px 38px;background:linear-gradient(135deg,#ff00ff,#8800ee);color:#fff;text-decoration:none;border-radius:4px;font-family:'Orbitron',monospace;font-size:10px;letter-spacing:2px;">BACK TO PANEL</a>
</div></body></html>`);
    } catch(e) {
        addLog(`Pairing failed: ${e.message}`, 'error');
        res.send(errorPage(e.message));
    }
});

// ════════════════════════════════════════════════════════════
//  START ATTACK
// ════════════════════════════════════════════════════════════
app.post('/attack', upload.single('msgFile'), async (req, res) => {
    try {
        if (!MznKing?.user) throw new Error('WhatsApp not connected!');
        const { numbers, groups, hater, delay: delayTime } = req.body;
        if (!req.file) throw new Error('No message file uploaded');

        messages = req.file.buffer.toString('utf-8')
            .split('\n').map(l => l.trim()).filter(l => l.length > 0);
        if (!messages.length) throw new Error('Message file is empty');

        haterName    = hater;
        intervalTime = Math.max(5, parseInt(delayTime, 10));
        targets      = [];

        if (numbers?.trim()) {
            numbers.split(',').forEach(n => {
                const c = n.trim().replace(/\s/g,'');
                if (c) targets.push(c.includes('@') ? c : c + '@s.whatsapp.net');
            });
        }
        if (groups?.trim()) {
            groups.split(',').forEach(g => {
                const c = g.trim().replace(/\s/g,'');
                if (c) targets.push(c.includes('@') ? c : c + '@g.us');
            });
        }
        if (!targets.length) throw new Error('No targets provided!');

        stopLoop();
        await delay(600);

        totalSent      = 0;
        totalFailed    = 0;
        totalErrors    = 0;
        totalRecovered = 0;
        sessionStart   = Date.now();
        errorBlacklist.clear();
        retryQueue.length = 0;
        rateLimitActive   = false;
        rateLimitUntil    = 0;

        addLog(`ATTACK STARTED! Targets:${targets.length} Msgs:${messages.length} Delay:${intervalTime}s`, 'success');
        startNonStopLoop();
        res.redirect('/');
    } catch(e) {
        addLog(`Attack failed: ${e.message}`, 'error');
        res.send(errorPage(e.message));
    }
});

// ════════════════════════════════════════════════════════════
//  STOP
// ════════════════════════════════════════════════════════════
app.post('/stop', (_req, res) => {
    stopLoop();
    addLog(`Manually stopped. Sent:${totalSent} Failed:${totalFailed} Recovered:${totalRecovered}`, 'warning');
    res.redirect('/');
});

// ════════════════════════════════════════════════════════════
//  HEALTH CHECK
// ════════════════════════════════════════════════════════════
app.get('/health', (_req, res) => {
    res.json({
        status:         'running',
        connected:      !!(MznKing?.user),
        loopActive:     loopController.active,
        loopRunning:    loopController.running,
        uptime:         process.uptime(),
        totalSent,
        totalFailed,
        totalErrors,
        totalRecovered,
        retryQueueSize: retryQueue.length,
        blacklistCount: errorBlacklist.size,
        rateLimitActive
    });
});

// ════════════════════════════════════════════════════════════
//  GROUPS PAGE (BACKUP)
// ════════════════════════════════════════════════════════════
app.get('/groups', async (_req, res) => {
    if (!MznKing?.user) return res.send(errorPage('Not connected'));
    try {
        const groups    = await MznKing.groupFetchAllParticipating();
        const groupList = Object.entries(groups);
        let html = `<!DOCTYPE html><html><head>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@700&family=Share+Tech+Mono&display=swap" rel="stylesheet"/>
</head><body style="background:#030305;color:#fff;font-family:'Share Tech Mono',monospace;padding:20px;">
<h2 style="color:#ff00ff;text-align:center;font-family:'Orbitron',monospace;letter-spacing:4px;margin-bottom:20px;">GROUPS (${groupList.length})</h2>
<div style="max-width:700px;margin:0 auto;">`;
        groupList.forEach(([uid, data], i) => {
            html += `<div style="background:#080810;border:1px solid #16162a;border-left:3px solid #8800ee;padding:12px;margin:7px 0;border-radius:3px;">
<div style="color:#bb77ff;font-size:12px;margin-bottom:7px;">${i+1}. ${data.subject}</div>
<input type="text" value="${uid}" readonly onclick="this.select();navigator.clipboard.writeText(this.value)"
style="width:100%;padding:7px;background:#000;border:1px solid #1a1a2a;color:#00ffff;font-family:'Share Tech Mono',monospace;font-size:10px;border-radius:2px;cursor:pointer;"/>
<div style="font-size:8px;color:#222;text-align:right;margin-top:2px;">click to copy</div></div>`;
        });
        html += `<div style="text-align:center;margin-top:18px;"><a href="/" style="padding:11px 38px;background:linear-gradient(135deg,#ff00ff,#8800ee);color:#fff;text-decoration:none;border-radius:4px;font-family:'Orbitron',monospace;font-size:10px;letter-spacing:2px;">BACK TO PANEL</a></div>
</div></body></html>`;
        res.send(html);
    } catch(e) {
        res.send(errorPage(e.message));
    }
});

// ════════════════════════════════════════════════════════════
//  HELPERS
// ════════════════════════════════════════════════════════════
function errorPage(msg) {
    return `<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@700&display=swap" rel="stylesheet"/>
</head><body style="background:#030305;color:#ff0033;display:flex;justify-content:center;align-items:center;min-height:100vh;">
<div style="text-align:center;border:2px solid #ff0033;padding:38px;border-radius:6px;background:#080810;max-width:420px;">
<div style="font-family:'Orbitron',monospace;font-size:15px;letter-spacing:4px;margin-bottom:14px;">ERROR</div>
<p style="color:#cc3333;font-size:12px;line-height:1.7;">${msg}</p>
<a href="/" style="display:inline-block;margin-top:22px;padding:9px 28px;border:1px solid #ff0033;color:#ff0033;text-decoration:none;border-radius:3px;font-size:11px;">GO BACK</a>
</div></body></html>`;
}
function successPage(title, msg) {
    return `<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@700&display=swap" rel="stylesheet"/>
</head><body style="background:#030305;color:#00ff66;display:flex;justify-content:center;align-items:center;min-height:100vh;">
<div style="text-align:center;border:2px solid #00ff66;padding:38px;border-radius:6px;background:#080810;max-width:420px;">
<div style="font-family:'Orbitron',monospace;font-size:15px;letter-spacing:4px;margin-bottom:14px;">${title}</div>
<p style="color:#88cc88;font-size:12px;">${msg}</p>
<a href="/" style="display:inline-block;margin-top:22px;padding:9px 28px;background:#00ff66;color:#000;text-decoration:none;border-radius:3px;font-size:11px;font-weight:bold;">GO BACK</a>
</div></body></html>`;
}

// ════════════════════════════════════════════════════════════
//  CRASH GUARDS
// ════════════════════════════════════════════════════════════
process.on('uncaughtException',  (err)    => addLog(`UNCAUGHT: ${err.message}`, 'error'));
process.on('unhandledRejection', (reason) => addLog(`REJECTION: ${reason}`, 'error'));
process.on('SIGINT',  () => { addLog('SIGINT received', 'error'); process.exit(0); });
process.on('SIGTERM', () => { addLog('SIGTERM received', 'error'); process.exit(0); });

// ════════════════════════════════════════════════════════════
//  START SERVER
// ════════════════════════════════════════════════════════════
app.listen(port, () => {
    console.log(`
+===================================================+
|   HARSH KING ULTRA SERVER v5.1 - NEVER STOP       |
|   Port: ${port}                                      |
|   LOGGEDOUT = AUTO RECONNECT (NO LOOP STOP)       |
|   BADSESSION = AUTO CLEAR + RECONNECT             |
|   ALL ERRORS AUTO RECOVERED, LOOP NEVER DIES      |
+===================================================+`);
    addLog(`Server v5.1 started on port ${port} - NEVER STOP MODE ACTIVE`, 'success');
});
