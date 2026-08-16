// ============================================================================
// MUSKAN WITH YANKI v8.5 – ENTERPRISE EDITION (TARGETS FIXED)
// ============================================================================
import express from 'express';
import fs from 'fs';
import pino from 'pino';
import multer from 'multer';
import NodeCache from 'node-cache';
import pLimit from 'p-limit';
import crypto from 'crypto';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { makeWASocket, useMultiFileAuthState, delay, DisconnectReason,
         fetchLatestBaileysVersion, makeCacheableSignalKeyStore,
         Browsers } from '@whiskeysockets/baileys';
import cluster from 'cluster';
import os from 'os';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';

// ============================================================================
// 1. CONFIGURATION
// ============================================================================
const CONFIG = {
    PORT: process.env.PORT || 5000,
    SESSION_DIR: './auth_info',
    LOG_FILE: './logs/wa.log',
    BLACKLIST_THRESHOLD: 5,
    BLACKLIST_RESET_MS: 60000,
    MAX_RETRY_QUEUE: 2000,
    DEFAULT_INTERVAL_SECONDS: 10,
    MIN_INTERVAL_SECONDS: 3,
    MAX_INTERVAL_SECONDS: 300,
    CONNECT_TIMEOUT_MS: 120000,
    DEFAULT_QUERY_TIMEOUT_MS: 90000,
    KEEPALIVE_INTERVAL_MS: 3000,
    RETRY_REQUEST_DELAY_MS: 500,
    MAX_MS_RECONNECT_WAIT: 3000,
    RECONNECT_BASE_DELAY_MS: 1000,
    RECONNECT_MAX_DELAY_MS: 60000,
    RATE_LIMIT_COOLDOWN_MS: 30000,
    CONSECUTIVE_ERROR_THRESHOLD: 5,
    MAX_LOGS: 500,
    MAX_FILE_SIZE: 20 * 1024 * 1024,
    CACHE_TTL_SECONDS: 7200,
    HEARTBEAT_INTERVAL_MS: 3000,
    PRESENCE_INTERVAL_MS: 15000,
    BATCH_SIZE: 20,
    MAX_RETRIES_SEND: 15,
    GROUP_METADATA_CONCURRENCY: 10,
    GC_INTERVAL_MS: 120000,
    MAX_CACHE_KEYS: 10000,
    CSRF_SECRET: process.env.CSRF_SECRET || crypto.randomBytes(64).toString('hex'),
    RATE_LIMIT_WINDOW_MS: 15 * 60 * 1000,
    RATE_LIMIT_MAX: 200,
    CSRF_TTL: 7200000,
    MAX_CONCURRENT_SENDS: 5,
    BATCH_DELAY_MS: 100,
    CIRCUIT_BREAKER_THRESHOLD: 10,
    CIRCUIT_BREAKER_TIMEOUT: 60000,
    AUTO_RESTART_INTERVAL: 3600000,
    HEALTH_CHECK_INTERVAL: 5000,
    ENABLE_COMPRESSION: true,
    ENABLE_METRICS: true,
    LOG_LEVEL: process.env.LOG_LEVEL || 'info',
    MAX_MEMORY_MB: 1024,
};

// ============================================================================
// 2. LOGGER
// ============================================================================
fs.mkdirSync('./logs', { recursive: true });

const logger = pino({
    level: CONFIG.LOG_LEVEL,
    transport: {
        targets: [
            { 
                target: 'pino/file', 
                options: { 
                    destination: CONFIG.LOG_FILE, 
                    mkdir: true, 
                    sync: false,
                    append: true,
                } 
            },
            { 
                target: 'pino/file', 
                options: { 
                    destination: './logs/error.log', 
                    level: 'error',
                    mkdir: true,
                } 
            },
            ...(process.env.NODE_ENV !== 'production'
                ? [{ target: 'pino-pretty', options: { colorize: true, translateTime: true } }]
                : [])
        ]
    },
});

class LogBuffer {
    constructor(max = CONFIG.MAX_LOGS) {
        this.max = max;
        this.buffer = [];
        this.errorBuffer = [];
    }
    
    add(entry) {
        this.buffer.unshift(entry);
        if (entry.type === 'error') this.errorBuffer.unshift(entry);
        if (this.buffer.length > this.max) this.buffer.pop();
        if (this.errorBuffer.length > 100) this.errorBuffer.pop();
    }
    
    get(filter = null) {
        if (!filter) return this.buffer;
        if (filter === 'error') return this.errorBuffer;
        return this.buffer.filter(l => l.type === filter);
    }
    
    clear() { 
        this.buffer = []; 
        this.errorBuffer = [];
    }
}
const logBuffer = new LogBuffer();

const info = (msg, type = 'info') => {
    const timestamp = new Date().toLocaleTimeString();
    logBuffer.add({ timestamp, message: msg, type });
    logger[type === 'success' ? 'info' : type](msg);
};

// ============================================================================
// 3. INPUT SANITIZATION - FIXED
// ============================================================================
const sanitize = (input) => {
    if (typeof input !== 'string') return input;
    return input
        .replace(/[&<>"']/g, (match) => {
            const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
            return map[match];
        })
        .replace(/[\x00-\x1F\x7F-\x9F]/g, '')
        .trim();
};

// FIXED: Better JID sanitization
const sanitizeJid = (jid) => {
    if (!jid) return null;
    
    // Remove all whitespace
    let cleaned = jid.replace(/\s/g, '');
    
    // Remove special characters except @ and .
    cleaned = cleaned.replace(/[^0-9@.]/g, '');
    
    // If no @ symbol, add appropriate suffix
    if (!cleaned.includes('@')) {
        // Check if it's a group ID (starts with numbers and has -)
        if (cleaned.includes('-')) {
            cleaned += '@g.us';
        } else if (cleaned.length > 9 && cleaned.length < 16) {
            cleaned += '@s.whatsapp.net';
        } else {
            return null;
        }
    }
    
    // Validate final format
    if (!cleaned.includes('@') || cleaned.length < 10) {
        return null;
    }
    
    return cleaned;
};

// FIXED: Better phone number formatting
const formatPhoneNumber = (num) => {
    if (!num) return null;
    // Remove all non-numeric characters
    let cleaned = String(num).replace(/[^0-9]/g, '');
    // Remove leading zeros
    cleaned = cleaned.replace(/^0+/, '');
    // Must be between 10-15 digits
    if (cleaned.length < 10 || cleaned.length > 15) {
        return null;
    }
    return cleaned;
};

// ============================================================================
// 4. CSRF TOKEN HANDLING
// ============================================================================
class CSRFManager {
    constructor() {
        this.tokens = new Map();
        this.cleanupInterval = setInterval(() => this.cleanup(), 300000).unref();
    }

    generate(sessionId) {
        const token = crypto.randomBytes(64).toString('hex');
        const expiresAt = Date.now() + CONFIG.CSRF_TTL;
        this.tokens.set(sessionId, { token, expiresAt, createdAt: Date.now() });
        return token;
    }

    validate(sessionId, token) {
        if (!token) return false;
        const entry = this.tokens.get(sessionId);
        if (!entry) return false;
        if (entry.token !== token) return false;
        if (Date.now() > entry.expiresAt) {
            this.tokens.delete(sessionId);
            return false;
        }
        entry.expiresAt = Date.now() + CONFIG.CSRF_TTL;
        return true;
    }

    cleanup() {
        const now = Date.now();
        for (const [key, value] of this.tokens) {
            if (now > value.expiresAt) {
                this.tokens.delete(key);
            }
        }
    }

    revoke(sessionId) { this.tokens.delete(sessionId); }
    get size() { return this.tokens.size; }
}
const csrfManager = new CSRFManager();

// ============================================================================
// 5. BLACKLIST MANAGER
// ============================================================================
class BlacklistManager {
    constructor() {
        this.errorCounts = new Map();
        this.createdAt = new Map();
        this.totalBlacklisted = 0;
        this.autoCleanInterval = setInterval(() => this.autoClean(), CONFIG.BLACKLIST_RESET_MS / 2).unref();
    }

    markFail(target) {
        const count = (this.errorCounts.get(target) || 0) + 1;
        this.errorCounts.set(target, count);
        if (!this.createdAt.has(target)) this.createdAt.set(target, Date.now());
        
        const thresholdReached = count >= CONFIG.BLACKLIST_THRESHOLD;
        if (thresholdReached) {
            this.totalBlacklisted++;
            info(`[BLACKLIST] ${target.split('@')[0]} blacklisted (${count} errors)`, 'warn');
        }
        return thresholdReached;
    }

    clearSuccess(target) { 
        this.errorCounts.delete(target); 
        this.createdAt.delete(target); 
    }

    isBlacklisted(target) { 
        return (this.errorCounts.get(target) || 0) >= CONFIG.BLACKLIST_THRESHOLD; 
    }

    clearAll() { 
        this.errorCounts.clear(); 
        this.createdAt.clear();
        this.totalBlacklisted = 0;
        info('[BLACKLIST] All targets cleared', 'info');
    }

    autoClean() {
        const now = Date.now();
        let cleaned = 0;
        for (const [target, createdAt] of this.createdAt) {
            if (now - createdAt > CONFIG.BLACKLIST_RESET_MS) {
                this.errorCounts.delete(target);
                this.createdAt.delete(target);
                cleaned++;
            }
        }
        if (cleaned > 0) {
            info(`[AUTO-CLEAR] Removed ${cleaned} expired blacklist entries`, 'debug');
        }
    }

    get size() { return this.errorCounts.size; }
}
const blacklistManager = new BlacklistManager();

// ============================================================================
// 6. APP STATE
// ============================================================================
class AppState {
    constructor() {
        this.targets = [];
        this.messages = [];
        this.haterName = '';
        this.intervalTime = 10;
        this.loopActive = false;
        this.loopRunning = false;
        this.totalSent = 0;
        this.totalFailed = 0;
        this.totalErrors = 0;
        this.cycleCount = 0;
        this.crashCount = 0;
        this.forcedClearCount = 0;
        this.currentMsgIndex = 0;
        this.currentTargetIndex = 0;
        this.sessionStart = Date.now();
        this.isPaired = false;
    }

    reset(targets, messages, haterName, intervalTime) {
        this.targets = targets;
        this.messages = messages;
        this.haterName = haterName;
        this.intervalTime = intervalTime;
        this.loopActive = false;
        this.loopRunning = false;
        this.totalSent = 0;
        this.totalFailed = 0;
        this.totalErrors = 0;
        this.cycleCount = 0;
        this.crashCount = 0;
        this.forcedClearCount = 0;
        this.currentMsgIndex = 0;
        this.currentTargetIndex = 0;
        this.sessionStart = Date.now();
    }

    getStats() {
        const uptime = Date.now() - this.sessionStart;
        const rate = uptime > 0 ? (this.totalSent / (uptime / 60000)) : 0;
        return {
            totalSent: this.totalSent,
            totalFailed: this.totalFailed,
            totalErrors: this.totalErrors,
            rate: Math.round(rate * 100) / 100,
            uptime: this.formatUptime(uptime),
            successRate: this.totalSent + this.totalFailed > 0 
                ? Math.round((this.totalSent / (this.totalSent + this.totalFailed)) * 100)
                : 0,
            isPaired: this.isPaired,
        };
    }

    formatUptime(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        return `${days}d ${hours % 24}h ${minutes % 60}m ${seconds % 60}s`;
    }
}
const appState = new AppState();

// ============================================================================
// 7. CACHES
// ============================================================================
const msgRetryCounterCache = new NodeCache({ stdTTL: CONFIG.CACHE_TTL_SECONDS, checkperiod: 120, maxKeys: CONFIG.MAX_CACHE_KEYS });
const groupMetadataCache = new NodeCache({ stdTTL: CONFIG.CACHE_TTL_SECONDS, checkperiod: 120, maxKeys: CONFIG.MAX_CACHE_KEYS });
const contactCache = new NodeCache({ stdTTL: CONFIG.CACHE_TTL_SECONDS, checkperiod: 120, maxKeys: CONFIG.MAX_CACHE_KEYS });
const chatCache = new NodeCache({ stdTTL: CONFIG.CACHE_TTL_SECONDS, checkperiod: 120, maxKeys: CONFIG.MAX_CACHE_KEYS });

setInterval(() => {
    msgRetryCounterCache.flushAll();
    groupMetadataCache.flushAll();
    contactCache.flushAll();
    chatCache.flushAll();
}, 300000).unref();

// ============================================================================
// 8. STORE
// ============================================================================
const STORE_FILE = './store.json';

class SimpleStore {
    constructor() {
        this.data = {};
        this._ev = null;
        this.load();
    }

    bind(ev) {
        this._ev = ev;
        ev.on('messages.upsert', this._onMessagesUpsert.bind(this));
    }

    unbind() {
        if (this._ev) {
            this._ev.removeAllListeners('messages.upsert');
            this._ev = null;
        }
    }

    _onMessagesUpsert({ messages }) {
        messages.forEach(m => {
            const key = m.key;
            const jid = key.remoteJid;
            if (!jid) return;
            if (!this.data[jid]) this.data[jid] = {};
            this.data[jid][key.id] = m;
        });
    }

    loadMessage(jid, id) {
        if (this.data[jid] && this.data[jid][id]) return this.data[jid][id];
        return null;
    }

    save() {
        try {
            fs.writeFileSync(STORE_FILE, JSON.stringify(this.data));
        } catch (e) { 
            info(`[STORE] Write error: ${e.message}`, 'error'); 
        }
    }

    load() {
        try {
            if (fs.existsSync(STORE_FILE)) {
                const content = fs.readFileSync(STORE_FILE, 'utf-8');
                this.data = JSON.parse(content);
                info('[STORE] Restored from file', 'info');
            }
        } catch (e) {
            info(`[STORE] Failed to restore: ${e.message}`, 'error');
            this.data = {};
        }
    }
}
const store = new SimpleStore();

setInterval(() => {
    try { store.save(); } catch (e) {}
}, 30000).unref();

// ============================================================================
// 9. CONNECTION MANAGER
// ============================================================================
let cachedVersion = null;

class ConnectionManager {
    constructor() {
        this.sock = null;
        this.store = store;
        this.isConnecting = false;
        this.reconnectAttempts = 0;
        this.reconnectTimer = null;
        this._onConnected = null;
        this._onDisconnected = null;
        this.isOnline = false;
        this.healthCheckInterval = null;
        this.presenceInterval = null;
        this._initialized = false;
        this.pairingCode = null;
    }

    setCallbacks({ onConnected, onDisconnected }) {
        this._onConnected = onConnected;
        this._onDisconnected = onDisconnected;
    }

    async connect() {
        if (this.isConnecting) {
            info('[CONNECTION] Already connecting...', 'debug');
            return;
        }
        
        this.isConnecting = true;
        info('📱 Connecting to WhatsApp...', 'info');
        
        try {
            if (!fs.existsSync(CONFIG.SESSION_DIR)) {
                fs.mkdirSync(CONFIG.SESSION_DIR, { recursive: true });
            }

            if (!cachedVersion) {
                cachedVersion = await fetchLatestBaileysVersion();
            }
            const { version } = cachedVersion;

            const { state, saveCreds } = await useMultiFileAuthState(CONFIG.SESSION_DIR);

            if (this.sock) {
                try {
                    if (this.store.unbind) this.store.unbind();
                    this.sock.ws?.close();
                    this.sock.ev?.removeAllListeners();
                } catch (e) {}
                this.sock = null;
            }

            this.sock = makeWASocket({
                logger: pino({ level: 'silent' }),
                printQRInTerminal: false,
                browser: Browsers.ubuntu('Chrome'),
                auth: {
                    creds: state.creds,
                    keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }))
                },
                version,
                connectTimeoutMs: CONFIG.CONNECT_TIMEOUT_MS,
                defaultQueryTimeoutMs: CONFIG.DEFAULT_QUERY_TIMEOUT_MS,
                keepAliveIntervalMs: CONFIG.KEEPALIVE_INTERVAL_MS,
                emitOwnEvents: false,
                retryRequestDelayMs: CONFIG.RETRY_REQUEST_DELAY_MS,
                maxMsReconnectWait: CONFIG.MAX_MS_RECONNECT_WAIT,
                generateHighQualityLinkPreview: false,
                patchMessageBeforeSending: (msg) => msg,
                getMessage: async (key) => ({
                    conversation: (await this.store.loadMessage(key.remoteJid, key.id))?.message?.conversation || ''
                }),
                msgRetryCounterCache,
                markOnlineOnConnect: false,
                shouldIgnoreJid: () => false,
                syncFullHistory: false,
            });

            this.store.bind(this.sock.ev);
            this._attachEvents(saveCreds);

            this.isConnecting = false;
            this.isOnline = false;
            this._initialized = true;
            info('[CONNECTION] Socket created, waiting for connection...', 'info');

        } catch (error) {
            this.isConnecting = false;
            info(`❌ Connection setup error: ${error.message}`, 'error');
            this._scheduleReconnect(CONFIG.RECONNECT_BASE_DELAY_MS);
        }
    }

    _attachEvents(saveCreds) {
        if (!this.sock) return;

        this.sock.ev.on('connection.update', (update) => {
            const { connection, lastDisconnect, qr } = update;
            
            if (qr) {
                info('[QR] QR Code received. Use /pair with phone number instead.', 'info');
                this.pairingCode = 'QR_CODE_RECEIVED';
            }
            
            if (connection === 'open') {
                this.isConnecting = false;
                this.reconnectAttempts = 0;
                this.isOnline = true;
                appState.isPaired = true;
                info('✅ CONNECTED!', 'success');
                if (this._onConnected) this._onConnected();
                this._startHealthChecks();
                this._sendPresence();
            }

            if (connection === 'close') {
                this.isConnecting = false;
                this.isOnline = false;
                appState.isPaired = false;
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                info(`🔌 Disconnected (statusCode: ${statusCode})`, 'warn');
                
                if (statusCode === DisconnectReason.loggedOut) {
                    info('🚫 Logged out – clearing session', 'error');
                    try {
                        fs.rmSync(CONFIG.SESSION_DIR, { recursive: true, force: true });
                    } catch (e) {}
                    this._scheduleReconnect(5000);
                } else {
                    this.reconnectAttempts++;
                    const delayMs = Math.min(
                        CONFIG.RECONNECT_MAX_DELAY_MS,
                        CONFIG.RECONNECT_BASE_DELAY_MS * Math.pow(1.5, this.reconnectAttempts - 1) + Math.random() * 2000
                    );
                    info(`🔄 Reconnecting in ${Math.round(delayMs / 1000)}s (attempt ${this.reconnectAttempts})`, 'info');
                    this._scheduleReconnect(delayMs);
                }
                
                if (this._onDisconnected) this._onDisconnected();
                this._stopHealthChecks();
            }
        });

        this.sock.ev.on('creds.update', async () => {
            try { await saveCreds(); } catch (e) {}
        });

        this.sock.ev.on('contacts.update', (updates) => {
            updates.forEach(u => { if (u.id) contactCache.set(u.id, u); });
        });

        this.sock.ev.on('chats.update', (updates) => {
            updates.forEach(u => { if (u.id) chatCache.set(u.id, u); });
        });
    }

    _startHealthChecks() {
        if (this.healthCheckInterval) clearInterval(this.healthCheckInterval);
        this.healthCheckInterval = setInterval(async () => {
            try {
                if (!this.sock?.user) {
                    info('[HEALTH] Socket user missing, reconnecting...', 'warn');
                    this.connect();
                    return;
                }
                await this.sock.sendPresenceUpdate('available');
            } catch (e) {
                info(`[HEALTH] Health check failed: ${e.message}`, 'error');
                if (this.isOnline) {
                    this.isOnline = false;
                    this.connect();
                }
            }
        }, CONFIG.HEALTH_CHECK_INTERVAL).unref();
    }

    _stopHealthChecks() {
        if (this.healthCheckInterval) {
            clearInterval(this.healthCheckInterval);
            this.healthCheckInterval = null;
        }
    }

    _sendPresence() {
        if (this.presenceInterval) clearInterval(this.presenceInterval);
        this.presenceInterval = setInterval(async () => {
            try {
                if (this.sock?.user && this.isOnline) {
                    await this.sock.sendPresenceUpdate('available').catch(() => {});
                }
            } catch (e) {}
        }, CONFIG.PRESENCE_INTERVAL_MS).unref();
    }

    _scheduleReconnect(waitMs) {
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (!this.isConnecting && !this.isOnline) {
                this.connect();
            }
        }, waitMs);
    }

    disconnect() {
        if (this.sock) {
            try {
                if (this.store.unbind) this.store.unbind();
                this.sock.ws?.close();
                this.sock.ev?.removeAllListeners();
            } catch (e) {}
            this.sock = null;
        }
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this._stopHealthChecks();
        if (this.presenceInterval) {
            clearInterval(this.presenceInterval);
            this.presenceInterval = null;
        }
        this.isConnecting = false;
        this.isOnline = false;
        this._initialized = false;
        appState.isPaired = false;
    }

    getSocket() { 
        return this.sock; 
    }
    
    isConnected() {
        return this.isOnline && !!this.sock?.user;
    }

    getStatus() {
        return {
            connected: this.isConnected(),
            connecting: this.isConnecting,
            reconnectAttempts: this.reconnectAttempts,
            isPaired: appState.isPaired,
            hasSocket: !!this.sock,
            userId: this.sock?.user?.id || null,
        };
    }
}
const connectionManager = new ConnectionManager();

// ============================================================================
// 10. GROUP NAME FETCHER
// ============================================================================
const groupMetadataLimit = pLimit(CONFIG.GROUP_METADATA_CONCURRENCY);

async function fetchGroupName(sock, jid) {
    const cached = groupMetadataCache.get(jid);
    if (cached) return cached;
    try {
        const metadata = await sock.groupMetadata(jid);
        const name = metadata.subject || 'Unknown';
        groupMetadataCache.set(jid, name);
        return name;
    } catch (e) {
        return 'Unknown';
    }
}

async function fetchAllGroupNames(sock, groupIds) {
    const tasks = groupIds.map(jid =>
        groupMetadataLimit(async () => {
            if (!groupMetadataCache.has(jid)) {
                try {
                    const metadata = await sock.groupMetadata(jid);
                    groupMetadataCache.set(jid, metadata.subject || 'Unknown');
                } catch (e) {
                    groupMetadataCache.set(jid, 'Unknown');
                }
            }
            return { id: jid, name: groupMetadataCache.get(jid) };
        })
    );
    return Promise.all(tasks);
}

// ============================================================================
// 11. SMART SENDER
// ============================================================================
class SmartSender {
    constructor() {
        this.consecutiveErrors = 0;
        this.isTempBlocked = false;
        this.blockEndTime = 0;
        this.retryQueue = [];
        this.tokens = 50;
        this.lastRefill = Date.now();
        this.bucketSize = 50;
        this.refillRate = 15;
        this.circuitBreakerState = 'closed';
        this.circuitBreakerFailures = 0;
        this.circuitBreakerLastFailure = 0;
    }

    async _getToken() {
        const now = Date.now();
        const elapsed = (now - this.lastRefill) / 1000;
        this.tokens = Math.min(this.bucketSize, this.tokens + elapsed * this.refillRate);
        this.lastRefill = now;
        
        if (this.tokens < 1) {
            const waitMs = ((1 - this.tokens) * 1000) / this.refillRate;
            await delay(Math.max(10, waitMs));
            this.tokens = 1;
        }
        this.tokens -= 1;
        return true;
    }

    async _checkCircuitBreaker() {
        if (this.circuitBreakerState === 'open') {
            if (Date.now() - this.circuitBreakerLastFailure > CONFIG.CIRCUIT_BREAKER_TIMEOUT) {
                this.circuitBreakerState = 'half-open';
                return true;
            }
            return false;
        }
        return true;
    }

    _updateCircuitBreaker(success) {
        if (success) {
            this.circuitBreakerFailures = 0;
            if (this.circuitBreakerState === 'half-open') {
                this.circuitBreakerState = 'closed';
            }
        } else {
            this.circuitBreakerFailures++;
            if (this.circuitBreakerState === 'half-open' || 
                this.circuitBreakerFailures >= CONFIG.CIRCUIT_BREAKER_THRESHOLD) {
                this.circuitBreakerState = 'open';
                this.circuitBreakerLastFailure = Date.now();
                info(`[CIRCUIT] Circuit breaker tripped (${this.circuitBreakerFailures} failures)`, 'error');
            }
        }
    }

    async send(target, message) {
        if (!target || !message) {
            info('[SEND] Invalid target or message', 'error');
            return false;
        }
        
        if (blacklistManager.isBlacklisted(target)) {
            info(`[SEND] ${target.split('@')[0]} is blacklisted`, 'warn');
            return false;
        }
        
        if (!await this._checkCircuitBreaker()) {
            info('[SEND] Circuit open - queuing message', 'warn');
            this.retryQueue.push({ target, message, addedAt: Date.now(), attempts: 0 });
            return false;
        }

        await this._getToken();

        if (this.isTempBlocked) {
            const waitTime = Math.max(0, this.blockEndTime - Date.now());
            if (waitTime > 0) {
                info(`[SEND] Waiting ${Math.ceil(waitTime / 1000)}s for temp block`, 'warn');
                await delay(waitTime);
                this.isTempBlocked = false;
            }
        }

        for (let attempt = 1; attempt <= CONFIG.MAX_RETRIES_SEND; attempt++) {
            try {
                const sock = connectionManager.getSocket();
                
                if (!sock) {
                    info('[SEND] No socket available, waiting...', 'warn');
                    await delay(2000);
                    continue;
                }
                
                if (!sock.user) {
                    info('[SEND] Socket not authenticated, waiting...', 'warn');
                    await delay(2000);
                    continue;
                }

                await sock.sendMessage(target, { text: message });
                
                this.consecutiveErrors = 0;
                blacklistManager.clearSuccess(target);
                appState.totalSent++;
                appState.totalErrors = 0;
                
                if (appState.totalSent % 5 === 0) {
                    const display = target.includes('@g.us')
                        ? `Group:${target.split('@')[0]}`
                        : target.split('@')[0];
                    info(`📨 #${appState.totalSent} → ${display}`, 'success');
                }

                this._updateCircuitBreaker(true);
                await delay(Math.random() * 500 + 100);
                return true;

            } catch (err) {
                appState.totalErrors++;
                const errMsg = err?.message || String(err);
                info(`[SEND] Attempt ${attempt}/${CONFIG.MAX_RETRIES_SEND} failed: ${errMsg}`, 'error');
                
                if (errMsg.includes('429') || errMsg.includes('rate') || errMsg.includes('too many')) {
                    this.consecutiveErrors++;
                    if (this.consecutiveErrors >= CONFIG.CONSECUTIVE_ERROR_THRESHOLD) {
                        this.isTempBlocked = true;
                        this.blockEndTime = Date.now() + CONFIG.RATE_LIMIT_COOLDOWN_MS;
                        info(`⚠️ TEMPORARY BLOCK - ${CONFIG.RATE_LIMIT_COOLDOWN_MS / 1000}s cooldown`, 'error');
                        await delay(CONFIG.RATE_LIMIT_COOLDOWN_MS);
                        this.isTempBlocked = false;
                        this.consecutiveErrors = 0;
                        continue;
                    }
                }

                this._updateCircuitBreaker(false);
                blacklistManager.markFail(target);
                
                if (attempt < CONFIG.MAX_RETRIES_SEND) {
                    const backoff = Math.min(15000, 2000 * Math.pow(1.5, attempt - 1));
                    await delay(backoff);
                }
            }
        }

        appState.totalFailed++;
        if (this.retryQueue.length < CONFIG.MAX_RETRY_QUEUE) {
            this.retryQueue.push({ target, message, addedAt: Date.now(), attempts: 0 });
        }
        return false;
    }

    async processRetryQueue() {
        if (this.retryQueue.length === 0) return;
        
        const now = Date.now();
        const batch = this.retryQueue
            .filter(item => now - item.addedAt <= 3600000 && item.attempts < 5)
            .slice(0, CONFIG.BATCH_SIZE);

        this.retryQueue = this.retryQueue.filter(item => 
            !batch.includes(item) && now - item.addedAt <= 3600000
        );

        if (batch.length > 0) {
            info(`[RETRY] Processing ${batch.length} queued messages`, 'debug');
            for (const item of batch) {
                item.attempts++;
                await this.send(item.target, item.message);
            }
        }
    }

    reset() {
        this.consecutiveErrors = 0;
        this.isTempBlocked = false;
        this.blockEndTime = 0;
        this.retryQueue = [];
        this.tokens = this.bucketSize;
        this.lastRefill = Date.now();
        this.circuitBreakerState = 'closed';
        this.circuitBreakerFailures = 0;
    }

    get queueSize() { return this.retryQueue.length; }
}
const sender = new SmartSender();

// ============================================================================
// 12. ATTACK ENGINE
// ============================================================================
class AttackEngine {
    constructor() {
        this._running = false;
        this._lock = false;
        this._heartbeatInterval = null;
        this._startedAt = Date.now();
        this._totalCycles = 0;
    }

    start() {
        if (this._lock) {
            info('[ENGINE] Already starting/running', 'debug');
            return;
        }
        
        this._lock = true;
        this._running = true;
        appState.loopActive = true;
        appState.loopRunning = true;
        appState.sessionStart = Date.now();
        
        info('🔥 INFINITE LOOP ENGAGED 🔥', 'success');
        
        this._startIntervals();
        this._runMainLoop().catch((err) => {
            info(`[ENGINE] Main loop crashed: ${err.message}`, 'error');
            this._running = false;
            appState.loopRunning = false;
            this._lock = false;
            this._cleanup();
            if (appState.loopActive) {
                info('[ENGINE] Auto-restarting in 5 seconds...', 'warn');
                setTimeout(() => this.start(), 5000);
            }
        });
    }

    async _runMainLoop() {
        let consecutiveIdle = 0;

        while (appState.loopActive) {
            try {
                if (!connectionManager.isConnected()) {
                    if (consecutiveIdle < 3) {
                        info('[ENGINE] Waiting for connection...', 'debug');
                    }
                    await delay(3000);
                    consecutiveIdle++;
                    if (consecutiveIdle > 10) {
                        info('[ENGINE] Connection lost - reconnecting', 'warn');
                        connectionManager.connect();
                        await delay(5000);
                    }
                    continue;
                }

                consecutiveIdle = 0;

                const { targets, messages, haterName, intervalTime } = appState;
                
                if (!messages?.length || !targets?.length) {
                    info('[ENGINE] No targets or messages, waiting...', 'warn');
                    await delay(2000);
                    continue;
                }

                let targetIndex = -1;
                let checked = 0;
                const startIdx = appState.currentTargetIndex % targets.length;
                
                for (let i = 0; i < targets.length && checked < targets.length; i++) {
                    const idx = (startIdx + i) % targets.length;
                    const target = targets[idx];
                    if (target && !blacklistManager.isBlacklisted(target)) {
                        targetIndex = idx;
                        break;
                    }
                    checked++;
                }

                if (targetIndex === -1) {
                    info('[CLEAR] All targets blacklisted – clearing', 'warn');
                    blacklistManager.clearAll();
                    appState.forcedClearCount++;
                    await delay(3000);
                    continue;
                }

                const messageIdx = appState.currentMsgIndex % messages.length;
                const fullMessage = `${haterName} ${messages[messageIdx]}`;
                const target = targets[targetIndex];

                const success = await sender.send(target, fullMessage);

                if (success) {
                    appState.currentTargetIndex = (targetIndex + 1) % targets.length;
                    if (appState.currentTargetIndex === 0) {
                        appState.currentMsgIndex++;
                        appState.cycleCount++;
                        this._totalCycles++;
                        
                        if (appState.cycleCount % 5 === 0) {
                            info(`📊 Cycle ${appState.cycleCount} | Sent:${appState.totalSent} | Failed:${appState.totalFailed}`, 'info');
                        }
                    }
                }

                if (appState.cycleCount % 10 === 0 && appState.cycleCount > 0) {
                    await sender.processRetryQueue();
                }

                if (appState.loopActive && intervalTime > 0) {
                    const waitTime = Math.max(100, (intervalTime * 1000) + Math.random() * 500 - 250);
                    await delay(waitTime);
                }

                appState.loopRunning = true;

            } catch (err) {
                appState.crashCount++;
                info(`⚠️ Loop iteration error #${appState.crashCount}: ${err.message}`, 'error');
                await delay(2000);
            }
        }

        this._running = false;
        appState.loopRunning = false;
        this._lock = false;
        this._cleanup();
        info('[ENGINE] Stopped', 'warn');
    }

    _startIntervals() {
        if (this._heartbeatInterval) clearInterval(this._heartbeatInterval);
        this._heartbeatInterval = setInterval(() => {
            try {
                blacklistManager.autoClean();
                
                if (appState.loopActive && !appState.loopRunning && !this._running) {
                    info('[FORCE] Loop dead – restarting', 'warn');
                    this.start();
                }
            } catch (e) {
                info(`[HEARTBEAT] Error: ${e.message}`, 'error');
            }
        }, CONFIG.HEARTBEAT_INTERVAL_MS);
    }

    _cleanup() {
        if (this._heartbeatInterval) {
            clearInterval(this._heartbeatInterval);
            this._heartbeatInterval = null;
        }
    }

    stop() {
        this._running = false;
        appState.loopActive = false;
        appState.loopRunning = false;
        this._cleanup();
        this._lock = false;
        info('⛔ Loop stopped by user', 'warn');
    }

    get isRunning() { return this._running; }
}
const attackEngine = new AttackEngine();

// ============================================================================
// 13. WEB APPLICATION
// ============================================================================
const app = express();

app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
    crossOriginOpenerPolicy: false,
    crossOriginResourcePolicy: false,
    referrerPolicy: false,
}));

const apiLimiter = rateLimit({
    windowMs: CONFIG.RATE_LIMIT_WINDOW_MS,
    max: CONFIG.RATE_LIMIT_MAX,
    message: 'Too many requests, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
});

app.use('/api/', apiLimiter);
app.use('/pair', apiLimiter);

app.use(express.urlencoded({ limit: '2mb', extended: true }));
app.use(express.json({ limit: '2mb' }));

if (CONFIG.ENABLE_COMPRESSION) {
    const compression = (await import('compression')).default;
    app.use(compression());
}

const upload = multer({ 
    storage: multer.memoryStorage(), 
    limits: { fileSize: CONFIG.MAX_FILE_SIZE },
    fileFilter: (req, file, cb) => {
        if (file.mimetype === 'text/plain' || file.originalname.endsWith('.txt')) {
            cb(null, true);
        } else {
            cb(new Error('Only .txt files are allowed'));
        }
    }
});

connectionManager.setCallbacks({
    onConnected: () => {
        info('[CALLBACK] Connected!', 'success');
        if (appState.loopActive && !attackEngine.isRunning) {
            info('[RESUME] Starting loop after reconnect', 'info');
            attackEngine.start();
        }
    },
    onDisconnected: () => {
        info('[CALLBACK] Disconnected, waiting for reconnect...', 'warn');
    }
});

// ============================================================================
// 14. API ROUTES
// ============================================================================

app.get('/api/status', (req, res) => {
    const mem = process.memoryUsage();
    const connectionStatus = connectionManager.getStatus();
    
    res.json({
        connected: connectionStatus.connected,
        connection: connectionStatus,
        active: appState.loopActive,
        running: appState.loopRunning,
        targets: appState.targets.length,
        messages: appState.messages?.length || 0,
        totalSent: appState.totalSent,
        totalFailed: appState.totalFailed,
        totalErrors: appState.totalErrors,
        blacklistCount: blacklistManager.size,
        retryQueueSize: sender.queueSize,
        uptime: appState.getStats().uptime,
        cycleCount: appState.cycleCount,
        performance: appState.getStats(),
        isPaired: appState.isPaired,
        memory: {
            rss: `${Math.round(mem.rss / 1024 / 1024)} MB`,
            heapUsed: `${Math.round(mem.heapUsed / 1024 / 1024)} MB`,
        },
        logs: logBuffer.buffer.length,
    });
});

let logsCache = { data: null, timestamp: 0 };
app.get('/api/logs', (req, res) => {
    const now = Date.now();
    if (logsCache.data && now - logsCache.timestamp < 1500) {
        return res.json(logsCache.data);
    }
    
    const filter = req.query.filter || null;
    const limit = parseInt(req.query.limit) || 100;
    const logs = logBuffer.get(filter).slice(0, limit);
    
    const data = { 
        logs, 
        connected: connectionManager.isConnected(),
        active: appState.loopActive,
        total: logBuffer.buffer.length,
        filter: filter || 'all',
    };
    
    logsCache = { data, timestamp: now };
    res.json(data);
});

app.get('/api/groups', async (req, res) => {
    try {
        const sock = connectionManager.getSocket();
        if (!sock?.user) {
            return res.status(503).json({ error: 'WhatsApp not connected' });
        }
        
        const groups = await sock.groupFetchAllParticipating();
        const groupIds = Object.keys(groups);
        const groupNames = await fetchAllGroupNames(sock, groupIds);
        
        const groupList = groupNames.map(({ id, name }) => ({
            id,
            name: name || groups[id]?.subject || 'Unknown',
            participants: groups[id]?.participants?.length || 0,
        }));
        
        res.json({ groups: groupList, count: groupList.length });
    } catch (e) {
        info(`[GROUPS] Error: ${e.message}`, 'error');
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/pair-status', async (req, res) => {
    try {
        const sock = connectionManager.getSocket();
        res.json({
            connected: !!sock?.user,
            connecting: connectionManager.isConnecting,
            ready: !!sock?.user && !connectionManager.isConnecting,
            hasSocket: !!sock,
            isPaired: appState.isPaired,
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ============================================================================
// 15. PAIR HANDLERS
// ============================================================================

app.post('/pair', async (req, res) => {
    try {
        const sessionId = req.ip + (req.headers['user-agent'] || '');
        const token = req.body._csrf;
        
        if (!token || !csrfManager.validate(sessionId, token)) {
            return res.status(403).send(`
                <!DOCTYPE html>
                <html>
                <head><title>Error</title>
                <style>body{background:#0a0a0f;color:#ff4444;font-family:monospace;padding:20px;text-align:center}
                .container{max-width:500px;margin:0 auto;background:#111;padding:40px;border-radius:10px;border:2px solid #ff4444}
                h2{color:#ff4444}
                .back{color:#ff00ff;text-decoration:none;display:inline-block;margin-top:20px}</style>
                </head>
                <body>
                <div class="container">
                    <h2>❌ Invalid or expired CSRF token</h2>
                    <p>Please refresh the page and try again</p>
                    <a href="/pair" class="back">← TRY AGAIN</a>
                </div>
                </body>
                </html>
            `);
        }

        let phone = req.body.phone;
        if (!phone) {
            return res.status(400).send(`
                <!DOCTYPE html>
                <html>
                <head><title>Error</title>
                <style>body{background:#0a0a0f;color:#ff4444;font-family:monospace;padding:20px;text-align:center}</style>
                </head>
                <body>
                <h2>❌ Phone number is required</h2>
                <a href="/pair">← TRY AGAIN</a>
                </body>
                </html>
            `);
        }

        phone = phone.replace(/[^0-9]/g, '');
        if (phone.length < 10 || phone.length > 15) {
            return res.status(400).send(`
                <!DOCTYPE html>
                <html>
                <head><title>Error</title>
                <style>body{background:#0a0a0f;color:#ff4444;font-family:monospace;padding:20px;text-align:center}</style>
                </head>
                <body>
                <h2>❌ Invalid phone number format</h2>
                <p>Please enter 10-15 digits with country code</p>
                <a href="/pair">← TRY AGAIN</a>
                </body>
                </html>
            `);
        }

        info(`[PAIR] Attempting to pair phone: ${phone}`, 'info');
        
        let sock = connectionManager.getSocket();
        let attempts = 0;
        const maxAttempts = 20;
        
        if (!sock) {
            info('[PAIR] No socket found, connecting...', 'warn');
            connectionManager.connect();
            
            while (!sock && attempts < maxAttempts) {
                await delay(2000);
                sock = connectionManager.getSocket();
                attempts++;
                info(`[PAIR] Waiting for socket... (${attempts}/${maxAttempts})`, 'debug');
            }
        }

        if (!sock) {
            return res.status(503).send(`
                <!DOCTYPE html>
                <html>
                <head><title>Error</title>
                <style>
                    body{background:#0a0a0f;color:#ff4444;font-family:monospace;padding:20px;text-align:center}
                    .container{max-width:500px;margin:0 auto;background:#111;padding:40px;border-radius:10px;border:2px solid #ff4444}
                    h2{color:#ff4444}
                    .back{color:#ff00ff;text-decoration:none;display:inline-block;margin-top:20px}
                </style>
                </head>
                <body>
                <div class="container">
                    <h2>❌ WhatsApp Service Not Ready</h2>
                    <p>Could not establish connection. Please wait and try again.</p>
                    <a href="/pair" class="back">← TRY AGAIN</a>
                </div>
                </body>
                </html>
            `);
        }

        if (sock.user) {
            appState.isPaired = true;
            return res.send(`
                <!DOCTYPE html>
                <html>
                <head><title>Already Connected</title>
                <style>body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px;text-align:center}
                .container{max-width:500px;margin:0 auto;background:#111;padding:40px;border-radius:10px;border:2px solid #00ff88}
                h2{color:#00ff88}
                .back{color:#ff00ff;text-decoration:none;display:inline-block;margin-top:20px}</style>
                </head>
                <body>
                <div class="container">
                    <h2>✅ Already connected!</h2>
                    <p>Device is already paired</p>
                    <a href="/" class="back">← GO TO DASHBOARD</a>
                </div>
                </body>
                </html>
            `);
        }

        info(`[PAIR] Requesting pairing code...`, 'info');
        const code = await sock.requestPairingCode(phone);
        
        if (!code) {
            throw new Error('No code received');
        }

        const formatted = code.match(/.{1,4}/g)?.join('-') || code;
        info(`[PAIR] ✅ Code: ${formatted}`, 'success');

        res.send(`
            <!DOCTYPE html>
            <html>
            <head>
                <title>Pairing Code</title>
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    *{margin:0;padding:0;box-sizing:border-box}
                    body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px;text-align:center;min-height:100vh;display:flex;align-items:center;justify-content:center}
                    .container{max-width:500px;margin:0 auto;background:#111;padding:40px;border-radius:10px;border:2px solid #ff00ff;box-shadow:0 0 50px rgba(255,0,255,0.2)}
                    h1{color:#ff00ff;font-size:2em;margin-bottom:10px}
                    .code{font-size:4em;color:#ff00ff;margin:30px 0;padding:20px;background:#000;border-radius:10px;border:2px solid #ff00ff;font-weight:bold;letter-spacing:5px;word-break:break-all}
                    .info{color:#888;margin:20px 0;line-height:1.6}
                    .steps{text-align:left;background:#000;padding:20px;border-radius:5px;margin:20px 0}
                    .steps li{color:#00ff88;margin:10px 0;list-style-type:decimal}
                    .back{color:#ff00ff;text-decoration:none;display:inline-block;margin-top:20px;font-size:1.2em}
                    .phone{color:#00ff88;font-weight:bold}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>📱 PAIRING CODE</h1>
                    <p>Phone: <span class="phone">${phone}</span></p>
                    <div class="code">${sanitize(formatted)}</div>
                    <div class="info">
                        <strong>How to use:</strong>
                        <ol class="steps">
                            <li>Open WhatsApp on your phone</li>
                            <li>Go to <strong>Settings</strong> → <strong>Linked Devices</strong></li>
                            <li>Tap <strong>Link a Device</strong></li>
                            <li>Enter this code</li>
                        </ol>
                    </div>
                    <a href="/" class="back">← GO TO DASHBOARD</a>
                    <br>
                    <a href="/pair" class="back" style="font-size:0.8em">🔄 New code</a>
                </div>
            </body>
            </html>
        `);

    } catch (error) {
        info(`[PAIR] Error: ${error.message}`, 'error');
        res.status(500).send(`
            <!DOCTYPE html>
            <html>
            <head><title>Error</title>
            <style>
                body{background:#0a0a0f;color:#ff4444;font-family:monospace;padding:20px;text-align:center}
                .container{max-width:500px;margin:0 auto;background:#111;padding:40px;border-radius:10px;border:2px solid #ff4444}
                h2{color:#ff4444}
                .back{color:#ff00ff;text-decoration:none;display:inline-block;margin-top:20px}
            </style>
            </head>
            <body>
            <div class="container">
                <h2>❌ Pairing Failed</h2>
                <p>${sanitize(error.message)}</p>
                <a href="/pair" class="back">← TRY AGAIN</a>
            </div>
            </body>
            </html>
        `);
    }
});

app.get('/pair', (req, res) => {
    const sessionId = req.ip + (req.headers['user-agent'] || '');
    const csrfToken = csrfManager.generate(sessionId);
    
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Pair WhatsApp - KRIX ULTRA</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                *{margin:0;padding:0;box-sizing:border-box}
                body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px;text-align:center;min-height:100vh;display:flex;align-items:center;justify-content:center}
                .container{max-width:500px;margin:0 auto;background:#111;padding:40px;border-radius:10px;border:2px solid #ff00ff;box-shadow:0 0 50px rgba(255,0,255,0.1)}
                h1{color:#ff00ff;font-size:2.5em;margin-bottom:10px}
                .subtitle{color:#888;margin-bottom:30px}
                input{width:100%;padding:15px;margin:20px 0;background:#222;border:2px solid #444;color:white;font-family:monospace;font-size:1.2em;border-radius:5px;transition:border-color 0.3s}
                input:focus{outline:none;border-color:#ff00ff}
                button{background:linear-gradient(135deg,#ff00ff,#8800ee);color:white;padding:15px 40px;border:none;cursor:pointer;font-family:monospace;font-size:1.2em;border-radius:5px;font-weight:bold;transition:all 0.3s;width:100%}
                button:hover{transform:scale(1.02);box-shadow:0 0 30px rgba(255,0,255,0.3)}
                button:disabled{opacity:0.5;cursor:not-allowed}
                .back{color:#ff00ff;text-decoration:none;display:inline-block;margin-top:20px}
                .status{color:#888;font-size:0.9em;margin:10px 0;padding:10px;border-radius:5px}
                .status.connected{color:#00ff88;background:#002200;border:1px solid #00ff88}
                .status.connecting{color:#ffaa00;background:#221100;border:1px solid #ffaa00}
                .status.error{color:#ff4444;background:#220000;border:1px solid #ff4444}
                .loader{display:inline-block;width:20px;height:20px;border:3px solid #333;border-top-color:#ff00ff;border-radius:50%;animation:spin 0.8s linear infinite;margin-right:10px;vertical-align:middle}
                @keyframes spin{to{transform:rotate(360deg)}}
                .info-box{background:#000;padding:15px;border-radius:5px;margin:10px 0;text-align:left;font-size:0.9em;color:#888}
                .info-box strong{color:#ff00ff}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📱 PAIR WHATSAPP</h1>
                <p class="subtitle">Enter your phone number with country code</p>
                
                <div id="status" class="status connecting">
                    <span class="loader"></span> Checking connection...
                </div>
                
                <form action="/pair" method="post" id="pairForm">
                    <input type="hidden" name="_csrf" value="${csrfToken}">
                    <input type="text" name="phone" id="phone" placeholder="919999999999" required>
                    <button type="submit" id="pairBtn">🔗 GET CODE</button>
                </form>
                
                <div class="info-box">
                    <strong>📌 Instructions:</strong><br>
                    • Enter number with country code (no + or spaces)<br>
                    • Example: 919876543210 for India
                </div>
                
                <a href="/" class="back">← BACK TO DASHBOARD</a>
            </div>

            <script>
                let checkInterval;

                async function checkConnection() {
                    try {
                        const response = await fetch('/api/status');
                        const data = await response.json();
                        const statusEl = document.getElementById('status');
                        
                        if (data.isPaired || data.connected) {
                            statusEl.className = 'status connected';
                            statusEl.innerHTML = '✅ WhatsApp is <strong>CONNECTED</strong> and PAIRED!';
                            document.getElementById('pairBtn').disabled = true;
                            document.getElementById('pairBtn').textContent = '✅ ALREADY PAIRED';
                            document.getElementById('phone').disabled = true;
                            if (checkInterval) clearInterval(checkInterval);
                            return;
                        }
                        
                        if (data.connection?.hasSocket && !data.connection?.connected) {
                            statusEl.className = 'status connecting';
                            statusEl.innerHTML = '🔄 Socket created, waiting for connection... <span class="loader"></span>';
                            document.getElementById('pairBtn').disabled = false;
                            document.getElementById('phone').disabled = false;
                            return;
                        }
                        
                        if (data.connection?.connecting) {
                            statusEl.className = 'status connecting';
                            statusEl.innerHTML = '🔄 Connecting to WhatsApp... <span class="loader"></span>';
                            document.getElementById('pairBtn').disabled = true;
                            document.getElementById('phone').disabled = true;
                            return;
                        }
                        
                        statusEl.className = 'status error';
                        statusEl.innerHTML = '❌ WhatsApp is not connected. Please wait...';
                        document.getElementById('pairBtn').disabled = true;
                        document.getElementById('phone').disabled = true;
                        
                    } catch (e) {
                        document.getElementById('status').className = 'status error';
                        document.getElementById('status').innerHTML = '❌ Error checking connection';
                    }
                }

                checkInterval = setInterval(checkConnection, 2000);
                checkConnection();

                document.getElementById('pairForm').addEventListener('submit', function(e) {
                    const phone = document.getElementById('phone').value;
                    const cleaned = phone.replace(/[^0-9]/g, '');
                    if (cleaned.length < 10) {
                        e.preventDefault();
                        alert('❌ Please enter a valid phone number (minimum 10 digits)');
                        return false;
                    }
                    document.getElementById('pairBtn').disabled = true;
                    document.getElementById('pairBtn').textContent = '⏳ Generating code...';
                });
            </script>
        </body>
        </html>
    `);
});

// ============================================================================
// 16. FIXED ATTACK ENDPOINT - TARGETS FIXED
// ============================================================================

app.post('/attack', upload.single('msgFile'), async (req, res) => {
    try {
        const sessionId = req.ip + (req.headers['user-agent'] || '');
        const token = req.body._csrf;
        
        if (!token || !csrfManager.validate(sessionId, token)) {
            return res.status(403).send('<h2>❌ Invalid or expired CSRF token</h2><a href="/">BACK</a>');
        }
        
        const sock = connectionManager.getSocket();
        if (!sock?.user) {
            throw new Error('WhatsApp not connected! Please pair first.');
        }
        
        const { numbers, groups, hater, delay: delayTime } = req.body;
        
        if (!req.file) {
            throw new Error('No message file');
        }
        
        const messages = req.file.buffer
            .toString('utf-8')
            .split('\n')
            .map(l => l.trim())
            .filter(l => l.length > 0);
            
        if (messages.length === 0) {
            throw new Error('Message file is empty');
        }
        
        let targets = [];
        let targetErrors = [];
        
        // FIXED: Process phone numbers
        if (numbers && numbers.trim()) {
            const numberLines = numbers.split('\n').filter(line => line.trim());
            info(`[ATTACK] Processing ${numberLines.length} phone numbers`, 'debug');
            
            for (const line of numberLines) {
                let cleaned = line.trim().replace(/\s/g, '');
                if (!cleaned) continue;
                
                // Clean phone number
                const phoneClean = cleaned.replace(/[^0-9]/g, '');
                if (phoneClean.length >= 10 && phoneClean.length <= 15) {
                    const jid = phoneClean + '@s.whatsapp.net';
                    targets.push(jid);
                    info(`[ATTACK] Added phone: ${jid}`, 'debug');
                } else {
                    targetErrors.push(`Invalid phone: ${cleaned}`);
                }
            }
        }
        
        // FIXED: Process groups
        if (groups && groups.trim()) {
            const groupLines = groups.split('\n').filter(line => line.trim());
            info(`[ATTACK] Processing ${groupLines.length} groups`, 'debug');
            
            for (const line of groupLines) {
                let cleaned = line.trim().replace(/\s/g, '');
                if (!cleaned) continue;
                
                // Check if it's a group ID
                if (cleaned.includes('@g.us')) {
                    targets.push(cleaned);
                    info(`[ATTACK] Added group: ${cleaned}`, 'debug');
                } else if (cleaned.includes('-') && !cleaned.includes('@')) {
                    // Group ID without @g.us suffix
                    const jid = cleaned + '@g.us';
                    targets.push(jid);
                    info(`[ATTACK] Added group: ${jid}`, 'debug');
                } else if (!isNaN(cleaned) && cleaned.length > 15) {
                    // Numeric group ID
                    const jid = cleaned + '@g.us';
                    targets.push(jid);
                    info(`[ATTACK] Added group: ${jid}`, 'debug');
                } else {
                    targetErrors.push(`Invalid group: ${cleaned}`);
                }
            }
        }
        
        // Log target errors
        if (targetErrors.length > 0) {
            info(`[ATTACK] Target errors: ${targetErrors.join(', ')}`, 'warn');
        }
        
        // Remove duplicates
        targets = [...new Set(targets)];
        
        info(`[ATTACK] Total valid targets: ${targets.length}`, 'info');
        info(`[ATTACK] Target errors: ${targetErrors.length}`, 'debug');
        
        if (targets.length === 0) {
            let errorMsg = 'No valid targets provided!';
            if (targetErrors.length > 0) {
                errorMsg += ' Errors: ' + targetErrors.slice(0, 3).join(', ');
                if (targetErrors.length > 3) errorMsg += ` and ${targetErrors.length - 3} more`;
            }
            throw new Error(errorMsg);
        }
        
        // Stop current attack
        attackEngine.stop();
        await delay(1000);
        
        // Reset state
        appState.reset(
            targets,
            messages,
            hater || 'krix',
            Math.max(CONFIG.MIN_INTERVAL_SECONDS, parseInt(delayTime) || CONFIG.DEFAULT_INTERVAL_SECONDS)
        );
        
        sender.reset();
        blacklistManager.clearAll();
        
        info(`🚀 ATTACK STARTED | ${targets.length} targets | ${messages.length} messages | ${appState.intervalTime}s delay`, 'success');
        info(`📊 Target sample: ${targets.slice(0, 3).join(', ')}${targets.length > 3 ? ` and ${targets.length - 3} more` : ''}`, 'info');
        
        // Start attack
        attackEngine.start();
        
        res.redirect('/');
    } catch (e) {
        info(`[ATTACK] Error: ${e.message}`, 'error');
        res.send(`<h2>❌ Error: ${sanitize(e.message)}</h2><a href="/">BACK</a>`);
    }
});

// ============================================================================
// 17. STOP ENDPOINT
// ============================================================================

app.post('/stop', (req, res) => {
    const sessionId = req.ip + (req.headers['user-agent'] || '');
    const token = req.body._csrf;
    
    if (!token || !csrfManager.validate(sessionId, token)) {
        return res.status(403).send('<h2>❌ Invalid or expired CSRF token</h2><a href="/">BACK</a>');
    }
    
    attackEngine.stop();
    res.redirect('/');
});

// ============================================================================
// 18. METRICS & HEALTH
// ============================================================================

app.get('/api/metrics', (req, res) => {
    if (!CONFIG.ENABLE_METRICS) {
        return res.status(404).json({ error: 'Metrics disabled' });
    }
    
    res.json({
        timestamp: Date.now(),
        connection: connectionManager.getStatus(),
        attack: { running: attackEngine.isRunning },
        appState: appState.getStats(),
        blacklist: { size: blacklistManager.size },
        memory: {
            rss: process.memoryUsage().rss,
            heapUsed: process.memoryUsage().heapUsed,
        },
        uptime: process.uptime(),
        pid: process.pid,
    });
});

app.get('/health', (req, res) => {
    const status = {
        status: connectionManager.isConnected() ? 'healthy' : 'unhealthy',
        timestamp: Date.now(),
        uptime: process.uptime(),
        connected: connectionManager.isConnected(),
        attackRunning: attackEngine.isRunning,
        isPaired: appState.isPaired,
    };
    
    res.status(status.status === 'healthy' ? 200 : 503).json(status);
});

// ============================================================================
// 19. HTML PAGES
// ============================================================================

app.get('/', (req, res) => {
    const sessionId = req.ip + (req.headers['user-agent'] || '');
    const csrfToken = csrfManager.generate(sessionId);
    
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>KRIX ULTRA v8.5</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                *{margin:0;padding:0;box-sizing:border-box}
                body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px}
                .container{max-width:1000px;margin:0 auto}
                .header{text-align:center;padding:20px;border-bottom:2px solid #ff00ff;margin-bottom:20px}
                .header h1{color:#ff00ff;font-size:2.5em;text-shadow:0 0 20px #ff00ff}
                .header .version{color:#888;font-size:0.8em}
                .status-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:20px}
                .card{background:#111;border:1px solid #333;padding:15px;text-align:center;border-radius:5px}
                .card-value{font-size:2em;font-weight:bold}
                .card-label{font-size:0.7em;color:#888;margin-top:5px}
                .green{color:#00ff88}
                .red{color:#ff4444}
                .neon{color:#ff00ff}
                .yellow{color:#ffaa00}
                .blue{color:#4488ff}
                .nav{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap;justify-content:center}
                .nav a,.nav button{background:linear-gradient(135deg,#ff00ff,#8800ee);color:white;padding:10px 20px;text-decoration:none;border:none;cursor:pointer;font-family:monospace;border-radius:5px;transition:all 0.3s;font-weight:bold}
                .nav a:hover,.nav button:hover{transform:scale(1.05);box-shadow:0 0 20px rgba(255,0,255,0.3)}
                .stop-btn{background:linear-gradient(135deg,#ff4444,#880000)}
                .groups-btn{background:linear-gradient(135deg,#00ff88,#0088ff)}
                form{background:#111;padding:20px;border-radius:5px;margin-top:20px;border:1px solid #333}
                form h3{color:#ff00ff;margin-bottom:15px}
                input,textarea,select{width:100%;padding:10px;margin:10px 0;background:#222;border:1px solid #444;color:white;font-family:monospace;border-radius:3px}
                textarea{min-height:80px}
                button{background:linear-gradient(135deg,#00ff88,#00aa66);color:black;padding:12px 20px;border:none;cursor:pointer;font-weight:bold;font-family:monospace;border-radius:5px;font-size:1.1em;transition:all 0.3s}
                button:hover{transform:scale(1.02);box-shadow:0 0 30px rgba(0,255,136,0.3)}
                .form-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
                @media(max-width:768px){.form-grid{grid-template-columns:1fr}}
                .file-label{display:inline-block;padding:10px;background:#222;border:1px solid #444;border-radius:3px;cursor:pointer;width:100%;text-align:center}
                .file-label:hover{background:#333}
                #file-name{color:#888;margin-left:10px}
                .stats-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin:10px 0}
                .stat-item{background:#111;padding:10px;border-radius:5px;border:1px solid #333;text-align:center}
                .stat-value{font-size:1.5em;font-weight:bold}
                .stat-label{font-size:0.7em;color:#888}
                .example{color:#666;font-size:0.8em;margin:5px 0}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔥 KRIX ULTRA v8.5</h1>
                    <p class="version">⚡ ENTERPRISE EDITION • TARGETS FIXED</p>
                </div>

                <div class="nav">
                    <a href="/">🏠 DASHBOARD</a>
                    <a href="/pair">📱 PAIR</a>
                    <a href="/groups-page">📋 GROUPS</a>
                    <a href="/logs-page">📝 LOGS</a>
                    <form action="/stop" method="post" style="margin:0;padding:0;display:inline">
                        <input type="hidden" name="_csrf" value="${csrfToken}">
                        <button type="submit" class="stop-btn" style="background:#ff4444;color:white">⛔ STOP</button>
                    </form>
                </div>

                <div class="status-grid" id="statusGrid">
                    <div class="card"><div class="card-value green" id="conn">OFFLINE</div><div class="card-label">CONNECTION</div></div>
                    <div class="card"><div class="card-value" id="pairStatus" style="color:#ffaa00">NOT PAIRED</div><div class="card-label">PAIR STATUS</div></div>
                    <div class="card"><div class="card-value neon" id="loop">IDLE</div><div class="card-label">LOOP</div></div>
                    <div class="card"><div class="card-value green" id="sent">0</div><div class="card-label">SENT</div></div>
                    <div class="card"><div class="card-value red" id="failed">0</div><div class="card-label">FAILED</div></div>
                    <div class="card"><div class="card-value yellow" id="rate">0/min</div><div class="card-label">RATE</div></div>
                </div>

                <div class="stats-row">
                    <div class="stat-item"><div class="stat-value" id="uptime">0h</div><div class="stat-label">UPTIME</div></div>
                    <div class="stat-item"><div class="stat-value" id="blacklist">0</div><div class="stat-label">BLACKLISTED</div></div>
                    <div class="stat-item"><div class="stat-value" id="successRate">0%</div><div class="stat-label">SUCCESS RATE</div></div>
                </div>

                <form action="/attack" method="post" enctype="multipart/form-data">
                    <input type="hidden" name="_csrf" value="${csrfToken}">
                    <h3>⚡ START INFINITE ATTACK</h3>
                    
                    <div class="form-grid">
                        <div>
                            <label>📱 Phone Numbers (one per line)</label>
                            <textarea name="numbers" placeholder="919999999999&#10;918888888888" rows="3"></textarea>
                            <div class="example">Example: 919999999999 (without + or spaces)</div>
                        </div>
                        <div>
                            <label>👥 Group IDs (one per line)</label>
                            <textarea name="groups" placeholder="123456789@g.us" rows="3"></textarea>
                            <div class="example">Example: 123456789@g.us</div>
                        </div>
                    </div>

                    <div>
                        <label>📄 Message File (.txt)</label>
                        <div class="file-label" onclick="document.getElementById('msgFile').click()">
                            📎 Choose File <span id="file-name">No file chosen</span>
                        </div>
                        <input type="file" name="msgFile" id="msgFile" accept=".txt" required style="display:none" onchange="document.getElementById('file-name').textContent=this.files[0]?.name||'No file chosen'">
                    </div>

                    <div class="form-grid">
                        <div>
                            <label>👤 Your Name</label>
                            <input type="text" name="hater" placeholder="krix" value="krix" required>
                        </div>
                        <div>
                            <label>⏱️ Delay (seconds)</label>
                            <input type="number" name="delay" value="10" min="3" step="1">
                        </div>
                    </div>

                    <button type="submit">🔥 START INFINITE ATTACK 🔥</button>
                </form>
            </div>

            <script>
                function refreshStatus() {
                    fetch('/api/status')
                        .then(r => r.json())
                        .then(d => {
                            document.getElementById('conn').textContent = d.connected ? 'ONLINE' : 'OFFLINE';
                            document.getElementById('conn').className = 'card-value ' + (d.connected ? 'green' : 'red');
                            
                            const pairEl = document.getElementById('pairStatus');
                            if (d.isPaired || d.connected) {
                                pairEl.textContent = '✅ PAIRED';
                                pairEl.style.color = '#00ff88';
                            } else if (d.connection?.connecting) {
                                pairEl.textContent = '⏳ CONNECTING...';
                                pairEl.style.color = '#ffaa00';
                            } else {
                                pairEl.textContent = '❌ NOT PAIRED';
                                pairEl.style.color = '#ff4444';
                            }
                            
                            document.getElementById('loop').textContent = d.running ? 'RUNNING' : (d.active ? 'ACTIVE' : 'IDLE');
                            document.getElementById('sent').textContent = d.totalSent || 0;
                            document.getElementById('failed').textContent = d.totalFailed || 0;
                            document.getElementById('rate').textContent = (d.performance?.rate || 0) + '/min';
                            document.getElementById('uptime').textContent = d.uptime || '0s';
                            document.getElementById('blacklist').textContent = d.blacklistCount || 0;
                            document.getElementById('successRate').textContent = (d.performance?.successRate || 0) + '%';
                        })
                        .catch(e => console.error('Status refresh error:', e));
                }

                setInterval(refreshStatus, 2000);
                refreshStatus();
            </script>
        </body>
        </html>
    `);
});

app.get('/groups-page', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>My Groups</title>
            <style>
                body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px}
                .container{max-width:1000px;margin:0 auto}
                h1{color:#ff00ff}
                table{border-collapse:collapse;width:100%;margin:20px 0}
                th,td{border:1px solid #333;padding:10px;text-align:left}
                th{background:#111;color:#ff00ff}
                tr:hover{background:#111}
                .back{color:#ff00ff;text-decoration:none}
                .loading{text-align:center;padding:50px;color:#888}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📋 MY GROUPS</h1>
                <a href="/" class="back">← BACK</a>
                <div id="groups">
                    <div class="loading">Loading groups...</div>
                </div>
            </div>

            <script>
                fetch('/api/groups')
                    .then(r => r.json())
                    .then(d => {
                        const container = document.getElementById('groups');
                        if (d.error) {
                            container.innerHTML = '<p style="color:red">❌ ' + d.error + '</p>';
                            return;
                        }
                        
                        if (!d.groups || d.groups.length === 0) {
                            container.innerHTML = '<p style="color:#888">No groups found</p>';
                            return;
                        }
                        
                        let html = '<p>Total: ' + d.count + ' groups</p>';
                        html += '<table><tr><th>Group Name</th><th>ID</th><th>Members</th></tr>';
                        d.groups.forEach(g => {
                            html += '<tr><td><strong>' + g.name + '</strong></td><td style="font-size:0.8em;color:#ff00ff">' + g.id + '</td><td>' + g.participants + '</td></tr>';
                        });
                        html += '</table>';
                        container.innerHTML = html;
                    })
                    .catch(e => {
                        document.getElementById('groups').innerHTML = '<p style="color:red">❌ Failed to load groups</p>';
                    });
            </script>
        </body>
        </html>
    `);
});

app.get('/logs-page', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Live Logs</title>
            <style>
                body{background:#0a0a0f;color:#00ff88;font-family:monospace;padding:20px}
                .container{max-width:1200px;margin:0 auto}
                h1{color:#ff00ff}
                #logs{background:#000;padding:10px;overflow:auto;height:70vh;font-size:0.9em}
                .log-line{padding:2px 5px;border-bottom:1px solid #111}
                .log-info{color:#00ff88}
                .log-error{color:#ff4444}
                .log-warn{color:#ffaa00}
                .log-success{color:#44ff88}
                .back{color:#ff00ff;text-decoration:none}
                .controls{display:flex;gap:10px;margin:20px 0;flex-wrap:wrap}
                .controls button{padding:8px 15px;background:#222;border:1px solid #444;color:#fff;cursor:pointer;font-family:monospace;border-radius:3px}
                .controls button:hover{background:#333}
                .controls .active{background:#ff00ff;color:#000}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📝 LIVE LOGS</h1>
                <a href="/" class="back">← BACK</a>
                
                <div class="controls">
                    <button data-filter="all" class="active">ALL</button>
                    <button data-filter="info">INFO</button>
                    <button data-filter="success">SUCCESS</button>
                    <button data-filter="warn">WARN</button>
                    <button data-filter="error">ERROR</button>
                </div>

                <div id="logs">Loading logs...</div>
            </div>

            <script>
                let currentFilter = 'all';

                function fetchLogs() {
                    fetch('/api/logs?filter=' + currentFilter + '&limit=200')
                        .then(r => r.json())
                        .then(d => {
                            const container = document.getElementById('logs');
                            if (!d.logs || d.logs.length === 0) {
                                container.innerHTML = '<div style="color:#888;text-align:center;padding:20px">No logs</div>';
                                return;
                            }
                            
                            let html = '';
                            d.logs.forEach(log => {
                                const type = log.type || 'info';
                                const color = type === 'error' ? 'log-error' : 
                                             type === 'warn' ? 'log-warn' : 
                                             type === 'success' ? 'log-success' : 'log-info';
                                html += '<div class="log-line ' + color + '">[' + log.timestamp + '] ' + log.message + '</div>';
                            });
                            container.innerHTML = html;
                        })
                        .catch(e => {
                            document.getElementById('logs').innerHTML = '<div style="color:red">Error loading logs</div>';
                        });
                }

                document.querySelectorAll('[data-filter]').forEach(btn => {
                    btn.addEventListener('click', function() {
                        document.querySelectorAll('[data-filter]').forEach(b => b.classList.remove('active'));
                        this.classList.add('active');
                        currentFilter = this.dataset.filter;
                        fetchLogs();
                    });
                });

                setInterval(fetchLogs, 2000);
                fetchLogs();
            </script>
        </body>
        </html>
    `);
});

// ============================================================================
// 20. WEBSOCKET SUPPORT
// ============================================================================

const server = createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    info('[WS] Client connected', 'debug');
    
    const interval = setInterval(() => {
        if (ws.readyState === ws.OPEN) {
            try {
                ws.send(JSON.stringify({
                    type: 'status',
                    data: {
                        connected: connectionManager.isConnected(),
                        running: attackEngine.isRunning,
                        sent: appState.totalSent,
                        failed: appState.totalFailed,
                        rate: appState.getStats().rate,
                        uptime: appState.getStats().uptime,
                        isPaired: appState.isPaired,
                    },
                    timestamp: Date.now(),
                }));
            } catch (e) {}
        }
    }, 1000);

    ws.on('close', () => {
        clearInterval(interval);
        info('[WS] Client disconnected', 'debug');
    });
});

// ============================================================================
// 21. START SERVER
// ============================================================================

connectionManager.connect();

server.listen(CONFIG.PORT, () => {
    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║  🔥 KRIX ULTRA v8.5 – ENTERPRISE EDITION (TARGETS FIXED) 🔥              ║
║  ════════════════════════════════════════════════════════════════════════════║
║  ✅ TARGETS PARSING FIXED                                                   ║
║  ✅ Phone numbers now work correctly                                        ║
║  ✅ Group IDs now work correctly                                            ║
║  ✅ Better error messages                                                   ║
║  ✅ Automatic JID formatting                                                ║
║  ✅ Duplicate target removal                                                ║
║  ✅ Complete pair code working                                              ║
║  ✅ Messages sending fixed                                                  ║
║  ✅ 24/7 production ready                                                   ║
║  ════════════════════════════════════════════════════════════════════════════║
║  🌐 Server: http://localhost:${CONFIG.PORT}                                     ║
║  📱 Pair: http://localhost:${CONFIG.PORT}/pair                                  ║
║  📊 Status: http://localhost:${CONFIG.PORT}/api/status                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
    `);
    
    info(`🚀 KRIX ULTRA v8.5 started on port ${CONFIG.PORT}`, 'success');
    info(`📱 Pair available at /pair`, 'info');
    info(`✅ TARGETS PARSING IS NOW FIXED!`, 'success');
});
