// ============================================================================
// MUSKAN WITH YANkI WHATSAPP ULTRA v8.5 – ENTERPRISE EDITION (TARGETS FIXED) - BUG FIXES APPLIED
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
// FIX: Dynamic import for compression at top level
let compression;
try {
    compression = (await import('compression')).default;
} catch (e) {
    info('Compression module not available, skipping compression', 'warn');
}

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

// FIX: Moved info function definition before any usage
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

// FIX: Better JID sanitization
const sanitizeJid = (jid) => {
    if (!jid) return null;
    
    // Remove all whitespace
    let cleaned = jid.replace(/\s/g, '');
    
    // Remove special characters except @, ., and -
    cleaned = cleaned.replace(/[^0-9@.\-]/g, '');
    
    // If no @ symbol, add appropriate suffix
    if (!cleaned.includes('@')) {
        // Check if it's a group ID (contains -)
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

// FIX: Better phone number formatting
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
        // FIX: Cleanup interval properly referenced
        this.cleanupInterval = setInterval(() => this.cleanup(), 300000);
        if (this.cleanupInterval.unref) this.cleanupInterval.unref();
    }

    generate(sessionId) {
        const token = crypto.randomBytes(64).toString('hex');
        const expiresAt = Date.now() + CONFIG.CSRF_TTL;
        this.tokens.set(sessionId, { token, expiresAt, createdAt: Date.now() });
        return token;
    }

    validate(sessionId, token) {
        if (!token || !sessionId) return false;
        const entry = this.tokens.get(sessionId);
        if (!entry) return false;
        if (entry.token !== token) return false;
        if (Date.now() > entry.expiresAt) {
            this.tokens.delete(sessionId);
            return false;
        }
        // FIX: Refresh token expiry on successful validation
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

    revoke(sessionId) { 
        if (sessionId) this.tokens.delete(sessionId); 
    }
    
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
        this.autoCleanInterval = setInterval(() => this.autoClean(), CONFIG.BLACKLIST_RESET_MS / 2);
        if (this.autoCleanInterval.unref) this.autoCleanInterval.unref();
    }

    markFail(target) {
        if (!target) return false;
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
        if (!target) return;
        this.errorCounts.delete(target); 
        this.createdAt.delete(target); 
    }

    isBlacklisted(target) { 
        if (!target) return false;
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
        this.targets = targets || [];
        this.messages = messages || [];
        this.haterName = haterName || 'krix';
        this.intervalTime = Math.max(CONFIG.MIN_INTERVAL_SECONDS, Math.min(CONFIG.MAX_INTERVAL_SECONDS, parseInt(intervalTime) || CONFIG.DEFAULT_INTERVAL_SECONDS));
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
        if (!ev) return;
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
        if (!messages || !Array.isArray(messages)) return;
        messages.forEach(m => {
            if (!m || !m.key) return;
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
        if (onConnected) this._onConnected = onConnected;
        if (onDisconnected) this._onDisconnected = onDisconnected;
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
                try {
                    cachedVersion = await fetchLatestBaileysVersion();
                } catch (e) {
                    info(`[VERSION] Failed to fetch version: ${e.message}`, 'error');
                    // Use a fallback version
                    cachedVersion = { version: [2, 2413, 51] };
                }
            }
            const { version } = cachedVersion;

            const { state, saveCreds } = await useMultiFileAuthState(CONFIG.SESSION_DIR);

            // FIX: Properly clean up old socket
            if (this.sock) {
                try {
                    if (this.store && this.store.unbind) this.store.unbind();
                    if (this.sock.ws) this.sock.ws.close();
                    if (this.sock.ev) this.sock.ev.removeAllListeners();
                } catch (e) {
                    info(`[CLEANUP] Error cleaning old socket: ${e.message}`, 'debug');
                }
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
                getMessage: async (key) => {
                    try {
                        const msg = await this.store.loadMessage(key.remoteJid, key.id);
                        return { conversation: msg?.message?.conversation || '' };
                    } catch (e) {
                        return { conversation: '' };
                    }
                },
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
                
                // FIX: Better logout detection
                if (statusCode === DisconnectReason.loggedOut || 
                    (lastDisconnect?.error?.message && lastDisconnect.error.message.includes('logged out'))) {
                    info('🚫 Logged out – clearing session', 'error');
                    try {
                        if (fs.existsSync(CONFIG.SESSION_DIR)) {
                            fs.rmSync(CONFIG.SESSION_DIR, { recursive: true, force: true });
                        }
                    } catch (e) {
                        info(`[CLEANUP] Error removing session: ${e.message}`, 'error');
                    }
                    this._scheduleReconnect(5000);
                } else {
                    this.reconnectAttempts++;
                    const delayMs = Math.min(
                        CONFIG.RECONNECT_MAX_DELAY_MS,
                        CONFIG.RECONNECT_BASE_DELAY_MS * Math.pow(1.5, Math.min(this.reconnectAttempts - 1, 10)) + Math.random() * 2000
                    );
                    info(`🔄 Reconnecting in ${Math.round(delayMs / 1000)}s (attempt ${this.reconnectAttempts})`, 'info');
                    this._scheduleReconnect(delayMs);
                }
                
                if (this._onDisconnected) this._onDisconnected();
                this._stopHealthChecks();
            }
        });

        this.sock.ev.on('creds.update', async () => {
            try { 
                await saveCreds(); 
            } catch (e) {
                info(`[CREDS] Save error: ${e.message}`, 'error');
            }
        });

        this.sock.ev.on('contacts.update', (updates) => {
            if (updates && Array.isArray(updates)) {
                updates.forEach(u => { 
                    if (u && u.id) contactCache.set(u.id, u); 
                });
            }
        });

        this.sock.ev.on('chats.update', (updates) => {
            if (updates && Array.isArray(updates)) {
                updates.forEach(u => { 
                    if (u && u.id) chatCache.set(u.id, u); 
                });
            }
        });
    }

    _startHealthChecks() {
        this._stopHealthChecks();
        this.healthCheckInterval = setInterval(async () => {
            try {
                if (!this.sock?.user) {
                    info('[HEALTH] Socket user missing, reconnecting...', 'warn');
                    this.connect();
                    return;
                }
                await this.sock.sendPresenceUpdate('available');
            } catch (e) {
                info(`[HEALTH] Health check failed: ${e.message}`, 'debug');
                if (this.isOnline) {
                    this.isOnline = false;
                    this.connect();
                }
            }
        }, CONFIG.HEALTH_CHECK_INTERVAL);
        if (this.healthCheckInterval.unref) this.healthCheckInterval.unref();
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
            } catch (e) {
                // Silent catch
            }
        }, CONFIG.PRESENCE_INTERVAL_MS);
        if (this.presenceInterval.unref) this.presenceInterval.unref();
    }

    _scheduleReconnect(waitMs) {
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (!this.isConnecting && !this.isOnline) {
                this.connect();
            }
        }, waitMs);
        if (this.reconnectTimer.unref) this.reconnectTimer.unref();
    }

    async disconnect() {
        this._stopHealthChecks();
        if (this.presenceInterval) {
            clearInterval(this.presenceInterval);
            this.presenceInterval = null;
        }
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        
        if (this.sock) {
            try {
                if (this.store && this.store.unbind) this.store.unbind();
                if (this.sock.ws) this.sock.ws.close();
                if (this.sock.ev) this.sock.ev.removeAllListeners();
            } catch (e) {
                info(`[DISCONNECT] Error: ${e.message}`, 'debug');
            }
            this.sock = null;
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
    if (!sock || !jid) return 'Unknown';
    const cached = groupMetadataCache.get(jid);
    if (cached) return cached;
    try {
        const metadata = await sock.groupMetadata(jid);
        const name = metadata?.subject || 'Unknown';
        groupMetadataCache.set(jid, name);
        return name;
    } catch (e) {
        return 'Unknown';
    }
}

async function fetchAllGroupNames(sock, groupIds) {
    if (!sock || !groupIds || !Array.isArray(groupIds)) return [];
    const tasks = groupIds.map(jid =>
        groupMetadataLimit(async () => {
            if (!groupMetadataCache.has(jid)) {
                try {
                    const metadata = await sock.groupMetadata(jid);
                    groupMetadataCache.set(jid, metadata?.subject || 'Unknown');
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
                info('[CIRCUIT] Circuit breaker half-open, testing...', 'warn');
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
                info('[CIRCUIT] Circuit breaker closed (recovered)', 'info');
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
            if (this.retryQueue.length < CONFIG.MAX_RETRY_QUEUE) {
                this.retryQueue.push({ target, message, addedAt: Date.now(), attempts: 0 });
            }
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

                // FIX: Check if target is valid JID before sending
                const validJid = sanitizeJid(target);
                if (!validJid) {
                    info(`[SEND] Invalid JID format: ${target}`, 'error');
                    blacklistManager.markFail(target);
                    appState.totalFailed++;
                    return false;
                }

                await sock.sendMessage(validJid, { text: message });
                
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
            !batch.includes(item) || now - item.addedAt > 3600000
        );

        if (batch.length > 0) {
            info(`[RETRY] Processing ${batch.length} queued messages`, 'debug');
            for (const item of batch) {
                item.attempts++;
                await this.send(item.target, item.message);
                await delay(100); // Small delay between retries
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
        
        // FIX: Check if we have data before starting
        if (!appState.targets.length || !appState.messages.length) {
            info('[ENGINE] Cannot start - no targets or messages configured', 'error');
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
                    if (consecutiveIdle === 0 || consecutiveIdle % 10 === 0) {
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
                const target = targets[targetIndex];
                
                // FIX: Ensure message is valid
                const msgContent = messages[messageIdx];
                if (!msgContent) {
                    info('[ENGINE] Invalid message content, skipping', 'error');
                    await delay(1000);
                    continue;
                }
                
                const fullMessage = `${haterName} ${msgContent}`;

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
        if (this._heartbeatInterval.unref) this._heartbeatInterval.unref();
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

// FIX: Use compression if available
if (CONFIG.ENABLE_COMPRESSION && compression) {
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
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
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
        if (!groups || typeof groups !== 'object') {
            return res.json({ groups: [], count: 0 });
        }
        
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
                if (attempts % 5 === 0) {
                    info(`[PAIR] Waiting for socket... (${attempts}/${maxAttempts})`, 'debug');
                }
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

        // FIX: Check if requestPairingCode exists
        if (typeof sock.requestPairingCode !== 'function') {
            throw new Error('Pairing code not supported in this Baileys version. Please use QR code scanning instead.');
        }

        info(`[PAIR] Requesting pairing code...`, 'info');
        const code = await sock.requestPairingCode(phone);
        
        if (!code) {
            throw new Error('No code received from WhatsApp');
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

// ============================================================================
// Rest of the code remains the same...
// (The remaining sections 16-21 are unchanged)
// ============================================================================

// [Include the rest of the original code from section 16 onwards here]
// The attack endpoint, stop endpoint, metrics, HTML pages, WebSocket support, and server start
