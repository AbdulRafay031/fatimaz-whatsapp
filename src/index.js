import "dotenv/config";
import os from "node:os";
import process from "node:process";
import { randomUUID } from "node:crypto";
import makeWASocket, { DisconnectReason, makeCacheableSignalKeyStore } from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import { MongoClient } from "mongodb";
import pino from "pino";
import QRCode from "qrcode";
import { useMongoAuthState } from "./mongoAuthState.js";
import { resolveDatabaseName } from "./mongoConfig.js";

const uri = process.env.MONGODB_URI;
if (!uri) throw new Error("MONGODB_URI is required for the Fatimaz WhatsApp worker");

const databaseName = resolveDatabaseName(uri, process.env.MONGODB_DB);
const client = new MongoClient(uri, { maxPoolSize: 12 });
await client.connect();
const db = databaseName ? client.db(databaseName) : client.db();
console.log(`[whatsapp] MongoDB database: ${db.databaseName}`);
const jobs = db.collection("whatsappjobs");
const connections = db.collection("whatsappconnections");
const locks = db.collection("whatsappworkerlocks");

const workerId = process.env.WHATSAPP_WORKER_ID || "fatimaz-whatsapp-production";
const instanceId = `${workerId}:${os.hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
const logger = pino({ level: process.env.WHATSAPP_LOG_LEVEL || "warn" });

const QUEUE_CONCURRENCY = clampInt(process.env.WHATSAPP_QUEUE_CONCURRENCY, 2, 1, 5);
const SEND_INTERVAL_MS = clampInt(process.env.WHATSAPP_SEND_INTERVAL_MS, 400, 100, 5000);
const WORKER_LEASE_MS = clampInt(process.env.WHATSAPP_WORKER_LEASE_MS, 30_000, 15_000, 120_000);
const JOB_LOCK_MS = clampInt(process.env.WHATSAPP_JOB_LOCK_MS, 120_000, 60_000, 600_000);
const SOCKET_READY_TIMEOUT_MS = clampInt(process.env.WHATSAPP_SOCKET_READY_TIMEOUT_MS, 15_000, 3_000, 60_000);

let sock = null;
let connected = false;
let connecting = false;
let connectingSince = 0;
let shuttingDown = false;
let authTools = null;
let reconnectTimer = null;
let socketGeneration = 0;
let socketStartPromise = null;
let ownsLease = false;
let leaseExpiresAt = null;
let pumpRunning = false;
let lastSendStartedAt = 0;
let sendGate = Promise.resolve();
const activeJobs = new Map();

function clampInt(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function ensureConnectionDoc() {
  await connections.updateOne(
    { key: "primary" },
    { $setOnInsert: { key: "primary", status: "disconnected", command: "" } },
    { upsert: true }
  );
}

async function ensureWorkerLockDoc() {
  await locks.updateOne(
    { _id: "primary" },
    { $setOnInsert: { ownerId: "", workerId: "", leaseUntil: new Date(0), heartbeatAt: new Date(0) } },
    { upsert: true }
  );
}

async function setConnection(fields, { requireLease = true } = {}) {
  if (requireLease && !ownsLease) return;
  const filter = requireLease ? { key: "primary", workerInstanceId: instanceId } : { key: "primary" };
  const update = {
    ...fields,
    workerId,
    workerInstanceId: instanceId,
    workerHost: os.hostname(),
    workerPid: process.pid,
    workerLeaseExpiresAt: leaseExpiresAt,
  };
  const result = await connections.updateOne(filter, { $set: update }, { upsert: !requireLease });
  if (requireLease && result.matchedCount === 0 && ownsLease) {
    await connections.updateOne(
      { key: "primary" },
      { $set: update, $setOnInsert: { key: "primary", command: "" } },
      { upsert: true }
    );
  }
}

function phoneFromUser(user) {
  const id = String(user?.id || "");
  return id.split(":")[0].split("@")[0];
}

function currentSendReady() {
  return Boolean(ownsLease && connected && sock && !connecting && !shuttingDown);
}

async function acquireOrRenewLease() {
  if (shuttingDown) return false;
  await ensureWorkerLockDoc();
  const now = new Date();
  const nextLease = new Date(Date.now() + WORKER_LEASE_MS);
  const raw = await locks.findOneAndUpdate(
    {
      _id: "primary",
      $or: [
        { ownerId: instanceId },
        { ownerId: "" },
        { ownerId: { $exists: false } },
        { leaseUntil: { $lte: now } },
        { leaseUntil: null },
      ],
    },
    {
      $set: {
        ownerId: instanceId,
        workerId,
        host: os.hostname(),
        pid: process.pid,
        heartbeatAt: now,
        leaseUntil: nextLease,
      },
    },
    { returnDocument: "after" }
  );
  const doc = raw?.value || raw;
  const acquired = doc?.ownerId === instanceId;
  const wasOwner = ownsLease;
  ownsLease = acquired;
  leaseExpiresAt = acquired ? nextLease : doc?.leaseUntil || null;

  if (acquired && !wasOwner) {
    console.log(`[whatsapp] active-worker lease acquired (${instanceId})`);
    await setConnection({
      workerHeartbeatAt: now,
      queueConsumerHeartbeatAt: now,
      queueConcurrency: QUEUE_CONCURRENCY,
      queueActiveJobs: activeJobs.size,
      sendReady: false,
      lastQueueError: "",
    }, { requireLease: false });
    if (!connected && !connecting) startSocket().catch((e) => console.error("[whatsapp] socket start error", e));
  } else if (!acquired && wasOwner) {
    console.error("[whatsapp] active-worker lease lost; closing this instance socket to prevent duplicate sessions");
    await stopCurrentSocket("Worker lease lost");
  }

  return acquired;
}

async function releaseLease() {
  await locks.updateOne(
    { _id: "primary", ownerId: instanceId },
    { $set: { ownerId: "", leaseUntil: new Date(0), heartbeatAt: new Date() } }
  ).catch(() => {});
  ownsLease = false;
}

function reconnectDelayFor(code) {
  if (code === DisconnectReason.restartRequired || code === 515) return 750;
  if (code === DisconnectReason.timedOut || code === 408) return 2500;
  if (code === DisconnectReason.connectionReplaced || code === 440) return 4000;
  return 3000;
}

function scheduleReconnect(delay = 3000) {
  if (!ownsLease || shuttingDown) return;
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    startSocket().catch((e) => console.error("[whatsapp] reconnect error", e));
  }, delay);
}

async function stopCurrentSocket(reason = "Socket reset") {
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  const old = sock;
  socketGeneration += 1; // invalidates all listeners from the old socket immediately
  sock = null;
  connected = false;
  connecting = false;
  connectingSince = 0;
  if (old) {
    try { old.end?.(new Error(reason)); } catch {}
  }
}

async function startSocket() {
  if (!ownsLease || shuttingDown || connected) return;
  if (socketStartPromise) return socketStartPromise;

  socketStartPromise = (async () => {
    if (!ownsLease || shuttingDown || connected) return;
    connecting = true;
    connectingSince = Date.now();
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
    const generation = ++socketGeneration;
    await setConnection({ status: "connecting", lastError: "", qrDataUrl: "", sendReady: false });

    try {
      authTools = await useMongoAuthState(db, "primary");
      const { state, saveCreds } = authTools;
      const wa = makeWASocket({
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, logger),
        },
        logger,
        markOnlineOnConnect: false,
        syncFullHistory: false,
        generateHighQualityLinkPreview: false,
      });
      sock = wa;

      wa.ev.on("creds.update", () => saveCreds().catch((error) => console.error("[whatsapp] auth save error", error)));
      wa.ev.on("connection.update", async (update) => {
        try {
          if (generation !== socketGeneration || !ownsLease || shuttingDown) return;
          const { connection, lastDisconnect, qr } = update;

        if (qr) {
          const qrDataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 420 });
          await setConnection({ status: "qr", qrDataUrl, qrUpdatedAt: new Date(), lastError: "", sendReady: false });
        }

        if (connection === "open") {
          connected = true;
          connecting = false;
          connectingSince = 0;
          await setConnection({
            status: "connected",
            phone: phoneFromUser(wa.user),
            displayName: wa.user?.name || "",
            qrDataUrl: "",
            lastConnectedAt: new Date(),
            lastError: "",
            sendReady: true,
            connectionGeneration: generation,
          });
          console.log(`[whatsapp] connected as ${phoneFromUser(wa.user) || "linked device"}`);
          pumpQueue().catch(() => {});
        }

        if (connection === "close") {
          connected = false;
          connecting = false;
          connectingSince = 0;
          if (sock === wa) sock = null;
          const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
          const loggedOut = code === DisconnectReason.loggedOut;
          const reason = lastDisconnect?.error?.message || `Disconnected (${code || "unknown"})`;
          await setConnection({
            status: loggedOut ? "logged_out" : "disconnected",
            qrDataUrl: "",
            lastDisconnectedAt: new Date(),
            lastError: loggedOut ? "WhatsApp session was logged out. Connect again to scan a new QR." : reason,
            sendReady: false,
          });
          if (loggedOut) {
            await authTools?.clearAuth?.();
          } else {
            scheduleReconnect(reconnectDelayFor(code));
          }
        }
        } catch (error) {
          console.error("[whatsapp] connection event error", error);
          await setConnection({ lastError: error?.message || String(error), sendReady: false }).catch(() => {});
        }
      });
    } catch (error) {
      if (generation !== socketGeneration) return;
      connected = false;
      connecting = false;
      connectingSince = 0;
      sock = null;
      await setConnection({
        status: "error",
        lastError: error?.message || String(error),
        lastDisconnectedAt: new Date(),
        sendReady: false,
      });
      scheduleReconnect(5000);
    }
  })().finally(() => {
    socketStartPromise = null;
  });

  return socketStartPromise;
}

async function processCommand() {
  if (!ownsLease) return;
  const raw = await connections.findOneAndUpdate(
    { key: "primary", command: { $in: ["connect", "reconnect", "logout"] } },
    { $set: { command: "" } },
    { returnDocument: "before" }
  );
  const doc = raw?.value || raw;
  const command = doc?.command;
  if (!command) return;

  if (command === "logout") {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
    const old = sock;
    try { await old?.logout?.(); } catch {}
    socketGeneration += 1;
    sock = null;
    connected = false;
    connecting = false;
    connectingSince = 0;
    try { await authTools?.clearAuth?.(); } catch {}
    await setConnection({
      status: "logged_out",
      phone: "",
      displayName: "",
      qrDataUrl: "",
      lastError: "Logged out by admin. Press Connect to pair WhatsApp again.",
      sendReady: false,
    });
    return;
  }

  if (command === "reconnect") {
    await stopCurrentSocket("Admin reconnect requested");
    setTimeout(() => startSocket().catch(console.error), 650);
    return;
  }

  if (command === "connect" && !connected) {
    await startSocket();
  }
}

function jidFor(phone) {
  return `${String(phone || "").replace(/\D/g, "")}@s.whatsapp.net`;
}

function retryDelay(attempt) {
  const schedule = [10_000, 30_000, 120_000, 600_000, 1_800_000];
  return schedule[Math.min(Math.max(attempt - 1, 0), schedule.length - 1)];
}

async function waitForSocketReady(timeoutMs = SOCKET_READY_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (currentSendReady()) return sock;
    if (!ownsLease || shuttingDown) break;
    await sleep(200);
  }
  throw new Error("WhatsApp socket is not send-ready");
}

async function waitForSendSlot() {
  let release;
  const mine = new Promise((resolve) => { release = resolve; });
  const previous = sendGate;
  sendGate = mine;
  await previous;
  try {
    const wait = Math.max(0, SEND_INTERVAL_MS - (Date.now() - lastSendStartedAt));
    if (wait) await sleep(wait);
    lastSendStartedAt = Date.now();
  } finally {
    release();
  }
}

async function claimJob() {
  if (!currentSendReady() || activeJobs.size >= QUEUE_CONCURRENCY) return null;
  const now = new Date();
  const raw = await jobs.findOneAndUpdate(
    {
      status: { $in: ["queued", "failed"] },
      $and: [
        { $or: [{ nextAttemptAt: { $lte: now } }, { nextAttemptAt: null }, { nextAttemptAt: { $exists: false } }] },
        { $expr: { $lt: [{ $ifNull: ["$attempts", 0] }, { $ifNull: ["$maxAttempts", 5] }] } },
      ],
    },
    {
      $set: {
        status: "sending",
        lockedAt: now,
        lockExpiresAt: new Date(Date.now() + JOB_LOCK_MS),
        workerId,
        workerInstanceId: instanceId,
      },
    },
    { sort: { priority: -1, createdAt: 1 }, returnDocument: "after" }
  );
  const job = raw?.value || raw;
  if (job?._id) await setConnection({ lastJobClaimedAt: new Date(), lastQueueError: "" }).catch(() => {});
  return job?._id ? job : null;
}

async function releaseWithoutAttempt(job, reason) {
  await jobs.updateOne(
    { _id: job._id, status: "sending", workerInstanceId: instanceId },
    {
      $set: {
        status: "queued",
        nextAttemptAt: new Date(Date.now() + 5000),
        lastError: reason,
        lockedAt: null,
        lockExpiresAt: null,
        workerId: "",
        workerInstanceId: "",
      },
    }
  );
}

async function processClaimedJob(job) {
  const key = String(job._id);
  try {
    let activeSocket;
    try {
      activeSocket = await waitForSocketReady();
    } catch (error) {
      await releaseWithoutAttempt(job, error.message || "Waiting for WhatsApp connection");
      return;
    }

    await waitForSendSlot();
    if (!currentSendReady() || activeSocket !== sock) {
      await releaseWithoutAttempt(job, "WhatsApp connection changed before send");
      return;
    }

    const attemptNo = Number(job.attempts || 0) + 1;
    const maxAttempts = Number(job.maxAttempts || 5);
    const attemptUpdate = await jobs.updateOne(
      { _id: job._id, status: "sending", workerInstanceId: instanceId },
      { $inc: { attempts: 1 }, $set: { lastAttemptAt: new Date(), lockExpiresAt: new Date(Date.now() + JOB_LOCK_MS) } }
    );
    if (!attemptUpdate.modifiedCount) return;

    try {
      const result = await activeSocket.sendMessage(jidFor(job.phone), { text: job.body });
      await jobs.updateOne(
        { _id: job._id, status: "sending", workerInstanceId: instanceId },
        {
          $set: {
            status: "sent",
            sentAt: new Date(),
            completedAt: new Date(),
            messageId: result?.key?.id || "",
            lastError: "",
            lockedAt: null,
            lockExpiresAt: null,
            workerId: "",
            workerInstanceId: "",
          },
        }
      );
      await setConnection({ lastMessageSentAt: new Date(), lastQueueError: "" }).catch(() => {});
    } catch (error) {
      const dead = attemptNo >= maxAttempts;
      const message = String(error?.message || error).slice(0, 1200);
      await jobs.updateOne(
        { _id: job._id, status: "sending", workerInstanceId: instanceId },
        {
          $set: {
            status: dead ? "dead" : "failed",
            lastError: message,
            nextAttemptAt: new Date(Date.now() + retryDelay(attemptNo)),
            lockedAt: null,
            lockExpiresAt: null,
            workerId: "",
            workerInstanceId: "",
            completedAt: dead ? new Date() : null,
          },
        }
      );
      await setConnection({ lastQueueError: message }).catch(() => {});
    }
  } finally {
    activeJobs.delete(key);
    pumpQueue().catch(() => {});
  }
}

async function pumpQueue() {
  if (pumpRunning || !currentSendReady() || shuttingDown || !ownsLease) return;
  pumpRunning = true;
  try {
    while (currentSendReady() && activeJobs.size < QUEUE_CONCURRENCY) {
      const job = await claimJob();
      if (!job) break;
      const key = String(job._id);
      if (activeJobs.has(key)) continue;
      activeJobs.set(key, true);
      processClaimedJob(job).catch(async (error) => {
        console.error("[whatsapp] job processing error", error);
        await setConnection({ lastQueueError: error?.message || String(error) }).catch(() => {});
      });
    }
  } finally {
    pumpRunning = false;
  }
}

async function recoverStaleJobs() {
  const now = new Date();
  const legacyStale = new Date(Date.now() - JOB_LOCK_MS);
  await jobs.updateMany(
    {
      status: "sending",
      workerInstanceId: { $ne: instanceId },
      $or: [
        { lockExpiresAt: { $lte: now } },
        { lockExpiresAt: null, lockedAt: { $lte: legacyStale } },
        { lockExpiresAt: { $exists: false }, lockedAt: { $lte: legacyStale } },
      ],
    },
    {
      $set: {
        status: "queued",
        nextAttemptAt: new Date(),
        lockedAt: null,
        lockExpiresAt: null,
        workerId: "",
        workerInstanceId: "",
        lastError: "Recovered after previous worker stopped before completing the message.",
      },
    }
  );
}

async function backfillQueueFields() {
  await jobs.updateMany({ priority: { $exists: false } }, { $set: { priority: 50 } });
  await jobs.updateMany({ maxAttempts: { $exists: false } }, { $set: { maxAttempts: 5 } });
  await jobs.updateMany({ attempts: { $exists: false } }, { $set: { attempts: 0 } });
  await jobs.updateMany({ nextAttemptAt: { $exists: false } }, { $set: { nextAttemptAt: new Date() } });
}

async function updateHealth() {
  if (!ownsLease) return;
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000);
  const [queuedCount, processingCount, failedCount, deadCount, sentTodayCount] = await Promise.all([
    jobs.countDocuments({ status: "queued" }),
    jobs.countDocuments({ status: "sending" }),
    jobs.countDocuments({ status: "failed" }),
    jobs.countDocuments({ status: "dead" }),
    jobs.countDocuments({ status: "sent", sentAt: { $gte: since } }),
  ]);
  await setConnection({
    workerHeartbeatAt: new Date(),
    queueConsumerHeartbeatAt: new Date(),
    queueConcurrency: QUEUE_CONCURRENCY,
    queueActiveJobs: activeJobs.size,
    queuedCount,
    processingCount,
    failedCount,
    deadCount,
    sentTodayCount,
    sendReady: currentSendReady(),
  });
}

async function connectionWatchdog() {
  if (!ownsLease || shuttingDown) return;
  if (connecting && connectingSince && Date.now() - connectingSince > 45_000) {
    console.error("[whatsapp] connection attempt exceeded 45s; rebuilding socket");
    await stopCurrentSocket("Connection watchdog timeout");
    scheduleReconnect(1000);
    return;
  }
  if (!connected && !connecting && !reconnectTimer) scheduleReconnect(500);
}

await ensureConnectionDoc();
await backfillQueueFields();
await recoverStaleJobs();
await acquireOrRenewLease();
if (!ownsLease) console.log(`[whatsapp] standby: another worker owns the active lease (${instanceId})`);

setInterval(() => acquireOrRenewLease().catch((e) => console.error("[whatsapp] lease error", e)), 8000);
setInterval(() => processCommand().catch((e) => console.error("[whatsapp] command error", e)), 1500);
setInterval(() => pumpQueue().catch((e) => console.error("[whatsapp] queue pump error", e)), 350);
setInterval(() => recoverStaleJobs().catch((e) => console.error("[whatsapp] recovery error", e)), 30_000);
setInterval(() => updateHealth().catch((e) => console.error("[whatsapp] health error", e)), 10_000);
setInterval(() => connectionWatchdog().catch((e) => console.error("[whatsapp] watchdog error", e)), 10_000);

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[whatsapp] ${signal} received, shutting down`);
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  await stopCurrentSocket("Service shutdown").catch(() => {});
  await connections.updateOne(
    { key: "primary", workerInstanceId: instanceId },
    { $set: { status: "disconnected", sendReady: false, lastDisconnectedAt: new Date(), lastError: "Worker stopped" } }
  ).catch(() => {});
  await releaseLease();
  await client.close().catch(() => {});
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

console.log(`[whatsapp] Fatimaz Baileys worker started (${workerId})`);
console.log(`[whatsapp] queue concurrency=${QUEUE_CONCURRENCY}, send interval=${SEND_INTERVAL_MS}ms, instance=${instanceId}`);
