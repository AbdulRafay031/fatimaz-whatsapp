# Fatimaz WhatsApp / Baileys worker

This directory is a separate always-on Node.js service. Do **not** run it as a Vercel Function. The Fatimaz Next.js app can stay on Vercel; both services communicate through the same MongoDB database.

## Production setup

1. Deploy the main Fatimaz app to Vercel with its existing `MONGODB_URI`.
2. Deploy this `whatsapp-service` directory to an always-on Node/Docker host (Railway, Render worker, VPS, Fly.io, etc.). Use Node 20+.
3. Set the same `MONGODB_URI` on the worker. If your URI does not include the DB name, set `MONGODB_DB` too.
4. Keep the hosting platform at **one replica** for this WhatsApp account. A MongoDB lease is also enforced so an overlapping redeploy/second new worker stays standby instead of opening a competing Baileys socket.
5. Start with `npm install && npm start` (or build the Dockerfile). `.env` is loaded automatically through `dotenv`.
6. Open Fatimaz **Admin → WhatsApp**. Check Worker Online, Queue Healthy and Send Ready.
7. Press **Connect** if needed. A QR appears when no valid saved session exists.
8. WhatsApp phone: **WhatsApp → Linked Devices → Link a Device**, then scan the QR.

After pairing, encrypted Baileys credentials and Signal keys are persisted in MongoDB. Normal Vercel/worker redeploys reconnect without another QR unless WhatsApp revokes the linked session.

## Reliability behavior

- MongoDB durable queue: commerce actions never wait for WhatsApp.
- Atomic job claiming: one message cannot be claimed by two compliant workers.
- Active-worker lease: only one worker instance owns the WhatsApp socket; overlapping deployments remain standby.
- Default queue concurrency: **2** messages in flight.
- Default send-start spacing: **400ms** to avoid sudden bursts.
- Priority queue: payment/support/order/gift-card events can move ahead of low-priority alerts.
- Retry schedule after a real send failure: **10 sec, 30 sec, 2 min, 10 min, 30 min** (max 5 attempts).
- A disconnected socket does **not** consume an attempt; the job returns to the queue.
- Stale `sending` jobs from a dead worker are recovered automatically after the lock expires.
- Connection watchdog rebuilds a socket that gets stuck while connecting.
- Socket generation guard ignores late events from a replaced/old socket.
- Event keys make business notifications idempotent, preventing the normal application flow from queuing the same event twice.

### Throughput example

If 10 customers place orders together, all 10 orders finish normally and create 10 durable queue jobs immediately. The worker sends up to 2 concurrently, starting sends at least 400ms apart. A temporary connection failure leaves remaining jobs queued/retriable instead of failing checkout.

## Environment variables

See `.env.example`. Defaults are production-safe for a small/medium transactional store:

- `WHATSAPP_QUEUE_CONCURRENCY=2`
- `WHATSAPP_SEND_INTERVAL_MS=400`
- `WHATSAPP_WORKER_LEASE_MS=30000`
- `WHATSAPP_JOB_LOCK_MS=120000`
- `WHATSAPP_SOCKET_READY_TIMEOUT_MS=15000`

Do not raise concurrency aggressively. Baileys is an unofficial WhatsApp Web client and WhatsApp does not publish a supported Baileys throughput limit.

## Collections used

- `whatsappauthstates` — Baileys credentials + Signal keys. Treat as secrets.
- `whatsappconnections` — QR, connection, worker/queue health and admin commands.
- `whatsappworkerlocks` — single-active-worker lease.
- `whatsappjobs` — durable transactional queue and delivery log.
- `whatsappsettings` — per-event enable flags and editable templates.

## Important

Baileys is an unofficial WhatsApp Web client. This architecture makes the application resilient to worker/socket outages, but it cannot provide the delivery SLA of Meta's official WhatsApp Business Platform. Use a dedicated business number and keep messages transactional/customer-relevant.

## If you see `Invalid namespace`

Check `.env` first. `MONGODB_DB` must contain only the database name, for example:

```env
MONGODB_DB=Fatimaz
```

Do not append commands such as `npm run dev` to that line. The worker now validates this value before opening any WhatsApp collections and prints the database it actually connected to.
