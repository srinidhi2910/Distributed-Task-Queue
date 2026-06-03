# Distributed Task Queue Engine

A production-grade distributed task queue built from scratch using raw Redis primitives — no BullMQ, no Agenda, no abstraction libraries. Built to understand distributed systems internals.

![Node.js](https://img.shields.io/badge/Node.js-20-green)
![Redis](https://img.shields.io/badge/Redis-3%2B-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Producer (API)                           │
│              POST /api/jobs → enqueue()                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                ┌───────────▼───────────┐
                │        Redis          │
                │ tq:queue    (FIFO)    │
                │ tq:priority (ZADD)    │
                │ tq:delayed  (ZADD)    │
                │ tq:dlq      (LPUSH)   │
                └───────────┬───────────┘
                            │
                     BRPOP / ZRANGE
                            │
                ┌───────────▼───────────┐
                │      Worker Pool      │
                │ worker-1  worker-2    │
                │ worker-3  worker-4    │
                │ worker-5              │
                │                       │
                │ • Retry + backoff     │
                │ • Dead-letter queue   │
                │ • Graceful shutdown   │
                └───────────┬───────────┘
                            │
                ┌───────────▼───────────┐
                │      PostgreSQL       │
                │ jobs table            │
                │ Full audit trail      │
                │ Status lifecycle      │
                └───────────┬───────────┘
                            │
                ┌───────────▼───────────┐
                │    React Dashboard    │
                │ Live stats via SSE    │
                │ Job table + filters   │
                │ Enqueue form          │
                └───────────────────────┘
```

---

## Features

**Core Queue Engine**
- FIFO queue using Redis `LPUSH` / `BRPOP` — no polling, zero CPU waste while idle
- 5-worker concurrent pool, each with its own dedicated blocking connection
- Dual-store architecture: Redis for delivery speed, PostgreSQL for durability

**Reliability**
- Exponential backoff with jitter: `delay = base * 2^attempt + random(0-1000ms)`
- Dead-letter queue for jobs that exhaust all retries — inspect and replay via API
- Graceful shutdown — workers finish current job before exiting, no job is ever dropped

**Priority & Scheduling**
- Priority queue using Redis Sorted Sets — urgent jobs preempt regular queue instantly
- Delayed job scheduler with 2-second resolution using `ZADD` scored by Unix timestamp
- Background scanner moves due jobs back to the correct queue (priority-aware)

**REST API**
- `POST   /api/jobs` — enqueue a job with type, payload, priority, delay
- `GET    /api/jobs` — list jobs with filters (status, type, pagination)
- `GET    /api/jobs/:id` — full job details including result and error
- `DELETE /api/jobs/:id` — cancel a pending job
- `GET    /api/queues/stats` — live queue depths and throughput metrics
- `GET    /api/jobs/dlq` — inspect dead-letter queue
- `POST   /api/jobs/dlq/:id/replay` — replay a dead job

**React Dashboard**
- Server-Sent Events (SSE) for live stats — no polling, server pushes every 2 seconds
- Real-time job table with status badges, priority, attempt tracking
- Filter tabs: all / pending / active / completed / failed / dead
- Enqueue form — create jobs directly from the browser

---

## Load Test Results

Tested on Windows 11, Node.js v22, Redis 3, PostgreSQL 15 — 5 concurrent workers:
-Total jobs         : 500
-Completed          : 484   (96.8% success rate)
-Failed/Dead        : 16    (expected — 10% simulated SMTP failure rate)
-Total time         : 7.39s
-Worker throughput  : 68 jobs/sec
-Enqueue throughput : 1048 jobs/sec
