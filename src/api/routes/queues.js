const express = require("express");
const router  = express.Router();
const { query }        = require("../../config/database");
const { createRedisClient } = require("../../config/redis");
const logger           = require("../../utils/logger");

const redisClient = createRedisClient("api-stats");
const QUEUE_NAME  = process.env.QUEUE_NAME || "task_queue";

// ─── GET /api/queues/stats ────────────────────────────────────────────────────
// Returns a full snapshot of queue health — this feeds the dashboard
router.get("/stats", async (req, res, next) => {
  try {
    // Run all queries in parallel for speed
    const [
      statusCounts,
      recentThroughput,
      redisQueueLength,
      redisPriorityLength,
      redisDelayedLength,
      redisDlqLength,
    ] = await Promise.all([
      // Count jobs by status
      query(`
        SELECT status, COUNT(*) as count
        FROM jobs
        GROUP BY status
      `),

      // Jobs completed in the last hour (throughput metric)
      query(`
        SELECT COUNT(*) as count
        FROM jobs
        WHERE status = 'completed'
          AND completed_at > NOW() - INTERVAL '1 hour'
      `),

      // Live Redis queue lengths
      redisClient.llen(`tq:queue:${QUEUE_NAME}`),
      redisClient.zcard(`tq:priority:${QUEUE_NAME}`),
      redisClient.zcard(`tq:delayed:${QUEUE_NAME}`),
      redisClient.llen(`tq:dlq:${QUEUE_NAME}`),
    ]);

    // Turn the status rows into a clean object: { pending: 5, active: 2, ... }
    const byStatus = {};
    for (const row of statusCounts.rows) {
      byStatus[row.status] = parseInt(row.count);
    }

    res.json({
      queue: QUEUE_NAME,
      timestamp: new Date().toISOString(),
      redis: {
        mainQueueLength:     redisQueueLength,
        priorityQueueLength: redisPriorityLength,
        delayedQueueLength:  redisDelayedLength,
        dlqLength:           redisDlqLength,
      },
      postgres: {
        byStatus: {
          pending:   byStatus.pending   || 0,
          active:    byStatus.active    || 0,
          completed: byStatus.completed || 0,
          failed:    byStatus.failed    || 0,
          dead:      byStatus.dead      || 0,
          cancelled: byStatus.cancelled || 0,
        },
        completedLastHour: parseInt(recentThroughput.rows[0].count),
      },
    });
  } catch (err) {
    next(err);
  }
});
// ─── GET /api/queues/stream ───────────────────────────────────────────────────
// Server-Sent Events endpoint — pushes stats to dashboard every 2 seconds.
// The browser connects once and receives a continuous stream.
router.get("/stream", (req, res) => {
  // SSE requires these specific headers
  res.setHeader("Content-Type",  "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection",    "keep-alive");

  // Allow the React dev server to connect (CORS)
  res.setHeader("Access-Control-Allow-Origin", "*");

  // Send an initial ping so the browser knows it's connected
  res.write("data: {\"type\":\"connected\"}\n\n");

  // Push stats every 2 seconds
  const interval = setInterval(async () => {
    try {
      const [statusCounts, recentThroughput, queueLengths] = await Promise.all([
        query(`SELECT status, COUNT(*) as count FROM jobs GROUP BY status`),
        query(`SELECT COUNT(*) as count FROM jobs WHERE status='completed'
               AND completed_at > NOW() - INTERVAL '1 hour'`),
        Promise.all([
          redisClient.llen(`tq:queue:${QUEUE_NAME}`),
          redisClient.zcard(`tq:priority:${QUEUE_NAME}`),
          redisClient.zcard(`tq:delayed:${QUEUE_NAME}`),
          redisClient.llen(`tq:dlq:${QUEUE_NAME}`),
        ]),
      ]);

      const byStatus = {};
      for (const row of statusCounts.rows) {
        byStatus[row.status] = parseInt(row.count);
      }

      const payload = {
        type:      "stats",
        timestamp: new Date().toISOString(),
        redis: {
          mainQueue:     queueLengths[0],
          priorityQueue: queueLengths[1],
          delayedQueue:  queueLengths[2],
          dlq:           queueLengths[3],
        },
        postgres: {
          pending:           byStatus.pending   || 0,
          active:            byStatus.active    || 0,
          completed:         byStatus.completed || 0,
          failed:            byStatus.failed    || 0,
          dead:              byStatus.dead      || 0,
          completedLastHour: parseInt(recentThroughput.rows[0].count),
        },
      };

      // SSE format: must be "data: <json>\n\n"
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    } catch (err) {
      logger.error("SSE stream error", { error: err.message });
    }
  }, 2000);

  // Clean up when browser disconnects
  req.on("close", () => {
    clearInterval(interval);
    logger.info("SSE client disconnected");
  });
});
module.exports = router;