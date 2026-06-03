const express = require("express");
const router  = express.Router();
const { query } = require("../../config/database");
const Queue     = require("../../core/queue");
const logger    = require("../../utils/logger");

// We create one shared Queue instance for the API.
// This is the "producer" side — it only enqueues, never dequeues.
const queue = new Queue(process.env.QUEUE_NAME || "task_queue");

// ─── POST /api/jobs ───────────────────────────────────────────────────────────
// Enqueue a new job
router.post("/", async (req, res, next) => {
  try {
    const { jobType, payload, priority, maxRetries, delayMs } = req.body;

    // Basic validation
    if (!jobType) {
      return res.status(400).json({ error: "jobType is required" });
    }
    if (!payload || typeof payload !== "object") {
      return res.status(400).json({ error: "payload must be an object" });
    }

    const job = await queue.enqueue(jobType, payload, {
      priority:   priority   || 0,
      maxRetries: maxRetries || 3,
      delayMs:    delayMs    || 0,
    });

    res.status(201).json({
      message: "Job enqueued successfully",
      job: {
        id:          job.id,
        jobType:     job.job_type,
        status:      job.status,
        priority:    job.priority,
        createdAt:   job.created_at,
        scheduledFor: job.scheduled_for,
      },
    });
  } catch (err) {
    next(err);
  }
});

// ─── GET /api/jobs ────────────────────────────────────────────────────────────
// List jobs with optional filters
// Query params: ?status=pending&jobType=send_email&limit=20&offset=0
router.get("/", async (req, res, next) => {
  try {
    const {
      status,
      jobType,
      limit  = 20,
      offset = 0,
    } = req.query;

    // Build query dynamically based on which filters were provided
    const conditions = [];
    const params     = [];
    let   paramIndex = 1;

    if (status) {
      conditions.push(`status = $${paramIndex++}`);
      params.push(status);
    }
    if (jobType) {
      conditions.push(`job_type = $${paramIndex++}`);
      params.push(jobType);
    }

    const whereClause = conditions.length
      ? `WHERE ${conditions.join(" AND ")}`
      : "";

    // Count total for pagination
    const countResult = await query(
      `SELECT COUNT(*) FROM jobs ${whereClause}`,
      params
    );
    const total = parseInt(countResult.rows[0].count);

    // Fetch jobs, newest first
    params.push(parseInt(limit), parseInt(offset));
    const jobsResult = await query(
      `SELECT id, queue_name, job_type, status, priority,
              attempts, max_retries, error_message,
              created_at, started_at, completed_at, scheduled_for
       FROM jobs
       ${whereClause}
       ORDER BY created_at DESC
       LIMIT $${paramIndex++} OFFSET $${paramIndex++}`,
      params
    );

    res.json({
      total,
      limit:  parseInt(limit),
      offset: parseInt(offset),
      jobs:   jobsResult.rows,
    });
  } catch (err) {
    next(err);
  }
});

// ─── GET /api/jobs/dlq ────────────────────────────────────────────────────────
// View dead-letter queue contents
// IMPORTANT: this route must come BEFORE /api/jobs/:id
// otherwise Express matches "dlq" as the :id parameter
router.get("/dlq", async (req, res, next) => {
  try {
    const deadJobs = await queue.getDeadLetterJobs();
    res.json({ total: deadJobs.length, jobs: deadJobs });
  } catch (err) {
    next(err);
  }
});

// ─── POST /api/jobs/dlq/:id/replay ───────────────────────────────────────────
// Replay a dead job — re-enqueue it fresh
router.post("/dlq/:id/replay", async (req, res, next) => {
  try {
    const job = await queue.replayDeadJob(req.params.id);
    res.json({ message: "Job replayed successfully", jobId: job.id });
  } catch (err) {
    next(err);
  }
});

// ─── GET /api/jobs/:id ────────────────────────────────────────────────────────
// Get full details of a single job including result and error
router.get("/:id", async (req, res, next) => {
  try {
    const result = await query(
      "SELECT * FROM jobs WHERE id = $1",
      [req.params.id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Job not found" });
    }

    res.json({ job: result.rows[0] });
  } catch (err) {
    next(err);
  }
});

// ─── DELETE /api/jobs/:id ─────────────────────────────────────────────────────
// Cancel a pending job — only works if it hasn't been picked up yet
router.delete("/:id", async (req, res, next) => {
  try {
    const result = await query(
      `UPDATE jobs
       SET status = 'cancelled', completed_at = NOW()
       WHERE id = $1 AND status = 'pending'
       RETURNING id`,
      [req.params.id]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({
        error: "Job cannot be cancelled — it is not in pending status",
      });
    }

    // Note: the job ID might still be in Redis. When the worker picks it up,
    // it will load from Postgres, see 'cancelled', and skip it.
    // We handle this gracefully in the worker in a moment.
    res.json({ message: "Job cancelled", jobId: req.params.id });
  } catch (err) {
    next(err);
  }
});

module.exports = router;