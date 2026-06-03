const { v4: uuidv4 } = require("uuid");
const { createRedisClient } = require("../config/redis");
const { query } = require("../config/database");
const logger = require("../utils/logger");

class Queue {
  constructor(queueName) {
    this.queueName = queueName;

    // The Redis key where pending jobs live.
    // Using a namespace prefix avoids collisions if Redis is shared.
    this.redisKey = `tq:queue:${queueName}`;

    // Two separate Redis clients:
    // 1. publisher — used for all normal commands (LPUSH, HSET, etc.)
    // 2. subscriber/blocker — dedicated to BRPOP, which blocks the connection
    //    You CANNOT use the same connection for both — a blocked connection
    //    cannot process any other commands.
    this.publisher = createRedisClient(`${queueName}-publisher`);
    this.subscriber = createRedisClient(`${queueName}-subscriber`);
  }

  // ─── ENQUEUE ────────────────────────────────────────────────────────────────
  // Adding a job to the queue. Two things happen:
  //   1. Write the job's full data to PostgreSQL (permanent record)
  //   2. Push just the job ID to Redis (fast delivery mechanism)
  //
  // Why only push the ID to Redis and not the whole job?
  // Redis is in-memory — keeping big payloads in RAM is expensive.
  // We only need Redis to be a fast "pointer" — the worker gets the ID,
  // then loads the full job from Postgres.
async enqueue(jobType, payload, options = {}) {
  const {
    priority   = 0,
    maxRetries = parseInt(process.env.MAX_RETRIES) || 3,
    delayMs    = 0,   // NEW: intentional delay in milliseconds
  } = options;

  const jobId = uuidv4();
  const now   = new Date().toISOString();

  // If delayed, scheduledFor = now + delayMs, otherwise null
  const scheduledFor = delayMs > 0
    ? new Date(Date.now() + delayMs).toISOString()
    : null;

  const insertSQL = `
    INSERT INTO jobs (
      id, queue_name, job_type, payload, status,
      priority, max_retries, scheduled_for, created_at
    ) VALUES ($1, $2, $3, $4, 'pending', $5, $6, $7, $8)
    RETURNING *
  `;

  const dbResult = await query(insertSQL, [
    jobId,
    this.queueName,
    jobType,
    JSON.stringify(payload),
    priority,
    maxRetries,
    scheduledFor,
    now,
  ]);

  const job = dbResult.rows[0];

  if (delayMs > 0) {
    // Put directly into the delayed sorted set — scanner will
    // move it to the right queue when the time comes
    const delayedKey = `tq:delayed:${this.queueName}`;
    const runAt = Date.now() + delayMs;
    await this.publisher.zadd(delayedKey, runAt, jobId);

    logger.info("Job enqueued as delayed", {
      jobId, jobType, delayMs,
      runAt: new Date(runAt).toISOString(),
    });

  } else if (priority > 0) {
    // Put into priority sorted set — score IS the priority value
    // We invert it (multiply by -1) so higher priority number = lower score
    // = comes out of ZRANGEBYSCORE first
    const priorityKey = `tq:priority:${this.queueName}`;
    await this.publisher.zadd(priorityKey, -priority, jobId);

    logger.info("Job enqueued with priority", {
      jobId, jobType, priority,
    });

  } else {
    // Regular FIFO queue
    const queueLength = await this.publisher.lpush(this.redisKey, jobId);
    logger.info("Job enqueued", {
      jobId, jobType,
      queue: this.queueName,
      priority, queueLength,
    });
  }

  return job;
}

  // ─── DEQUEUE ─────────────────────────────────────────────────────────────────
  // Called by workers. Blocks until a job is available.
  // BRPOP returns: [keyName, value] or null if timeout expires.
  // We use a 5-second timeout so the worker can periodically check
  // if it should shut down (graceful shutdown — covered next week).
async dequeue(timeoutSeconds = 5) {
  const priorityKey = `tq:priority:${this.queueName}`;

  // ZRANGE key 0 0 returns array with 1 element (lowest score member)
  // This works on Redis 2.0+ unlike ZPOPMIN which requires 5.0+
  const topItems = await this.subscriber.zrange(priorityKey, 0, 0);

  if (topItems && topItems.length > 0) {
    const jobId = topItems[0];
    // Atomically claim it — ZREM returns 1 if we removed it,
    // 0 if another worker already took it (race condition safety)
    const removed = await this.subscriber.zrem(priorityKey, jobId);

    if (removed === 1) {
      return await this._loadAndActivateJob(jobId);
    }
    // If removed === 0, fall through to BRPOP below
  }

  // No priority jobs — block on regular FIFO list
  const result = await this.subscriber.brpop(this.redisKey, timeoutSeconds);
  if (!result) return null;

  const [_key, jobId] = result;
  return await this._loadAndActivateJob(jobId);
}

// Extracted helper — loads job from Postgres and marks it active
async _loadAndActivateJob(jobId) {
  const dbResult = await query(
    "SELECT * FROM jobs WHERE id = $1",
    [jobId]
  );

  if (dbResult.rows.length === 0) {
    logger.warn("Job ID in Redis but not in database", { jobId });
    return null;
  }

  const job = dbResult.rows[0];

  await query(
    `UPDATE jobs
     SET status = 'active', started_at = NOW(), attempts = attempts + 1
     WHERE id = $1`,
    [jobId]
  );

  logger.info("Job dequeued", {
    jobId:   job.id,
    jobType: job.job_type,
    attempt: job.attempts + 1,
  });

  return { ...job, attempts: job.attempts + 1 };
}

  // ─── COMPLETE / FAIL ─────────────────────────────────────────────────────────
  // Called by the worker after processing finishes (success or failure)

  async markComplete(jobId, result = null) {
    await query(
      `UPDATE jobs
       SET status = 'completed', completed_at = NOW(), result = $2
       WHERE id = $1`,
      [jobId, result ? JSON.stringify(result) : null]
    );
    logger.info("Job completed", { jobId });
  }

  async markFailed(jobId, errorMessage) {
    // Don't retry yet — the worker decides whether to retry.
    // This just records the failure.
    await query(
      `UPDATE jobs
       SET status = 'failed', error_message = $2, completed_at = NOW()
       WHERE id = $1`,
      [jobId, errorMessage]
    );
    logger.error("Job failed", { jobId, error: errorMessage });
  }

  // ─── STATS ───────────────────────────────────────────────────────────────────
  // How many jobs are currently waiting in the Redis list?
  async getQueueLength() {
    return await this.publisher.llen(this.redisKey);
  }

  // ─── RETRY LOGIC ─────────────────────────────────────────────────────────────

// Called by the worker when a job fails but still has retries left.
// Calculates the backoff delay and pushes job to the delayed sorted set.
async scheduleRetry(job, errorMessage) {
  const attempt = job.attempts; // already incremented by dequeue()
  const delay = this._calculateBackoff(attempt);
  const runAt = Date.now() + delay; // Unix ms timestamp

  // Update Postgres: status back to 'pending', record the error
  await query(
    `UPDATE jobs
     SET status = 'pending',
         error_message = $2,
         completed_at  = NULL
     WHERE id = $1`,
    [job.id, `Attempt ${attempt} failed: ${errorMessage}`]
  );

  // Push to Redis sorted set with future timestamp as score.
  // The delayed job scanner (below) will move it back to the
  // main queue when the time comes.
  const delayedKey = `tq:delayed:${this.queueName}`;
  await this.publisher.zadd(delayedKey, runAt, job.id);

  logger.warn("Job scheduled for retry", {
    jobId:   job.id,
    attempt,
    delayMs: delay,
    runAt:   new Date(runAt).toISOString(),
  });
}

// Exponential backoff formula: baseDelay * 2^(attempt-1) + jitter
// Jitter (random 0-1000ms) prevents the "thundering herd" problem —
// if 100 jobs all failed at once, without jitter they'd all retry
// at exactly the same moment and overwhelm the service again.
_calculateBackoff(attempt) {
  const baseDelay = 2000; // 2 seconds
  const expDelay  = baseDelay * Math.pow(2, attempt - 1);
  const jitter    = Math.floor(Math.random() * 1000);
  const maxDelay  = 60000; // cap at 60 seconds
  return Math.min(expDelay + jitter, maxDelay);
}

// ─── DEAD-LETTER QUEUE ───────────────────────────────────────────────────────

// Called when a job has exhausted all retries.
// Moves it to a separate DLQ list in Redis and marks it 'dead' in Postgres.
async sendToDeadLetterQueue(job, errorMessage) {
  const dlqKey = `tq:dlq:${this.queueName}`;

  // Store full job context in the DLQ entry so we can inspect it later
  const dlqEntry = JSON.stringify({
    jobId:        job.id,
    jobType:      job.job_type,
    payload:      job.payload,
    attempts:     job.attempts,
    lastError:    errorMessage,
    diedAt:       new Date().toISOString(),
  });

  await this.publisher.lpush(dlqKey, dlqEntry);

  await query(
    `UPDATE jobs
     SET status = 'dead', error_message = $2, completed_at = NOW()
     WHERE id = $1`,
    [job.id, `Dead after ${job.attempts} attempts. Last error: ${errorMessage}`]
  );

  logger.error("Job sent to dead-letter queue", {
    jobId:    job.id,
    jobType:  job.job_type,
    attempts: job.attempts,
  });
}

// ─── DELAYED JOB SCANNER ─────────────────────────────────────────────────────

// Checks the delayed sorted set for jobs whose time has come
// and moves them back to the main queue.
// This runs on a timer inside WorkerManager.
async scanDelayedJobs() {
  const delayedKey  = `tq:delayed:${this.queueName}`;
  const priorityKey = `tq:priority:${this.queueName}`;
  const now = Date.now();

  const dueJobIds = await this.publisher.zrangebyscore(delayedKey, 0, now);
  if (dueJobIds.length === 0) return;

  for (const jobId of dueJobIds) {
    const removed = await this.publisher.zrem(delayedKey, jobId);
    if (removed !== 1) continue; // another scanner got it first

    // Look up the job's priority to route it correctly
    const result = await query(
      "SELECT priority FROM jobs WHERE id = $1",
      [jobId]
    );

    if (result.rows.length === 0) continue;

    const { priority } = result.rows[0];

    if (priority > 0) {
      await this.publisher.zadd(priorityKey, -priority, jobId);
      logger.info("Delayed priority job moved to priority queue", {
        jobId, priority,
      });
    } else {
      await this.publisher.lpush(this.redisKey, jobId);
      logger.info("Delayed job moved back to main queue", { jobId });
    }
  }
}

// ─── DLQ INSPECTOR ───────────────────────────────────────────────────────────

// Returns all jobs in the dead-letter queue — used by the API/dashboard
async getDeadLetterJobs() {
  const dlqKey = `tq:dlq:${this.queueName}`;
  const entries = await this.publisher.lrange(dlqKey, 0, -1);
  return entries.map((e) => JSON.parse(e));
}

// Replay a dead job — re-enqueue it fresh with reset attempts
async replayDeadJob(jobId) {
  const result = await query(
    "SELECT * FROM jobs WHERE id = $1",
    [jobId]
  );

  if (result.rows.length === 0) {
    throw new Error(`Job ${jobId} not found`);
  }

  const job = result.rows[0];

  // Reset the job in Postgres
  await query(
    `UPDATE jobs
     SET status = 'pending', attempts = 0,
         error_message = NULL, completed_at = NULL
     WHERE id = $1`,
    [jobId]
  );

  // Push back to the main Redis queue
  await this.publisher.lpush(this.redisKey, jobId);

  // Remove from DLQ
  const dlqKey = `tq:dlq:${this.queueName}`;
  const entries = await this.publisher.lrange(dlqKey, 0, -1);
  for (const entry of entries) {
    const parsed = JSON.parse(entry);
    if (parsed.jobId === jobId) {
      await this.publisher.lrem(dlqKey, 1, entry);
      break;
    }
  }

  logger.info("Dead job replayed", { jobId });
  return job;
}

  // Close Redis connections cleanly on shutdown
  async close() {
    await this.publisher.quit();
    await this.subscriber.quit();
    logger.info("Queue connections closed", { queue: this.queueName });
  }
}

module.exports = Queue;