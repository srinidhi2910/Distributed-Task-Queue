const { processJob } = require("../jobs/jobHandlers");
const logger = require("../utils/logger");

class Worker {
  constructor(workerId, queue) {
    this.workerId    = workerId;
    this.queue       = queue;
    this.isRunning   = false;
    this.currentJob  = null;
    this.jobsProcessed = 0;
    this.jobsFailed    = 0;
    this.jobsRetried   = 0;
    this.jobsDead      = 0;
  }

  start() {
    this.isRunning = true;
    logger.info("Worker started", { workerId: this.workerId });
    this._loop();
  }

  stop() {
    this.isRunning = false;
    logger.info("Worker stopping after current job...", {
      workerId: this.workerId,
    });
  }

  async _loop() {
    while (this.isRunning) {
      try {
        await this._processNext();
      } catch (err) {
        logger.error("Worker loop error", {
          workerId: this.workerId,
          error: err.message,
        });
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
    logger.info("Worker stopped", {
      workerId:      this.workerId,
      jobsProcessed: this.jobsProcessed,
      jobsFailed:    this.jobsFailed,
      jobsRetried:   this.jobsRetried,
      jobsDead:      this.jobsDead,
    });
  }

  async _processNext() {
    const job = await this.queue.dequeue(5);
    if (!job) return;
    // Guard: job was cancelled via API after being queued in Redis
    if (job.status === "cancelled") {
      logger.info("Skipping cancelled job", { workerId: this.workerId, jobId: job.id });
      return;
    }

    this.currentJob = job;
    logger.info("Worker picked up job", {
      workerId: this.workerId,
      jobId:    job.id,
      jobType:  job.job_type,
      attempt:  job.attempts,
    });

    try {
      const result = await processJob(job);
      await this.queue.markComplete(job.id, result);
      this.jobsProcessed++;
      logger.info("Worker completed job", {
        workerId: this.workerId,
        jobId:    job.id,
        jobType:  job.job_type,
      });

    } catch (err) {
      this.jobsFailed++;
      logger.error("Job processing failed", {
        workerId: this.workerId,
        jobId:    job.id,
        attempt:  job.attempts,
        maxRetries: job.max_retries,
        error:    err.message,
      });

      const hasRetriesLeft = job.attempts < job.max_retries;

      if (hasRetriesLeft) {
        // Schedule a retry with exponential backoff
        await this.queue.scheduleRetry(job, err.message);
        this.jobsRetried++;
      } else {
        // All retries exhausted — send to dead-letter queue
        await this.queue.sendToDeadLetterQueue(job, err.message);
        this.jobsDead++;
      }

    } finally {
      this.currentJob = null;
    }
  }

  getStatus() {
    return {
      workerId:      this.workerId,
      isRunning:     this.isRunning,
      currentJob:    this.currentJob ? this.currentJob.id : null,
      jobsProcessed: this.jobsProcessed,
      jobsFailed:    this.jobsFailed,
      jobsRetried:   this.jobsRetried,
      jobsDead:      this.jobsDead,
    };
  }
}

module.exports = Worker;