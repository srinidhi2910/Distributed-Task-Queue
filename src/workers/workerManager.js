require("dotenv").config({ silent: true });

const Queue = require("../core/queue");
const Worker = require("./worker");
const logger = require("../utils/logger");
const { initializeSchema } = require("../db/schema");

const CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY) || 5;
const QUEUE_NAME = process.env.QUEUE_NAME || "task_queue";

class WorkerManager {
  constructor() {
    this.workers = [];
    this.queues = [];   // each worker gets its own queue (its own BRPOP connection)
    this.isShuttingDown = false;
  }

  async start() {
    logger.info("WorkerManager starting...", { concurrency: CONCURRENCY });

    // Ensure DB schema exists
    await initializeSchema();

    // Spawn N workers
    for (let i = 1; i <= CONCURRENCY; i++) {
      const workerId = `worker-${i}`;

      // Each worker needs its own Queue instance because each Queue
      // creates its own BRPOP connection. You cannot share one
      // blocking connection across multiple workers.
      const queue = new Queue(QUEUE_NAME);
      this.queues.push(queue);

      const worker = new Worker(workerId, queue);
      this.workers.push(worker);

      worker.start();
    }

    logger.info(`Worker pool ready — ${CONCURRENCY} workers listening`, {
      queue: QUEUE_NAME,
    });
    this._startDelayedJobScanner(); 

    // Print a status summary every 30 seconds
    this.statusInterval = setInterval(() => this._logStatus(), 30000);

    // Set up graceful shutdown on Ctrl+C or process termination
    this._setupShutdown();
  }

  _logStatus() {
    const statuses = this.workers.map((w) => w.getStatus());
    const active = statuses.filter((s) => s.currentJob !== null).length;
    const totalProcessed = statuses.reduce((sum, s) => sum + s.jobsProcessed, 0);
    const totalFailed = statuses.reduce((sum, s) => sum + s.jobsFailed, 0);

    logger.info("Worker pool status", {
      totalWorkers: this.workers.length,
      activeWorkers: active,
      idleWorkers: this.workers.length - active,
      totalProcessed,
      totalFailed,
    });
  }
  // Starts a timer that checks the delayed sorted set every 2 seconds.
// When a job's scheduled time arrives, it gets moved back to the main queue.
_startDelayedJobScanner() {
  // We only need one queue instance to scan — use the first worker's queue
  const scannerQueue = this.queues[0];

  this.scannerInterval = setInterval(async () => {
    try {
      await scannerQueue.scanDelayedJobs();
    } catch (err) {
      logger.error("Delayed job scanner error", { error: err.message });
    }
  }, 2000); // check every 2 seconds

  logger.info("Delayed job scanner started (interval: 2s)");
}

  _setupShutdown() {
    const shutdown = async (signal) => {
      if (this.isShuttingDown) return; // prevent double-shutdown
      this.isShuttingDown = true;

      logger.info(`Received ${signal} — starting graceful shutdown...`);
      clearInterval(this.statusInterval);
      clearInterval(this.scannerInterval);

      // Tell all workers to stop after their current job
      this.workers.forEach((w) => w.stop());

      // Wait up to 30 seconds for workers to finish current jobs
      const timeout = 30000;
      const start = Date.now();

      while (Date.now() - start < timeout) {
        const anyActive = this.workers.some((w) => w.currentJob !== null);
        if (!anyActive) break;
        logger.info("Waiting for active jobs to finish...");
        await new Promise((r) => setTimeout(r, 1000));
      }

      // Close all Redis connections
      for (const queue of this.queues) {
        await queue.close();
      }

      logger.info("Graceful shutdown complete. Goodbye.");
      process.exit(0);
    };

    process.on("SIGINT", () => shutdown("SIGINT"));   // Ctrl+C
    process.on("SIGTERM", () => shutdown("SIGTERM")); // kill command / Docker stop
  }
}

// Run the manager
const manager = new WorkerManager();
manager.start().catch((err) => {
  logger.error("WorkerManager failed to start", { error: err.message });
  process.exit(1);
});