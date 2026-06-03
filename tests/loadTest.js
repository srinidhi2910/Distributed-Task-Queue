require("dotenv").config();
const logger = require("../src/utils/logger");
logger.level = "warn";

const Queue  = require("../src/core/queue");
const { query, pool } = require("../src/config/database");

const TOTAL_JOBS      = 500;   // total jobs to enqueue
const BATCH_SIZE      = 50;    // enqueue in batches to avoid overwhelming Postgres
const POLL_INTERVAL   = 500;   // check completion every 500ms
const TIMEOUT_MS      = 180000; // give up after 60 seconds

async function runLoadTest() {
  console.log("\n" + "=".repeat(60));
  console.log("  DISTRIBUTED TASK QUEUE — LOAD TEST");
  console.log("=".repeat(60));
  console.log(`  Jobs to enqueue : ${TOTAL_JOBS}`);
  console.log(`  Batch size      : ${BATCH_SIZE}`);
  console.log("=".repeat(60) + "\n");

  const queue = new Queue(process.env.QUEUE_NAME || "task_queue");

  // ── Clean slate ────────────────────────────────────────────────────────────
  // Delete only load-test jobs so we don't wipe your existing data
  await query("DELETE FROM jobs WHERE queue_name = 'load_test_queue'");

  const loadQueue = new Queue("load_test_queue");

  // ── Enqueue Phase ──────────────────────────────────────────────────────────
  console.log("Phase 1: Enqueueing jobs...");
  const enqueueStart = Date.now();
  let enqueued = 0;

  for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
    const batchPromises = [];
    const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_JOBS);

    for (let j = i; j < batchEnd; j++) {
      // Mix of job types and priorities to simulate real load
      const jobType = ["send_email", "resize_image", "generate_report"][j % 3];
      const priority = j % 5; // 0-4

      batchPromises.push(
        loadQueue.enqueue(
          jobType,
          { to: `user${j}@test.com`, subject: `Load test ${j}`, index: j },
          { priority, maxRetries: 2 }
        )
      );
    }

    await Promise.all(batchPromises);
    enqueued += batchEnd - i;
    process.stdout.write(`\r  Enqueued: ${enqueued}/${TOTAL_JOBS}`);
  }

  const enqueueTime = Date.now() - enqueueStart;
  console.log(`\n  Done in ${enqueueTime}ms`);
  console.log(`  Enqueue throughput: ${Math.round(TOTAL_JOBS / (enqueueTime / 1000))} jobs/sec\n`);

  // ── Wait for Workers Phase ─────────────────────────────────────────────────
  // Workers are already running — they'll pick up load_test_queue jobs too
  // because WorkerManager needs to also watch this queue.
  // For the load test, we'll use the main task_queue and just measure timing.

  // Re-run enqueue on the MAIN queue for accurate worker measurement
  console.log("Phase 2: Enqueueing to main queue for worker throughput test...");
  await query("UPDATE jobs SET status='cancelled' WHERE queue_name='load_test_queue'");

  const workerTestStart = Date.now();

  // Enqueue all jobs to the real queue
  const allEnqueuePromises = [];
  for (let i = 0; i < TOTAL_JOBS; i++) {
    const jobType = ["send_email", "resize_image", "generate_report"][i % 3];
    allEnqueuePromises.push(
      queue.enqueue(
        jobType,
        { to: `loadtest${i}@test.com`, subject: `Perf test ${i}`, loadTest: true },
        { priority: i % 3, maxRetries: 1 }
      )
    );
    // Small stagger to not overwhelm Postgres insert throughput
    if (i % 50 === 0) await new Promise(r => setTimeout(r, 10));
  }
  await Promise.all(allEnqueuePromises);
  console.log(`  All ${TOTAL_JOBS} jobs in queue. Waiting for workers...\n`);

  // ── Poll Until Complete ────────────────────────────────────────────────────
  console.log("Phase 3: Measuring worker throughput...");
  const deadline = Date.now() + TIMEOUT_MS;
  let lastCompleted = 0;

  while (Date.now() < deadline) {
    const result = await query(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'completed') as completed,
        COUNT(*) FILTER (WHERE status = 'failed')    as failed,
        COUNT(*) FILTER (WHERE status = 'active')    as active,
        COUNT(*) FILTER (WHERE status = 'pending')   as pending,
        COUNT(*) FILTER (WHERE status = 'dead')      as dead
      FROM jobs
      WHERE queue_name = 'task_queue'
  AND created_at > NOW() - INTERVAL '2 minutes'
    `);

    const r = result.rows[0];
    const completed  = parseInt(r.completed);
    const failed     = parseInt(r.failed);
    const active     = parseInt(r.active);
    const pending    = parseInt(r.pending);
    const dead       = parseInt(r.dead);
    const total      = completed + failed + active + pending + dead;
    const elapsed    = ((Date.now() - workerTestStart) / 1000).toFixed(1);
    const throughput = completed > 0
      ? Math.round(completed / ((Date.now() - workerTestStart) / 1000))
      : 0;

    process.stdout.write(
      `\r  [${elapsed}s] completed: ${completed} | active: ${active} | pending: ${pending} | throughput: ${throughput}/s    `
    );

    if (completed + dead >= TOTAL_JOBS) {
      const totalTime  = Date.now() - workerTestStart;
      const finalThroughput = Math.round(TOTAL_JOBS / (totalTime / 1000));

      console.log("\n\n" + "=".repeat(60));
      console.log("  LOAD TEST RESULTS");
      console.log("=".repeat(60));
      console.log(`  Total jobs         : ${TOTAL_JOBS}`);
      console.log(`  Completed          : ${completed}`);
      console.log(`  Failed/Dead        : ${failed + dead}`);
      console.log(`  Total time         : ${(totalTime/1000).toFixed(2)}s`);
      console.log(`  Worker throughput  : ${finalThroughput} jobs/sec`);
      console.log(`  Enqueue throughput : ${Math.round(TOTAL_JOBS / (enqueueTime / 1000))} jobs/sec`);
      console.log("=".repeat(60) + "\n");
      break;
    }

    await new Promise(r => setTimeout(r, POLL_INTERVAL));
  }

  await queue.close();
  await loadQueue.close();
  await pool.end();
  process.exit(0);
}

runLoadTest().catch(err => {
  console.error("Load test failed:", err.message);
  process.exit(1);
});