require("dotenv").config();
const { query, pool } = require("../src/config/database");
const { createRedisClient } = require("../src/config/redis");

async function cleanup() {
  console.log("Cleaning up...");

  // Cancel all pending jobs in Postgres
  const result = await query(
    "UPDATE jobs SET status = 'cancelled' WHERE status = 'pending'"
  );
  console.log(`Cancelled ${result.rowCount} pending jobs in Postgres`);

  // Clear Redis queues
  const redis = createRedisClient("cleanup");
  await redis.del(
    "tq:queue:task_queue",
    "tq:priority:task_queue",
    "tq:delayed:task_queue"
  );
  console.log("Redis queues cleared");

  await redis.quit();
  await pool.end();
  console.log("Done. Ready for a clean load test.");
}

cleanup().catch(err => {
  console.error(err.message);
  process.exit(1);
});