const { query } = require("../config/database");
const logger = require("../utils/logger");

// This function creates our tables if they don't already exist.
// We call it once on app startup.
async function initializeSchema() {
  // The jobs table stores every job ever created.
  // Each column maps to a piece of the job lifecycle.
  const createJobsTable = `
    CREATE TABLE IF NOT EXISTS jobs (
      id            UUID PRIMARY KEY,          -- unique job identifier
      queue_name    VARCHAR(255) NOT NULL,      -- which queue it belongs to
      job_type      VARCHAR(255) NOT NULL,      -- what kind of job (e.g. "send_email")
      payload       JSONB NOT NULL,             -- the job's input data
      status        VARCHAR(50) NOT NULL DEFAULT 'pending',
                                               -- pending | active | completed | failed | dead
      priority      INTEGER NOT NULL DEFAULT 0, -- higher = more important
      attempts      INTEGER NOT NULL DEFAULT 0, -- how many times we've tried
      max_retries   INTEGER NOT NULL DEFAULT 3,
      result        JSONB,                      -- output of the job (if succeeded)
      error_message TEXT,                       -- last error (if failed)
      scheduled_for TIMESTAMPTZ,               -- for delayed jobs (Week 3)
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      started_at    TIMESTAMPTZ,               -- when a worker picked it up
      completed_at  TIMESTAMPTZ                -- when it finished
    );
  `;

  // Indexes make lookups fast. Without these, Postgres would scan every
  // row every time we search by status or queue_name.
  const createIndexes = `
    CREATE INDEX IF NOT EXISTS idx_jobs_status
      ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_queue_name
      ON jobs(queue_name);
    CREATE INDEX IF NOT EXISTS idx_jobs_created_at
      ON jobs(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_jobs_priority
      ON jobs(priority DESC, created_at ASC);
  `;

  // Queue metrics table — aggregated stats for the dashboard
  const createMetricsTable = `
    CREATE TABLE IF NOT EXISTS queue_metrics (
      id            SERIAL PRIMARY KEY,
      queue_name    VARCHAR(255) NOT NULL,
      metric_name   VARCHAR(255) NOT NULL,     -- e.g. "jobs_completed"
      metric_value  NUMERIC NOT NULL DEFAULT 0,
      recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `;

  try {
    await query(createJobsTable);
    await query(createIndexes);
    await query(createMetricsTable);
    logger.info("Database schema initialized successfully");
  } catch (err) {
    logger.error("Failed to initialize schema", { error: err.message });
    throw err;
  }
}

module.exports = { initializeSchema };