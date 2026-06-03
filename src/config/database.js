const { Pool } = require("pg");
const logger = require("../utils/logger");

const pool = new Pool({
  host: process.env.PG_HOST || "localhost",
  port: parseInt(process.env.PG_PORT) || 5432,
  database: process.env.PG_DATABASE || "taskqueue",
  user: process.env.PG_USER || "postgres",
  password: process.env.PG_PASSWORD || "postgres",
  // Pool settings:
  max: 10,              // maximum 10 open connections at once
  idleTimeoutMillis: 30000,   // close idle connections after 30s
  connectionTimeoutMillis: 2000, // fail fast if can't connect in 2s
});

// Test the connection when the module first loads
pool.on("connect", () => {
  logger.info("PostgreSQL client connected to pool");
});

pool.on("error", (err) => {
  logger.error("PostgreSQL pool error", { error: err.message });
});

// A thin wrapper so you can do: db.query(sql, params) anywhere
async function query(text, params) {
  const start = Date.now();
  try {
    const result = await pool.query(text, params);
    const duration = Date.now() - start;
    logger.debug("PostgreSQL query executed", {
      query: text.substring(0, 80), // log first 80 chars of query
      duration: `${duration}ms`,
      rows: result.rowCount,
    });
    return result;
  } catch (err) {
    logger.error("PostgreSQL query failed", {
      query: text.substring(0, 80),
      error: err.message,
    });
    throw err;
  }
}

module.exports = { pool, query };