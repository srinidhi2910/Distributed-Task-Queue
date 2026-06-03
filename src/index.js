require("dotenv").config({ silent: true });

const app    = require("./api/app");
const logger = require("./utils/logger");
const { initializeSchema } = require("./db/schema");

const PORT = process.env.PORT || 3000;

async function main() {
  await initializeSchema();

  app.listen(PORT, () => {
    logger.info(`API server running on http://localhost:${PORT}`);
    logger.info("Available endpoints:", {
      health:     `GET  /health`,
      enqueue:    `POST /api/jobs`,
      listJobs:   `GET  /api/jobs`,
      getJob:     `GET  /api/jobs/:id`,
      cancelJob:  `DELETE /api/jobs/:id`,
      stats:      `GET  /api/queues/stats`,
      dlq:        `GET  /api/jobs/dlq`,
      replay:     `POST /api/jobs/dlq/:id/replay`,
    });
  });
}

main().catch((err) => {
  logger.error("Server failed to start", { error: err.message });
  process.exit(1);
});