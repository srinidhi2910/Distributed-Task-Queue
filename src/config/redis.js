const Redis = require("ioredis");
const logger = require("../utils/logger");

// ioredis options — we configure reconnection behavior explicitly
// so the app doesn't silently die if Redis restarts
const redisConfig = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT) || 6379,
  // Retry connection with exponential backoff up to 30 seconds
  retryStrategy(times) {
    const delay = Math.min(times * 500, 30000);
    logger.warn(`Redis reconnecting... attempt ${times}, next try in ${delay}ms`);
    return delay;
  },
  // If a command fails because Redis is reconnecting, re-queue it
  enableReadyCheck: true,
  maxRetriesPerRequest: 3,
};

function createRedisClient(name = "default") {
  const client = new Redis(redisConfig);

  client.on("connect", () => {
    logger.info(`Redis [${name}] connected`, {
      host: redisConfig.host,
      port: redisConfig.port,
    });
  });

  client.on("ready", () => {
    logger.info(`Redis [${name}] ready to accept commands`);
  });

  client.on("error", (err) => {
    logger.error(`Redis [${name}] error`, { error: err.message });
  });

  client.on("close", () => {
    logger.warn(`Redis [${name}] connection closed`);
  });

  return client;
}

module.exports = { createRedisClient };