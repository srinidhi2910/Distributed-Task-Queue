const logger = require("../utils/logger");

// For load testing: 10ms max so we measure queue throughput not fake I/O
function simulateWork(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.min(ms, 10)));
}

const handlers = {
  async send_email(job) {
    const { to, subject } = job.payload;
    logger.info("Sending email", { jobId: job.id, to, subject });

    await simulateWork(300);

    if (Math.random() < 0.1) {
      throw new Error(`SMTP timeout for ${to}`);
    }

    logger.info("Email sent successfully", { jobId: job.id, to });
    return { sent: true, to, timestamp: new Date().toISOString() };
  },

  async resize_image(job) {
    const { imageUrl, width, height } = job.payload;
    logger.info("Resizing image", { jobId: job.id, imageUrl, width, height });

    await simulateWork(1200);

    logger.info("Image resized successfully", { jobId: job.id });
    return { resized: true, imageUrl, width, height };
  },

  async generate_report(job) {
    const { reportType, userId } = job.payload;
    logger.info("Generating report", { jobId: job.id, reportType, userId });

    await simulateWork(2000);

    logger.info("Report generated", { jobId: job.id, reportType });
    return { reportType, userId, generatedAt: new Date().toISOString() };
  },
};

async function processJob(job) {
  const handler = handlers[job.job_type];

  if (!handler) {
    throw new Error(
      `No handler registered for job type: "${job.job_type}". ` +
      `Registered types: ${Object.keys(handlers).join(", ")}`
    );
  }

  return await handler(job);
}

module.exports = { processJob, handlers };