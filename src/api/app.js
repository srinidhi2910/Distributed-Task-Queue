const express    = require("express");
const app        = express();
const logger     = require("../utils/logger");

// Parse JSON request bodies
app.use(express.json());
// Allow React dev server to call the API
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin",  "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// Request logging middleware — logs every incoming request
app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    logger.info("HTTP request", {
      method:   req.method,
      path:     req.path,
      status:   res.statusCode,
      duration: `${Date.now() - start}ms`,
    });
  });
  next();
});

// Routes (we'll create these next)
const jobRoutes   = require("./routes/jobs");
const queueRoutes = require("./routes/queues");

app.use("/api/jobs",   jobRoutes);
app.use("/api/queues", queueRoutes);

// Health check — useful for Docker and load balancers later
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Global error handler — catches any error thrown inside route handlers
app.use((err, req, res, next) => {
  logger.error("Unhandled API error", { error: err.message, path: req.path });
  res.status(500).json({ error: "Internal server error", message: err.message });
});

module.exports = app;