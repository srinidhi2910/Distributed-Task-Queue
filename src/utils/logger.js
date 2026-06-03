const winston = require("winston");
const path = require("path");

// Define log format: timestamp + level + message + any extra metadata
const logFormat = winston.format.combine(
  winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
  winston.format.errors({ stack: true }), // prints full stack trace on errors
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    // If extra data was passed (like a job ID), append it as JSON
    const metaStr = Object.keys(meta).length ? JSON.stringify(meta) : "";
    return `[${timestamp}] ${level.toUpperCase()}: ${message} ${metaStr}`;
  })
);

const logger = winston.createLogger({
  level: process.env.NODE_ENV === "production" ? "info" : "debug",
  format: logFormat,
  transports: [
    // Always log to console
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(), // adds color in terminal
        logFormat
      ),
    }),
    // Write all logs to combined.log
    new winston.transports.File({
      filename: path.join("logs", "combined.log"),
      maxsize: 5 * 1024 * 1024, // 5MB max per file
      maxFiles: 5,              // keep last 5 rotated files
    }),
    // Write only errors to error.log
    new winston.transports.File({
      filename: path.join("logs", "error.log"),
      level: "error",
    }),
  ],
});

module.exports = logger;