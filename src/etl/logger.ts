import { createLogger, transports, format as winstonFormat } from "winston";
import path from "path";
import fs from "fs";

// Setup log directory
const logDirectory = path.join(__dirname, "logs");

// Ensure log directory exists
if (!fs.existsSync(logDirectory)) fs.mkdirSync(logDirectory);

export const logger = createLogger({
  level: "info",
  format: winstonFormat.combine(
    winstonFormat.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
    winstonFormat.printf(
      ({ timestamp, level, message }) =>
        `${timestamp} [${level.toUpperCase()}]: ${message}`,
    ),
  ),
  transports: [
    new transports.File({
      filename: path.join(logDirectory, "preprocess.log"),
    }),
    new transports.Console(),
  ],
});
