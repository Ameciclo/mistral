import fs from "fs";
import path from "path";
import { logger } from "./logger";

// Validates if a string follows the HH:mm:ss format
export function isValidTime(time: string): boolean {
  const timeRegex = /^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$/; // 24-hour format
  return timeRegex.test(time);
}

// Combines date and time into ISO8601 with UTC-3 offset
export function combineDateTime(
  dateString: string,
  timeString?: string,
): string {
  const cleanedDate = dateString.split("T")[0].trim();
  let cleanedTime = "00:00:00"; // Default fallback time

  if (timeString && isValidTime(timeString.split(".")[0].trim())) {
    cleanedTime = timeString.split(".")[0].trim();
  } else {
    logger.warn(
      `Invalid or missing time value "${timeString}", using fallback "00:00:00".`,
    );
  }

  return `${cleanedDate}T${cleanedTime}-03:00`; // UTC-3 offset (Recife)
}

// Detects the delimiter by checking the first line of the file
export function detectDelimiter(filePath: string): string {
  const firstLine = fs.readFileSync(filePath, "utf-8").split("\n")[0];

  if (firstLine.includes("\t")) return "\t";
  if (firstLine.includes(";")) return ";";
  if (firstLine.includes(",")) return ",";

  logger.warn(
    `Could not detect delimiter in file: ${filePath}. Defaulting to ','`,
  );
  return ","; // Default delimiter
}

// Detects if the file is CSV or TSV based on its extension
export function detectFileFormat(fileName: string): "csv" | "tsv" {
  return fileName.endsWith(".csv") ? "csv" : "tsv";
}
