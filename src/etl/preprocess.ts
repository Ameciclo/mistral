// preprocess.ts
import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";
import { pipeline } from "stream";
import { promisify } from "util";
import chalk from "chalk";
import ora from "ora";
import { format } from "date-fns";
import { createLogger, transports, format as winstonFormat } from "winston";

const pipelineAsync = promisify(pipeline);

const inputDirectory = path.join(__dirname, "data");
const outputDirectory = path.join(__dirname, "processed");
const logDirectory = path.join(__dirname, "logs");

if (!fs.existsSync(outputDirectory)) {
  fs.mkdirSync(outputDirectory);
}

if (!fs.existsSync(logDirectory)) {
  fs.mkdirSync(logDirectory);
}

// Configure the logger
const logger = createLogger({
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

// Function to determine the separator based on file extension
function detectSeparator(filePath: string): string {
  if (filePath.endsWith(".tsv")) {
    return ";";
  } else if (filePath.endsWith(".csv")) {
    return ",";
  } else {
    // Default separator if unknown
    return ",";
  }
}

// Function to standardize headers
function standardizeHeader(header: string): string {
  return header
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}

// Function to get unique headers from all files
async function collectUniqueHeaders(files: string[]): Promise<string[]> {
  const allHeaders = new Set<string>();

  for (const file of files) {
    const filePath = path.join(inputDirectory, file);
    const separator = detectSeparator(filePath);

    try {
      const headers = await new Promise<string[]>((resolve, reject) => {
        const readStream = fs
          .createReadStream(filePath)
          .pipe(
            csv({
              separator: separator,
              mapHeaders: ({ header }) => standardizeHeader(header),
              maxRows: 1, // Only need the headers
            }),
          )
          .on("headers", (headers) => {
            headers.forEach((header) => allHeaders.add(header));
            readStream.destroy(); // Stop reading after getting headers
            resolve(headers);
          })
          .on("error", (error) => {
            logger.error(
              `Error reading headers from ${file}: ${error.message}`,
            );
            reject(error);
          });
      });
    } catch (error) {
      logger.error(`Failed to process file ${file} during header collection.`);
    }
  }

  return Array.from(allHeaders).sort(); // Sort for consistency
}

// Normalize date to 'YYYY-MM-DD' format
function normalizeDate(dateString: string): string {
  if (!dateString) return "";
  const parsedDate = new Date(dateString);
  if (isNaN(parsedDate.getTime())) return "";
  return format(parsedDate, "yyyy-MM-dd");
}

// Normalize time to 'HH:mm:ss' format
function normalizeTime(timeString: string): string {
  if (!timeString) return "";
  const timeParts = timeString.split(":");
  if (timeParts.length >= 2) {
    const hours = timeParts[0].padStart(2, "0");
    const minutes = timeParts[1].padStart(2, "0");
    const seconds = timeParts[2] ? timeParts[2].padStart(2, "0") : "00";
    return `${hours}:${minutes}:${seconds}`;
  }
  return "";
}

// Parse numeric fields, handle commas and dots
function parseNumericField(value: string): number {
  if (!value) return 0;
  const numericValue = value.replace(",", ".");
  const parsed = parseFloat(numericValue);
  return isNaN(parsed) ? 0 : parsed;
}

// Preprocess all files
async function preprocessAllFiles() {
  const files = fs
    .readdirSync(inputDirectory)
    .filter(
      (file) =>
        file.startsWith("sinistros") &&
        (file.endsWith(".csv") ||
          file.endsWith(".tsv") ||
          file.endsWith(".txt")),
    );

  if (files.length === 0) {
    logger.warn("No files found to process.");
    console.log(chalk.yellow("🚫 No files to process."));
    return;
  }

  // Collect all unique headers from all files
  logger.info("Starting header collection from all files.");
  console.log(chalk.blue("Collecting unique headers from all files..."));
  const allHeaders = await collectUniqueHeaders(files);
  logger.info(`Collected ${allHeaders.length} unique headers.`);
  console.log(chalk.green(`Found ${allHeaders.length} unique headers.`));

  for (const file of files) {
    await preprocessFile(file, allHeaders);
  }

  logger.info("Preprocessing completed successfully.");
  console.log(chalk.green("\nPreprocessing completed."));
}

// Function to preprocess a single file
async function preprocessFile(
  fileName: string,
  allHeaders: string[],
): Promise<void> {
  const filePath = path.join(inputDirectory, fileName);
  const outputFileName = path.parse(fileName).name + "_processed.csv"; // Output as .csv
  const outputFilePath = path.join(outputDirectory, outputFileName);

  const spinner = ora(`Processing ${fileName}`).start();
  logger.info(`Processing file: ${fileName}`);

  try {
    const separator = detectSeparator(filePath);

    // Create read and write streams
    const readStream = fs.createReadStream(filePath);
    const writeStream = fs.createWriteStream(outputFilePath);

    const csvWriter = createObjectCsvWriter({
      path: outputFilePath,
      header: allHeaders.map((header) => ({ id: header, title: header })),
      fieldDelimiter: ";", // Choose a consistent delimiter for output
      encoding: "utf8",
    });

    // Process rows using pipeline
    await pipelineAsync(
      readStream,
      csv({
        separator: separator,
        mapHeaders: ({ header }) => standardizeHeader(header),
        skipLines: 0,
      }),
      async function* (source) {
        for await (const row of source) {
          const standardizedRow: any = {};

          // Map existing columns to standardized columns
          for (const header of allHeaders) {
            standardizedRow[header] = row[header] || "";
          }

          // Additional data transformations
          if (standardizedRow["data"]) {
            standardizedRow["data"] = normalizeDate(standardizedRow["data"]);
          }

          if (standardizedRow["hora"]) {
            standardizedRow["hora"] = normalizeTime(standardizedRow["hora"]);
          }

          // Identify numeric fields dynamically
          const numericFields = allHeaders.filter((header) => {
            // Heuristics to identify numeric fields
            return (
              [
                "auto",
                "moto",
                "ciclom",
                "ciclista",
                "pedestre",
                "onibus",
                "caminhao",
                "viatura",
                "outros",
                "vitimas",
                "vitimasfatais",
              ].includes(header) ||
              header.startsWith("num_") ||
              header.endsWith("_count")
            );
          });

          for (const field of numericFields) {
            standardizedRow[field] = parseNumericField(standardizedRow[field]);
          }

          yield standardizedRow;
        }
      },
      async function* (source) {
        // Write data using csv-writer
        for await (const row of source) {
          await csvWriter.writeRecords([row]);
        }
      },
    );

    spinner.succeed(`Processed and saved: ${outputFileName}`);
    logger.info(`Successfully processed file: ${fileName}`);
  } catch (error) {
    spinner.fail(`Error processing ${fileName}: ${error.message}`);
    logger.error(`Error processing file ${fileName}: ${error.message}`);
  }
}

preprocessAllFiles().catch((error) => {
  logger.error(`Unhandled error during preprocessing: ${error.message}`);
  console.error(chalk.red("Error during preprocessing:"), error);
});
