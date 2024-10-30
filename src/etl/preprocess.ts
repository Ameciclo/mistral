import fs from "fs";
import path from "path";
import { parse as fastCsvParse } from "fast-csv";
import { pipeline, Transform } from "stream";
import { promisify } from "util";
import chalk from "chalk";
import ora from "ora";
import { createLogger, transports, format as winstonFormat } from "winston";

const pipelineAsync = promisify(pipeline);

// Setup directories
const inputDirectory = path.join(__dirname, "data");
const outputDirectory = path.join(__dirname, "processed");
const logDirectory = path.join(__dirname, "logs");

// Ensure directories exist
if (!fs.existsSync(outputDirectory)) fs.mkdirSync(outputDirectory);
if (!fs.existsSync(logDirectory)) fs.mkdirSync(logDirectory);

// Logger configuration
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

// Validates if a string follows the HH:mm:ss format
function isValidTime(time: string): boolean {
  const timeRegex = /^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$/; // 24-hour format
  return timeRegex.test(time);
}

// Combines date and time into ISO8601 with UTC-3 offset
function combineDateTime(dateString: string, timeString?: string): string {
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
function detectDelimiter(filePath: string): string {
  const firstLine = fs.readFileSync(filePath, "utf-8").split("\n")[0];

  if (firstLine.includes("\t")) return "\t";
  if (firstLine.includes(";")) return ";";
  if (firstLine.includes(",")) return ",";

  logger.warn(
    `Could not detect delimiter in file: ${filePath}. Defaulting to ','`,
  );
  return ","; // Default delimiter
}

// Process all files in the input directory
async function preprocessAllFiles(): Promise<void> {
  const files = fs.readdirSync(inputDirectory);

  if (files.length === 0) {
    logger.warn("No files found to process.");
    console.log(chalk.yellow("🚫 No files to process."));
    return;
  }

  for (const file of files) {
    const format = detectFileFormat(file);
    await preprocessFile(file, format);
  }

  logger.info("Preprocessing completed successfully.");
  console.log(chalk.green("\nPreprocessing completed."));
}

// Detects if the file is CSV or TSV based on its extension
function detectFileFormat(fileName: string): "csv" | "tsv" {
  return fileName.endsWith(".csv") ? "csv" : "tsv";
}

// Dynamically process a single file and output NDJSON
async function preprocessFile(
  fileName: string,
  format: "csv" | "tsv",
): Promise<void> {
  const filePath = path.join(inputDirectory, fileName);
  const outputFileName = fileName.replace(/\.(csv|tsv)$/, "_processed.ndjson");
  const outputFilePath = path.join(outputDirectory, outputFileName);

  const spinner = ora(`Processing ${fileName}`).start();
  logger.info(`Processing file: ${fileName} (Overwriting if exists)`);

  let lineNumber = 0; // Track line numbers for error reporting

  try {
    const delimiter = detectDelimiter(filePath); // Detect delimiter dynamically
    const readStream = fs.createReadStream(filePath);
    const writeStream = fs.createWriteStream(outputFilePath, { flags: "w" }); // Always overwrite

    const transformStream = new Transform({
      objectMode: true,
      transform(row: any, encoding, callback) {
        try {
          lineNumber++; // Increment line number for each row

          // Normalize `tipo` from either `natureza_acidente` or `tipo`
          const tipo = row.natureza_acidente || row.natureza || row.tipo || "";
          if (!tipo) {
            logger.warn(
              `Missing 'tipo', 'natureza', or 'natureza_acidente' in row at line ${lineNumber}.`,
            );
          }

          const situacao = row.situacao || "";
          const data = row.data || "";
          const hora = row.hora || "";

          // Combine `data` and `hora` into an ISO8601 datetime string
          const datahora = combineDateTime(data, hora);

          // Collect remaining fields into `meta`
          const meta = JSON.stringify(
            Object.fromEntries(
              Object.entries(row).filter(
                ([key]) =>
                  ![
                    "tipo",
                    "natureza_acidente",
                    "natureza",
                    "situacao",
                    "data",
                    "hora",
                  ].includes(key),
              ),
            ),
          );

          const transformedRow = { tipo, situacao, datahora, meta };

          // Write the transformed row as a JSON line
          writeStream.write(JSON.stringify(transformedRow) + "\n");
          callback();
        } catch (error) {
          logger.error(
            `Row transformation error in file "${fileName}" at line ${lineNumber}: ${
              error instanceof Error ? error.message : JSON.stringify(error)
            }\nRow: ${JSON.stringify(row)}`,
          );
          callback(); // Skip this row on error
        }
      },
    });

    await pipelineAsync(
      readStream,
      fastCsvParse({ delimiter, headers: true, ignoreEmpty: true }),
      transformStream,
    );

    spinner.succeed(`Processed and saved: ${outputFileName}`);
    logger.info(`Successfully processed file: ${fileName}`);
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : JSON.stringify(error);
    spinner.fail(`Error processing ${fileName}: ${errorMessage}`);
    logger.error(`Error processing file ${fileName}: ${errorMessage}`);
  }
}

// Start the preprocessing pipeline
preprocessAllFiles().catch((error: unknown) => {
  const errorMessage =
    error instanceof Error ? error.message : JSON.stringify(error);
  logger.error(`Unhandled error: ${errorMessage}`);
  console.error(chalk.red("Error during preprocessing:"), error);
});
