// Import necessary modules
import fs from "node:fs";
import { fileURLToPath } from "url";
import path from "node:path";
import csv from "csv-parser";
import { performance } from "perf_hooks";
import chalk from "chalk";
import cliProgress from "cli-progress";
import figlet from "figlet";
import ora from "ora";

// Define __filename and __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Interface for file metrics
interface FileMetrics {
  rowCount: number;
  errorCount: number;
  rowProcessingTimeTotal: number; // in milliseconds
}

async function processFile(
  filePath: string,
  fileIndex: number,
  totalFiles: number,
): Promise<FileMetrics> {
  return new Promise<FileMetrics>((resolve, reject) => {
    let rowCount = 0;
    let errorCount = 0;
    let rowProcessingTimeTotal = 0;

    const fileName = path.basename(filePath);
    const startTime = performance.now();

    console.log(
      chalk.blue(
        `\n📄 Processing file ${fileIndex}/${totalFiles}: ${chalk.bold(fileName)}`,
      ),
    );

    // Initialize progress bar for the file
    const progressBar = new cliProgress.SingleBar({
      format: `${chalk.cyan("{bar}")} {percentage}% | {value}/{total} rows`,
      barCompleteChar: "\u2588",
      barIncompleteChar: "\u2591",
      hideCursor: true,
    });

    // Count total rows to initialize the progress bar
    let totalRows = 0;
    const countSpinner = ora(`Counting rows in ${fileName}`).start();

    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", () => {
        totalRows++;
      })
      .on("end", () => {
        countSpinner.succeed(`Total rows in ${fileName}: ${totalRows}`);
        progressBar.start(totalRows, 0);

        const stream = fs.createReadStream(filePath).pipe(
          csv({
            separator: detectSeparator(filePath),
            mapHeaders: ({ header }) => standardizeHeader(header),
            skipLines: detectSkipLines(filePath),
          }),
        );

        stream.on("data", async (row) => {
          // Pause the stream to prevent reading new data until we're done processing
          stream.pause();

          const rowStartTime = performance.now();

          try {
            await processRow(row);
          } catch (error) {
            errorCount += 1;
          } finally {
            const rowEndTime = performance.now();
            const rowProcessingTime = rowEndTime - rowStartTime;
            rowProcessingTimeTotal += rowProcessingTime;
            rowCount += 1;
            // Update progress bar
            progressBar.update(rowCount);
            // Resume the stream after processing the current row
            stream.resume();
          }
        });

        stream.on("end", () => {
          progressBar.stop();
          const endTime = performance.now();
          const processingTime = endTime - startTime;
          console.log(
            chalk.green(
              `✅ Completed file ${fileIndex}/${totalFiles}: ${chalk.bold(fileName)} (${formatTime(processingTime)})`,
            ),
          );
          resolve({
            rowCount,
            errorCount,
            rowProcessingTimeTotal,
          });
        });

        stream.on("error", (error) => {
          progressBar.stop();
          console.error(
            chalk.red(
              `❌ Error processing file ${fileIndex}/${totalFiles}: ${fileName}`,
            ),
          );
          reject(error);
        });
      })
      .on("error", (error) => {
        countSpinner.fail(`Error counting rows in ${fileName}`);
        console.error(
          chalk.red(`❌ Error counting rows in ${fileName}:`),
          error,
        );
        reject(error);
      });
  });
}

// The rest of your code remains the same, with minor adjustments
// ...

// Update the main function to include the stylized heading
async function main() {
  try {
    // Display a stylized heading using figlet
    console.log(
      chalk.yellow(
        figlet.textSync("ETL Processor", { horizontalLayout: "default" }),
      ),
    );

    const directoryPath = path.join(__dirname, "data");
    const files = fs
      .readdirSync(directoryPath)
      .filter(
        (file) =>
          file.startsWith("sinistros") &&
          (file.endsWith(".csv") || file.endsWith(".tsv")),
      );

    if (files.length === 0) {
      console.log(chalk.red("🚫 No files to process."));
      return;
    }

    const totalFiles = files.length;
    const overallStartTime = performance.now();
    let totalRowCount = 0;
    let totalErrorCount = 0;
    let totalRowProcessingTime = 0;

    for (let i = 0; i < totalFiles; i++) {
      const file = files[i];
      const fileIndex = i + 1;
      const filePath = path.join(directoryPath, file);
      const fileMetrics = await processFile(filePath, fileIndex, totalFiles);

      // Accumulate totals
      totalRowCount += fileMetrics.rowCount;
      totalErrorCount += fileMetrics.errorCount;
      totalRowProcessingTime += fileMetrics.rowProcessingTimeTotal;
    }

    const overallEndTime = performance.now();
    const overallProcessingTime = overallEndTime - overallStartTime;
    const overallAverageRowProcessingTime =
      totalRowCount > 0 ? totalRowProcessingTime / totalRowCount : 0;
    const overallProcessingSpeed =
      totalRowCount / (overallProcessingTime / 1000); // rows per second
    const memoryUsage = process.memoryUsage();

    // Create a pretty output with colors
    console.log(chalk.magenta("\n=========================================="));
    console.log(chalk.bold("             ETL Processing Summary         "));
    console.log(chalk.magenta("=========================================="));
    console.log(
      `${chalk.blue("📂 Total files processed   :")} ${chalk.white(totalFiles)}`,
    );
    console.log(
      `${chalk.blue("📝 Total rows processed    :")} ${chalk.white(totalRowCount)}`,
    );
    console.log(
      `${chalk.blue("❗ Total errors            :")} ${chalk.white(totalErrorCount)}`,
    );
    console.log(
      `${chalk.blue("⏰ Total processing time   :")} ${chalk.white(formatTime(overallProcessingTime))}`,
    );
    console.log(
      `${chalk.blue("⏳ Average time per row    :")} ${chalk.white(formatTime(overallAverageRowProcessingTime))}`,
    );
    console.log(
      `${chalk.blue("⚡ Processing speed        :")} ${chalk.white(`${overallProcessingSpeed.toFixed(2)} rows/second`)}`,
    );
    console.log(
      `${chalk.blue("🧠 Memory usage            :")} ${chalk.white(`${formatMemoryUsage(memoryUsage.heapUsed)} MB`)}`,
    );
    console.log(chalk.magenta("=========================================="));
  } catch (err) {
    console.error(chalk.red("❌ Error:"), err);
  }
}

main();

// Rest of the helper functions...

function detectSeparator(filePath: string): string {
  return filePath.endsWith(".csv") ? "," : ";";
}

function detectSkipLines(filePath: string): number {
  return 0;
}

function standardizeHeader(header: string): string {
  return header
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}

async function processRow(row: any) {
  // Implement your actual row processing logic here
}

function formatTime(milliseconds: number): string {
  const seconds = milliseconds / 1000;
  return `${seconds.toFixed(3)} seconds`;
}

function formatMemoryUsage(bytes: number): string {
  const megabytes = bytes / (1024 * 1024);
  return megabytes.toFixed(2);
}
