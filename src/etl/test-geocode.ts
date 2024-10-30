import fs from "node:fs";
import path from "node:path";
import chalk from "chalk";
import ndjson from "ndjson";

// Define types for row and geocode result
interface Row {
  meta: string;
  tipo: string;
  situacao: string;
  datahora: string;
}

interface Coordinates {
  latitude: number | null;
  longitude: number | null;
}

// Nominatim API URL
const NOMINATIM_API_URL = "https://nominatim.openstreetmap.org/search";

// Extracts address components from metadata
function extractAddress(meta: Record<string, string>): string {
  const {
    endereco,
    numero,
    bairro,
    complemento,
    endereco_cruzamento,
    bairro_cruzamento,
  } = meta;

  if (endereco && numero) {
    return `${endereco}, ${numero}, ${bairro}`;
  } else if (endereco && complemento) {
    return `${endereco}, ${complemento}, ${bairro}`;
  } else if (endereco && endereco_cruzamento) {
    return `${endereco} com ${endereco_cruzamento}, ${bairro_cruzamento || bairro}`;
  } else {
    return `${endereco || ""}, ${bairro || ""}`.trim();
  }
}

// Geocode function using native fetch
async function geocodeAddress(address: string): Promise<Coordinates> {
  const params = new URLSearchParams({ q: address, format: "json" });
  const apiUrl = `${NOMINATIM_API_URL}?${params}`;

  try {
    const response = await fetch(apiUrl);
    if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);

    const data = await response.json();
    if (data.length === 0) throw new Error("No results found");

    const { lat, lon } = data[0];
    return { latitude: parseFloat(lat), longitude: parseFloat(lon) };
  } catch (error) {
    console.error(`Geocoding failed for "${address}": ${error.message}`);
    return { latitude: null, longitude: null }; // Fallback
  }
}

// Reads and collects all rows from all NDJSON files
async function collectAllRows(directoryPath: string): Promise<Row[]> {
  const files = fs
    .readdirSync(directoryPath)
    .filter((file) => file.endsWith(".ndjson"));

  const allRows: Row[] = [];

  for (const file of files) {
    const filePath = path.join(directoryPath, file);
    const rows: Row[] = await new Promise((resolve, reject) => {
      const fileRows: Row[] = [];
      fs.createReadStream(filePath)
        .pipe(ndjson.parse())
        .on("data", (row: Row) => fileRows.push(row))
        .on("end", () => resolve(fileRows))
        .on("error", (error) => reject(error));
    });
    allRows.push(...rows);
  }

  return allRows;
}

// Selects 30 random rows from all collected rows
function pickRandomRows(rows: Row[], sampleSize: number = 30): Row[] {
  return rows.sort(() => 0.5 - Math.random()).slice(0, sampleSize);
}

// Test mode to geocode 30 random lines
async function runTest(): Promise<void> {
  console.log(chalk.blue("\n🔍 Running geocode test..."));

  const directoryPath = path.join(__dirname, "processed");

  try {
    const allRows = await collectAllRows(directoryPath);
    const sampleRows = pickRandomRows(allRows);

    let successCount = 0;

    for (const row of sampleRows) {
      try {
        const meta = JSON.parse(row.meta);
        const address = extractAddress(meta);
        console.log(chalk.yellow(`\n📍 Address: ${address}`));

        const coordinates = await geocodeAddress(address);
        console.log(
          chalk.green(`🌐 Coordinates: ${JSON.stringify(coordinates)}`),
        );

        // Count successful geocodes (valid coordinates)
        if (coordinates.latitude !== null && coordinates.longitude !== null) {
          successCount++;
        }
      } catch (error) {
        console.error(`Error processing row: ${error.message}`);
      }
    }

    // Calculate and display the success percentage
    const successPercentage = (successCount / sampleRows.length) * 100;
    console.log(
      chalk.magenta(
        `\n✅ Success rate: ${successPercentage.toFixed(2)}% (${successCount}/${sampleRows.length})`,
      ),
    );
  } catch (error) {
    console.error(chalk.red("Error in geocode test:"), error);
  }
}

// Execute the test function
runTest().catch((error) =>
  console.error(chalk.red("Error during geocode test execution:"), error),
);
