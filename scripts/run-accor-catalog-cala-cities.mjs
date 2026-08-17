#!/usr/bin/env node
/**
 * Export + optional apply for priority CALA destination catalog pulls.
 *
 *   node scripts/run-accor-catalog-cala-cities.mjs
 *   node scripts/run-accor-catalog-cala-cities.mjs --apply --fetch-amenities
 *   node scripts/run-accor-catalog-cala-cities.mjs --cities lima,santiago --apply
 */
import "../load-env.js";
import { spawnSync } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

/** @type {{ destination: string, countryCode: string, country: string }[]} */
export const ACCOR_CALA_CATALOG_CITIES = [
  { destination: "lima", countryCode: "PE", country: "Peru" },
  { destination: "santiago", countryCode: "CL", country: "Chile" },
  { destination: "sao paulo", countryCode: "BR", country: "Brazil" },
  { destination: "rio de janeiro", countryCode: "BR", country: "Brazil" },
  { destination: "mexico city", countryCode: "MX", country: "Mexico" },
  { destination: "buenos aires", countryCode: "AR", country: "Argentina" },
  { destination: "cancun", countryCode: "MX", country: "Mexico" },
  { destination: "panama city", countryCode: "PA", country: "Panama" },
];

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const fetchAmenities = args.includes("--fetch-amenities");
const citiesArg = args.find((a) => a.startsWith("--cities="))?.split("=")[1];
const onlyPrimary = args.includes("--primary-only");

let cities = onlyPrimary
  ? ACCOR_CALA_CATALOG_CITIES.slice(0, 3)
  : ACCOR_CALA_CATALOG_CITIES;

if (citiesArg) {
  const want = new Set(citiesArg.split(",").map((s) => s.trim().toLowerCase()));
  cities = cities.filter((c) => want.has(c.destination.toLowerCase()));
}

function run(script, scriptArgs) {
  const res = spawnSync("node", [join(__dirname, script), ...scriptArgs], {
    cwd: ROOT,
    stdio: "inherit",
    encoding: "utf8",
  });
  if (res.status !== 0) {
    throw new Error(`${script} failed with exit ${res.status}`);
  }
}

console.log("=== Accor CALA catalog cities ===\n");
console.log("Cities:", cities.map((c) => c.destination).join(", "));
console.log("Apply:", apply, "| Fetch amenities:", fetchAmenities);
console.log("");

/** @type {object[]} */
const summary = [];

for (const city of cities) {
  console.log(`\n--- ${city.destination} (${city.country}) ---\n`);
  run("export-accor-catalog-destination.mjs", [
    "--destination",
    city.destination,
    "--country-code",
    city.countryCode,
  ]);

  if (apply) {
    const applyArgs = [
      "--destination",
      city.destination,
      `--country=${city.country}`,
      "--apply",
    ];
    if (fetchAmenities) {
      applyArgs.push("--fetch-amenities", "--delay-ms=600");
    }
    run("apply-accor-catalog-destination-batch.mjs", applyArgs);
  }

  summary.push({ ...city, exported: true, applied: apply });
}

console.log("\n=== Done ===");
console.log(JSON.stringify(summary, null, 2));
