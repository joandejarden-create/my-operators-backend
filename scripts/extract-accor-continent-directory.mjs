#!/usr/bin/env node
/**
 * Crawl Accor continent browse pages (JSON-LD) for CALA hotel discovery.
 *
 *   node scripts/extract-accor-continent-directory.mjs
 *   node scripts/extract-accor-continent-directory.mjs --continent southAmerica --max-pages=5
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, readFileSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { extractAccorContinentHotels } from "../lib/accor-continent-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = join(__dirname, "..", "reports", "accor-continent-directory-extract.json");
const EXTRACT = join(__dirname, "..", "reports", "accor-property-directory-extract.json");
const PLAN = join(__dirname, "..", "reports", "accor-census-match-expansion-plan.json");

const continent = process.argv.find((a) => a.startsWith("--continent="))?.split("=")[1];
const maxPages = process.argv.find((a) => a.startsWith("--max-pages="))
  ? Number(process.argv.find((a) => a.startsWith("--max-pages="))?.split("=")[1])
  : null;

console.log("=== Accor continent directory extract ===\n");
const result = await extractAccorContinentHotels({
  continent: continent || undefined,
  maxPages: maxPages || undefined,
  delayMs: 120,
});

mkdirSync(join(__dirname, "..", "reports"), { recursive: true });
writeFileSync(OUT, JSON.stringify({ generatedAt: new Date().toISOString(), ...result }, null, 2));

console.log("Total property codes:", result.propertyRows.length);
for (const row of result.summary.byContinent) {
  console.log(`  ${row.label}: ${row.count}`);
}
console.log("Written:", OUT);

// Gap vs current metadata extract
if (existsSync(EXTRACT)) {
  const current = JSON.parse(readFileSync(EXTRACT, "utf8"));
  const currentIds = new Set(
    (current.propertyRows || []).map((r) => String(r.propertyId || "").toUpperCase()).filter(Boolean)
  );
  const newCodes = result.propertyRows.filter((r) => !currentIds.has(r.propertyId));
  console.log("\nNot in current property extract:", newCodes.length);
  if (newCodes.length) {
    console.log("Sample new codes:", newCodes.slice(0, 10).map((r) => `${r.propertyId} ${r.inferredHotelName}`));
  }
}

// Gap vs assigned expansion plan
if (existsSync(PLAN)) {
  const plan = JSON.parse(readFileSync(PLAN, "utf8"));
  const assigned = new Set(
    [...(plan.planRows || []), ...(plan.stewardRows || [])].map((r) =>
      String(r.propertyId || "").toUpperCase()
    )
  );
  const unassigned = result.propertyRows.filter((r) => !assigned.has(r.propertyId));
  console.log("Not in last match expansion (333 assigned):", unassigned.length);
}
