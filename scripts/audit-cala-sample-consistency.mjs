#!/usr/bin/env node
/**
 * Report cross-field inconsistencies in CALA sample fixtures (full intake).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { auditIntakeFieldConsistency } from "../lib/cala-sample-intake-consistency.js";

const DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "fixtures", "sample-deals");

const CALA_SLUGS = [
  "proyecto-reforma",
  "playa-dorada",
  "cartagena",
  "merida-centro",
  "san-juan",
  "panama-city",
  "aeropuerto-cancun",
  "cusco",
  "colonial-city",
  "riviera-maya",
  "andean",
  "cascadas",
];

const FILES = fs
  .readdirSync(DIR)
  .filter((f) => f.endsWith(".example.json") && CALA_SLUGS.some((s) => f.includes(s)));

let total = 0;
for (const file of FILES) {
  const record = JSON.parse(fs.readFileSync(path.join(DIR, file), "utf8"));
  const issues = auditIntakeFieldConsistency(record.fictionalDeal?.fields || {}, "fictional");
  if (issues.length) {
    console.log(`\n${file}`);
    for (const i of issues) console.log("  -", i);
    total += issues.length;
  }
}
console.log(total ? `\n${total} issue(s)` : "\nNo CALA intake consistency issues found.");
process.exit(total ? 1 : 0);
