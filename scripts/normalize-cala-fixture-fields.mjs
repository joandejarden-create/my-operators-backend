#!/usr/bin/env node
/**
 * Normalize fictional + reference fields in CALA sample-deal fixtures to form select options.
 * Usage: node scripts/normalize-cala-fixture-fields.mjs [fixture.json ...]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeDealSetupFields } from "../lib/deal-setup-form-value-normalize.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const DEFAULT_DIR = path.join(ROOT, "fixtures", "sample-deals");

const CALA_FIXTURES = [
  "proyecto-reforma-urban-conversion.example.json",
  "playa-dorada-resort-repositioning.example.json",
  "cartagena-walled-city-collection.example.json",
  "merida-centro-select-service.example.json",
  "san-juan-bay-turnaround.example.json",
  "panama-city-mixed-use-hotel-component.example.json",
  "aeropuerto-cancun-select-service.example.json",
  "cusco-heritage-palace-hotel.example.json",
  "colonial-city-lifestyle-conversion.example.json",
  "riviera-maya-wellness-resort-repositioning.example.json",
  "andean-business-hotel-reflag.example.json",
  "cascadas-lifestyle-hotel-component.example.json",
];

const files = process.argv.slice(2).length
  ? process.argv.slice(2).map((f) => path.resolve(f))
  : CALA_FIXTURES.map((f) => path.join(DEFAULT_DIR, f));

let totalCoercions = 0;

for (const filePath of files) {
  if (!fs.existsSync(filePath)) {
    console.warn("Skip (missing):", filePath);
    continue;
  }
  const record = JSON.parse(fs.readFileSync(filePath, "utf8"));
  let coercions = 0;

  for (const layer of ["referenceProperty", "fictionalDeal"]) {
    const block = record[layer];
    if (!block?.fields) continue;
    const { fields, coercions: c } = normalizeDealSetupFields(block.fields);
    block.fields = fields;
    coercions += c.length;
  }

  fs.writeFileSync(filePath, JSON.stringify(record, null, 2) + "\n", "utf8");
  totalCoercions += coercions;
  console.log(path.basename(filePath), coercions, "field coercions");
}

console.log("Done.", totalCoercions, "total coercions");
