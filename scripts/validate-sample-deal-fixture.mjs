#!/usr/bin/env node
/**
 * Validate a sample-deal JSON fixture against governance rules.
 * Usage: node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/foo.json
 */
import fs from "node:fs";
import path from "node:path";
import { validateSampleDealRecord } from "../lib/sample-opportunity-deal-schema.js";

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/validate-sample-deal-fixture.mjs <path-to.json>");
  process.exit(1);
}

const abs = path.resolve(file);
const raw = fs.readFileSync(abs, "utf8");
let record;
try {
  record = JSON.parse(raw);
} catch (e) {
  console.error("Invalid JSON:", e.message);
  process.exit(1);
}

const result = validateSampleDealRecord(record);
for (const w of result.warnings) console.warn("WARN:", w);
for (const e of result.errors) console.error("ERROR:", e);

if (result.ok) {
  console.log("OK:", abs);
  process.exit(0);
}
console.error("Validation failed:", abs);
process.exit(1);
