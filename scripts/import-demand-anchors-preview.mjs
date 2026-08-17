#!/usr/bin/env node
/**
 * CLI preview for Demand Anchors import.
 *   node scripts/import-demand-anchors-preview.mjs --file fixtures/demand-anchors-cancun.json
 *   node scripts/import-demand-anchors-preview.mjs --file fixtures/demand-anchors-cancun.json --market Cancun --country Mexico
 */
import "../load-env.js";
import { readFileSync } from "fs";
import { previewDemandAnchorsImport } from "../lib/demand-anchors/import-commit.js";
import { parseRequireVerifiedFixtureFlag } from "../lib/location-verification/verified-fixture-gating.js";

const args = process.argv.slice(2);
const DRY = args.includes("--dry-run") || !args.includes("--apply");
const fileIdx = args.indexOf("--file");
const file = fileIdx >= 0 ? args[fileIdx + 1] : null;
const marketIdx = args.indexOf("--market");
const countryIdx = args.indexOf("--country");
const regionIdx = args.indexOf("--region");
const limitIdx = args.indexOf("--limit");
const limit = limitIdx >= 0 ? Number(args[limitIdx + 1]) : null;
const requireVerifiedFile = parseRequireVerifiedFixtureFlag(args);

if (!file) {
  console.error("Usage: node scripts/import-demand-anchors-preview.mjs --file <path> [--market] [--country] [--region] [--limit N]");
  process.exit(1);
}

const payload = JSON.parse(readFileSync(file, "utf8"));
const body = {
  market: marketIdx >= 0 ? args[marketIdx + 1] : payload.market || "",
  country: countryIdx >= 0 ? args[countryIdx + 1] : payload.country || "",
  region: regionIdx >= 0 ? args[regionIdx + 1] : payload.region || "",
  verification: payload.verification || null,
  requireVerifiedFile,
  points: (payload.points || []).slice(0, limit || undefined),
};

const result = await previewDemandAnchorsImport(body);
if (!result.ok) {
  console.error(result.message || result.error);
  process.exit(1);
}

console.log("Preview summary:", result.summary);
console.log("Duplicates:", result.duplicates.length);
for (const row of result.preview) {
  const flag = row.valid ? (row.duplicateStatus === "possible_duplicate" ? "DUP" : "OK") : "ERR";
  console.log(flag, row.name, "—", row.pointType, row.warnings?.length ? "(" + row.warnings.join("; ") + ")" : "");
  if (row.errors?.length) console.log("  errors:", row.errors.join("; "));
}

if (DRY) console.log("\nDry run — pass --apply on commit script to write records.");
