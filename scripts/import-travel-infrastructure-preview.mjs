#!/usr/bin/env node
import "../load-env.js";
import { readFileSync } from "fs";
import { previewTravelInfrastructureImport, commitTravelInfrastructureImport } from "../lib/travel-infrastructure/import-commit.js";

import { parseRequireVerifiedFixtureFlag } from "../lib/location-verification/verified-fixture-gating.js";

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const fileIdx = args.indexOf("--file");
const file = fileIdx >= 0 ? args[fileIdx + 1] : null;
const requireVerifiedFile = parseRequireVerifiedFixtureFlag(args);
if (!file) {
  console.error("Usage: node scripts/import-travel-infrastructure-preview.mjs --file <path> [--require-verified-fixture]");
  process.exit(1);
}

const payload = JSON.parse(readFileSync(file, "utf8"));
const body = {
  market: payload.market || "",
  country: payload.country || "",
  region: payload.region || "",
  verification: payload.verification || null,
  requireVerifiedFile,
  points: payload.points || [],
};

const result = await previewTravelInfrastructureImport(body);
if (!result.ok) {
  console.error(result.message || result.error);
  process.exit(1);
}

console.log("Preview summary:", result.summary);
console.log("Duplicates:", result.duplicates?.length || 0);
for (const row of result.preview) {
  const flag = row.valid ? (row.duplicateStatus === "possible_duplicate" ? "DUP" : "OK") : "ERR";
  console.log(flag, row.name, "—", row.pointType);
  if (row.errors?.length) console.log("  errors:", row.errors.join("; "));
}

if (!APPLY) {
  console.log("\nDry run — use import-travel-infrastructure-commit.mjs --apply to write.");
}
