#!/usr/bin/env node
import "../load-env.js";
import { readFileSync } from "fs";
import { previewTravelInfrastructureImport, commitTravelInfrastructureImport } from "../lib/travel-infrastructure/import-commit.js";
import { filterCommitRecords } from "../lib/demand-anchors/import-validation.js";
import { parseRequireVerifiedFixtureFlag } from "../lib/location-verification/verified-fixture-gating.js";

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const fileIdx = args.indexOf("--file");
const file = fileIdx >= 0 ? args[fileIdx + 1] : null;
const requireVerifiedFile = parseRequireVerifiedFixtureFlag(args);
if (!file) {
  console.error("Usage: node scripts/import-travel-infrastructure-commit.mjs --file <path> [--apply] [--require-verified-fixture]");
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

const preview = await previewTravelInfrastructureImport(body);
if (!preview.ok) {
  console.error(preview.message || preview.error);
  process.exit(1);
}

const rows = filterCommitRecords(preview.preview, true);
console.log("Ready to import:", rows.length, "valid rows,", preview.summary.duplicates, "duplicate flags");

if (!APPLY) {
  console.log("Dry run — pass --apply to write records.");
  process.exit(0);
}

const result = await commitTravelInfrastructureImport(rows, {
  market: body.market,
  country: body.country,
  region: body.region,
  skipDuplicates: true,
});

console.log("Created:", result.summary?.created || result.created?.length || 0);
console.log("Skipped:", result.summary?.skipped || result.skipped?.length || 0);
console.log("Errors:", result.summary?.failed || result.errors?.length || 0);
if (result.errors?.length) {
  for (const e of result.errors) console.error(e);
  process.exit(1);
}
