#!/usr/bin/env node
/**
 * CLI commit for Demand Anchors import.
 *   node scripts/import-demand-anchors-commit.mjs --file fixtures/demand-anchors-cartagena.json --apply
 *   node scripts/import-demand-anchors-commit.mjs --file fixtures/demand-anchors-cartagena.json --apply --limit 5
 */
import "../load-env.js";
import { readFileSync } from "fs";
import {
  previewDemandAnchorsImport,
  commitDemandAnchorsImport,
} from "../lib/demand-anchors/import-commit.js";
import { parseRequireVerifiedFixtureFlag } from "../lib/location-verification/verified-fixture-gating.js";

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const fileIdx = args.indexOf("--file");
const file = fileIdx >= 0 ? args[fileIdx + 1] : null;
const skipDup = !args.includes("--allow-duplicates");
const limitIdx = args.indexOf("--limit");
const limit = limitIdx >= 0 ? Number(args[limitIdx + 1]) : null;
const requireVerifiedFile = parseRequireVerifiedFixtureFlag(args);

if (!file) {
  console.error("Usage: node scripts/import-demand-anchors-commit.mjs --file <path> [--apply] [--limit N] [--allow-duplicates]");
  process.exit(1);
}

const payload = JSON.parse(readFileSync(file, "utf8"));
const body = {
  market: payload.market || "",
  country: payload.country || "",
  region: payload.region || "",
  verification: payload.verification || null,
  requireVerifiedFile,
  points: (payload.points || []).slice(0, limit || undefined),
};

const preview = await previewDemandAnchorsImport(body);
if (!preview.ok) {
  console.error(preview.message || preview.error);
  process.exit(1);
}

const records = preview.preview.filter((r) => r.valid);
console.log("Ready to import:", records.length, "valid rows,", preview.duplicates.length, "duplicate flags");

if (!APPLY) {
  console.log("Dry run — re-run with --apply to write to Airtable.");
  process.exit(0);
}

const result = await commitDemandAnchorsImport(records, {
  skipDuplicates: skipDup,
  market: body.market,
  country: body.country,
  region: body.region,
});

console.log("Created:", result.created?.length || 0);
console.log("Skipped:", result.skipped?.length || 0);
console.log("Errors:", result.errors?.length || 0);
if (result.errors?.length) {
  result.errors.forEach((e) => console.error(" ", e.name, e.message));
  process.exit(1);
}
