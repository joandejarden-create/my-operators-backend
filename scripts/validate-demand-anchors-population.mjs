#!/usr/bin/env node
/**
 * Validate Demand Anchors population state across markets.
 *   node scripts/validate-demand-anchors-population.mjs
 */
import "../load-env.js";
import { readFileSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { previewDemandAnchorsImport } from "../lib/demand-anchors/import-commit.js";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import { POINT_TYPES } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturesDir = join(__dirname, "../fixtures");

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

const fixtureFiles = readdirSync(fixturesDir).filter((f) => {
  if (!f.startsWith("demand-anchors-") || !f.endsWith(".json")) return false;
  if (f.includes("google-verification-report")) return false;
  if (f.includes("google-review-pass-summary")) return false;
  if (f.includes("-review-pass-summary")) return false;
  if (f.includes("-candidates") && !f.includes("-real")) return false;
  return true;
});

for (const file of fixtureFiles) {
  const payload = JSON.parse(readFileSync(join(fixturesDir, file), "utf8"));
  if (!Array.isArray(payload.points) || payload.points.length === 0) {
    console.log("skip:", file, "(no points array)");
    continue;
  }
  const preview = await previewDemandAnchorsImport({
    market: payload.market,
    country: payload.country,
    region: payload.region,
    points: payload.points,
  });
  assert(preview.ok, file + " preview ok");
  assert(preview.summary.valid === payload.points.length, file + " all points valid in preview");
  assert(preview.preview.length === payload.points.length, file + " preview row count");
}

const live = await fetchDemandAnchorRecords({ includeHidden: true });
if (live.error) {
  console.warn("WARN: could not fetch live anchors:", live.error);
} else {
  const count = live.allPoints?.length || live.points?.length || 0;
  assert(count >= 12, "live Demand Anchors count >= 12 (got " + count + ")");
  const types = new Set((live.allPoints || live.points || []).map((p) => p.pointType));
  assert(types.size >= 1, "live records have point types");
  console.log("Live anchors:", count, "types represented:", types.size, "/", POINT_TYPES.length);
}

const tiFixture = JSON.parse(
  readFileSync(join(fixturesDir, "travel-infrastructure-additional-types-sample.json"), "utf8")
);
const { previewTravelInfrastructureImport } = await import("../lib/travel-infrastructure/import-commit.js");
const tiPreview = await previewTravelInfrastructureImport({
  market: tiFixture.market,
  country: tiFixture.country,
  region: tiFixture.region,
  points: tiFixture.points,
});
assert(tiPreview.ok, "travel infra additional types fixture preview ok");
assert(tiPreview.summary.valid === tiFixture.points.length, "travel infra fixture all valid");

if (failed) process.exit(1);
console.log("\nDemand anchors population validation passed.");
