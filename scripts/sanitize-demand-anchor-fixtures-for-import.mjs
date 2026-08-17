#!/usr/bin/env node
/**
 * Strip Google QA metadata from demand-anchor fixture points (real + micro-pass).
 *   node scripts/sanitize-demand-anchor-fixtures-for-import.mjs
 *   node scripts/sanitize-demand-anchor-fixtures-for-import.mjs --file fixtures/demand-anchors-colombia-countrywide-real.json
 */
import { readFileSync, writeFileSync, readdirSync } from "fs";
import { join } from "path";
import {
  pointHasGoogleMetadataFields,
  stripGoogleMetadataFromPoints,
} from "../lib/location-verification/verified-fixture-gating.js";

const args = process.argv.slice(2);
const fileIdx = args.indexOf("--file");
const explicitFiles =
  fileIdx >= 0
    ? [args[fileIdx + 1]]
    : readdirSync("fixtures")
        .filter((f) => /^demand-anchors-.+-(real|micro-pass)\.json$/.test(f))
        .map((f) => join("fixtures", f));

let changedFiles = 0;
let strippedPoints = 0;

for (const rel of explicitFiles) {
  const payload = JSON.parse(readFileSync(rel, "utf8"));
  const points = Array.isArray(payload.points) ? payload.points : [];
  const dirty = points.filter((p) => pointHasGoogleMetadataFields(p) || Object.hasOwn(p, "manuallyVerified"));
  if (!dirty.length) continue;

  payload.points = stripGoogleMetadataFromPoints(points);
  writeFileSync(rel, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  changedFiles += 1;
  strippedPoints += dirty.length;
  console.log(`${rel}: stripped ${dirty.length} point(s)`);
}

console.log(`Done. ${changedFiles} file(s) updated, ${strippedPoints} point(s) cleaned.`);
