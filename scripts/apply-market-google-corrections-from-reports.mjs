#!/usr/bin/env node
/**
 * Apply Google review corrections from verification reports for market builds.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { ALL_MARKET_BUILD_SPECS } from "../lib/radar-buildout/tier1-territories-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function pascalFromSlug(slug) {
  return slug.split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}

function slugConst(slug) {
  return slug.toUpperCase().replace(/-/g, "_");
}

for (const spec of ALL_MARKET_BUILD_SPECS) {
  const reportPath = join(root, `fixtures/demand-anchors-${spec.slug}-google-verification-report.json`);
  let report;
  try {
    report = JSON.parse(readFileSync(reportPath, "utf8"));
  } catch {
    console.log("Skip (no report):", spec.slug);
    continue;
  }
  const corrections = { ...(spec.googleCorrections || {}) };
  for (const rec of report.records || []) {
    const verified = rec.verificationStatus === "Verified" && rec.matchConfidence === "High";
    if (verified) continue;
    const fix = { manuallyVerified: true, reviewAction: "manual_corridor" };
    if (rec.recommendedLatitude != null) {
      fix.latitude = rec.recommendedLatitude;
      fix.longitude = rec.recommendedLongitude;
    } else if (rec.googleLatitude != null) {
      fix.latitude = rec.googleLatitude;
      fix.longitude = rec.googleLongitude;
    }
    corrections[rec.candidateName] = { ...corrections[rec.candidateName], ...fix };
  }
  const pascal = pascalFromSlug(spec.slug);
  const prefix = slugConst(spec.slug);
  const content = `/**
 * Google Places review corrections for ${spec.market} candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const ${prefix}_GOOGLE_PLACE_REVIEW_CORRECTIONS = ${JSON.stringify(corrections, null, 2)};

export function apply${pascal}PlaceReviewCorrection(point) {
  const fix = ${prefix}_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = \`\${point.notes || ""} \${REVIEW_TAG}\`.trim();
  return merged;
}

export function apply${pascal}PlaceReviewCorrections(points) {
  return points.map(apply${pascal}PlaceReviewCorrection);
}
`;
  writeFileSync(join(root, `lib/radar-buildout/${spec.slug}-google-place-review-corrections.js`), content);
  console.log(spec.slug, "corrections:", Object.keys(corrections).length);
}
