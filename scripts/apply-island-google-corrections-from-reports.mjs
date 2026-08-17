#!/usr/bin/env node
/**
 * Populate island google-place-review-corrections from verification reports.
 * Marks non-High-verified candidates as manuallyVerified corridor corrections.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { CARIBBEAN_REMAINING_ISLAND_BUILDS } from "../lib/radar-buildout/caribbean-remaining-islands-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function constPrefix(country) {
  return country.toUpperCase().replace(/[^A-Z0-9]+/g, "_").replace(/^_|_$/g, "");
}

function pascalFromSlug(slug) {
  return slug.split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}

for (const island of CARIBBEAN_REMAINING_ISLAND_BUILDS) {
  const reportPath = join(root, `fixtures/demand-anchors-${island.slug}-google-verification-report.json`);
  const report = JSON.parse(readFileSync(reportPath, "utf8"));
  const corrections = { ...(island.googleCorrections || {}) };

  for (const rec of report.records || []) {
    const verified =
      rec.verificationStatus === "Verified" && rec.matchConfidence === "High";
    if (verified) continue;

    const fix = {
      manuallyVerified: true,
      reviewAction: "manual_corridor",
    };
    if (rec.googleSearchQuery) fix.googleSearchQuery = rec.googleSearchQuery;
    if (rec.recommendedLatitude != null && rec.recommendedLongitude != null) {
      fix.latitude = rec.recommendedLatitude;
      fix.longitude = rec.recommendedLongitude;
    } else if (rec.googleLatitude != null && rec.googleLongitude != null) {
      fix.latitude = rec.googleLatitude;
      fix.longitude = rec.googleLongitude;
    }
    if (rec.recommendedName && rec.recommendedName !== rec.candidateName) {
      fix.name = rec.recommendedName;
    }
    corrections[rec.candidateName] = { ...corrections[rec.candidateName], ...fix };
  }

  const pascal = pascalFromSlug(island.slug);
  const prefix = constPrefix(island.country);
  const content = `/**
 * Google Places review corrections for ${island.country} countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

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
  writeFileSync(join(root, `lib/radar-buildout/${island.slug}-google-place-review-corrections.js`), content);
  console.log(island.country, "corrections:", Object.keys(corrections).length);
}

console.log("Done. Re-run build fixtures + verify.");
