#!/usr/bin/env node
/** Apply google corrections from report for tier-1 countrywide fixtures. */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const JOBS = [
  {
    country: "Colombia",
    report: "fixtures/demand-anchors-colombia-google-verification-report.json",
    correctionsFile: "lib/radar-buildout/colombia-google-place-review-corrections.js",
    exportName: "COLOMBIA_GOOGLE_PLACE_REVIEW_CORRECTIONS",
    applyFn: "applyColombiaPlaceReviewCorrection",
    applyPlural: "applyColombiaPlaceReviewCorrections",
  },
  {
    country: "Panama",
    report: "fixtures/demand-anchors-panama-google-verification-report.json",
    correctionsFile: "lib/radar-buildout/panama-google-place-review-corrections.js",
    exportName: "PANAMA_GOOGLE_PLACE_REVIEW_CORRECTIONS",
    applyFn: "applyPanamaPlaceReviewCorrection",
    applyPlural: "applyPanamaPlaceReviewCorrections",
  },
  {
    country: "Costa Rica",
    report: "fixtures/demand-anchors-costa-rica-google-verification-report.json",
    correctionsFile: "lib/radar-buildout/costa-rica-google-place-review-corrections.js",
    exportName: "COSTA_RICA_GOOGLE_PLACE_REVIEW_CORRECTIONS",
    applyFn: "applyCostaRicaPlaceReviewCorrection",
    applyPlural: "applyCostaRicaPlaceReviewCorrections",
  },
];

const REVIEW_TAG = "[Google review correction applied]";

for (const job of JOBS) {
  let report;
  try {
    report = JSON.parse(readFileSync(join(root, job.report), "utf8"));
  } catch {
    console.log("Skip (no report):", job.country);
    continue;
  }
  let existing = {};
  try {
    const mod = await import(`../${job.correctionsFile}`);
    existing = mod[job.exportName] || {};
  } catch {
    existing = {};
  }
  const corrections = { ...existing };
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
  const content = `/**
 * Google Places review corrections for ${job.country} candidates.
 */
const REVIEW_TAG = "${REVIEW_TAG}";

/** @type {Record<string, object>} */
export const ${job.exportName} = ${JSON.stringify(corrections, null, 2)};

export function ${job.applyFn}(point) {
  const fix = ${job.exportName}[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = \`\${point.notes || ""} \${REVIEW_TAG}\`.trim();
  return merged;
}

export function ${job.applyPlural}(points) {
  return points.map(${job.applyFn});
}
`;
  writeFileSync(join(root, job.correctionsFile), content);
  console.log(job.country, "corrections:", Object.keys(corrections).length);
}
