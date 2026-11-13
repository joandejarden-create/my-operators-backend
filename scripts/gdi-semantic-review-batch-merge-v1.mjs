#!/usr/bin/env node
/**
 * Merge Cursor-agent batch review JSON files into the semantic audit deliverables
 * and recompute the scorecard. Does not call paid APIs.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  computeScorecard,
  writeJson,
  defaultReportDir,
  GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
} from "../lib/group-demand-intelligence/customer-consistency/gdi-full-semantic-commercial-audit-v1.js";

const OUT = defaultReportDir();
const BATCH_DIR = path.join(OUT, "08-review-batches");

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

const packs = loadJson(path.join(OUT, "07-rendered-packs", "all-packs.json"));
const existingReviews = loadJson(path.join(OUT, "01-semantic-opportunity-reviews.json"));
const byKey = new Map(
  existingReviews.map((r) => [`${r.hotelId}::${r.opportunityId}`, r])
);

const batchFiles = fs
  .readdirSync(BATCH_DIR)
  .filter((f) => /-reviews\.json$/i.test(f))
  .sort();

let merged = 0;
for (const f of batchFiles) {
  const doc = loadJson(path.join(BATCH_DIR, f));
  const rows = Array.isArray(doc) ? doc : doc.reviews || doc.opportunities || [];
  for (const r of rows) {
    if (!r?.opportunityId || !r?.hotelId) continue;
    if (r.reviewStatus !== "REVIEWED") continue;
    byKey.set(`${r.hotelId}::${r.opportunityId}`, {
      ...r,
      reviewStatus: "REVIEWED",
      reviewEngine: r.reviewEngine || `cursor-agent-batch:${f}`,
    });
    merged += 1;
  }
}

const reviews = [...byKey.values()];
const researchRequired = loadJson(path.join(OUT, "04-research-required.json"));
const scorecard = computeScorecard({
  packs,
  reviews,
  researchRequiredCount: researchRequired.filter((r) =>
    String(r.kind || "").includes("CLASSIFICATION")
  ).length,
  technicalConsistencyScore: 100,
});

const contradictions = [];
for (const r of reviews) {
  for (const c of r.contradictions || []) {
    contradictions.push({
      hotelId: r.hotelId,
      opportunityId: r.opportunityId,
      displayTitle: r.displayTitle,
      ...c,
    });
  }
}
for (const pack of packs) {
  for (const f of pack.crossSectionFindings || []) {
    if (
      !contradictions.some(
        (c) =>
          c.hotelId === pack.hotelId &&
          c.opportunityId === pack.opportunityId &&
          c.code === f.code
      )
    ) {
      contradictions.push({
        hotelId: pack.hotelId,
        opportunityId: pack.opportunityId,
        displayTitle: pack.displayTitle,
        ...f,
      });
    }
  }
}

const corrections = [];
for (const r of reviews) {
  for (const s of r.suggestedCorrections || []) {
    corrections.push({
      hotelId: r.hotelId,
      opportunityId: r.opportunityId,
      ...s,
    });
  }
}

const coverage = loadJson(path.join(OUT, "06-coverage-manifest.json"));
coverage.generatedAt = new Date().toISOString();
coverage.totals.reviewed = reviews.filter((r) => r.reviewStatus === "REVIEWED").length;
coverage.totals.heuristicAssist = reviews.filter(
  (r) => r.reviewStatus === "HEURISTIC_ASSIST_ONLY"
).length;
coverage.opportunities = coverage.opportunities.map((o) => {
  const r = byKey.get(`${o.hotelId}::${o.opportunityId}`);
  return {
    ...o,
    reviewStatus: r?.reviewStatus || o.reviewStatus,
    reviewEngine: r?.reviewEngine || o.reviewEngine,
  };
});

writeJson(path.join(OUT, "01-semantic-opportunity-reviews.json"), reviews);
writeJson(path.join(OUT, "02-contradictions.json"), contradictions);
writeJson(path.join(OUT, "03-content-corrections.json"), corrections);
writeJson(path.join(OUT, "05-quality-scorecard.json"), scorecard);
writeJson(path.join(OUT, "06-coverage-manifest.json"), coverage);

console.log(
  JSON.stringify(
    {
      schema: GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
      batchFiles: batchFiles.length,
      mergedReviewedRows: merged,
      totals: coverage.totals,
      overall: scorecard.overall,
      overallStatus: scorecard.overallStatus,
    },
    null,
    2
  )
);
