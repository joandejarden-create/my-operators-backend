#!/usr/bin/env node
/**
 * Phase 3A.4 — Reprocess stored evidence into Recommendation Rate + Top-3 snapshots.
 * No provider calls. Does not modify raw evidence/responses.
 *
 * Usage:
 *   node scripts/ai-visibility-reprocess-recommendation-metrics.mjs --dry-run
 *   node scripts/ai-visibility-reprocess-recommendation-metrics.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createAiVisibilityStore } from "../lib/ai-visibility/storage/index.js";
import { resolveAiVisibilityStoreRoot } from "../lib/ai-visibility/storage/resolve-store-root.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import {
  computeRecommendationRate,
  computeTop3RecommendationRate,
  computeFirstRecommendationRate,
  computeRecommendationShare,
  METRIC_VERSION,
} from "../lib/ai-visibility/metrics.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const apply = process.argv.includes("--apply");
const dryRun = !apply || process.argv.includes("--dry-run");

async function main() {
  const resolved = resolveAiVisibilityStoreRoot({});
  const store = createAiVisibilityStore({ rootDir: resolved.rootDir });
  const summaries = (await store.listBatchSummaries({})) || [];
  const completed = summaries.filter(
    (s) => s.status === "completed" || s.status === "partial"
  );

  let batches = 0;
  let snapshotsWritten = 0;
  let errors = [];
  const report = {
    mode: dryRun && !apply ? "dry-run" : "apply",
    storeRoot: store.rootDir,
    metricVersion: METRIC_VERSION,
    NEW_PROVIDER_CALLS: 0,
    RAW_RESPONSES_MODIFIED: false,
    batches: [],
  };

  for (const summary of completed) {
    batches += 1;
    try {
      const { observations } = await loadObservationsFromBatchSummary(store, summary);
      const entityIds = [
        ...new Set(
          observations.flatMap((o) => [
            ...(o.presentEntityIds || []),
            ...(o.recommendedEntityIds || []),
            ...(o.top3RecommendedEntityIds || []),
          ])
        ),
      ];
      const batchReport = {
        batchId: summary.batchId,
        observationCount: observations.length,
        entityCount: entityIds.length,
        snapshots: [],
      };

      for (const entityId of entityIds) {
        const rec = computeRecommendationRate(observations, entityId);
        const top3 = computeTop3RecommendationRate(observations, entityId);
        const first = computeFirstRecommendationRate(observations, entityId);
        const share = computeRecommendationShare(observations, entityId);
        for (const detail of [rec, top3, first, share]) {
          const snap = {
            batchId: summary.batchId,
            batchDate: summary.completedAt || summary.startedAt || summary.savedAt,
            entityId,
            metric: detail.metric,
            value: detail.value,
            numerator: detail.numerator ?? detail.count ?? null,
            denominator: detail.denominator ?? null,
            geographyScope: summary.cohort?.geographyScope || null,
            commercialRegion: summary.cohort?.commercialRegion || null,
            country: summary.cohort?.country || null,
            provider: summary.provider?.name || summary.provider || "openai",
            metricVersion: METRIC_VERSION,
            reprocessedAt: new Date().toISOString(),
            reprocessSource: "phase3a4_stored_evidence",
          };
          batchReport.snapshots.push({
            entityId,
            metric: detail.metric,
            value: detail.value,
          });
          if (apply) {
            await store.saveMetricSnapshot(snap);
            snapshotsWritten += 1;
          }
        }
      }
      report.batches.push(batchReport);
    } catch (err) {
      errors.push({ batchId: summary.batchId, error: err.message });
    }
  }

  report.BATCHES_REPROCESSED = batches;
  report.DERIVED_SNAPSHOTS_UPDATED = apply ? snapshotsWritten : 0;
  report.DERIVED_SNAPSHOTS_PREVIEW = dryRun && !apply ? "see batches[].snapshots" : undefined;
  report.REPROCESS_ERRORS = errors;
  report.LIVE_PROVIDER_CALLS = 0;

  const outPath = path.join(
    __dirname,
    "..",
    "data",
    "ai-visibility",
    "phase3a4-recommendation-reprocess-report.json"
  );
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        BATCHES_REPROCESSED: report.BATCHES_REPROCESSED,
        DERIVED_SNAPSHOTS_UPDATED: report.DERIVED_SNAPSHOTS_UPDATED,
        REPROCESS_ERRORS: errors.length,
        reportPath: outPath,
        NEW_PROVIDER_CALLS: 0,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
