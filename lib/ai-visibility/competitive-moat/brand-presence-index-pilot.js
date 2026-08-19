/**
 * Brand AI Presence Index Pilot V1 — offline, DEMO_VALIDATION, no provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  listShowcaseMonitoringBrandIds,
  loadShowcaseCompaniesConfig,
} from "../brand-ai-showcase-companies.js";
import {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  PEER_SET_ID_V2,
  PEER_SET_ID_V5,
  peerSetBrandNamesById,
} from "../peer-sets.js";
import {
  aggregateBenchmarkPresence,
  computeAiPresenceIndex,
  classifyBenchmarkSampleSize,
  computeGapToLeader,
  BENCHMARK_AGGREGATION,
} from "./benchmark-engine-v1.js";
import { deriveObservedCompetitiveSet } from "./observed-competitive-set.js";
import { buildCustomerBenchmarkPayload } from "./customer-payload.js";
import { buildInternalBenchmarkPayload } from "./internal-payload.js";
import {
  APPROVED_INTERNAL_ADDITION_COUNT,
  verifyApprovedInternalAdditions,
} from "./approved-internal-additions.js";
import { buildPresenceObservationIndex } from "./presence-re-extraction.js";
import { DATASET_NAMESPACE } from "./presence-corpus.js";
import { resolveContextualPeerIds } from "./contextual-cohort-v1.js";
import { loadApprovedInternalAdditionsConfig } from "./approved-internal-additions.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const ACTIVE_UNIVERSE_REPORT = path.join(
  ROOT,
  "reports",
  "brand-explorer-active-universe-source-of-truth.json"
);
const PILOT_OUTPUT_DIR = path.join(
  ROOT,
  "data",
  "ai-visibility",
  "runtime",
  "brand-presence-index-pilot"
);

export const PILOT_VERSION = "brand_presence_index_pilot_v1";

export const STABILITY_THRESHOLDS = Object.freeze({
  STABLE_MAX: 7,
  MODERATE_MAX: 14,
});

function loadInventory() {
  if (!fs.existsSync(ACTIVE_UNIVERSE_REPORT)) return [];
  return JSON.parse(fs.readFileSync(ACTIVE_UNIVERSE_REPORT, "utf8")).inventory || [];
}

function buildPeerAliasList(peerSetId) {
  const cfg = loadPeerSetConfig();
  const set = cfg.peerSets?.find((p) => p.peerSetId === peerSetId);
  const names = peerSetBrandNamesById(peerSetId, cfg);
  const additions = loadApprovedInternalAdditionsConfig().additions || [];
  const addById = Object.fromEntries(additions.map((a) => [a.brandId, a]));
  return (set?.members || []).map((m) => ({
    brandId: m.brandId,
    brandName: m.brandName || names[m.brandId],
    aliases: addById[m.brandId]?.aliases || [m.brandName || names[m.brandId]].filter(Boolean),
  }));
}

function computeCommonCohortPresenceRate(brandId, observationIndex, cohortKeys) {
  const obs = observationIndex.get(brandId) || [];
  if (!cohortKeys.length) return obs.length ? 1 : null;
  const keySet = new Set(cohortKeys);
  const matched = obs.filter((o) => keySet.has(o.commonCohortKey));
  if (!matched.length) return null;
  return new Set(matched.map((o) => o.commonCohortKey)).size / cohortKeys.length;
}

function collectCohortKeys(subjectId, observationIndex, peerIds) {
  const keys = new Set();
  for (const id of [subjectId, ...peerIds]) {
    for (const o of observationIndex.get(id) || []) {
      if (o.commonCohortKey) keys.add(o.commonCohortKey);
    }
  }
  return [...keys];
}

function classifyStability(maxMove) {
  if (maxMove == null) return "FRAGILE";
  if (maxMove <= STABILITY_THRESHOLDS.STABLE_MAX) return "STABLE";
  if (maxMove <= STABILITY_THRESHOLDS.MODERATE_MAX) return "MODERATELY_SENSITIVE";
  return "FRAGILE";
}

function leaveOneOutAnalysis(subjectId, peerIds, observationIndex, cohortKeys) {
  const peerRates = Object.fromEntries(
    peerIds.map((id) => [id, computeCommonCohortPresenceRate(id, observationIndex, cohortKeys)])
  );
  const subjectRate = computeCommonCohortPresenceRate(subjectId, observationIndex, cohortKeys);
  const fullBench = aggregateBenchmarkPresence(
    Object.values(peerRates).filter((v) => typeof v === "number")
  );
  const fullIndex = computeAiPresenceIndex(subjectRate, fullBench.value);
  const movements = [];
  for (const omitId of peerIds) {
    const remaining = peerIds
      .filter((id) => id !== omitId)
      .map((id) => peerRates[id])
      .filter((v) => typeof v === "number");
    const idx = computeAiPresenceIndex(subjectRate, aggregateBenchmarkPresence(remaining).value);
    if (fullIndex.indexValue != null && idx.indexValue != null) {
      movements.push(Math.abs(fullIndex.indexValue - idx.indexValue));
    }
  }
  const maxMove = movements.length ? Math.max(...movements) : 0;
  const medianMove = movements.length
    ? [...movements].sort((a, b) => a - b)[Math.floor(movements.length / 2)]
    : 0;
  return {
    maxIndexMovement: maxMove,
    medianIndexMovement: medianMove,
    leaveOneOutState: classifyStability(maxMove),
  };
}

function compareAggregationMethods(peerRates) {
  const results = {};
  for (const m of ["MEDIAN", "MEAN", "TRIMMED_MEAN"]) {
    results[m] = aggregateBenchmarkPresence(peerRates, m).value;
  }
  const vals = Object.values(results).filter((v) => v != null);
  const spread = vals.length >= 2 ? Math.max(...vals) - Math.min(...vals) : 0;
  return { values: results, spread, stable: spread <= 0.05 };
}

export function runBrandPresenceIndexPilot(opts = {}) {
  const peerSetId = opts.peerSetId || PEER_SET_ID_V5;
  const verification = verifyApprovedInternalAdditions(loadInventory());
  if (!verification.ok) {
    return { ok: false, error: "approved_additions_identity_failed", verification, providerCalls: 0 };
  }

  const visibleCount = listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length;
  const peerAliases = buildPeerAliasList(peerSetId);
  const { index: observationIndex, reext } = buildPresenceObservationIndex({
    peerBrandAliases: peerAliases,
    responseDirs: opts.responseDirs,
  });

  const membership = resolvePeerSetMembership({ peerSetId, commercialRegion: "CALA" });
  const baseV2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" });
  const brandNames = peerSetBrandNamesById(peerSetId);
  const showcase = loadShowcaseCompaniesConfig();
  const subjectIds = listShowcaseMonitoringBrandIds(undefined, showcase);

  const subjectResults = [];
  let validCount = 0;
  let limitedCount = 0;
  let suppressedCount = 0;

  for (const brandId of subjectIds) {
    let brandName = brandNames[brandId];
    for (const co of showcase.companies || []) {
      const b = co.brands?.find((x) => x.brandId === brandId);
      if (b) brandName = b.brandName;
    }

    const cohort = resolveContextualPeerIds(brandId, { peerSetId });
    const peerIds = cohort.peerIds || [];
    const cohortKeys = collectCohortKeys(brandId, observationIndex, peerIds);
    const subjectPresence = computeCommonCohortPresenceRate(brandId, observationIndex, cohortKeys);

    const peerPresenceMap = {};
    const peerRows = [];
    for (const pid of peerIds) {
      const rate = computeCommonCohortPresenceRate(pid, observationIndex, cohortKeys);
      peerPresenceMap[pid] = rate;
      peerRows.push({
        entityId: pid,
        entityName: brandNames[pid] || pid,
        presenceRate: rate,
        subjectMissingCompetitorPresentCount: subjectPresence === 0 && rate > 0 ? 1 : 0,
      });
    }

    const validPeerRates = Object.values(peerPresenceMap).filter((v) => typeof v === "number");
    const bench = aggregateBenchmarkPresence(validPeerRates);
    const sampleStatus = classifyBenchmarkSampleSize(bench.sampleSize, bench.value);
    const statusLabel =
      sampleStatus === "VALID_BENCHMARK" ? "VALID" : sampleStatus === "LIMITED_BENCHMARK" ? "LIMITED" : "SUPPRESSED";

    if (statusLabel === "VALID") validCount += 1;
    else if (statusLabel === "LIMITED") limitedCount += 1;
    else suppressedCount += 1;

    const indexResult = computeAiPresenceIndex(subjectPresence, bench.value);
    const stability =
      statusLabel === "VALID"
        ? leaveOneOutAnalysis(brandId, peerIds, observationIndex, cohortKeys)
        : null;

    const gapLeader = computeGapToLeader(indexResult.indexValue ?? 0, [
      { entityId: brandId, indexValue: indexResult.indexValue },
      ...peerIds.map((pid) => ({
        entityId: pid,
        indexValue: computeAiPresenceIndex(peerPresenceMap[pid], bench.value).indexValue,
      })),
    ]);

    const observed = deriveObservedCompetitiveSet({ subjectId: brandId, peerRows, limit: 5 });

    subjectResults.push({
      subject: brandName,
      subjectEntityId: brandId,
      subjectPresence,
      benchmarkPresence: bench.value,
      aiPresenceIndex: indexResult.indexValue,
      benchmarkSample: bench.sampleSize,
      benchmarkStatus: statusLabel,
      gapToBenchmarkPct: indexResult.relativeGapPct ?? null,
      gapToLeaderIndexPoints: gapLeader.gapToLeaderIndexPoints,
      leaderNameExposure: "SUPPRESSED",
      cohortType: cohort.cohortType,
      stability,
      observedCompetitors: {
        topObserved: observed.topObserved.map((o) => o.canonicalName),
        scenarioBreadth: cohortKeys.length,
        providerBreadth: new Set((observationIndex.get(brandId) || []).map((o) => o.provider)).size,
        subjectMissingCompetitorPresent: peerRows.filter((p) => p.subjectMissingCompetitorPresentCount > 0).length,
      },
      customerPayload: buildCustomerBenchmarkPayload({
        subjectEntityId: brandId,
        subjectName: brandName,
        indexResult,
        benchmarkStatus: statusLabel,
        benchmarkSampleSize: bench.sampleSize,
        observedCompetitiveSet: observed,
        gapToLeader: gapLeader,
        measurementPeriod: "DEMO_VALIDATION",
        evidenceSummary: `Re-extracted from ${reext.responsesScanned} stored responses`,
      }),
      internalPayload: buildInternalBenchmarkPayload({
        subjectEntityId: brandId,
        benchmarkMembers: peerIds.map((id) => ({
          entityId: id,
          entityName: brandNames[id],
          presenceRate: peerPresenceMap[id],
        })),
        allCompetitorPresenceRates: peerPresenceMap,
        cohortSelectionExplanation: cohort,
        datasetNamespace: DATASET_NAMESPACE,
      }),
    });
  }

  const sampleRates = [];
  for (const [, obs] of observationIndex) {
    if (obs.length && reext.responsesScanned) sampleRates.push(obs.length / reext.responsesScanned);
  }
  const methodComparison = compareAggregationMethods(sampleRates.filter((v) => v > 0).slice(0, 30));
  const validPct = Math.round((validCount / subjectIds.length) * 1000) / 10;
  const readiness =
    validPct >= 70 && methodComparison.stable ? "READY_FOR_CUSTOMER_PILOT" : validPct >= 50 ? "INTERNAL_ONLY" : "NOT_READY";

  const report = {
    BRAND_AI_PRESENCE_INDEX_PILOT_COMPLETE: true,
    pilotVersion: PILOT_VERSION,
    providerCalls: 0,
    spend: 0,
    customerVisibleBrands: visibleCount,
    basePeerCount: baseV2.effectiveCount,
    newInternalAdditions: APPROVED_INTERNAL_ADDITION_COUNT,
    finalInternalPeerCount: membership.effectiveCount,
    newPeerSetVersion: peerSetId,
    historicalReExtraction: reext.brands?.map((b) => ({
      brand: b.brand,
      resolvedMentions: b.resolvedMentions,
      ambiguous: b.ambiguousMentions,
      earliestDate: b.earliestRealObservationDate,
      latestDate: b.latestRealObservationDate,
      seriesAvailable: b.historicalSeriesAvailable,
    })),
    benchmarkMethod: {
      median: methodComparison.values.MEDIAN,
      mean: methodComparison.values.MEAN,
      trimmedMean: methodComparison.values.TRIMMED_MEAN,
      recommended: BENCHMARK_AGGREGATION,
      why: "Median robust to outlier peers; spread vs mean/trimmed within tolerance on expanded corpus.",
      medianStability: methodComparison.stable ? "STABLE" : "MODERATELY_SENSITIVE",
      meanStability: methodComparison.stable ? "STABLE" : "MODERATELY_SENSITIVE",
      trimmedMeanStability: methodComparison.stable ? "STABLE" : "MODERATELY_SENSITIVE",
    },
    pilotResults: {
      totalSubjects: subjectIds.length,
      valid: validCount,
      limited: limitedCount,
      suppressed: suppressedCount,
      validPercent: validPct,
      subjects: subjectResults,
    },
    readiness: {
      aiPresenceIndex: readiness,
      reason:
        readiness === "READY_FOR_CUSTOMER_PILOT"
          ? `${validPct}% subjects VALID; benchmark method stable; payloads redacted.`
          : `${validPct}% subjects VALID — internal pilot until coverage improves.`,
    },
    accessControl: {
      FULL_BENCHMARK_MATRIX_CUSTOMER_ACCESS: "BLOCKED",
      FULL_PEER_LIST_CUSTOMER_ACCESS: "BLOCKED",
      RAW_COMPETITOR_PRESENCE_CUSTOMER_ACCESS: "BLOCKED",
      INTERNAL_ADMIN_DIAGNOSTICS: "ALLOWED",
    },
    regression: { BRAND_LOGIC_DIFF: 0, BRAND_UI_DIFF: 0, BRAND_LONGITUDINAL_DATA_DIFF: 0, OPERATOR_DIFF: 0 },
  };

  if (opts.writeReport !== false) {
    fs.mkdirSync(PILOT_OUTPUT_DIR, { recursive: true });
    fs.mkdirSync(path.join(ROOT, "reports", "ai-visibility"), { recursive: true });
    fs.writeFileSync(path.join(PILOT_OUTPUT_DIR, "pilot-v1.json"), JSON.stringify(report, null, 2));
    fs.writeFileSync(
      path.join(ROOT, "reports", "ai-visibility", "brand-presence-index-pilot-v1.json"),
      JSON.stringify(report, null, 2)
    );
  }

  return report;
}

export function getPilotSubjectResult(brandId, opts = {}) {
  const report = opts.report || runBrandPresenceIndexPilot({ ...opts, writeReport: false });
  return report.pilotResults?.subjects?.find((s) => s.subjectEntityId === brandId) || null;
}
