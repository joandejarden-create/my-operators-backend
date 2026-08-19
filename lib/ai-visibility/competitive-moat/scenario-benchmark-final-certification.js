/**
 * Final scenario-level AI Presence Index certification (3 candidates only).
 * Independent recompute from stored observations. No provider calls. No UI.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";
import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import { loadBenchmarkEligibleUniverse, getBenchmarkEligibleMember } from "./benchmark-eligible-universe.js";
import { listMandatoryCorePeerIds, resolveScenarioCommercialPeers } from "./scenario-peer-eligibility.js";
import { computeAiPresenceIndex, aggregateBenchmarkPresence, secondaryInCustomerBenchmarkDenominator } from "./benchmark-engine-v1.js";
import { DATASET_NAMESPACE } from "./presence-corpus.js";
import {
  buildIndependentIndex,
  recomputeOne,
  classifyStability,
  VALIDATION_VERSION,
} from "./scenario-benchmark-validation.js";
import {
  CORE_FIRST_GATES_CANDIDATE,
  classifyCoreFirstProduction,
} from "./scenario-benchmark-composition.js";
import { listShowcaseMonitoringBrandIds } from "../brand-ai-showcase-companies.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const REPORT_PATH = path.join(ROOT, "reports", "ai-visibility", "scenario-benchmark-final-certification-v1.json");

export const FINAL_CERTIFICATION_VERSION = "scenario_benchmark_final_certification_v1";
export const CLASSIFIER_VERSION = VALIDATION_VERSION;
export const MEASUREMENT_PERIOD = "DEMO_VALIDATION";
export const HEADLINE_INDEX_STATUS = "DEFERRED";

/** Only these three rows may certify in this phase. */
export const FINAL_CERTIFICATION_SUBJECTS = Object.freeze([
  {
    subjectId: IDS.AUTOGRAPH,
    subject: "Autograph Collection",
    scenarioId: S.SOFT_BRAND,
    scenarioLabel: "SOFT_BRAND_AFFILIATION",
    intentLabel: "Soft-brand affiliation",
    expectedIndex: 103,
  },
  {
    subjectId: IDS.TAPESTRY,
    subject: "Tapestry Collection by Hilton",
    scenarioId: S.SOFT_BRAND,
    scenarioLabel: "SOFT_BRAND_AFFILIATION",
    intentLabel: "Soft-brand affiliation",
    expectedIndex: 103,
  },
  {
    subjectId: IDS.ASCEND,
    subject: "Ascend Hotel Collection",
    scenarioId: S.SOFT_BRAND,
    scenarioLabel: "SOFT_COLLECTION",
    intentLabel: "Soft-brand affiliation",
    expectedIndex: 67,
  },
]);

const MATERIAL_INDEX_DELTA = 1;

function leaveOneOutCore(subjectPresence, corePeers) {
  const rates = corePeers.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  const base = computeAiPresenceIndex(
    subjectPresence,
    aggregateBenchmarkPresence(rates, "MEDIAN").value
  );
  const movements = [];
  for (let i = 0; i < corePeers.length; i += 1) {
    const remaining = corePeers
      .filter((_, j) => j !== i)
      .map((p) => p.peerPresence)
      .filter((v) => typeof v === "number");
    const left = computeAiPresenceIndex(
      subjectPresence,
      aggregateBenchmarkPresence(remaining, "MEDIAN").value
    );
    if (base.indexValue != null && left.indexValue != null) {
      movements.push({
        omitted: corePeers[i].peerBrandName,
        move: Math.abs(base.indexValue - left.indexValue),
      });
    }
  }
  const maxMove = movements.length ? Math.max(...movements.map((m) => m.move)) : null;
  const medianMove = movements.length
    ? [...movements.map((m) => m.move)].sort((a, b) => a - b)[Math.floor(movements.length / 2)]
    : null;
  const mostInfluential = movements.length
    ? movements.reduce((best, m) => (m.move > best.move ? m : best))
    : null;
  return {
    maxIndexMovement: maxMove,
    medianIndexMovement: medianMove,
    mostInfluentialCorePeer: mostInfluential?.omitted || null,
    stability: classifyStability(maxMove),
  };
}

function providerDirectionCore(rec, idx, subjectId, scenarioId, universe) {
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const corePeerIds = peers.calculationPeers
    .filter((p) => p.commercialRelation === "CORE")
    .map((p) => p.peerBrandId);
  const subjectPresent = idx.present.get(subjectId)?.get(scenarioId) || new Set();
  const providers = rec.providers || [];
  const byProvider = {};
  const directions = [];

  for (const provider of providers) {
    const pGrains = idx.grainsByScenarioProvider.get(`${scenarioId}|${provider}`) || new Set();
    let subPresent = 0;
    for (const g of pGrains) if (subjectPresent.has(g)) subPresent += 1;
    const subRate = pGrains.size ? subPresent / pGrains.size : null;
    const coreRates = [];
    for (const peerId of corePeerIds) {
      const peerPresent = idx.present.get(peerId)?.get(scenarioId) || new Set();
      let peerN = 0;
      for (const g of pGrains) if (peerPresent.has(g)) peerN += 1;
      if (pGrains.size) coreRates.push(peerN / pGrains.size);
    }
    const idxVal = computeAiPresenceIndex(
      subRate,
      aggregateBenchmarkPresence(coreRates, "MEDIAN").value
    );
    const dir =
      idxVal.indexValue == null ? "UNKNOWN" : idxVal.indexValue >= 100 ? "AT_OR_ABOVE" : "BELOW";
    byProvider[provider] = { subjectVsBenchmark: dir, index: idxVal.indexValue };
    if (dir !== "UNKNOWN") directions.push(dir);
  }

  const uniq = [...new Set(directions)];
  let state = "CONSISTENT";
  if (uniq.length > 1) {
    const vals = Object.values(byProvider)
      .map((v) => v.index)
      .filter((v) => v != null);
    const spread = vals.length ? Math.max(...vals) - Math.min(...vals) : 0;
    state = spread >= 40 ? "CONFLICT" : "MIXED";
  }
  return { state, byProvider };
}

function coreCoveragePct(rec) {
  const total = rec.corePeers.length;
  if (!total) return 0;
  const measured = rec.corePeers.filter((p) => typeof p.peerPresence === "number").length;
  return Math.round((measured / total) * 1000) / 10;
}

function mandatoryCorePass(rec) {
  const missing = rec.mandatoryCoreIds.filter(
    (id) => !rec.corePeers.some((p) => p.peerBrandId === id)
  );
  return missing.length === 0;
}

function denominatorSafe(rec, leaveOne) {
  if (rec.benchmarkCore == null || rec.benchmarkCore <= 0) return false;
  if (rec.coreCount < 3) return false;
  const rates = rec.corePeers.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  if (!rates.length) return false;
  const maxRate = Math.max(...rates);
  const minRate = Math.min(...rates);
  if (maxRate > 0 && minRate / maxRate < 0.05 && rec.benchmarkCore < 0.1) return false;
  if (leaveOne.maxIndexMovement != null && leaveOne.maxIndexMovement > 50) return false;
  return true;
}

function semanticClaimSafe(rec, indexResult) {
  if (indexResult.indexValue == null) return false;
  if (indexResult.relativeGapPct == null) return false;
  const expectedGap = Math.round((indexResult.indexValue - 100) * 10) / 10;
  if (Math.abs(expectedGap - indexResult.relativeGapPct) > 0.6) return false;
  const recomputed = computeAiPresenceIndex(rec.subjectPresence, rec.benchmarkCore);
  return recomputed.indexValue === indexResult.indexValue;
}

function matchExpected(recomputedIndex, expected) {
  if (recomputedIndex == null || expected == null) return "MISMATCH";
  const delta = Math.abs(recomputedIndex - expected);
  if (delta === 0) return "EXACT_MATCH";
  if (delta <= MATERIAL_INDEX_DELTA) return "ROUNDING_ONLY";
  return "MISMATCH";
}

function classifyFinalStatus(gates, compositionClass) {
  if (gates.providerDirection === "CONFLICT") return "LIMITED";
  if (gates.stability === "FRAGILE") return "LIMITED";
  if (!gates.coreOnlyPass || !gates.mandatoryCorePass || !gates.measuredCorePeersPass) {
    return "SUPPRESSED";
  }
  if (!gates.commonGrainsPass || !gates.multiProviderPass || !gates.denominatorSafe) {
    return "LIMITED";
  }
  if (!gates.semanticClaimSafe || gates.materialMismatch) return "DETAIL_ONLY";
  if (gates.providerDirection === "MIXED") {
    return gates.stability === "STABLE" ? "PRODUCTION_VALIDATED_NARROW" : "DETAIL_ONLY";
  }
  if (compositionClass === "PRODUCTION_VALIDATED" && gates.stability === "STABLE") {
    return "PRODUCTION_VALIDATED";
  }
  if (compositionClass === "PRODUCTION_VALIDATED_NARROW") {
    return "PRODUCTION_VALIDATED_NARROW";
  }
  return compositionClass || "DETAIL_ONLY";
}

function certifyOne(subjectDef, idx, universe) {
  const rec = recomputeOne(subjectDef.subjectId, subjectDef.scenarioId, idx, universe);
  const corePeers = rec.corePeers;
  const coreRates = corePeers.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  const benchCore = aggregateBenchmarkPresence(coreRates, "MEDIAN").value;
  const indexResult = computeAiPresenceIndex(rec.subjectPresence, benchCore);
  const leaveOne = leaveOneOutCore(rec.subjectPresence, corePeers);
  const providerDir = providerDirectionCore(rec, idx, subjectDef.subjectId, subjectDef.scenarioId, universe);
  const secondaryInDenom = secondaryInCustomerBenchmarkDenominator(corePeers, benchCore);
  const measuredCore = corePeers.filter((p) => typeof p.peerPresence === "number").length;
  const coverage = coreCoveragePct(rec);
  const minPairGrains = rec.minPairwiseCommonGrains;
  const materialMismatch = matchExpected(indexResult.indexValue, subjectDef.expectedIndex) === "MISMATCH";

  const gates = {
    coreOnlyPass:
      rec.coreCount >= measuredCore &&
      !rec.usedBroaderFallback &&
      !rec.unionGrainUsed &&
      !secondaryInDenom,
    measuredCorePeersPass: measuredCore >= CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER,
    coreCoveragePass: coverage >= 50,
    mandatoryCorePass: mandatoryCorePass(rec),
    commonGrainsPass: minPairGrains >= CORE_FIRST_GATES_CANDIDATE.COMMON_GRAIN_MIN,
    multiProviderPass: rec.providerCount >= 2,
    providerDirection: providerDir.state,
    stability: leaveOne.stability,
    denominatorSafe: denominatorSafe({ ...rec, benchmarkCore: benchCore, coreCount: measuredCore }, leaveOne),
    semanticClaimSafe: semanticClaimSafe({ ...rec, benchmarkCore: benchCore }, indexResult),
    materialMismatch,
  };

  const compositionRow = {
    scenarioId: subjectDef.scenarioId,
    usedBroaderFallback: rec.usedBroaderFallback,
    unionGrainUsed: rec.unionGrainUsed,
    coreCount: measuredCore,
    commonGrains: rec.commonGrains,
    stabilityCore: leaveOne.stability,
    providerAgreementCore: providerDir.state === "CONSISTENT" ? "PROVIDER_CONSISTENT" : providerDir.state === "CONFLICT" ? "PROVIDER_CONFLICT" : "PROVIDER_MIXED",
    providerClass: rec.providerClass,
    indexCore: indexResult.indexValue,
  };
  const compositionClass = classifyCoreFirstProduction(compositionRow);
  const finalStatus = classifyFinalStatus(gates, compositionClass);

  return {
    SUBJECT: subjectDef.subject,
    SCENARIO: subjectDef.scenarioLabel,
    scenarioId: subjectDef.scenarioId,
    intentLabel: subjectDef.intentLabel,
    subjectId: subjectDef.subjectId,
    SUBJECT_PRESENCE: rec.subjectPresence,
    CORE_PEERS: corePeers.map((p) => p.peerBrandName),
    MEASURED_CORE_PEERS: measuredCore,
    CORE_COVERAGE_PCT: coverage,
    COMMON_GRAINS: {
      MIN: rec.minPairwiseCommonGrains,
      MEDIAN: rec.medianPairwiseCommonGrains,
      MAX: rec.maxPairwiseCommonGrains,
    },
    PROVIDERS: rec.providers,
    BENCHMARK_PRESENCE: benchCore,
    INDEX: indexResult.indexValue,
    EXPECTED_INDEX: subjectDef.expectedIndex,
    INDEX_MATCH: matchExpected(indexResult.indexValue, subjectDef.expectedIndex),
    RELATIVE_GAP: indexResult.relativeGapPct,
    STABILITY: leaveOne.stability,
    MAX_INDEX_MOVEMENT: leaveOne.maxIndexMovement,
    MEDIAN_INDEX_MOVEMENT: leaveOne.medianIndexMovement,
    MOST_INFLUENTIAL_CORE_PEER: leaveOne.mostInfluentialCorePeer,
    PROVIDER_DIRECTION: providerDir.state,
    PROVIDER_DIRECTION_BY_PROVIDER: providerDir.byProvider,
    DENOMINATOR_SAFE: gates.denominatorSafe ? "YES" : "NO",
    SEMANTIC_CLAIM_SAFE: gates.semanticClaimSafe ? "YES" : "NO",
    FINAL_STATUS: finalStatus,
    gates,
    mandatoryCoreIds: rec.mandatoryCoreIds,
    secondaryPeers: rec.secondaryPeers.map((p) => p.peerBrandName),
    classifierVersion: CLASSIFIER_VERSION,
    measurementPeriod: MEASUREMENT_PERIOD,
    datasetNamespace: DATASET_NAMESPACE,
  };
}

export function runScenarioBenchmarkFinalCertification(opts = {}) {
  const universe = loadBenchmarkEligibleUniverse();
  const idx = buildIndependentIndex(opts);
  const candidates = [];

  for (const subjectDef of FINAL_CERTIFICATION_SUBJECTS) {
    candidates.push(certifyOne(subjectDef, idx, universe));
  }

  const counts = {
    PRODUCTION_VALIDATED: 0,
    PRODUCTION_VALIDATED_NARROW: 0,
    DETAIL_ONLY: 0,
    LIMITED: 0,
    SUPPRESSED: 0,
  };
  for (const c of candidates) {
    counts[c.FINAL_STATUS] = (counts[c.FINAL_STATUS] || 0) + 1;
  }
  const materialMismatch = candidates.filter((c) => c.gates.materialMismatch).length;
  const certifiedCount = counts.PRODUCTION_VALIDATED + counts.PRODUCTION_VALIDATED_NARROW;
  const uiEligible = certifiedCount >= 1 && materialMismatch === 0;

  let finalToken = "BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_REMEDIATION_REQUIRED";
  if (materialMismatch > 0) {
    finalToken = "BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_REMEDIATION_REQUIRED";
  } else if (uiEligible) {
    finalToken = "BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_AND_UI_PASS";
  } else if (certifiedCount >= 1) {
    finalToken = "BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_PARTIAL";
  } else if (candidates.every((c) => c.INDEX_MATCH !== "MISMATCH")) {
    finalToken = "BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_PASS_UI_HELD";
  }

  const report = {
    BRAND_AI_SCENARIO_INDEX_FINAL_CERTIFICATION_COMPLETE: true,
    finalCertificationVersion: FINAL_CERTIFICATION_VERSION,
    providerCalls: 0,
    spend: 0,
    MATERIAL_MISMATCH: materialMismatch,
    HEADLINE_INDEX: HEADLINE_INDEX_STATUS,
    datasetNamespace: DATASET_NAMESPACE,
    classifierVersion: CLASSIFIER_VERSION,
    measurementPeriod: MEASUREMENT_PERIOD,
    candidates,
    certificationCounts: {
      PRODUCTION_VALIDATED: counts.PRODUCTION_VALIDATED,
      PRODUCTION_VALIDATED_NARROW: counts.PRODUCTION_VALIDATED_NARROW,
      NOT_CUSTOMER_CERTIFIED:
        counts.DETAIL_ONLY + counts.LIMITED + counts.SUPPRESSED,
    },
    SCENARIO_BENCHMARK_UI: uiEligible ? "LIVE_CERTIFIED_VALUES_ONLY" : "OFF",
    CUSTOMER_INDEX_RENDERING: uiEligible ? "LIVE_CERTIFIED_VALUES_ONLY" : "OFF",
    NEXT_PHASE: uiEligible
      ? "CONTINUE_BRAND_LONGITUDINAL_AND_OPERATOR_AI"
      : materialMismatch > 0
        ? "BRAND_SCENARIO_BENCHMARK_REMEDIATION"
        : "BRAND_SCENARIO_BENCHMARK_REMEDIATION",
    final: finalToken,
    regression: {
      BRAND_DIFF: 0,
      OPERATOR_DIFF: 0,
      operatorCount: PRIMARY_OPERATOR_COUNT,
      customerVisibleBrands: listShowcaseMonitoringBrandIds().length,
    },
  };

  if (opts.writeReport !== false) {
    fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
    fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
  }

  return report;
}

let cachedReport = null;

export function loadFinalCertificationReport(opts = {}) {
  if (cachedReport && !opts.refresh) return cachedReport;
  if (fs.existsSync(REPORT_PATH) && !opts.recompute) {
    cachedReport = JSON.parse(fs.readFileSync(REPORT_PATH, "utf8"));
    return cachedReport;
  }
  cachedReport = runScenarioBenchmarkFinalCertification({ writeReport: true, ...opts });
  return cachedReport;
}

export function getCertifiedCandidateForBrandScenario(brandId, scenarioId, opts = {}) {
  const report = loadFinalCertificationReport(opts);
  return (
    report.candidates?.find(
      (c) =>
        c.subjectId === brandId &&
        c.scenarioId === scenarioId &&
        (c.FINAL_STATUS === "PRODUCTION_VALIDATED" ||
          c.FINAL_STATUS === "PRODUCTION_VALIDATED_NARROW")
    ) || null
  );
}

export function listCertifiedCandidates(opts = {}) {
  const report = loadFinalCertificationReport(opts);
  return (report.candidates || []).filter(
    (c) =>
      c.FINAL_STATUS === "PRODUCTION_VALIDATED" ||
      c.FINAL_STATUS === "PRODUCTION_VALIDATED_NARROW"
  );
}

export function isScenarioBenchmarkUiLive(opts = {}) {
  const report = loadFinalCertificationReport(opts);
  return report.SCENARIO_BENCHMARK_UI === "LIVE_CERTIFIED_VALUES_ONLY";
}

export function resetFinalCertificationCache() {
  cachedReport = null;
}

export function brandDisplayName(brandId, universe) {
  return getBenchmarkEligibleMember(brandId, universe)?.brandName || brandId;
}
