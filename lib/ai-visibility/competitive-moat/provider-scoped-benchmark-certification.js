/**
 * Provider-scoped benchmark certification — exact scope = brand × scenario × provider.
 * Offline recompute from stored corpus only. No provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import { CORE_FIRST_GATES_CANDIDATE } from "./scenario-benchmark-composition.js";
import {
  buildIndependentIndex,
  classifyStability,
  recomputeOne,
} from "./scenario-benchmark-validation.js";
import {
  loadFinalCertificationReport,
  FINAL_CERTIFICATION_SUBJECTS,
} from "./scenario-benchmark-final-certification.js";
import { loadBenchmarkEligibleUniverse } from "./benchmark-eligible-universe.js";
import {
  resolveScenarioCommercialPeers,
  listMandatoryCorePeerIds,
} from "./scenario-peer-eligibility.js";
import { computeAiPresenceIndex, aggregateBenchmarkPresence, secondaryInCustomerBenchmarkDenominator } from "./benchmark-engine-v1.js";
import { listShowcaseMonitoringBrandIds } from "../brand-ai-showcase-companies.js";
import { CUSTOMER_SCENARIO_DISPLAY_ORDER } from "./scenario-benchmark-customer-service.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const DEFAULT_REGISTRY_PATH = path.join(
  ROOT,
  "reports",
  "ai-visibility",
  "provider-scoped-benchmark-certification-v1.json"
);

export const PROVIDER_SCOPED_CERTIFICATION_VERSION = "provider_scoped_benchmark_certification_v1";

export const BENCHMARK_SCOPES = Object.freeze({
  ALL_PROVIDERS: "ALL_PROVIDERS",
  OPENAI: "OPENAI",
  GEMINI: "GEMINI",
  PERPLEXITY: "PERPLEXITY",
  CLAUDE: "CLAUDE",
});

export const PROVIDER_SCOPE_IDS = Object.freeze([
  BENCHMARK_SCOPES.OPENAI,
  BENCHMARK_SCOPES.GEMINI,
  BENCHMARK_SCOPES.PERPLEXITY,
  BENCHMARK_SCOPES.CLAUDE,
]);

/** Explicit provider-specific certification contract. */
export const PROVIDER_SPECIFIC_CERTIFICATION_GATES = Object.freeze({
  MIN_CORE_PEERS: CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER,
  CORE_COVERAGE_RATIO: 0.5,
  COMMON_GRAIN_MIN: CORE_FIRST_GATES_CANDIDATE.COMMON_GRAIN_MIN,
  STABILITY_REQUIRED: "STABLE",
  REPEATABILITY_REQUIREMENT:
    "ONE_PERIOD_STRONG_STABILITY — single measurement period with leave-one-core-peer-out STABLE",
  SECONDARY_IN_DENOMINATOR: false,
  NO_BROAD_FALLBACK: true,
  NO_UNION_GRAIN: true,
  SEMANTIC_CLAIM_REQUIRED: true,
  DENOMINATOR_SAFE_REQUIRED: true,
  MULTI_PROVIDER_REQUIRED: false,
});

const PROVIDER_ID_BY_SCOPE = Object.freeze({
  [BENCHMARK_SCOPES.OPENAI]: "openai",
  [BENCHMARK_SCOPES.GEMINI]: "gemini",
  [BENCHMARK_SCOPES.PERPLEXITY]: "perplexity",
  [BENCHMARK_SCOPES.CLAUDE]: "claude",
});

const SCOPE_BY_PROVIDER = Object.freeze({
  openai: BENCHMARK_SCOPES.OPENAI,
  gemini: BENCHMARK_SCOPES.GEMINI,
  perplexity: BENCHMARK_SCOPES.PERPLEXITY,
  claude: BENCHMARK_SCOPES.CLAUDE,
  all: BENCHMARK_SCOPES.ALL_PROVIDERS,
  "all providers": BENCHMARK_SCOPES.ALL_PROVIDERS,
});

let cachedRegistry = null;

export function benchmarkScopeFromProvider(provider, allProvidersMode = false) {
  if (allProvidersMode) return BENCHMARK_SCOPES.ALL_PROVIDERS;
  const key = String(provider || "")
    .trim()
    .toLowerCase();
  return SCOPE_BY_PROVIDER[key] || BENCHMARK_SCOPES.ALL_PROVIDERS;
}

export function providerIdFromBenchmarkScope(scope) {
  if (!scope || scope === BENCHMARK_SCOPES.ALL_PROVIDERS) return null;
  return PROVIDER_ID_BY_SCOPE[scope] || null;
}

function presenceOn(presentSet, grainSet) {
  if (!grainSet?.size) return { commonGrains: 0, presentGrains: 0, presence: null };
  let n = 0;
  for (const g of grainSet) {
    if (presentSet.has(g)) n += 1;
  }
  return { commonGrains: grainSet.size, presentGrains: n, presence: n / grainSet.size };
}

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
  return {
    maxIndexMovement: maxMove,
    stability: classifyStability(maxMove),
    mostInfluentialCorePeer: movements.length
      ? movements.reduce((best, m) => (m.move > best.move ? m : best)).omitted
      : null,
  };
}

function denominatorSafe(rec, leaveOne) {
  if (rec.benchmarkCore == null || rec.benchmarkCore <= 0) return false;
  if (rec.coreCount < PROVIDER_SPECIFIC_CERTIFICATION_GATES.MIN_CORE_PEERS) return false;
  const rates = rec.corePeers.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  if (!rates.length) return false;
  if (leaveOne.maxIndexMovement != null && leaveOne.maxIndexMovement > 50) return false;
  return true;
}

function semanticClaimSafe(rec, indexResult) {
  if (indexResult.indexValue == null || indexResult.relativeGapPct == null) return false;
  const recomputed = computeAiPresenceIndex(rec.subjectPresence, rec.benchmarkCore);
  return recomputed.indexValue === indexResult.indexValue;
}

function mandatoryCorePass(rec) {
  const missing = rec.mandatoryCoreIds.filter(
    (id) => !rec.corePeers.some((p) => p.peerBrandId === id)
  );
  return missing.length === 0;
}

function coreCoveragePct(rec) {
  const total = rec.corePeers.length;
  if (!total) return 0;
  const measured = rec.corePeers.filter((p) => typeof p.peerPresence === "number").length;
  return Math.round((measured / total) * 1000) / 10;
}

/**
 * Recompute brand × scenario for one provider scope using provider grains only.
 */
export function recomputeOneForBenchmarkScope(subjectId, scenarioId, idx, universe, scope) {
  if (scope === BENCHMARK_SCOPES.ALL_PROVIDERS) {
    return recomputeOne(subjectId, scenarioId, idx, universe);
  }
  const providerId = providerIdFromBenchmarkScope(scope);
  const grainSet =
    idx.grainsByScenarioProvider.get(`${scenarioId}|${providerId}`) || new Set();
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const subjectPresent = idx.present.get(subjectId)?.get(scenarioId) || new Set();
  const subjectMath = presenceOn(subjectPresent, grainSet);

  const pairwise = [];
  for (const p of peers.calculationPeers) {
    const peerPresent = idx.present.get(p.peerBrandId)?.get(scenarioId) || new Set();
    const peerMath = presenceOn(peerPresent, grainSet);
    pairwise.push({
      peerBrandId: p.peerBrandId,
      peerBrandName: p.peerBrandName,
      commercialRelation: p.commercialRelation,
      commonGrains: peerMath.commonGrains,
      subjectPresentGrains: subjectMath.presentGrains,
      peerPresentGrains: peerMath.presentGrains,
      subjectPresence: subjectMath.presence,
      peerPresence: peerMath.presence,
    });
  }

  const corePairwise = pairwise.filter((p) => p.commercialRelation === "CORE");
  const coreRates = corePairwise.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  const benchCore = aggregateBenchmarkPresence(coreRates, "MEDIAN").value;
  const indexResult = computeAiPresenceIndex(subjectMath.presence, benchCore);
  const leaveOne = leaveOneOutCore(subjectMath.presence, corePairwise);
  const secondaryInDenom = secondaryInCustomerBenchmarkDenominator(
    corePairwise,
    benchCore
  );

  return {
    subjectId,
    scenarioId,
    scope,
    providerId,
    commonGrains: subjectMath.commonGrains,
    subjectPresence: subjectMath.presence,
    corePeers: corePairwise,
    secondaryPeers: pairwise.filter((p) => p.commercialRelation === "SECONDARY"),
    coreCount: corePairwise.filter((p) => typeof p.peerPresence === "number").length,
    benchmarkCore: benchCore,
    indexCore: indexResult.indexValue,
    relativeGapPct: indexResult.relativeGapPct,
    minPairwiseCommonGrains: corePairwise.length
      ? Math.min(...corePairwise.map((p) => p.commonGrains))
      : 0,
    usedBroaderFallback: peers.usedBroaderFallback,
    unionGrainUsed: false,
    secondaryInDenom,
    mandatoryCoreIds: listMandatoryCorePeerIds(subjectId, scenarioId),
    stability: leaveOne.stability,
    maxIndexMovement: leaveOne.maxIndexMovement,
    mostInfluentialCorePeer: leaveOne.mostInfluentialCorePeer,
  };
}

function classifyProviderScopeStatus(rec, gates) {
  if (
    gates.coreOnlyPass &&
    gates.measuredCorePeersPass &&
    gates.coreCoveragePass &&
    gates.mandatoryCorePass &&
    gates.commonGrainsPass &&
    gates.stabilityPass &&
    gates.denominatorSafe &&
    gates.semanticClaimSafe &&
    !gates.secondaryInDenom
  ) {
    return "PRODUCTION_VALIDATED";
  }
  return "NOT_CERTIFIED";
}

export function certifyProviderScope(subjectId, scenarioId, scope, idx, universe) {
  const rec = recomputeOneForBenchmarkScope(subjectId, scenarioId, idx, universe, scope);
  const measuredCore = rec.corePeers.filter((p) => typeof p.peerPresence === "number").length;
  const coverage = coreCoveragePct(rec);
  const leaveOne = {
    maxIndexMovement: rec.maxIndexMovement,
    stability: rec.stability,
  };
  const indexResult = {
    indexValue: rec.indexCore,
    relativeGapPct: rec.relativeGapPct,
  };

  const gates = {
    coreOnlyPass:
      rec.coreCount >= measuredCore &&
      !rec.usedBroaderFallback &&
      !rec.unionGrainUsed &&
      !rec.secondaryInDenom,
    measuredCorePeersPass: measuredCore >= PROVIDER_SPECIFIC_CERTIFICATION_GATES.MIN_CORE_PEERS,
    coreCoveragePass: coverage >= PROVIDER_SPECIFIC_CERTIFICATION_GATES.CORE_COVERAGE_RATIO * 100,
    mandatoryCorePass: mandatoryCorePass(rec),
    commonGrainsPass: rec.minPairwiseCommonGrains >= PROVIDER_SPECIFIC_CERTIFICATION_GATES.COMMON_GRAIN_MIN,
    stabilityPass: rec.stability === PROVIDER_SPECIFIC_CERTIFICATION_GATES.STABILITY_REQUIRED,
    denominatorSafe: denominatorSafe(
      { ...rec, coreCount: measuredCore, benchmarkCore: rec.benchmarkCore },
      leaveOne
    ),
    semanticClaimSafe: semanticClaimSafe(
      { ...rec, benchmarkCore: rec.benchmarkCore },
      indexResult
    ),
    secondaryInDenom: rec.secondaryInDenom,
    multiProviderRequired: false,
  };

  const certificationStatus = classifyProviderScopeStatus(rec, gates);

  return {
    subjectBrandId: subjectId,
    scenarioId,
    scope,
    certificationStatus,
    certifiedIndex: certificationStatus === "PRODUCTION_VALIDATED" ? rec.indexCore : null,
    relativeGapPct:
      certificationStatus === "PRODUCTION_VALIDATED" ? rec.relativeGapPct : null,
    subjectPresence: rec.subjectPresence,
    measurementPeriod: "DEMO_VALIDATION",
    certificationVersion: PROVIDER_SCOPED_CERTIFICATION_VERSION,
    certifiedAt: certificationStatus === "PRODUCTION_VALIDATED" ? "2026-08-18" : null,
    gates,
    corePeers: rec.corePeers.map((p) => p.peerBrandName),
    commonGrains: rec.commonGrains,
  };
}

function allProvidersRecordFromFinalCert(subjectId, scenarioId, finalReport) {
  const row = (finalReport.candidates || []).find(
    (c) => c.subjectId === subjectId && c.scenarioId === scenarioId
  );
  if (!row) return null;
  const status =
    row.FINAL_STATUS === "PRODUCTION_VALIDATED" ||
    row.FINAL_STATUS === "PRODUCTION_VALIDATED_NARROW"
      ? row.FINAL_STATUS
      : "NOT_CERTIFIED";
  return {
    subjectBrandId: subjectId,
    scenarioId,
    scope: BENCHMARK_SCOPES.ALL_PROVIDERS,
    certificationStatus: status,
    certifiedIndex: status.startsWith("PRODUCTION_VALIDATED") ? row.INDEX : null,
    relativeGapPct: status.startsWith("PRODUCTION_VALIDATED") ? row.RELATIVE_GAP : null,
    subjectPresence: row.SUBJECT_PRESENCE,
    measurementPeriod: row.measurementPeriod || "DEMO_VALIDATION",
    certificationVersion: PROVIDER_SCOPED_CERTIFICATION_VERSION,
    certifiedAt: status.startsWith("PRODUCTION_VALIDATED") ? "2026-08-18" : null,
    corePeers: row.CORE_PEERS || [],
  };
}

export function certificationRegistryKey(subjectId, scenarioId, scope) {
  return `${subjectId}|${scenarioId}|${scope}`;
}

export function runProviderScopedCertificationAudit(opts = {}) {
  const universe = loadBenchmarkEligibleUniverse();
  const idx = buildIndependentIndex(opts);
  const finalReport = loadFinalCertificationReport(opts);
  const brandIds = opts.brandIds || listShowcaseMonitoringBrandIds();
  const scenarioIds = opts.scenarioIds || CUSTOMER_SCENARIO_DISPLAY_ORDER;
  const records = [];

  for (const subjectId of brandIds) {
    for (const scenarioId of scenarioIds) {
      const allRec = allProvidersRecordFromFinalCert(subjectId, scenarioId, finalReport);
      if (allRec) records.push(allRec);

      for (const scope of PROVIDER_SCOPE_IDS) {
        records.push(certifyProviderScope(subjectId, scenarioId, scope, idx, universe));
      }
    }
  }

  const counts = {
    ALL_PROVIDERS_CERTIFIED: records.filter(
      (r) =>
        r.scope === BENCHMARK_SCOPES.ALL_PROVIDERS &&
        String(r.certificationStatus).startsWith("PRODUCTION_VALIDATED")
    ).length,
    OPENAI_CERTIFIED_NOW: records.filter(
      (r) => r.scope === BENCHMARK_SCOPES.OPENAI && r.certificationStatus === "PRODUCTION_VALIDATED"
    ).length,
    GEMINI_CERTIFIED_NOW: records.filter(
      (r) => r.scope === BENCHMARK_SCOPES.GEMINI && r.certificationStatus === "PRODUCTION_VALIDATED"
    ).length,
    PERPLEXITY_CERTIFIED_NOW: records.filter(
      (r) =>
        r.scope === BENCHMARK_SCOPES.PERPLEXITY && r.certificationStatus === "PRODUCTION_VALIDATED"
    ).length,
    CLAUDE_CERTIFIED_NOW: records.filter(
      (r) => r.scope === BENCHMARK_SCOPES.CLAUDE && r.certificationStatus === "PRODUCTION_VALIDATED"
    ).length,
  };

  const report = {
    PROVIDER_SCOPED_BENCHMARK_CERTIFICATION_COMPLETE: true,
    certificationVersion: PROVIDER_SCOPED_CERTIFICATION_VERSION,
    providerCalls: 0,
    spend: 0,
    gates: PROVIDER_SPECIFIC_CERTIFICATION_GATES,
    counts,
    secondaryInDenomByScope: countSecondaryInDenomByScope(records),
    records,
    certifiedByScope: Object.fromEntries(
      PROVIDER_SCOPE_IDS.map((scope) => [
        scope,
        records
          .filter((r) => r.scope === scope && r.certificationStatus === "PRODUCTION_VALIDATED")
          .map((r) => ({
            subjectBrandId: r.subjectBrandId,
            scenarioId: r.scenarioId,
            certifiedIndex: r.certifiedIndex,
          })),
      ])
    ),
  };

  const outPath = opts.registryPath || DEFAULT_REGISTRY_PATH;
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  return report;
}

export function loadProviderScopedCertificationRegistry(opts = {}) {
  if (cachedRegistry && !opts.refresh) return cachedRegistry;
  const filePath = opts.registryPath || DEFAULT_REGISTRY_PATH;
  if (!fs.existsSync(filePath)) {
    cachedRegistry = { records: [], counts: {} };
    return cachedRegistry;
  }
  cachedRegistry = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return cachedRegistry;
}

export function lookupScopeCertification(subjectId, scenarioId, scope, opts = {}) {
  const registry = loadProviderScopedCertificationRegistry(opts);
  const key = certificationRegistryKey(subjectId, scenarioId, scope);
  const map = new Map(
    (registry.records || []).map((r) => [
      certificationRegistryKey(r.subjectBrandId, r.scenarioId, r.scope),
      r,
    ])
  );
  return map.get(key) || null;
}

export function listScopeCertificationsForBrand(subjectId, scope, opts = {}) {
  const registry = loadProviderScopedCertificationRegistry(opts);
  return (registry.records || []).filter(
    (r) => r.subjectBrandId === subjectId && r.scope === scope
  );
}

export function verifyAllProvidersFrozenBaseline(opts = {}) {
  const registry = loadProviderScopedCertificationRegistry(opts);
  const diffs = {};
  for (const subject of FINAL_CERTIFICATION_SUBJECTS) {
    const rec = lookupScopeCertification(subject.subjectId, subject.scenarioId, BENCHMARK_SCOPES.ALL_PROVIDERS, opts);
    const key =
      subject.subjectId === IDS.AUTOGRAPH
        ? "AUTOGRAPH_103"
        : subject.subjectId === IDS.TAPESTRY
          ? "TAPESTRY_103"
          : subject.subjectId === IDS.ASCEND
            ? "ASCEND_67"
            : subject.subjectId;
    diffs[key] = (rec?.certifiedIndex ?? null) - subject.expectedIndex;
  }
  return {
    ok: Object.values(diffs).every((d) => d === 0),
    ...Object.fromEntries(Object.entries(diffs).map(([k, v]) => [`${k}_DIFF`, v])),
  };
}

/**
 * Deep-dive audit for one brand × scenario × provider scope (internal diagnostics).
 */
export function auditProviderScopeCandidate(subjectId, scenarioId, scope, opts = {}) {
  const universe = loadBenchmarkEligibleUniverse();
  const idx = buildIndependentIndex(opts);
  const rec = recomputeOneForBenchmarkScope(subjectId, scenarioId, idx, universe, scope);
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const coverage = coreCoveragePct(rec);
  const certified = certifyProviderScope(subjectId, scenarioId, scope, idx, universe);

  return {
    subjectId,
    scenarioId,
    scope,
    SUBJECT_PRESENCE: rec.subjectPresence,
    CORE_PEERS: rec.corePeers.map((p) => ({
      name: p.peerBrandName,
      relation: p.commercialRelation,
      presence: p.peerPresence,
      commonGrains: p.commonGrains,
    })),
    SECONDARY_PEERS: rec.secondaryPeers.map((p) => ({
      name: p.peerBrandName,
      presence: p.peerPresence,
      commonGrains: p.commonGrains,
    })),
    CONDITIONAL_PEERS: peers.calculationPeers
      .filter((p) => p.commercialRelation === "CONDITIONAL")
      .map((p) => p.peerBrandName),
    PEERS_IN_DENOMINATOR: rec.corePeers
      .filter((p) => typeof p.peerPresence === "number")
      .map((p) => p.peerBrandName),
    SECONDARY_IN_DENOMINATOR: rec.secondaryInDenom ? 1 : 0,
    COMMON_GRAINS_PER_CORE_PEER: rec.corePeers.map((p) => ({
      peer: p.peerBrandName,
      commonGrains: p.commonGrains,
    })),
    CORE_COVERAGE_PCT: coverage,
    MANDATORY_CORE_PASS: mandatoryCorePass(rec),
    WORKING_BENCHMARK_PRESENCE: rec.benchmarkCore,
    WORKING_INDEX: rec.indexCore,
    STABILITY: rec.stability,
    MAX_INDEX_MOVEMENT: rec.maxIndexMovement,
    gates: certified.gates,
    FINAL_STATUS: certified.certificationStatus,
    REMAINING_BLOCKER: certified.certificationStatus === "PRODUCTION_VALIDATED"
      ? null
      : Object.entries(certified.gates || {})
          .filter(([, v]) => v === false)
          .map(([k]) => k),
  };
}

export function countSecondaryInDenomByScope(records = []) {
  const counts = Object.fromEntries(PROVIDER_SCOPE_IDS.map((s) => [s, 0]));
  for (const r of records) {
    if (r.scope === BENCHMARK_SCOPES.ALL_PROVIDERS) continue;
    if (r.gates?.secondaryInDenom) counts[r.scope] = (counts[r.scope] || 0) + 1;
  }
  return counts;
}
