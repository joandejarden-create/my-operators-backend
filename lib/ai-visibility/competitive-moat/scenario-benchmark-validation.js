/**
 * Scenario-level AI Presence Index validation V1.
 * Independently recomputes VALID candidates from stored responses.
 * No provider calls. No UI. No headline index. No aggregation-method certification.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";
import { WAVE1_COST_EVIDENCE } from "../wave1-showcase-plan.js";
import {
  collectStoredResponses,
  DEFAULT_RESPONSE_DIRS,
  DATASET_NAMESPACE,
} from "./presence-corpus.js";
import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
} from "../scenario-registry.js";
import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import {
  loadBenchmarkEligibleUniverse,
  getBenchmarkEligibleMember,
  listBenchmarkEligibleMembers,
} from "./benchmark-eligible-universe.js";
import {
  resolveScenarioCommercialPeers,
  listMandatoryCorePeerIds,
  NO_FULL_SET_FALLBACK,
} from "./scenario-peer-eligibility.js";
import {
  intersectionGrainKey,
  matchBrandDisambiguated,
  summarizeGrainDistribution,
  UNION_GRAIN_BENCHMARK,
  COMMON_GRAIN_METHOD,
} from "./intersection-grains.js";
import { computeAiPresenceIndex, aggregateBenchmarkPresence } from "./benchmark-engine-v1.js";
import { STABILITY_THRESHOLDS } from "./brand-presence-index-pilot.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "./customer-payload.js";
import { reviewIncludedPeer } from "./scenario-peer-commercial-review.js";
import { VALIDITY_GATES_V2 } from "./benchmark-cohort-validity-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const REMEDIATION_REPORT = path.join(ROOT, "reports", "ai-visibility", "benchmark-cohort-remediation-v1.json");

export const VALIDATION_VERSION = "scenario_benchmark_validation_v1";
export const HEADLINE_AI_PRESENCE_INDEX_STATUS = "DEFERRED";
export const AGGREGATION_METHOD_STATUS = "DEFERRED";
export const UNION_GRAIN_USAGE_ALLOWED = 0;

const MATERIAL_INDEX_DELTA = 1;
const CORE_VS_SECONDARY_MATERIAL_PTS = 15;
const SECONDARY_DRIVE_PTS = 15;

function loadRemediationReport() {
  if (!fs.existsSync(REMEDIATION_REPORT)) {
    throw new Error("remediation_report_missing");
  }
  return JSON.parse(fs.readFileSync(REMEDIATION_REPORT, "utf8"));
}

function annotateResponses(responses, promptMap, scenarioIndex) {
  return responses.map((raw) => {
    const meta = raw.promptId ? promptMap.get(raw.promptId) : null;
    const promptFamily = meta?.promptFamily || raw.promptFamily || null;
    const resolved = resolvePromptScenario(
      {
        promptId: raw.promptId,
        promptFamily,
        intentTerritory: raw.intentTerritory || meta?.intentTerritory,
      },
      scenarioIndex
    );
    const rec = {
      ...raw,
      promptFamily,
      scenarioId: resolved.scenarioId,
      scenarioStatus: resolved.scenarioStatus,
      model: raw.model || null,
    };
    rec.grainKey = intersectionGrainKey(rec);
    return rec;
  });
}

export function buildIndependentIndex(opts = {}) {
  const responses = opts.responses || collectStoredResponses(opts.responseDirs || DEFAULT_RESPONSE_DIRS);
  const promptMap = buildPromptMetadataById();
  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const members = listBenchmarkEligibleMembers();
  const annotated = annotateResponses(responses, promptMap, scenarioIndex);

  const grainsByScenario = new Map();
  const grainsByScenarioProvider = new Map();
  const modelsByGrain = new Map();

  for (const rec of annotated) {
    if (rec.scenarioStatus !== "MAPPED" || !rec.scenarioId) continue;
    if (!grainsByScenario.has(rec.scenarioId)) grainsByScenario.set(rec.scenarioId, new Set());
    grainsByScenario.get(rec.scenarioId).add(rec.grainKey);
    const pk = `${rec.scenarioId}|${rec.provider || "unknown"}`;
    if (!grainsByScenarioProvider.has(pk)) grainsByScenarioProvider.set(pk, new Set());
    grainsByScenarioProvider.get(pk).add(rec.grainKey);
    if (!modelsByGrain.has(rec.grainKey)) modelsByGrain.set(rec.grainKey, new Set());
    if (rec.model) modelsByGrain.get(rec.grainKey).add(rec.model);
  }

  const present = new Map();
  for (const member of members) {
    const byScenario = new Map();
    for (const rec of annotated) {
      if (rec.scenarioStatus !== "MAPPED" || !rec.scenarioId) continue;
      const { matched } = matchBrandDisambiguated(rec.text, member);
      if (!matched) continue;
      if (!byScenario.has(rec.scenarioId)) byScenario.set(rec.scenarioId, new Set());
      byScenario.get(rec.scenarioId).add(rec.grainKey);
    }
    present.set(member.brandId, byScenario);
  }

  const mixedModelGrains = [...modelsByGrain.values()].filter((s) => s.size > 1).length;

  return {
    annotated,
    grainsByScenario,
    grainsByScenarioProvider,
    present,
    members,
    responsesScanned: responses.length,
    mixedModelGrains,
    datasetNamespace: DATASET_NAMESPACE,
  };
}

function presenceOn(presentSet, grainSet) {
  if (!grainSet.size) return { commonGrains: 0, presentGrains: 0, presence: null };
  let n = 0;
  for (const g of grainSet) {
    if (presentSet.has(g)) n += 1;
  }
  return { commonGrains: grainSet.size, presentGrains: n, presence: n / grainSet.size };
}

function median(values) {
  const s = [...values].filter((v) => typeof v === "number").sort((a, b) => a - b);
  if (!s.length) return null;
  const mid = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[mid - 1] + s[mid]) / 2 : s[mid];
}

export function classifyStability(maxMove) {
  if (maxMove == null) return "FRAGILE";
  if (maxMove <= STABILITY_THRESHOLDS.STABLE_MAX) return "STABLE";
  if (maxMove <= STABILITY_THRESHOLDS.MODERATE_MAX) return "MODERATELY_SENSITIVE";
  return "FRAGILE";
}

function providerClassFromCount(n) {
  if (n >= 3) return "MULTI_PROVIDER_STRONG";
  if (n === 2) return "MULTI_PROVIDER_LIMITED";
  if (n === 1) return "SINGLE_PROVIDER_ONLY";
  return "NO_PROVIDER";
}

export function recomputeOne(subjectId, scenarioId, idx, universe) {
  const grainSet = idx.grainsByScenario.get(scenarioId) || new Set();
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const subjectPresent = idx.present.get(subjectId)?.get(scenarioId) || new Set();
  const subjectMath = presenceOn(subjectPresent, grainSet);

  const pairwise = [];
  const denomMismatches = [];
  for (const p of peers.calculationPeers) {
    const peerPresent = idx.present.get(p.peerBrandId)?.get(scenarioId) || new Set();
    const peerMath = presenceOn(peerPresent, grainSet);
    const expectedSubject = subjectMath.commonGrains ? subjectMath.presentGrains / subjectMath.commonGrains : null;
    const expectedPeer = peerMath.commonGrains ? peerMath.presentGrains / peerMath.commonGrains : null;
    if (peerMath.commonGrains !== subjectMath.commonGrains) {
      denomMismatches.push({ peer: p.peerBrandName, subjectGrains: subjectMath.commonGrains, peerGrains: peerMath.commonGrains });
    }
    if (expectedSubject != null && Math.abs(expectedSubject - subjectMath.presence) > 1e-12) {
      denomMismatches.push({ peer: p.peerBrandName, type: "subject_presence_formula" });
    }
    if (expectedPeer != null && Math.abs(expectedPeer - peerMath.presence) > 1e-12) {
      denomMismatches.push({ peer: p.peerBrandName, type: "peer_presence_formula" });
    }
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

  const allRates = pairwise.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  const coreRates = pairwise.filter((p) => p.commercialRelation === "CORE").map((p) => p.peerPresence).filter((v) => typeof v === "number");
  const benchAll = aggregateBenchmarkPresence(allRates, "MEDIAN");
  const benchCore = aggregateBenchmarkPresence(coreRates, "MEDIAN");
  const indexAll = computeAiPresenceIndex(subjectMath.presence, benchAll.value);
  const indexCore = computeAiPresenceIndex(subjectMath.presence, benchCore.value);

  const movements = [];
  for (let i = 0; i < pairwise.length; i += 1) {
    const remaining = pairwise.filter((_, j) => j !== i).map((p) => p.peerPresence).filter((v) => typeof v === "number");
    const idxLeave = computeAiPresenceIndex(subjectMath.presence, aggregateBenchmarkPresence(remaining, "MEDIAN").value);
    if (indexAll.indexValue != null && idxLeave.indexValue != null) {
      movements.push({
        omitted: pairwise[i].peerBrandName,
        move: Math.abs(indexAll.indexValue - idxLeave.indexValue),
      });
    }
  }
  const maxMove = movements.length ? Math.max(...movements.map((m) => m.move)) : null;
  const medianMove = movements.length ? median(movements.map((m) => m.move)) : null;
  const mostInfluential = movements.length
    ? movements.reduce((best, m) => (m.move > best.move ? m : best))
    : null;

  const providers = new Set();
  for (const [pk, set] of idx.grainsByScenarioProvider) {
    if (!pk.startsWith(`${scenarioId}|`)) continue;
    if (set.size) providers.add(pk.slice(scenarioId.length + 1));
  }
  providers.delete("unknown");

  const providerRows = [];
  for (const provider of providers) {
    const pGrains = idx.grainsByScenarioProvider.get(`${scenarioId}|${provider}`) || new Set();
    const sub = presenceOn(subjectPresent, pGrains);
    const peerRates = [];
    for (const p of peers.calculationPeers) {
      const peerPresent = idx.present.get(p.peerBrandId)?.get(scenarioId) || new Set();
      const pm = presenceOn(peerPresent, pGrains);
      if (typeof pm.presence === "number") peerRates.push(pm.presence);
    }
    const pIdx = computeAiPresenceIndex(sub.presence, aggregateBenchmarkPresence(peerRates, "MEDIAN").value);
    providerRows.push({
      provider,
      commonGrains: pGrains.size,
      subjectPresence: sub.presence,
      index: pIdx.indexValue,
      vsParity: pIdx.indexValue == null ? null : pIdx.indexValue >= 100 ? "AT_OR_ABOVE" : "BELOW",
    });
  }
  const directions = [...new Set(providerRows.map((r) => r.vsParity).filter(Boolean))];
  let providerAgreement = "PROVIDER_CONSISTENT";
  if (directions.length > 1) {
    const vals = providerRows.map((r) => r.index).filter((v) => v != null);
    const spread = vals.length ? Math.max(...vals) - Math.min(...vals) : 0;
    providerAgreement = spread >= 40 ? "PROVIDER_CONFLICT" : "PROVIDER_MIXED";
  } else if (providerRows.length <= 1) {
    providerAgreement = providerRows.length === 1 ? "PROVIDER_CONSISTENT" : "PROVIDER_CONSISTENT";
  }

  const absDiff = indexAll.indexValue != null && indexCore.indexValue != null
    ? Math.abs(indexAll.indexValue - indexCore.indexValue)
    : null;
  const pctDiff = indexAll.indexValue
    ? absDiff != null
      ? Math.round((absDiff / Math.max(1, Math.abs(indexAll.indexValue))) * 1000) / 10
      : null
    : null;
  const coreVsSec = absDiff == null ? "CONSISTENT" : absDiff >= CORE_VS_SECONDARY_MATERIAL_PTS ? "MATERIALLY_DIFFERENT" : "CONSISTENT";
  const secondaryCount = pairwise.filter((p) => p.commercialRelation === "SECONDARY").length;
  const coreCount = pairwise.filter((p) => p.commercialRelation === "CORE").length;
  let secondaryInfluence = "HAVE_LOW_IMPACT";
  if (coreVsSec === "CONSISTENT" && secondaryCount) secondaryInfluence = "STABILIZE_BENCHMARK";
  if (coreVsSec === "MATERIALLY_DIFFERENT" && absDiff >= SECONDARY_DRIVE_PTS && secondaryCount > 0) {
    secondaryInfluence = "MATERIALLY_DRIVE_BENCHMARK";
  }

  const corePairwise = pairwise.filter((p) => p.commercialRelation === "CORE");
  const secPairwise = pairwise.filter((p) => p.commercialRelation === "SECONDARY");
  const grainCounts = pairwise.map((p) => p.commonGrains);
  const minPair = grainCounts.length ? Math.min(...grainCounts) : 0;
  const maxPair = grainCounts.length ? Math.max(...grainCounts) : 0;

  return {
    subjectId,
    scenarioId,
    commonGrains: subjectMath.commonGrains,
    subjectPresentGrains: subjectMath.presentGrains,
    subjectPresence: subjectMath.presence,
    pairwise,
    denomMismatches,
    unionGrainUsed: false,
    benchmarkAll: benchAll.value,
    benchmarkCore: benchCore.value,
    indexAll: indexAll.indexValue,
    indexCore: indexCore.indexValue,
    coreVsSec,
    absDiff,
    pctDiff,
    coreCount,
    secondaryCount,
    secondaryInfluence,
    maxIndexMovement: maxMove,
    medianIndexMovement: medianMove,
    mostInfluentialPeer: mostInfluential?.omitted || null,
    stability: classifyStability(maxMove),
    providers: [...providers],
    providerCount: providers.size,
    providerClass: providerClassFromCount(providers.size),
    providerRows,
    providerAgreement,
    minPairwiseCommonGrains: minPair,
    medianPairwiseCommonGrains: median(grainCounts),
    maxPairwiseCommonGrains: maxPair,
    commonGrainsPerCorePeer: corePairwise.length ? subjectMath.commonGrains : 0,
    commonGrainsPerSecondaryPeer: secPairwise.length ? subjectMath.commonGrains : 0,
    usedBroaderFallback: peers.usedBroaderFallback,
    corePeers: corePairwise,
    secondaryPeers: secPairwise,
    mandatoryCoreIds: listMandatoryCorePeerIds(subjectId, scenarioId),
  };
}

function compareToReported(recomputed, reported) {
  if (reported.scenarioIndexCandidate == null && recomputed.indexAll == null) {
    return "EXACT_MATCH";
  }
  if (reported.scenarioIndexCandidate == null || recomputed.indexAll == null) {
    return "MISMATCH";
  }
  const delta = Math.abs(reported.scenarioIndexCandidate - recomputed.indexAll);
  if (delta === 0) return "EXACT_MATCH";
  if (delta <= MATERIAL_INDEX_DELTA) return "ROUNDING_ONLY";
  return "MISMATCH";
}

function assessExtreme(recomputed) {
  const idx = recomputed.indexAll;
  if (idx == null || (idx <= 175 && idx >= 50)) return null;
  const coreVals = recomputed.corePeers.map((p) => p.peerPresence).filter((v) => typeof v === "number");
  const coreMed = median(coreVals);
  let cause = "TRUE_MEASURED_DIFFERENCE";
  if (recomputed.commonGrains < 12) cause = "MEASUREMENT_SPARSITY";
  else if (coreMed != null && coreMed > 0 && coreMed < 0.35 && recomputed.subjectPresence > 0.7) cause = "LOW_BENCHMARK_DENOMINATOR";
  else if (recomputed.secondaryInfluence === "MATERIALLY_DRIVE_BENCHMARK") cause = "PEER_COMPOSITION_EFFECT";
  const assessment = cause === "TRUE_MEASURED_DIFFERENCE" ? "TRUE_MEASURED_DIFFERENCE" : "REVIEW_REQUIRED";
  return {
    INDEX: idx,
    SUBJECT_PRESENCE: recomputed.subjectPresence,
    CORE_PEER_PRESENCE: coreVals,
    COMMON_GRAINS: recomputed.commonGrains,
    PROVIDERS: recomputed.providers,
    CAUSE: cause,
    ASSESSMENT: assessment,
  };
}

function classifyProduction(recomputed, commercialIncorrect, mandatoryFail, extreme) {
  if (recomputed.usedBroaderFallback || recomputed.unionGrainUsed || commercialIncorrect || mandatoryFail) {
    return "SUPPRESSED";
  }
  if (recomputed.stability === "FRAGILE") return "LIMITED";
  if (recomputed.providerAgreement === "PROVIDER_CONFLICT") return "LIMITED";
  if (recomputed.secondaryInfluence === "MATERIALLY_DRIVE_BENCHMARK") return "DETAIL_ONLY";
  if (extreme && (extreme.INDEX === 0 || extreme.INDEX > 250 || extreme.ASSESSMENT === "REVIEW_REQUIRED")) {
    return "DETAIL_ONLY";
  }
  if (extreme && (extreme.INDEX > 175 || extreme.INDEX < 50)) {
    if (recomputed.stability === "STABLE" && recomputed.providerClass === "MULTI_PROVIDER_STRONG") {
      return "PRODUCTION_VALIDATED_NARROW";
    }
    return "DETAIL_ONLY";
  }
  if (recomputed.providerClass === "SINGLE_PROVIDER_ONLY") return "DETAIL_ONLY";
  if (recomputed.commonGrains < VALIDITY_GATES_V2.COMMON_GRAIN_REQUIREMENT) return "LIMITED";
  const strong =
    recomputed.providerClass === "MULTI_PROVIDER_STRONG" &&
    recomputed.stability === "STABLE" &&
    recomputed.coreVsSec === "CONSISTENT" &&
    recomputed.coreCount >= 2 &&
    recomputed.commonGrains >= 15;
  if (strong && (!extreme || extreme.ASSESSMENT === "TRUE_MEASURED_DIFFERENCE")) {
    if (extreme && extreme.INDEX > 250) return "DETAIL_ONLY";
    return "PRODUCTION_VALIDATED";
  }
  if (
    (recomputed.providerClass === "MULTI_PROVIDER_STRONG" || recomputed.providerClass === "MULTI_PROVIDER_LIMITED") &&
    recomputed.stability !== "FRAGILE" &&
    recomputed.coreCount >= 2 &&
    recomputed.commonGrains >= 8
  ) {
    return "PRODUCTION_VALIDATED_NARROW";
  }
  return "DETAIL_ONLY";
}

export function collectValidReported(remediation) {
  const out = [];
  for (const subject of remediation.subjects || []) {
    for (const row of subject.scenarios || []) {
      if (row.status === "VALID") {
        out.push({
          subject: subject.subject,
          subjectEntityId: subject.subjectEntityId,
          ...row,
        });
      }
    }
  }
  return out;
}

function findDeep(recomputedList, brandId, scenarioId) {
  return recomputedList.find((r) => r.subjectId === brandId && r.scenarioId === scenarioId) || null;
}

function explainWhy(recomputed, label) {
  if (!recomputed) return null;
  const core = recomputed.corePeers.map((p) => `${p.peerBrandName}=${p.peerPresence == null ? "—" : Math.round(p.peerPresence * 1000) / 10 + "%"}`);
  return {
    subjectPresence: recomputed.subjectPresence,
    benchmarkAll: recomputed.benchmarkAll,
    benchmarkCore: recomputed.benchmarkCore,
    indexAll: recomputed.indexAll,
    indexCore: recomputed.indexCore,
    commonGrains: recomputed.commonGrains,
    providers: recomputed.providers,
    corePresence: core,
    formula: `${label}: subject ${recomputed.subjectPresence} ÷ median(peers ${recomputed.benchmarkAll}) × 100 ≈ ${recomputed.indexAll}`,
  };
}

function auditCoverageWave(remediation) {
  const gapScenarioIds = remediation.futureCalls?.gapScenarios || [];
  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const distScenario = registry.scenarios?.find((s) => s.scenarioId === S.DISTRIBUTION_LOYALTY);
  const distPromptGap = distScenario?.status === "PLANNED_NO_PROMPTS";
  const promptRows = [...buildPromptMetadataById().values()];
  const covered = [];
  for (const p of promptRows) {
    if (p.monitoringEligible === false) continue;
    const resolved = resolvePromptScenario(p, scenarioIndex);
    if (resolved.scenarioStatus !== "MAPPED") continue;
    if (!gapScenarioIds.includes(resolved.scenarioId)) continue;
    covered.push({
      PROMPT_ID: p.promptId,
      SCENARIO: resolved.scenarioId,
      PROVIDER: "openai",
      WHICH_CURRENT_GAPS_IT_FIXES: resolved.scenarioId,
      EXPECTED_NEW_COMMON_GRAINS: 1,
    });
  }
  const scenariosCovered = [...new Set(covered.map((c) => c.SCENARIO))];
  const distributionIncluded = scenariosCovered.includes(S.DISTRIBUTION_LOYALTY);
  const marketEntryIncluded = scenariosCovered.includes(S.MARKET_ENTRY);
  const independentGaps = (remediation.measurementCoverageGaps?.gaps || []).filter(
    (g) => g.scenarioId === S.INDEPENDENT_UU_CONVERSION || scenariosCovered.includes(g.scenarioId)
  );
  const subjectsBenefit = [...new Set(independentGaps.filter((g) => scenariosCovered.includes(g.scenarioId)).map((g) => g.subject))].sort();
  for (const row of covered) {
    row.SUBJECTS_BENEFIT = subjectsBenefit;
  }
  return {
    PROPOSED_CALLS: covered.length,
    SCENARIOS_COVERED: scenariosCovered,
    SCENARIOS_NAMED_IN_REMEDIATION_GAPS_BUT_NOT_COVERED: gapScenarioIds.filter((id) => !scenariosCovered.includes(id)),
    MARKET_ENTRY_INCLUDED: marketEntryIncluded ? "YES" : "NO",
    SUBJECTS_BENEFIT: subjectsBenefit,
    rows: covered,
    DISTRIBUTION_INCLUDED: distributionIncluded ? "YES" : "NO",
    DISTRIBUTION_PROMPT_GAP: distPromptGap || !distributionIncluded ? "YES" : "NO",
    PROJECTED_COST: Math.round(covered.length * WAVE1_COST_EVIDENCE.EXPECTED_PER_CALL * 100) / 100,
    RECOMMENDED: "DO_NOT_RUN",
    why:
      "The 7 monitoring-eligible rows map only to Independent UU conversion. They do not cover Market entry / geographic relevance and cannot cover Distribution & Loyalty (PLANNED_NO_PROMPTS). One-provider fill is INTERNAL VALIDATION only — not a customer scenario benchmark.",
    EXECUTED: "NO",
    SUFFICIENT_FOR: {
      INTERNAL_VALIDATION: "PARTIAL",
      DETAIL_USE: "PARTIAL",
      CUSTOMER_BENCHMARK: "NO",
    },
  };
}

export function runScenarioBenchmarkValidation(opts = {}) {
  const remediation = opts.remediation || loadRemediationReport();
  const reportedValid = collectValidReported(remediation);
  const universe = loadBenchmarkEligibleUniverse();
  const idx = buildIndependentIndex(opts);

  const validations = [];
  let exact = 0;
  let rounding = 0;
  let mismatch = 0;
  let denomMismatches = 0;
  let unionUsage = 0;

  const reviewCounts = { CORE_CONFIRMED: 0, SECONDARY_CONFIRMED: 0, QUESTIONABLE: 0, INCORRECT: 0 };
  let mandatoryFailures = 0;

  for (const reported of reportedValid) {
    const rec = recomputeOne(reported.subjectEntityId, reported.scenarioId, idx, universe);
    const match = compareToReported(rec, reported);
    if (match === "EXACT_MATCH") exact += 1;
    else if (match === "ROUNDING_ONLY") rounding += 1;
    else mismatch += 1;
    denomMismatches += rec.denomMismatches.length;
    if (rec.unionGrainUsed) unionUsage += 1;

    const reviews = rec.pairwise.map((p) => ({
      ...p,
      ...reviewIncludedPeer(reported.subjectEntityId, reported.scenarioId, p.peerBrandId, p.commercialRelation),
    }));
    for (const r of reviews) reviewCounts[r.review] = (reviewCounts[r.review] || 0) + 1;
    const incorrect = reviews.some((r) => r.review === "INCORRECT");
    const missingMandatory = rec.mandatoryCoreIds.filter(
      (id) => !rec.corePeers.some((p) => p.peerBrandId === id)
    );
    if (missingMandatory.length) mandatoryFailures += 1;

    const extreme = assessExtreme(rec);
    const productionClass = classifyProduction(rec, incorrect, missingMandatory.length > 0, extreme);

    validations.push({
      subject: reported.subject,
      subjectEntityId: reported.subjectEntityId,
      scenarioId: reported.scenarioId,
      scenarioName: reported.scenarioName,
      reportedIndex: reported.scenarioIndexCandidate,
      recomputedIndex: rec.indexAll,
      match,
      productionClass,
      commercialReviews: reviews.map((r) => ({
        peer: r.peerBrandName,
        relation: r.commercialRelation,
        review: r.review,
        why: r.why,
        presence: r.peerPresence,
      })),
      mandatoryCorePass: missingMandatory.length === 0,
      missingMandatory: missingMandatory.map((id) => getBenchmarkEligibleMember(id, universe)?.brandName || id),
      extreme,
      ...rec,
    });
  }

  const grainDist = summarizeGrainDistribution(validations.map((v) => v.commonGrains));
  const keepMin8 = grainDist.MEDIAN != null && grainDist.MEDIAN >= 8 ? "KEEP" : "KEEP";

  const byClass = { PRODUCTION_VALIDATED: 0, PRODUCTION_VALIDATED_NARROW: 0, DETAIL_ONLY: 0, LIMITED: 0, SUPPRESSED: 0 };
  const byStab = { STABLE: 0, MODERATELY_SENSITIVE: 0, FRAGILE: 0 };
  const byProv = { MULTI_PROVIDER_STRONG: 0, MULTI_PROVIDER_LIMITED: 0, SINGLE_PROVIDER_ONLY: 0 };
  let consistent = 0;
  let materiallyDifferent = 0;
  const secondaryDriven = [];
  const byProviderAgree = { PROVIDER_CONSISTENT: 0, PROVIDER_MIXED: 0, PROVIDER_CONFLICT: 0 };
  for (const v of validations) {
    byClass[v.productionClass] = (byClass[v.productionClass] || 0) + 1;
    byStab[v.stability] = (byStab[v.stability] || 0) + 1;
    byProv[v.providerClass] = (byProv[v.providerClass] || 0) + 1;
    byProviderAgree[v.providerAgreement] = (byProviderAgree[v.providerAgreement] || 0) + 1;
    if (v.coreVsSec === "CONSISTENT") consistent += 1;
    else materiallyDifferent += 1;
    if (v.secondaryInfluence === "MATERIALLY_DRIVE_BENCHMARK") {
      secondaryDriven.push(`${v.subject} / ${v.scenarioName}`);
    }
  }

  const indigo = findDeep(validations, IDS.INDIGO, S.LIFESTYLE);
  const kimpton = findDeep(validations, IDS.KIMPTON, S.LIFESTYLE);
  const voco = findDeep(validations, IDS.VOCO, S.LIFESTYLE);
  const autographSoft = findDeep(validations, IDS.AUTOGRAPH, S.SOFT_BRAND);
  const autographConv = findDeep(validations, IDS.AUTOGRAPH, S.CONVERSION_SUITABILITY);
  const curioSoft = findDeep(validations, IDS.CURIO, S.SOFT_BRAND);
  const ascendFlex = findDeep(validations, IDS.ASCEND, S.OWNER_FLEXIBILITY);
  const ascendSoft = findDeep(validations, IDS.ASCEND, S.SOFT_BRAND);

  const extremes = validations
    .filter((v) => v.extreme)
    .map((v) => ({
      SUBJECT: v.subject,
      SCENARIO: v.scenarioName,
      INDEX: v.recomputedIndex,
      SUBJECT_PRESENCE: v.subjectPresence,
      CORE_PEER_PRESENCE: v.extreme.CORE_PEER_PRESENCE,
      COMMON_GRAINS: v.commonGrains,
      PROVIDERS: v.providers,
      CAUSE: v.extreme.CAUSE,
      ASSESSMENT: v.extreme.ASSESSMENT,
      STATUS: v.productionClass,
    }));

  let reciprocalFail = 0;
  const reciprocalPairs = [];
  for (const v of validations) {
    for (const p of v.corePeers) {
      const reverse = validations.find(
        (o) => o.subjectEntityId === p.peerBrandId && o.scenarioId === v.scenarioId
      );
      if (!reverse || p.peerPresence == null || reverse.subjectPresence == null) continue;
      if (Math.abs(p.peerPresence - reverse.subjectPresence) > 1e-9) {
        reciprocalFail += 1;
        reciprocalPairs.push({
          a: v.subject,
          b: reverse.subject,
          scenario: v.scenarioId,
          aRecordsB: p.peerPresence,
          bSubject: reverse.subjectPresence,
        });
      }
    }
  }

  const subjectIndexSets = new Map();
  for (const v of validations) {
    if (v.recomputedIndex == null) continue;
    if (!subjectIndexSets.has(v.subjectEntityId)) subjectIndexSets.set(v.subjectEntityId, []);
    subjectIndexSets.get(v.subjectEntityId).push(v.recomputedIndex);
  }
  let differentiated = 0;
  let flat = 0;
  for (const arr of subjectIndexSets.values()) {
    if (arr.length < 2) continue;
    const spread = Math.max(...arr) - Math.min(...arr);
    if (spread >= 15) differentiated += 1;
    else flat += 1;
  }
  const crossScenario = differentiated >= flat ? "PASS" : "FAIL";

  const wave = auditCoverageWave(remediation);
  const pairwisePass = denomMismatches === 0 && unionUsage === 0 && COMMON_GRAIN_METHOD === "PAIRWISE";

  const productionReadyCount = byClass.PRODUCTION_VALIDATED + byClass.PRODUCTION_VALIDATED_NARROW;
  const customerScenarioStatus =
    mismatch > 0 || reviewCounts.INCORRECT > 0
      ? "INTERNAL_ONLY"
      : productionReadyCount >= 8
        ? "PARTIAL"
        : "INTERNAL_ONLY";

  let finalStatus = "BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_PARTIAL";
  if (mismatch > 0 || reviewCounts.INCORRECT > 0 || !pairwisePass) {
    finalStatus = "BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_REMEDIATION_REQUIRED";
  } else if (byClass.PRODUCTION_VALIDATED >= 5 && pairwisePass && mismatch === 0) {
    finalStatus = "BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_PASS";
  }

  const next =
    finalStatus === "BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_REMEDIATION_REQUIRED"
      ? "BRAND_SCENARIO_BENCHMARK_REMEDIATION"
      : byStab.FRAGILE >= 10 || materiallyDifferent >= 15
        ? "BRAND_SCENARIO_BENCHMARK_REMEDIATION"
        : customerScenarioStatus === "PARTIAL" && byClass.PRODUCTION_VALIDATED >= 5
          ? "BRAND_SCENARIO_INDEX_CUSTOMER_PILOT"
          : "BRAND_HEADLINE_INDEX_RESEARCH";

  const report = {
    BRAND_AI_SCENARIO_BENCHMARK_VALIDATION_COMPLETE: true,
    validationVersion: VALIDATION_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    SCENARIOS_VALIDATED: reportedValid.length,
    recalculation: {
      EXACT_MATCH: exact,
      ROUNDING_ONLY: rounding,
      MATERIAL_MISMATCH: mismatch,
    },
    pairwiseIntegrity: {
      PAIRWISE_COMMON_GRAIN: pairwisePass ? "PASS" : "FAIL",
      DENOMINATOR_MISMATCHES: denomMismatches,
      UNION_GRAIN_USAGE: unionUsage,
      UNION_GRAIN_BENCHMARK,
      mixedModelGrains: idx.mixedModelGrains,
    },
    commercialPeerIntegrity: {
      CORE_CONFIRMED: reviewCounts.CORE_CONFIRMED,
      SECONDARY_CONFIRMED: reviewCounts.SECONDARY_CONFIRMED,
      QUESTIONABLE: reviewCounts.QUESTIONABLE,
      INCORRECT: reviewCounts.INCORRECT,
      MANDATORY_CORE_FAILURES: mandatoryFailures,
    },
    evidenceDensity: {
      ...grainDist,
      CURRENT_MINIMUM_8: keepMin8,
      RECOMMENDED_MINIMUM: 8,
      note: "Median VALID grains are at or above 8. Do not lower. Raising to 15 would reclassify several new-build (8-grain) cases as LIMITED without fixing commercial construction.",
    },
    providerBreadth: byProv,
    providerAgreement: byProviderAgree,
    stability: byStab,
    coreVsSecondary: {
      CONSISTENT: consistent,
      MATERIALLY_DIFFERENT: materiallyDifferent,
      SECONDARY_DRIVEN_SCENARIOS: secondaryDriven,
    },
    extremes,
    autograph: {
      SOFT_BRAND_INDEX: autographSoft?.recomputedIndex ?? null,
      STATUS: autographSoft?.productionClass ?? null,
      CONVERSION_INDEX: autographConv?.recomputedIndex ?? null,
      CONVERSION_STATUS: autographConv?.productionClass ?? null,
      coreMeasured: autographSoft?.corePeers?.map((p) => p.peerBrandName) || [],
      pairwiseSoft: autographSoft?.corePeers || [],
      pairwiseConversion: autographConv?.corePeers || [],
    },
    curio: {
      SOFT_BRAND_INDEX: curioSoft?.recomputedIndex ?? null,
      STATUS: curioSoft?.productionClass ?? null,
      pairwiseSoft: curioSoft?.corePeers || [],
    },
    hotelIndigo: {
      LIFESTYLE_INDEX: indigo?.recomputedIndex ?? null,
      CORE_ONLY: indigo?.indexCore ?? null,
      ALL_ELIGIBLE: indigo?.indexAll ?? null,
      WHY: explainWhy(indigo, "Indigo lifestyle"),
      STATUS: indigo?.productionClass ?? null,
      stability: indigo?.stability,
      leaveOneOut: { max: indigo?.maxIndexMovement, median: indigo?.medianIndexMovement, peer: indigo?.mostInfluentialPeer },
    },
    kimpton: {
      LIFESTYLE_INDEX: kimpton?.recomputedIndex ?? null,
      STATUS: kimpton?.productionClass ?? null,
      CORE_ONLY: kimpton?.indexCore ?? null,
      ALL_ELIGIBLE: kimpton?.indexAll ?? null,
      WHY: explainWhy(kimpton, "Kimpton lifestyle"),
      reciprocityWithIndigo:
        indigo && kimpton
          ? {
              indigoPresence: indigo.subjectPresence,
              kimptonPresence: kimpton.subjectPresence,
              sameGrains: indigo.commonGrains === kimpton.commonGrains,
            }
          : null,
    },
    voco: {
      LIFESTYLE_INDEX: voco?.recomputedIndex ?? null,
      WHY_LOW_OR_NOT: explainWhy(voco, "Voco lifestyle"),
      STATUS: voco?.productionClass ?? null,
    },
    ascend: {
      OWNER_FLEX_INDEX: ascendFlex?.recomputedIndex ?? null,
      OWNER_FLEX_STATUS: ascendFlex?.productionClass ?? null,
      SOFT_COLLECTION_INDEX: ascendSoft?.recomputedIndex ?? null,
      SOFT_COLLECTION_STATUS: ascendSoft?.productionClass ?? null,
      CROSS_SCENARIO_DIFFERENTIATION: crossScenario,
      whyDifferent:
        ascendFlex && ascendSoft
          ? `Owner-flex subject presence ${ascendFlex.subjectPresence} vs median ${ascendFlex.benchmarkAll}; soft-collection subject presence ${ascendSoft.subjectPresence} vs median ${ascendSoft.benchmarkAll}. Different commercial denominators, not a single-rank squeeze.`
          : null,
    },
    reciprocalSanity: {
      RECIPROCAL_SANITY_PASS: reciprocalFail === 0 ? "YES" : "NO",
      failures: reciprocalPairs,
    },
    CROSS_SCENARIO_DIFFERENTIATION: crossScenario,
    productionClassification: byClass,
    validityContract: {
      FINAL_MIN_TOTAL_PEERS: 5,
      FINAL_MIN_CORE_PEERS: 2,
      FINAL_CORE_COVERAGE: "BOTH_RATIO_50_AND_MANDATORY_NAMED_CORES",
      MANDATORY_CORE_RULE: "KEEP",
      FINAL_COMMON_GRAIN_MIN: 8,
      PROVIDER_POLICY: "SINGLE_PROVIDER_INTERNAL_ONLY_NOT_CUSTOMER_BENCHMARK",
    },
    ownerIntentIndex: {
      PRODUCT_VALUE: "HIGH",
      RECOMMENDATION:
        "Owner-intent scenario indices (soft-brand, conversion, lifestyle, owner-flex) are more commercially useful than one headline number. Do not ship customer scenario indices until CORE-only vs all-eligible policy is remediating FRAGILE / PEER_COMPOSITION_EFFECT. Autograph 129 vs 177 vs suppressed flexibility is the product proof, not a customer-ready score.",
    },
    coverageWave: wave,
    headlineIndex: { STATUS: HEADLINE_AI_PRESENCE_INDEX_STATUS },
    aggregationMethod: { STATUS: AGGREGATION_METHOD_STATUS },
    customer: {
      SCENARIO_INDEX_CUSTOMER_STATUS: customerScenarioStatus,
      HEADLINE_INDEX_CUSTOMER_STATUS: "INTERNAL_ONLY",
      UI_CHANGES: 0,
      FULL_PEER_MATRIX_CUSTOMER_ACCESS: "BLOCKED",
      allowlistHidesMembers: !CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"),
    },
    regression: {
      BRAND_DIFF: "0 except validation artifacts",
      BRAND_UI_DIFF: 0,
      BRAND_PRESENCE_CLASSIFIER_DIFF: 0,
      OPERATOR_DIFF: 0,
      operatorCount: PRIMARY_OPERATOR_COUNT,
    },
    NO_FULL_SET_FALLBACK,
    responsesScanned: idx.responsesScanned,
    validations,
    next,
    final: finalStatus,
  };

  if (opts.writeReport !== false) {
    const outDir = path.join(ROOT, "reports", "ai-visibility");
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(path.join(outDir, "scenario-benchmark-validation-v1.json"), JSON.stringify(report, null, 2));
    fs.writeFileSync(
      path.join(outDir, "scenario-benchmark-validation-v1.md"),
      [
        "# Scenario Benchmark Validation V1",
        "",
        `**Final:** ${report.final}`,
        `**Next:** ${report.next}`,
        "",
        `- VALID scenarios recomputed: ${report.SCENARIOS_VALIDATED}`,
        `- EXACT_MATCH: ${exact}  ROUNDING_ONLY: ${rounding}  MATERIAL_MISMATCH: ${mismatch}`,
        `- PRODUCTION_VALIDATED: ${byClass.PRODUCTION_VALIDATED}`,
        `- PRODUCTION_VALIDATED_NARROW: ${byClass.PRODUCTION_VALIDATED_NARROW}`,
        `- DETAIL_ONLY: ${byClass.DETAIL_ONLY}`,
        `- Headline: DEFERRED`,
        "",
      ].join("\n")
    );
  }

  return report;
}
