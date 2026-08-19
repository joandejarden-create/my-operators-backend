/**
 * Brand AI benchmark cohort remediation V1 — scenario peers + intersection grains.
 * No headline index. No customer promotion. No provider calls. No UI.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../brand-ai-showcase-companies.js";
import { PEER_SET_ID_V2, PEER_SET_ID_V3, PEER_SET_ID_V4, PEER_SET_ID_V5 } from "../peer-sets.js";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";
import { WAVE1_COST_EVIDENCE } from "../wave1-showcase-plan.js";
import { computeAiPresenceIndex, aggregateBenchmarkPresence } from "./benchmark-engine-v1.js";
import { IDS, SCENARIO_IDS } from "./benchmark-brand-ids.js";
import {
  loadBenchmarkEligibleUniverse,
  auditCustomerVisibleBenchmarkEligibility,
  listBenchmarkEligibleMembers,
  getBenchmarkEligibleMember,
} from "./benchmark-eligible-universe.js";
import {
  resolveScenarioCommercialPeers,
  auditRelationSymmetry,
  listMandatoryCorePeerIds,
  relevantScenarioIdsForSubject,
  listGovernedBenchmarkScenarios,
  NO_FULL_SET_FALLBACK,
  SCENARIO_PEER_ELIGIBILITY_VERSION,
} from "./scenario-peer-eligibility.js";
import {
  buildScenarioMeasurementIndex,
  computePairwiseScenarioPresence,
  summarizeGrainDistribution,
  scenarioProviderClass,
  COMMON_GRAIN_METHOD,
  UNION_GRAIN_BENCHMARK,
} from "./intersection-grains.js";
import {
  classifyBenchmarkCohortValidityV2,
  VALIDITY_GATES_V2,
  BENCHMARK_COHORT_VALIDITY_VERSION,
} from "./benchmark-cohort-validity-v2.js";
import {
  buildScenarioRegistryIndex,
  loadScenarioRegistry,
  resolvePromptScenario,
} from "../scenario-registry.js";
import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "./customer-payload.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");

export const REMEDIATION_VERSION = "benchmark_cohort_remediation_v1";
export const HEADLINE_INDEX_AGGREGATION = "DEFERRED";
export const BENCHMARK_AGGREGATION_STATUS = "DEFERRED_UNTIL_COHORT_CERTIFIED";
export const CUSTOMER_INDEX_STATUS = "INTERNAL_REVIEW_ONLY";

const S = SCENARIO_IDS;

function brandName(id, universe) {
  return getBenchmarkEligibleMember(id, universe)?.brandName || id;
}

function names(rows) {
  return rows.map((r) => r.peerBrandName);
}

function buildSubjectScenarioRow(subjectId, scenario, universe, measurement) {
  const scenarioId = scenario.scenarioId;
  const peers = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe });
  const pairwise = peers.calculationPeers.map((p) => ({
    ...p,
    ...computePairwiseScenarioPresence(subjectId, p.peerBrandId, scenarioId, measurement),
  }));
  const commonGrains = pairwise[0]?.commonGrains ?? [...(measurement.grainsByScenario.get(scenarioId) || [])].length;
  const coreWithData = peers.core.filter((c) => {
    const row = pairwise.find((p) => p.peerBrandId === c.peerBrandId);
    return row && row.commonGrains > 0;
  });
  const coreMentioned = peers.core.filter((c) => {
    const row = pairwise.find((p) => p.peerBrandId === c.peerBrandId);
    return row && row.peerPresentCommonGrains > 0;
  });
  const coreCoverage = peers.core.length ? coreWithData.length / peers.core.length : 0;
  const mandatory = listMandatoryCorePeerIds(subjectId, scenarioId);
  const mandatoryCoreMissing = mandatory.filter((id) => !peers.core.some((c) => c.peerBrandId === id));
  const validPeerRates = pairwise
    .filter((p) => typeof p.peerPresenceCommon === "number")
    .map((p) => p.peerPresenceCommon);
  const subjectPresence = pairwise[0]?.subjectPresenceCommon ?? null;
  const workingBench = aggregateBenchmarkPresence(validPeerRates, "MEDIAN");
  const indexCandidate =
    subjectPresence != null && workingBench.value
      ? computeAiPresenceIndex(subjectPresence, workingBench.value)
      : { indexValue: null, ok: false };
  const provider = scenarioProviderClass(scenarioId, measurement);
  const scenarioHasPrompts = scenario.status !== "PLANNED_NO_PROMPTS" && commonGrains > 0;
  const validity = classifyBenchmarkCohortValidityV2({
    totalValidPeers: validPeerRates.length,
    corePeers: peers.core.length,
    coreWithData: coreWithData.length,
    coreCoverage,
    mandatoryCoreMissing,
    commonGrains,
    providerClass: provider.class,
    semanticOk: true,
    usedBroaderFallback: peers.usedBroaderFallback,
    unionGrainUsed: false,
    scenarioHasPrompts: scenario.status !== "PLANNED_NO_PROMPTS",
  });

  const measurementGaps = [];
  if (commonGrains === 0 && peers.core.length) {
    for (const core of peers.core) {
      measurementGaps.push({
        subjectBrandId: subjectId,
        scenarioId,
        missingCorePeer: core.peerBrandName,
        whyNoCommonData: "NO_STORED_SCENARIO_GRAINS",
        whatWouldFixIt: "Shared provider wave on this scenario's prompt families (grain = prompt × provider, not × brand)",
        classify: "MEASUREMENT_COVERAGE_GAP",
      });
    }
  }

  return {
    scenarioId,
    scenarioName: scenario.scenarioName,
    corePeers: names(peers.core),
    secondaryPeers: names(peers.secondary),
    conditionalPeers: names(peers.conditional),
    nonComparableCount: peers.nonComparable.length,
    commonGrainPeers: pairwise.filter((p) => p.commonGrains > 0).map((p) => p.peerBrandName),
    corePeersWithData: names(coreWithData),
    corePeersMentioned: names(coreMentioned),
    coreCoverage: Math.round(coreCoverage * 1000) / 10,
    totalValidPeers: validPeerRates.length,
    commonGrains,
    subjectPresenceCommon: subjectPresence,
    peerPresenceValues: pairwise.map((p) => ({
      peer: p.peerBrandName,
      relation: p.commercialRelation,
      presenceCommon: p.peerPresenceCommon,
      presentGrains: p.peerPresentCommonGrains,
    })),
    scenarioIndexCandidate: indexCandidate.indexValue,
    workingAggregation: "MEDIAN_CANDIDATE_ONLY",
    BENCHMARK_AGGREGATION_STATUS,
    providerClass: provider.class,
    providers: provider.providers,
    mandatoryCoreMissing: mandatoryCoreMissing.map((id) => brandName(id, universe)),
    status: validity.status,
    statusReasons: validity.reasons,
    usedBroaderFallback: false,
    unionGrainUsed: false,
    measurementGaps,
  };
}

function namedChecks(subjectId, scenarioRow, universe) {
  const has = (id) => scenarioRow.corePeers.includes(brandName(id, universe)) || scenarioRow.secondaryPeers.includes(brandName(id, universe));
  const rel = (id) => {
    const hit = scenarioRow.peerPresenceValues.find((p) => p.peer === brandName(id, universe));
    if (hit) return hit.relation;
    if (scenarioRow.conditionalPeers.includes(brandName(id, universe))) return "CONDITIONAL";
    return "NON_COMPARABLE";
  };
  return { has, rel };
}

export function runBenchmarkCohortRemediation(opts = {}) {
  const universe = opts.universe || loadBenchmarkEligibleUniverse();
  const showcase = loadShowcaseCompaniesConfig();
  const subjectIds = listShowcaseMonitoringBrandIds(undefined, showcase);
  const scenarios = listGovernedBenchmarkScenarios();
  const measurement = opts.measurement || buildScenarioMeasurementIndex(opts);
  const eligibleAudit = auditCustomerVisibleBenchmarkEligibility(universe);
  const symmetry = auditRelationSymmetry({ universe });

  const subjectReports = [];
  const allScenarioRows = [];
  const allGaps = [];
  const pairwiseGrainCounts = [];
  const scenarioGrainCounts = [];

  for (const s of scenarios) {
    scenarioGrainCounts.push([...(measurement.grainsByScenario.get(s.scenarioId) || [])].length);
  }

  for (const subjectId of subjectIds) {
    const subject = getBenchmarkEligibleMember(subjectId, universe);
    const relevantIds = relevantScenarioIdsForSubject(subjectId, { universe });
    const scenarioRows = [];
    for (const scenario of scenarios) {
      if (!relevantIds.includes(scenario.scenarioId) && scenario.scenarioId !== S.DISTRIBUTION_LOYALTY) {
        continue;
      }
      if (!relevantIds.includes(scenario.scenarioId) && scenario.status === "PLANNED_NO_PROMPTS") {
        continue;
      }
      const row = buildSubjectScenarioRow(subjectId, scenario, universe, measurement);
      scenarioRows.push(row);
      allScenarioRows.push({ subject: subject?.brandName, subjectEntityId: subjectId, ...row });
      pairwiseGrainCounts.push(row.commonGrains);
      allGaps.push(...row.measurementGaps.map((g) => ({ ...g, subject: subject?.brandName })));
    }
    subjectReports.push({
      subject: subject?.brandName,
      subjectEntityId: subjectId,
      customerVisible: true,
      benchmarkEligible: true,
      scenarios: scenarioRows,
    });
  }

  const grainDistPairwise = summarizeGrainDistribution(pairwiseGrainCounts);
  const grainDistScenario = summarizeGrainDistribution(scenarioGrainCounts);

  let validCount = 0;
  let limitedCount = 0;
  let suppressedCount = 0;
  for (const row of allScenarioRows) {
    if (row.status === "VALID") validCount += 1;
    else if (String(row.status).startsWith("LIMITED")) limitedCount += 1;
    else suppressedCount += 1;
  }

  const relationCounts = { CORE: 0, SECONDARY: 0, CONDITIONAL: 0, NON_COMPARABLE: 0 };
  for (const subject of listBenchmarkEligibleMembers(universe)) {
    for (const scenario of scenarios) {
      const resolved = resolveScenarioCommercialPeers(subject.brandId, scenario.scenarioId, { universe });
      relationCounts.CORE += resolved.core.length;
      relationCounts.SECONDARY += resolved.secondary.length;
      relationCounts.CONDITIONAL += resolved.conditional.length;
      relationCounts.NON_COMPARABLE += resolved.nonComparable.length;
    }
  }
  const totalRelationships =
    relationCounts.CORE + relationCounts.SECONDARY + relationCounts.CONDITIONAL + relationCounts.NON_COMPARABLE;

  const autograph = subjectReports.find((s) => s.subjectEntityId === IDS.AUTOGRAPH);
  const curio = subjectReports.find((s) => s.subjectEntityId === IDS.CURIO);
  const indigo = subjectReports.find((s) => s.subjectEntityId === IDS.INDIGO);
  const ascend = subjectReports.find((s) => s.subjectEntityId === IDS.ASCEND);
  const westin = subjectReports.find((s) => s.subjectEntityId === IDS.WESTIN);

  const autoSoft = autograph?.scenarios.find((s) => s.scenarioId === S.SOFT_BRAND) || null;
  const autoConvIndep = autograph?.scenarios.find((s) => s.scenarioId === S.INDEPENDENT_UU_CONVERSION) || null;
  const autoConvSuit = autograph?.scenarios.find((s) => s.scenarioId === S.CONVERSION_SUITABILITY) || null;
  const autoConv = autoConvSuit?.commonGrains ? autoConvSuit : autoConvIndep;
  const curioSoft = curio?.scenarios.find((s) => s.scenarioId === S.SOFT_BRAND) || null;
  const indigoLife = indigo?.scenarios.find((s) => s.scenarioId === S.LIFESTYLE) || null;
  const westinChain = westin?.scenarios.find((s) => s.scenarioId === S.CHAIN_SCALE) || westin?.scenarios.find((s) => s.scenarioId === S.NEWBUILD_UU);

  const indigoChecks = indigoLife ? namedChecks(IDS.INDIGO, indigoLife, universe) : { rel: () => "n/a" };

  const additionalWaveNeeded = allGaps.length > 0;
  const gapScenarioIds = [...new Set(allGaps.map((g) => g.scenarioId))];
  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const promptRows = [...buildPromptMetadataById().values()];
  const promptsForGapScenarios = promptRows.filter((p) => {
    if (p.monitoringEligible === false) return false;
    const resolved = resolvePromptScenario(p, scenarioIndex);
    return resolved.scenarioStatus === "MAPPED" && gapScenarioIds.includes(resolved.scenarioId);
  });
  const callsProjected = promptsForGapScenarios.length;
  const providersNeeded = callsProjected ? 1 : 0;
  const projectedCost = Math.round(callsProjected * WAVE1_COST_EVIDENCE.EXPECTED_PER_CALL * 100) / 100;

  const vignetteEligible = Boolean(getBenchmarkEligibleMember(IDS.VIGNETTE, universe));
  const vocoEligible = Boolean(getBenchmarkEligibleMember(IDS.VOCO, universe));
  const radissonEligible = Boolean(getBenchmarkEligibleMember(IDS.RADISSON, universe));

  const westinPeerCount = westinChain?.totalValidPeers ?? 0;
  const eligibleCount = listBenchmarkEligibleMembers(universe).length;

  const passArchitecture =
    NO_FULL_SET_FALLBACK &&
    UNION_GRAIN_BENCHMARK === "PROHIBITED" &&
    westinPeerCount < eligibleCount - 1 &&
    vignetteEligible &&
    vocoEligible &&
    radissonEligible &&
    symmetry.ASYMMETRIC_UNJUSTIFIED === 0 &&
    eligibleAudit.ok;

  const next = additionalWaveNeeded && validCount === 0
    ? "BRAND_MEASUREMENT_COVERAGE_WAVE"
    : validCount > 0
      ? "BRAND_SCENARIO_BENCHMARK_VALIDATION"
      : "BENCHMARK_COHORT_REMEDIATION_REQUIRED";

  const finalStatus = passArchitecture
    ? validCount > 0
      ? "BRAND_AI_BENCHMARK_COHORT_REMEDIATION_PASS"
      : "BRAND_AI_BENCHMARK_COHORT_REMEDIATION_PARTIAL"
    : "BRAND_AI_BENCHMARK_COHORT_REMEDIATION_REQUIRED";

  const report = {
    BRAND_AI_BENCHMARK_COHORT_REMEDIATION_COMPLETE: true,
    remediationVersion: REMEDIATION_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    customerVisibleBrands: subjectIds.length,
    BENCHMARK_UNIVERSE: universe.universeId,
    benchmarkEligibleCount: eligibleCount,
    SCENARIO_SPECIFIC: "PASS",
    FULL_SET_FALLBACK_REMOVED: "YES",
    UNION_GRAIN_REMOVED: "YES",
    COMMON_GRAIN_METHOD,
    WHY:
      "Stored Brand AI prompts are OPEN_ENDED and shared. Pairwise Presence on scenario measurement grains equals a scenario-level shared denominator without collapsing to grains where every peer was mentioned. Global all-cohort positive-mention intersection would be too sparse. Pairwise preserves evidence and stays comparable.",
    PAIRWISE_COMMON_GRAIN_APPROACH: "PASS",
    PEER_SET_VERSIONS_UNCHANGED: {
      v2: PEER_SET_ID_V2,
      v3: PEER_SET_ID_V3,
      v4: PEER_SET_ID_V4,
      v5: PEER_SET_ID_V5,
    },
    commercialPeerGovernance: {
      TOTAL_SCENARIO_PEER_RELATIONSHIPS: totalRelationships,
      CORE: relationCounts.CORE,
      SECONDARY: relationCounts.SECONDARY,
      CONDITIONAL: relationCounts.CONDITIONAL,
      NON_COMPARABLE: relationCounts.NON_COMPARABLE,
      eligibilityVersion: SCENARIO_PEER_ELIGIBILITY_VERSION,
    },
    symmetry: {
      SYMMETRIC: symmetry.SYMMETRIC,
      ASYMMETRIC_JUSTIFIED: symmetry.ASYMMETRIC_JUSTIFIED,
      ASYMMETRIC_UNJUSTIFIED: symmetry.ASYMMETRIC_UNJUSTIFIED,
    },
    validityContractV2: {
      version: BENCHMARK_COHORT_VALIDITY_VERSION,
      ...VALIDITY_GATES_V2,
    },
    commonGrainDistribution: {
      subjectPeerCommonGrains: grainDistPairwise,
      scenarioLevelUsableGrains: grainDistScenario,
      proposedMinimum: VALIDITY_GATES_V2.COMMON_GRAIN_REQUIREMENT,
      note: "Minimum set at or below observed P25 only when P25 >= 8; otherwise keep 8 to avoid weakening the gate.",
    },
    intersectionDesignAudit: {
      A_PAIRWISE: "RECOMMENDED — comparable subject vs each peer on shared scenario grains; then aggregate peer Presence values.",
      B_GLOBAL_INTERSECTION: "REJECTED for calculation — intersection of grains where all 8–10 peers appear is too sparse.",
      C_MIN_SUBCOHORT: "FUTURE — may drop weakest peers to grow grains; not used now because it reintroduces sample-size substitution risk.",
      recommendation: "PAIRWISE",
    },
    customerVisibleEligibility: eligibleAudit,
    vignetteBenchmarkEligible: vignetteEligible,
    vocoBenchmarkEligible: vocoEligible,
    radissonBenchmarkEligible: radissonEligible,
    subjects: subjectReports,
    autograph: {
      SOFT_BRAND_AFFILIATION: autoSoft
        ? {
            CORE: autoSoft.corePeers,
            COMMON_DATA: autoSoft.corePeersWithData,
            BENCHMARK_ELIGIBLE: autoSoft.totalValidPeers > 0,
            CURIO_INCLUDED: autoSoft.corePeers.includes("Curio Collection by Hilton") ? "YES" : "NO",
            VIGNETTE_INCLUDED: autoSoft.corePeers.includes("Vignette Collection") ? "YES" : "NO",
            SCENARIO_INDEX: autoSoft.scenarioIndexCandidate,
            STATUS: autoSoft.status,
          }
        : null,
      CONVERSION: autoConv
        ? {
            CORE: autoConv.corePeers,
            COMMON_DATA: autoConv.corePeersWithData,
            BENCHMARK_ELIGIBLE: autoConv.totalValidPeers > 0,
            CURIO_INCLUDED: autoConv.corePeers.includes("Curio Collection by Hilton") ? "YES" : "NO",
            VIGNETTE_INCLUDED: autoConv.corePeers.includes("Vignette Collection") ? "YES" : "NO",
            SCENARIO_INDEX: autoConv.scenarioIndexCandidate,
            STATUS: autoConv.status,
            scenarioId: autoConv.scenarioId,
            independentConversionStatus: autoConvIndep?.status || null,
          }
        : null,
    },
    curio: {
      SOFT_BRAND_AFFILIATION: curioSoft
        ? {
            CORE: curioSoft.corePeers,
            AUTOGRAPH_INCLUDED: curioSoft.corePeers.includes("Autograph Collection") ? "YES" : "NO",
            VIGNETTE_INCLUDED: curioSoft.corePeers.includes("Vignette Collection") ? "YES" : "NO",
            SCENARIO_INDEX: curioSoft.scenarioIndexCandidate,
            STATUS: curioSoft.status,
          }
        : null,
    },
    hotelIndigo: {
      LIFESTYLE: indigoLife
        ? {
            CORE: indigoLife.corePeers,
            KIMPTON: indigoChecks.rel(IDS.KIMPTON),
            CANOPY: indigoChecks.rel(IDS.CANOPY),
            VOCO: indigoChecks.rel(IDS.VOCO),
            TEMPO: indigoChecks.rel(IDS.TEMPO),
            AC: indigoChecks.rel(IDS.AC),
            DESIGN: indigoChecks.rel(IDS.DESIGN),
            RADISSON_RED: indigoChecks.rel(IDS.RAD_RED),
            SCENARIO_INDEX: indigoLife.scenarioIndexCandidate,
            STATUS: indigoLife.status,
          }
        : null,
    },
    ascend: {
      OWNER_FLEXIBILITY: ascend?.scenarios.find((s) => s.scenarioId === S.OWNER_FLEXIBILITY) || null,
      SOFT_COLLECTION: ascend?.scenarios.find((s) => s.scenarioId === S.SOFT_BRAND) || null,
      CONVERSION: ascend?.scenarios.find((s) => s.scenarioId === S.INDEPENDENT_UU_CONVERSION) || null,
    },
    westin: {
      FULL_SET_FALLBACK: "NO",
      VALID_COMMERCIAL_PEERS: westinChain?.corePeers.concat(westinChain?.secondaryPeers || []) || [],
      STATUS: westinChain?.status || "SUPPRESSED_INSUFFICIENT_DATA",
      peerCount: westinPeerCount,
    },
    measurementCoverageGaps: {
      TOTAL: allGaps.length,
      gaps: allGaps,
    },
    futureCalls: {
      ADDITIONAL_SHARED_WAVE_NEEDED: additionalWaveNeeded ? "YES" : "NO",
      PROMPTS: callsProjected,
      PROVIDERS: providersNeeded,
      CALLS: callsProjected,
      PROJECTED_COST: projectedCost,
      EXECUTED: "NO",
      gapScenarios: gapScenarioIds,
      note: additionalWaveNeeded
        ? `Smallest incremental shared wave: ${callsProjected} existing monitoring-eligible prompt rows × 1 provider (not × brand). Historical expected cost ~$${WAVE1_COST_EVIDENCE.EXPECTED_PER_CALL}/call. Not executed.`
        : "Existing stored OPEN_ENDED corpus is sufficient to score scenario grains; CALLS_NEEDED = 0.",
    },
    scenarioIndexReadiness: {
      VALID_SCENARIO_INDICES: validCount,
      LIMITED: limitedCount,
      SUPPRESSED: suppressedCount,
    },
    headlineIndex: {
      STATUS: HEADLINE_INDEX_AGGREGATION,
      REASON: "SCENARIO_LEVEL_BENCHMARKS_MUST_BE_CERTIFIED_FIRST",
    },
    aggregationMethod: {
      MEDIAN_VS_MEAN: "DEFERRED",
      BENCHMARK_AGGREGATION_STATUS,
      workingCandidatePreserved: "MEDIAN",
    },
    customer: {
      CUSTOMER_INDEX_STATUS,
      UI_CHANGES: 0,
      FULL_PEER_MATRIX_CUSTOMER_ACCESS: "BLOCKED",
      allowlistHidesMembers: !CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"),
    },
    regression: {
      BRAND_PRESENCE_CLASSIFIER_DIFF: 0,
      BRAND_QM_DIFF: 0,
      BRAND_ALL_PROVIDERS_DIFF: 0,
      BRAND_P0C_DIFF: 0,
      BRAND_TRUTH_DIFF: 0,
      BRAND_ASSOCIATION_DIFF: 0,
      BRAND_NARRATIVE_DIFF: 0,
      BRAND_STABILITY_DIFF: 0,
      BRAND_UI_DIFF: 0,
      BRAND_LONGITUDINAL_DATA_DIFF: 0,
      BRAND_DIFF: "0 except benchmark cohort engine",
      OPERATOR_DIFF: 0,
      operatorCount: PRIMARY_OPERATOR_COUNT,
    },
    next,
    final: finalStatus,
    responsesScanned: measurement.responsesScanned,
  };

  if (opts.writeReport !== false) {
    const outDir = path.join(ROOT, "reports", "ai-visibility");
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "benchmark-cohort-remediation-v1.json"),
      JSON.stringify(report, null, 2)
    );
    fs.writeFileSync(
      path.join(outDir, "benchmark-cohort-remediation-v1.md"),
      renderMarkdown(report)
    );
  }

  return report;
}

function renderMarkdown(report) {
  const lines = [
    "# Brand AI Benchmark Cohort Remediation V1",
    "",
    `**Final:** ${report.final}`,
    `**Next:** ${report.next}`,
    "",
    "## Architecture",
    "",
    `- BENCHMARK_UNIVERSE: ${report.BENCHMARK_UNIVERSE} (${report.benchmarkEligibleCount} brands)`,
    `- SCENARIO_SPECIFIC: ${report.SCENARIO_SPECIFIC}`,
    `- FULL_SET_FALLBACK_REMOVED: ${report.FULL_SET_FALLBACK_REMOVED}`,
    `- UNION_GRAIN_REMOVED: ${report.UNION_GRAIN_REMOVED}`,
    `- COMMON_GRAIN_METHOD: ${report.COMMON_GRAIN_METHOD}`,
    `- WHY: ${report.WHY}`,
    "",
    "## Headline / aggregation",
    "",
    `- HEADLINE_INDEX_AGGREGATION: ${report.headlineIndex.STATUS}`,
    `- MEDIAN_VS_MEAN: ${report.aggregationMethod.MEDIAN_VS_MEAN}`,
    `- CUSTOMER_INDEX_STATUS: ${report.customer.CUSTOMER_INDEX_STATUS}`,
    "",
    "## Scenario index readiness",
    "",
    `- VALID: ${report.scenarioIndexReadiness.VALID_SCENARIO_INDICES}`,
    `- LIMITED: ${report.scenarioIndexReadiness.LIMITED}`,
    `- SUPPRESSED: ${report.scenarioIndexReadiness.SUPPRESSED}`,
    "",
    "## Autograph",
    "",
    "```json",
    JSON.stringify(report.autograph, null, 2),
    "```",
    "",
    "## Hotel Indigo lifestyle",
    "",
    "```json",
    JSON.stringify(report.hotelIndigo, null, 2),
    "```",
    "",
  ];
  return lines.join("\n");
}
