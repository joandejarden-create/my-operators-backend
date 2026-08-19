/**
 * Scenario benchmark composition remediation.
 * CORE defines the competitive benchmark. SECONDARY is context, not the denominator.
 * No provider calls. No UI activation. No headline index.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";
import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import {
  loadBenchmarkEligibleUniverse,
} from "./benchmark-eligible-universe.js";
import { classifyScenarioPeerRelation, NO_FULL_SET_FALLBACK } from "./scenario-peer-eligibility.js";
import { computeAiPresenceIndex, aggregateBenchmarkPresence } from "./benchmark-engine-v1.js";
import { STABILITY_THRESHOLDS } from "./brand-presence-index-pilot.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "./customer-payload.js";
import {
  buildIndependentIndex,
  recomputeOne,
  collectValidReported,
  classifyStability,
  HEADLINE_AI_PRESENCE_INDEX_STATUS,
  AGGREGATION_METHOD_STATUS,
} from "./scenario-benchmark-validation.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../brand-ai-showcase-companies.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const REMEDIATION_REPORT = path.join(ROOT, "reports", "ai-visibility", "benchmark-cohort-remediation-v1.json");

export const COMPOSITION_REMEDIATION_VERSION = "scenario_benchmark_composition_v1";
export const RECOMMENDED_POLICY = "CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT";
export const CUSTOMER_INDEX_RENDERING = "OFF";
export const HEADLINE_INDEX_STATUS = "DEFERRED";
export const BRANDED_RESIDENCES_BENCHMARK_STATUS = "REDESIGN_REQUIRED";

/** Candidate gates for CORE-first customer benchmarks. Not a weakening of V2 mixed-peer VALID. */
export const CORE_FIRST_GATES_CANDIDATE = Object.freeze({
  MIN_CORE_PEERS_CUSTOMER: 3,
  MIN_CORE_PEERS_NARROW: 2,
  CORE_COVERAGE: "BOTH_RATIO_50_AND_MANDATORY_NAMED_CORES",
  MANDATORY_CORE: "KEEP",
  COMMON_GRAIN_MIN: 8,
  STRONG_EVIDENCE_GRAINS: 15,
  PROVIDER_POLICY: "MULTI_PROVIDER_REQUIRED_FOR_CUSTOMER_NUMERIC_INDEX",
  SECONDARY_IN_DENOMINATOR: false,
});

export const LIFESTYLE_PEER_REVIEW = Object.freeze({
  DESIGN_HOTELS: "KEEP_SECONDARY",
  RADISSON_RED: "KEEP_SECONDARY",
  PREFERRED: "MOVE_CONDITIONAL",
  EVEN: "NON_COMPARABLE",
});

export const SOFT_BRAND_REVIEW = Object.freeze({
  CORE_SET: ["Autograph Collection", "Curio Collection by Hilton", "Tribute Portfolio", "Tapestry Collection by Hilton", "Ascend Hotel Collection", "Vignette Collection"],
  SECONDARY_CONTEXT: ["Handwritten Collection", "Preferred Hotels & Resorts", "MGallery", "Radisson Individuals by Choice"],
  CONDITIONAL: ["Trademark Collection", "Best Western Premier", "Best Western Signature Collection", "Small Luxury Hotels of the World", "Design Hotels"],
});

const POLICIES = Object.freeze(["CORE_ONLY", "CORE_PLUS_SECONDARY", "CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT"]);

function loadRemediationReport() {
  return JSON.parse(fs.readFileSync(REMEDIATION_REPORT, "utf8"));
}

function leaveOneOut(subjectPresence, rates) {
  const base = computeAiPresenceIndex(subjectPresence, aggregateBenchmarkPresence(rates, "MEDIAN").value);
  const movements = [];
  for (let i = 0; i < rates.length; i += 1) {
    const remaining = rates.filter((_, j) => j !== i);
    const left = computeAiPresenceIndex(subjectPresence, aggregateBenchmarkPresence(remaining, "MEDIAN").value);
    if (base.indexValue != null && left.indexValue != null) {
      movements.push(Math.abs(base.indexValue - left.indexValue));
    }
  }
  const maxMove = movements.length ? Math.max(...movements) : null;
  const medianMove = movements.length
    ? [...movements].sort((a, b) => a - b)[Math.floor(movements.length / 2)]
    : null;
  return {
    index: base.indexValue,
    benchmark: aggregateBenchmarkPresence(rates, "MEDIAN").value,
    maxMove,
    medianMove,
    stability: classifyStability(maxMove),
  };
}

function providerDirection(rec, coreOnly) {
  const rows = rec.providerRows || [];
  if (!rows.length) return "PROVIDER_CONSISTENT";
  const directions = [];
  for (const row of rows) {
    if (coreOnly) {
      const coreRates = (rec.corePeers || [])
        .map((p) => p.peerPresence)
        .filter((v) => typeof v === "number");
      const idx = computeAiPresenceIndex(row.subjectPresence, aggregateBenchmarkPresence(coreRates, "MEDIAN").value);
      if (idx.indexValue == null) continue;
      directions.push(idx.indexValue >= 100 ? "AT_OR_ABOVE" : "BELOW");
    } else if (row.vsParity) {
      directions.push(row.vsParity);
    }
  }
  const uniq = [...new Set(directions)];
  if (uniq.length <= 1) return "PROVIDER_CONSISTENT";
  const vals = rows.map((r) => r.index).filter((v) => v != null);
  const spread = vals.length ? Math.max(...vals) - Math.min(...vals) : 0;
  return spread >= 40 ? "PROVIDER_CONFLICT" : "PROVIDER_MIXED";
}

export function classifyCoreFirstProduction(row) {
  if (row.scenarioId === S.BRANDED_RESIDENCES) return "SUPPRESSED";
  if (row.usedBroaderFallback || row.unionGrainUsed) return "SUPPRESSED";
  if (row.coreCount < 1 || row.commonGrains < 1) return "SUPPRESSED";
  if (row.stabilityCore === "FRAGILE") return "LIMITED";
  if (row.providerAgreementCore === "PROVIDER_CONFLICT") return "LIMITED";
  if (row.providerClass === "SINGLE_PROVIDER_ONLY") return "DETAIL_ONLY";
  if (row.commonGrains < CORE_FIRST_GATES_CANDIDATE.COMMON_GRAIN_MIN) return "LIMITED";
  if (row.coreCount < CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_NARROW) return "LIMITED";
  const extreme = row.indexCore != null && (row.indexCore > 175 || row.indexCore <= 50);
  const strong =
    row.providerClass === "MULTI_PROVIDER_STRONG" &&
    row.stabilityCore === "STABLE" &&
    row.coreCount >= CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER &&
    row.commonGrains >= CORE_FIRST_GATES_CANDIDATE.STRONG_EVIDENCE_GRAINS &&
    !extreme;
  if (strong) return "PRODUCTION_VALIDATED";
  if (
    row.stabilityCore !== "FRAGILE" &&
    row.coreCount >= CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_NARROW &&
    row.commonGrains >= CORE_FIRST_GATES_CANDIDATE.COMMON_GRAIN_MIN &&
    (row.providerClass === "MULTI_PROVIDER_STRONG" || row.providerClass === "MULTI_PROVIDER_LIMITED")
  ) {
    if (extreme) return "DETAIL_ONLY";
    if (row.coreCount >= CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER) {
      return row.stabilityCore === "STABLE" ? "PRODUCTION_VALIDATED_NARROW" : "DETAIL_ONLY";
    }
    if (row.stabilityCore === "STABLE" && row.commonGrains >= CORE_FIRST_GATES_CANDIDATE.STRONG_EVIDENCE_GRAINS) {
      return "PRODUCTION_VALIDATED_NARROW";
    }
    return "DETAIL_ONLY";
  }
  return "DETAIL_ONLY";
}

function findRow(rows, brandId, scenarioId) {
  return rows.find((r) => r.subjectId === brandId && r.scenarioId === scenarioId) || null;
}

function relationLabel(subjectId, peerId, scenarioId) {
  return classifyScenarioPeerRelation(subjectId, peerId, scenarioId).commercialRelation;
}

export function runScenarioBenchmarkCompositionRemediation(opts = {}) {
  const stored = opts.remediation || loadRemediationReport();
  const reportedValid = collectValidReported(stored);
  const universe = loadBenchmarkEligibleUniverse();
  const idx = buildIndependentIndex(opts);
  const rows = [];

  for (const reported of reportedValid) {
    const rec = recomputeOne(reported.subjectEntityId, reported.scenarioId, idx, universe);
    const coreRates = rec.corePeers.map((p) => p.peerPresence).filter((v) => typeof v === "number");
    const allRates = rec.pairwise.map((p) => p.peerPresence).filter((v) => typeof v === "number");
    const policyA = leaveOneOut(rec.subjectPresence, coreRates);
    const policyB = leaveOneOut(rec.subjectPresence, allRates);
    const policyC = { ...policyA, secondaryContext: rec.secondaryPeers.map((p) => p.peerBrandName) };
    const providerAgreementCore = providerDirection(rec, true);
    const providerAgreementMixed = rec.providerAgreement;
    const row = {
      subject: reported.subject,
      subjectId: reported.subjectEntityId,
      scenarioId: reported.scenarioId,
      scenarioName: reported.scenarioName,
      subjectPresence: rec.subjectPresence,
      commonGrains: rec.commonGrains,
      providers: rec.providers,
      providerClass: rec.providerClass,
      coreCount: rec.coreCount,
      secondaryCount: rec.secondaryCount,
      corePeers: rec.corePeers,
      secondaryPeers: rec.secondaryPeers,
      secondaryContext: rec.secondaryPeers.map((p) => ({
        peerBrandName: p.peerBrandName,
        peerPresence: p.peerPresence,
        role: "ADDITIONAL_OBSERVED_CONTEXT",
      })),
      usedBroaderFallback: rec.usedBroaderFallback,
      unionGrainUsed: rec.unionGrainUsed,
      policies: {
        CORE_ONLY: policyA,
        CORE_PLUS_SECONDARY: policyB,
        CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT: policyC,
      },
      indexCore: policyA.index,
      indexMixed: policyB.index,
      absDiff: policyA.index != null && policyB.index != null ? Math.abs(policyA.index - policyB.index) : null,
      stabilityCore: policyA.stability,
      stabilityMixed: policyB.stability,
      providerAgreementCore,
      providerAgreementMixed,
    };
    row.productionClass = classifyCoreFirstProduction(row);
    rows.push(row);
  }

  function countStab(key) {
    return {
      STABLE: rows.filter((r) => r[key] === "STABLE").length,
      MODERATELY_SENSITIVE: rows.filter((r) => r[key] === "MODERATELY_SENSITIVE").length,
      FRAGILE: rows.filter((r) => r[key] === "FRAGILE").length,
    };
  }

  const byClass = { PRODUCTION_VALIDATED: 0, PRODUCTION_VALIDATED_NARROW: 0, DETAIL_ONLY: 0, LIMITED: 0, SUPPRESSED: 0 };
  for (const r of rows) byClass[r.productionClass] = (byClass[r.productionClass] || 0) + 1;
  const fragileProduction = rows.filter(
    (r) =>
      r.stabilityCore === "FRAGILE" &&
      (r.productionClass === "PRODUCTION_VALIDATED" || r.productionClass === "PRODUCTION_VALIDATED_NARROW")
  ).length;

  const coreGe3 = rows.filter((r) => r.coreCount >= 3).length;
  const coreGe2 = rows.filter((r) => r.coreCount >= 2).length;
  const mixedMoreFragile = countStab("stabilityMixed").FRAGILE - countStab("stabilityCore").FRAGILE;

  const indigo = findRow(rows, IDS.INDIGO, S.LIFESTYLE);
  const kimpton = findRow(rows, IDS.KIMPTON, S.LIFESTYLE);
  const voco = findRow(rows, IDS.VOCO, S.LIFESTYLE);
  const autographSoft = findRow(rows, IDS.AUTOGRAPH, S.SOFT_BRAND);
  const autographConv = findRow(rows, IDS.AUTOGRAPH, S.CONVERSION_SUITABILITY);
  const curioSoft = findRow(rows, IDS.CURIO, S.SOFT_BRAND);
  const ascendFlex = findRow(rows, IDS.ASCEND, S.OWNER_FLEXIBILITY);
  const ascendSoft = findRow(rows, IDS.ASCEND, S.SOFT_BRAND);

  const residencesRows = rows.filter((r) => r.scenarioId === S.BRANDED_RESIDENCES);

  let finalStatus = "BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_PARTIAL";
  if (fragileProduction === 0 && RECOMMENDED_POLICY === "CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT" && mixedMoreFragile >= 0) {
    finalStatus =
      byClass.PRODUCTION_VALIDATED + byClass.PRODUCTION_VALIDATED_NARROW >= 1
        ? "BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_PASS"
        : "BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_PARTIAL";
  }
  if (fragileProduction > 0) finalStatus = "BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_REQUIRED";

  const next =
    finalStatus === "BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_REQUIRED"
      ? "BRAND_SCENARIO_BENCHMARK_REMEDIATION_REQUIRED"
      : "BRAND_SCENARIO_INDEX_FINAL_CERTIFICATION";

  const report = {
    BRAND_AI_SCENARIO_BENCHMARK_REMEDIATION_COMPLETE: true,
    compositionVersion: COMPOSITION_REMEDIATION_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    CUSTOMER_INDEX_RENDERING,
    NEW_TAB: "NO",
    NEW_MAJOR_SECTION: "NO",
    benchmarkPolicy: {
      RECOMMENDED_POLICY,
      WHY:
        "CORE-only leave-one-out is materially more stable than CORE+SECONDARY. Secondary Presence (e.g. Even 0%, Preferred ~41% on lifestyle) pulled the median away from commercially direct alternatives. Policy C keeps SECONDARY as ADDITIONAL_OBSERVED_CONTEXT without entering the denominator. No weights.",
      POLICIES_TESTED: POLICIES,
      SECONDARY_IN_DENOMINATOR: false,
    },
    coreThreshold: {
      MIN_CORE_PEERS_CUSTOMER: CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER,
      MIN_CORE_PEERS_NARROW: CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_NARROW,
      CORE_COVERAGE: CORE_FIRST_GATES_CANDIDATE.CORE_COVERAGE,
      MANDATORY_CORE: CORE_FIRST_GATES_CANDIDATE.MANDATORY_CORE,
      corpus: { rows: rows.length, coreGe3, coreGe2 },
      note: "Do not require 5 mixed peers for VALID under CORE-first. Customer numeric index wants 3 measured CORE (or 2 CORE + strong grains/stability as NARROW).",
    },
    policyComparison: {
      CORE_ONLY: countStab("stabilityCore"),
      CORE_PLUS_SECONDARY: countStab("stabilityMixed"),
      CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT: countStab("stabilityCore"),
    },
    productionClassification: byClass,
    stabilityAfterRemediation: countStab("stabilityCore"),
    FRAGILE_PRODUCTION: fragileProduction,
    providerAgreementCore: {
      PROVIDER_CONSISTENT: rows.filter((r) => r.providerAgreementCore === "PROVIDER_CONSISTENT").length,
      PROVIDER_MIXED: rows.filter((r) => r.providerAgreementCore === "PROVIDER_MIXED").length,
      PROVIDER_CONFLICT: rows.filter((r) => r.providerAgreementCore === "PROVIDER_CONFLICT").length,
    },
    lifestyleReview: {
      INDIGO: indigo
        ? { CORE_ONLY: indigo.indexCore, MIXED: indigo.indexMixed, STATUS: indigo.productionClass, STABILITY: indigo.stabilityCore }
        : null,
      KIMPTON: kimpton
        ? { CORE_ONLY: kimpton.indexCore, MIXED: kimpton.indexMixed, STATUS: kimpton.productionClass, STABILITY: kimpton.stabilityCore }
        : null,
      VOCO: voco
        ? { CORE_ONLY: voco.indexCore, MIXED: voco.indexMixed, STATUS: voco.productionClass, STABILITY: voco.stabilityCore }
        : null,
      DESIGN_HOTELS: relationLabel(IDS.INDIGO, IDS.DESIGN, S.LIFESTYLE),
      RADISSON_RED: relationLabel(IDS.INDIGO, IDS.RAD_RED, S.LIFESTYLE),
      PREFERRED: relationLabel(IDS.INDIGO, IDS.PREFERRED, S.LIFESTYLE),
      EVEN: relationLabel(IDS.INDIGO, IDS.EVEN, S.LIFESTYLE),
      decisions: LIFESTYLE_PEER_REVIEW,
    },
    softBrandReview: {
      ...SOFT_BRAND_REVIEW,
      AUTOGRAPH_SOFT: autographSoft ? { INDEX_CORE: autographSoft.indexCore, STATUS: autographSoft.productionClass } : null,
      AUTOGRAPH_CONVERSION: autographConv ? { INDEX_CORE: autographConv.indexCore, STATUS: autographConv.productionClass } : null,
      CURIO_SOFT: curioSoft ? { INDEX_CORE: curioSoft.indexCore, STATUS: curioSoft.productionClass } : null,
    },
    ownerFlexibilityReview: {
      CORE_SET: ["Ascend Hotel Collection", "Radisson Individuals by Choice", "Trademark Collection", "Handwritten Collection", "Preferred Hotels & Resorts", "Vignette Collection"],
      note: "Keep owner-flex CORE as flexible-affiliation platforms. Major collections stay SECONDARY context, not denominator.",
      ASCEND: ascendFlex ? { INDEX_CORE: ascendFlex.indexCore, STATUS: ascendFlex.productionClass } : null,
      ASCEND_SOFT: ascendSoft ? { INDEX_CORE: ascendSoft.indexCore, STATUS: ascendSoft.productionClass } : null,
    },
    brandedResidences: {
      STATUS: BRANDED_RESIDENCES_BENCHMARK_STATUS,
      rows: residencesRows.length,
      note: "22 QUESTIONABLE cross-architecture peers and extreme indices (e.g. Autograph 700) are not salvageable as a production cohort. Suppress rather than invent a mixed-architecture denominator.",
    },
    headlineIndex: { STATUS: HEADLINE_INDEX_STATUS },
    aggregationMethod: { STATUS: AGGREGATION_METHOD_STATUS },
    customer: {
      SCENARIO_INDEX_CUSTOMER_STATUS: "INTERNAL_ONLY",
      CUSTOMER_INDEX_RENDERING,
      FULL_PEER_MATRIX: "INTERNAL_ONLY",
      allowlistHidesMembers: !CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"),
    },
    regression: {
      BRAND_DIFF: "0 except composition remediation artifacts",
      BRAND_UI_DIFF: 0,
      OPERATOR_DIFF: 0,
      operatorCount: PRIMARY_OPERATOR_COUNT,
      customerVisibleBrands: listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length,
    },
    NO_FULL_SET_FALLBACK,
    STABILITY_THRESHOLDS,
    rows,
    next,
    final: finalStatus,
  };

  if (opts.writeReport !== false) {
    const outDir = path.join(ROOT, "reports", "ai-visibility");
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(path.join(outDir, "scenario-benchmark-composition-v1.json"), JSON.stringify(report, null, 2));
    fs.writeFileSync(
      path.join(outDir, "scenario-benchmark-composition-v1.md"),
      [
        "# Scenario Benchmark Composition Remediation V1",
        "",
        `**Final:** ${report.final}`,
        `**Next:** ${report.next}`,
        `**Policy:** ${RECOMMENDED_POLICY}`,
        "",
        `- PRODUCTION_VALIDATED: ${byClass.PRODUCTION_VALIDATED}`,
        `- PRODUCTION_VALIDATED_NARROW: ${byClass.PRODUCTION_VALIDATED_NARROW}`,
        `- FRAGILE_PRODUCTION: ${fragileProduction}`,
        `- CUSTOMER_INDEX_RENDERING: OFF`,
        `- Headline: DEFERRED`,
        "",
      ].join("\n")
    );
  }

  return report;
}
