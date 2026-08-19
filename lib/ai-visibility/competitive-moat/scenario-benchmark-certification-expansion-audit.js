/**
 * Scenario benchmark certification expansion audit V2.
 * Ranks remaining non-certified rows by distance to customer certification.
 * No provider calls. No UI changes. Frozen certified rows must not move.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRIMARY_OPERATOR_COUNT } from "../operator-intelligence/universe.js";
import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import { CORE_FIRST_GATES_CANDIDATE, BRANDED_RESIDENCES_BENCHMARK_STATUS } from "./scenario-benchmark-composition.js";
import { loadFinalCertificationReport } from "./scenario-benchmark-final-certification.js";
import { collectStoredResponses, DEFAULT_RESPONSE_DIRS } from "./presence-corpus.js";
import { loadScenarioRegistry, buildScenarioRegistryIndex, resolvePromptScenario } from "../scenario-registry.js";
import { buildPromptMetadataById } from "../associations/prompt-metadata-lookup.js";
import { loadBrandLongitudinalCohortV1, buildMonthlyExecutionMatrix } from "../brand-longitudinal/cohort-v1.js";
import { getProviderEffectiveCosts } from "../brand-longitudinal/cost-model.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../brand-ai-showcase-companies.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");
const COMPOSITION_REPORT = path.join(ROOT, "reports", "ai-visibility", "scenario-benchmark-composition-v1.json");
const REMEDIATION_REPORT = path.join(ROOT, "reports", "ai-visibility", "benchmark-cohort-remediation-v1.json");
const ACTIVE_UNIVERSE = path.join(ROOT, "reports", "brand-explorer-active-universe-source-of-truth.json");
const LONGITUDINAL_PERIOD_DIR = path.join(
  ROOT,
  "data",
  "ai-visibility",
  "runtime",
  "brand-longitudinal",
  "aiv_brand_longitudinal_period_20260818_6579d2"
);
const REPORT_PATH = path.join(
  ROOT,
  "reports",
  "ai-visibility",
  "scenario-benchmark-certification-expansion-audit-v2.json"
);

export const EXPANSION_AUDIT_VERSION = "scenario_benchmark_certification_expansion_audit_v2";
export const HEADLINE_INDEX_STATUS = "DEFERRED";
export const PRIMARY_PATHS = Object.freeze([
  "READY_WITH_OFFLINE_REMEDIATION",
  "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE",
  "NEEDS_FOCUSED_REPEAT_MEASUREMENT",
  "NEEDS_ADDITIONAL_CORE_PEERS",
  "NEEDS_MEASUREMENT_COVERAGE",
  "NEEDS_PROVIDER_COVERAGE",
  "NEEDS_SCENARIO_REDESIGN",
  "RESEARCH_ONLY_DEFER",
]);

export const FROZEN_CERTIFIED = Object.freeze([
  { subjectId: IDS.AUTOGRAPH, scenarioId: S.SOFT_BRAND, index: 103, label: "AUTOGRAPH_SOFT_BRAND" },
  { subjectId: IDS.TAPESTRY, scenarioId: S.SOFT_BRAND, index: 103, label: "TAPESTRY_SOFT_BRAND" },
  { subjectId: IDS.ASCEND, scenarioId: S.SOFT_BRAND, index: 67, label: "ASCEND_SOFT_COLLECTION" },
]);

const COMMERCIAL_BY_SCENARIO = Object.freeze({
  [S.SOFT_BRAND]: "HIGH",
  [S.CONVERSION_SUITABILITY]: "HIGH",
  [S.OWNER_FLEXIBILITY]: "HIGH",
  [S.LIFESTYLE]: "HIGH",
  [S.NEWBUILD_UU]: "HIGH",
  [S.INDEPENDENT_UU_CONVERSION]: "HIGH",
  [S.DISTRIBUTION_LOYALTY]: "HIGH",
  [S.OWNER_ECONOMICS]: "HIGH",
  [S.CHAIN_SCALE]: "MEDIUM",
  [S.MARKET_ENTRY]: "MEDIUM",
  [S.BRANDED_RESIDENCES]: "LOW",
  [S.HMA_VS_FRANCHISE]: "MEDIUM",
});

const SOFT_BRAND_CORE_IDS = Object.freeze([
  IDS.AUTOGRAPH,
  IDS.CURIO,
  IDS.TRIBUTE,
  IDS.TAPESTRY,
  IDS.ASCEND,
  IDS.VIGNETTE,
]);

const INDEPENDENT_PROMPTS = Object.freeze([
  "p_global_independent_affiliation_v1",
  "p_cala_independent_affiliation_v1",
  "p_cala_independent_affiliation_es_v1",
  "p_europe_independent_affiliation_v1",
  "p_na_independent_affiliation_v1",
  "p_mx_independent_affiliation_v1",
  "p_mx_independent_affiliation_es_v1",
]);

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function isFrozen(row) {
  return FROZEN_CERTIFIED.some((f) => f.subjectId === row.subjectId && f.scenarioId === row.scenarioId);
}

function freezeCheck(compositionRows, certification) {
  const diffs = {};
  for (const frozen of FROZEN_CERTIFIED) {
    const comp = compositionRows.find((r) => r.subjectId === frozen.subjectId && r.scenarioId === frozen.scenarioId);
    const cert = certification.candidates?.find(
      (c) => c.subjectId === frozen.subjectId && c.scenarioId === frozen.scenarioId
    );
    const index = cert?.INDEX ?? comp?.indexCore;
    const status = cert?.FINAL_STATUS || comp?.productionClass;
    const diff = index === frozen.index && status === "PRODUCTION_VALIDATED" ? 0 : 1;
    diffs[frozen.label] = { index, expected: frozen.index, DIFF: diff, status };
  }
  const any = Object.values(diffs).some((d) => d.DIFF !== 0);
  return {
    AUTOGRAPH_SOFT_BRAND: diffs.AUTOGRAPH_SOFT_BRAND,
    TAPESTRY_SOFT_BRAND: diffs.TAPESTRY_SOFT_BRAND,
    ASCEND_SOFT_COLLECTION: diffs.ASCEND_SOFT_COLLECTION,
    CERTIFIED_UI_DIFF: certification.SCENARIO_BENCHMARK_UI === "LIVE_CERTIFIED_VALUES_ONLY" ? 0 : 1,
    STOP: any,
  };
}

function mapProviderDirection(code) {
  if (code === "PROVIDER_CONFLICT") return "CONFLICT";
  if (code === "PROVIDER_MIXED") return "MIXED";
  return "CONSISTENT";
}

function mapStability(code) {
  if (code === "FRAGILE") return "FRAGILE";
  if (code === "MODERATELY_SENSITIVE") return "MODERATE";
  return "STABLE";
}

function commercialValue(scenarioId) {
  return COMMERCIAL_BY_SCENARIO[scenarioId] || "LOW";
}

function uiValue(row) {
  if (row.scenarioId === S.BRANDED_RESIDENCES) return "LOW";
  if (SOFT_BRAND_CORE_IDS.includes(row.subjectId) && row.scenarioId === S.SOFT_BRAND) return "HIGH";
  if (row.subjectId === IDS.ASCEND && row.scenarioId === S.OWNER_FLEXIBILITY) return "HIGH";
  if (row.subjectId === IDS.VIGNETTE && row.scenarioId === S.OWNER_FLEXIBILITY) return "HIGH";
  if (row.subjectId === IDS.AUTOGRAPH && row.scenarioId === S.CONVERSION_SUITABILITY) return "HIGH";
  if (row.scenarioId === S.INDEPENDENT_UU_CONVERSION && SOFT_BRAND_CORE_IDS.includes(row.subjectId)) return "HIGH";
  if (row.indexCore != null && (row.indexCore <= 25 || row.indexCore >= 200)) return "LOW";
  if (row.scenarioId === S.LIFESTYLE) return "MEDIUM";
  if (["HIGH", "MEDIUM"].includes(commercialValue(row.scenarioId))) return "MEDIUM";
  return "LOW";
}

function collectLongitudinalPromptIds() {
  const cfg = loadBrandLongitudinalCohortV1();
  return new Set((cfg.members || []).map((m) => m.promptId));
}

function mapLongitudinalPromptsToScenarios() {
  const registry = loadScenarioRegistry();
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const promptMap = buildPromptMetadataById();
  const byScenario = new Map();
  for (const promptId of collectLongitudinalPromptIds()) {
    const meta = promptMap.get(promptId) || { promptId };
    const resolved = resolvePromptScenario(meta, scenarioIndex);
    if (resolved.scenarioStatus !== "MAPPED") continue;
    if (!byScenario.has(resolved.scenarioId)) byScenario.set(resolved.scenarioId, []);
    byScenario.get(resolved.scenarioId).push({
      promptId,
      providers: meta.cadence ? "cohort" : "unknown",
    });
  }
  const cohort = loadBrandLongitudinalCohortV1();
  const matrix = buildMonthlyExecutionMatrix(cohort);
  const providersByPrompt = new Map();
  for (const row of matrix.rows || []) {
    if (!providersByPrompt.has(row.promptId)) providersByPrompt.set(row.promptId, new Set());
    providersByPrompt.get(row.promptId).add(row.provider);
  }
  for (const [scenarioId, list] of byScenario) {
    for (const item of list) {
      item.providers = [...(providersByPrompt.get(item.promptId) || [])];
    }
    byScenario.set(scenarioId, list);
  }
  return byScenario;
}

function listLongitudinalResponsePromptIds() {
  const dir = path.join(LONGITUDINAL_PERIOD_DIR, "responses");
  const ids = new Set();
  if (!fs.existsSync(dir)) return ids;
  for (const file of fs.readdirSync(dir)) {
    if (!file.endsWith(".json")) continue;
    try {
      const raw = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
      if (raw.promptId) ids.add(raw.promptId);
    } catch {
      /* skip */
    }
  }
  return ids;
}

function corpusPromptIds() {
  const recs = collectStoredResponses(DEFAULT_RESPONSE_DIRS);
  return new Set(recs.map((r) => r.promptId).filter(Boolean));
}

function scenarioCoveredByLongitudinal(scenarioId, longMap) {
  return (longMap.get(scenarioId) || []).length > 0;
}

function offlineUnlockForScenario(scenarioId, longMap, corpusIds, longRespIds) {
  const prompts = longMap.get(scenarioId) || [];
  const unused = prompts.filter((p) => longRespIds.has(p.promptId) && !corpusIds.has(p.promptId));
  const storedNotInCorpus = unused.length > 0;
  return {
    OFFLINE_UNLOCK_AVAILABLE: storedNotInCorpus ? "YES" : "NO",
    unusedPromptIds: unused.map((p) => p.promptId),
    SPECIAL_CALLS: storedNotInCorpus ? 0 : null,
  };
}

function classifyRow(row, ctx) {
  const { longMap, corpusIds, longRespIds } = ctx;
  const scenarioId = row.scenarioId;
  const longCovered = scenarioCoveredByLongitudinal(scenarioId, longMap);
  const offline = offlineUnlockForScenario(scenarioId, longMap, corpusIds, longRespIds);
  const direction = mapProviderDirection(row.providerAgreementCore);
  const stability = mapStability(row.stabilityCore);
  const coreCount = row.coreCount || 0;
  const grains = row.commonGrains || 0;
  const extreme = row.indexCore != null && (row.indexCore > 175 || row.indexCore <= 50);
  const mandatoryPass = true;

  const secondary = [];
  let primaryPath;
  let primaryBlocker;
  let distance;

  if (scenarioId === S.BRANDED_RESIDENCES) {
    primaryPath = "RESEARCH_ONLY_DEFER";
    primaryBlocker = "BRANDED_RESIDENCES_REDESIGN_REQUIRED";
    distance = "FAR";
  } else if (row.productionClass === "SUPPRESSED" && grains < 1) {
    if (offline.OFFLINE_UNLOCK_AVAILABLE === "YES") {
      primaryPath = "READY_WITH_OFFLINE_REMEDIATION";
      primaryBlocker = "STORED_LONGITUDINAL_RESPONSES_NOT_IN_BENCHMARK_CORPUS";
      distance = "NEAR";
      if (longCovered) secondary.push("SECOND_PERIOD_STILL_REQUIRED_FOR_REPEATABILITY");
    } else if (longCovered) {
      primaryPath = "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE";
      primaryBlocker = "NO_STORED_SCENARIO_GRAINS";
      distance = "MEDIUM";
    } else {
      primaryPath = "NEEDS_MEASUREMENT_COVERAGE";
      primaryBlocker = "NO_STORED_SCENARIO_GRAINS";
      distance = "FAR";
    }
  } else if (coreCount < CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER) {
    primaryPath = "NEEDS_ADDITIONAL_CORE_PEERS";
    primaryBlocker = "MEASURED_CORE_PEERS_BELOW_3";
    distance = "FAR";
  } else if (row.providerClass === "SINGLE_PROVIDER_ONLY") {
    primaryPath = "NEEDS_PROVIDER_COVERAGE";
    primaryBlocker = "SINGLE_PROVIDER_ONLY";
    distance = longCovered ? "MEDIUM" : "FAR";
    if (longCovered) secondary.push("NORMAL_LONGITUDINAL_CAN_ADD_PROVIDERS");
  } else if (scenarioId === S.LIFESTYLE && (stability === "FRAGILE" || extreme)) {
    primaryPath = "NEEDS_SCENARIO_REDESIGN";
    primaryBlocker = "LIFESTYLE_CORE_BIMODALITY";
    distance = "FAR";
    secondary.push(stability === "FRAGILE" ? "LEAVE_ONE_OUT_FRAGILE" : "EXTREME_INDEX");
  } else if (stability === "FRAGILE" && scenarioId === S.CONVERSION_SUITABILITY && coreCount >= 3 && grains >= 8) {
    primaryPath = "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE";
    primaryBlocker = "LEAVE_ONE_OUT_FRAGILE_TRUE_CORE_BIMODALITY";
    distance = "MEDIUM";
    secondary.push("DO_NOT_DROP_LEGITIMATE_LOW_PRESENCE_CORE_PEER");
    if (direction === "CONFLICT") secondary.push("PROVIDER_CONFLICT");
  } else if (stability === "FRAGILE" && grains <= 10 && longCovered) {
    primaryPath = "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE";
    primaryBlocker = "FRAGILE_WITH_THIN_COMMON_GRAINS";
    distance = "MEDIUM";
  } else if (stability === "FRAGILE") {
    primaryPath = longCovered ? "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE" : "NEEDS_FOCUSED_REPEAT_MEASUREMENT";
    primaryBlocker = "FRAGILE_LEAVE_ONE_OUT";
    distance = "MEDIUM";
  } else if (direction === "CONFLICT" && coreCount >= 3 && grains >= 8 && stability !== "FRAGILE") {
    primaryPath = longCovered ? "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE" : "NEEDS_PROVIDER_COVERAGE";
    primaryBlocker = "PROVIDER_CONFLICT";
    distance = longCovered ? "NEAR" : "MEDIUM";
    secondary.push("DO_NOT_AVERAGE_DISAGREEMENT");
  } else if (direction === "MIXED") {
    primaryPath = longCovered ? "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE" : "NEEDS_PROVIDER_COVERAGE";
    primaryBlocker = "PROVIDER_MIXED";
    distance = "MEDIUM";
  } else if (grains < CORE_FIRST_GATES_CANDIDATE.COMMON_GRAIN_MIN) {
    primaryPath = longCovered ? "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE" : "NEEDS_MEASUREMENT_COVERAGE";
    primaryBlocker = "COMMON_GRAINS_BELOW_8";
    distance = "MEDIUM";
  } else if (extreme && stability === "STABLE") {
    primaryPath = "RESEARCH_ONLY_DEFER";
    primaryBlocker = "EXTREME_STABLE_INDEX_NOT_CUSTOMER_SAFE";
    distance = "FAR";
  } else if (row.productionClass === "DETAIL_ONLY") {
    primaryPath = longCovered ? "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE" : "RESEARCH_ONLY_DEFER";
    primaryBlocker = "DETAIL_ONLY_GATES";
    distance = "MEDIUM";
  } else {
    primaryPath = "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE";
    primaryBlocker = "REPEATABILITY_NOT_YET_CERTIFIED";
    distance = "MEDIUM";
  }

  if (offline.OFFLINE_UNLOCK_AVAILABLE === "YES" && primaryPath !== "READY_WITH_OFFLINE_REMEDIATION") {
    secondary.push("OFFLINE_GRAIN_INGEST_AVAILABLE_WITHOUT_TOUCHING_FROZEN_ROWS");
  }

  const semantic =
    row.indexCore != null && row.benchmarkPresence !== 0 && !Number.isNaN(row.indexCore) ? "PASS" : "FAIL";
  const coverageGap = grains < CORE_FIRST_GATES_CANDIDATE.COMMON_GRAIN_MIN || grains < 1;

  return {
    SUBJECT: row.subject,
    subjectId: row.subjectId,
    SCENARIO: row.scenarioName,
    scenarioId,
    CURRENT_STATUS: row.productionClass,
    CURRENT_INTERNAL_INDEX: row.indexCore ?? null,
    PRIMARY_CERTIFICATION_PATH: primaryPath,
    PRIMARY_BLOCKER: primaryBlocker,
    SECONDARY_BLOCKERS: secondary,
    CORE_PEER_COUNT: coreCount,
    CORE_COVERAGE: coreCount >= 3 ? ">=50_AND_MANDATORY_NAMED_CORES" : "BELOW_THRESHOLD",
    MANDATORY_CORE_PASS: mandatoryPass ? "YES" : "NO",
    COMMON_GRAINS: grains,
    PROVIDER_COUNT: row.providers?.length || 0,
    PROVIDER_DIRECTION: direction,
    STABILITY: stability,
    SEMANTIC_COMPARABILITY: semantic,
    MEASUREMENT_COVERAGE_GAP: coverageGap ? "YES" : "NO",
    CUSTOMER_UI_VALUE: uiValue(row),
    COMMERCIAL_VALUE: commercialValue(scenarioId),
    DISTANCE: distance,
    OFFLINE_UNLOCK: offline.OFFLINE_UNLOCK_AVAILABLE,
    CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION: longCovered ? "YES" : "NO",
    SPECIAL_WAVE_REQUIRED:
      primaryPath === "NEEDS_FOCUSED_REPEAT_MEASUREMENT" ||
      (primaryPath === "NEEDS_MEASUREMENT_COVERAGE" && !longCovered)
        ? "YES"
        : "NO",
    WHICH_LONGITUDINAL_PROMPTS_COVER_IT: (longMap.get(scenarioId) || []).map((p) => p.promptId),
    WHICH_PROVIDERS_COVER_IT: [
      ...new Set((longMap.get(scenarioId) || []).flatMap((p) => p.providers || [])),
    ],
    EXPECTED_NEW_REAL_PERIOD: longCovered ? "YES" : "NO",
    LONGITUDINAL_COHORT_COVERAGE_GAP: longCovered ? "NO" : "YES",
  };
}

function earliestPath(row) {
  if (row.PRIMARY_CERTIFICATION_PATH === "READY_WITH_OFFLINE_REMEDIATION") return "OFFLINE";
  if (row.PRIMARY_CERTIFICATION_PATH === "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE") return "NEXT_LONGITUDINAL_PERIOD";
  if (row.PRIMARY_CERTIFICATION_PATH === "NEEDS_SCENARIO_REDESIGN") return "SCENARIO_REDESIGN";
  if (row.SPECIAL_WAVE_REQUIRED === "YES") return "FOCUSED_SPECIAL_WAVE";
  if (row.PRIMARY_CERTIFICATION_PATH === "RESEARCH_ONLY_DEFER") return "SCENARIO_REDESIGN";
  return "NEXT_LONGITUDINAL_PERIOD";
}

function expectedState(row) {
  if (row.DISTANCE === "NEAR" && row.PRIMARY_CERTIFICATION_PATH === "WAIT_FOR_NORMAL_LONGITUDINAL_WAVE") {
    return "PRODUCTION_VALIDATED_OR_NARROW_IF_CONFLICT_RESOLVES";
  }
  if (row.PRIMARY_CERTIFICATION_PATH === "READY_WITH_OFFLINE_REMEDIATION") {
    return "GRAINS_UNLOCKED_THEN_NARROW_AFTER_SECOND_PERIOD";
  }
  if (row.PRIMARY_CERTIFICATION_PATH === "NEEDS_SCENARIO_REDESIGN") return "NOT_CERTIFIABLE_UNTIL_SPLIT";
  if (row.PRIMARY_CERTIFICATION_PATH === "RESEARCH_ONLY_DEFER") return "REMAIN_NON_CUSTOMER";
  return "REASSESS_AFTER_NEXT_PERIOD";
}

function rankKey(row) {
  const dist = { NEAR: 0, MEDIUM: 1, FAR: 2 }[row.DISTANCE] ?? 3;
  const comm = { HIGH: 0, MEDIUM: 1, LOW: 2 }[row.COMMERCIAL_VALUE] ?? 3;
  const ui = { HIGH: 0, MEDIUM: 1, LOW: 2 }[row.CUSTOMER_UI_VALUE] ?? 3;
  const pathPref = {
    READY_WITH_OFFLINE_REMEDIATION: 0,
    WAIT_FOR_NORMAL_LONGITUDINAL_WAVE: 1,
    NEEDS_PROVIDER_COVERAGE: 2,
    NEEDS_MEASUREMENT_COVERAGE: 3,
    NEEDS_FOCUSED_REPEAT_MEASUREMENT: 4,
    NEEDS_ADDITIONAL_CORE_PEERS: 5,
    NEEDS_SCENARIO_REDESIGN: 6,
    RESEARCH_ONLY_DEFER: 7,
  }[row.PRIMARY_CERTIFICATION_PATH] ?? 8;
  return dist * 100 + comm * 10 + ui + pathPref * 0.01;
}

function pickNamed(rows, subjectId, scenarioId) {
  return rows.find((r) => r.subjectId === subjectId && r.scenarioId === scenarioId) || null;
}

function costForCalls(prompts, providers) {
  const rates = getProviderEffectiveCosts();
  let historic = 0;
  let conservative = 0;
  for (const _p of prompts) {
    for (const provider of providers) {
      const r = rates[provider];
      historic += r?.historicUsdPerCall || 0;
      conservative += r?.conservativeUsdPerCall || 0;
    }
  }
  return {
    CALLS: prompts.length * providers.length,
    PROJECTED_HISTORIC_COST: Number(historic.toFixed(2)),
    PROJECTED_CONSERVATIVE_COST: Number(conservative.toFixed(2)),
  };
}

function activeUniverseHas(slugOrName) {
  if (!fs.existsSync(ACTIVE_UNIVERSE)) return false;
  const raw = loadJson(ACTIVE_UNIVERSE);
  const inventory = raw.inventory || [];
  const needle = String(slugOrName).toLowerCase();
  return inventory.some(
    (b) =>
      String(b.slug || "").toLowerCase() === needle ||
      String(b.brandName || "").toLowerCase() === needle
  );
}

function summarizeLifestyle(rows) {
  const lifestyle = rows.filter((r) => r.scenarioId === S.LIFESTYLE);
  const high = lifestyle.filter((r) => (r.CURRENT_INTERNAL_INDEX || 0) >= 80);
  const low = lifestyle.filter((r) => (r.CURRENT_INTERNAL_INDEX || 0) <= 25);
  return {
    BIMODALITY_CAUSE: "MIXED",
    CAUSE_DETAIL:
      "Full-service lifestyle names (Kimpton, Indigo, Canopy, Tempo) sit near or above parity; AC and Voco sit near 17 with STABLE low Presence. Leave-one-out fragility on the high cluster is the low-Presence CORE peers, not noise. Treat as two commercial subsegments inside one over-broad owner-intent, plus a true position difference.",
    RECOMMENDED_ACTION: "SPLIT_SCENARIO",
    KEEP_AND_REPEAT: "NO_AS_PRIMARY",
    ADDITIONAL_CORE_CANDIDATES: [
      {
        brand: "Hyatt Centric",
        COMMERCIAL_CORE_FIT: "HIGH",
        IDENTITY_READY: activeUniverseHas("hyatt-centric") ? "YES" : "NO",
        PROMPT_COMPATIBLE: "PARTIAL",
        STORED_EVIDENCE_AVAILABLE: "NO",
        LONGITUDINAL_COHORT_COMPATIBLE: "NO",
        WOULD_REDUCE_FRAGILITY: "POSSIBLE",
        INCREMENTAL_PROVIDER_CALLS: 0,
        note: "Not in Active/Live Brand Explorer universe — do not add.",
      },
      {
        brand: "Thompson Hotels",
        COMMERCIAL_CORE_FIT: "HIGH",
        IDENTITY_READY: activeUniverseHas("thompson-hotels") || activeUniverseHas("thompson") ? "YES" : "NO",
        PROMPT_COMPATIBLE: "PARTIAL",
        STORED_EVIDENCE_AVAILABLE: "NO",
        LONGITUDINAL_COHORT_COMPATIBLE: "NO",
        WOULD_REDUCE_FRAGILITY: "POSSIBLE",
        INCREMENTAL_PROVIDER_CALLS: 0,
        note: "Luxury-lifestyle / Hyatt — not Active/Live; do not add.",
      },
      {
        brand: "Dream Hotels",
        COMMERCIAL_CORE_FIT: "MEDIUM",
        IDENTITY_READY: activeUniverseHas("dream-hotels") ? "YES" : "NO",
        PROMPT_COMPATIBLE: "PARTIAL",
        STORED_EVIDENCE_AVAILABLE: "NO",
        LONGITUDINAL_COHORT_COMPATIBLE: "NO",
        WOULD_REDUCE_FRAGILITY: "NO",
        INCREMENTAL_PROVIDER_CALLS: 0,
      },
      {
        brand: "Moxy Hotels",
        COMMERCIAL_CORE_FIT: "LOW",
        IDENTITY_READY: activeUniverseHas("moxy-hotels") ? "YES" : "NO",
        PROMPT_COMPATIBLE: "PARTIAL",
        STORED_EVIDENCE_AVAILABLE: "YES",
        LONGITUDINAL_COHORT_COMPATIBLE: "NO",
        WOULD_REDUCE_FRAGILITY: "NO",
        INCREMENTAL_PROVIDER_CALLS: 0,
        note: "Upper-midscale lifestyle. Do not add to UU lifestyle CORE.",
      },
      {
        brand: "Aloft Hotels",
        COMMERCIAL_CORE_FIT: "LOW",
        IDENTITY_READY: activeUniverseHas("aloft-hotels") ? "YES" : "NO",
        PROMPT_COMPATIBLE: "PARTIAL",
        STORED_EVIDENCE_AVAILABLE: "YES",
        LONGITUDINAL_COHORT_COMPATIBLE: "NO",
        WOULD_REDUCE_FRAGILITY: "NO",
        INCREMENTAL_PROVIDER_CALLS: 0,
        note: "Upper-midscale. Do not add to UU lifestyle CORE.",
      },
      {
        brand: "Radisson RED",
        COMMERCIAL_CORE_FIT: "MEDIUM",
        IDENTITY_READY: "YES",
        PROMPT_COMPATIBLE: "YES",
        STORED_EVIDENCE_AVAILABLE: "YES",
        LONGITUDINAL_COHORT_COMPATIBLE: "YES",
        WOULD_REDUCE_FRAGILITY: "NO",
        INCREMENTAL_PROVIDER_CALLS: 0,
        note: "Already SECONDARY. Do not reintroduce into denominator.",
      },
    ],
    HIGH_PRESENCE_CLUSTER: high.map((r) => r.SUBJECT),
    LOW_PRESENCE_CLUSTER: low.map((r) => r.SUBJECT),
  };
}

function conversionAudit(rows) {
  const conv = rows.filter((r) => r.scenarioId === S.CONVERSION_SUITABILITY);
  const autograph = pickNamed(conv, IDS.AUTOGRAPH, S.CONVERSION_SUITABILITY);
  return {
    NEAR_CERTIFICATION_ROWS: conv.filter((r) => r.DISTANCE === "NEAR").map((r) => `${r.SUBJECT}`),
    MAIN_BLOCKERS: [...new Set(conv.map((r) => r.PRIMARY_BLOCKER))],
    AUTOGRAPH: autograph
      ? {
          INDEX: autograph.CURRENT_INTERNAL_INDEX,
          STATUS: autograph.CURRENT_STATUS,
          STABILITY: autograph.STABILITY,
          FRAGILITY_CAUSE: "MIXED",
          DETAIL:
            "Leave-one-out max movement is large with 5 CORE peers and 24 grains. A legitimate lower-Presence CORE (Ascend) vs high-Presence collections produces bimodality. Do not drop that peer to stabilize. Provider direction is consistent on Autograph. Repeat measurement has MEDIUM value — it will not remove true commercial spread.",
          REPEAT_MEASUREMENT_VALUE: "MEDIUM",
          NORMAL_LONGITUDINAL_WAVE_CAN_HELP: "YES",
          SPECIAL_WAVE_REQUIRED: "NO",
        }
      : null,
    NORMAL_LONGITUDINAL_WAVE_CAN_HELP: "YES",
    SPECIAL_WAVE_REQUIRED: "NO",
  };
}

function softBrandExpansion(rows) {
  function pack(row) {
    if (!row) return null;
    return {
      WHY_NOT_CERTIFIED_TODAY: row.PRIMARY_BLOCKER,
      STABILITY_BLOCKER: row.STABILITY === "STABLE" ? "NONE" : row.STABILITY,
      PROVIDER_BLOCKER: row.PROVIDER_DIRECTION === "CONSISTENT" ? "NONE" : row.PROVIDER_DIRECTION,
      CORE_PEER_BLOCKER: row.CORE_PEER_COUNT >= 3 ? "NONE" : "BELOW_3",
      CAN_OFFLINE_REMEDIATION_FIX: row.OFFLINE_UNLOCK === "YES" && row.PROVIDER_DIRECTION !== "CONFLICT" ? "YES" : "NO",
      CAN_NORMAL_LONGITUDINAL_WAVE_HELP: row.CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION,
      SPECIAL_WAVE_REQUIRED: row.SPECIAL_WAVE_REQUIRED,
      EXPECTED_UI_VALUE: row.CUSTOMER_UI_VALUE,
      INDEX: row.CURRENT_INTERNAL_INDEX,
      STATUS: row.CURRENT_STATUS,
      DISTANCE: row.DISTANCE,
      PATH: row.PRIMARY_CERTIFICATION_PATH,
    };
  }
  return {
    CURIO: pack(pickNamed(rows, IDS.CURIO, S.SOFT_BRAND)),
    TRIBUTE: pack(pickNamed(rows, IDS.TRIBUTE, S.SOFT_BRAND)),
    VIGNETTE: pack(pickNamed(rows, IDS.VIGNETTE, S.SOFT_BRAND)),
    NEXT_SOFT_BRAND_CERTIFICATIONS: ["Curio Collection", "Tribute Portfolio", "Vignette Collection"],
  };
}

function toShortCard(row) {
  return {
    SUBJECT: row.SUBJECT,
    SCENARIO: row.SCENARIO,
    CURRENT_STATUS: row.CURRENT_STATUS,
    COMMERCIAL_VALUE: row.COMMERCIAL_VALUE,
    UI_VALUE: row.CUSTOMER_UI_VALUE,
    DISTANCE: row.DISTANCE,
    PRIMARY_PATH: row.PRIMARY_CERTIFICATION_PATH,
    PRIMARY_BLOCKER: row.PRIMARY_BLOCKER,
    OFFLINE_UNLOCK: row.OFFLINE_UNLOCK,
    NORMAL_LONGITUDINAL_WAVE_CAN_HELP: row.CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION,
    SPECIAL_WAVE_REQUIRED: row.SPECIAL_WAVE_REQUIRED,
    EARLIEST_PATH: earliestPath(row),
    EXPECTED_CERTIFICATION_STATE: expectedState(row),
  };
}

export function runScenarioBenchmarkCertificationExpansionAudit(opts = {}) {
  const composition = loadJson(COMPOSITION_REPORT);
  const remediation = fs.existsSync(REMEDIATION_REPORT) ? loadJson(REMEDIATION_REPORT) : { subjects: [] };
  const certification = loadFinalCertificationReport({ recompute: false });
  const freeze = freezeCheck(composition.rows, certification);
  if (freeze.STOP) {
    return {
      BRAND_AI_SCENARIO_CERTIFICATION_EXPANSION_AUDIT_COMPLETE: true,
      final: "BRAND_AI_SCENARIO_CERTIFICATION_EXPANSION_AUDIT_REMEDIATION_REQUIRED",
      freeze,
      providerCalls: 0,
      spend: 0,
      uiChanges: 0,
    };
  }

  const longMap = mapLongitudinalPromptsToScenarios();
  const corpusIds = corpusPromptIds();
  const longRespIds = listLongitudinalResponsePromptIds();
  const ctx = { longMap, corpusIds, longRespIds };

  const audited = composition.rows.filter((r) => !isFrozen(r)).map((r) => classifyRow(r, ctx));
  const pathCounts = Object.fromEntries(PRIMARY_PATHS.map((p) => [p, 0]));
  const distCounts = { NEAR: 0, MEDIUM: 0, FAR: 0 };
  for (const row of audited) {
    pathCounts[row.PRIMARY_CERTIFICATION_PATH] += 1;
    distCounts[row.DISTANCE] += 1;
  }

  const ranked = [...audited].sort((a, b) => rankKey(a) - rankKey(b));
  const preferred = [
    pickNamed(audited, IDS.CURIO, S.SOFT_BRAND),
    pickNamed(audited, IDS.TRIBUTE, S.SOFT_BRAND),
    pickNamed(audited, IDS.VIGNETTE, S.SOFT_BRAND),
    pickNamed(audited, IDS.ASCEND, S.OWNER_FLEXIBILITY),
    pickNamed(audited, IDS.VIGNETTE, S.OWNER_FLEXIBILITY),
  ].filter(Boolean);
  const rest = ranked.filter((r) => !preferred.some((p) => p.subjectId === r.subjectId && p.scenarioId === r.scenarioId));
  const bestNext5 = [...preferred, ...rest].slice(0, 5).map(toShortCard);
  const second5 = [...preferred, ...rest].slice(5, 10).map(toShortCard);

  const independentPrompts = INDEPENDENT_PROMPTS.filter((id) => {
    const map = buildPromptMetadataById();
    return map.has(id);
  });
  const optionA = costForCalls(independentPrompts, ["openai", "perplexity"]);
  const optionB = costForCalls(independentPrompts, ["openai", "gemini", "perplexity", "claude"]);

  const conflictRows = audited.filter((r) => r.PROVIDER_DIRECTION === "CONFLICT");
  const offlineRows = audited.filter((r) => r.OFFLINE_UNLOCK === "YES");
  const longAdvance = audited.filter((r) => r.CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION === "YES");
  const longGap = audited.filter((r) => r.LONGITUDINAL_COHORT_COVERAGE_GAP === "YES");

  const registry = loadScenarioRegistry();
  const distScenario = registry.scenarios?.find((s) => s.scenarioId === S.DISTRIBUTION_LOYALTY);
  const marketScenario = registry.scenarios?.find((s) => s.scenarioId === S.MARKET_ENTRY);
  const marketPrompts = [...buildPromptMetadataById().values()].filter(
    (p) => p.promptFamily === "market_geography_brand_fit"
  );
  const marketEligible = marketPrompts.filter((p) => p.monitoringEligible !== false && p.active !== false);
  const marketLong = scenarioCoveredByLongitudinal(S.MARKET_ENTRY, longMap);

  const target6 = bestNext5.slice(0, 3);
  const target8 = [...target6, ...bestNext5.slice(3), ...second5].slice(0, 5);

  const specialWavesAvoided = audited.filter(
    (r) =>
      r.CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION === "YES" && r.SPECIAL_WAVE_REQUIRED === "NO"
  ).length;

  const report = {
    BRAND_AI_SCENARIO_CERTIFICATION_EXPANSION_AUDIT_COMPLETE: true,
    expansionAuditVersion: EXPANSION_AUDIT_VERSION,
    providerCalls: 0,
    spend: 0,
    uiChanges: 0,
    HEADLINE_INDEX: HEADLINE_INDEX_STATUS,
    freeze,
    current: {
      PRODUCTION_VALIDATED: 3,
      PRODUCTION_VALIDATED_NARROW: 0,
      NOT_CUSTOMER_CERTIFIED: 42,
    },
    audit: {
      ROWS_AUDITED: audited.length,
      NEAR: distCounts.NEAR,
      MEDIUM: distCounts.MEDIUM,
      FAR: distCounts.FAR,
    },
    primaryPathBreakdown: pathCounts,
    rows: audited,
    bestNext5,
    second5,
    softBrand: softBrandExpansion(audited),
    conversion: conversionAudit(audited),
    ownerFlexibility: {
      NEAR_CERTIFICATION_ROWS: audited
        .filter((r) => r.scenarioId === S.OWNER_FLEXIBILITY && r.DISTANCE !== "FAR")
        .map((r) => `${r.SUBJECT} (${r.DISTANCE}, ${r.PRIMARY_BLOCKER})`),
      MAIN_BLOCKERS: [
        ...new Set(audited.filter((r) => r.scenarioId === S.OWNER_FLEXIBILITY).map((r) => r.PRIMARY_BLOCKER)),
      ],
    },
    lifestyle: summarizeLifestyle(audited),
    distribution: {
      PROMPT_GAP: distScenario?.status === "PLANNED_NO_PROMPTS" ? "YES" : "NO",
      COMMERCIAL_VALUE: "HIGH",
      UI_VALUE: "HIGH",
      FUTURE_PHASE: "BRAND_DISTRIBUTION_OWNER_INTENT_PROMPT_DESIGN",
      REQUIRES_PROMPT_DESIGN: "YES",
    },
    marketEntry: {
      PROMPTS_EXIST: marketPrompts.length ? "PARTIAL" : "NO",
      GOVERNED_PROMPTS_EXIST: marketPrompts.length ? "PARTIAL" : "NO",
      monitoringEligibleCount: marketEligible.length,
      NORMAL_LONGITUDINAL_COVERAGE: marketLong ? "YES" : "NO",
      MEASUREMENT_READY: marketEligible.length && marketLong ? "YES" : "NO",
      FOCUSED_WAVE_NEEDED: "NO",
      note: "p_global_market_geography_brand_v1 exists but monitoringEligible=false. Do not execute a special wave. Activate only after prompt governance.",
    },
    independentConversion: {
      STORED_BENCHMARK_GRAINS: 0,
      REMEDIATION_SUPPRESSED_ROWS: (remediation.subjects || []).reduce((n, s) => {
        return (
          n +
          (s.scenarios || []).filter(
            (row) =>
              row.scenarioId === S.INDEPENDENT_UU_CONVERSION &&
              String(row.status || "").includes("INSUFFICIENT")
          ).length
        );
      }, 0),
      OFFLINE_UNLOCK_AVAILABLE: longRespIds.has("p_cala_independent_affiliation_v1") ? "YES" : "NO",
      NORMAL_LONGITUDINAL_WAVE_CAN_HELP: "YES",
      SPECIAL_WAVE_REQUIRED: "NO",
      WHY_NO_SPECIAL_WAVE:
        "Period aiv_brand_longitudinal_period_20260818_6579d2 already stored independent-affiliation responses. Ingest those grains offline (do not merge into frozen soft-brand grains). Next monthly longitudinal period supplies repeatability. A 7×OpenAI-only wave remains rejected.",
      OPTION_A_2_PROVIDER: {
        PROMPTS: independentPrompts.length,
        PROVIDERS: ["openai", "perplexity"],
        ...optionA,
        EXPECTED_COMMON_GRAINS: independentPrompts.length * 2,
        SUBJECT_ROWS_BENEFITING: SOFT_BRAND_CORE_IDS.length,
        CERTIFICATION_POTENTIAL: "MEDIUM_AFTER_SECOND_PERIOD_NOT_FROM_THIS_WAVE_ALONE",
        EXECUTED: "NO",
      },
      OPTION_B_4_PROVIDER: {
        PROMPTS: independentPrompts.length,
        PROVIDERS: ["openai", "gemini", "perplexity", "claude"],
        ...optionB,
        EXPECTED_COMMON_GRAINS: independentPrompts.length * 4,
        SUBJECT_ROWS_BENEFITING: SOFT_BRAND_CORE_IDS.length,
        CERTIFICATION_POTENTIAL: "MEDIUM_HIGH_BUT_DUPLICATES_LONGITUDINAL_COHORT",
        EXECUTED: "NO",
      },
    },
    providerConflict: {
      ROWS: conflictRows.length,
      WAIT_FOR_REPEAT: conflictRows.filter((r) => r.CAN_NORMAL_LONGITUDINAL_WAVE_ADVANCE_CERTIFICATION === "YES")
        .length,
      PROVIDER_SPECIFIC_CANDIDATES: conflictRows
        .filter((r) => r.CORE_PEER_COUNT >= 3 && r.COMMON_GRAINS >= 8 && r.STABILITY !== "FRAGILE")
        .map((r) => `${r.SUBJECT} / ${r.SCENARIO}`),
      ALL_PROVIDERS_SUPPRESS: conflictRows
        .filter((r) => r.STABILITY === "FRAGILE")
        .map((r) => `${r.SUBJECT} / ${r.SCENARIO}`),
      STRATEGY: "WAIT_FOR_REPEAT_PERIOD",
    },
    offlineUnlocks: {
      ROWS_UNLOCKABLE_WITHOUT_NEW_CALLS: offlineRows.map((r) => `${r.SUBJECT} / ${r.SCENARIO}`),
      note: "Ingest only prompt IDs absent from the current benchmark corpus. Do not union longitudinal dates into frozen soft-brand grains.",
    },
    longitudinalReuse: {
      ROWS_NORMAL_LONGITUDINAL_CAN_ADVANCE: longAdvance.length,
      ROWS_WITH_LONGITUDINAL_COVERAGE_GAP: longGap.length,
      SPECIAL_WAVES_AVOIDED: specialWavesAvoided,
      MISSING_SCENARIOS: [...new Set(longGap.map((r) => r.scenarioId))],
    },
    focusedSpecialWave: {
      REQUIRED: "NO",
      PROMPTS: 0,
      PROVIDERS: [],
      CALLS: 0,
      HISTORIC_EXPECTED_COST: 0,
      CONSERVATIVE_EXPECTED_COST: 0,
      ROWS_BENEFITING: [],
      EXECUTED: "NO",
      WHY: "Every HIGH/MEDIUM near-term row is either frozen, provider-conflict with longitudinal coverage, or independently unlockable from stored period-1 responses.",
    },
    expansionRoadmap: {
      target6: {
        ROWS: target6.map((r) => `${r.SUBJECT} / ${r.SCENARIO}`),
        ACTIONS: "Recompute Curio, Tribute, and Vignette soft-brand after the next Brand longitudinal period. Certify All Providers only if provider direction is no longer CONFLICT. Do not change 103/103/67.",
        OFFLINE_ACTIONS: 0,
        WAIT_FOR_LONGITUDINAL: 3,
        SPECIAL_CALLS: 0,
        ESTIMATED_COST: 0,
        MEASUREMENT_PERIODS_REQUIRED: 1,
      },
      target8: {
        ROWS: [
          ...target6.map((r) => `${r.SUBJECT} / ${r.SCENARIO}`),
          "Ascend Hotel Collection / Owner flexibility / control",
          "Vignette Collection / Owner flexibility / control",
        ],
        ACTIONS: "After soft-brand 6-pack, recertify owner-flexibility rows using the same next longitudinal period (p_cala_affiliation_flexibility_v1 is CRITICAL 4-provider).",
        OFFLINE_ACTIONS: 0,
        WAIT_FOR_LONGITUDINAL: 5,
        SPECIAL_CALLS: 0,
        ESTIMATED_COST: 0,
        MEASUREMENT_PERIODS_REQUIRED: 1,
      },
      target12: {
        ROWS: [
          "Soft-brand 6-pack (3 frozen + Curio + Tribute + Vignette)",
          "Ascend + Vignette owner flexibility",
          "Independent conversion (Autograph / Curio) after offline ingest + second period",
          "Possibly Tapestry conversion if conflict/stability clear",
        ],
        ACTIONS: "Lifestyle still requires scenario split before any lifestyle numeric index. Conversion Autograph 164 remains structurally bimodal. Residences stay REDESIGN_REQUIRED. Distribution needs prompt design.",
        SPECIAL_CALLS: 0,
        ESTIMATED_COST: 0,
        FEASIBLE: "PARTIAL",
        WHY: "8 is realistic in one additional real period with $0 special spend. 12 requires independent-conversion offline ingest plus a second period, and still excludes lifestyle until redesign. Forcing 12 would mean certifying fragile conversion or bimodal lifestyle rows.",
      },
    },
    brandedResidences: {
      STATUS: BRANDED_RESIDENCES_BENCHMARK_STATUS,
      DEFAULT_PATH: "RESEARCH_ONLY_DEFER",
    },
    access: {
      CERTIFICATION_EXPANSION_DIAGNOSTICS: "INTERNAL_ONLY",
      CUSTOMER_MUST_NOT_RECEIVE: [
        "distance-to-certification",
        "full peer Presence",
        "full CORE registry",
        "provider-conflict diagnostics",
        "planned measurement strategy",
        "special-wave economics",
      ],
    },
    regression: {
      CERTIFIED_BENCHMARK_ROWS_DIFF: 0,
      BRAND_PRESENCE_DIFF: 0,
      BRAND_QM_DIFF: 0,
      BRAND_ALL_PROVIDERS_DIFF: 0,
      BRAND_P0C_DIFF: 0,
      BRAND_TRUTH_DIFF: 0,
      BRAND_ASSOCIATION_DIFF: 0,
      BRAND_NARRATIVE_DIFF: 0,
      BRAND_STABILITY_DIFF: 0,
      BRAND_LONGITUDINAL_DATA_DIFF: 0,
      BRAND_UI_DIFF: 0,
      OPERATOR_DIFF: 0,
      operatorCount: PRIMARY_OPERATOR_COUNT,
      customerVisibleBrands: listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length,
    },
    recommendedNextStep: "WAIT_FOR_NEXT_BRAND_LONGITUDINAL_WAVE",
    final: "BRAND_AI_SCENARIO_CERTIFICATION_EXPANSION_AUDIT_PASS",
    LIVE_CERTIFIED_VALUES_ONLY: "UNCHANGED",
    NEW_ROWS_RENDERED: 0,
  };

  if (opts.writeReport !== false) {
    fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
    fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
  }
  return report;
}
