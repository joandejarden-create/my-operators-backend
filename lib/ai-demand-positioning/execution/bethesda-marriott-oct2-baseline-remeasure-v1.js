/**
 * Bethesda Marriott — Oct 2 2026 official pilot baseline remasurement.
 * Locks to the Sept certified control query set (63 scenarios).
 * Does NOT use the expanded generic_property_capability layer (~78).
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * Do not change queries/weights/providers/models after seeing results.
 */

import { existsSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { MEASUREMENT_CONTRACT_VERSION } from "../contracts/adp-measurement-contract-v1.js";
import { loadPropertyProfile, savePeriod, PROVIDERS } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { executeMonitoringPeriod, estimateCost } from "./multi-provider-runner.js";
import { parsePeriodObservations } from "./response-parser.js";
import { applyGovernedInterpretation } from "../subject-presence/canonical-subject-presence-v1.js";
import { attachFirstOfficialPropertyPeriodMetadata } from "../period-eligibility-v1.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { BETHESDA_MONTGOMERY_ENTITY_VERSION } from "../metrics/bethesda-montgomery-entity-registry.js";
import { getCensusLinkEntry } from "../census-link-registry.js";
import {
  BETHESDA_MARRIOTT_COST_CAP_USD,
  BETHESDA_IDENTITY_KEY,
} from "./bethesda-marriott-foundation-v1.js";
import {
  BETHESDA_PROPERTY_ID,
  loadFrozenContractHash,
  buildBethesdaCoreGovernanceSummary,
} from "./bethesda-marriott-baseline-period-001-v1.js";

export const BETHESDA_OCT2_BASELINE_ID = "BETHESDA_ADP_BASELINE_V2";
export const BETHESDA_OCT2_BASELINE_MARKER =
  "ADP_BETHESDA_MARRIOTT_BASELINE_PERIOD_002_OCT2_2026";
export const BETHESDA_CONTROL_QUERY_SET_ID =
  "BETHESDA_ADP_OCTOBER_CONTROL_QUERY_SET_V1";
export const BETHESDA_SEP_PERIOD_ID =
  "adp_period_adp_bethesda_marriott_20260909091016_9f3a60";

const CONTROL_SET_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/bethesda-adp-october-control-query-set-v1.json"
);

/** Frozen scenario IDs from Sept certified period (63). */
export const BETHESDA_CONTROL_SCENARIO_IDS = Object.freeze([
  "prop_btmd_01",
  "prop_btmd_02",
  "prop_btmd_03",
  "prop_btmd_04",
  "prop_btmd_05",
  "prop_btmd_06",
  "prop_btmd_07",
  "prop_btmd_08",
  "prop_btmd_09",
  "prop_btmd_10",
  "prop_btmd_11",
  "prop_btmd_12",
  "prop_btmd_13",
  "prop_btmd_14",
  "prop_btmd_15",
  "std_btmd_adv_01",
  "std_btmd_adv_02",
  "std_btmd_biz_01",
  "std_btmd_biz_02",
  "std_btmd_biz_03",
  "std_btmd_biz_04",
  "std_btmd_biz_05",
  "std_btmd_biz_06",
  "std_btmd_biz_07",
  "std_btmd_biz_08",
  "std_btmd_biz_09",
  "std_btmd_biz_10",
  "std_btmd_cel_01",
  "std_btmd_cel_02",
  "std_btmd_cel_03",
  "std_btmd_cel_04",
  "std_btmd_cel_05",
  "std_btmd_cpl_01",
  "std_btmd_cpl_02",
  "std_btmd_cpl_03",
  "std_btmd_cpl_04",
  "std_btmd_cpl_05",
  "std_btmd_cpl_06",
  "std_btmd_cpl_07",
  "std_btmd_fam_01",
  "std_btmd_fam_02",
  "std_btmd_fam_03",
  "std_btmd_fam_04",
  "std_btmd_fam_05",
  "std_btmd_grp_01",
  "std_btmd_grp_02",
  "std_btmd_grp_03",
  "std_btmd_grp_04",
  "std_btmd_grp_05",
  "std_btmd_grp_06",
  "std_btmd_grp_07",
  "std_btmd_grp_08",
  "std_btmd_lei_01",
  "std_btmd_lei_02",
  "std_btmd_lei_03",
  "std_btmd_lei_04",
  "std_btmd_lei_05",
  "std_btmd_lei_06",
  "std_btmd_lei_07",
  "std_btmd_lei_08",
  "std_btmd_wel_01",
  "std_btmd_wel_02",
  "std_btmd_wel_03",
]);

function availableProviders() {
  return PROVIDERS.filter((p) => {
    if (p === "openai")
      return !!(process.env.OPENAI_API_KEY || process.env.FDD_OPENAI_API_KEY);
    if (p === "gemini") {
      return !!(
        process.env.GEMINI_API_KEY ||
        process.env.GOOGLE_GENAI_API_KEY ||
        process.env.GOOGLE_GENERATIVE_AI_API_KEY ||
        process.env.FDD_GEMINI_API_KEY
      );
    }
    if (p === "perplexity")
      return !!(process.env.PERPLEXITY_API_KEY || process.env.PPLX_API_KEY);
    if (p === "claude") {
      return !!(
        process.env.ANTHROPIC_API_KEY ||
        process.env.CLAUDE_API_KEY ||
        process.env.FDD_ANTHROPIC_API_KEY
      );
    }
    return false;
  });
}

export function buildBethesdaControlScenarioUniverse(profile) {
  const idSet = new Set(BETHESDA_CONTROL_SCENARIO_IDS);
  const all = buildScenarioUniverse(profile);
  const locked = all.filter((s) => idSet.has(s.scenarioId || s.id));
  const missing = BETHESDA_CONTROL_SCENARIO_IDS.filter(
    (id) => !locked.some((s) => (s.scenarioId || s.id) === id)
  );
  if (missing.length) {
    throw new Error(
      `BETHESDA_CONTROL_QUERY_SET_INCOMPLETE missing=${missing.join(",")}`
    );
  }
  if (locked.length !== 63) {
    throw new Error(
      `BETHESDA_CONTROL_QUERY_SET_COUNT_${locked.length}_EXPECTED_63`
    );
  }
  return locked;
}

export function freezeBethesdaControlQuerySetDoc() {
  const profile = loadPropertyProfile(BETHESDA_PROPERTY_ID);
  const scenarios = buildBethesdaControlScenarioUniverse(profile);
  const byIntent = {};
  for (const s of scenarios) byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;
  const doc = {
    controlQuerySetId: BETHESDA_CONTROL_QUERY_SET_ID,
    version: "V1",
    propertyId: BETHESDA_PROPERTY_ID,
    hotelId: BETHESDA_IDENTITY_KEY,
    sourcePeriodId: BETHESDA_SEP_PERIOD_ID,
    frozenAt: new Date().toISOString(),
    queryCount: scenarios.length,
    demandFamilies: byIntent,
    methodologyVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash: loadFrozenContractHash(),
    scenarioUniverseVersion: "adp_scenario_universe_v1",
    promptVersion: "adp_scenario_universe_v1",
    providers: [...PROVIDERS],
    models: {
      openai: "gpt-4o",
      gemini: "gemini-3.6-flash",
      perplexity: "sonar",
      claude: "claude-sonnet-4-6",
    },
    scenarioIds: scenarios.map((s) => s.scenarioId || s.id),
    scenarios: scenarios.map((s) => ({
      scenarioId: s.scenarioId || s.id,
      intent: s.intent,
      frame: s.frame || null,
      query: s.query,
      source: s.source,
    })),
  };
  mkdirSync(join(process.cwd(), "data/ai-demand-positioning/contracts"), {
    recursive: true,
  });
  writeFileSync(CONTROL_SET_PATH, JSON.stringify(doc, null, 2) + "\n");
  return doc;
}

function summarizeProviderCompleteness(period, scenarioCount, providers) {
  const obs = period.observations || [];
  const byProvider = {};
  let success = 0;
  let failed = 0;
  for (const p of providers) {
    const rows = obs.filter((o) => o.provider === p);
    const ok = rows.filter(
      (o) => !o.error && (o.rawResponse || o.parsed || o.dryRun)
    ).length;
    const fail = rows.filter((o) => o.error).length;
    success += ok;
    failed += fail;
    byProvider[p] = {
      attempted: rows.length,
      successful: ok,
      failed: fail,
      expected: scenarioCount,
      complete: ok >= scenarioCount,
    };
  }
  return {
    byProvider,
    success,
    failed,
    attempted: obs.length,
    retryCalls: 0,
  };
}

/**
 * Remasurement preflight — allows already-published / dropdown-visible property.
 */
export function buildBethesdaOct2RemeasurePreflight() {
  const measurementContractHash = loadFrozenContractHash();
  const profile = loadPropertyProfile(BETHESDA_PROPERTY_ID);
  const blockers = [];
  const censusLink = getCensusLinkEntry(BETHESDA_PROPERTY_ID);

  if (!profile) blockers.push("PROPERTY_PROFILE_MISSING");
  if (!censusLink?.censusRecordId) blockers.push("CENSUS_LINK_MISSING");
  if (censusLink?.censusRecordId !== "recLuxvwwxID7U2B8") {
    blockers.push("CENSUS_ID_MISMATCH");
  }
  if (
    censusLink?.canonicalHotelId &&
    censusLink.canonicalHotelId !== BETHESDA_IDENTITY_KEY
  ) {
    blockers.push("CANONICAL_HOTEL_ID_MISMATCH");
  }

  let scenarios = [];
  try {
    scenarios = buildBethesdaControlScenarioUniverse(profile);
  } catch (err) {
    blockers.push(String(err?.message || err));
  }

  const cost = estimateCost(scenarios.length || 0);
  const calls = scenarios.length * PROVIDERS.length;
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > BETHESDA_MARRIOTT_COST_CAP_USD)
    blockers.push("COST_CAP_EXCEEDED");

  const byIntent = {};
  for (const s of scenarios) byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;

  const sep = existsSync(
    join(
      process.cwd(),
      `data/ai-demand-positioning/runtime/${BETHESDA_SEP_PERIOD_ID}.json`
    )
  );

  const PREFLIGHT =
    blockers.length === 0 && roundedTotal <= BETHESDA_MARRIOTT_COST_CAP_USD
      ? "PASS"
      : "FAIL";

  return {
    propertyId: BETHESDA_PROPERTY_ID,
    hotelId: BETHESDA_IDENTITY_KEY,
    CENSUS_ID: censusLink?.censusRecordId || null,
    remasurementMode: true,
    controlQuerySetId: BETHESDA_CONTROL_QUERY_SET_ID,
    QUERY_SET_VERSION: "V1",
    QUERY_COUNT: scenarios.length,
    DEMAND_FAMILIES: byIntent,
    METHODOLOGY_VERSION: MEASUREMENT_CONTRACT_VERSION,
    PROMPT_VERSION: "adp_scenario_universe_v1",
    PROVIDER_MODEL_SET: {
      openai: "gpt-4o",
      gemini: "gemini-3.6-flash",
      perplexity: "sonar",
      claude: "claude-sonnet-4-6",
    },
    measurementContractHash,
    TOTAL_SCENARIOS: scenarios.length,
    TOTAL_PLANNED_CALLS: calls,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD: BETHESDA_MARRIOTT_COST_CAP_USD,
    CORE: buildBethesdaCoreGovernanceSummary(),
    sepPeriodPreserved: sep,
    priorPeriodId: BETHESDA_SEP_PERIOD_ID,
    officialBaselineId: BETHESDA_OCT2_BASELINE_ID,
    blockers,
    PREFLIGHT,
  };
}

export async function executeBethesdaOct2OfficialBaseline({
  dryRun = true,
  onProgress = null,
  certify = false,
} = {}) {
  const preflight = buildBethesdaOct2RemeasurePreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return { ok: false, status: "BASELINE_ABORTED_PREFLIGHT", preflight };
  }
  if (preflight.TOTAL_ESTIMATED_COST > BETHESDA_MARRIOTT_COST_CAP_USD) {
    return { ok: false, status: "BASELINE_ABORTED_COST_CAP", preflight };
  }

  const providers = dryRun ? [...PROVIDERS] : availableProviders();
  if (!dryRun && providers.length < 4) {
    return {
      ok: false,
      status: "BASELINE_ABORTED_MISSING_PROVIDER_KEYS",
      providers,
      preflight,
    };
  }

  freezeBethesdaControlQuerySetDoc();

  const RUN_START = new Date().toISOString();
  const profile = loadPropertyProfile(BETHESDA_PROPERTY_ID);
  const scenarios = buildBethesdaControlScenarioUniverse(profile);

  const period = await executeMonitoringPeriod({
    propertyId: BETHESDA_PROPERTY_ID,
    scenarios,
    dryRun,
    providers,
    delayMsOverride: dryRun ? 0 : 200,
    checkpointEvery: 25,
    onProgress,
  });

  let finalPeriod = period;
  if (!dryRun) {
    finalPeriod = parsePeriodObservations(period, profile);
    const ts = new Date().toISOString();
    finalPeriod = {
      ...finalPeriod,
      observations: (finalPeriod.observations || []).map((obs) =>
        applyGovernedInterpretation(obs, profile, {
          timestamp: ts,
          correctionReason: "CANONICAL_OPTION_A_BETHESDA_OCT2_BASELINE_PARSE",
        })
      ),
      governedSubjectPresenceAppliedAt: ts,
      governedSubjectPresenceVersion:
        "adp_bethesda_marriott_baseline_period_002_oct2_path_a",
    };
  }

  finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
    measurementContractHash: preflight.measurementContractHash,
    baselineMarker: BETHESDA_OCT2_BASELINE_MARKER,
    baselineSequence: 2,
    scenarioUniverseVersion: "adp_scenario_universe_v1",
    entityResolutionVersion: BETHESDA_MONTGOMERY_ENTITY_VERSION,
    sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
    providerSet: providers,
    certified: false,
    priorComparablePeriod: BETHESDA_SEP_PERIOD_ID,
  });

  finalPeriod.baselineId = BETHESDA_OCT2_BASELINE_ID;
  finalPeriod.baselineDate = "2026-10-02";
  finalPeriod.pilotStartDate = "2026-10-01";
  finalPeriod.baselineMeasurementDate = "2026-10-02";
  finalPeriod.controlQuerySetId = BETHESDA_CONTROL_QUERY_SET_ID;
  finalPeriod.priorPeriodRole = "PRE_PILOT_REFERENCE";
  finalPeriod.customerVisible = false;
  finalPeriod.customerTrendEligible = false;
  finalPeriod.externalShareDistributed = false;
  finalPeriod.publicationBlocked = true;
  finalPeriod.publicationBlockReason =
    "FOUNDER_REVIEW_REQUIRED_BEFORE_OCT2_BASELINE_PUBLISH";
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;

  if (certify === true) {
    finalPeriod.measurementCertified = true;
    finalPeriod.measurementCertifiedAt = new Date().toISOString();
  }

  const completeness = summarizeProviderCompleteness(
    finalPeriod,
    scenarios.length,
    providers
  );
  const actualCost = Math.round((finalPeriod.costEstimate?.total || 0) * 100) / 100;
  if (actualCost > BETHESDA_MARRIOTT_COST_CAP_USD) {
    finalPeriod.status = "COST_CAP_BREACH";
  }

  savePeriod(finalPeriod);

  const RUN_END = new Date().toISOString();
  const incompleteProviders = Object.entries(completeness.byProvider)
    .filter(([, v]) => v.expected > 0 && v.successful / v.expected < 0.8)
    .map(([p]) => p);

  const completenessPct =
    Math.round(
      (completeness.success / Math.max(1, scenarios.length * providers.length)) *
        1000
    ) / 10;

  const report = {
    ok: incompleteProviders.length === 0 && actualCost <= BETHESDA_MARRIOTT_COST_CAP_USD,
    status: incompleteProviders.length
      ? "PARTIAL_PROVIDER_COMPLETENESS"
      : dryRun
        ? "DRY_RUN_COMPLETE"
        : "EXECUTION_COMPLETE_UNPUBLISHED",
    OFFICIAL_BASELINE_ID: BETHESDA_OCT2_BASELINE_ID,
    PERIOD_MARKER: BETHESDA_OCT2_BASELINE_MARKER,
    PERIOD_ID: finalPeriod.periodId,
    MEASUREMENT_DATE: "2026-10-02",
    PILOT_START: "2026-10-01",
    RUN_START,
    RUN_END,
    QUERY_COUNT: scenarios.length,
    CALLS_ATTEMPTED: completeness.attempted,
    CALLS_SUCCESSFUL: completeness.success,
    CALLS_FAILED: completeness.failed,
    EXPECTED_CALLS: scenarios.length * providers.length,
    COMPLETENESS_PCT: completenessPct,
    EXPECTED_COST: preflight.TOTAL_ESTIMATED_COST,
    ACTUAL_SPEND: actualCost,
    HARD_COST_CAP: BETHESDA_MARRIOTT_COST_CAP_USD,
    CAP_RESPECTED: actualCost <= BETHESDA_MARRIOTT_COST_CAP_USD,
    PROVIDER_COMPLETENESS: completeness.byProvider,
    incompleteProviders,
    controlQuerySetId: BETHESDA_CONTROL_QUERY_SET_ID,
    priorPeriodId: BETHESDA_SEP_PERIOD_ID,
    PUBLISHED: false,
    CERTIFIED: false,
    preflight,
  };

  const outDir = join(
    process.cwd(),
    "reports/bethesda-pilot/adp-baseline/2026-10-02"
  );
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "OCT2_RUN_REPORT.json"),
    JSON.stringify(report, null, 2)
  );

  return report;
}
