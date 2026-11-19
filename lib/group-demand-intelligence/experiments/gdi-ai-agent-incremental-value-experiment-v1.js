/**
 * GDI AI Agent Incremental Value Experiment V1 — thin harness.
 *
 * Fair comparison wrapper over existing 20-opp baseline/agentic runners.
 * Wave 0 (default): offline reasoning parity — both arms, no network.
 * Wave 1: requires GDI_AI_AGENT_LIVE_PUBLIC_AUTH=1 — both arms get the same
 * fetch/budget/allowlist caps; Arm A = existing GDI research path, Arm B = adaptive.
 *
 * Does NOT invent a second research platform. Does NOT write Airtable / publish.
 * Does NOT authorize paid providers. Does NOT run Wave 1 unless founder-gated.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runBaselineCase,
  runAgenticCase,
} from "./gdi-20opp-adaptive-investigation-experiment-v1.js";
import { loadFrozenCase } from "./gdi-20opp-live-research-prep-v1.js";
import {
  DISCOVERY_YIELD_LABEL,
  assessOpportunityDrawerReadiness,
  classifyCommercialDisposition,
  PHASE2B_EXCLUDED_OPPORTUNITY_IDS,
} from "../research/gdi-discovery-intake-disposition-v1.js";
import {
  assertZeroPaidBilling,
  buildSharedParityContext,
  runExistingGdiActiveResearchArm,
  runAgentAdaptiveResearchArm,
  WAVE1_ARMS_VERSION,
} from "./gdi-ai-agent-wave1-fair-parity-arms-v1.js";

export const EXPERIMENT_ID = "gdi_ai_agent_incremental_value_experiment_v1";
export const EXPERIMENT_VERSION = "2026-11-19.3";
export const MAX_FETCHES_PER_CASE = 12;
export const MAX_PAID_USD = 0;
export const WAVE1_AUTH_ENV = "GDI_AI_AGENT_LIVE_PUBLIC_AUTH";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FOCUS_REL =
  "fixtures/gdi-experiments/gdi-ai-agent-incremental-value-experiment-v1/focus-eight-cases.json";
const FROZEN_REL =
  "fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/frozen-case-snapshots.json";
const REPORT_DIR_REL = "reports/gdi-ai-agent-incremental-value-experiment-v1";

export const ATTRIBUTION_TAGS = Object.freeze({
  REASONING_ONLY: "REASONING_ONLY",
  RETRIEVAL_STRATEGY: "RETRIEVAL_STRATEGY",
  PROVIDER_ACCESS: "PROVIDER_ACCESS",
  BUDGET_ASYMMETRY: "BUDGET_ASYMMETRY",
});

export const CONCLUSION_SCOPE = Object.freeze({
  A_EXISTING_OPPORTUNITY_RESEARCH: "A_incremental_research_on_existing_opportunities",
  B_NEW_OPPORTUNITY_DISCOVERY: "B_incremental_discovery_of_genuinely_new_opportunities",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function loadFocusEight() {
  const abs = path.join(ROOT, FOCUS_REL);
  const data = JSON.parse(fs.readFileSync(abs, "utf8"));
  if (!Array.isArray(data.cases) || data.cases.length !== 8) {
    throw new Error("focus-eight-cases.json must lock exactly 8 cases");
  }
  return { abs, data };
}

export function assertFocusExcludesCmaaDdaa(focus = loadFocusEight().data) {
  const ids = new Set((focus.cases || []).map((c) => c.opportunityId));
  for (const banned of [
    ...PHASE2B_EXCLUDED_OPPORTUNITY_IDS,
    ...(focus.excludedFromFocusAndDeepenLoops || []),
  ]) {
    if (ids.has(banned)) {
      throw new Error(`Focus subset must not include deepen-excluded id ${banned}`);
    }
  }
  return true;
}

export function wave1Authorized(env = process.env) {
  return String(env[WAVE1_AUTH_ENV] || "") === "1";
}

export function resolveWaveMode(opts = {}) {
  const requested = text(opts.wave || opts.mode || "wave0").toLowerCase();
  if (requested === "wave1" || requested === "live_public") {
    if (!wave1Authorized(opts.env || process.env) && opts.forceWave1 !== true) {
      return {
        wave: "wave0",
        blockedWave1: true,
        reason: `${WAVE1_AUTH_ENV}=1 required for Wave 1; defaulting to Wave 0`,
      };
    }
    return { wave: "wave1", blockedWave1: false, reason: null };
  }
  if (requested === "wave2" || requested === "paid") {
    return {
      wave: "wave0",
      blockedWave1: false,
      reason: "Wave 2 paid providers require separate founder authorization — not started",
    };
  }
  return { wave: "wave0", blockedWave1: false, reason: null };
}

/**
 * Shared execution limits — both arms must receive identical caps.
 */
export function sharedExecutionLimits(wave) {
  return {
    maxPaidUsd: MAX_PAID_USD,
    maxFetchesPerCase: wave === "wave1" ? MAX_FETCHES_PER_CASE : 0,
    networkEnabled: wave === "wave1",
    allowlistPolicy: wave === "wave1" ? "official_page_same_origin_shared" : "none",
    paidProviders: "denied",
  };
}

function loadFrozenByOpportunityId(focusCases = []) {
  const map = new Map();
  for (const focusCase of focusCases) {
    const id = focusCase.opportunityId;
    try {
      const loaded = loadFrozenCase(id);
      if (loaded?.frozenOpportunity) {
        map.set(id, loaded);
        continue;
      }
    } catch {
      // fall through to bulk fixture
    }
  }
  if (map.size === focusCases.length) return map;

  const abs = path.join(ROOT, FROZEN_REL);
  const frozen = JSON.parse(fs.readFileSync(abs, "utf8"));
  for (const [k, row] of Object.entries(frozen)) {
    const opp = row.frozenOpportunity || row;
    const id = text(opp.id || opp.opportunityId || row.opportunityId);
    if (id && !map.has(id)) {
      map.set(id, { key: k, ...row, frozenOpportunity: opp });
    }
  }
  return map;
}

function commercialPacket(opp, armResult) {
  const drawer = assessOpportunityDrawerReadiness(opp);
  const disposition = classifyCommercialDisposition(opp, {
    targetHotel: opp.hotelName || opp.targetHotelName || "",
  });
  const challenger = armResult.independentChallenger || armResult.challenger || {};
  return {
    eligibilityStatus: armResult.sharedEligibility?.status || null,
    salesReady: armResult.sharedEligibility?.salesReady === true,
    challengerBlocked: challenger.salesReadyBlockedByChallenger === true,
    verifiedNamedCount:
      challenger.contact?.verifiedNamedCount ||
      challenger.contactRelevance?.verifiedNamedCount ||
      0,
    genuinelyNewVerifiedCount: (challenger.buckets?.genuinelyNewVerifiedEvidence || []).length,
    drawerReady: drawer.ready,
    drawerGaps: drawer.gaps,
    disposition: disposition.commercialDisposition,
    durationMs: armResult.durationMs,
    actualPaidUsd: armResult.costs?.actualPaidUsd || 0,
    fetchCount: Number(armResult.costs?.externalRetrievals || armResult.costs?.fetchCount || 0),
  };
}

/**
 * Fail-closed readiness check: Wave 1 must not run with frozen-only Arm A
 * or deferred/non-fetching Arm B.
 */
export function assertFairWave1Capability() {
  const billing = assertZeroPaidBilling();
  const missing = [];
  if (!billing.ok) missing.push("PUBLIC_FETCH_COST_USD_NOT_ZERO");
  if (typeof runExistingGdiActiveResearchArm !== "function") {
    missing.push("ARM_A_ACTIVE_RESEARCH_MISSING");
  }
  if (typeof runAgentAdaptiveResearchArm !== "function") {
    missing.push("ARM_B_ADAPTIVE_RESEARCH_MISSING");
  }
  return {
    ok: missing.length === 0,
    missing,
    armsVersion: WAVE1_ARMS_VERSION,
    billing,
  };
}

/**
 * Wave 0 Arm A: existing GDI evaluation on frozen evidence (no adaptive planner).
 * Wave 1 Arm A: active deterministic official_page sequence under shared caps.
 */
export async function runArmA(frozenOpportunity, meta, wave, limits, parity = null) {
  if (wave === "wave1") {
    const arm = await runExistingGdiActiveResearchArm(frozenOpportunity, {
      maxFetchesPerCase: limits.maxFetchesPerCase,
      allowedOrigins: parity?.allowedOrigins || [],
      targetHotel: meta.hotelName || "",
    });
    arm.fairness = {
      arm: "A_existing_gdi",
      wave,
      limits,
      parity,
      activeResearch: true,
      adaptivePlanner: false,
    };
    return arm;
  }

  const baseline = runBaselineCase(frozenOpportunity, meta);
  baseline.fairness = {
    arm: "A_existing_gdi",
    wave,
    limits,
    activeResearch: false,
    adaptivePlanner: false,
  };
  baseline.costs = {
    ...(baseline.costs || {}),
    fetchCount: 0,
    fetchBudgetCap: 0,
    actualPaidUsd: 0,
  };
  return baseline;
}

/**
 * Wave 0 Arm B: adaptive planning on frozen bag only.
 * Wave 1 Arm B: adaptive bounded public research under identical fetch cap.
 */
export async function runArmB(frozenOpportunity, meta, wave, limits, parity = null) {
  if (wave === "wave1") {
    const arm = await runAgentAdaptiveResearchArm(frozenOpportunity, {
      maxFetchesPerCase: limits.maxFetchesPerCase,
      allowedOrigins: parity?.allowedOrigins || [],
      targetHotel: meta.hotelName || "",
    });
    arm.fairness = {
      arm: "B_agent_assisted",
      wave,
      limits,
      parity,
      activeResearch: true,
      adaptivePlanner: true,
    };
    return arm;
  }

  const agentic = await runAgenticCase(frozenOpportunity, meta, {
    authorizeExternalRetrieval: false,
    maxSteps: 3,
    limits: {
      maxFetches: 0,
      maxPaidUsd: 0,
    },
  });
  agentic.fairness = {
    arm: "B_agent_assisted",
    wave,
    limits,
    activeResearch: false,
    adaptivePlanner: true,
  };
  agentic.costs = {
    ...(agentic.costs || {}),
    fetchCount: 0,
    fetchBudgetCap: 0,
    actualPaidUsd: 0,
  };
  return agentic;
}

function attributeDelta(wave, armA, armB) {
  if (wave === "wave0") {
    return {
      tag: ATTRIBUTION_TAGS.REASONING_ONLY,
      note: "Wave 0 — identical frozen bag; no network",
    };
  }
  const aFetches = Number(armA.costs?.fetchCount || 0);
  const bFetches = Number(armB.costs?.fetchCount || 0);
  const aCap = Number(armA.costs?.fetchBudgetCap ?? armA.fairness?.limits?.maxFetchesPerCase ?? 0);
  const bCap = Number(armB.costs?.fetchBudgetCap ?? armB.fairness?.limits?.maxFetchesPerCase ?? 0);
  if (bCap > aCap || (armB.costs?.actualPaidUsd || 0) > (armA.costs?.actualPaidUsd || 0)) {
    return {
      tag: ATTRIBUTION_TAGS.BUDGET_ASYMMETRY,
      note: "Invalid — Arm B received higher budget/fetch cap",
    };
  }
  if (!armA.activeResearch || !armB.activeResearch) {
    return {
      tag: ATTRIBUTION_TAGS.BUDGET_ASYMMETRY,
      note: "Invalid Wave 1 — one arm was not actively researching",
    };
  }
  if (aFetches !== bFetches || (armA.retrievalRecords || []).length || (armB.retrievalRecords || []).length) {
    return {
      tag: ATTRIBUTION_TAGS.RETRIEVAL_STRATEGY,
      note: "Within shared caps — Arm B adaptive selection vs Arm A deterministic sequence",
      aFetches,
      bFetches,
    };
  }
  return { tag: ATTRIBUTION_TAGS.REASONING_ONLY, note: "Same fetch count under Wave 1" };
}

function commercialFromArm(arm, frozenOpportunity) {
  if (arm?.commercial) return arm.commercial;
  return commercialPacket(frozenOpportunity, arm);
}

export async function runFocusCase(focusCase, frozenMap, wave) {
  const limits = sharedExecutionLimits(wave);
  const row = frozenMap.get(focusCase.opportunityId);
  if (!row?.frozenOpportunity) {
    return {
      ok: false,
      focusIndex: focusCase.focusIndex,
      opportunityId: focusCase.opportunityId,
      error: "FROZEN_SNAPSHOT_MISSING",
    };
  }
  const meta = {
    opportunityId: focusCase.opportunityId,
    hotelId: focusCase.hotelId,
    hotelName: focusCase.hotelName,
    slot: focusCase.slot,
    focusIndex: focusCase.focusIndex,
  };

  const parity =
    wave === "wave1"
      ? buildSharedParityContext(row.frozenOpportunity, limits)
      : null;

  const armA = await runArmA(row.frozenOpportunity, meta, wave, limits, parity);
  const armB = await runArmB(row.frozenOpportunity, meta, wave, limits, parity);
  const packetA = commercialFromArm(armA, row.frozenOpportunity);
  const packetB = commercialFromArm(armB, row.frozenOpportunity);
  const attribution = attributeDelta(wave, armA, armB);

  const materialLift =
    Number(packetB.genuinelyNewVerifiedCount || 0) >
      Number(packetA.genuinelyNewVerifiedCount || 0) ||
    Number(packetB.verifiedNamedCount || 0) > Number(packetA.verifiedNamedCount || 0) ||
    (packetA.drawerReady === false && packetB.drawerReady === true);

  return {
    ok: true,
    focusIndex: focusCase.focusIndex,
    opportunityId: focusCase.opportunityId,
    slot: focusCase.slot,
    hotelId: focusCase.hotelId,
    hotelName: focusCase.hotelName,
    conclusionScope: CONCLUSION_SCOPE.A_EXISTING_OPPORTUNITY_RESEARCH,
    doesNotEstablish: CONCLUSION_SCOPE.B_NEW_OPPORTUNITY_DISCOVERY,
    wave,
    limits,
    parity,
    attribution,
    armA: {
      arm: armA.arm,
      executionKind: armA.executionKind,
      activeResearch: armA.activeResearch === true,
      adaptivePlanner: armA.adaptivePlanner === true,
      fairness: armA.fairness,
      costs: armA.costs,
      retrievalRecords: armA.retrievalRecords || [],
      liveCycleSummary: armA.liveCycleSummary || null,
      pages: armA.pages || null,
      sharedEligibility: armA.sharedEligibility,
      independentChallenger: armA.independentChallenger || armA.challenger || null,
      drawer: armA.drawer || null,
      disposition: armA.disposition || null,
      commercial: packetA,
      durationMs: armA.durationMs,
    },
    armB: {
      arm: armB.arm,
      executionKind: armB.executionKind,
      activeResearch: armB.activeResearch === true,
      adaptivePlanner: armB.adaptivePlanner === true,
      fairness: armB.fairness,
      costs: armB.costs,
      retrievalRecords: armB.retrievalRecords || [],
      liveCycleSummary: armB.liveCycleSummary || null,
      sharedEligibility: armB.sharedEligibility,
      independentChallenger: armB.independentChallenger || armB.challenger || null,
      drawer: armB.drawer || null,
      disposition: armB.disposition || null,
      commercial: packetB,
      durationMs: armB.durationMs,
    },
    deltas: {
      genuinelyNewVerifiedCount:
        Number(packetB.genuinelyNewVerifiedCount || 0) -
        Number(packetA.genuinelyNewVerifiedCount || 0),
      verifiedNamedCount:
        Number(packetB.verifiedNamedCount || 0) - Number(packetA.verifiedNamedCount || 0),
      drawerReady: packetA.drawerReady === false && packetB.drawerReady === true,
      durationMs: Number(packetB.durationMs || 0) - Number(packetA.durationMs || 0),
      fetchCount: Number(packetB.fetchCount || 0) - Number(packetA.fetchCount || 0),
      paidUsd: Number(packetB.actualPaidUsd || 0) - Number(packetA.actualPaidUsd || 0),
      budgetAsymmetry: attribution.tag === ATTRIBUTION_TAGS.BUDGET_ASYMMETRY,
      materialLift,
    },
    // Conclusion A: researching existing frozen opportunities — not new-org discovery.
    discoveryYieldNote: DISCOVERY_YIELD_LABEL.INVENTORY_REOPEN,
  };
}

export function decideFromWave0(results) {
  // Wave 0 cannot ADOPT
  const budgetFail = results.some((r) => r.deltas?.budgetAsymmetry);
  if (budgetFail) {
    return {
      decision: "REJECT",
      reason: "BUDGET_ASYMMETRY",
      adoptMeans: null,
    };
  }
  return {
    decision: "ITERATE",
    reason: "WAVE0_REASONING_ONLY_CANNOT_ADOPT",
    adoptMeans:
      "ADOPT would mean further validation / selective integration only — never automatic production-wide deployment",
    conclusionScope: CONCLUSION_SCOPE.A_EXISTING_OPPORTUNITY_RESEARCH,
    doesNotEstablish: CONCLUSION_SCOPE.B_NEW_OPPORTUNITY_DISCOVERY,
  };
}

/**
 * Wave 1 decision per locked protocol (Conclusion A only).
 * ADOPT = further validation / selective integration — not production-wide deploy.
 */
export function decideFromWave1(results) {
  const adoptMeans =
    "Further validation or selective integration only — not automatic production-wide deployment";
  const base = {
    adoptMeans,
    conclusionScope: CONCLUSION_SCOPE.A_EXISTING_OPPORTUNITY_RESEARCH,
    doesNotEstablish: CONCLUSION_SCOPE.B_NEW_OPPORTUNITY_DISCOVERY,
  };

  if (results.some((r) => r.ok === false)) {
    return { decision: "REJECT", reason: "FOCUS_CASE_EXECUTION_FAILURE", ...base };
  }
  if (results.some((r) => r.deltas?.budgetAsymmetry)) {
    return { decision: "REJECT", reason: "BUDGET_ASYMMETRY", ...base };
  }
  if (results.some((r) => !r.armA?.activeResearch || !r.armB?.activeResearch)) {
    return { decision: "REJECT", reason: "UNFAIR_PARITY_ARM_NOT_ACTIVE", ...base };
  }
  if (results.some((r) => Number(r.armA?.commercial?.actualPaidUsd || 0) > 0 || Number(r.armB?.commercial?.actualPaidUsd || 0) > 0)) {
    return { decision: "REJECT", reason: "PAID_SPEND_NOT_ZERO", ...base };
  }

  const falseSalesReady = results.filter(
    (r) =>
      r.armB?.commercial?.salesReady === true &&
      r.armB?.commercial?.challengerBlocked === true,
  );
  if (falseSalesReady.length) {
    return {
      decision: "REJECT",
      reason: "FALSE_SALES_READY_PRESSURE",
      falseSalesReadyIds: falseSalesReady.map((r) => r.opportunityId),
      ...base,
    };
  }

  const speculative = results.filter((r) => r.slot === "rejected_speculative");
  const speculativeHeld = speculative.every(
    (r) => r.armB?.commercial?.salesReady !== true,
  );
  const competitor = results.filter((r) => r.slot === "competitor_cross_hotel");
  const competitorNotFalselyPromoted = competitor.every(
    (r) => r.armB?.commercial?.salesReady !== true,
  );

  const materialCases = results.filter((r) => r.deltas?.materialLift === true);
  const qualityDemotion =
    speculativeHeld &&
    competitorNotFalselyPromoted &&
    results.some(
      (r) =>
        (r.slot === "rejected_speculative" || r.slot === "competitor_cross_hotel") &&
        r.armB?.commercial?.challengerBlocked === true,
    );

  if (
    materialCases.length >= 2 &&
    speculativeHeld &&
    competitorNotFalselyPromoted
  ) {
    return {
      decision: "ADOPT",
      reason: "GE_2_MATERIAL_LIFTS_WITH_SAFE_GATES",
      materialCaseIds: materialCases.map((r) => r.opportunityId),
      ...base,
    };
  }

  if (materialCases.length >= 1 || qualityDemotion) {
    return {
      decision: "ITERATE",
      reason:
        materialCases.length >= 1
          ? "SOME_MATERIAL_LIFT_BELOW_ADOPT_THRESHOLD"
          : "QUALITY_DEMOTIONS_WITHOUT_MATERIAL_LIFT",
      materialCaseIds: materialCases.map((r) => r.opportunityId),
      ...base,
    };
  }

  return {
    decision: "REJECT",
    reason: "NO_CONFIRMED_COMMERCIAL_LIFT_OR_QUALITY_GAIN",
    ...base,
  };
}

export async function runExperiment(opts = {}) {
  const startedAt = new Date().toISOString();
  const { data: focus } = loadFocusEight();
  assertFocusExcludesCmaaDdaa(focus);
  const resolved = resolveWaveMode(opts);
  const wave = resolved.wave;
  const limits = sharedExecutionLimits(wave);

  if (wave === "wave1") {
    const readiness = assertFairWave1Capability();
    if (!readiness.ok) {
      return {
        schema: EXPERIMENT_ID,
        version: EXPERIMENT_VERSION,
        wave,
        ok: false,
        blocked: true,
        reason: "FAIR_WAVE1_CAPABILITY_MISSING",
        missing: readiness.missing,
        decision: {
          decision: "REJECT",
          reason: "INVALID_EXPERIMENT_HARNESS",
        },
      };
    }
  }

  const frozenMap = loadFrozenByOpportunityId(focus.cases);

  const results = [];
  for (const focusCase of focus.cases) {
    results.push(await runFocusCase(focusCase, frozenMap, wave));
  }

  const decision =
    wave === "wave0"
      ? decideFromWave0(results)
      : decideFromWave1(results);

  const paidUsd = results.reduce(
    (s, r) =>
      s +
      Number(r.armA?.commercial?.actualPaidUsd || 0) +
      Number(r.armB?.commercial?.actualPaidUsd || 0),
    0,
  );

  const report = {
    schema: EXPERIMENT_ID,
    version: EXPERIMENT_VERSION,
    startedAt,
    finishedAt: new Date().toISOString(),
    wave,
    waveResolution: resolved,
    limits,
    paidUsd,
    airtableWrites: 0,
    customerPublication: false,
    wave1Executed: wave === "wave1",
    focusCaseCount: results.length,
    focusOpportunityIds: results.map((r) => r.opportunityId),
    excludedDeepenIds: focus.excludedFromFocusAndDeepenLoops,
    conclusionScope: CONCLUSION_SCOPE.A_EXISTING_OPPORTUNITY_RESEARCH,
    doesNotEstablish: CONCLUSION_SCOPE.B_NEW_OPPORTUNITY_DISCOVERY,
    fairnessContract: {
      armAActiveInWave1: true,
      armBAdaptiveInWave1: true,
      identicalFetchBudgetRequired: true,
      frozenOnlyBaselineForbiddenInWave1: true,
      armsVersion: WAVE1_ARMS_VERSION,
    },
    decision,
    results,
    disclaimer:
      "This experiment tests incremental research quality on existing opportunities (Conclusion A). It does not establish discovery of genuinely new opportunities (Conclusion B). ADOPT ≠ production-wide deployment.",
  };

  if (opts.writeReports !== false) {
    const outDir = path.join(ROOT, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    const name = wave === "wave1" ? "WAVE1_RESULTS.json" : "WAVE0_HARNESS_RESULTS.json";
    fs.writeFileSync(path.join(outDir, name), `${JSON.stringify(report, null, 2)}\n`, "utf8");
    report.reportDir = REPORT_DIR_REL;
    report.reportFile = name;
  }

  return report;
}

export default runExperiment;
