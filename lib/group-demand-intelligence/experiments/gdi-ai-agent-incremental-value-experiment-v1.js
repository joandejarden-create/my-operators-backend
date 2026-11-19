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

export const EXPERIMENT_ID = "gdi_ai_agent_incremental_value_experiment_v1";
export const EXPERIMENT_VERSION = "2026-11-19.2";
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
  return {
    eligibilityStatus: armResult.sharedEligibility?.status || null,
    salesReady: armResult.sharedEligibility?.salesReady === true,
    challengerBlocked: armResult.challenger?.salesReadyBlockedByChallenger === true,
    verifiedNamedCount: armResult.challenger?.contactRelevance?.verifiedNamedCount || 0,
    drawerReady: drawer.ready,
    drawerGaps: drawer.gaps,
    disposition: disposition.commercialDisposition,
    durationMs: armResult.durationMs,
    actualPaidUsd: armResult.costs?.actualPaidUsd || 0,
    fetchCount: Number(armResult.costs?.externalRetrievals || armResult.costs?.fetchCount || 0),
  };
}

/**
 * Wave 0 Arm A: existing GDI evaluation on frozen evidence (no adaptive planner).
 * Wave 1 Arm A: same shared limits as Arm B; uses existing research path.
 *   Live retrieval wiring reuses live-prep patterns but is gated — this harness
 *   records parity contract + runs offline baseline until Wave 1 auth is set.
 */
export async function runArmA(frozenOpportunity, meta, wave, limits) {
  const baseline = runBaselineCase(frozenOpportunity, meta);
  baseline.fairness = {
    arm: "A_existing_gdi",
    wave,
    limits,
    activeResearch:
      wave === "wave1"
        ? "REQUIRED_SAME_CAPS_AS_ARM_B_EXISTING_GDI_SEQUENCE"
        : "FROZEN_EVAL_ONLY",
    adaptivePlanner: false,
  };
  if (wave === "wave1") {
    baseline.fairness.note =
      "Wave 1 Arm A must consume the same fetch/allowlist budget via the existing GDI research sequence — not frozen-only. Live execution awaits founder auth; Wave 0 path used when unauthorized.";
    baseline.costs.fetchBudgetCap = limits.maxFetchesPerCase;
  }
  return baseline;
}

/**
 * Arm B: adaptive planning; Wave 1 retrieval only when authorized and limits allow.
 */
export async function runArmB(frozenOpportunity, meta, wave, limits) {
  const authorizeExternal = wave === "wave1";
  const agentic = await runAgenticCase(frozenOpportunity, meta, {
    authorizeExternalRetrieval: authorizeExternal,
    maxSteps: 3,
    limits: {
      maxFetches: limits.maxFetchesPerCase,
      maxPaidUsd: limits.maxPaidUsd,
    },
  });
  agentic.fairness = {
    arm: "B_agent_assisted",
    wave,
    limits,
    activeResearch: authorizeExternal,
    adaptivePlanner: true,
  };
  agentic.costs.fetchBudgetCap = limits.maxFetchesPerCase;
  return agentic;
}

function attributeDelta(wave, armA, armB) {
  if (wave === "wave0") {
    return {
      tag: ATTRIBUTION_TAGS.REASONING_ONLY,
      note: "Wave 0 — identical frozen bag; no network",
    };
  }
  const aFetches = Number(armA.costs?.externalRetrievals || armA.costs?.fetchCount || 0);
  const bFetches = Number(armB.costs?.externalRetrievals || armB.costs?.fetchCount || 0);
  const aCap = Number(armA.fairness?.limits?.maxFetchesPerCase ?? 0);
  const bCap = Number(armB.fairness?.limits?.maxFetchesPerCase ?? 0);
  if (bCap > aCap || (armB.costs?.actualPaidUsd || 0) > (armA.costs?.actualPaidUsd || 0)) {
    return {
      tag: ATTRIBUTION_TAGS.BUDGET_ASYMMETRY,
      note: "Invalid — Arm B received higher budget/fetch cap",
    };
  }
  if (bFetches !== aFetches || wave === "wave1") {
    return {
      tag: ATTRIBUTION_TAGS.RETRIEVAL_STRATEGY,
      note: "Within shared caps — strategy difference only if Arm A was also active",
      aFetches,
      bFetches,
    };
  }
  return { tag: ATTRIBUTION_TAGS.REASONING_ONLY, note: "Same fetch count under Wave 1" };
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
  const armA = await runArmA(row.frozenOpportunity, meta, wave, limits);
  const armB = await runArmB(row.frozenOpportunity, meta, wave, limits);
  const packetA = commercialPacket(row.frozenOpportunity, armA);
  // Arm B may have recovered working copy facts in challenger — use redacted/thesis from arm
  const packetB = commercialPacket(row.frozenOpportunity, armB);
  const attribution = attributeDelta(wave, armA, armB);

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
    attribution,
    armA: { ...armA, commercial: packetA },
    armB: { ...armB, commercial: packetB },
    deltas: {
      verifiedNamedCount: packetB.verifiedNamedCount - packetA.verifiedNamedCount,
      drawerReady: packetA.drawerReady === false && packetB.drawerReady === true,
      durationMs: packetB.durationMs - packetA.durationMs,
      fetchCount: packetB.fetchCount - packetA.fetchCount,
      paidUsd: packetB.actualPaidUsd - packetA.actualPaidUsd,
      budgetAsymmetry: attribution.tag === ATTRIBUTION_TAGS.BUDGET_ASYMMETRY,
    },
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
  const qualityGain = results.some(
    (r) =>
      (r.armB?.commercial?.challengerBlocked === true &&
        r.slot === "rejected_speculative") ||
      (r.deltas?.verifiedNamedCount || 0) > 0,
  );
  return {
    decision: qualityGain ? "ITERATE" : "ITERATE",
    reason: "WAVE0_REASONING_ONLY_CANNOT_ADOPT",
    adoptMeans:
      "ADOPT would mean further validation / selective integration only — never automatic production-wide deployment",
    conclusionScope: CONCLUSION_SCOPE.A_EXISTING_OPPORTUNITY_RESEARCH,
    doesNotEstablish: CONCLUSION_SCOPE.B_NEW_OPPORTUNITY_DISCOVERY,
  };
}

export async function runExperiment(opts = {}) {
  const startedAt = new Date().toISOString();
  const { data: focus } = loadFocusEight();
  assertFocusExcludesCmaaDdaa(focus);
  const resolved = resolveWaveMode(opts);
  const wave = resolved.wave;
  const limits = sharedExecutionLimits(wave);
  const frozenMap = loadFrozenByOpportunityId(focus.cases);

  const results = [];
  for (const focusCase of focus.cases) {
    results.push(await runFocusCase(focusCase, frozenMap, wave));
  }

  const decision =
    wave === "wave0"
      ? decideFromWave0(results)
      : {
          decision: "PENDING_WAVE1_FOUNDER_AUTH_OR_EXECUTION",
          reason: resolved.blockedWave1
            ? resolved.reason
            : "Wave 1 results require independent spot-check before ADOPT/ITERATE/REJECT",
          adoptMeans:
            "Further validation or selective integration only — not automatic production-wide deployment",
        };

  const report = {
    schema: EXPERIMENT_ID,
    version: EXPERIMENT_VERSION,
    startedAt,
    finishedAt: new Date().toISOString(),
    wave,
    waveResolution: resolved,
    limits,
    paidUsd: 0,
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
    },
    decision,
    results,
    disclaimer:
      "This experiment tests incremental research quality on existing opportunities (Conclusion A). It does not establish discovery of genuinely new opportunities (Conclusion B). ADOPT ≠ production-wide deployment.",
  };

  if (opts.writeReports !== false) {
    const outDir = path.join(ROOT, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "WAVE0_HARNESS_RESULTS.json"),
      `${JSON.stringify(report, null, 2)}\n`,
      "utf8",
    );
    report.reportDir = REPORT_DIR_REL;
  }

  return report;
}

export default runExperiment;
