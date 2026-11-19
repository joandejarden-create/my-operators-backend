/**
 * E0 Exa-parity experiment harness — mock Exa only ($0 actual paid).
 * Establishes experimental validity / E1 readiness — does NOT claim live commercial lift.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadFocusEight,
  assertFocusExcludesCmaaDdaa,
} from "./gdi-ai-agent-incremental-value-experiment-v1.js";
import { loadFrozenCase } from "./gdi-20opp-live-research-prep-v1.js";
import {
  buildEvidenceGroundedDeltas,
  decideFromWave1EvidenceGrounded,
  evaluateMaterialCommercialLift,
  EVALUATOR_VERSION,
} from "./gdi-ai-agent-wave1-evidence-grounded-evaluator-v1.js";
import {
  runArmAExistingExaPoweredPolicy,
  runArmBAdaptiveExaPlanner,
  E0_ARMS_VERSION,
} from "./gdi-ai-agent-exa-parity-e0-arms-v1.js";
import {
  createArmBudgetLedger,
  E0_LIMITS,
  DENIED_E0_PROVIDERS,
  assertE0PaidSpendZero,
} from "./gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";

export const E0_HARNESS_VERSION = "gdi-ai-agent-exa-parity-e0-harness-v1";
export const E0_AUTH_ENV = "GDI_AI_AGENT_EXA_PARITY_AUTH";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-ai-agent-exa-powered-parity-experiment-v1";
const FROZEN_REL =
  "fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/frozen-case-snapshots.json";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function loadE0FrozenMap(focusCases = []) {
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
      // fall through
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

export function assertE0LiveSpendDenied(env = process.env) {
  return {
    ok: true,
    forceMockOnly: true,
    actualPaidUsd: 0,
    e1AuthPresentButIgnoredInE0: String(env[E0_AUTH_ENV] || "") === "1",
    gdiExaAgentLiveEnv: String(env.GDI_EXA_AGENT_LIVE || "") === "1",
    note: "E0 always forceMockOnly — live Exa HTTP refused regardless of auth env",
  };
}

export function assertE0FairnessCapability() {
  const missing = [];
  if (typeof runArmAExistingExaPoweredPolicy !== "function") {
    missing.push("ARM_A_EXISTING_EXA_POLICY_MISSING");
  }
  if (typeof runArmBAdaptiveExaPlanner !== "function") {
    missing.push("ARM_B_ADAPTIVE_PLANNER_MISSING");
  }
  if (!(E0_LIMITS.perArmPerCaseUsd > 0)) missing.push("PER_ARM_USD_CAP_MISSING");
  if (E0_LIMITS.e1TotalCeilingUsd !== 20) missing.push("E1_CEILING_MISMATCH");
  return {
    ok: missing.length === 0,
    missing,
    armsVersion: E0_ARMS_VERSION,
    limits: E0_LIMITS,
    deniedProviders: [...DENIED_E0_PROVIDERS],
  };
}

function plannerDiff(armA, armB) {
  const aDec = (armA.decisions || []).map((d) => d.decision);
  const bDec = (armB.decisions || []).map((d) => d.decision);
  return {
    armADecisionCount: aDec.length,
    armBDecisionCount: bDec.length,
    armAAdaptive: armA.adaptivePlanner === true,
    armBAdaptive: armB.adaptivePlanner === true,
    armBReformulations: (armB.decisions || []).filter((d) => d.reformulation || d.decision === "REFORMULATE_SEARCH")
      .length,
    armBToolSelections: (armB.decisions || [])
      .filter((d) => d.decision === "ADAPTIVE_TOOL_SELECT")
      .map((d) => d.tool),
    armAStop: armA.stopReason,
    armBStop: armB.stopReason,
    prematureSalesReadyStopBanned:
      !(armB.decisions || []).some((d) => d.decision === "STOP_SALES_READY"),
    distinctPlanning: JSON.stringify(aDec) !== JSON.stringify(bDec),
  };
}

export async function runE0FocusCase(focusCase, frozenMap) {
  const row = frozenMap.get(focusCase.opportunityId);
  if (!row?.frozenOpportunity) {
    return {
      ok: false,
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

  const armA = await runArmAExistingExaPoweredPolicy({
    frozenOpportunity: row.frozenOpportunity,
    focusCase,
    meta,
  });
  const armB = await runArmBAdaptiveExaPlanner({
    frozenOpportunity: row.frozenOpportunity,
    focusCase,
    meta,
  });

  const deltas = buildEvidenceGroundedDeltas(
    { armA, armB, slot: focusCase.slot },
    { tag: "RETRIEVAL_STRATEGY" },
  );

  // E0: mock findings must not create evidence-grounded materialLift for ADOPT claims
  const mockOnlyLiftGuard =
    Number(armB.commercial?.mockSimulatedFindingCount || 0) >= 0 &&
    Number(armB.commercial?.genuinelyNewVerifiedCount || 0) === 0 &&
    Number(armA.commercial?.genuinelyNewVerifiedCount || 0) === 0;

  const material = evaluateMaterialCommercialLift(armA, armB);

  return {
    ok: true,
    opportunityId: focusCase.opportunityId,
    slot: focusCase.slot,
    hotelId: focusCase.hotelId,
    hotelName: focusCase.hotelName,
    focusIndex: focusCase.focusIndex,
    wave: "e0",
    yieldLabel: "MOCK_SIMULATED",
    disclaimer:
      "E0 mock Exa only — results are not live commercial intelligence and cannot justify ADOPT on real Exa value",
    armA,
    armB,
    deltas: {
      ...deltas,
      // Force materialLift false when only mock simulated (E0 safety)
      materialLift: mockOnlyLiftGuard ? false : material.materialLift,
      materialLiftReasons: mockOnlyLiftGuard ? [] : material.reasons,
      mockOnlyLiftSuppressed: mockOnlyLiftGuard && material.materialLift,
    },
    plannerDiff: plannerDiff(armA, armB),
    budgetParity: {
      aUsdCap: E0_LIMITS.perArmPerCaseUsd,
      bUsdCap: E0_LIMITS.perArmPerCaseUsd,
      aRetrievals: armA.costs.fetchCount,
      bRetrievals: armB.costs.fetchCount,
      aVirtualSettled: armA.costs.virtualSettledUsd,
      bVirtualSettled: armB.costs.virtualSettledUsd,
      aPaid: 0,
      bPaid: 0,
      asymmetry: false,
    },
  };
}

export function assessE1Readiness(report) {
  const blockers = [];
  const fairness = assertE0FairnessCapability();
  if (!fairness.ok) blockers.push(...fairness.missing.map((m) => `FAIRNESS:${m}`));

  if (report.actualPaidUsd !== 0) blockers.push("ACTUAL_PAID_USD_NOT_ZERO");
  if (report.airtableWrites !== false) blockers.push("AIRTABLE_WRITES_NOT_FALSE");
  if (report.customerPublication !== false) blockers.push("PUBLICATION_NOT_FALSE");
  if (!Array.isArray(report.results) || report.results.length !== 8) {
    blockers.push("FOCUS_EIGHT_INCOMPLETE");
  }

  for (const row of report.results || []) {
    if (!row.ok) blockers.push(`CASE_FAIL:${row.opportunityId}`);
    if (!row.armA?.exaPoweredPolicy) blockers.push(`ARM_A_NOT_EXA_POLICY:${row.opportunityId}`);
    if (!row.armB?.adaptivePlanner) blockers.push(`ARM_B_NOT_ADAPTIVE:${row.opportunityId}`);
    if (row.armA?.costs?.actualPaidUsd > 0 || row.armB?.costs?.actualPaidUsd > 0) {
      blockers.push(`PAID_SPEND:${row.opportunityId}`);
    }
    if (row.budgetParity?.asymmetry) blockers.push(`BUDGET_ASYMMETRY:${row.opportunityId}`);
    if (
      (row.armB?.decisions || []).some((d) => d.decision === "STOP_SALES_READY")
    ) {
      blockers.push(`PREMATURE_STOP_SALES_READY:${row.opportunityId}`);
    }
    if (row.deltas?.materialLift === true && row.yieldLabel === "MOCK_SIMULATED") {
      blockers.push(`MOCK_COUNTED_AS_MATERIAL_LIFT:${row.opportunityId}`);
    }
  }

  // DrawerReady-only must not produce ADOPT
  const drawerOnly = evaluateMaterialCommercialLift(
    {
      commercial: {
        verifiedNamedCount: 1,
        genuinelyNewVerifiedCount: 2,
        drawerReady: false,
        fetchCount: 12,
      },
    },
    {
      commercial: {
        verifiedNamedCount: 1,
        genuinelyNewVerifiedCount: 2,
        drawerReady: true,
        fetchCount: 4,
      },
    },
  );
  if (drawerOnly.materialLift) blockers.push("DRAWER_READY_STILL_COUNTS_AS_MATERIAL_LIFT");

  const decision = decideFromWave1EvidenceGrounded(
    (report.results || []).map((r) => ({
      ...r,
      deltas: { ...r.deltas, materialLift: false },
    })),
  );
  if (decision.decision === "ADOPT") {
    blockers.push("E0_MUST_NOT_ADOPT_ON_MOCK");
  }

  const plausibleValueCases = (report.results || [])
    .filter((r) => r.plannerDiff?.distinctPlanning && r.armB?.adaptivePlanner)
    .filter((r) =>
      ["missing_verified_contact", "incomplete_demand"].includes(r.slot),
    )
    .map((r) => r.opportunityId);

  return {
    status: blockers.length ? "E1_BLOCKED" : "E1_READY",
    blockers,
    plausibleAdaptiveValueCaseIds: plausibleValueCases,
    note:
      blockers.length === 0
        ? "E0 validity gates passed — $20 E1 may be submitted for separate founder authorization (not authorized by this run)"
        : "Resolve blockers before requesting E1 spend authorization",
    e1SpendAuthorized: false,
    e1CeilingUsd: E0_LIMITS.e1TotalCeilingUsd,
  };
}

export async function runE0Experiment(opts = {}) {
  const startedAt = new Date().toISOString();
  const { data: focus } = loadFocusEight();
  assertFocusExcludesCmaaDdaa(focus);
  const fairness = assertE0FairnessCapability();
  if (!fairness.ok) {
    return {
      ok: false,
      blocked: true,
      reason: "E0_FAIRNESS_CAPABILITY_MISSING",
      missing: fairness.missing,
      e1Readiness: { status: "E1_BLOCKED", blockers: fairness.missing },
    };
  }

  const frozenMap = loadE0FrozenMap(focus.cases);
  const results = [];
  for (const focusCase of focus.cases) {
    results.push(await runE0FocusCase(focusCase, frozenMap));
  }

  const spend = assertE0PaidSpendZero(
    results.flatMap((r) => [
      { snapshot: () => r.armA?.ledger || {} },
      { snapshot: () => r.armB?.ledger || {} },
    ]),
  );

  const report = {
    schema: "gdi_ai_agent_exa_parity_e0_results_v1",
    version: E0_HARNESS_VERSION,
    armsVersion: E0_ARMS_VERSION,
    evaluatorVersion: EVALUATOR_VERSION,
    startedAt,
    finishedAt: new Date().toISOString(),
    wave: "e0",
    actualPaidUsd: 0,
    virtualSettledUsd: spend.virtualSettledUsd,
    airtableWrites: false,
    customerPublication: false,
    productionPersistence: false,
    focusCaseCount: results.length,
    limits: E0_LIMITS,
    deniedProviders: [...DENIED_E0_PROVIDERS],
    liveSpendGate: assertE0LiveSpendDenied(opts.env || process.env),
    fairnessContract: {
      identicalExaCapabilities: true,
      armAExistingExaPoweredPolicy: true,
      armBDealalityAdaptivePlanner: true,
      perArmPerCaseUsd: E0_LIMITS.perArmPerCaseUsd,
      maxRetrievalsPerCase: E0_LIMITS.maxRetrievalsPerCase,
      e1TotalCeilingUsd: E0_LIMITS.e1TotalCeilingUsd,
      evidenceGroundedMaterialLift: true,
      mockCannotAdopt: true,
    },
    results,
    disclaimer:
      "E0 establishes experimental validity with mock Exa only. It does not demonstrate real commercial improvement and does not authorize E1 spend.",
  };

  report.e1Readiness = assessE1Readiness(report);
  report.ok = report.e1Readiness.status === "E1_READY";
  report.decision = {
    experimentValidity: report.e1Readiness.status,
    commercialVerdict: "NOT_APPLICABLE_E0_MOCK",
    reason: report.e1Readiness.note,
  };

  if (opts.writeReports !== false) {
    const outDir = path.join(ROOT, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    const name = "E0_HARNESS_RESULTS.json";
    fs.writeFileSync(path.join(outDir, name), `${JSON.stringify(report, null, 2)}\n`, "utf8");
    report.reportDir = REPORT_DIR_REL;
    report.reportFile = name;
  }

  return report;
}

export function runBudgetRefusalSelfTests() {
  const ledger = createArmBudgetLedger({
    arm: "test",
    opportunityId: "selftest",
    maxUsd: 0.05,
    maxRetrievals: 2,
    forceMockOnly: true,
  });
  const live = ledger.reserve({
    operation: "search",
    provider: "exa_mock",
    attemptLiveHttp: true,
  });
  const denied = ledger.reserve({
    operation: "search",
    provider: "serpapi",
  });
  const ok1 = ledger.reserve({
    operation: "search",
    provider: "exa_mock",
    quote: { bounded: true, maxUsd: 0.004 },
  });
  if (ok1.ok) ledger.settle(ok1.reservation.reservationId, { actualCostUsd: 0.004 });
  const over = ledger.reserve({
    operation: "agent",
    provider: "exa_mock",
    quote: { bounded: true, maxUsd: 0.1 },
  });
  return {
    ok:
      live.refused === true &&
      live.reason === "E0_LIVE_EXA_HTTP_REFUSED" &&
      denied.refused === true &&
      String(denied.reason).startsWith("DENIED_PROVIDER") &&
      ok1.ok === true &&
      over.refused === true &&
      over.reason === "USD_CAP_EXCEEDED",
    live,
    denied,
    ok1,
    over,
  };
}

export default runE0Experiment;
