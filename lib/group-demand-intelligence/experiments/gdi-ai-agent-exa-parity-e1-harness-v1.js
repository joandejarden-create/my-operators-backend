/**
 * E1 live Exa-parity experiment harness.
 * Arm A = existing Exa-powered GDI policy; Arm B = same Exa + Dealality adaptive planning.
 * Hard ceiling $20 actual paid; dual founder auth required; no Airtable / publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadFocusEight,
  assertFocusExcludesCmaaDdaa,
} from "./gdi-ai-agent-incremental-value-experiment-v1.js";
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
  createExperimentBudgetLedger,
  E0_LIMITS,
  DENIED_E0_PROVIDERS,
} from "./gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";
import {
  assertE0FairnessCapability,
  loadE0FrozenMap,
} from "./gdi-ai-agent-exa-parity-e0-harness-v1.js";
import {
  assertE1LiveAuthorization,
  createLiveExaExecutor,
  E1_LIVE_EXA_VERSION,
  E1_AUTH_ENV,
  E1_AUTHORIZATION_ID,
} from "./gdi-ai-agent-exa-parity-e1-live-exa-v1.js";

export const E1_HARNESS_VERSION = "gdi-ai-agent-exa-parity-e1-harness-v1";
export { E1_AUTH_ENV, E1_AUTHORIZATION_ID, assertE1LiveAuthorization };

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-ai-agent-exa-powered-parity-experiment-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function plannerDiff(armA, armB) {
  const aDec = (armA.decisions || []).map((d) => d.decision);
  const bDec = (armB.decisions || []).map((d) => d.decision);
  return {
    armADecisionCount: aDec.length,
    armBDecisionCount: bDec.length,
    armAAdaptive: armA.adaptivePlanner === true,
    armBAdaptive: armB.adaptivePlanner === true,
    armBReformulations: (armB.decisions || []).filter(
      (d) => d.reformulation || d.decision === "REFORMULATE_SEARCH",
    ).length,
    armBToolSelections: (armB.decisions || [])
      .filter((d) => d.decision === "ADAPTIVE_TOOL_SELECT")
      .map((d) => d.tool),
    armAStop: armA.stopReason,
    armBStop: armB.stopReason,
    prematureSalesReadyStopBanned: !(armB.decisions || []).some(
      (d) => d.decision === "STOP_SALES_READY",
    ),
    distinctPlanning: JSON.stringify(aDec) !== JSON.stringify(bDec),
    attributionToAdaptivePlanning:
      armB.adaptivePlanner === true && JSON.stringify(aDec) !== JSON.stringify(bDec),
  };
}

export function assertE1Preflight(env = process.env) {
  const auth = assertE1LiveAuthorization(env);
  const fairness = assertE0FairnessCapability();
  const blockers = [];
  if (!auth.ok) blockers.push(...auth.blockers);
  if (!fairness.ok) blockers.push(...fairness.missing.map((m) => `FAIRNESS:${m}`));
  if (E0_LIMITS.perArmPerCaseUsd !== 1.25) blockers.push("PER_ARM_USD_MISMATCH");
  if (E0_LIMITS.e1TotalCeilingUsd !== 20) blockers.push("E1_CEILING_MISMATCH");
  return {
    ok: blockers.length === 0,
    blockers,
    auth,
    fairness,
    limits: E0_LIMITS,
    deniedProviders: [...DENIED_E0_PROVIDERS],
    authorizationId: E1_AUTHORIZATION_ID,
    note: blockers.length
      ? "E1_BLOCKED — resolve blockers before any paid Exa spend"
      : "E1 preflight green — live Exa spend authorized within $20 ceiling",
  };
}

export async function runE1FocusCase(focusCase, frozenMap, {
  experimentLedger,
  env = process.env,
} = {}) {
  const row = frozenMap.get(focusCase.opportunityId);
  if (!row?.frozenOpportunity) {
    return {
      ok: false,
      opportunityId: focusCase.opportunityId,
      error: "FROZEN_SNAPSHOT_MISSING",
    };
  }

  if (experimentLedger && experimentLedger.remainingUsd() < 0.004) {
    return {
      ok: false,
      opportunityId: focusCase.opportunityId,
      error: "EXPERIMENT_USD_CAP_EXCEEDED",
      remainingUsd: experimentLedger.remainingUsd(),
      stoppedBeforeSpend: true,
    };
  }

  const meta = {
    opportunityId: focusCase.opportunityId,
    hotelId: focusCase.hotelId,
    hotelName: focusCase.hotelName,
    slot: focusCase.slot,
    focusIndex: focusCase.focusIndex,
  };

  const createExaExecutor = ({ ledger, arm }) =>
    createLiveExaExecutor({ ledger, arm, env, authorizeExaAgentLive: true });

  const armA = await runArmAExistingExaPoweredPolicy({
    frozenOpportunity: row.frozenOpportunity,
    focusCase,
    meta,
    forceMockOnly: false,
    experimentLedger,
    createExaExecutor,
  });
  const armB = await runArmBAdaptiveExaPlanner({
    frozenOpportunity: row.frozenOpportunity,
    focusCase,
    meta,
    forceMockOnly: false,
    experimentLedger,
    createExaExecutor,
  });

  const deltas = buildEvidenceGroundedDeltas(
    { armA, armB, slot: focusCase.slot },
    { tag: "ADAPTIVE_PLANNING" },
  );
  const material = evaluateMaterialCommercialLift(armA, armB);

  const aPaid = num(armA.costs?.actualPaidUsd);
  const bPaid = num(armB.costs?.actualPaidUsd);

  return {
    ok: true,
    opportunityId: focusCase.opportunityId,
    slot: focusCase.slot,
    hotelId: focusCase.hotelId,
    hotelName: focusCase.hotelName,
    focusIndex: focusCase.focusIndex,
    wave: "e1",
    yieldLabel: "LIVE_EXA",
    disclaimer:
      "E1 live Exa-parity — evidence is paid Exa retrieval under founder $20 ceiling; ADOPT ≠ production deployment",
    armA,
    armB,
    deltas: {
      ...deltas,
      materialLift: material.materialLift,
      materialLiftReasons: material.reasons,
    },
    plannerDiff: plannerDiff(armA, armB),
    budgetParity: {
      aUsdCap: E0_LIMITS.perArmPerCaseUsd,
      bUsdCap: E0_LIMITS.perArmPerCaseUsd,
      aRetrievals: armA.costs.fetchCount,
      bRetrievals: armB.costs.fetchCount,
      aVirtualSettled: armA.costs.virtualSettledUsd,
      bVirtualSettled: armB.costs.virtualSettledUsd,
      aPaid,
      bPaid,
      casePaid: Number((aPaid + bPaid).toFixed(6)),
      asymmetry: false,
    },
    incrementalCommercial: {
      verifiedNamedDelta: material.verifiedNamedDelta,
      genuinelyNewVerifiedDelta: material.genuinelyNewVerifiedDelta,
      materialLift: material.materialLift,
      attributedToAdaptivePlanning:
        material.materialLift === true &&
        plannerDiff(armA, armB).attributionToAdaptivePlanning === true,
    },
  };
}

function sideBySideCase(row) {
  const contacts = (arm) =>
    (arm?.workingOpportunity?.buyerContacts || [])
      .map((c) => ({ name: c.name, role: c.role || null }))
      .concat(
        arm?.workingOpportunity?.namedBuyer
          ? [{ name: arm.workingOpportunity.namedBuyer, role: arm.workingOpportunity.buyerRole || null }]
          : [],
      );
  const uniq = (list) => {
    const seen = new Set();
    const out = [];
    for (const c of list) {
      const k = text(c.name).toLowerCase();
      if (!k || seen.has(k)) continue;
      seen.add(k);
      out.push(c);
    }
    return out;
  };
  return {
    opportunityId: row.opportunityId,
    slot: row.slot,
    hotelName: row.hotelName,
    armA: {
      evidenceCount: row.armA?.commercial?.genuinelyNewVerifiedCount || 0,
      verifiedNamedCount: row.armA?.commercial?.verifiedNamedCount || 0,
      contacts: uniq(contacts(row.armA)),
      eligibility: row.armA?.commercial?.eligibilityStatus || null,
      stopReason: row.armA?.stopReason || null,
      paidUsd: row.budgetParity?.aPaid || 0,
      fetchCount: row.armA?.costs?.fetchCount || 0,
    },
    armB: {
      evidenceCount: row.armB?.commercial?.genuinelyNewVerifiedCount || 0,
      verifiedNamedCount: row.armB?.commercial?.verifiedNamedCount || 0,
      contacts: uniq(contacts(row.armB)),
      eligibility: row.armB?.commercial?.eligibilityStatus || null,
      stopReason: row.armB?.stopReason || null,
      paidUsd: row.budgetParity?.bPaid || 0,
      fetchCount: row.armB?.costs?.fetchCount || 0,
      adaptive: row.armB?.adaptivePlanner === true,
      reformulations: row.plannerDiff?.armBReformulations || 0,
    },
    incremental: row.incrementalCommercial || null,
    materialLift: row.deltas?.materialLift === true,
    materialLiftReasons: row.deltas?.materialLiftReasons || [],
  };
}

function renderFounderMarkdown(report) {
  const lines = [];
  lines.push(`# GDI AI Agent — E1 Live Exa-Parity Founder Report`);
  lines.push("");
  lines.push(`**Status:** ${report.status}`);
  lines.push(`**Commercial verdict:** ${report.decision?.decision || "N/A"}`);
  lines.push(`**Actual paid USD:** $${num(report.actualPaidUsd).toFixed(4)} / $20.00 ceiling`);
  lines.push(`**Authorization:** ${report.authorizationId}`);
  lines.push(`**Harness:** ${report.version}`);
  lines.push("");
  lines.push(`## Verdict`);
  lines.push("");
  lines.push(`- Decision: **${report.decision?.decision || "N/A"}**`);
  lines.push(`- Reason: ${report.decision?.reason || "n/a"}`);
  lines.push(
    `- ADOPT means further selective validation only — not production deployment.`,
  );
  lines.push("");
  lines.push(`## Guardrails`);
  lines.push("");
  lines.push(`- Airtable writes: ${report.airtableWrites}`);
  lines.push(`- Customer publication: ${report.customerPublication}`);
  lines.push(`- Production persistence: ${report.productionPersistence}`);
  lines.push(`- Extra paid providers: denied (${(report.deniedProviders || []).join(", ")})`);
  lines.push(`- Cohort: frozen eight cases only`);
  lines.push("");
  lines.push(`## Eight-case side-by-side`);
  lines.push("");
  for (const c of report.sideBySide || []) {
    lines.push(`### ${c.opportunityId} — ${c.slot} — ${c.hotelName}`);
    lines.push("");
    lines.push(
      `| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |`,
    );
    lines.push(`|-----|----------|----------------|-------------|----------|---------|------|`);
    lines.push(
      `| A existing Exa GDI | ${c.armA.evidenceCount} | ${c.armA.verifiedNamedCount} | ${c.armA.eligibility} | $${num(c.armA.paidUsd).toFixed(4)} | ${c.armA.fetchCount} | ${c.armA.stopReason} |`,
    );
    lines.push(
      `| B Exa + adaptive | ${c.armB.evidenceCount} | ${c.armB.verifiedNamedCount} | ${c.armB.eligibility} | $${num(c.armB.paidUsd).toFixed(4)} | ${c.armB.fetchCount} | ${c.armB.stopReason} |`,
    );
    lines.push("");
    lines.push(
      `- Material lift: ${c.materialLift ? "YES" : "no"} (${(c.materialLiftReasons || []).join(", ") || "none"})`,
    );
    lines.push(
      `- Arm A contacts: ${(c.armA.contacts || []).map((x) => x.name).join("; ") || "—"}`,
    );
    lines.push(
      `- Arm B contacts: ${(c.armB.contacts || []).map((x) => x.name).join("; ") || "—"}`,
    );
    lines.push(
      `- Adaptive reformulations (B): ${c.armB.reformulations}`,
    );
    lines.push("");
  }
  lines.push(`## Spend by case`);
  lines.push("");
  for (const r of report.results || []) {
    if (!r.ok) {
      lines.push(`- ${r.opportunityId}: FAILED (${r.error || "unknown"})`);
      continue;
    }
    lines.push(
      `- ${r.opportunityId}: A=$${num(r.budgetParity?.aPaid).toFixed(4)} B=$${num(r.budgetParity?.bPaid).toFixed(4)} total=$${num(r.budgetParity?.casePaid).toFixed(4)}`,
    );
  }
  lines.push("");
  lines.push(`## Independent challenger note`);
  lines.push("");
  lines.push(
    report.decision?.challengerNote ||
      "Evidence-grounded evaluator + independent challenger; drawerReady-only lifts excluded.",
  );
  lines.push("");
  return `${lines.join("\n")}\n`;
}

export async function runE1Experiment(opts = {}) {
  const startedAt = new Date().toISOString();
  const env = opts.env || process.env;
  const preflight = assertE1Preflight(env);
  if (!preflight.ok) {
    const blocked = {
      schema: "gdi_ai_agent_exa_parity_e1_results_v1",
      version: E1_HARNESS_VERSION,
      status: "E1_BLOCKED",
      ok: false,
      blocked: true,
      reason: "E1_PREFLIGHT_FAILED",
      blockers: preflight.blockers,
      actualPaidUsd: 0,
      airtableWrites: false,
      customerPublication: false,
      productionPersistence: false,
      authorizationId: E1_AUTHORIZATION_ID,
      startedAt,
      finishedAt: new Date().toISOString(),
      decision: {
        decision: "REJECT",
        reason: "E1_BLOCKED_BEFORE_SPEND",
        blockers: preflight.blockers,
      },
      disclaimer:
        "No Exa spend occurred — fail-closed preflight (auth/credential/fairness).",
    };
    if (opts.writeReports !== false) {
      const outDir = path.join(ROOT, REPORT_DIR_REL);
      fs.mkdirSync(outDir, { recursive: true });
      fs.writeFileSync(
        path.join(outDir, "E1_BLOCKED_PREFLIGHT.json"),
        `${JSON.stringify(blocked, null, 2)}\n`,
        "utf8",
      );
      fs.writeFileSync(
        path.join(outDir, "E1_FOUNDER_REPORT.md"),
        renderFounderMarkdown({
          ...blocked,
          sideBySide: [],
          results: [],
          deniedProviders: [...DENIED_E0_PROVIDERS],
        }),
        "utf8",
      );
      blocked.reportDir = REPORT_DIR_REL;
      blocked.reportFile = "E1_BLOCKED_PREFLIGHT.json";
    }
    return blocked;
  }

  const { data: focus } = loadFocusEight();
  assertFocusExcludesCmaaDdaa(focus);
  const frozenMap = loadE0FrozenMap(focus.cases);
  const experimentLedger = createExperimentBudgetLedger({
    maxUsd: E0_LIMITS.e1TotalCeilingUsd,
    authorizationId: E1_AUTHORIZATION_ID,
  });

  const results = [];
  for (const focusCase of focus.cases) {
    if (experimentLedger.remainingUsd() < 0.004) {
      results.push({
        ok: false,
        opportunityId: focusCase.opportunityId,
        error: "EXPERIMENT_USD_CAP_EXCEEDED",
        stoppedBeforeSpend: true,
        remainingUsd: experimentLedger.remainingUsd(),
      });
      break;
    }
    results.push(
      await runE1FocusCase(focusCase, frozenMap, { experimentLedger, env }),
    );
  }

  const actualPaidUsd = num(experimentLedger.snapshot().actualPaidUsd);
  const decision = decideFromWave1EvidenceGrounded(results.filter((r) => r.ok), {
    allowPaidSpend: true,
    maxPaidUsd: E0_LIMITS.e1TotalCeilingUsd,
    conclusionScope: "A_incremental_research_on_existing_opportunities_exa_parity",
  });

  const sideBySide = results.filter((r) => r.ok).map(sideBySideCase);
  const materialCaseIds = sideBySide
    .filter((c) => c.materialLift)
    .map((c) => c.opportunityId);

  const report = {
    schema: "gdi_ai_agent_exa_parity_e1_results_v1",
    version: E1_HARNESS_VERSION,
    armsVersion: E0_ARMS_VERSION,
    liveExaVersion: E1_LIVE_EXA_VERSION,
    evaluatorVersion: EVALUATOR_VERSION,
    status: "E1_COMPLETE",
    ok: true,
    startedAt,
    finishedAt: new Date().toISOString(),
    wave: "e1",
    authorizationId: E1_AUTHORIZATION_ID,
    actualPaidUsd,
    experimentLedger: experimentLedger.snapshot(),
    airtableWrites: false,
    customerPublication: false,
    productionPersistence: false,
    focusCaseCount: results.filter((r) => r.ok).length,
    limits: E0_LIMITS,
    deniedProviders: [...DENIED_E0_PROVIDERS],
    fairnessContract: {
      identicalExaCapabilities: true,
      armAExistingExaPoweredPolicy: true,
      armBDealalityAdaptivePlanner: true,
      perArmPerCaseUsd: E0_LIMITS.perArmPerCaseUsd,
      maxRetrievalsPerCase: E0_LIMITS.maxRetrievalsPerCase,
      e1TotalCeilingUsd: E0_LIMITS.e1TotalCeilingUsd,
      evidenceGroundedMaterialLift: true,
      forceMockOnly: false,
    },
    results,
    sideBySide,
    materialCaseIds,
    decision: {
      ...decision,
      challengerNote:
        "Independent evidence challenger + evidence-grounded materialLift; drawerReady-only excluded; double-count avoided via paired baseline deltas.",
    },
    disclaimer:
      "E1 compares Dealality adaptive planning vs existing Exa-powered GDI under matched Exa tools/budgets. ADOPT ≠ production deployment.",
  };

  if (opts.writeReports !== false) {
    const outDir = path.join(ROOT, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    const name = "E1_HARNESS_RESULTS.json";
    fs.writeFileSync(path.join(outDir, name), `${JSON.stringify(report, null, 2)}\n`, "utf8");
    fs.writeFileSync(
      path.join(outDir, "E1_FOUNDER_REPORT.md"),
      renderFounderMarkdown(report),
      "utf8",
    );
    fs.writeFileSync(
      path.join(outDir, "E1_SIDE_BY_SIDE.json"),
      `${JSON.stringify({ materialCaseIds, sideBySide, decision: report.decision }, null, 2)}\n`,
      "utf8",
    );
    report.reportDir = REPORT_DIR_REL;
    report.reportFile = name;
  }

  return report;
}

export default runE1Experiment;
