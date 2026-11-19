/**
 * GDI Agent Test 001 — Phase 2 live-research PREPARATION (5-case).
 *
 * Extends the frozen 20-opp experiment with a gated, budget-bounded live
 * public-research harness. Default execution is mocked / $0 only.
 *
 * Founder must separately authorize:
 *   GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1  → live $0 official_page fetches
 *   GDI_20OPP_LIVE_PAID_BUDGET_FOUNDER_AUTH=<id> + hardCeiling → paid calls
 *
 * Does NOT publish, write Airtable, or auto-execute the $20 paid budget.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { evaluateOpportunity } from "../research/opportunity-research-supervisor-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { recoverCommercialEvidence } from "../commercial-evidence-recovery-v1.js";
import {
  selectAdaptiveNextAction,
  runBoundedPublicResearchCycle,
  DENIED_METERED_PROVIDERS,
  PHASE2G_PILOT_LIMITS,
} from "../adaptive-public-research-controller-v1.js";
import { buildAdaptiveResearchPreflight } from "../adaptive-research-preflight-v1.js";
import { redactOpportunityForShadowExport } from "../gdi2-shadow-publication-evaluation-v1.js";
import {
  runEvidenceChallenger,
  runBaselineCase,
} from "./gdi-20opp-adaptive-investigation-experiment-v1.js";

export const PREP_EXPERIMENT_ID = "gdi_20opp_live_research_prep_v1";
export const PREP_VERSION = "2026-11-19.1";
export const MAX_PER_CASE_USD = 4;
export const MAX_TOTAL_USD = 20;
export const LIVE_PUBLIC_AUTH_ENV = "GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH";
export const LIVE_PAID_AUTH_ENV = "GDI_20OPP_LIVE_PAID_BUDGET_FOUNDER_AUTH";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FROZEN_PATH = path.join(
  ROOT,
  "fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/frozen-case-snapshots.json",
);
const COHORT_PATH = path.join(
  ROOT,
  "fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/cohort-manifest.public.json",
);
const PHASE1_RESULTS = path.join(
  ROOT,
  "reports/gdi-20opp-adaptive-investigation-experiment-v1/RESULTS.json",
);

/** Provider allowlist for this prep harness. */
export const PROVIDER_ALLOWLIST = Object.freeze({
  always: Object.freeze(["mock", "offline_recovery", "deterministic_ors"]),
  livePublicZeroCost: Object.freeze(["official_page", "direct_public"]),
  paidRequiresSeparateAuth: Object.freeze([...DENIED_METERED_PROVIDERS]),
});

/**
 * Five cases from the frozen 20-case cohort (immutable IDs).
 * Selection rationale is frozen here; held-out labels are never passed to arms.
 */
export const FIVE_CASE_SELECTION = Object.freeze([
  {
    slot: "incomplete_demand_evidence",
    opportunityId: "disp_recLuxvwwxID7U2B8_aledade",
    cohortCaseIndex: 2,
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    reason:
      "Baseline gaps include MISSING_HOTEL_DEMAND_THESIS + SOURCES_INSUFFICIENT — incomplete commercial demand evidence.",
    researchQuestions: [
      "What concrete Bethesda/DMV hotel demand (rooms, dates, event) is evidenced for Aledade?",
      "Which official sources corroborate a forward-looking lodging need?",
    ],
    potentialCommercialValue:
      "Local corporate/association meetings account with possible recurring training lodging near HQ.",
    desiredOutcomes: ["evidence-backed demand thesis", "source provenance"],
  },
  {
    slot: "missing_verified_contact",
    opportunityId: "gdi_lmc_00562bf81b1e1957",
    cohortCaseIndex: 3,
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    reason:
      "MISSING_NAMED_RELEVANT_BUYER + verifiedNamed=0 — contact gap is the primary commercial blocker.",
    researchQuestions: [
      "Who is the named meetings/catering buyer for CMAA San Diego Chapter?",
      "Is there a verified public contact route (email/role) without paid enrichment?",
    ],
    potentialCommercialValue:
      "Local meetings & catering recurring account for waterfront hotel inventory.",
    desiredOutcomes: ["verified named buyer or credible entry point"],
  },
  {
    slot: "forward_looking_demand",
    opportunityId: "gdi_opp_2027_ddaa_annual_conference_8",
    cohortCaseIndex: 10,
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    reason:
      "2027 DDAA annual conference — forward-looking project/workforce-adjacent demand with thesis gap; potential undiscovered lodging need.",
    researchQuestions: [
      "Are 2027 conference dates/city confirmed on official pages?",
      "Is overflow/VIP housing still open (not already contracted elsewhere)?",
    ],
    potentialCommercialValue:
      "Multi-night conference overflow / VIP housing near Rockville–Bethesda corridor.",
    desiredOutcomes: ["confirmed or reliably inferred 2027 demand", "timing provenance"],
  },
  {
    slot: "rejected_unsupported",
    opportunityId: "gdi_opp_ajas_hq_rockville_watch",
    cohortCaseIndex: 17,
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    reason:
      "Cohort slot rejected_unsupported (SPECULATIVE_WATCH-class). Tests whether live research correctly keeps or overturns rejection.",
    researchQuestions: [
      "Is there any evidence-backed lodging demand beyond HQ proximity speculation?",
      "Should commercial eligibility remain RESEARCH_CONTINUATION / ineligible?",
    ],
    potentialCommercialValue:
      "Low unless new confirmed demand appears; value may be correct rejection.",
    desiredOutcomes: ["correct non-promotion if still speculative"],
  },
  {
    slot: "cross_hotel_booked",
    opportunityId: "gdi_opp_wrcamp_altaroma_2027_altaroma_organizer",
    cohortCaseIndex: 19,
    hotelId: "rece0or38cxo3Fymb",
    hotelName: "W Rome",
    reason:
      "Already-contracted / organizer-placed Altaroma case — tests duplicate and cross-hotel reuse handling under live research.",
    researchQuestions: [
      "Confirm venue already placed / contracted status from public sources.",
      "Is there residual overflow demand usable by W Rome without double-counting?",
    ],
    potentialCommercialValue:
      "Possible overflow only; primary value is avoiding false sales-ready promotion.",
    desiredOutcomes: ["duplicate/contracted detection", "cross-hotel reuse note"],
  },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha256(v) {
  return createHash("sha256").update(String(v || "")).digest("hex");
}

export function loadFrozenCase(opportunityId) {
  const frozen = JSON.parse(fs.readFileSync(FROZEN_PATH, "utf8"));
  for (const [key, row] of Object.entries(frozen)) {
    if (row.opportunityId === opportunityId) {
      return {
        caseKey: key,
        opportunityId: row.opportunityId,
        hotelId: row.hotelId,
        snapshotSha256: row.snapshotSha256,
        frozenOpportunity: JSON.parse(JSON.stringify(row.frozenOpportunity)),
      };
    }
  }
  throw new Error(`Frozen opportunity not found: ${opportunityId}`);
}

export function loadHeldOutLabels(opportunityId) {
  const cohort = JSON.parse(fs.readFileSync(COHORT_PATH, "utf8"));
  const row = (cohort.cases || []).find((c) => c.opportunityId === opportunityId);
  return row?.evaluationLabelsHeldOut || null;
}

/**
 * Authorization gate — fail-closed.
 */
export function assertResearchAuthorization(opts = {}) {
  const mode = opts.mode || "mock";
  const env = opts.env || process.env;
  const livePublic =
    mode === "live_public" ||
    opts.authorizeLivePublicResearch === true ||
    String(env[LIVE_PUBLIC_AUTH_ENV] || "") === "1";
  const paidAuth = text(env[LIVE_PAID_AUTH_ENV] || opts.paidAuthorizationId || "");
  const authorizePaid = opts.authorizePaidResearch === true && Boolean(paidAuth);

  // Paid path is always refused by this prep harness when explicitly requested.
  if (opts.authorizePaidResearch === true || mode === "paid") {
    return {
      ok: false,
      mode: "paid_blocked_until_explicit_execution_auth",
      authorizeLivePublicResearch: false,
      authorizePaidResearch: false,
      paidAuthorizationIdPresent: Boolean(paidAuth),
      maxPerCaseUsd: MAX_PER_CASE_USD,
      maxTotalUsd: MAX_TOTAL_USD,
      hardCeilingUsd: 0,
      note:
        "Paid execution was requested, but is intentionally blocked in this prep harness. Use a separate founder-authorized execution step later.",
      refused: true,
    };
  }

  if (livePublic && mode !== "mock") {
    return {
      ok: true,
      mode: "live_public_zero_cost",
      authorizeLivePublicResearch: true,
      authorizePaidResearch: false,
      allowedProviders: PROVIDER_ALLOWLIST.livePublicZeroCost,
      deniedProviders: PROVIDER_ALLOWLIST.paidRequiresSeparateAuth,
      maxPerCaseUsd: MAX_PER_CASE_USD,
      maxTotalUsd: MAX_TOTAL_USD,
      hardCeilingUsd: 0,
      note: "Live official_page/direct_public only at verified $0. Metered providers denied.",
    };
  }

  if (mode === "mock" || (!livePublic && !authorizePaid)) {
    return {
      ok: true,
      mode: "mock",
      authorizeLivePublicResearch: false,
      authorizePaidResearch: false,
      maxPerCaseUsd: MAX_PER_CASE_USD,
      maxTotalUsd: MAX_TOTAL_USD,
      hardCeilingUsd: 0,
      note: "Mock/offline only — $0 actual spend. Live and paid budgets not authorized.",
    };
  }

  return {
    ok: false,
    mode: "unauthorized",
    refused: true,
    note: "Research authorization failed closed.",
  };
}

/**
 * Experiment budget ledger with atomic reservations (supports $0 unit cost).
 */
export function createExperimentBudgetLedger({
  hardCeilingUsd = 0,
  maxPerCaseUsd = MAX_PER_CASE_USD,
  outPath = null,
} = {}) {
  return {
    version: "gdi-20opp-experiment-budget-ledger-v1",
    hardCeilingUsd: Number(hardCeilingUsd),
    maxPerCaseUsd: Number(maxPerCaseUsd),
    spentUsd: 0,
    reservedUsd: 0,
    perCaseSpent: {},
    operations: [],
    blocked: [],
    outPath,
    createdAt: new Date().toISOString(),
  };
}

export function remainingExperimentBudget(ledger) {
  return Math.max(
    0,
    Number(ledger.hardCeilingUsd || 0) -
      Number(ledger.spentUsd || 0) -
      Number(ledger.reservedUsd || 0),
  );
}

export function reserveCaseBudget(ledger, { opportunityId, quotedMaxUsd }) {
  const quoted = Number(quotedMaxUsd || 0);
  const caseSpent = Number(ledger.perCaseSpent[opportunityId] || 0);
  if (quoted < 0) {
    return { ok: false, reason: "NEGATIVE_QUOTE" };
  }
  if (caseSpent + quoted > Number(ledger.maxPerCaseUsd) + 1e-9) {
    ledger.blocked.push({
      at: new Date().toISOString(),
      opportunityId,
      reason: "PER_CASE_CEILING",
      quoted,
      caseSpent,
    });
    return { ok: false, reason: "PER_CASE_CEILING", caseSpent };
  }
  if (quoted > remainingExperimentBudget(ledger) + 1e-9) {
    ledger.blocked.push({
      at: new Date().toISOString(),
      opportunityId,
      reason: "TOTAL_CEILING",
      quoted,
      remaining: remainingExperimentBudget(ledger),
    });
    return { ok: false, reason: "TOTAL_CEILING" };
  }
  ledger.reservedUsd = Number((ledger.reservedUsd + quoted).toFixed(6));
  return { ok: true, quoted, remaining: remainingExperimentBudget(ledger) };
}

export function settleCaseBudget(ledger, { opportunityId, quotedMaxUsd, actualCostUsd = 0, meta = {} }) {
  const quoted = Number(quotedMaxUsd || 0);
  const actual = Number(actualCostUsd || 0);
  ledger.reservedUsd = Number(Math.max(0, ledger.reservedUsd - quoted).toFixed(6));
  ledger.spentUsd = Number((ledger.spentUsd + actual).toFixed(6));
  ledger.perCaseSpent[opportunityId] = Number(
    ((ledger.perCaseSpent[opportunityId] || 0) + actual).toFixed(6),
  );
  ledger.operations.push({
    at: new Date().toISOString(),
    opportunityId,
    quotedMaxUsd: quoted,
    actualCostUsd: actual,
    ...meta,
  });
  if (ledger.outPath) {
    fs.mkdirSync(path.dirname(ledger.outPath), { recursive: true });
    fs.writeFileSync(ledger.outPath, JSON.stringify(ledger, null, 2));
  }
  return ledger;
}

/**
 * Independent challenger — separates recovered vs new vs inferred vs speculation.
 * Does not award points for narrative length.
 */
export function runIndependentEvidenceChallenger({
  baselineOpportunity,
  workingOpportunity,
  recovery,
  retrievalRecords = [],
} = {}) {
  const base = runEvidenceChallenger(baselineOpportunity || {});
  const after = runEvidenceChallenger(workingOpportunity || {});

  const recoveredEvidence = [];
  if (recovery?.overlaysApplied?.demand) {
    recoveredEvidence.push({
      kind: "existing_recovered",
      field: "demand_thesis_overlay",
      category: recovery.category || null,
    });
  }
  if (recovery?.overlaysApplied?.buyer) {
    recoveredEvidence.push({
      kind: "existing_recovered",
      field: "buyer_responsibility_overlay",
      category: recovery.category || null,
    });
  }

  const newVerified = [];
  for (const rec of retrievalRecords) {
    if (rec.informationFound === true && rec.mocked !== true && Number(rec.retrievals || 0) > 0) {
      newVerified.push({
        kind: "genuinely_new_verified",
        strategyId: rec.strategyId || null,
        urls: rec.urls || [],
        provenance: rec.provenance || null,
      });
    } else if (rec.mocked === true && rec.informationFound === true) {
      newVerified.push({
        kind: "mock_simulated_finding",
        strategyId: rec.strategyId || null,
        note: "Mock provider only — not counted as live verified evidence",
      });
    }
  }

  const inferred = after.reliablyInferredDemand && !after.confirmedDemand;
  const speculation = after.speculative === true;

  return {
    schema: "gdi-independent-evidence-challenger-v1",
    baseline: base,
    after,
    buckets: {
      existingRecoveredEvidence: recoveredEvidence,
      genuinelyNewVerifiedEvidence: newVerified.filter((x) => x.kind === "genuinely_new_verified"),
      mockSimulatedFindings: newVerified.filter((x) => x.kind === "mock_simulated_finding"),
      plausibleInferredDemand: inferred,
      unsupportedSpeculation: speculation,
    },
    contact: after.contactRelevance,
    salesReadyBlockedByChallenger: after.salesReadyBlockedByChallenger,
    materialClaimCount:
      recoveredEvidence.length +
      newVerified.filter((x) => x.kind === "genuinely_new_verified").length,
    scoringNote:
      "Do not award commercial improvement for longer narratives or mock-only steps.",
  };
}

/**
 * Bias-aware rubric for Phase 2 — recovery-only overlays do NOT count as new verified findings.
 */
export function scorePhase2Arm({
  shared,
  challenger,
  costs,
  genuinelyNewVerifiedCount = 0,
  recoveryApplied = false,
} = {}) {
  const status = shared?.status || null;
  const salesReady = shared?.salesReady === true && !challenger?.salesReadyBlockedByChallenger;
  const scores = {
    newVerifiedFactualFindings: Math.min(5, genuinelyNewVerifiedCount * 2),
    contactsNewlyVerified: Math.min(
      5,
      (challenger?.contact?.verifiedNamedCount ||
        challenger?.contactRelevance?.verifiedNamedCount ||
        0) * 2,
    ),
    eligibilityCorrectness: status === "SALES_READY" ? (salesReady ? 4 : 1) : status === "RESEARCH_CONTINUATION" ? 3 : 4,
    commercialActionability: salesReady ? 4 : 2,
    demandThesisQuality: challenger?.after?.speculative
      ? 1
      : challenger?.after?.confirmedDemand
        ? 4
        : challenger?.after?.reliablyInferredDemand
          ? 3
          : 2,
    hotelRelevance: 3,
    crossHotelReuse: 3,
    researchCostEfficiency: Number(costs?.actualPaidUsd || 0) === 0 ? 5 : 2,
    falsePositiveRisk: challenger?.salesReadyBlockedByChallenger ? 4 : salesReady ? 3 : 3,
    // Explicitly non-scoring diagnostic
    recoveryOverlayAppliedDiagnostic: recoveryApplied ? 1 : 0,
  };
  // recoveryOverlayAppliedDiagnostic excluded from total
  const { recoveryOverlayAppliedDiagnostic, ...scored } = scores;
  const total = Object.values(scored).reduce((a, b) => a + b, 0);
  return {
    scores: scored,
    diagnostics: { recoveryOverlayAppliedDiagnostic },
    total,
    maxTotal: Object.keys(scored).length * 5,
  };
}

function mockResearchStep(opportunity, selection) {
  const next = selectAdaptiveNextAction(opportunity, {
    useBuyerDiscoveryStrategies: true,
    hotelCommercialAlignment: true,
  });
  const preflight = buildAdaptiveResearchPreflight(opportunity, { skipRecovery: true });
  return {
    mocked: true,
    continue: Boolean(next.continue),
    decision: next.decision || null,
    strategyId: next.strategy?.id || null,
    reason: next.reason || null,
    gapType: next.gapType || null,
    preflightGaps: preflight?.sharedGaps || [],
    informationFound: false,
    retrievals: 0,
    urls: [],
    provenance: {
      provider: "mock",
      slot: selection.slot,
      note: "Planned strategy only — no network retrieval",
    },
    plannedResearchCalls: next.strategy
      ? [
          {
            provider: "official_page",
            strategyId: next.strategy.id,
            estimatedCostUsd: 0,
            maxChargeUsd: 0,
            status: "planned_not_executed",
          },
        ]
      : [],
  };
}

export async function runAgenticPrepCase(selection, opts = {}) {
  const t0 = Date.now();
  const auth = assertResearchAuthorization(opts);
  const frozen = loadFrozenCase(selection.opportunityId);
  const heldOut = loadHeldOutLabels(selection.opportunityId);
  const baselineOpp = frozen.frozenOpportunity;
  const baseline = runBaselineCase(baselineOpp, {
    opportunityId: selection.opportunityId,
  });

  const ledger =
    opts.ledger ||
    createExperimentBudgetLedger({
      hardCeilingUsd: auth.hardCeilingUsd || 0,
      maxPerCaseUsd: MAX_PER_CASE_USD,
    });

  const recovery = recoverCommercialEvidence(baselineOpp);
  let working = recovery.recoveredOpportunity || JSON.parse(JSON.stringify(baselineOpp));
  const retrievalRecords = [];
  let actualPaidUsd = 0;
  let liveCycle = null;

  const quote = auth.mode === "live_public_zero_cost" ? 0 : 0;
  const reserve = reserveCaseBudget(ledger, {
    opportunityId: selection.opportunityId,
    quotedMaxUsd: quote,
  });

  if (!reserve.ok && quote > 0) {
    return {
      slot: selection.slot,
      opportunityId: selection.opportunityId,
      refused: true,
      reason: reserve.reason,
      auth,
      costs: { actualPaidUsd: 0, estimatedUsd: 0 },
    };
  }

  if (auth.mode === "mock" || auth.refused) {
    const step = mockResearchStep(working, selection);
    retrievalRecords.push(step);
  } else if (auth.mode === "live_public_zero_cost") {
    // Live $0 official_page path — still no metered providers.
    liveCycle = await runBoundedPublicResearchCycle(working, {
      limits: {
        ...PHASE2G_PILOT_LIMITS,
        maxActionsPerOpportunity: 2,
        maxUrlsPerAction: 2,
        maxExternalRetrievalsPilot: 4,
      },
      maxActionsThisCycle: 2,
    });
    working = liveCycle.workingOpportunity || working;
    for (const a of liveCycle.actions || []) {
      retrievalRecords.push({
        mocked: false,
        strategyId: a.strategyId,
        informationFound: a.informationFound === true,
        retrievals: a.retrievals || 0,
        urls: a.urlSelection?.selected?.map((s) => s.url) || [],
        provenance: { provider: "official_page", phase: "2G" },
      });
    }
    actualPaidUsd = 0;
  }

  settleCaseBudget(ledger, {
    opportunityId: selection.opportunityId,
    quotedMaxUsd: quote,
    actualCostUsd: actualPaidUsd,
    meta: { mode: auth.mode, slot: selection.slot },
  });

  const independent = runIndependentEvidenceChallenger({
    baselineOpportunity: baselineOpp,
    workingOpportunity: working,
    recovery,
    retrievalRecords,
  });

  let shared = evaluateSharedCommercialEligibility(working);
  if (independent.salesReadyBlockedByChallenger && shared.salesReady === true) {
    shared = {
      ...shared,
      status: "RESEARCH_CONTINUATION",
      salesReady: false,
      commercialEligible: false,
      challengerOverride: "blocked_unsupported_or_unverified_buyer",
      gaps: [...(shared.gaps || []), "CHALLENGER_BLOCKED_SALES_READY"],
    };
  }

  const supervisor = evaluateOpportunity(working, { requireFutureDemand: false });
  const costs = {
    actualPaidUsd,
    estimatedUsd: 0,
    mode: auth.mode,
    providerCalls: retrievalRecords,
  };

  const rubric = scorePhase2Arm({
    shared,
    challenger: independent,
    costs,
    genuinelyNewVerifiedCount:
      independent.buckets.genuinelyNewVerifiedEvidence.length,
    recoveryApplied:
      recovery?.overlaysApplied?.demand === true ||
      recovery?.overlaysApplied?.buyer === true,
  });

  const baselineRubric = scorePhase2Arm({
    shared: baseline.sharedEligibility,
    challenger: { ...baseline.challenger, after: baseline.challenger, contact: baseline.challenger.contactRelevance },
    costs: baseline.costs,
    genuinelyNewVerifiedCount: 0,
    recoveryApplied: false,
  });

  return {
    slot: selection.slot,
    opportunityId: selection.opportunityId,
    organization: text(
      working.organizationName || working.companyName || baselineOpp.organizationName,
    ),
    hotelId: selection.hotelId,
    hotelName: selection.hotelName,
    selection,
    heldOutLabelsExcludedFromInputs: heldOut,
    snapshotSha256: frozen.snapshotSha256,
    auth,
    durationMs: Date.now() - t0,
    baseline: {
      sharedEligibility: baseline.sharedEligibility,
      challenger: baseline.challenger,
      rubric: baselineRubric,
      costs: baseline.costs,
      supervisor: baseline.supervisor,
    },
    agentic: {
      recovery: {
        applied:
          recovery?.overlaysApplied?.demand === true ||
          recovery?.overlaysApplied?.buyer === true,
        category: recovery?.category || null,
        evidenceBackedNewSalesReady: recovery?.evidenceBackedNewSalesReady === true,
        beforeStatus: recovery?.before?.status || null,
        afterStatus: recovery?.after?.status || null,
      },
      retrievalRecords,
      liveCycleSummary: liveCycle
        ? {
            retrievalsUsed: liveCycle.retrievalsUsed || 0,
            stopLog: liveCycle.stopLog || [],
            actionCount: (liveCycle.actions || []).length,
          }
        : null,
      independentChallenger: independent,
      sharedEligibility: {
        status: shared.status,
        salesReady: shared.salesReady === true,
        gaps: shared.gaps || [],
        challengerOverride: shared.challengerOverride || null,
      },
      supervisor: {
        blockers: (supervisor.blockers || []).map((b) => b.key || b),
        publishable: supervisor.publishable === true,
        hardStopReason: supervisor.hardStop?.reason || null,
      },
      rubric,
      costs,
      redacted: redactOpportunityForShadowExport(working),
    },
    comparison: {
      baselineStatus: baseline.sharedEligibility.status,
      agenticStatus: shared.status,
      deltaTotal: rubric.total - baselineRubric.total,
      genuinelyNewVerifiedCount:
        independent.buckets.genuinelyNewVerifiedEvidence.length,
      recoveryOnly: independent.buckets.genuinelyNewVerifiedEvidence.length === 0,
      actualPaidUsd,
    },
  };
}

export async function runFiveCasePrepExperiment(opts = {}) {
  const auth = assertResearchAuthorization(opts);
  const outDir =
    opts.outDir ||
    path.join(ROOT, "reports", "gdi-20opp-live-research-prep-v1");
  fs.mkdirSync(outDir, { recursive: true });

  const ledger = createExperimentBudgetLedger({
    hardCeilingUsd: auth.hardCeilingUsd || 0,
    maxPerCaseUsd: MAX_PER_CASE_USD,
    outPath: path.join(outDir, "BUDGET_LEDGER.json"),
  });

  const caseResults = [];
  for (const selection of FIVE_CASE_SELECTION) {
    const result = await runAgenticPrepCase(selection, {
      ...opts,
      ledger,
      mode: auth.mode === "live_public_zero_cost" ? "live_public" : "mock",
      authorizeLivePublicResearch: auth.authorizeLivePublicResearch,
      authorizePaidResearch: false,
    });
    caseResults.push(result);
  }

  const totals = {
    cases: caseResults.length,
    actualPaidUsdTotal: caseResults.reduce(
      (s, c) => s + Number(c.comparison?.actualPaidUsd || 0),
      0,
    ),
    baselineAvg: average(caseResults.map((c) => c.baseline.rubric.total)),
    agenticAvg: average(caseResults.map((c) => c.agentic.rubric.total)),
    genuinelyNewVerifiedTotal: caseResults.reduce(
      (s, c) => s + (c.comparison?.genuinelyNewVerifiedCount || 0),
      0,
    ),
    recoveryOnlyCases: caseResults.filter((c) => c.comparison?.recoveryOnly).length,
  };

  const plannedLiveCalls = caseResults.map((c) => ({
    opportunityId: c.opportunityId,
    slot: c.slot,
    planned:
      c.agentic.retrievalRecords.flatMap((r) => r.plannedResearchCalls || []) ||
      [],
    maxPerCaseUsd: MAX_PER_CASE_USD,
  }));

  const report = {
    schema: PREP_EXPERIMENT_ID,
    version: PREP_VERSION,
    executedAt: new Date().toISOString(),
    parentExperiment: "gdi_20opp_adaptive_investigation_experiment_v1",
    parentPr: 411,
    auth,
    budget: {
      maxPerCaseUsd: MAX_PER_CASE_USD,
      maxTotalUsd: MAX_TOTAL_USD,
      hardCeilingUsdApplied: auth.hardCeilingUsd || 0,
      actualPaidUsdTotal: totals.actualPaidUsdTotal,
      ledgerPath: path.relative(ROOT, ledger.outPath).replace(/\\/g, "/"),
    },
    scoringBiasNote: loadScoringBiasAudit(),
    eligibilityFlipNote: loadEligibilityFlipAudit(),
    fiveCases: FIVE_CASE_SELECTION,
    plannedLiveCalls,
    totals,
    caseResults,
    gatedPaidExecution: {
      status: "NOT_EXECUTED",
      requiresFounderAuth: true,
      envVars: [LIVE_PUBLIC_AUTH_ENV, LIVE_PAID_AUTH_ENV],
      command: buildAuthorizedExecutionCommand(),
      successCriteria: SUCCESS_CRITERIA,
    },
    safety: {
      productionWrites: false,
      airtableWrites: false,
      customerPublication: false,
      unauthorizedContactEnrichment: false,
    },
  };

  fs.writeFileSync(path.join(outDir, "RESULTS.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(outDir, "FIVE_CASE_MANIFEST.json"),
    JSON.stringify(
      {
        schema: PREP_EXPERIMENT_ID,
        version: PREP_VERSION,
        cases: FIVE_CASE_SELECTION,
        frozenSource: path.relative(ROOT, FROZEN_PATH).replace(/\\/g, "/"),
        heldOutPolicy: "evaluationLabelsHeldOut never passed into investigator inputs",
      },
      null,
      2,
    ),
  );
  fs.writeFileSync(path.join(outDir, "BUDGET_LEDGER.json"), JSON.stringify(ledger, null, 2));
  fs.writeFileSync(path.join(outDir, "PREP_REPORT.md"), renderPrepMarkdown(report));
  fs.writeFileSync(
    path.join(outDir, "AUTHORIZATION_GATE.md"),
    renderAuthGateMarkdown(report),
  );

  const fixtureDir = path.join(
    ROOT,
    "fixtures/gdi-experiments/gdi-20opp-live-research-prep-v1",
  );
  fs.mkdirSync(fixtureDir, { recursive: true });
  fs.writeFileSync(
    path.join(fixtureDir, "five-case-manifest.json"),
    JSON.stringify(
      {
        schema: PREP_EXPERIMENT_ID,
        version: PREP_VERSION,
        cases: FIVE_CASE_SELECTION,
      },
      null,
      2,
    ),
  );

  return { report, outDir, fixtureDir, ledger };
}

function average(nums) {
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

export function loadScoringBiasAudit() {
  if (!fs.existsSync(PHASE1_RESULTS)) {
    return { available: false };
  }
  const r = JSON.parse(fs.readFileSync(PHASE1_RESULTS, "utf8"));
  return {
    available: true,
    finding:
      "Phase 1 20/20 positive deltas were structural scoring bias, not live quality lift.",
    evidence: {
      improvedCount: r.totals?.improvedCount,
      worsenedCount: r.totals?.worsenedCount,
      avgDelta: r.decision?.avgDelta,
      eligibilityFlipCount: r.decision?.eligibilityFlipCount,
      contactLiftCount: r.decision?.contactLiftCount,
      dominantLiftDimensions: [
        "meaningfulNewVerifiedFindings (+3.0 avg) awarded whenever recovery.applied — including A_ALREADY_SUPPORTED overlays",
        "thesisCompleteness (+1.5 avg) from local remapping, not new web evidence",
      ],
      recommendation:
        "Phase 2 rubric awards newVerifiedFactualFindings only for genuinely new verified retrievals; recovery overlays are diagnostic only.",
    },
  };
}

export function loadEligibilityFlipAudit() {
  if (!fs.existsSync(PHASE1_RESULTS)) return { available: false };
  const r = JSON.parse(fs.readFileSync(PHASE1_RESULTS, "utf8"));
  const flip = (r.caseResults || []).find(
    (c) => c.comparison?.baselineStatus !== c.comparison?.agenticStatus,
  );
  if (!flip) return { available: true, flipCount: 0 };
  return {
    available: true,
    flipCount: 1,
    opportunityId: flip.opportunityId,
    organization: flip.organization,
    hotelName: flip.hotelName,
    baselineStatus: flip.comparison.baselineStatus,
    agenticStatus: flip.comparison.agenticStatus,
    challengerOverride: flip.agentic?.sharedEligibility?.challengerOverride || null,
    explanation:
      "ISCT (rb_gdi_disc_dc428c1bd4c4e412) was SALES_READY under shared eligibility but the evidence challenger blocked sales-ready because verifiedNamedCount=0 (no verified named buyer). Agentic arm correctly demoted to RESEARCH_CONTINUATION — a quality correction, not an eligibility upgrade.",
  };
}

export const SUCCESS_CRITERIA = Object.freeze([
  "Actual paid spend ≤ authorized ceiling (default $0 until founder auth)",
  "≥1 genuinely new verified factual finding OR correct sustained rejection on rejected slot",
  "No false SALES_READY without challenger-confirmed demand + verified buyer/entry",
  "Cross-hotel/contracted case not promoted as exclusive hotel demand",
  "Per-case spend ≤ $4 and total ≤ $20 when paid auth is eventually granted",
  "Idempotent job: re-run does not double-spend reservations",
  "No Airtable / production / customer publication writes",
]);

export function buildAuthorizedExecutionCommand() {
  return [
    `# Step A — $0 live official_page only (still denies Exa/SerpAPI/PDL/Surfe):`,
    `set ${LIVE_PUBLIC_AUTH_ENV}=1`,
    `npm run gdi:20opp-live-research-prep-v1 -- --mode live_public`,
    ``,
    `# Step B — PAID ≤$20 total / ≤$4 per case — REQUIRES SEPARATE FOUNDER AUTH (not enabled by this prep PR):`,
    `# set ${LIVE_PAID_AUTH_ENV}=<founder-authorization-id>`,
    `# npm run gdi:20opp-live-research-prep-v1 -- --mode paid --authorize-paid-research`,
    `# (Currently hard-refuses paid execution even if env is set.)`,
  ].join("\n");
}

function renderPrepMarkdown(report) {
  return `# GDI 20-Opp Live Research Prep V1

**Version:** ${report.version}  
**Mode:** ${report.auth.mode}  
**Actual paid spend:** $${report.totals.actualPaidUsdTotal}

## Parent close-out
- PR #411 merged (parent experiment)
- Scoring bias: ${report.scoringBiasNote.finding || "n/a"}
- Eligibility flip: ${report.eligibilityFlipNote.explanation || "n/a"}

## Five cases
${report.fiveCases.map((c) => `- **${c.slot}** \`${c.opportunityId}\` @ ${c.hotelName} — ${c.reason}`).join("\n")}

## Totals
| Metric | Baseline | Agentic | Diff |
|--------|----------|---------|------|
| Avg Phase-2 rubric | ${report.totals.baselineAvg.toFixed(2)} | ${report.totals.agenticAvg.toFixed(2)} | ${(report.totals.agenticAvg - report.totals.baselineAvg).toFixed(2)} |
| Genuinely new verified | — | ${report.totals.genuinelyNewVerifiedTotal} | — |
| Recovery-only cases | — | ${report.totals.recoveryOnlyCases} | — |
| Actual paid USD | 0 | ${report.totals.actualPaidUsdTotal} | 0 |

## Gated paid execution
**${report.gatedPaidExecution.status}**

\`\`\`
${report.gatedPaidExecution.command}
\`\`\`

## Success criteria
${report.gatedPaidExecution.successCriteria.map((s) => `- ${s}`).join("\n")}
`;
}

function renderAuthGateMarkdown(report) {
  return `# Authorization gate — ${PREP_EXPERIMENT_ID}

## Current status
- Prep harness mode: \`${report.auth.mode}\`
- Live public auth env: \`${LIVE_PUBLIC_AUTH_ENV}\`
- Paid budget auth env: \`${LIVE_PAID_AUTH_ENV}\`
- Paid execution: **blocked** in this prep (even if paid env present)
- Actual paid spend this run: **$${report.totals.actualPaidUsdTotal}**

## Exact authorized commands
\`\`\`bat
${report.gatedPaidExecution.command}
\`\`\`

## Allowlists
- Always: ${PROVIDER_ALLOWLIST.always.join(", ")}
- Live public $0: ${PROVIDER_ALLOWLIST.livePublicZeroCost.join(", ")}
- Paid (separate auth, currently refused): ${PROVIDER_ALLOWLIST.paidRequiresSeparateAuth.join(", ")}
`;
}

export function planHash() {
  return sha256(JSON.stringify(FIVE_CASE_SELECTION));
}
