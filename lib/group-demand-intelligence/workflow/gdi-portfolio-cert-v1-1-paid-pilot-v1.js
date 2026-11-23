/**
 * GDI Portfolio Certification V1.1 — controlled paid pilot controls.
 *
 * Dual spend caps (fail-closed):
 *   - Per hotel: $12.50 durable ledger (authorization-scoped)
 *   - Aggregate: $25 via sum of authorized hotel caps (2 hotels × $12.50)
 *
 * Reuses canonical Full E2E + gdi-authorized-paid-budget-ledger-v1.
 * No Surfe/Findymail/PDL/FullEnrich. No Airtable apply. No publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import {
  loadOrCreatePaidBudgetLedger,
  remainingBudgetUsd,
  canAffordQuotedCall,
  GDI_AUTHORIZED_PAID_BUDGET_LEDGER_V1,
} from "../research/gdi-authorized-paid-budget-ledger-v1.js";
import {
  runCanonicalFullE2eResearch,
  preflightCanonicalFullE2e,
  assessCanonicalLiveReadiness,
  CANONICAL_EXECUTION_MODE,
  GDI_CANONICAL_FULL_E2E_ENTRY_V1,
} from "./gdi-canonical-full-e2e-entry-v1.js";
import {
  resolveHotelIdentity,
  preflightHotelEligibility,
  assessHotelCommercialQuality,
} from "./gdi-portfolio-processing-certification-v1.js";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import { evaluateResearchPersistenceAuthorization } from "./gdi-full-e2e-research-persistence-bridge-v1.js";
import { buildDurableWeeklyChangeAnnotation } from "./gdi-weekly-change-durability-v1.js";

export const GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT =
  "gdi-portfolio-cert-v1-1-paid-pilot";

export const PILOT_AGGREGATE_CEILING_USD = 25;
export const PILOT_PER_HOTEL_CEILING_USD = 12.5;

export const PILOT_HOTELS = Object.freeze([
  {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    authorizationId: "FOUNDER_GDI_PORTFOLIO_CERT_V1_1_BETHESDA_12_50",
    perHotelCeilingUsd: PILOT_PER_HOTEL_CEILING_USD,
    // Commercially prioritized subset — not all 14 paid.
    prioritySegments: [
      "associations_conferences_events",
      "corporate_accounts_scaling",
      "government_public_sector",
      "education_scientific",
      "sports_entertainment",
      "corporate_meetings_incentives",
    ],
  },
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelName: "YOTEL Geneva Lake",
    authorizationId: "FOUNDER_GDI_PORTFOLIO_CERT_V1_1_YOTEL_12_50",
    perHotelCeilingUsd: PILOT_PER_HOTEL_CEILING_USD,
    prioritySegments: [
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "preferred_lodging_partnerships",
      "travel_trade_group_intermediaries",
      "education_scientific",
      "professional_event_planners",
    ],
  },
]);

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeJson(filePath, body) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(body, null, 2)}\n`, "utf8");
  return filePath;
}

function readJsonSafe(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Verify dual-cap arithmetic and durable ledger init without spending.
 */
export function verifyDualSpendCaps({
  hotels = PILOT_HOTELS,
  aggregateCeilingUsd = PILOT_AGGREGATE_CEILING_USD,
  effort = EXA_AGENT_EFFORT.MEDIUM,
  root = ROOT,
} = {}) {
  const sumCaps = hotels.reduce(
    (s, h) => s + Number(h.perHotelCeilingUsd || 0),
    0,
  );
  const errors = [];
  if (sumCaps > aggregateCeilingUsd + 1e-9) {
    errors.push({
      code: "PER_HOTEL_CAPS_EXCEED_AGGREGATE",
      sumCaps,
      aggregateCeilingUsd,
    });
  }
  // Full pilot cohort is exactly two hotels; single-hotel verify is allowed
  // when executeHotelPaidPilot checks one ledger in isolation.
  if (hotels === PILOT_HOTELS && hotels.length !== 2) {
    errors.push({ code: "PILOT_REQUIRES_EXACTLY_TWO_HOTELS" });
  }
  const quote = quoteExaAgentRun({ effort });
  if (quote.bounded !== true || !(Number(quote.maxUsd) > 0)) {
    errors.push({
      code: "PROVIDER_QUOTE_UNBOUNDED",
      quote,
    });
  }
  const hotelLedgers = [];
  for (const h of hotels) {
    const loaded = loadOrCreatePaidBudgetLedger({
      authorizationId: h.authorizationId,
      hardCeilingUsd: h.perHotelCeilingUsd,
      effort,
      root,
      resume: true,
      allowCeilingRaise: false,
    });
    if (!loaded.ok) {
      errors.push({
        code: "LEDGER_INIT_FAILED",
        hotelId: h.hotelId,
        reason: loaded.reason,
      });
      continue;
    }
    const remaining = remainingBudgetUsd(loaded.ledger);
    const afford = canAffordQuotedCall(loaded.ledger, quote.maxUsd);
    hotelLedgers.push({
      hotelId: h.hotelId,
      authorizationId: h.authorizationId,
      durablePath: loaded.durablePath,
      hardCeilingUsd: loaded.ledger.hardCeilingUsd,
      spentUsd: loaded.ledger.spentUsd,
      remainingUsd: remaining,
      canAffordOneMediumCall: afford.ok === true,
      affordReason: afford.ok ? null : afford.reason,
      ledgerVersion: GDI_AUTHORIZED_PAID_BUDGET_LEDGER_V1,
    });
    if (Number(loaded.ledger.hardCeilingUsd) > h.perHotelCeilingUsd + 1e-9) {
      errors.push({
        code: "LEDGER_CEILING_ABOVE_AUTHORIZED",
        hotelId: h.hotelId,
      });
    }
  }
  const aggregateSpent = hotelLedgers.reduce(
    (s, h) => s + Number(h.spentUsd || 0),
    0,
  );
  const aggregateRemaining = Number(
    (aggregateCeilingUsd - aggregateSpent).toFixed(6),
  );
  return {
    ok: errors.length === 0,
    contract: GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT,
    aggregateCeilingUsd,
    sumPerHotelCaps: sumCaps,
    aggregateSpentUsd: aggregateSpent,
    aggregateRemainingUsd: aggregateRemaining,
    quote,
    hotelLedgers,
    errors,
    concurrency: "SEQUENTIAL_ONLY",
    retrySafe:
      "Durable per-hotel ledgers resume spentUsd; retries cannot reset ceiling",
    publicationAuthorized: false,
    airtableApplyAuthorized: false,
    surfeAuthorized: false,
  };
}

/**
 * Zero-cost provider-call plan for founder verification before spend.
 */
export function buildProviderCallPlan({
  hotels = PILOT_HOTELS,
  effort = EXA_AGENT_EFFORT.MEDIUM,
  maxCandidatesPerHotel = 4,
  root = ROOT,
} = {}) {
  const caps = verifyDualSpendCaps({ hotels, effort, root });
  const allSegments = listFourteenWorkflowSegmentIds();
  const hotelPlans = hotels.map((h) => {
    const identity = resolveHotelIdentity(h.hotelId, { root });
    const eligibility = preflightHotelEligibility(h.hotelId, { root });
    const quality = assessHotelCommercialQuality(h.hotelId, { root });
    const ledger = caps.hotelLedgers.find((x) => x.hotelId === h.hotelId);
    const remaining = Number(ledger?.remainingUsd || 0);
    const unit = Number(caps.quote?.maxUsd || 0);
    const maxAffordableCalls =
      unit > 0 ? Math.floor(remaining / unit + 1e-9) : 0;
    const skippedSegments = allSegments.filter(
      (s) => !(h.prioritySegments || []).includes(s),
    );
    return {
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      identity,
      eligibilityOk: eligibility.eligible,
      baselineOpportunityCount: quality.opportunityCount || 0,
      baselineOpportunityIds: identity.opportunityIds || [],
      prioritySegments: h.prioritySegments,
      skippedSegmentsCostOrRelevance: skippedSegments,
      skippedReason: "NOT_IN_PILOT_PRIORITY_SET_COST_RELEVANCE",
      authorizationId: h.authorizationId,
      perHotelCeilingUsd: h.perHotelCeilingUsd,
      remainingUsd: remaining,
      quoteUnitMaxUsd: unit,
      maxAffordableMediumCalls: maxAffordableCalls,
      maxCandidatesPerHotel,
      providers: [
        {
          provider: "exa_agent",
          operations: ["discovery", "deep_research"],
          effort,
          quotedMaxUsdPerCall: unit,
          bounded: caps.quote?.bounded === true,
          authorized: true,
        },
        {
          provider: "surfe",
          authorized: false,
          note: "HOLD — founder excluded paid Surfe for this pilot",
        },
        {
          provider: "findymail",
          authorized: false,
        },
        {
          provider: "pdl",
          authorized: false,
        },
        {
          provider: "fullenrich",
          authorized: false,
        },
      ],
      expectedSpendBandUsd: {
        min: 0,
        max: Math.min(h.perHotelCeilingUsd, remaining),
        note: "Actual spend depends on retained candidates and early stop; do not force budget consumption",
      },
    };
  });

  const exaKeyPresent = Boolean(
    process.env.EXA_API_KEY || process.env.EXA_KEY,
  );
  const liveEnv = String(process.env.GDI_EXA_AGENT_LIVE || "") === "1";

  const planBlockers = [...(caps.errors || [])];
  const executeBlockers = [];
  if (!exaKeyPresent) {
    planBlockers.push({ code: "EXA_API_KEY_ABSENT" });
    executeBlockers.push({ code: "EXA_API_KEY_ABSENT" });
  }
  if (!liveEnv) {
    executeBlockers.push({
      code: "GDI_EXA_AGENT_LIVE_ENV_OFF",
      note: "Set GDI_EXA_AGENT_LIVE=1 only for the paid execution window",
    });
  }
  for (const p of hotelPlans) {
    if (!p.eligibilityOk) {
      planBlockers.push({ code: "HOTEL_INELIGIBLE", hotelId: p.hotelId });
    }
    if (p.maxAffordableMediumCalls < 1) {
      planBlockers.push({
        code: "INSUFFICIENT_REMAINING_FOR_ONE_QUOTED_CALL",
        hotelId: p.hotelId,
        remainingUsd: p.remainingUsd,
      });
    }
  }

  const planOk = planBlockers.length === 0 && caps.ok;
  const executeReady = planOk && executeBlockers.length === 0;

  return {
    ok: planOk,
    executeReady,
    contract: GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT,
    generatedAt: nowIso(),
    canonicalEngine: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
    caps,
    hotelPlans,
    fourteenSegmentIds: allSegments,
    safety: {
      aggregateCeilingUsd: PILOT_AGGREGATE_CEILING_USD,
      perHotelCeilingUsd: PILOT_PER_HOTEL_CEILING_USD,
      sequentialOnly: true,
      publication: false,
      airtableApply: false,
      surfe: false,
      weeklyAutomation: false,
      forceSpendForbidden: true,
    },
    env: {
      exaKeyPresent,
      gdiExaAgentLive: liveEnv,
    },
    blockers: planBlockers,
    executeBlockers,
    spendAuthorized: false,
    note: "PREFLIGHT ONLY — no provider calls executed",
  };
}

/**
 * Execute controlled paid pilot for one hotel through canonical engine.
 */
export async function executeHotelPaidPilot(hotelSpec, opts = {}) {
  const effort = opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM;
  const reportRoot =
    opts.reportRoot ||
    path.join(ROOT, "reports", "gdi-portfolio-cert-v1-1-paid-pilot");
  const hotelOut = path.join(
    reportRoot,
    "hotels",
    hotelSpec.hotelId,
    opts.hotelRunId || `paid_${Date.now().toString(16)}`,
  );
  ensureDir(hotelOut);

  const identityBefore = resolveHotelIdentity(hotelSpec.hotelId);
  const caps = verifyDualSpendCaps({
    hotels: [hotelSpec],
    aggregateCeilingUsd: hotelSpec.perHotelCeilingUsd,
    effort,
  });
  if (!caps.ok) {
    return {
      ok: false,
      blocked: true,
      hotelId: hotelSpec.hotelId,
      error: "SPEND_CAP_PREFLIGHT_FAILED",
      caps,
      spentUsd: 0,
    };
  }

  if (!isExaAgentLiveAuthorized({ authorizeExaAgentLive: true })) {
    return {
      ok: false,
      blocked: true,
      hotelId: hotelSpec.hotelId,
      error: "EXA_LIVE_NOT_AUTHORIZED",
      hint: "Require authorizeExaAgentLive + GDI_EXA_AGENT_LIVE=1",
      spentUsd: 0,
    };
  }

  const remaining = caps.hotelLedgers[0]?.remainingUsd || 0;
  const hotelCeiling = Math.min(
    hotelSpec.perHotelCeilingUsd,
    remaining + Number(caps.hotelLedgers[0]?.spentUsd || 0),
  );
  // Pass remaining as effective ceiling for this run via ledger resume —
  // durable ledger already holds spent; hardCeiling stays at 12.50.
  const budgetCeilingUsd = hotelSpec.perHotelCeilingUsd;

  const liveReadiness = assessCanonicalLiveReadiness({
    hotelId: hotelSpec.hotelId,
    mode: CANONICAL_EXECUTION_MODE.LIVE,
    authorizePaidDiscovery: true,
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    budgetCeilingUsd,
    authorizationId: hotelSpec.authorizationId,
    exaEffort: effort,
  });
  if (!liveReadiness.liveReady) {
    writeJson(path.join(hotelOut, "live-readiness-blocked.json"), liveReadiness);
    return {
      ok: false,
      blocked: true,
      hotelId: hotelSpec.hotelId,
      error: "LIVE_READINESS_BLOCKED",
      liveReadiness,
      spentUsd: 0,
      reportDir: hotelOut,
    };
  }

  const result = await runCanonicalFullE2eResearch({
    hotelId: hotelSpec.hotelId,
    mode: CANONICAL_EXECUTION_MODE.LIVE,
    dryRun: false,
    authorizePaidDiscovery: true,
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    authorizePublish: false,
    budgetCeilingUsd,
    authorizationId: hotelSpec.authorizationId,
    exaEffort: effort,
    maxCandidates: opts.maxCandidates ?? 4,
    allowPartial: true,
    outDir: hotelOut,
    executionRunId: opts.hotelRunId,
    segmentIds: hotelSpec.prioritySegments,
    allowHistoricalReplay: true,
    planResearchPersistence: true,
    authorizeResearchPersistence: false,
    applyResearchPersistence: false,
    // Allow HPC enrichment when local config exists; legacy Design Hotels
    // fields are mapped in hotel-gdi-profile-resolution-v1.
    skipHpc: opts.skipHpc === true,
  });

  const identityAfter = resolveHotelIdentity(hotelSpec.hotelId);
  const ledgerAfter = loadOrCreatePaidBudgetLedger({
    authorizationId: hotelSpec.authorizationId,
    hardCeilingUsd: hotelSpec.perHotelCeilingUsd,
    effort,
    resume: true,
    allowCeilingRaise: false,
  });

  const spentUsd = Number(
    result?.spentUsd ??
      ledgerAfter.ledger?.spentUsd ??
      0,
  );

  const row = {
    ok: result?.ok !== false && !result?.blocked,
    hotelId: hotelSpec.hotelId,
    hotelName: hotelSpec.hotelName,
    authorizationId: hotelSpec.authorizationId,
    prioritySegments: hotelSpec.prioritySegments,
    spentUsd,
    remainingUsd: remainingBudgetUsd(ledgerAfter.ledger),
    durableLedgerPath: ledgerAfter.durablePath,
    identityBefore: {
      opportunityCount: identityBefore.opportunityIds?.length || 0,
      opportunityIds: identityBefore.opportunityIds || [],
    },
    identityAfter: {
      opportunityCount: identityAfter.opportunityIds?.length || 0,
      opportunityIds: identityAfter.opportunityIds || [],
    },
    opportunityIdsUnchanged:
      JSON.stringify(identityBefore.opportunityIds || []) ===
      JSON.stringify(identityAfter.opportunityIds || []),
    canonical: {
      ok: result?.ok,
      blocked: result?.blocked,
      verdict: result?.verdict || result?.runVerdict,
      spentUsd: result?.spentUsd,
      budgetsRespected: result?.budgetsRespected,
      processedCount: Array.isArray(result?.processed)
        ? result.processed.length
        : result?.processedCount,
      discoveryCells: result?.discoveryCells,
    },
    publicationAuthorized: false,
    airtableApply: false,
    reportDir: hotelOut,
    finishedAt: nowIso(),
  };
  writeJson(path.join(hotelOut, "paid-pilot-result.json"), row);
  writeJson(path.join(hotelOut, "canonical-summary.json"), {
    ok: result?.ok,
    blocked: result?.blocked,
    error: result?.error,
    verdict: result?.verdict,
    spentUsd: result?.spentUsd,
    hardCeilingUsd: result?.hardCeilingUsd,
    budgetsRespected: result?.budgetsRespected,
  });
  return row;
}

/**
 * Sequential two-hotel paid pilot.
 */
export async function runPortfolioPaidPilot(opts = {}) {
  const reportRoot =
    opts.reportRoot ||
    path.join(ROOT, "reports", "gdi-portfolio-cert-v1-1-paid-pilot");
  const portfolioRunId =
    text(opts.portfolioRunId) ||
    `gdi_portfolio_cert_v1_1_${new Date()
      .toISOString()
      .replace(/[-:TZ.]/g, "")
      .slice(0, 14)}`;
  ensureDir(reportRoot);

  if (opts.executePaid !== true) {
    const plan = buildProviderCallPlan({
      hotels: PILOT_HOTELS,
      maxCandidatesPerHotel: opts.maxCandidates ?? 4,
    });
    writeJson(path.join(reportRoot, "PROVIDER_CALL_PLAN.json"), plan);
    return {
      ok: plan.ok,
      mode: "preflight",
      spentUsd: 0,
      plan,
      reportRoot,
      note: "Set executePaid:true only after founder preflight verification",
    };
  }

  // Hard gate: dual caps + env + plan must pass before first paid call
  const plan = buildProviderCallPlan({
    hotels: PILOT_HOTELS,
    maxCandidatesPerHotel: opts.maxCandidates ?? 4,
  });
  writeJson(path.join(reportRoot, "PROVIDER_CALL_PLAN.json"), plan);
  if (!plan.ok || plan.executeReady !== true) {
    return {
      ok: false,
      blocked: true,
      error: "PREFLIGHT_BLOCKED_BEFORE_SPEND",
      plan,
      spentUsd: 0,
      reportRoot,
    };
  }

  const hotels = [];
  let aggregateSpend = 0;
  for (const hotelSpec of PILOT_HOTELS) {
    if (aggregateSpend >= PILOT_AGGREGATE_CEILING_USD - 1e-9) {
      hotels.push({
        hotelId: hotelSpec.hotelId,
        status: "BLOCKED_AGGREGATE_BUDGET",
        spentUsd: 0,
      });
      continue;
    }
    const hotelRunId = `${portfolioRunId}__${hotelSpec.hotelId}`;
    const row = await executeHotelPaidPilot(hotelSpec, {
      ...opts,
      reportRoot,
      hotelRunId,
      maxCandidates: opts.maxCandidates ?? 4,
    });
    hotels.push(row);
    aggregateSpend = Number(
      (aggregateSpend + Number(row.spentUsd || 0)).toFixed(6),
    );
    if (aggregateSpend > PILOT_AGGREGATE_CEILING_USD + 1e-9) {
      // Should be impossible if per-hotel ledgers hold — freeze report
      writeJson(path.join(reportRoot, "AGGREGATE_OVERRUN.json"), {
        aggregateSpend,
        ceiling: PILOT_AGGREGATE_CEILING_USD,
      });
      break;
    }
  }

  const persistenceGate = evaluateResearchPersistenceAuthorization({
    authorizeResearchPersistence: false,
    apply: false,
  });

  const summary = {
    contract: GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT,
    portfolioRunId,
    generatedAt: nowIso(),
    hotels,
    aggregateSpentUsd: aggregateSpend,
    aggregateCeilingUsd: PILOT_AGGREGATE_CEILING_USD,
    perHotelCeilingUsd: PILOT_PER_HOTEL_CEILING_USD,
    safety: {
      publicationExecuted: false,
      airtableProductionWrites: false,
      surfeExecuted: false,
      weeklyAutomationActivated: false,
      sequentialExecution: true,
      forceSpend: false,
    },
    persistenceDryRunBlocked: true,
    persistenceGate,
    planOk: plan.ok,
  };
  writeJson(path.join(reportRoot, `${portfolioRunId}.summary.json`), summary);
  writeJson(path.join(reportRoot, "PAID_PILOT_SUMMARY.json"), summary);
  return { ok: hotels.every((h) => h.ok !== false), ...summary, reportRoot };
}

/**
 * Zero-paid replay using saved evidence / historical replay.
 */
export async function runZeroPaidReplay(opts = {}) {
  const reportRoot =
    opts.reportRoot ||
    path.join(ROOT, "reports", "gdi-portfolio-cert-v1-1-paid-pilot");
  const replayId =
    text(opts.replayRunId) ||
    `gdi_portfolio_cert_v1_1_replay_${Date.now().toString(16).slice(0, 8)}`;
  const hotels = [];
  for (const hotelSpec of PILOT_HOTELS) {
    const before = resolveHotelIdentity(hotelSpec.hotelId);
    const ledgerBefore = loadOrCreatePaidBudgetLedger({
      authorizationId: hotelSpec.authorizationId,
      hardCeilingUsd: hotelSpec.perHotelCeilingUsd,
      resume: true,
      allowCeilingRaise: false,
    });
    const spentBefore = Number(ledgerBefore.ledger?.spentUsd || 0);
    const outDir = path.join(
      reportRoot,
      "replay",
      hotelSpec.hotelId,
      replayId,
    );
    const result = await runCanonicalFullE2eResearch({
      hotelId: hotelSpec.hotelId,
      mode: CANONICAL_EXECUTION_MODE.DRY_RUN,
      dryRun: true,
      authorizePaidDiscovery: false,
      authorizePaidResearch: false,
      authorizeExaAgentLive: false,
      authorizePublish: false,
      budgetCeilingUsd: 0,
      maxCandidates: opts.maxCandidates ?? 4,
      outDir,
      segmentIds: hotelSpec.prioritySegments,
      allowHistoricalReplay: true,
      planResearchPersistence: true,
      authorizeResearchPersistence: false,
      applyResearchPersistence: false,
    });
    const ledgerAfter = loadOrCreatePaidBudgetLedger({
      authorizationId: hotelSpec.authorizationId,
      hardCeilingUsd: hotelSpec.perHotelCeilingUsd,
      resume: true,
      allowCeilingRaise: false,
    });
    const after = resolveHotelIdentity(hotelSpec.hotelId);
    const sampleId = before.opportunityIds?.[0] || "gdi_opp_replay_probe";
    const weekly = buildDurableWeeklyChangeAnnotation({
      baselineOpportunity: {
        id: sampleId,
        title: "Replay baseline",
        organizationName: "Replay Org",
      },
      currentOpportunity: {
        id: sampleId,
        title: "Replay baseline",
        organizationName: "Replay Org",
      },
      runId: replayId,
    });
    hotels.push({
      hotelId: hotelSpec.hotelId,
      ok: result?.ok !== false,
      spentUsdDelta:
        Number(ledgerAfter.ledger?.spentUsd || 0) - spentBefore,
      opportunityIdsStable:
        JSON.stringify(before.opportunityIds || []) ===
        JSON.stringify(after.opportunityIds || []),
      weeklyDeltaState: weekly.weeklyDeltaState,
      reportDir: outDir,
    });
  }
  const doc = {
    contract: GDI_PORTFOLIO_CERT_V1_1_PAID_PILOT,
    mode: "zero_paid_replay",
    replayId,
    generatedAt: nowIso(),
    hotels,
    noAdditionalPaidCalls: hotels.every((h) => Number(h.spentUsdDelta) === 0),
  };
  writeJson(path.join(reportRoot, `${replayId}.replay.json`), doc);
  return doc;
}

export function getPilotReportRoot() {
  return path.join(ROOT, "reports", "gdi-portfolio-cert-v1-1-paid-pilot");
}
