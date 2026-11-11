/**
 * Option B Stage 1 live runner — fail-closed.
 *
 * Proves Apify → normalize → shared pool → inventory dedup → one Exa/ORS deep call.
 * Does NOT run Stage 2. Does NOT publish. Does NOT write Airtable opportunities.
 *
 * Requires ALL of:
 *   - authorizeOptionBStage1Live === true
 *   - approvalPhrase === OPTION_B_STAGE1_APPROVAL_PHRASE
 *   - authorizeApifyLive + APIFY_TOKEN + GDI_APIFY_LIVE=1
 *   - authorizeExaLive + EXA_API_KEY
 *   - executeLive === true
 *   - apifyBudgetUsd / exaBudgetUsd / aggregateBudgetUsd within Stage 1 ceilings
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OPTION_B_STAGE1_APPROVAL_PHRASE,
  buildOptionBStagePlansV7,
} from "./unified-discovery-option-b-stage-plans-v7.js";
import {
  runApifyDiscovery,
  createApifySpendLedger,
  isApifyLiveAuthorized,
} from "../research/apify/apify-discovery-provider-v1.js";
import { buildSharedCandidatePoolV7 } from "./unified-discovery-candidate-schema-v7.js";
import {
  PREQUAL_DECISION,
  rankCandidatesForDeepResearch,
  buildAdaptiveDeepResearchPlan,
} from "./unified-discovery-commercial-prequalify-v7.js";
import { loadOpportunities } from "../repository.js";
import { mergeLocalShadowCommercialOpportunities } from "../local-shadow-commercial-portfolio-v1.js";
import { WATERSTONE_V7_HOTEL } from "./unified-discovery-engine-v7-contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

/** High-effort Exa Agent fixed quote (adapter FIXED_EFFORT_USD.high). */
const FIXED_EFFORT_HIGH_USD = 0.5;

const WATCH_CODES = new Set(["AES", "WIO", "URA"]);

function text(v) {
  return String(v || "").trim();
}

function summarizeCandidate(c = {}) {
  return {
    dedupeKey: c.dedupeKey || null,
    title: c.sourceTitle || c.title || null,
    organizationName: c.organizationName || null,
    startDate: c.startDate || null,
    endDate: c.endDate || null,
    sourceUrl: c.sourceUrl || null,
    city: c.city || null,
    commercialScreen: c.commercialScreen || null,
    researchTrack: c.researchTrack || null,
    provider: c.provider || null,
    actorOrRequestId: c.actorOrRequestId || null,
  };
}

/**
 * Map unified pool row → shape expected by commercial prequalify/rank.
 */
function toPrequalInput(c = {}) {
  const actor = String(c.actorOrRequestId || c.provenance?.actorId || "");
  const relationship =
    c.researchTrack === "ACCOUNT_RELATIONSHIP_DEMAND" ||
    c.commercialScreen === "RELATIONSHIP_CONTINUATION_CANDIDATE" ||
    /knot|wedding/i.test(actor);
  return {
    title: c.sourceTitle || c.eventOrProgrammeIdentity || c.title,
    organizationName:
      c.organizationName && c.organizationName !== "[object Object]"
        ? c.organizationName
        : null,
    summary: c.summary || null,
    category: c.category || null,
    venueName: c.location || c.venueName || null,
    sourceUrl: c.sourceUrl || null,
    city: c.city || null,
    region: c.region || null,
    startDate: c.startDate || null,
    endDate: c.endDate || null,
    recordKind: relationship
      ? "VENDOR_OR_VENUE_LISTING"
      : c.demandMechanism || c.recordKind || null,
    relationshipTrackOnly: relationship,
    headline: c.sourceTitle || c.title || null,
  };
}

/**
 * V7.1 Stage 1/2 selection — commercial thesis first; dates are not dominant.
 * Rejects nightlife/generic listings before any Deep Research spend.
 */
export function selectStage1DeepResearchCandidate(pool = []) {
  const rejected = [];
  const eligible = [];

  for (const c of pool) {
    const title = text(c.sourceTitle || c.eventOrProgrammeIdentity || c.title);
    const org = text(c.organizationName);
    const url = text(c.sourceUrl);
    const blob = `${title} ${org} ${url} ${c.location || ""} ${c.city || ""}`.toLowerCase();

    if (c.commercialScreen === "DEMOTE_COMPETITOR_HOST_RISK") {
      rejected.push({ reason: "COMPETITOR_HOST_RISK", ...summarizeCandidate(c) });
      continue;
    }
    if (WATCH_CODES.has(title.toUpperCase()) || WATCH_CODES.has(org.toUpperCase())) {
      rejected.push({ reason: "WATCH_CONTINUATION_WITHOUT_NEW_EVIDENCE", ...summarizeCandidate(c) });
      continue;
    }
    if (!title || title.length < 6) {
      rejected.push({ reason: "THIN_TITLE", ...summarizeCandidate(c) });
      continue;
    }
    if (!url || /example\.com|mock-/i.test(url)) {
      rejected.push({ reason: "MISSING_OR_MOCK_URL", ...summarizeCandidate(c) });
      continue;
    }
    if (/\b(hilton|marriott|hyatt|ihg|westin|sheraton)\b/.test(blob)) {
      rejected.push({ reason: "COMPETING_HOTEL_SIGNAL", ...summarizeCandidate(c) });
      continue;
    }
    eligible.push(c);
  }

  const ranked = rankCandidatesForDeepResearch(eligible.map(toPrequalInput));
  const advance = ranked.filter(
    (r) =>
      r.prequal.decision === PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH ||
      r.prequal.decision === PREQUAL_DECISION.RELATIONSHIP_RESEARCH,
  );

  for (const r of ranked) {
    if (
      r.prequal.decision === PREQUAL_DECISION.REJECT_EARLY ||
      r.prequal.decision === PREQUAL_DECISION.HOLD_FOR_MORE_DISCOVERY
    ) {
      const orig = eligible.find(
        (c) =>
          text(c.sourceUrl) === text(r.candidate.sourceUrl) &&
          text(c.sourceTitle || c.title) === text(r.candidate.title),
      );
      rejected.push({
        reason: r.prequal.rejectionReason || r.prequal.decision,
        qualityClass: r.classification.qualityClass,
        rankReasons: r.rankReasons,
        ...(orig ? summarizeCandidate(orig) : { title: r.candidate.title }),
      });
    }
  }

  const pick = advance[0] || null;
  if (!pick) {
    return {
      selected: null,
      tier: null,
      reasons: [],
      rejected,
      ranked,
      deepResearchPlan: null,
    };
  }

  const selectedOrig =
    eligible.find(
      (c) =>
        text(c.sourceUrl) === text(pick.candidate.sourceUrl) &&
        text(c.sourceTitle || c.title) === text(pick.candidate.title),
    ) || eligible[0];

  const tier =
    pick.prequal.decision === PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH
      ? "COMMERCIAL_THESIS_DATED_OR_PLAUSIBLE"
      : "WEDDING_PLANNER_RELATIONSHIP_LEAD";

  return {
    selected: selectedOrig,
    tier,
    reasons: pick.rankReasons,
    rejected,
    ranked,
    deepResearchPlan: buildAdaptiveDeepResearchPlan(pick),
    qualityClass: pick.classification.qualityClass,
    prequalDecision: pick.prequal.decision,
  };
}

function createExaLedger() {
  return {
    version: "gdi-exa-spend-ledger-option-b-stage1",
    provider: "exa",
    spentUsd: 0,
    entries: [],
    halted: false,
    haltReason: null,
  };
}

function authorizationGate(input = {}) {
  const plans = buildOptionBStagePlansV7();
  const s1 = plans.stage1;
  const reasons = [];

  if (input.authorizeOptionBStage1Live !== true) {
    reasons.push("authorizeOptionBStage1Live_required");
  }
  if (text(input.approvalPhrase) !== OPTION_B_STAGE1_APPROVAL_PHRASE) {
    reasons.push("approval_phrase_mismatch");
  }
  if (input.authorizeApifyLive !== true) reasons.push("authorizeApifyLive_required");
  if (input.authorizeExaLive !== true && input.authorizePaidResearch !== true) {
    reasons.push("authorizeExaLive_required");
  }
  if (input.executeLive !== true) reasons.push("executeLive_required");
  if (input.authorizePublication === true) reasons.push("publication_must_remain_false");
  if (input.authorizeAirtableMutation === true) {
    reasons.push("airtable_opportunity_writes_must_remain_false");
  }

  const apifyBudget = Number(input.apifyBudgetUsd || 0);
  const exaBudget = Number(input.exaBudgetUsd || 0);
  const aggregateBudget = Number(
    input.aggregateBudgetUsd || apifyBudget + exaBudget || 0,
  );

  if (apifyBudget <= 0 || apifyBudget > s1.providerCeilings.apifyUsd) {
    reasons.push(`apifyBudgetUsd_must_be_in_(0,${s1.providerCeilings.apifyUsd}]`);
  }
  if (exaBudget <= 0 || exaBudget > s1.providerCeilings.exaUsd) {
    reasons.push(`exaBudgetUsd_must_be_in_(0,${s1.providerCeilings.exaUsd}]`);
  }
  if (aggregateBudget <= 0 || aggregateBudget > s1.hardCeilingUsd) {
    reasons.push(`aggregateBudgetUsd_must_be_in_(0,${s1.hardCeilingUsd}]`);
  }
  if (!isApifyLiveAuthorized({ authorizeApifyLive: true })) {
    reasons.push("APIFY_TOKEN_or_GDI_APIFY_LIVE_missing");
  }
  if (!text(process.env.EXA_API_KEY)) reasons.push("EXA_API_KEY_missing");

  return {
    ok: reasons.length === 0,
    reasons,
    ceilings: s1.providerCeilings,
    aggregateCeilingUsd: s1.hardCeilingUsd,
    plans,
  };
}

/**
 * Preflight or (when fully authorized) Stage 1 live execution.
 * Default: dry-run / blocked — never spends from the Option B preflight prompt.
 */
export async function runOptionBStage1LiveRunnerV7(input = {}) {
  const plans = buildOptionBStagePlansV7();
  const hotel = {
    hotelId: WATERSTONE_V7_HOTEL.hotelId,
    name: WATERSTONE_V7_HOTEL.name,
    city: "Boca Raton",
  };

  const outDir =
    input.outDir ||
    path.join(
      ROOT,
      "reports/gdi-unified-discovery-engine-v7/waterstone-2027/live-execution-preflight",
      "stage1-runner-dry",
    );
  fs.mkdirSync(outDir, { recursive: true });

  const auth = authorizationGate(input);
  const inventoryStore = loadOpportunities(hotel.hotelId);
  let inventory = [...(inventoryStore?.opportunities || [])];
  try {
    inventory = mergeLocalShadowCommercialOpportunities(hotel.hotelId, inventory);
  } catch {
    /* optional */
  }

  const baseReport = {
    version: "gdi-option-b-stage1-live-runner-v7",
    hotel,
    needPeriods: WATERSTONE_V7_HOTEL.needPeriods,
    stage1Plan: plans.stage1,
    inventoryCount: inventory.length,
    auth: {
      ok: auth.ok,
      reasons: auth.reasons,
      ceilings: auth.ceilings,
    },
    publicationBlocked: true,
    airtableOpportunityWritesBlocked: true,
    stage2AutoLaunch: false,
    artifactClass: auth.ok && input.executeLive === true ? "LIVE_EXECUTION" : "PREFLIGHT_OR_DRY_RUN",
  };

  if (!auth.ok || input.executeLive !== true) {
    const blocked = {
      ...baseReport,
      ok: true,
      executed: false,
      spentUsd: { apify: 0, exa: 0, aggregate: 0 },
      message: auth.ok
        ? "Authorization shape OK but executeLive!==true — dry-run only. No paid calls."
        : "Stage 1 live blocked — missing founder authorization / budgets / tokens.",
      intendedCalls: plans.stage1.calls,
      exaHandoffDesign: {
        module: "runExaAgentResearch",
        importFrom: "lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js",
        effort: "high",
        maxCostDollars: plans.stage1.calls.exa[0].maxCostDollars,
        note: "Invoked only after live Apify pool + commercial screen selects one net-new candidate",
      },
    };
    fs.writeFileSync(
      path.join(outDir, "STAGE1-RUNNER-STATUS.json"),
      `${JSON.stringify(blocked, null, 2)}\n`,
    );
    return blocked;
  }

  // ——— LIVE PATH (only when fully authorized) ———
  const apifyLedger = createApifySpendLedger();
  const exaLedger = createExaLedger();
  const apifyCeiling = Number(input.apifyBudgetUsd);
  const exaCeiling = Number(input.exaBudgetUsd);
  const aggregateCeiling = Number(input.aggregateBudgetUsd);
  const allCandidates = [];
  const apifyRuns = [];

  for (const call of plans.stage1.calls.apify) {
    const est = call.estimateUsd;
    if (apifyLedger.spentUsd + est > apifyCeiling) {
      apifyLedger.halted = true;
      apifyLedger.haltReason = "APIFY_STAGE1_CEILING";
      break;
    }
    if (apifyLedger.spentUsd + exaLedger.spentUsd + est > aggregateCeiling) {
      apifyLedger.halted = true;
      apifyLedger.haltReason = "AGGREGATE_STAGE1_CEILING";
      break;
    }
    const run = await runApifyDiscovery({
      actorId: call.actorId,
      input: call.input,
      hotel,
      researchObjective: call.purpose,
      maxItems: call.maxItems,
      forceMockOnly: false,
      authorizeApifyLive: true,
      campaignCeilingUsd: Math.min(apifyCeiling, call.hardCapUsd),
      ledger: apifyLedger,
    });
    apifyRuns.push({
      actorId: call.actorId,
      ok: run.ok,
      mode: run.mode,
      runId: run.runId,
      apifyRunId: run.apifyRunId,
      candidateCount: run.candidates?.length || 0,
      spentUsd: run.spentUsd,
      error: run.error || null,
    });
    if (run.ok && run.mode === "live") {
      allCandidates.push(...(run.candidates || []));
    }
  }

  const gdiInventory = inventory.map((o) => ({
    title: o.title || o.name,
    organizationName: o.organizationName || o.organizerName,
    sourceUrl: o.sourceUrl || o.url,
    startDate: o.startDate,
  }));

  const pool = buildSharedCandidatePoolV7({
    apifyCandidates: allCandidates,
    exaCandidates: [],
    inventory: {
      gdi: gdiInventory,
      exa: [],
      priorApify: [],
      continuation: WATERSTONE_V7_HOTEL.priorCampaign.watchContinuation.map((c) => ({
        title: c,
        organizationName: c,
      })),
    },
  });

  // Quality-gated selection — do not force Deep Research on weak rows
  const selection = selectStage1DeepResearchCandidate(pool.pool || []);
  const selected = selection.selected;
  const selectionRejected = selection.rejected;

  let exaResult = null;
  let exaSkipped = null;
  if (!selected) {
    exaSkipped = {
      reason: "NO_SUITABLE_CANDIDATE_FOR_DEEP_RESEARCH",
      detail:
        "No commercially promising net-new candidate met Stage 1 quality gate — Exa spend skipped by founder rule",
      rejectedCount: selectionRejected.length,
    };
  } else if (apifyLedger.halted) {
    exaSkipped = { reason: "APIFY_LEDGER_HALTED", detail: apifyLedger.haltReason };
  } else {
    const maxExa = Math.min(
      plans.stage1.calls.exa[0].maxCostDollars,
      Number(FIXED_EFFORT_HIGH_USD),
      exaCeiling - exaLedger.spentUsd,
      aggregateCeiling - apifyLedger.spentUsd - exaLedger.spentUsd,
    );
    if (maxExa < 0.4) {
      exaLedger.halted = true;
      exaLedger.haltReason = "NO_REMAINING_EXA_OR_AGGREGATE_BUDGET";
      exaSkipped = { reason: "BUDGET_TOO_LOW_FOR_HIGH_EFFORT", maxExa };
    } else {
      const { runExaAgentResearch, quoteExaAgentRun } = await import(
        "../research/exa-agent/exa-agent-provider-adapter-v1.js"
      );
      const quote = quoteExaAgentRun({
        effort: "high",
        maxCostDollars: maxExa,
      });
      const plan = selection.deepResearchPlan;
      const contextBlock = [
        `Hotel: Waterstone Resort & Marina Boca Raton (HPC recgMYovrrZDJMqzX).`,
        `Need periods (TEST_SCENARIO): August–October 2027.`,
        `Candidate title: ${selected.sourceTitle || selected.eventOrProgrammeIdentity}`,
        `Organization: ${selected.organizationName || "unknown"}`,
        `Venue/location: ${selected.location || selected.city || "South Florida"}`,
        `Dates: ${selected.startDate || "unconfirmed"} – ${selected.endDate || "unconfirmed"}`,
        `Source URL: ${selected.sourceUrl || "none"}`,
        `Discovery provider: apify · actor/request: ${selected.actorOrRequestId || "n/a"}`,
        `Selection tier: ${selection.tier}`,
        `Quality class: ${selection.qualityClass || "n/a"}`,
        `Prequal decision: ${selection.prequalDecision || "n/a"}`,
        `Research track: ${selected.researchTrack}`,
        plan?.deepResearchQuestion
          ? `Primary research question: ${plan.deepResearchQuestion}`
          : null,
        plan?.primaryDemandHypothesis
          ? `Demand hypothesis: ${plan.primaryDemandHypothesis}`
          : null,
        `Investigate: demand mechanism, verified named buyer or legitimate referral entry point,`,
        `hotel fit for Waterstone, commercial accessibility (DIRECT_ADDRESSABLE / CREDIBLY_CONTESTABLE /`,
        `VERIFIED_PARTNERSHIP_REFERRAL / EVIDENCED_OVERFLOW — not speculative overflow or competitor-committed),`,
        `timing evidence for Aug–Oct 2027 if claiming need-month match, and recommended next commercial action.`,
        `Do not re-summarize the source listing alone. Stop if thesis collapses to nightlife/leisure-only.`,
        `Public evidence only. Do not use Surfe. Do not invent dates or room nights from attendance.`,
      ]
        .filter(Boolean)
        .join("\n");

      exaResult = await runExaAgentResearch({
        mode: "deep_research",
        contextBlock,
        effort: "high",
        maxCostDollars: maxExa,
        authorizeExaAgentLive: true,
        forceMockOnly: false,
      });
      const charged = Number(
        exaResult?.costDollars?.total ??
          exaResult?.spentUsd ??
          quote?.maxUsd ??
          FIXED_EFFORT_HIGH_USD,
      );
      if (apifyLedger.spentUsd + exaLedger.spentUsd + charged > aggregateCeiling) {
        exaLedger.halted = true;
        exaLedger.haltReason = "AGGREGATE_CEILING_WOULD_EXCEED_AFTER_EXA";
        exaSkipped = {
          reason: "AGGREGATE_CEILING",
          wouldCharge: charged,
          note: "Exa call may have completed — reconcile actual billing; ledger flags ceiling breach risk",
        };
      }
      exaLedger.spentUsd = Number((exaLedger.spentUsd + charged).toFixed(4));
      exaLedger.entries.push({
        purpose: "stage1_ors_deep_on_apify_candidate",
        candidateKey: selected.dedupeKey,
        selectionTier: selection.tier,
        spentUsd: charged,
        quote,
        exaOk: exaResult?.ok !== false,
        exaStatus: exaResult?.status || null,
        exaRunId: exaResult?.runId || null,
        at: new Date().toISOString(),
      });
    }
  }

  const liveReport = {
    ...baseReport,
    ok: true,
    executed: true,
    artifactClass: "LIVE_EXECUTION",
    apifyRuns,
    sharedPoolMetrics: pool.metrics,
    poolSample: (pool.pool || []).slice(0, 15).map(summarizeCandidate),
    selectionRejected: selectionRejected.slice(0, 40),
    selectedCandidate: selected
      ? {
          ...summarizeCandidate(selected),
          selectionTier: selection.tier,
          selectionReasons: selection.reasons,
          qualityClass: selection.qualityClass || null,
          prequalDecision: selection.prequalDecision || null,
          deepResearchPlan: selection.deepResearchPlan || null,
        }
      : null,
    exaOrs: exaResult
      ? {
          ok: exaResult.ok !== false,
          status: exaResult.status || null,
          runId: exaResult.runId || null,
          spentUsd: exaLedger.spentUsd,
          structured: exaResult.structured || null,
          textPreview: String(exaResult.text || "").slice(0, 2000),
          note: "Live Exa/ORS deep_research — Stage 1 only",
        }
      : {
          ok: false,
          skipped: true,
          ...exaSkipped,
        },
    spentUsd: {
      apify: apifyLedger.spentUsd,
      exa: exaLedger.spentUsd,
      aggregate: Number((apifyLedger.spentUsd + exaLedger.spentUsd).toFixed(4)),
      ceilings: {
        apify: apifyCeiling,
        exa: exaCeiling,
        aggregate: aggregateCeiling,
      },
      withinCeilings:
        apifyLedger.spentUsd <= apifyCeiling &&
        exaLedger.spentUsd <= exaCeiling &&
        apifyLedger.spentUsd + exaLedger.spentUsd <= aggregateCeiling,
    },
    ledgers: { apify: apifyLedger, exa: exaLedger },
    stage2Blocked: true,
    stage2Authorized: false,
    stage2Message:
      "Stage 2 NOT AUTHORIZED — stop for founder review; do not auto-launch",
    airtableOpportunityWrites: "DENIED",
    publication: "DENIED",
    surfe: "DENIED",
    firecrawl: "DENIED",
  };

  fs.writeFileSync(
    path.join(outDir, "STAGE1-LIVE-RESULT.json"),
    `${JSON.stringify(liveReport, null, 2)}\n`,
  );
  return liveReport;
}
