/**
 * Option B Stage 2 LIVE runner V7.2 — fail-closed until founder GO.
 *
 * Diversified dated demand + capped planner relationship Deep Research.
 * Enforces Apify/Exa/aggregate ceilings. No publication / Airtable / Surfe / Firecrawl.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OPTION_B_STAGE2_APPROVAL_PHRASE,
  OPTION_B_AGGREGATE_MAXIMUM_USD,
} from "./unified-discovery-option-b-stage-plans-v7.js";
import {
  STAGE1_ACTUAL_SPEND_USD,
  STAGE2_TRACK,
  V71_PLANNER_PRIORITY_QUEUE,
  verifyStage2BudgetHardStops,
  buildFourteenWorkflowStage2DiscoveryPlanV72,
} from "./unified-discovery-stage2-commercial-depth-plan-v72.js";
import {
  runApifyDiscovery,
  createApifySpendLedger,
  isApifyLiveAuthorized,
} from "../research/apify/apify-discovery-provider-v1.js";
import { buildSharedCandidatePoolV7 } from "./unified-discovery-candidate-schema-v7.js";
import {
  PREQUAL_DECISION,
  classifyDiscoveryCandidate,
  prequalifyForDeepResearch,
  rankCandidatesForDeepResearch,
  buildAdaptiveDeepResearchPlan,
} from "./unified-discovery-commercial-prequalify-v7.js";
import { buildResearchToDrawerIntelligenceV2 } from "../research-to-drawer-intelligence-v2.js";
import { loadOpportunities } from "../repository.js";
import { mergeLocalShadowCommercialOpportunities } from "../local-shadow-commercial-portfolio-v1.js";
import { WATERSTONE_V7_HOTEL } from "./unified-discovery-engine-v7-contract.js";
import { dedupeRepeatedDisplayName } from "../research/apify/apify-source-normalize-v1.js";
import {
  attachMultiRevenueClassification,
  buildMultiRevenueCibFields,
  HOTEL_REVENUE_STREAM_CLASS,
} from "../multi-revenue-qualification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXED_HIGH = 0.5;
const FIXED_MEDIUM = 0.1;
const WATCH = new Set(["AES", "WIO", "URA"]);

function text(v) {
  return String(v || "").trim();
}

function createExaLedger() {
  return {
    version: "gdi-exa-spend-ledger-option-b-stage2",
    provider: "exa",
    spentUsd: 0,
    entries: [],
    halted: false,
    haltReason: null,
  };
}

function remaining(apifyLedger, exaLedger, ceilings) {
  return {
    apify: Number((ceilings.apifyUsd - apifyLedger.spentUsd).toFixed(4)),
    exa: Number((ceilings.exaUsd - exaLedger.spentUsd).toFixed(4)),
    aggregate: Number(
      (ceilings.aggregateUsd - apifyLedger.spentUsd - exaLedger.spentUsd).toFixed(4),
    ),
  };
}

function canAfford(apifyLedger, exaLedger, ceilings, provider, estimate) {
  const rem = remaining(apifyLedger, exaLedger, ceilings);
  if (estimate > rem.aggregate) return false;
  if (provider === "apify" && estimate > rem.apify) return false;
  if (provider === "exa" && estimate > rem.exa) return false;
  return true;
}

function authorizeStage2(input = {}) {
  const reasons = [];
  const hardStops = verifyStage2BudgetHardStops({
    stage1SpentUsd: STAGE1_ACTUAL_SPEND_USD.aggregate,
    stage2ProposedCeilingUsd: Number(input.stage2AggregateBudgetUsd || 36),
    campaignMaximumUsd: OPTION_B_AGGREGATE_MAXIMUM_USD,
    stage2ApifyCeilingUsd: Number(input.stage2ApifyBudgetUsd || 4),
    stage2ExaCeilingUsd: Number(input.stage2ExaBudgetUsd || 32),
  });

  if (input.authorizeOptionBStage2Live !== true) reasons.push("authorizeOptionBStage2Live_required");
  if (text(input.approvalPhrase) !== OPTION_B_STAGE2_APPROVAL_PHRASE) {
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
  if (input.authorizeSurfe === true) reasons.push("surfe_must_remain_false");
  if (input.authorizeFirecrawl === true) reasons.push("firecrawl_must_remain_false");

  const apifyB = Number(input.stage2ApifyBudgetUsd);
  const exaB = Number(input.stage2ExaBudgetUsd);
  const aggB = Number(input.stage2AggregateBudgetUsd);
  if (!(apifyB > 0 && apifyB <= hardStops.providerCeilings.apifyUsd + 1e-9)) {
    reasons.push(`stage2ApifyBudgetUsd_must_be_in_(0,${hardStops.providerCeilings.apifyUsd}]`);
  }
  if (!(exaB > 0 && exaB <= 32 + 1e-9)) {
    reasons.push("stage2ExaBudgetUsd_must_be_in_(0,32]");
  }
  if (!(aggB > 0 && aggB <= hardStops.authorizedStage2CeilingUsd + 1e-9)) {
    reasons.push(
      `stage2AggregateBudgetUsd_must_be_in_(0,${hardStops.authorizedStage2CeilingUsd}]`,
    );
  }
  if (apifyB + exaB < aggB - 0.01 && aggB > apifyB + exaB) {
    // ok — aggregate is the hard combined cap; provider caps are independent
  }
  if (!isApifyLiveAuthorized({ authorizeApifyLive: true })) {
    reasons.push("APIFY_TOKEN_or_GDI_APIFY_LIVE_missing");
  }
  if (!text(process.env.EXA_API_KEY)) reasons.push("EXA_API_KEY_missing");
  if (!hardStops.ok) reasons.push("budget_hard_stops_failed");

  return {
    ok: reasons.length === 0,
    reasons,
    hardStops,
    ceilings: {
      apifyUsd: Math.min(apifyB, hardStops.providerCeilings.apifyUsd),
      exaUsd: Math.min(exaB, hardStops.providerCeilings.exaUsd, aggB),
      aggregateUsd: Math.min(aggB, hardStops.authorizedStage2CeilingUsd),
    },
  };
}

function toPrequalShape(c) {
  const actor = String(c.actorOrRequestId || c.provenance?.actorId || "");
  const relationship =
    c.researchTrack === "ACCOUNT_RELATIONSHIP_DEMAND" ||
    c.relationshipTrackOnly ||
    /knot|wedding|planner/i.test(actor) ||
    c.trackSeed === "V71_PLANNER";
  return {
    title: dedupeRepeatedDisplayName(c.sourceTitle || c.title || c.eventOrProgrammeIdentity),
    organizationName:
      c.organizationName === "[object Object]" ? null : c.organizationName,
    summary: c.summary || c.notes || null,
    category: c.category || null,
    venueName: c.location || c.venueName || null,
    sourceUrl: c.sourceUrl || null,
    city: c.city || "Boca Raton",
    region: c.region || "FL",
    startDate: c.startDate || c.activityDate || null,
    endDate: c.endDate || null,
    recordKind: relationship
      ? "VENDOR_OR_VENUE_LISTING"
      : c.recordKind || c.demandMechanism || "PUBLISHED_EVENT",
    relationshipTrackOnly: relationship,
    headline: c.sourceTitle || c.title || null,
  };
}

function extractExaDiscoveryCandidates(structured, ctx = {}) {
  const out = [];
  const list = structured?.candidates || [];
  for (const cand of list) {
    const name =
      cand.companyIdentity?.legalOrTradingName ||
      cand.companyIdentity?.name ||
      cand.label ||
      null;
    if (!name) continue;
    const upper = name.toUpperCase();
    if (WATCH.has(upper) || WATCH.has(upper.slice(0, 3))) continue;
    out.push({
      discoveryProvider: "exa",
      title: name,
      organizationName: name,
      sourceUrl:
        cand.supportingEvidence?.[0]?.url ||
        cand.companyIdentity?.officialWebsite ||
        null,
      startDate: cand.activityDate || cand.likelyDemandTiming || null,
      city: "Boca Raton",
      summary: [
        cand.travelGeneratingMechanism,
        cand.potentialGuestroomDemand,
        cand.potentialMeetingDemand,
      ]
        .filter(Boolean)
        .join(" · "),
      category: ctx.workflow || null,
      relationshipTrackOnly: false,
      recordKind: "EXA_DISCOVERY_CANDIDATE",
      provenance: {
        provider: "exa",
        researchObjective: ctx.purpose,
        workflow: ctx.workflow,
        retrievedAt: new Date().toISOString(),
      },
      exaDiscoveryMeta: {
        gate1: cand.gate1OperationalActivity,
        gate2: cand.gate2HospitalityDemand,
        hospitalityDemandEvidenceLevel: cand.hospitalityDemandEvidenceLevel,
        hotelMarketRelevance: cand.hotelMarketRelevance,
      },
      rawRef: { exaCandidate: true },
    });
  }
  return out;
}

function qualifyFromDeep(structured, candidate, plan) {
  const buyers = structured?.buyerCandidates || [];
  const namedBuyer = buyers.find(
    (b) =>
      text(b.name || b.fullName || b.personName) &&
      !/unknown|tbd|n\/a/i.test(text(b.name || b.fullName || b.personName)),
  );
  const routes = structured?.organizationalContactRoutes || [];
  const entryPoint =
    namedBuyer ||
    routes.find((r) => text(r.route || r.url || r.email || r.path));
  const thesis =
    structured?.hotelDemandHypotheses?.primaryMechanism ||
    structured?.hotelDemandHypotheses?.guestroomHypothesis ||
    structured?.hotelDemandHypotheses?.meetingsHypothesis ||
    structured?.hotelDemandHypotheses?.cateringHypothesis ||
    structured?.hotelSpecificFit?.advantages;
  const evidenceStatus = structured?.hotelDemandHypotheses?.evidenceStatus;
  const multiRevenue = attachMultiRevenueClassification(
    {},
    {
      thesis,
      hotelDemandHypotheses: structured?.hotelDemandHypotheses,
      hotelDemandEvidence: structured?.hotelDemandEvidence,
      summary: structured?.activityAndChronology?.summary,
      recommendedAction: structured?.recommendedCommercialAction,
    },
  );
  const disposition = text(
    structured?.researchDisposition?.status ||
      structured?.researchDisposition?.recommendation ||
      structured?.researchDisposition,
  ).toLowerCase();
  const timingRaw =
    structured?.commercialTiming?.needMonthRelevance ||
    structured?.commercialTiming?.activityDate ||
    structured?.activityAndChronology?.activityDate ||
    candidate.startDate ||
    "";
  const timingStr = String(timingRaw);
  const needMonthHit = /2027-08|2027-09|2027-10|aug(ust)?\s*2027|sep(tember)?\s*2027|oct(ober)?\s*2027/i.test(
    timingStr,
  );
  const timingConfirmed =
    structured?.commercialTiming?.timingConfidence === "high" ||
    structured?.commercialTiming?.sourceSupportedTiming === true ||
    (needMonthHit &&
      /documented|confirmed|verified/i.test(
        String(structured?.commercialTiming?.timingEvidenceStatus || evidenceStatus || ""),
      ));

  const accessibility =
    structured?.sourcingAccessibility?.class ||
    structured?.sourcingAccessibility?.accessibilityClass ||
    structured?.sourcingAccessibility?.summary ||
    null;

  const rejectHints =
    /not.?commercially|non.?qualif|reject|unsupported|nightlife|no hotel|speculative overflow|competitor.?committed/i.test(
      disposition + " " + String(evidenceStatus || ""),
    ) || evidenceStatus === "unsupported";

  // Guestrooms are NOT mandatory — EVENT_FNB_ONLY / COMBINED / GUESTROOMS_ONLY all qualify on revenue
  const hasCredibleRevenue = multiRevenue.multiRevenue?.qualifiesOnRevenue === true;
  const revenueUnconfirmed =
    multiRevenue.revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;

  if (rejectHints || (!thesis && !hasCredibleRevenue)) {
    return {
      bucket: "REJECTED_OR_INACCESSIBLE",
      track: null,
      namedBuyer: namedBuyer || null,
      entryPoint: entryPoint || null,
      thesis: thesis || null,
      accessibility,
      timingConfirmed: false,
      reason: rejectHints ? "ORS_NON_QUALIFY_OR_UNSUPPORTED" : "NO_REVENUE_THESIS",
      ...multiRevenue,
    };
  }

  if (revenueUnconfirmed) {
    return {
      bucket: "RESEARCH_CONTINUATION",
      track: null,
      namedBuyer: namedBuyer || null,
      entryPoint: entryPoint || null,
      thesis,
      accessibility,
      timingConfirmed: false,
      reason: "REVENUE_STREAM_UNCONFIRMED",
      ...multiRevenue,
    };
  }

  if (!entryPoint && !namedBuyer) {
    return {
      bucket: "RESEARCH_CONTINUATION",
      track: null,
      namedBuyer: null,
      entryPoint: null,
      thesis,
      accessibility,
      timingConfirmed: false,
      reason: "MISSING_NAMED_BUYER_OR_ENTRY_POINT",
      ...multiRevenue,
    };
  }

  const salesReady =
    /DIRECT_ADDRESSABLE|CREDIBLY_CONTESTABLE|VERIFIED_PARTNERSHIP|EVIDENCED_OVERFLOW/i.test(
      String(accessibility || ""),
    );

  if (timingConfirmed || (needMonthHit && salesReady)) {
    return {
      bucket: "VERIFIED_NEED_DATE",
      track: STAGE2_TRACK.A_VERIFIED_NEED_DATE,
      namedBuyer: namedBuyer || null,
      entryPoint: entryPoint || null,
      thesis,
      accessibility,
      timingConfirmed: true,
      needMonthHint: timingStr,
      reason: "TIMING_AND_COMMERCIAL_GATES",
      ...multiRevenue,
    };
  }

  if (
    candidate.relationshipTrackOnly ||
    candidate.trackSeed === "V71_PLANNER" ||
    /relationship|partnership|referral/i.test(String(accessibility || disposition))
  ) {
    return {
      bucket: "QUALIFIED_RELATIONSHIP",
      track: STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP,
      namedBuyer: namedBuyer || null,
      entryPoint: entryPoint || null,
      thesis,
      accessibility: accessibility || "TIMING_UNCONFIRMED_RELATIONSHIP",
      timingConfirmed: false,
      reason: "RELATIONSHIP_GATES_WITHOUT_NEED_MONTH",
      ...multiRevenue,
    };
  }

  return {
    bucket: "RESEARCH_CONTINUATION",
    track: null,
    namedBuyer: namedBuyer || null,
    entryPoint: entryPoint || null,
    thesis,
    accessibility,
    timingConfirmed: false,
    reason: "UNRESOLVED_TIMING_OR_ACCESSIBILITY",
    ...multiRevenue,
  };
}

function buildCibPack(candidate, deepStructured, qualification) {
  const cibRevenue = buildMultiRevenueCibFields(qualification.multiRevenue || qualification);
  const opp = {
    title: candidate.sourceTitle || candidate.title,
    organizationName: candidate.organizationName,
    sourceUrl: candidate.sourceUrl,
    startDate: candidate.startDate,
    hotelOpportunityThesis: qualification.thesis,
    uncertainties: deepStructured?.materialUncertainties || [],
    commercialIntelligenceBrief: {
      potentialBusinessForThisHotel: qualification.thesis,
      applicableHotelRevenueStreams: cibRevenue.applicableHotelRevenueStreams,
      potentialBusinessGuestroomThesis: cibRevenue.potentialBusinessGuestroomThesis,
      potentialBusinessEventFnbThesis: cibRevenue.potentialBusinessEventFnbThesis,
    },
  };
  const drawer = buildResearchToDrawerIntelligenceV2(opp, {
    run: { structured: deepStructured },
  });
  return {
    candidateTitle: opp.title,
    organizationName: opp.organizationName,
    track: qualification.track,
    bucket: qualification.bucket,
    revenueStreamClass: qualification.revenueStreamClass || cibRevenue.applicableHotelRevenueStreams,
    revenueStreamLabel: qualification.revenueStreamLabel || cibRevenue.applicableHotelRevenueStreamsLabel,
    multiRevenueCib: cibRevenue,
    qualification,
    tenSectionPresent: Boolean(drawer),
    drawerIntelligenceV2: drawer,
    orsDisposition: deepStructured?.researchDisposition || null,
    recommendedAction: deepStructured?.recommendedCommercialAction || null,
    buyerCandidates: deepStructured?.buyerCandidates || [],
    supportingSources: deepStructured?.supportingSources || [],
  };
}

async function chargeExa(exaLedger, apifyLedger, ceilings, entry) {
  const spent = Number(entry.spentUsd || 0);
  if (!canAfford(apifyLedger, exaLedger, ceilings, "exa", spent)) {
    exaLedger.halted = true;
    exaLedger.haltReason = "WOULD_EXCEED_CEILING";
    return false;
  }
  exaLedger.spentUsd = Number((exaLedger.spentUsd + spent).toFixed(4));
  exaLedger.entries.push({ ...entry, at: new Date().toISOString() });
  return true;
}

/**
 * Stage 2 live execution.
 */
export async function runOptionBStage2LiveRunnerV72(input = {}) {
  const outDir =
    input.outDir ||
    path.join(
      ROOT,
      "reports/gdi-unified-discovery-engine-v7/waterstone-2027/live-execution-stage2",
    );
  fs.mkdirSync(outDir, { recursive: true });

  const auth = authorizeStage2(input);
  const hotel = {
    hotelId: WATERSTONE_V7_HOTEL.hotelId,
    name: WATERSTONE_V7_HOTEL.name,
    city: "Boca Raton",
  };
  const fourteen = buildFourteenWorkflowStage2DiscoveryPlanV72();

  const base = {
    version: "gdi-option-b-stage2-live-runner-v7.2",
    hotel,
    needPeriods: WATERSTONE_V7_HOTEL.needPeriods,
    auth: { ok: auth.ok, reasons: auth.reasons, ceilings: auth.ceilings },
    hardStops: auth.hardStops,
    stage1ActualSpendUsd: STAGE1_ACTUAL_SPEND_USD,
    publicationBlocked: true,
    airtableOpportunityWritesBlocked: true,
    surfeBlocked: true,
    firecrawlBlocked: true,
    additionalCampaignBlocked: true,
    artifactClass: auth.ok && input.executeLive === true ? "LIVE_EXECUTION" : "BLOCKED",
  };

  if (!auth.ok || input.executeLive !== true) {
    const blocked = {
      ...base,
      ok: true,
      executed: false,
      message: "Stage 2 live blocked — authorization incomplete or executeLive!==true",
    };
    fs.writeFileSync(path.join(outDir, "STAGE2-BLOCKED.json"), `${JSON.stringify(blocked, null, 2)}\n`);
    return blocked;
  }

  const ceilings = auth.ceilings;
  const apifyLedger = createApifySpendLedger();
  const exaLedger = createExaLedger();
  // Preserve adaptive reserve until blockers assessed
  let adaptiveReserveUsd = Math.min(4.0, ceilings.aggregateUsd * 0.12);
  const waveLog = [];
  const persist = (name, obj) => {
    fs.writeFileSync(path.join(outDir, name), `${JSON.stringify(obj, null, 2)}\n`);
  };

  // ——— WAVE A: inventory (zero paid) ———
  const inventoryStore = loadOpportunities(hotel.hotelId);
  let inventory = [...(inventoryStore?.opportunities || [])];
  try {
    inventory = mergeLocalShadowCommercialOpportunities(hotel.hotelId, inventory);
  } catch {
    /* optional */
  }
  const inventorySummary = {
    count: inventory.length,
    watchExcluded: ["AES", "WIO", "URA"],
    titlesSample: inventory.slice(0, 20).map((o) => o.title || o.name),
  };
  waveLog.push({ wave: "WAVE_A", paid: false, inventoryCount: inventory.length });
  persist("WAVE-A-INVENTORY.json", inventorySummary);

  // Seed Track B planners from V7.1 (no paid Knot re-scrape as strategy)
  const plannerSeeds = V71_PLANNER_PRIORITY_QUEUE.map((p) => ({
    discoveryProvider: "v71_remediation_seed",
    title: p.title,
    organizationName: p.title,
    sourceUrl: p.sourceUrl,
    city: "Boca Raton",
    relationshipTrackOnly: true,
    recordKind: "VENDOR_OR_VENUE_LISTING",
    trackSeed: "V71_PLANNER",
    researchTrack: "ACCOUNT_RELATIONSHIP_DEMAND",
    summary: p.why,
    provenance: { provider: "v71_seed", retrievedAt: new Date().toISOString() },
  }));

  // ——— WAVE B: Apify diversified discovery ———
  const apifyPlan = [
    {
      actorId: "solidcode/eventbrite-scraper",
      purpose: "Dated Boca public events Aug–Oct 2027 (meetings/business/social filter via volume + prequal)",
      workflow: "meetings_incentives",
      input: {
        country: "united-states",
        city: "Boca Raton",
        startDate: "2027-08-01",
        endDate: "2027-10-31",
        eventFormat: "in-person",
        maxResults: 35,
      },
      maxItems: 35,
      estimateUsd: 0.15,
    },
    {
      actorId: "solidcode/eventbrite-scraper",
      purpose: "Dated West Palm Beach public events Aug–Oct 2027 — association/meeting catchment",
      workflow: "association_events",
      input: {
        country: "united-states",
        city: "West Palm Beach",
        startDate: "2027-08-01",
        endDate: "2027-10-31",
        eventFormat: "in-person",
        maxResults: 25,
      },
      maxItems: 25,
      estimateUsd: 0.12,
    },
    {
      actorId: "filip_cicvarek/meetup-scraper",
      purpose: "Meetup Boca networking/professional groups — local catering & meetings thesis",
      workflow: "local_catering_day_meetings",
      input: {
        mode: "events",
        searchKeyword: "business networking",
        city: "Boca Raton",
        state: "FL",
        country: "United States",
        startDateRange: "2027-08-01",
        endDateRange: "2027-10-31",
        maxResults: 25,
      },
      maxItems: 25,
      estimateUsd: 0.08,
    },
    {
      actorId: "solidcode/eventbrite-scraper",
      purpose: "Dated Delray Beach events Aug–Oct 2027 — social/sports/celebration catchment",
      workflow: "social_special_events",
      input: {
        country: "united-states",
        city: "Delray Beach",
        startDate: "2027-08-01",
        endDate: "2027-10-31",
        eventFormat: "in-person",
        maxResults: 20,
      },
      maxItems: 20,
      estimateUsd: 0.1,
    },
  ];

  const apifyRuns = [];
  const apifyCandidates = [];
  for (const call of apifyPlan) {
    const rem = remaining(apifyLedger, exaLedger, ceilings);
    // Keep headroom for Exa — do not spend entire aggregate on Apify
    const apifySoftCap = Math.min(ceilings.apifyUsd, 3.8);
    if (apifyLedger.spentUsd + call.estimateUsd > apifySoftCap) {
      apifyRuns.push({ actorId: call.actorId, skipped: true, reason: "APIFY_SOFT_CAP" });
      break;
    }
    if (!canAfford(apifyLedger, exaLedger, ceilings, "apify", call.estimateUsd)) {
      apifyLedger.halted = true;
      apifyLedger.haltReason = "CEILING";
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
      campaignCeilingUsd: ceilings.apifyUsd,
      ledger: apifyLedger,
    });
    apifyRuns.push({
      actorId: call.actorId,
      purpose: call.purpose,
      workflow: call.workflow,
      ok: run.ok,
      mode: run.mode,
      runId: run.runId,
      apifyRunId: run.apifyRunId,
      candidateCount: run.candidates?.length || 0,
      spentUsd: run.spentUsd,
      error: run.error || null,
    });
    if (run.ok && run.mode === "live") {
      for (const c of run.candidates || []) {
        apifyCandidates.push({ ...c, stage2Workflow: call.workflow });
      }
    }
    persist("WAVE-B-APIFY-PROGRESS.json", { apifyRuns, spent: apifyLedger.spentUsd, rem });
  }
  waveLog.push({
    wave: "WAVE_B_APIFY",
    paid: true,
    runs: apifyRuns.length,
    spentUsd: apifyLedger.spentUsd,
  });

  // ——— WAVE B: Exa discovery (diversified segments) ———
  const { runExaAgentResearch, quoteExaAgentRun } = await import(
    "../research/exa-agent/exa-agent-provider-adapter-v1.js"
  );

  const exaDiscoveryPlan = [
    {
      workflow: "meetings_incentives",
      purpose: "Corporate meetings & training demand owners Boca/Palm Beach Aug–Oct 2027",
      effort: "high",
    },
    {
      workflow: "association_events",
      purpose: "Association/conference programmes Boca–Palm Beach corridor Aug–Oct 2027 (demand owners, not hotel calendars)",
      effort: "high",
    },
    {
      workflow: "sports_entertainment",
      purpose: "Sports/entertainment travel lodging demand South Florida Aug–Oct 2027",
      effort: "high",
    },
    {
      workflow: "local_catering_day_meetings",
      purpose: "Local organizations generating banquet/day-meeting demand in Boca Raton 2027",
      effort: "medium",
    },
    {
      workflow: "scaling_companies",
      purpose: "Scaling/corporate accounts near Boca with group lodging or meeting potential",
      effort: "high",
    },
    {
      workflow: "social_special_events",
      purpose: "Dated social/wedding-related GROUP demand Boca Aug–Oct 2027 (not planner directories)",
      effort: "high",
    },
    {
      workflow: "corporate_celebrations",
      purpose: "Corporate celebrations/galas/fundraisers Boca–Palm Beach Aug–Oct 2027",
      effort: "medium",
    },
    {
      workflow: "education_scientific",
      purpose: "Education/scientific programmes FAU/Nova/Palm Beach Aug–Oct 2027 with lodging thesis",
      effort: "medium",
    },
    {
      workflow: "preferred_lodging_partnerships",
      purpose: "Boca venues/facilities lacking guestrooms that may need Waterstone partner lodging",
      effort: "medium",
    },
    {
      workflow: "travel_trade",
      purpose: "DMCs / group intermediaries placing Boca marina resort groups",
      effort: "medium",
    },
  ];

  const exaDiscoveryRuns = [];
  const exaCandidates = [];
  // Reserve adaptive + planned deep research headroom
  const deepReserveTarget = Math.min(18, ceilings.exaUsd * 0.55);
  for (const d of exaDiscoveryPlan) {
    const rem = remaining(apifyLedger, exaLedger, ceilings);
    const quote = quoteExaAgentRun({
      effort: d.effort,
      maxCostDollars: d.effort === "high" ? FIXED_HIGH : FIXED_MEDIUM,
    });
    const est = Number(quote?.maxUsd ?? (d.effort === "high" ? FIXED_HIGH : FIXED_MEDIUM));
    // Leave room for deep + adaptive
    if (rem.exa - est < adaptiveReserveUsd + 3) {
      exaDiscoveryRuns.push({ workflow: d.workflow, skipped: true, reason: "RESERVE_FOR_DEEP_AND_ADAPTIVE" });
      continue;
    }
    if (!canAfford(apifyLedger, exaLedger, ceilings, "exa", est)) {
      exaLedger.halted = true;
      exaLedger.haltReason = "DISCOVERY_CEILING";
      break;
    }
    const contextBlock = [
      `Hotel: ${hotel.name} (HPC ${hotel.hotelId}).`,
      `Need periods: August, September, October 2027 (TEST_SCENARIO).`,
      `Research workflow: ${d.workflow}.`,
      `Objective: ${d.purpose}.`,
      `Prioritize demand owners and programmes Waterstone could pursue.`,
      `Do NOT prioritize competing hotel venue calendars as primary demand.`,
      `Do NOT invent room nights, wedding dates, or RFPs.`,
      `Exclude AES, WIO, URA unless genuinely new evidence.`,
      `Return hospitality-demand-first candidates with timing evidence when available.`,
      `Public evidence only. No Surfe.`,
    ].join("\n");

    const run = await runExaAgentResearch({
      mode: "discovery",
      contextBlock,
      effort: d.effort,
      maxCostDollars: est,
      authorizeExaAgentLive: true,
      forceMockOnly: false,
      pollUntilFinished: true,
      maxPolls: 90,
    });
    const charged = Number(run?.costDollars?.total ?? est);
    await chargeExa(exaLedger, apifyLedger, ceilings, {
      purpose: "stage2_exa_discovery",
      workflow: d.workflow,
      spentUsd: charged,
      runId: run?.runId,
      status: run?.status,
      ok: run?.ok !== false,
    });
    const extracted = extractExaDiscoveryCandidates(run?.structured, d);
    exaCandidates.push(...extracted);
    exaDiscoveryRuns.push({
      workflow: d.workflow,
      purpose: d.purpose,
      effort: d.effort,
      ok: run?.ok !== false,
      status: run?.status,
      runId: run?.runId,
      spentUsd: charged,
      candidateCount: extracted.length,
      rejectedOrFlagged: run?.structured?.rejectedOrFlagged || [],
    });
    persist("WAVE-B-EXA-DISCOVERY-PROGRESS.json", {
      exaDiscoveryRuns,
      spent: exaLedger.spentUsd,
      rem: remaining(apifyLedger, exaLedger, ceilings),
    });
  }
  waveLog.push({
    wave: "WAVE_B_EXA_DISCOVERY",
    paid: true,
    runs: exaDiscoveryRuns.filter((r) => !r.skipped).length,
    spentUsd: exaLedger.entries
      .filter((e) => e.purpose === "stage2_exa_discovery")
      .reduce((s, e) => s + e.spentUsd, 0),
  });

  // ——— Shared pool + WAVE C prequal ———
  const pool = buildSharedCandidatePoolV7({
    apifyCandidates,
    exaCandidates: [...exaCandidates, ...plannerSeeds],
    inventory: {
      gdi: inventory.map((o) => ({
        title: o.title || o.name,
        organizationName: o.organizationName || o.organizerName,
        sourceUrl: o.sourceUrl || o.url,
        startDate: o.startDate,
      })),
      exa: [],
      priorApify: [],
      continuation: WATERSTONE_V7_HOTEL.priorCampaign.watchContinuation.map((c) => ({
        title: c,
        organizationName: c,
      })),
    },
  });

  // Attach planner seeds if deduped away incorrectly — ensure Track B seeds present
  const poolTitles = new Set(
    (pool.pool || []).map((c) => dedupeRepeatedDisplayName(c.sourceTitle || c.title).toLowerCase()),
  );
  for (const seed of plannerSeeds) {
    const t = seed.title.toLowerCase();
    if (!poolTitles.has(t)) {
      pool.pool.push(
        ...buildSharedCandidatePoolV7({
          apifyCandidates: [],
          exaCandidates: [seed],
          inventory: { gdi: [], exa: [], priorApify: [], continuation: [] },
        }).pool,
      );
      poolTitles.add(t);
    }
  }

  const ranked = rankCandidatesForDeepResearch(
    (pool.pool || []).map((c) => toPrequalShape({ ...c, trackSeed: c.provenance?.provider === "v71_seed" ? "V71_PLANNER" : null })),
  );
  const advanceDated = [];
  const advanceRel = [];
  const rejectedEarly = [];
  for (const row of ranked) {
    const title = row.candidate.title || "";
    if (WATCH.has(title.toUpperCase())) {
      rejectedEarly.push({ title, reason: "WATCH_EXCLUDED" });
      continue;
    }
    if (/party bus/i.test(title)) {
      rejectedEarly.push({ title, reason: "NIGHTLIFE_REJECT" });
      continue;
    }
    if (/chic bahamas/i.test(title)) {
      rejectedEarly.push({ title, reason: "V71_DEPRIORITIZE_DESTINATION_SKEW" });
      continue;
    }
    if (row.prequal.decision === PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH) {
      advanceDated.push(row);
    } else if (row.prequal.decision === PREQUAL_DECISION.RELATIONSHIP_RESEARCH) {
      advanceRel.push(row);
    } else {
      rejectedEarly.push({
        title,
        reason: row.prequal.rejectionReason || row.prequal.decision,
        qualityClass: row.classification.qualityClass,
      });
    }
  }

  // Prefer Exa discovery candidates with gate2 for dated track
  const datedQueue = [
    ...advanceDated,
    ...ranked.filter(
      (r) =>
        r.prequal.decision !== PREQUAL_DECISION.REJECT_EARLY &&
        /2027-08|2027-09|2027-10/.test(String(r.candidate.startDate || "")) &&
        !/party bus|nightlife|yoga|paint/i.test(r.candidate.title || ""),
    ),
  ].filter((v, i, a) => a.findIndex((x) => x.candidate.title === v.candidate.title) === i);

  const relQueue = [
    ...V71_PLANNER_PRIORITY_QUEUE.map((p) => {
      const hit = advanceRel.find((r) => new RegExp(p.title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(r.candidate.title || ""));
      return (
        hit || {
          candidate: toPrequalShape(plannerSeeds.find((s) => s.title === p.title) || p),
          classification: classifyDiscoveryCandidate(
            toPrequalShape(plannerSeeds.find((s) => s.title === p.title) || { ...p, relationshipTrackOnly: true }),
          ),
          prequal: prequalifyForDeepResearch(
            toPrequalShape({
              ...p,
              relationshipTrackOnly: true,
              recordKind: "VENDOR_OR_VENUE_LISTING",
              city: "Boca Raton",
            }),
          ),
          priority: 2,
          rankReasons: ["v71_planner_seed"],
        }
      );
    }),
    ...advanceRel.filter(
      (r) => !V71_PLANNER_PRIORITY_QUEUE.some((p) => p.title === dedupeRepeatedDisplayName(r.candidate.title)),
    ),
  ].filter((v, i, a) => a.findIndex((x) => x.candidate.title === v.candidate.title) === i);

  persist("WAVE-C-PREQUAL.json", {
    poolMetrics: pool.metrics,
    datedQueue: datedQueue.slice(0, 40).map((r) => ({
      title: r.candidate.title,
      decision: r.prequal.decision,
      qualityClass: r.classification.qualityClass,
    })),
    relQueue: relQueue.slice(0, 20).map((r) => ({
      title: r.candidate.title,
      decision: r.prequal.decision,
    })),
    rejectedEarly: rejectedEarly.slice(0, 80),
  });
  waveLog.push({
    wave: "WAVE_C",
    paid: false,
    datedQueue: datedQueue.length,
    relQueue: relQueue.length,
    rejectedEarly: rejectedEarly.length,
  });

  // ——— WAVE D: Deep Research ———
  async function runDeepOn(row, trackLabel) {
    const rem = remaining(apifyLedger, exaLedger, ceilings);
    const est = FIXED_HIGH;
    // Hold adaptive reserve until Wave E
    if (rem.aggregate - est < adaptiveReserveUsd - 0.01 && trackLabel !== "ADAPTIVE") {
      return { skipped: true, reason: "ADAPTIVE_RESERVE" };
    }
    if (!canAfford(apifyLedger, exaLedger, ceilings, "exa", est)) {
      return { skipped: true, reason: "CEILING" };
    }
    const plan = buildAdaptiveDeepResearchPlan(row);
    const contextBlock = [
      `Hotel: ${hotel.name} (HPC ${hotel.hotelId}).`,
      `Need periods: August–October 2027.`,
      `Track: ${trackLabel}.`,
      `Candidate: ${row.candidate.title}`,
      `Organization: ${row.candidate.organizationName || "unknown"}`,
      `URL: ${row.candidate.sourceUrl || "none"}`,
      `Dates listed: ${row.candidate.startDate || "unconfirmed"}`,
      `Primary research question: ${plan.deepResearchQuestion}`,
      `Demand hypothesis: ${plan.primaryDemandHypothesis}`,
      `Investigate: demand mechanism, named buyer/entry point, lodging/meeting/catering needs,`,
      `programme timing evidence for Aug–Oct 2027, hotel sourcing status, existing commitments,`,
      `Waterstone fit (marina/F&B/meeting), commercial accessibility, practical sales next step.`,
      `Do not invent room nights, wedding dates, or RFPs.`,
      `Do not merely re-summarize the source listing.`,
      `Stop if thesis collapses to nightlife/leisure-only or competitor-locked with no contestable path.`,
      `Public evidence only. No Surfe.`,
    ].join("\n");

    const run = await runExaAgentResearch({
      mode: "deep_research",
      contextBlock,
      effort: "high",
      maxCostDollars: est,
      authorizeExaAgentLive: true,
      forceMockOnly: false,
      pollUntilFinished: true,
      maxPolls: 90,
    });
    const charged = Number(run?.costDollars?.total ?? est);
    await chargeExa(exaLedger, apifyLedger, ceilings, {
      purpose: "stage2_deep_research",
      track: trackLabel,
      candidate: row.candidate.title,
      spentUsd: charged,
      runId: run?.runId,
      status: run?.status,
      ok: run?.ok !== false,
    });
    const qualification = qualifyFromDeep(run?.structured, row.candidate, plan);
    const cib = buildCibPack(row.candidate, run?.structured, qualification);
    return {
      skipped: false,
      candidate: row.candidate,
      plan,
      runId: run?.runId,
      status: run?.status,
      spentUsd: charged,
      structured: run?.structured || null,
      qualification,
      cib,
      textPreview: String(run?.text || "").slice(0, 1500),
    };
  }

  const deepTrackA = [];
  const maxDatedDeep = 12;
  for (const row of datedQueue.slice(0, maxDatedDeep)) {
    const result = await runDeepOn(row, STAGE2_TRACK.A_VERIFIED_NEED_DATE);
    deepTrackA.push(result);
    persist("WAVE-D-TRACK-A-PROGRESS.json", {
      completed: deepTrackA.length,
      spent: exaLedger.spentUsd,
      rem: remaining(apifyLedger, exaLedger, ceilings),
    });
    if (result.skipped && result.reason === "CEILING") break;
  }

  const deepTrackB = [];
  const maxRelDeep = 5;
  for (const row of relQueue.slice(0, maxRelDeep)) {
    const result = await runDeepOn(row, STAGE2_TRACK.B_QUALIFIED_RELATIONSHIP);
    deepTrackB.push(result);
    persist("WAVE-D-TRACK-B-PROGRESS.json", {
      completed: deepTrackB.length,
      spent: exaLedger.spentUsd,
      rem: remaining(apifyLedger, exaLedger, ceilings),
    });
    if (result.skipped && result.reason === "CEILING") break;
  }
  waveLog.push({
    wave: "WAVE_D",
    paid: true,
    trackA: deepTrackA.filter((d) => !d.skipped).length,
    trackB: deepTrackB.filter((d) => !d.skipped).length,
  });

  // ——— WAVE E: adaptive gap resolution ———
  // Release reserve for promising continuations
  adaptiveReserveUsd = Math.min(
    adaptiveReserveUsd,
    remaining(apifyLedger, exaLedger, ceilings).aggregate,
  );
  const continuationCandidates = [...deepTrackA, ...deepTrackB].filter(
    (d) =>
      !d.skipped &&
      d.qualification?.bucket === "RESEARCH_CONTINUATION" &&
      d.structured?.unresolvedResearchTasks?.length,
  );
  const adaptiveRuns = [];
  for (const prior of continuationCandidates.slice(0, 6)) {
    const rem = remaining(apifyLedger, exaLedger, ceilings);
    if (rem.aggregate < FIXED_HIGH) break;
    if (!canAfford(apifyLedger, exaLedger, ceilings, "exa", FIXED_HIGH)) break;
    const tasks = (prior.structured.unresolvedResearchTasks || []).slice(0, 3).join("; ");
    const row = {
      candidate: prior.candidate,
      classification: { qualityClass: "ADAPTIVE_FOLLOWUP", geo: "STRONG_BOCA", needMonth: false },
      prequal: {
        decision: PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH,
        answers: {
          hotelRevenueMechanism: prior.qualification.thesis,
          deepResearchQuestion: tasks || "Resolve remaining commercial blockers from prior ORS",
        },
      },
    };
    const result = await runDeepOn(row, "ADAPTIVE");
    adaptiveRuns.push(result);
    persist("WAVE-E-ADAPTIVE-PROGRESS.json", {
      completed: adaptiveRuns.length,
      spent: exaLedger.spentUsd,
    });
    if (result.skipped) break;
  }
  // If reserve unused and dated queue remains, spend remaining on more Track A
  const moreDated = [];
  for (const row of datedQueue.slice(maxDatedDeep, maxDatedDeep + 8)) {
    const rem = remaining(apifyLedger, exaLedger, ceilings);
    if (rem.aggregate < FIXED_HIGH + 0.05) break;
    adaptiveReserveUsd = 0;
    const result = await runDeepOn(row, STAGE2_TRACK.A_VERIFIED_NEED_DATE);
    moreDated.push(result);
    if (result.skipped) break;
  }
  deepTrackA.push(...moreDated);
  waveLog.push({
    wave: "WAVE_E",
    paid: true,
    adaptive: adaptiveRuns.filter((d) => !d.skipped).length,
    extraDated: moreDated.filter((d) => !d.skipped).length,
  });

  // ——— WAVE F/G: certify + coverage ———
  const allDeep = [...deepTrackA, ...deepTrackB, ...adaptiveRuns].filter((d) => !d.skipped);
  const verifiedNeedDate = [];
  const qualifiedRelationship = [];
  const researchContinuation = [];
  const rejected = [...rejectedEarly];

  for (const d of allDeep) {
    const q = d.qualification;
    if (!q) continue;
    if (q.bucket === "VERIFIED_NEED_DATE") verifiedNeedDate.push(d);
    else if (q.bucket === "QUALIFIED_RELATIONSHIP") qualifiedRelationship.push(d);
    else if (q.bucket === "RESEARCH_CONTINUATION") researchContinuation.push(d);
    else rejected.push({ title: d.candidate?.title, reason: q.reason, from: "ORS" });
  }

  // Deduplicate by title within buckets
  const dedupeDeep = (arr) => {
    const seen = new Set();
    return arr.filter((d) => {
      const k = text(d.candidate?.title).toLowerCase();
      if (!k || seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  };

  const yieldPack = {
    verifiedNeedDateOpportunities: dedupeDeep(verifiedNeedDate).map((d) => d.cib),
    qualifiedRelationshipOpportunities: dedupeDeep(qualifiedRelationship).map((d) => d.cib),
    researchContinuation: dedupeDeep(researchContinuation).map((d) => ({
      title: d.candidate?.title,
      reason: d.qualification?.reason,
      unresolved: d.structured?.unresolvedResearchTasks || [],
      runId: d.runId,
    })),
    rejectedOrInaccessible: rejected.slice(0, 100),
    counts: {
      verifiedNeedDate: dedupeDeep(verifiedNeedDate).length,
      qualifiedRelationship: dedupeDeep(qualifiedRelationship).length,
      researchContinuation: dedupeDeep(researchContinuation).length,
      rejected: rejected.length,
      neverCombineNote: "Do not sum Track A + Track B into one inflated qualified total for sales-ready claims",
    },
  };

  const spentUsd = {
    apify: apifyLedger.spentUsd,
    exa: exaLedger.spentUsd,
    aggregate: Number((apifyLedger.spentUsd + exaLedger.spentUsd).toFixed(4)),
    ceilings,
    withinCeilings:
      apifyLedger.spentUsd <= ceilings.apifyUsd + 1e-9 &&
      exaLedger.spentUsd <= ceilings.exaUsd + 1e-9 &&
      apifyLedger.spentUsd + exaLedger.spentUsd <= ceilings.aggregateUsd + 1e-9,
    campaignTotalWithStage1: Number(
      (STAGE1_ACTUAL_SPEND_USD.aggregate + apifyLedger.spentUsd + exaLedger.spentUsd).toFixed(4),
    ),
    campaignMaximumUsd: OPTION_B_AGGREGATE_MAXIMUM_USD,
  };

  const coverage = {
    workflows: fourteen.workflows.map((w) => ({
      workflowNumber: w.workflowNumber,
      strategyId: w.strategyId,
      pursuitMode: w.pursuitMode,
      apifyRuns: apifyRuns.filter((r) => r.workflow === w.strategyId).length,
      exaDiscoveryRuns: exaDiscoveryRuns.filter((r) => r.workflow === w.strategyId && !r.skipped)
        .length,
    })),
    needMonths: ["2027-08", "2027-09", "2027-10"],
    waveLog,
  };

  const liveReport = {
    ...base,
    ok: true,
    executed: true,
    artifactClass: "LIVE_EXECUTION",
    waveLog,
    inventorySummary,
    apifyRuns,
    exaDiscoveryRuns,
    sharedPoolMetrics: pool.metrics,
    deepResearch: {
      trackA: deepTrackA.map((d) => ({
        title: d.candidate?.title,
        skipped: d.skipped,
        reason: d.reason,
        runId: d.runId,
        status: d.status,
        spentUsd: d.spentUsd,
        bucket: d.qualification?.bucket,
      })),
      trackB: deepTrackB.map((d) => ({
        title: d.candidate?.title,
        skipped: d.skipped,
        reason: d.reason,
        runId: d.runId,
        status: d.status,
        spentUsd: d.spentUsd,
        bucket: d.qualification?.bucket,
      })),
      adaptive: adaptiveRuns.map((d) => ({
        title: d.candidate?.title,
        skipped: d.skipped,
        runId: d.runId,
        bucket: d.qualification?.bucket,
      })),
    },
    yield: yieldPack,
    coverage,
    spentUsd,
    ledgers: { apify: apifyLedger, exa: exaLedger },
    cibCompleteness: {
      qualifiedWithDrawer: [...yieldPack.verifiedNeedDateOpportunities, ...yieldPack.qualifiedRelationshipOpportunities]
        .filter((c) => c.tenSectionPresent).length,
      standard: "Universal Research-to-Drawer V2",
    },
    airtableOpportunityWrites: "DENIED",
    publication: "DENIED",
    surfe: "DENIED",
    firecrawl: "DENIED",
    stop: "STAGE2_COMPLETE_NO_FURTHER_CAMPAIGN",
  };

  persist("00-STAGE2-LIVE-RESULT.json", liveReport);
  persist("01-YIELD-SEPARATED.json", yieldPack);
  persist("02-SPEND-RECONCILIATION.json", spentUsd);
  persist("03-COVERAGE.json", coverage);
  persist("04-DEEP-RESEARCH-ARTIFACTS.json", {
    trackA: deepTrackA.filter((d) => !d.skipped),
    trackB: deepTrackB.filter((d) => !d.skipped),
    adaptive: adaptiveRuns.filter((d) => !d.skipped),
  });

  return liveReport;
}
