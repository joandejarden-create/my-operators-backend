/**
 * Waterstone Need-Date Intelligence V4 — live Exa campaign ($18 aggregate).
 *
 * Exa Discovery + Deep Research only. Firecrawl / Surfe / publication blocked.
 * Month ceilings $6 each (Aug/Sep/Oct 2027). Local staging only.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { loadOpportunities, loadHotelProfile } from "../repository.js";
import { mergeLocalShadowCommercialOpportunities } from "../local-shadow-commercial-portfolio-v1.js";
import {
  isExaAgentLiveAuthorized,
  quoteExaAgentRun,
} from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import {
  executeExaEventsAgentOrsStrategy,
  EXA_EVENTS_RESEARCH_STRATEGIES,
} from "../research/exa-agent/exa-agent-events-ors-strategy-v1.js";
import { mapEventsAgentResultToOpportunity } from "../research/exa-agent/exa-agent-events-to-cib-mapping-v1.js";
import { executeSegment4912ExaAgentOrsStrategy } from "../research/exa-agent/exa-agent-segments-4-9-12-ors-strategy-v1.js";
import { mapSegment4912AgentResultToOpportunity } from "../research/exa-agent/exa-agent-segments-4-9-12-to-cib-mapping-v1.js";
import {
  executeExaAgentOrsStrategy,
  EXA_AGENT_RESEARCH_STRATEGIES,
} from "../research/exa-agent/exa-agent-ors-strategy-v1.js";
import { applyValidatedDeepResearchToScalingBrief } from "../research/exa-agent/exa-agent-to-cib-mapping-v1.js";
import {
  listNeedPeriodsFromAirtable,
  ensureWaterstoneAirtableTestPeriods,
} from "./need-date-airtable-store-v3.js";
import { matchInventoryToNeedPeriods } from "./need-period-inventory-match-v1.js";
import { selectStrategiesForCampaign } from "./need-period-strategy-selection-v1.js";
import {
  WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
  NEED_PERIOD_STATUS,
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
} from "./need-date-demand-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const NEED_DATE_WATERSTONE_LIVE_V4 =
  "gdi-need-date-waterstone-live-campaign-v4";

export const WATERSTONE_V4_APPROVAL_PHRASE =
  "AUTHORIZED: Waterstone Need-Date Intelligence V4 live Exa research, maximum aggregate spending USD $18.";

export const WATERSTONE_V4_AGGREGATE_CEILING_USD = 18;
export const WATERSTONE_V4_MONTH_CEILINGS = Object.freeze({
  "2027-08": 6,
  "2027-09": 6,
  "2027-10": 6,
});

const MONTH_LABELS = Object.freeze({
  "2027-08": "August 2027",
  "2027-09": "September 2027",
  "2027-10": "October 2027",
});

const MONTH_WINDOWS = Object.freeze({
  "2027-08": { start: "2027-08-01", end: "2027-08-31" },
  "2027-09": { start: "2027-09-01", end: "2027-09-30" },
  "2027-10": { start: "2027-10-01", end: "2027-10-31" },
});

/**
 * Priority order per month — not equal spend. First strategies get discovery budget first.
 */
const MONTH_STRATEGY_PRIORITY = Object.freeze({
  "2027-08": [
    "association_events",
    "local_catering_day_meetings",
    "corporate_celebrations",
  ],
  "2027-09": [
    "association_events",
    "professional_event_planners",
    "meetings_incentives",
  ],
  "2027-10": [
    "meetings_incentives",
    "scaling_companies",
    "local_catering_day_meetings",
  ],
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`);
  return fp;
}

function chargeCost(run, quote) {
  if (!run?.ok) return Number(run?.costDollars?.total) || 0;
  const actual = Number(run?.costDollars?.total);
  if (Number.isFinite(actual) && actual > 0) return actual;
  return Number(quote?.maxUsd) || 0;
}

function monthKeyFromPeriod(p) {
  const start = text(p.startDate || "");
  if (/^2027-08/.test(start)) return "2027-08";
  if (/^2027-09/.test(start)) return "2027-09";
  if (/^2027-10/.test(start)) return "2027-10";
  return null;
}

function exclusionKeys(hotelId) {
  const keys = [];
  try {
    const store = loadOpportunities(hotelId);
    for (const o of store.opportunities || []) {
      const k = text(o.organizationName || o.companyName || o.venueName || o.title);
      if (k) keys.push(k);
    }
  } catch {
    /* optional */
  }
  try {
    const merged = mergeLocalShadowCommercialOpportunities(hotelId, []);
    for (const o of merged || []) {
      const k = text(o.organizationName || o.companyName || o.venueName || o.title);
      if (k) keys.push(k);
    }
  } catch {
    /* optional */
  }
  return [...new Set(keys)].slice(0, 80);
}

function loadHotelCtx(hotelId) {
  const profile = loadHotelProfile(hotelId) || {};
  const cfgPath = path.join(
    ROOT,
    `config/group-demand-intelligence/hotels/${hotelId}.json`,
  );
  const cfg = fs.existsSync(cfgPath)
    ? JSON.parse(fs.readFileSync(cfgPath, "utf8"))
    : {};
  return {
    hotelId,
    id: hotelId,
    name:
      profile.identity?.hotelName ||
      profile.name ||
      cfg.displayName ||
      "Waterstone Resort & Marina Boca Raton",
    city: profile.identity?.city || cfg.city || "Boca Raton",
    state: profile.identity?.state || cfg.state || "Florida",
    country: profile.identity?.country || cfg.country || "United States",
    market: "Boca Raton / South Florida",
    address: profile.identity?.address || cfg.address || null,
    profile,
    config: cfg,
  };
}

function needDateContextBlock(monthKey, period, strategyId) {
  const win = MONTH_WINDOWS[monthKey];
  return [
    "",
    "=== NEED-DATE INTELLIGENCE V4 CONSTRAINTS ===",
    `Target need month: ${MONTH_LABELS[monthKey]} (${win.start} to ${win.end}).`,
    `Need period ID: ${period.needPeriodId || "(unknown)"}`,
    `Revenue focus: Group guestrooms, Meetings, Catering/Banquets.`,
    `Strategy: ${strategyId}`,
    "Status: Founder-defined TEST_SCENARIO research benchmark — not a confirmed hotel occupancy shortfall.",
    "Do NOT invent room-night or revenue targets.",
    "Geographic relevance: Boca Raton / Palm Beach County / South Florida — Florida-anywhere is insufficient.",
    "Require source-supported timing aligned to the need month (or honest TIMING_UNCONFIRMED).",
    "Reject hotel marketing pages, venue directories, placeholder titles, thin snippets, unrelated contacts.",
    "Prefer incremental opportunities not already in the hotel's GDI inventory.",
    strategyId === "professional_event_planners"
      ? "Focus: professional event planners, DMCs, and intermediaries who book Boca/South Florida marina-resort product."
      : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function createLedger(outDir) {
  return {
    version: NEED_DATE_WATERSTONE_LIVE_V4,
    hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
    approvalPhrase: WATERSTONE_V4_APPROVAL_PHRASE,
    aggregateCeilingUsd: WATERSTONE_V4_AGGREGATE_CEILING_USD,
    monthCeilingsUsd: { ...WATERSTONE_V4_MONTH_CEILINGS },
    spentUsd: 0,
    spentByMonth: { "2027-08": 0, "2027-09": 0, "2027-10": 0 },
    remainingAggregateUsd: WATERSTONE_V4_AGGREGATE_CEILING_USD,
    remainingByMonth: { ...WATERSTONE_V4_MONTH_CEILINGS },
    providers: {
      exa: "AUTHORIZED",
      firecrawl: "NOT_AUTHORIZED",
      surfe: "NOT_AUTHORIZED",
      paidMapping: "NOT_AUTHORIZED",
      publication: "NOT_AUTHORIZED",
    },
    entries: [],
    halted: false,
    haltReason: null,
    outDir,
    startedAt: new Date().toISOString(),
  };
}

function canAfford(ledger, monthKey, nextCost) {
  const cost = Number(nextCost) || 0;
  if (!(cost > 0)) return false;
  if (ledger.halted) return false;
  if (ledger.spentUsd + cost > ledger.aggregateCeilingUsd + 1e-9) return false;
  if (ledger.spentByMonth[monthKey] + cost > ledger.monthCeilingsUsd[monthKey] + 1e-9) {
    return false;
  }
  return true;
}

function recordSpend(ledger, entry) {
  const cost = Number(entry.costUsd) || 0;
  ledger.spentUsd = Number((ledger.spentUsd + cost).toFixed(6));
  ledger.spentByMonth[entry.monthKey] = Number(
    (ledger.spentByMonth[entry.monthKey] + cost).toFixed(6),
  );
  ledger.remainingAggregateUsd = Number(
    (ledger.aggregateCeilingUsd - ledger.spentUsd).toFixed(6),
  );
  for (const mk of Object.keys(ledger.monthCeilingsUsd)) {
    ledger.remainingByMonth[mk] = Number(
      (ledger.monthCeilingsUsd[mk] - ledger.spentByMonth[mk]).toFixed(6),
    );
  }
  ledger.entries.push({ ...entry, recordedAt: new Date().toISOString() });
}

/**
 * Permanent negative gates before staging a candidate as qualified.
 * Reads Events + Segment deep-research field shapes (cibMappingHints, organizerContactCandidates).
 */
export function applyNeedDateQualificationGates(candidate = {}, deep = {}, monthKey) {
  const rejects = [];
  const hints = deep.cibMappingHints || {};
  const overview = deep.eventsOpportunityOverviewFields || {};
  const title = text(
    deep.programmeIdentity ||
      deep.eventIdentity ||
      overview.eventDisplayName ||
      candidate.programmeName ||
      candidate.eventName ||
      candidate.eventIdentity ||
      candidate.title ||
      candidate.organizationName,
  );
  const what = text(
    hints.whatIsHappening ||
      deep.whatIsHappening ||
      deep.demandMechanismSummary ||
      deep.findings ||
      candidate.demandMechanismSummary ||
      candidate.whatIsHappening ||
      candidate.evidenceOfHospitalityDemand,
  );
  const thesis = text(
    hints.whyThisHotel ||
      hints.potentialBusiness ||
      deep.hotelDemandThesis ||
      candidate.hotelDemandThesis ||
      deep.whyThisHotel,
  );
  const sources = Array.isArray(deep.supportingSources)
    ? deep.supportingSources
    : Array.isArray(candidate.supportingSources)
      ? candidate.supportingSources
      : [];
  const contacts = [
    ...(Array.isArray(deep.organizerContactCandidates)
      ? deep.organizerContactCandidates
      : []),
    ...(Array.isArray(deep.namedContacts) ? deep.namedContacts : []),
    ...(Array.isArray(candidate.namedContacts) ? candidate.namedContacts : []),
  ];

  if (!title || /untitled|tbd|placeholder|coming soon/i.test(title)) {
    rejects.push("PLACEHOLDER_OR_EMPTY_TITLE");
  }

  // Reject only when the *identity* is a hotel marketing / venue directory page —
  // not when supportingSources cite hilton.com/marriott.com as venue evidence.
  const identityBlob = `${title} ${text(candidate.organizationName)} ${text(candidate.programmeName)}`.toLowerCase();
  if (
    /venues?\s+directory|eventective|cvent\s+supplier|weddingwire|theknot/i.test(
      identityBlob,
    ) ||
    (/^https?:\/\/(www\.)?(marriott|hilton|hyatt|ihg)\.com\//i.test(
      text(candidate.sourceUrl || candidate.url || ""),
    ) &&
      !text(candidate.organizationName || candidate.organizingEntity))
  ) {
    rejects.push("HOTEL_MARKETING_OR_VENUE_DIRECTORY");
  }

  if (what.length < 80 && title.length < 80) {
    rejects.push("THIN_WHAT_IS_HAPPENING");
  } else if (what.length < 40 && title.length >= 80) {
    // Title carries substantive identity; still require a short demand note.
    if (thesis.length < 40) rejects.push("THIN_WHAT_IS_HAPPENING");
  }
  if (thesis.length < 40) rejects.push("MISSING_HOTEL_DEMAND_THESIS");
  if (!sources.length) rejects.push("NO_SUPPORTING_SOURCES");
  if (!contacts.some((c) => text(c?.name))) {
    // Events deep often puts contacts only in cibMappingHints.whoToContact prose.
    const who = text(hints.whoToContact);
    if (!/\b[A-Z][a-z]+\s+[A-Z][a-z]+\b/.test(who)) {
      rejects.push("NO_NAMED_BUYER_OR_REFERRAL");
    }
  }

  // Prefer date-bearing narrative fields; avoid full JSON (assessment timestamps false-hit months).
  const timingPrimary = [
    overview.dateDisplay,
    deep.verifiedDates,
    deep.verifiedStartDate,
    deep.eventStartDate,
    candidate.verifiedDates,
    candidate.eventDates,
    candidate.eventStartDate,
    hints.whatIsHappening,
    hints.potentialBusiness,
    title,
  ]
    .map((x) => text(x))
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  const timingBlob = timingPrimary;
  // Month + 2027 must co-occur (avoids "October 22, 2026 prospectus" false hits).
  const monthIn2027 =
    monthKey === "2027-08"
      ? /(?:\baug(?:ust)?\b[^.]{0,48}2027|2027[^.]{0,48}\baug(?:ust)?\b|2027-08)/i
      : monthKey === "2027-09"
        ? /(?:\bsept?(?:ember)?\b[^.]{0,48}2027|2027[^.]{0,48}\bsept?(?:ember)?\b|2027-09)/i
        : /(?:\boct(?:ober)?\b[^.]{0,48}2027|2027[^.]{0,48}\boct(?:ober)?\b|2027-10)/i;
  let timingClass = "TIMING_UNCONFIRMED";
  if (monthIn2027.test(timingBlob)) {
    timingClass = /confirmed|verified|scheduled|announced/i.test(timingBlob)
      ? "CONFIRMED_PERIOD_OVERLAP"
      : "SUPPORTED_PROBABLE_OVERLAP";
  }

  const hasExplicitOtherMonth2027 =
    /2027/.test(timingBlob) &&
    !monthIn2027.test(timingBlob) &&
    /(?:\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|nov(?:ember)?|dec(?:ember)?)\b[^.]{0,48}2027|2027-0[1-7]|2027-1[12])/i.test(
      timingBlob,
    );
  const isPriorYearOnly =
    /\b2026\b/.test(timingBlob) &&
    !/\b2027\b/.test(timingBlob);

  if (hasExplicitOtherMonth2027 || isPriorYearOnly) {
    rejects.push("EVENTS_OUTSIDE_NEED_MONTH");
    timingClass = "OUTSIDE_PERIOD";
  } else if (!monthIn2027.test(timingBlob)) {
    rejects.push("NEED_DATE_TIMING_UNCONFIRMED");
    timingClass = "TIMING_UNCONFIRMED";
  }

  return {
    ok: rejects.length === 0,
    rejects,
    timingClass,
    title,
  };
}

async function runDiscovery({
  strategyId,
  hotel,
  period,
  monthKey,
  exclusions,
  authorizeLive,
}) {
  const ctxExtra = needDateContextBlock(monthKey, period, strategyId);
  const hotelEnriched = {
    ...hotel,
    needDateResearchNotes: ctxExtra,
  };

  if (strategyId === "association_events" || strategyId === "professional_event_planners") {
    return executeExaEventsAgentOrsStrategy({
      strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DISCOVERY,
      hotel: {
        ...hotelEnriched,
        researchFocusOverride:
          strategyId === "professional_event_planners"
            ? "professional_event_planners_dmc_intermediaries"
            : "association_conferences_events",
      },
      exclusions,
      authorizeExaAgentLive: authorizeLive,
      effort: "medium",
      limit: 8,
    });
  }

  if (strategyId === "scaling_companies") {
    return executeExaAgentOrsStrategy({
      strategyId: EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DISCOVERY,
      hotel: hotelEnriched,
      exclusions,
      authorizeExaAgentLive: authorizeLive,
      effort: "medium",
    });
  }

  const segmentMap = {
    local_catering_day_meetings: "local_catering_day_meetings",
    corporate_celebrations: "corporate_celebrations_private_events",
    meetings_incentives: "corporate_meetings_incentives",
  };
  const segmentId = segmentMap[strategyId];
  if (!segmentId) {
    return {
      run: { ok: false, status: "unknown_strategy", error: strategyId },
      quote: quoteExaAgentRun({ effort: "medium" }),
    };
  }
  return executeSegment4912ExaAgentOrsStrategy({
    segmentId,
    mode: "discovery",
    hotel: hotelEnriched,
    exclusions,
    authorizeExaAgentLive: authorizeLive,
    effort: "medium",
    limit: 8,
  });
}

async function runDeep({
  strategyId,
  hotel,
  period,
  monthKey,
  candidate,
  authorizeLive,
}) {
  const hotelEnriched = {
    ...hotel,
    needDateResearchNotes: needDateContextBlock(monthKey, period, strategyId),
  };

  if (strategyId === "association_events" || strategyId === "professional_event_planners") {
    return executeExaEventsAgentOrsStrategy({
      strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DEEP_RESEARCH,
      hotel: hotelEnriched,
      candidate,
      authorizeExaAgentLive: authorizeLive,
      effort: "high",
    });
  }

  if (strategyId === "scaling_companies") {
    return executeExaAgentOrsStrategy({
      strategyId: EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DEEP_RESEARCH,
      hotel: hotelEnriched,
      opportunity: candidate,
      authorizeExaAgentLive: authorizeLive,
      effort: "high",
    });
  }

  const segmentMap = {
    local_catering_day_meetings: "local_catering_day_meetings",
    corporate_celebrations: "corporate_celebrations_private_events",
    meetings_incentives: "corporate_meetings_incentives",
  };
  const segmentId = segmentMap[strategyId];
  return executeSegment4912ExaAgentOrsStrategy({
    segmentId,
    mode: "deep_research",
    hotel: hotelEnriched,
    candidate,
    authorizeExaAgentLive: authorizeLive,
    effort: "high",
  });
}

function extractCandidates(strategyId, discovery) {
  const structured = discovery?.run?.structured || {};
  if (strategyId === "scaling_companies") {
    return (discovery.discoveryCandidates || structured.candidates || []).map(
      (c) => ({
        ...c,
        organizationName: text(c.organizationName || c.companyName),
        programmeName: text(c.programmeName || c.title),
        key: text(c.organizationName || c.companyName).toLowerCase(),
      }),
    ).filter((c) => c.key);
  }
  const list = Array.isArray(structured.candidates)
    ? structured.candidates
    : Array.isArray(structured.events)
      ? structured.events
      : [];
  return list
    .map((c) => ({
      ...c,
      organizationName: text(
        c.organizationName || c.organizingEntity || c.companyName,
      ),
      programmeName: text(
        c.programmeName || c.eventName || c.eventIdentity || c.title,
      ),
      key: text(
        c.organizationName ||
          c.organizingEntity ||
          c.companyName ||
          c.eventIdentity ||
          c.title,
      ).toLowerCase(),
    }))
    .filter((c) => c.key);
}

function mapToStagedOpportunity({
  strategyId,
  hotel,
  candidate,
  deep,
  monthKey,
  period,
  gate,
}) {
  const hotelId = hotel.hotelId;
  let opp = null;
  try {
    if (strategyId === "association_events" || strategyId === "professional_event_planners") {
      opp = mapEventsAgentResultToOpportunity({
        hotel,
        candidate,
        deepResearch: deep?.run?.structured || {},
      });
    } else if (strategyId === "scaling_companies") {
      const mapped = applyValidatedDeepResearchToScalingBrief(
        candidate,
        deep?.run?.structured || {},
        hotel,
      );
      opp = mapped?.opportunity || mapped;
    } else {
      const segmentMap = {
        local_catering_day_meetings: "local_catering_day_meetings",
        corporate_celebrations: "corporate_celebrations_private_events",
        meetings_incentives: "corporate_meetings_incentives",
      };
      opp = mapSegment4912AgentResultToOpportunity({
        hotel,
        segmentId: segmentMap[strategyId],
        candidate,
        deepResearch: deep?.run?.structured || {},
      });
    }
  } catch (err) {
    opp = {
      title: gate.title,
      organizationName: candidate.organizationName,
      mappingError: String(err?.message || err),
    };
  }

  const idSeed = `${hotelId}|needdate-v4|${strategyId}|${candidate.key}|${monthKey}`;
  const id = `gdi_ndv4_${createHash("sha256").update(idSeed).digest("hex").slice(0, 12)}`;

  return {
    ...opp,
    id,
    hotelId,
    needDateCampaign: NEED_DATE_WATERSTONE_LIVE_V4,
    needPeriodIds: [period.needPeriodId].filter(Boolean),
    needPeriodMonths: [MONTH_LABELS[monthKey]],
    needDateTimingClass: gate.timingClass,
    strategyId,
    localShadowOnly: true,
    customerVisible: false,
    publicationStatus: "STAGED_AWAITING_FOUNDER_REVIEW",
    doesNotAffectProduction: true,
    airtablePublication: "BLOCKED",
    stagedAt: new Date().toISOString(),
  };
}

export async function runWaterstoneNeedDateLivePreflight(input = {}) {
  const hotelId = text(input.hotelId) || WATERSTONE_NEED_DATE_PILOT_HOTEL_ID;
  const gates = [];
  const push = (gate, ok, detail = null) => gates.push({ gate, outcome: ok ? "PASS" : "FAIL", detail });

  push("HOTEL_IDENTITY", hotelId === WATERSTONE_NEED_DATE_PILOT_HOTEL_ID, { hotelId });
  const hotel = loadHotelCtx(hotelId);
  push("HOTEL_CONTEXT_LOADED", Boolean(hotel.name), { name: hotel.name });

  const ensured = await ensureWaterstoneAirtableTestPeriods();
  const listed = await listNeedPeriodsFromAirtable(hotelId);
  const wantedIds = new Set([
    "ndp_airtable_waterstone_aug_2027",
    "ndp_airtable_waterstone_sep_2027",
    "ndp_airtable_waterstone_oct_2027",
  ]);
  const periods = (listed.periods || []).filter((p) =>
    wantedIds.has(p.needPeriodId),
  );
  const byMonth = {};
  for (const p of periods) {
    const mk = monthKeyFromPeriod(p);
    if (mk) byMonth[mk] = p;
  }
  push(
    "AIRTABLE_NEED_PERIODS",
    Boolean(
      byMonth["2027-08"] &&
        byMonth["2027-09"] &&
        byMonth["2027-10"] &&
        periods.every(
          (p) =>
            p.status === NEED_PERIOD_STATUS.TEST_SCENARIO ||
            String(p.status).includes("TEST"),
        ),
    ),
    {
      count: periods.length,
      months: Object.keys(byMonth),
      statuses: Object.fromEntries(
        periods.map((p) => [p.needPeriodId, p.status]),
      ),
      ids: Object.fromEntries(
        Object.entries(byMonth).map(([k, p]) => [k, p.needPeriodId]),
      ),
      ensured,
    },
  );

  const budgetOk =
    Number(input.budgetUSD) > 0 &&
    Number(input.budgetUSD) <= WATERSTONE_V4_AGGREGATE_CEILING_USD &&
    input.authorizePaidResearch === true;
  push("EXA_BUDGET_AUTHORIZATION", budgetOk, {
    budgetUSD: input.budgetUSD,
    ceiling: WATERSTONE_V4_AGGREGATE_CEILING_USD,
  });

  const approvalOk =
    text(input.approvalPhrase) === WATERSTONE_V4_APPROVAL_PHRASE ||
    input.founderAuthorizedV4 === true;
  push("FOUNDER_APPROVAL_PHRASE", approvalOk);

  process.env.GDI_EXA_AGENT_LIVE = process.env.GDI_EXA_AGENT_LIVE || "1";
  const liveAuth = isExaAgentLiveAuthorized({ authorizeExaAgentLive: true });
  push("EXA_LIVE_ENV", liveAuth && Boolean(process.env.EXA_API_KEY), {
    GDI_EXA_AGENT_LIVE: process.env.GDI_EXA_AGENT_LIVE,
    hasExaKey: Boolean(process.env.EXA_API_KEY),
  });

  push("FIRECRAWL_DISABLED", true);
  push("SURFE_DISABLED", true);
  push("PUBLICATION_BLOCKED", input.authorizePublication !== true);

  const discQ = quoteExaAgentRun({ effort: "medium" });
  const deepQ = quoteExaAgentRun({ effort: "high" });
  push(
    "COST_BOUNDED",
    discQ.bounded === true && deepQ.bounded === true,
    { discoveryMaxUsd: discQ.maxUsd, deepMaxUsd: deepQ.maxUsd },
  );

  const inventory = loadOpportunities(hotelId);
  const invCount = (inventory?.opportunities || []).length;
  push("INVENTORY_LOADED", invCount > 0, { inventoryCount: invCount });

  const ok = gates.every((g) => g.outcome === "PASS");
  return {
    ok,
    version: NEED_DATE_WATERSTONE_LIVE_V4,
    hotel,
    periods: Object.values(byMonth),
    periodsByMonth: byMonth,
    inventoryCount: invCount,
    gates,
    quotes: { discovery: discQ, deep: deepQ },
  };
}

/**
 * Execute the authorized Waterstone V4 live Exa campaign.
 */
export async function runWaterstoneNeedDateLiveCampaignV4(input = {}) {
  const outDir =
    input.outDir ||
    path.join(
      ROOT,
      "reports/gdi-need-date-demand-intelligence-v4/waterstone-live-2027",
      `run-${new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14)}`,
    );
  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(path.join(outDir, "artifacts"), { recursive: true });
  fs.mkdirSync(path.join(outDir, "staging"), { recursive: true });

  const budgetUSD = Math.min(
    Number(input.budgetUSD) || 0,
    WATERSTONE_V4_AGGREGATE_CEILING_USD,
  );
  const preflight = await runWaterstoneNeedDateLivePreflight({
    ...input,
    budgetUSD,
  });
  writeJson(path.join(outDir, "04-PREFLIGHT.json"), preflight);

  if (!preflight.ok) {
    return {
      ok: false,
      stoppedAt: "PREFLIGHT",
      preflight,
      outDir,
      spentUsd: 0,
    };
  }

  if (input.executeLive !== true) {
    return {
      ok: true,
      dryRun: true,
      message: "Preflight PASS. Pass executeLive:true to spend Exa.",
      preflight,
      outDir,
      spentUsd: 0,
    };
  }

  const hotel = preflight.hotel;
  const ledger = createLedger(outDir);
  ledger.aggregateCeilingUsd = budgetUSD;
  ledger.remainingAggregateUsd = budgetUSD;

  const inventory = loadOpportunities(hotel.hotelId);
  let opps = [...(inventory?.opportunities || [])];
  try {
    opps = mergeLocalShadowCommercialOpportunities(hotel.hotelId, opps);
  } catch {
    /* optional */
  }

  const periods = preflight.periods;
  const inventoryMatch = matchInventoryToNeedPeriods(periods, opps);
  writeJson(path.join(outDir, "05-INVENTORY-MATCH.json"), {
    inventoryCount: opps.length,
    match: inventoryMatch,
  });

  const strategyPlan = selectStrategiesForCampaign({
    needPeriods: periods,
    hotel,
    inventoryMatch,
    maxStrategies: 6,
  });
  writeJson(path.join(outDir, "06-STRATEGY-SELECTION.json"), strategyPlan);

  const exclusions = exclusionKeys(hotel.hotelId);
  const discQuote = quoteExaAgentRun({ effort: "medium" });
  const deepQuote = quoteExaAgentRun({ effort: "high" });

  const staged = [];
  const rejected = [];
  const byMonthResults = {};
  const seenKeys = new Set(exclusions.map((k) => k.toLowerCase()));

  for (const monthKey of ["2027-08", "2027-09", "2027-10"]) {
    const period = preflight.periodsByMonth[monthKey];
    if (!period) continue;
    const monthResult = {
      monthKey,
      label: MONTH_LABELS[monthKey],
      needPeriodId: period.needPeriodId,
      discoveries: [],
      deepRuns: [],
      stagedIds: [],
      rejected: [],
      spentUsd: 0,
    };
    byMonthResults[monthKey] = monthResult;

    const strategies = MONTH_STRATEGY_PRIORITY[monthKey] || [];
    const monthCandidates = [];

    for (const strategyId of strategies) {
      const nextCost = Number(discQuote.maxUsd) || 0.1;
      if (!canAfford(ledger, monthKey, nextCost)) {
        ledger.halted = ledger.halted || ledger.spentUsd >= budgetUSD - 1e-9;
        break;
      }

      let discovery;
      try {
        discovery = await runDiscovery({
          strategyId,
          hotel,
          period,
          monthKey,
          exclusions: [...exclusions, ...[...seenKeys]],
          authorizeLive: true,
        });
      } catch (err) {
        discovery = {
          run: {
            ok: false,
            status: "execute_threw",
            error: String(err?.message || err),
          },
          quote: discQuote,
        };
      }

      const cost = chargeCost(discovery.run, discovery.quote || discQuote);
      if (!canAfford(ledger, monthKey, cost) && cost > 0) {
        ledger.halted = true;
        ledger.haltReason = "COST_WOULD_EXCEED_AFTER_PROVIDER_RETURN";
        break;
      }
      recordSpend(ledger, {
        monthKey,
        strategyId,
        mode: "discovery",
        costUsd: cost,
        runId: discovery.run?.runId || discovery.run?.id || null,
        ok: discovery.run?.ok === true,
        status: discovery.run?.status || null,
        error: discovery.run?.error || null,
        objective: `${MONTH_LABELS[monthKey]} ${strategyId} discovery`,
      });
      monthResult.spentUsd = ledger.spentByMonth[monthKey];
      monthResult.discoveries.push({
        strategyId,
        ok: discovery.run?.ok === true,
        costUsd: cost,
        runId: discovery.run?.runId || null,
      });
      writeJson(
        path.join(outDir, "artifacts", `${monthKey}-${strategyId}-discovery.json`),
        discovery,
      );

      if (discovery.run?.ok) {
        for (const c of extractCandidates(strategyId, discovery)) {
          if (seenKeys.has(c.key)) continue;
          monthCandidates.push({ ...c, strategyId });
        }
      }

      // Prefer quality: one discovery per strategy max for this month unless budget remains after deeps.
    }

    // Deep research: top candidates across strategies for this month
    monthCandidates.sort((a, b) => {
      const ra = Number(a.relevanceScore || a.score || 0);
      const rb = Number(b.relevanceScore || b.score || 0);
      return rb - ra;
    });

    const deepCap = 4;
    let deepDone = 0;
    for (const candidate of monthCandidates) {
      if (deepDone >= deepCap) break;
      const nextCost = Number(deepQuote.maxUsd) || 0.5;
      if (!canAfford(ledger, monthKey, nextCost)) break;

      let deep;
      try {
        deep = await runDeep({
          strategyId: candidate.strategyId,
          hotel,
          period,
          monthKey,
          candidate,
          authorizeLive: true,
        });
      } catch (err) {
        deep = {
          run: {
            ok: false,
            status: "execute_threw",
            error: String(err?.message || err),
          },
          quote: deepQuote,
        };
      }

      const cost = chargeCost(deep.run, deep.quote || deepQuote);
      if (!canAfford(ledger, monthKey, cost) && cost > 0) {
        ledger.halted = true;
        ledger.haltReason = "DEEP_COST_EXCEED";
        break;
      }
      recordSpend(ledger, {
        monthKey,
        strategyId: candidate.strategyId,
        mode: "deep_research",
        costUsd: cost,
        runId: deep.run?.runId || deep.run?.id || null,
        ok: deep.run?.ok === true,
        candidateKey: candidate.key,
        candidateName: candidate.organizationName || candidate.programmeName,
        status: deep.run?.status || null,
        error: deep.run?.error || null,
        objective: `${MONTH_LABELS[monthKey]} deep ${candidate.key}`,
      });
      monthResult.spentUsd = ledger.spentByMonth[monthKey];
      deepDone += 1;
      writeJson(
        path.join(
          outDir,
          "artifacts",
          `${monthKey}-${candidate.strategyId}-deep-${deepDone}.json`,
        ),
        deep,
      );

      if (!deep.run?.ok) {
        rejected.push({
          monthKey,
          reason: "DEEP_RUN_FAILED",
          candidate: candidate.key,
          error: deep.run?.error,
        });
        monthResult.rejected.push({
          key: candidate.key,
          reason: "DEEP_RUN_FAILED",
        });
        continue;
      }

      const gate = applyNeedDateQualificationGates(
        candidate,
        deep.run.structured || {},
        monthKey,
      );
      if (!gate.ok) {
        rejected.push({
          monthKey,
          reason: gate.rejects,
          candidate: candidate.key,
          title: gate.title,
        });
        monthResult.rejected.push({
          key: candidate.key,
          reason: gate.rejects,
          title: gate.title,
        });
        continue;
      }

      const opp = mapToStagedOpportunity({
        strategyId: candidate.strategyId,
        hotel,
        candidate,
        deep,
        monthKey,
        period,
        gate,
      });

      // Cross-month dedupe: same org → attach month, do not create second canonical
      const existing = staged.find(
        (s) => text(s.organizationName).toLowerCase() === candidate.key,
      );
      if (existing) {
        existing.needPeriodIds = [
          ...new Set([
            ...(existing.needPeriodIds || []),
            period.needPeriodId,
          ]),
        ];
        existing.needPeriodMonths = [
          ...new Set([
            ...(existing.needPeriodMonths || []),
            MONTH_LABELS[monthKey],
          ]),
        ];
        monthResult.stagedIds.push(existing.id);
      } else {
        staged.push(opp);
        seenKeys.add(candidate.key);
        monthResult.stagedIds.push(opp.id);
      }
    }
  }

  ledger.completedAt = new Date().toISOString();
  writeJson(path.join(outDir, "07-EXA-COST-LEDGER.json"), ledger);
  writeJson(path.join(outDir, "staging", "staged-opportunities.json"), {
    version: NEED_DATE_WATERSTONE_LIVE_V4,
    publicationStatus: "STAGED_AWAITING_FOUNDER_REVIEW",
    customerVisible: false,
    opportunities: staged,
  });
  writeJson(path.join(outDir, "staging", "rejected-candidates.json"), rejected);
  writeJson(path.join(outDir, "08-10-MONTH-RESULTS.json"), byMonthResults);

  const shortlist = staged.map((o) => ({
    id: o.id,
    name: o.title || o.displayTitle || o.organizationName,
    months: o.needPeriodMonths,
    strategyId: o.strategyId,
    timing: o.needDateTimingClass,
    demandThesis: text(o.hotelOpportunityThesis || o.whatIsHappening).slice(
      0,
      280,
    ),
    buyer: o.contactName || o.primaryContact?.name || null,
    nextStep: o.recommendedNextStep || o.recommendedAction || null,
    uncertainties: (o.uncertainties || []).slice(0, 3),
    sources: (o.sources || o.evidenceUrls || []).slice(0, 5),
  }));

  writeJson(path.join(outDir, "14-BEST-INCREMENTAL-OPPORTUNITIES.json"), {
    count: shortlist.length,
    note: "Quality over count — only gate-passing staged opportunities.",
    shortlist,
  });

  const summary = {
    ok: true,
    version: NEED_DATE_WATERSTONE_LIVE_V4,
    parentVersion: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    outDir,
    inventoryCount: opps.length,
    existingTimingMatches: inventoryMatch.existingMatchCount || 0,
    strategiesSelected: strategyPlan.strategyIds,
    spentUsd: ledger.spentUsd,
    remainingUsd: ledger.remainingAggregateUsd,
    spentByMonth: ledger.spentByMonth,
    stagedCount: staged.length,
    rejectedCount: rejected.length,
    publication: "BLOCKED",
    firecrawl: "NOT_USED",
    surfe: "NOT_USED",
  };
  writeJson(path.join(outDir, "00-SUMMARY.json"), summary);
  return { ...summary, ledger, staged, rejected, byMonthResults, preflight };
}
