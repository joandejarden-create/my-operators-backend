/**
 * Hotel Need Dates Management service — orchestrates portal CRUD + #15 match preview.
 * Never auto-authorizes paid Exa or Airtable mutation.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
  NEED_PERIOD_CHANGE_TYPE,
  NEED_PERIOD_STATUS,
  NEED_REVENUE_TYPE,
  NEED_REVENUE_TYPE_LABEL,
  NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
  AIRTABLE_MUTATION_BLOCKED,
  PAID_PROVIDER_BLOCKED,
  WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
} from "./need-date-demand-contract-v1.js";
import { monthBoundsUtc } from "./need-period-normalize-v1.js";
import {
  ensureVersionedNeedPeriod,
  isNeedPeriodCommerciallyOpen,
} from "./need-period-lifecycle-v1.js";
import {
  summarizeMonthVisual,
  countActiveNeedMonths,
  customerFriendlyChangeType,
  NEED_MONTH_VISUAL_STATUS_LABEL,
  NEED_CHANGE_INDICATOR_LABEL,
} from "./need-month-visual-status-v1.js";
import { buildAdaptiveNeedDateResearchPlan } from "./need-date-adaptive-research-v1.js";
import {
  listNeedPeriodsForHotel,
  saveNeedPeriodChange,
  getNeedPeriodById,
  NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
  clearIsolatedNeedPeriodStore,
} from "./need-period-hotel-store-v1.js";
import { matchInventoryToNeedPeriods } from "./need-period-inventory-match-v1.js";
import { selectStrategiesForCampaign } from "./need-period-strategy-selection-v1.js";
import { buildNeedDateMatchDrawerComponent } from "./need-period-drawer-component-v1.js";
import { proposeNeedDateAirtableSchema } from "./need-period-airtable-schema-proposal-v1.js";
import { getCommercialPrioritiesEnums } from "./hotel-commercial-priorities-contract-v1.js";
import {
  isNeedDateAirtableLiveEnabled,
  listNeedPeriodsFromAirtable,
  getNeedPeriodFromAirtable,
  listHotelNeedPeriodChangeHistoryFromAirtable,
  saveNeedPeriodChangeToAirtable,
  upsertNeedPeriodOpportunityMatch,
} from "./need-date-airtable-store-v3.js";
import {
  loadOpportunities,
  loadHotelProfile,
} from "../repository.js";
import { mergeLocalShadowCommercialOpportunities } from "../local-shadow-commercial-portfolio-v1.js";

export const NEED_DATES_RESEARCH_STATE = Object.freeze({
  NOT_RESEARCHED: "NOT_RESEARCHED",
  MATCHING_EXISTING: "MATCHING_EXISTING",
  RESEARCH_RECOMMENDED: "RESEARCH_RECOMMENDED",
  AWAITING_AUTHORIZATION: "AWAITING_AUTHORIZATION",
  RESEARCHING: "RESEARCHING",
  RESULTS_AVAILABLE: "RESULTS_AVAILABLE",
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED",
  RESEARCH_FAILED: "RESEARCH_FAILED",
});

export const NEED_DATES_RESEARCH_STATE_LABEL = Object.freeze({
  [NEED_DATES_RESEARCH_STATE.NOT_RESEARCHED]: "Not Researched",
  [NEED_DATES_RESEARCH_STATE.MATCHING_EXISTING]: "Matching Existing Opportunities",
  [NEED_DATES_RESEARCH_STATE.RESEARCH_RECOMMENDED]: "Research Recommended",
  [NEED_DATES_RESEARCH_STATE.AWAITING_AUTHORIZATION]: "Awaiting Authorization",
  [NEED_DATES_RESEARCH_STATE.RESEARCHING]: "Researching",
  [NEED_DATES_RESEARCH_STATE.RESULTS_AVAILABLE]: "Results Available",
  [NEED_DATES_RESEARCH_STATE.RESEARCH_BLOCKED]: "Research Blocked / Failed",
  [NEED_DATES_RESEARCH_STATE.RESEARCH_FAILED]: "Research Blocked / Failed",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadInventory(hotelId) {
  const store = loadOpportunities(hotelId);
  const baseline = [...(store?.opportunities || [])];
  const merged = mergeLocalShadowCommercialOpportunities(hotelId, baseline, {
    env: process.env,
  });
  return merged;
}

function hotelContext(hotelId) {
  const profile = loadHotelProfile(hotelId) || {};
  return {
    hotelId,
    name:
      profile.identity?.hotelName ||
      profile.name ||
      profile.displayName ||
      hotelId,
    city: profile.identity?.city || profile.city || null,
  };
}

function classifyMatchBucket(m) {
  const cat = String(m.association?.outputCategory || "").toUpperCase();
  if (cat.includes("NEW")) return "NEW_OPPORTUNITY";
  if (cat.includes("ENRICH")) return "EXISTING_ENRICHED";
  if (cat.includes("CONTINUATION") || m.researchContinuation) {
    return "RESEARCH_CONTINUATION";
  }
  return "EXISTING_MATCH";
}

function attachMatchPresentation(matches, periods) {
  const byPeriod = {};
  const seenOpp = new Set();
  for (const p of periods) {
    byPeriod[p.needPeriodId] = {
      needPeriodId: p.needPeriodId,
      periodLabel: p.periodLabel,
      existingMatch: [],
      existingEnriched: [],
      newOpportunity: [],
      researchContinuation: [],
    };
  }
  for (const m of matches || []) {
    const bucket = classifyMatchBucket(m);
    const row = {
      opportunityId: m.opportunityId,
      title: m.title,
      timingOverlapStatus: m.timing?.overlapStatus || null,
      confirmedRevenueTiming: Boolean(m.timing?.confirmedRevenueTiming),
      demandStartDate: m.timing?.demandStartDate || null,
      demandEndDate: m.timing?.demandEndDate || null,
      actionable: Boolean(m.actionable),
      researchGapHints: m.researchGapHints || [],
      bucket,
      firstSeenAsNew: !seenOpp.has(m.opportunityId) && bucket === "NEW_OPPORTUNITY",
    };
    seenOpp.add(m.opportunityId);
    const slot = byPeriod[m.needPeriodId];
    if (!slot) continue;
    if (bucket === "EXISTING_ENRICHED") slot.existingEnriched.push(row);
    else if (bucket === "NEW_OPPORTUNITY") {
      // Count as newly discovered only once across months
      if (row.firstSeenAsNew) slot.newOpportunity.push(row);
      else slot.existingMatch.push({ ...row, bucket: "EXISTING_MATCH" });
    } else if (bucket === "RESEARCH_CONTINUATION") {
      slot.researchContinuation.push(row);
    } else slot.existingMatch.push(row);
  }
  return byPeriod;
}

function deriveResearchState(period, periodMatches) {
  if (period.researchState === NEED_DATES_RESEARCH_STATE.RESEARCHING) {
    // Only show Researching when an actual authorized execution exists
    if (period.authorizedResearchRunId) {
      return NEED_DATES_RESEARCH_STATE.RESEARCHING;
    }
    return NEED_DATES_RESEARCH_STATE.AWAITING_AUTHORIZATION;
  }
  if (period.authorizedResearchRunId && period.researchState === NEED_DATES_RESEARCH_STATE.RESULTS_AVAILABLE) {
    return NEED_DATES_RESEARCH_STATE.RESULTS_AVAILABLE;
  }
  const confirmed = (periodMatches?.existingMatch || []).filter(
    (m) => m.timingOverlapStatus === "CONFIRMED_PERIOD_OVERLAP",
  ).length;
  const open = isNeedPeriodCommerciallyOpen(period);
  if (!open) {
    return period.researchState || NEED_DATES_RESEARCH_STATE.RESULTS_AVAILABLE;
  }
  if (confirmed > 0 && (periodMatches?.newOpportunity || []).length === 0) {
    return NEED_DATES_RESEARCH_STATE.MATCHING_EXISTING;
  }
  if (open && confirmed === 0) {
    return NEED_DATES_RESEARCH_STATE.RESEARCH_RECOMMENDED;
  }
  return period.researchState || NEED_DATES_RESEARCH_STATE.NOT_RESEARCHED;
}

/**
 * Build dashboard payload for the management page.
 */
export async function buildNeedDatesManagementDashboard(hotelId, opts = {}) {
  const useAirtable =
    opts.useAirtable === true ||
    (opts.useAirtable !== false &&
      isNeedDateAirtableLiveEnabled() &&
      opts.isolation !== NEED_DATES_PORTAL_ISOLATION_NAMESPACE);

  const isolation =
    useAirtable
      ? null
      : opts.isolation === true ||
          opts.isolation === NEED_DATES_PORTAL_ISOLATION_NAMESPACE
        ? NEED_DATES_PORTAL_ISOLATION_NAMESPACE
        : opts.isolation || null;

  const listed = useAirtable
    ? await listNeedPeriodsFromAirtable(hotelId, {
        testScenarioOnly: opts.testScenarioOnly === true,
      })
    : listNeedPeriodsForHotel(hotelId, { isolation });
  const periods = listed.periods;
  const hotel = hotelContext(hotelId);
  const inventory = loadInventory(hotelId);
  const openPeriods = periods.filter((p) => isNeedPeriodCommerciallyOpen(p));
  const inventoryMatch = matchInventoryToNeedPeriods(
    openPeriods.length ? openPeriods : periods,
    inventory,
  );
  const byPeriodMatches = attachMatchPresentation(
    inventoryMatch.matches || [],
    periods,
  );

  let commercialProfile = null;
  try {
    const { loadCommercialProfile, buildCommercialProfileSuggestions } =
      await import("./hotel-commercial-profile-store-v1.js");
    commercialProfile = await loadCommercialProfile(hotelId, {
      useAirtable,
      allowLocalFallback: true,
    });
    commercialProfile.suggestions = buildCommercialProfileSuggestions(
      hotelId,
      hotel,
      inventory,
    );
  } catch {
    commercialProfile = null;
  }

  const strategyPlan = selectStrategiesForCampaign({
    needPeriods: openPeriods,
    hotel,
    inventoryMatch,
    maxStrategies: 6,
    commercialProfile,
  });

  const year = Number(opts.year) || new Date().getUTCFullYear();

  let changeHistory = { changes: [] };
  if (useAirtable) {
    try {
      changeHistory = await listHotelNeedPeriodChangeHistoryFromAirtable(hotelId);
    } catch {
      changeHistory = { changes: [] };
    }
  }

  const changesByPeriodId = {};
  for (const ch of changeHistory.changes || []) {
    const id = ch.needPeriodId;
    if (!id) continue;
    if (!changesByPeriodId[id]) changesByPeriodId[id] = [];
    changesByPeriodId[id].push(ch);
  }

  const periodCards = periods.map((p) => {
    const matches = byPeriodMatches[p.needPeriodId] || {
      existingMatch: [],
      existingEnriched: [],
      newOpportunity: [],
      researchContinuation: [],
    };
    const researchState = deriveResearchState(p, matches);
    const drawerBlocks = [];
    for (const m of inventoryMatch.matches || []) {
      if (m.needPeriodId !== p.needPeriodId) continue;
      const block = buildNeedDateMatchDrawerComponent({
        needPeriod: p,
        timingMatch: m.timing,
        association: m.association,
        opportunity: { id: m.opportunityId, title: m.title },
      });
      if (block) drawerBlocks.push(block);
    }
    return {
      ...p,
      researchState,
      researchStateLabel: NEED_DATES_RESEARCH_STATE_LABEL[researchState],
      matchedOpportunityCount:
        matches.existingMatch.length +
        matches.existingEnriched.length +
        matches.newOpportunity.length +
        matches.researchContinuation.length,
      matches,
      drawerBlocks,
      changeHistory: (changesByPeriodId[p.needPeriodId] || []).slice(0, 20).map((c) => ({
        ...c,
        changeTypeLabel: customerFriendlyChangeType(c.changeType),
      })),
    };
  });

  const monthlyOverview = buildMonthlyOverview(periodCards, year, changesByPeriodId);
  const activeNeedMonthCount = countActiveNeedMonths(monthlyOverview);

  const adaptivePlan = buildAdaptiveNeedDateResearchPlan({
    hotelId,
    hotel,
    needPeriods: openPeriods,
    commercialProfile,
    inventoryMatch,
    authorizePaidFollowUp: false,
  });

  const summary = {
    activeNeedPeriods: openPeriods.length,
    activeNeedMonths: activeNeedMonthCount,
    inactiveOrHistoricalPeriods: periods.length - openPeriods.length,
    matchedGdiOpportunities: inventoryMatch.existingMatchCount || 0,
    newNeedDateResearchOpportunities: periodCards.reduce(
      (n, p) => n + (p.matches.newOpportunity?.length || 0),
      0,
    ),
    researchStatus: openPeriods.length
      ? periodCards.some(
          (p) =>
            p.researchState === NEED_DATES_RESEARCH_STATE.RESEARCH_RECOMMENDED,
        )
        ? "Research Recommended"
        : "Matching Existing Opportunities"
      : "No Active Need Periods",
  };

  // Persist match associations to Airtable when live (idempotent; no opp duplication)
  if (useAirtable) {
    for (const m of inventoryMatch.matches || []) {
      try {
        await upsertNeedPeriodOpportunityMatch({
          hotelId,
          needPeriodId: m.needPeriodId,
          opportunityId: m.opportunityId,
          matchingEvidence: {
            title: m.title,
            timing: m.timing?.overlapStatus,
          },
          timingOverlap: m.timing?.overlapStatus || null,
          revenueRelevance: m.association?.matchedRevenue || null,
          matchClass: "Existing Match",
        });
      } catch {
        /* match write is best-effort during dashboard read */
      }
    }
  }

  return {
    ok: true,
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    workflowNumber: NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
    hotelId,
    hotel,
    year,
    isolation: listed.isolation,
    customerVisible: useAirtable ? true : listed.customerVisible,
    dataMode: useAirtable
      ? "AIRTABLE_AI_DEMAND_POSITIONING"
      : listed.isolation
        ? "ISOLATED_PORTAL_SHADOW"
        : "HOTEL_NEED_PERIODS_LOCAL",
    lastUpdated: listed.meta?.updatedAt || null,
    summary: {
      ...summary,
      revenuePriorities: (commercialProfile?.revenuePriorities || []).length,
      preferredPeakRooms: commercialProfile?.peakRoomsPreset || null,
      localDemandCategories: (
        commercialProfile?.demandGeneratorCategories || []
      ).length,
      knownAccounts: (commercialProfile?.knownAccounts || []).length,
    },
    monthlyOverview,
    periods: periodCards,
    commercialProfile,
    changeHistory: {
      changes: (changeHistory.changes || []).slice(0, 100).map((c) => ({
        ...c,
        changeTypeLabel: customerFriendlyChangeType(c.changeType),
      })),
      count: (changeHistory.changes || []).length,
    },
    adaptiveResearch: {
      version: adaptivePlan.version,
      targetQualifiedRange: adaptivePlan.targetQualifiedRange,
      waves: adaptivePlan.waves,
      paidExecutionBlockedUntilFounderGo: true,
      strategyRelevanceHigh: (
        adaptivePlan.strategyRelevance?.selectedForPursuit || []
      ).slice(0, 8),
    },
    strategyPlan: {
      strategyIds: strategyPlan.strategyIds || [],
      selected: strategyPlan.selected || [],
      paidExecutionBlockedUntilFounderGo: true,
      commercialProfileApplied: strategyPlan.commercialProfileApplied === true,
      estimatedSpendUsd: null,
      allFourteenEvaluated: true,
    },
    inventoryBaseline: {
      count: inventory.length,
      note: "Existing GDI inventory — no synthetic dashboard counts",
    },
    paidProvider: PAID_PROVIDER_BLOCKED,
    airtableMutation: useAirtable
      ? {
          code: "NEED_DATE_AIRTABLE_LIVE",
          message:
            "Need-period CRUD authorized on AI Demand Positioning. Paid Exa and publication remain blocked.",
          base: "AI Demand Positioning",
        }
      : AIRTABLE_MUTATION_BLOCKED,
    schemaProposal: proposeNeedDateAirtableSchema(),
    enums: getNeedDatesManagementEnums(),
  };
}

function buildMonthlyOverview(periods, year, changesByPeriodId = {}) {
  const months = [];
  for (let m = 1; m <= 12; m++) {
    const bounds = monthBoundsUtc(year, m);
    const hits = periods.filter((p) => {
      if (!p.startDate || !p.endDate) return false;
      return p.startDate <= bounds.endDate && p.endDate >= bounds.startDate;
    });
    const visual = summarizeMonthVisual(hits, changesByPeriodId);
    months.push({
      year,
      month: m,
      label: new Date(Date.UTC(year, m - 1, 1)).toLocaleString("en-US", {
        month: "long",
        timeZone: "UTC",
      }),
      startDate: bounds.startDate,
      endDate: bounds.endDate,
      hasActiveNeed: visual.hasActiveNeed,
      activeCount: visual.activeCount,
      inactiveCount: visual.inactiveCount,
      multipleNeeds: visual.multipleNeeds,
      headlinePriority: visual.headlinePriority,
      visualStatus: visual.visualStatus,
      visualStatusLabel: visual.visualStatusLabel,
      visualStatusIcon: visual.visualStatusIcon,
      changeIndicator: visual.changeIndicator,
      changeIndicatorLabel: visual.changeIndicatorLabel,
      lastUpdatedAt: visual.lastUpdatedAt,
      periods: hits.map((p) => ({
        needPeriodId: p.needPeriodId,
        periodLabel: p.periodLabel,
        priority: p.priority,
        status: p.status,
        revenueNeed: p.revenueNeed,
        commerciallyOpen: isNeedPeriodCommerciallyOpen(p),
        researchState: p.researchState || null,
        matchedOpportunityCount: p.matchedOpportunityCount || 0,
      })),
    });
  }
  return months;
}

export function getNeedDatesManagementEnums() {
  const commercialPriorities = getCommercialPrioritiesEnums();
  const commercial = {
    monthNeedChips: commercialPriorities.monthNeedChips || [
      { id: "GROUP_GUESTROOMS", label: "Guestrooms / Groups" },
      { id: "MEETING_SPACE", label: "Meetings" },
      { id: "LOCAL_CATERING", label: "Catering" },
      { id: "BANQUETS", label: "Banquets" },
      { id: "PRIVATE_DINING", label: "Private Dining" },
      { id: "EXTENDED_STAY", label: "Extended Stay" },
    ],
    priorityChips: commercialPriorities.priorityChips || [
      { id: "CRITICAL", label: "Critical" },
      { id: "HIGH", label: "High" },
      { id: "MEDIUM", label: "Medium" },
    ],
  };
  return {
    revenueTypes: Object.entries(NEED_REVENUE_TYPE_LABEL).map(([id, label]) => ({
      id,
      label,
    })),
    priorities: ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
    statuses: Object.values(NEED_PERIOD_STATUS),
    changeTypes: Object.values(NEED_PERIOD_CHANGE_TYPE),
    researchStates: Object.entries(NEED_DATES_RESEARCH_STATE_LABEL).map(
      ([id, label]) => ({ id, label }),
    ),
    commercial,
    commercialPriorities,
  };
}

/**
 * Create one or more need periods from hotel-friendly input.
 * Nonconsecutive months → independent periods unless continuousRange=true.
 */
function persistChange(args, opts = {}) {
  const useAirtable =
    opts.useAirtable === true ||
    (opts.useAirtable !== false &&
      isNeedDateAirtableLiveEnabled() &&
      opts.isolation !== NEED_DATES_PORTAL_ISOLATION_NAMESPACE);
  if (useAirtable) {
    return saveNeedPeriodChangeToAirtable({
      hotelId: args.hotelId,
      changeType: args.changeType,
      needPeriodId: args.needPeriodId,
      patch: args.patch,
      actorSource: args.actorSource,
      actorUser: args.actorUser,
      reason: args.reason,
      expectedVersion: args.expectedVersion,
      correlationId: args.correlationId,
    });
  }
  return Promise.resolve(
    saveNeedPeriodChange({
      hotelId: args.hotelId,
      isolation: opts.isolation || null,
      changeType: args.changeType,
      needPeriodId: args.needPeriodId,
      patch: args.patch,
      actorSource: args.actorSource,
      actorUser: args.actorUser,
      reason: args.reason,
      expectedVersion: args.expectedVersion,
    }),
  );
}

export async function createNeedPeriodsFromInput(hotelId, body = {}, opts = {}) {
  const isolation = opts.isolation || null;
  const actorUser = opts.actorUser || null;
  const actorSource = opts.actorSource || "hotel_sales";
  const months = Array.isArray(body.months) ? body.months : [];
  const continuousRange = body.continuousRange === true;
  const revenueNeed = Array.isArray(body.revenueNeed)
    ? body.revenueNeed
    : body.revenueTypes || [];
  const priority = text(body.priority) || "MEDIUM";
  const created = [];

  if (continuousRange && body.startDate && body.endDate) {
    const r = await persistChange(
      {
        hotelId,
        changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
        patch: {
          hotelId,
          periodLabel: text(body.periodLabel) || `${body.startDate} → ${body.endDate}`,
          startDate: body.startDate,
          endDate: body.endDate,
          periodGranularity: "Date Range",
          revenueNeed,
          priority,
          targetVolume: body.targetRoomNights ?? body.targetVolume ?? null,
          targetRevenue: body.targetRevenue ?? null,
          minGroupSize: body.minGroupSize ?? null,
          maxGroupSize: body.maxGroupSize ?? null,
          preferredWeekpart:
            body.preferredDaysOfWeek ||
            body.preferredWeekpart ||
            body.preferredDays ||
            null,
          marketPreferences: body.marketPreferences || null,
          commercialRestrictions: body.restrictions || body.commercialRestrictions || null,
          hotelSalesNotes: body.salesNotes || body.hotelSalesNotes || null,
          preferenceOverrides: body.preferenceOverrides || null,
          status: body.status || NEED_PERIOD_STATUS.ACTIVE,
          researchState: NEED_DATES_RESEARCH_STATE.MATCHING_EXISTING,
          isTestScenario: Boolean(body.isTestScenario),
          source: body.source || "hotel_portal",
        },
        actorSource,
        actorUser,
        reason: body.reason || "Hotel added need period",
      },
      opts,
    );
    created.push(r.change.needPeriodId);
    return { ...r, createdNeedPeriodIds: created };
  }

  const monthSpecs = months.length
    ? months
    : body.year && body.month
      ? [{ year: body.year, month: body.month }]
      : body.startDate && body.endDate
        ? null
        : [];

  if (monthSpecs === null) {
    const r = await persistChange(
      {
        hotelId,
        changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
        patch: {
          hotelId,
          periodLabel: text(body.periodLabel) || `${body.startDate} → ${body.endDate}`,
          startDate: body.startDate,
          endDate: body.endDate,
          revenueNeed,
          priority,
          targetVolume: body.targetRoomNights ?? null,
          targetRevenue: body.targetRevenue ?? null,
          minGroupSize: body.minGroupSize ?? null,
          maxGroupSize: body.maxGroupSize ?? null,
          preferredWeekpart: body.preferredDaysOfWeek || null,
          commercialRestrictions: body.restrictions || null,
          hotelSalesNotes: body.salesNotes || null,
          status: body.status || NEED_PERIOD_STATUS.ACTIVE,
          researchState: NEED_DATES_RESEARCH_STATE.MATCHING_EXISTING,
          isTestScenario: Boolean(body.isTestScenario),
          source: body.source || "hotel_portal",
        },
        actorSource,
        actorUser,
        reason: body.reason || "Hotel added need period",
      },
      opts,
    );
    return { ...r, createdNeedPeriodIds: [r.change.needPeriodId] };
  }

  if (!monthSpecs.length) {
    const err = new Error("need_period_months_or_range_required");
    err.code = "VALIDATION_ERROR";
    err.status = 400;
    throw err;
  }

  let last = null;
  for (const spec of monthSpecs) {
    const year = Number(spec.year);
    const month = Number(spec.month);
    const bounds = monthBoundsUtc(year, month);
    const label =
      text(spec.periodLabel) ||
      `${new Date(Date.UTC(year, month - 1, 1)).toLocaleString("en-US", {
        month: "long",
        timeZone: "UTC",
      })} ${year}`;
    last = await persistChange(
      {
        hotelId,
        changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
        patch: {
          needPeriodId: spec.needPeriodId || undefined,
          hotelId,
          periodLabel: label,
          startDate: bounds.startDate,
          endDate: bounds.endDate,
          periodGranularity: "Month",
          revenueNeed,
          priority: spec.priority || priority,
          targetVolume: body.targetRoomNights ?? null,
          targetRevenue: body.targetRevenue ?? null,
          minGroupSize: body.minGroupSize ?? null,
          maxGroupSize: body.maxGroupSize ?? null,
          preferredWeekpart:
            body.preferredDaysOfWeek ||
            body.preferredWeekpart ||
            body.preferredDays ||
            null,
          commercialRestrictions: body.restrictions || null,
          hotelSalesNotes: body.salesNotes || null,
          preferenceOverrides: body.preferenceOverrides || spec.preferenceOverrides || null,
          status: body.status || NEED_PERIOD_STATUS.ACTIVE,
          researchState: NEED_DATES_RESEARCH_STATE.MATCHING_EXISTING,
          isTestScenario: Boolean(body.isTestScenario),
          source: body.source || "hotel_portal",
        },
        actorSource,
        actorUser,
        reason: body.reason || `Hotel added ${label}`,
      },
      opts,
    );
    created.push(last.change.needPeriodId);
  }

  return {
    ok: true,
    hotelId,
    isolation: last?.isolation || isolation,
    createdNeedPeriodIds: created,
    periods: last?.periods || [],
    history: last?.history || null,
    change: last?.change || null,
    meta: last?.meta || null,
  };
}

export async function applyPortalNeedPeriodChange(hotelId, needPeriodId, body = {}, opts = {}) {
  const changeType = text(body.changeType || body.statusChange || "MODIFY").toUpperCase();
  const mapped =
    {
      PARTIALLY_FILLED: NEED_PERIOD_CHANGE_TYPE.PARTIALLY_FILLED,
      FILLED: NEED_PERIOD_CHANGE_TYPE.FILLED,
      CLOSED: NEED_PERIOD_CHANGE_TYPE.CLOSED,
      CANCEL: NEED_PERIOD_CHANGE_TYPE.CANCEL,
      WITHDRAWN: NEED_PERIOD_CHANGE_TYPE.CANCEL,
      REOPEN: NEED_PERIOD_CHANGE_TYPE.REOPEN,
      REOPENED: NEED_PERIOD_CHANGE_TYPE.REOPEN,
      REPLACE: NEED_PERIOD_CHANGE_TYPE.REPLACE,
      REPLACED: NEED_PERIOD_CHANGE_TYPE.REPLACE,
      PAUSED: NEED_PERIOD_CHANGE_TYPE.MODIFY,
      PRIORITY_CHANGED: NEED_PERIOD_CHANGE_TYPE.PRIORITY_CHANGED,
      MODIFY: NEED_PERIOD_CHANGE_TYPE.MODIFY,
      SPLIT: NEED_PERIOD_CHANGE_TYPE.SPLIT,
    }[changeType] || changeType;

  if (
    (mapped === NEED_PERIOD_CHANGE_TYPE.REOPEN ||
      mapped === NEED_PERIOD_CHANGE_TYPE.REPLACE) &&
    !text(body.reason)
  ) {
    const err = new Error("reason_required_for_reopen_or_replace");
    err.code = "VALIDATION_ERROR";
    err.status = 400;
    throw err;
  }

  const patch = { ...(body.patch || {}) };
  if (body.priority) patch.priority = body.priority;
  if (body.revenueNeed) patch.revenueNeed = body.revenueNeed;
  if (body.startDate) patch.startDate = body.startDate;
  if (body.endDate) patch.endDate = body.endDate;
  if (body.periodLabel) patch.periodLabel = body.periodLabel;
  if (body.targetRoomNights != null) patch.targetVolume = body.targetRoomNights;
  if (body.targetRevenue != null) patch.targetRevenue = body.targetRevenue;
  if (body.restrictions != null) patch.commercialRestrictions = body.restrictions;
  if (body.salesNotes != null) patch.hotelSalesNotes = body.salesNotes;
  if (body.status === "Paused" || changeType === "PAUSED") {
    patch.status = NEED_PERIOD_STATUS.REVIEW;
    patch.paused = true;
  }
  if (body.successorPeriod) patch.successorPeriod = body.successorPeriod;
  if (body.childPeriods) patch.childPeriods = body.childPeriods;

  return persistChange(
    {
      hotelId,
      changeType: mapped,
      needPeriodId,
      patch,
      actorSource: opts.actorSource || "hotel_sales",
      actorUser: opts.actorUser || null,
      reason: body.reason || null,
      expectedVersion: body.expectedVersion ?? body.currentVersion ?? null,
    },
    opts,
  );
}

export async function requestNeedDateResearchAuthorization(hotelId, body = {}, opts = {}) {
  // Never auto-run paid research — only mark awaiting authorization
  const useAirtable =
    opts.useAirtable === true ||
    (opts.useAirtable !== false &&
      isNeedDateAirtableLiveEnabled() &&
      opts.isolation !== NEED_DATES_PORTAL_ISOLATION_NAMESPACE);
  const listed = useAirtable
    ? await listNeedPeriodsFromAirtable(hotelId)
    : listNeedPeriodsForHotel(hotelId, { isolation: opts.isolation || null });
  const ids = Array.isArray(body.needPeriodIds)
    ? body.needPeriodIds
    : listed.periods.map((p) => p.needPeriodId);
  let last = null;
  for (const id of ids) {
    const current = useAirtable
      ? await listNeedPeriodsFromAirtable(hotelId)
      : listNeedPeriodsForHotel(hotelId, { isolation: opts.isolation || null });
    const period = current.periods.find((p) => p.needPeriodId === id);
    if (!period) continue;
    last = await persistChange(
      {
        hotelId,
        changeType: NEED_PERIOD_CHANGE_TYPE.MODIFY,
        needPeriodId: id,
        patch: {
          researchState: NEED_DATES_RESEARCH_STATE.AWAITING_AUTHORIZATION,
        },
        actorSource: opts.actorSource || "hotel_sales",
        actorUser: opts.actorUser || null,
        reason: body.reason || "Hotel requested need-date research authorization",
        expectedVersion: period.currentVersion,
      },
      opts,
    );
  }
  return {
    ok: true,
    hotelId,
    status: NEED_DATES_RESEARCH_STATE.AWAITING_AUTHORIZATION,
    paidProvider: PAID_PROVIDER_BLOCKED,
    message:
      "Research marked Awaiting Authorization. Paid Exa will not run without separate founder spend GO.",
    periods: last?.periods || listed.periods,
  };
}

export async function seedWaterstonePortalTestScenarios({ clear = true } = {}) {
  const hotelId = WATERSTONE_NEED_DATE_PILOT_HOTEL_ID;
  if (clear) clearIsolatedNeedPeriodStore(hotelId);
  const created = await createNeedPeriodsFromInput(
    hotelId,
    {
      months: [
        { year: 2027, month: 8, needPeriodId: "ndp_portal_waterstone_aug_2027" },
        { year: 2027, month: 9, needPeriodId: "ndp_portal_waterstone_sep_2027" },
        { year: 2027, month: 10, needPeriodId: "ndp_portal_waterstone_oct_2027" },
      ],
      revenueNeed: [
        NEED_REVENUE_TYPE.GROUP_GUESTROOMS,
        NEED_REVENUE_TYPE.MEETING_SPACE,
        NEED_REVENUE_TYPE.BANQUETS,
        NEED_REVENUE_TYPE.LOCAL_CATERING,
      ],
      priority: "HIGH",
      status: NEED_PERIOD_STATUS.TEST_SCENARIO,
      isTestScenario: true,
      source: "portal_v1_isolated_test",
      salesNotes:
        "Founder-selected RESEARCH TEST PERIODS — not confirmed occupancy gaps.",
    },
    {
      isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
      actorSource: "founder_test",
      actorUser: { id: "founder_test", email: "founder@dealality.test" },
    },
  );
  return {
    ok: true,
    hotelId,
    isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
    createdNeedPeriodIds: created.createdNeedPeriodIds,
    customerVisible: false,
  };
}

export {
  NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
  getNeedPeriodById,
  ensureVersionedNeedPeriod,
};
