/**
 * GDI universal Need-Date Intelligence tile icon preview V1.
 *
 * Presentation-only classification for the compact calendar SVG on opportunity
 * tiles. Reuses Need-Date Demand Intelligence #15 matching, date-flexibility
 * V8, and commercial-accessibility signals. Does not invent matches, mutate
 * inventory, call paid providers, or write Airtable.
 *
 * Timing ≠ commercial accessibility. A verified date overlap must never imply
 * open sourcing, available rooms, or confirmed buyer flexibility.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_PERIOD_STATUS,
  NEED_TIMING_OVERLAP,
} from "./need-date-demand-contract-v1.js";
import { matchInventoryToNeedPeriods } from "./need-period-inventory-match-v1.js";
import { isNeedPeriodCommerciallyOpen } from "./need-period-lifecycle-v1.js";
import {
  classifyDateFlexibility,
  DATE_FLEXIBILITY_CLASS,
} from "./date-flexibility-intelligence-v8.js";
import {
  attachNeedDateMatchToOpportunity,
  buildNeedDateMatchDrawerComponent,
} from "./need-period-drawer-component-v1.js";
import {
  NEED_PERIOD_PROVENANCE,
  applyNeedDateMatchProvenanceGuard,
  resolveHotelNeedDateConfiguration,
  toCustomerSafeHotelNeedDateStatus,
} from "./gdi-universal-need-date-intelligence-v1.js";
import { attachUniversalOpportunityNeedDateIndicator } from "./gdi-universal-opportunity-need-date-indicator-v2.js";

export const GDI_NEED_DATE_TILE_PREVIEW_V1 = "GDI_NEED_DATE_TILE_PREVIEW_V1";

export const NEED_DATE_TILE_INDICATOR = Object.freeze({
  VERIFIED_DATE_MATCH: "VERIFIED_DATE_MATCH",
  POTENTIALLY_FLEXIBLE: "POTENTIALLY_FLEXIBLE",
  TIMING_RESEARCH_REQUIRED: "TIMING_RESEARCH_REQUIRED",
  NO_MEANINGFUL_MATCH: "NO_MEANINGFUL_MATCH",
});

export const NEED_DATE_TILE_ICON = Object.freeze({
  [NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH]: "calendar-check",
  [NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE]: "calendar-clock",
  [NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED]: "calendar-search",
  [NEED_DATE_TILE_INDICATOR.NO_MEANINGFUL_MATCH]: null,
});

/** CSS color tokens (Dealality platform). */
export const NEED_DATE_TILE_COLOR_TOKEN = Object.freeze({
  [NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH]: "var(--system--green-300)",
  [NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE]: "var(--secondary--color-5)",
  [NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED]: "var(--accent--primary-1)",
  [NEED_DATE_TILE_INDICATOR.NO_MEANINGFUL_MATCH]: null,
});

export const NEED_DATE_TILE_COLOR_CLASS = Object.freeze({
  [NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH]: "gdi-need-date-icon--verified",
  [NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE]: "gdi-need-date-icon--flexible",
  [NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED]: "gdi-need-date-icon--research",
  [NEED_DATE_TILE_INDICATOR.NO_MEANINGFUL_MATCH]: null,
});

const ACCESSIBILITY_SUPPRESS_POSITIVE = new Set([
  "COMPETITOR_COMMITTED",
  "SPECULATIVE_OVERFLOW",
  "REJECTED",
  "ARCHIVED",
  "COMMERCIALLY_INACCESSIBLE",
  "NOT_ADDRESSABLE",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function unique(arr) {
  return [...new Set((arr || []).filter(Boolean))];
}

/**
 * Customer-facing Need-Date periods only:
 * - commercially open
 * - not TEST_SCENARIO
 * - hotel-reported / actual hotel input when provenance is present
 */
export function filterCustomerFacingNeedPeriodsForTileIcons(periods = []) {
  return (Array.isArray(periods) ? periods : []).filter((p) => {
    if (!p?.needPeriodId) return false;
    if (!isNeedPeriodCommerciallyOpen(p)) return false;
    const status = text(p.status);
    if (status === NEED_PERIOD_STATUS.TEST_SCENARIO) return false;
    if (p.isTestScenario === true) return false;
    const scenario = upper(
      p.preferenceOverrides?.scenarioClassification || p.scenarioClassification,
    );
    if (scenario === "TEST_SCENARIO" || scenario === "SYNTHETIC_TEST") return false;
    const source = upper(p.source);
    const taxonomy = Array.isArray(p.preferenceOverrides?.sourceTaxonomy)
      ? p.preferenceOverrides.sourceTaxonomy.map(upper)
      : [];
    const hotelReported =
      !source ||
      source === "HOTEL_REPORTED" ||
      source === "HOTEL_SALES_FEEDBACK" ||
      source === "HOTEL_PORTAL" ||
      source === "HOTEL_SALES" ||
      source === "AIRTABLE" ||
      taxonomy.includes("HOTEL_REPORTED") ||
      taxonomy.includes("HOTEL_SALES_FEEDBACK") ||
      scenario === "ACTUAL_HOTEL_INPUT" ||
      scenario === "HOTEL_REPORTED";
    // Explicit research/synthetic/test provenance: never drive customer-facing icons.
    if (
      /SYNTHETIC|FIXTURE|FOUNDER_TEST|RESEARCH_BENCHMARK|FOUNDER_LIFECYCLE/i.test(
        source,
      ) ||
      scenario === "FOUNDER_DEFINED_RESEARCH" ||
      scenario === "SYNTHETIC_TEST"
    ) {
      return false;
    }
    // Unknown source on Active periods: allow (legacy / portal rows). Hotel ID
    // scoping happens at load time — never Bethesda-only.
    return hotelReported || !source;
  });
}

export function isArchivedOpportunity(opp = {}) {
  return (
    opp.archived === true ||
    opp.isArchived === true ||
    opp.prospectPublicationWithdrawn === true ||
    upper(opp.archiveState) === "ARCHIVED" ||
    upper(opp.customerFacingState) === "ARCHIVED" ||
    upper(opp.customerFacingState) === "CLOSED"
  );
}

/**
 * Competitor-committed / commercially inaccessible → never show a positive
 * Need-Date tile indicator (even if timing overlap exists internally).
 */
export function isPositiveNeedDateIconSuppressed(opp = {}) {
  if (isArchivedOpportunity(opp)) return true;
  const a = upper(
    opp.commercialAccessibilityClass ||
      opp.needDateAccessibilityClass ||
      opp.accessibilityClass ||
      opp.accessibilityV6?.class ||
      "",
  );
  if (ACCESSIBILITY_SUPPRESS_POSITIVE.has(a)) return true;
  if (opp.salesReadyEligible === false && a === "COMPETITOR_COMMITTED") return true;
  const blob = [
    opp.title,
    opp.displayTitle,
    opp.organizationName,
    opp.demandMechanism,
    opp.hotelSourcingStatus,
    opp.venueSourcingStatus,
    opp.existingVenueOrHotelCommitments,
    opp.venueConfirmationNote,
    opp.recommendedAction,
    opp.disqualificationReason,
  ]
    .map(text)
    .join(" ");
  if (
    /already contracted|venue already|competitor.?committed|committed elsewhere|primary business committed/i.test(
      blob,
    )
  ) {
    return true;
  }
  if (/CLOSED_DISQUALIFIED|DISQUALIFIED/i.test(String(opp.demandMechanism || ""))) {
    return true;
  }
  if (/_disqualified$/i.test(String(opp.id || opp.opportunityId || ""))) {
    return true;
  }
  return false;
}

function hasNeedDateResearchAssociation(opp = {}, matches = []) {
  if (matches.length > 0) return true;
  if (opp.needDateAssociations?.length) return true;
  if (opp.needDateCampaignId || opp.needDateResearchCampaignId) return true;
  if (opp.needDateResearch === true || opp.needDateCampaign === true) return true;
  const lane = upper(opp.researchLane || opp.discoveryLane || opp.campaignType);
  if (/NEED.?DATE|NEED_DATE/.test(lane)) return true;
  const provenance = text(
    opp.researchProvenance ||
      opp.needDateResearchProvenance ||
      opp.discoveryProvenance ||
      "",
  );
  if (/need.?date|need_date|#15|workflow.?15/i.test(provenance)) return true;
  if (upper(opp.needDateTimingClass || opp.needDateMatchClass)) return true;
  return false;
}

function periodLabel(period) {
  return text(period?.periodLabel || period?.label) || null;
}

function formatMonthList(labels = []) {
  const labs = unique(labels);
  if (!labs.length) return null;
  if (labs.length === 1) return labs[0];
  if (labs.length === 2) return `${labs[0]} and ${labs[1]}`;
  return `${labs.slice(0, -1).join(", ")}, and ${labs[labs.length - 1]}`;
}

function emptyPreview(extra = {}) {
  return {
    version: GDI_NEED_DATE_TILE_PREVIEW_V1,
    contract: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    indicatorState: NEED_DATE_TILE_INDICATOR.NO_MEANINGFUL_MATCH,
    visible: false,
    icon: null,
    colorToken: null,
    colorClass: null,
    needPeriodIds: [],
    displayMonthLabels: [],
    dateEvidenceStatus: null,
    flexibilityClassification: null,
    commercialAccessibilityQualification: null,
    tooltipText: null,
    accessibleLabel: null,
    researchProvenance: null,
    timingDoesNotImplyAccessibility: true,
    ...extra,
  };
}

function compactMonthPhrase(monthLabels = []) {
  const labs = unique(monthLabels);
  if (!labs.length) return null;
  // Verified overlaps usually 1–2 months — list exactly.
  // Broad ongoing/unconfirmed associations against many need months stay readable.
  if (labs.length <= 3) return formatMonthList(labs);
  return `hotel need periods (incl. ${labs.slice(0, 2).join(", ")}, +${labs.length - 2} more)`;
}

function buildTooltip(state, monthLabels, opts = {}) {
  const monthsExact = formatMonthList(monthLabels);
  const months =
    state === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH
      ? monthsExact
      : compactMonthPhrase(monthLabels);
  if (state === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH) {
    return months
      ? `Date match: ${months} — opportunity dates verified.`
      : "Date match — opportunity dates verified.";
  }
  if (state === NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE) {
    return months
      ? `Potentially flexible timing — confirm ${months} availability with organizer.`
      : "Potentially flexible timing — confirm availability with organizer.";
  }
  if (state === NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED) {
    if (opts.unresolvedQuestion) return opts.unresolvedQuestion;
    return months
      ? `Timing unconfirmed — investigate whether a session is planned for ${months}.`
      : "Timing unconfirmed — investigate whether a future session is planned.";
  }
  return null;
}

function buildAccessibleLabel(state, monthLabels) {
  const months = formatMonthList(monthLabels);
  if (state === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH) {
    return months
      ? `Need-date verified match for ${months}`
      : "Need-date verified match";
  }
  if (state === NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE) {
    return months
      ? `Need-date potentially flexible for ${months}`
      : "Need-date potentially flexible";
  }
  if (state === NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED) {
    return months
      ? `Need-date timing research required for ${months}`
      : "Need-date timing research required";
  }
  return null;
}

function buildDrawerNarrative({
  hotelName,
  state,
  monthLabels,
  flexibility,
  accessibility,
} = {}) {
  const hotel = text(hotelName) || "This hotel";
  const months = formatMonthList(monthLabels) || "an identified need period";
  if (state === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH) {
    return `${hotel} has identified ${months} as a commercial need period. Opportunity dates overlap that period with source-supported timing. Date overlap does not prove hotel sourcing is open — confirm commercial accessibility and remaining inventory before pitching.`;
  }
  if (state === NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE) {
    if (flexibility?.mayPresentAsConfirmedFlexible === true) {
      return `${hotel} has identified ${months} as a commercial need period. Buyer-confirmed scheduling flexibility is supported. Potential timing flexibility identified. Confirm event or travel dates with the organizer. Date flexibility does not prove hotel sourcing is open.`;
    }
    return `${hotel} has identified ${months} as a commercial need period. Potential timing flexibility identified. Confirm event or travel dates with the organizer before treating ${months} as a committed fill. Date flexibility does not prove hotel sourcing is open.`;
  }
  if (state === NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED) {
    return `${hotel} has identified ${months} as a commercial need period. Opportunity timing has not been confirmed. Further qualification is required before treating this as a Need-Date match.`;
  }
  return null;
}

/**
 * Pure classifier — hotel-scoped matches + opportunity evidence only.
 */
export function classifyTileNeedDatePreview({
  opportunity = {},
  needPeriods = [],
  matches = [],
  hotelId = null,
  hotelName = null,
} = {}) {
  const periods = filterCustomerFacingNeedPeriodsForTileIcons(needPeriods);
  if (!periods.length) {
    return emptyPreview({
      reason: "no_active_hotel_reported_need_periods",
      hotelId: hotelId || opportunity.hotelId || null,
    });
  }

  if (isArchivedOpportunity(opportunity)) {
    return emptyPreview({
      reason: "archived_or_closed",
      hotelId: hotelId || opportunity.hotelId || null,
      suppressedPositiveSignal: true,
    });
  }

  const periodById = new Map(periods.map((p) => [p.needPeriodId, p]));
  const relevant = (Array.isArray(matches) ? matches : []).filter(
    (m) => m?.needPeriodId && periodById.has(m.needPeriodId),
  );

  const flex = classifyDateFlexibility(opportunity);
  const accessibilityClass = upper(
    opportunity.commercialAccessibilityClass ||
      opportunity.needDateAccessibilityClass ||
      opportunity.accessibilityClass ||
      opportunity.accessibilityV6?.class ||
      "",
  );
  const suppressPositive = isPositiveNeedDateIconSuppressed(opportunity);

  const confirmed = relevant.filter(
    (m) =>
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP ||
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP,
  );
  const ongoing = relevant.filter(
    (m) =>
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL,
  );
  const unconfirmed = relevant.filter(
    (m) => m.timing?.overlapStatus === NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED,
  );

  const labelsFor = (list) =>
    unique(
      list.map((m) => periodLabel(periodById.get(m.needPeriodId))).filter(Boolean),
    );
  const idsFor = (list) => unique(list.map((m) => m.needPeriodId));

  let state = NEED_DATE_TILE_INDICATOR.NO_MEANINGFUL_MATCH;
  let used = [];
  let dateEvidenceStatus = null;
  let reason = "no_meaningful_match";
  let researchProvenance = null;

  if (confirmed.length && !suppressPositive) {
    state = NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH;
    used = confirmed;
    dateEvidenceStatus =
      confirmed[0].timing?.overlapStatus ||
      NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP;
    reason = "verified_or_supported_period_overlap";
  } else if (
    !suppressPositive &&
    ongoing.length &&
    (flex.class === DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE ||
      flex.class === DATE_FLEXIBILITY_CLASS.FLEXIBLE_CONFIRMED ||
      flex.class === DATE_FLEXIBILITY_CLASS.HISTORICAL_RECURRING)
  ) {
    // Recurring / schedulable account with a plausible link to a need month.
    state = NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE;
    used = ongoing;
    dateEvidenceStatus = NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL;
    reason = "ongoing_account_with_flexibility_signal";
  } else if (
    !suppressPositive &&
    unconfirmed.length &&
    hasNeedDateResearchAssociation(opportunity, unconfirmed) &&
    // Do not treat bare unknown dates as flexible; research/unconfirmed only.
    flex.class !== DATE_FLEXIBILITY_CLASS.FIXED_VERIFIED
  ) {
    // Recurring / schedulable mechanisms → potentially flexible (buyer confirm still required).
    // Unknown mechanism with need-date research association → timing research required.
    if (
      flex.class === DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE ||
      flex.class === DATE_FLEXIBILITY_CLASS.FLEXIBLE_CONFIRMED ||
      flex.class === DATE_FLEXIBILITY_CLASS.HISTORICAL_RECURRING
    ) {
      state = NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE;
      used = unconfirmed;
      dateEvidenceStatus = NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED;
      reason = "need_date_research_with_potential_flexibility";
    } else {
      state = NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED;
      used = unconfirmed;
      dateEvidenceStatus = NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED;
      reason = "need_date_research_timing_unconfirmed";
      researchProvenance =
        text(opportunity.needDateResearchProvenance) ||
        text(opportunity.researchProvenance) ||
        "need_date_inventory_match";
    }
  } else if (confirmed.length && suppressPositive) {
    reason = "timing_overlap_suppressed_commercially_inaccessible";
  }

  if (state === NEED_DATE_TILE_INDICATOR.NO_MEANINGFUL_MATCH) {
    return emptyPreview({
      reason,
      hotelId: hotelId || opportunity.hotelId || null,
      flexibilityClassification: flex.class,
      commercialAccessibilityQualification: accessibilityClass || null,
      suppressedPositiveSignal: suppressPositive || undefined,
      internalTimingMatchCount: relevant.length,
    });
  }

  const displayMonthLabels = labelsFor(used);
  const needPeriodIds = idsFor(used);
  const tooltipText = buildTooltip(state, displayMonthLabels);
  const accessibleLabel = buildAccessibleLabel(state, displayMonthLabels);
  const drawerNarrative = buildDrawerNarrative({
    hotelName: hotelName || opportunity.hotelDisplayName || opportunity.hotelName,
    state,
    monthLabels: displayMonthLabels,
    flexibility: flex,
    accessibility: accessibilityClass,
  });
  const qualificationQuestion =
    state === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH
      ? `Confirm remaining inventory and buyer path for ${formatMonthList(displayMonthLabels) || "the matched need period"} — date overlap alone is not open sourcing.`
      : state === NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE
        ? `Can an upcoming session be scheduled during ${formatMonthList(displayMonthLabels) || "the hotel need period"}?`
        : `Is a future edition planned that could land in ${formatMonthList(displayMonthLabels) || "a hotel need period"}?`;

  return {
    version: GDI_NEED_DATE_TILE_PREVIEW_V1,
    contract: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    indicatorState: state,
    visible: true,
    icon: NEED_DATE_TILE_ICON[state],
    colorToken: NEED_DATE_TILE_COLOR_TOKEN[state],
    colorClass: NEED_DATE_TILE_COLOR_CLASS[state],
    needPeriodIds,
    displayMonthLabels,
    dateEvidenceStatus,
    flexibilityClassification: flex.class,
    flexibilityMayPresentAsConfirmed: flex.mayPresentAsConfirmedFlexible === true,
    commercialAccessibilityQualification: accessibilityClass || null,
    tooltipText,
    accessibleLabel,
    researchProvenance,
    timingDoesNotImplyAccessibility: true,
    qualificationQuestion,
    recommendedSalesApproach: drawerNarrative,
    drawerTitle: "Hotel Need-Date Match",
    reason,
    hotelId: hotelId || opportunity.hotelId || null,
  };
}

export function attachTileNeedDatePreview(opportunity = {}, preview, opts = {}) {
  const p =
    preview ||
    emptyPreview({ hotelId: opportunity.hotelId || opts.hotelId || null });
  let out = {
    ...opportunity,
    tileNeedDatePreview: p,
  };

  // Conditional drawer block — only when a visible indicator exists.
  if (p.visible && Array.isArray(opts.matches) && opts.matches.length) {
    const periodById = new Map(
      (opts.needPeriods || []).map((x) => [x.needPeriodId, x]),
    );
    const associations = [];
    for (const m of opts.matches) {
      if (!p.needPeriodIds.includes(m.needPeriodId)) continue;
      const needPeriod = periodById.get(m.needPeriodId);
      if (!needPeriod) continue;
      associations.push({
        needPeriod,
        timing: m.timing,
        timingMatch: m.timing,
        association: m.association,
        outputCategory: m.outputCategory || m.category,
      });
    }
    if (associations.length) {
      out.needDateAssociations = associations;
      out = attachNeedDateMatchToOpportunity(out, { associations });
      if (out.needDateMatchV1 && p.recommendedSalesApproach) {
        out.needDateMatchV1 = {
          ...out.needDateMatchV1,
          whyThisMatches: p.recommendedSalesApproach,
          suggestedSalesAction: p.qualificationQuestion,
          tileIndicatorState: p.indicatorState,
          flexibilityClassification: p.flexibilityClassification,
          commercialAccessibilityQualification:
            p.commercialAccessibilityQualification,
          timingDoesNotImplyAccessibility: true,
        };
      }
    } else if (p.recommendedSalesApproach) {
      out.needDateMatchV1 = {
        version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
        componentId: "hotel_need_date_match",
        title: "Hotel Need-Date Match",
        placement: "near_opportunity_overview",
        replacesTenSections: false,
        needPeriodIds: p.needPeriodIds,
        needPeriodLabel: formatMonthList(p.displayMonthLabels),
        timingOverlapStatus: p.dateEvidenceStatus,
        whyThisMatches: p.recommendedSalesApproach,
        suggestedSalesAction: p.qualificationQuestion,
        tileIndicatorState: p.indicatorState,
        flexibilityClassification: p.flexibilityClassification,
        commercialAccessibilityQualification:
          p.commercialAccessibilityQualification,
        timingDoesNotImplyAccessibility: true,
        confirmedNeedDateMatch:
          p.indicatorState === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
      };
    }
  }

  return out;
}

/**
 * Attach previews for a hotel inventory batch (one need-period load; no N+1).
 */
function resolveHotelConfigForIndicators(hotelId, opts = {}) {
  if (opts.hotelNeedDateConfig && typeof opts.hotelNeedDateConfig === "object") {
    return opts.hotelNeedDateConfig;
  }
  try {
    return toCustomerSafeHotelNeedDateStatus(
      resolveHotelNeedDateConfiguration(hotelId, opts),
      { includeUnpublishedEstimates: true },
    );
  } catch {
    return {
      hotelId,
      status: "NOT_ESTABLISHED",
      confirmedPeriodCount: 0,
      estimatedPeriodCount: 0,
    };
  }
}

function finalizeWithUniversalIndicator(opportunity, timingAttached, hotelConfig) {
  return attachUniversalOpportunityNeedDateIndicator(timingAttached, {
    hotelNeedDateConfig: hotelConfig,
    timingPreview: timingAttached.tileNeedDatePreview || null,
  });
}

export function attachHotelNeedDateTilePreviewsSync(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const allPeriods = Array.isArray(opts.needPeriods) ? opts.needPeriods : [];
  const periods = filterCustomerFacingNeedPeriodsForTileIcons(allPeriods);
  const hotelName = opts.hotelName || null;
  const hotelConfig = resolveHotelConfigForIndicators(hotelId, opts);

  if (!periods.length) {
    return (opportunities || []).map((o) => {
      const attached = attachTileNeedDatePreview(
        o,
        emptyPreview({
          hotelId,
          reason: "no_active_hotel_reported_need_periods",
        }),
      );
      return finalizeWithUniversalIndicator(attached, attached, hotelConfig);
    });
  }

  const matchResult = matchInventoryToNeedPeriods(periods, opportunities || []);
  const byOpp = new Map();
  for (const m of matchResult.matches || []) {
    const id = m.opportunityId;
    if (!id) continue;
    const list = byOpp.get(id) || [];
    list.push(m);
    byOpp.set(id, list);
  }

  return (opportunities || []).map((o) => {
    const id = o.id || o.opportunityId;
    const matches = byOpp.get(id) || [];
    let preview = classifyTileNeedDatePreview({
      opportunity: o,
      needPeriods: periods,
      matches,
      hotelId,
      hotelName,
    });
    // Propagate strongest matched period provenance — MARKET_ESTIMATED never
    // yields an unqualified verified hotel need-date match.
    const matchedPeriods = matches
      .map((m) => periodByIdGet(periods, m.needPeriodId))
      .filter(Boolean);
    const hasEstimated = matchedPeriods.some(
      (p) =>
        upper(p.provenance || p.needPeriodProvenance || "") ===
        NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
    );
    const hasConfirmed = matchedPeriods.some((p) => {
      const prov = upper(p.provenance || p.needPeriodProvenance || "");
      return (
        prov === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED ||
        (!prov && upper(p.source || "").includes("HOTEL"))
      );
    });
    const periodProvenance = hasEstimated && !hasConfirmed
      ? NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED
      : hasConfirmed
        ? NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED
        : preview.periodProvenance || null;
    preview = applyNeedDateMatchProvenanceGuard(preview, periodProvenance);
    const attached = attachTileNeedDatePreview(o, preview, {
      needPeriods: periods,
      matches,
      hotelId,
    });
    return finalizeWithUniversalIndicator(attached, attached, hotelConfig);
  });
}

function periodByIdGet(periods, needPeriodId) {
  const id = text(needPeriodId);
  return (periods || []).find((p) => text(p.needPeriodId || p.id) === id) || null;
}

/** Short in-process cache so Opportunities list + Tab 2 do not re-hit Airtable. */
const NEED_PERIOD_TILE_CACHE_TTL_MS = 45_000;
const needPeriodTileCache = new Map();

export function invalidateNeedPeriodTileCache(hotelId = null) {
  if (!hotelId) {
    needPeriodTileCache.clear();
    return;
  }
  const prefix = `${text(hotelId)}::`;
  for (const key of needPeriodTileCache.keys()) {
    if (key.startsWith(prefix)) needPeriodTileCache.delete(key);
  }
}

/**
 * Load hotel-reported periods once (Airtable when live, else local store).
 * Read-only — never writes.
 */
export async function loadCustomerFacingNeedPeriodsForHotel(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { periods: [], source: "none" };

  if (Array.isArray(opts.needPeriods)) {
    return {
      periods: filterCustomerFacingNeedPeriodsForTileIcons(opts.needPeriods),
      source: "provided",
    };
  }

  const cacheKey = `${id}::${opts.useAirtable === true ? "at" : opts.useAirtable === false ? "local" : "auto"}::${opts.isolation || ""}`;
  if (opts.skipCache !== true) {
    const hit = needPeriodTileCache.get(cacheKey);
    if (hit && Date.now() - hit.at < NEED_PERIOD_TILE_CACHE_TTL_MS) {
      return { ...hit.value, cacheHit: true };
    }
  }

  let result = { periods: [], source: "unavailable" };

  try {
    const { isNeedDateAirtableLiveEnabled, listNeedPeriodsFromAirtable } =
      await import("./need-date-airtable-store-v3.js");
    if (
      opts.useAirtable !== false &&
      (opts.useAirtable === true || isNeedDateAirtableLiveEnabled())
    ) {
      const listed = await listNeedPeriodsFromAirtable(id, {
        env: opts.env,
      });
      result = {
        periods: filterCustomerFacingNeedPeriodsForTileIcons(listed.periods || []),
        source: "airtable",
      };
    }
  } catch {
    /* fall through to local */
  }

  if (result.source === "unavailable") {
    try {
      const { listNeedPeriodsForHotel } = await import(
        "./need-period-hotel-store-v1.js"
      );
      const listed = listNeedPeriodsForHotel(id, {
        isolation: opts.isolation || null,
      });
      result = {
        periods: filterCustomerFacingNeedPeriodsForTileIcons(listed.periods || []),
        source: "local",
      };
    } catch {
      result = { periods: [], source: "unavailable" };
    }
  }

  needPeriodTileCache.set(cacheKey, { at: Date.now(), value: result });
  return result;
}

export async function attachHotelNeedDateTilePreviews(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const loaded = await loadCustomerFacingNeedPeriodsForHotel(hotelId, opts);
  return attachHotelNeedDateTilePreviewsSync(hotelId, opportunities, {
    ...opts,
    needPeriods: loaded.periods,
  });
}

/** Deterministic gate fixtures for permanent regression. */
export function runNeedDateTilePreviewGate() {
  const failures = [];
  const hotelId = "recLuxvwwxID7U2B8";
  const periods = [
    {
      needPeriodId: "ndp_bethesda_marriott_2027_01",
      hotelId,
      periodLabel: "January 2027",
      startDate: "2027-01-01",
      endDate: "2027-01-31",
      status: NEED_PERIOD_STATUS.ACTIVE,
      source: "HOTEL_REPORTED",
      revenueNeed: [],
      preferenceOverrides: { scenarioClassification: "ACTUAL_HOTEL_INPUT" },
    },
    {
      needPeriodId: "ndp_bethesda_marriott_2027_02",
      hotelId,
      periodLabel: "February 2027",
      startDate: "2027-02-01",
      endDate: "2027-02-28",
      status: NEED_PERIOD_STATUS.ACTIVE,
      source: "HOTEL_REPORTED",
      revenueNeed: [],
      preferenceOverrides: { scenarioClassification: "ACTUAL_HOTEL_INPUT" },
    },
  ];

  // Customer-facing expect = V3 four-state; underlyingExpect = preserved V2 intelligence.
  const cases = [
    {
      id: "verified_accessible",
      opp: {
        id: "opp_verified",
        hotelId,
        title: "Regional Scientific Symposium 2027",
        eventStartDate: "2027-01-14",
        eventEndDate: "2027-01-16",
        opportunityType: "Association Conference",
        guestroomThesis: "Peak room block for delegates",
      },
      expect: "MATCHES_NEED_DATE",
      underlyingExpect: NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
      expectIcon: "calendar-check",
    },
    {
      id: "span_two_months",
      opp: {
        id: "opp_span",
        hotelId,
        title: "Winter Leadership Programme",
        eventStartDate: "2027-01-28",
        eventEndDate: "2027-02-03",
        opportunityType: "Corporate meetings",
      },
      expect: "MATCHES_NEED_DATE",
      underlyingExpect: NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH,
      expectMonthsMin: 2,
      expectIcon: "calendar-check",
    },
    {
      id: "potentially_flexible",
      opp: {
        id: "opp_flex",
        hotelId,
        title: "Acme Corp leadership training programme",
        opportunityType: "CORPORATE_ACCOUNT",
        segment: "Scaling Corporate Account",
        whatIsHappening:
          "Recurring corporate training workshops with potentially flexible scheduling",
        needDateResearchProvenance: "need_date_inventory_match",
      },
      expect: "POTENTIAL_MATCH",
      underlyingExpect: NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE,
      expectIcon: "calendar-clock",
    },
    {
      id: "timing_research",
      opp: {
        id: "opp_research",
        hotelId,
        title: "National Policy Forum — edition timing unknown",
        opportunityType: "Association Conference",
        whatIsHappening:
          "Organization identified via need-date research; future session dates not published",
        needDateResearchCampaignId: "nd_campaign_test",
        researchProvenance: "need-date workflow #15",
      },
      expect: "NOT_YET_ASSESSED",
      underlyingExpect: NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED,
      expectIcon: "calendar-question",
    },
    {
      id: "no_match",
      opp: {
        id: "opp_nomatch",
        hotelId,
        title: "Summer Outdoor Festival 2028",
        eventStartDate: "2028-06-10",
        eventEndDate: "2028-06-12",
      },
      // V2 underlying NO_DATE_MATCH → V3 NO_CURRENT_MATCH (never hidden).
      expect: "NO_CURRENT_MATCH",
      underlyingExpect: "NO_DATE_MATCH",
      expectVisible: true,
      expectIcon: "calendar-x",
    },
    {
      id: "competitor_committed_suppressed",
      opp: {
        id: "gdi_opp_asa_advance_2027_disqualified",
        hotelId,
        title:
          "ASA ADVANCE 2027 — already contracted at Gaylord National (National Harbor)",
        eventStartDate: "2027-01-22",
        eventEndDate: "2027-01-24",
        demandMechanism: "CLOSED_DISQUALIFIED",
        commercialAccessibilityClass: "COMPETITOR_COMMITTED",
        existingVenueOrHotelCommitments:
          "already contracted at Gaylord National (National Harbor)",
      },
      // Never claim verified when commercially inaccessible — still visible as Not yet assessed.
      expect: "NOT_YET_ASSESSED",
      underlyingExpect: "TIMING_RESEARCH_REQUIRED",
      expectVisible: true,
      expectSuppressed: true,
      expectIcon: "calendar-question",
    },
    {
      id: "archived_hidden",
      opp: {
        id: "opp_archived",
        hotelId,
        title: "Archived January Event",
        eventStartDate: "2027-01-10",
        archived: true,
      },
      // Archived still gets an explicit indicator (dates present → no current match).
      expect: "NO_CURRENT_MATCH",
      underlyingExpect: "NO_DATE_MATCH",
      expectVisible: true,
      expectIcon: "calendar-x",
    },
  ];

  const attached = attachHotelNeedDateTilePreviewsSync(hotelId, cases.map((c) => c.opp), {
    needPeriods: periods,
    hotelName: "Bethesda Marriott",
    hotelNeedDateConfig: {
      hotelId,
      status: "HOTEL_CONFIRMED",
      confirmedPeriodCount: periods.length,
      estimatedPeriodCount: 0,
    },
  });
  const byId = new Map(attached.map((o) => [o.id, o]));

  for (const c of cases) {
    const row = byId.get(c.opp.id);
    const preview = row?.tileNeedDatePreview;
    const indicator = row?.needDateIndicator;
    if (!preview || !indicator) {
      failures.push({ id: c.id, detail: "missing_preview_or_indicator" });
      continue;
    }
    if (preview.indicatorState !== c.expect && indicator.state !== c.expect) {
      failures.push({
        id: c.id,
        detail: "state_mismatch",
        got: preview.indicatorState,
        indicator: indicator.state,
        expect: c.expect,
      });
    }
    if (
      c.underlyingExpect &&
      indicator.underlyingState !== c.underlyingExpect &&
      preview.underlyingState !== c.underlyingExpect
    ) {
      failures.push({
        id: c.id,
        detail: "underlying_state_mismatch",
        got: indicator.underlyingState || preview.underlyingState,
        expect: c.underlyingExpect,
      });
    }
    if (c.expectVisible === true && (preview.visible !== true || !preview.icon || !indicator.icon)) {
      failures.push({ id: c.id, detail: "must_be_visible_with_icon", got: preview });
    }
    if (c.expectIcon && preview.icon !== c.expectIcon && indicator.icon !== c.expectIcon) {
      failures.push({ id: c.id, detail: "icon_mismatch", got: preview.icon, expect: c.expectIcon });
    }
    if (c.expect === "MATCHES_NEED_DATE") {
      if (!preview.visible || preview.icon !== "calendar-check") {
        failures.push({ id: c.id, detail: "verified_icon", got: preview });
      }
      if (
        !/Matches a Confirmed Need Date/i.test(preview.tooltipText || "") &&
        !/Matches a need date/i.test(preview.tooltipText || "") &&
        !/Date match:/i.test(preview.tooltipText || "")
      ) {
        failures.push({ id: c.id, detail: "verified_tooltip", got: preview.tooltipText });
      }
    }
    if (c.expectMonthsMin && preview.displayMonthLabels.length < c.expectMonthsMin) {
      failures.push({
        id: c.id,
        detail: "multi_month",
        got: preview.displayMonthLabels,
      });
    }
    if (
      c.expectSuppressed &&
      (indicator.state === "MATCHES_NEED_DATE" ||
        indicator.state === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH)
    ) {
      failures.push({ id: c.id, detail: "suppress_must_not_verify", got: indicator });
    }
  }

  // TEST_SCENARIO exclusion — must never produce VERIFIED_DATE_MATCH
  const testPeriods = [
    {
      needPeriodId: "ndp_waterstone_test_2027_08",
      hotelId: "recgMYovrrZDJMqzX",
      periodLabel: "August 2027",
      startDate: "2027-08-01",
      endDate: "2027-08-31",
      status: NEED_PERIOD_STATUS.TEST_SCENARIO,
      source: "FOUNDER_TEST",
      isTestScenario: true,
      revenueNeed: ["MEETING_SPACE"],
    },
  ];
  const waterstone = attachHotelNeedDateTilePreviewsSync(
    "recgMYovrrZDJMqzX",
    [
      {
        id: "ws_aug",
        hotelId: "recgMYovrrZDJMqzX",
        title: "August Seminar",
        eventStartDate: "2027-08-15",
        eventEndDate: "2027-08-16",
      },
    ],
    {
      needPeriods: testPeriods,
      hotelNeedDateConfig: {
        hotelId: "recgMYovrrZDJMqzX",
        status: "MARKET_ESTIMATED",
        confirmedPeriodCount: 0,
        estimatedPeriodCount: 4,
      },
    },
  );
  if (
    waterstone[0]?.needDateIndicator?.state === "MATCHES_NEED_DATE" ||
    waterstone[0]?.needDateIndicator?.state === "VERIFIED_DATE_MATCH" ||
    waterstone[0]?.needDateIndicator?.underlyingState === "VERIFIED_DATE_MATCH"
  ) {
    failures.push({
      id: "test_scenario_exclusion",
      detail: "TEST_SCENARIO must not drive verified customer-facing icons",
      got: waterstone[0].needDateIndicator,
    });
  }
  if (!waterstone[0]?.needDateIndicator?.icon) {
    failures.push({
      id: "test_scenario_still_has_indicator",
      detail: "estimated hotel must still show an opportunity calendar SVG",
      got: waterstone[0]?.needDateIndicator,
    });
  }

  // No need periods → customer Not yet assessed (underlying HOTEL_NEED_DATES_NOT_ESTABLISHED)
  const none = attachHotelNeedDateTilePreviewsSync("hotel_none", [
    { id: "x", title: "Anything", eventStartDate: "2027-01-01" },
  ], {
    needPeriods: [],
    hotelNeedDateConfig: {
      hotelId: "hotel_none",
      status: "NOT_ESTABLISHED",
      confirmedPeriodCount: 0,
      estimatedPeriodCount: 0,
    },
  });
  if (
    none[0]?.needDateIndicator?.state !== "NOT_YET_ASSESSED" ||
    none[0]?.needDateIndicator?.icon !== "calendar-question" ||
    none[0]?.needDateIndicator?.underlyingState !==
      "HOTEL_NEED_DATES_NOT_ESTABLISHED"
  ) {
    failures.push({
      id: "no_need_periods",
      detail:
        "expected customer NOT_YET_ASSESSED calendar-question (underlying HOTEL_NEED_DATES_NOT_ESTABLISHED)",
      got: none[0]?.needDateIndicator,
    });
  }

  // Hotel scope: same org/dates at another hotel with different needs → NO_DATE_MATCH
  const otherHotelPeriods = [
    {
      needPeriodId: "ndp_other_2027_07",
      hotelId: "hotel_other",
      periodLabel: "July 2027",
      startDate: "2027-07-01",
      endDate: "2027-07-31",
      status: NEED_PERIOD_STATUS.ACTIVE,
      source: "HOTEL_REPORTED",
      revenueNeed: [],
    },
  ];
  const other = attachHotelNeedDateTilePreviewsSync(
    "hotel_other",
    [
      {
        id: "opp_verified",
        hotelId: "hotel_other",
        title: "Regional Scientific Symposium 2027",
        eventStartDate: "2027-01-14",
        eventEndDate: "2027-01-16",
      },
    ],
    {
      needPeriods: otherHotelPeriods,
      hotelNeedDateConfig: {
        hotelId: "hotel_other",
        status: "HOTEL_CONFIRMED",
        confirmedPeriodCount: 1,
        estimatedPeriodCount: 0,
      },
    },
  );
  if (
    other[0]?.needDateIndicator?.state !== "NO_CURRENT_MATCH" ||
    other[0]?.needDateIndicator?.underlyingState !== "NO_DATE_MATCH"
  ) {
    failures.push({
      id: "hotel_scope",
      detail: "January event must not match July-only hotel needs",
      got: other[0]?.needDateIndicator,
    });
  }

  // Drawer builder still works for verified
  const drawer = buildNeedDateMatchDrawerComponent({
    needPeriod: periods[0],
    timingMatch: {
      overlapStatus: NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP,
      demandStartDate: "2027-01-14",
      demandEndDate: "2027-01-16",
    },
    opportunity: cases[0].opp,
  });
  if (!drawer || drawer.title !== "Hotel Need-Date Match") {
    failures.push({ id: "drawer_component", detail: drawer });
  }

  return {
    pass: failures.length === 0,
    failures,
    caseCount: cases.length + 3,
  };
}
