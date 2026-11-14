/**
 * Permanent GDI commercial-actionability gate V1.
 *
 * Complements commercial-expiration-gate-v1: temporal status alone is not enough.
 * A future date does not imply an open hotel booking opportunity; a past
 * announcement does not expire multiyear project demand.
 *
 * Active Hotel Market requires a credible current/future hotel-specific commercial
 * opportunity AND a realistic sales action today.
 */

import {
  GDI_COMMERCIAL_EXPIRATION_GATE_V1,
  COMMERCIAL_TEMPORAL_DISPOSITION,
  classifyCommercialTemporalStatus,
  applyCommercialExpirationGate,
  assertCustomerInventoryPassesExpirationGate,
  RESEARCH_WATCH_DISPOSITIONS,
} from "./commercial-expiration-gate-v1.js";

export const GDI_COMMERCIAL_ACTIONABILITY_GATE_V1 =
  "gdi-commercial-actionability-gate-v1";

export const GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION = "1.0.0";

/** Founder-facing assessed states (reuse temporal where 1:1). */
export const COMMERCIAL_ACTIONABILITY_STATE = Object.freeze({
  ACTIONABLE_CURRENT_OR_FUTURE: "ACTIONABLE_CURRENT_OR_FUTURE",
  ONGOING_ACCOUNT_DEVELOPMENT: "ONGOING_ACCOUNT_DEVELOPMENT",
  FUTURE_CYCLE_VERIFIED: "FUTURE_CYCLE_VERIFIED",
  FUTURE_CYCLE_RESEARCH_REQUIRED: "FUTURE_CYCLE_RESEARCH_REQUIRED",
  SOURCING_CLOSED_OR_COMMITTED: "SOURCING_CLOSED_OR_COMMITTED",
  EXPIRED_ONE_TIME_EVENT: "EXPIRED_ONE_TIME_EVENT",
  DATE_UNVERIFIED: "DATE_UNVERIFIED",
  HISTORICAL_RESEARCH_ONLY: "HISTORICAL_RESEARCH_ONLY",
  NOT_CURRENTLY_ACTIONABLE: "NOT_CURRENTLY_ACTIONABLE",
});

/** Active Hotel Market membership. */
export const ACTIVE_HOTEL_MARKET_STATES = Object.freeze([
  COMMERCIAL_ACTIONABILITY_STATE.ACTIONABLE_CURRENT_OR_FUTURE,
  COMMERCIAL_ACTIONABILITY_STATE.ONGOING_ACCOUNT_DEVELOPMENT,
  COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_VERIFIED,
]);

/** Customer-visible Competitive (CMI) may use SOURCING_CLOSED_OR_COMMITTED. */
export const ACTIVE_COMPETITIVE_MARKET_STATES = Object.freeze([
  COMMERCIAL_ACTIONABILITY_STATE.SOURCING_CLOSED_OR_COMMITTED,
  COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_VERIFIED,
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function parseIsoDate(v) {
  const s = text(v);
  if (!s || /^(dates?\s*tbd|ongoing|timing\s*tbd|tbd)$/i.test(s)) return null;
  const m = s.match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

/**
 * Detect research/Exa discovery dates wrongly stored as event start.
 * Heuristic: YYYY-MM-DD within ±3 days of reference AND title/year conflict,
 * or explicit audit flag.
 */
export function isLikelyResearchTimestampContamination(opp = {}, referenceDate) {
  if (opp.commercialExpirationGateV1?.storedIsResearchTimestamp === true) {
    return true;
  }
  if (opp.storedIsResearchTimestamp === true) return true;
  const ref = parseIsoDate(referenceDate);
  const start =
    parseIsoDate(opp.eventStartDate) || parseIsoDate(opp.tileEventStartDate);
  if (!ref || !start) return false;
  // Pill says Dates TBD but ISO start equals discovery-ish date
  if (/dates?\s*tbd/i.test(text(opp.datePillDisplayLabel)) && start) {
    const refMs = Date.parse(ref);
    const startMs = Date.parse(start);
    if (Number.isFinite(refMs) && Number.isFinite(startMs)) {
      const days = Math.abs(refMs - startMs) / 86400000;
      if (days <= 14) return true;
    }
  }
  return false;
}

function mapTemporalToActionability(temporal, hints = {}) {
  if (hints.actionabilityOverride) {
    return hints.actionabilityOverride;
  }
  if (hints.competitorCommitted === true || hints.cmiVerified === true) {
    return COMMERCIAL_ACTIONABILITY_STATE.SOURCING_CLOSED_OR_COMMITTED;
  }
  if (hints.nearTermSourcingClosed === true) {
    return COMMERCIAL_ACTIONABILITY_STATE.NOT_CURRENTLY_ACTIONABLE;
  }
  switch (temporal.disposition) {
    case COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_FUTURE_EVENT:
      return COMMERCIAL_ACTIONABILITY_STATE.ACTIONABLE_CURRENT_OR_FUTURE;
    case COMMERCIAL_TEMPORAL_DISPOSITION.ACTIVE_ONGOING_DEMAND:
      return COMMERCIAL_ACTIONABILITY_STATE.ONGOING_ACCOUNT_DEVELOPMENT;
    case COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_VERIFIED:
      return COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_VERIFIED;
    case COMMERCIAL_TEMPORAL_DISPOSITION.FUTURE_CYCLE_RESEARCH_REQUIRED:
      return COMMERCIAL_ACTIONABILITY_STATE.FUTURE_CYCLE_RESEARCH_REQUIRED;
    case COMMERCIAL_TEMPORAL_DISPOSITION.EXPIRED_ONE_TIME_EVENT:
      return COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT;
    case COMMERCIAL_TEMPORAL_DISPOSITION.DATE_UNVERIFIED:
      return COMMERCIAL_ACTIONABILITY_STATE.DATE_UNVERIFIED;
    case COMMERCIAL_TEMPORAL_DISPOSITION.HISTORICAL_ACCOUNT_DEVELOPMENT:
      return COMMERCIAL_ACTIONABILITY_STATE.HISTORICAL_RESEARCH_ONLY;
    case COMMERCIAL_TEMPORAL_DISPOSITION.DUPLICATE_EVENT_RECORD:
      return COMMERCIAL_ACTIONABILITY_STATE.HISTORICAL_RESEARCH_ONLY;
    default:
      return COMMERCIAL_ACTIONABILITY_STATE.NOT_CURRENTLY_ACTIONABLE;
  }
}

/**
 * Classify commercial actionability for one opportunity.
 */
export function classifyCommercialActionability(opportunity = {}, opts = {}) {
  const ref =
    parseIsoDate(opts.referenceDate) || new Date().toISOString().slice(0, 10);
  const contaminated = isLikelyResearchTimestampContamination(opportunity, ref);
  const temporal = classifyCommercialTemporalStatus(opportunity, {
    ...opts,
    referenceDate: ref,
    storedIsResearchTimestamp:
      opts.storedIsResearchTimestamp === true || contaminated,
    actualEventStart: opts.actualEventStart || null,
    actualEventEnd: opts.actualEventEnd || null,
    dispositionOverride: opts.dispositionOverride || null,
  });

  const cmiVerified =
    opportunity.cmiVerified === true ||
    opts.cmiVerified === true ||
    upper(opportunity.commercialActionability) ===
      "COMPETITIVE_MARKET_INTELLIGENCE";
  const competitorCommitted =
    opts.competitorCommitted === true ||
    cmiVerified ||
    /competitor (?:venue|host|block) confirmed/i.test(
      text(opportunity.hotelSourcingStatusLabel),
    );

  const state = mapTemporalToActionability(temporal, {
    actionabilityOverride: opts.actionabilityOverride || null,
    competitorCommitted,
    cmiVerified,
    nearTermSourcingClosed: opts.nearTermSourcingClosed === true,
  });

  const activeHotelMarketEligible =
    ACTIVE_HOTEL_MARKET_STATES.includes(state) &&
    !competitorCommitted &&
    opts.forceExcludeFromHotel !== true;

  const activeCompetitiveEligible =
    competitorCommitted &&
    (ACTIVE_COMPETITIVE_MARKET_STATES.includes(state) ||
      state === COMMERCIAL_ACTIONABILITY_STATE.SOURCING_CLOSED_OR_COMMITTED);

  const researchWatchOnly =
    !activeHotelMarketEligible && !activeCompetitiveEligible;

  return {
    version: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    contractVersion: GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
    referenceDate: ref,
    state,
    temporalDisposition: temporal.disposition,
    temporal,
    researchTimestampContamination: contaminated,
    competitorCommitted,
    activeHotelMarketEligible,
    activeCompetitiveEligible,
    researchWatchOnly,
    customerVisibleEligible: activeHotelMarketEligible || activeCompetitiveEligible,
    reason: opts.reason || temporal.provenance || null,
    actualEventStart: opts.actualEventStart || temporal.actualEventStart || null,
    actualEventEnd: opts.actualEventEnd || temporal.actualEventEnd || null,
  };
}

/**
 * Apply gate across inventory. Preserves all rows; partitions visibility.
 */
export function applyCommercialActionabilityGate(opportunities = [], opts = {}) {
  const rows = [];
  const activeHotel = [];
  const activeCompetitive = [];
  const researchWatch = [];

  for (const opp of opportunities) {
    const hint = (opts.hintsById && opts.hintsById[opp.id]) || {};
    const classification = classifyCommercialActionability(opp, {
      ...opts,
      ...hint,
    });

    let next = {
      ...opp,
      commercialActionabilityStateV1: classification.state,
      commercialActionabilityGateV1: classification,
      commercialTemporalDisposition: classification.temporalDisposition,
      commercialExpirationGateV1: classification.temporal,
    };

    if (hint.datePillDisplayLabel != null) {
      next.datePillDisplayLabel = hint.datePillDisplayLabel;
      next.eventDateDisplay = hint.datePillDisplayLabel;
    }
    if (hint.actualEventStart) {
      next.eventStartDate = hint.actualEventStart;
      next.tileEventStartDate = hint.actualEventStart;
    }
    if (hint.actualEventEnd) {
      next.eventEndDate = hint.actualEventEnd;
      next.tileEventEndDate = hint.actualEventEnd;
    }
    if (hint.clearContaminatedEventDates || classification.researchTimestampContamination) {
      if (!hint.actualEventStart) {
        next.eventStartDate = null;
        next.tileEventStartDate = null;
        if (!hint.datePillDisplayLabel) {
          next.datePillDisplayLabel = "Dates TBD";
          next.eventDateDisplay = "Dates TBD";
        }
      }
    }

    // tileBadgePlan is preferred by the customer tile renderer for date pills.
    // Contaminated tileEventStartDate / stale plan labels must be rewritten or
    // the UI will show research/publication dates even when top-level fields are clean.
    if (
      next.tileBadgePlan &&
      typeof next.tileBadgePlan === "object" &&
      (hint.datePillDisplayLabel != null ||
        hint.actualEventStart ||
        hint.actualEventEnd ||
        hint.clearContaminatedEventDates ||
        classification.researchTimestampContamination)
    ) {
      const pill =
        next.datePillDisplayLabel ||
        hint.datePillDisplayLabel ||
        next.tileBadgePlan.datePillDisplayLabel ||
        "Dates TBD";
      const start =
        hint.actualEventStart ||
        (hint.clearContaminatedEventDates ||
        classification.researchTimestampContamination
          ? null
          : next.tileEventStartDate || next.tileBadgePlan.tileEventStartDate) ||
        null;
      const end =
        hint.actualEventEnd ||
        (hint.clearContaminatedEventDates ||
        classification.researchTimestampContamination
          ? null
          : next.tileEventEndDate || next.tileBadgePlan.tileEventEndDate) ||
        null;
      next.tileBadgePlan = {
        ...next.tileBadgePlan,
        datePillDisplayLabel: pill,
        datePillLabel: pill,
        showDatePill: true,
        hasVerifiedEventDate: Boolean(start) && !/^dates?\s*tbd$/i.test(pill),
        tileEventStartDate: start,
        tileEventEndDate: end,
        canonicalTiming: {
          ...(next.tileBadgePlan.canonicalTiming || {}),
          startDate: start,
          endDate: end,
          displayLabel: pill,
          dateStatus: /^ongoing$/i.test(pill)
            ? "ONGOING"
            : start
              ? "VERIFIED"
              : "UNKNOWN",
          occurrences: start
            ? [{ startDate: start, endDate: end || start, historical: false }]
            : [],
        },
      };
    }

    // Market Scope from actionability (do not leave expired in Hotel)
    if (classification.activeCompetitiveEligible) {
      next.hotelMarketScopeEligible = false;
      next.competitiveMarketScopeEligible = true;
      next.inventoryPopulations = ["COMPETITIVE_MARKET_INTELLIGENCE"];
      next.inventoryPopulationPrimary = "COMPETITIVE_MARKET_INTELLIGENCE";
    } else if (classification.activeHotelMarketEligible) {
      next.hotelMarketScopeEligible = true;
      next.competitiveMarketScopeEligible = false;
      next.inventoryPopulations = ["SALES_OPPORTUNITIES"];
      next.inventoryPopulationPrimary = "SALES_OPPORTUNITIES";
    } else {
      next.hotelMarketScopeEligible = false;
      next.competitiveMarketScopeEligible = false;
      next.inventoryPopulations = [];
      next.inventoryPopulationPrimary = null;
      next.customerVisible = false;
      next.researchWatchOnly = true;
    }

    rows.push(next);
    if (classification.activeHotelMarketEligible) activeHotel.push(next);
    else if (classification.activeCompetitiveEligible) activeCompetitive.push(next);
    else researchWatch.push(next);
  }

  const byState = rows.reduce((m, r) => {
    const s = r.commercialActionabilityStateV1;
    m[s] = (m[s] || 0) + 1;
    return m;
  }, {});

  return {
    version: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    contractVersion: GDI_COMMERCIAL_ACTIONABILITY_CONTRACT_VERSION,
    referenceDate: opts.referenceDate || null,
    counts: {
      input: opportunities.length,
      activeHotelMarket: activeHotel.length,
      activeCompetitive: activeCompetitive.length,
      researchWatch: researchWatch.length,
      customerVisible: activeHotel.length + activeCompetitive.length,
      byState,
    },
    activeHotel,
    activeCompetitive,
    researchWatch,
    customerVisible: [...activeHotel, ...activeCompetitive],
    rows,
  };
}

/**
 * Fail closed: expired / non-actionable must not remain in Hotel Market.
 */
export function assertCustomerInventoryPassesActionabilityGate(
  opportunities = [],
  opts = {},
) {
  const blockers = [];
  for (const opp of opportunities) {
    if (opp.hotelMarketScopeEligible !== true) continue;
    const c = classifyCommercialActionability(opp, {
      ...opts,
      ...((opts.hintsById && opts.hintsById[opp.id]) || {}),
    });
    if (!c.activeHotelMarketEligible) {
      blockers.push({
        id: opp.id,
        state: c.state,
        reason: "HOTEL_MARKET_CONTAINS_NON_ACTIONABLE",
      });
    }
    if (c.state === COMMERCIAL_ACTIONABILITY_STATE.EXPIRED_ONE_TIME_EVENT) {
      blockers.push({
        id: opp.id,
        state: c.state,
        reason: "EXPIRED_ONE_TIME_IN_HOTEL_MARKET",
      });
    }
  }
  return {
    ok: blockers.length === 0,
    version: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
    blockers,
  };
}

export {
  GDI_COMMERCIAL_EXPIRATION_GATE_V1,
  COMMERCIAL_TEMPORAL_DISPOSITION,
  classifyCommercialTemporalStatus,
  applyCommercialExpirationGate,
  assertCustomerInventoryPassesExpirationGate,
  RESEARCH_WATCH_DISPOSITIONS,
};

export default {
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  COMMERCIAL_ACTIONABILITY_STATE,
  classifyCommercialActionability,
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
};
