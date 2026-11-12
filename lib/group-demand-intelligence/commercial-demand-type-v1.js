/**
 * GDI Commercial Demand Type V1
 *
 * Founder-facing commercial business type for every opportunity:
 *   GUEST_ROOMS | LOCAL_CATERING | BOTH
 *
 * Maps 1:1 onto existing HOTEL_REVENUE_STREAM_CLASS (multi-revenue-qualification-v1).
 * Does not invent a parallel taxonomy. Icons, filters, and persistence all use
 * the same canonical revenueStreamClass field.
 *
 * Filter inclusion (sales browse):
 *   Guest Rooms  → GUESTROOMS_ONLY + COMBINED
 *   Local Catering → EVENT_FNB_ONLY + COMBINED
 *   Both         → COMBINED only
 *
 * UNDETERMINED / NOT_APPLICABLE → REVENUE_STREAM_UNCONFIRMED (no active icons).
 */

import {
  HOTEL_REVENUE_STREAM_CLASS,
  HOTEL_REVENUE_STREAM_LABEL,
  QUALIFYING_REVENUE_STREAM_CLASSES,
} from "./multi-revenue-qualification-v1.js";
import {
  mapRevenueStreamClassToIconStates,
  resolveOpportunityRevenueStreamIcons,
  GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
  REVENUE_STREAM_ICON_STATE,
} from "./opportunity-revenue-stream-icons-v1.js";

export const GDI_COMMERCIAL_DEMAND_TYPE_V1 = "gdi-commercial-demand-type-v1";

/** Founder / sales-facing demand type (commercial business type). */
export const COMMERCIAL_DEMAND_TYPE = Object.freeze({
  GUEST_ROOMS: "GUEST_ROOMS",
  LOCAL_CATERING: "LOCAL_CATERING",
  BOTH: "BOTH",
  UNDETERMINED: "UNDETERMINED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const COMMERCIAL_DEMAND_TYPE_LABEL = Object.freeze({
  [COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS]: "Guest Rooms",
  [COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING]: "Local Catering",
  [COMMERCIAL_DEMAND_TYPE.BOTH]: "Both",
  [COMMERCIAL_DEMAND_TYPE.UNDETERMINED]: "Undetermined",
  [COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE]: "Not Applicable",
});

/** Accessible SVG icon labels (customer UI). */
export const COMMERCIAL_DEMAND_ICON_LABEL = Object.freeze({
  guestRooms: "Guest Rooms",
  localCatering: "Local Catering",
});

/** Browse filter keys for revenue demand type (single-select subgroup). */
export const COMMERCIAL_DEMAND_TYPE_FILTER = Object.freeze({
  ALL: "",
  GUEST_ROOMS: "GUEST_ROOMS",
  LOCAL_CATERING: "LOCAL_CATERING",
  BOTH: "BOTH",
});

export const COMMERCIAL_DEMAND_TYPE_FILTER_LABEL = Object.freeze({
  [COMMERCIAL_DEMAND_TYPE_FILTER.ALL]: "All",
  [COMMERCIAL_DEMAND_TYPE_FILTER.GUEST_ROOMS]: "Guest Rooms",
  [COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING]: "Local Catering",
  [COMMERCIAL_DEMAND_TYPE_FILTER.BOTH]: "Both",
});

const VALID_REVENUE = new Set(Object.values(HOTEL_REVENUE_STREAM_CLASS));

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/**
 * Map founder demand type → canonical revenue stream class.
 */
export function demandTypeToRevenueStreamClass(demandType) {
  switch (upper(demandType)) {
    case COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS:
    case "GUESTROOMS_ONLY":
    case "ROOMS":
      return HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
    case COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING:
    case "EVENT_FNB_ONLY":
    case "CATERING":
      return HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
    case COMMERCIAL_DEMAND_TYPE.BOTH:
    case "COMBINED_GUESTROOMS_EVENT_FNB":
    case "COMBINED":
      return HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
    case COMMERCIAL_DEMAND_TYPE.UNDETERMINED:
    case COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE:
    case "REVENUE_STREAM_UNCONFIRMED":
    case "UNCONFIRMED":
      return HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
    default:
      return null;
  }
}

/**
 * Map canonical revenue stream class → founder demand type.
 * @param {string} revenueStreamClass
 * @param {{ notApplicable?: boolean }} [opts]
 */
export function revenueStreamClassToDemandType(revenueStreamClass, opts = {}) {
  const cls = upper(revenueStreamClass);
  if (opts.notApplicable === true) return COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE;
  if (cls === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY) {
    return COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS;
  }
  if (cls === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY) {
    return COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING;
  }
  if (cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB) {
    return COMMERCIAL_DEMAND_TYPE.BOTH;
  }
  if (cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED || !cls) {
    return COMMERCIAL_DEMAND_TYPE.UNDETERMINED;
  }
  return COMMERCIAL_DEMAND_TYPE.UNDETERMINED;
}

export function normalizeRevenueStreamClass(raw) {
  const c = upper(raw).replace(/\s+/g, "_");
  if (VALID_REVENUE.has(c)) return c;
  return demandTypeToRevenueStreamClass(c);
}

/**
 * Whether a filter key matches an opportunity's revenueStreamClass.
 * Rooms includes BOTH; Catering includes BOTH; Both is exact COMBINED.
 */
export function matchesCommercialDemandTypeFilter(opp = {}, filterKey = "") {
  if (!filterKey) return true;
  const cls = normalizeRevenueStreamClass(
    opp.revenueStreamClass ||
      opp.applicableHotelRevenueStreams ||
      opp.multiRevenue?.revenueStreamClass ||
      opp.commercialDemandType,
  );
  if (!cls || cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
    return false;
  }
  const key = upper(filterKey);
  if (key === COMMERCIAL_DEMAND_TYPE_FILTER.GUEST_ROOMS) {
    return (
      cls === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY ||
      cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB
    );
  }
  if (key === COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING) {
    return (
      cls === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY ||
      cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB
    );
  }
  if (key === COMMERCIAL_DEMAND_TYPE_FILTER.BOTH) {
    return cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  }
  return false;
}

/**
 * Facet counts for demand-type chips (no double-count within a single chip).
 */
export function countCommercialDemandTypeFacets(opportunities = [], matchOther = null) {
  const base = (opportunities || []).filter((o) =>
    typeof matchOther === "function" ? matchOther(o) : true,
  );
  const counts = {
    [COMMERCIAL_DEMAND_TYPE_FILTER.ALL]: base.length,
    [COMMERCIAL_DEMAND_TYPE_FILTER.GUEST_ROOMS]: 0,
    [COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING]: 0,
    [COMMERCIAL_DEMAND_TYPE_FILTER.BOTH]: 0,
    unconfirmed: 0,
  };
  for (const o of base) {
    if (matchesCommercialDemandTypeFilter(o, COMMERCIAL_DEMAND_TYPE_FILTER.GUEST_ROOMS)) {
      counts[COMMERCIAL_DEMAND_TYPE_FILTER.GUEST_ROOMS] += 1;
    }
    if (matchesCommercialDemandTypeFilter(o, COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING)) {
      counts[COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING] += 1;
    }
    if (matchesCommercialDemandTypeFilter(o, COMMERCIAL_DEMAND_TYPE_FILTER.BOTH)) {
      counts[COMMERCIAL_DEMAND_TYPE_FILTER.BOTH] += 1;
    }
    const cls = normalizeRevenueStreamClass(
      o.revenueStreamClass || o.applicableHotelRevenueStreams,
    );
    if (!cls || cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
      counts.unconfirmed += 1;
    }
  }
  return counts;
}

/**
 * Whether icons should render as commercially active (not muted for DQ/quarantine).
 */
export function shouldDisplayCommercialDemandIcons(opp = {}) {
  const priority = upper(opp.priority || opp.priorityFilterKey);
  if (priority === "DISQUALIFIED") return false;
  if (opp.quarantined === true || opp.qualityQuarantine === true) return false;
  if (upper(opp.commercialEventFamilyRole) === "QUARANTINED") return false;
  if (/TITLE_SOURCE_INVALID/i.test(text(opp.historicalRecoveryDisposition || opp.titleDefect))) {
    return false;
  }
  const cls = normalizeRevenueStreamClass(
    opp.revenueStreamClass || opp.applicableHotelRevenueStreams,
  );
  if (!cls || cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
    return false;
  }
  return QUALIFYING_REVENUE_STREAM_CLASSES.includes(cls);
}

/**
 * Build persistence fields for an opportunity from a demand-type classification result.
 */
export function buildCommercialDemandTypePersistFields(classification = {}) {
  const revenueStreamClass =
    normalizeRevenueStreamClass(classification.revenueStreamClass) ||
    demandTypeToRevenueStreamClass(classification.commercialDemandType) ||
    HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  const commercialDemandType =
    classification.commercialDemandType ||
    revenueStreamClassToDemandType(revenueStreamClass, {
      notApplicable: classification.notApplicable === true,
    });
  const guestroomThesis = text(classification.guestroomThesis) || null;
  const eventFnbThesis = text(classification.eventFnbThesis) || null;
  const icons = mapRevenueStreamClassToIconStates(revenueStreamClass);
  const displayIcons = shouldDisplayCommercialDemandIcons({
    ...classification,
    revenueStreamClass,
    priority: classification.priority,
  })
    ? icons
    : {
        guestrooms: REVENUE_STREAM_ICON_STATE.MUTED,
        eventFnb: REVENUE_STREAM_ICON_STATE.MUTED,
      };

  return {
    commercialDemandType,
    commercialDemandTypeLabel:
      COMMERCIAL_DEMAND_TYPE_LABEL[commercialDemandType] || null,
    revenueStreamClass,
    revenueStreamLabel: HOTEL_REVENUE_STREAM_LABEL[revenueStreamClass],
    applicableHotelRevenueStreams: revenueStreamClass,
    applicableHotelRevenueStreamsLabel: HOTEL_REVENUE_STREAM_LABEL[revenueStreamClass],
    guestroomThesis,
    eventFnbThesis,
    hotelDemandHypotheses: {
      guestroomHypothesis: guestroomThesis,
      meetingsHypothesis: eventFnbThesis,
      cateringHypothesis:
        commercialDemandType === COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING ||
        commercialDemandType === COMMERCIAL_DEMAND_TYPE.BOTH
          ? eventFnbThesis
          : null,
      primaryMechanism: text(classification.primaryMechanism) || null,
    },
    guestroomDemandConfidence: classification.guestroomConfidence || null,
    eventFnbDemandConfidence: classification.eventFnbConfidence || null,
    commercialDemandTypeEvidence: {
      version: GDI_COMMERCIAL_DEMAND_TYPE_V1,
      guestroomRationale: text(classification.guestroomRationale) || guestroomThesis,
      cateringRationale: text(classification.cateringRationale) || eventFnbThesis,
      guestroomConfidence: classification.guestroomConfidence || null,
      eventFnbConfidence: classification.eventFnbConfidence || null,
      sources: Array.isArray(classification.sources) ? classification.sources : [],
      classifiedAt:
        classification.classifiedAt || new Date().toISOString(),
      classifier: classification.classifier || GDI_COMMERCIAL_DEMAND_TYPE_V1,
      qualityState: classification.qualityState || null,
      notes: classification.notes || null,
    },
    revenueStreamIcons: displayIcons,
    revenueStreamIconsAria: {
      guestrooms:
        displayIcons.guestrooms === REVENUE_STREAM_ICON_STATE.ACTIVE
          ? `${COMMERCIAL_DEMAND_ICON_LABEL.guestRooms} — applicable`
          : COMMERCIAL_DEMAND_ICON_LABEL.guestRooms,
      eventFnb:
        displayIcons.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE
          ? `${COMMERCIAL_DEMAND_ICON_LABEL.localCatering} — applicable`
          : COMMERCIAL_DEMAND_ICON_LABEL.localCatering,
      group: `Commercial demand: ${COMMERCIAL_DEMAND_TYPE_LABEL[commercialDemandType]}`,
    },
    revenueStreamIconsVersion: GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
    commercialDemandTypeClassifiedAt:
      classification.classifiedAt || new Date().toISOString(),
  };
}

/**
 * Quality gate: actionable records must have a qualifying demand type with evidence.
 */
export function evaluateCommercialDemandTypeQuality(opp = {}, ctx = {}) {
  const priority = upper(opp.priority || opp.priorityFilterKey);
  const familyRole = upper(ctx.familyRole || opp.commercialEventFamilyRole);
  const disqualified =
    priority === "DISQUALIFIED" ||
    familyRole === "QUARANTINED" ||
    opp.quarantined === true;
  const cls = normalizeRevenueStreamClass(
    opp.revenueStreamClass || opp.applicableHotelRevenueStreams,
  );
  const demandType = revenueStreamClassToDemandType(cls, {
    notApplicable: disqualified,
  });
  const guestThesis = text(
    opp.guestroomThesis || opp.hotelDemandHypotheses?.guestroomHypothesis,
  );
  const eventThesis = text(
    opp.eventFnbThesis ||
      opp.hotelDemandHypotheses?.meetingsHypothesis ||
      opp.hotelDemandHypotheses?.cateringHypothesis,
  );
  const blockers = [];

  if (disqualified || familyRole === "LINKED_RESEARCH") {
    // Unconfirmed / muted icons expected — do not fail actionable gate
    if (
      cls &&
      cls !== HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED &&
      shouldDisplayCommercialDemandIcons(opp) === false
    ) {
      // OK: class may be stored for audit but icons muted
    }
    return {
      ok: true,
      axis: "COMMERCIAL_DEMAND_TYPE",
      commercialDemandType: demandType,
      revenueStreamClass: cls || HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      actionable: false,
      blockers: [],
      detail: { exempt: true, reason: disqualified ? "DISQUALIFIED_OR_QUARANTINED" : familyRole },
    };
  }

  const actionable =
    priority === "HIGH_PRIORITY" ||
    priority === "MEDIUM_PRIORITY" ||
    opp.inventorySalesReady === true ||
    opp.customerVisible === true;

  if (actionable) {
    if (!cls || cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
      blockers.push("MISSING_DEMAND_CLASSIFICATION");
    }
    if (
      cls === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY &&
      !guestThesis
    ) {
      blockers.push("GUEST_ROOMS_WITHOUT_EVIDENCE");
    }
    if (
      cls === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY &&
      !eventThesis
    ) {
      blockers.push("LOCAL_CATERING_WITHOUT_EVIDENCE");
    }
    if (cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB) {
      if (!guestThesis) blockers.push("BOTH_MISSING_ROOM_EVIDENCE");
      if (!eventThesis) blockers.push("BOTH_MISSING_CATERING_EVIDENCE");
    }
  }

  // Icon/DTO consistency when class persisted
  if (cls && QUALIFYING_REVENUE_STREAM_CLASSES.includes(cls)) {
    const resolved = resolveOpportunityRevenueStreamIcons(opp);
    if (resolved.revenueStreamClass !== cls && actionable) {
      // Consistency guard may mute — only fail hard mismatches on BOTH without evidence
      if (
        cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB &&
        resolved.revenueStreamClass !== cls
      ) {
        blockers.push("BOTH_ICON_EVIDENCE_MISMATCH");
      }
    }
  }

  return {
    ok: blockers.length === 0,
    axis: "COMMERCIAL_DEMAND_TYPE",
    commercialDemandType: demandType,
    revenueStreamClass: cls || null,
    actionable,
    blockers,
    detail: {
      guestroomThesis: guestThesis || null,
      eventFnbThesis: eventThesis || null,
      iconsDisplayable: shouldDisplayCommercialDemandIcons(opp),
    },
  };
}

export {
  HOTEL_REVENUE_STREAM_CLASS,
  HOTEL_REVENUE_STREAM_LABEL,
  QUALIFYING_REVENUE_STREAM_CLASSES,
};
