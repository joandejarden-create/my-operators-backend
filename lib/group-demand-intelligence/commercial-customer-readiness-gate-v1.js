/**
 * Commercial customer-readiness gate V1.
 *
 * Structural ten-section drawers ≠ commercial readiness.
 * Distinguishes:
 *   researchComplete · contactResearchComplete · commercialQualificationComplete · customerReady
 *
 * Dispositions: SALES_READY | RESEARCH_CONTINUATION_REQUIRED | WATCH | NOT_CUSTOMER_READY
 *
 * Zero paid. Does not mutate customer visibility by itself.
 */
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { assessContactResearchCompletion } from "./contact-research-completion-gate-v1.js";
import { classifyCommercialDemandThesis } from "./commercial-demand-thesis-standard-v1.js";
import { CONTACT_COMMERCIAL_DISPOSITION } from "./research/universal-contact-research-contract-v1.js";

export const COMMERCIAL_CUSTOMER_READINESS_GATE_V1 =
  "gdi-commercial-customer-readiness-gate-v1";

export const CUSTOMER_READINESS_DISPOSITION = CONTACT_COMMERCIAL_DISPOSITION;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function parseDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

function classifyTiming(o = {}, asOf = new Date()) {
  const start = parseDate(o.eventStartDate || o.startDate || o.needDateStart);
  const end = parseDate(o.eventEndDate || o.endDate || o.needDateEnd);
  const undated =
    !start &&
    !end &&
    !/ongoing|recurring|relationship|tbd|dates?\s*tbd/i.test(
      `${o.eventDateDisplay || ""} ${o.title || ""}`,
    );
  const recurring = /recurring|ongoing|pipeline|relationship|future.?cycle/i.test(
    `${o.opportunityType || ""} ${o.eventDateDisplay || ""} ${o.title || ""} ${o.whyMonitor || ""}`,
  );
  if (end && end < asOf && !recurring) {
    return { timingClass: "HISTORICAL", start, end, recurring: false };
  }
  if (start && start < asOf && (!end || end < asOf) && !recurring) {
    return { timingClass: "HISTORICAL", start, end, recurring: false };
  }
  if (recurring || /FUTURE_CYCLE|CURRENT_CYCLE_CLOSED/i.test(`${o.opportunityType || ""} ${o.venueSourcingStatus || ""}`)) {
    return {
      timingClass: end && end < asOf ? "HISTORICAL_WITH_FUTURE_CYCLE_MONITOR" : "RECURRING_OR_FUTURE_CYCLE",
      start,
      end,
      recurring: true,
    };
  }
  if (undated) return { timingClass: "UNDATED", start, end, recurring: false };
  return { timingClass: "FUTURE_OR_CURRENT", start, end, recurring: false };
}

/**
 * Detect demand theses that are speculative or commercially inverted
 * (e.g. competitor meeting-space product pages framed as hotel demand).
 */
export function assessDemandMechanismQuality(opportunity = {}) {
  const blob = [
    opportunity.whatIsHappening,
    opportunity.summaryWhat,
    opportunity.hotelOpportunityThesis,
    opportunity.proposedHotelUse,
    opportunity.localPresence,
    opportunity.datedGrowthEvidence,
    opportunity.potentialBusinessForThisHotel,
    opportunity.salesBrief?.potentialBusinessForThisHotel,
    opportunity.salesBrief?.companyAndLocalActivity,
    JSON.stringify(opportunity.evidenceUrls || []).slice(0, 500),
  ]
    .map(text)
    .join(" ");

  const competingMeetingSpace =
    /meeting rooms? & conference|conference room rentals?|virtual office|coworking|co-working|day.?office|serviced office/i.test(
      blob,
    ) &&
    /alliancevirtualoffices|meeting-room|conference-room/i.test(blob);

  const officeListingOnly =
    /office footprint|local office \/ workspace|workspace footprint was publicly announced/i.test(
      blob,
    ) &&
    !/room block|rfp|preferred.?hotel|visiting (staff|engineers|customers)|overnight|lodging programme|housing/i.test(
      blob,
    );

  const sameMarketOnly =
    /sits in the same commercial market/i.test(blob) &&
    !/confirmed|documented (overflow|block|programme|housing)|lease|tournament housing|Stay Smart|official hotel/i.test(
      blob,
    );

  const geographyUnknown =
    /event geography not confirmed|geography unknown|location.?unknown|outside realistic territory/i.test(
      `${opportunity.eventLocationSummary || ""} ${opportunity.demandTerritoryFit || ""} ${blob}`,
    );

  const historicalClosed =
    /CURRENT_CYCLE_CLOSED|FULLY_PLACED|Venue Confirmed|TOO_EARLY/i.test(
      `${opportunity.venueSourcingStatus || ""} ${opportunity.qualificationFailureReason || ""} ${opportunity.priorityReason || ""}`,
    );

  let demandClass = "CREDIBLE_OR_RELIABLE_INFERRED";
  let credible = true;
  const reasons = [];

  const competitorVenueDayEvent =
    /Fontainebleau|brand wall for Global Sales Kickoff/i.test(blob) &&
    !/Waterstone.*(room|lodging|overnight|block)/i.test(blob);

  if (competingMeetingSpace) {
    demandClass = "COMPETING_MEETING_SPACE_PRODUCT";
    credible = false;
    reasons.push(
      "Evidence describes competing meeting-space / virtual-office product sales — not Waterstone hotel demand",
    );
  } else if (competitorVenueDayEvent) {
    demandClass = "THIN_DAY_EVENT_OR_COMPETITOR_VENUE";
    credible = false;
    reasons.push(
      "Evidence describes day-event / competitor-venue production, not Waterstone overnight demand",
    );
  } else if (officeListingOnly && sameMarketOnly) {
    demandClass = "SPECULATIVE_OFFICE_FOOTPRINT";
    credible = false;
    reasons.push(
      "Office/workspace listing without evidenced lodging, visitor, or training hotel demand",
    );
  } else if (sameMarketOnly && !opportunity.primaryContact?.name) {
    demandClass = "WEAK_SAME_MARKET_INFERENCE";
    credible = false;
    reasons.push("Demand thesis is geography co-location only");
  }

  let sharedThesisClass = null;
  let speculativeShared = false;
  try {
    const thesis = classifyCommercialDemandThesis(opportunity);
    sharedThesisClass = thesis?.thesisStandard || thesis?.class || null;
    speculativeShared =
      thesis?.mustRemainResearchContinuation === true ||
      sharedThesisClass === "SPECULATIVE_DEMAND";
    // Shared SPECULATIVE alone is not fatal — it blocks SALES_READY but may
    // remain RESEARCH_CONTINUATION when a hotel-demand pathway is still plausible.
    if (speculativeShared && credible && !competingMeetingSpace && !officeListingOnly) {
      demandClass = "SPECULATIVE_NEEDS_VERIFICATION";
      reasons.push(
        "Demand thesis still speculative — continue research; do not treat as sales-ready",
      );
    }
  } catch {
    /* thesis classifier optional */
  }

  return {
    credibleHotelDemand: credible,
    /** Plausible enough to keep researching (false only for inverted/empty theses). */
    researchWorthy:
      (credible || (!competingMeetingSpace && !officeListingOnly)) &&
      !competitorVenueDayEvent
        ? true
        : !competingMeetingSpace,
    demandClass,
    competingMeetingSpace,
    competitorVenueDayEvent,
    officeListingOnly,
    sameMarketOnly,
    speculativeShared,
    sharedThesisClass,
    geographyUnknown,
    historicalClosed,
    reasons,
  };
}

function namedPrimary(o = {}, contactGate = {}) {
  const name =
    contactGate.primaryName ||
    o.primaryContact?.name ||
    o.contactName ||
    null;
  return looksLikePersonName(name) ? text(name) : null;
}

function actionSupported(o = {}) {
  const action = text(
    o.recommendedAction ||
      o.salesActionRecommended ||
      o.suggestedAction ||
      "",
  );
  if (action.length < 40) return false;
  if (/introduce Bethesda Marriott|NIH\s*\/\s*Metro/i.test(action)) return false;
  // Explicit research-continuation copy cannot satisfy Sales Ready action gate
  if (
    /Continue contact\/demand research before sales assignment/i.test(action)
  ) {
    return false;
  }
  if (
    /Contact the workplace \/ HR \/ facilities or travel owner via the official company contact path/i.test(
      action,
    ) &&
    !namedPrimary(o)
  ) {
    // Generic boilerplate with no named person — weak support
    return false;
  }
  return true;
}

/**
 * Full commercial customer-readiness assessment for one opportunity.
 */
export function assessCommercialCustomerReadiness(
  opportunity = {},
  { hotelId = null, asOf = new Date() } = {},
) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const contact = assessContactResearchCompletion(o);
  let eligibility = null;
  try {
    eligibility = evaluateSharedCommercialEligibility(o, {
      hotelId: hotelId || o.hotelId || null,
    });
  } catch (err) {
    eligibility = {
      status: "RESEARCH_CONTINUATION",
      salesReady: false,
      gaps: ["ELIGIBILITY_EVAL_ERROR"],
      error: String(err?.message || err),
    };
  }

  const demand = assessDemandMechanismQuality(o);
  const timing = classifyTiming(o, asOf);
  const primaryName = namedPrimary(o, contact);
  const secondaryName = contact.secondaryRelevantPersonFound
    ? contact.secondaryName
    : null;
  const authority = contact.hotelSourcingAuthorityVerified === true;
  const orgRouteOnly =
    !primaryName && contact.evidenceClass === "ORGANIZATION_ROUTE_ONLY";
  const actionOk = actionSupported(o);

  const researchComplete = Boolean(
    o.researchRunId ||
      o.researchRunIds?.length ||
      o.deepResearchRunId ||
      o.commercialIntelligenceBrief ||
      o.salesBrief ||
      String(o.id || "").startsWith("gdi_ws_v72_") ||
      (Array.isArray(o.sources) && o.sources.length > 0),
  );

  const contactResearchComplete =
    contact.primaryRelevantPersonFound === true &&
    contact.commercialRelevanceSupported === true;

  const commercialQualificationComplete =
    demand.credibleHotelDemand &&
    !demand.geographyUnknown &&
    timing.timingClass !== "HISTORICAL" &&
    (eligibility?.salesReady === true ||
      (primaryName && demand.credibleHotelDemand && actionOk));

  // Disposition rules (strict — structural drawer never overrides)
  let disposition = CUSTOMER_READINESS_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
  const rationale = [];

  const hardNotReady =
    demand.competingMeetingSpace ||
    demand.demandClass === "SPECULATIVE_OFFICE_FOOTPRINT" ||
    demand.demandClass === "COMPETING_MEETING_SPACE_PRODUCT";

  const thinDayEventWatch =
    demand.competitorVenueDayEvent === true ||
    demand.demandClass === "THIN_DAY_EVENT_OR_COMPETITOR_VENUE";

  if (hardNotReady) {
    disposition = CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push(...demand.reasons);
  } else if (thinDayEventWatch) {
    disposition = CUSTOMER_READINESS_DISPOSITION.WATCH;
    rationale.push(
      ...(demand.reasons.length
        ? demand.reasons
        : [
            "Day-event / competitor-venue evidence — watch for Waterstone overnight demand",
          ]),
    );
  } else if (
    timing.timingClass === "HISTORICAL" &&
    !/FUTURE_CYCLE|relationship expansion|monitor/i.test(
      `${o.opportunityType || ""} ${o.title || ""}`,
    )
  ) {
    disposition = CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push("Event dates are historical with no evidenced future edition");
  } else if (
    timing.timingClass === "HISTORICAL_WITH_FUTURE_CYCLE_MONITOR" ||
    /CURRENT_CYCLE_CLOSED|FULLY_PLACED|FUTURE_CYCLE/i.test(
      `${o.venueSourcingStatus || ""} ${o.opportunityType || ""}`,
    )
  ) {
    disposition = CUSTOMER_READINESS_DISPOSITION.WATCH;
    rationale.push(
      "Current cycle closed / future-cycle monitor — not near-term sales-ready",
    );
  } else if (
    demand.geographyUnknown &&
    !/TERRITORY_CORE|TERRITORY_NEARBY/i.test(String(o.demandTerritoryFit || ""))
  ) {
    disposition = CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push("Hotel geography / event location not commercially established");
  } else if (
    // SALES_READY requires non-speculative credible demand + named contact + action
    !demand.speculativeShared &&
    demand.credibleHotelDemand &&
    primaryName &&
    contact.commercialRelevanceSupported &&
    actionOk &&
    timing.timingClass !== "HISTORICAL" &&
    (eligibility?.salesReady === true || demand.demandClass === "CREDIBLE_OR_RELIABLE_INFERRED")
  ) {
    disposition = CUSTOMER_READINESS_DISPOSITION.SALES_READY;
    rationale.push(
      "Credible demand + named commercially relevant contact + supported action",
    );
  } else if (
    demand.researchWorthy &&
    (demand.speculativeShared || !primaryName || !actionOk)
  ) {
    disposition = CUSTOMER_READINESS_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
    if (demand.speculativeShared) {
      rationale.push(
        "Demand mechanism needs verification before sales-ready (speculative thesis)",
      );
    }
    if (!primaryName) {
      rationale.push("Named commercially relevant contact not yet established");
    }
    if (primaryName && !actionOk) {
      rationale.push("Recommended action still generic / unsupported");
    }
  } else if (!demand.credibleHotelDemand && !demand.researchWorthy) {
    disposition = CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push(
      ...(demand.reasons.length
        ? demand.reasons
        : ["No credible hotel-demand mechanism"]),
    );
  } else if (
    /monitor for fresher|WATCHLIST|too early/i.test(
      `${o.priority || ""} ${o.recommendedAction || ""} ${o.salesActionRecommended || ""} ${o.priorityReason || ""}`,
    )
  ) {
    disposition = CUSTOMER_READINESS_DISPOSITION.WATCH;
    rationale.push("Near-term accessibility or demand weak — watch");
  } else {
    disposition = CUSTOMER_READINESS_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
    rationale.push("Gaps remain before sales-ready");
  }

  // Contact gate alone must never force SALES_READY when demand is hard-not-ready
  if (
    disposition === CUSTOMER_READINESS_DISPOSITION.SALES_READY &&
    (hardNotReady || demand.speculativeShared)
  ) {
    disposition = demand.speculativeShared
      ? CUSTOMER_READINESS_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED
      : CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY;
    rationale.push(
      demand.speculativeShared
        ? "Named contact present but demand still speculative — research continuation"
        : "Contact present but demand thesis not credible — override",
    );
  }

  const customerReady =
    disposition === CUSTOMER_READINESS_DISPOSITION.SALES_READY;

  const revenueClass =
    o.revenueStreamClass ||
    o.revenueStreamLabel ||
    (o.guestroomsApplicable && o.meetingsApplicable
      ? "COMBINED_GUESTROOMS_EVENT_FNB"
      : o.guestroomsApplicable
        ? "GUESTROOMS_ONLY"
        : o.meetingsApplicable
          ? "EVENT_FNB_ONLY"
          : "UNCONFIRMED");

  return {
    version: COMMERCIAL_CUSTOMER_READINESS_GATE_V1,
    opportunityId: o.id || null,
    title: o.title || null,
    organizationName: o.organizationName || null,
    segment: o.segment || o.researchSegment || null,
    disposition,
    rationale,
    layers: {
      researchComplete,
      contactResearchComplete,
      commercialQualificationComplete: Boolean(commercialQualificationComplete),
      customerReady,
      structuralDrawerIrrelevant: true,
    },
    demand,
    timing,
    contact: {
      primaryName,
      secondaryName,
      primaryRelevant: contact.primaryRelevantPersonFound,
      secondaryRelevant: contact.secondaryRelevantPersonFound,
      authorityVerified: authority,
      orgRouteOnly,
      evidenceClass: contact.evidenceClass,
      contactDispositionAlone: contact.commercialDisposition,
    },
    eligibilityStatus: eligibility?.status || null,
    eligibilitySalesReady: eligibility?.salesReady === true,
    eligibilityGaps: eligibility?.gaps || [],
    actionSupported: actionOk,
    revenueClass,
    venueSourcingStatus: o.venueSourcingStatus || null,
    demandTerritoryFit: o.demandTerritoryFit || null,
    recommendBuyerResearch:
      disposition ===
        CUSTOMER_READINESS_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED &&
      demand.researchWorthy !== false &&
      !demand.competingMeetingSpace &&
      !demand.officeListingOnly &&
      !primaryName,
    recommendRemoveFromActiveCustomerView:
      disposition === CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY,
    preserveOpportunityId: true,
    doNotAutoHide: true,
  };
}

export function assessCommercialCustomerReadinessPortfolio(
  opportunities = [],
  opts = {},
) {
  const rows = (opportunities || []).map((o) =>
    assessCommercialCustomerReadiness(o, opts),
  );
  const count = (d) => rows.filter((r) => r.disposition === d).length;
  return {
    version: COMMERCIAL_CUSTOMER_READINESS_GATE_V1,
    total: rows.length,
    distribution: {
      SALES_READY: count(CUSTOMER_READINESS_DISPOSITION.SALES_READY),
      RESEARCH_CONTINUATION_REQUIRED: count(
        CUSTOMER_READINESS_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED,
      ),
      WATCH: count(CUSTOMER_READINESS_DISPOSITION.WATCH),
      NOT_CUSTOMER_READY: count(
        CUSTOMER_READINESS_DISPOSITION.NOT_CUSTOMER_READY,
      ),
    },
    namedPrimary: rows.filter((r) => r.contact.primaryName).length,
    namedSecondary: rows.filter((r) => r.contact.secondaryName).length,
    verifiedAuthority: rows.filter((r) => r.contact.authorityVerified).length,
    lackingCredibleDemand: rows.filter((r) => !r.demand.credibleHotelDemand)
      .length,
    recommendBuyerResearch: rows.filter((r) => r.recommendBuyerResearch),
    recommendRemoveFromActiveCustomerView: rows.filter(
      (r) => r.recommendRemoveFromActiveCustomerView,
    ),
    rows,
  };
}
