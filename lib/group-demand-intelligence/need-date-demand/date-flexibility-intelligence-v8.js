/**
 * Need-Date date-flexibility intelligence V8 — shared, hotel-agnostic.
 *
 * Complements dated need-period matching with flexible / recurring prospecting.
 * Never presents POTENTIALLY_FLEXIBLE as confirmed.
 * Never assigns a need month without evidence.
 */
export const DATE_FLEXIBILITY_INTELLIGENCE_V8 =
  "gdi-date-flexibility-intelligence-v8";

export const DATE_FLEXIBILITY_CLASS = Object.freeze({
  FIXED_VERIFIED: "FIXED_VERIFIED",
  FLEXIBLE_CONFIRMED: "FLEXIBLE_CONFIRMED",
  POTENTIALLY_FLEXIBLE: "POTENTIALLY_FLEXIBLE",
  HISTORICAL_RECURRING: "HISTORICAL_RECURRING",
  UNKNOWN: "UNKNOWN",
});

export const NEED_DATE_MATCH_CLASS_V8 = Object.freeze({
  VERIFIED_NEED_DATE_MATCH: "VERIFIED_NEED_DATE_MATCH",
  FLEXIBLE_DATE_CONFIRMED_MATCH: "FLEXIBLE_DATE_CONFIRMED_MATCH",
  FLEXIBLE_DATE_PROSPECT: "FLEXIBLE_DATE_PROSPECT",
  RECURRING_FUTURE_CYCLE_RESEARCH: "RECURRING_FUTURE_CYCLE_RESEARCH",
  RELATIONSHIP_OPPORTUNITY: "RELATIONSHIP_OPPORTUNITY",
  NOT_A_MATCH: "NOT_A_MATCH",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function blobOf(opportunity = {}) {
  return [
    opportunity.title,
    opportunity.whatIsHappening,
    opportunity.opportunityType,
    opportunity.demandMechanism,
    opportunity.segment,
    opportunity.hotelOpportunityThesis,
    opportunity.whyThisHotel,
    opportunity.recommendedAction,
    opportunity.timingType,
    opportunity.eventDateStatus,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();
}

/**
 * Classify scheduling flexibility from existing evidence only.
 */
export function classifyDateFlexibility(opportunity = {}, opts = {}) {
  const blob = blobOf(opportunity);
  const start = text(opportunity.eventStartDate || opportunity.tileEventStartDate).slice(
    0,
    10,
  );
  const hasVerifiedDate = /^\d{4}-\d{2}-\d{2}$/.test(start);
  const buyerConfirmedFlexible =
    opportunity.dateFlexibilityConfirmed === true ||
    opportunity.schedulingFlexibilityVerified === true ||
    /buyer confirmed.*flex|flexible.?confirmed|can (move|shift|reschedule)/i.test(
      blob,
    );

  if (buyerConfirmedFlexible) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      class: DATE_FLEXIBILITY_CLASS.FLEXIBLE_CONFIRMED,
      evidence: "Buyer/source confirms scheduling flexibility",
      mayPresentAsConfirmedFlexible: true,
    };
  }

  if (
    hasVerifiedDate &&
    (/championship|nationals|boat show|convention|summit of the americas|convergence/i.test(
      blob,
    ) ||
      opportunity.venueSourcingStatus === "PRIMARY_VENUE_SELECTED" ||
      /fixed|published dates|cannot be shifted/i.test(blob))
  ) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      class: DATE_FLEXIBILITY_CLASS.FIXED_VERIFIED,
      evidence: "Documented event/programme dates with low shift likelihood",
      mayPresentAsConfirmedFlexible: false,
      verifiedStart: start || null,
    };
  }

  if (
    /recurring|annual|each year|yearly|another cycle|historical|prior edition|series/i.test(
      blob,
    ) &&
    !hasVerifiedDate
  ) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      class: DATE_FLEXIBILITY_CLASS.HISTORICAL_RECURRING,
      evidence: "Historical/recurring language without confirmed next dates",
      mayPresentAsConfirmedFlexible: false,
    };
  }

  if (
    /training|workshop|board meeting|sales meeting|committee|retreat|celebration|preferred.?rate|corporate account|leadership/i.test(
      blob,
    ) ||
    /CORPORATE_ACCOUNT|SCALING|PREFERRED/i.test(
      String(opportunity.opportunityType || opportunity.segment || ""),
    )
  ) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      class: DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE,
      evidence:
        "Demand mechanism suggests possible scheduling flexibility — buyer confirmation required",
      mayPresentAsConfirmedFlexible: false,
    };
  }

  if (hasVerifiedDate) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      class: DATE_FLEXIBILITY_CLASS.FIXED_VERIFIED,
      evidence: "Verified start date present",
      mayPresentAsConfirmedFlexible: false,
      verifiedStart: start,
    };
  }

  return {
    version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
    class: DATE_FLEXIBILITY_CLASS.UNKNOWN,
    evidence: "Insufficient evidence",
    mayPresentAsConfirmedFlexible: false,
  };
}

/**
 * Map flexibility + need-period timing into Need-Date match class V8.
 * Does not invent need-month assignment without evidence.
 */
export function classifyNeedDateMatchV8({
  opportunity = {},
  needPeriods = [],
  timingOverlap = null,
  revenueFit = false,
  hotelFit = false,
  commerciallyAccessible = false,
} = {}) {
  const flex = classifyDateFlexibility(opportunity);
  const periods = Array.isArray(needPeriods) ? needPeriods : [];
  const overlapOk =
    timingOverlap === "CONFIRMED_PERIOD_OVERLAP" ||
    timingOverlap === "SUPPORTED_PROBABLE_OVERLAP" ||
    opportunity.needDateOverlapStatus === "CONFIRMED_PERIOD_OVERLAP";

  if (!hotelFit && opportunity.hotelFit === false) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      matchClass: NEED_DATE_MATCH_CLASS_V8.NOT_A_MATCH,
      dateFlexibility: flex,
      reason: "insufficient_hotel_fit",
    };
  }

  if (
    flex.class === DATE_FLEXIBILITY_CLASS.FLEXIBLE_CONFIRMED &&
    revenueFit &&
    (hotelFit || opportunity.hotelFit !== false) &&
    periods.length
  ) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      matchClass: NEED_DATE_MATCH_CLASS_V8.FLEXIBLE_DATE_CONFIRMED_MATCH,
      dateFlexibility: flex,
      needPeriodsConsidered: periods.map((p) => p.id || p.month || p.label),
      reason: "buyer_confirmed_flexibility_with_revenue_fit",
    };
  }

  if (overlapOk && revenueFit && flex.class === DATE_FLEXIBILITY_CLASS.FIXED_VERIFIED) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      matchClass: NEED_DATE_MATCH_CLASS_V8.VERIFIED_NEED_DATE_MATCH,
      dateFlexibility: flex,
      reason: "verified_timing_overlaps_need_period",
    };
  }

  if (flex.class === DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE && revenueFit) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      matchClass: NEED_DATE_MATCH_CLASS_V8.FLEXIBLE_DATE_PROSPECT,
      dateFlexibility: flex,
      reason: "possible_flexibility_buyer_confirmation_required",
      caution: "Do not present as confirmed need-date match",
    };
  }

  if (flex.class === DATE_FLEXIBILITY_CLASS.HISTORICAL_RECURRING) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      matchClass: NEED_DATE_MATCH_CLASS_V8.RECURRING_FUTURE_CYCLE_RESEARCH,
      dateFlexibility: flex,
      reason: "historical_recurrence_next_dates_unconfirmed",
    };
  }

  if (
    /account|relationship|planner|referral/i.test(blobOf(opportunity)) ||
    String(opportunity.customerReadinessDisposition || "").includes("SALES_READY")
  ) {
    return {
      version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
      matchClass: NEED_DATE_MATCH_CLASS_V8.RELATIONSHIP_OPPORTUNITY,
      dateFlexibility: flex,
      reason: "relationship_or_account_without_current_need_date_match",
    };
  }

  return {
    version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
    matchClass: NEED_DATE_MATCH_CLASS_V8.NOT_A_MATCH,
    dateFlexibility: flex,
    reason: "insufficient_evidence_for_need_date_match",
    commerciallyAccessible,
  };
}

/**
 * Build Need-Date reactivation queue rows from existing inventory (no new IDs).
 */
export function buildNeedDateReactivationQueue(
  opportunities = [],
  needPeriods = [],
  opts = {},
) {
  const months = (needPeriods || []).map((p) => ({
    id: p.id || p.periodId || null,
    label: p.label || p.monthLabel || p.month || null,
    start: p.startDate || p.start || null,
    end: p.endDate || p.end || null,
    testScenario: p.testScenario !== false,
  }));

  const rows = [];
  for (const o of opportunities || []) {
    if (!o?.id) continue;
    if (o.archived === true && opts.includeArchived !== true) continue;
    const flex = classifyDateFlexibility(o);
    const interesting =
      flex.class === DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE ||
      flex.class === DATE_FLEXIBILITY_CLASS.HISTORICAL_RECURRING ||
      flex.class === DATE_FLEXIBILITY_CLASS.FLEXIBLE_CONFIRMED ||
      (!o.eventStartDate &&
        /account|training|meeting|retreat|preferred/i.test(blobOf(o)));
    if (!interesting) continue;

    const match = classifyNeedDateMatchV8({
      opportunity: o,
      needPeriods: months,
      revenueFit: true,
      hotelFit: true,
    });

    rows.push({
      opportunityId: o.id,
      title: o.title || o.displayTitle || o.organizationName || null,
      demandMechanism:
        o.opportunityType || o.demandMechanism || o.segment || null,
      dateFlexibilityClass: flex.class,
      needDateMatchClass: match.matchClass,
      relevantNeedMonths: months.map((m) => m.label).filter(Boolean),
      needMonthsAreTestScenario: true,
      revenueStreamClass: o.revenueStreamClass || null,
      disposition: o.customerReadinessDisposition || null,
      existingContact:
        o.primaryContact?.name || o.tileContactName || null,
      missingQuestion:
        o.contactResearchGap ||
        (flex.class === DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE
          ? "Can scheduling be aligned to Aug/Sep/Oct 2027 TEST_SCENARIO need periods?"
          : flex.class === DATE_FLEXIBILITY_CLASS.HISTORICAL_RECURRING
            ? "What is the next confirmed cycle / edition date?"
            : "Confirm timing relevance to Waterstone need periods"),
      recommendedVerificationStrategy:
        flex.class === DATE_FLEXIBILITY_CLASS.POTENTIALLY_FLEXIBLE
          ? "Zero-paid outreach to existing contact asking about date flexibility; do not invent programmes"
          : "Verify next published cycle from organizer sources before need-month assignment",
      caution:
        "TEST_SCENARIO need periods only — not hotel occupancy forecasts. Never present POTENTIALLY_FLEXIBLE as confirmed.",
    });
  }

  return {
    version: DATE_FLEXIBILITY_INTELLIGENCE_V8,
    hotelNeedPeriods: months,
    count: rows.length,
    rows,
  };
}
