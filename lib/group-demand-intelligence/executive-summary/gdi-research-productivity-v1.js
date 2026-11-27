/**
 * GDI Research Productivity V1
 *
 * Estimates manual research effort a hotel sales team would otherwise spend
 * to produce equivalent commercial intelligence.
 *
 * Formula (base case):
 *   Equivalent Manual Hours = Σ (verified activity volume × benchmark.base hours)
 *   Estimated Hours Saved   = Equivalent − residual hotel research/verification
 *   Working Days Saved      = Hours Saved ÷ working-day assumption
 *   Labor Value             = Hours Saved × authorized fully loaded $/hour (optional)
 *
 * Does not multiply qualified count by a single generic duration.
 * Does not treat provisional benchmarks as measured industry facts.
 * Does not mix estimated labor value with verified commercial outcomes.
 * Does not use AI processing elapsed time as human labor hours.
 *
 * Scope: GDI only.
 */

import {
  GDI_RESEARCH_EFFORT_BENCHMARK_CONTRACT_ID,
  GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
  BENCHMARK_VALIDATION_STATUS,
  WORKING_DAY_ASSUMPTION,
  RESIDUAL_HOTEL_RESEARCH_FRACTION,
  RESEARCH_EFFORT_BENCHMARKS,
  getResearchEffortBenchmark,
} from "./gdi-research-effort-benchmark-contract-v1.js";
import { RESEARCH_RECEIPT_AVAILABILITY } from "./gdi-research-receipt-contract-v1.js";
import { OPPORTUNITY_TYPE_FILTER_LABEL } from "../opportunity-type-filter-v1.js";

export const GDI_RESEARCH_PRODUCTIVITY_CONTRACT_ID =
  "gdi-research-productivity-v1";

export const PRODUCTIVITY_ESTIMATE_CLASS = Object.freeze({
  PROVISIONAL_ESTIMATE: "PROVISIONAL_ESTIMATE",
  VALIDATED_CONSERVATIVE: "VALIDATED_CONSERVATIVE",
  UNAVAILABLE: "UNAVAILABLE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function roundHours(n) {
  if (n == null || !Number.isFinite(n)) return null;
  // Avoid unjustified precision — nearest 0.5 hour for display math.
  return Math.round(n * 2) / 2;
}

function roundDays(n) {
  if (n == null || !Number.isFinite(n)) return null;
  // Half-day presentation under the documented eight-hour working-day assumption.
  return Math.round(n * 2) / 2;
}

function roundCurrency(n) {
  if (n == null || !Number.isFinite(n)) return null;
  if (Math.abs(n) >= 1000) return Math.round(n / 50) * 50;
  return Math.round(n);
}

function hasNamedContact(opp) {
  return Boolean(
    text(opp?.primaryContact?.name) ||
      text(opp?.primaryContact?.fullName) ||
      text(opp?.tileContactPreview?.name) ||
      text(opp?.contactName),
  );
}

function hasOutreachChannel(opp) {
  const c = opp?.primaryContact || {};
  return Boolean(
    text(c.email) ||
      text(c.verifiedEmail) ||
      text(c.phone) ||
      text(c.linkedinUrl) ||
      text(c.sourceUrl),
  );
}

function hasDeepResearchMarkers(opp) {
  return Boolean(
    opp?.deepResearchCompleted === true ||
      opp?.dossierDepth === "DEEP" ||
      opp?.researchDepth === "DEEP" ||
      (Array.isArray(opp?.evidence) && opp.evidence.length >= 3) ||
      text(opp?.hotelOpportunityThesis) ||
      text(opp?.whyThisHotel) ||
      text(opp?.whatIsHappening) ||
      text(opp?.potentialHotelBusiness),
  );
}

function hasWhoHow(opp) {
  return Boolean(
    hasNamedContact(opp) ||
      opp?.whoHowCompleted === true ||
      opp?.whoHowPresentation ||
      opp?.contactIntelligence ||
      text(opp?.whoToContact) ||
      text(opp?.primaryContactRole),
  );
}

function hasAltEntry(opp) {
  const alt =
    opp?.alternativeEntryPoints ||
    opp?.altEntryPoints ||
    opp?.whoHow?.alternativeEntryPoints ||
    opp?.contactIntelligence?.alternativeEntryPoints;
  if (Array.isArray(alt) && alt.length > 0) return true;
  return Boolean(
    opp?.verifiedAlternativeEntryPoints > 0 ||
      text(opp?.alternativeEntrySummary),
  );
}

function categoryOf(opp) {
  return text(
    opp?.opportunityTypeFilterCategory ||
      opp?.segmentId ||
      opp?.demandSegment ||
      "",
  ).toUpperCase();
}

function isCompetitive(opp) {
  const scope = text(
    opp?.marketScope || opp?.marketScopeLabel || opp?.opportunityMarketScope,
  ).toUpperCase();
  return (
    opp?.competitiveMarketScopeEligible === true ||
    scope.includes("COMPETITIVE") ||
    opp?.competitiveOverflow === true ||
    opp?.isCompetitiveMarketIntelligence === true
  );
}

function isPlp(opp) {
  const cat = categoryOf(opp);
  const label = text(
    opp?.opportunityTypeFilterLabel || OPPORTUNITY_TYPE_FILTER_LABEL[cat] || "",
  );
  return (
    /PREFERRED_LODGING|PLP/i.test(cat) || /Preferred Lodging/i.test(label)
  );
}

function isLeisure(opp) {
  const cat = categoryOf(opp);
  const label = text(
    opp?.opportunityTypeFilterLabel || OPPORTUNITY_TYPE_FILTER_LABEL[cat] || "",
  );
  return (
    /LEISURE|SPORTS|ENTERTAINMENT/i.test(cat) ||
    /Leisure|Sports & Entertainment/i.test(label)
  );
}

function hasSalesAction(opp) {
  return Boolean(
    text(opp?.recommendedNextAction) ||
      text(opp?.salesAction) ||
      text(opp?.recommendedAction) ||
      text(opp?.recommendedNextStep),
  );
}

/**
 * Derive verified activity volumes from qualified inventory (+ optional ledgers).
 * Screened-out and weekly revalidation stay UNAVAILABLE without durable telemetry.
 */
export function deriveResearchActivityVolumes(input = {}) {
  const opportunities = Array.isArray(input.opportunities)
    ? input.opportunities
    : [];
  const weeklyComparisonAvailable = input.weeklyComparisonAvailable === true;
  const weeklyChangedCount = Number(input.weeklyChangedCount);
  const screenedOutCount =
    input.screenedOutCount == null ? null : Number(input.screenedOutCount);

  let deep = 0;
  let competitive = 0;
  let plp = 0;
  let leisure = 0;
  let whoHow = 0;
  let altEntry = 0;
  let outreach = 0;
  let salesAction = 0;
  let discoveryOnly = 0;
  let qualificationOnly = 0;

  for (const opp of opportunities) {
    const deepRow = hasDeepResearchMarkers(opp);
    if (deepRow) deep += 1;
    else discoveryOnly += 1;
    if (isCompetitive(opp)) competitive += 1;
    if (isPlp(opp)) plp += 1;
    if (isLeisure(opp)) leisure += 1;
    if (hasWhoHow(opp)) whoHow += 1;
    // Alt-entry only when evidence exists AND primary WHO/HOW is absent —
    // prevents double-counting the same decision-maker research pass.
    if (hasAltEntry(opp) && !hasWhoHow(opp)) altEntry += 1;
    if (hasOutreachChannel(opp)) outreach += 1;
    if (hasSalesAction(opp)) salesAction += 1;
    // Qualification hours only when deep dossier markers are absent (deep already covers fit write-up).
    if (!deepRow) qualificationOnly += 1;
  }

  return {
    demand_discovery_and_screening: {
      // Overlap prevention: discovery hours apply only when deep research is not already counted.
      count: discoveryOnly,
      availability:
        opportunities.length > 0
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      note:
        "Discovery/screening hours for rows without deep-dossier markers. Deep-researched rows are counted under deep research only.",
    },
    deep_opportunity_research: {
      count: deep,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      note: "Deep dossier markers on qualified rows only (not discovery).",
    },
    competitive_account_discovery: {
      count: competitive,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      note: "Competitive-scope opportunities in qualified inventory (overlapping Market Scope).",
    },
    plp_referral_research: {
      count: plp,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    },
    leisure_demand_research: {
      count: leisure,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    },
    who_how_resolution: {
      count: whoHow,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      note: "WHO/HOW or named decision-maker present; separate from contact verification.",
    },
    alternative_entry_point_research: {
      count: altEntry,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      note: "Counted only when alt-entry evidence exists without primary WHO/HOW.",
    },
    professional_contact_verification: {
      count: outreach,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      note: "Professional channel present; distinct from decision-maker identification.",
    },
    hotel_specific_commercial_qualification: {
      count: qualificationOnly,
      availability:
        opportunities.length > 0
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      note:
        "Qualification hours only when deep research is not already counted for the same opportunity.",
    },
    sales_pursuit_strategy_preparation: {
      // Ordinary commercial pursuit prep is excluded from customer hours-saved.
      count: null,
      availability: RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      note:
        "Excluded from hours-saved — ordinary commercial pursuit is not research productivity.",
    },
    weekly_research_revalidation: {
      count: weeklyComparisonAvailable ? weeklyChangedCount || 0 : null,
      availability: weeklyComparisonAvailable
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      note: weeklyComparisonAvailable
        ? null
        : "Comparable Weekly Change history unavailable — not treated as zero.",
    },
    executive_report_preparation: {
      count: opportunities.length > 0 ? 1 : 0,
      availability:
        opportunities.length > 0
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      note: "One executive briefing preparation unit per research package delivery.",
    },
    opportunities_screened_out: {
      count:
        screenedOutCount != null && Number.isFinite(screenedOutCount)
          ? screenedOutCount
          : null,
      availability:
        screenedOutCount != null && Number.isFinite(screenedOutCount)
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
          : RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
      note:
        screenedOutCount != null
          ? "Screened-out work protects sales time and is included when ledgered."
          : "Durable screened-out ledger with hotel attribution required.",
    },
  };
}

function estimateBand(count, hours) {
  if (count == null || !Number.isFinite(count) || count < 0) {
    return { low: null, base: null, high: null };
  }
  return {
    low: count * hours.low,
    base: count * hours.base,
    high: count * hours.high,
  };
}

/**
 * @param {object} input
 * @param {object[]} [input.opportunities]
 * @param {boolean} [input.weeklyComparisonAvailable]
 * @param {number} [input.weeklyChangedCount]
 * @param {number|null} [input.screenedOutCount]
 * @param {number|null} [input.fullyLoadedLaborCostPerHour] — authorized only
 * @param {boolean} [input.laborCostAuthorized]
 * @param {number|null} [input.dealalityInvestmentForPeriod]
 * @param {boolean} [input.dealalityInvestmentAuthorized]
 * @param {number|null} [input.actualHumanResearchHours] — measured Dealality human effort
 * @param {number|null} [input.providerSpendUsd]
 * @param {'low'|'base'|'high'} [input.estimateCase]
 */
export function buildGdiResearchProductivity(input = {}) {
  const estimateCase = ["low", "base", "high"].includes(input.estimateCase)
    ? input.estimateCase
    : "base";
  const volumes = deriveResearchActivityVolumes(input);
  const activityLines = [];

  let equivalentLow = 0;
  let equivalentBase = 0;
  let equivalentHigh = 0;
  let anyMeasured = false;

  for (const bench of RESEARCH_EFFORT_BENCHMARKS) {
    const vol = volumes[bench.activityKey] || {
      count: null,
      availability: RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
    };
    const available =
      vol.availability === RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED ||
      vol.availability === RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING ||
      vol.availability === RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE;

    const band = available
      ? estimateBand(vol.count, bench.hours)
      : { low: null, base: null, high: null };

    if (available && band.base != null) {
      anyMeasured = true;
      equivalentLow += band.low;
      equivalentBase += band.base;
      equivalentHigh += band.high;
    }

    activityLines.push({
      activityKey: bench.activityKey,
      researchActivity: bench.researchActivity,
      customerFacingActivityLabel: bench.customerFacingActivityLabel,
      salesActivityKey: bench.salesActivityKey,
      countingUnit: bench.countingUnit,
      volume: available ? vol.count : null,
      volumeAvailability: vol.availability,
      volumeNote: vol.note || null,
      hoursPerUnit: bench.hours,
      estimatedHours: available
        ? {
            low: roundHours(band.low),
            base: roundHours(band.base),
            high: roundHours(band.high),
          }
        : { low: null, base: null, high: null },
      benchmarkVersion: bench.benchmarkVersion,
      validationStatus: bench.validationStatus,
      provisional: bench.validationStatus === BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
    });
  }

  const residualFrac = RESIDUAL_HOTEL_RESEARCH_FRACTION[estimateCase] ??
    RESIDUAL_HOTEL_RESEARCH_FRACTION.base;

  const pick = (low, base, high) =>
    estimateCase === "low" ? low : estimateCase === "high" ? high : base;

  const equivalentRaw = anyMeasured
    ? pick(equivalentLow, equivalentBase, equivalentHigh)
    : null;
  const residualRaw =
    equivalentRaw == null ? null : equivalentRaw * residualFrac;
  const savedRaw =
    equivalentRaw == null ? null : Math.max(0, equivalentRaw - residualRaw);

  const hoursPerDay = WORKING_DAY_ASSUMPTION.hoursPerWorkingDay;
  const daysSaved =
    savedRaw == null ? null : roundDays(savedRaw / hoursPerDay);

  const laborAuthorized =
    input.laborCostAuthorized === true &&
    Number.isFinite(Number(input.fullyLoadedLaborCostPerHour)) &&
    Number(input.fullyLoadedLaborCostPerHour) > 0;
  const laborRate = laborAuthorized
    ? Number(input.fullyLoadedLaborCostPerHour)
    : null;
  const laborValue =
    laborAuthorized && savedRaw != null
      ? roundCurrency(savedRaw * laborRate)
      : null;

  const investmentAuthorized =
    input.dealalityInvestmentAuthorized === true &&
    Number.isFinite(Number(input.dealalityInvestmentForPeriod)) &&
    Number(input.dealalityInvestmentForPeriod) > 0;
  const investment = investmentAuthorized
    ? Number(input.dealalityInvestmentForPeriod)
    : null;
  const laborVsInvestment =
    laborValue != null && investment != null && investment > 0
      ? Math.round((laborValue / investment) * 10) / 10
      : null;

  const actualHuman =
    Number.isFinite(Number(input.actualHumanResearchHours)) &&
    Number(input.actualHumanResearchHours) > 0
      ? Number(input.actualHumanResearchHours)
      : null;
  const productivityMultiple =
    equivalentRaw != null && actualHuman != null
      ? Math.round((equivalentRaw / actualHuman) * 10) / 10
      : null;

  const providerSpend =
    Number.isFinite(Number(input.providerSpendUsd)) &&
    Number(input.providerSpendUsd) >= 0
      ? Number(input.providerSpendUsd)
      : null;

  // Aggregate sales-facing activity buckets (sum of base hours by salesActivityKey).
  const salesBuckets = new Map();
  for (const line of activityLines) {
    if (line.estimatedHours.base == null) continue;
    const key = line.salesActivityKey;
    const prev = salesBuckets.get(key) || {
      salesActivityKey: key,
      label: line.customerFacingActivityLabel,
      estimatedHoursBase: 0,
      volume: 0,
      activities: [],
    };
    prev.estimatedHoursBase += line.estimatedHours.base;
    prev.volume += Number(line.volume) || 0;
    prev.activities.push(line.activityKey);
    salesBuckets.set(key, prev);
  }

  const salesTimeSavedBreakdown = [...salesBuckets.values()].map((b) => ({
    ...b,
    estimatedHoursBase: roundHours(b.estimatedHoursBase),
  }));

  const customerMetrics = {
    equivalentManualResearchHours: {
      key: "equivalentManualResearchHours",
      label: "Equivalent Manual Research Hours",
      value: roundHours(equivalentRaw),
      range: anyMeasured
        ? {
            low: roundHours(equivalentLow),
            base: roundHours(equivalentBase),
            high: roundHours(equivalentHigh),
          }
        : null,
      availability: anyMeasured
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      estimateClass: PRODUCTIVITY_ESTIMATE_CLASS.PROVISIONAL_ESTIMATE,
      customerFacing: true,
      note:
        "Estimated hours a hotel sales researcher would spend to produce equivalent intelligence. Provisional benchmarks — not measured industry facts.",
    },
    estimatedResearchHoursSaved: {
      key: "estimatedResearchHoursSaved",
      label: "Estimated Research Hours Saved",
      value: roundHours(savedRaw),
      availability: anyMeasured
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      estimateClass: PRODUCTIVITY_ESTIMATE_CLASS.PROVISIONAL_ESTIMATE,
      customerFacing: true,
      note: `Net of an estimated ${Math.round(residualFrac * 100)}% residual hotel review/verification. Ordinary pursuit work is excluded.`,
    },
    equivalentWorkingDaysSaved: {
      key: "equivalentWorkingDaysSaved",
      label: "Equivalent Working Days Saved",
      value: daysSaved,
      availability: anyMeasured
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      estimateClass: PRODUCTIVITY_ESTIMATE_CLASS.PROVISIONAL_ESTIMATE,
      customerFacing: true,
      note: WORKING_DAY_ASSUMPTION.note,
    },
    estimatedResearchLaborValue: {
      key: "estimatedResearchLaborValue",
      label: "Estimated Research Labor Value",
      value: laborValue,
      currency: laborAuthorized ? "USD" : null,
      availability: laborAuthorized
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.HOTEL_INPUT_REQUIRED,
      estimateClass: laborAuthorized
        ? PRODUCTIVITY_ESTIMATE_CLASS.PROVISIONAL_ESTIMATE
        : PRODUCTIVITY_ESTIMATE_CLASS.UNAVAILABLE,
      customerFacing: true,
      note: laborAuthorized
        ? "Uses an authorized fully loaded labor rate. Estimated productivity value — not payroll savings or booked revenue."
        : "Requires an authorized fully loaded labor cost. No hardcoded hotel salary assumptions.",
    },
    researchProductivityMultiple: {
      key: "researchProductivityMultiple",
      label: "Research Productivity Multiple",
      value: productivityMultiple,
      availability:
        productivityMultiple != null
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      customerFacing: false,
      note:
        "Equivalent manual hours ÷ measured Dealality human research hours. AI elapsed time is excluded.",
    },
    researchLaborValueRelativeToInvestment: {
      key: "researchLaborValueRelativeToInvestment",
      label: "Research Labor Value Relative To Dealality Investment",
      value: laborVsInvestment,
      availability:
        laborVsInvestment != null
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      customerFacing: false,
      note:
        "Only when estimated labor value and authorized Dealality investment share the same reporting period.",
    },
  };

  return {
    contractId: GDI_RESEARCH_PRODUCTIVITY_CONTRACT_ID,
    benchmarkContractId: GDI_RESEARCH_EFFORT_BENCHMARK_CONTRACT_ID,
    benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
    estimateCase,
    estimateClass: PRODUCTIVITY_ESTIMATE_CLASS.PROVISIONAL_ESTIMATE,
    certificationStatus: "PROVISIONAL_NOT_CERTIFIED",
    disclaimer:
      "Provisional research-time estimate — not payroll savings, not booked revenue, and not a certified productivity audit. Ordinary commercial pursuit is excluded.",
    customerHeadline: "Your Time. Better Invested.",
    customerLede:
      "Less time searching for demand. More time building relationships and converting opportunities into bookings.",
    methodologySummary:
      "Verified research activity volumes × provisional activity benchmarks − residual hotel review time. Overlap rules: deep research replaces discovery/qualification for the same opportunity; alt-entry is counted only without primary WHO/HOW; pursuit prep is excluded. Benchmarks remain provisional until timed validation.",
    workingDayAssumption: WORKING_DAY_ASSUMPTION,
    residualHotelResearch: RESIDUAL_HOTEL_RESEARCH_FRACTION,
    activityLines,
    salesTimeSavedBreakdown,
    customerMetrics,
    internalMetrics: {
      equivalentManualResearchHours: customerMetrics.equivalentManualResearchHours,
      residualHotelResearchHours: {
        label: "Residual Hotel Research Hours",
        value: roundHours(residualRaw),
        availability: anyMeasured
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      },
      actualHumanResearchAndReviewHours: {
        label: "Actual Human Research And Review Effort",
        value: actualHuman,
        availability:
          actualHuman != null
            ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
            : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
        note: "Measured Dealality human effort only — not AI wall-clock time.",
      },
      automatedResearchActivity: {
        label: "Automated Research Activity",
        value: null,
        availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
        note: "Requires durable job telemetry join.",
      },
      actualProviderExpenditureUsd: {
        label: "Actual Provider Expenditure",
        value: providerSpend,
        availability:
          providerSpend != null
            ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
            : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
        note: "Internal only — never shown on customer Research Receipt.",
      },
      researchProductivityMultiple: customerMetrics.researchProductivityMultiple,
      costPerQualifiedOpportunity: {
        label: "Cost Per Qualified Opportunity",
        value:
          providerSpend != null &&
          Array.isArray(input.opportunities) &&
          input.opportunities.length > 0
            ? Math.round((providerSpend / input.opportunities.length) * 100) /
              100
            : null,
        availability:
          providerSpend != null &&
          Array.isArray(input.opportunities) &&
          input.opportunities.length > 0
            ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
            : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      },
      researchReuseEfficiency: {
        label: "Research Reuse Efficiency",
        value: null,
        availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
      },
      benchmarkValidationStatus: BENCHMARK_VALIDATION_STATUS.PROVISIONAL,
      laborValueRelativeToInvestment:
        customerMetrics.researchLaborValueRelativeToInvestment,
    },
    investmentRecoverySplit: {
      estimatedResearchProductivityValue: {
        category: "A",
        label: "Estimated Research Productivity Value",
        hoursSaved: roundHours(savedRaw),
        laborValue,
        note: "Redeployable research time estimate — not actual payroll savings.",
      },
      verifiedIncrementalCommercialValue: {
        category: "B",
        label: "Verified Incremental Commercial Value",
        note: "Uses hotel-reported or otherwise verified commercial outcomes only. Never combined with category A into a single realized ROI claim.",
      },
    },
    governance: {
      provisionalBenchmarks: true,
      syntheticCountsProhibited: true,
      hardcodedSalaryAssumptionsProhibited: true,
      providerCostsCustomerFacingProhibited: true,
      aiElapsedTimeNotHumanLabor: true,
      sharedMarketVsHotelBenefit:
        "Hotel-level benefit shown here; portfolio aggregation must not sum shared underlying effort as unique research spend.",
      futureProducts:
        "Documented for ADP, Hotel Explorer, and Deal Opportunities — implement in those product cycles, not as a cross-product engine.",
    },
  };
}

export function getBenchmarkByActivityKey(activityKey) {
  return getResearchEffortBenchmark(activityKey);
}

export default {
  GDI_RESEARCH_PRODUCTIVITY_CONTRACT_ID,
  PRODUCTIVITY_ESTIMATE_CLASS,
  deriveResearchActivityVolumes,
  buildGdiResearchProductivity,
};
