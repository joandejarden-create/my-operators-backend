/**
 * Repeat / rotation intelligence + pre-RFP research triggers.
 */

import {
  SERIES_TIMING_STATE,
  classifySeriesTimingState,
  seriesMayBeValidFutureWatch,
  buildSeriesRecord,
  WATCH_ONLY_TIMING,
} from "../discovery-expansion-v3/association-series.js";

export const RESEARCH_TRIGGER = Object.freeze({
  NEXT_CYCLE_ANNOUNCEMENT: "NEXT_CYCLE_ANNOUNCEMENT",
  RFP_EXPECTED_WINDOW: "RFP_EXPECTED_WINDOW",
  SITE_SELECTION_WINDOW: "SITE_SELECTION_WINDOW",
  HOUSING_RFP_WINDOW: "HOUSING_RFP_WINDOW",
  REGISTRATION_OPEN: "REGISTRATION_OPEN",
  VENUE_DECISION_WINDOW: "VENUE_DECISION_WINDOW",
  PROCUREMENT_RELEASE: "PROCUREMENT_RELEASE",
  ROTATION_CONFIRMATION: "ROTATION_CONFIRMATION",
});

export { SERIES_TIMING_STATE, WATCH_ONLY_TIMING, seriesMayBeValidFutureWatch };

/**
 * Infer rotation pattern from host-city-by-year list (no fabricated next city).
 */
export function inferRotationPattern(hostCityByYear = []) {
  const cities = (hostCityByYear || [])
    .map((h) => String(h.city || h.hostCity || "").trim())
    .filter(Boolean);
  if (cities.length < 2) {
    return { rotates: false, pattern: null, uniqueCities: [...new Set(cities)] };
  }
  const unique = [...new Set(cities.map((c) => c.toLowerCase()))];
  const rotates = unique.length >= 2;
  return {
    rotates,
    pattern: rotates ? "MULTI_CITY_ROTATION" : "FIXED_CITY",
    uniqueCities: unique,
    lastHost: cities[cities.length - 1] || null,
  };
}

/**
 * Next research trigger — never invents RFP dates.
 */
export function computeNextResearchTrigger(series = {}) {
  const state = series.timingState || classifySeriesTimingState(series);
  if (state === SERIES_TIMING_STATE.HISTORICAL_ONLY) {
    return {
      trigger: RESEARCH_TRIGGER.NEXT_CYCLE_ANNOUNCEMENT,
      rationale: "Only historical cycle known — watch for next-cycle announcement",
      inventedDate: false,
    };
  }
  if (state === SERIES_TIMING_STATE.ROTATION_PREDICTED) {
    return {
      trigger: RESEARCH_TRIGGER.ROTATION_CONFIRMATION,
      rationale: "Rotation predicted — confirm next host geography before treating as ready",
      inventedDate: false,
    };
  }
  if (state === SERIES_TIMING_STATE.RECURRING_EXPECTED) {
    return {
      trigger: RESEARCH_TRIGGER.SITE_SELECTION_WINDOW,
      rationale: "Recurring series — monitor site selection / housing RFP windows",
      inventedDate: false,
    };
  }
  if (/PROCUREMENT|RFP|TENDER/i.test(String(series.demandEngine || series.subsegment || ""))) {
    return {
      trigger: RESEARCH_TRIGGER.PROCUREMENT_RELEASE,
      rationale: "Procurement-shaped demand — watch tender release",
      inventedDate: false,
    };
  }
  if (!series.typicalLodgingSignal && !series.housingPartnerHistory?.length) {
    return {
      trigger: RESEARCH_TRIGGER.HOUSING_RFP_WINDOW,
      rationale: "No lodging evidence yet — watch housing/RFP announcements",
      inventedDate: false,
    };
  }
  return {
    trigger: RESEARCH_TRIGGER.REGISTRATION_OPEN,
    rationale: "Monitor registration / housing open signals",
    inventedDate: false,
  };
}

/**
 * Build rotation intelligence record for a series/opportunity.
 */
export function buildGdiRotationIntelligence(input = {}) {
  const hostCityByYear = input.hostCityByYear || input.historicalHostCities || [];
  const rotation = inferRotationPattern(
    Array.isArray(hostCityByYear) && typeof hostCityByYear[0] === "string"
      ? hostCityByYear.map((city, i) => ({ city, year: input.historicalCycles?.[i] }))
      : hostCityByYear
  );

  const base = buildSeriesRecord({
    ...input,
    historicalHostCities: rotation.uniqueCities,
    rotationPattern: rotation.pattern,
    historicalRecurrenceReal:
      input.historicalRecurrenceReal ??
      ((input.historicalCycles || []).length >= 2 || rotation.rotates),
  });

  const trigger = computeNextResearchTrigger({
    ...base,
    demandEngine: input.demandEngine,
    housingPartnerHistory: input.housingPartnerHistory,
  });

  return {
    ...base,
    hostCityByYear,
    geographicRotation: rotation,
    organizer: input.organizer || null,
    agency: input.agency || null,
    contactPath: input.contactPath || null,
    housingPartnerHistory: input.housingPartnerHistory || [],
    nextDecisionWindow: input.nextDecisionWindow || null,
    nextResearchTrigger: trigger.trigger,
    nextResearchTriggerRationale: trigger.rationale,
    preRfpEligible:
      base.timingState === SERIES_TIMING_STATE.RECURRING_EXPECTED ||
      base.timingState === SERIES_TIMING_STATE.ROTATION_PREDICTED ||
      base.timingState === SERIES_TIMING_STATE.FUTURE_UNCONFIRMED,
    customerReadyBlockedByTiming: WATCH_ONLY_TIMING.has(base.timingState),
    mayBeValidFutureWatch: seriesMayBeValidFutureWatch({
      ...base,
      nextValidationTrigger: trigger.trigger,
    }),
  };
}

/**
 * Extract rotation candidates from opportunity bag (deterministic).
 */
export function extractRotationCandidatesFromOpportunities(opportunities = [], opts = {}) {
  const out = [];
  for (const opp of opportunities || []) {
    const title = String(opp.title || "");
    const recurring =
      /annual|yearly|édition|edicion|\b20\d{2}\b.*\b20\d{2}\b|rotat|congress|congrès/i.test(
        title + " " + (opp.organizationName || "")
      ) ||
      (Array.isArray(opp.futureCycleSignals) && opp.futureCycleSignals.length > 0) ||
      String(opp.opportunityType || "").includes("FUTURE");
    if (!recurring) continue;

    const cycles = [];
    if (opp.eventYear) cycles.push(String(opp.eventYear));
    if (opp.eventStartDate) cycles.push(String(opp.eventStartDate).slice(0, 4));
    const hosts = [];
    if (opp.eventLocationSummary || opp.destinationStatus) {
      hosts.push({
        city: opp.eventLocationSummary || opp.destinationStatus,
        year: cycles[0] || null,
      });
    }

    const intel = buildGdiRotationIntelligence({
      organization: opp.organizationName || opp.company,
      eventSeriesId: opp.eventSeriesId || opp.eventSeriesKey || `series_${opp.id}`,
      historicalCycles: [...new Set(cycles)],
      hostCityByYear: hosts,
      typicalMonth: null,
      typicalLodgingSignal: opp.lodgingEvidence ? "PRESENT" : null,
      nextKnownCycle: opp.eventStartDate || opp.eventYear || null,
      nextCycleThesis: opp.hotelOpportunityThesis || opp.whyMonitor || null,
      hotelFitScore: opp.hotelFitScore,
      sourceUrl: opp.officialSource || opp.discoverySource,
      eventStartDate: opp.eventStartDate,
      eventEndDate: opp.eventEndDate,
      eventYear: opp.eventYear,
      organizer: opp.organizationName,
      contactPath: opp.organizationContactUrl || opp.officialContactPath || null,
      demandEngine: opts.classifyEngine?.(opp)?.demandEngine,
      opportunityId: opp.id,
    });

    out.push({
      hotelId: opts.hotelId || opp.hotelId,
      opportunityId: opp.id,
      title: opp.title,
      ...intel,
    });
  }
  return out;
}
