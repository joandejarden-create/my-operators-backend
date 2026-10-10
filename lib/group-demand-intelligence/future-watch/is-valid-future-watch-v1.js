/**
 * Hotel-agnostic Future Watch quality gate.
 *
 * Future Watch must represent a real future commercial thesis — not an
 * evidence dump, generic event parking lot, duplicate storage, or
 * unresolved research backlog.
 */

import { assertExternalDemandForCustomerOpportunity } from "../external-demand-invariant-v1.js";

export const WATCH_VALIDATION_CLASS = Object.freeze({
  VALID_FUTURE_WATCH: "VALID_FUTURE_WATCH",
  RESEARCH_BACKLOG_NOT_WATCH: "RESEARCH_BACKLOG_NOT_WATCH",
  INSUFFICIENT_EVIDENCE_NOT_WATCH: "INSUFFICIENT_EVIDENCE_NOT_WATCH",
  DUPLICATE: "DUPLICATE",
  STALE: "STALE",
  OUT_OF_MARKET: "OUT_OF_MARKET",
  NO_HOTEL_FIT: "NO_HOTEL_FIT",
  PLACED_NO_OVERFLOW: "PLACED_NO_OVERFLOW",
  ENTITY_INVALID: "ENTITY_INVALID",
  OTHER: "OTHER",
});

export const WATCH_TRIGGER_TYPE = Object.freeze({
  DATE_WINDOW: "DATE_WINDOW",
  NEXT_CYCLE_ANNOUNCEMENT: "NEXT_CYCLE_ANNOUNCEMENT",
  RFP_RELEASE: "RFP_RELEASE",
  HOUSING_OPEN: "HOUSING_OPEN",
  VENUE_SELECTION: "VENUE_SELECTION",
  REGISTRATION_OPEN: "REGISTRATION_OPEN",
  PROCUREMENT_NOTICE: "PROCUREMENT_NOTICE",
  ROTATION_CONFIRMATION: "ROTATION_CONFIRMATION",
  OTHER: "OTHER",
});

const GENERIC_ORG_RE =
  /^(unknown|various|various (corporations|corporate entities|associations)|n\/?a|tbd|local isles|not specified|none|tbc)$/i;
const GENERIC_TITLE_RE =
  /^(leadership retreat|corporate meetings|incentive travel( rfp| programs)?|destination weddings( in \w+)?|future cycle incentive travel|future conferences|grenada official hotel block|executive retreats|leadership offsite)/i;
const DIRECTORY_OTA_RE =
  /\b(trivago|booking\.com|expedia|hotels\.com|familyfriendlyhotels|vaudhotels|conferenceinc|bookmycoliving)\b/i;
const SUPPLY_SIDE_RE =
  /\b(intercontinental|hilton|marriott|accor|ihg)\b.+\b(opening|launch|debut)\b|\bopening of\b.+\bhotel\b/i;
const WEBINAR_RE = /\bwebinar\b/i;
const PAST_YEAR_CUTOFF = "2026-01-01";

function textBlob(opp = {}) {
  return [
    opp.title,
    opp.opportunityName,
    opp.organizationName,
    opp.company,
    opp.officialSource,
    opp.discoverySource,
    opp.eventLocationSummary,
    opp.eventLocation,
    opp.hotelOpportunityThesis,
    opp.whyMonitor,
    opp.whyNow,
  ]
    .map((x) => String(x || ""))
    .join(" | ");
}

function hasStableEntity(opp = {}) {
  const title = String(opp.title || opp.opportunityName || "").trim();
  const org = String(opp.organizationName || opp.company || "").trim();
  if (!title || title.length < 4) return false;
  if (GENERIC_TITLE_RE.test(title)) return false;
  if (DIRECTORY_OTA_RE.test(textBlob(opp))) return false;
  if (!org || GENERIC_ORG_RE.test(org)) return false;
  if (/^various\b/i.test(org)) return false;
  return true;
}

function hasFutureCycleBasis(opp = {}) {
  const fut = String(opp.futureCycleEvidenceState || "").toUpperCase();
  if (fut === "CURRENT_FUTURE_CYCLE_CONFIRMED") return true;
  if (fut === "FUTURE_CYCLE_UNCONFIRMED" && opp.eventStartDate) {
    return String(opp.eventStartDate).slice(0, 10) >= PAST_YEAR_CUTOFF;
  }
  if (opp.eventStartDate && String(opp.eventStartDate).slice(0, 10) >= PAST_YEAR_CUTOFF) {
    return true;
  }
  if (Array.isArray(opp.futureCycleSignals) && opp.futureCycleSignals.length > 0) {
    return true;
  }
  return false;
}

function hasHotelFitThesis(opp = {}) {
  const thesis = String(
    opp.hotelOpportunityThesis || opp.fitExplanation || opp.summaryWhyHotel || ""
  ).trim();
  if (thesis.length < 24) return false;
  if (/may require accommodation|potential demand|attendees needing/i.test(thesis) && thesis.length < 80) {
    // Thin template thesis alone is not enough without fit score
    if (opp.hotelFitScore == null && opp.hotelFit == null) return false;
  }
  const fit = opp.hotelFitScore ?? opp.hotelFit;
  if (fit != null && Number(fit) < 40) return false;
  return true;
}

function isTemplateMonitorRationale(why = "") {
  return /recurring\s*\/\s*future-cycle pattern/i.test(String(why || ""));
}

function whyNotActionableNow(opp = {}) {
  const reasons = [];
  const venue = String(opp.venueSourcingStatus || opp.venueStatus || "").toUpperCase();
  const room = String(opp.roomDemandStatus || "").toUpperCase();
  const fut = String(opp.futureCycleEvidenceState || "").toUpperCase();
  if (/FULLY_PLACED|PRIMARY_VENUE_SELECTED_NO_OVERFLOW/i.test(venue)) {
    reasons.push("venue_placed_or_closed_path");
  }
  if (fut.includes("UNCONFIRMED") || fut.includes("NO_FUTURE")) {
    reasons.push("future_cycle_not_confirmed_for_sourcing");
  }
  if (/UNKNOWN|ESTIMATED/i.test(room)) {
    reasons.push("room_demand_not_verified");
  }
  if (!opp.eventStartDate) reasons.push("dates_unconfirmed");
  const why = String(opp.whyMonitor || "").trim();
  if (why && !isTemplateMonitorRationale(why)) {
    reasons.push("explicit_monitor_rationale");
  }
  return reasons;
}

function resolveTrigger(opp = {}) {
  const explicit =
    opp.nextTriggerType ||
    opp.watchNextTriggerType ||
    opp.watchValidation?.nextTriggerType ||
    null;
  if (explicit && WATCH_TRIGGER_TYPE[explicit]) {
    return {
      type: explicit,
      condition:
        opp.nextTriggerCondition ||
        opp.watchValidation?.nextTriggerCondition ||
        null,
      researchDate:
        opp.nextResearchDate ||
        opp.watchNextResearchDate ||
        opp.watchValidation?.nextResearchDate ||
        null,
    };
  }
  if (opp.eventStartDate) {
    const start = String(opp.eventStartDate).slice(0, 10);
    // Heuristic research window: 120–60 days before start
    const startMs = Date.parse(start);
    if (Number.isFinite(startMs)) {
      const researchMs = startMs - 120 * 86400000;
      return {
        type: WATCH_TRIGGER_TYPE.DATE_WINDOW,
        condition: `Re-check housing / registration / venue 120–60 days before ${start}`,
        researchDate: new Date(researchMs).toISOString().slice(0, 10),
        inferred: true,
      };
    }
  }
  if (String(opp.futureCycleEvidenceState || "").includes("UNCONFIRMED")) {
    return {
      type: WATCH_TRIGGER_TYPE.NEXT_CYCLE_ANNOUNCEMENT,
      condition: "Next published cycle dates / host / housing page",
      researchDate: null,
      inferred: true,
      incomplete: true,
    };
  }
  return null;
}

function latestEvidenceDate(opp = {}) {
  const candidates = [
    opp.lastEvidenceDate,
    opp.sourceLastVerifiedAt,
    opp.lastResearchedAt,
    opp.lastVerifiedAt,
    opp.lastMaterialChangeAt,
    opp.retrievedAt,
    opp.updatedAt,
  ];
  if (Array.isArray(opp.evidence)) {
    for (const e of opp.evidence) {
      candidates.push(e?.capturedAt, e?.retrievedAt, e?.checkedAt, e?.verifiedAt);
    }
  }
  const stamps = candidates
    .map((c) => Date.parse(c))
    .filter((t) => Number.isFinite(t));
  if (!stamps.length) return null;
  return new Date(Math.max(...stamps)).toISOString();
}

function hasEvidence(opp = {}) {
  if (opp.officialSource || opp.discoverySource) return true;
  if (Array.isArray(opp.sources) && opp.sources.some((s) => s?.url || typeof s === "string")) {
    return true;
  }
  if (Array.isArray(opp.evidence) && opp.evidence.length) return true;
  return false;
}

function isPlacedNoOverflow(opp = {}) {
  const venue = String(opp.venueSourcingStatus || opp.venueStatus || "").toUpperCase();
  const blob = textBlob(opp);
  if (/FULLY_PLACED|PRIMARY_.*NO_OVERFLOW/i.test(venue)) return true;
  if (/overnight stays? (included|at the)|package includes?.*(room|lodging)|hosted at .{0,40}palace/i.test(blob)) {
    return true;
  }
  return false;
}

function isStale(opp = {}, nowDate = new Date().toISOString().slice(0, 10)) {
  const end = String(opp.eventEndDate || opp.eventStartDate || "").slice(0, 10);
  if (end && end < nowDate) return true;
  // Past-year conferences without next-cycle proof
  if (end && end < PAST_YEAR_CUTOFF && !hasFutureCycleBasis(opp)) return true;
  return false;
}

function isOutOfMarket(opp = {}, marketHints = []) {
  const blob = textBlob(opp).toLowerCase();
  const dest = String(
    opp.destinationStatus || opp.eventLocationSummary || opp.eventLocation || ""
  ).toLowerCase();
  if (
    opp.geoClass === "GEO_CONFLICT" &&
    /FAR_STATE|WRONG_MARKET|FAR_CITY|OUTSIDE_CATCHMENT/i.test(String(opp.geoConflictReason || ""))
  ) {
    return true;
  }
  if (/out_of_market|outside_catchment/i.test(String(opp.geoClass || ""))) return true;
  // English Football League "Championship" is not Galicia demand
  if (/\befl\b|championship 26\/27|premier league\b/i.test(blob) && marketHints.some((h) => /coru|galicia/i.test(h))) {
    return true;
  }
  if (marketHints.length) {
    const hints = marketHints.map((h) => String(h).toLowerCase()).filter(Boolean);
    const inHint = (s) => hints.some((h) => s.includes(h) || h.includes(s));
    // Explicit destination city that does not match hotel market hints
    const farCities =
      /\b(hannover|hanover|hamburg|berlin|frankfurt|cologne|köln|koln|stuttgart|düsseldorf|dusseldorf|bordeaux|talence|paris|london|new york|los angeles|tokyo|dubai|madrid|barcelona|rome|milan)\b/i;
    const destFar = farCities.test(dest) && !inHint(dest);
    const blobFar =
      farCities.test(blob) &&
      !hints.some((h) => blob.includes(h));
    if (destFar || blobFar) return true;
  }
  return false;
}

function isDuplicateOf(opp, seenKeys) {
  const series =
    opp.eventSeriesKey ||
    opp.eventSeriesId ||
    opp.seriesId ||
    null;
  const titleNorm = String(opp.title || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 48);
  const start = String(opp.eventStartDate || "").slice(0, 10);
  const key = series
    ? `${series}|${start || "nodate"}`
    : `${titleNorm}|${start || "nodate"}|${String(opp.organizationName || "")
        .toLowerCase()
        .slice(0, 24)}`;
  if (seenKeys.has(key)) return { duplicate: true, key };
  seenKeys.add(key);
  return { duplicate: false, key };
}

/**
 * @param {object} opportunity
 * @param {{ nowDate?: string, marketHints?: string[], seenKeys?: Set<string>, allowInferredTrigger?: boolean }} [opts]
 * @returns {{ ok: boolean, class: string, reasons: string[], trigger: object|null, latestEvidenceDate: string|null, duplicateKey: string|null }}
 */
export function isValidFutureWatch(opportunity = {}, opts = {}) {
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const marketHints = opts.marketHints || [];
  const seenKeys = opts.seenKeys || new Set();
  const allowInferredTrigger = opts.allowInferredTrigger !== false;
  const reasons = [];

  if (!opportunity || !(opportunity.id || opportunity.opportunityId || opportunity.title)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.OTHER,
      reasons: ["missing_opportunity"],
      trigger: null,
      latestEvidenceDate: null,
      duplicateKey: null,
    };
  }

  if (opportunity.watchExcludedFromFutureWatch === true && opts.respectExclusion !== false) {
    return {
      ok: false,
      class:
        opportunity.watchValidation?.class ||
        WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["watch_excluded_from_future_watch"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: null,
    };
  }

  const priority = String(opportunity.priority || "").toUpperCase();
  if (priority === "DISQUALIFIED" || priority === "CLOSED") {
    // Still allow content re-audit when caller asks
    if (opts.ignoreTerminalPriority !== true) {
      return {
        ok: false,
        class: WATCH_VALIDATION_CLASS.OTHER,
        reasons: ["already_closed_or_disqualified"],
        trigger: null,
        latestEvidenceDate: latestEvidenceDate(opportunity),
        duplicateKey: null,
      };
    }
  }

  if (isPlacedNoOverflow(opportunity)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.PLACED_NO_OVERFLOW,
      reasons: ["placed_no_overflow"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: null,
    };
  }

  if (isStale(opportunity, nowDate)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.STALE,
      reasons: ["past_or_stale_cycle"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: null,
    };
  }

  if (isOutOfMarket(opportunity, marketHints)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.OUT_OF_MARKET,
      reasons: ["out_of_market_geography"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: null,
    };
  }

  if (SUPPLY_SIDE_RE.test(textBlob(opportunity)) || /self.?promot|resort is targeting/i.test(textBlob(opportunity))) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.NO_HOTEL_FIT,
      reasons: ["supply_side_or_self_promotion"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: null,
    };
  }

  if (WEBINAR_RE.test(String(opportunity.title || "")) && !opportunity.publishedPeakRooms) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.NO_HOTEL_FIT,
      reasons: ["webinar_no_lodging_thesis"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: null,
    };
  }

  const dup = isDuplicateOf(opportunity, seenKeys);
  if (dup.duplicate) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.DUPLICATE,
      reasons: ["duplicate_series_or_entity_cycle"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  if (!hasStableEntity(opportunity)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.ENTITY_INVALID,
      reasons: ["unstable_or_generic_demand_entity"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  // External-demand invariant — hotel-hosted product is not Valid Watch
  // unless an independent external demand entity + incremental rooms exist.
  const externalDemand = assertExternalDemandForCustomerOpportunity(opportunity);
  if (!externalDemand.ok) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.ENTITY_INVALID,
      reasons: externalDemand.reasons,
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  if (!hasHotelFitThesis(opportunity)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.NO_HOTEL_FIT,
      reasons: ["missing_or_thin_hotel_fit_thesis"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  if (!hasEvidence(opportunity)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.INSUFFICIENT_EVIDENCE_NOT_WATCH,
      reasons: ["no_evidence_provenance"],
      trigger: null,
      latestEvidenceDate: null,
      duplicateKey: dup.key,
    };
  }

  if (!hasFutureCycleBasis(opportunity)) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["no_future_cycle_basis"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  const futState = String(opportunity.futureCycleEvidenceState || "").toUpperCase();
  const whyMon = String(opportunity.whyMonitor || "").trim();
  // Template "recurring pattern" monitor text is research backlog, not Future Watch,
  // unless the future cycle is already confirmed.
  if (
    isTemplateMonitorRationale(whyMon) &&
    futState !== "CURRENT_FUTURE_CYCLE_CONFIRMED"
  ) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["template_monitor_rationale_without_confirmed_cycle"],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  const notNow = whyNotActionableNow(opportunity);
  if (!notNow.length) {
    reasons.push("missing_why_not_actionable_now");
  }

  const trigger = resolveTrigger(opportunity);
  if (!trigger) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["no_next_validation_trigger", ...reasons],
      trigger: null,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }
  if (trigger.incomplete && !allowInferredTrigger) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["trigger_incomplete_no_research_window", ...reasons],
      trigger,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }
  // Inferred NEXT_CYCLE without date window is backlog unless explicit monitor rationale + entity
  if (
    trigger.type === WATCH_TRIGGER_TYPE.NEXT_CYCLE_ANNOUNCEMENT &&
    !trigger.researchDate &&
    !String(opportunity.whyMonitor || "").trim()
  ) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["future_unconfirmed_without_research_window", ...reasons],
      trigger,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  if (!notNow.length) {
    return {
      ok: false,
      class: WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH,
      reasons: ["missing_why_not_actionable_now"],
      trigger,
      latestEvidenceDate: latestEvidenceDate(opportunity),
      duplicateKey: dup.key,
    };
  }

  return {
    ok: true,
    class: WATCH_VALIDATION_CLASS.VALID_FUTURE_WATCH,
    reasons: ["passes_future_watch_law", ...notNow],
    trigger: {
      type: trigger.type,
      condition: trigger.condition,
      researchDate: trigger.researchDate || null,
      inferred: Boolean(trigger.inferred),
    },
    latestEvidenceDate: latestEvidenceDate(opportunity),
    duplicateKey: dup.key,
  };
}

/**
 * Count valid watches from a bag (deduping via seenKeys).
 */
export function countValidFutureWatches(opportunities = [], opts = {}) {
  const seenKeys = opts.seenKeys || new Set();
  let valid = 0;
  const byClass = {};
  for (const o of opportunities || []) {
    const v = isValidFutureWatch(o, { ...opts, seenKeys });
    byClass[v.class] = (byClass[v.class] || 0) + 1;
    if (v.ok) valid += 1;
  }
  return { valid, byClass, total: (opportunities || []).length };
}
