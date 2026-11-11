/**
 * GDI 2.0 Phase 2G — demand mechanism research classifier.
 *
 * Distinguishes verified accommodation requirements from inferred / speculative /
 * contradictory evidence. Never fabricates room blocks, RFPs, attendance, or intent.
 *
 * Commercial thesis standard mapping (binding):
 *   A → CONFIRMED_DEMAND
 *   B → RELIABLE_INFERRED_DEMAND  (may qualify sales-ready with buyer + hotel fit)
 *   C → SPECULATIVE_DEMAND       (research continuation only)
 *   D → CONTRADICTORY_OR_NEGATIVE
 *
 * GDI discovers proactive demand — Class B does NOT require confirmed room nights,
 * group blocks, RFPs, or booking announcements.
 */

export const DEMAND_MECHANISM_RESEARCH_V1 = "gdi-demand-mechanism-research-v1";

export const DEMAND_EVIDENCE_CLASS = Object.freeze({
  A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT: "A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT",
  B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND: "B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND",
  C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS: "C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS",
  D_CONTRADICTORY_OR_NEGATIVE_EVIDENCE: "D_CONTRADICTORY_OR_NEGATIVE_EVIDENCE",
});

/** @deprecated Prefer DEMAND_THESIS_STANDARD from commercial-demand-thesis-standard-v1.js */
export const DEMAND_THESIS_STANDARD_ALIAS = Object.freeze({
  CONFIRMED_DEMAND: "CONFIRMED_DEMAND",
  RELIABLE_INFERRED_DEMAND: "RELIABLE_INFERRED_DEMAND",
  SPECULATIVE_DEMAND: "SPECULATIVE_DEMAND",
  CONTRADICTORY_OR_NEGATIVE: "CONTRADICTORY_OR_NEGATIVE",
});

const VERIFIED_ACCOMMODATION_RE =
  /\b(room.?block|guestroom|guestrooms|hotel block|official hotel|housing bureau|housing block|alojamiento oficial|hospedaje oficial|reservation link|book your hotel|host hotel)\b/i;

const REALISTIC_DEMAND_RE =
  /\b(housing|accommodation|lodging|overflow|overnight|delegate travel|visiting (?:team|staff|crew)|extended stay|training cohort|offsite|meeting rooms?|catering|banquet|workshop|reception|deployment|field team|exhibitor|hospedaje|alojamiento|sal[oó]n)\b/i;

const MECHANISM_HINTS = Object.freeze({
  TRAVELING_MEETING_PARTICIPANTS: /\b(meeting|meetings|offsite|summit|workshop|retreat)\b/i,
  CORPORATE_TRAINING_COHORTS: /\b(training|cohort|academy|onboarding|l&d|learning)\b/i,
  PROJECT_DEPLOYMENTS_EXTENDED_STAYS: /\b(deployment|implementation|extended stay|project team|field service|site work)\b/i,
  ASSOCIATION_HOUSING: /\b(housing|room.?block|congress|conference|delegate|hospedaje)\b/i,
  EXHIBITOR_SUPPLIER_ACCOMMODATION: /\b(exhibitor|booth|trade show|supplier|contractor)\b/i,
  RECURRING_CORPORATE_VISITATION: /\b(weekly|monthly|recurring|regular travel|visiting office)\b/i,
  LOCAL_CATERING_MEETING_SPACE: /\b(catering|banquet|meeting room|ballroom|sal[oó]n|coffee break)\b/i,
});

const NEGATIVE_RE =
  /\b(no hotel|without lodging|virtual.?only|fully online|no overnight|day.?only event|cancelled housing|does not provide accommodation)\b/i;

/** Stored evidence.supportsFact codes that directly record accommodation or catering demand. */
export const CONFIRMED_DEMAND_FACT_CODES = Object.freeze([
  "hotel_block",
  "host_hotel",
  "housing",
  "housing_partner",
  "dates_housing",
  "stay_to_play",
  "natcher_lodging_precedent",
  "2026_hotel",
  "historical_host",
  "peak_rooms_estimate",
]);

export const NEGATIVE_DEMAND_FACT_CODES = Object.freeze(["no_housing_partner"]);

const TIMING_FACT_RE = /dates/;
const LOCATION_FACT_CODES = new Set([
  "dates_city",
  "destination",
  "destination_dc",
  "venue",
  "2026_venue",
  "2027_venue_status",
  "2027_city_dates",
  "dates_venue",
  "2027_destination",
  "venue_pattern",
  "2026_venue_pattern",
]);
const SCALE_FACT_CODES = new Set([
  "2026_attendance",
  "2025_attendance",
  "attendance_pattern",
  "2026_scale",
  "team_count_pattern",
  "size_fit",
  "format_ceiling",
  "peak_rooms_estimate",
]);
const DIRECT_DEMAND_RE =
  /\b(room.?block|guestroom|guestrooms|hotel block|official hotel|housing bureau|housing block|host hotel|accommodation|lodging|housing|overnight|meeting rooms?|catering|banquet|alojamiento|hospedaje|sal[oó]n)\b/i;
const GROUP_ACTIVITY_RE =
  /\b(conference|congress|summit|convention|exhibition|trade show|training|workshop|tournament|festival|wedding|relocation|offsite|banquet|catering)\b/i;
const SCALE_TEXT_RE =
  /\b\d{2,5}\+?\s*(attendees|delegates|participants|exhibitors|visitors|guests|teams)\b/i;
const TIMING_TEXT_RE =
  /\b(20[2-3]\d|january|february|march|april|may|june|july|august|september|october|november|december)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function collectDemandBlob(opportunity = {}, extraction = {}) {
  return [
    opportunity.hotelDemandThesis,
    opportunity.hotelOpportunityThesis,
    opportunity.potentialHotelBusiness,
    opportunity.proposedHotelUse,
    opportunity.summaryWhyHotel,
    opportunity.roomDemandRationale,
    opportunity.evidenceQuote,
    extraction.excerpt,
    extraction.hasHousingLanguage ? "housing accommodation lodging" : "",
  ]
    .map(text)
    .filter(Boolean)
    .join(" — ");
}

/**
 * Classify demand evidence for an opportunity (+ optional page extraction).
 */
function collectFactCodes(opportunity = {}) {
  const codes = [];
  for (const item of Array.isArray(opportunity.evidence) ? opportunity.evidence : []) {
    if (item?.supportsFact) codes.push(String(item.supportsFact));
  }
  for (const source of Array.isArray(opportunity.sources) ? opportunity.sources : []) {
    if (source?.supportsFact) codes.push(String(source.supportsFact));
  }
  return codes;
}

function collectIndependentDemandBlob(opportunity = {}, extraction = {}) {
  const quotes = [];
  for (const source of Array.isArray(opportunity.sources) ? opportunity.sources : []) {
    if (source && typeof source === "object") {
      quotes.push(
        source.quote,
        source.excerpt,
        source.evidenceQuote,
        source.text,
        source.evidenceText,
        source.title,
        source.name,
        source.claim,
        source.establishes
      );
    }
  }
  for (const item of Array.isArray(opportunity.evidence) ? opportunity.evidence : []) {
    if (item && typeof item === "object") {
      quotes.push(
        item.quote,
        item.excerpt,
        item.snippet,
        item.text,
        item.evidenceText,
        item.sourceTitle,
        item.extractedText
      );
    }
  }
  return [
    opportunity.evidenceQuote,
    opportunity.evidenceExcerpt,
    opportunity.eventDateEvidenceText,
    opportunity.attendanceEvidenceText,
    opportunity.peakRoomsEvidenceText,
    opportunity.claimEvidence,
    extraction.excerpt,
    extraction.hasHousingLanguage === true
      ? "verified housing accommodation lodging requirement"
      : "",
    ...quotes,
  ]
    .filter(Boolean)
    .join("\n");
}

function hasReliableInferredSupport(independent, factCodes) {
  const activity = GROUP_ACTIVITY_RE.test(independent);
  const timing = TIMING_TEXT_RE.test(independent) || factCodes.some((code) => TIMING_FACT_RE.test(code));
  const location = factCodes.some((code) => LOCATION_FACT_CODES.has(code));
  const scale = SCALE_TEXT_RE.test(independent) || factCodes.some((code) => SCALE_FACT_CODES.has(code));
  return activity && timing && location && scale;
}

export function classifyDemandMechanismEvidence(opportunity = {}, extraction = {}, opts = {}) {
  const blob = collectDemandBlob(opportunity, extraction);
  const independent = collectIndependentDemandBlob(opportunity, extraction);
  const factCodes = collectFactCodes(opportunity);
  const mechanisms = [];
  for (const [key, re] of Object.entries(MECHANISM_HINTS)) {
    if (re.test(independent)) mechanisms.push(key);
  }

  let evidenceClass = DEMAND_EVIDENCE_CLASS.C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS;
  let reason = "insufficient_hotel_demand_language";
  const fabricated = false;
  const confirmedFact = factCodes.some((code) => CONFIRMED_DEMAND_FACT_CODES.includes(code));
  const negativeFact = factCodes.some((code) => NEGATIVE_DEMAND_FACT_CODES.includes(code));

  if ((NEGATIVE_RE.test(independent) || negativeFact) && !confirmedFact && !DIRECT_DEMAND_RE.test(independent)) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.D_CONTRADICTORY_OR_NEGATIVE_EVIDENCE;
    reason = "negative_or_contradictory_accommodation_language";
  } else if (confirmedFact || DIRECT_DEMAND_RE.test(independent) || extraction.hasHousingLanguage === true) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT;
    reason = confirmedFact
      ? "stored_evidence_fact_confirms_accommodation_or_catering"
      : "verified_accommodation_or_housing_language_present";
  } else if (hasReliableInferredSupport(independent, factCodes)) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND;
    reason = "activity_timing_location_and_scale_in_independent_evidence";
  } else if (VERIFIED_ACCOMMODATION_RE.test(blob) || REALISTIC_DEMAND_RE.test(blob)) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS;
    reason = "generated_thesis_wording_is_not_independent_evidence";
  } else if (blob.length > 40) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS;
    reason = "narrative_present_without_hotel_demand_anchors";
  }

  const strengthensEligibility =
    evidenceClass === DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT ||
    evidenceClass === DEMAND_EVIDENCE_CLASS.B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND;

  const thesisStandard =
    evidenceClass === DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT
      ? DEMAND_THESIS_STANDARD_ALIAS.CONFIRMED_DEMAND
      : evidenceClass === DEMAND_EVIDENCE_CLASS.B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND
        ? DEMAND_THESIS_STANDARD_ALIAS.RELIABLE_INFERRED_DEMAND
        : evidenceClass === DEMAND_EVIDENCE_CLASS.D_CONTRADICTORY_OR_NEGATIVE_EVIDENCE
          ? DEMAND_THESIS_STANDARD_ALIAS.CONTRADICTORY_OR_NEGATIVE
          : DEMAND_THESIS_STANDARD_ALIAS.SPECULATIVE_DEMAND;

  return {
    version: DEMAND_MECHANISM_RESEARCH_V1,
    evidenceClass,
    thesisStandard,
    reason,
    mechanismsDetected: mechanisms,
    strengthensEligibility,
    mayQualifySalesReadyDemandGate: strengthensEligibility,
    fabricatedRoomBlocks: fabricated,
    fabricatedRfpOrIntent: false,
    excerpt: blob.slice(0, 280) || null,
    note:
      "CONFIRMED (A) and RELIABLE_INFERRED (B) may satisfy the demand gate with a verified commercially relevant buyer and hotel fit. SPECULATIVE (C) stays in research continuation. Does not require confirmed room nights, group blocks, RFP, or booking announcement.",
    sourceUrl: opts.sourceUrl || null,
  };
}

/**
 * Suggest whether extracted page text should update hotelDemandThesis (clone only).
 */
export function proposeDemandThesisUpdate(opportunity = {}, extraction = {}) {
  const classified = classifyDemandMechanismEvidence(opportunity, extraction);
  if (
    classified.evidenceClass !== DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT &&
    classified.evidenceClass !== DEMAND_EVIDENCE_CLASS.B_EVIDENCE_GROUNDED_REALISTIC_HOTEL_DEMAND
  ) {
    return { update: false, classified };
  }
  const existing = text(opportunity.hotelDemandThesis || opportunity.hotelOpportunityThesis);
  if (existing && /guestroom|lodging|housing|hotel|accommodation|room block|meeting/i.test(existing)) {
    return { update: false, classified, reason: "existing_thesis_already_grounded" };
  }
  if (!extraction.hasHousingLanguage && classified.evidenceClass !== DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT) {
    return { update: false, classified, reason: "no_new_page_housing_language" };
  }
  const proposed = [
    existing,
    "Official source references housing / hotel / accommodation for this activity.",
  ]
    .filter(Boolean)
    .join(" — ")
    .slice(0, 500);
  return {
    update: true,
    classified,
    proposedHotelDemandThesis: proposed,
    proposedHotelUse:
      text(opportunity.proposedHotelUse) ||
      "Overflow / housing participation supported by official event language",
  };
}
