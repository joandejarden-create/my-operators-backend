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
  /\b(conference|congress|summit|convention|exhibition|trade show|training|workshop|tournament|festival|wedding|relocation|offsite|banquet|catering|advocacy|symposium|forum|showcase|cup|meeting)\b/i;
const SCALE_TEXT_RE =
  /\b(?:~?\d{2,5}(?:\s*[–-]\s*\d{2,5})?\+?\s*(?:attendees|delegates|participants|exhibitors|visitors|guests|teams|peak rooms|rooms)|(?:attendance|attendees|delegates|participants|teams|peak rooms)\s*(?:of\s*)?~?\d{2,5})\b/i;
const TIMING_TEXT_RE =
  /\b(20[2-3]\d|january|february|march|april|may|june|july|august|september|october|november|december)\b/i;
const LOCATION_TEXT_RE =
  /\b(downtown|convention center|convention centre|held at|held in|\bcity\b|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*(?:[A-Z]{2}|DC|D\.C\.)|Washington(?:\s*,?\s*DC)?|New York|San Diego|Arlington|Bethesda|Canc[uú]n|Guadalajara|Punta Cana|Boca Raton|Times Square)\b/i;
const PLACEHOLDER_FACT_RE =
  /^(official source|source|n\/a|unknown|tbd|none)$/i;

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

function collectIndependentStatements(opportunity = {}, extraction = {}) {
  const statements = [];
  for (const source of Array.isArray(opportunity.sources) ? opportunity.sources : []) {
    if (source && typeof source === "object") {
      statements.push(
        source.quote,
        source.excerpt,
        source.evidenceQuote,
        source.text,
        source.evidenceText,
        source.claim,
        source.establishes,
        source.title,
        source.name,
      );
    }
  }
  for (const item of Array.isArray(opportunity.evidence) ? opportunity.evidence : []) {
    if (item && typeof item === "object") {
      const value = text(item.value);
      statements.push(
        item.quote,
        item.excerpt,
        item.snippet,
        item.text,
        item.evidenceText,
        PLACEHOLDER_FACT_RE.test(value) ? "" : item.value,
        item.extractedText,
        item.sourceTitle,
        item.title,
      );
    }
  }
  statements.push(
    opportunity.evidenceQuote,
    opportunity.evidenceExcerpt,
    opportunity.eventDateEvidenceText,
    opportunity.attendanceEvidenceText,
    opportunity.peakRoomsEvidenceText,
    opportunity.claimEvidence,
    // Structured fields only when already persisted as research facts (not generated thesis).
    opportunity.eventLocationSummary,
    opportunity.eventLocation,
    opportunity.estimatedAttendance && opportunity.estimatedAttendance !== "UNKNOWN"
      ? `${opportunity.estimatedAttendance} attendees`
      : "",
    opportunity.estimatedPeakRooms && opportunity.estimatedPeakRooms !== "UNKNOWN"
      ? `${opportunity.estimatedPeakRooms} peak rooms`
      : "",
    extraction.excerpt,
    extraction.hasHousingLanguage === true ? "verified housing accommodation lodging requirement" : ""
  );
  return statements.map(text).filter(Boolean);
}

function collectIndependentDemandBlob(opportunity = {}, extraction = {}) {
  return collectIndependentStatements(opportunity, extraction).join("\n");
}

const EXPLICIT_DEMAND_RE =
  /\b(stay[- ]to[- ]play|must book through|reservations must be made|room blocks?|guestroom blocks?|housing bureau|housing block|group block|official host hotel|host hotel|official hotel|book your hotel|hotel program|accommodations?(?:\s+(?:at|through|via|partner))?)\b/i;
const NAMED_HOST_HOTEL_RE =
  /\b(?:marriott|hilton|hyatt|westin|sheraton|renaissance|wyndham|voco|hotel)\b[\s\S]{0,60}\bnamed\b|\bnamed\b[\s\S]{0,60}\b(?:marriott|hilton|hyatt|westin|sheraton|renaissance|wyndham|voco|hotel)\b/i;
const STATEMENT_DOES_NOT_PROVE_DEMAND_RE =
  /\b(does not establish|do not establish|not proof|no dedicated|no official|no venue|not yet posted|location to be announced|venue to be announced|exclusion of|without lodging|no hotel|responsibilit|room inventory|address and inventory)\b/i;

function statementSupportsExplicitDemand(statement) {
  if (STATEMENT_DOES_NOT_PROVE_DEMAND_RE.test(statement)) return false;
  if (/\bno\b[\s\S]{0,80}\b(housing|stay|room|hotel|block)\b/i.test(statement)) return false;
  if (/\bnot found\b/i.test(statement)) return false;
  return EXPLICIT_DEMAND_RE.test(statement) || NAMED_HOST_HOTEL_RE.test(statement);
}

function hasReliableInferredSupport(independent, factCodes = []) {
  if (/\b(location to be announced|venue to be announced|hotel tba)\b/i.test(independent)) return false;
  const activity =
    GROUP_ACTIVITY_RE.test(independent) ||
    factCodes.some((code) => /venue|hotel|housing|attendance|scale|team_count|dates/i.test(code));
  const timing = TIMING_TEXT_RE.test(independent) || factCodes.some((code) => TIMING_FACT_RE.test(code) || /20\d{2}/.test(code));
  const location =
    LOCATION_TEXT_RE.test(independent) ||
    factCodes.some((code) => LOCATION_FACT_CODES.has(code) || /city|destination|venue/i.test(code));
  const scale =
    SCALE_TEXT_RE.test(independent) ||
    factCodes.some((code) => SCALE_FACT_CODES.has(code) || /attendance|scale|team_count|peak_rooms|size_fit/i.test(code));
  // Historical host / official hotel precedent plus future timing+location is enough for inferred demand.
  const historicalHost =
    factCodes.some((code) => /historical_host|2026_hotel|host_hotel|hotel_block/i.test(code)) &&
    /\b(marriott|hilton|hyatt|westin|sheraton|renaissance|wyndham|hotel|host)\b/i.test(independent);
  if (historicalHost && timing && location) return true;
  return activity && timing && location && scale;
}

export function classifyDemandMechanismEvidence(opportunity = {}, extraction = {}, opts = {}) {
  const blob = collectDemandBlob(opportunity, extraction);
  const statements = collectIndependentStatements(opportunity, extraction);
  const independent = statements.join("\n");
  const factCodes = collectFactCodes(opportunity);
  const mechanisms = [];
  for (const [key, re] of Object.entries(MECHANISM_HINTS)) {
    if (re.test(independent)) mechanisms.push(key);
  }

  let evidenceClass = DEMAND_EVIDENCE_CLASS.C_PLAUSIBLE_UNSUPPORTED_HYPOTHESIS;
  let reason = "insufficient_hotel_demand_language";
  const fabricated = false;
  const explicitDemand = statements.some(statementSupportsExplicitDemand);
  const negativeFact = factCodes.includes("no_housing_partner") || statements.some((statement) => NEGATIVE_RE.test(statement) || /\bno dedicated hotel\b/i.test(statement));

  if (negativeFact && !explicitDemand) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.D_CONTRADICTORY_OR_NEGATIVE_EVIDENCE;
    reason = "negative_or_contradictory_accommodation_language";
  } else if (explicitDemand || extraction.hasHousingLanguage === true) {
    evidenceClass = DEMAND_EVIDENCE_CLASS.A_VERIFIED_HOTEL_OR_ACCOMMODATION_REQUIREMENT;
    reason = "independent_fact_text_states_explicit_demand";
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
    excerpt: independent.slice(0, 280) || null,
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
