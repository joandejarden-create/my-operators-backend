/**
 * Shared customer-presentation sanitization for list + detail paths.
 * Hotel-agnostic: no Mayflower/San Diego-only branches.
 *
 * - Assessment-status prose is not a sales summary (belongs in fit caveats).
 * - Provider brand names never reach customer-facing copy.
 * - Withheld overall scores never fall back to stale flat numerics.
 */

export const CUSTOMER_PRESENTATION_SANITIZE_VERSION = "gdi-customer-presentation-sanitize-v3";

/** Top-level flags / IDs that must never reach customer list/detail/share DTOs. */
export const CUSTOMER_INTERNAL_PROVIDER_KEYS = Object.freeze([
  "webhoundUsed",
  "ampfyUsed",
  "surfeUsed",
  "parallelUsed",
  "providerRunId",
  "enrichmentId",
  "creditsConsumed",
  "providerCostUsd",
  "surfeRequestId",
  "parallelTaskId",
  "researchProvider",
  "researchLevel",
]);

/** Keys stripped from evidence / contact nested objects (URLs and titles stay). */
export const CUSTOMER_EVIDENCE_INTERNAL_KEYS = Object.freeze([
  "researchProvider",
  "researchLevel",
  "providerRunId",
  "enrichmentId",
  "creditsConsumed",
  "providerCostUsd",
  "surfeRequestId",
  "requestId",
  "provider",
  "enrichmentProvider",
]);

function normalizeProseKey(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Reject hotel-thesis fallbacks that merely repeat the occasion summary. */
function distinctFromOccasion(candidate, occasion) {
  const a = normalizeProseKey(candidate);
  const b = normalizeProseKey(occasion);
  if (!a) return null;
  if (b && a === b) return null;
  if (b && a.length > 48 && b.includes(a.slice(0, Math.min(96, a.length)))) {
    return null;
  }
  return candidate;
}

export function isAssessmentStatusProse(text) {
  const t = String(text || "");
  return (
    /dimension-level hotel-fit assessment/i.test(t) ||
    /overall numeric (?:score )?(provisional\/)?withheld/i.test(t) ||
    /weights renormalized/i.test(t) ||
    /applicable dimensions still lacking evidence/i.test(t) ||
    /^provisional hotel-fit score\b/i.test(t)
  );
}

export function scrubProviderBrandNames(text) {
  if (text == null || text === "") return text == null ? null : "";
  return String(text)
    .replace(/\bpreviously verified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bverified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bvia Surfe\b/gi, "from prior verified enrichment")
    .replace(/\bSurfe only if\b/gi, "Paid enrichment only if")
    .replace(/\bSurfe\b/gi, "enrichment provider")
    .replace(/\bWebhound\b/gi, "research provider")
    .replace(/\bwebhound\b/gi, "research provider")
    .replace(/\bApollo\.io\b/gi, "enrichment provider")
    .replace(/\bZoomInfo\b/gi, "enrichment provider")
    .replace(/\bParallel\.ai\b/gi, "research provider")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Map internal channel-source enums to customer-safe labels.
 * Preserves first-party / verified meaning; drops provider brand tokens.
 */
export function scrubChannelSourceLabel(value) {
  if (value == null || value === "") return value == null ? null : "";
  let s = String(value).trim();
  if (!s) return "";
  s = s
    .replace(/\bWEBHOUND[_-]?/gi, "")
    .replace(/\bSURFE[_-]?/gi, "")
    .replace(/\bPARALLEL[_-]?/gi, "")
    .replace(/\bAPOLLO[_-]?/gi, "")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!s) return "VERIFIED_PUBLIC";
  if (/^FIRST_PARTY$/i.test(s)) return "FIRST_PARTY";
  if (/^OFFICIAL/i.test(s)) return s.toUpperCase();
  return s;
}

function stripInternalKeys(obj, keys) {
  if (!obj || typeof obj !== "object") return obj;
  const next = { ...obj };
  for (const key of keys) delete next[key];
  return next;
}

function scrubEvidenceItem(item) {
  if (!item || typeof item !== "object") return item;
  const next = stripInternalKeys(item, CUSTOMER_EVIDENCE_INTERNAL_KEYS);
  if (next.sourceTitle) next.sourceTitle = scrubProviderBrandNames(next.sourceTitle);
  if (next.value != null && typeof next.value === "string") {
    next.value = scrubProviderBrandNames(next.value);
  }
  if (next.extractedText) next.extractedText = scrubProviderBrandNames(next.extractedText);
  return next;
}

function scrubEvidenceList(list) {
  if (!Array.isArray(list)) return list || null;
  return list.map(scrubEvidenceItem);
}

function salesFacingField(primary, fallback) {
  const a = scrubProviderBrandNames(primary);
  if (a && !isAssessmentStatusProse(a)) return a;
  const b = scrubProviderBrandNames(fallback);
  if (b && !isAssessmentStatusProse(b)) return b;
  return null;
}

/**
 * Prefer structured assessment over a stale flat Hotel Fit column.
 * When overall numeric is withheld, never surface a leftover score.
 * Uses the shared assessment contract when a structured assessment is present.
 */
export function reconcileHotelFitScoreForDisplay(opportunity = {}) {
  const structured =
    opportunity.hotelFitAssessment && typeof opportunity.hotelFitAssessment === "object"
      ? opportunity.hotelFitAssessment
      : null;
  if (structured) {
    // Lazy import avoided — contract lives next to assessment; inline mirror of
    // withheld precedence so display cannot revive stale flats when assessment says no.
    const basis = structured.calculationBasis || opportunity.hotelFitCalculationBasis || {};
    const status = String(
      structured.overallStatus || opportunity.hotelFitOverallStatus || ""
    ).toLowerCase();
    const rationale = String(
      structured.summary ||
        opportunity.hotelFitRationale ||
        opportunity.hotelFitScoreNote ||
        ""
    );
    const withheldExplicit =
      structured.numericScoreEmitted === false ||
      structured.scored === false ||
      basis.numericScoreEmitted === false ||
      status === "insufficient_evidence" ||
      /overall numeric withheld|withheld — insufficient|not scored — evidence/i.test(
        rationale
      ) ||
      /overall numeric withheld/i.test(String(basis.reason || ""));
    if (withheldExplicit) return null;
    if (structured.numericScoreEmitted === true && structured.hotelFitScore != null) {
      const n = Number(structured.hotelFitScore);
      return Number.isFinite(n) ? n : null;
    }
    if (structured.scored === true) {
      const raw = structured.numericScore ?? structured.hotelFitScore;
      if (raw != null) {
        const n = Number(raw);
        return Number.isFinite(n) ? n : null;
      }
    }
    // Version-only structured object must not revive flat score.
    if (structured.scoringVersion || structured.version) {
      if (structured.numericScoreEmitted !== true && structured.scored !== true) {
        return null;
      }
    }
  }

  const basis = opportunity.hotelFitCalculationBasis || {};
  const status = String(opportunity.hotelFitOverallStatus || "").toLowerCase();
  const rationale = String(opportunity.hotelFitRationale || opportunity.hotelFitScoreNote || "");
  const withheldExplicit =
    basis.numericScoreEmitted === false ||
    status === "insufficient_evidence" ||
    /overall numeric withheld|withheld — insufficient|not scored — evidence/i.test(rationale) ||
    /overall numeric withheld/i.test(String(basis.reason || ""));
  if (withheldExplicit) {
    return null;
  }
  const raw = opportunity.hotelFitScore;
  if (raw == null || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function scrubContactNotes(contact) {
  if (!contact || typeof contact !== "object") return contact || null;
  let next = stripInternalKeys(contact, CUSTOMER_EVIDENCE_INTERNAL_KEYS);
  for (const key of [
    "whyThisContact",
    "notes",
    "verificationNotes",
    "enrichmentNotes",
    "sourceNotes",
    "relevanceEvidence",
  ]) {
    if (next[key]) next[key] = scrubProviderBrandNames(next[key]);
  }
  for (const key of [
    "phoneSource",
    "emailSource",
    "channelSource",
    "sourceType",
    "phoneSourceType",
    "emailSourceType",
  ]) {
    if (next[key]) next[key] = scrubChannelSourceLabel(next[key]);
  }
  // Never expose provider/cost metadata on customer contact objects
  delete next.provider;
  delete next.enrichmentProvider;
  delete next.providerCostUsd;
  delete next.surfeRequestId;
  delete next.requestId;
  delete next.enrichmentId;
  delete next.providerRunId;
  delete next.creditsConsumed;
  return next;
}

function scrubContactList(list) {
  if (!Array.isArray(list)) return list || null;
  return list.map(scrubContactNotes);
}

/**
 * Apply presentation sanitization to one opportunity (list or detail).
 * Does not invent fit scores, contacts, or prose.
 */
export function sanitizeOpportunityForCustomerPresentation(opp = {}) {
  const summaryWhat = salesFacingField(opp.summaryWhat, opp.summaryWhyMatters);
  // Never fall back hotel-fit thesis fields to the occasion summary — that made
  // "Why this hotel" repeat Account/Occasion and hid missing property rationale.
  const hotelOpportunityThesis = distinctFromOccasion(
    salesFacingField(opp.hotelOpportunityThesis, null),
    summaryWhat || opp.summaryWhat
  );
  const summaryWhyHotel = distinctFromOccasion(
    salesFacingField(opp.summaryWhyHotel, null),
    summaryWhat || opp.summaryWhat
  );
  const whyThisHotel = distinctFromOccasion(
    salesFacingField(opp.whyThisHotel, hotelOpportunityThesis || summaryWhyHotel),
    summaryWhat || opp.summaryWhat
  );

  // Assessment-status prose may live in fitExplanation/fitRationale; keep it only
  // for structured caveats (hotelFitRationale / score note), never as sales thesis.
  // When overall is provisional/numeric, drop stale "withheld" fitExplanation copy.
  const fitExplanation = salesFacingField(opp.fitExplanation, null);
  const fitRationale = salesFacingField(opp.fitRationale, null);
  const summaryWhyMatters = salesFacingField(opp.summaryWhyMatters, null);

  const whyHotelDemandRaw = salesFacingField(opp.whyHotelDemand, null);
  const whyHotelDemand = distinctFromOccasion(whyHotelDemandRaw, summaryWhat || opp.summaryWhat);

  const out = {
    ...opp,
    summaryWhat: summaryWhat || opp.summaryWhat || null,
    summaryWhyMatters,
    hotelOpportunityThesis,
    summaryWhyHotel,
    whyThisHotel,
    whyHotelDemand: whyHotelDemand || null,
    hotelFitScore: reconcileHotelFitScoreForDisplay(opp),
    fitExplanation,
    fitRationale,
    hotelFitRationale:
      scrubProviderBrandNames(opp.hotelFitRationale) || opp.hotelFitRationale || null,
    evidenceConfidenceExplanation:
      scrubProviderBrandNames(opp.evidenceConfidenceExplanation) ||
      opp.evidenceConfidenceExplanation ||
      null,
    recommendedAction:
      scrubProviderBrandNames(opp.recommendedAction) || opp.recommendedAction || null,
    whyNow: scrubProviderBrandNames(opp.whyNow) || opp.whyNow || null,
    whoPrimaryReason: scrubProviderBrandNames(opp.whoPrimaryReason) || opp.whoPrimaryReason || null,
    primaryContact: scrubContactNotes(opp.primaryContact),
    backupContacts: scrubContactList(opp.backupContacts),
    contacts: scrubContactList(opp.contacts),
    evidence: scrubEvidenceList(opp.evidence),
    customerPresentationSanitizeVersion: CUSTOMER_PRESENTATION_SANITIZE_VERSION,
  };

  // Drop internal provider flag/ID keys (key names themselves match leak scanners).
  for (const key of CUSTOMER_INTERNAL_PROVIDER_KEYS) {
    delete out[key];
  }

  // Channel ownership labels on the opportunity root (legacy phone/email provenance).
  for (const key of [
    "phoneSource",
    "emailSource",
    "channelSource",
    "contactPhoneSource",
    "contactEmailSource",
  ]) {
    if (out[key]) out[key] = scrubChannelSourceLabel(out[key]);
  }

  return out;
}
