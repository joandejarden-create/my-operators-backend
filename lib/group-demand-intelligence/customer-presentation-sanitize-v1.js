/**
 * Shared customer-presentation sanitization for list + detail paths.
 * Hotel-agnostic: no Mayflower/San Diego-only branches.
 *
 * - Assessment-status prose is not a sales summary (belongs in fit caveats).
 * - Provider brand names never reach customer-facing copy.
 * - Withheld overall scores never fall back to stale flat numerics.
 */

export const CUSTOMER_PRESENTATION_SANITIZE_VERSION =
  "gdi-customer-presentation-sanitize-v1";

export function isAssessmentStatusProse(text) {
  const t = String(text || "");
  return (
    /dimension-level hotel-fit assessment/i.test(t) ||
    /overall numeric (provisional\/)?withheld/i.test(t) ||
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
    .replace(/\bApollo\.io\b/gi, "enrichment provider")
    .replace(/\bZoomInfo\b/gi, "enrichment provider")
    .replace(/\s+/g, " ")
    .trim();
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
 */
export function reconcileHotelFitScoreForDisplay(opportunity = {}) {
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
  const next = { ...contact };
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
  // Never expose provider/cost metadata on customer contact objects
  delete next.provider;
  delete next.enrichmentProvider;
  delete next.providerCostUsd;
  delete next.surfeRequestId;
  delete next.requestId;
  return next;
}

/**
 * Apply presentation sanitization to one opportunity (list or detail).
 * Does not invent fit scores, contacts, or prose.
 */
export function sanitizeOpportunityForCustomerPresentation(opp = {}) {
  const summaryWhat = salesFacingField(opp.summaryWhat, opp.summaryWhyMatters);
  const hotelOpportunityThesis = salesFacingField(
    opp.hotelOpportunityThesis,
    summaryWhat || opp.summaryWhyMatters
  );
  const summaryWhyHotel = salesFacingField(
    opp.summaryWhyHotel,
    summaryWhat || opp.summaryWhyMatters
  );
  const whyThisHotel = salesFacingField(opp.whyThisHotel, hotelOpportunityThesis);

  return {
    ...opp,
    summaryWhat: summaryWhat || opp.summaryWhat || null,
    hotelOpportunityThesis,
    summaryWhyHotel,
    whyThisHotel,
    hotelFitScore: reconcileHotelFitScoreForDisplay(opp),
    fitExplanation:
      scrubProviderBrandNames(opp.fitExplanation) || opp.fitExplanation || null,
    hotelFitRationale:
      scrubProviderBrandNames(opp.hotelFitRationale) || opp.hotelFitRationale || null,
    evidenceConfidenceExplanation:
      scrubProviderBrandNames(opp.evidenceConfidenceExplanation) ||
      opp.evidenceConfidenceExplanation ||
      null,
    recommendedAction:
      scrubProviderBrandNames(opp.recommendedAction) || opp.recommendedAction || null,
    whyNow: scrubProviderBrandNames(opp.whyNow) || opp.whyNow || null,
    whoPrimaryReason:
      scrubProviderBrandNames(opp.whoPrimaryReason) || opp.whoPrimaryReason || null,
    primaryContact: scrubContactNotes(opp.primaryContact),
    customerPresentationSanitizeVersion: CUSTOMER_PRESENTATION_SANITIZE_VERSION,
  };
}
