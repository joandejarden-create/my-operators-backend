/**
 * Separate hotel/property facts from agreement existence, terms, and current status.
 * A bad extracted date must not force the whole agreement into CONFLICTED.
 */

export const CONTRACT_INTELLIGENCE_IDENTITY_LAYERS_V1 = "contract-intelligence-identity-layers-v1";

export const IDENTITY_LAYER = Object.freeze({
  HOTEL_PROPERTY_FACT: "HOTEL_PROPERTY_FACT",
  AGREEMENT_EXISTENCE: "AGREEMENT_EXISTENCE",
  AGREEMENT_TERMS: "AGREEMENT_TERMS",
  CONTRACTUAL_EVENT: "CONTRACTUAL_EVENT",
  UNRESOLVED_OR_CONFLICTING: "UNRESOLVED_OR_CONFLICTING",
});

export const VERIFICATION_GATE = Object.freeze({
  AGREEMENT_EXISTENCE_VERIFIED: "AGREEMENT_EXISTENCE_VERIFIED",
  CONTRACT_TERMS_VERIFIED: "CONTRACT_TERMS_VERIFIED",
  CURRENT_STATUS_VERIFIED: "CURRENT_STATUS_VERIFIED",
});

/**
 * Opening / year-built dates are hotel property facts, not contract expirations.
 */
export function classifyOpeningDateAsHotelFact(obs = {}) {
  return {
    layer: IDENTITY_LAYER.HOTEL_PROPERTY_FACT,
    factKind: "OPENING_OR_YEAR_BUILT",
    primaryDate: obs.primaryDate || null,
    hotelName: obs.hotelName || null,
    sourceUrl: (obs.sourceUrls || [])[0] || obs.sourceUrl || null,
    evidenceSnippet: obs.evidenceSnippet || null,
    notContractExpiration: true,
    hpcCopyAuthorized: false,
    note: "Preserve as hotel/property fact research; do not treat as franchise/management expiration",
  };
}

/**
 * Status policy when only the dated claim failed semantic audit.
 */
export function contractStatusAfterDateQuarantine(opts = {}) {
  if (opts.agreementEvidenceContradictory) {
    return {
      contractStatus: "CONFLICTED",
      reason: "agreement_evidence_contradictory",
    };
  }
  return {
    contractStatus: opts.preferredStatus || "EXPIRED_STATUS_UNCONFIRMED",
    reason: "date_claim_quarantined_agreement_row_preserved",
    decisionWindowsEligible: false,
    preserveNotesQuarantine: true,
  };
}

export default {
  IDENTITY_LAYER,
  VERIFICATION_GATE,
  classifyOpeningDateAsHotelFact,
  contractStatusAfterDateQuarantine,
};
