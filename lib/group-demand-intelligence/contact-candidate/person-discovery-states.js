/**
 * Employment + event-relationship states for GDI official-source person discovery.
 * Separate from candidate score / reachability.
 */

export const EMPLOYMENT_STATUS = Object.freeze({
  CURRENT_CONFIRMED: "CURRENT_CONFIRMED",
  CURRENT_PROBABLE: "CURRENT_PROBABLE",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  FORMER_EMPLOYEE: "FORMER_EMPLOYEE",
  UNKNOWN: "UNKNOWN",
});

export const EVENT_RELATIONSHIP = Object.freeze({
  CURRENT_EVENT_CONTACT: "CURRENT_EVENT_CONTACT",
  CURRENT_ROLE_LIKELY_OWNER: "CURRENT_ROLE_LIKELY_OWNER",
  HISTORICAL_EVENT_CONTACT: "HISTORICAL_EVENT_CONTACT",
  CURRENT_SUCCESSOR_ROLE: "CURRENT_SUCCESSOR_ROLE",
  ORGANIZATION_ROLE_ONLY: "ORGANIZATION_ROLE_ONLY",
  WEAK_CONTEXT: "WEAK_CONTEXT",
});

export const PRIMARY_KIND = Object.freeze({
  NAMED_PERSON: "NAMED_PERSON",
  FUNCTIONAL_ENTITY: "FUNCTIONAL_ENTITY",
  UNRESOLVED: "UNRESOLVED",
});

export const ENRICHMENT_GAP = Object.freeze({
  EMAIL_GAP: "EMAIL_GAP",
  PHONE_GAP: "PHONE_GAP",
  BOTH_MISSING: "BOTH_MISSING",
  NO_ENRICHMENT_NEEDED: "NO_ENRICHMENT_NEEDED",
});

export const SOURCE_TYPE = Object.freeze({
  OFFICIAL_EVENT_SITE: "OFFICIAL_EVENT_SITE",
  OFFICIAL_ORG_SITE: "OFFICIAL_ORG_SITE",
  STAFF_DIRECTORY: "STAFF_DIRECTORY",
  PROSPECTUS_PDF: "PROSPECTUS_PDF",
  REGISTRATION_PAGE: "REGISTRATION_PAGE",
  HOUSING_PAGE: "HOUSING_PAGE",
  PRIOR_YEAR_EVENT: "PRIOR_YEAR_EVENT",
  PUBLIC_PROFESSIONAL_PROFILE: "PUBLIC_PROFESSIONAL_PROFILE",
  STANDARD_WEB: "STANDARD_WEB",
});

/** LinkedIn-only must not establish event ownership. */
export function isLinkedInOnlyEvidence(contact = {}) {
  const urls = [contact.sourceUrl, contact.source, ...(contact.evidenceUrls || [])]
    .filter(Boolean)
    .map((u) => String(u).toLowerCase());
  if (!urls.length) return false;
  const allLi = urls.every((u) => /linkedin\.com/i.test(u));
  return allLi;
}

/**
 * Apply employment / event-relationship gates before primary selection.
 */
export function applyPersonDiscoveryGates(contact = {}, opportunity = {}) {
  const out = { ...contact };
  const employment = out.employmentStatus || EMPLOYMENT_STATUS.UNKNOWN;
  const eventRel = out.eventRelationship || EVENT_RELATIONSHIP.WEAK_CONTEXT;

  if (employment === EMPLOYMENT_STATUS.FORMER_EMPLOYEE) {
    out.rejected = true;
    out.rejectReason = "former_employee";
    out.staleUnverified = true;
    out.stillEmployed = false;
  }

  if (isLinkedInOnlyEvidence(out) && eventRel !== EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT) {
    // LinkedIn corroboration alone cannot establish event ownership
    if (
      eventRel === EVENT_RELATIONSHIP.WEAK_CONTEXT ||
      eventRel === EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY ||
      !out.sourceUrl ||
      /linkedin\.com/i.test(String(out.sourceUrl))
    ) {
      out.identityStatus = "IDENTITY_UNCONFIRMED";
      out.linkedinOnlyBlocked = true;
      // Do not auto-reject if another official URL exists in evidenceUrls
      const hasOfficial = (out.evidenceUrls || []).some(
        (u) => u && !/linkedin\.com/i.test(String(u))
      );
      if (!hasOfficial && /linkedin\.com/i.test(String(out.sourceUrl || ""))) {
        out.rejected = true;
        out.rejectReason = "linkedin_only_insufficient_for_event_ownership";
      }
    }
  }

  if (employment === EMPLOYMENT_STATUS.HISTORICAL_ONLY && !out.stillEmployed) {
    out.historicalOnly = true;
  }

  if (
    employment === EMPLOYMENT_STATUS.CURRENT_CONFIRMED ||
    employment === EMPLOYMENT_STATUS.CURRENT_PROBABLE
  ) {
    out.stillEmployed = true;
  }

  if (
    eventRel === EVENT_RELATIONSHIP.HISTORICAL_EVENT_CONTACT &&
    (employment === EMPLOYMENT_STATUS.CURRENT_CONFIRMED ||
      employment === EMPLOYMENT_STATUS.CURRENT_PROBABLE)
  ) {
    out.priorEventInvolvement = true;
    out.reactivationBoost = true;
    out.historicalOnly = true;
  }

  if (eventRel === EVENT_RELATIONSHIP.CURRENT_EVENT_CONTACT) {
    out.eventSpecificEvidence = true;
  }

  if (eventRel === EVENT_RELATIONSHIP.CURRENT_SUCCESSOR_ROLE) {
    out.eventSpecificEvidence = true;
    out.reactivationBoost = true;
  }

  return out;
}

export function classifyEnrichmentGap(contact = {}) {
  const email = Boolean(contact.email);
  const phone = Boolean(contact.phone);
  if (email && phone) return ENRICHMENT_GAP.NO_ENRICHMENT_NEEDED;
  if (!email && !phone) return ENRICHMENT_GAP.BOTH_MISSING;
  if (!email) return ENRICHMENT_GAP.EMAIL_GAP;
  return ENRICHMENT_GAP.PHONE_GAP;
}

export function isSurfeEligiblePerson(candidate = {}, primaryKind) {
  if (primaryKind !== PRIMARY_KIND.NAMED_PERSON) return false;
  if (!candidate?.name) return false;
  if (!["HIGH", "MEDIUM"].includes(candidate.candidateConfidence)) return false;
  if (candidate.employmentStatus === EMPLOYMENT_STATUS.FORMER_EMPLOYEE) return false;
  if (candidate.linkedinOnlyBlocked) return false;
  if (candidate.eventRelationship === EVENT_RELATIONSHIP.WEAK_CONTEXT) return false;
  if (candidate.eventRelationship === EVENT_RELATIONSHIP.ORGANIZATION_ROLE_ONLY) {
    // Org-role-only is fine for WHO discovery, but too weak for paid enrichment
    return false;
  }
  if ((candidate.candidateScore || 0) < 65) return false;
  const gap = classifyEnrichmentGap(candidate);
  return gap !== ENRICHMENT_GAP.NO_ENRICHMENT_NEEDED;
}
