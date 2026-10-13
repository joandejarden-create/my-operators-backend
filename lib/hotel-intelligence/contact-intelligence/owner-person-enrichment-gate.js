import { supportedOwnershipEvidence, supportedAffiliationEvidence, domainEvidenceSupported, classifyContactPurpose } from "./research-evidence.js";
/**
 * Owner-person enrichment gate (hotel → owner/sponsor → person → paid enrich).
 *
 * Owner gate requires captured evidence contents; status flags and URLs alone fail.
 * People search / candidate discovery must NOT use this workflow — discovery
 * may return Surfe-only profiles before corroboration.
 *
 * Explicit hotel_contact and gdi_contact workflows keep validateProviderSubmissionInput alone
 * and must not inherit hotel→owner requirements.
 */

import {
  validateProviderSubmissionInput,
  SUBMISSION_DECISION,
  SUBMISSION_REJECT_REASON,
  nameTokens,
} from "./provider-submission-gate.js";

export const OWNER_PERSON_ENRICHMENT_GATE_VERSION = "ci-owner-person-enrichment-gate-v2";

export const OWNER_PERSON_ENRICHMENT_WORKFLOW = "owner_person_enrichment";
/** Discovery-only — do not require person affiliation corroboration. */
export const OWNER_PERSON_SEARCH_WORKFLOW = "owner_person_search";

export const OWNER_PERSON_REJECT = Object.freeze({
  HOTEL_OWNER_UNSUPPORTED: "HOTEL_OWNER_UNSUPPORTED",
  ORG_IDENTITY_UNRESOLVED: "ORG_IDENTITY_UNRESOLVED",
  PERSON_IDENTITY_UNRESOLVED: "PERSON_IDENTITY_UNRESOLVED",
  AFFILIATION_SURFE_ONLY: "AFFILIATION_SURFE_ONLY",
  AFFILIATION_UNCROBORATED: "AFFILIATION_UNCROBORATED",
  ROLE_NOT_RELEVANT: "ROLE_NOT_RELEVANT",
  EVIDENCE_REFS_MISSING: "EVIDENCE_REFS_MISSING",
  RELATED_DOMAIN_UNATTRIBUTED: "RELATED_DOMAIN_UNATTRIBUTED",
  RETURNED_WRONG_PERSON: "RETURNED_WRONG_PERSON",
  IDENTITY_CONFLICT: "IDENTITY_CONFLICT",
});

export const AFFILIATION_SOURCE_CLASS = Object.freeze({
  SURFE_ONLY: "SURFE_ONLY",
  PROVIDER_DIRECTORY: "PROVIDER_DIRECTORY",
  LINKEDIN_SELF: "LINKEDIN_SELF",
  OFFICIAL_LEADERSHIP: "OFFICIAL_LEADERSHIP",
  COMPANY_ANNOUNCEMENT: "COMPANY_ANNOUNCEMENT",
  FILING: "FILING",
  CREDIBLE_INDEPENDENT: "CREDIBLE_INDEPENDENT",
  CONFERENCE_BIO: "CONFERENCE_BIO",
  NONE: "NONE",
});

const CORROBORATING_CLASSES = new Set([
  AFFILIATION_SOURCE_CLASS.OFFICIAL_LEADERSHIP,
  AFFILIATION_SOURCE_CLASS.COMPANY_ANNOUNCEMENT,
  AFFILIATION_SOURCE_CLASS.FILING,
  AFFILIATION_SOURCE_CLASS.CREDIBLE_INDEPENDENT,
  AFFILIATION_SOURCE_CLASS.CONFERENCE_BIO,
  AFFILIATION_SOURCE_CLASS.LINKEDIN_SELF,
]);

const RELEVANT_ROLE_RE =
  /\b(development|desarroll|acquisition|investment|invers|asset\s*manag|cfo|chief\s*financial|principal|owner|sponsor|ceo|chief\s*executive|president|director|executive\s*vp|evp|svp|managing\s*director|founder|co-?owner|strategy|project\s*development|hospitality\s*portfolio)\b/i;

const WEAK_ROLE_RE =
  /\b(training|hr\b|human\s*resources|front\s*desk|reservations|concierge|sales\s*coordinator|social\s*media|receptionist)\b/i;

function asList(v) {
  if (!v) return [];
  return Array.isArray(v) ? v.filter(Boolean) : [v].filter(Boolean);
}

function hostOf(domainOrUrl) {
  const s = String(domainOrUrl || "").trim();
  if (!s) return "";
  try {
    const u = s.includes("://") ? new URL(s) : new URL(`https://${s}`);
    return u.hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return s.replace(/^www\./, "").split("/")[0].toLowerCase();
  }
}

function domainCoreLabels(host) {
  return String(host || "")
    .toLowerCase()
    .replace(/\.(com|net|org|co|ky|mx|es|io|example)(\.[a-z]{2})?$/i, "")
    .split(/[.-]/)
    .filter((t) => t.length >= 4 && !["grupo", "group", "www", "mail"].includes(t));
}

function domainsRelated(a, b) {
  const ha = hostOf(a);
  const hb = hostOf(b);
  if (!ha || !hb) return false;
  if (ha === hb) return true;
  if (ha.endsWith(`.${hb}`) || hb.endsWith(`.${ha}`)) return true;
  // Sibling brand pattern: iberostar.com ↔ grupoiberostar.com (shared core label)
  const la = domainCoreLabels(ha);
  const lb = domainCoreLabels(hb);
  return la.some((t) => lb.some((u) => t === u || t.includes(u) || u.includes(t)));
}

/**
 * True when affiliation evidence is independently usable for owner-person enrich.
 * Surfe-only / directory-only never qualifies.
 */
export function isAffiliationCorroborated(affiliation = {}, context = {}) {
  const sourceClass = String(affiliation.source_class || affiliation.sourceClass || "").toUpperCase();
  if (!CORROBORATING_CLASSES.has(sourceClass) || /SURFE_ONLY|UNCORROBORATED|FORMER/.test(affiliation.status || "")) return false;
  return supportedAffiliationEvidence(
    affiliation.evidence_refs || affiliation.evidenceRefs,
    context.personName, context.organizationName, context.title
  ).length > 0;
}

export function isRoleRelevantForOwnerContact(title = "") {
  const t = String(title || "");
  if (!t.trim()) return false;
  if (WEAK_ROLE_RE.test(t)) return false;
  return RELEVANT_ROLE_RE.test(t);
}

/**
 * Pre-enrichment gate for owner-person paid lookups (email/mobile).
 * Does not block organization-based people *search*.
 */
export function validateOwnerPersonEnrichmentSubmission(input = {}) {
  const violations = [];
  const warnings = [];
  const hotelOwner = input.hotel_to_owner || input.hotelToOwner || {};
  const affiliation = input.affiliation_corroboration || input.affiliation || {};
  const person = input.person || {};
  const organization = input.organization || {};
  const evidenceRefs = [
    ...asList(hotelOwner.evidence_refs || hotelOwner.evidenceRefs),
    ...asList(affiliation.evidence_refs || affiliation.evidenceRefs),
    ...asList(input.evidence_refs),
  ];

  const ownerSupported = supportedOwnershipEvidence(
    hotelOwner.evidence_refs || hotelOwner.evidenceRefs,
    input.hotel_name || hotelOwner.hotel_name,
    organization.name
  ).length > 0 && hotelOwner.supported !== false &&
    ["PROPERTY_OWNER", "ECONOMIC_OWNER_OR_SPONSOR", "ECONOMIC_OWNER"].includes(
      String(hotelOwner.relationship_class || hotelOwner.relationshipClass || "").toUpperCase());

  if (!ownerSupported) {
    violations.push({
      code: OWNER_PERSON_REJECT.HOTEL_OWNER_UNSUPPORTED,
      detail: "hotel_to_owner_sponsor_not_supported",
    });
  } else if (!asList(hotelOwner.evidence_refs || hotelOwner.evidenceRefs).length && !evidenceRefs.length) {
    violations.push({
      code: OWNER_PERSON_REJECT.EVIDENCE_REFS_MISSING,
      detail: "hotel_owner_evidence_refs_required",
    });
  }

  if (!organization.name && !organization.entity_id) {
    violations.push({
      code: OWNER_PERSON_REJECT.ORG_IDENTITY_UNRESOLVED,
      detail: "organization_identity_required",
    });
  }
  if (organization.relationship_supported === false || organization.rejected === true) {
    violations.push({
      code: OWNER_PERSON_REJECT.ORG_IDENTITY_UNRESOLVED,
      detail: organization.reject_reason || "organization_rejected",
    });
  }

  const personName = person.display_name || person.full_name || "";
  if (!personName || person.identity_supported === false || person.identity_resolved === false) {
    violations.push({
      code: OWNER_PERSON_REJECT.PERSON_IDENTITY_UNRESOLVED,
      detail: "person_identity_not_resolved",
    });
  }

  const sourceClass = String(affiliation.source_class || affiliation.sourceClass || "NONE").toUpperCase();
  if (sourceClass === AFFILIATION_SOURCE_CLASS.SURFE_ONLY || String(affiliation.status || "").toUpperCase() === "SURFE_ONLY") {
    violations.push({
      code: OWNER_PERSON_REJECT.AFFILIATION_SURFE_ONLY,
      detail: "surfe_only_affiliation_cannot_pass_owner_person_enrichment",
    });
  } else if (!isAffiliationCorroborated(affiliation, { personName, organizationName: organization.name, title: person.title || person.job_title })) {
    violations.push({
      code: OWNER_PERSON_REJECT.AFFILIATION_UNCROBORATED,
      detail: "target_affiliation_not_independently_corroborated",
      source_class: sourceClass || null,
    });
  }

  const title = person.title || person.job_title || person.why_relevant || "";
  if (!isRoleRelevantForOwnerContact(title)) {
    violations.push({
      code: OWNER_PERSON_REJECT.ROLE_NOT_RELEVANT,
      detail: `role_not_development_investment_or_ownership_adjacent:${title || "missing"}`,
    });
  }

  if (!asList(affiliation.evidence_refs || affiliation.evidenceRefs).length) {
    violations.push({
      code: OWNER_PERSON_REJECT.EVIDENCE_REFS_MISSING,
      detail: "affiliation_evidence_refs_required_for_gate",
    });
  }

  const domain = input.identifiers?.domain;
  if ((domain?.value || typeof domain === "string") && !domainEvidenceSupported(
    domain?.evidence_refs || organization.domain_evidence,
    domain?.value || domain, organization.name
  )) violations.push({ code: OWNER_PERSON_REJECT.ORG_IDENTITY_UNRESOLVED, detail: "domain_evidence_contents_required" });

  // Shared identifier / domain / LinkedIn checks (hotel & GDI paths use this alone).
  const base = validateProviderSubmissionInput({
    ...input,
    provider: input.provider || "surfe",
    allow_linkedin_only: input.allow_linkedin_only,
  });
  violations.push(...(base.violations || []));
  warnings.push(...(base.warnings || []));

  const ok = violations.length === 0;
  return {
    ok,
    version: OWNER_PERSON_ENRICHMENT_GATE_VERSION,
    workflow: OWNER_PERSON_ENRICHMENT_WORKFLOW,
    decision: ok ? SUBMISSION_DECISION.ALLOW : SUBMISSION_DECISION.REJECT,
    violations,
    warnings,
    sanitized_identifiers: base.sanitized_identifiers,
    alternative_chain_allowed: base.alternative_chain_allowed,
    evidence_refs_seen: evidenceRefs,
  };
}

/**
 * Returned-result checks — independent of whether input was well-qualified.
 */
export function evaluateOwnerPersonReturnedContact({
  personName,
  targetOrganization,
  targetDomain,
  providerEmail,
  providerCompanyName,
  providerFullName,
  providerValidationStatus,
  relatedDomainAllowedWithOrgSupport = false,
  organizationRelationshipSupported = false,
  personAttributionSupported = false,
  identityConflict = false,
  qualificationInput = null,
} = {}) {
  const labels = {
    evidence_qualification: "UNRESOLVED",
    provider_deliverability: String(providerValidationStatus || "UNKNOWN").toUpperCase() || "UNKNOWN",
    freshness: "UNKNOWN",
    usage_rights: "LICENSING_PENDING_CUSTOMER_DISPLAY_BLOCKED",
  };

  if (identityConflict) {
    return {
      accept_person_attributed_email: false,
      contact_class: "IDENTITY_CONFLICT",
      reason: OWNER_PERSON_REJECT.IDENTITY_CONFLICT,
      labels,
    };
  }

  if (providerFullName && personName) {
    const want = nameTokens(personName);
    const got = nameTokens(providerFullName);
    const firstOk = want[0] && got.some((t) => t === want[0]);
    const surOk =
      want.length < 2 ||
      want.slice(1).some((s) => got.some((t) => t === s));
    if (!firstOk || !surOk) {
      return {
        accept_person_attributed_email: false,
        contact_class: "RETURNED_WRONG_PERSON",
        reason: OWNER_PERSON_REJECT.RETURNED_WRONG_PERSON,
        labels: { ...labels, evidence_qualification: "REJECTED" },
      };
    }
  }

  const purpose = classifyContactPurpose(providerEmail);
  if (purpose !== "UNDETERMINED") return { accept_person_attributed_email: false, contact_class: purpose, reason: "NOT_PERSON_BUSINESS_ROUTE", labels };

  if (!providerEmail) {
    return {
      accept_person_attributed_email: false,
      contact_class: "NO_CONTACT_FOUND",
      reason: "EMAIL_MISS",
      labels,
    };
  }

  const host = hostOf(providerEmail.includes("@") ? providerEmail.split("@")[1] : providerEmail);
  const target = hostOf(targetDomain);
  const exactOrSub = target && (host === target || host.endsWith(`.${target}`));
  const related = target && host && !exactOrSub && domainsRelated(host, target);

  // Domain similarity never establishes affiliation; callers must provide the original evidence-bearing input.
  const qualification = qualificationInput && validateOwnerPersonEnrichmentSubmission(qualificationInput);
  const samePerson = nameTokens(personName).join(" ") === nameTokens(qualificationInput?.person?.display_name || qualificationInput?.person?.full_name).join(" ");
  const sameDomain = target && target === hostOf(qualification?.sanitized_identifiers?.domain);
  const qualified = qualification?.ok && samePerson && sameDomain;
  if (!related && !qualified) return {
    accept_person_attributed_email: false, contact_class: "PERSON_ATTRIBUTION_UNQUALIFIED",
    reason: "EVIDENCE_QUALIFICATION_REQUIRED", labels,
  };

  if (related) {
    // Legacy boolean permissions cannot establish a sibling-domain relationship.
    // Leave these candidates unresolved until a separately evidenced domain/person binding exists.
    const allow = false;
    return {
      accept_person_attributed_email: Boolean(allow),
      contact_class: allow
        ? "RELATED_DOMAIN_ATTRIBUTED"
        : "RELATED_DOMAIN_UNATTRIBUTED",
      reason: allow ? null : OWNER_PERSON_REJECT.RELATED_DOMAIN_UNATTRIBUTED,
      labels: {
        ...labels,
        evidence_qualification: allow ? "ACCEPTED_RELATED_DOMAIN" : "RELATED_DOMAIN_INSUFFICIENT",
      },
      email_host: host,
      target_domain: target,
    };
  }

  if (!exactOrSub) {
    return {
      accept_person_attributed_email: false,
      contact_class: "FOREIGN_OR_MISMATCHED_DOMAIN",
      reason: "DOMAIN_MISMATCH",
      labels: { ...labels, evidence_qualification: "REJECTED" },
      email_host: host,
      target_domain: target,
    };
  }

  if (!providerFullName || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(providerEmail)) return {
    accept_person_attributed_email: false, contact_class: "RETURNED_IDENTITY_OR_EMAIL_UNRESOLVED", reason: "NAMED_RESULT_REQUIRED", labels,
  };

  // Target-domain match: provider VALID is deliverability signal only.
  // Affiliation evidence gate must already have passed (qualified) — never promote
  // provider-valid alone to accepted owner contact.
  const providerValid = String(providerValidationStatus || "").toUpperCase() === "VALID";
  if (!qualified) {
    return {
      accept_person_attributed_email: false,
      contact_class: "PERSON_COMPANY_RELATIONSHIP_UNSUPPORTED",
      reason: "AFFILIATION_EVIDENCE_GATE_REQUIRED",
      labels: { ...labels, evidence_qualification: "REJECTED", provider_deliverability: providerValid ? "PROVIDER_VALID" : String(providerValidationStatus || "UNKNOWN") },
      note: "Provider VALID ≠ accepted owner contact without corroborated person–company affiliation.",
    };
  }
  return {
    accept_person_attributed_email: providerValid,
    contact_class: providerValid
      ? "CORROBORATED_PERSON_PROVIDER_ATTRIBUTED_EMAIL"
      : "CORROBORATED_PERSON_PROVIDER_EMAIL_UNCERTAIN",
    reason: null,
    labels: {
      ...labels,
      evidence_qualification: "ACCEPTED_PROVIDER_ATTRIBUTED",
      provider_deliverability: providerValid ? "PROVIDER_VALID" : String(providerValidationStatus || "UNKNOWN"),
      freshness: "UNKNOWN",
    },
    email_host: host,
    target_domain: target,
    note: "Provider VALID ≠ independent deliverability verification; verification timestamp unknown unless provider supplies one.",
    provider_company_name: providerCompanyName || null,
    target_organization: targetOrganization || null,
  };
}

/**
 * Explicit acceptance helper: provider email/phone never counts as owner contact
 * unless hotel→owner + person–company affiliation evidence gates pass.
 */
export function acceptProviderContactAsOwnerContact({
  enrichmentGateResult = null,
  returnedContactEval = null,
  providerValidationStatus = null,
} = {}) {
  const gateOk = enrichmentGateResult?.ok === true;
  const returnedOk = returnedContactEval?.accept_person_attributed_email === true;
  const providerValid = String(providerValidationStatus || "").toUpperCase() === "VALID";
  if (!gateOk) {
    return {
      accepted: false,
      reason: "ENRICHMENT_GATE_FAILED",
      provider_valid_ignored: providerValid,
    };
  }
  if (!returnedOk) {
    return {
      accepted: false,
      reason: returnedContactEval?.reason || "RETURNED_CONTACT_REJECTED",
      provider_valid_ignored: providerValid && !returnedOk,
    };
  }
  return { accepted: true, reason: null, provider_valid_ignored: false };
}

/**
 * Supported economic sponsor may qualify without deed/UBO.
 */
export function sponsorQualifiesWithoutDeed({
  relationshipClass,
  evidenceSupported = false,
} = {}) {
  const c = String(relationshipClass || "").toUpperCase();
  if (!evidenceSupported) return { qualifies: false, reason: "evidence_not_supported" };
  if (c === "ECONOMIC_OWNER_OR_SPONSOR" || c === "PROPERTY_OWNER") {
    return {
      qualifies: true,
      reason: "supported_sponsor_or_owner",
      deed_ubo_required: false,
      limitation: "Deed/UBO may remain unresolved; economic sponsorship can still qualify.",
    };
  }
  if (c === "BRAND" || c === "OPERATOR" || c === "REGISTERED_BUSINESS") {
    return {
      qualifies: false,
      reason: "brand_operator_or_registered_business_alone_insufficient",
    };
  }
  return { qualifies: false, reason: "unresolved_or_unsupported_class" };
}
