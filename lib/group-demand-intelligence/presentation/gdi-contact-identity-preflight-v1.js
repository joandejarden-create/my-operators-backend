/**
 * GDI Contact Identity Preflight V1
 *
 * Permanent reusable component: maximize correct-person identification and
 * Surfe enrichment readiness BEFORE any paid reveal.
 *
 * Does NOT call Surfe. Does NOT invent LinkedIn profiles.
 * Reuses LinkedIn classification + official-domain heuristics.
 *
 * Statuses: READY | PARTIAL | UNRESOLVED
 */

import {
  classifyLinkedInProfileMatch,
  LINKEDIN_PROFILE_MATCH,
} from "./gdi-google-grounded-contact-discovery-v1.js";
import { resolveOfficialDomains } from "../contact-candidate/native-who-v3/official-domain.js";

export { LINKEDIN_PROFILE_MATCH };

export const GDI_CONTACT_IDENTITY_PREFLIGHT_V1 =
  "gdi-contact-identity-preflight-v1";

export const CONTACT_IDENTITY_READINESS = Object.freeze({
  READY: "READY",
  PARTIAL: "PARTIAL",
  UNRESOLVED: "UNRESOLVED",
});

export const EMPLOYER_RELATIONSHIP = Object.freeze({
  DIRECT_EMPLOYER: "DIRECT_EMPLOYER",
  PARENT_COMPANY: "PARENT_COMPANY",
  EVENT_ORGANIZER: "EVENT_ORGANIZER",
  ASSOCIATION_OR_HOST: "ASSOCIATION_OR_HOST",
  HOTEL_MANAGEMENT_COMPANY: "HOTEL_MANAGEMENT_COMPANY",
  THIRD_PARTY_MEETING_PLANNER: "THIRD_PARTY_MEETING_PLANNER",
  COMMERCIAL_INTERMEDIARY: "COMMERCIAL_INTERMEDIARY",
  DEMAND_ORG_SAME_AS_EMPLOYER: "DEMAND_ORG_SAME_AS_EMPLOYER",
  UNKNOWN: "UNKNOWN",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/**
 * Split a full name into first/last without inventing middle-name policy.
 */
export function splitContactPersonName(fullName = "") {
  const parts = text(fullName).split(/\s+/).filter(Boolean);
  if (!parts.length) {
    return {
      firstName: null,
      lastName: null,
      fullName: null,
      nameVariants: [],
      surnameAbbreviated: false,
    };
  }
  const firstName = parts[0];
  let lastName = parts.length > 1 ? parts.slice(1).join(" ") : null;
  // Single-letter surnames ("K.", "Y") are not verified last names.
  const surnameAbbreviated = Boolean(
    lastName && /^[A-Za-z]\.?$/.test(lastName.split(/\s+/).pop()),
  );
  if (surnameAbbreviated) {
    lastName = null;
  }
  const full = parts.join(" ");
  const variants = new Set([full, stripAccents(full)]);
  if (lastName) {
    variants.add(`${firstName} ${lastName.split(/\s+/).pop()}`);
    variants.add(`${stripAccents(firstName)} ${stripAccents(lastName)}`);
  }
  // Drop honorifics from variants when present as first token.
  if (/^(lic|dr|dra|ing|mr|mrs|ms)\.?$/i.test(firstName) && parts.length > 2) {
    const rest = parts.slice(1);
    variants.add(rest.join(" "));
    variants.add(`${rest[0]} ${rest[rest.length - 1]}`);
  }
  return {
    firstName,
    lastName,
    fullName: full,
    nameVariants: [...variants].filter(Boolean),
    surnameAbbreviated,
  };
}

function isCompanyLinkedIn(url) {
  return /linkedin\.com\/(company|school)\//i.test(text(url));
}

function isPersonalLinkedIn(url) {
  return /linkedin\.com\/in\//i.test(text(url));
}

function domainFromEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@")) return null;
  const host = e.split("@")[1];
  if (!host || /gmail|yahoo|hotmail|outlook|icloud|proton/i.test(host)) return null;
  return host.replace(/^www\./, "");
}

function normalizeDomain(domain) {
  return text(domain)
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
}

/**
 * Classify employer vs opportunity demand organization relationship.
 */
export function classifyEmployerRelationship({
  employerName,
  demandOrganizationName,
  title,
  hints = {},
} = {}) {
  const emp = stripAccents(employerName).toLowerCase();
  const demand = stripAccents(demandOrganizationName).toLowerCase();
  const role = stripAccents(title).toLowerCase();
  if (!emp) return EMPLOYER_RELATIONSHIP.UNKNOWN;
  if (demand && (emp === demand || emp.includes(demand.slice(0, 12)) || demand.includes(emp.slice(0, 12)))) {
    return EMPLOYER_RELATIONSHIP.DEMAND_ORG_SAME_AS_EMPLOYER;
  }
  if (hints.relationship && EMPLOYER_RELATIONSHIP[hints.relationship]) {
    return hints.relationship;
  }
  if (/association|federation|chamber|congress|conference|summit/i.test(emp)) {
    return EMPLOYER_RELATIONSHIP.ASSOCIATION_OR_HOST;
  }
  if (/organizer|organiser|pco|planner|housing bureau/i.test(`${emp} ${role}`)) {
    return EMPLOYER_RELATIONSHIP.THIRD_PARTY_MEETING_PLANNER;
  }
  if (/management|operator|hospitality management/i.test(emp)) {
    return EMPLOYER_RELATIONSHIP.HOTEL_MANAGEMENT_COMPANY;
  }
  if (demand && emp && emp !== demand) {
    return EMPLOYER_RELATIONSHIP.DIRECT_EMPLOYER;
  }
  return EMPLOYER_RELATIONSHIP.DIRECT_EMPLOYER;
}

/**
 * Resolve employer official domain from email, explicit domain, or seed URLs.
 */
export function resolveEmployerDomain({
  employerName,
  email,
  explicitDomain,
  seedUrls = [],
} = {}) {
  const fromExplicit = normalizeDomain(explicitDomain);
  if (fromExplicit) {
    return {
      domain: fromExplicit,
      source: "explicit",
      verified: true,
      confidence: "high",
    };
  }
  const fromEmail = domainFromEmail(email);
  if (fromEmail) {
    return {
      domain: fromEmail,
      source: "named_email_domain",
      verified: true,
      confidence: "high",
    };
  }
  try {
    const urls = (Array.isArray(seedUrls) ? seedUrls : [])
      .map((s) => (typeof s === "string" ? s : s?.url || s?.href))
      .filter(Boolean);
    const ranked = resolveOfficialDomains({
      organization: employerName,
      eventSourceUrls: urls,
    });
    const top = Array.isArray(ranked) ? ranked[0] : null;
    const domain = normalizeDomain(top?.host);
    if (domain) {
      return {
        domain,
        source: "official_domain_heuristic",
        verified: false,
        confidence: "medium",
        candidates: ranked.slice(0, 5),
      };
    }
  } catch {
    // heuristic optional
  }
  return {
    domain: null,
    source: null,
    verified: false,
    confidence: "none",
    gap: "EMPLOYER_DOMAIN_UNRESOLVED",
  };
}

/**
 * Build LinkedIn discovery query strings (for Gemini/Exa/Apify — not executed here).
 */
export function buildLinkedInIdentityQueries({
  fullName,
  employerName,
  title,
  domain,
} = {}) {
  const name = text(fullName);
  const org = text(employerName);
  const role = text(title);
  const host = normalizeDomain(domain);
  const q = [];
  if (name && org) {
    q.push(`"${name}" "${org}" linkedin`);
    q.push(`site:linkedin.com/in "${name}" "${org}"`);
  }
  if (name && role) q.push(`"${name}" "${role}" linkedin`);
  if (name && host) q.push(`"${name}" "${host}"`);
  return q;
}

/**
 * Evaluate a LinkedIn URL for identity preflight.
 * Company pages are rejected. Only VERIFIED may be trusted for Surfe.
 */
export function evaluatePersonalLinkedInUrl({
  url,
  personName,
  employerName,
  title,
  evidenceText,
} = {}) {
  const u = text(url);
  if (!u) {
    return {
      status: LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND,
      url: null,
      trustedForSurfe: false,
      reasons: ["no_url"],
    };
  }
  if (isCompanyLinkedIn(u)) {
    return {
      status: LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND,
      url: null,
      trustedForSurfe: false,
      rejected: true,
      rejectReason: "COMPANY_LINKEDIN_PAGE_NOT_PERSONAL",
      reasons: ["company_or_school_page"],
    };
  }
  if (!isPersonalLinkedIn(u)) {
    return {
      status: LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND,
      url: null,
      trustedForSurfe: false,
      rejected: true,
      rejectReason: "NOT_PERSONAL_IN_URL",
      reasons: ["not_linkedin_in"],
    };
  }
  const classified = classifyLinkedInProfileMatch({
    url: u,
    personName,
    organization: employerName,
    title,
    evidenceText,
  });
  return {
    ...classified,
    trustedForSurfe:
      classified.status === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE,
    rejected:
      classified.status === LINKEDIN_PROFILE_MATCH.PROFILE_MATCH_UNCERTAIN &&
      !classified.reasons?.includes("name_tokens_in_slug"),
  };
}

/**
 * Compute READY / PARTIAL / UNRESOLVED.
 * READY requires verified name + employer + trusted personal LinkedIn.
 * Missing LinkedIn alone → PARTIAL (does not block publication).
 */
export function computeContactIdentityReadiness(identity = {}) {
  const gaps = [];
  const firstName = text(identity.firstName);
  const lastName = text(identity.lastName);
  const employerName = text(identity.employerName);
  const domain = normalizeDomain(identity.employerDomain);
  const domainVerified = identity.employerDomainVerified === true;
  const liStatus = identity.linkedinStatus || LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND;
  const liTrusted =
    identity.linkedinTrustedForSurfe === true ||
    liStatus === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE;

  if (!firstName || !lastName) gaps.push("NAME_INCOMPLETE");
  if (identity.surnameAbbreviated === true) {
    gaps.push("ABBREVIATED_SURNAME");
  }
  if (!employerName) gaps.push("EMPLOYER_UNRESOLVED");
  if (!domain) gaps.push("EMPLOYER_DOMAIN_MISSING");
  else if (!domainVerified) gaps.push("EMPLOYER_DOMAIN_UNVERIFIED");
  if (!liTrusted) {
    if (liStatus === LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE) {
      gaps.push("LINKEDIN_CANDIDATE_NOT_VERIFIED");
    } else if (liStatus === LINKEDIN_PROFILE_MATCH.PROFILE_MATCH_UNCERTAIN) {
      gaps.push("LINKEDIN_UNCERTAIN");
    } else {
      gaps.push("LINKEDIN_MISSING");
    }
  }

  const nameOk =
    Boolean(firstName && lastName) && identity.surnameAbbreviated !== true;
  const employerOk = Boolean(employerName);
  if (!nameOk || !employerOk) {
    return {
      status: CONTACT_IDENTITY_READINESS.UNRESOLVED,
      gaps,
      reason: !nameOk
        ? identity.surnameAbbreviated
          ? "Abbreviated surname is not a fully verified professional identity"
          : "Named person identity insufficient"
        : "Employer association insufficiently verified",
    };
  }
  // READY = verified name + employer + trusted personal LinkedIn (Phase 5).
  // Domain strengthens Surfe prep but does not block READY when LinkedIn is verified.
  if (liTrusted) {
    return {
      status: CONTACT_IDENTITY_READINESS.READY,
      gaps: gaps.filter((g) => g.startsWith("EMPLOYER_DOMAIN")),
      reason: domain
        ? "Verified name, employer, and personal LinkedIn"
        : "Verified name, employer, and personal LinkedIn (employer domain still open)",
    };
  }
  return {
    status: CONTACT_IDENTITY_READINESS.PARTIAL,
    gaps,
    reason: "Verified name and employer; personal LinkedIn not verified",
  };
}

/**
 * Build canonical contact identity record from opportunity + contact fields.
 */
export function buildContactIdentityRecord(args = {}) {
  const opportunity = args.opportunity || {};
  const contact = args.contact || opportunity.primaryContact || {};
  const verifiedAt = args.verifiedAt || new Date().toISOString();

  const fullName =
    text(contact.name || contact.fullName || opportunity.primaryContactName) ||
    null;
  const nameParts = splitContactPersonName(fullName);
  // Propagate abbreviated-surname flag into readiness computation.

  const demandOrganization =
    text(
      opportunity.organizationName ||
        opportunity.displayCompanyName ||
        args.demandOrganizationName,
    ) || null;

  // Prefer explicit employer; do NOT default demand org without flag.
  const employerName =
    text(
      contact.employerName ||
        contact.organization ||
        contact.companyName ||
        args.employerName ||
        (args.assumeDemandOrgIsEmployer === true ? demandOrganization : null),
    ) || null;

  const email = text(contact.email || contact.workEmail) || null;
  const domainInfo = resolveEmployerDomain({
    employerName,
    email,
    explicitDomain:
      contact.employerDomain ||
      contact.companyDomain ||
      args.employerDomain ||
      (args.assumeDemandOrgIsEmployer === true
        ? opportunity.organizationDomain
        : null),
    seedUrls: args.seedUrls || opportunity.sources || [],
  });

  const relationship = classifyEmployerRelationship({
    employerName,
    demandOrganizationName: demandOrganization,
    title: contact.title || contact.role,
    hints: args.employerHints || {},
  });

  const linkedInEval = evaluatePersonalLinkedInUrl({
    url: contact.linkedinUrl || contact.profileUrl || args.linkedinUrl,
    personName: fullName,
    employerName,
    title: contact.title || contact.role,
    evidenceText: args.linkedinEvidenceText || contact.linkedinEvidenceText,
  });

  const identity = {
    version: GDI_CONTACT_IDENTITY_PREFLIGHT_V1,
    firstName: nameParts.firstName,
    lastName: nameParts.lastName,
    fullName: nameParts.fullName,
    nameVariants: nameParts.nameVariants,
    surnameAbbreviated: nameParts.surnameAbbreviated === true,
    title: text(contact.title || contact.role) || null,
    employerName,
    employerDomain: domainInfo.domain,
    employerDomainVerified: domainInfo.verified === true,
    employerDomainSource: domainInfo.source,
    employerRelationship: relationship,
    demandOrganizationName: demandOrganization,
    demandOrganizationDistinct: Boolean(
      demandOrganization &&
        employerName &&
        stripAccents(demandOrganization).toLowerCase() !==
          stripAccents(employerName).toLowerCase(),
    ),
    linkedinUrl: linkedInEval.trustedForSurfe ? linkedInEval.url : null,
    linkedinCandidateUrl:
      !linkedInEval.trustedForSurfe && linkedInEval.url
        ? linkedInEval.url
        : null,
    linkedinStatus: linkedInEval.status,
    linkedinTrustedForSurfe: linkedInEval.trustedForSurfe === true,
    linkedinRejectReason: linkedInEval.rejectReason || null,
    linkedinReasons: linkedInEval.reasons || [],
    opportunityId: opportunity.id || opportunity.opportunityId || args.opportunityId || null,
    hotelId: opportunity.hotelId || args.hotelId || null,
    commercialClassification:
      args.commercialClassification ||
      contact.commercialClassification ||
      "NAMED_CONTACT",
    sourceEvidence: args.sourceEvidence || contact.sourceUrl || null,
    verifiedAt,
    linkedInQueries: buildLinkedInIdentityQueries({
      fullName: nameParts.fullName,
      employerName,
      title: contact.title || contact.role,
      domain: domainInfo.domain,
    }),
  };

  const readiness = computeContactIdentityReadiness(identity);
  identity.readiness = readiness.status;
  identity.readinessGaps = readiness.gaps;
  identity.readinessReason = readiness.reason;

  return identity;
}

/**
 * Surfe enrichment input adapter — NO paid calls.
 * Only VERIFIED LinkedIn URLs are included.
 */
export function buildSurfeEnrichmentIdentityInput(identity = {}) {
  const firstName = text(identity.firstName);
  const lastName = text(identity.lastName);
  const companyName = text(identity.employerName);
  const companyDomain = normalizeDomain(identity.employerDomain);
  const linkedinUrl =
    identity.linkedinTrustedForSurfe === true &&
    isPersonalLinkedIn(identity.linkedinUrl)
      ? text(identity.linkedinUrl).replace(/\/$/, "")
      : null;

  const fieldsProvided = {
    firstName: {
      value: firstName || null,
      verified: Boolean(firstName),
    },
    lastName: {
      value: lastName || null,
      verified: Boolean(lastName),
    },
    companyName: {
      value: companyName || null,
      verified: Boolean(companyName),
    },
    companyDomain: {
      value: companyDomain || null,
      verified: identity.employerDomainVerified === true,
    },
    linkedinUrl: {
      value: linkedinUrl,
      verified: Boolean(linkedinUrl),
      status: identity.linkedinStatus || null,
    },
  };

  // Surfe person payload (matches lib/surfe/client + provider conventions)
  const person = {};
  if (linkedinUrl) {
    person.linkedInUrl = linkedinUrl;
    if (firstName) person.firstName = firstName;
    if (lastName) person.lastName = lastName;
    // Still attach company context when verified — strengthens match
    if (companyName) person.companyName = companyName;
    if (companyDomain && identity.employerDomainVerified) {
      person.companyDomain = companyDomain;
    }
  } else {
    if (firstName) person.firstName = firstName;
    if (lastName) person.lastName = lastName;
    if (companyName) person.companyName = companyName;
    if (companyDomain) person.companyDomain = companyDomain;
  }

  const sufficient = Boolean(
    person.linkedInUrl ||
      (person.firstName && (person.companyName || person.companyDomain)),
  );

  return {
    version: GDI_CONTACT_IDENTITY_PREFLIGHT_V1,
    paidCallAuthorized: false,
    surfeCallExecuted: false,
    sufficient,
    readiness: identity.readiness || null,
    person,
    /** Exact Surfe-oriented field names for logging / QA */
    surfeFieldNames: {
      firstName: person.firstName || null,
      lastName: person.lastName || null,
      companyName: person.companyName || null,
      companyDomain: person.companyDomain || null,
      linkedInUrl: person.linkedInUrl || null,
    },
    fieldsProvided,
    omittedUnverifiedLinkedIn: Boolean(
      identity.linkedinCandidateUrl && !linkedinUrl,
    ),
    message: sufficient
      ? linkedinUrl
        ? "Surfe input prepared with verified LinkedIn + name/company context (no paid call)."
        : "Surfe input prepared with verified name + employer/domain fallback (no paid call)."
      : "Identity insufficient for Surfe enrichment input.",
  };
}

/**
 * Run identity preflight for one opportunity contact (pure / offline-capable).
 */
export function runContactIdentityPreflight(args = {}) {
  const identity = buildContactIdentityRecord(args);
  const surfeInput = buildSurfeEnrichmentIdentityInput(identity);
  return {
    ok: identity.readiness !== CONTACT_IDENTITY_READINESS.UNRESOLVED ||
      Boolean(identity.fullName),
    version: GDI_CONTACT_IDENTITY_PREFLIGHT_V1,
    identity,
    surfeInput,
    researchContinuation:
      identity.readiness === CONTACT_IDENTITY_READINESS.READY
        ? null
        : {
            prioritize: identity.readinessGaps || [],
            linkedInQueries: identity.linkedInQueries || [],
            note: "Continue bounded LinkedIn/employer research; do not invent fields.",
          },
  };
}

export default {
  GDI_CONTACT_IDENTITY_PREFLIGHT_V1,
  CONTACT_IDENTITY_READINESS,
  EMPLOYER_RELATIONSHIP,
  LINKEDIN_PROFILE_MATCH,
  splitContactPersonName,
  classifyEmployerRelationship,
  resolveEmployerDomain,
  buildLinkedInIdentityQueries,
  evaluatePersonalLinkedInUrl,
  computeContactIdentityReadiness,
  buildContactIdentityRecord,
  buildSurfeEnrichmentIdentityInput,
  runContactIdentityPreflight,
};
