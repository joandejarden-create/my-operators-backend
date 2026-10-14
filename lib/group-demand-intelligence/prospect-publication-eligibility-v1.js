/**
 * Hotel-facing GDI sales-prospect eligibility (integration / publication prep).
 *
 * Separates the intended hotel sales-prospect standard from strict Ready/ACTIONABLE.
 * Does NOT lower isGdiCustomerOpportunityReady / maturity ACTIONABLE.
 *
 * Usable contact route (minimum for hotel-facing action list):
 *   - named or functional email, OR
 *   - relevant telephone, OR
 *   - specific contact-form / functional contact URL, OR
 *   - documented intermediary with reachable contact
 * Homepage-only / vague “meetings team” / identify-buyer alone → fail.
 *
 * Named person + direct phone are enrichment targets, not absolute blockers
 * when a relevant functional route exists.
 */

import { classifyBuyerContactPath } from "./buyer-contact-path-taxonomy-v1.js";

export const PROSPECT_ELIGIBILITY = Object.freeze({
  HOTEL_FACING: "HOTEL_FACING",
  ENRICHMENT_QUEUE: "ENRICHMENT_QUEUE",
  NOT_A_PROSPECT: "NOT_A_PROSPECT",
});

export const CONTACT_COMPLETENESS_GROUP = Object.freeze({
  PERSON_METHOD_PHONE: 1,
  PERSON_METHOD_NO_PHONE: 2,
  FUNCTIONAL_ORG_ONLY: 3,
  NO_USABLE_ROUTE: 4,
});

const HOMEPAGE_ONLY_RE =
  /^https?:\/\/[^/]+\/?(?:en\/|es\/|de\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isEmail(v) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text(v));
}

function isPhone(v) {
  const d = text(v).replace(/[^\d+]/g, "");
  return d.length >= 10 && !/^not\s*established$/i.test(text(v));
}

function isPersonName(v) {
  const n = text(v);
  if (!n || /^not\s*established$/i.test(n) || n === "UNKNOWN") return false;
  if (/^(staff|team|office|desk|info|contact)\b/i.test(n)) return false;
  return n.split(/\s+/).length >= 2 || /^[A-Z][a-z]+ [A-Z]/.test(n);
}

/**
 * Extract contact fields from opportunity bag or hotel-review contact package.
 */
export function extractContactSurface(opp = {}) {
  const pkg = opp.contactPackage || {};
  const person =
    text(pkg.person) ||
    text(opp.primaryContact?.name) ||
    text(opp.primaryContactName) ||
    text(opp.contactName) ||
    text(opp.namedBuyer);
  const title =
    text(pkg.title) ||
    text(opp.primaryContactRole) ||
    text(opp.primaryContact?.role) ||
    text(opp.buyerRole);
  const email =
    text(pkg.contactMethod).match(/^[^\s@]+@[^\s@]+/)?.[0] ||
    text(opp.primaryContact?.email) ||
    text(opp.primaryContactEmail) ||
    text(opp.functionalContactEmail) ||
    text(opp.contactEmail);
  const phone =
    text(pkg.businessPhone) ||
    text(opp.primaryContact?.phone) ||
    text(opp.primaryContactPhone) ||
    text(opp.contactPhone);
  const phoneType = text(pkg.phoneType || opp.primaryContactPhoneType || opp.contactPhoneType);
  const contactUrl =
    text(pkg.contactPageUrl) ||
    text(opp.publicContactPath) ||
    text(opp.organizationContactUrl) ||
    text(opp.officialContactPath) ||
    text(opp.contactOfficialUrl);
  const methodType = text(pkg.contactMethodType || opp.contactMethodType);
  const intermediary =
    pkg.bookingIntermediary && typeof pkg.bookingIntermediary === "object"
      ? pkg.bookingIntermediary
      : null;
  const intermediaryContact =
    text(intermediary?.contact) ||
    (typeof pkg.bookingIntermediary === "string" &&
    isEmail(pkg.bookingIntermediary)
      ? pkg.bookingIntermediary
      : "");
  const intermediaryPhone = text(intermediary?.phone);

  return {
    person,
    title,
    email,
    phone,
    phoneType,
    contactUrl,
    methodType,
    intermediaryContact,
    intermediaryPhone,
    methodRaw: text(pkg.contactMethod || opp.contactMethod),
  };
}

/**
 * Usable contact route for hotel-facing action list (intended standard).
 */
export function hasUsableContactRoute(opp = {}) {
  const c = extractContactSurface(opp);
  const method = text(c.methodRaw);

  if (/^not\s*established$/i.test(method) && !c.email && !c.phone && !c.contactUrl) {
    return { ok: false, reason: "not_established", surface: c };
  }

  if (isEmail(c.email) || isEmail(method)) {
    return { ok: true, reason: "email", surface: c };
  }
  if (isPhone(c.phone) || isPhone(method)) {
    return { ok: true, reason: "phone", surface: c };
  }
  if (isEmail(c.intermediaryContact) || isPhone(c.intermediaryPhone)) {
    return { ok: true, reason: "intermediary", surface: c };
  }
  if (
    c.contactUrl &&
    /^https?:\/\//i.test(c.contactUrl) &&
    !HOMEPAGE_ONLY_RE.test(c.contactUrl) &&
    /\/(contact|contacts|housing|hotel|accommodation|events?|meetings?|reservations?|exhibit|rfp|planner)/i.test(
      c.contactUrl
    )
  ) {
    return { ok: true, reason: "functional_contact_url", surface: c };
  }
  if (/contact\s*form|mailto:|@/.test(method) && !/homepage|home\s*page|meetings?\s*team/i.test(method)) {
    if (isEmail(method) || /form/i.test(method)) {
      return { ok: true, reason: "method_string", surface: c };
    }
  }

  // Homepage / vague instruction alone
  if (
    !c.email &&
    !c.phone &&
    (HOMEPAGE_ONLY_RE.test(c.contactUrl) ||
      /homepage|meetings?\s*team|identify\s+the\s+buyer|contact\s+the\s+team/i.test(method || c.contactUrl))
  ) {
    return { ok: false, reason: "homepage_or_vague_only", surface: c };
  }

  return { ok: false, reason: "no_usable_route", surface: c };
}

export function classifyContactCompletenessGroup(opp = {}) {
  const route = hasUsableContactRoute(opp);
  const c = route.surface;
  const personOk = isPersonName(c.person);
  const phoneOk = isPhone(c.phone) || isPhone(c.intermediaryPhone);

  if (!route.ok) return CONTACT_COMPLETENESS_GROUP.NO_USABLE_ROUTE;
  if (personOk && phoneOk) return CONTACT_COMPLETENESS_GROUP.PERSON_METHOD_PHONE;
  if (personOk) return CONTACT_COMPLETENESS_GROUP.PERSON_METHOD_NO_PHONE;
  return CONTACT_COMPLETENESS_GROUP.FUNCTIONAL_ORG_ONLY;
}

/**
 * Prospect suitability (commercial credibility) — independent of contact completeness
 * and of confirmed buying / open sourcing.
 */
export function evaluateProspectSuitability(opp = {}) {
  const failed = [];
  const org = text(opp.organizationName || opp.company || opp.account);
  const title = text(opp.title || opp.opportunityName);
  if (!org) failed.push("organization");
  if (!title && !opp.timing) failed.push("occasion_or_title");
  const hasTravel =
    opp.travelEvidence === true ||
    Boolean(text(opp.travelEvidenceNote || opp.relevantTravelers || opp.summaryWhat)) ||
    Boolean(text(opp.timing));
  if (!hasTravel) failed.push("travel_or_recurring_evidence");
  const hasFit =
    Boolean(text(opp.fitExplanation || opp.summaryWhyHotel || opp.hotelFitRationale)) ||
    opp.hotelFitScore != null;
  if (!hasFit) failed.push("hotel_fit");
  const hasSource =
    Boolean(opp.officialSource || opp.discoverySource) ||
    (Array.isArray(opp.sources) && opp.sources.length > 0) ||
    Boolean(text(opp.sourceUrl));
  if (!hasSource) failed.push("source");
  return { ok: failed.length === 0, failed };
}

/**
 * Agreed hotel-facing publication eligibility for sales-prospect action lists.
 * Unknown hotel sourcing / unknown rooms / TBA do NOT fail this gate.
 * Confirmed buying is a separate commercial label.
 */
export function evaluateProspectPublicationEligibility(opp = {}, opts = {}) {
  const suitability = evaluateProspectSuitability(opp);
  const route = hasUsableContactRoute(opp);
  const group = classifyContactCompletenessGroup(opp);
  const readyContact = classifyBuyerContactPath(opp);

  if (opp.isTestData === true || text(opp.priority).toUpperCase() === "DISQUALIFIED") {
    return {
      eligibility: PROSPECT_ELIGIBILITY.NOT_A_PROSPECT,
      hotelFacing: false,
      enrichmentQueue: false,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      reasons: ["disqualified_or_test"],
    };
  }

  if (!suitability.ok) {
    return {
      eligibility: PROSPECT_ELIGIBILITY.ENRICHMENT_QUEUE,
      hotelFacing: false,
      enrichmentQueue: true,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      reasons: suitability.failed,
      nextTask: `Resolve suitability gaps: ${suitability.failed.join(", ")}`,
    };
  }

  if (!route.ok) {
    return {
      eligibility: PROSPECT_ELIGIBILITY.ENRICHMENT_QUEUE,
      hotelFacing: false,
      enrichmentQueue: true,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      reasons: [route.reason],
      nextTask: "Establish source-supported usable contact route (email, phone, form, or intermediary).",
    };
  }

  return {
    eligibility: PROSPECT_ELIGIBILITY.HOTEL_FACING,
    hotelFacing: true,
    enrichmentQueue: false,
    suitability,
    route,
    contactCompletenessGroup: group,
    readyContactClass: readyContact.class,
    readyContactEligible: readyContact.readyEligible === true,
    reasons: [],
    note:
      readyContact.readyEligible === true
        ? "Also satisfies strict Ready contact taxonomy"
        : "Hotel-facing via usable route; may still fail strict Ready/ACTIONABLE until full packet fields stamp",
  };
}

/**
 * Map a hotel-review contact package + register row into opportunity-shaped fields
 * so Ready contact taxonomy and UI serializers see the same surface.
 */
function cleanPersonName(raw = "") {
  // Strip parenthetical aliases so isLikelyPersonName / Ready contact taxonomy accept the name
  return text(raw)
    .replace(/\s*\([^)]*\)\s*/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function applyContactPackageToOpportunity(opp = {}, pkg = {}) {
  const next = { ...opp, contactPackage: pkg };
  const person = cleanPersonName(pkg.person);
  const email = isEmail(pkg.contactMethod)
    ? text(pkg.contactMethod)
    : text(pkg.contactMethod).match(/^[^\s@]+@[^\s@]+\.[^\s@]+/)?.[0] || "";
  const phone = /^not\s*established$/i.test(text(pkg.businessPhone))
    ? ""
    : text(pkg.businessPhone);

  if (isPersonName(person)) {
    next.primaryContact = {
      ...(typeof next.primaryContact === "object" ? next.primaryContact : {}),
      name: person,
      role: text(pkg.title),
      email: email || undefined,
      phone: phone || undefined,
      phoneType: text(pkg.phoneType) || undefined,
      sourceUrl: text(pkg.sourceUrl) || undefined,
      verificationDate: text(pkg.verificationDate) || undefined,
    };
    next.primaryContactName = person;
    next.primaryContactRole = text(pkg.title);
    next.primaryContactEmail = email || next.primaryContactEmail;
    next.primaryContactPhone = phone || next.primaryContactPhone;
    next.primaryContactPhoneType = text(pkg.phoneType) || next.primaryContactPhoneType;
  } else if (email) {
    next.functionalContactEmail = email;
    next.primaryContactEmail = email;
  }

  if (phone && !next.primaryContactPhone) next.primaryContactPhone = phone;

  if (pkg.bookingIntermediary && typeof pkg.bookingIntermediary === "object") {
    next.housingIntermediaryName = pkg.bookingIntermediary.name;
    next.housingIntermediaryEmail = pkg.bookingIntermediary.contact;
    next.housingIntermediaryPhone = pkg.bookingIntermediary.phone;
    if (isEmail(pkg.bookingIntermediary.contact) && !next.functionalContactEmail) {
      next.functionalContactEmail = pkg.bookingIntermediary.contact;
    }
  }

  next.contactResearchAttempted = true;
  next.whoResearchAttempted = true;
  next.contactVerificationDate = text(pkg.verificationDate) || next.contactVerificationDate;
  next.bookingResponsibilityClass = text(pkg.bookingResponsibility) || next.bookingResponsibilityClass;
  next.contactCompletenessGroup = pkg.completenessGroup ?? classifyContactCompletenessGroup(next);
  next.officialSource = next.officialSource || text(pkg.sourceUrl);
  return next;
}
