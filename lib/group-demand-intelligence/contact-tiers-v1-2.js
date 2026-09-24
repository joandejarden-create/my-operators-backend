/**
 * GDI Contact Coverage Tiers V1.2 — customer-facing WHO path classification.
 * Does not invent contacts. Surfe/provider PII is never required for tiering.
 */

import { gradeContact, hasNamedPerson } from "./contact-resolution.js";
import { isGenericMailboxEmail } from "../hotel-intelligence/contact-intelligence/dimensions.js";

export const CONTACT_TIER = Object.freeze({
  NAMED_DIRECT: "NAMED_DIRECT",
  NAMED_PARTIAL: "NAMED_PARTIAL",
  FUNCTIONAL_CONTACT: "FUNCTIONAL_CONTACT",
  ORGANIZATION_PATH: "ORGANIZATION_PATH",
  GENERIC_ONLY: "GENERIC_ONLY",
  NO_CONTACT: "NO_CONTACT",
});

export const CONTACT_TIER_LABEL = Object.freeze({
  NAMED_DIRECT: "Named decision-maker with direct reachability",
  NAMED_PARTIAL: "Named decision-maker — reachability incomplete",
  FUNCTIONAL_CONTACT: "Functional events / meetings / housing contact",
  ORGANIZATION_PATH: "Organization or official contact path",
  GENERIC_ONLY: "Generic inbox only",
  NO_CONTACT: "No usable contact path yet",
});

export const CONTACT_BLANK_REASON = Object.freeze({
  NO_PUBLIC_STAFF: "NO_PUBLIC_STAFF",
  ROLE_AMBIGUOUS: "ROLE_AMBIGUOUS",
  ORGANIZER_UNKNOWN: "ORGANIZER_UNKNOWN",
  EVENT_PAGE_BLOCKED: "EVENT_PAGE_BLOCKED",
  SOURCE_TOO_THIN: "SOURCE_TOO_THIN",
  ONLY_GENERIC_CONTACT: "ONLY_GENERIC_CONTACT",
  PRIVACY_POLICY: "PRIVACY_POLICY",
  OTHER: "OTHER",
});

const FUNCTIONAL_EMAIL_RE =
  /^(events|meetings|conference|housing|registration|groupsales|group\.?sales|inquiries|sales|weddings|banquets)@/i;
const GENERIC_EMAIL_RE = /^(info|contact|office|hello|admin|support|mail)@/i;

function contactOf(opp = {}) {
  return opp.primaryContact && typeof opp.primaryContact === "object"
    ? opp.primaryContact
    : {
        name: opp.primaryContactName,
        role: opp.primaryContactRole,
        email: opp.primaryContactEmail,
        phone: opp.primaryContactPhone,
        sourceUrl: opp.contactSourceUrl || opp.officialSource,
        functionalEntity: opp.contactFunctionalEntity,
        contactGrade: opp.contactGrade,
        contactQuality: opp.contactQuality,
      };
}

function isFunctionalEmail(email) {
  const e = String(email || "").trim();
  if (!e) return false;
  if (FUNCTIONAL_EMAIL_RE.test(e)) return true;
  return false;
}

function isGenericEmail(email) {
  const e = String(email || "").trim();
  if (!e) return false;
  if (GENERIC_EMAIL_RE.test(e)) return true;
  try {
    return isGenericMailboxEmail(e);
  } catch {
    return false;
  }
}

/**
 * Classify opportunity into CONTACT_TIER.
 */
export function classifyContactTier(opportunity = {}) {
  const c = contactOf(opportunity);
  const name = String(c.name || "").trim();
  const email = String(c.email || "").trim() || null;
  const phone = String(c.phone || "").trim() || null;
  const named = hasNamedPerson({ name });
  const officialUrl =
    opportunity.contactOfficialUrl ||
    c.officialContactUrl ||
    c.sourceUrl ||
    opportunity.officialSource ||
    opportunity.discoverySource ||
    null;
  const functional =
    Boolean(c.functionalEntity) ||
    isFunctionalEmail(email) ||
    /housing partner|event services|meetings desk|group sales/i.test(
      `${name} ${c.role || ""}`
    );

  if (named && (email || phone) && !isGenericEmail(email)) {
    const g = c.contactGrade || gradeContact(c, opportunity).contactGrade;
    if (g === "A" || (email && phone)) return CONTACT_TIER.NAMED_DIRECT;
    return CONTACT_TIER.NAMED_PARTIAL;
  }
  if (named) return CONTACT_TIER.NAMED_PARTIAL;
  if (functional || isFunctionalEmail(email)) return CONTACT_TIER.FUNCTIONAL_CONTACT;
  if (email && isGenericEmail(email) && !officialUrl) return CONTACT_TIER.GENERIC_ONLY;
  if (email && isGenericEmail(email) && officialUrl) return CONTACT_TIER.GENERIC_ONLY;
  if (officialUrl || email || phone || name) return CONTACT_TIER.ORGANIZATION_PATH;
  return CONTACT_TIER.NO_CONTACT;
}

export function contactTierLabel(tier) {
  return CONTACT_TIER_LABEL[tier] || CONTACT_TIER_LABEL.NO_CONTACT;
}

export function summarizeContactCoverage(opportunities = []) {
  const counts = Object.fromEntries(Object.values(CONTACT_TIER).map((t) => [t, 0]));
  const grades = { A: 0, B: 0, C: 0, D: 0, E: 0 };
  const rows = [];
  for (const o of opportunities) {
    const tier = classifyContactTier(o);
    counts[tier] = (counts[tier] || 0) + 1;
    const c = contactOf(o);
    const g =
      c.contactGrade ||
      o.contactGrade ||
      gradeContact(c, o).contactGrade ||
      "E";
    if (grades[g] != null) grades[g] += 1;
    rows.push({
      opportunityId: o.id,
      title: o.title,
      opportunityType: o.opportunityType || o.demandType,
      organization: o.organizationName || o.venueName,
      contactTier: tier,
      contactTierLabel: contactTierLabel(tier),
      namedPerson: hasNamedPerson(c),
      role: c.role || c.title || null,
      email: c.email || null,
      phone: c.phone || null,
      functionalContact: Boolean(c.functionalEntity) || isFunctionalEmail(c.email),
      officialContactPage: o.contactOfficialUrl || c.sourceUrl || o.officialSource || null,
      contactGrade: g,
      source: c.sourceUrl || o.officialSource || null,
    });
  }
  return { counts, grades, rows, total: opportunities.length };
}

/**
 * Whether customer UI should show "Get Contact Details" (named person, missing HOW).
 */
export function shouldShowGetContactDetailsCta(opportunity = {}) {
  const tier = classifyContactTier(opportunity);
  const c = contactOf(opportunity);
  if (!hasNamedPerson(c)) return false;
  if (c.email || c.phone) return false;
  return (
    tier === CONTACT_TIER.NAMED_PARTIAL || tier === CONTACT_TIER.NAMED_DIRECT
  );
}

/**
 * Strip Surfe / provider PII before any Airtable write.
 * Public name/role/org/source URL may remain.
 */
export function stripSurfeProviderPii(contact = {}, { surfeUsed = false } = {}) {
  if (!contact || typeof contact !== "object") return contact;
  const next = { ...contact };
  if (surfeUsed || next.surfeEnriched === true || next.provider === "surfe") {
    delete next.email;
    delete next.phone;
    delete next.mobile;
    delete next.linkedinUrl;
    delete next.linkedin;
    delete next.profileUrl;
    delete next.surfePersonId;
    delete next.providerPayload;
    next.surfeEnriched = false;
    next.reachabilityDeferred = true;
    next.reachabilityNote = "Get Contact Details — provider enrichment on demand only";
  }
  return next;
}

export function isUsableContactPath(tier) {
  return (
    tier === CONTACT_TIER.NAMED_DIRECT ||
    tier === CONTACT_TIER.NAMED_PARTIAL ||
    tier === CONTACT_TIER.FUNCTIONAL_CONTACT ||
    tier === CONTACT_TIER.ORGANIZATION_PATH
  );
}
