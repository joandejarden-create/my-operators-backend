/**
 * GDI Contact Intelligence V1.2 — missing-contact reason taxonomy + skip policy.
 *
 * Classifies WHY contact is missing before any deeper research.
 * Disqualified / closed / low-value watches must not consume contact budget.
 */

import { CONTACT_TIER, classifyContactTier } from "./contact-tiers-v1-2.js";
import {
  resolveDomainFromOpportunity,
  DOMAIN_CONFIDENCE,
} from "./contact-source-recovery-v1-1.js";

export const CONTACT_GAP_REASON = Object.freeze({
  NO_OFFICIAL_DOMAIN: "NO_OFFICIAL_DOMAIN",
  NO_OFFICIAL_SOURCE_PATH: "NO_OFFICIAL_SOURCE_PATH",
  EVENT_OWNER_NOT_OBSERVABLE: "EVENT_OWNER_NOT_OBSERVABLE",
  PROGRAM_OWNER_NOT_OBSERVABLE: "PROGRAM_OWNER_NOT_OBSERVABLE",
  ORGANIZATION_IDENTITY_WEAK: "ORGANIZATION_IDENTITY_WEAK",
  SOURCE_BLOCKED: "SOURCE_BLOCKED",
  ONLY_GENERIC_CONTACT: "ONLY_GENERIC_CONTACT",
  ROLE_AMBIGUITY: "ROLE_AMBIGUITY",
  NO_CONTACT_ON_PUBLIC_WEB: "NO_CONTACT_ON_PUBLIC_WEB",
  LOW_VALUE_NO_DEEP_RESEARCH: "LOW_VALUE_NO_DEEP_RESEARCH",
  ACCOUNT_LEVEL_EVIDENCE_REQUIRED: "ACCOUNT_LEVEL_EVIDENCE_REQUIRED",
  OTHER: "OTHER",
});

export const CONTACT_RESEARCH_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  IN_PROGRESS: "IN_PROGRESS",
  IMPROVED: "IMPROVED",
  UNCHANGED: "UNCHANGED",
  STOPPED_SUFFICIENT: "STOPPED_SUFFICIENT",
  STOPPED_NO_EVIDENCE: "STOPPED_NO_EVIDENCE",
  STOPPED_LOW_VALUE: "STOPPED_LOW_VALUE",
  SKIPPED: "SKIPPED",
});

/**
 * Opportunities that must not receive deep contact research.
 */
export function shouldSkipDeepContactResearch(opportunity = {}) {
  const id = String(opportunity.id || "");
  const type = String(opportunity.opportunityType || "");
  const priority = String(opportunity.priority || "").toUpperCase();
  const title = String(opportunity.title || "");
  if (/_disqualified$/i.test(id)) return { skip: true, reason: CONTACT_GAP_REASON.LOW_VALUE_NO_DEEP_RESEARCH };
  if (priority === "DISQUALIFIED" || type === "CLOSED_DISQUALIFIED") {
    return { skip: true, reason: CONTACT_GAP_REASON.LOW_VALUE_NO_DEEP_RESEARCH };
  }
  if (/rejected|closed|stale historical/i.test(title) && priority === "DISQUALIFIED") {
    return { skip: true, reason: CONTACT_GAP_REASON.LOW_VALUE_NO_DEEP_RESEARCH };
  }
  // Category watches that explicitly need account-level evidence — public path ceiling
  if (
    /needs account-level evidence/i.test(title) ||
    (/marriott hq/i.test(title) && /corporate|corridor|category watch/i.test(title))
  ) {
    return {
      skip: false,
      softStop: true,
      reason: CONTACT_GAP_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED,
    };
  }
  return { skip: false, reason: null };
}

/**
 * Classify why contact is currently weak / missing.
 */
export function classifyContactGapReason(opportunity = {}, opts = {}) {
  const skip = shouldSkipDeepContactResearch(opportunity);
  if (skip.skip) return skip.reason;
  if (skip.softStop && skip.reason) {
    // Prefer account-level reason when still NO_CONTACT
    const tier = classifyContactTier(opportunity);
    if (tier === CONTACT_TIER.NO_CONTACT) return skip.reason;
  }

  const tier = classifyContactTier(opportunity);
  const domain =
    opts.domainState || resolveDomainFromOpportunity(opportunity);
  const hasDomain =
    domain.confidence === DOMAIN_CONFIDENCE.OFFICIAL_CONFIRMED ||
    domain.confidence === DOMAIN_CONFIDENCE.OFFICIAL_LIKELY;
  const hasOfficialUrl = Boolean(
    opportunity.officialSource ||
      opportunity.contactOfficialUrl ||
      opportunity.discoverySource ||
      domain.seedUrl
  );
  const email = opportunity.primaryContactEmail || opportunity.primaryContact?.email || "";
  const name = opportunity.primaryContactName || opportunity.primaryContact?.name || "";
  const org =
    opportunity.organizationName || opportunity.organization || "";

  if (tier === CONTACT_TIER.GENERIC_ONLY || /^(info|contact|office|hello)@/i.test(email)) {
    return CONTACT_GAP_REASON.ONLY_GENERIC_CONTACT;
  }
  if (!org || /category watch|near .* corridor|tbd|unknown/i.test(org)) {
    return CONTACT_GAP_REASON.ORGANIZATION_IDENTITY_WEAK;
  }
  if (!hasDomain && !hasOfficialUrl) {
    return CONTACT_GAP_REASON.NO_OFFICIAL_DOMAIN;
  }
  if (hasDomain && !hasOfficialUrl) {
    return CONTACT_GAP_REASON.NO_OFFICIAL_SOURCE_PATH;
  }
  if (
    opportunity.contactResearchAudit?.blankReason === "EVENT_PAGE_BLOCKED" ||
    /blocked|403|401|cloudflare/i.test(opportunity.contactResearchResult || "")
  ) {
    return CONTACT_GAP_REASON.SOURCE_BLOCKED;
  }
  if (name && /unknown|n\/a|staff|team|organizers?/i.test(name) && !email) {
    return CONTACT_GAP_REASON.ROLE_AMBIGUITY;
  }
  if (
    tier === CONTACT_TIER.ORGANIZATION_PATH ||
    tier === CONTACT_TIER.FUNCTIONAL_CONTACT
  ) {
    if (/university|alumni|advancement/i.test(`${org} ${opportunity.title || ""}`)) {
      return CONTACT_GAP_REASON.PROGRAM_OWNER_NOT_OBSERVABLE;
    }
    return CONTACT_GAP_REASON.EVENT_OWNER_NOT_OBSERVABLE;
  }
  if (tier === CONTACT_TIER.NO_CONTACT) {
    return hasOfficialUrl
      ? CONTACT_GAP_REASON.NO_CONTACT_ON_PUBLIC_WEB
      : CONTACT_GAP_REASON.NO_OFFICIAL_DOMAIN;
  }
  return CONTACT_GAP_REASON.OTHER;
}

/**
 * Compact audit row for founder reports.
 */
export function buildContactGapAuditRow(opportunity = {}, opts = {}) {
  const domain =
    opts.domainState || resolveDomainFromOpportunity(opportunity);
  const skip = shouldSkipDeepContactResearch(opportunity);
  return {
    opportunityId: opportunity.id || null,
    title: opportunity.title || null,
    opportunityType: opportunity.opportunityType || null,
    organization: opportunity.organizationName || opportunity.organization || null,
    program: opportunity.programName || opportunity.eventName || null,
    qualification: opportunity.opportunityQualification || opportunity.qualificationStatus || null,
    priority: opportunity.priority || null,
    contactTier: classifyContactTier(opportunity),
    gapReason: classifyContactGapReason(opportunity, { domainState: domain }),
    skipDeepResearch: Boolean(skip.skip),
    softStop: Boolean(skip.softStop),
    officialDomainKnown: Boolean(domain.host),
    domainConfidence: domain.confidence || null,
    officialUrls: [
      opportunity.officialSource,
      opportunity.contactOfficialUrl,
      opportunity.discoverySource,
    ].filter(Boolean),
    lastContactResearchAt: opportunity.lastContactResearchAt || null,
    contactResearchStatus: opportunity.contactResearchStatus || null,
    unresolvedReason: opportunity.unresolvedContactReason || null,
    nextBestPath: opportunity.nextBestContactPath || null,
  };
}
