/**
 * GDI Contact Candidate Score 0–100 — probability someone is the right WHO.
 * Separate from Contact Grade / reachability quality.
 */

import { hasNamedPerson, isActionableEntityContact } from "../contact-resolution.js";
import {
  CANDIDATE_CONFIDENCE,
  CANDIDATE_STATUS,
  EVENT_FAMILY,
  GDI_CONTACT_ROLE,
  IDENTITY_STATUS,
  ROLE_RELEVANCE,
  classifyEventFamily,
  classifyGdiContactRole,
  classifyRoleRelevance,
  rolePriorityScore,
} from "./ontology.js";

export const CANDIDATE_SCORE_WEIGHTS = Object.freeze({
  roleRelevance: 0.3,
  eventSpecificEvidence: 0.2,
  organizationMatch: 0.15,
  currentEmploymentRecency: 0.1,
  priorEventInvolvement: 0.1,
  contactability: 0.1,
  geographyContextFit: 0.05,
});

function clamp100(n) {
  return Math.max(0, Math.min(100, Math.round(n)));
}

function hasIdentity(contact, opportunity) {
  return hasNamedPerson(contact) || isActionableEntityContact(contact, opportunity);
}

function scoreRoleRelevance(contact, opportunity, family) {
  const role = classifyGdiContactRole(contact, opportunity);
  return rolePriorityScore(role, family);
}

function scoreEventSpecificEvidence(contact, opportunity) {
  let score = 15;
  const rel = String(contact.relationshipToEvent || "").toLowerCase();
  const why = String(contact.whyThisContact || contact.whyThisPerson || "").toLowerCase();
  const src = String(contact.sourceUrl || contact.source || "").toLowerCase();
  const blob = `${rel} ${why} ${src} ${contact.role || ""} ${contact.title || ""}`;
  const eventRel = String(contact.eventRelationship || "");

  if (eventRel === "CURRENT_EVENT_CONTACT") score += 45;
  else if (eventRel === "CURRENT_ROLE_LIKELY_OWNER" || eventRel === "CURRENT_SUCCESSOR_ROLE")
    score += 30;
  else if (eventRel === "HISTORICAL_EVENT_CONTACT") score += 18;
  else if (eventRel === "ORGANIZATION_ROLE_ONLY") score += 8;

  if (contact.eventSpecificEvidence === true || /named (as |on )?(the )?(contact|tournament|event|summit)/i.test(blob)) {
    score += 25;
  }
  if (/official (event|staff|board|cvent|prospectus|housing)/i.test(blob) || /prospectus|exhibitor packet/i.test(blob)) {
    score += 15;
  }
  if (/prior.?year|previous cycle|historical|still current/i.test(blob) || contact.priorEventInvolvement) {
    score += 10;
  }
  if (/linkedin|current title/i.test(blob)) score += 5;
  if (contact.claimKind === "FACT" && src) score += 10;
  if (!rel && !why && !src && !eventRel) score = 5;
  if (contact.genericOrgOnly) score = Math.min(score, 20);
  return clamp100(score);
}

function scoreOrganizationMatch(contact, opportunity) {
  const org = String(contact.organization || "").toLowerCase();
  const oppOrg = String(opportunity.organizationName || "").toLowerCase();
  const title = String(opportunity.title || "").toLowerCase();
  if (!org) return 30;
  if (oppOrg && (org.includes(oppOrg.slice(0, 12)) || oppOrg.includes(org.slice(0, 12)))) return 95;
  if (/hbc|housing|passtkey|onpeak/i.test(org) && /overflow|housing|cup|tournament|soccer/i.test(title)) {
    return 90;
  }
  if (org && opportunity.organizationName) return 55;
  return 40;
}

function scoreEmploymentRecency(contact) {
  if (contact.staleUnverified || contact.identityStatus === IDENTITY_STATUS.STALE) return 5;
  if (contact.historicalOnly && !contact.stillEmployed) return 15;
  if (contact.stillEmployed === false) return 10;
  if (contact.stillEmployed === true) return 95;
  const verifiedAt = Date.parse(contact.lastVerifiedAt || contact.emailLastVerified || "");
  if (Number.isFinite(verifiedAt)) {
    const ageDays = (Date.now() - verifiedAt) / 86400000;
    if (ageDays <= 180) return 90;
    if (ageDays <= 365) return 70;
    if (ageDays <= 540) return 45;
    return 20;
  }
  if (contact.claimKind === "FACT") return 65;
  return 40;
}

function scorePriorEventInvolvement(contact, opportunity) {
  const eventRel = String(contact.eventRelationship || "");
  if (eventRel === "CURRENT_EVENT_CONTACT") return 70;
  if (eventRel === "CURRENT_SUCCESSOR_ROLE") return 65;
  if (eventRel === "HISTORICAL_EVENT_CONTACT") {
    // Still useful, but must not outrank a current event contact on this component alone
    return contact.stillEmployed === false ? 20 : 48;
  }
  if (contact.priorEventInvolvement || contact.reactivationBoost) return 55;
  if (opportunity.opportunityType === "REACTIVATION" && contact.historicalOnly) return 50;
  if (/prior|previous|last year|returning/i.test(String(contact.relationshipToEvent || ""))) return 45;
  return 25;
}

function scoreContactability(contact) {
  // Intentionally capped influence — easy email must not win over weak role.
  let score = 10;
  if (contact.email) score += 40;
  if (contact.phone) score += 25;
  const et = String(contact.emailType || "");
  if (et === "DIRECT_WORK" || /direct/i.test(et)) score += 20;
  else if (et === "ROLE_BASED" || /role/i.test(et)) score += 10;
  else if (contact.email) score += 5;
  return clamp100(score);
}

function scoreGeographyContext(contact, opportunity) {
  const hints = [
    ...(Array.isArray(opportunity.geographyHints) ? opportunity.geographyHints : []),
    opportunity.demandTerritoryFit,
    opportunity.market,
    opportunity.hotelMarket,
    opportunity.city,
    opportunity.region,
  ]
    .filter(Boolean)
    .map((x) => String(x).toLowerCase());

  const blob = `${contact.organization || ""} ${contact.role || ""} ${opportunity.title || ""} ${opportunity.organizationName || ""}`.toLowerCase();

  // Hotel-agnostic: if caller provides geography hints, score overlap; else neutral.
  if (hints.length) {
    const hit = hints.some((h) => h && blob.includes(String(h).toLowerCase().slice(0, 12)));
    return hit ? 80 : 45;
  }
  return 50;
}

/**
 * Transparent Candidate Contact Score 0–100 with breakdown.
 */
export function scoreContactCandidate(contact = {}, opportunity = {}, opts = {}) {
  const family = opts.eventFamily || classifyEventFamily(opportunity);
  const gdiRole = classifyGdiContactRole(contact, opportunity);
  const roleRelevance = classifyRoleRelevance(gdiRole, family);

  if (contact.rejected || contact.candidateStatus === CANDIDATE_STATUS.REJECTED) {
    return {
      candidateScore: 0,
      breakdown: {},
      gdiContactRole: gdiRole,
      roleRelevance,
      eventFamily: family,
      rejected: true,
    };
  }

  const components = {
    roleRelevance: scoreRoleRelevance(contact, opportunity, family),
    eventSpecificEvidence: scoreEventSpecificEvidence(contact, opportunity),
    organizationMatch: scoreOrganizationMatch(contact, opportunity),
    currentEmploymentRecency: scoreEmploymentRecency(contact),
    priorEventInvolvement: scorePriorEventInvolvement(contact, opportunity),
    contactability: scoreContactability(contact),
    geographyContextFit: scoreGeographyContext(contact, opportunity),
  };

  let total = 0;
  for (const [k, w] of Object.entries(CANDIDATE_SCORE_WEIGHTS)) {
    total += (components[k] || 0) * w;
  }

  // Hard penalties
  if (!hasIdentity(contact, opportunity)) {
    total = Math.min(total, 35);
  }
  if (contact.staleUnverified || contact.identityStatus === IDENTITY_STATUS.STALE) {
    total = Math.min(total, 40);
  }
  if (contact.historicalOnly && contact.stillEmployed === false) {
    total = Math.min(total, 25);
  }
  // Reactivation boost only when no stronger current-event signal is already present
  if (
    (opportunity.opportunityType === "REACTIVATION" || contact.reactivationBoost) &&
    contact.stillEmployed !== false &&
    (contact.priorEventInvolvement || contact.historicalOnly) &&
    contact.eventRelationship !== "CURRENT_EVENT_CONTACT"
  ) {
    total = Math.min(100, total + 5);
  }

  return {
    candidateScore: clamp100(total),
    breakdown: components,
    weights: { ...CANDIDATE_SCORE_WEIGHTS },
    gdiContactRole: gdiRole,
    gdiContactRoleLabel: gdiRole,
    roleRelevance,
    eventFamily: family,
  };
}

export function classifyCandidateConfidence(scored, contact = {}, opportunity = {}) {
  if (!hasIdentity(contact, opportunity)) return CANDIDATE_CONFIDENCE.UNRESOLVED;
  if (contact.identityStatus === IDENTITY_STATUS.STALE || contact.staleUnverified) {
    return CANDIDATE_CONFIDENCE.LOW;
  }
  const score = scored.candidateScore ?? 0;
  const eventEv = scored.breakdown?.eventSpecificEvidence ?? 0;
  const hasSrc = Boolean(contact.sourceUrl || contact.source);
  const officialFact = contact.claimKind === "FACT" && hasSrc;
  // Official housing/event owners with strong evidence qualify as HIGH slightly earlier
  if (
    officialFact &&
    score >= 68 &&
    eventEv >= 45 &&
    (scored.gdiContactRole === GDI_CONTACT_ROLE.HOUSING_OWNER ||
      scored.gdiContactRole === GDI_CONTACT_ROLE.MEETINGS_OWNER ||
      scored.gdiContactRole === GDI_CONTACT_ROLE.EVENT_OWNER ||
      scored.gdiContactRole === GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR)
  ) {
    return CANDIDATE_CONFIDENCE.HIGH;
  }
  if (score >= 72 && eventEv >= 50 && hasSrc) return CANDIDATE_CONFIDENCE.HIGH;
  if (score >= 55) return CANDIDATE_CONFIDENCE.MEDIUM;
  if (score >= 35) return CANDIDATE_CONFIDENCE.LOW;
  return CANDIDATE_CONFIDENCE.UNRESOLVED;
}

export function classifyCandidateStatus(scored, contact = {}, opportunity = {}) {
  if (contact.rejected) return CANDIDATE_STATUS.REJECTED;
  if (contact.staleUnverified || contact.identityStatus === IDENTITY_STATUS.STALE) {
    return CANDIDATE_STATUS.STALE;
  }
  if (!hasIdentity(contact, opportunity)) return CANDIDATE_STATUS.UNRESOLVED;

  const score = scored.candidateScore ?? 0;
  const role = scored.gdiContactRole;
  const eventEv = scored.breakdown?.eventSpecificEvidence ?? 0;
  const family = scored.eventFamily;

  if (
    eventEv >= 70 &&
    (role === GDI_CONTACT_ROLE.EVENT_OWNER ||
      role === GDI_CONTACT_ROLE.HOUSING_OWNER ||
      role === GDI_CONTACT_ROLE.MEETINGS_OWNER ||
      role === GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR) &&
    score >= 70
  ) {
    return CANDIDATE_STATUS.VERIFIED_EVENT_OWNER;
  }
  if (scored.roleRelevance === ROLE_RELEVANCE.PRIMARY_DECISION_MAKER && eventEv >= 55 && score >= 65) {
    return CANDIDATE_STATUS.VERIFIED_ROLE_MATCH;
  }
  if (score >= 68) return CANDIDATE_STATUS.STRONG_PROBABLE;
  if (score >= 50) return CANDIDATE_STATUS.PROBABLE;
  if (score >= 30) return CANDIDATE_STATUS.WEAK;
  return CANDIDATE_STATUS.UNRESOLVED;
}

export function classifyIdentityStatus(contact = {}) {
  if (contact.rejected) return IDENTITY_STATUS.REJECTED;
  if (contact.staleUnverified || contact.identityStatus === IDENTITY_STATUS.STALE) {
    return IDENTITY_STATUS.STALE;
  }
  if (contact.identityStatus === IDENTITY_STATUS.IDENTITY_UNCONFIRMED) {
    return IDENTITY_STATUS.IDENTITY_UNCONFIRMED;
  }
  const named = hasNamedPerson(contact) || isActionableEntityContact(contact, {});
  if (!named) return IDENTITY_STATUS.IDENTITY_UNCONFIRMED;
  if (contact.stillEmployed === false) return IDENTITY_STATUS.STALE;
  if (
    contact.claimKind === "FACT" &&
    (contact.sourceUrl || contact.source) &&
    contact.stillEmployed !== false
  ) {
    return IDENTITY_STATUS.CONFIRMED;
  }
  if (contact.name && contact.organization && (contact.role || contact.title)) {
    return IDENTITY_STATUS.IDENTITY_UNCONFIRMED;
  }
  return IDENTITY_STATUS.IDENTITY_UNCONFIRMED;
}

export function buildWhyThisPerson(contact = {}, opportunity = {}, scored = {}) {
  if (contact.whyThisPerson || contact.whyThisContact) {
    return String(contact.whyThisPerson || contact.whyThisContact).trim();
  }
  const name = contact.name || "This contact";
  const role = contact.role || contact.title || "listed role";
  const org = contact.organization || opportunity.organizationName || "the organization";
  const family = scored.eventFamily || classifyEventFamily(opportunity);
  if (scored.gdiContactRole === GDI_CONTACT_ROLE.HOUSING_OWNER || family === EVENT_FAMILY.HOUSING_OVERFLOW) {
    return `${name} (${role}) is the housing / hotel-program path for ${org}, so they outrank general executives for room-block inclusion.`;
  }
  if (scored.gdiContactRole === GDI_CONTACT_ROLE.MEETINGS_OWNER) {
    return `${name} is the meetings / events owner (${role}) for ${org} and is the most probable hotel/venue decision influencer.`;
  }
  if (scored.gdiContactRole === GDI_CONTACT_ROLE.EVENT_OWNER) {
    return `${name} is listed as ${role} with direct event ownership for this opportunity.`;
  }
  return `${name} (${role}) at ${org} is a probable contact based on role and available event evidence.`;
}

/**
 * Fully annotate a raw contact as a scored candidate (does not mutate reachability).
 */
export function annotateCandidate(contact = {}, opportunity = {}, opts = {}) {
  const scored = scoreContactCandidate(contact, opportunity, opts);
  const identityStatus = classifyIdentityStatus(contact);
  const candidateStatus = classifyCandidateStatus(scored, contact, opportunity);
  const candidateConfidence = classifyCandidateConfidence(scored, contact, opportunity);
  return {
    ...contact,
    gdiContactRole: scored.gdiContactRole,
    gdiContactRoleLabel: scored.gdiContactRole,
    roleRelevance: scored.roleRelevance,
    eventFamily: scored.eventFamily,
    candidateScore: scored.candidateScore,
    candidateScoreBreakdown: scored.breakdown,
    candidateConfidence,
    candidateStatus,
    identityStatus,
    whyThisPerson: buildWhyThisPerson(contact, opportunity, scored),
    reachability: {
      email: contact.email || null,
      phone: contact.phone || null,
      emailType: contact.emailType || null,
      phoneType: contact.phoneType || contact.phoneLineHint || null,
      hasEmail: Boolean(contact.email),
      hasPhone: Boolean(contact.phone),
      gap:
        !contact.email && !contact.phone
          ? "NO_EMAIL_NO_PHONE"
          : !contact.email
            ? "MISSING_EMAIL"
            : !contact.phone
              ? "MISSING_PHONE"
              : "REACHABLE",
    },
  };
}
