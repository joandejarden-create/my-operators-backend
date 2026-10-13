/**
 * GDI contact resolution — grades, role match, confidence, and merge rules.
 * Consumes canonical Contact Intelligence reachability helpers.
 * Contact research runs AFTER opportunity qualification (see contact-resolution-pass).
 */

import {
  CONTACT_GRADE,
  CONTACT_GRADE_LABEL,
  CONTACT_QUALITY,
  CONTACT_QUALITY_LABEL,
  EMAIL_TYPE,
  EMAIL_TYPE_LABEL,
  EMAIL_VERIFICATION_STATUS,
  EMAIL_VERIFICATION_STATUS_LABEL,
  OPPORTUNITY_QUALIFICATION,
  OPPORTUNITY_TYPE,
  PHONE_TYPE,
  PHONE_TYPE_LABEL,
  PRIORITY,
  TARGET_ROLE_MATCH,
  TARGET_ROLE_MATCH_LABEL,
} from "./claim-types.js";
import {
  classifyEmailType,
  classifyEmailVerificationStatus,
  classifyPhoneType,
  personDedupeKey,
  preferStrongerEmail,
  preferStrongerPhone,
} from "../hotel-intelligence/contact-intelligence/contact-reachability.js";
import { isGenericMailboxEmail } from "../hotel-intelligence/contact-intelligence/dimensions.js";

const HIGH_VALUE_ROLE_MATCHES = new Set([
  TARGET_ROLE_MATCH.DIRECT_DECISION_MAKER,
  TARGET_ROLE_MATCH.EVENT_MEETINGS_OWNER,
  TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT,
  TARGET_ROLE_MATCH.EVENT_OPERATIONS_CONTACT,
]);

const PLACEHOLDER_NAME_RE =
  /^(unknown|n\/?a|tbd|staff|office|program office|conference staff|meetings staff|none)$/i;

/** Org / desk tokens that must never count as a human WHO name. */
const NON_PERSON_NAME_RE =
  /\b(team|association|office|headquarters|procurement|management|desk|housing|events|staff|inc\.?|llc|corp|company|organization|council|society|league|alliance|championship|tournament|government|university|college|department|committee|board|contact|services|source|movements|spirit|juniors|federation|markets|informa|varsity|bluewater|executive|administration|organizers|bureau|vendor|partners?|view|more|click|here|learn|read|share|follow|subscribe|login|menu|home|about|privacy|terms|cookie|transportation|program|post|information|training|what|before|know|register|registration|membership|operations|newsletter|download|pdf)\b/i;

/** Last-token title fragments from truncated "Name, Title" parses. */
const TITLE_LAST_TOKEN_RE =
  /^(Vice|President|Director|Manager|Coordinator|Officer|Chief|Chair|Secretary|Treasurer|Senior|Junior|Jr|Sr|II|III|IV|PhD|MD|CPA|Email|Phone)$/i;

/**
 * True human name (first + last). Stricter than historical hasNamedPerson.
 * Used for WHO promotion — org desks must not pass.
 */
export function isLikelyPersonName(name) {
  const n = String(name || "").trim();
  if (!n || PLACEHOLDER_NAME_RE.test(n)) return false;
  if (NON_PERSON_NAME_RE.test(n)) return false;
  if (n.includes("/")) {
    return n
      .split("/")
      .map((s) => s.trim())
      .every((p) => isLikelyPersonName(p));
  }
  // Allow optional middle initial / particle: Jane A. Smith, Mary Anne Jones
  if (!/^[A-Z][a-z]+(?:\s+[A-Z](?:\.|[a-z'.-]+)){1,3}$/.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.some((p) => TITLE_LAST_TOKEN_RE.test(p))) return false;
  // Reject very short last tokens that are UI chrome ("If", "Go", "To")
  const last = parts[parts.length - 1];
  if (last && last.length <= 2 && !/^[A-Z]\.?$/.test(last)) return false;
  return true;
}

export function hasNamedPerson(contact = {}) {
  if (contact == null || typeof contact !== "object") return false;
  const name = String(contact.name || contact.fullName || "").trim();
  if (!name || PLACEHOLDER_NAME_RE.test(name)) return false;
  if (/^(nist|acts|amwa|nado|bebpa|afcea|hbc)\b/i.test(name) && !/\s/.test(name)) return false;
  // Require likely human name (excludes "SIOR Events Team", "PLUS Headquarters", etc.)
  if (!isLikelyPersonName(name)) return false;
  // Require at least first + last token for "named person"
  const parts = name.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return false;
  if (parts.some((p) => PLACEHOLDER_NAME_RE.test(p))) return false;
  // Org desks (HBC Event Services) are entities, not people
  if (/\b(event services|program office|conference staff|meetings staff)\b/i.test(name)) {
    return false;
  }
  if (/\b(services|associates|llc|inc)\b/i.test(name)) return false;
  // Functional desk personas — not a named stakeholder
  if (/\b(webmaster|web[- ]?master|sysadmin|postmaster|noreply|no[- ]?reply|donotreply)\b/i.test(name)) {
    return false;
  }
  return true;
}

/**
 * Infer target role match from title + opportunity context.
 */
export function classifyTargetRoleMatch(contact = {}, opportunity = {}) {
  if (contact.targetRoleMatch && Object.values(TARGET_ROLE_MATCH).includes(contact.targetRoleMatch)) {
    return contact.targetRoleMatch;
  }
  const blob = [
    contact.role,
    contact.title,
    contact.relationshipToEvent,
    contact.relationshipToOpportunity,
    contact.organization,
    contact.name,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  const isOverflow =
    opportunity.opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING ||
    /overflow|housing|stay-to-play|hbc/i.test(String(opportunity.title || ""));

  if (/hbc|housing manager|housing partner|group housing|lodging coordinator|passtkey|onpeak|stay-to-play housing|team travel source|travel source/i.test(blob)) {
    return TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT;
  }
  if (
    /tournament director|director of meetings|vp.{0,24}meetings|vp.{0,24}events|vp.{0,24}business development|business development|conference director|meeting planner|hotel sourcing/i.test(
      blob
    )
  ) {
    return isOverflow && /hbc|housing|travel source/i.test(blob)
      ? TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT
      : TARGET_ROLE_MATCH.DIRECT_DECISION_MAKER;
  }
  if (/executive director|director of nice|president\b/i.test(blob) && !/health it summit/i.test(blob)) {
    return TARGET_ROLE_MATCH.EXECUTIVE_SPONSOR;
  }
  if (/health it summit|special events|conference|events? (lead|manager|contact)|program manager/i.test(blob)) {
    return TARGET_ROLE_MATCH.EVENT_MEETINGS_OWNER;
  }
  if (/operations|communications and engagement|group office manager/i.test(blob)) {
    return TARGET_ROLE_MATCH.EVENT_OPERATIONS_CONTACT;
  }
  if (/association management|amc|conference services firm/i.test(blob)) {
    return TARGET_ROLE_MATCH.ASSOCIATION_MANAGEMENT_CONTACT;
  }
  if (!hasNamedPerson(contact)) {
    return TARGET_ROLE_MATCH.UNKNOWN;
  }
  return TARGET_ROLE_MATCH.GENERAL_ORGANIZATION_CONTACT;
}

/**
 * Housing / AMC entities can be the right "who" even without a personal name.
 */
export function isActionableEntityContact(contact = {}, opportunity = {}) {
  const name = String(contact.name || contact.fullName || "").trim();
  if (!name) return false;
  if (contact.functionalEntity === true) {
    const roleMatch = classifyTargetRoleMatch(contact, opportunity);
    if (
      roleMatch === TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT ||
      contact.gdiContactRole === "HOUSING_OWNER" ||
      /housing|travel source|hbc|onpeak|passtkey|event services/i.test(
        `${name} ${contact.organization || ""} ${contact.role || ""}`
      )
    ) {
      return true;
    }
    // Functional org entity with event relationship still actionable for WHO (not a person)
    if (
      contact.eventRelationship === "CURRENT_EVENT_CONTACT" ||
      contact.eventSpecificEvidence === true
    ) {
      return true;
    }
  }
  const roleMatch = classifyTargetRoleMatch(contact, opportunity);
  if (roleMatch === TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT) {
    return /hbc|housing|passtkey|onpeak|event services|travel source|team travel/i.test(
      `${name} ${contact.organization || ""} ${contact.role || ""}`
    );
  }
  return false;
}

export function hasActionableIdentity(contact = {}, opportunity = {}) {
  return hasNamedPerson(contact) || isActionableEntityContact(contact, opportunity);
}

export function mapRoleMatchToContactQuality(roleMatch, { named, actionable, genericEmail } = {}) {
  const identity = actionable || named;
  if (!identity && genericEmail) return CONTACT_QUALITY.GENERIC_INBOX;
  if (!identity && !genericEmail) return CONTACT_QUALITY.NO_CONTACT;
  switch (roleMatch) {
    case TARGET_ROLE_MATCH.DIRECT_DECISION_MAKER:
      return CONTACT_QUALITY.NAMED_DECISION_MAKER;
    case TARGET_ROLE_MATCH.EVENT_MEETINGS_OWNER:
    case TARGET_ROLE_MATCH.EVENT_OPERATIONS_CONTACT:
      return CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT;
    case TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT:
      return CONTACT_QUALITY.HOUSING_SOURCING_CONTACT;
    case TARGET_ROLE_MATCH.ASSOCIATION_MANAGEMENT_CONTACT:
      return CONTACT_QUALITY.ASSOCIATION_MANAGEMENT_CONTACT;
    case TARGET_ROLE_MATCH.EXECUTIVE_SPONSOR:
      return identity
        ? CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT
        : CONTACT_QUALITY.GENERAL_ORGANIZATION_CONTACT;
    case TARGET_ROLE_MATCH.GENERAL_ORGANIZATION_CONTACT:
      return CONTACT_QUALITY.GENERAL_ORGANIZATION_CONTACT;
    default:
      return genericEmail ? CONTACT_QUALITY.GENERIC_INBOX : CONTACT_QUALITY.NO_CONTACT;
  }
}

function normalizeEmailFields(contact = {}) {
  const email = contact.email || null;
  const inferred =
    contact.emailType === EMAIL_TYPE.INFERRED ||
    contact.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.DOMAIN_PATTERN_INFERRED ||
    Boolean(contact.inferred);
  const emailType =
    contact.emailType ||
    classifyEmailType(email, { contactName: contact.name || contact.fullName, inferred });
  const emailVerificationStatus =
    contact.emailVerificationStatus ||
    classifyEmailVerificationStatus({
      email,
      contactName: contact.name || contact.fullName,
      fromOfficialSource:
        contact.claimKind === "FACT" ||
        /official|staff page|nist\.gov|nar\.realtor|potomacsoccer|afcea/i.test(
          String(contact.sourceUrl || contact.source || contact.emailSource || "")
        ),
      providerVerified: Boolean(contact.providerVerified),
      multiSource: Boolean(contact.multiSource),
      inferred,
    });
  return {
    email,
    emailType,
    emailTypeLabel: EMAIL_TYPE_LABEL[emailType] || emailType,
    emailVerificationStatus,
    emailVerificationStatusLabel:
      EMAIL_VERIFICATION_STATUS_LABEL[emailVerificationStatus] || emailVerificationStatus,
    emailConfidence: contact.emailConfidence ?? (email ? (inferred ? 35 : 75) : null),
    emailSource: contact.emailSource || contact.sourceUrl || contact.source || null,
    emailLastVerified: contact.emailLastVerified || contact.lastVerifiedAt || null,
    emailFirstSeenAt: contact.emailFirstSeenAt || contact.firstSeenAt || null,
  };
}

function normalizePhoneFields(contact = {}) {
  const phone = contact.phone || null;
  const phoneType =
    contact.phoneType ||
    classifyPhoneType(phone, {
      lineHint: contact.phoneLineHint || contact.lineHint || null,
      claimedDirect: Boolean(contact.claimedDirectPhone),
    });
  return {
    phone,
    phoneType,
    phoneTypeLabel: PHONE_TYPE_LABEL[phoneType] || phoneType,
    phoneConfidence: contact.phoneConfidence ?? (phone ? 70 : null),
    phoneSource: contact.phoneSource || contact.sourceUrl || contact.source || null,
    phoneLastVerified: contact.phoneLastVerified || contact.lastVerifiedAt || null,
    phoneFirstSeenAt: contact.phoneFirstSeenAt || contact.firstSeenAt || null,
  };
}

/**
 * Grade A–E per product contract.
 */
export function gradeContact(contact = {}, opportunity = {}) {
  if (!contact || (!contact.name && !contact.email && !contact.phone && !contact.role)) {
    return {
      contactGrade: CONTACT_GRADE.E,
      contactGradeLabel: CONTACT_GRADE_LABEL.E,
    };
  }

  const named = hasNamedPerson(contact);
  const actionable = hasActionableIdentity(contact, opportunity);
  const emailFields = normalizeEmailFields(contact);
  const phoneFields = normalizePhoneFields(contact);
  const roleMatch = classifyTargetRoleMatch(contact, opportunity);
  const relevant =
    HIGH_VALUE_ROLE_MATCHES.has(roleMatch) || roleMatch === TARGET_ROLE_MATCH.EXECUTIVE_SPONSOR;
  const genericOnly =
    !actionable &&
    (emailFields.emailType === EMAIL_TYPE.GENERIC_ORGANIZATION ||
      emailFields.emailType === EMAIL_TYPE.ROLE_BASED ||
      isGenericMailboxEmail(emailFields.email));

  const verifiedEmail =
    emailFields.email &&
    (emailFields.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.OFFICIAL_SOURCE_VERIFIED ||
      emailFields.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.MULTI_SOURCE_CONFIRMED ||
      emailFields.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.PROVIDER_VERIFIED) &&
    emailFields.emailType !== EMAIL_TYPE.INFERRED;

  const usefulPhone =
    phoneFields.phone &&
    phoneFields.phoneType !== PHONE_TYPE.MAIN_ORGANIZATION &&
    phoneFields.phoneType !== PHONE_TYPE.UNKNOWN;

  const strongEventRel =
    /official|tournament director|housing|summit|meetings|conference|gad/i.test(
      String(contact.relationshipToEvent || contact.role || "")
    ) || relevant;

  if (!actionable && (genericOnly || !emailFields.email)) {
    if (emailFields.email || phoneFields.phone) {
      return { contactGrade: CONTACT_GRADE.D, contactGradeLabel: CONTACT_GRADE_LABEL.D };
    }
    return { contactGrade: CONTACT_GRADE.E, contactGradeLabel: CONTACT_GRADE_LABEL.E };
  }

  if (actionable && relevant && verifiedEmail && usefulPhone && strongEventRel) {
    return { contactGrade: CONTACT_GRADE.A, contactGradeLabel: CONTACT_GRADE_LABEL.A };
  }
  if (actionable && relevant && verifiedEmail) {
    return { contactGrade: CONTACT_GRADE.B, contactGradeLabel: CONTACT_GRADE_LABEL.B };
  }
  if (actionable && (relevant || roleMatch === TARGET_ROLE_MATCH.GENERAL_ORGANIZATION_CONTACT)) {
    return { contactGrade: CONTACT_GRADE.C, contactGradeLabel: CONTACT_GRADE_LABEL.C };
  }
  if (genericOnly || !named) {
    return { contactGrade: CONTACT_GRADE.D, contactGradeLabel: CONTACT_GRADE_LABEL.D };
  }
  return { contactGrade: CONTACT_GRADE.C, contactGradeLabel: CONTACT_GRADE_LABEL.C };
}

/**
 * Contact Confidence 0–100 — separate from Evidence Confidence.
 */
export function scoreContactConfidence(contact = {}, opportunity = {}) {
  let score = 0;
  const named = hasNamedPerson(contact);
  const actionable = hasActionableIdentity(contact, opportunity);
  const roleMatch = classifyTargetRoleMatch(contact, opportunity);
  const email = normalizeEmailFields(contact);
  const phone = normalizePhoneFields(contact);

  if (named) score += 20;
  else if (actionable) score += 16;
  if (HIGH_VALUE_ROLE_MATCHES.has(roleMatch)) score += 25;
  else if (roleMatch === TARGET_ROLE_MATCH.EXECUTIVE_SPONSOR) score += 15;
  else if (roleMatch === TARGET_ROLE_MATCH.GENERAL_ORGANIZATION_CONTACT) score += 5;

  if (email.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.MULTI_SOURCE_CONFIRMED) score += 20;
  else if (email.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.OFFICIAL_SOURCE_VERIFIED)
    score += 18;
  else if (email.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.PROVIDER_VERIFIED) score += 12;
  else if (email.emailVerificationStatus === EMAIL_VERIFICATION_STATUS.DOMAIN_PATTERN_INFERRED)
    score += 3;
  else if (email.email) score += 6;

  if (email.emailType === EMAIL_TYPE.DIRECT_WORK) score += 8;
  else if (email.emailType === EMAIL_TYPE.ROLE_BASED) score += 4;
  else if (
    email.emailType === EMAIL_TYPE.GENERIC_ORGANIZATION &&
    roleMatch === TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT
  ) {
    score += 3;
  }

  if (phone.phoneType === PHONE_TYPE.DIRECT) score += 12;
  else if (phone.phoneType === PHONE_TYPE.OFFICE || phone.phoneType === PHONE_TYPE.EVENT_LINE)
    score += 8;
  else if (phone.phoneType === PHONE_TYPE.MAIN_ORGANIZATION) score += 3;
  else if (phone.phone) score += 2;

  const verifiedAt = Date.parse(contact.lastVerifiedAt || email.emailLastVerified || "");
  if (Number.isFinite(verifiedAt)) {
    const ageDays = (Date.now() - verifiedAt) / 86400000;
    if (ageDays <= 180) score += 5;
    else if (ageDays > 540) score -= 10;
  }

  if (contact.historicalOnly || contact.staleUnverified) score -= 20;

  return Math.max(0, Math.min(100, Math.round(score)));
}

export function buildWhyThisContact(contact = {}, opportunity = {}) {
  if (contact.whyThisContact) return String(contact.whyThisContact).trim();
  const name = contact.name || contact.fullName || "This contact";
  const role = contact.role || contact.title || "a listed contact";
  const org = contact.organization || opportunity.organizationName || "the organization";
  const roleMatch = classifyTargetRoleMatch(contact, opportunity);
  const src = contact.sourceUrl || contact.source || contact.emailSource || null;

  if (roleMatch === TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT) {
    return `${name} controls or operates the official hotel / housing program for this event (${org}), making them the right path for room-block inclusion.`;
  }
  if (roleMatch === TARGET_ROLE_MATCH.DIRECT_DECISION_MAKER) {
    return `${name} is listed as ${role} for ${org}${src ? " on an official event source" : ""}, and can influence hotel selection or housing for this opportunity.`;
  }
  if (roleMatch === TARGET_ROLE_MATCH.EVENT_MEETINGS_OWNER) {
    return `${name} owns or co-owns event / meetings delivery for this opportunity (${role}), so they are the primary outreach path.`;
  }
  if (roleMatch === TARGET_ROLE_MATCH.EXECUTIVE_SPONSOR) {
    return `${name} is an executive / program lead for ${org}; use when a named meetings planner is not yet public.`;
  }
  if (!hasNamedPerson(contact)) {
    return `Only a generic or role-based channel is published for ${org}; escalate to a named decision maker before treating outreach as complete.`;
  }
  return `${name} (${role}) is organizationally linked to the event; confirm event-specific hotel authority before heavy pursuit.`;
}

/**
 * Build a fully normalized primary contact package for an opportunity.
 */
export function enrichContactRecord(rawContact = {}, opportunity = {}, extras = {}) {
  if (!rawContact || (!rawContact.name && !rawContact.email && !rawContact.phone)) {
    return null;
  }
  const now = extras.verifiedAt || new Date().toISOString().slice(0, 10);
  const roleMatch = classifyTargetRoleMatch(rawContact, opportunity);
  const emailFields = normalizeEmailFields({
    ...rawContact,
    emailLastVerified: rawContact.emailLastVerified || now,
    emailFirstSeenAt: rawContact.emailFirstSeenAt || rawContact.firstSeenAt || now,
  });
  const phoneFields = normalizePhoneFields({
    ...rawContact,
    phoneLastVerified: rawContact.phoneLastVerified || (rawContact.phone ? now : null),
  });
  const grade = gradeContact({ ...rawContact, ...emailFields, ...phoneFields, targetRoleMatch: roleMatch }, opportunity);
  const named = hasNamedPerson(rawContact);
  const actionable = hasActionableIdentity(rawContact, opportunity);
  const contactQuality = mapRoleMatchToContactQuality(roleMatch, {
    named,
    actionable,
    genericEmail:
      emailFields.emailType === EMAIL_TYPE.GENERIC_ORGANIZATION ||
      isGenericMailboxEmail(emailFields.email),
  });
  const contactConfidence = scoreContactConfidence(
    { ...rawContact, ...emailFields, ...phoneFields, targetRoleMatch: roleMatch },
    opportunity
  );
  const whyThisContact = buildWhyThisContact(
    { ...rawContact, targetRoleMatch: roleMatch },
    opportunity
  );

  return {
    personId: rawContact.personId || null,
    name: rawContact.name || rawContact.fullName || null,
    fullName: rawContact.fullName || rawContact.name || null,
    title: rawContact.title || rawContact.role || null,
    role: rawContact.role || rawContact.title || null,
    organization: rawContact.organization || opportunity.organizationName || null,
    relationshipToEvent:
      rawContact.relationshipToEvent ||
      (roleMatch === TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT
        ? "Official housing / hotel program path"
        : "Event-linked contact"),
    relationshipToOpportunity:
      rawContact.relationshipToOpportunity ||
      "Influences hotel selection, housing, or room-block inclusion",
    targetRoleMatch: roleMatch,
    targetRoleMatchLabel: TARGET_ROLE_MATCH_LABEL[roleMatch] || roleMatch,
    contactRoleMatch: roleMatch,
    contactRoleMatchLabel: TARGET_ROLE_MATCH_LABEL[roleMatch] || roleMatch,
    roleConfidence: rawContact.roleConfidence || (actionable ? "HIGH" : "LOW"),
    ...emailFields,
    ...phoneFields,
    contactGrade: grade.contactGrade,
    contactGradeLabel: grade.contactGradeLabel,
    contactQuality,
    contactQualityLabel: CONTACT_QUALITY_LABEL[contactQuality],
    contactConfidence,
    whyThisContact,
    source: rawContact.sourceUrl || rawContact.source || emailFields.emailSource || null,
    sourceUrl: rawContact.sourceUrl || rawContact.source || null,
    sourceDate: rawContact.sourceDate || rawContact.accessDate || now,
    firstSeenAt: rawContact.firstSeenAt || now,
    lastVerifiedAt: rawContact.lastVerifiedAt || now,
    claimKind: rawContact.claimKind || "FACT",
    confidence: rawContact.confidence ?? contactConfidence,
    historicalOnly: Boolean(rawContact.historicalOnly),
    dedupeKey: personDedupeKey(rawContact.name || rawContact.fullName, rawContact.organization),
  };
}

/**
 * Only spend contact enrichment budget on qualified / high-value opportunities.
 */
export function shouldEnrichContact(opportunity = {}) {
  const priority = opportunity.priority;
  const qual = opportunity.opportunityQualification;
  const type = opportunity.opportunityType;

  if (priority === PRIORITY.DISQUALIFIED) return false;
  if (priority === PRIORITY.WATCHLIST && type !== OPPORTUNITY_TYPE.FUTURE_CYCLE) {
    // weak watchlist — skip unless strong reactivation/future explicitly flagged
    if (qual === OPPORTUNITY_QUALIFICATION.WEAK || qual === OPPORTUNITY_QUALIFICATION.CLOSED) {
      return false;
    }
  }
  if (priority === PRIORITY.HIGH) return true;
  if (type === OPPORTUNITY_TYPE.OVERFLOW_HOUSING && qual !== OPPORTUNITY_QUALIFICATION.WEAK) {
    return true;
  }
  if (type === OPPORTUNITY_TYPE.REACTIVATION) return true;
  if (
    priority === PRIORITY.MEDIUM &&
    (qual === OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN ||
      qual === OPPORTUNITY_QUALIFICATION.STRONG ||
      qual === OPPORTUNITY_QUALIFICATION.MODERATE)
  ) {
    return true;
  }
  if (
    type === OPPORTUNITY_TYPE.FUTURE_CYCLE &&
    (qual === OPPORTUNITY_QUALIFICATION.STRONG || qual === OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN)
  ) {
    return true;
  }
  return false;
}

/**
 * Prefer housing provider over unrelated association ED for overflow opportunities.
 */
export function selectPrimaryAmongCandidates(candidates = [], opportunity = {}) {
  const list = (candidates || []).filter(Boolean);
  if (!list.length) return null;
  const isOverflow = opportunity.opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING;

  const scored = list.map((c) => {
    const enriched = enrichContactRecord(c, opportunity);
    let rank = enriched.contactConfidence;
    if (isOverflow && enriched.targetRoleMatch === TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT) {
      rank += 40;
    }
    if (HIGH_VALUE_ROLE_MATCHES.has(enriched.targetRoleMatch)) rank += 15;
    if (enriched.historicalOnly) rank -= 50;
    if (enriched.contactGrade === CONTACT_GRADE.D || enriched.contactGrade === CONTACT_GRADE.E) {
      rank -= 20;
    }
    return { enriched, rank };
  });
  scored.sort((a, b) => b.rank - a.rank);
  return scored[0].enriched;
}

export function dedupeContactCandidates(candidates = []) {
  const byKey = new Map();
  for (const c of candidates || []) {
    if (!c) continue;
    const key =
      c.dedupeKey ||
      personDedupeKey(c.name || c.fullName, c.organization) ||
      String(c.email || "").toLowerCase() ||
      String(c.phone || "");
    if (!key) continue;
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, c);
      continue;
    }
    const email = preferStrongerEmail(
      {
        email: existing.email,
        emailType: existing.emailType,
        emailVerificationStatus: existing.emailVerificationStatus,
      },
      {
        email: c.email,
        emailType: c.emailType,
        emailVerificationStatus: c.emailVerificationStatus,
      }
    );
    const phone = preferStrongerPhone(
      { phone: existing.phone, phoneType: existing.phoneType },
      { phone: c.phone, phoneType: c.phoneType }
    );
    byKey.set(key, {
      ...existing,
      ...c,
      email: email?.email || existing.email || c.email,
      emailType: email?.emailType || existing.emailType,
      emailVerificationStatus:
        email?.emailVerificationStatus || existing.emailVerificationStatus,
      phone: phone?.phone || existing.phone || c.phone,
      phoneType: phone?.phoneType || existing.phoneType,
    });
  }
  return [...byKey.values()];
}

export function contactabilityAdjustmentFromGrade(grade) {
  switch (grade) {
    case CONTACT_GRADE.A:
      return 15;
    case CONTACT_GRADE.B:
      return 8;
    case CONTACT_GRADE.C:
      return -5;
    case CONTACT_GRADE.D:
      return -18;
    case CONTACT_GRADE.E:
      return -28;
    default:
      return 0;
  }
}

export {
  CONTACT_GRADE,
  CONTACT_GRADE_LABEL,
  TARGET_ROLE_MATCH,
  EMAIL_TYPE,
  EMAIL_VERIFICATION_STATUS,
  PHONE_TYPE,
  HIGH_VALUE_ROLE_MATCHES,
};
