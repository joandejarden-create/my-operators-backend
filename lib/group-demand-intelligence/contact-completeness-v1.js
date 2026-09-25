/**
 * GDI Contact Intelligence Completeness V1
 *
 * Grades A–E, research population selection, commercial-motion → role ladder,
 * public-data ceiling + nextContactResearchAt, customer-safe drawer copy,
 * WHO/HOW separation helpers. Deterministic evidence remains SoT.
 *
 * Surfe: never auto-called; HOW enrichment only after WHO is resolved.
 */

import {
  CONTACT_TIER,
  CONTACT_TIER_LABEL,
  classifyContactTier,
} from "./contact-tiers-v1-2.js";
import {
  GDI_CONTACT_ROLE,
  GDI_CONTACT_ROLE_LABEL,
  ROLE_PRIORITY_BY_FAMILY,
  EVENT_FAMILY,
  classifyEventFamily,
  classifyGdiContactRole,
} from "./contact-candidate/ontology.js";
import {
  shouldSkipDeepContactResearch,
  classifyContactGapReason,
  CONTACT_GAP_REASON,
} from "./contact-gap-classify-v1-2.js";
import { hasNamedPerson } from "./contact-resolution.js";

/** Letter grades mapped from CONTACT_TIER (customer + research). */
export const CONTACT_GRADE = Object.freeze({
  A: "A", // NAMED_DIRECT
  B: "B", // NAMED_PARTIAL
  C: "C", // FUNCTIONAL
  D: "D", // ORGANIZATION_PATH
  E: "E", // NO_CONTACT / GENERIC_ONLY (weak)
});

export const CONTACT_GRADE_FROM_TIER = Object.freeze({
  [CONTACT_TIER.NAMED_DIRECT]: CONTACT_GRADE.A,
  [CONTACT_TIER.NAMED_PARTIAL]: CONTACT_GRADE.B,
  [CONTACT_TIER.FUNCTIONAL_CONTACT]: CONTACT_GRADE.C,
  [CONTACT_TIER.ORGANIZATION_PATH]: CONTACT_GRADE.D,
  [CONTACT_TIER.GENERIC_ONLY]: CONTACT_GRADE.E,
  [CONTACT_TIER.NO_CONTACT]: CONTACT_GRADE.E,
});

export const CONTACT_APPLICABILITY_SCOPE = Object.freeze({
  ORGANIZATION_WIDE: "ORGANIZATION_WIDE",
  SERIES_SPECIFIC: "SERIES_SPECIFIC",
  CYCLE_SPECIFIC: "CYCLE_SPECIFIC",
  HOUSING_SPECIFIC: "HOUSING_SPECIFIC",
});

/** Why contact remains unresolved after ladder research. */
export const PUBLIC_CONTACT_CEILING_REASON = Object.freeze({
  NO_PUBLIC_STAFF: "NO_PUBLIC_STAFF",
  NO_EVENT_CONTACT: "NO_EVENT_CONTACT",
  FUTURE_CYCLE_TOO_EARLY: "FUTURE_CYCLE_TOO_EARLY",
  HOUSING_VENDOR_NOT_PUBLISHED: "HOUSING_VENDOR_NOT_PUBLISHED",
  ORGANIZATION_PATH_ONLY: "ORGANIZATION_PATH_ONLY",
  PRIVATE_CONTACT: "PRIVATE_CONTACT",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  ACCOUNT_LEVEL_EVIDENCE_REQUIRED: "ACCOUNT_LEVEL_EVIDENCE_REQUIRED",
  ONLY_GENERIC_CONTACT: "ONLY_GENERIC_CONTACT",
  NO_CONTACT_AFTER_RESEARCH: "NO_CONTACT_AFTER_RESEARCH",
  OTHER: "OTHER",
});

export const CONTACT_LADDER_LEVEL = Object.freeze({
  L1_OPPORTUNITY_OFFICIAL: "L1_OPPORTUNITY_OFFICIAL",
  L2_EVENT_SERIES: "L2_EVENT_SERIES",
  L3_ORG_OFFICIAL: "L3_ORG_OFFICIAL",
  L4_OFFICIAL_DOCUMENTS: "L4_OFFICIAL_DOCUMENTS",
  L5_PUBLIC_PROFESSIONAL: "L5_PUBLIC_PROFESSIONAL",
  L6_FUNCTIONAL_PATH: "L6_FUNCTIONAL_PATH",
  L7_RESEARCHED_NO_CONTACT: "L7_RESEARCHED_NO_CONTACT",
});

export const DEFAULT_CONTACT_RESEARCH_BUDGET = Object.freeze({
  maxDomainQueries: 6,
  maxAdditionalFetches: 10,
  maxRenderedFetches: 2,
  maxRuntimeMs: 3 * 60 * 1000,
  fetchTimeoutMs: 20000,
});

/** Commercial motion buckets → preferred role classes (ordered). */
export const COMMERCIAL_MOTION = Object.freeze({
  FULL_HOTEL_RFP: "FULL_HOTEL_RFP",
  HOUSING: "HOUSING",
  OVERFLOW: "OVERFLOW",
  SPORTS: "SPORTS",
  GOVERNMENT_SCIENTIFIC: "GOVERNMENT_SCIENTIFIC",
  PRIVATE_EVENT: "PRIVATE_EVENT",
  ASSOCIATION_CONFERENCE: "ASSOCIATION_CONFERENCE",
  UNIVERSITY: "UNIVERSITY",
  OTHER: "OTHER",
});

export const ROLE_PRIORITY_BY_COMMERCIAL_MOTION = Object.freeze({
  [COMMERCIAL_MOTION.FULL_HOTEL_RFP]: [
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
  ],
  [COMMERCIAL_MOTION.HOUSING]: [
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
  ],
  [COMMERCIAL_MOTION.OVERFLOW]: [
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
  ],
  [COMMERCIAL_MOTION.SPORTS]: [
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.HOUSING_OWNER,
    GDI_CONTACT_ROLE.REGISTRATION_OWNER,
  ],
  [COMMERCIAL_MOTION.GOVERNMENT_SCIENTIFIC]: [
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
    GDI_CONTACT_ROLE.EVENT_OWNER,
  ],
  [COMMERCIAL_MOTION.PRIVATE_EVENT]: [
    GDI_CONTACT_ROLE.SALES_OWNER,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.PARTNERSHIPS_OWNER,
  ],
  [COMMERCIAL_MOTION.ASSOCIATION_CONFERENCE]: [
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
    GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR,
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.HOUSING_OWNER,
  ],
  [COMMERCIAL_MOTION.UNIVERSITY]: [
    GDI_CONTACT_ROLE.EVENT_OWNER,
    GDI_CONTACT_ROLE.PROGRAM_OWNER,
    GDI_CONTACT_ROLE.MEETINGS_OWNER,
  ],
  [COMMERCIAL_MOTION.OTHER]: ROLE_PRIORITY_BY_FAMILY[EVENT_FAMILY.UNKNOWN],
});

export function tierToGrade(tier) {
  return CONTACT_GRADE_FROM_TIER[tier] || CONTACT_GRADE.E;
}

export function gradeContactCompleteness(opportunity = {}) {
  const tier = classifyContactTier(opportunity);
  const grade = tierToGrade(tier);
  const genericOnly = tier === CONTACT_TIER.GENERIC_ONLY;
  return {
    tier,
    grade,
    genericOnly,
    tierLabel: CONTACT_TIER_LABEL[tier] || null,
  };
}

export function classifyCommercialMotion(opportunity = {}) {
  const type = String(opportunity.opportunityType || "").toUpperCase();
  const signal = String(opportunity.demandSignalType || "").toUpperCase();
  const blob = [
    opportunity.title,
    opportunity.segment,
    opportunity.demandType,
    opportunity.organizationName,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" ");

  if (type === "OVERFLOW_HOUSING" || signal === "OVERFLOW_HOUSING") {
    return COMMERCIAL_MOTION.OVERFLOW;
  }
  if (/sport|soccer|tournament|cup|stay[- ]?to[- ]?play/i.test(blob)) {
    return COMMERCIAL_MOTION.SPORTS;
  }
  if (
    /government|nih|nci|nist|federal|defense|scientific|symposium|rna biology|ctn\b|hrhr|ddaa/i.test(
      blob
    ) ||
    signal === "GOVERNMENT_CONTRACTOR_PROGRAM" ||
    signal === "MEDICAL_HEALTHCARE_PROGRAM"
  ) {
    return COMMERCIAL_MOTION.GOVERNMENT_SCIENTIFIC;
  }
  if (/private[- ]?event|wedding|banquet|venue sales|group sales/i.test(blob)) {
    return COMMERCIAL_MOTION.PRIVATE_EVENT;
  }
  if (/university|alumni|homecoming|georgetown|umd|college/i.test(blob)) {
    return COMMERCIAL_MOTION.UNIVERSITY;
  }
  if (
    /housing|room block|hotel block|accommodat|lodging/i.test(blob) &&
    !/overflow/i.test(blob)
  ) {
    return COMMERCIAL_MOTION.HOUSING;
  }
  if (/rfp|full hotel|annual meeting|conference|exposition/i.test(blob)) {
    return COMMERCIAL_MOTION.FULL_HOTEL_RFP;
  }
  if (/association|advocacy|legislative|meetings/i.test(blob)) {
    return COMMERCIAL_MOTION.ASSOCIATION_CONFERENCE;
  }
  return COMMERCIAL_MOTION.OTHER;
}

export function preferredRolesForOpportunity(opportunity = {}) {
  const motion = classifyCommercialMotion(opportunity);
  const roles =
    ROLE_PRIORITY_BY_COMMERCIAL_MOTION[motion] ||
    ROLE_PRIORITY_BY_FAMILY[classifyEventFamily(opportunity)] ||
    ROLE_PRIORITY_BY_FAMILY[EVENT_FAMILY.UNKNOWN];
  return {
    commercialMotion: motion,
    preferredRoles: [...roles],
    preferredRoleLabels: roles.map(
      (r) => GDI_CONTACT_ROLE_LABEL[r] || r
    ),
  };
}

/**
 * Research population: E + weak D + GENERIC_ONLY + unclear FUNCTIONAL.
 * Skips DISQUALIFIED / closed. Does not re-research strong A/B.
 */
export function selectContactResearchPopulation(opportunities = [], opts = {}) {
  const includeFunctionalWeak = opts.includeFunctionalWeak !== false;
  const out = [];
  for (const o of opportunities || []) {
    const skip = shouldSkipDeepContactResearch(o);
    if (skip.skip) continue;
    const { tier, grade, genericOnly } = gradeContactCompleteness(o);
    if (tier === CONTACT_TIER.NAMED_DIRECT || tier === CONTACT_TIER.NAMED_PARTIAL) {
      continue;
    }
    const named = hasNamedPerson(
      o.primaryContact || { name: o.primaryContactName }
    );
    const weakFunctional =
      includeFunctionalWeak &&
      tier === CONTACT_TIER.FUNCTIONAL_CONTACT &&
      !named;
    const orgPathDeeper =
      tier === CONTACT_TIER.ORGANIZATION_PATH &&
      !skip.softStop;
    const emptyOrGeneric =
      tier === CONTACT_TIER.NO_CONTACT || genericOnly;

    if (emptyOrGeneric || orgPathDeeper || weakFunctional) {
      out.push({
        opportunity: o,
        id: o.id,
        tier,
        grade,
        genericOnly,
        gapReason: classifyContactGapReason(o),
        softStop: Boolean(skip.softStop),
        softStopReason: skip.reason || null,
        ...preferredRolesForOpportunity(o),
      });
    }
  }
  return out;
}

export function summarizeContactBaseline(opportunities = []) {
  const counts = {
    TOTAL: 0,
    NAMED_DIRECT: 0,
    NAMED_PARTIAL: 0,
    FUNCTIONAL: 0,
    ORGANIZATION_PATH: 0,
    GENERIC_ONLY: 0,
    NO_CONTACT: 0,
  };
  for (const o of opportunities || []) {
    counts.TOTAL += 1;
    const tier = classifyContactTier(o);
    if (tier === CONTACT_TIER.NAMED_DIRECT) counts.NAMED_DIRECT += 1;
    else if (tier === CONTACT_TIER.NAMED_PARTIAL) counts.NAMED_PARTIAL += 1;
    else if (tier === CONTACT_TIER.FUNCTIONAL_CONTACT) counts.FUNCTIONAL += 1;
    else if (tier === CONTACT_TIER.ORGANIZATION_PATH) counts.ORGANIZATION_PATH += 1;
    else if (tier === CONTACT_TIER.GENERIC_ONLY) counts.GENERIC_ONLY += 1;
    else counts.NO_CONTACT += 1;
  }
  const research = selectContactResearchPopulation(opportunities);
  return {
    ...counts,
    RESEARCH_POPULATION: research.length,
    researchIds: research.map((r) => r.id),
  };
}

export function mapGapToCeilingReason(gapReason, opportunity = {}) {
  const type = String(opportunity.opportunityType || "");
  if (gapReason === CONTACT_GAP_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED) {
    return PUBLIC_CONTACT_CEILING_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED;
  }
  if (gapReason === CONTACT_GAP_REASON.ONLY_GENERIC_CONTACT) {
    return PUBLIC_CONTACT_CEILING_REASON.ONLY_GENERIC_CONTACT;
  }
  if (
    type === "FUTURE_CYCLE" ||
    /destination not|not yet announced|future cycle|too early/i.test(
      opportunity.title || ""
    )
  ) {
    return PUBLIC_CONTACT_CEILING_REASON.FUTURE_CYCLE_TOO_EARLY;
  }
  if (gapReason === CONTACT_GAP_REASON.NO_OFFICIAL_DOMAIN) {
    return PUBLIC_CONTACT_CEILING_REASON.NO_EVENT_CONTACT;
  }
  if (gapReason === CONTACT_GAP_REASON.EVENT_OWNER_NOT_OBSERVABLE) {
    return PUBLIC_CONTACT_CEILING_REASON.NO_PUBLIC_STAFF;
  }
  if (gapReason === CONTACT_GAP_REASON.PROGRAM_OWNER_NOT_OBSERVABLE) {
    return PUBLIC_CONTACT_CEILING_REASON.NO_EVENT_CONTACT;
  }
  if (/housing|overflow/i.test(type) && /vendor|block|housing/i.test(gapReason || "")) {
    return PUBLIC_CONTACT_CEILING_REASON.HOUSING_VENDOR_NOT_PUBLISHED;
  }
  return PUBLIC_CONTACT_CEILING_REASON.PUBLIC_DATA_CEILING;
}

/**
 * Schedule next contact research — avoid weekly re-research of early-cycle blanks.
 */
export function computeNextContactResearchAt(opportunity = {}, ceilingReason = null) {
  const now = Date.now();
  const eventStart =
    opportunity.eventStartDate ||
    opportunity.startDate ||
    opportunity.dates?.start ||
    null;
  let ms = 90 * 24 * 60 * 60 * 1000; // default 90 days

  if (
    ceilingReason === PUBLIC_CONTACT_CEILING_REASON.FUTURE_CYCLE_TOO_EARLY ||
    opportunity.opportunityType === "FUTURE_CYCLE"
  ) {
    if (eventStart) {
      const t = Date.parse(eventStart);
      if (Number.isFinite(t)) {
        // 6 months before event, or +180d from now if event far
        const sixBefore = t - 180 * 24 * 60 * 60 * 1000;
        ms = Math.max(sixBefore - now, 120 * 24 * 60 * 60 * 1000);
      } else {
        ms = 180 * 24 * 60 * 60 * 1000;
      }
    } else {
      ms = 180 * 24 * 60 * 60 * 1000;
    }
  } else if (
    ceilingReason === PUBLIC_CONTACT_CEILING_REASON.HOUSING_VENDOR_NOT_PUBLISHED
  ) {
    ms = 60 * 24 * 60 * 60 * 1000;
  } else if (
    ceilingReason === PUBLIC_CONTACT_CEILING_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED
  ) {
    ms = 365 * 24 * 60 * 60 * 1000; // do not burn public budget
  }

  return new Date(now + ms).toISOString();
}

export function splitWhoHow(opportunity = {}) {
  const c = opportunity.primaryContact || {};
  const name = c.name || opportunity.primaryContactName || null;
  const role =
    c.role ||
    c.title ||
    opportunity.primaryContactRole ||
    null;
  const roleClass =
    c.gdiContactRole ||
    classifyGdiContactRole(
      { name, role, title: role },
      opportunity
    );
  const who = {
    personIdentity: name,
    title: role,
    roleClass,
    roleRelevance: c.whyThisContact || opportunity.whoPrimaryReason || null,
    evidence: c.sourceUrl || opportunity.contactOfficialUrl || opportunity.officialSource || null,
    confidence: c.contactConfidence ?? opportunity.contactConfidence ?? null,
    named: Boolean(name && hasNamedPerson({ name })),
  };
  const how = {
    email: c.email || opportunity.primaryContactEmail || null,
    phone: c.phone || opportunity.primaryContactPhone || null,
    contactForm: c.contactFormUrl || null,
    linkedIn: c.linkedinUrl || c.linkedIn || null,
    functionalInbox:
      c.functionalEntity || opportunity.contactFunctionalEntity || null,
    officialUrl:
      opportunity.contactOfficialUrl ||
      c.officialContactUrl ||
      opportunity.officialSource ||
      null,
  };
  const whoResolved = who.named;
  const howComplete = Boolean(how.email || how.phone || how.contactForm);
  const whoResolvedHowMissing = whoResolved && !howComplete;
  return { who, how, whoResolved, howComplete, whoResolvedHowMissing };
}

export function inferContactApplicabilityScope(opportunity = {}) {
  const type = String(opportunity.opportunityType || "");
  if (type === "OVERFLOW_HOUSING") return CONTACT_APPLICABILITY_SCOPE.HOUSING_SPECIFIC;
  if (type === "FUTURE_CYCLE") return CONTACT_APPLICABILITY_SCOPE.CYCLE_SPECIFIC;
  if (/series|annual|recurring/i.test(opportunity.title || "")) {
    return CONTACT_APPLICABILITY_SCOPE.SERIES_SPECIFIC;
  }
  return CONTACT_APPLICABILITY_SCOPE.ORGANIZATION_WIDE;
}

/**
 * Customer-safe contact drawer presentation — never expose raw NO_CONTACT / NULL codes.
 */
export function buildCustomerContactDrawerModel(opportunity = {}) {
  const { tier, grade, genericOnly, tierLabel } = gradeContactCompleteness(
    opportunity
  );
  const { who, how, whoResolved, howComplete, whoResolvedHowMissing } =
    splitWhoHow(opportunity);
  const roles = preferredRolesForOpportunity(opportunity);
  const ceiling =
    opportunity.publicContactCeilingReason ||
    opportunity.unresolvedContactReason ||
    null;
  const scope =
    opportunity.contactApplicabilityScope ||
    inferContactApplicabilityScope(opportunity);

  let primaryLabel = null;
  let roleDisplay = who.title || roles.preferredRoleLabels[0] || null;
  let whyRelevant =
    opportunity.whoPrimaryReason ||
    who.roleRelevance ||
    (roles.preferredRoleLabels[0]
      ? `Likely ${roles.preferredRoleLabels[0].toLowerCase()} for ${roles.commercialMotion.replace(/_/g, " ").toLowerCase()} motion`
      : null);
  let functionalPath =
    how.functionalInbox ||
    opportunity.commercialContactPathLabel ||
    opportunity.commercialContactPath ||
    null;
  let confidence = who.confidence;
  let source = how.officialUrl;
  let publicCeilingCopy = null;
  let actionHint = null;

  if (whoResolved) {
    primaryLabel = who.personIdentity;
    actionHint = who.title
      ? `Contact ${who.personIdentity}, ${who.title}, regarding this opportunity.`
      : `Contact ${who.personIdentity} regarding this opportunity.`;
  } else if (tier === CONTACT_TIER.FUNCTIONAL_CONTACT || functionalPath) {
    primaryLabel = functionalPath || "Organization meetings / events path";
    publicCeilingCopy = null;
    actionHint = `Contact the organization's ${
      roles.preferredRoleLabels[0]?.toLowerCase() || "meetings team"
    } regarding this opportunity.`;
  } else if (tier === CONTACT_TIER.ORGANIZATION_PATH) {
    primaryLabel = functionalPath || "Official organization / program path";
    actionHint = `Use the official organization contact path for ${
      opportunity.organizationName || "this organizer"
    }.`;
  } else {
    primaryLabel = null;
    publicCeilingCopy =
      "Named event contact not publicly identified yet";
    actionHint =
      "Monitor for published staff, housing, or registration contacts.";
  }

  // Never surface internal codes
  const banned = /^(NO_CONTACT|NULL|undefined|GENERIC_ONLY|E)$/i;
  if (banned.test(String(primaryLabel || ""))) primaryLabel = null;

  return {
    version: "contact_completeness_v1",
    grade,
    tier,
    tierLabel,
    genericOnly,
    primaryContact: primaryLabel,
    role: roleDisplay,
    organization: opportunity.organizationName || opportunity.organization || null,
    whyRelevant,
    email: how.email,
    phone: how.phone,
    functionalPath,
    source,
    confidence,
    publicCeilingCopy,
    recommendedAction: actionHint,
    contactApplicabilityScope: scope,
    whoResolved,
    howComplete,
    whoResolvedHowMissing,
    surfeEligible: whoResolvedHowMissing,
    nextContactResearchAt: opportunity.nextContactResearchAt || null,
    // Internal only — not customer copy
    _ceilingReasonInternal: ceiling,
  };
}

export function attachCompletenessFields(opportunity = {}, extras = {}) {
  const gradeInfo = gradeContactCompleteness(opportunity);
  const roles = preferredRolesForOpportunity(opportunity);
  const whoHow = splitWhoHow(opportunity);
  const drawer = buildCustomerContactDrawerModel({
    ...opportunity,
    ...extras,
  });
  const next = {
    ...opportunity,
    ...extras,
    contactGrade: gradeInfo.grade,
    contactTier: gradeInfo.tier,
    contactTierLabel: gradeInfo.tierLabel,
    commercialMotion: roles.commercialMotion,
    preferredContactRoles: roles.preferredRoles,
    contactApplicabilityScope:
      extras.contactApplicabilityScope ||
      opportunity.contactApplicabilityScope ||
      inferContactApplicabilityScope(opportunity),
    whoRecord: whoHow.who,
    howRecord: whoHow.how,
    customerContactDrawer: drawer,
    whoPrimaryReason:
      opportunity.whoPrimaryReason || drawer.whyRelevant || null,
  };
  if (
    gradeInfo.grade === CONTACT_GRADE.E ||
    gradeInfo.tier === CONTACT_TIER.ORGANIZATION_PATH
  ) {
    const ceiling =
      extras.publicContactCeilingReason ||
      mapGapToCeilingReason(
        extras.unresolvedContactReason ||
          opportunity.unresolvedContactReason ||
          classifyContactGapReason(opportunity),
        opportunity
      );
    next.publicContactCeilingReason = ceiling;
    if (!next.nextContactResearchAt) {
      next.nextContactResearchAt = computeNextContactResearchAt(
        opportunity,
        ceiling
      );
    }
  }
  return next;
}

export function computeGradeUplift(beforeGrade, afterGrade) {
  const order = { E: 0, D: 1, C: 2, B: 3, A: 4 };
  const b = order[beforeGrade] ?? 0;
  const a = order[afterGrade] ?? 0;
  return {
    upgraded: a > b,
    delta: a - b,
    beforeGrade,
    afterGrade,
  };
}
