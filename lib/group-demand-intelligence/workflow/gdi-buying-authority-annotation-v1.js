/**
 * GDI Buying Authority Annotation V1
 *
 * Evidence-based lodging buying-authority annotation for the primary contact of
 * each opportunity. Product-facing enums; never inferred from title alone.
 *
 * Contract: GDI_BUYING_AUTHORITY_ANNOTATION_V1
 * Consumed by: evaluateContactBestEffortCertificationGate
 *   (gdi-contact-research-best-effort-v1) — primary.buyingAuthorityStatus,
 *   opportunity.requiresAlternativeEntryPoints,
 *   opportunity.alternativeEntryResearchStatus.
 *
 * Rules:
 *  - Title / role text alone NEVER yields a verified lodging decision-maker.
 *  - `buyerVsEntry: DIRECT_BUYER` on the package is a heuristic label, not evidence.
 *  - Never invents people; never changes names, emails, phones, or LinkedIn URLs.
 *  - Pure / in-memory: returns new objects; callers decide whether to persist.
 */
import { classifyPersonIdentityCompleteness, PERSON_IDENTITY_COMPLETENESS } from "../presentation/gdi-abbreviated-contact-identity-v1.js";

export const GDI_BUYING_AUTHORITY_ANNOTATION_V1 =
  "gdi-buying-authority-annotation-v1";

export const BUYING_AUTHORITY_STATUS = Object.freeze({
  VERIFIED_DIRECT_LODGING_DECISION_MAKER:
    "VERIFIED_DIRECT_LODGING_DECISION_MAKER",
  VERIFIED_BUYER_INFLUENCER: "VERIFIED_BUYER_INFLUENCER",
  VERIFIED_ALTERNATIVE_ENTRY_POINT: "VERIFIED_ALTERNATIVE_ENTRY_POINT",
  BUYING_AUTHORITY_NOT_VERIFIED: "BUYING_AUTHORITY_NOT_VERIFIED",
  IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH:
    "IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH",
});

export const ALTERNATIVE_ENTRY_RESEARCH_STATUS = Object.freeze({
  COMPLETED: "COMPLETED",
  REQUIRED_NOT_COMPLETED: "REQUIRED_NOT_COMPLETED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

const WHO_HOW_DIRECT = "VERIFIED_DIRECT_BUYER";
const WHO_HOW_INFLUENCER = "VERIFIED_BUYER_INFLUENCER";
const WHO_HOW_ALT = "VERIFIED_ALTERNATIVE_ENTRY";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function personName(p) {
  return text(p?.fullName || p?.name);
}

/** Resolve WHO/HOW status from evidence arg, opportunity, or nested resolution. */
function resolveWhoHowStatus(opportunity = {}, evidence = {}) {
  const raw =
    evidence.whoHowStatus ||
    evidence.whoHowResolution?.whoHowStatus ||
    opportunity.whoHowStatus ||
    opportunity.whoHow?.whoHowStatus ||
    opportunity.whoHowResolution?.whoHowStatus ||
    "";
  return upper(raw);
}

function resolveWhoHowDirectBuyer(opportunity = {}, evidence = {}) {
  return (
    evidence.whoHowResolution?.directBuyer ||
    evidence.directBuyer ||
    opportunity.whoHowResolution?.directBuyer ||
    opportunity.whoHow?.directBuyer ||
    null
  );
}

/** True only when lodging/hotel buying authority is explicitly verified. */
function hasExplicitLodgingAuthorityVerification(person = {}, directBuyer = null) {
  const flags = [person, directBuyer].filter(Boolean);
  for (const p of flags) {
    if (
      p.hotelBuyingAuthorityVerified === true ||
      p.lodgingBuyingAuthorityVerified === true ||
      p.buyerAuthorityVerified === true
    ) {
      return true;
    }
    const status = upper(p.buyingAuthorityStatus || p.contractingAuthorityStatus);
    if (
      status === "VERIFIED_DIRECT_LODGING_DECISION_MAKER" ||
      status === "CONTRACTING_AUTHORITY_VERIFIED" ||
      status === "VERIFIED_LODGING_DECISION_MAKER"
    ) {
      return true;
    }
  }
  return false;
}

/**
 * When a WHO/HOW directBuyer record names a different person than the primary,
 * its verification flags must not be applied to the primary.
 */
function directBuyerMatchesPrimary(primary, directBuyer) {
  if (!directBuyer) return true;
  const a = personName(primary).toLowerCase();
  const b = personName(directBuyer).toLowerCase();
  if (!a || !b) return true;
  return a === b;
}

function hasCommercialRelevance(person = {}) {
  const label = upper(person.contactRelevanceLabel);
  const cls = text(person.commercialClass).toLowerCase();
  if (label === "CONTACT_RELEVANT") return true;
  if (/influencer|referral|alternative|entry/.test(cls)) {
    return label !== "CONTACT_NOT_RELEVANT" && label !== "NOT_RELEVANT";
  }
  return false;
}

function isAlternativeEntryPerson(person = {}) {
  // Deliberately excludes buyingAuthorityStatus / contractingAuthorityStatus so
  // our own output (or legacy NOT_VERIFIED_* strings) cannot self-reinforce.
  const hay = [
    person.relationshipToOpportunity,
    person.commercialClass,
    person.buyerVsEntry,
  ]
    .map(text)
    .join(" ");
  return /alternative[_\s-]*entry|entry[_\s-]*point|influencer|referral/i.test(hay);
}

function listAlternativePeople(opportunity = {}) {
  const alts = Array.isArray(opportunity.alternativeEntryPoints)
    ? opportunity.alternativeEntryPoints
    : Array.isArray(opportunity.altEntryPoints)
      ? opportunity.altEntryPoints
      : [];
  const named = alts.filter((p) => personName(p));
  const sec = personName(opportunity.secondaryContact)
    ? [opportunity.secondaryContact]
    : [];
  return { alternatives: named, secondary: sec };
}

/**
 * Classify one person-level lodging buying authority from evidence only.
 * @returns {{status:string, basis:string[], reasons:string[]}}
 */
export function classifyPersonBuyingAuthority(person = {}, ctx = {}) {
  const basis = [];
  const reasons = [];
  const name = personName(person);
  if (!name) {
    return {
      status: BUYING_AUTHORITY_STATUS.IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH,
      basis,
      reasons: ["NO_NAMED_PERSON"],
    };
  }

  const whoHow = upper(ctx.whoHowStatus);
  const directBuyer = ctx.directBuyer || null;
  const explicitDirect =
    whoHow === WHO_HOW_DIRECT &&
    directBuyerMatchesPrimary(person, directBuyer) &&
    hasExplicitLodgingAuthorityVerification(person, directBuyer);
  if (explicitDirect) {
    basis.push("WHO_HOW_VERIFIED_DIRECT_BUYER", "LODGING_AUTHORITY_EXPLICITLY_VERIFIED");
    return {
      status: BUYING_AUTHORITY_STATUS.VERIFIED_DIRECT_LODGING_DECISION_MAKER,
      basis,
      reasons,
    };
  }
  if (whoHow === WHO_HOW_DIRECT) {
    reasons.push("WHO_HOW_DIRECT_BUYER_WITHOUT_EXPLICIT_LODGING_AUTHORITY_VERIFICATION");
  }

  const personStatus = upper(person.buyingAuthorityStatus || person.contractingAuthorityStatus);
  if (
    whoHow === WHO_HOW_INFLUENCER ||
    personStatus === "VERIFIED_BUYER_INFLUENCER" ||
    person.buyerInfluencerVerified === true
  ) {
    basis.push(
      whoHow === WHO_HOW_INFLUENCER
        ? "WHO_HOW_VERIFIED_BUYER_INFLUENCER"
        : "PERSON_VERIFIED_BUYER_INFLUENCER",
    );
    return {
      status: BUYING_AUTHORITY_STATUS.VERIFIED_BUYER_INFLUENCER,
      basis,
      reasons,
    };
  }

  const completeness = classifyPersonIdentityCompleteness(name);
  if (completeness !== PERSON_IDENTITY_COMPLETENESS.FULL) {
    reasons.push(`IDENTITY_${completeness}`);
    return {
      status: BUYING_AUTHORITY_STATUS.IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH,
      basis,
      reasons,
    };
  }
  const relevanceLabel = upper(person.contactRelevanceLabel);
  if (relevanceLabel === "CONTACT_NOT_RELEVANT" || relevanceLabel === "NOT_RELEVANT") {
    reasons.push("CONTACT_RELEVANCE_NEGATIVE");
    return {
      status: BUYING_AUTHORITY_STATUS.IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH,
      basis,
      reasons,
    };
  }

  if (
    whoHow === WHO_HOW_ALT ||
    (isAlternativeEntryPerson(person) && hasCommercialRelevance(person))
  ) {
    basis.push(
      whoHow === WHO_HOW_ALT
        ? "WHO_HOW_VERIFIED_ALTERNATIVE_ENTRY"
        : "ALTERNATIVE_ENTRY_WITH_COMMERCIAL_RELEVANCE",
    );
    reasons.push("LODGING_AUTHORITY_NOT_CLAIMED");
    return {
      status: BUYING_AUTHORITY_STATUS.VERIFIED_ALTERNATIVE_ENTRY_POINT,
      basis,
      reasons,
    };
  }

  reasons.push("TITLE_OR_HEURISTIC_ROLE_ONLY_NOT_AUTHORITY_EVIDENCE");
  return {
    status: BUYING_AUTHORITY_STATUS.BUYING_AUTHORITY_NOT_VERIFIED,
    basis,
    reasons,
  };
}

/**
 * Annotate one opportunity. Returns a NEW opportunity (inputs untouched) plus a
 * per-opportunity report row.
 */
export function annotateOpportunityBuyingAuthority(opportunity = {}, evidence = {}) {
  const primary = opportunity.primaryContact || null;
  const whoHowStatus = resolveWhoHowStatus(opportunity, evidence);
  const directBuyer = resolveWhoHowDirectBuyer(opportunity, evidence);
  const { alternatives, secondary } = listAlternativePeople(opportunity);
  const altResearchDoneFlag =
    evidence.alternativeEntryResearchCompleted === true ||
    opportunity.whoHow?.alternativeEntryResearchCompleted === true ||
    opportunity.whoHowResolution?.alternativeEntryResearchCompleted === true ||
    upper(opportunity.alternativeEntryResearchStatus) ===
      ALTERNATIVE_ENTRY_RESEARCH_STATUS.COMPLETED;

  const out = { ...opportunity };
  const oppId = opportunity.id || opportunity.opportunityId || null;

  let primaryClass = {
    status: BUYING_AUTHORITY_STATUS.IDENTITY_OR_RELEVANCE_REQUIRES_FURTHER_RESEARCH,
    basis: [],
    reasons: ["NO_PRIMARY_CONTACT"],
  };
  if (primary && personName(primary)) {
    primaryClass = classifyPersonBuyingAuthority(primary, {
      whoHowStatus,
      directBuyer,
    });
    out.primaryContact = {
      ...primary,
      buyingAuthorityStatus: primaryClass.status,
      buyingAuthorityAnnotation: {
        version: GDI_BUYING_AUTHORITY_ANNOTATION_V1,
        status: primaryClass.status,
        basis: primaryClass.basis,
        reasons: primaryClass.reasons,
        titleAloneUpgradeBlocked: true,
      },
    };
  }

  const verifiedDirect =
    primaryClass.status ===
    BUYING_AUTHORITY_STATUS.VERIFIED_DIRECT_LODGING_DECISION_MAKER;
  out.requiresAlternativeEntryPoints = !verifiedDirect;

  const altPresent = alternatives.length + secondary.length >= 1;
  let altStatus;
  if (altResearchDoneFlag || altPresent) {
    altStatus = ALTERNATIVE_ENTRY_RESEARCH_STATUS.COMPLETED;
  } else if (out.requiresAlternativeEntryPoints) {
    altStatus = ALTERNATIVE_ENTRY_RESEARCH_STATUS.REQUIRED_NOT_COMPLETED;
  } else {
    altStatus = ALTERNATIVE_ENTRY_RESEARCH_STATUS.NOT_APPLICABLE;
  }
  out.alternativeEntryResearchStatus = altStatus;

  // Per-person annotation on secondary / alternatives: fill only when absent
  // (preserve legacy statuses such as NOT_VERIFIED_LODGING_DECISION_MAKER).
  const annotateSideContact = (p) => {
    if (!p || !personName(p)) return p;
    if (text(p.buyingAuthorityStatus)) return p;
    const c = classifyPersonBuyingAuthority(p, {
      // WHO/HOW status describes the primary route; do not apply it to side contacts.
      whoHowStatus: "",
      directBuyer: null,
    });
    return {
      ...p,
      buyingAuthorityStatus: c.status,
      buyingAuthorityAnnotation: {
        version: GDI_BUYING_AUTHORITY_ANNOTATION_V1,
        status: c.status,
        basis: c.basis,
        reasons: c.reasons,
        titleAloneUpgradeBlocked: true,
      },
    };
  };
  if (opportunity.secondaryContact) {
    out.secondaryContact = annotateSideContact(opportunity.secondaryContact);
  }
  if (Array.isArray(opportunity.alternativeEntryPoints)) {
    out.alternativeEntryPoints = opportunity.alternativeEntryPoints.map(
      annotateSideContact,
    );
  }

  out.buyingAuthorityAnnotation = {
    version: GDI_BUYING_AUTHORITY_ANNOTATION_V1,
    primaryStatus: primaryClass.status,
    requiresAlternativeEntryPoints: out.requiresAlternativeEntryPoints,
    alternativeEntryResearchStatus: altStatus,
  };

  return {
    opportunity: out,
    report: {
      opportunityId: oppId,
      organization: text(opportunity.organizationName || opportunity.displayCompanyName) || null,
      primaryName: primary ? personName(primary) || null : null,
      primaryTitle: text(primary?.title || primary?.role) || null,
      legacyBuyerVsEntry: primary?.buyerVsEntry || null,
      whoHowStatus: whoHowStatus || null,
      statusBefore:
        text(primary?.buyingAuthorityStatus || primary?.contractingAuthorityStatus) || null,
      status: primaryClass.status,
      basis: primaryClass.basis,
      reasons: primaryClass.reasons,
      requiresAlternativeEntryPoints: out.requiresAlternativeEntryPoints,
      alternativeEntryResearchStatus: altStatus,
      alternativeContactCount: alternatives.length,
      secondaryContactPresent: secondary.length > 0,
    },
  };
}

/**
 * Annotate a hotel opportunity set.
 * @param {object[]} opportunities
 * @param {{evidenceByOpportunityId?: Record<string, object>}} [opts]
 */
export function annotateHotelOpportunitySet(opportunities = [], opts = {}) {
  const annotated = [];
  const rows = [];
  for (const o of opportunities) {
    const id = o.id || o.opportunityId;
    const ev = (opts.evidenceByOpportunityId && opts.evidenceByOpportunityId[id]) || {};
    const r = annotateOpportunityBuyingAuthority(o, ev);
    annotated.push(r.opportunity);
    rows.push(r.report);
  }
  const countBy = (key) =>
    rows.reduce((acc, r) => {
      const v = r[key] || "NONE";
      acc[v] = (acc[v] || 0) + 1;
      return acc;
    }, {});
  return {
    opportunities: annotated,
    report: {
      version: GDI_BUYING_AUTHORITY_ANNOTATION_V1,
      opportunityCount: rows.length,
      primaryStatusCounts: countBy("status"),
      alternativeEntryResearchStatusCounts: countBy("alternativeEntryResearchStatus"),
      requiresAlternativeEntryPoints: rows.filter((r) => r.requiresAlternativeEntryPoints).length,
      verifiedDirectLodgingDecisionMakers: rows.filter(
        (r) =>
          r.status ===
          BUYING_AUTHORITY_STATUS.VERIFIED_DIRECT_LODGING_DECISION_MAKER,
      ).length,
      titleOnlyUpgradesBlocked: rows.filter((r) =>
        r.reasons.includes("TITLE_OR_HEURISTIC_ROLE_ONLY_NOT_AUTHORITY_EVIDENCE"),
      ).length,
      rows,
    },
  };
}
