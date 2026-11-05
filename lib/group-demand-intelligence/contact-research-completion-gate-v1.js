/**
 * Contact research completion gate V1.
 *
 * Shared assessment for every GDI opportunity:
 * - Was buyer research attempted?
 * - Primary / secondary relevant people found?
 * - Affiliation + commercial relevance supported?
 * - Hotel-sourcing authority verified?
 * - Commercial disposition (SALES_READY / RESEARCH_CONTINUATION / WATCH / NOT_CUSTOMER_READY)
 *
 * Does not invent people. Zero paid.
 */
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  CONTACT_COMMERCIAL_DISPOSITION,
  CONTACT_EVIDENCE_CLASS,
  UNIVERSAL_CONTACT_RESEARCH_CONTRACT_V1,
} from "./research/universal-contact-research-contract-v1.js";

export const CONTACT_RESEARCH_COMPLETION_GATE_V1 =
  "gdi-contact-research-completion-gate-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function namedPerson(raw) {
  if (!raw) return null;
  if (typeof raw === "string") {
    return looksLikePersonName(raw) ? { name: text(raw) } : null;
  }
  const name = text(raw.fullName || raw.name || raw.person || raw.suggestedContact);
  if (!name || !looksLikePersonName(name)) return null;
  return { ...raw, name };
}

function collectNamedFromOpportunity(o = {}) {
  const out = [];
  const push = (src, raw) => {
    const p = namedPerson(raw);
    if (p) out.push({ src, ...p });
  };
  push("primaryContact", o.primaryContact);
  push("secondaryContact", o.secondaryContact);
  push("contactName", o.contactName ? { name: o.contactName, title: o.contactRole } : null);
  (o.contacts || []).forEach((c, i) => push(`contacts[${i}]`, c));
  (o.buyerCandidates || []).forEach((c, i) => push(`buyerCandidates[${i}]`, c));
  (o.namedContacts || []).forEach((c, i) => push(`namedContacts[${i}]`, c));
  const who =
    o.commercialIntelligenceBrief?.whoToContact ||
    o.associationEventsGoldBrief?.whoToContact ||
    o.salesBrief?.whoToContact ||
    {};
  push("cib.suggested", who.suggestedContact);
  push("cib.primary", who.primary);
  push("cib.secondary", who.secondary || who.alternativeContact);
  (who.additional || []).forEach((c, i) => push(`cib.additional[${i}]`, c));
  // Dedupe by name
  const seen = new Set();
  return out.filter((p) => {
    const k = text(p.name).toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function relevanceSupported(person = {}) {
  const blob = `${person.whyRelevant || ""} ${person.whyThisContact || ""} ${person.title || ""} ${person.relevanceClass || ""}`.toLowerCase();
  if (!blob) return false;
  if (/not.?commercially|unrelated|speaker only|presenter only|press contact/i.test(blob)) {
    return false;
  }
  if (
    /travel|meeting|event|hotel|lodging|housing|procurement|sourcing|planner|coordinator|operations|mobility|venue|wedding|incentive|programme|program|catering|facilities|workplace|ea\b|assistant/i.test(
      blob,
    )
  ) {
    return true;
  }
  // Named with any whyRelevant prose counts as influencer/referral candidate
  return text(person.whyRelevant || person.whyThisContact).length >= 24;
}

function authorityVerified(person = {}, opportunity = {}) {
  if (person.contractingAuthorityVerified === true) return true;
  if (
    /CONTRACTING_AUTHORITY_VERIFIED:\s*yes/i.test(
      String(person.whyRelevant || ""),
    )
  ) {
    return true;
  }
  return (
    opportunity.primaryContact?.contractingAuthorityVerified === true ||
    opportunity.contactAuthorityVerified === true
  );
}

function buyerResearchAttempted(o = {}) {
  if (o.contactResearchAttempted === true) return true;
  if (o.contactResearchStatus && o.contactResearchStatus !== "NOT_ATTEMPTED") {
    return true;
  }
  if ((o.buyerCandidates || []).length || (o.namedContacts || []).length) {
    return true;
  }
  if (o.researchRunId || o.researchRunIds?.length || o.deepResearchRunId) {
    return true;
  }
  if (o.evidence?.[0]?.researchProvider || o.sources?.[0]?.researchProvider) {
    return true;
  }
  // Local Stage 2 / Exa shadow rows
  if (String(o.id || "").startsWith("gdi_ws_v72_")) return true;
  if (o.publicationStatus === "LOCAL_SHADOW_ONLY" && o.primaryContact) return true;
  return false;
}

function orgRoutePresent(o = {}) {
  const urls = [
    o.contactOfficialUrl,
    o.contactUrl,
    o.primaryContact?.officialContactUrl,
    o.primaryContact?.organizationContactRoute,
    o.primaryContact?.url,
    o.organizationContactEmail,
    o.companyPhone,
    o.sources?.[0]?.url,
  ];
  return urls.some((u) => {
    const s = text(u);
    return s.includes("@") || /^https?:/i.test(s) || /\d{7,}/.test(s);
  });
}

/**
 * Assess contact research completion for one opportunity.
 */
export function assessContactResearchCompletion(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const named = collectNamedFromOpportunity(o);
  const tw = o.scalingTwoContactPresentation || {};
  const drawerPrimary = namedPerson(tw.primary) || namedPerson(o.primaryContact);
  const drawerSecondary =
    namedPerson(tw.secondary) || namedPerson(o.secondaryContact);

  const primary = drawerPrimary || named[0] || null;
  const secondary =
    drawerSecondary ||
    named.find(
      (n) => text(n.name).toLowerCase() !== text(primary?.name).toLowerCase(),
    ) ||
    null;

  const primaryRelevant = primary ? relevanceSupported(primary) : false;
  const secondaryRelevant = secondary ? relevanceSupported(secondary) : false;
  const authority = primary ? authorityVerified(primary, o) : false;
  const researchAttempted = buyerResearchAttempted(o);
  const orgRoute = orgRoutePresent(o);

  let evidenceClass = CONTACT_EVIDENCE_CLASS.NO_NAMED_CONTACT;
  if (authority) {
    evidenceClass = CONTACT_EVIDENCE_CLASS.CONTRACTING_AUTHORITY_VERIFIED;
  } else if (primary && primaryRelevant) {
    evidenceClass = CONTACT_EVIDENCE_CLASS.CONTACT_COMMERCIALLY_RELEVANT;
  } else if (primary || named.length) {
    evidenceClass = CONTACT_EVIDENCE_CLASS.CONTACT_FOUND_IN_RESEARCH;
  } else if (orgRoute) {
    evidenceClass = CONTACT_EVIDENCE_CLASS.ORGANIZATION_ROUTE_ONLY;
  }

  const closed =
    /closed|disqualified|fully.?placed|venue.?already/i.test(
      `${o.priority || ""} ${o.opportunityType || ""} ${o.venueSourcingStatus || ""} ${o.qualificationFailureReason || ""}`,
    ) || o.opportunityQualification === "CLOSED";

  // Contact-layer disposition only. A named relevant person does NOT make an
  // opportunity SALES_READY — commercial demand + fit are required separately
  // via commercial-customer-readiness-gate-v1 (structural drawers ≠ ready).
  let disposition = CONTACT_COMMERCIAL_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
  if (closed && !primaryRelevant) {
    disposition = CONTACT_COMMERCIAL_DISPOSITION.NOT_CUSTOMER_READY;
  } else if (closed && primaryRelevant) {
    disposition = CONTACT_COMMERCIAL_DISPOSITION.WATCH;
  } else if (primaryRelevant && (researchAttempted || primary)) {
    // Contact research complete enough to support a commercial decision —
    // not automatic customer sales-ready.
    disposition = CONTACT_COMMERCIAL_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
  } else if (!researchAttempted && !primary) {
    disposition = CONTACT_COMMERCIAL_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
  } else if (orgRoute && !primary) {
    disposition = CONTACT_COMMERCIAL_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED;
  }

  const unresolved = [];
  if (!primary) {
    unresolved.push("Identify at least one commercially relevant named person.");
  } else if (!primaryRelevant) {
    unresolved.push(
      "Verify commercial relevance of the named person to hotel / travel / meetings demand.",
    );
  }
  if (primary && !secondary) {
    unresolved.push(
      "Secondary / referral named contact not yet verified (preferred, not mandatory).",
    );
  }
  if (primary && !authority) {
    unresolved.push(
      "Hotel-sourcing / contracting authority not verified for the named contact.",
    );
  }

  return {
    version: CONTACT_RESEARCH_COMPLETION_GATE_V1,
    contractVersion: UNIVERSAL_CONTACT_RESEARCH_CONTRACT_V1,
    opportunityId: o.id || o.opportunityId || null,
    buyerResearchAttempted: researchAttempted,
    researchStrategiesExecuted: [
      o.researchProvider || o.evidence?.[0]?.researchProvider || null,
      o.deepResearchProvider || null,
      o.discoveryProvider || null,
    ].filter(Boolean),
    primaryRelevantPersonFound: Boolean(primary && primaryRelevant),
    secondaryRelevantPersonFound: Boolean(secondary && secondaryRelevant),
    primaryName: primary?.name || null,
    secondaryName: secondary?.name || null,
    currentAffiliationSupported: Boolean(
      primary &&
        (primary.organization ||
          o.organizationName ||
          primary.sourceUrl ||
          primary.linkedinUrl),
    ),
    commercialRelevanceSupported: primaryRelevant,
    usableContactOrReferralRoute: Boolean(
      (primary && primaryRelevant) || orgRoute,
    ),
    hotelSourcingAuthorityVerified: authority,
    evidenceClass,
    commercialDisposition: disposition,
    unresolvedContactResearchQuestions: unresolved,
    namedPeopleInResearch: named.map((n) => ({
      name: n.name,
      title: n.title || n.role || null,
      src: n.src,
    })),
  };
}

/**
 * Attach completion assessment onto opportunity (non-destructive).
 */
export function attachContactResearchCompletionGate(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const assessment = assessContactResearchCompletion(o);
  return {
    ...o,
    contactResearchCompletion: assessment,
    contactEvidenceClass: assessment.evidenceClass,
    contactCommercialDisposition: assessment.commercialDisposition,
  };
}
