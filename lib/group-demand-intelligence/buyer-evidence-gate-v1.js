/**
 * GDI 2.0 Phase 2F — buyer evidence gate.
 *
 * Separates candidate discovery from verified commercial buyer eligibility.
 * POTENTIALLY_RELEVANT / PROBABLE_RELEVANT alone must NOT imply BUYER_ELIGIBLE.
 *
 * Contact philosophy (Scaling Companies FullE2E):
 *   DIRECT_BUYER or RELEVANT_COMMERCIAL_ENTRY_POINT may satisfy commercial
 *   relevance. Direct hotel-booking authority is preferred, not required.
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  evaluateBuyerCommercialResponsibility,
  extractBuyerRoleFromOpportunity,
  resolveUsableContactRouteForEligibility,
  normalizeAttributionForSharedEligibility,
} from "./commercial-eligibility-contract-v1.js";
import { detectInvalidBuyerContactAttribution } from "./scaling-publication-qualification-v1.js";
import {
  classifyContactEligibility,
  buildContactEligibilityBriefFields,
  CONTACT_ELIGIBILITY_CLASS,
} from "./contact-eligibility-standard-v1.js";

export const BUYER_EVIDENCE_GATE_V1 = "gdi-buyer-evidence-gate-v1";

export const BUYER_EVIDENCE_STAGE = Object.freeze({
  NAMED_PERSON_IDENTIFIED: "NAMED_PERSON_IDENTIFIED",
  ORGANIZATIONAL_AFFILIATION_VERIFIED: "ORGANIZATIONAL_AFFILIATION_VERIFIED",
  ROLE_VERIFIED: "ROLE_VERIFIED",
  COMMERCIAL_RELEVANCE_VERIFIED: "COMMERCIAL_RELEVANCE_VERIFIED",
  CONTACT_ROUTE_VERIFIED: "CONTACT_ROUTE_VERIFIED",
  BUYER_ELIGIBLE: "BUYER_ELIGIBLE",
});

const NON_COMMERCIAL_ROUTE_RE =
  /^(recruiting|careers|jobs|talent|hr|press|media|pr|noreply|no-reply|support|helpdesk|billing)@/i;

/** Corporate / directory / publisher strings mistaken for people */
const ORG_MASQUERADING_AS_PERSON_RE =
  /\b(verlag|gmbh|ltd|llc|inc|corp|corporation|company|associat(?:ion|e)s?|foundation|institute|directory|publisher|publishing|construct|data\b|holdings|group|agencia|empresa|organización|organizacion|s\.?l\.?|s\.?a\.?)\b/i;

/** Role blobs that are page copy / spam, not job titles */
const ROLE_SPAM_OR_NON_TITLE_RE =
  /\b(contactan|regularmen|cookies|utilizamos|directory contact|expo guide|international fairs|fair,?\s*expo|página|informaci[oó]n|click here|read more)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function cleanTitle(raw) {
  let t = text(raw);
  // Strip culture-copy pollution after title ("Manager of X We Inspire…")
  t = t.replace(/\s+We\s+(Inspire|Have Fun|Are|Believe|Value)\b[\s\S]*$/i, "");
  t = t.replace(/\s+IF\s+work\b[\s\S]*$/i, "");
  if (t.length > 80) t = t.slice(0, 80).trim();
  return t;
}

/**
 * Evaluate staged buyer evidence without mutating the opportunity.
 */
export function evaluateBuyerEvidenceGate(person = {}, opportunity = {}, opts = {}) {
  const stages = {
    [BUYER_EVIDENCE_STAGE.NAMED_PERSON_IDENTIFIED]: false,
    [BUYER_EVIDENCE_STAGE.ORGANIZATIONAL_AFFILIATION_VERIFIED]: false,
    [BUYER_EVIDENCE_STAGE.ROLE_VERIFIED]: false,
    [BUYER_EVIDENCE_STAGE.COMMERCIAL_RELEVANCE_VERIFIED]: false,
    [BUYER_EVIDENCE_STAGE.CONTACT_ROUTE_VERIFIED]: false,
    [BUYER_EVIDENCE_STAGE.BUYER_ELIGIBLE]: false,
  };
  const notes = [];

  const name = text(person.name || person.person || opportunity.contactName || "");
  let namedOk = Boolean(name && looksLikePersonName(name));
  if (namedOk && ORG_MASQUERADING_AS_PERSON_RE.test(name)) {
    namedOk = false;
    notes.push("org_or_directory_name_masquerading_as_person");
  }
  if (namedOk && name.split(/\s+/).length >= 3 && /\b(data|fair|expo|guide|international)\b/i.test(name)) {
    namedOk = false;
    notes.push("multi_token_corporate_phrase_rejected_as_person");
  }
  stages[BUYER_EVIDENCE_STAGE.NAMED_PERSON_IDENTIFIED] = namedOk;
  if (!namedOk) {
    if (!notes.includes("org_or_directory_name_masquerading_as_person") &&
        !notes.includes("multi_token_corporate_phrase_rejected_as_person")) {
      notes.push("named_person_not_identified");
    }
    return summarize(stages, notes, person, opportunity, null);
  }

  const org = text(
    person.organization ||
      person.currentOrganization ||
      opportunity.organizationName ||
      "",
  );
  const affiliationOk =
    Boolean(org) &&
    !/employer_affiliation_mismatch/i.test(String(opts.affiliationReason || ""));
  // Soft affiliation: same page org name as opportunity is accepted when source is org-owned
  stages[BUYER_EVIDENCE_STAGE.ORGANIZATIONAL_AFFILIATION_VERIFIED] =
    affiliationOk &&
    (opts.affiliationVerified === true ||
      opts.contactDataOrigin === "PUBLIC_SOURCE" ||
      Boolean(text(opportunity.organizationName)));
  if (!stages[BUYER_EVIDENCE_STAGE.ORGANIZATIONAL_AFFILIATION_VERIFIED]) {
    notes.push("organizational_affiliation_unverified");
  }

  const role = cleanTitle(
    person.role ||
      person.title ||
      extractBuyerRoleFromOpportunity(person, opportunity) ||
      "",
  );
  let roleOk =
    Boolean(role) &&
    role.length >= 3 &&
    role.length <= 80 &&
    !/^unknown$/i.test(role) &&
    !ROLE_SPAM_OR_NON_TITLE_RE.test(role);
  if (role && ROLE_SPAM_OR_NON_TITLE_RE.test(role)) {
    notes.push("role_looks_like_page_copy_not_job_title");
  }
  stages[BUYER_EVIDENCE_STAGE.ROLE_VERIFIED] = roleOk;
  if (!roleOk && !notes.includes("role_looks_like_page_copy_not_job_title")) {
    notes.push("role_not_verified");
  }

  const contactEligibility = roleOk
    ? classifyContactEligibility({ ...person, name, role, title: role }, opportunity, opts)
    : null;

  let commercialOk = false;
  if (roleOk && contactEligibility) {
    if (contactEligibility.salesReadyContactOk) {
      commercialOk = true;
      if (contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.DIRECT_BUYER) {
        notes.push("contact_class_direct_buyer");
      } else if (
        contactEligibility.class ===
        CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT
      ) {
        notes.push("contact_class_relevant_commercial_entry_point");
        if (contactEligibility.continueResearchForDirectBuyer) {
          notes.push("continue_research_for_direct_buyer_when_justified");
        }
      }
    } else if (
      contactEligibility.class ===
      CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK
    ) {
      notes.push("leadership_without_evidenced_demand_activity_connection");
    } else if (
      contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_IRRELEVANT ||
      contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.UNRELATED_CONTACT
    ) {
      notes.push("role_not_hotel_commercially_relevant");
    } else if (contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE) {
      notes.push("research_candidate_preserve_for_further_buyer_research");
    } else {
      notes.push(contactEligibility.reason || "commercial_relevance_not_verified");
    }
  }

  // Classification hints from public buyer pass cannot alone verify commercial relevance
  const classHint = text(
    `${person.responsibilityClass || ""} ${person.buyerClassification || ""} ${person.decisionMakerClass || ""}`,
  );
  if (
    /POTENTIALLY_RELEVANT|PROBABLE_RELEVANT|POTENTIAL_DECISION|INFLUENCER/i.test(classHint) &&
    !commercialOk
  ) {
    notes.push("potential_or_probable_relevance_insufficient_without_role_evidence");
  }

  stages[BUYER_EVIDENCE_STAGE.COMMERCIAL_RELEVANCE_VERIFIED] = commercialOk;

  const responsibility = evaluateBuyerCommercialResponsibility(
    { ...person, name, role, title: role },
    opportunity,
  );
  if (responsibility.ok && !commercialOk) {
    notes.push("shared_responsibility_ok_but_gate_commercial_relevance_pending");
  }

  const attribution = normalizeAttributionForSharedEligibility(
    detectInvalidBuyerContactAttribution({
      ...opportunity,
      primaryContact: { ...(opportunity.primaryContact || {}), name, role, email: person.email },
      contactEmail: person.email || opportunity.contactEmail,
    }),
    opportunity,
  );
  const route = resolveUsableContactRouteForEligibility(opportunity, {
    name,
    email: person.email,
  });
  // Functional org routes are acceptable when they can reach the buyer.
  // ORG_ROUTE_EMAIL_BELONGS_TO_DIFFERENT_IDENTITY means "do not present as person-direct"
  // — it does NOT invalidate CONTACT_ROUTE_VERIFIED.
  const orgRouteAttributionOnly =
    attribution?.invalid === true &&
    /ORG_ROUTE_EMAIL_BELONGS_TO_DIFFERENT_IDENTITY|organizational_route/i.test(
      `${attribution?.code || ""} ${attribution?.note || ""} ${route?.reason || ""}`,
    );
  let routeOk =
    route.ok === true && (attribution.invalid !== true || orgRouteAttributionOnly);
  const routeEmail = text(
    opportunity.organizationContactEmail ||
      opportunity.functionalContactEmail ||
      person.email ||
      opportunity.contactEmail ||
      "",
  );
  if (routeEmail && NON_COMMERCIAL_ROUTE_RE.test(routeEmail)) {
    routeOk = false;
    notes.push("contact_route_not_commercially_appropriate_for_hotel_buyer");
  }
  if (orgRouteAttributionOnly && routeOk) {
    notes.push("functional_org_route_accepted_not_person_direct");
  }
  stages[BUYER_EVIDENCE_STAGE.CONTACT_ROUTE_VERIFIED] = routeOk;

  const buyerEligible =
    stages[BUYER_EVIDENCE_STAGE.NAMED_PERSON_IDENTIFIED] &&
    stages[BUYER_EVIDENCE_STAGE.ORGANIZATIONAL_AFFILIATION_VERIFIED] &&
    stages[BUYER_EVIDENCE_STAGE.ROLE_VERIFIED] &&
    stages[BUYER_EVIDENCE_STAGE.COMMERCIAL_RELEVANCE_VERIFIED] &&
    stages[BUYER_EVIDENCE_STAGE.CONTACT_ROUTE_VERIFIED] &&
    commercialOk;

  stages[BUYER_EVIDENCE_STAGE.BUYER_ELIGIBLE] = buyerEligible;
  if (!buyerEligible) notes.push("buyer_not_eligible_pending_verified_stages");

  const briefFields = buildContactEligibilityBriefFields(contactEligibility || {});

  return summarize(stages, notes, { ...person, name, role, title: role }, opportunity, {
    responsibility,
    route,
    attribution,
    cleanedRole: role,
    contactEligibility,
    ...briefFields,
  });
}

function summarize(stages, notes, person, opportunity, detail) {
  const completed = Object.entries(stages)
    .filter(([, v]) => v)
    .map(([k]) => k);
  const contactEligibility = detail?.contactEligibility || null;
  const briefFields = buildContactEligibilityBriefFields(contactEligibility || {});
  return {
    version: BUYER_EVIDENCE_GATE_V1,
    stages,
    completedStages: completed,
    buyerEligible: stages[BUYER_EVIDENCE_STAGE.BUYER_ELIGIBLE] === true,
    candidateOnly: stages[BUYER_EVIDENCE_STAGE.NAMED_PERSON_IDENTIFIED] && !stages[BUYER_EVIDENCE_STAGE.BUYER_ELIGIBLE],
    person: person
      ? {
          name: text(person.name) || null,
          role: text(person.role || person.title) || null,
        }
      : null,
    contactEligibilityClass: briefFields.contactEligibilityClass,
    contactTypeForBrief: briefFields.contactTypeForBrief,
    isDirectBuyer: briefFields.isDirectBuyer,
    isRelevantCommercialEntryPoint: briefFields.isRelevantCommercialEntryPoint,
    continueResearchForDirectBuyer: briefFields.continueResearchForDirectBuyer,
    notes: [...new Set(notes)],
    detail,
    preserveCandidate: true,
    note:
      "Candidates and evidence are preserved even when BUYER_ELIGIBLE is false. DIRECT_BUYER preferred; RELEVANT_COMMERCIAL_ENTRY_POINT also eligible.",
  };
}

/**
 * Should research extraction promote this stakeholder onto the opportunity
 * as the primary commercial buyer?
 */
export function shouldPromoteStakeholderAsBuyer(stakeholder = {}, opportunity = {}) {
  const gate = evaluateBuyerEvidenceGate(
    {
      name: stakeholder.name,
      role: stakeholder.title || stakeholder.role,
      title: stakeholder.title || stakeholder.role,
      email: stakeholder.email,
      organization: stakeholder.currentOrganization,
      responsibilityClass: stakeholder.responsibilityClass,
      buyerClassification: stakeholder.buyerClassification,
      decisionMakerClass: stakeholder.decisionMakerClass,
    },
    opportunity,
    {
      contactDataOrigin: stakeholder.contactDataOrigin,
      affiliationVerified: Boolean(stakeholder.currentOrganization),
    },
  );
  return {
    promote: gate.buyerEligible === true,
    gate,
  };
}

/**
 * Shadow decision helper for audited sales-ready flips.
 * POTENTIALLY/PROBABLE relevance without hotel commercial remit → UNSUPPORTED.
 */
export function classifyQualificationIntegrity({
  beforeSalesReady,
  afterSalesReady,
  buyerGate,
  demandOk,
  evidenceSupportsHotelBuyer,
} = {}) {
  if (!afterSalesReady) {
    return {
      status: "NOT_SALES_READY",
      integrity: beforeSalesReady ? "REGRESSED" : "CONTINUATION",
      shadowDisposition: "RESEARCH_CONTINUATION",
    };
  }
  if (buyerGate?.buyerEligible && demandOk && evidenceSupportsHotelBuyer) {
    return {
      status: "VERIFIED",
      integrity: "EVIDENCE_BACKED",
      shadowDisposition: "SALES_READY",
    };
  }

  const notes = buyerGate?.notes || [];
  const contradicted =
    notes.some((n) =>
      /role_not_hotel_commercially_relevant|potential_or_probable_relevance_insufficient|contact_route_not_commercially_appropriate|leadership_without_evidenced_demand_activity|leadership_title_without_hotel_commercial_remit/i.test(
        n,
      ),
    ) || evidenceSupportsHotelBuyer === false;

  if (contradicted || !buyerGate?.stages?.[BUYER_EVIDENCE_STAGE.COMMERCIAL_RELEVANCE_VERIFIED]) {
    return {
      status: "UNSUPPORTED",
      integrity: "INVALID_PROMOTION",
      shadowDisposition: "RESEARCH_CONTINUATION",
      reason: notes.join("; ") || "insufficient_buyer_or_demand_evidence",
    };
  }

  if (buyerGate?.stages?.[BUYER_EVIDENCE_STAGE.NAMED_PERSON_IDENTIFIED] && demandOk) {
    return {
      status: "PROVISIONAL",
      integrity: "INCOMPLETE_BUYER_STAGES",
      shadowDisposition: "RESEARCH_CONTINUATION",
      reason: notes.join("; "),
    };
  }
  return {
    status: "UNSUPPORTED",
    integrity: "INVALID_PROMOTION",
    shadowDisposition: "RESEARCH_CONTINUATION",
    reason: notes.join("; ") || "insufficient_buyer_or_demand_evidence",
  };
}
