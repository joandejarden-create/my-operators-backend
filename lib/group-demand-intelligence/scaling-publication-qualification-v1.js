/**
 * Scaling Corporate Accounts — publication qualification (existing business rule).
 *
 * Mandatory for customer-facing publication:
 *   A. Verified relevant company activity
 *   B. Realistic evidence-grounded hotel-demand thesis
 *   C. At least ONE verified named, commercially relevant potential buyer
 *   D. Usable verified contact route (direct preferred; org route OK if labeled)
 *
 * Explicitly NOT required: RFP, room-night volume, purchase intent, second named buyer.
 *
 * Research continuation ≠ publication. Never invent contacts. Never silent-withdraw.
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  extractPrimaryPersonName,
  classifyScalingContactCoverage,
  SCALING_CONTACT_COVERAGE,
} from "./scaling-two-contact-standard-v1.js";
import { secondaryContactHasUsableRoute } from "./scaling-companies-sales-presentation-v1.js";
import {
  evaluateProspectSuitability,
  hasUsableContactRoute,
  classifyProspectUseCase,
  PROSPECT_USE_CASE,
} from "./prospect-publication-eligibility-v1.js";
import { classifyCommercialDemandThesis } from "./commercial-demand-thesis-standard-v1.js";
import { assessTargetHotelDemandAvailability } from "./target-hotel-demand-availability-v1.js";

export const SCALING_PUBLICATION_QUALIFICATION_V1 =
  "gdi-scaling-publication-qualification-v1";

export const SCALING_PUBLICATION_STATUS = Object.freeze({
  PUBLICATION_QUALIFIED: "PUBLICATION_QUALIFIED",
  RESEARCH_CONTINUATION_BUYER: "RESEARCH_CONTINUATION_BUYER",
  RESEARCH_CONTINUATION_DEMAND: "RESEARCH_CONTINUATION_DEMAND",
  RESEARCH_CONTINUATION_ACTIVITY: "RESEARCH_CONTINUATION_ACTIVITY",
  RESEARCH_CONTINUATION_ROUTE: "RESEARCH_CONTINUATION_ROUTE",
  COMMERCIALLY_INELIGIBLE: "COMMERCIALLY_INELIGIBLE",
  NOT_SCALING_SEGMENT: "NOT_SCALING_SEGMENT",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const IRRELEVANT_ROLE_RE =
  /\b(media|press|pr\b|public relations|spokesperson|communications director|journalist|reporter|investor relations|ir\b|external affairs)\b/i;

const RELEVANT_ROLE_RE =
  /\b(travel|procurement|purchasing|sourcing|meetings?|events?|catering|f&b|food.?and.?beverage|workplace|facilities|office manager|operations|ops\b|hr\b|human resources|people ops|talent|training|learning|onboarding|admin|administrator|general manager|site lead|local (?:lead|manager|director)|branch manager|office lead|executive assistant|chief of staff|hospitality|lodging|housing)\b/i;

/**
 * Detect employer/org mismatch in title strings like "Director … at Other Company".
 */
export function detectEmployerAffiliationMismatch(person = {}, opportunity = {}) {
  const role = text(
    person.role || person.title || person.relevantResponsibility || "",
  );
  const org = text(opportunity.organizationName || opportunity.company || "");
  if (!role || !org) return { mismatch: false };
  const atMatch = role.match(/\bat\s+(.+)$/i);
  if (!atMatch) return { mismatch: false };
  const statedEmployer = text(atMatch[1]).replace(/[,.].*$/, "");
  if (!statedEmployer || statedEmployer.length < 3) return { mismatch: false };
  const orgNorm = org.toLowerCase().replace(/[^a-z0-9]/g, "");
  const empNorm = statedEmployer.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (!empNorm) return { mismatch: false };
  // Allow if employer string contains org or vice versa (SOTEREON.AI vs Sotereon)
  if (empNorm.includes(orgNorm) || orgNorm.includes(empNorm)) {
    return { mismatch: false, statedEmployer, organizationName: org };
  }
  // Short token overlap (e.g. "Sotereon AI")
  const orgTokens = org
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 4);
  if (orgTokens.some((t) => empNorm.includes(t))) {
    return { mismatch: false, statedEmployer, organizationName: org };
  }
  return {
    mismatch: true,
    code: "NAMED_PERSON_EMPLOYER_MISMATCH",
    statedEmployer,
    organizationName: org,
    note: "Person title attributes them to a different organization — do not treat as company buyer.",
  };
}

/**
 * Is this person a commercially relevant potential buyer / influencer / referral?
 * Unknown role is allowed when name is verified and demand thesis exists —
 * but media/PR/IR titles and wrong-employer attributions are rejected.
 */
export function isCommerciallyRelevantScalingBuyer(person = {}, opportunity = {}) {
  const name = text(person.name || person.person || "");
  if (!name || !looksLikePersonName(name)) {
    return { ok: false, reason: "not_a_person_name" };
  }
  if (/relevant contact being researched/i.test(name)) {
    return { ok: false, reason: "placeholder_name" };
  }
  const role = text(
    person.role ||
      person.title ||
      person.relevantResponsibility ||
      opportunity.contactResponsibility ||
      opportunity.buyerRole ||
      "",
  );
  const affiliation = detectEmployerAffiliationMismatch(
    { name, role, title: role },
    opportunity,
  );
  if (affiliation.mismatch) {
    return {
      ok: false,
      reason: "employer_affiliation_mismatch",
      role,
      affiliation,
    };
  }
  if (role && IRRELEVANT_ROLE_RE.test(role)) {
    return { ok: false, reason: "irrelevant_role_media_pr_ir", role };
  }
  const demandBlob = `${opportunity.potentialHotelBusiness || ""} ${opportunity.proposedHotelUse || ""} ${opportunity.salesBrief?.potentialBusinessForThisHotel || ""}`;
  const roleOk =
    !role ||
    RELEVANT_ROLE_RE.test(role) ||
    /director|manager|vp|vice president|head of|lead|partner|principal|founder|ceo|coo|cfo|chro|cso|cto|cmo|chief\b|officer\b/i.test(
      role,
    );
  if (!roleOk) {
    return { ok: false, reason: "role_not_commercially_justified", role };
  }
  // Soft relevance: if role empty, still OK when demand thesis exists (qualify later)
  return {
    ok: true,
    reason: role
      ? RELEVANT_ROLE_RE.test(role)
        ? "role_matches_demand_functions"
        : "leadership_or_local_management"
      : "named_person_role_unspecified_qualify_on_outreach",
    role: role || null,
    demandContextPresent: Boolean(text(demandBlob)),
  };
}

/**
 * Detect likely contact mis-attribution (email local-part vs person name,
 * or org-route email belonging to a different named identity).
 */
export function detectInvalidBuyerContactAttribution(opportunity = {}) {
  const name = extractPrimaryPersonName(opportunity);
  const email = text(
    opportunity.primaryContact?.email ||
      opportunity.contactEmail ||
      opportunity.primaryContactEmail ||
      "",
  );
  const orgRoute = text(
    opportunity.primaryContact?.organizationContactRoute ||
      opportunity.organizationContactEmail ||
      opportunity.functionalContactEmail ||
      "",
  );
  const affiliation = detectEmployerAffiliationMismatch(
    {
      name,
      role:
        opportunity.primaryContact?.role ||
        opportunity.primaryContact?.title ||
        opportunity.contactResponsibility,
      title: opportunity.primaryContact?.title,
    },
    opportunity,
  );
  if (affiliation.mismatch) {
    return {
      invalid: true,
      code: affiliation.code,
      name,
      ...affiliation,
      note: "Do not grandfather invalid buyer associations — stage correction.",
    };
  }
  const isGenericOrgMailbox = (em) =>
    /^(info|contact|hello|press|pr|media|sales|support|admin|office|enquiries|inquiries|newbiz|newbusiness|hq|careers|recruiting|recruitment|team|general|customerservice|customer\.?service|custserv|help|noreply|no-reply|bocaraton|atlanta|jacksonville|miami|dallas|denver|dcmetro)@/i.test(
      text(em),
    );

  const emailsToCheck = [email, orgRoute].filter((e) => e && e.includes("@"));
  for (const em of emailsToCheck) {
    if (!name) continue;
    // Generic org mailboxes (info@, contact@) are valid ask-for routes — not person misattribution.
    if (isGenericOrgMailbox(em)) continue;
    const local = em.split("@")[0].toLowerCase().replace(/[^a-z]/g, "");
    const tokens = name
      .toLowerCase()
      .split(/\s+/)
      .map((t) => t.replace(/[^a-z]/g, ""))
      .filter((t) => t.length >= 3);
    const matches =
      tokens.some((t) => local.includes(t) || t.includes(local.slice(0, 4))) ||
      (local.length >= 4 &&
        name.toLowerCase().replace(/[^a-z]/g, "").includes(local.slice(0, 4)));
    // Person-shaped org-route email (e.g. jbrinkerhoff@) attached while primary is a different person.
    if (!matches && em === orgRoute && name) {
      return {
        invalid: true,
        code: "ORG_ROUTE_EMAIL_BELONGS_TO_DIFFERENT_IDENTITY",
        name,
        email: em,
        note: "Organization route email does not match the named primary — do not present as that person's contact. Stage correction.",
      };
    }
    if (!matches && em === email) {
      return {
        invalid: true,
        code: "EMAIL_LOCAL_PART_DOES_NOT_MATCH_NAMED_PERSON",
        name,
        email: em,
        note: "Do not grandfather invalid buyer associations — stage correction.",
      };
    }
  }
  return { invalid: false, name, email: email || null };
}

function extractNamedBuyers(opportunity = {}) {
  const buyers = [];
  const primaryName = extractPrimaryPersonName(opportunity);
  if (primaryName) {
    buyers.push({
      slot: "primary",
      name: primaryName,
      role:
        opportunity.primaryContact?.role ||
        opportunity.primaryContact?.title ||
        opportunity.contactResponsibility ||
        null,
      email: opportunity.primaryContact?.email || opportunity.contactEmail || null,
      phone: opportunity.primaryContact?.phone || opportunity.contactPhone || null,
    });
  }
  const sec = opportunity.secondaryContact;
  const secName = text(sec?.name);
  if (secName && looksLikePersonName(secName)) {
    buyers.push({
      slot: "secondary",
      name: secName,
      role: sec.role || sec.title || sec.relevantResponsibility || null,
      email: sec.email || null,
      phone: sec.phone || null,
    });
  }
  // namedStakeholders array
  for (const s of opportunity.namedStakeholders || []) {
    const n = text(s?.name || s?.person);
    if (!n || !looksLikePersonName(n)) continue;
    if (buyers.some((b) => b.name.toLowerCase() === n.toLowerCase())) continue;
    buyers.push({
      slot: "stakeholder",
      name: n,
      role: s.role || s.title || null,
      email: s.email || null,
      phone: s.phone || null,
    });
  }
  return buyers;
}

function hasUsableRouteForNamedBuyer(opportunity = {}, buyer = {}) {
  const route = hasUsableContactRoute(opportunity);
  if (!route.ok) return { ok: false, reason: route.reason || "no_usable_route" };
  // Direct person email/phone preferred
  if (text(buyer.email) && buyer.email.includes("@")) {
    return { ok: true, reason: "direct_or_attributed_email", labeling: "person_or_org" };
  }
  if (text(buyer.phone)) {
    return { ok: true, reason: "direct_or_attributed_phone", labeling: "verify_ownership" };
  }
  // Org route that enables outreach to the named person
  if (
    text(opportunity.companyPhone) ||
    text(opportunity.organizationContactEmail) ||
    text(opportunity.functionalContactEmail) ||
    text(opportunity.contactUrl) ||
    text(opportunity.organizationContactUrl)
  ) {
    return {
      ok: true,
      reason: "organizational_route_ask_for_named_person",
      labeling: "must_not_present_as_person_direct",
    };
  }
  if (secondaryContactHasUsableRoute(opportunity.secondaryContact || {})) {
    return { ok: true, reason: "secondary_usable_route" };
  }
  return { ok: false, reason: "named_buyer_without_usable_route" };
}

/**
 * Realistic hotel-demand thesis — inferred OK; RFP/volume NOT required.
 * Independent evidence classifier is authoritative; generated thesis wording alone is not.
 */
export function evaluateRealisticHotelDemandThesis(opportunity = {}) {
  const classified = classifyCommercialDemandThesis(opportunity);
  const blob = [
    opportunity.potentialHotelBusiness,
    opportunity.proposedHotelUse,
    opportunity.hotelDemandThesis,
    opportunity.salesBrief?.potentialBusinessForThisHotel,
    opportunity.salesBrief?.whyThisHotel,
    opportunity.demandMechanism,
    opportunity.hotelOpportunityThesis,
    opportunity.summaryWhyHotel,
    opportunity.roomDemandRationale,
    opportunity.bethesdaWinThesis,
    opportunity.fitExplanation,
    opportunity.whyThisHotel,
    opportunity.summaryWhyMatters,
    ...(opportunity.inferredHotelDemand || []),
    classified.mechanism?.excerpt || "",
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");

  const hotelSpecific =
    /hotel|marriott|wyndham|waterstone|fiesta|bayside|property|airport.?connected|meeting room/i.test(
      blob,
    ) || Boolean(text(opportunity.whyThisHotel || opportunity.salesBrief?.whyThisHotel));

  // Explicitly do NOT require these
  const falselyRequired =
    /must have confirmed RFP|requires signed contract|requires confirmed room.?night volume/i.test(
      blob,
    );

  if (!classified.mayQualifySalesReadyDemandGate) {
    return {
      ok: false,
      reason:
        classified.thesisStandard === "CONTRADICTORY_OR_NEGATIVE"
          ? "contradictory_or_negative_demand_evidence"
          : "no_credible_demand_mechanism",
      thesisStandard: classified.thesisStandard,
      note: "Need guestroom/meetings/catering thesis grounded in independent evidence — not generic speculation.",
    };
  }
  const targetAvailability = assessTargetHotelDemandAvailability(opportunity);
  if (targetAvailability.ok === false) {
    return {
      ok: false,
      reason: targetAvailability.reason,
      thesisStandard: classified.thesisStandard,
      note: "Competitor/official host arrangements are not automatically demand available to this hotel.",
      targetHotelAvailability: targetAvailability,
    };
  }
  if (!hotelSpecific && !text(opportunity.proposedHotelUse)) {
    return {
      ok: false,
      reason: "demand_not_tied_to_hotel",
      thesisStandard: classified.thesisStandard,
      note: "Thesis must connect company activity to this hotel's product.",
    };
  }
  return {
    ok: true,
    reason: classified.reason || "realistic_inferred_or_evidenced_thesis",
    thesisStandard: classified.thesisStandard,
    requiresRfp: false,
    requiresVolume: false,
    requiresConfirmedRoomNights: false,
    requiresGroupBlock: false,
    requiresBookingAnnouncement: false,
    requiresActiveSourcingConfirmation: false,
    falselyRequiredLanguage: falselyRequired,
    note: "Reliable inferred demand may qualify — confirmed room nights, group blocks, RFP, or booking announcement are not required.",
  };
}

/**
 * Full Scaling publication qualification evaluation.
 */
export function evaluateScalingPublicationQualification(opportunity = {}, opts = {}) {
  const useCase = classifyProspectUseCase(opportunity);
  const isScaling =
    useCase === PROSPECT_USE_CASE.SCALING_CORPORATE ||
    opportunity.researchSegment === "scaling_companies" ||
    /scaling/i.test(String(opportunity.segment || ""));

  if (!isScaling && opts.requireScalingSegment !== false) {
    return {
      version: SCALING_PUBLICATION_QUALIFICATION_V1,
      status: SCALING_PUBLICATION_STATUS.NOT_SCALING_SEGMENT,
      publicationQualified: false,
      researchContinuation: false,
      useCase,
    };
  }

  const suitability = evaluateProspectSuitability(opportunity);
  const hasActivity =
    Boolean(text(opportunity.growthEvidenceDate)) ||
    Boolean(text(opportunity.datedGrowthEvidence)) ||
    Boolean(text(opportunity.activityHeadline)) ||
    Boolean(text(opportunity.whatIsHappening) && text(opportunity.localConnection)) ||
    Boolean(opportunity.operatingExpansionEvidence === true) ||
    (Array.isArray(opportunity.growthSignals) && opportunity.growthSignals.length > 0) ||
    (suitability.ok === true &&
      !suitability.failed?.includes("dated_growth_or_expansion_evidence"));

  const demand = evaluateRealisticHotelDemandThesis(opportunity);
  const route = hasUsableContactRoute(opportunity);
  const buyers = extractNamedBuyers(opportunity);
  const attribution = detectInvalidBuyerContactAttribution(opportunity);

  let relevantBuyer = null;
  let buyerEval = null;
  for (const b of buyers) {
    if (attribution.invalid && b.slot === "primary") {
      // Invalid association does not count
      continue;
    }
    const ev = isCommerciallyRelevantScalingBuyer(b, opportunity);
    if (ev.ok) {
      const routeForBuyer = hasUsableRouteForNamedBuyer(opportunity, b);
      if (routeForBuyer.ok) {
        relevantBuyer = { ...b, relevance: ev, route: routeForBuyer };
        buyerEval = ev;
        break;
      }
      buyerEval = { ...ev, routeOk: false, routeReason: routeForBuyer.reason };
    } else {
      buyerEval = ev;
    }
  }

  const coverage = classifyScalingContactCoverage(opportunity);
  const secondBuyerResearchDesired =
    coverage !== SCALING_CONTACT_COVERAGE.TWO_NAMED_CONTACTABLE;

  const gates = {
    verifiedCompanyActivity: hasActivity,
    realisticHotelDemandThesis: demand.ok === true,
    oneNamedRelevantBuyer: Boolean(relevantBuyer),
    usableContactRoute: route.ok === true || Boolean(relevantBuyer?.route?.ok),
    // Explicit non-gates
    confirmedRoomVolumeRequired: false,
    confirmedRfpRequired: false,
    secondNamedBuyerRequired: false,
  };

  const gaps = [];
  if (!gates.verifiedCompanyActivity) gaps.push("VERIFIED_COMPANY_ACTIVITY");
  if (!gates.realisticHotelDemandThesis) gaps.push("REALISTIC_HOTEL_DEMAND_THESIS");
  if (!gates.oneNamedRelevantBuyer) gaps.push("ONE_NAMED_RELEVANT_BUYER");
  if (!gates.usableContactRoute) gaps.push("USABLE_CONTACT_ROUTE");
  if (attribution.invalid) gaps.push("INVALID_BUYER_CONTACT_ATTRIBUTION");

  let status = SCALING_PUBLICATION_STATUS.PUBLICATION_QUALIFIED;
  let researchContinuation = false;
  let publicationQualified = gaps.length === 0;

  if (!publicationQualified) {
    researchContinuation = true;
    if (!gates.verifiedCompanyActivity) {
      status = SCALING_PUBLICATION_STATUS.RESEARCH_CONTINUATION_ACTIVITY;
    } else if (!gates.realisticHotelDemandThesis) {
      status = SCALING_PUBLICATION_STATUS.RESEARCH_CONTINUATION_DEMAND;
    } else if (!gates.oneNamedRelevantBuyer) {
      status = SCALING_PUBLICATION_STATUS.RESEARCH_CONTINUATION_BUYER;
    } else if (!gates.usableContactRoute || attribution.invalid) {
      status = SCALING_PUBLICATION_STATUS.RESEARCH_CONTINUATION_ROUTE;
    }
  }

  return {
    version: SCALING_PUBLICATION_QUALIFICATION_V1,
    status,
    publicationQualified,
    researchContinuation,
    preserveOpportunity: true,
    doNotSilentWithdraw: true,
    gates,
    gaps,
    demand,
    route,
    coverage,
    relevantBuyer,
    buyerEval,
    attribution,
    secondBuyerResearchDesired,
    secondBuyerRequiredForPublication: false,
    suitability,
    useCase,
    note:
      "One verified named relevant buyer + usable route + activity + demand thesis. No RFP/volume/second-buyer gate. GDI 2.0 shared contract shadow: evaluateSharedCommercialEligibility / evaluatePublicationDecision (stricter unknown-role rule; does not change this result).",
  };
}

/**
 * Apply Scaling gate on top of generic prospect eligibility for Scaling rows.
 * Does not change non-Scaling segments.
 */
export function applyScalingPublicationGateToEligibility(
  eligibility = {},
  opportunity = {},
  opts = {},
) {
  const useCase =
    eligibility.suitability?.useCase || classifyProspectUseCase(opportunity);
  const isScaling =
    useCase === PROSPECT_USE_CASE.SCALING_CORPORATE ||
    opportunity.researchSegment === "scaling_companies" ||
    /scaling/i.test(String(opportunity.segment || ""));

  if (!isScaling) return { ...eligibility, scalingQualification: null };

  const q = evaluateScalingPublicationQualification(opportunity, opts);
  if (q.publicationQualified) {
    return {
      ...eligibility,
      hotelFacing: eligibility.hotelFacing === true,
      scalingQualification: q,
      scalingPublicationQualified: true,
    };
  }

  // Fail closed for NEW publication — keep enrichment queue, never pretend HOTEL_FACING
  return {
    ...eligibility,
    hotelFacing: false,
    enrichmentQueue: true,
    eligibility: "ENRICHMENT_QUEUE",
    scalingQualification: q,
    scalingPublicationQualified: false,
    reasons: [
      ...(eligibility.reasons || []),
      ...q.gaps.map((g) => `scaling_${g.toLowerCase()}`),
    ],
    nextTask:
      q.status === SCALING_PUBLICATION_STATUS.RESEARCH_CONTINUATION_BUYER
        ? "Continue named-buyer research — retain opportunity; do not publish as sales-ready."
        : q.status === SCALING_PUBLICATION_STATUS.RESEARCH_CONTINUATION_DEMAND
          ? "Strengthen hotel-demand thesis from company activity — do not fabricate demand."
          : "Continue Scaling research before customer-facing publication.",
  };
}
