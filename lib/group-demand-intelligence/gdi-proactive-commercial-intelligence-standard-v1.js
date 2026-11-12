/**
 * GDI Full E2E — Proactive Commercial Intelligence Standard V1
 *
 * Permanent operating policy for the EXISTING GDI architecture.
 * Does not create a parallel engine. Preserves CONFIRMED_DEMAND /
 * RELIABLE_INFERRED_DEMAND / SPECULATIVE_DEMAND terminology.
 *
 * Confirmed hotel booking, announced RFP, contracted room block, or verified
 * room-night volume STRENGTHEN an opportunity but are NOT universal eligibility
 * requirements. SPECULATIVE_DEMAND remains research continuation.
 *
 * Demand confidence stays separate from research completeness, commercial
 * maturity, sales priority, contact quality, revenue type, publication-review
 * readiness, and customer publication authorization.
 */

import {
  DEMAND_THESIS_STANDARD,
  classifyCommercialDemandThesis,
  thesisStandardMayQualifySalesReady,
  DEMAND_THESIS_NOT_REQUIRED,
  SALES_READY_REQUIRES,
} from "./commercial-demand-thesis-standard-v1.js";
import { evaluateAccountDevelopmentEligibility } from "./account-development-eligibility-v1.js";
import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import { attachCommercialDemandTypeClassification } from "./classify-opportunity-commercial-demand-type-v1.js";

export const GDI_PROACTIVE_COMMERCIAL_INTELLIGENCE_STANDARD_V1 =
  "gdi-proactive-commercial-intelligence-standard-v1";

export const DEMAND_CONFIDENCE_LEVEL = Object.freeze({
  LEVEL_1_CONFIRMED_DEMAND: DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND,
  LEVEL_2_RELIABLE_INFERRED_DEMAND: DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND,
  LEVEL_3_SPECULATIVE_DEMAND: DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND,
});

/** Explicit non-requirements — absence must never block Account Development. */
export const PROACTIVE_CI_NOT_REQUIRED = Object.freeze([
  ...DEMAND_THESIS_NOT_REQUIRED,
  "confirmed_hotel_selection",
  "announced_meeting",
  "verified_room_night_volume",
  "active_rfp",
  "two_named_contacts",
  "universal_direct_hotel_booking_responsibility",
]);

export const FIVE_PART_THESIS_PARTS = Object.freeze({
  VERIFIED_TRIGGER: "VERIFIED_TRIGGER",
  POTENTIAL_HOTEL_DEMAND_MECHANISM: "POTENTIAL_HOTEL_DEMAND_MECHANISM",
  HOTEL_SPECIFIC_COMMERCIAL_FIT: "HOTEL_SPECIFIC_COMMERCIAL_FIT",
  VERIFIED_NAMED_COMMERCIAL_ENTRY_POINT: "VERIFIED_NAMED_COMMERCIAL_ENTRY_POINT",
  ACTIONABLE_OUTREACH_THESIS: "ACTIONABLE_OUTREACH_THESIS",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function collectIndependentStatements(opportunity = {}) {
  const out = [];
  for (const item of Array.isArray(opportunity.evidence) ? opportunity.evidence : []) {
    if (!item || typeof item !== "object") continue;
    out.push(item.statement, item.quote, item.excerpt, item.text, item.value);
  }
  for (const claim of Array.isArray(opportunity.acceptedClaims)
    ? opportunity.acceptedClaims
    : []) {
    if (!claim || typeof claim !== "object") continue;
    out.push(claim.statement, claim.quote, claim.excerpt);
  }
  for (const source of Array.isArray(opportunity.sources) ? opportunity.sources : []) {
    if (!source || typeof source !== "object") continue;
    out.push(
      source.quote,
      source.excerpt,
      source.evidenceQuote,
      source.text,
      source.title,
      source.establishes,
    );
  }
  out.push(
    opportunity.evidenceQuote,
    opportunity.attendanceEvidenceText,
    opportunity.peakRoomsEvidenceText,
    opportunity.eventDateEvidenceText,
    opportunity.activityHeadline,
    opportunity.whatIsHappening,
    opportunity.datedGrowthEvidence,
    opportunity.title,
    opportunity.programmeName,
  );
  return out.map(text).filter(Boolean);
}

const VERIFIED_TRIGGER_PATTERNS = Object.freeze([
  {
    code: "LOCAL_OFFICE_OPENING_OR_EXPANSION",
    re: /\b(?:opened?|opening|opens|expand(?:ed|ing|s)?|announced).{0,40}\b(?:office|headquarters|hq|campus|location)\b|\bnew (?:office|headquarters|hq|campus)\b/i,
  },
  {
    code: "NEW_FACILITY_OR_MANUFACTURING_INVESTMENT",
    re: /\b(?:\$[\d.,]+\s*(?:billion|million).{0,80})?(?:manufacturing|facility|plant|laboratory|lab|clinical manufacturing|biologics).{0,40}(?:investment|expansion|facility|plant)?|\bnew (?:facility|plant|manufacturing)\b/i,
  },
  {
    code: "PROJECT_OR_WORKFORCE_DEPLOYMENT",
    re: /\b(?:project (?:team|deployment|kickoff)|workforce relocation|extended stay|field (?:team|staff|crew)|implementation team|visiting (?:workforce|staff|crew))\b/i,
  },
  {
    code: "TRAINING_OR_ONBOARDING_PROGRAM",
    re: /\b(?:training (?:program|centre|center|academy|cohort)|onboarding|l&d|learning and development)\b/i,
  },
  {
    code: "RECURRING_ASSOCIATION_OR_MEETING_ACTIVITY",
    re: /\b(?:annual (?:conference|meeting|summit)|recurring (?:meeting|event)|member meetings?|chapter (?:meeting|event)|legislative (?:conference|meeting))\b/i,
  },
  {
    code: "VENUE_WITHOUT_GUESTROOMS_REFERRAL",
    re: /\b(?:ballroom|patio venue|event venue|private (?:mansion|club|venue)|weddings?,?\s*corporate|fundraisers?|outside caterers?|approved.?caterers?)\b/i,
  },
  {
    code: "PROFESSIONAL_PLANNER_OR_DMC",
    re: /\b(?:event planner|meeting planner|destination management|DMC|event[- ]management (?:firm|company)|wedding planner)\b/i,
  },
  {
    code: "LOCAL_CATERING_OR_DAY_MEETING_ACTIVITY",
    re: /\b(?:local (?:meeting|meetings|reception|training)|day meeting|awards dinner|board (?:meeting|dinner)|corporate entertaining|receptions?)\b/i,
  },
  {
    code: "CORPORATE_TRAVEL_OR_RELOCATION",
    re: /\b(?:corporate travel|employee relocation|recruitment visits?|executive (?:travel|visits?)|partner collaboration|customer engagement)\b/i,
  },
]);

const DEMAND_MECHANISM_RE =
  /\b(guest.?rooms?|lodging|overnight|accommodation|housing|visiting|travel(?:ers?|ling)?|relocation|extended stay|training|meeting(?:s)?|catering|reception|banquet|referral|partner(?:ship)?|overflow|corporate entertain|customer engagement|partner collaboration|executive visit|onboarding|project (?:team|deployment)|field (?:team|staff)|recruitment|hiring)\b/i;

const HOTEL_FIT_RE =
  /\b(this hotel|bethesda marriott|marriott|wyndham|waterstone|hotel|property|drive[- ]?time|same (?:commercial )?market|overflow|meeting (?:space|room)|ballroom|catering|guestroom|lodging partner|preferred lodging|nearby|local market)\b/i;

const GENERIC_OUTREACH_RE =
  /\b(potential group business|may need hotels?|could have meetings?|explore synergies|contact the company to explore)\b/i;

const OUT_OF_MARKET_RE =
  /\b(?:austin|seattle|denver|chicago|miami|atlanta|dallas|boston|san francisco|los angeles|london|paris|tokyo|singapore).{0,40}(?:office|facility|campus|headquarters)\b/i;

/**
 * Extract verified demand-generating trigger from independent evidence (not invented).
 */
export function extractVerifiedCommercialTrigger(opportunity = {}) {
  const statements = collectIndependentStatements(opportunity);
  const independent = statements.join("\n");
  const matches = [];
  for (const pattern of VERIFIED_TRIGGER_PATTERNS) {
    if (pattern.re.test(independent)) {
      const hit = statements.find((s) => pattern.re.test(s)) || statements[0] || null;
      matches.push({
        code: pattern.code,
        statement: hit ? hit.slice(0, 320) : null,
      });
    }
  }
  const strippedLocal = independent.replace(
    /\bno\b[\s\S]{0,60}\b(bethesda|rockville|gaithersburg|arlington|washington|montgomery|maryland|dmv|northern virginia)\b/gi,
    " ",
  );
  const positiveLocal =
    /\b(bethesda|rockville|gaithersburg|arlington|washington|montgomery|maryland|dmv|northern virginia)\b/i.test(
      strippedLocal,
    );
  const outOfMarket = OUT_OF_MARKET_RE.test(independent) && !positiveLocal;

  return {
    version: GDI_PROACTIVE_COMMERCIAL_INTELLIGENCE_STANDARD_V1,
    hasVerifiedTrigger: matches.length > 0 && !outOfMarket,
    triggers: matches,
    primaryTrigger: matches[0] || null,
    outOfMarketRejected: outOfMarket === true,
    independentExcerpt: independent.slice(0, 400) || null,
    note:
      "Trigger is a verified organizational activity — not a confirmed hotel requirement.",
  };
}

/**
 * Build / evaluate the five-part commercial thesis without inventing facts.
 */
export function evaluateFivePartCommercialThesis(opportunity = {}, opts = {}) {
  const demand = classifyCommercialDemandThesis(opportunity, opts.extraction || {});
  const trigger = extractVerifiedCommercialTrigger(opportunity);
  const statements = collectIndependentStatements(opportunity);
  const independent = statements.join("\n");
  const narrative = [
    opportunity.hotelDemandThesis,
    opportunity.hotelOpportunityThesis,
    opportunity.potentialHotelBusiness,
    opportunity.proposedHotelUse,
    opportunity.whyThisHotel,
    opportunity.summaryWhyHotel,
    opportunity.summaryWhyMatters,
    opportunity.recommendedNextStep,
    opportunity.recommendedAction,
    opportunity.commercialNextStep,
    opportunity.salesBrief?.potentialBusinessForThisHotel,
    opportunity.salesBrief?.whyThisHotel,
    opportunity.salesBrief?.whyReachOutNow,
  ]
    .map(text)
    .filter(Boolean)
    .join(" — ");

  const mechanismOk =
    DEMAND_MECHANISM_RE.test(independent) || DEMAND_MECHANISM_RE.test(narrative);
  const hotelFitOk =
    HOTEL_FIT_RE.test(narrative) ||
    Boolean(text(opportunity.whyThisHotel || opportunity.summaryWhyHotel));

  const contactName = text(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      opportunity.namedStakeholders?.[0]?.name ||
      "",
  );
  const contactRole = text(
    opportunity.primaryContact?.role ||
      opportunity.primaryContact?.title ||
      opportunity.contactRole ||
      opportunity.namedStakeholders?.[0]?.title ||
      "",
  );
  const relevance = text(
    [
      opportunity.primaryContact?.contactRelevanceLabel,
      opportunity.primaryContact?.whyThisContact,
      opportunity.primaryContact?.commercialClass,
      opportunity.contactRelevance,
      contactRole,
    ]
      .filter(Boolean)
      .join(" "),
  );
  const entryPointOk =
    looksLikePersonName(contactName) &&
    Boolean(contactRole) &&
    /CONTACT_RELEVANT|DIRECT_BUYER|RELEVANT_COMMERCIAL|hotel_sourcing|CONTRACTING_AUTHORITY|facilities|events?|meetings?|procurement|workplace|operations|travel|relocation|planner|venue|catering|hr|employee experience|project/i.test(
      `${relevance} ${contactRole}`,
    );

  const nextStep = text(
    opportunity.recommendedNextStep ||
      opportunity.recommendedAction ||
      opportunity.commercialNextStep ||
      opportunity.salesBrief?.whyReachOutNow ||
      "",
  );
  const outreachOk =
    nextStep.length >= 40 &&
    !GENERIC_OUTREACH_RE.test(nextStep) &&
    (DEMAND_MECHANISM_RE.test(nextStep) ||
      /reach out|approach|introduce|call|email|ask|propose|offer/i.test(nextStep));

  const demandEligible =
    demand.thesisStandard === DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND ||
    demand.thesisStandard === DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND;

  const parts = {
    [FIVE_PART_THESIS_PARTS.VERIFIED_TRIGGER]: {
      ok: trigger.hasVerifiedTrigger === true,
      detail: trigger.primaryTrigger,
    },
    [FIVE_PART_THESIS_PARTS.POTENTIAL_HOTEL_DEMAND_MECHANISM]: {
      ok: mechanismOk === true,
      detail: mechanismOk
        ? "Evidence-grounded guestroom / catering / meetings / referral mechanism present"
        : "No credible hotel-demand mechanism grounded in evidence",
    },
    [FIVE_PART_THESIS_PARTS.HOTEL_SPECIFIC_COMMERCIAL_FIT]: {
      ok: hotelFitOk === true && trigger.outOfMarketRejected !== true,
      detail: hotelFitOk
        ? "Hotel-specific fit language present"
        : "Missing hotel-specific commercial fit",
    },
    [FIVE_PART_THESIS_PARTS.VERIFIED_NAMED_COMMERCIAL_ENTRY_POINT]: {
      ok: entryPointOk === true,
      detail: entryPointOk
        ? { name: contactName, role: contactRole, class: relevance.slice(0, 120) }
        : "Missing verified named commercial entry point",
    },
    [FIVE_PART_THESIS_PARTS.ACTIONABLE_OUTREACH_THESIS]: {
      ok: outreachOk === true,
      detail: outreachOk ? nextStep.slice(0, 280) : "Missing actionable, activity-grounded outreach thesis",
    },
  };

  const gaps = Object.entries(parts)
    .filter(([, v]) => !v.ok)
    .map(([k]) => k);

  const complete =
    demandEligible &&
    gaps.length === 0 &&
    trigger.outOfMarketRejected !== true;

  const revenueType =
    opportunity.commercialDemandType ||
    attachCommercialDemandTypeClassification(opportunity).commercialDemandType ||
    null;

  const outreachSuggestion = buildActionableOutreachSuggestion(opportunity, {
    trigger,
    contactName,
    contactRole,
    revenueType,
  });

  return {
    version: GDI_PROACTIVE_COMMERCIAL_INTELLIGENCE_STANDARD_V1,
    demandConfidence: demand.thesisStandard,
    demandEligibleForQualification: demandEligible,
    mayQualifySalesReadyDemandGate: thesisStandardMayQualifySalesReady(
      demand.thesisStandard,
    ),
    complete,
    gaps,
    parts,
    trigger,
    revenueType,
    confirmedFactsVsInference: {
      confirmedFacts: statements.slice(0, 3),
      inferenceNote:
        demand.thesisStandard === DEMAND_THESIS_STANDARD.RELIABLE_INFERRED_DEMAND
          ? "Hotel demand is a reasonable inference from the verified activity — not a confirmed booking."
          : demand.thesisStandard === DEMAND_THESIS_STANDARD.CONFIRMED_DEMAND
            ? "Independent evidence states a concrete hotel-relevant requirement."
            : "Insufficient verified activity or mechanism — speculative only.",
    },
    whyReachOutNow: outreachSuggestion,
    notRequired: [...PROACTIVE_CI_NOT_REQUIRED],
    salesReadyRequires: [...SALES_READY_REQUIRES],
    philosophy:
      "Help hotel sales discover and develop business BEFORE customers decide to book — without inventing demand or presenting inference as confirmed fact.",
  };
}

export function buildActionableOutreachSuggestion(opportunity = {}, ctx = {}) {
  const org = text(opportunity.organizationName || opportunity.company);
  const hotel = text(opportunity.hotelName || "this hotel");
  const trigger =
    ctx.trigger?.primaryTrigger?.statement ||
    text(opportunity.activityHeadline || opportunity.title || opportunity.whatIsHappening);
  const name = text(ctx.contactName || opportunity.primaryContact?.name || opportunity.contactName);
  const role = text(ctx.contactRole || opportunity.primaryContact?.title || opportunity.contactRole);
  const revenue =
    ctx.revenueType ||
    opportunity.commercialDemandType ||
    "GUEST_ROOMS";

  if (!trigger || trigger.length < 24) {
    return {
      ok: false,
      whatHappened: null,
      whyItMatters: null,
      whyThisHotel: null,
      whoToApproach: null,
      whatToSay: null,
      whatToVerify: ["Need a verified organizational trigger before outreach"],
    };
  }

  const whatHappened = trigger.slice(0, 280);
  const whyItMatters =
    revenue === "LOCAL_CATERING" || revenue === "EVENT_FNB_ONLY"
      ? `This activity can create local meeting, reception, or catering demand that ${hotel} can pursue before a formal RFP appears.`
      : revenue === "BOTH" || revenue === "COMBINED_GUESTROOMS_EVENT_FNB"
        ? `This activity can create guestroom and/or meeting-catering demand that ${hotel} can develop proactively.`
        : `This activity can create guestroom / visiting-traveler demand that ${hotel} can develop before a booking decision is made.`;
  const whyThisHotel = text(
    opportunity.whyThisHotel ||
      opportunity.summaryWhyHotel ||
      opportunity.hotelOpportunityThesis,
  ).slice(0, 320);
  const whoToApproach = name
    ? `${name}${role ? ` (${role})` : ""}`
    : "Identify a verified named commercial entry point before outreach";
  const whatToSay = name
    ? `Reference ${org}'s verified activity (${whatHappened.slice(0, 120)}) and ask how ${hotel} can support visiting teams, meetings, or lodging needs tied to that initiative — without claiming an existing RFP or room block.`
    : null;
  const whatToVerify = [
    "Whether lodging or meeting needs already exist for this activity",
    "Preferred dates / volume if any planning is underway",
    "Whether another hotel or venue already holds the business",
    "Who owns travel, meetings, or facilities decisions for this initiative",
  ];

  return {
    ok: Boolean(name && whyThisHotel && whatToSay),
    whatHappened,
    whyItMatters,
    whyThisHotel,
    whoToApproach,
    whatToSay,
    whatToVerify,
    note: "Confirmed facts and reasonable inference must stay distinct in customer copy.",
  };
}

/**
 * Apply outreach thesis fields in-memory when five-part evaluation can ground them.
 * Never invents triggers or contacts.
 */
export function applyProactiveOutreachThesisInChain(opportunity = {}, opts = {}) {
  const thesis = evaluateFivePartCommercialThesis(opportunity, opts);
  const next = { ...opportunity };
  const outreach = thesis.whyReachOutNow || {};
  let applied = false;

  if (outreach.ok && outreach.whatToSay) {
    const existing = text(next.recommendedNextStep || next.recommendedAction);
    if (!existing || existing.length < 40 || GENERIC_OUTREACH_RE.test(existing) || /^Monitor\b/i.test(existing)) {
      next.recommendedNextStep = outreach.whatToSay;
      next.recommendedAction = outreach.whatToSay;
      applied = true;
    }
  }
  if (outreach.whatHappened && !text(next.whatIsHappening)) {
    next.whatIsHappening = outreach.whatHappened;
    applied = true;
  }
  if (outreach.whyItMatters && !text(next.summaryWhyMatters)) {
    next.summaryWhyMatters = outreach.whyItMatters;
    applied = true;
  }
  next.proactiveCommercialIntelligenceV1 = thesis;
  next.whyReachOutNow = outreach;
  return { opportunity: next, thesis, applied };
}

/**
 * Account Development under the proactive CI standard.
 * Requires CONFIRMED or RELIABLE_INFERRED + five-part thesis.
 * Does not elevate to Sales Ready.
 */
export function evaluateProactiveAccountDevelopment(opportunity = {}, opts = {}) {
  const thesis = evaluateFivePartCommercialThesis(opportunity, opts);
  const baseAd = evaluateAccountDevelopmentEligibility(opportunity, opts);
  const gaps = [...(baseAd.gaps || [])];

  if (!thesis.demandEligibleForQualification) {
    gaps.push("DEMAND_CONFIDENCE_SPECULATIVE_OR_NEGATIVE");
  }
  for (const g of thesis.gaps || []) {
    if (!gaps.includes(g)) gaps.push(`FIVE_PART_${g}`);
  }

  const ok =
    thesis.complete === true &&
    baseAd.accountDevelopment === true &&
    !gaps.includes("DEMAND_CONFIDENCE_SPECULATIVE_OR_NEGATIVE") &&
    !(thesis.gaps || []).length;

  return {
    version: GDI_PROACTIVE_COMMERCIAL_INTELLIGENCE_STANDARD_V1,
    ok,
    accountDevelopment: ok,
    status: ok
      ? "ACCOUNT_DEVELOPMENT"
      : thesis.demandEligibleForQualification
        ? baseAd.status || "RESEARCH_CONTINUATION"
        : "RESEARCH_CONTINUATION",
    gaps: [...new Set(gaps)],
    demandConfidence: thesis.demandConfidence,
    fivePartThesis: thesis,
    baseAccountDevelopment: baseAd,
    doesNotElevateToSalesReady: true,
    note:
      "Account Development under proactive CI — CONFIRMED or RELIABLE_INFERRED required; confirmed hotel booking is not.",
  };
}

/**
 * Publication-REVIEW readiness for Sales Ready OR complete AD bodies.
 * Never sets customerPublicationReady.
 */
export function evaluateProactivePublicationReviewReadiness(opportunity = {}, opts = {}) {
  const hotel = opts.hotel || {
    hotelId: opportunity.hotelId,
    displayName: opportunity.hotelName || "",
  };
  const eligibility =
    opts.eligibilityResult ||
    opportunity.commercialEligibilityContractV1 ||
    evaluateSharedCommercialEligibility(opportunity, { hotel, ...opts });
  const proactiveAd =
    opts.proactiveAdResult || evaluateProactiveAccountDevelopment(opportunity, opts);

  const sections = opportunity.tenSectionDrawer?.sections || null;
  const sectionCount = sections ? Object.keys(sections).length : 0;
  const completePersistedContent =
    opportunity.tenSectionDrawer?.completePersistedContent === true ||
    sectionCount >= 8;

  const salesReady = eligibility?.salesReady === true;
  const accountDevelopment = proactiveAd.accountDevelopment === true;
  const publicationReviewReady =
    completePersistedContent === true &&
    (salesReady === true || accountDevelopment === true);

  const blockers = [];
  if (!salesReady && !accountDevelopment) {
    blockers.push(
      ...(proactiveAd.gaps || []),
      ...(eligibility?.gaps || ["COMMERCIAL_ELIGIBILITY_NOT_QUALIFIED"]),
    );
  }
  if (!completePersistedContent) blockers.push("TEN_SECTION_CONTENT_INCOMPLETE");

  return {
    version: GDI_PROACTIVE_COMMERCIAL_INTELLIGENCE_STANDARD_V1,
    publicationReviewReady,
    customerPublicationReady: false,
    founderAuthorizationRequired: true,
    salesReady,
    accountDevelopment,
    demandConfidence: proactiveAd.demandConfidence || null,
    completePersistedContent,
    sectionCount,
    blockers: [...new Set(blockers)],
    note:
      "publicationReviewReady stages Sales Ready or complete Account Development bodies for founder review — never customer publication.",
  };
}

export default {
  GDI_PROACTIVE_COMMERCIAL_INTELLIGENCE_STANDARD_V1,
  DEMAND_CONFIDENCE_LEVEL,
  PROACTIVE_CI_NOT_REQUIRED,
  FIVE_PART_THESIS_PARTS,
  extractVerifiedCommercialTrigger,
  evaluateFivePartCommercialThesis,
  buildActionableOutreachSuggestion,
  applyProactiveOutreachThesisInChain,
  evaluateProactiveAccountDevelopment,
  evaluateProactivePublicationReviewReadiness,
};
