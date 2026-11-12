/**
 * GDI Full E2E — Commercial Hypothesis Development V1
 *
 * Additive ORS stage: after verified trigger + demand-mechanism investigation,
 * develop primary/secondary revenue hypotheses BEFORE forcing Guest Rooms /
 * Local Catering / Both from segment membership alone.
 *
 * Distinguishes:
 *   - proposedRevenueType (what Dealality believes could be sold)
 *   - demandConfidence (CONFIRMED / RELIABLE_INFERRED / SPECULATIVE)
 *   - confirmedCustomerRequirement (YES / NO / UNKNOWN)
 *
 * Hotel-specific geography: broad metro/"same commercial market" language is
 * NOT sufficient. Practical drive-time relevance is required for Account
 * Development / publication-review readiness.
 *
 * Confirmed-demand discovery remains first-class; CHD does not crowd it out.
 * No invented bookings, dates, volumes, or buyer authority.
 */

import {
  COMMERCIAL_DEMAND_TYPE,
} from "./commercial-demand-type-v1.js";
import {
  DEMAND_THESIS_STANDARD,
  classifyCommercialDemandThesis,
} from "./commercial-demand-thesis-standard-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";

export const GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1 =
  "gdi-commercial-hypothesis-development-v1";

export const REVENUE_HYPOTHESIS_TYPE = Object.freeze({
  GUEST_ROOMS: "GUEST_ROOMS",
  LOCAL_CATERING: "LOCAL_CATERING",
});

export const CONFIRMED_CUSTOMER_REQUIREMENT = Object.freeze({
  YES: "YES",
  NO: "NO",
  UNKNOWN: "UNKNOWN",
});

export const HOTEL_GEOGRAPHY_FIT = Object.freeze({
  STRONG_LOCAL: "STRONG_LOCAL",
  PRACTICAL_ADJACENT: "PRACTICAL_ADJACENT",
  CONTESTED_METRO: "CONTESTED_METRO",
  WEAK_OR_OUT_OF_MARKET: "WEAK_OR_OUT_OF_MARKET",
  UNKNOWN: "UNKNOWN",
});

export const HYPOTHESIS_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  NONE: "NONE",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function collectEvidenceBlob(opportunity = {}) {
  const parts = [];
  for (const item of Array.isArray(opportunity.evidence) ? opportunity.evidence : []) {
    if (!item || typeof item !== "object") continue;
    parts.push(item.statement, item.quote, item.excerpt, item.text);
  }
  for (const claim of Array.isArray(opportunity.acceptedClaims)
    ? opportunity.acceptedClaims
    : []) {
    if (!claim || typeof claim !== "object") continue;
    parts.push(claim.statement, claim.quote);
  }
  for (const source of Array.isArray(opportunity.sources) ? opportunity.sources : []) {
    if (!source || typeof source !== "object") continue;
    parts.push(source.quote, source.excerpt, source.establishes, source.title);
  }
  parts.push(
    opportunity.title,
    opportunity.programmeName,
    opportunity.whatIsHappening,
    opportunity.activityHeadline,
    opportunity.activityLocation,
    opportunity.localConnection,
    opportunity.datedGrowthEvidence,
    opportunity.evidenceQuote,
  );
  return parts.map(text).filter(Boolean).join("\n");
}

function collectNarrativeBlob(opportunity = {}) {
  return [
    opportunity.hotelOpportunityThesis,
    opportunity.hotelDemandThesis,
    opportunity.whyThisHotel,
    opportunity.summaryWhyHotel,
    opportunity.hotelFitRationale,
    opportunity.potentialHotelBusiness,
    opportunity.proposedHotelUse,
    opportunity.recommendedNextStep,
  ]
    .map(text)
    .filter(Boolean)
    .join("\n");
}

const GUESTROOM_MECHANISM_PATTERNS = Object.freeze([
  { code: "VISITING_EXECUTIVES_OR_CUSTOMERS", re: /\b(visiting|customer engagement|partner collaboration|executive (?:travel|visit)|leadership.{0,20}office)\b/i },
  { code: "PROJECT_OR_FIELD_TEAMS", re: /\b(project (?:team|deployment|personnel)|field (?:team|staff|crew)|commissioning|implementation team|technical personnel|subcontractor)\b/i },
  { code: "RELOCATION_OR_EXTENDED_STAY", re: /\b(relocation|extended stay|temporary housing|workforce relocation)\b/i },
  { code: "TRAINING_PARTICIPANTS_OVERNIGHT", re: /\b(training|onboarding|academy|cohort).{0,40}(overnight|lodging|hotel|stay|room)\b/i },
  { code: "RECRUITMENT_VISITS", re: /\b(recruitment|hiring|interview).{0,30}(visit|travel|candidate)\b|\bhiring focused on customer-facing\b/i },
  { code: "EVENT_ATTENDEE_OVERFLOW", re: /\b(?:seek(?:s|ing)? overflow|overflow hotel|attendee lodging|delegate travel|housing bureau|official (?:hotel|housing)|host hotel)\b/i },
  { code: "SPORTS_TEAM_LODGING", re: /\b(team hotel|sports.?team|tournament lodging|stay.?to.?play)\b/i },
  { code: "VENUE_REFERRAL_LODGING", re: /\b(ballroom|patio venue|private (?:mansion|venue)|weddings?|fundraisers?).{0,80}(hotel|lodging|overnight|accommodation|guest)\b|\breferral.{0,20}(hotel|lodging)\b/i },
  { code: "OFFICE_EXPANSION_TRAVEL", re: /\b(?:opened?|opening|expand(?:ed|ing)).{0,40}\boffice\b/i },
  { code: "INFRASTRUCTURE_PROJECT_TRAVEL", re: /\b(?:\$[\d.,]+\s*(?:million|billion)|energy|infrastructure|SCADA|modernization|performance period|naval research|NRL).{0,60}(project|contract|campus)?\b/i },
]);

const CATERING_MECHANISM_PATTERNS = Object.freeze([
  { code: "CORPORATE_TRAINING_OR_ONBOARDING", re: /\b((?:training|onboarding|academy|l&d|workshop).{0,40}(?:meeting|session|catering|breakfast|cohort)|multi-day (?:training|onboarding))\b/i },
  { code: "BOARD_OR_CLIENT_MEETINGS", re: /\b(board meeting|client (?:meeting|presentation)|offsite)\b/i },
  { code: "ASSOCIATION_MEMBER_GATHERINGS", re: /\b(member meetings?|chapter (?:meeting|event)|awards dinner|receptions?|annual meeting)\b/i },
  { code: "CORPORATE_ENTERTAINING_OR_RECEPTIONS", re: /\b(corporate entertain|receptions?|fundraisers?|private functions?|celebrations?)\b/i },
  { code: "PROJECT_COORDINATION_MEETINGS", re: /\b(coordination meetings?|project meetings?|kickoff)\b/i },
  { code: "LOCAL_DAY_MEETINGS", re: /\b(day meeting|local (?:meeting|meetings|training)|breakfast meetings?)\b/i },
]);

const CONFIRMED_REQUIREMENT_RE =
  /\b(room.?block|official hotel|host hotel|housing bureau|seek(?:s|ing)? overflow hotel|reservation link|book your hotel|catering contract|confirmed (?:lodging|catering|meeting))\b/i;

const CHD_TRIGGER_PATTERNS = Object.freeze([
  {
    code: "LOCAL_OFFICE_OPENING_OR_EXPANSION",
    re: /\b(?:opened?|opening|opens|expand(?:ed|ing|s)?|announced).{0,40}\b(?:office|headquarters|hq|campus|location)\b|\bnew (?:office|headquarters|hq|campus)\b/i,
  },
  {
    code: "NEW_FACILITY_OR_PROJECT",
    re: /\b(?:\$[\d.,]+\s*(?:billion|million).{0,80})?(?:manufacturing|facility|plant|infrastructure|SCADA|modernization|naval research|\bNRL\b).{0,40}(?:investment|expansion|project|contract|facility)?\b/i,
  },
  {
    code: "RECURRING_ASSOCIATION_OR_MEETING",
    re: /\b(?:annual (?:conference|meeting|summit)|member meetings?|chapter (?:meeting|event))\b/i,
  },
  {
    code: "VENUE_WITHOUT_GUESTROOMS",
    re: /\b(?:ballroom|patio venue|private (?:mansion|club|venue)|weddings?,?\s*corporate|fundraisers?)\b/i,
  },
  {
    code: "PROFESSIONAL_PLANNER",
    re: /\b(?:event planner|meeting planner|destination management|\bDMC\b)\b/i,
  },
  {
    code: "TRAINING_OR_ONBOARDING",
    re: /\b(?:training (?:program|academy|cohort)|onboarding)\b/i,
  },
  {
    code: "CONFIRMED_OR_ANNOUNCED_HOTEL_DEMAND",
    re: /\b(?:overflow hotel|room blocks?|host hotel|official hotel|housing bureau|housing call|seek(?:s|ing)? overflow|book your hotel)\b/i,
  },
  {
    code: "ANNOUNCED_EVENT_OR_PROGRAM",
    re: /\b(?:summit|conference|symposium|annual meeting|tournament).{0,60}(?:bethesda|attendees|delegates|venue)\b/i,
  },
]);

function extractTriggerLocal(opportunity = {}) {
  const independent = collectEvidenceBlob(opportunity);
  const matches = [];
  for (const pattern of CHD_TRIGGER_PATTERNS) {
    if (pattern.re.test(independent)) {
      const lines = independent.split("\n").map(text).filter(Boolean);
      const hit = lines.find((s) => pattern.re.test(s)) || lines[0] || null;
      matches.push({ code: pattern.code, statement: hit ? hit.slice(0, 320) : null });
    }
  }
  return {
    hasVerifiedTrigger: matches.length > 0,
    triggers: matches,
    primaryTrigger: matches[0] || null,
  };
}

const GENERIC_MARKET_FIT_RE =
  /\bsame commercial market|broader (?:dc|washington|metro)|washington[, ]+d\.?c\.? (?:metro|region|area)|dmv\b|metropolitan area\b/i;

const TRUNCATION_RE = /\bthe market\b|\.\.\.|…|with hiring f\b|Gaithersb\b/i;

/** Bethesda Marriott practical geography anchors. */
const STRONG_LOCAL_RE =
  /\b(bethesda|chevy chase|friendship heights|medical center|nih|walter reed|montgomery mall|westbard|bradley hills)\b/i;
const PRACTICAL_ADJACENT_RE =
  /\b(rockville|north bethesda|white flint|twinbrook|silver spring|kensington|potomac(?!\s*memorial)|gaithersburg|germantown)\b/i;
const CONTESTED_METRO_RE =
  /\b(arlington|crystal city|rosslyn|alexandria|downtown (?:dc|washington)|navy yard|capitol hill|national mall|dupont|foggy bottom|naval research laboratory|\bnrl\b|washington,\s*d\.?c\.?|washington dc)\b/i;
const WEAK_REMOTE_RE =
  /\b(austin|seattle|denver|chicago|miami|atlanta|dallas|boston|san francisco|los angeles|london|paris|tokyo|frederick(?!\s*,?\s*maryland biologics)|chesapeake bay detachment)\b/i;

function stripHotelNameMentions(blob, hotelName = "") {
  let out = String(blob || "");
  const name = text(hotelName);
  if (name) {
    out = out.replace(new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi"), " ");
  }
  // Prevent "Bethesda Marriott sits in…" from counting as activity geography.
  out = out.replace(/\bbethesda marriott\b/gi, " ");
  out = out.replace(/\bthis hotel\b/gi, " ");
  // Negated / unconfirmed Bethesda mentions are not positive local anchors.
  out = out.replace(/\bbethesda-area operating geography is not confirmed\b/gi, " ");
  out = out.replace(/\bexact bethesda[^.\\n]{0,80}not confirmed\b/gi, " ");
  out = out.replace(/\b(?:no|without|not in)\s+bethesda\b/gi, " ");
  return out;
}

function stripNegatedDemandLanguage(blob) {
  return String(blob || "")
    .replace(/\bno confirmed\b[\s\S]{0,80}\b(room.?block|rfp|hotel|lodging|catering)\b/gi, " ")
    .replace(/\bwithout\b[\s\S]{0,40}\b(room.?block|lodging|hotel)\b/gi, " ")
    .replace(/\bnot (?:a |an )?(?:confirmed|established)\b[\s\S]{0,60}\b(room.?block|rfp|lodging)\b/gi, " ");
}

/**
 * Assess practical hotel geography — broad metro is not enough.
 * Uses activity/evidence location only — never hotel-name thesis copy.
 */
export function assessHotelSpecificGeographyFit(opportunity = {}, opts = {}) {
  const hotelName = text(opts.hotelName || opportunity.hotelName || "");
  const isBethesdaTarget = /bethesda/i.test(hotelName) || opportunity.hotelId === "recLuxvwwxID7U2B8";
  const evidence = stripHotelNameMentions(collectEvidenceBlob(opportunity), hotelName);
  const narrative = stripHotelNameMentions(collectNarrativeBlob(opportunity), hotelName);
  const locationBlob = stripHotelNameMentions(
    [
      opportunity.activityLocation,
      opportunity.localConnection,
      opportunity.eventLocation,
      opportunity.eventLocationSummary,
      evidence,
    ]
      .map(text)
      .join("\n"),
    hotelName,
  );

  const onlyGenericMarketFit =
    GENERIC_MARKET_FIT_RE.test(narrative) &&
    !STRONG_LOCAL_RE.test(locationBlob) &&
    !PRACTICAL_ADJACENT_RE.test(locationBlob);

  if (!isBethesdaTarget) {
    // Hotel-agnostic fallback: require non-generic location language.
    if (WEAK_REMOTE_RE.test(locationBlob) && !STRONG_LOCAL_RE.test(locationBlob) && !PRACTICAL_ADJACENT_RE.test(locationBlob)) {
      return {
        version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
        tier: HOTEL_GEOGRAPHY_FIT.WEAK_OR_OUT_OF_MARKET,
        okForPublicationReview: false,
        okForAccountDevelopment: false,
        reason: "activity_geography_weak_or_out_of_market",
        onlyGenericMarketFit,
      };
    }
    if (onlyGenericMarketFit) {
      return {
        version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
        tier: HOTEL_GEOGRAPHY_FIT.CONTESTED_METRO,
        okForPublicationReview: false,
        okForAccountDevelopment: false,
        reason: "generic_metro_fit_insufficient",
        onlyGenericMarketFit: true,
      };
    }
    return {
      version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
      tier: HOTEL_GEOGRAPHY_FIT.PRACTICAL_ADJACENT,
      okForPublicationReview: true,
      okForAccountDevelopment: true,
      reason: "non_bethesda_hotel_fit_deferred_to_existing_gates",
      onlyGenericMarketFit: false,
    };
  }

  if (STRONG_LOCAL_RE.test(locationBlob)) {
    return {
      version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
      tier: HOTEL_GEOGRAPHY_FIT.STRONG_LOCAL,
      okForPublicationReview: true,
      okForAccountDevelopment: true,
      reason: "activity_anchored_in_bethesda_corridor",
      onlyGenericMarketFit: false,
    };
  }

  if (PRACTICAL_ADJACENT_RE.test(locationBlob)) {
    // Practical adjacent is AD-ok; publication-review still requires non-generic thesis.
    const generic = onlyGenericMarketFit || GENERIC_MARKET_FIT_RE.test(narrative);
    return {
      version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
      tier: HOTEL_GEOGRAPHY_FIT.PRACTICAL_ADJACENT,
      okForPublicationReview: generic ? false : true,
      okForAccountDevelopment: true,
      reason: generic
        ? "adjacent_geography_but_generic_market_copy"
        : "practical_drive_time_adjacent_to_bethesda",
      onlyGenericMarketFit: generic,
    };
  }

  if (CONTESTED_METRO_RE.test(locationBlob)) {
    return {
      version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
      tier: HOTEL_GEOGRAPHY_FIT.CONTESTED_METRO,
      okForPublicationReview: false,
      okForAccountDevelopment: false,
      reason:
        "contested_metro_geography_without_bethesda_side_practical_fit — Arlington/Downtown DC/NRL alone is not automatic Bethesda Marriott demand",
      onlyGenericMarketFit: onlyGenericMarketFit || GENERIC_MARKET_FIT_RE.test(narrative),
      researchQuestions: [
        "Where will visiting personnel actually stay overnight relative to the activity site?",
        "Is there a Bethesda/Montgomery County operating footprint beyond the metro label?",
        "Why would travelers choose Bethesda Marriott over hotels nearer the activity site?",
      ],
    };
  }

  if (WEAK_REMOTE_RE.test(locationBlob) || onlyGenericMarketFit) {
    return {
      version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
      tier: HOTEL_GEOGRAPHY_FIT.WEAK_OR_OUT_OF_MARKET,
      okForPublicationReview: false,
      okForAccountDevelopment: false,
      reason: onlyGenericMarketFit
        ? "generic_same_commercial_market_language_insufficient"
        : "activity_geography_weak_or_out_of_market",
      onlyGenericMarketFit,
    };
  }

  return {
    version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
    tier: HOTEL_GEOGRAPHY_FIT.UNKNOWN,
    okForPublicationReview: false,
    okForAccountDevelopment: false,
    reason: "activity_geography_unresolved",
    onlyGenericMarketFit: false,
    researchQuestions: [
      "Where is the demand-generating activity physically located?",
      "Is the location within practical drive-time of this hotel?",
    ],
  };
}

function matchMechanisms(patterns, blob) {
  const hits = [];
  for (const p of patterns) {
    if (p.re.test(blob)) hits.push(p.code);
  }
  return hits;
}

function evidenceRefs(opportunity = {}, limit = 3) {
  const refs = [];
  for (const item of Array.isArray(opportunity.evidence) ? opportunity.evidence : []) {
    if (!item?.url && !item?.statement) continue;
    refs.push({
      url: item.url || null,
      statement: text(item.statement || item.quote).slice(0, 220) || null,
    });
    if (refs.length >= limit) break;
  }
  return refs;
}

/**
 * Build a single revenue hypothesis object (or null if not credible).
 */
export function buildRevenueHypothesis(opportunity = {}, type, opts = {}) {
  const evidence = stripNegatedDemandLanguage(collectEvidenceBlob(opportunity));
  // Prefer independent evidence for mechanisms; narrative alone can inflate false hits.
  const narrative = stripNegatedDemandLanguage(
    stripHotelNameMentions(collectNarrativeBlob(opportunity), opportunity.hotelName),
  );
  const blob = evidence.length >= 40 ? evidence : `${evidence}\n${narrative}`;
  const trigger = opts.trigger || extractTriggerLocal(opportunity);
  const patterns =
    type === REVENUE_HYPOTHESIS_TYPE.LOCAL_CATERING
      ? CATERING_MECHANISM_PATTERNS
      : GUESTROOM_MECHANISM_PATTERNS;
  const mechanisms = matchMechanisms(patterns, blob);
  if (!mechanisms.length) {
    return null;
  }

  // Office expansion alone supports guestroom travel hypothesis; catering needs extra signal.
  if (
    type === REVENUE_HYPOTHESIS_TYPE.LOCAL_CATERING &&
    mechanisms.length === 1 &&
    mechanisms[0] === "CORPORATE_TRAINING_OR_ONBOARDING" &&
    !/\b(training|onboarding|meeting|workshop|reception|dinner)\b/i.test(evidence)
  ) {
    // Weak narrative-only training mention — treat as speculative secondary only if caller asks.
    if (opts.allowSpeculative !== true) return null;
  }

  const geo = opts.geography || assessHotelSpecificGeographyFit(opportunity, opts);
  const demand = opts.demand || classifyCommercialDemandThesis(opportunity);
  const confirmedReq = CONFIRMED_REQUIREMENT_RE.test(evidence)
    ? CONFIRMED_CUSTOMER_REQUIREMENT.YES
    : CONFIRMED_CUSTOMER_REQUIREMENT.UNKNOWN;

  let confidence = HYPOTHESIS_CONFIDENCE.MEDIUM;
  if (confirmedReq === CONFIRMED_CUSTOMER_REQUIREMENT.YES) {
    confidence = HYPOTHESIS_CONFIDENCE.HIGH;
  } else if (
    demand.thesisStandard === DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND ||
    !geo.okForAccountDevelopment
  ) {
    confidence = HYPOTHESIS_CONFIDENCE.LOW;
  }

  const hotelCompeteReason = text(
    opportunity.whyThisHotel || opportunity.hotelOpportunityThesis || "",
  ).slice(0, 320);

  const qualificationQuestions =
    type === REVENUE_HYPOTHESIS_TYPE.LOCAL_CATERING
      ? [
          "Does this activity create local meetings, training, receptions, or catering needs?",
          "Who owns meeting/catering decisions for this initiative?",
          "Are dates, headcount, or preferred venues already under discussion?",
        ]
      : [
          "Will this activity generate overnight stays for visitors, project teams, or recruits?",
          "Where do travelers typically stay relative to the activity site?",
          "Is there an existing preferred hotel, volume commitment, or travel policy constraint?",
          "What volume and timing would make a Bethesda Marriott relationship useful?",
        ];

  return {
    revenueType: type,
    demandMechanism: mechanisms,
    verifiedTrigger: trigger.primaryTrigger || null,
    evidenceReferences: evidenceRefs(opportunity),
    hotelCompeteReason: hotelCompeteReason || null,
    confidence,
    uncertainties: [
      confirmedReq === CONFIRMED_CUSTOMER_REQUIREMENT.UNKNOWN
        ? "Actual customer lodging/catering requirement not independently confirmed"
        : null,
      !geo.okForAccountDevelopment
        ? `Hotel geography fit: ${geo.reason}`
        : null,
      ...(geo.researchQuestions || []),
    ].filter(Boolean),
    qualificationQuestions,
    confirmedCustomerRequirement: confirmedReq,
  };
}

/**
 * Develop primary + optional secondary commercial hypotheses.
 */
export function developCommercialHypotheses(opportunity = {}, opts = {}) {
  const trigger = opts.trigger || extractTriggerLocal(opportunity);
  const demand = classifyCommercialDemandThesis(opportunity, opts.extraction || {});
  const geography = assessHotelSpecificGeographyFit(opportunity, opts);
  const evidence = collectEvidenceBlob(opportunity);

  const guestRooms = buildRevenueHypothesis(opportunity, REVENUE_HYPOTHESIS_TYPE.GUEST_ROOMS, {
    trigger,
    demand,
    geography,
    ...opts,
  });
  const catering = buildRevenueHypothesis(opportunity, REVENUE_HYPOTHESIS_TYPE.LOCAL_CATERING, {
    trigger,
    demand,
    geography,
    ...opts,
  });

  // Prefer segment-aware ordering without forcing BOTH.
  const segment = text(opportunity.researchSegment || opportunity.demandType);
  let primary = null;
  let secondary = null;

  if (/local_catering|social_special|corporate_celebrations/i.test(segment) && catering) {
    primary = catering;
    secondary = guestRooms;
  } else if (/preferred_lodging/i.test(segment) && guestRooms) {
    primary = guestRooms;
    // Catering only if independently supported beyond venue entertaining language.
    secondary =
      catering &&
      catering.demandMechanism.some((m) =>
        /ASSOCIATION|BOARD|TRAINING|LOCAL_DAY|PROJECT_COORDINATION/i.test(m),
      )
        ? catering
        : null;
  } else if (guestRooms && catering) {
    primary = guestRooms;
    // Secondary catering only when evidence (not segment) supports it beyond office-opening boilerplate.
    const cateringOnEvidence = matchMechanisms(CATERING_MECHANISM_PATTERNS, evidence);
    secondary = cateringOnEvidence.length ? catering : null;
  } else {
    primary = guestRooms || catering;
    secondary = null;
  }

  // BOTH only when both hypotheses are independently credible (not speculative add-ons).
  const bothOk =
    primary &&
    secondary &&
    primary.confidence !== HYPOTHESIS_CONFIDENCE.NONE &&
    secondary.confidence !== HYPOTHESIS_CONFIDENCE.NONE &&
    secondary.demandMechanism?.length > 0;

  let proposedRevenueType = COMMERCIAL_DEMAND_TYPE.UNDETERMINED;
  if (bothOk) proposedRevenueType = COMMERCIAL_DEMAND_TYPE.BOTH;
  else if (primary?.revenueType === REVENUE_HYPOTHESIS_TYPE.GUEST_ROOMS) {
    proposedRevenueType = COMMERCIAL_DEMAND_TYPE.GUEST_ROOMS;
  } else if (primary?.revenueType === REVENUE_HYPOTHESIS_TYPE.LOCAL_CATERING) {
    proposedRevenueType = COMMERCIAL_DEMAND_TYPE.LOCAL_CATERING;
  }

  const confirmedCustomerRequirement =
    primary?.confirmedCustomerRequirement === CONFIRMED_CUSTOMER_REQUIREMENT.YES ||
    secondary?.confirmedCustomerRequirement === CONFIRMED_CUSTOMER_REQUIREMENT.YES
      ? CONFIRMED_CUSTOMER_REQUIREMENT.YES
      : primary
        ? CONFIRMED_CUSTOMER_REQUIREMENT.UNKNOWN
        : CONFIRMED_CUSTOMER_REQUIREMENT.NO;

  const hasCrediblePrimary =
    Boolean(primary) &&
    demand.thesisStandard !== DEMAND_THESIS_STANDARD.SPECULATIVE_DEMAND &&
    trigger.hasVerifiedTrigger === true;

  // Secondary uncertainty must NOT block a supported primary.
  const primaryBlocks =
    !hasCrediblePrimary
      ? ["NO_CREDIBLE_PRIMARY_REVENUE_HYPOTHESIS"]
      : [];
  if (!geography.okForAccountDevelopment) {
    primaryBlocks.push("HOTEL_SPECIFIC_GEOGRAPHY_NOT_DEFENSIBLE");
  }

  const targetedResearchQuestions = [
    ...(geography.researchQuestions || []),
    ...(primary?.uncertainties || []).filter((u) => /Where|Is there|Will this/i.test(u)),
    "Who coordinates travel and meetings for this activity?",
    "Has a competitor hotel or venue already captured related demand?",
  ].filter((v, i, a) => a.indexOf(v) === i);

  const contactName = text(
    opportunity.primaryContact?.name || opportunity.contactName || "",
  );
  const contactRole = text(
    opportunity.primaryContact?.role ||
      opportunity.primaryContact?.title ||
      opportunity.contactRole ||
      "",
  );

  const salesCopy = buildHypothesisSalesRecommendation(opportunity, {
    trigger,
    primary,
    secondary,
    geography,
    proposedRevenueType,
    contactName,
    contactRole,
  });

  return {
    version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
    demandConfidence: demand.thesisStandard,
    proposedRevenueType,
    revenueConfidence: demand.thesisStandard,
    confirmedCustomerRequirement,
    primaryHypothesis: primary,
    secondaryHypothesis: bothOk ? secondary : secondary && hasCrediblePrimary ? {
      ...secondary,
      confidence: HYPOTHESIS_CONFIDENCE.LOW,
      note: "Secondary hypothesis retained as uncertain — does not block primary.",
    } : null,
    bothIndependentlySupported: bothOk === true,
    geography,
    hasCrediblePrimary,
    blocksPublicationReview: primaryBlocks,
    targetedResearchQuestions,
    salesRecommendation: salesCopy,
    truncatedOrGenericCopy:
      TRUNCATION_RE.test(collectNarrativeBlob(opportunity)) ||
      GENERIC_MARKET_FIT_RE.test(collectNarrativeBlob(opportunity)),
    philosophy:
      "Develop defensible hotel-sales hypotheses from verified business activity before customers issue RFPs — without inventing demand or treating broad metro labels as hotel fit.",
  };
}

export function buildHypothesisSalesRecommendation(opportunity = {}, ctx = {}) {
  const org = text(opportunity.organizationName || opportunity.company);
  const hotel = text(opportunity.hotelName || "this hotel");
  const triggerStmt =
    ctx.trigger?.primaryTrigger?.statement ||
    text(opportunity.activityHeadline || opportunity.title || opportunity.whatIsHappening);
  const primary = ctx.primary;
  const secondary = ctx.secondary;
  const geo = ctx.geography;

  const whatHappened = triggerStmt ? triggerStmt.slice(0, 280) : null;
  const whyHotelBusiness = primary
    ? `Primary hypothesis: ${primary.revenueType.replace(/_/g, " ").toLowerCase()} via ${primary.demandMechanism
        .slice(0, 2)
        .join(", ")
        .toLowerCase()
        .replace(/_/g, " ")}.${
        secondary
          ? ` Secondary (uncertain): ${secondary.revenueType.replace(/_/g, " ").toLowerCase()}.`
          : ""
      }`
    : null;

  const whyThisHotel =
    geo && !geo.okForAccountDevelopment
      ? `Hotel-specific fit is not yet defensible: ${geo.reason}. Do not claim ${hotel} is the natural overnight or catering choice based only on a broad metro connection.`
      : text(opportunity.whyThisHotel || opportunity.hotelOpportunityThesis).slice(0, 320);

  const who =
    looksLikePersonName(ctx.contactName)
      ? `${ctx.contactName}${ctx.contactRole ? ` (${ctx.contactRole})` : ""}`
      : null;

  const whatToSay =
    whatHappened && who && geo?.okForAccountDevelopment
      ? `Following ${org}'s verified activity (${whatHappened.slice(0, 140)}), contact ${who} to qualify whether visiting teams, recruitment, or related travel could create recurring accommodation${
          primary?.revenueType === REVENUE_HYPOTHESIS_TYPE.LOCAL_CATERING ||
          ctx.proposedRevenueType === COMMERCIAL_DEMAND_TYPE.BOTH
            ? " or meeting/catering"
            : ""
        } needs near ${hotel} — without claiming an existing RFP or room block.`
      : whatHappened && who
        ? `Acknowledge ${org}'s verified activity (${whatHappened.slice(0, 120)}) and ask ${who} where visiting personnel stay and whether any Montgomery County / Bethesda-side operating footprint exists before pitching ${hotel}.`
        : null;

  const whatToVerify = [
    ...(primary?.qualificationQuestions || []),
    ...(geo?.researchQuestions || []),
  ].filter((v, i, a) => a.indexOf(v) === i);

  return {
    whatHappened,
    whyItCouldGenerateHotelBusiness: whyHotelBusiness,
    whyThisHotel,
    whoToApproach: who,
    whyReachOutNow: whatHappened
      ? `The verified trigger is current commercial news — use it to open a qualification conversation before a competitor captures the relationship.`
      : null,
    whatToSay,
    whatToVerify,
    confirmedFactsVsInference: {
      confirmedFacts: [whatHappened].filter(Boolean),
      inference:
        "Guestroom/catering demand is a commercial hypothesis until independently confirmed by the buyer.",
    },
  };
}

/**
 * Apply CHD onto an opportunity clone (in-memory). Sets proposed revenue fields
 * without inventing confirmed requirements. Does not write stores.
 */
export function applyCommercialHypothesisDevelopmentInChain(opportunity = {}, opts = {}) {
  const hyp = developCommercialHypotheses(opportunity, opts);
  const next = { ...opportunity };

  next.commercialHypothesisDevelopmentV1 = hyp;
  next.proposedRevenueType = hyp.proposedRevenueType;
  next.revenueConfidence = hyp.revenueConfidence;
  next.confirmedCustomerRequirement = hyp.confirmedCustomerRequirement;
  next.primaryRevenueHypothesis = hyp.primaryHypothesis;
  next.secondaryRevenueHypothesis = hyp.secondaryHypothesis;
  next.hotelGeographyFitV1 = hyp.geography;
  next.targetedHypothesisResearchQuestions = hyp.targetedResearchQuestions;
  next.hypothesisSalesRecommendation = hyp.salesRecommendation;

  // Do not leave UNDETERMINED when a credible proposed primary exists.
  if (
    hyp.proposedRevenueType &&
    hyp.proposedRevenueType !== COMMERCIAL_DEMAND_TYPE.UNDETERMINED &&
    (!next.commercialDemandType ||
      next.commercialDemandType === COMMERCIAL_DEMAND_TYPE.UNDETERMINED)
  ) {
    next.commercialDemandType = hyp.proposedRevenueType;
    next.commercialDemandTypeSource = "commercial_hypothesis_development_v1";
  }

  // Replace generic / truncated outreach when CHD can ground a better recommendation.
  const existing = text(next.recommendedNextStep || next.recommendedAction);
  const improved = text(hyp.salesRecommendation?.whatToSay);
  if (
    improved &&
    (!existing ||
      existing.length < 40 ||
      /^Monitor\b/i.test(existing) ||
      /explore potential|same commercial market|the market/i.test(existing) ||
      TRUNCATION_RE.test(existing))
  ) {
    next.recommendedNextStep = improved;
    next.recommendedAction = improved;
  }

  // Surface geography honesty for downstream gates.
  if (!hyp.geography.okForAccountDevelopment) {
    next.hotelFitHonesty =
      next.hotelFitHonesty ||
      (hyp.geography.tier === HOTEL_GEOGRAPHY_FIT.WEAK_OR_OUT_OF_MARKET
        ? "OUT_OF_MARKET"
        : "CONDITIONAL_STRETCH");
    next.hotelGeographyBlockReason = hyp.geography.reason;
  }

  if (hyp.salesRecommendation?.whyThisHotel && hyp.geography.okForAccountDevelopment) {
    // Keep existing non-generic whyThisHotel; do not overwrite strong local copy.
  } else if (!hyp.geography.okForAccountDevelopment) {
    next.summaryWhyHotel = hyp.salesRecommendation?.whyThisHotel || next.summaryWhyHotel;
  }

  return {
    opportunity: next,
    hypothesis: hyp,
    applied: true,
  };
}

/**
 * CHD-aware publication-review gate additions.
 */
export function evaluateHypothesisPublicationGates(opportunity = {}, opts = {}) {
  const hyp =
    opportunity.commercialHypothesisDevelopmentV1 ||
    developCommercialHypotheses(opportunity, opts);
  const blockers = [...(hyp.blocksPublicationReview || [])];

  if (hyp.truncatedOrGenericCopy) {
    blockers.push("GENERIC_OR_TRUNCATED_CUSTOMER_COPY");
  }
  if (!hyp.hasCrediblePrimary) {
    blockers.push("NO_CREDIBLE_PRIMARY_REVENUE_HYPOTHESIS");
  }
  if (!hyp.geography?.okForPublicationReview) {
    blockers.push("HOTEL_SPECIFIC_GEOGRAPHY_NOT_DEFENSIBLE");
  }
  if (
    hyp.proposedRevenueType === COMMERCIAL_DEMAND_TYPE.BOTH &&
    !hyp.bothIndependentlySupported
  ) {
    blockers.push("BOTH_WITHOUT_INDEPENDENT_SECONDARY_THESIS");
  }

  return {
    version: GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
    ok: blockers.length === 0,
    blockers: [...new Set(blockers)],
    proposedRevenueType: hyp.proposedRevenueType,
    confirmedCustomerRequirement: hyp.confirmedCustomerRequirement,
    demandConfidence: hyp.demandConfidence,
    geographyTier: hyp.geography?.tier || null,
  };
}

export default {
  GDI_COMMERCIAL_HYPOTHESIS_DEVELOPMENT_V1,
  REVENUE_HYPOTHESIS_TYPE,
  CONFIRMED_CUSTOMER_REQUIREMENT,
  HOTEL_GEOGRAPHY_FIT,
  HYPOTHESIS_CONFIDENCE,
  assessHotelSpecificGeographyFit,
  buildRevenueHypothesis,
  developCommercialHypotheses,
  buildHypothesisSalesRecommendation,
  applyCommercialHypothesisDevelopmentInChain,
  evaluateHypothesisPublicationGates,
};
