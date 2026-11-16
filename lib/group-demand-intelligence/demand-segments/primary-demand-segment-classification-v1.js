/**
 * Primary demand-segment classification V1 — mechanism-first Full E2E guard.
 *
 * One opportunity → one defensible primary demand segment.
 * Scaling Companies (corporate_accounts_scaling) requires an evidence-backed
 * corporate growth/expansion trigger with a credible hotel-demand thesis.
 * Association conferences, trade shows, and generic hiring alone do not qualify.
 *
 * Does not invent Airtable fields. Does not rewrite historical research text —
 * attaches classificationDecision metadata for audit.
 */

import {
  DEMAND_SEGMENT,
  resolveDemandSegment,
} from "./eight-demand-segment-registry-v1.js";

export const PRIMARY_DEMAND_SEGMENT_CLASSIFICATION_V1 =
  "gdi-primary-demand-segment-classification-v1";

const ASSOC = DEMAND_SEGMENT.ASSOCIATIONS_CONFERENCES_EVENTS.id;
const SCALING = DEMAND_SEGMENT.CORPORATE_ACCOUNTS_SCALING.id;
const MEETINGS = DEMAND_SEGMENT.CORPORATE_MEETINGS_INCENTIVES.id;
const EDU = DEMAND_SEGMENT.EDUCATION_SCIENTIFIC.id;
const CATERING = DEMAND_SEGMENT.LOCAL_CATERING_DAY_MEETINGS.id;
const PLANNER = "professional_event_planners";
const TRADE = DEMAND_SEGMENT.TRAVEL_TRADE_GROUP_INTERMEDIARIES.id;
const PREFERRED = "preferred_lodging_partnership";

/** Filter keys aligned to opportunity-type-filter-v1 (no circular import). */
const SEGMENT_TO_FILTER = Object.freeze({
  [ASSOC]: "ASSOCIATION_CONFERENCE_GROUP",
  [SCALING]: "SCALING_COMPANY",
  [MEETINGS]: "CORPORATE_MEETINGS_INCENTIVES",
  [EDU]: "EDUCATION_SCIENTIFIC",
  [CATERING]: "LOCAL_CATERING_DAY_MEETINGS",
  [TRADE]: "TRAVEL_TRADE_GROUP_INTERMEDIARIES",
  corporate_celebrations_private_events:
    "CORPORATE_CELEBRATIONS_PRIVATE_EVENTS",
  government_public_sector: "GOVERNMENT_PUBLIC_SECTOR",
  sports_entertainment: "SPORTS_ENTERTAINMENT",
  projects_infrastructure: "PROJECTS_INFRASTRUCTURE",
  social_special_events: "SOCIAL_SPECIAL_EVENTS",
  relocation_extended_stay: "RELOCATION_EXTENDED_STAY",
  [PREFERRED]: "PREFERRED_LODGING_PARTNERSHIP",
  [PLANNER]: "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP",
});

const SEGMENT_LABEL = Object.freeze({
  [ASSOC]: "Association / Conference / Events",
  [SCALING]: "Scaling Company",
  [MEETINGS]: "Corporate Meetings & Incentives",
  [EDU]: "Education & Scientific",
  [CATERING]: "Local Catering & Day Meetings",
  [TRADE]: "Travel Trade & Group Intermediaries",
  [PREFERRED]: "Preferred Lodging Partnership",
  [PLANNER]: "Professional Event Planner Partnership",
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

function blobOf(opp = {}) {
  return [
    opp.displayTitle,
    opp.title,
    opp.eventName,
    opp.organizationName,
    opp.displayCompanyName,
    opp.whatIsHappening,
    opp.summaryWhat,
    opp.potentialHotelBusiness,
    opp.potentialBusiness,
    opp.whyThisHotel,
    opp.whyActNow,
    opp.hotelDemandThesis,
    opp.roomDemandRationale,
    opp.demandMechanism,
    opp.segmentDisplayLabel,
    opp.opportunityType,
    opp.researchSegment,
  ]
    .map(text)
    .filter(Boolean)
    .join("\n")
    .toLowerCase();
}

/** Named organized gatherings that are not corporate expansion accounts. */
export function looksLikeOrganizedConferenceOrShow(opp = {}) {
  const title = text(opp.displayTitle || opp.title || opp.eventName);
  const blob = blobOf(opp);
  if (
    /\b(?:congress|conference|symposium|summit|forum|expo|exhibition|trade\s*show|annual\s+meeting|biennial\s+meeting|convention|colloquium|workshop\s+series|model\s+united\s+nations|\bmun\b)\b/i.test(
      title,
    )
  ) {
    return true;
  }
  if (
    /\b(?:association|society|chamber of|federation|institute)\b/i.test(blob) &&
    /\b(?:conference|congress|symposium|annual meeting|convention|expo)\b/i.test(
      blob,
    )
  ) {
    return true;
  }
  if (
    /\b(?:trade\s*show|design show|boat show|consumer show|public show)\b/i.test(
      blob,
    )
  ) {
    return true;
  }
  return false;
}

/**
 * Evidence-backed corporate growth / expansion trigger with hotel demand thesis.
 * Hiring announcements or "company is growing" alone are insufficient.
 */
export function hasCredibleScalingExpansionTrigger(opp = {}) {
  const blob = blobOf(opp);
  const expansion =
    /\b(?:expand(?:s|ing|ed)? into|new (?:regional |national )?headquarters|hq (?:open|launch)|facility (?:open|opening|commission)|commissioning|new (?:plant|office|campus|market entry)|market entry|integration program|post[- ]merger|visiting (?:project|commissioning|construction) team|employee mobilization|international assignment|operational launch|new (?:store|site) rollout|corporate (?:expansion|scale[- ]?up)|emerging corporate account)\b/i.test(
      blob,
    );
  const hotelThesis =
    /\b(?:room(?:s| block| nights)?|lodging|hotel|accommodation|extended[- ]stay|visiting (?:staff|teams?|crew|engineers)|project travel|overnight)\b/i.test(
      blob,
    );
  const hiringOnly =
    /\b(?:hiring|recruiting|headcount|job opening|we're growing|we are growing|company growth)\b/i.test(
      blob,
    ) &&
    !expansion &&
    !/\b(?:visiting|room block|project team|mobilization|headquarters|facility open)\b/i.test(
      blob,
    );
  if (hiringOnly) return false;
  return expansion && hotelThesis;
}

export function evaluateScalingCompaniesEligibility(opp = {}) {
  if (looksLikeOrganizedConferenceOrShow(opp)) {
    return {
      ok: false,
      code: "ORGANIZED_CONFERENCE_OR_SHOW_NOT_SCALING",
      message:
        "Organized conferences, association conventions, exhibitions, and trade shows are not Scaling Companies demand.",
      suggestedSegmentId: ASSOC,
    };
  }
  if (!hasCredibleScalingExpansionTrigger(opp)) {
    return {
      ok: false,
      code: "MISSING_EXPANSION_HOTEL_DEMAND_TRIGGER",
      message:
        "Scaling Companies requires an evidence-backed growth/expansion trigger and a credible hotel-demand thesis. Generic hiring or growth claims are insufficient.",
      suggestedSegmentId: null,
    };
  }
  return {
    ok: true,
    code: "SCALING_ELIGIBLE",
    message:
      "Corporate growth/expansion trigger with hotel-demand thesis evidenced.",
    suggestedSegmentId: SCALING,
  };
}

function priorSegmentId(opp = {}) {
  const raw =
    opp.demandSegmentId ||
    opp.customerDemandSegmentId ||
    opp.opportunityType ||
    opp.researchSegment ||
    opp.segment ||
    "";
  const resolved = resolveDemandSegment(raw);
  if (resolved?.id) return resolved.id;
  const t = text(raw).toLowerCase();
  if (t === "scaling_companies" || t.includes("scaling")) return SCALING;
  if (t === "association_events" || t.includes("assoc")) return ASSOC;
  if (t === PLANNER || t.includes("professional_event_planner")) return PLANNER;
  if (t.includes("preferred_lodging")) return PREFERRED;
  if (t.includes("travel_trade")) return TRADE;
  if (t.includes("local_catering") || t.includes("local_meetings")) {
    return CATERING;
  }
  if (t.includes("education") || t.includes("scientific")) return EDU;
  if (t.includes("corporate_meetings")) return MEETINGS;
  return null;
}

/**
 * Resolve one primary demand segment for Full E2E + publication + UI.
 */
export function resolvePrimaryDemandSegment(opp = {}) {
  const prior = priorSegmentId(opp);
  const conference = looksLikeOrganizedConferenceOrShow(opp);
  const scalingGate = evaluateScalingCompaniesEligibility(opp);

  // Explicit preferred lodging / planner subtypes win when evidenced.
  const subtype = text(
    opp.demandOpportunitySubtype || opp.opportunityType || "",
  ).toUpperCase();
  if (
    subtype === "PREFERRED_LODGING_PARTNERSHIP" ||
    opp.preferredLodgingPartnershipV1 === true
  ) {
    if (
      /\b(?:preferred|recurring|corporate travel|rooming list|rate agreement|lodging program)\b/i.test(
        blobOf(opp),
      )
    ) {
      return decision(PREFERRED, "preferred_lodging_evidenced", prior);
    }
  }
  if (
    subtype === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
    opp.professionalEventPlannerPartnershipV1 === true ||
    prior === PLANNER
  ) {
    // Conference organized by a PCO/Acavent-style firm is still a conference,
    // not a planner-account partnership, unless a managed group program is evidenced.
    const blob = blobOf(opp);
    // Require affirmative planner-program evidence — ignore negated phrases
    // like "no evidenced planner-managed housing program".
    const plannerProgram =
      /\b(?:planner[- ]managed|destination management|\bdmc\b|managed group program|housing bureau|official housing)\b/i.test(
        blob,
      ) &&
      !/\b(?:no |not |without |lacks? )(?:evidenced |evidence of )?(?:planner[- ]managed|managed group|official housing|housing bureau|dmc)\b/i.test(
        blob,
      );
    if (conference && !plannerProgram) {
      return decision(ASSOC, "conference_not_planner_partnership", prior);
    }
    if (
      !plannerProgram &&
      !/\b(?:\bdmc\b|housing desk|destination management)\b/i.test(blob)
    ) {
      return decision(
        conference ? ASSOC : prior || ASSOC,
        "planner_listing_without_group_program",
        prior,
      );
    }
  }

  if (prior === SCALING || /scaling/i.test(text(opp.opportunityType))) {
    if (!scalingGate.ok) {
      if (scalingGate.suggestedSegmentId || conference) {
        return decision(
          scalingGate.suggestedSegmentId || ASSOC,
          scalingGate.code,
          prior,
          scalingGate,
        );
      }
      // Invalid scaling claim without an alternate primary — leave unresolved
      // rather than preserving the incorrect Scaling label.
      return {
        version: PRIMARY_DEMAND_SEGMENT_CLASSIFICATION_V1,
        demandSegmentId: null,
        opportunityTypeFilterCategory: null,
        segmentDisplayLabel: null,
        reason: scalingGate.code,
        priorSegmentId: prior,
        scalingEligibility: scalingGate,
        changed: true,
      };
    }
    return decision(SCALING, "scaling_eligible", prior, scalingGate);
  }

  if (conference && (prior === TRADE || prior === MEETINGS || prior === EDU)) {
    // Keep education_scientific when the title/org is clearly academic CPD,
    // otherwise association/conference is the primary demand source.
    if (
      prior === EDU &&
      /\b(?:university|faculty|scientific|colloquium|cpd|medical education)\b/i.test(
        blobOf(opp),
      )
    ) {
      return decision(EDU, "education_scientific_conference", prior);
    }
    if (
      prior === TRADE &&
      !/\b(?:tour operator|dmc|travel advisor|wholesaler|incentive house)\b/i.test(
        blobOf(opp),
      )
    ) {
      return decision(ASSOC, "conference_not_travel_trade", prior);
    }
  }

  if (prior === CATERING) {
    // Do not let the prior segment label itself satisfy catering evidence.
    const evidenceBlob = [
      opp.whatIsHappening,
      opp.summaryWhat,
      opp.potentialHotelBusiness,
      opp.potentialBusiness,
      opp.whyThisHotel,
    ]
      .map(text)
      .join("\n")
      .toLowerCase();
    const cateringDemand =
      /\b(?:catering|day[- ]meeting|meeting room|banquet|coffee break|local attendees|day[- ]use|no overnight|guestroomRequired false)\b/i.test(
        evidenceBlob,
      );
    // Named multi-day association conferences are not Local Catering primary.
    if (conference && !cateringDemand) {
      return decision(ASSOC, "conference_not_local_catering", prior);
    }
    if (
      conference &&
      /\b(?:biennial conference|annual conference|congress|symposium)\b/i.test(
        text(opp.displayTitle || opp.title),
      )
    ) {
      return decision(ASSOC, "conference_not_local_catering", prior);
    }
  }

  if (prior) {
    if (prior === SCALING && !scalingGate.ok) {
      return decision(
        scalingGate.suggestedSegmentId || ASSOC,
        scalingGate.code,
        prior,
        scalingGate,
      );
    }
    return decision(prior, "preserve_prior_primary", prior, scalingGate);
  }

  if (conference) {
    return decision(ASSOC, "conference_default", prior);
  }
  if (scalingGate.ok) {
    return decision(SCALING, "scaling_eligible_inferred", prior, scalingGate);
  }

  return {
    version: PRIMARY_DEMAND_SEGMENT_CLASSIFICATION_V1,
    demandSegmentId: null,
    opportunityTypeFilterCategory: null,
    segmentDisplayLabel: null,
    reason: "unresolved_requires_research",
    priorSegmentId: prior,
    scalingEligibility: scalingGate,
    changed: false,
  };
}

function decision(segmentId, reason, prior, scalingEligibility = null) {
  const resolved = resolveDemandSegment(segmentId);
  const id = resolved?.id || segmentId;
  return {
    version: PRIMARY_DEMAND_SEGMENT_CLASSIFICATION_V1,
    demandSegmentId: id,
    opportunityTypeFilterCategory: SEGMENT_TO_FILTER[id] || null,
    segmentDisplayLabel:
      SEGMENT_LABEL[id] || resolved?.label || id || null,
    opportunityType: id,
    reason,
    priorSegmentId: prior,
    scalingEligibility,
    changed: Boolean(prior && prior !== id),
  };
}

/**
 * Apply primary classification onto a shallow clone (publication remediations).
 * Preserves id, sources, contacts, CMI, commercial fields.
 */
export function applyPrimaryDemandSegmentClassification(opp = {}) {
  const routed = resolvePrimaryDemandSegment(opp);
  if (!routed.demandSegmentId) {
    return {
      ...opp,
      classificationDecision: routed,
    };
  }
  const priorType = opp.opportunityType || null;
  const priorFilter = opp.opportunityTypeFilterCategory || null;
  const priorLabel = opp.segmentDisplayLabel || null;
  return {
    ...opp,
    priorOpportunityType: opp.priorOpportunityType || priorType,
    priorOpportunityTypeFilterCategory:
      opp.priorOpportunityTypeFilterCategory || priorFilter,
    priorSegmentDisplayLabel: opp.priorSegmentDisplayLabel || priorLabel,
    demandSegmentId: routed.demandSegmentId,
    customerDemandSegmentId: routed.demandSegmentId,
    opportunityType: routed.opportunityType || routed.demandSegmentId,
    opportunityTypeFilterCategory: routed.opportunityTypeFilterCategory,
    segmentDisplayLabel: routed.segmentDisplayLabel,
    classificationDecision: routed,
  };
}

export function filterCategoryForSegmentId(segmentId) {
  return SEGMENT_TO_FILTER[segmentId] || null;
}

export {
  ASSOC as SEGMENT_ASSOCIATIONS_CONFERENCES_EVENTS,
  SCALING as SEGMENT_CORPORATE_ACCOUNTS_SCALING,
  MEETINGS as SEGMENT_CORPORATE_MEETINGS_INCENTIVES,
  EDU as SEGMENT_EDUCATION_SCIENTIFIC,
  CATERING as SEGMENT_LOCAL_CATERING_DAY_MEETINGS,
  TRADE as SEGMENT_TRAVEL_TRADE,
  PLANNER as SEGMENT_PROFESSIONAL_EVENT_PLANNERS,
  PREFERRED as SEGMENT_PREFERRED_LODGING,
};
