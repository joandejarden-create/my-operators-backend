/**
 * Commercial QA overrides for Bethesda Marriott GDI corpus.
 * Evidence-based corrections from L3 field/housing verification.
 * Does not invent hotel CRM facts. $0 Webhound.
 *
 * Applied after standard qualification classifiers so geography/name
 * false positives (e.g. "Bethesda" in organizer name) cannot keep High.
 */

import {
  DEMAND_TERRITORY_FIT,
  EVENT_LOCATION_STATUS,
  EVENT_LOCATION_STATUS_LABEL,
  OPPORTUNITY_QUALIFICATION,
  OPPORTUNITY_QUALIFICATION_LABEL,
  PRIORITY,
  QUALIFICATION_FAILURE_REASON,
} from "./claim-types.js";

export const COMMERCIAL_QA_PASS_ID = "commercial_qa_high_retention_v1_20260915";

/**
 * @type {Record<string, object>}
 */
export const COMMERCIAL_QA_OVERRIDES_BY_OPP_ID = Object.freeze({
  gdi_opp_bethesda_premier_cup_2026: {
    verdict: "DOWNGRADE_HIGH_TO_MEDIUM",
    forceMaxPriority: PRIORITY.MEDIUM,
    forcePriority: PRIORITY.MEDIUM,
    destinationStatus:
      "Multi-site DMV fields (not Bethesda-centered): Alexandria VA (U9–U12); Poolesville; Olney; Liberty Park Upper Marlboro (U15 boys); Maryland SoccerPlex Boyds (U16–U19)",
    venueStatus:
      "Fields-based multi-site; stay-to-play housing exclusively via HBC Event Services (official hotel list controlled by HBC)",
    demandTerritoryFit: DEMAND_TERRITORY_FIT.DMV_COMPETITIVE,
    demandTerritoryFitLocked: true,
    demandTerritoryRationale:
      "ACTUAL EVENT GEOGRAPHY > ORGANIZATION NAME. Official Premier Cup field map places age groups in Alexandria, Upper Marlboro, Poolesville, Olney, and SoccerPlex Boyds. Club name/HQ in Bethesda does not make Bethesda Marriott a core housing location. HBC partner hotels typically cluster near assigned fields (I-270 north / Alexandria / PG), not Bethesda CBD.",
    eventLocationStatus: EVENT_LOCATION_STATUS.VERIFIED_VENUE,
    eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.VERIFIED_VENUE,
    eventLocationSummary:
      "Verified multi-site field schedule (Alexandria / Poolesville / Olney / Upper Marlboro / SoccerPlex Boyds)",
    eventLocationSource: "official_fields_page",
    geographyFitScore: 42,
    opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
    opportunityQualificationLabel: OPPORTUNITY_QUALIFICATION_LABEL.MODERATE,
    qualificationGatePassed: true,
    qualificationNotes:
      "Housing program is real (HBC stay-to-play) but not verified as an open High-priority lodging opportunity specifically for Bethesda Marriott given multi-site field geography.",
    qualificationFailureReason: QUALIFICATION_FAILURE_REASON.EVENT_LOCATION_POOR_FIT,
    hotelOpportunityThesis:
      "HBC Event Services runs mandatory stay-to-play lodging. That is a real hotel-sales path — but only if Bethesda Marriott can get onto the official HBC list for age groups whose fields are within a commercially reasonable drive. Primary field clusters are SoccerPlex Boyds, Alexandria, Poolesville/Olney, and Upper Marlboro (U15), not Bethesda. Treat as a qualify/ask-HBC Medium, not a High 'pursue as Bethesda lodging' lead.",
    bethesdaWinThesis:
      "Possible only for SoccerPlex-assigned older age groups if HBC accepts a Bethesda property; do not pitch as the natural host hotel for Alexandria or Upper Marlboro fields.",
    whyNow:
      "QUALIFY NOW: Confirm with HBC whether Bethesda Marriott can join the official hotel inventory for SoccerPlex-assigned weekends. Do not treat the club name 'Bethesda' as proof of Bethesda lodging demand.",
    recommendedAction:
      "Email HBC Event Services (support@hbceventservices.com / 505-346-0522) to request inclusion for SoccerPlex Boyds age groups only; ask which submarkets they are filling. Do not spend sales time assuming Bethesda CBD lodging for Alexandria or Upper Marlboro brackets.",
    priorityReason:
      "Commercial QA 2026-09-15: verified housing program exists, but event geography is multi-site DMV — not a verified open High opportunity for Bethesda Marriott specifically.",
    commercialQa: {
      passId: COMMERCIAL_QA_PASS_ID,
      checkedAt: "2026-09-15",
      sources: [
        "https://premiercup.bethesdasoccertournaments.com/fields/",
        "https://premiercup.bethesdasoccertournaments.com/hotel-transportationn/",
        "https://bethesdapremiercuphotels.com/",
      ],
      findings: [
        "Fields span Alexandria VA, Upper Marlboro, Poolesville, Olney, SoccerPlex Boyds",
        "Stay-to-play mandatory via HBC; hotel list not published as Bethesda-centric",
        "Organizer name contains Bethesda — insufficient for Core High retention",
      ],
    },
  },

  gdi_opp_potomac_memorial_2027: {
    verdict: "RETAIN_HIGH",
    forceMaxPriority: null,
    destinationStatus:
      "Maryland SoccerPlex, Boyds MD (primary) — games also in Montgomery & Frederick Counties",
    venueStatus:
      "Fields-based; stay-to-play for teams >100 miles via HBC Event Services (hotel link releases closer to event)",
    demandTerritoryFit: DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE,
    demandTerritoryFitLocked: true,
    demandTerritoryRationale:
      "Primary venue is Maryland SoccerPlex in Boyds (Montgomery County). ~25–40 minutes from Bethesda Marriott via I-270 — commercially credible for Montgomery County stay-to-play, though Gaithersburg/Germantown hotels also compete. Event geography (not club name alone) supports Core.",
    eventLocationStatus: EVENT_LOCATION_STATUS.VERIFIED_VENUE,
    eventLocationStatusLabel: EVENT_LOCATION_STATUS_LABEL.VERIFIED_VENUE,
    eventLocationSummary: "Maryland SoccerPlex, Boyds MD (primary site)",
    eventLocationSource: "official_tournament_page",
    hotelOpportunityThesis:
      "Large Memorial Day weekend stay-to-play (~450 teams historically) with mandatory HBC housing for traveling teams. Primary fields at SoccerPlex Boyds make Bethesda Marriott a realistic Montgomery County lodging option if HBC includes the property. Named tournament director is actionable. This is overflow/housing demand, not a primary host-hotel RFP.",
    bethesdaWinThesis:
      "I-270 / Montgomery County access to SoccerPlex Boyds; full-service rooms + breakfast capability typical of HBC partner asks; compete vs Gaithersburg/Germantown inventory by asking HBC for list inclusion early.",
    whyNow:
      "CONTACT NOW: Housing planning for the next Memorial cycle happens before HBC inventory locks. Confirm 2027 stay-to-play window with Kathy Hauschild / HBC and request Bethesda Marriott inclusion.",
    recommendedAction:
      "Contact tournament director Kathy Hauschild and HBC (support@hbceventservices.com) to request official hotel-list inclusion for SoccerPlex Boyds lodging; confirm peak dates and breakfast/comp requirements.",
    commercialQa: {
      passId: COMMERCIAL_QA_PASS_ID,
      checkedAt: "2026-09-15",
      sources: [
        "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/",
        "https://potomacsoccer.org/memorial_tournament/hotel-policy/",
      ],
      findings: [
        "Primary site SoccerPlex Boyds confirmed",
        "Stay-to-play via HBC for teams outside 100 miles",
        "Named director + HBC path actionable for Bethesda Marriott",
        "Hotel reservation link for next cycle not yet posted — still Contact/Qualify now for list inclusion",
      ],
    },
  },
});

/**
 * Medium-downgrade commercial QA notes (documentation only — no auto-promote).
 */
export const MEDIUM_CONTACT_VS_QUALIFICATION_NOTES = Object.freeze({
  gdi_opp_amwa_2027_annual: {
    bucket: "A_WEAK_OR_INCOMPLETE_QUALIFICATION",
    note:
      "Venue TBD is promising, but room demand remains UNKNOWN and contact is a generic association inbox. Medium is correct — not merely a named-contact miss.",
  },
  gdi_opp_nice_2027: {
    bucket: "B_STRONG_OPEN_MISSING_NAMED_CONTACT",
    note:
      "Qualification STRONG / venue open / estimated rooms / NIST-adjacent — blocked from High only by generic inbox (nice@nist.gov). High contact rule is appropriately strict for 'act this week'; Medium correctly preserves the opportunity for contact deepening.",
  },
  gdi_opp_acts_ts27: {
    bucket: "B_STRONG_OPEN_MISSING_NAMED_CONTACT",
    note:
      "Qualification STRONG / DC venue TBD / estimated rooms / NIH-adjacent — blocked from High by unnamed staff desk. Do not auto-promote; deepen named meetings contact first.",
  },
});

export function getCommercialQaOverride(opportunityId) {
  return COMMERCIAL_QA_OVERRIDES_BY_OPP_ID[opportunityId] || null;
}

/**
 * Merge commercial QA override onto an opportunity input before buildOpportunity.
 */
export function applyCommercialQaOverride(opportunity = {}) {
  const ovr = getCommercialQaOverride(opportunity.id);
  if (!ovr) return { opportunity, applied: false, override: null };

  const fitComponents = {
    physicalFit: opportunity.fitComponents?.physicalFit ?? opportunity.physicalFitScore,
    geographyFit:
      ovr.geographyFitScore ??
      opportunity.fitComponents?.geographyFit ??
      opportunity.geographyFitScore,
    timing: opportunity.fitComponents?.timing ?? opportunity.timingScore,
    commercialValue:
      opportunity.fitComponents?.commercialValue ?? opportunity.commercialValueScore,
    historicalFit:
      opportunity.fitComponents?.historicalFit ?? opportunity.historicalFitScore,
    competitiveAccessibility:
      opportunity.fitComponents?.competitiveAccessibility ??
      opportunity.competitiveAccessibilityScore,
    contactability:
      opportunity.fitComponents?.contactability ?? opportunity.contactabilityScore,
  };

  const labels = Array.from(
    new Set([...(opportunity.labels || []), "commercial_qa_v1", String(ovr.verdict || "").toLowerCase()])
  );

  return {
    applied: true,
    override: ovr,
    opportunity: {
      ...opportunity,
      destinationStatus: ovr.destinationStatus ?? opportunity.destinationStatus,
      venueStatus: ovr.venueStatus ?? opportunity.venueStatus,
      demandTerritoryFit: ovr.demandTerritoryFit ?? opportunity.demandTerritoryFit,
      demandTerritoryFitLocked: ovr.demandTerritoryFitLocked ?? true,
      demandTerritoryRationale:
        ovr.demandTerritoryRationale ?? opportunity.demandTerritoryRationale,
      eventLocationStatus: ovr.eventLocationStatus ?? opportunity.eventLocationStatus,
      eventLocationStatusLabel:
        ovr.eventLocationStatusLabel ?? opportunity.eventLocationStatusLabel,
      eventLocationSummary: ovr.eventLocationSummary ?? opportunity.eventLocationSummary,
      eventLocationSource: ovr.eventLocationSource ?? opportunity.eventLocationSource,
      hotelOpportunityThesis: ovr.hotelOpportunityThesis ?? opportunity.hotelOpportunityThesis,
      bethesdaWinThesis: ovr.bethesdaWinThesis ?? opportunity.bethesdaWinThesis,
      whyNow: ovr.whyNow ?? opportunity.whyNow,
      recommendedAction: ovr.recommendedAction ?? opportunity.recommendedAction,
      fitComponents,
      labels,
      commercialQa: ovr.commercialQa,
    },
    forceMaxPriority: ovr.forceMaxPriority || null,
  };
}

/**
 * Re-apply sticky commercial QA fields after buildOpportunity (classifiers can overwrite).
 */
export function finalizeCommercialQaOpportunity(built, override, forceMaxPriority) {
  if (!built || !override) return built;
  let next = {
    ...built,
    destinationStatus: override.destinationStatus ?? built.destinationStatus,
    venueStatus: override.venueStatus ?? built.venueStatus,
    demandTerritoryFit: override.demandTerritoryFit ?? built.demandTerritoryFit,
    demandTerritoryFitLocked: true,
    demandTerritoryRationale:
      override.demandTerritoryRationale ?? built.demandTerritoryRationale,
    demandTerritoryFitLabel:
      override.demandTerritoryFit === DEMAND_TERRITORY_FIT.DMV_COMPETITIVE
        ? "DMV Competitive"
        : override.demandTerritoryFit === DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE
          ? "Bethesda / Montgomery Core"
          : built.demandTerritoryFitLabel,
    eventLocationStatus: override.eventLocationStatus ?? built.eventLocationStatus,
    eventLocationStatusLabel:
      override.eventLocationStatusLabel ?? built.eventLocationStatusLabel,
    eventLocationSummary: override.eventLocationSummary ?? built.eventLocationSummary,
    eventLocationSource: override.eventLocationSource ?? built.eventLocationSource,
    hotelOpportunityThesis: override.hotelOpportunityThesis ?? built.hotelOpportunityThesis,
    bethesdaWinThesis: override.bethesdaWinThesis ?? built.bethesdaWinThesis,
    whyNow: override.whyNow ?? built.whyNow,
    recommendedAction: override.recommendedAction ?? built.recommendedAction,
    commercialQa: override.commercialQa,
    geographyFitScore: override.geographyFitScore ?? built.geographyFitScore,
  };

  if (override.opportunityQualification) {
    next.opportunityQualification = override.opportunityQualification;
    next.opportunityQualificationLabel =
      override.opportunityQualificationLabel || next.opportunityQualificationLabel;
    next.qualificationNotes = override.qualificationNotes ?? next.qualificationNotes;
    next.qualificationFailureReason =
      override.qualificationFailureReason ?? next.qualificationFailureReason;
    next.qualificationGatePassed =
      override.qualificationGatePassed ?? next.qualificationGatePassed;
  }

  if (override.priorityReason) {
    next.priorityReason = override.priorityReason;
  }

  next = clampPriorityAfterCommercialQa(next, forceMaxPriority);
  if (override.forcePriority) {
    next = {
      ...next,
      priority: override.forcePriority,
      priorityReason:
        override.priorityReason ||
        next.priorityReason ||
        `Commercial QA set priority to ${override.forcePriority}`,
    };
  }
  return next;
}

/**
 * Clamp priority after buildOpportunity when commercial QA caps High.
 */
export function clampPriorityAfterCommercialQa(built, forceMaxPriority) {
  if (!forceMaxPriority || !built) return built;
  const rank = {
    HIGH_PRIORITY: 0,
    MEDIUM_PRIORITY: 1,
    WATCHLIST: 2,
    DISQUALIFIED: 3,
  };
  const cur = rank[built.priority] ?? 9;
  const max = rank[forceMaxPriority] ?? 9;
  if (cur < max) {
    return {
      ...built,
      priority: forceMaxPriority,
      priorityReason:
        built.priorityReason ||
        `Commercial QA capped priority at ${forceMaxPriority}`,
    };
  }
  return built;
}
