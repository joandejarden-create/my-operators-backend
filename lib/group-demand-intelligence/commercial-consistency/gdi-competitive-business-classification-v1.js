/**
 * Competitive business classification — analytical dispositions within the
 * existing Three-Layer Research SoT / CAD vocabulary.
 *
 * Does NOT create a parallel engine or new Airtable table.
 * Does NOT equate historical venue association with a confirmed hotel contract.
 * Does NOT promote to FQO — independent six-rule FQO remains separate.
 */

import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
  COMMERCIAL_ACTIONABILITY,
} from "./commercial-status-dimensions-v1.js";
import {
  isEventVenueOnlyMention,
  extractCompetitorHotelFromResearchEvidence,
} from "./gdi-research-competitor-commitment-mapping-v1.js";

export const GDI_COMPETITIVE_BUSINESS_CLASSIFICATION_V1 =
  "gdi-competitive-business-classification-v1";

export const COMPETITIVE_BUSINESS_CLASS = Object.freeze({
  DIRECT_PURSUIT: "DIRECT_PURSUIT",
  COMPETITIVE_DISPLACEMENT: "COMPETITIVE_DISPLACEMENT",
  OVERFLOW_OR_ANCILLARY: "OVERFLOW_OR_ANCILLARY",
  FUTURE_RECURRING_OPPORTUNITY: "FUTURE_RECURRING_OPPORTUNITY",
  HISTORICAL_OR_COMMITTED: "HISTORICAL_OR_COMMITTED",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/**
 * Classify competitive / commercial posture for a deep dossier or CAD row.
 * Prefer explicit CAD / competitor-commitment fields when present.
 */
export function classifyCompetitiveBusiness(opportunity = {}, context = {}) {
  const research = {
    ...opportunity,
    ...(opportunity.exaAgentDeepResearch?.structured || {}),
    ...(context.research || {}),
  };
  const competitorHotel =
    extractCompetitorHotelFromResearchEvidence(research) ||
    text(opportunity.competitorHotel || opportunity.hostHotel);
  const venueOnly =
    competitorHotel && isEventVenueOnlyMention(research, competitorHotel);
  const competitorStatus = upper(
    opportunity.competitorVenueStatus ||
      opportunity.cad?.competitorVenueStatus ||
      "",
  );
  const sourcing = upper(
    opportunity.targetHotelSourcingStatus ||
      opportunity.cad?.targetHotelSourcingStatus ||
      opportunity.sourcingStatus ||
      "",
  );
  const actionability = upper(
    opportunity.commercialActionability ||
      opportunity.cad?.commercialActionability ||
      "",
  );
  const intake = opportunity.intakeDisposition || {};
  const blob = [
    opportunity.title,
    opportunity.hotelOpportunityThesis,
    research.hotelSourcingDecisionEvidence,
    research.futureDemandPathway,
    research.overflowOpportunity,
  ]
    .map(text)
    .join(" ");

  const hasVerifiedCommitment =
    Boolean(competitorHotel) &&
    venueOnly !== true &&
    (/COMMITTED|CONTRACTED|HOST_HOTEL|PRIMARY/i.test(competitorStatus) ||
      intake.commercialDisposition === "COMPETITOR_COMMITTED" ||
      /CURRENT_HOST_COMMITTED|PRIMARY_HOST/i.test(
        String(intake.sourcingStatus || opportunity.activityStatus || ""),
      ));

  const overflowSignal =
    /overflow|ancillary|exhibitor|attendee lodging|spillover|satellite/i.test(
      blob,
    ) ||
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED ||
    /OVERFLOW/i.test(String(opportunity.pursuitStrategy || ""));

  const futureRecurring =
    /202[6-9]|next edition|annual|recurring|future cycle/i.test(blob) ||
    sourcing === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR ||
    /FUTURE|RECURRING/i.test(String(opportunity.activityStatus || ""));

  const displacement =
    hasVerifiedCommitment &&
    (futureRecurring ||
      /DISPLACEMENT|COMPETE|CONTEST/i.test(
        String(opportunity.pursuitStrategy || blob),
      ));

  let classification = COMPETITIVE_BUSINESS_CLASS.RESEARCH_REQUIRED;
  let reason = "unresolved_commercial_evidence";

  if (hasVerifiedCommitment && overflowSignal) {
    classification = COMPETITIVE_BUSINESS_CLASS.OVERFLOW_OR_ANCILLARY;
    reason = "committed_program_with_overflow_or_ancillary_pathway";
  } else if (displacement) {
    classification = COMPETITIVE_BUSINESS_CLASS.COMPETITIVE_DISPLACEMENT;
    reason = "evidence_backed_future_or_contestability_vs_committed_host";
  } else if (hasVerifiedCommitment && !overflowSignal && !futureRecurring) {
    classification = COMPETITIVE_BUSINESS_CLASS.HISTORICAL_OR_COMMITTED;
    reason = "verified_competitor_commitment_without_present_actionable_pathway";
  } else if (futureRecurring && !hasVerifiedCommitment) {
    classification = COMPETITIVE_BUSINESS_CLASS.FUTURE_RECURRING_OPPORTUNITY;
    reason = "future_or_recurring_cycle_sourcing_status_identified";
  } else if (
    !hasVerifiedCommitment &&
    (sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED ||
      sourcing === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
      sourcing === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN ||
      sourcing === "" ||
      intake.commercialDisposition === "OPEN" ||
      actionability === COMMERCIAL_ACTIONABILITY.SALES_READY ||
      actionability === COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT ||
      actionability === COMMERCIAL_ACTIONABILITY.QUALIFICATION_REQUIRED)
  ) {
    classification = COMPETITIVE_BUSINESS_CLASS.DIRECT_PURSUIT;
    reason = "credible_direct_pursuit_without_verified_competitor_lock";
  }

  // Venue-only mention must never count as confirmed hotel contract.
  if (venueOnly) {
    classification = COMPETITIVE_BUSINESS_CLASS.RESEARCH_REQUIRED;
    reason = "venue_mention_only_not_confirmed_hotel_contract";
  }

  return {
    version: GDI_COMPETITIVE_BUSINESS_CLASSIFICATION_V1,
    classification,
    reason,
    competitorHotel: competitorHotel || null,
    venueOnlyMention: venueOnly === true,
    mapsToCad: {
      competitorVenueStatus: competitorStatus || null,
      targetHotelSourcingStatus: sourcing || null,
      commercialActionability: actionability || null,
    },
    fqoAutoPromote: false,
    note: "Analytical disposition only — FQO requires independent six-rule evaluation.",
  };
}

/** Attach classification onto an opportunity row without mutating CAD schema. */
export function applyCompetitiveBusinessClassification(opportunity = {}, context = {}) {
  const classified = classifyCompetitiveBusiness(opportunity, context);
  return {
    ...opportunity,
    competitiveBusinessClassification: classified,
  };
}

export default {
  GDI_COMPETITIVE_BUSINESS_CLASSIFICATION_V1,
  COMPETITIVE_BUSINESS_CLASS,
  classifyCompetitiveBusiness,
  applyCompetitiveBusinessClassification,
};
