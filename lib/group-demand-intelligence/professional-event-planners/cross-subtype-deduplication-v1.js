/**
 * Cross-workflow deduplication: Preferred Lodging (venues) ↔ Professional
 * Event Planners, plus segment-mechanism routing hints (Seg 3/8/10/11).
 * Avoid double-counting the same event's room-night potential.
 */
import { text } from "./professional-event-planner-contract-v1.js";
import { isPreferredLodgingPartnershipOpportunity } from "../preferred-lodging-partnership/preferred-lodging-partnership-contract-v1.js";
import { isProfessionalEventPlannerOpportunity } from "./professional-event-planner-contract-v1.js";

export const PLANNER_CROSS_SUBTYPE_DEDUP_V1 =
  "gdi-planner-cross-subtype-deduplication-v1";

/**
 * When principal demand mechanism is intermediary group sourcing / incentive
 * travel brokerage, prefer Segment 11 rather than Segment 8 planner subtype.
 */
export function suggestPrimaryClassification({
  opportunity = {},
  planner = {},
} = {}) {
  const blob = [
    opportunity.summaryWhat,
    opportunity.hotelOpportunityThesis,
    planner.specialization,
    planner.principalDemandMechanism,
    opportunity.principalDemandMechanism,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();

  if (
    /\bincentive travel\b|\bdmc\b|destination management|group tour operator|travel trade intermediary/i.test(
      blob,
    ) &&
    !/\bwedding planner\b|\bsocial event\b|\bbanquet planner\b/i.test(blob)
  ) {
    return {
      preferredSegmentId: "travel_trade_group_intermediaries",
      preferredSubtype: null,
      reason: "principal_mechanism_travel_trade_intermediary",
    };
  }
  if (
    /\bcorporate meeting\b|\bincentive programme\b|\bmice\b/i.test(blob) &&
    !/\bwedding\b|\bsocial celebration\b|\bprivate dining\b/i.test(blob)
  ) {
    return {
      preferredSegmentId: "corporate_meetings_incentives",
      preferredSubtype: null,
      reason: "principal_mechanism_corporate_meetings_incentives",
    };
  }
  if (
    /\bcorporate celebration\b|\bemployee recognition gala\b|\bbrand activation\b/i.test(
      blob,
    ) &&
    !/\bwedding\b/i.test(blob)
  ) {
    return {
      preferredSegmentId: "corporate_celebrations_private_events",
      preferredSubtype: null,
      reason: "principal_mechanism_corporate_celebrations",
    };
  }
  return {
    preferredSegmentId: "social_special_events",
    preferredSubtype: "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP",
    reason: "principal_mechanism_social_special_events_planner",
  };
}

/**
 * Link venue partnership + planner partnership without duplicating event RN potential.
 */
export function buildVenuePlannerRelationship({
  venueOpportunity = null,
  plannerOpportunity = null,
  sharedEventRefs = [],
} = {}) {
  const venueOk =
    venueOpportunity && isPreferredLodgingPartnershipOpportunity(venueOpportunity);
  const plannerOk =
    plannerOpportunity &&
    isProfessionalEventPlannerOpportunity(plannerOpportunity);
  if (!venueOk && !plannerOk) return null;

  const eventRefs = (sharedEventRefs || [])
    .map((r) => ({
      eventKey: text(r.eventKey || r.id || r.label),
      label: text(r.label || r.eventName),
      roomNightAttribution: r.roomNightAttribution || "shared_do_not_double_count",
    }))
    .filter((r) => r.eventKey || r.label);

  return {
    version: PLANNER_CROSS_SUBTYPE_DEDUP_V1,
    venueOpportunityId: venueOpportunity?.id || null,
    plannerOpportunityId: plannerOpportunity?.id || null,
    venueName:
      text(
        venueOpportunity?.displayTitle ||
          venueOpportunity?.organizationName ||
          venueOpportunity?.venueName,
      ) || null,
    plannerName:
      text(
        plannerOpportunity?.displayTitle ||
          plannerOpportunity?.organizationName ||
          plannerOpportunity?.plannerName,
      ) || null,
    sharedEventRefs: eventRefs,
    roomNightAccountingRule:
      "Attribute overnight demand once per verified event — do not sum identical RN across venue and planner accounts.",
    relationshipType: "VENUE_AND_PLANNER_COMPLEMENTARY",
  };
}

/**
 * Detect duplicate planner account opportunities within one hotel.
 */
export function findDuplicatePlannerAccounts(opportunities = [], hotelId = null) {
  const seen = new Map();
  const duplicates = [];
  for (const o of opportunities || []) {
    if (hotelId && o.hotelId && o.hotelId !== hotelId) continue;
    if (!isProfessionalEventPlannerOpportunity(o)) continue;
    const fp =
      o.opportunityIdentity?.plannerPartnershipFingerprint ||
      `pep_${o.hotelId}_${text(o.organizationName || o.displayTitle)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")}`;
    if (seen.has(fp)) {
      duplicates.push({
        fingerprint: fp,
        existingId: seen.get(fp),
        duplicateId: o.id,
      });
    } else {
      seen.set(fp, o.id);
    }
  }
  return duplicates;
}
