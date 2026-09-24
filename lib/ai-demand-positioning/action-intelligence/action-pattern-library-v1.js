/**
 * Governed ADP Action Pattern Library V1.
 * Patterns are reusable playbooks. Customer actions are property-specific instances.
 */

import {
  ACTION_PATTERN_LIFECYCLE,
  SOURCE_CATALOG,
} from "./action-executability-contract-v1.js";

const G = ACTION_PATTERN_LIFECYCLE.GOVERNED;

function pattern(def) {
  return Object.freeze({
    ...def,
    lifecycle: def.lifecycle || G,
    variantsAllowed: true,
    variants: def.variants || [],
    learningKeys: Object.freeze([
      "actionPatternId",
      "implementationStepsUsed",
      "hotelAttributes",
      "issuePattern",
      "sourcesChanged",
      "completionTime",
      "metricBefore",
      "metricNextRun",
      "metricSecondNextRun",
      "evidenceChanges",
      "outcomeClassification",
    ]),
  });
}

export const ACTION_PATTERN_LIBRARY_V1 = Object.freeze({
  ACTION_PARKING_INFORMATION_RECONCILIATION: pattern({
    actionPatternId: "ACTION_PARKING_INFORMATION_RECONCILIATION",
    categoryLabel: "Parking information",
    titleTemplate:
      "Reconcile parking policy across first-party and priority booking/review sources",
    applicability: ["parking_gap", "parking_confusion", "parking_competitor_advantage"],
    checklist: [
      "confirm whether parking is available",
      "self / valet / both",
      "daily / nightly fee",
      "taxes if relevant",
      "in/out privileges",
      "reservation requirement",
      "garage location / arrival instructions",
      "vehicle height restrictions",
      "oversized vehicle policy",
      "EV charging",
      "accessible parking",
      "third-party/public parking alternative if hotel has none",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.BRAND_BOOKING,
      SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
      SOURCE_CATALOG.TRIPADVISOR,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
    ],
  }),

  ACTION_PET_POLICY_RECONCILIATION: pattern({
    actionPatternId: "ACTION_PET_POLICY_RECONCILIATION",
    categoryLabel: "Pet policy",
    titleTemplate:
      "Publish one canonical pet policy and reconcile it across booking and profile sources",
    applicability: ["pet_friendly_gap", "pet_policy_inconsistency"],
    checklist: [
      "pets accepted? yes/no",
      "dogs/cats/other",
      "max pets",
      "weight limits",
      "per-stay / per-night fee",
      "deposit",
      "refundable / non-refundable",
      "restricted room types",
      "restricted hotel areas",
      "service-animal policy",
      "cleaning fee if applicable",
      "advance notice / reservation requirements",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
      SOURCE_CATALOG.TRIPADVISOR,
    ],
  }),

  ACTION_FAMILY_TRAVEL_INFORMATION_COMPLETION: pattern({
    actionPatternId: "ACTION_FAMILY_TRAVEL_INFORMATION_COMPLETION",
    categoryLabel: "Family content",
    titleTemplate:
      "Complete missing family-travel facts identified in monitoring evidence",
    applicability: ["family_gap", "family_omission"],
    checklist: [
      "connecting rooms",
      "adjoining rooms",
      "room occupancy limits",
      "rollaway / crib availability",
      "family suites",
      "pool suitability",
      "children's pool",
      "beach safety/access",
      "kids club",
      "minimum age restrictions",
      "children's menus",
      "breakfast policy for children",
      "babysitting",
      "nearby family attractions",
      "distance / transport to attractions",
      "family package availability",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.BRAND_BOOKING,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
      SOURCE_CATALOG.TRIPADVISOR,
    ],
  }),

  ACTION_ACCESSIBILITY_INFORMATION_COMPLETION: pattern({
    actionPatternId: "ACTION_ACCESSIBILITY_INFORMATION_COMPLETION",
    categoryLabel: "Accessibility content",
    titleTemplate:
      "Complete accurate accessibility information on public sources (information only; not a compliance certification)",
    applicability: ["accessibility_gap"],
    checklist: [
      "accessible guest rooms",
      "roll-in showers",
      "grab bars",
      "accessible public entrances",
      "elevator access",
      "accessible parking",
      "pool lift",
      "accessible restaurant/bar",
      "hearing-accessible rooms",
      "visual alarms",
      "service-animal information",
      "accessible route between major guest areas",
      "accessible meeting space",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
    ],
    notes: [
      "Do not infer ADA/legislative compliance. Action is about accurate public information only.",
    ],
  }),

  ACTION_MANDATORY_FEE_INFORMATION_RECONCILIATION: pattern({
    actionPatternId: "ACTION_MANDATORY_FEE_INFORMATION_RECONCILIATION",
    categoryLabel: "Mandatory / resort fee",
    titleTemplate:
      "Reconcile mandatory fee amount, inclusions, and payment terms across booking sources",
    applicability: ["mandatory_fee_gap", "resort_fee_inconsistency"],
    checklist: [
      "exact mandatory fee amount",
      "per night / per stay",
      "taxable?",
      "included amenities",
      "excluded services",
      "exemptions",
      "payment timing",
      "whether fee applies to points stays if known",
      "destination/resort/urban amenity fee terminology",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.BRAND_BOOKING,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
      SOURCE_CATALOG.TRIPADVISOR,
    ],
  }),

  ACTION_ALL_INCLUSIVE_OFFER_RECONCILIATION: pattern({
    actionPatternId: "ACTION_ALL_INCLUSIVE_OFFER_RECONCILIATION",
    categoryLabel: "All-inclusive offer",
    titleTemplate:
      "Document and reconcile the all-inclusive offer package across first-party and major booking sources",
    applicability: ["all_inclusive_gap"],
    checklist: [
      "whether an all-inclusive option exists",
      "what is included (meals, beverages, activities, spa credits, etc.)",
      "what is excluded",
      "pricing basis (per person / per room / per night)",
      "blackout or seasonality rules",
      "how the option is labeled on booking paths",
      "whether OTAs expose the same package name and inclusions",
      "FAQ / terms page location",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.BRAND_BOOKING,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
      SOURCE_CATALOG.TRIPADVISOR,
    ],
  }),

  ACTION_MEETINGS_DECISION_PACKAGE_COMPLETION: pattern({
    actionPatternId: "ACTION_MEETINGS_DECISION_PACKAGE_COMPLETION",
    categoryLabel: "Meetings information",
    titleTemplate:
      "Complete the meetings decision package with missing capacity, space, and contact facts",
    applicability: ["meetings_gap", "group_meeting_whitespace"],
    checklist: [
      "total meeting space",
      "number of rooms",
      "largest room",
      "largest capacity",
      "theater capacity",
      "classroom capacity",
      "banquet capacity",
      "reception capacity",
      "boardroom capacity",
      "breakout-room count",
      "ceiling height where relevant",
      "AV capabilities",
      "hybrid meeting capabilities",
      "Wi-Fi",
      "catering",
      "outdoor event space",
      "loading / access",
      "parking",
      "group room blocks",
      "meeting sales contact",
      "downloadable floorplans",
      "event fact sheet",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.MEETING_FACT_SHEET,
      SOURCE_CATALOG.BRAND_BOOKING,
      SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
    ],
  }),

  ACTION_TRIPADVISOR_MANAGEMENT_RESPONSE_PROGRAM: pattern({
    actionPatternId: "ACTION_TRIPADVISOR_MANAGEMENT_RESPONSE_PROGRAM",
    categoryLabel: "TripAdvisor responses",
    titleTemplate:
      "Stand up a TripAdvisor management-response program with owner, SLA, and theme tracking",
    applicability: ["tripadvisor_response_gap", "reputation_response_gap"],
    checklist: [
      "designate accountable owner",
      "establish daily review-monitoring process",
      "set response SLA (property-configured; do not invent a default unless governed)",
      "define which review scores require response",
      "define response quality standard",
      "prohibit generic copy-paste responses",
      "identify recurring complaint themes monthly",
      "identify recurring praise themes",
      "correct factual misconceptions where appropriate",
      "escalate recurring operational problems",
      "track response rate",
      "track median response time",
    ],
    defaultSources: [SOURCE_CATALOG.TRIPADVISOR],
    variants: [
      "reputation_recovery",
      "factual_misconception_correction",
      "service_recovery_response",
      "expectation_setting_reinforcement",
      "positive_theme_reinforcement",
    ],
  }),

  ACTION_GOOGLE_BUSINESS_PROFILE_COMPLETENESS_AUDIT: pattern({
    actionPatternId: "ACTION_GOOGLE_BUSINESS_PROFILE_COMPLETENESS_AUDIT",
    categoryLabel: "Google Business Profile",
    titleTemplate:
      "Audit and correct Google Business Profile fields against the hotel’s canonical facts",
    applicability: ["gbp_gap", "broad_underrepresentation"],
    checklist: [
      "property name",
      "address",
      "phone",
      "website",
      "booking link",
      "category",
      "hotel class if applicable",
      "amenities",
      "accessibility attributes",
      "check-in/out",
      "parking",
      "pet policy",
      "photos",
      "description",
      "Q&A",
      "outdated user-added information",
    ],
    defaultSources: [SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE, SOURCE_CATALOG.HOTEL_WEBSITE],
  }),

  ACTION_ROOM_EXPECTATION_ALIGNMENT: pattern({
    actionPatternId: "ACTION_ROOM_EXPECTATION_ALIGNMENT",
    categoryLabel: "Room expectation alignment",
    titleTemplate:
      "Align first-party and OTA room descriptions with recurring expectation mismatches in evidence",
    applicability: ["room_expectation_gap", "boutique_room_mismatch"],
    checklist: [
      "identify exact recurring expectation mismatch",
      "update first-party room descriptions",
      "verify OTA room-type descriptions",
      "update FAQ where helpful",
      "ensure imagery accurately reflects room type",
      "monitor review language and AI descriptions next run",
    ],
    detailChecklist: [
      "room size",
      "bed size",
      "shared/private bathroom",
      "storage",
      "desk/workspace",
      "window/no-window",
      "noise",
      "floor access",
      "elevator",
      "occupancy",
      "housekeeping",
      "amenity limitations",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.OTA_ROOM_TYPES,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
      SOURCE_CATALOG.FAQ,
    ],
  }),

  ACTION_NEIGHBORHOOD_USE_CASE_CONTENT: pattern({
    actionPatternId: "ACTION_NEIGHBORHOOD_USE_CASE_CONTENT",
    categoryLabel: "Neighborhood / local positioning",
    titleTemplate:
      "Publish accurate neighborhood use-case facts for traveler needs where the hotel is absent",
    applicability: ["neighborhood_omission", "local_use_case_gap"],
    checklist: [
      "nearest major landmarks",
      "walking times",
      "transit stations",
      "business districts",
      "nightlife",
      "dining",
      "shopping",
      "venues",
      "universities",
      "hospitals",
      "convention centers",
      "airport/transit access",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
      SOURCE_CATALOG.FAQ,
      SOURCE_CATALOG.LOCAL_AUTHORITY,
    ],
    notes: ["Do not keyword-stuff. Use accurate, decision-useful information only."],
  }),

  ACTION_COMPETITIVE_ABSENCE_SOURCE_RECONCILIATION: pattern({
    actionPatternId: "ACTION_COMPETITIVE_ABSENCE_SOURCE_RECONCILIATION",
    categoryLabel: "Competitive displacement",
    titleTemplate:
      "Reconcile public facts for traveler needs where a named competitor appears and the subject hotel is absent",
    applicability: ["competitive_displacement"],
    checklist: [
      "name the displacement competitor and displacement count",
      "list traveler needs / scenarios where absence concentrates",
      "compare public facts the competitor is credited with in evidence",
      "identify subject facts that are missing or inconsistent on priority sources",
      "update first-party pages covering those facts",
      "reconcile the same facts on the priority OTAs/review profiles involved",
      "store before/after URLs for each corrected source",
    ],
    defaultSources: [
      SOURCE_CATALOG.HOTEL_WEBSITE,
      SOURCE_CATALOG.GOOGLE_BUSINESS_PROFILE,
      SOURCE_CATALOG.TRIPADVISOR,
      SOURCE_CATALOG.BOOKING_COM,
      SOURCE_CATALOG.EXPEDIA,
    ],
  }),
});

export function getActionPattern(actionPatternId) {
  return ACTION_PATTERN_LIBRARY_V1[actionPatternId] || null;
}

export function listGovernedActionPatterns() {
  return Object.values(ACTION_PATTERN_LIBRARY_V1).filter(
    (p) =>
      p.lifecycle === ACTION_PATTERN_LIFECYCLE.GOVERNED ||
      p.lifecycle === ACTION_PATTERN_LIFECYCLE.REUSABLE
  );
}
