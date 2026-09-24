/**
 * Group Demand Intelligence — claim typing and enums.
 * FACT vs INFERENCE must never be mixed in salesperson-facing copy without labels.
 */

export const CLAIM_KIND = Object.freeze({
  FACT: "FACT",
  /** UI label: Verified */
  VERIFIED: "FACT",
  INFERENCE: "INFERENCE",
  ESTIMATED: "ESTIMATED",
  UNKNOWN: "UNKNOWN",
});

/** Customer-facing claim labels (map stored claimKind → display). */
export const CLAIM_KIND_LABEL = Object.freeze({
  FACT: "VERIFIED",
  INFERENCE: "INFERRED",
  ESTIMATED: "ESTIMATED",
  UNKNOWN: "UNKNOWN",
});

export const DEMAND_STATUS = Object.freeze({
  CONFIRMED_DEMAND: "CONFIRMED_DEMAND",
  EMERGING_DEMAND: "EMERGING_DEMAND",
  RECURRING_PREDICTED: "RECURRING_PREDICTED",
  SPECULATIVE_WATCH: "SPECULATIVE_WATCH",
});

export const BOOKING_WINDOW = Object.freeze({
  CONTACT_NOW: "CONTACT_NOW",
  /** Preferred GM-facing label for qualify/research stage */
  QUALIFY_NOW: "QUALIFY_NOW",
  /** Legacy alias retained for existing opportunity records */
  RESEARCH_FURTHER: "RESEARCH_FURTHER",
  WATCH: "WATCH",
  TOO_EARLY: "TOO_EARLY",
  LIKELY_TOO_LATE: "LIKELY_TOO_LATE",
});

export const BOOKING_WINDOW_LABEL = Object.freeze({
  CONTACT_NOW: "CONTACT NOW",
  QUALIFY_NOW: "QUALIFY NOW",
  RESEARCH_FURTHER: "QUALIFY NOW",
  WATCH: "WATCH",
  TOO_EARLY: "TOO EARLY",
  LIKELY_TOO_LATE: "LIKELY TOO LATE",
});

export const PRIORITY = Object.freeze({
  HIGH: "HIGH_PRIORITY",
  MEDIUM: "MEDIUM_PRIORITY",
  WATCHLIST: "WATCHLIST",
  DISQUALIFIED: "DISQUALIFIED",
});

/**
 * Demand Territory Fit — could Bethesda Marriott realistically compete?
 * Geography outside Montgomery County is allowed when credible.
 */
/**
 * Demand Territory Fit codes.
 * Portable canonical codes: TERRITORY_* (see demand-territory.js).
 * Bethesda/DMV values remain valid aliases for historical pilot rows.
 */
export const DEMAND_TERRITORY_FIT = Object.freeze({
  TERRITORY_CORE: "TERRITORY_CORE",
  TERRITORY_NEARBY: "TERRITORY_NEARBY",
  TERRITORY_COMPETITIVE: "TERRITORY_COMPETITIVE",
  TERRITORY_STRETCH: "TERRITORY_STRETCH",
  OUTSIDE_REALISTIC_TERRITORY: "OUTSIDE_REALISTIC_TERRITORY",
  // Legacy Bethesda pilot aliases (normalize via toTerritoryRole)
  BETHESDA_MONTGOMERY_CORE: "BETHESDA_MONTGOMERY_CORE",
  NORTH_DC_MEDICAL_CORRIDOR: "NORTH_DC_MEDICAL_CORRIDOR",
  DMV_COMPETITIVE: "DMV_COMPETITIVE",
  DMV_STRETCH: "DMV_STRETCH",
});

export const DEMAND_TERRITORY_FIT_LABEL = Object.freeze({
  TERRITORY_CORE: "Core",
  TERRITORY_NEARBY: "Nearby / Adjacent",
  TERRITORY_COMPETITIVE: "Competitive",
  TERRITORY_STRETCH: "Stretch",
  OUTSIDE_REALISTIC_TERRITORY: "Outside Realistic Territory",
  BETHESDA_MONTGOMERY_CORE: "Bethesda / Montgomery Core",
  NORTH_DC_MEDICAL_CORRIDOR: "North DC / Medical Corridor",
  DMV_COMPETITIVE: "DMV Competitive",
  DMV_STRETCH: "DMV Stretch",
});

/**
 * Sourcing status — do NOT invent "new to hotel" from public research alone.
 * Default external research result is UNKNOWN.
 */
export const SOURCING_STATUS = Object.freeze({
  UNKNOWN: "UNKNOWN",
  PUBLIC_EVIDENCE_OF_HOTEL_SOURCING: "PUBLIC_EVIDENCE_OF_HOTEL_SOURCING",
  PUBLIC_EVIDENCE_OF_BROADER_RFP_HOUSING: "PUBLIC_EVIDENCE_OF_BROADER_RFP_HOUSING",
  HOTEL_CONFIRMED_ALREADY_SOURCED: "HOTEL_CONFIRMED_ALREADY_SOURCED",
  HOTEL_CONFIRMED_NOT_SOURCED: "HOTEL_CONFIRMED_NOT_SOURCED",
  HOTEL_CONFIRMED_ALREADY_PURSUED: "HOTEL_CONFIRMED_ALREADY_PURSUED",
  HOTEL_CONFIRMED_LOST: "HOTEL_CONFIRMED_LOST",
  HOTEL_CONFIRMED_WON: "HOTEL_CONFIRMED_WON",
});

export const SOURCING_STATUS_LABEL = Object.freeze({
  UNKNOWN: "Unknown — requires hotel validation",
  PUBLIC_EVIDENCE_OF_HOTEL_SOURCING: "Public evidence of hotel sourcing",
  PUBLIC_EVIDENCE_OF_BROADER_RFP_HOUSING: "Public evidence of broader RFP / housing process",
  HOTEL_CONFIRMED_ALREADY_SOURCED: "Hotel confirmed already sourced",
  HOTEL_CONFIRMED_NOT_SOURCED: "Hotel confirmed not sourced",
  HOTEL_CONFIRMED_ALREADY_PURSUED: "Hotel confirmed already pursued",
  HOTEL_CONFIRMED_LOST: "Hotel confirmed lost",
  HOTEL_CONFIRMED_WON: "Hotel confirmed won",
});

/**
 * Incremental value — hotel-feedback driven. Never infer NEW_TO_HOTEL from public web alone.
 */
export const INCREMENTAL_VALUE_STATUS = Object.freeze({
  UNKNOWN: "UNKNOWN",
  NEW_TO_HOTEL: "NEW_TO_HOTEL",
  ALREADY_KNOWN_USEFUL_ADDITIONAL: "ALREADY_KNOWN_USEFUL_ADDITIONAL",
  ALREADY_KNOWN_NO_INCREMENTAL: "ALREADY_KNOWN_NO_INCREMENTAL",
  DUPLICATE_OF_EXISTING_SALES_LEAD: "DUPLICATE_OF_EXISTING_SALES_LEAD",
  NEW_OPPORTUNITY: "NEW_OPPORTUNITY",
  NEW_TIMING_CONTACT_COMPETITIVE_INTEL: "NEW_TIMING_CONTACT_COMPETITIVE_INTEL",
});

export const INCREMENTAL_VALUE_STATUS_LABEL = Object.freeze({
  UNKNOWN: "Unknown — awaiting hotel feedback",
  NEW_TO_HOTEL: "New to hotel",
  ALREADY_KNOWN_USEFUL_ADDITIONAL: "Already known / useful additional intelligence",
  ALREADY_KNOWN_NO_INCREMENTAL: "Already known / no incremental value",
  DUPLICATE_OF_EXISTING_SALES_LEAD: "Duplicate of existing sales lead",
  NEW_OPPORTUNITY: "New opportunity",
  NEW_TIMING_CONTACT_COMPETITIVE_INTEL: "New timing / contact / competitive intelligence",
});

export const COMPETITOR_CLASS = Object.freeze({
  STR_COMP_SET: "STR_COMP_SET",
  RELEVANT_GROUP_DEMAND_ALTERNATIVE: "RELEVANT_GROUP_DEMAND_ALTERNATIVE",
  OPPORTUNITY_MARKET: "OPPORTUNITY_MARKET",
});

export const COMPETITOR_CLASS_LABEL = Object.freeze({
  STR_COMP_SET: "STR Comp Set",
  RELEVANT_GROUP_DEMAND_ALTERNATIVE: "Relevant Group-Demand Alternative",
  OPPORTUNITY_MARKET: "Opportunity Market Competitor",
});

/** Hotel validation — Familiarity */
export const HOTEL_FAMILIARITY = Object.freeze([
  "Never Seen",
  "Familiar",
  "Already Received",
  "Already Pursuing",
]);

/** Hotel validation — Commercial Status */
export const HOTEL_COMMERCIAL_STATUS = Object.freeze([
  "Worth Pursuing",
  "Not a Fit",
  "Already Lost",
  "Already Won",
  "Already Booked Elsewhere",
]);

/** Hotel validation — Value */
export const HOTEL_VALUE_FEEDBACK = Object.freeze([
  "Excellent",
  "Useful",
  "Marginal",
  "No Incremental Value",
]);

export const SEGMENTS = Object.freeze([
  "Associations",
  "Medical",
  "Healthcare",
  "Scientific",
  "Corporate",
  "Government",
  "Government contractor",
  "Weekend group",
  "Social",
  "Sports",
  "University / education",
]);

/** Legacy quality labels — still accepted for older feedback rows */
export const FEEDBACK_QUALITY = Object.freeze([
  "Excellent Lead",
  "Worth Pursuing",
  "Maybe",
  "Not Useful",
  "Wrong / False Positive",
  ...HOTEL_VALUE_FEEDBACK,
]);

export const SALES_OUTCOME = Object.freeze([
  "Not Reviewed",
  "Researching",
  "Contacted",
  "Response Received",
  "RFP Received",
  "Site Visit",
  "Proposal",
  "Lost",
  "Won",
  "Not Pursued",
  ...HOTEL_COMMERCIAL_STATUS,
]);

export const SOURCE_TIERS = Object.freeze({
  A: "Tier_A",
  B: "Tier_B",
  C: "Tier_C",
  D: "Tier_D",
});

export const RESEARCH_LEVELS_GDI = Object.freeze({
  L1_EXISTING_KNOWLEDGE: "L1_EXISTING_DEALALITY_KNOWLEDGE",
  L2_CANONICAL_METHODS: "L2_CANONICAL_DEALALITY_RESEARCH_METHODS",
  L3_STANDARD_WEB: "L3_STANDARD_APPROVED_WEB_RESEARCH",
  L4_SUPPLEMENTAL: "L4_AMPFY_APIFY_SUPPLEMENTAL",
  L5_WEBHOUND: "L5_WEBHOUND_DEEP_RESEARCH",
});

/**
 * Hard Webhound ceiling per hotel research run (USD).
 * GDI pilot raised from $5 → $15 to test incremental commercial ROI.
 * Independent of HI Research Center pilot cap.
 */
export const GDI_WEBHOUND_HARD_CAP_USD = 15;

/**
 * Opportunity Type — what kind of hotel sales path exists (not just "an event exists").
 */
export const OPPORTUNITY_TYPE = Object.freeze({
  PRIMARY_PURSUIT: "PRIMARY_PURSUIT",
  OVERFLOW_HOUSING: "OVERFLOW_HOUSING",
  REACTIVATION: "REACTIVATION",
  FUTURE_CYCLE: "FUTURE_CYCLE",
  /** Private Events V1 — one known upcoming private event */
  SPECIFIC_PRIVATE_EVENT: "SPECIFIC_PRIVATE_EVENT",
  /** Private Events V1 — recurring venue demand source */
  VENUE_PARTNERSHIP: "VENUE_PARTNERSHIP",
  CLOSED_DISQUALIFIED: "CLOSED_DISQUALIFIED",
});

export const OPPORTUNITY_TYPE_LABEL = Object.freeze({
  PRIMARY_PURSUIT: "Primary Pursuit",
  OVERFLOW_HOUSING: "Overflow / Housing",
  REACTIVATION: "Reactivation",
  FUTURE_CYCLE: "Future Cycle",
  SPECIFIC_PRIVATE_EVENT: "Specific Private Event",
  VENUE_PARTNERSHIP: "Venue Partnership",
  CLOSED_DISQUALIFIED: "Closed / Disqualified",
});

/**
 * Venue / housing sourcing status — evidence-based gate before priority.
 * Distinct from SOURCING_STATUS (hotel-validation / public RFP evidence).
 */
export const VENUE_SOURCING_STATUS = Object.freeze({
  OPEN_UNRESOLVED: "OPEN_UNRESOLVED",
  RFP_ACTIVE_SOURCING: "RFP_ACTIVE_SOURCING",
  HOTEL_VENUE_TBD: "HOTEL_VENUE_TBD",
  PARTIALLY_PLACED: "PARTIALLY_PLACED",
  PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
  PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE: "PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE",
  FULLY_PLACED: "FULLY_PLACED",
  CURRENT_CYCLE_CLOSED: "CURRENT_CYCLE_CLOSED",
  UNKNOWN: "UNKNOWN",
});

export const VENUE_SOURCING_STATUS_LABEL = Object.freeze({
  OPEN_UNRESOLVED: "Open / Unresolved",
  RFP_ACTIVE_SOURCING: "RFP / Active Sourcing",
  HOTEL_VENUE_TBD: "Hotel / Venue TBD",
  PARTIALLY_PLACED: "Partially Placed",
  PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE: "Primary Venue Selected / Overflow Possible",
  PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE: "Primary Venue Selected / No Overflow Evidence",
  FULLY_PLACED: "Fully Placed",
  CURRENT_CYCLE_CLOSED: "Current Cycle Closed",
  UNKNOWN: "Unknown",
});

export const EVENT_LOCATION_STATUS = Object.freeze({
  VERIFIED_VENUE: "VERIFIED_VENUE",
  VERIFIED_CITY: "VERIFIED_CITY",
  REGION_ONLY: "REGION_ONLY",
  ESTIMATED: "ESTIMATED",
  UNKNOWN: "UNKNOWN",
});

export const EVENT_LOCATION_STATUS_LABEL = Object.freeze({
  VERIFIED_VENUE: "Verified Venue",
  VERIFIED_CITY: "Verified City",
  REGION_ONLY: "Region Only",
  ESTIMATED: "Estimated",
  UNKNOWN: "Unknown",
});

export const ROOM_DEMAND_STATUS = Object.freeze({
  VERIFIED_ROOM_BLOCK: "VERIFIED_ROOM_BLOCK",
  VERIFIED_HOUSING_PROGRAM: "VERIFIED_HOUSING_PROGRAM",
  STRONG_ROOM_DEMAND_EVIDENCE: "STRONG_ROOM_DEMAND_EVIDENCE",
  ESTIMATED_ROOM_DEMAND: "ESTIMATED_ROOM_DEMAND",
  LOCAL_LIMITED_ROOM_DEMAND: "LOCAL_LIMITED_ROOM_DEMAND",
  OVERFLOW_ONLY: "OVERFLOW_ONLY",
  UNKNOWN: "UNKNOWN",
});

export const ROOM_DEMAND_STATUS_LABEL = Object.freeze({
  VERIFIED_ROOM_BLOCK: "Verified Room Block",
  VERIFIED_HOUSING_PROGRAM: "Verified Housing Program",
  STRONG_ROOM_DEMAND_EVIDENCE: "Strong Room Demand Evidence",
  ESTIMATED_ROOM_DEMAND: "Estimated Room Demand",
  LOCAL_LIMITED_ROOM_DEMAND: "Local / Limited Room Demand",
  OVERFLOW_ONLY: "Overflow Only",
  UNKNOWN: "Unknown",
});

/** Role taxonomy (who they are relative to the opportunity). */
export const CONTACT_QUALITY = Object.freeze({
  NAMED_DECISION_MAKER: "NAMED_DECISION_MAKER",
  NAMED_EVENT_MEETINGS_CONTACT: "NAMED_EVENT_MEETINGS_CONTACT",
  HOUSING_SOURCING_CONTACT: "HOUSING_SOURCING_CONTACT",
  ASSOCIATION_MANAGEMENT_CONTACT: "ASSOCIATION_MANAGEMENT_CONTACT",
  GENERAL_ORGANIZATION_CONTACT: "GENERAL_ORGANIZATION_CONTACT",
  GENERIC_INBOX: "GENERIC_INBOX",
  NO_CONTACT: "NO_CONTACT",
});

export const CONTACT_QUALITY_LABEL = Object.freeze({
  NAMED_DECISION_MAKER: "Named Decision Maker",
  NAMED_EVENT_MEETINGS_CONTACT: "Named Event / Meetings Contact",
  HOUSING_SOURCING_CONTACT: "Housing / Sourcing Contact",
  ASSOCIATION_MANAGEMENT_CONTACT: "Association Management Contact",
  GENERAL_ORGANIZATION_CONTACT: "General Organization Contact",
  GENERIC_INBOX: "Generic Inbox",
  NO_CONTACT: "No Contact",
});

/**
 * Contact Quality Grade A–E — reachability + role relevance for sales action.
 * Separate from CONTACT_QUALITY role taxonomy and from Evidence Confidence.
 */
export const CONTACT_GRADE = Object.freeze({
  A: "A",
  B: "B",
  C: "C",
  D: "D",
  E: "E",
});

export const CONTACT_GRADE_LABEL = Object.freeze({
  A: "Grade A — named decision maker + verified email + useful phone",
  B: "Grade B — named event contact + verified email",
  C: "Grade C — named relevant person, incomplete or inferred details",
  D: "Grade D — generic inbox / main office only",
  E: "Grade E — no usable contact",
});

/** Target role match vs the opportunity (hotel selection / housing / sourcing). */
export const TARGET_ROLE_MATCH = Object.freeze({
  DIRECT_DECISION_MAKER: "DIRECT_DECISION_MAKER",
  EVENT_MEETINGS_OWNER: "EVENT_MEETINGS_OWNER",
  HOUSING_SOURCING_CONTACT: "HOUSING_SOURCING_CONTACT",
  EVENT_OPERATIONS_CONTACT: "EVENT_OPERATIONS_CONTACT",
  ASSOCIATION_MANAGEMENT_CONTACT: "ASSOCIATION_MANAGEMENT_CONTACT",
  EXECUTIVE_SPONSOR: "EXECUTIVE_SPONSOR",
  GENERAL_ORGANIZATION_CONTACT: "GENERAL_ORGANIZATION_CONTACT",
  UNKNOWN: "UNKNOWN",
});

export const TARGET_ROLE_MATCH_LABEL = Object.freeze({
  DIRECT_DECISION_MAKER: "Direct decision maker",
  EVENT_MEETINGS_OWNER: "Event / meetings owner",
  HOUSING_SOURCING_CONTACT: "Housing / sourcing contact",
  EVENT_OPERATIONS_CONTACT: "Event operations contact",
  ASSOCIATION_MANAGEMENT_CONTACT: "Association management contact",
  EXECUTIVE_SPONSOR: "Executive sponsor",
  GENERAL_ORGANIZATION_CONTACT: "General organization contact",
  UNKNOWN: "Unknown",
});

export const EMAIL_TYPE = Object.freeze({
  DIRECT_WORK: "DIRECT_WORK",
  ROLE_BASED: "ROLE_BASED",
  GENERIC_ORGANIZATION: "GENERIC_ORGANIZATION",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
});

export const EMAIL_TYPE_LABEL = Object.freeze({
  DIRECT_WORK: "Direct work",
  ROLE_BASED: "Role-based",
  GENERIC_ORGANIZATION: "Generic organization",
  INFERRED: "Inferred",
  UNKNOWN: "Unknown",
});

/** Customer-facing verification labels — never expose provider internals here. */
export const EMAIL_VERIFICATION_STATUS = Object.freeze({
  OFFICIAL_SOURCE_VERIFIED: "OFFICIAL_SOURCE_VERIFIED",
  PROVIDER_VERIFIED: "PROVIDER_VERIFIED",
  MULTI_SOURCE_CONFIRMED: "MULTI_SOURCE_CONFIRMED",
  DOMAIN_PATTERN_INFERRED: "DOMAIN_PATTERN_INFERRED",
  UNVERIFIED: "UNVERIFIED",
  INVALID: "INVALID",
});

export const EMAIL_VERIFICATION_STATUS_LABEL = Object.freeze({
  OFFICIAL_SOURCE_VERIFIED: "Official source verified",
  PROVIDER_VERIFIED: "Provider verified",
  MULTI_SOURCE_CONFIRMED: "Multi-source confirmed",
  DOMAIN_PATTERN_INFERRED: "Domain-pattern inferred",
  UNVERIFIED: "Unverified",
  INVALID: "Invalid",
});

export const PHONE_TYPE = Object.freeze({
  DIRECT: "DIRECT",
  OFFICE: "OFFICE",
  MOBILE_PUBLIC_PROFESSIONAL: "MOBILE_PUBLIC_PROFESSIONAL",
  EVENT_LINE: "EVENT_LINE",
  MAIN_ORGANIZATION: "MAIN_ORGANIZATION",
  UNKNOWN: "UNKNOWN",
});

export const PHONE_TYPE_LABEL = Object.freeze({
  DIRECT: "Direct",
  OFFICE: "Office",
  MOBILE_PUBLIC_PROFESSIONAL: "Mobile / public professional",
  EVENT_LINE: "Event line",
  MAIN_ORGANIZATION: "Main organization",
  UNKNOWN: "Unknown",
});

export const REACTIVATION_SIGNAL = Object.freeze({
  PRIOR_HOTEL_OPPORTUNITY: "PRIOR_HOTEL_OPPORTUNITY",
  PRIOR_MARRIOTT_OPPORTUNITY: "PRIOR_MARRIOTT_OPPORTUNITY",
  PRIOR_MARKET_PRESENCE: "PRIOR_MARKET_PRESENCE",
  PRIOR_COMPETITOR_USAGE: "PRIOR_COMPETITOR_USAGE",
  LAPSED_RELATIONSHIP: "LAPSED_RELATIONSHIP",
  NONE: "NONE",
  UNKNOWN: "UNKNOWN",
});

export const REACTIVATION_SIGNAL_LABEL = Object.freeze({
  PRIOR_HOTEL_OPPORTUNITY: "Prior Hotel Opportunity",
  PRIOR_MARRIOTT_OPPORTUNITY: "Prior Marriott Opportunity",
  PRIOR_MARKET_PRESENCE: "Prior Market Presence",
  PRIOR_COMPETITOR_USAGE: "Prior Competitor Usage",
  LAPSED_RELATIONSHIP: "Lapsed Relationship",
  NONE: "None",
  UNKNOWN: "Unknown",
});

/**
 * Opportunity Qualification — separate from Hotel Fit.
 * Hotel Fit = "Could the hotel serve it?" · Qualification = "Is there something to pursue?"
 */
export const OPPORTUNITY_QUALIFICATION = Object.freeze({
  VERIFIED_OPEN: "VERIFIED_OPEN",
  STRONG: "STRONG",
  MODERATE: "MODERATE",
  WEAK: "WEAK",
  CLOSED: "CLOSED",
});

export const OPPORTUNITY_QUALIFICATION_LABEL = Object.freeze({
  VERIFIED_OPEN: "Verified Open",
  STRONG: "Strong",
  MODERATE: "Moderate",
  WEAK: "Weak",
  CLOSED: "Closed",
});

/** Evaluation labels for false positives / weak leads — learning, not auto-retrain. */
export const QUALIFICATION_FAILURE_REASON = Object.freeze({
  VENUE_ALREADY_SELECTED: "VENUE_ALREADY_SELECTED",
  HOTEL_ALREADY_SELECTED: "HOTEL_ALREADY_SELECTED",
  NO_OVERFLOW_OPPORTUNITY: "NO_OVERFLOW_OPPORTUNITY",
  EVENT_LOCATION_POOR_FIT: "EVENT_LOCATION_POOR_FIT",
  ROOM_DEMAND_TOO_SMALL: "ROOM_DEMAND_TOO_SMALL",
  MOST_ATTENDEES_LOCAL: "MOST_ATTENDEES_LOCAL",
  WRONG_EVENT_CYCLE: "WRONG_EVENT_CYCLE",
  CONTACT_NOT_RELEVANT: "CONTACT_NOT_RELEVANT",
  DUPLICATE: "DUPLICATE",
  EVENT_NO_LONGER_ACTIVE: "EVENT_NO_LONGER_ACTIVE",
  TOO_EARLY: "TOO_EARLY",
  TOO_LATE: "TOO_LATE",
  WEAK_EVIDENCE: "WEAK_EVIDENCE",
  NOT_HOTEL_DEMAND: "NOT_HOTEL_DEMAND",
  OTHER: "OTHER",
});

export const QUALIFICATION_FAILURE_REASON_LABEL = Object.freeze({
  VENUE_ALREADY_SELECTED: "Venue already selected",
  HOTEL_ALREADY_SELECTED: "Hotel already selected",
  NO_OVERFLOW_OPPORTUNITY: "No overflow opportunity",
  EVENT_LOCATION_POOR_FIT: "Event location poor fit",
  ROOM_DEMAND_TOO_SMALL: "Room demand too small",
  MOST_ATTENDEES_LOCAL: "Most attendees local",
  WRONG_EVENT_CYCLE: "Wrong event cycle",
  CONTACT_NOT_RELEVANT: "Contact not relevant",
  DUPLICATE: "Duplicate",
  EVENT_NO_LONGER_ACTIVE: "Event no longer active",
  TOO_EARLY: "Too early",
  TOO_LATE: "Too late",
  WEAK_EVIDENCE: "Weak evidence",
  NOT_HOTEL_DEMAND: "Not hotel demand",
  OTHER: "Other",
});
