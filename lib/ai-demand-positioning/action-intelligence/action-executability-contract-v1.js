/**
 * ADP Action Intelligence V1 — executability contract.
 * Final customer actions must be operational playbooks, never generic slogans.
 */

export const ADP_ACTION_INTELLIGENCE_VERSION = "ADP_ACTION_INTELLIGENCE_V1";

export const ACTION_PATTERN_LIFECYCLE = Object.freeze({
  DISCOVERED: "DISCOVERED",
  DRAFT: "DRAFT",
  REVIEWED: "REVIEWED",
  GOVERNED: "GOVERNED",
  REUSABLE: "REUSABLE",
});

/** Category labels only — never emit as final customer action text. */
export const BANNED_GENERIC_ACTION_PATTERNS = Object.freeze([
  /\bimprove parking information\b/i,
  /\bimprove family content\b/i,
  /\bclarify pet policy\b/i,
  /\bstrengthen (pet|accessibility|local) (information|content|positioning)\b/i,
  /\bimprove meetings? information\b/i,
  /\boptimize tripadvisor\b/i,
  /\bimprove ota content\b/i,
  /\bstrengthen overall ai authority\b/i,
  /\bpublish clearer parking\b/i,
  /\bfix accessibility details\b/i,
  /\bimprove ai representation of\b/i,
  /\baddress displacement by\b/i,
  /\bpursue \d+ high-potential\b/i,
]);

export const REQUIRED_EXECUTABLE_FIELDS = Object.freeze([
  "actionTitle",
  "observedIssue",
  "evidence",
  "targetSources",
  "implementationSteps",
  "accountableOwnerRole",
  "supportingTeam",
  "targetDate",
  "definitionOfDone",
  "expectedSignal",
  "nextMonitoringCheck",
  "actionPatternId",
  "trace",
]);

export const ACTION_INTELLIGENCE_GATES = Object.freeze([
  "ADP_ACTION_EXECUTABILITY_STANDARD",
  "ADP_ACTION_DEFINITION_OF_DONE_COMPLETE",
  "ADP_ACTION_TRACEABILITY",
  "ADP_ACTION_TARGET_SOURCE_SPECIFICITY",
  "ADP_ACTION_IMPLEMENTATION_STEPS_COMPLETE",
  "ADP_ACTION_PATTERN_LEARNING_STRUCTURE_COMPLETE",
  "ADP_ACTION_NO_GENERIC_FINAL_RECOMMENDATIONS",
]);

export const SOURCE_CATALOG = Object.freeze({
  HOTEL_WEBSITE: "Hotel Website",
  BRAND_BOOKING: "Brand Booking Page",
  GOOGLE_BUSINESS_PROFILE: "Google Business Profile",
  TRIPADVISOR: "TripAdvisor",
  BOOKING_COM: "Booking.com",
  EXPEDIA: "Expedia",
  MEETING_FACT_SHEET: "Meeting / Event Fact Sheet",
  OTA_ROOM_TYPES: "OTA Room-Type Descriptions",
  FAQ: "Property FAQ",
  LOCAL_AUTHORITY: "Authoritative Local Source",
});

export function isBannedGenericActionText(text) {
  const s = String(text || "");
  return BANNED_GENERIC_ACTION_PATTERNS.some((re) => re.test(s));
}
