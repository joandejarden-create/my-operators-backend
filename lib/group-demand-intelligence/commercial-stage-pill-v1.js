/**
 * GDI Universal Commercial Stage Pill V1
 *
 * Customer-facing middle tile pill — exactly four approved values:
 *   Pursue Now | Future Capture | Relationship Development | Qualification Needed
 *
 * Maps legacy inventory lanes / Events stages / sales-action residue into the
 * four-stage contract. Sales-action strategies remain separate derived fields.
 * Presentation only — does not change qualification law.
 */

import {
  COMMERCIAL_STAGE_FILTER,
  COMMERCIAL_STAGE_FILTER_LABEL,
  normalizeCommercialStageForFilter,
} from "./sales-opportunity-quick-filters-v1.js";

export const COMMERCIAL_STAGE_PILL_V1 = "gdi-commercial-stage-pill-v1";

export const CANONICAL_COMMERCIAL_STAGE = COMMERCIAL_STAGE_FILTER;

export const CANONICAL_COMMERCIAL_STAGE_LABEL = COMMERCIAL_STAGE_FILTER_LABEL;

/** CSS token classes for the four customer-facing stage pills. */
export const COMMERCIAL_STAGE_PILL_CLASS = Object.freeze({
  [COMMERCIAL_STAGE_FILTER.PURSUE_NOW]: "gdi-pill gdi-pill-stage-pursue",
  [COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE]: "gdi-pill gdi-pill-stage-future",
  [COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT]:
    "gdi-pill gdi-pill-stage-relationship",
  [COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED]:
    "gdi-pill gdi-pill-stage-qualify",
});

function upper(v) {
  return String(v || "")
    .trim()
    .toUpperCase();
}

/**
 * Map any legacy stage / lane / sales-action residue → four-stage key.
 * Returns null only when no structured signal exists at all.
 */
export function mapLegacyToCanonicalCommercialStage(rawKey) {
  const key = upper(rawKey);
  if (!key || key === "EXCLUDED") {
    // EXCLUDED inventory still needs a customer stage when visible.
    if (key === "EXCLUDED") {
      return COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED;
    }
    return null;
  }
  if (
    key === "PURSUE_NOW" ||
    key === "SALES_OPPORTUNITY" ||
    key === "CONTACT_NOW"
  ) {
    return COMMERCIAL_STAGE_FILTER.PURSUE_NOW;
  }
  if (
    key === "FUTURE_CAPTURE" ||
    key === "FUTURE_OPPORTUNITY" ||
    key === "MONITOR" ||
    key === "TOO_EARLY" ||
    key === "WATCH" ||
    key === "FUTURE_WATCH"
  ) {
    return COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE;
  }
  if (
    key === "RELATIONSHIP_DEVELOPMENT" ||
    key === "ACCOUNT_DEVELOPMENT" ||
    key === "BUILD_ACCOUNT" ||
    key === "BUILD_VENUE_PARTNERSHIP" ||
    key === "ENGAGE_PLANNER" ||
    key === "WIN_ROOM_BLOCK" ||
    key === "CONTACT_BUYER"
  ) {
    return COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT;
  }
  if (
    key === "QUALIFICATION_NEEDED" ||
    key === "RESEARCH_NEEDED" ||
    key === "RESEARCH_WATCHLIST" ||
    key === "QUALIFY_OPPORTUNITY" ||
    key === "PURSUE_CATERING" ||
    key === "EXPLORATORY"
  ) {
    // Pursue Catering is a sales motion, not act-now stage by itself.
    // Default to Qualification Needed unless stronger stage evidence exists
    // upstream (normalizeCommercialStageForFilter / inventory lane).
    if (key === "PURSUE_CATERING") {
      return COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED;
    }
    return COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED;
  }
  return null;
}

/**
 * Resolve customer-facing commercial stage pill (always one of four).
 * @returns {{
 *   key: string,
 *   label: string,
 *   pillClass: string,
 *   show: true,
 *   source: string,
 *   missingMapped: boolean,
 *   version: string
 * }}
 */
export function resolveCustomerCommercialStagePill(opportunity = {}) {
  // 1) Authoritative filter normalizer (inventoryLane / commercialStageKey).
  let key = normalizeCommercialStageForFilter(opportunity);
  let source = "normalizeCommercialStageForFilter";

  // 2) Direct legacy key map from stage / lane / salesAction residue.
  if (!key) {
    const candidates = [
      opportunity.commercialStageFilterKey,
      opportunity.commercialStageKey,
      opportunity.tileBadgePlan?.commercialStageKey,
      opportunity.inventoryLane,
      opportunity.salesActionKey,
    ];
    for (const c of candidates) {
      const mapped = mapLegacyToCanonicalCommercialStage(c);
      if (mapped) {
        key = mapped;
        source = `legacy_map:${String(c)}`;
        break;
      }
    }
  }

  // 3) Events stage vocabulary already on the row.
  if (!key) {
    const eventsKey = upper(
      opportunity.eventsCommercialStage ||
        opportunity.eventsTileCommercialStage ||
        "",
    );
    const mapped = mapLegacyToCanonicalCommercialStage(eventsKey);
    if (mapped) {
      key = mapped;
      source = "events_stage";
    }
  }

  let missingMapped = false;
  if (!key) {
    key = COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED;
    source = "missing_stage_fallback";
    missingMapped = true;
  }

  return {
    key,
    label: CANONICAL_COMMERCIAL_STAGE_LABEL[key],
    pillClass: COMMERCIAL_STAGE_PILL_CLASS[key],
    show: true,
    source,
    missingMapped,
    version: COMMERCIAL_STAGE_PILL_V1,
  };
}
