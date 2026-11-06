/**
 * ADP_HOTEL_ATTRIBUTE_INTELLIGENCE_STANDARD_V1
 * Canonical taxonomy + coverage thresholds for Reality Gap property intelligence.
 */

import { collapseSynonymAttributesV2 } from "./adp-reality-gap-canonical-attribute-dedup-v1.js";

export const ADP_HOTEL_ATTRIBUTE_INTELLIGENCE_STANDARD_V1 =
  "ADP_HOTEL_ATTRIBUTE_INTELLIGENCE_STANDARD_V1";

export const ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_CERTIFICATION_V1 =
  "ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_CERTIFICATION_V1";

export const ADP_REALITY_GAP_INDEPENDENT_CERTIFICATION_V1 =
  "ADP_REALITY_GAP_INDEPENDENT_CERTIFICATION_V1";

/** Categories A–H */
export const ATTRIBUTE_CATEGORIES_V1 = Object.freeze({
  A_IDENTITY_POSITIONING: "A_IDENTITY_POSITIONING",
  B_LOCATION_ACCESSIBILITY: "B_LOCATION_ACCESSIBILITY",
  C_GUEST_ACCOMMODATIONS: "C_GUEST_ACCOMMODATIONS",
  D_MEETINGS_GROUPS: "D_MEETINGS_GROUPS",
  E_FOOD_BEVERAGE: "E_FOOD_BEVERAGE",
  F_WELLNESS_LEISURE: "F_WELLNESS_LEISURE",
  G_BUSINESS_GUEST_SERVICES: "G_BUSINESS_GUEST_SERVICES",
  H_PROPERTY_DIFFERENTIATION: "H_PROPERTY_DIFFERENTIATION",
});

export const ATTRIBUTE_FACT_STATE_V1 = Object.freeze({
  VERIFIED_PRESENT: "VERIFIED_PRESENT",
  VERIFIED_ABSENT: "VERIFIED_ABSENT",
  CONFLICTING: "CONFLICTING",
  UNVERIFIED: "UNVERIFIED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  RESEARCH_PENDING: "RESEARCH_PENDING",
});

export const ATTRIBUTE_MEASUREMENT_STATE_V1 = Object.freeze({
  VERIFIED_ATTRIBUTE_NOT_TESTED: "VERIFIED_ATTRIBUTE_NOT_TESTED",
  TESTED_RECOGNIZED: "TESTED_RECOGNIZED",
  TESTED_PARTIALLY_RECOGNIZED: "TESTED_PARTIALLY_RECOGNIZED",
  TESTED_NOT_RECOGNIZED: "TESTED_NOT_RECOGNIZED",
  TESTED_MISREPRESENTED: "TESTED_MISREPRESENTED",
  INSUFFICIENT_OBSERVATION_EVIDENCE: "INSUFFICIENT_OBSERVATION_EVIDENCE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

/**
 * Coverage floors by property class (verified PRESENT attributes only).
 * Synonym labels do not inflate counts.
 */
export const ATTRIBUTE_COVERAGE_THRESHOLDS_V1 = Object.freeze({
  FULL_SERVICE_URBAN: { min: 20, targetLow: 30, targetHigh: 40 },
  FULL_SERVICE_RESORT: { min: 25, targetLow: 35, targetHigh: 45 },
  BOUTIQUE_LIFESTYLE: { min: 15, targetLow: 20, targetHigh: 30 },
  SELECT_OR_LIMITED: { min: 12, targetLow: 15, targetHigh: 25 },
  SPECIALIZED: { min: 12, targetLow: 15, targetHigh: 25 },
});

/**
 * Synonym collapse — count once per dimension (coverage scoring).
 * Reality Gap customer cards use ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1
 * (restaurant↔on_site_restaurant; meeting_space_small and heavenly_spa stay distinct).
 */
export const ATTRIBUTE_SYNONYM_GROUPS_V1 = Object.freeze([
  Object.freeze(["luxury", "urban_luxury", "five_diamond"]),
  Object.freeze(["full_service", "full_service_hotel"]),
  Object.freeze(["upscale", "upper_upscale"]),
  Object.freeze(["meeting_space", "meetings", "meeting_rooms"]),
  Object.freeze(["spa", "full_service_spa"]),
  Object.freeze(["fitness_center", "fitness_center_24hr", "wellness_fitness", "wellness"]),
  Object.freeze(["beachfront", "beach_access", "oceanfront", "private_beach"]),
  Object.freeze(["large_ballroom", "ballroom"]),
  Object.freeze(["on_site_restaurant", "restaurant"]),
  Object.freeze(["historic_property", "historic_building"]),
]);

export function classifyPropertyCoverageClass(profile) {
  const type = String(profile?.propertyType || "").toLowerCase();
  const scale = String(profile?.chainScale || "").toLowerCase();
  const attrs = profile?.attributes || [];
  const blob = [
    profile?.name,
    profile?.positioning?.primary,
    ...(profile?.positioning?.differentiators || []),
    ...attrs,
  ]
    .join(" ")
    .toLowerCase();

  // Explicit lifestyle / soft-brand / design hotels before resort heuristic
  if (
    /boutique|lifestyle|nobu|yotel|design hotel|independent|vignette|curio|autograph/.test(blob) ||
    type.includes("boutique") ||
    type.includes("lifestyle")
  ) {
    return "BOUTIQUE_LIFESTYLE";
  }
  if (
    (/resort|beachfront|golf resort|spa resort|island resort/.test(blob) || type.includes("resort")) &&
    !/not a beach|not a .*resort/.test(blob)
  ) {
    return "FULL_SERVICE_RESORT";
  }
  if (/select.?service|limited.?service|extended.?stay/.test(blob + " " + type)) {
    return "SELECT_OR_LIMITED";
  }
  if (scale.includes("luxury") || type.includes("full")) {
    return "FULL_SERVICE_URBAN";
  }
  return "FULL_SERVICE_URBAN";
}

export function collapseSynonymAttributes(attributeIds) {
  // Align coverage counting with Reality Gap canonical dedup registry.
  return collapseSynonymAttributesV2(attributeIds);
}

export function coverageScoreForProfile(profile, governedAttributeIds) {
  const cls = classifyPropertyCoverageClass(profile);
  const thresholds = ATTRIBUTE_COVERAGE_THRESHOLDS_V1[cls];
  const unique = collapseSynonymAttributes(governedAttributeIds);
  const n = unique.length;
  const met = n >= thresholds.min;
  return {
    coverageClass: cls,
    thresholds,
    verifiedPresentUnique: n,
    metMinimum: met,
    status: met
      ? n >= thresholds.targetLow
        ? "MEETS_TARGET_BAND"
        : "MEETS_MINIMUM"
      : "BELOW_MINIMUM",
  };
}

export function standardMarkdown() {
  return `# ADP_HOTEL_ATTRIBUTE_INTELLIGENCE_STANDARD_V1

Contract: \`${ADP_HOTEL_ATTRIBUTE_INTELLIGENCE_STANDARD_V1}\`

## Purpose
Commercially useful, differentiated hotel attribute intelligence for Reality Gap — not generic descriptor inflation.

## Source of truth (do not fork)
1. **Hotel facts (census):** Hotel Property Census Airtable (\`appCCUsuGsE1ifoLk\` / \`tbl9aY5ijiuIzzWam\`) — Amenities Source Text / Structured Tags / strategic flags.
2. **ADP governed attributes (measurement):** \`fixtures/ai-demand-positioning/*-property-profile.json\` \`attributes[]\` keys registered in \`ATTRIBUTE_DEFINITIONS\`.
3. **Durable attribute-set artifacts:** \`data/ai-demand-positioning/attribute-sets/{propertyId}/\`.
4. **AI recognition (derived):** Core observation \`attributesRecognized\` via response-parser keywords — never manually edited as a fact.

## Categories
${Object.entries(ATTRIBUTE_CATEGORIES_V1)
  .map(([k, v]) => `- **${k}:** \`${v}\``)
  .join("\n")}

## Fact states
${Object.keys(ATTRIBUTE_FACT_STATE_V1)
  .map((k) => `- \`${k}\``)
  .join("\n")}

## Measurement states (eligibility ≠ recognition)
${Object.keys(ATTRIBUTE_MEASUREMENT_STATE_V1)
  .map((k) => `- \`${k}\``)
  .join("\n")}

**Do not convert VERIFIED_ATTRIBUTE_NOT_TESTED into an AI miss.**

## Coverage thresholds (verified PRESENT, synonym-collapsed)
| Class | Min | Target |
|-------|----:|--------|
| Full-service urban | 20 | 30–40 |
| Full-service resort | 25 | 35–45 |
| Boutique / lifestyle | 15 | 20–30 |
| Select / limited | 12 | 15–25 |

## Certification gates
- \`${ADP_PROPERTY_ATTRIBUTE_INTELLIGENCE_CERTIFICATION_V1}\`
- \`${ADP_REALITY_GAP_INDEPENDENT_CERTIFICATION_V1}\`
`;
}
