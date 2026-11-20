/**
 * Suggest commercially relevant ADP attributes from verified hotel capabilities
 * and demand-segment relevance. Suggestions are never auto-approved.
 */

import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import {
  mapAttributeProvenance,
  PROVENANCE_CLASS,
} from "../property-attributes/adp-attribute-provenance-mapper-v1.js";

export const ADP_ATTRIBUTE_SUGGESTION_CANDIDATES_V1 =
  "ADP_ATTRIBUTE_SUGGESTION_CANDIDATES_V1";

/** Segment → governed attribute keys (recommendation only; not RG weights). */
export const DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES = Object.freeze({
  corporate_meetings: ["meeting_space", "ballroom", "large_ballroom", "business_center", "full_service"],
  business_transient: ["full_service", "fitness_center_24hr", "fitness_center", "concierge", "executive_lounge"],
  associations_conventions: ["meeting_space", "ballroom", "large_ballroom", "event_space_outdoor"],
  medical_pharma: ["nih_access", "dc_metro_access", "meeting_space", "full_service", "montgomery_county"],
  family_leisure: ["family_friendly", "outdoor_pool", "heated_outdoor_pool", "suite_inventory"],
  luxury_leisure: ["luxury", "spa", "full_service_spa", "fine_dining", "five_diamond", "beachfront"],
  weddings_honeymoons: ["wedding_venue", "honeymoon_destination", "event_space_outdoor", "spa"],
  airport_travel: ["urban_lifestyle", "design_forward", "meeting_space", "fitness_center"],
  wellness_destination: ["spa", "full_service_spa", "wellness", "outdoor_pool"],
  soft_brand_collection: ["soft_brand", "boutique_feel", "luxury"],
});

const PROPERTY_SEGMENT_HINTS = Object.freeze({
  adp_bethesda_marriott: [
    "corporate_meetings",
    "business_transient",
    "associations_conventions",
    "medical_pharma",
  ],
  adp_spice_island_beach_resort: [
    "luxury_leisure",
    "weddings_honeymoons",
    "family_leisure",
    "wellness_destination",
    "soft_brand_collection",
  ],
  adp_yotel_geneva_lake: ["airport_travel", "corporate_meetings", "business_transient"],
});

function isVerified(row) {
  if (!row) return false;
  if (String(row.verificationStatus || "").startsWith("VERIFIED_")) return true;
  return [
    PROVENANCE_CLASS.VERIFIED_EXISTING_SOURCE,
    PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_BRAND,
    PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_AFFILIATION,
    PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_PROPERTY_TYPE,
    PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_LOCATION,
    PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_MEETING_SPACE,
    PROVENANCE_CLASS.VERIFIED_FROM_PROFILE_STEWARDSHIP,
    PROVENANCE_CLASS.VERIFIED_EXISTENCE_ONLY,
  ].includes(row.class);
}

/**
 * @param {object} propertyProfile
 * @param {object} [opts]
 */
export function suggestAttributeCandidates(propertyProfile, { selectedKeys = null } = {}) {
  const propertyId = propertyProfile?.propertyId || null;
  const selected = new Set(
    Array.isArray(selectedKeys)
      ? selectedKeys
      : Array.isArray(propertyProfile?.attributes)
        ? propertyProfile.attributes
        : []
  );
  const segments = PROPERTY_SEGMENT_HINTS[propertyId] || [];
  // Only segment-relevant candidates — never scan the full registry (avoids false
  // brand/geo matches manufacturing irrelevant suggestions to "fill" the minimum).
  const candidateKeys = new Set();
  for (const seg of segments) {
    for (const key of DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES[seg] || []) {
      candidateKeys.add(key);
    }
  }

  const suggestions = [];
  for (const key of candidateKeys) {
    if (selected.has(key)) continue;
    const def = ATTRIBUTE_DEFINITIONS[key];
    if (!def) continue;
    const provenance = mapAttributeProvenance(key, propertyProfile);
    const verified = isVerified(provenance);
    const relevantSegments = segments.filter((seg) =>
      (DEMAND_SEGMENT_ATTRIBUTE_CANDIDATES[seg] || []).includes(key)
    );
    suggestions.push({
      attribute: key,
      label: def.label,
      suggestedPriority: def.priority,
      supportingFact: provenance.note || null,
      evidenceUrl: provenance.evidenceUrl || null,
      verificationStatus: provenance.verificationStatus,
      verifiedSupport: verified,
      relevantSegments,
      reason: `Relevant to demand segments: ${relevantSegments.join(", ")}`,
      measurementDefinitionAvailable: true,
      suggestedInclusionStatus: verified ? "PROPOSED_ADDITION" : "PROPOSED_PENDING_VERIFICATION",
      autoApproved: false,
    });
  }

  suggestions.sort((a, b) => {
    if (a.verifiedSupport !== b.verifiedSupport) return a.verifiedSupport ? -1 : 1;
    const p = { high: 0, medium: 1, low: 2 };
    return (p[a.suggestedPriority] ?? 3) - (p[b.suggestedPriority] ?? 3);
  });

  return {
    contractVersion: ADP_ATTRIBUTE_SUGGESTION_CANDIDATES_V1,
    propertyId,
    demandSegmentsConsidered: segments,
    selectedCount: selected.size,
    suggestionCount: suggestions.length,
    suggestions,
    note:
      "Suggestions never overwrite existing attributes[]. Approval required before selection change.",
  };
}
