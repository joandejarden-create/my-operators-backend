/**
 * Autopilot V1 field routing — every researchable Census field from the frozen contract.
 * Routes via FIELD_RESEARCH_PLANS + source-lane preferences. No hardcoded short list.
 */

import { buildFieldContractEntries } from "../production-census-field-contract-v111.js";
import { FIELD_RESEARCH_PLANS } from "../clean-census/field-research.js";
import { SOURCE_LANE } from "./constants.js";

/** Primaries that Autopilot should attempt to research (not pure governance / link-inverse). */
const RESEARCHABLE_PRIMARIES = new Set([
  "contract_required",
  "contract_optional",
  "enrichment_target",
  "source_only",
]);

const NON_RESEARCHABLE_NAMES = new Set([
  "Hotel Property Source Evidence",
  "Hotel Property Brand Affiliations",
  "Hotel Property Steward Review",
  "Brand Explorer Slug if mapped",
  "VIC Freeze Hash",
  "Possible Operator Target",
  "Possible Soft-Brand Candidate",
  "Possible Brand Conversion Candidate",
  "Possible Owner Outreach Target",
  "Possible Financing Target",
  "Possible Dealality Opportunity",
]);

/** Explicit field → research plan key + preferred lane. */
const FIELD_ROUTE_OVERRIDES = Object.freeze({
  "Rooms / Keys": {
    plan_key: "rooms",
    preferred_lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    plan: FIELD_RESEARCH_PLANS.rooms,
  },
  "Opening Date": {
    plan_key: "openDate",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    plan: FIELD_RESEARCH_PLANS.openDate,
  },
  "Amenities - Source Text": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    plan: FIELD_RESEARCH_PLANS.amenities,
  },
  "Amenities - Structured Tags": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    plan: FIELD_RESEARCH_PLANS.amenities,
  },
  "F&B Flag": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    note: "Official dining / property amenities pages",
  },
  "Meeting Space Flag": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    note: "Official meetings/events pages",
  },
  "Spa Flag": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    note: "Property amenities/wellness — over-modeled flag; research evidence only",
  },
  "Pool Flag": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
  },
  "Fitness Flag": {
    plan_key: "amenities",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
  },
  "Operator / Management Company": {
    plan_key: "managementCompany",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    plan: FIELD_RESEARCH_PLANS.managementCompany,
    escalate_opaque: true,
  },
  "Owner Name": {
    plan_key: "owner",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    plan: FIELD_RESEARCH_PLANS.owner,
    escalate_opaque: true,
  },
  Latitude: {
    plan_key: "coordinates",
    preferred_lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    plan: FIELD_RESEARCH_PLANS.coordinates,
  },
  Longitude: {
    plan_key: "coordinates",
    preferred_lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    plan: FIELD_RESEARCH_PLANS.coordinates,
  },
  "Current Brand": {
    plan_key: "brand",
    preferred_lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    note: "Official property / brand directory",
  },
  "Affiliation Status": {
    plan_key: "status",
    preferred_lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    note: "Official bookable / current property page",
  },
  "Market / Submarket": {
    plan_key: "marketSubmarket",
    preferred_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
    plan: FIELD_RESEARCH_PLANS.marketSubmarket,
  },
});

/**
 * @param {object} entry contract entry
 */
export function isResearchableField(entry) {
  if (!entry || NON_RESEARCHABLE_NAMES.has(entry.name)) return false;
  if (entry.primary === "governance_field") return false;
  if (entry.primary === "internal_only" && !(entry.tags || []).includes("source_only")) return false;
  return RESEARCHABLE_PRIMARIES.has(entry.primary);
}

/**
 * Build full field routing registry from frozen contract.
 */
export function buildFieldRoutingRegistry() {
  const contract = buildFieldContractEntries();
  const researchable = [];
  const non_researchable = [];

  for (const entry of contract) {
    const row = {
      field: entry.name,
      group: entry.group,
      primary: entry.primary,
      tags: entry.tags || [],
      notes: entry.notes || "",
      researchable: isResearchableField(entry),
      route: null,
    };

    if (row.researchable) {
      const override = FIELD_ROUTE_OVERRIDES[entry.name] || null;
      row.route = {
        preferred_lane: override?.preferred_lane || defaultLaneForGroup(entry.group),
        fallback_lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
        escalation_lane: SOURCE_LANE.C_DEEP_ESCALATION,
        plan_key: override?.plan_key || null,
        plan: override?.plan || null,
        note: override?.note || entry.notes || null,
        escalate_opaque: Boolean(override?.escalate_opaque),
        never_sources: ["legacy_census", "cvent", "str_client_derived"],
      };
      researchable.push(row);
    } else {
      non_researchable.push(row);
    }
  }

  return {
    version: "census-autopilot-v1-field-routing-registry",
    contract_source: "production-census-field-contract-v111.js#buildFieldContractEntries",
    plans_source: "clean-census/field-research.js#FIELD_RESEARCH_PLANS",
    researchable_count: researchable.length,
    non_researchable_count: non_researchable.length,
    researchable,
    non_researchable,
  };
}

function defaultLaneForGroup(group) {
  if (/Brand|Affiliation|Source Evidence|Core Identity/i.test(group || "")) {
    return SOURCE_LANE.A_STRUCTURED_OFFICIAL;
  }
  if (/Owner|Operator|Asset/i.test(group || "")) {
    return SOURCE_LANE.B_INDEPENDENT_PROPERTY;
  }
  return SOURCE_LANE.B_INDEPENDENT_PROPERTY;
}
