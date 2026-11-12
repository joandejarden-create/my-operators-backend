/**
 * Shared Opportunity Type browse filter categories.
 * Primary classification: structured fields (researchSegment, demandSegmentId,
 * opportunityType, demandSignalType, demandFamily). Title matching is never primary.
 *
 * Aligned to TWELVE_DEMAND_SEGMENT_REGISTRY_V1 customer-facing filters.
 */

import { resolveDemandSegment } from "./demand-segments/eight-demand-segment-registry-v1.js";
import {
  INVENTORY_POPULATION,
  INVENTORY_POPULATION_LABEL,
  isInventoryPopulationValue,
} from "./bethesda-67-sales-baseline-cmi-v1.js";

export const OPPORTUNITY_TYPE_FILTER_VERSION = "gdi-opportunity-type-filter-v3";

export { INVENTORY_POPULATION, INVENTORY_POPULATION_LABEL, isInventoryPopulationValue };

export const OPPORTUNITY_TYPE_FILTER = Object.freeze({
  ALL: "",
  /** Inventory populations (Opportunity Type dropdown — not demand segments). */
  SALES_OPPORTUNITIES: INVENTORY_POPULATION.SALES_OPPORTUNITIES,
  COMPETITIVE_MARKET_INTELLIGENCE:
    INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE,
  ALL_COMMERCIAL_INTELLIGENCE:
    INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE,
  SCALING_COMPANY: "SCALING_COMPANY",
  ASSOCIATION_CONFERENCE_GROUP: "ASSOCIATION_CONFERENCE_GROUP",
  CORPORATE_MEETINGS_INCENTIVES: "CORPORATE_MEETINGS_INCENTIVES",
  GOVERNMENT_PUBLIC_SECTOR: "GOVERNMENT_PUBLIC_SECTOR",
  SPORTS_ENTERTAINMENT: "SPORTS_ENTERTAINMENT",
  EDUCATION_SCIENTIFIC: "EDUCATION_SCIENTIFIC",
  PROJECTS_INFRASTRUCTURE: "PROJECTS_INFRASTRUCTURE",
  SOCIAL_SPECIAL_EVENTS: "SOCIAL_SPECIAL_EVENTS",
  LOCAL_CATERING_DAY_MEETINGS: "LOCAL_CATERING_DAY_MEETINGS",
  CORPORATE_CELEBRATIONS_PRIVATE_EVENTS: "CORPORATE_CELEBRATIONS_PRIVATE_EVENTS",
  TRAVEL_TRADE_GROUP_INTERMEDIARIES: "TRAVEL_TRADE_GROUP_INTERMEDIARIES",
  RELOCATION_EXTENDED_STAY: "RELOCATION_EXTENDED_STAY",
  // Segment 8 subtypes + legacy aliases retained for older inventories / UI options
  PREFERRED_LODGING_PARTNERSHIP: "PREFERRED_LODGING_PARTNERSHIP",
  PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP:
    "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP",
  LOCAL_MEETINGS_CATERING: "LOCAL_MEETINGS_CATERING",
  CORPORATE_MEETINGS_TRAINING: "CORPORATE_MEETINGS_TRAINING",
  SPORTS_GROUP: "SPORTS_GROUP",
  SPECIFIC_PRIVATE_EVENT: "SPECIFIC_PRIVATE_EVENT",
  OTHER_UNCLASSIFIED: "OTHER_UNCLASSIFIED",
});

export const OPPORTUNITY_TYPE_FILTER_LABEL = Object.freeze({
  [OPPORTUNITY_TYPE_FILTER.SALES_OPPORTUNITIES]:
    INVENTORY_POPULATION_LABEL[INVENTORY_POPULATION.SALES_OPPORTUNITIES],
  [OPPORTUNITY_TYPE_FILTER.COMPETITIVE_MARKET_INTELLIGENCE]:
    INVENTORY_POPULATION_LABEL[
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
    ],
  [OPPORTUNITY_TYPE_FILTER.ALL_COMMERCIAL_INTELLIGENCE]:
    INVENTORY_POPULATION_LABEL[
      INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE
    ],
  [OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY]: "Scaling Company",
  [OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP]:
    "Association / Conference / Events",
  [OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_INCENTIVES]:
    "Corporate Meetings & Incentives",
  [OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR]: "Government & Public Sector",
  [OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT]: "Sports & Entertainment",
  [OPPORTUNITY_TYPE_FILTER.EDUCATION_SCIENTIFIC]: "Education & Scientific",
  [OPPORTUNITY_TYPE_FILTER.PROJECTS_INFRASTRUCTURE]: "Projects & Infrastructure",
  [OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS]: "Social & Special Events",
  [OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS]:
    "Local Catering & Day Meetings",
  [OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS]:
    "Corporate Celebrations & Private Events",
  [OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES]:
    "Travel Trade & Group Intermediaries",
  [OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY]: "Relocation & Extended Stay",
  [OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP]:
    "Preferred Lodging Partnership",
  [OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP]:
    "Professional Event Planner Partnership",
  [OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING]: "Local Meetings & Catering",
  [OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_TRAINING]:
    "Corporate Meetings / Training",
  [OPPORTUNITY_TYPE_FILTER.SPORTS_GROUP]: "Sports Group",
  [OPPORTUNITY_TYPE_FILTER.SPECIFIC_PRIVATE_EVENT]: "Specific Private Event",
  [OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED]: "Other / Unclassified",
});

/** Map canonical twelve-segment ids → filter keys. */
const SEGMENT_ID_TO_FILTER = Object.freeze({
  corporate_accounts_scaling: OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY,
  associations_conferences_events: OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP,
  corporate_meetings_incentives: OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_INCENTIVES,
  government_public_sector: OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR,
  sports_entertainment: OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT,
  education_scientific: OPPORTUNITY_TYPE_FILTER.EDUCATION_SCIENTIFIC,
  projects_infrastructure: OPPORTUNITY_TYPE_FILTER.PROJECTS_INFRASTRUCTURE,
  social_special_events: OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS,
  local_catering_day_meetings: OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS,
  corporate_celebrations_private_events:
    OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS,
  travel_trade_group_intermediaries:
    OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES,
  relocation_extended_stay: OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY,
});

function text(v) {
  return String(v || "")
    .trim()
    .toLowerCase();
}

/**
 * Map one opportunity to a stable filter category.
 * Prefer demandSegmentId / registry resolve over legacy type heuristics.
 */
export function classifyOpportunityTypeFilter(opp = {}) {
  const id = String(opp.id || opp.opportunityId || "");
  // Segment 8 subtypes win over parent social_special_events mapping.
  const subtype = String(
    opp.demandOpportunitySubtype || opp.opportunityType || "",
  ).toUpperCase();
  if (
    subtype === "PREFERRED_LODGING_PARTNERSHIP" ||
    opp.preferredLodgingPartnershipV1 === true
  ) {
    return OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP;
  }
  if (
    subtype === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
    opp.professionalEventPlannerPartnershipV1 === true
  ) {
    return OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP;
  }
  const explicit =
    opp.demandSegmentId ||
    opp.customerDemandSegmentId ||
    opp.demandSegmentRouting?.customerDemandSegmentId ||
    null;
  if (explicit) {
    const resolved = resolveDemandSegment(explicit);
    if (resolved && SEGMENT_ID_TO_FILTER[resolved.id]) {
      return SEGMENT_ID_TO_FILTER[resolved.id];
    }
  }

  // Protected Scaling live-campaign IDs stay Scaling unless explicitly
  // reclassified to Association/Conference/Events (Condesa preview).
  const segEarly = text(opp.researchSegment || opp.segment);
  const reclassEvents =
    opp.segmentReclassificationPreview === true &&
    (segEarly.includes("assoc") ||
      segEarly === "associations_government" ||
      segEarly.includes("conference"));
  if (/^gdi_sc_/i.test(id) && !reclassEvents) {
    return OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY;
  }

  const axis = opp.researchSegment || opp.segment || opp.researchAxis || "";
  const resolvedAxis = resolveDemandSegment(axis);
  // Only trust registry resolve for non-Scaling axes (avoid remapping
  // Scaling inventory via DMC/travel-trade researchAxis aliases).
  if (
    resolvedAxis &&
    SEGMENT_ID_TO_FILTER[resolvedAxis.id] &&
    resolvedAxis.id !== "corporate_accounts_scaling"
  ) {
    return SEGMENT_ID_TO_FILTER[resolvedAxis.id];
  }

  const segment = text(opp.researchSegment || opp.segment);
  const type = String(opp.opportunityType || "").toUpperCase();
  const signal = text(opp.demandSignalType || opp.demandFamily || "");
  const family = text(opp.demandFamily || "");

  if (
    segment === "scaling_companies" ||
    segment.includes("scaling") ||
    signal.includes("scaling")
  ) {
    return OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY;
  }

  if (
    segment === "local_catering_day_meetings" ||
    segment.includes("local_catering") ||
    segment === "local_meetings_catering" ||
    segment.includes("local_meetings") ||
    signal.includes("local_meetings") ||
    signal.includes("local_catering")
  ) {
    return OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS;
  }

  if (
    segment === "government_public_sector" ||
    segment.includes("government") ||
    signal.includes("government")
  ) {
    return OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR;
  }

  if (
    segment === "relocation_extended_stay" ||
    segment.includes("relocation") ||
    segment.includes("extended_stay") ||
    signal.includes("relocation")
  ) {
    return OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY;
  }

  if (
    type === "VENUE_PARTNERSHIP" ||
    segment === "private events" ||
    segment === "private_events" ||
    signal.includes("venue_partnership") ||
    signal.includes("preferred_lodging")
  ) {
    return OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP;
  }

  if (
    type === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
    signal.includes("professional_event_planner") ||
    signal.includes("event_planner_partnership")
  ) {
    return OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP;
  }

  if (type === "SPECIFIC_PRIVATE_EVENT") {
    return OPPORTUNITY_TYPE_FILTER.SPECIFIC_PRIVATE_EVENT;
  }

  if (
    segment.includes("sport") ||
    signal.includes("sport") ||
    family.includes("sport")
  ) {
    return OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT;
  }

  if (
    segment === "association" ||
    segment === "associations_government" ||
    segment.includes("assoc") ||
    segment.includes("conference / events") ||
    type === "OVERFLOW_HOUSING" ||
    type === "PRIMARY_PURSUIT" ||
    type === "FIXED_VENUE_OPEN_HOUSING" ||
    signal.includes("association") ||
    signal.includes("conference") ||
    signal.includes("association_conference_event")
  ) {
    return OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP;
  }

  if (
    type === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    segment.includes("corporate") ||
    signal.includes("corporate")
  ) {
    return OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_TRAINING;
  }

  return OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED;
}

export function opportunityMatchesTypeFilter(opp, filterValue) {
  if (!filterValue) return true;
  if (isInventoryPopulationValue(filterValue)) {
    if (filterValue === INVENTORY_POPULATION.ALL_COMMERCIAL_INTELLIGENCE) {
      return true;
    }
    const pops = Array.isArray(opp?.inventoryPopulations)
      ? opp.inventoryPopulations
      : [
          opp?.inventoryPopulationPrimary ||
            INVENTORY_POPULATION.SALES_OPPORTUNITIES,
        ];
    return pops.includes(filterValue);
  }
  const c = classifyOpportunityTypeFilter(opp);
  if (c === filterValue) return true;
  // Legacy alias: LOCAL_MEETINGS_CATERING ↔ LOCAL_CATERING_DAY_MEETINGS
  if (
    filterValue === OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING &&
    c === OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS
  ) {
    return true;
  }
  if (
    filterValue === OPPORTUNITY_TYPE_FILTER.SPORTS_GROUP &&
    c === OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT
  ) {
    return true;
  }
  return false;
}

/**
 * Counts by type from a pre-filtered inventory (other filters already applied).
 */
export function countByOpportunityTypeFilter(opportunities = []) {
  const counts = { all: 0 };
  for (const key of Object.values(OPPORTUNITY_TYPE_FILTER)) {
    if (key) counts[key] = 0;
  }
  for (const o of opportunities || []) {
    counts.all += 1;
    const c = classifyOpportunityTypeFilter(o);
    counts[c] = (counts[c] || 0) + 1;
  }
  return counts;
}

/** Preferred display order for filter dropdown (twelve registry first). */
const FILTER_DISPLAY_ORDER = Object.freeze([
  OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY,
  OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP,
  OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR,
  OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS,
  OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY,
  OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_INCENTIVES,
  OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT,
  OPPORTUNITY_TYPE_FILTER.EDUCATION_SCIENTIFIC,
  OPPORTUNITY_TYPE_FILTER.PROJECTS_INFRASTRUCTURE,
  OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS,
  OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS,
  OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES,
  OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP,
  OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP,
  OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_TRAINING,
  OPPORTUNITY_TYPE_FILTER.SPORTS_GROUP,
  OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING,
  OPPORTUNITY_TYPE_FILTER.SPECIFIC_PRIVATE_EVENT,
  OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED,
]);

export function opportunityTypeFilterOptions(counts = {}) {
  const options = [
    {
      value: OPPORTUNITY_TYPE_FILTER.SALES_OPPORTUNITIES,
      label: OPPORTUNITY_TYPE_FILTER_LABEL[OPPORTUNITY_TYPE_FILTER.SALES_OPPORTUNITIES],
      count: counts.SALES_OPPORTUNITIES ?? counts.all ?? 0,
    },
    {
      value: OPPORTUNITY_TYPE_FILTER.COMPETITIVE_MARKET_INTELLIGENCE,
      label:
        OPPORTUNITY_TYPE_FILTER_LABEL[
          OPPORTUNITY_TYPE_FILTER.COMPETITIVE_MARKET_INTELLIGENCE
        ],
      count: counts.COMPETITIVE_MARKET_INTELLIGENCE || 0,
    },
    {
      value: OPPORTUNITY_TYPE_FILTER.ALL_COMMERCIAL_INTELLIGENCE,
      label:
        OPPORTUNITY_TYPE_FILTER_LABEL[
          OPPORTUNITY_TYPE_FILTER.ALL_COMMERCIAL_INTELLIGENCE
        ],
      count:
        counts.ALL_COMMERCIAL_INTELLIGENCE ??
        counts.all ??
        0,
    },
  ];
  for (const value of FILTER_DISPLAY_ORDER) {
    const label = OPPORTUNITY_TYPE_FILTER_LABEL[value];
    if (!label) continue;
    const n = counts[value] || 0;
    if (n <= 0) continue;
    options.push({ value, label, count: n });
  }
  return options;
}
