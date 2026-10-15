/**
 * Shared Opportunity Type browse filter categories.
 * Primary classification: structured fields (researchSegment, opportunityType,
 * demandSignalType, demandFamily). Title matching is never primary.
 */

export const OPPORTUNITY_TYPE_FILTER_VERSION = "gdi-opportunity-type-filter-v1";

export const OPPORTUNITY_TYPE_FILTER = Object.freeze({
  ALL: "",
  SCALING_COMPANY: "SCALING_COMPANY",
  PREFERRED_LODGING_PARTNERSHIP: "PREFERRED_LODGING_PARTNERSHIP",
  ASSOCIATION_CONFERENCE_GROUP: "ASSOCIATION_CONFERENCE_GROUP",
  LOCAL_MEETINGS_CATERING: "LOCAL_MEETINGS_CATERING",
  CORPORATE_MEETINGS_TRAINING: "CORPORATE_MEETINGS_TRAINING",
  SPORTS_GROUP: "SPORTS_GROUP",
  SPECIFIC_PRIVATE_EVENT: "SPECIFIC_PRIVATE_EVENT",
  OTHER_UNCLASSIFIED: "OTHER_UNCLASSIFIED",
});

export const OPPORTUNITY_TYPE_FILTER_LABEL = Object.freeze({
  [OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY]: "Scaling Company",
  [OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP]:
    "Preferred Lodging Partnership",
  [OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP]:
    "Association / Conference Group",
  [OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING]: "Local Meetings & Catering",
  [OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_TRAINING]:
    "Corporate Meetings / Training",
  [OPPORTUNITY_TYPE_FILTER.SPORTS_GROUP]: "Sports Group",
  [OPPORTUNITY_TYPE_FILTER.SPECIFIC_PRIVATE_EVENT]: "Specific Private Event",
  [OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED]: "Other / Unclassified",
});

function text(v) {
  return String(v || "")
    .trim()
    .toLowerCase();
}

/**
 * Map one opportunity to a stable filter category.
 * Scaling companies stay distinguishable from generic corporate accounts.
 * VENUE_PARTNERSHIP → Preferred Lodging Partnership (prospect type, not proof of agreement).
 */
export function classifyOpportunityTypeFilter(opp = {}) {
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
    segment === "local_meetings_catering" ||
    segment.includes("local_meetings") ||
    signal.includes("local_meetings")
  ) {
    return OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING;
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

  if (type === "SPECIFIC_PRIVATE_EVENT") {
    return OPPORTUNITY_TYPE_FILTER.SPECIFIC_PRIVATE_EVENT;
  }

  if (
    segment.includes("sport") ||
    signal.includes("sport") ||
    family.includes("sport")
  ) {
    return OPPORTUNITY_TYPE_FILTER.SPORTS_GROUP;
  }

  if (
    segment === "association" ||
    segment.includes("assoc") ||
    type === "OVERFLOW_HOUSING" ||
    type === "PRIMARY_PURSUIT" ||
    type === "FIXED_VENUE_OPEN_HOUSING" ||
    signal.includes("association") ||
    signal.includes("conference")
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
  return classifyOpportunityTypeFilter(opp) === filterValue;
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

export function opportunityTypeFilterOptions(counts = {}) {
  const options = [{ value: "", label: "All types", count: counts.all || 0 }];
  for (const [value, label] of Object.entries(OPPORTUNITY_TYPE_FILTER_LABEL)) {
    const n = counts[value] || 0;
    if (n <= 0) continue;
    options.push({ value, label, count: n });
  }
  return options;
}
