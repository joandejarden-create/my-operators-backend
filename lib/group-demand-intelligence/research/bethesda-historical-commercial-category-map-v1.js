/**
 * Bethesda historical → existing GDI commercial taxonomy projection.
 *
 * Recovery enums (LOCAL_CATERING, POTENTIAL_PARTNERSHIPS, …) are archival.
 * List filters / detail labels require demandSegmentId + opportunityType
 * (and PLP/PEP subtype flags) from the twelve-segment registry.
 *
 * No new primary category identifiers.
 */

import { resolveDemandSegment } from "../demand-segments/eight-demand-segment-registry-v1.js";
import {
  OPPORTUNITY_TYPE_FILTER,
  OPPORTUNITY_TYPE_FILTER_LABEL,
  classifyOpportunityTypeFilter,
} from "../opportunity-type-filter-v1.js";
import {
  PREFERRED_LODGING_SUBTYPE,
  PREFERRED_LODGING_SEGMENT_ID,
  PREFERRED_LODGING_CLASSIFICATION_LINE,
} from "../preferred-lodging-partnership/preferred-lodging-partnership-contract-v1.js";
import {
  PROFESSIONAL_EVENT_PLANNER_SUBTYPE,
  PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
  PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE,
} from "../professional-event-planners/professional-event-planner-contract-v1.js";
import { RECOVERY_CATEGORY } from "./bethesda-historical-opportunity-recovery-v1.js";

export const BETHESDA_HISTORICAL_COMMERCIAL_CATEGORY_MAP_V1 =
  "gdi-bethesda-historical-commercial-category-map-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function segKey(row = {}) {
  return text(row.researchSegment || row.demandSegment || row.segment).toLowerCase();
}

/**
 * Normalize curated/recovery segment strings onto registry ids or partnership subtypes.
 */
export function resolveHistoricalDemandRouting(row = {}) {
  const raw = segKey(row);
  const cat = text(row.researchCategory);

  if (
    raw.includes("preferred_lodging") ||
    raw.includes("venue partnership") ||
    raw.includes("destination partnership")
  ) {
    return {
      kind: "PLP",
      demandSegmentId: PREFERRED_LODGING_SEGMENT_ID,
      filterCategory: OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP,
      preservedResearchSegment: text(row.researchSegment || row.demandSegment) || "preferred_lodging",
    };
  }

  if (
    raw === "professional_event_planners" ||
    raw.includes("professional_event_planner")
  ) {
    return {
      kind: "PEP",
      demandSegmentId: PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID,
      filterCategory: OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP,
      preservedResearchSegment:
        text(row.researchSegment || row.demandSegment) || "professional_event_planners",
    };
  }

  if (raw === "professional_planners_dmc" || raw.includes("professional_planners_dmc")) {
    return {
      kind: "DMC",
      demandSegmentId: "travel_trade_group_intermediaries",
      filterCategory: OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES,
      preservedResearchSegment: "professional_planners_dmc",
    };
  }

  // Association archival category with corporate_meetings routing → restore association.
  if (
    cat === RECOVERY_CATEGORY.ASSOCIATION_EVENTS &&
    (raw.includes("corporate_meetings") || raw.includes("incentives"))
  ) {
    return {
      kind: "SEGMENT",
      demandSegmentId: "associations_conferences_events",
      filterCategory: OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP,
      preservedResearchSegment: "associations_conferences_events",
      remappedFrom: text(row.researchSegment || row.demandSegment),
      remapReason: "ASSOCIATION_EVENTS_ARCHIVAL_CATEGORY",
    };
  }

  const resolved = resolveDemandSegment(raw) || resolveDemandSegment(raw.replace(/\s+/g, "_"));
  if (resolved?.id) {
    return {
      kind: "SEGMENT",
      demandSegmentId: resolved.id,
      preservedResearchSegment: text(row.researchSegment || row.demandSegment) || resolved.id,
    };
  }

  // Fallback by archival recovery category only when segment cannot resolve.
  if (cat === RECOVERY_CATEGORY.LOCAL_CATERING) {
    return {
      kind: "SEGMENT",
      demandSegmentId: "local_catering_day_meetings",
      preservedResearchSegment: "local_meetings_catering",
      evidenceGap: false,
    };
  }
  if (cat === RECOVERY_CATEGORY.CORPORATE_GROUP) {
    return {
      kind: "SEGMENT",
      demandSegmentId: "corporate_meetings_incentives",
      preservedResearchSegment: "corporate_meetings_incentives",
    };
  }
  if (cat === RECOVERY_CATEGORY.ASSOCIATION_EVENTS) {
    return {
      kind: "SEGMENT",
      demandSegmentId: "associations_conferences_events",
      preservedResearchSegment: "associations_conferences_events",
    };
  }
  if (cat === RECOVERY_CATEGORY.SPORTS_HOUSING) {
    return {
      kind: "SEGMENT",
      demandSegmentId: "sports_entertainment",
      preservedResearchSegment: "sports",
    };
  }
  if (cat === RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY) {
    return {
      kind: "SEGMENT",
      demandSegmentId: "relocation_extended_stay",
      preservedResearchSegment: "relocation_extended_stay",
    };
  }
  if (cat === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS) {
    return {
      kind: "PLP",
      demandSegmentId: PREFERRED_LODGING_SEGMENT_ID,
      filterCategory: OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP,
      preservedResearchSegment: "preferred_lodging",
      evidenceGapNote: "Partnership subtype inferred from archival POTENTIAL_PARTNERSHIPS only.",
    };
  }

  return {
    kind: "UNCLASSIFIED",
    demandSegmentId: null,
    filterCategory: OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED,
    preservedResearchSegment: text(row.researchSegment || row.demandSegment) || null,
    evidenceGap: true,
  };
}

function labelsForSegmentId(demandSegmentId) {
  const seg = resolveDemandSegment(demandSegmentId);
  const filter =
    demandSegmentId === "local_catering_day_meetings"
      ? OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS
      : demandSegmentId === "corporate_celebrations_private_events"
        ? OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS
        : demandSegmentId === "corporate_meetings_incentives"
          ? OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_INCENTIVES
          : demandSegmentId === "associations_conferences_events"
            ? OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
            : demandSegmentId === "sports_entertainment"
              ? OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT
              : demandSegmentId === "relocation_extended_stay"
                ? OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY
                : demandSegmentId === "travel_trade_group_intermediaries"
                  ? OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES
                  : demandSegmentId === "social_special_events"
                    ? OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS
                    : null;
  return {
    segmentDisplayLabel: seg?.label || OPPORTUNITY_TYPE_FILTER_LABEL[filter] || "Group Demand",
    filterCategory: filter,
    opportunityTypeLabel: OPPORTUNITY_TYPE_FILTER_LABEL[filter] || seg?.label || "Group Demand",
  };
}

/**
 * Stamp commercial taxonomy fields onto a historical research-stage row.
 * Preserves researchCategory / provenance. Never invents commercial facts.
 */
export function applyHistoricalCommercialCategoryProjection(row = {}) {
  const routing = resolveHistoricalDemandRouting(row);
  const out = { ...row };
  out.historicalCommercialCategoryMapVersion =
    BETHESDA_HISTORICAL_COMMERCIAL_CATEGORY_MAP_V1;
  out.researchCategoryArchival = row.researchCategory || null;
  out.researchSegmentPreserved =
    routing.preservedResearchSegment || row.researchSegment || row.demandSegment || null;

  if (routing.kind === "PLP") {
    out.demandSegmentId = PREFERRED_LODGING_SEGMENT_ID;
    out.customerDemandSegmentId = PREFERRED_LODGING_SEGMENT_ID;
    out.demandOpportunitySubtype = PREFERRED_LODGING_SUBTYPE;
    out.opportunityType = PREFERRED_LODGING_SUBTYPE;
    out.opportunityTypeLabel = "Preferred Lodging Partnership";
    out.demandSignalType = PREFERRED_LODGING_SUBTYPE;
    out.demandFamily = "PRIVATE_EVENTS";
    out.preferredLodgingPartnershipV1 = true;
    out.professionalEventPlannerPartnershipV1 = false;
    out.classificationLine = PREFERRED_LODGING_CLASSIFICATION_LINE;
    out.segment = "Preferred Lodging Partnership";
    out.demandSegment = out.researchSegmentPreserved;
    out.researchSegment = out.researchSegmentPreserved;
    out.opportunityTypeFilterCategory =
      OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP;
    return out;
  }

  if (routing.kind === "PEP") {
    out.demandSegmentId = PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID;
    out.customerDemandSegmentId = PROFESSIONAL_EVENT_PLANNER_SEGMENT_ID;
    out.demandOpportunitySubtype = PROFESSIONAL_EVENT_PLANNER_SUBTYPE;
    out.opportunityType = PROFESSIONAL_EVENT_PLANNER_SUBTYPE;
    out.opportunityTypeLabel = "Professional Event Planner Partnership";
    out.demandSignalType = PROFESSIONAL_EVENT_PLANNER_SUBTYPE;
    out.demandFamily = "PRIVATE_EVENTS";
    out.professionalEventPlannerPartnershipV1 = true;
    out.preferredLodgingPartnershipV1 = false;
    out.classificationLine = PROFESSIONAL_EVENT_PLANNER_CLASSIFICATION_LINE;
    out.segment = "Professional Event Planner Partnership";
    out.demandSegment = out.researchSegmentPreserved;
    out.researchSegment = out.researchSegmentPreserved;
    out.opportunityTypeFilterCategory =
      OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP;
    return out;
  }

  if (routing.kind === "DMC" || routing.kind === "SEGMENT") {
    const id = routing.demandSegmentId;
    const labels = labelsForSegmentId(id);
    out.demandSegmentId = id;
    out.customerDemandSegmentId = id;
    out.preferredLodgingPartnershipV1 = false;
    out.professionalEventPlannerPartnershipV1 = false;
    out.demandOpportunitySubtype = null;
    out.demandSegment = out.researchSegmentPreserved;
    out.researchSegment = out.researchSegmentPreserved;
    out.segment = labels.segmentDisplayLabel;
    out.opportunityTypeFilterCategory =
      routing.filterCategory || labels.filterCategory || null;

    if (id === "local_catering_day_meetings") {
      out.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
      out.opportunityTypeLabel = "Local Catering & Day Meetings";
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "CORPORATE_MEETING";
    } else if (id === "corporate_celebrations_private_events") {
      // Do not use VENUE_PARTNERSHIP here — list classifier maps that subtype
      // to Preferred Lodging Partnership before demandSegmentId can win.
      out.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
      out.opportunityTypeLabel = "Corporate Celebrations & Private Events";
      out.demandFamily = "PRIVATE_EVENTS";
      out.demandSignalType = "PRIVATE_EVENT";
    } else if (id === "corporate_meetings_incentives") {
      out.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
      out.opportunityTypeLabel = "Corporate Meetings & Incentives";
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "CORPORATE_MEETING";
    } else if (id === "associations_conferences_events") {
      const overflow =
        text(row.historicalRecoveryDisposition) === "OVERFLOW_WATCH" ||
        /competitor/i.test(text(row.whatIsHappening));
      out.opportunityType = overflow ? "OVERFLOW_HOUSING" : "PRIMARY_PURSUIT";
      out.opportunityTypeLabel = overflow
        ? "Overflow / preferred lodging"
        : "Association / Conference / Events";
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "ASSOCIATION_CONFERENCE";
    } else if (id === "sports_entertainment") {
      out.opportunityType = "OVERFLOW_HOUSING";
      out.opportunityTypeLabel = "Sports & Entertainment";
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "SPORTS_HOUSING";
    } else if (id === "relocation_extended_stay") {
      out.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
      out.opportunityTypeLabel = "Relocation & Extended Stay";
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "RELOCATION_EXTENDED_STAY";
    } else if (id === "travel_trade_group_intermediaries") {
      out.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
      out.opportunityTypeLabel = "Travel Trade & Group Intermediaries";
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "TRAVEL_TRADE";
    } else {
      out.opportunityType = "CORPORATE_ACCOUNT_RELATIONSHIP";
      out.opportunityTypeLabel = labels.opportunityTypeLabel;
      out.demandFamily = "GROUP_MEETINGS";
      out.demandSignalType = "GROUP_DEMAND";
    }

    if (routing.remappedFrom) {
      out.categoryRemap = {
        from: routing.remappedFrom,
        to: id,
        reason: routing.remapReason || null,
      };
    }
    return out;
  }

  out.demandSegmentId = null;
  out.opportunityType = "HISTORICAL_RESEARCH";
  out.opportunityTypeLabel = "Other / Unclassified";
  out.segment = "Other / Unclassified";
  out.opportunityTypeFilterCategory = OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED;
  out.categoryEvidenceGap = true;
  return out;
}

export function classifyProjectedHistoricalFilter(row = {}) {
  const projected = applyHistoricalCommercialCategoryProjection(row);
  return {
    filterCategory: classifyOpportunityTypeFilter(projected),
    label: OPPORTUNITY_TYPE_FILTER_LABEL[classifyOpportunityTypeFilter(projected)],
    projected,
  };
}

export function buildCategoryCrosswalkRow(beforeRow = {}, afterRow = {}) {
  const beforeFilter = classifyOpportunityTypeFilter({
    ...beforeRow,
    // Simulate pre-fix projection defect: recovery enum as opportunityType.
    opportunityType: beforeRow.researchCategory || beforeRow.opportunityType,
    opportunityTypeLabel: beforeRow.opportunityTypeLabel,
    demandSegmentId: beforeRow.demandSegmentId || null,
    preferredLodgingPartnershipV1: beforeRow.preferredLodgingPartnershipV1 === true,
    professionalEventPlannerPartnershipV1:
      beforeRow.professionalEventPlannerPartnershipV1 === true,
  });
  const afterFilter = classifyOpportunityTypeFilter(afterRow);
  return {
    id: text(afterRow.id || beforeRow.id),
    organizationName: text(afterRow.organizationName || beforeRow.organizationName),
    researchCategoryArchival: afterRow.researchCategoryArchival || beforeRow.researchCategory || null,
    researchSegmentPreserved:
      afterRow.researchSegmentPreserved || beforeRow.researchSegment || beforeRow.demandSegment || null,
    demandSegmentId: afterRow.demandSegmentId || null,
    opportunityType: afterRow.opportunityType || null,
    opportunityTypeLabel: afterRow.opportunityTypeLabel || null,
    beforeFilter,
    beforeFilterLabel: OPPORTUNITY_TYPE_FILTER_LABEL[beforeFilter] || beforeFilter,
    afterFilter,
    afterFilterLabel: OPPORTUNITY_TYPE_FILTER_LABEL[afterFilter] || afterFilter,
    changed: beforeFilter !== afterFilter,
  };
}
