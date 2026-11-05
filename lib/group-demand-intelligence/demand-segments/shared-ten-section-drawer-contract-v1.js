/**
 * Shared ten-section drawer contract for all eight demand segments.
 *
 * Sections 2–10 = Scaling Companies gold structure.
 * Section 1 = segment-specific overviewModel from registry schemas.
 */

import {
  SCALING_CANONICAL_SECTION_IDS,
  SCALING_CANONICAL_SECTION_TITLES,
  buildScalingDrawerNormalizedModel,
} from "../scaling-drawer-normalized-model-v1.js";
import { buildEventsDrawerNormalizedModel } from "../events-drawer-normalized-model-v1.js";
import { resolveDemandSegment } from "./eight-demand-segment-registry-v1.js";
import { buildSegmentOverviewModel } from "./segment-overview-schemas-v1.js";

export const SHARED_TEN_SECTION_DRAWER_CONTRACT_V1 =
  "gdi-shared-ten-section-drawer-contract-v1";

export const SHARED_CANONICAL_SECTION_IDS = SCALING_CANONICAL_SECTION_IDS;
export const SHARED_CANONICAL_SECTION_TITLES = SCALING_CANONICAL_SECTION_TITLES;

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Resolve canonical customer segment id for an opportunity.
 */
export function resolveOpportunityDemandSegmentId(opportunity = {}) {
  const explicit =
    opportunity.demandSegmentId ||
    opportunity.customerDemandSegmentId ||
    opportunity.demandSegmentRouting?.customerDemandSegmentId;
  if (explicit) {
    const s = resolveDemandSegment(explicit);
    if (s) return s.id;
  }
  const axis = opportunity.researchSegment || opportunity.segment || "";
  const s = resolveDemandSegment(axis);
  if (s) return s.id;
  // Heuristic: Events presentation stamp
  if (
    opportunity.eventsOpportunityOverview ||
    opportunity.associationEventsGoldBrief ||
    /assoc|conference|event/i.test(String(axis))
  ) {
    return "associations_conferences_events";
  }
  if (/scaling|corporate account/i.test(String(axis))) {
    return "corporate_accounts_scaling";
  }
  return null;
}

/**
 * Build shared drawer DTO: overviewModel + sections 2–10 from Scaling/Events builders.
 */
export function buildSharedTenSectionDrawerDto(opportunity = {}, opts = {}) {
  const segmentId =
    opts.segmentId || resolveOpportunityDemandSegmentId(opportunity);
  const segment = resolveDemandSegment(segmentId);

  let baseModel = null;
  if (segmentId === "associations_conferences_events") {
    baseModel = buildEventsDrawerNormalizedModel(opportunity, opts);
  } else if (segmentId === "corporate_accounts_scaling" || !segmentId) {
    baseModel = buildScalingDrawerNormalizedModel(opportunity, opts);
  } else {
    // New segments reuse Scaling sections 2–10 builder with force (not Scaling IDs).
    baseModel = buildScalingDrawerNormalizedModel(
      {
        ...opportunity,
        researchSegment: segment?.id || opportunity.researchSegment,
        segmentDisplayLabel: segment?.label || opportunity.segmentDisplayLabel,
        opportunityTypeLabel: segment?.label || opportunity.opportunityTypeLabel,
      },
      { ...opts, force: true },
    );
  }

  const overviewModel = buildSegmentOverviewModel(segmentId, opportunity, opts);

  return {
    version: SHARED_TEN_SECTION_DRAWER_CONTRACT_V1,
    segmentId,
    segmentLabel: segment?.label || null,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    hotelId: opts.hotelId || opportunity.hotelId || null,
    overviewModel,
    sectionIds: [...SHARED_CANONICAL_SECTION_IDS],
    sectionTitles: { ...SHARED_CANONICAL_SECTION_TITLES },
    sections: baseModel?.sections || null,
    drawerModel: baseModel,
    contacts: opportunity.scalingTwoContactPresentation || opportunity.whoToContact || null,
    supportingEvidence:
      baseModel?.sections?.supporting_evidence ||
      opportunity.commercialIntelligenceBrief?.supportingEvidence ||
      null,
    hotelFeedback: opportunity.hotelValidation || null,
    preservesScalingGold: segmentId === "corporate_accounts_scaling",
    preservesEventsSection1: segmentId === "associations_conferences_events",
  };
}

/** Contract-level certification checks (no browser). */
export function certifySharedDrawerDto(dto = {}) {
  const issues = [];
  if (dto.version !== SHARED_TEN_SECTION_DRAWER_CONTRACT_V1) {
    issues.push("bad_version");
  }
  if (!dto.segmentId) issues.push("missing_segmentId");
  if (!Array.isArray(dto.sectionIds) || dto.sectionIds.length !== 10) {
    issues.push("section_count");
  } else {
    for (let i = 0; i < 10; i++) {
      if (dto.sectionIds[i] !== SHARED_CANONICAL_SECTION_IDS[i]) {
        issues.push(`section_order_${i}`);
        break;
      }
    }
  }
  if (!dto.overviewModel) issues.push("missing_overviewModel");
  if (
    dto.segmentId === "associations_conferences_events" &&
    dto.overviewModel?.overviewMode !== "events_structured_grid_v1" &&
    !dto.overviewModel?.identityLine
  ) {
    issues.push("events_overview_shape");
  }
  if (
    dto.segmentId === "corporate_accounts_scaling" &&
    dto.overviewModel?.overviewMode !== "scaling_narrative_overview_v1"
  ) {
    issues.push("scaling_overview_mode");
  }
  if (
    dto.segmentId &&
    ![
      "associations_conferences_events",
      "corporate_accounts_scaling",
    ].includes(dto.segmentId) &&
    dto.overviewModel?.structuredGrid !== true
  ) {
    issues.push("new_segment_requires_structured_grid");
  }
  return {
    ok: issues.length === 0,
    issues,
    segmentId: dto.segmentId || null,
  };
}

export function assertNoInternalEnumLeakage(textBlob) {
  const t = text(textBlob);
  const forbidden = [
    "CONTACT_RELEVANT",
    "INELIGIBLE_UNSPECIFIED",
    "RESEARCH_SEGMENT",
    "ORS_INTERNAL",
    "COMMERCIALLY_REVIEWABLE_RELATIONSHIP",
  ];
  return forbidden.filter((f) => t.includes(f));
}
