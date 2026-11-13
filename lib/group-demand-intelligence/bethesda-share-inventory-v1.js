/**
 * Bethesda external-share inventory selection.
 *
 * Share must never use live customerFacing heuristics alone (they inflate past
 * the frozen published-19 boundary). Mirror the auth commercial path:
 *   union → commercial surface → selectBethesdaSalesOpportunityInventory
 *   → annotateInventoryPopulations
 *
 * Until founder publication approval: exact published 19.
 * After approval: full sales-baseline 67 (honest maturity; not all Sales Ready).
 */
import { filterCustomerFacingOpportunities } from "./customer-visibility.js";
import { mergeBethesdaInternalResearchOpportunities } from "./research/bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./research/bethesda-88-commercial-event-family-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "./gdi-commercial-surface-disposition-v1.js";
import {
  annotateInventoryPopulations,
  buildInventoryPopulationCounts,
  isBethesda67CustomerPublicationApproved,
  selectBethesdaSalesOpportunityInventory,
  BETHESDA_HOTEL_ID,
  BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
  BETHESDA_SALES_TEAM_LIST_SCOPE,
} from "./bethesda-67-sales-baseline-cmi-v1.js";

export function isBethesdaShareInventoryHotel(hotelId) {
  return String(hotelId || "").trim() === BETHESDA_HOTEL_ID;
}

/**
 * @returns {{
 *   opportunities: object[],
 *   listScope: string,
 *   approved: boolean,
 *   inventoryPopulationCounts: object,
 *   selection: object,
 * }}
 */
export function selectBethesdaExternalShareOpportunities(
  opportunities = [],
  opts = {}
) {
  const hotelId = String(opts.hotelId || BETHESDA_HOTEL_ID).trim();
  if (hotelId !== BETHESDA_HOTEL_ID) {
    const facing = filterCustomerFacingOpportunities(opportunities, {
      hotelId,
      env: opts.env,
    });
    const annotated = annotateInventoryPopulations(facing, { hotelId });
    return {
      opportunities: annotated,
      listScope: "CUSTOMER_FACING",
      approved: false,
      inventoryPopulationCounts: buildInventoryPopulationCounts(annotated),
      selection: {
        expectedCount: facing.length,
        selectedCount: facing.length,
        missingIds: [],
      },
    };
  }

  const union = mergeBethesdaInternalResearchOpportunities(
    hotelId,
    opportunities || []
  );
  let rows = union.opportunities || [];

  try {
    const family = applyBethesdaCommercialEventFamilyPresentation(rows, {
      hideLinkedFromList: true,
    });
    rows = family.opportunities;
  } catch {
    // Family collapse is best-effort on share; selection still fail-closed below.
  }

  rows = filterOpportunitiesByCommercialSurface(
    rows,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL
  );

  const approved = isBethesda67CustomerPublicationApproved({
    env: opts.env,
    receiptPath: opts.receiptPath,
    forceApproved: opts.forceApproved,
  });
  const selection = selectBethesdaSalesOpportunityInventory(rows, {
    salesTeamInventory: false,
    founderReview: false,
    forceApproved: opts.forceApproved,
    env: opts.env,
    receiptPath: opts.receiptPath,
  });
  const annotated = annotateInventoryPopulations(selection.selected, {
    hotelId,
  });

  return {
    opportunities: annotated,
    listScope: approved
      ? BETHESDA_SALES_TEAM_LIST_SCOPE
      : BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
    approved,
    inventoryPopulationCounts: buildInventoryPopulationCounts(annotated),
    selection: {
      expectedCount: selection.expectedCount,
      selectedCount: selection.selected.length,
      missingIds: selection.missingIds,
      publishedCount: selection.publishedCount,
      stagedPresentCount: selection.stagedPresentCount,
    },
  };
}
