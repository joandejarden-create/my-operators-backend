/**
 * Local/customer-path recovery when durable published registry is enabled for a
 * hotel but snapshots are missing: fall back to the deploy-bundled approved pack.
 *
 * Does NOT expose research inventory.
 * Does NOT republish, mint share tokens, or write Airtable.
 */

import {
  resolveApprovedPublicationOpportunities,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
  SPICE_ISLAND_HOTEL_ID,
} from "./gdi-approved-publication-opportunity-source-v1.js";
import {
  COMMERCIAL_PUBLICATION_CLASS,
} from "./gdi-published-inventory-registry-v1.js";

export const GDI_APPROVED_PACK_CUSTOMER_INVENTORY_FALLBACK_V1 =
  "GDI_APPROVED_PACK_CUSTOMER_INVENTORY_FALLBACK_V1";

const CERTIFIED_BY_HOTEL = Object.freeze({
  [SPICE_ISLAND_HOTEL_ID]: SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Stamp approved-pack rows so customer-facing filters accept them without
 * promoting research-only inventory.
 */
export function stampApprovedPackAsCustomerFacing(opportunities = [], hotelId) {
  return (opportunities || []).map((row) => {
    const classification =
      text(row.commercialInventoryClass || row.commercialClassification) ||
      COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT;
    const isSalesReady =
      classification === COMMERCIAL_PUBLICATION_CLASS.SALES_READY;
    return {
      ...row,
      hotelId: row.hotelId || hotelId,
      customerVisible: true,
      customerFacing: true,
      publicationState: row.publicationState || "PUBLISHED",
      commercialInventoryClass: classification,
      commercialClassification: classification,
      gdiPublishedInventoryRegistry: false,
      gdiApprovedPackCustomerFallback: true,
      publishedPresentationLocked: true,
      inventoryLane: isSalesReady ? "SALES_OPPORTUNITY" : "ACCOUNT_DEVELOPMENT",
      inventoryLaneLabel: isSalesReady
        ? "Sales opportunity"
        : "Account development",
      inventorySalesReady: isSalesReady,
      accountDevelopmentPublished: !isSalesReady,
    };
  });
}

/**
 * If current opportunities are empty for a certified hotel, resolve the
 * deploy-bundled approved pack as the customer inventory.
 */
export function resolveApprovedPackCustomerInventoryFallback(
  hotelId,
  opportunities = [],
  opts = {}
) {
  const id = text(hotelId);
  const certifiedIds = CERTIFIED_BY_HOTEL[id];
  if (!certifiedIds?.length) {
    return {
      used: false,
      reason: "hotel_has_no_certified_approved_pack",
      opportunities,
    };
  }
  if (Array.isArray(opportunities) && opportunities.length > 0) {
    return {
      used: false,
      reason: "opportunities_already_present",
      opportunities,
    };
  }

  const resolved = resolveApprovedPublicationOpportunities({
    hotelId: id,
    opportunityIds: [...certifiedIds],
    env: opts.env || process.env,
    root: opts.root,
  });
  if (!resolved.ok || !(resolved.opportunities || []).length) {
    return {
      used: false,
      reason: "approved_pack_unresolved",
      missing: resolved.missing || [],
      opportunities,
    };
  }

  return {
    used: true,
    reason: "empty_customer_inventory_used_approved_pack",
    contract: GDI_APPROVED_PACK_CUSTOMER_INVENTORY_FALLBACK_V1,
    opportunities: stampApprovedPackAsCustomerFacing(
      resolved.opportunities,
      id
    ),
    sources: resolved.sources,
  };
}
