/**
 * Minimal Waterstone clean-28 share merge for production.
 *
 * Intentionally does NOT import local-shadow-commercial-portfolio-v1 (large
 * uncommitted dependency tree). Railway HEAD deploys + this overlay only.
 */
import {
  applyWaterstoneCleanSalesInventory,
  mergeWaterstoneClean28ClientFacingPack,
  WATERSTONE_HOTEL_ID,
} from "./waterstone-clean-sales-inventory-v1.js";

export function mergeWaterstoneClean28ForShare(
  hotelId = "",
  opportunities = [],
  opts = {},
) {
  if (String(hotelId) !== WATERSTONE_HOTEL_ID) {
    return Array.isArray(opportunities) ? opportunities : [];
  }
  const withPack = mergeWaterstoneClean28ClientFacingPack(
    opportunities,
    hotelId,
    opts,
  );
  return applyWaterstoneCleanSalesInventory(withPack, hotelId, opts);
}
