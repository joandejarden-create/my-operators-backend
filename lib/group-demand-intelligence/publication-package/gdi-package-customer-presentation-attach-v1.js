/**
 * Read-time customer presentation attach for immutable publication packages.
 *
 * Does not mutate package bytes / fingerprint. Applies the same tile-contact
 * resolver used by live customer sanitize so PACKAGE list/detail DTOs do not
 * fall through to divergent client-only formatters.
 *
 * Revenue-stream icons: only attach when the package row already carries an
 * explicit class, icons, or thesis signal. Never invent COMBINED/active states
 * from thin package rows (would diverge from evidence-backed presentation and
 * require a new fingerprint to publish class into the package).
 */
import { attachUniversalTileContactPreview } from "../universal-tile-contact-preview-v1.js";
import { attachOpportunityRevenueStreamIcons } from "../opportunity-revenue-stream-icons-v1.js";

export const GDI_PACKAGE_CUSTOMER_PRESENTATION_ATTACH_V1 =
  "gdi-package-customer-presentation-attach-v1";

function hasExplicitRevenueStreamSignal(row = {}) {
  return Boolean(
    row.revenueStreamClass ||
      row.revenueStreamIcons ||
      row.applicableHotelRevenueStreams ||
      row.multiRevenue?.revenueStreamClass ||
      row.guestroomThesis ||
      row.eventFnbThesis ||
      row.potentialGuestroomDemand ||
      row.potentialMeetingDemand ||
      row.potentialCateringDemand ||
      row.hotelDemandHypotheses?.guestroomHypothesis ||
      row.hotelDemandHypotheses?.meetingsHypothesis ||
      row.hotelDemandHypotheses?.cateringHypothesis,
  );
}

/**
 * @param {object|object[]} opportunityOrList
 * @returns {object|object[]}
 */
export function attachPackageCustomerPresentation(opportunityOrList) {
  const asList = Array.isArray(opportunityOrList);
  const rows = asList ? opportunityOrList : [opportunityOrList];
  const out = rows.map((row) => {
    if (!row || typeof row !== "object") return row;
    let next = attachUniversalTileContactPreview(row);
    if (hasExplicitRevenueStreamSignal(row)) {
      next = attachOpportunityRevenueStreamIcons(next);
    }
    return {
      ...next,
      packageCustomerPresentationAttachVersion:
        GDI_PACKAGE_CUSTOMER_PRESENTATION_ATTACH_V1,
    };
  });
  return asList ? out : out[0];
}
