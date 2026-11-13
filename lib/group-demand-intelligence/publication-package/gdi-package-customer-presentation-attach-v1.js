/**
 * Read-time customer presentation attach for immutable publication packages.
 *
 * Does not mutate package bytes / fingerprint. Applies the same tile-contact
 * resolver, evidence-backed revenue-stream icons, and canonical customer
 * presentation contract used by authenticated customer PACKAGE preview so
 * auth↔share do not diverge via client-only fallbacks.
 *
 * Icon policy (no invention):
 * - Run the shared revenue-stream resolver against package row fields.
 * - Allow GUESTROOMS_ONLY / EVENT_FNB_ONLY when package evidence supports them.
 * - Demote derived COMBINED without event-F&B thesis prose to GUESTROOMS_ONLY
 *   (or UNCONFIRMED) — never light catering gold from keyword noise alone.
 */
import { attachUniversalTileContactPreview } from "../universal-tile-contact-preview-v1.js";
import {
  attachOpportunityRevenueStreamIcons,
  mapRevenueStreamClassToIconStates,
  REVENUE_STREAM_ICON_STATE,
  GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
} from "../opportunity-revenue-stream-icons-v1.js";
import { HOTEL_REVENUE_STREAM_CLASS } from "../multi-revenue-qualification-v1.js";
import { attachCanonicalCustomerPresentationContract } from "../presentation/gdi-canonical-customer-presentation-contract-v1.js";

export const GDI_PACKAGE_CUSTOMER_PRESENTATION_ATTACH_V1 =
  "gdi-package-customer-presentation-attach-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Prevent COMBINED/active-catering when package lacks event-F&B thesis prose.
 * Matches live auth RIC (GUESTROOMS_ONLY) without inventing catering demand.
 */
export function demotePackageCombinedWithoutEventThesis(opportunity = {}) {
  const cls = String(opportunity.revenueStreamClass || "").toUpperCase();
  if (cls !== HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB) {
    return opportunity;
  }
  if (text(opportunity.eventFnbThesis)) return opportunity;
  const roomsActive =
    opportunity.revenueStreamIcons?.guestrooms ===
      REVENUE_STREAM_ICON_STATE.ACTIVE ||
    Boolean(text(opportunity.guestroomThesis));
  if (roomsActive) {
    const icons = mapRevenueStreamClassToIconStates(
      HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
    );
    return {
      ...opportunity,
      revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
      revenueStreamIcons: icons,
      packageRevenueStreamDemotion: "COMBINED_WITHOUT_EVENT_THESIS_TO_GUESTROOMS",
    };
  }
  const muted = mapRevenueStreamClassToIconStates(
    HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
  );
  return {
    ...opportunity,
    revenueStreamClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
    revenueStreamIcons: muted,
    packageRevenueStreamDemotion: "COMBINED_WITHOUT_EVIDENCE_TO_UNCONFIRMED",
  };
}

function attachPackageRevenueStreamIcons(row = {}) {
  // Immutable publication rows already freeze revenueStreamClass + icons.
  // Prefer those bytes over live re-derivation (which can mute ACTIVE icons when
  // thesis prose was scrubbed or revenueStreamIconsVersion was not frozen).
  const frozenIcons = row.revenueStreamIcons;
  const frozenClass = text(row.revenueStreamClass);
  if (
    frozenIcons &&
    typeof frozenIcons === "object" &&
    frozenClass &&
    (frozenIcons.guestrooms === REVENUE_STREAM_ICON_STATE.ACTIVE ||
      frozenIcons.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE ||
      frozenIcons.guestrooms === REVENUE_STREAM_ICON_STATE.MUTED ||
      frozenIcons.eventFnb === REVENUE_STREAM_ICON_STATE.MUTED)
  ) {
    return {
      ...row,
      revenueStreamClass: frozenClass,
      revenueStreamIcons: {
        guestrooms: frozenIcons.guestrooms,
        eventFnb: frozenIcons.eventFnb,
      },
      revenueStreamIconsVersion: GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
      packageRevenueStreamSource: "FROZEN_PACKAGE_BYTES",
    };
  }
  let next = attachOpportunityRevenueStreamIcons(row);
  next = demotePackageCombinedWithoutEventThesis(next);
  return next;
}

/**
 * @param {object|object[]} opportunityOrList
 * @param {object} [meta] publication meta for contract fingerprint fields
 * @returns {object|object[]}
 */
export function attachPackageCustomerPresentation(opportunityOrList, meta = {}) {
  const asList = Array.isArray(opportunityOrList);
  const rows = asList ? opportunityOrList : [opportunityOrList];
  const out = rows.map((row) => {
    if (!row || typeof row !== "object") return row;
    let next = attachUniversalTileContactPreview(row);
    next = attachPackageRevenueStreamIcons(next);
    next = attachCanonicalCustomerPresentationContract(next, meta);
    return {
      ...next,
      packageCustomerPresentationAttachVersion:
        GDI_PACKAGE_CUSTOMER_PRESENTATION_ATTACH_V1,
      customerPresentationSurface: "PUBLICATION_PACKAGE",
    };
  });
  return asList ? out : out[0];
}
