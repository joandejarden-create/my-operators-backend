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

/**
 * Prefer immutable package tile-contact / role bytes over live re-derivation.
 * Local-master publications freeze approved displayRole strings (including
 * org-qualified titles and functional-inbox labels) that the live resolver
 * would otherwise strip or map to "Role Not Yet Verified".
 */
function preserveFrozenPackageTileContact(row = {}) {
  const frozen = row.tileContactPreview;
  const frozenRole = text(
    (frozen && typeof frozen === "object"
      ? frozen.displayRole || frozen.roleLabel
      : "") ||
      row.displayRole ||
      row.opportunityRole ||
      row.tileContactRoleLabel ||
      row.primaryContact?.professionalTitle ||
      row.primaryContact?.title ||
      row.primaryContact?.role ||
      row.contactRole,
  );
  const frozenName = text(
    (frozen && typeof frozen === "object"
      ? frozen.displayName || frozen.name
      : "") ||
      row.tileContactName ||
      row.primaryContact?.name ||
      row.primaryContact?.fullName ||
      row.contactName,
  );
  if (!frozenName || !frozenRole) return null;
  if (
    frozen &&
    typeof frozen === "object" &&
    frozen.kind === "named" &&
    text(frozen.displayRole || frozen.roleLabel)
  ) {
    const subtitle = text(frozen.displayRole || frozen.roleLabel);
    return {
      ...row,
      tileContactPreview: frozen,
      tileContactName: text(frozen.displayName || frozen.name) || frozenName,
      tileContactRoleLabel: subtitle,
      cardContactSubtitle: subtitle,
      packageTileContactSource: "FROZEN_PACKAGE_BYTES",
    };
  }
  const preview = {
    version: "gdi-universal-tile-contact-preview-v2",
    kind: "named",
    name: frozenName,
    roleLabel: frozenRole,
    displayName: frozenName,
    displayRole: frozenRole,
    displayRoleKind: "professional_title",
    professionalTitle: frozenRole,
    opportunityRole: row.opportunityRole || null,
    organization:
      row.primaryContact?.organizationName ||
      row.primaryContact?.organization ||
      null,
    buyerEligibleForGate: Boolean(
      frozen?.buyerEligibleForGate ?? row.primaryContact?.buyerEligibleForGate,
    ),
    buyerEligibility: frozen?.buyerEligibility || undefined,
    drawerSlot: frozen?.drawerSlot || "primary",
    packagePreserved: true,
  };
  return {
    ...row,
    tileContactPreview: preview,
    tileContactName: frozenName,
    tileContactRoleLabel: frozenRole,
    cardContactSubtitle: frozenRole,
    packageTileContactSource: "FROZEN_PACKAGE_CONTACT_FIELDS",
  };
}

/**
 * When package construction dropped thesis prose but kept an evidence-backed
 * revenueStreamClass + Sales Opportunities keys, restore icon states from the
 * class. Does not invent demand — only repairs presentation field loss.
 */
function restoreIconsFromPersistedClassAndSalesKeys(row = {}) {
  const frozenClass = text(row.revenueStreamClass).toUpperCase();
  if (
    !frozenClass ||
    frozenClass === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED
  ) {
    return null;
  }
  const keys = new Set(
    (Array.isArray(row.salesOpportunityQuickFilterKeys)
      ? row.salesOpportunityQuickFilterKeys
      : []
    ).map((k) => text(k).toUpperCase()),
  );
  const guestKey = keys.has("GUEST_ROOMS") || keys.has("BOTH");
  const cateringKey =
    keys.has("LOCAL_CATERING") ||
    keys.has("MEETINGS_EVENTS") ||
    keys.has("BOTH");
  const expectsGuest =
    frozenClass === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY ||
    frozenClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  const expectsEvent =
    frozenClass === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY ||
    frozenClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  // Require sales-key corroboration so class alone cannot light icons.
  if (expectsGuest && !guestKey) return null;
  if (expectsEvent && !cateringKey && frozenClass !== HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY) {
    // GUESTROOMS_ONLY may omit catering keys; COMBINED/EVENT require them.
    if (expectsEvent) return null;
  }
  if (frozenClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB) {
    if (!guestKey || !cateringKey) return null;
  }
  if (frozenClass === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY && !cateringKey) {
    return null;
  }
  const icons = mapRevenueStreamClassToIconStates(frozenClass);
  const guestThesis =
    text(row.guestroomThesis) ||
    text(row.potentialGuestroomDemand) ||
    text(row.hotelDemandHypotheses?.guestroomHypothesis) ||
    (guestKey
      ? "Guestroom demand indicated by Sales Opportunities classification (GUEST_ROOMS)."
      : null);
  const eventThesis =
    text(row.eventFnbThesis) ||
    text(row.potentialMeetingDemand) ||
    text(row.potentialCateringDemand) ||
    text(row.hotelDemandHypotheses?.meetingsHypothesis) ||
    (cateringKey
      ? "Meetings / catering demand indicated by Sales Opportunities classification."
      : null);
  return {
    ...row,
    revenueStreamClass: frozenClass,
    revenueStreamIcons: icons,
    revenueStreamIconsVersion: GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
    guestroomThesis: guestThesis,
    eventFnbThesis: eventThesis,
    packageRevenueStreamSource: "RESTORED_FROM_CLASS_AND_SALES_KEYS",
  };
}

function attachPackageRevenueStreamIcons(row = {}) {
  // Immutable publication rows already freeze revenueStreamClass + icons.
  // Prefer those approved bytes over live re-derivation (local-master authority).
  // Demotion applies only when icons were not frozen into the package.
  const frozenIcons = row.revenueStreamIcons;
  const frozenClass = text(row.revenueStreamClass).toUpperCase();
  if (
    frozenIcons &&
    typeof frozenIcons === "object" &&
    frozenClass &&
    (frozenIcons.guestrooms === REVENUE_STREAM_ICON_STATE.ACTIVE ||
      frozenIcons.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE)
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
  // Class present but icons muted/missing after thesis scrub — restore when
  // Sales Opportunities keys corroborate the frozen commercial classification.
  const restored = restoreIconsFromPersistedClassAndSalesKeys(row);
  if (
    restored &&
    (restored.revenueStreamIcons?.guestrooms ===
      REVENUE_STREAM_ICON_STATE.ACTIVE ||
      restored.revenueStreamIcons?.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE)
  ) {
    return restored;
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
    let next =
      preserveFrozenPackageTileContact(row) ||
      attachUniversalTileContactPreview(row);
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
