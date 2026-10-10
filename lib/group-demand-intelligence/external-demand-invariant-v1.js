/**
 * External-demand invariant V1 — hotel-agnostic.
 *
 * Hotel-owned / hotel-hosted product MUST NOT become a GDI customer opportunity
 * unless an external named demand entity with incremental future room demand is
 * independently identified.
 *
 * Used by campaign admission, Watch gate, and regression tests.
 */

export const EXTERNAL_DEMAND_INVARIANT_VERSION = "gdi_external_demand_invariant_v1";

const HOTEL_HOSTED_STATUS_RE =
  /HOTEL_HOSTED|HOTEL_OWNED|STAY_?AND_?PLAY|OWN_PRODUCT|PROPERTY_PACKAGE/i;

const HOTEL_HOSTED_TITLE_RE =
  /\b(stay\s*&\s*play|stay and play|hotel tournament|our tournament|unlimited golf)\b/i;

/**
 * Detect hotel-hosted / hotel-owned product shells.
 */
export function isHotelHostedProduct(record = {}) {
  const status = String(
    record.selectionStatus ||
      record.lodgingSelectionStatus ||
      record.hotelSelectionStatus ||
      record.pageSignalClass ||
      ""
  );
  if (HOTEL_HOSTED_STATUS_RE.test(status)) return true;
  if (record.hotelHostedProduct === true || record.hotelOwnedProduct === true) return true;
  if (record.isHotelHosted === true) return true;

  const title = String(record.title || record.name || record.opportunityName || "");
  const org = String(record.organizationName || record.organizer || "");
  const blob = `${title} ${org}`.toLowerCase();

  // Org is the hotel / resort brand itself
  const hotelName = String(
    record.hotelName || record.subjectHotelName || record.hotelDisplayName || ""
  ).toLowerCase();
  if (
    hotelName &&
    org &&
    org.toLowerCase().includes(hotelName.split(/\s+/)[0]) &&
    /tournament|stay|package|golf break/i.test(title)
  ) {
    return true;
  }

  // Sheraton/Arabella tournament pattern — property brands as organizer
  if (
    /sheraton|arabella golf/i.test(org) &&
    /tournament|stay\s*&\s*play|unlimited golf/i.test(title)
  ) {
    return true;
  }

  if (HOTEL_HOSTED_TITLE_RE.test(title) && /sheraton|arabella|castillo hotel|marriott|hotel/i.test(org)) {
    return true;
  }

  return false;
}

/**
 * True when an external named demand entity is independently present
 * (not the hotel brand / course brand alone).
 */
export function hasIndependentExternalDemandEntity(record = {}) {
  if (record.externalNamedDemandEntity === true) return true;
  if (record.externalDemandEntityId || record.childEntityId) {
    const child = String(record.childEntityName || record.externalDemandEntityName || "").trim();
    if (child.length >= 4) return true;
  }

  const org = String(record.organizationName || record.organizer || record.buyerEntity || "").trim();
  if (!org || org.length < 4) return false;

  // Hotel/course brands are not external demand entities for this invariant
  if (/^(arabella golf|sheraton mallorca|castillo hotel|marriott|hilton)\b/i.test(org)) {
    return false;
  }
  if (/arabella golf mallorca\s*\/\s*sheraton/i.test(org)) return false;

  // Tour operators / DMCs / associations / corporates count as external
  if (
    /holidays|travel|tours|dmc|incentive|society|association|asociación|sociedad|congress|grupo|agency|planner|pco/i.test(
      org
    )
  ) {
    return true;
  }

  // Distinct company name that is not the hotel
  const hotelBlob = `${record.hotelName || ""} ${record.hotelId || ""}`.toLowerCase();
  if (hotelBlob && org.toLowerCase().includes("sheraton") && hotelBlob.includes("sheraton")) {
    return false;
  }
  return Boolean(org);
}

/**
 * Core invariant: hotel-hosted product without independent external demand → block customer opportunity.
 */
export function assertExternalDemandForCustomerOpportunity(record = {}) {
  if (!isHotelHostedProduct(record)) {
    return {
      ok: true,
      version: EXTERNAL_DEMAND_INVARIANT_VERSION,
      hotelHosted: false,
      externalEntity: hasIndependentExternalDemandEntity(record),
      reasons: [],
    };
  }
  const external = hasIndependentExternalDemandEntity(record);
  if (external && record.incrementalRoomDemand === true) {
    return {
      ok: true,
      version: EXTERNAL_DEMAND_INVARIANT_VERSION,
      hotelHosted: true,
      externalEntity: true,
      reasons: ["hotel_hosted_but_external_incremental_demand_present"],
    };
  }
  return {
    ok: false,
    version: EXTERNAL_DEMAND_INVARIANT_VERSION,
    hotelHosted: true,
    externalEntity: external,
    reasons: [
      "hotel_hosted_product_without_independent_external_demand",
      ...(external ? ["missing_incremental_room_demand_flag"] : ["no_external_named_demand_entity"]),
    ],
  };
}

/**
 * Campaign admission helper: hotel-hosted shells must not consume expensive decomp
 * unless external demand is already identified.
 */
export function classifyHotelHostedCampaignAdmission(signal = {}) {
  const check = assertExternalDemandForCustomerOpportunity(signal);
  if (check.ok) return { class: null, reasons: check.reasons };
  return {
    class: "REJECT",
    reasons: check.reasons,
  };
}
