/**
 * Hotel Explorer — shared canonical hotel view model (V1).
 *
 * Single consumer contract for attributes shown across the 11 active HE tabs.
 * HPC is SoT for property identity / descriptive attributes.
 * HI Organizations + Hotel–Org Relationships are SoT for ownership / portfolio.
 * Never invent values; never copy PropCo/economic ownership from brand alone.
 */

export const HE_SHARED_HOTEL_VIEW_MODEL_V1 = "he-shared-hotel-view-model-v1";

const REC_RE = /^rec[A-Za-z0-9]{14}$/;

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function positiveInt(v) {
  if (v == null || v === "" || v === "—") return null;
  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

function partyName(party) {
  if (!party) return null;
  if (typeof party === "string") return text(party);
  return (
    text(party.tradingName) ||
    text(party.legalName) ||
    text(party.name) ||
    text(party.display_name) ||
    null
  );
}

function classificationOf(party, fallback = null) {
  if (!party || typeof party !== "object") return fallback;
  return (
    text(party.founder_classification) ||
    text(party.classification) ||
    text(party.verification_state) ||
    text(party.confidence) ||
    fallback
  );
}

/**
 * Build shared attributes from HPC DTO (+ optional ownership / physical-product overlays).
 * @param {object} opts
 * @param {object} [opts.hpcHotel] - formatHotelPropertyCensusAsBrandPresenceHotel result
 * @param {object} [opts.ownership] - golden-demo / HI ownership payload
 * @param {object} [opts.physicalProduct] - guestRoomsMeetingSpace inventory
 * @param {string} [opts.hotelId]
 */
export function buildSharedHotelViewModel(opts = {}) {
  const hpc = opts.hpcHotel && typeof opts.hpcHotel === "object" ? opts.hpcHotel : {};
  const ownership = opts.ownership && typeof opts.ownership === "object" ? opts.ownership : {};
  const hotel = ownership.hotel && typeof ownership.hotel === "object" ? ownership.hotel : {};
  const control =
    (ownership.report && ownership.report.ownership_and_control) ||
    ownership.ownership_and_control ||
    {};
  const physical =
    opts.physicalProduct ||
    hotel.guestRoomsMeetingSpace ||
    ownership.guestRoomsMeetingSpace ||
    null;

  const hotelId =
    text(opts.hotelId) ||
    text(hpc.id) ||
    text(hpc.recordId) ||
    text(hotel.id) ||
    text(hotel.airtable_record_id);

  const roomsFromPhysical = positiveInt(
    physical?.guestRooms?.totalGuestrooms?.value ??
      physical?.guestRooms?.totalGuestrooms ??
      physical?.totalGuestrooms
  );
  const rooms =
    roomsFromPhysical ||
    positiveInt(hpc.rooms) ||
    positiveInt(hotel.rooms) ||
    positiveInt(ownership.rooms) ||
    null;

  const brand =
    text(hpc.brand) ||
    text(hotel.brand) ||
    text(hotel.currentBrand) ||
    partyName(control.brand) ||
    null;

  const market =
    text(hpc.market) ||
    text(hotel.market) ||
    text(hotel.marketName) ||
    null;

  const submarket = text(hpc.submarket) || text(hotel.submarket) || null;

  const operator =
    text(hpc.managementCompany) ||
    text(hotel.managementCompany) ||
    text(hotel.operator) ||
    partyName(control.operator) ||
    null;

  const propertyCompany =
    partyName(control.property_company) ||
    partyName(control.legal_property_owner_propco) ||
    partyName(control.legal_owner) ||
    text(hotel.propertyCompany) ||
    null;

  const economicOwner = partyName(control.economic_owner) || text(hotel.economicOwner) || null;
  const economicOwnerClassification =
    classificationOf(control.economic_owner, null) ||
    text(hotel.economic_owner_classification) ||
    (economicOwner ? "Proposed" : null);

  const chainScale = text(hpc.chainScale) || text(hotel.chainScale) || null;
  const status =
    text(hpc.status) ||
    text(hpc.radarStatus) ||
    text(hotel.status) ||
    text(hotel.radarStatus) ||
    null;

  const city = text(hpc.city) || text(hotel.city) || null;
  const country = text(hpc.country) || text(hotel.country) || null;
  const name =
    text(hpc.name) ||
    text(hotel.name) ||
    text(hotel.propertyName) ||
    null;

  // Official property contact routes — HPC Official Property URL / Phone only.
  // Never inherit economic-sponsor or org-general websites/phones onto the hotel header.
  const corporate =
    (ownership.report && ownership.report.corporate_contacts) ||
    ownership.corporate_contacts ||
    {};
  const corporateIsPropertyHpc =
    String(corporate.source || "") === "HPC_PROPERTY_FALLBACK" ||
    /hotel property census/i.test(String(corporate.property_fallback_source || ""));
  const website =
    text(hpc.website) ||
    text(hotel.website) ||
    text(hotel.hotelWebsite) ||
    text(hotel.officialPropertyUrl) ||
    (corporateIsPropertyHpc ? text(corporate.website) : null) ||
    null;
  const telephone =
    text(hpc.telephone) ||
    text(hpc.phone) ||
    text(hotel.telephone) ||
    text(hotel.phone) ||
    text(corporate.property_phone) ||
    null;

  const sources = {
    rooms: roomsFromPhysical
      ? "physical_product_or_hpc"
      : positiveInt(hpc.rooms)
        ? "hpc"
        : rooms
          ? "ownership_hotel"
          : null,
    brand: text(hpc.brand) ? "hpc" : brand ? "ownership_or_hi" : null,
    market: text(hpc.market) ? "hpc" : market ? "ownership_hotel" : null,
    operator: text(hpc.managementCompany)
      ? "hpc"
      : partyName(control.operator)
        ? "hi_relationship"
        : operator
          ? "ownership_hotel"
          : null,
    propertyCompany: partyName(control.property_company) || partyName(control.legal_owner)
      ? "hi_legal_owner"
      : null,
    economicOwner: economicOwner ? "hi_economic_owner" : null,
    website: text(hpc.website)
      ? "hpc_official_property_url"
      : website
        ? "ownership_or_corporate_contacts"
        : null,
    telephone: text(hpc.telephone) || text(hpc.phone)
      ? "hpc_phone"
      : telephone
        ? "ownership_or_corporate_contacts"
        : null,
  };

  return {
    version: HE_SHARED_HOTEL_VIEW_MODEL_V1,
    hotelId: REC_RE.test(hotelId || "") ? hotelId : hotelId || null,
    name,
    rooms,
    roomsKnown: rooms != null,
    brand,
    market,
    submarket,
    operator,
    propertyCompany,
    economicOwner,
    economicOwnerClassification,
    chainScale,
    status,
    city,
    country,
    location: [city, country].filter(Boolean).join(", ") || null,
    website,
    telephone,
    phone: telephone,
    sources,
    transformations: {
      rooms:
        "HPC Rooms/Keys or verified physical-product totalGuestrooms; never invent; never treat unknown as 0.",
      brand: "HPC Current Brand / Affiliation; HI Brand relationship is corroboration only.",
      market: "HPC Market (Dealality geography) — never legacy Hotel Census.",
      operator: "HPC Operator/Management Company or HI Operator relationship.",
      propertyCompany: "HI Legal Owner / PropCo only — not economic sponsor.",
      economicOwner:
        "HI Economic Owner with founder classification preserved (Proposed vs Validated).",
      website:
        "HPC Official Property URL only — never sponsor/org-general websites as hotel header Website.",
      telephone:
        "HPC Phone / property switchboard only — never invent or copy unrelated org phones.",
    },
  };
}

/**
 * Overlay shared canonical attributes onto an owned-portfolio row.
 * Does not invent rooms/brand/market for unrelated hotels without HPC ids.
 */
export function hydratePortfolioRowFromSharedView(row, shared, { focusHotelId } = {}) {
  if (!row || typeof row !== "object") return row;
  const out = { ...row };
  const rowId = text(row.airtable_record_id || row.id || row.hotel_id);
  const focusId = text(focusHotelId || shared?.hotelId);
  const isFocus =
    (rowId && focusId && rowId === focusId) ||
    row.focus_hotel === true ||
    row.focusHotel === true;

  if (!shared || typeof shared !== "object") return out;

  if (isFocus) {
    if (shared.rooms != null && (out.rooms == null || !out.rooms_known)) {
      out.rooms = shared.rooms;
      out.rooms_known = true;
      out.rooms_display = String(shared.rooms);
      out.rooms_source = shared.sources?.rooms || "shared_hotel_view_model";
    }
    if (shared.brand && !text(out.brand_display || out.brand)) {
      out.brand = shared.brand;
      out.brand_display = shared.brand;
      out.brand_source = shared.sources?.brand || "shared_hotel_view_model";
    }
    if (shared.market && !text(out.market_display || out.market)) {
      out.market = shared.market;
      out.market_display = shared.market;
      out.market_source = shared.sources?.market || "shared_hotel_view_model";
    }
    if (shared.operator && !text(out.operator || out.management_company)) {
      out.operator = shared.operator;
      out.management_company = shared.operator;
      out.operator_source = shared.sources?.operator || "shared_hotel_view_model";
    }
    if (shared.name && (!text(out.hotel_name || out.name) || out.name === rowId)) {
      out.hotel_name = shared.name;
      out.name = shared.name;
    }
    if (shared.status && !text(out.status)) out.status = shared.status;
    if (shared.chainScale && !text(out.chain_scale || out.chainScale)) {
      out.chain_scale = shared.chainScale;
      out.chainScale = shared.chainScale;
    }
    out.pending_hpc_match = false;
    out.hydrated_from_shared_view = true;
  }

  return out;
}

export function hydratePortfolioHotelsFromSharedView(hotels = [], shared, opts = {}) {
  return (hotels || []).map((row) =>
    hydratePortfolioRowFromSharedView(row, shared, opts)
  );
}
