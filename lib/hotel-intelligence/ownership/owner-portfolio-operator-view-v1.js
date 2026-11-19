/**
 * Owner-centric portfolio view: Owned/Controlled hotels + operator grouping
 * over the *same* hotel set, with mandatory total reconciliation.
 *
 * Version: owner-portfolio-operator-view-v1
 */

export const OWNER_PORTFOLIO_OPERATOR_VIEW_VERSION = "owner-portfolio-operator-view-v1";
export const OPERATOR_NOT_VERIFIED_LABEL = "Operator Not Verified";
export const OWNER_OPERATED_LABEL = "Owner-Operated";

function normName(v) {
  return String(v || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function hotelIdentityKey(hotel) {
  const id = String(hotel?.airtable_record_id || hotel?.id || hotel?.hotel_id || "").trim();
  if (id) return `id:${id}`;
  const name = normName(hotel?.hotel_name || hotel?.name || hotel?.hotel);
  const city = normName(hotel?.city || hotel?.market);
  return `name:${name}|${city}`;
}

function parseRooms(raw) {
  if (raw == null || raw === "" || raw === "—") return null;
  const n = Number(String(raw).replace(/,/g, "").trim());
  return Number.isFinite(n) && n > 0 ? n : null;
}

function isOwnedRow(hotel) {
  if (!hotel) return false;
  if (hotel.include_as_owned === true || hotel.economic_owner_verified === true) return true;
  const rel = String(hotel.relationship_type || hotel.relationship || "").toUpperCase();
  return (
    rel === "OWNED_BY" ||
    rel === "CONTROLLED_BY" ||
    rel === "ECONOMIC_OWNER" ||
    rel === "LEGAL_OWNER" ||
    /OWNED|CONTROLLED|ECONOMIC OWNER|LEGAL OWNER/i.test(rel)
  );
}

function isCurrentHolding(hotel) {
  const t = String(hotel?.current_historical || hotel?.temporal_status || "CURRENT").toUpperCase();
  return !(t === "HISTORICAL" || t === "FORMER" || t === "DISPOSED" || t === "REALIZED");
}

/**
 * Resolve primary operator label for an owned hotel.
 * Owner-affiliated operators collapse to Owner-Operated.
 */
export function resolvePrimaryOperator(hotel, ownerDisplayName) {
  const raw =
    hotel?.operator ||
    hotel?.management_company ||
    hotel?.operating_company ||
    hotel?.operator_name ||
    null;
  const op = String(raw || "").trim();
  if (!op) {
    return {
      operatorKey: "__unverified__",
      operatorLabel: OPERATOR_NOT_VERIFIED_LABEL,
      managementModel: null,
      ownerOperated: false,
      verified: false,
      ambiguous: Boolean(hotel?.operator_ambiguous),
    };
  }
  const ownerKey = normName(ownerDisplayName);
  const opKey = normName(op);
  const ownerOperated =
    Boolean(hotel?.owner_operated) ||
    (ownerKey &&
      opKey &&
      (opKey === ownerKey ||
        opKey.includes(ownerKey) ||
        ownerKey.includes(opKey) ||
        /owner.?operat/i.test(op)));
  if (ownerOperated) {
    return {
      operatorKey: "__owner_operated__",
      operatorLabel: OWNER_OPERATED_LABEL,
      managementModel: hotel?.management_model || "Owner-operated",
      ownerOperated: true,
      verified: true,
      displayOperatorName: op,
      ambiguous: false,
    };
  }
  return {
    operatorKey: opKey || op,
    operatorLabel: op,
    managementModel: hotel?.management_model || null,
    ownerOperated: false,
    verified: true,
    ambiguous: Boolean(hotel?.operator_ambiguous),
  };
}

/**
 * Readable ownership relationship label (never raw codes like OPERATOR).
 */
export function formatOwnershipRelationshipLabel(hotel) {
  const raw = String(hotel?.relationship_type || hotel?.relationship || "").toUpperCase();
  if (hotel?.legal_owner && hotel?.economic_owner) {
    return "Economic control via PropCo";
  }
  if (raw === "OWNED_BY" || raw === "LEGAL_OWNER") return "Legal / economic ownership";
  if (raw === "CONTROLLED_BY" || raw === "ECONOMIC_OWNER") return "Economic / controlling ownership";
  if (raw === "SPONSORED_BY") return "Economic sponsor interest";
  if (/OWNED|CONTROL/i.test(raw)) return "Owned / controlled";
  if (raw) return raw.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  return "Owned / controlled";
}

export function formatOperatorRelationshipLabel(resolved, hotel) {
  if (resolved.ownerOperated) {
    return `Owner-operated${resolved.displayOperatorName ? ` (${resolved.displayOperatorName})` : ""}`;
  }
  if (!resolved.verified) return "Management company not verified";
  const brand = hotel?.brand_display || hotel?.brand;
  return `Managed by ${resolved.operatorLabel}${brand ? ` · branded ${brand}` : ""}`;
}

/**
 * Deduplicate owned hotels by canonical property identity.
 * Prefer rows with airtable_record_id and known rooms.
 */
export function dedupeOwnedHotels(hotels = []) {
  const map = new Map();
  for (const raw of hotels || []) {
    if (!raw) continue;
    if (!isOwnedRow(raw)) continue;
    if (!isCurrentHolding(raw)) continue;
    const key = hotelIdentityKey(raw);
    const rooms = parseRooms(raw.rooms_display != null ? raw.rooms_display : raw.rooms);
    const next = {
      ...raw,
      hotel_name: raw.hotel_name || raw.name || raw.hotel || "Hotel",
      name: raw.hotel_name || raw.name || raw.hotel || "Hotel",
      airtable_record_id: raw.airtable_record_id || raw.id || null,
      rooms: rooms,
      rooms_known: rooms != null,
      brand_display: raw.brand_display || raw.brand || null,
      market_display: raw.market_display || raw.market || raw.city || null,
      city: raw.city || null,
      operator: raw.operator || raw.management_company || raw.operating_company || null,
    };
    const prev = map.get(key);
    if (!prev) {
      map.set(key, next);
      continue;
    }
    const score = (h) =>
      (h.airtable_record_id ? 4 : 0) + (h.rooms_known ? 2 : 0) + (h.operator ? 1 : 0);
    map.set(key, score(next) >= score(prev) ? { ...prev, ...next } : prev);
  }
  return [...map.values()].sort((a, b) =>
    String(a.hotel_name).localeCompare(String(b.hotel_name))
  );
}

function sumRooms(hotels) {
  let known = 0;
  let unknownHotels = 0;
  for (const h of hotels) {
    if (h.rooms_known && h.rooms != null) known += h.rooms;
    else unknownHotels += 1;
  }
  return { roomsKnown: known, roomsUnknownHotelCount: unknownHotels };
}

/**
 * Build the owner portfolio + operator grouping view.
 * @param {object[]} ownedHotels - ownership portfolio rows (owned/controlled only)
 * @param {{ ownerDisplayName?: string, coverageLabel?: string, completeness?: string }} [opts]
 */
export function buildOwnerPortfolioOperatorView(ownedHotels = [], opts = {}) {
  const ownerDisplayName = opts.ownerDisplayName || "";
  const owned = dedupeOwnedHotels(ownedHotels);
  const ownedTotals = sumRooms(owned);

  const groupMap = new Map();
  const ambiguous = [];

  for (const hotel of owned) {
    const resolved = resolvePrimaryOperator(hotel, ownerDisplayName);
    if (resolved.ambiguous) ambiguous.push(hotelIdentityKey(hotel));
    const key = resolved.operatorKey;
    if (!groupMap.has(key)) {
      groupMap.set(key, {
        operatorKey: key,
        operatorName: resolved.operatorLabel,
        ownerOperated: resolved.ownerOperated,
        verified: resolved.verified,
        hotels: [],
      });
    }
    groupMap.get(key).hotels.push({
      ...hotel,
      operator_relationship_label: formatOperatorRelationshipLabel(resolved, hotel),
      ownership_relationship_label: formatOwnershipRelationshipLabel(hotel),
    });
  }

  const operatorGroups = [...groupMap.values()]
    .map((g) => {
      const roomSum = sumRooms(g.hotels);
      return {
        ...g,
        hotelCount: g.hotels.length,
        roomsKnown: roomSum.roomsKnown,
        roomsUnknownHotelCount: roomSum.roomsUnknownHotelCount,
        hotels: g.hotels.sort((a, b) => String(a.hotel_name).localeCompare(String(b.hotel_name))),
      };
    })
    .sort((a, b) => {
      if (a.ownerOperated !== b.ownerOperated) return a.ownerOperated ? -1 : 1;
      if (a.verified !== b.verified) return a.verified ? -1 : 1;
      return a.operatorName.localeCompare(b.operatorName);
    });

  const groupHotelSum = operatorGroups.reduce((s, g) => s + g.hotelCount, 0);
  const groupRoomSum = operatorGroups.reduce((s, g) => s + g.roomsKnown, 0);
  const groupUnknownSum = operatorGroups.reduce((s, g) => s + g.roomsUnknownHotelCount, 0);

  const hotelsMatch = groupHotelSum === owned.length;
  const roomsMatch =
    groupRoomSum === ownedTotals.roomsKnown &&
    groupUnknownSum === ownedTotals.roomsUnknownHotelCount;

  const completeness = String(opts.completeness || opts.portfolio_status || "PARTIAL").toUpperCase();
  const coverageLabel =
    opts.coverageLabel ||
    (completeness === "COMPLETE"
      ? "Verified Portfolio Coverage"
      : "Verified Portfolio Coverage");

  return {
    version: OWNER_PORTFOLIO_OPERATOR_VIEW_VERSION,
    ownerDisplayName,
    coverageLabel,
    completeness,
    ownedHotels: owned.map((h) => ({
      ...h,
      ownership_relationship_label: formatOwnershipRelationshipLabel(h),
      operator_relationship_label: formatOperatorRelationshipLabel(
        resolvePrimaryOperator(h, ownerDisplayName),
        h
      ),
    })),
    totals: {
      hotels: owned.length,
      roomsKnown: ownedTotals.roomsKnown,
      roomsUnknownHotelCount: ownedTotals.roomsUnknownHotelCount,
      labelHotels: "Verified portfolio hotels",
      labelRooms: "Verified portfolio rooms (known)",
    },
    operatorGroups,
    reconciliation: {
      hotelsMatch,
      roomsMatch,
      ownedHotels: owned.length,
      operatorGroupHotels: groupHotelSum,
      ownedRoomsKnown: ownedTotals.roomsKnown,
      operatorGroupRoomsKnown: groupRoomSum,
      ownedRoomsUnknownHotels: ownedTotals.roomsUnknownHotelCount,
      operatorGroupRoomsUnknownHotels: groupUnknownSum,
      ambiguousOperatorHotelKeys: ambiguous,
      ok: hotelsMatch && roomsMatch && ambiguous.length === 0,
    },
    stats: {
      hotelsOwnedControlled: owned.length,
      hotelsOwnerOperated: operatorGroups.find((g) => g.ownerOperated)?.hotelCount || 0,
      hotelsThirdPartyManaged: operatorGroups
        .filter((g) => g.verified && !g.ownerOperated)
        .reduce((s, g) => s + g.hotelCount, 0),
      hotelsOperatorUnresolved:
        operatorGroups.find((g) => g.operatorKey === "__unverified__")?.hotelCount || 0,
    },
  };
}

/**
 * Filter a mixed portfolio (owned + operated) down to owned/controlled current holdings.
 */
export function selectOwnedControlledHotels(portfolio = []) {
  return dedupeOwnedHotels(portfolio);
}
