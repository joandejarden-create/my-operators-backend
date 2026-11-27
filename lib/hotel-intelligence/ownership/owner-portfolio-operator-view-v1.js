/**
 * Owner-centric portfolio view: Owned/Controlled hotels + operator grouping
 * over the *same* hotel set, with mandatory total reconciliation and
 * verified vs proposed scope filtering.
 *
 * Version: owner-portfolio-operator-view-v1
 */

export const OWNER_PORTFOLIO_OPERATOR_VIEW_VERSION = "owner-portfolio-operator-view-v1";
export const OPERATOR_NOT_VERIFIED_LABEL = "Operator Not Verified";
export const OWNER_OPERATED_LABEL = "Owner-Operated";

export const PORTFOLIO_EVIDENCE = {
  VERIFIED: "VERIFIED",
  PROPOSED: "PROPOSED",
  HISTORICAL: "HISTORICAL",
  UNRESOLVED_MATCH: "UNRESOLVED_MATCH",
};

export const PORTFOLIO_SCOPE = {
  VERIFIED_ONLY: "verified_only",
  VERIFIED_AND_PROPOSED: "verified_and_proposed",
};

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
  if (hotel.include_as_owned === true) return true;
  const rel = String(hotel.relationship_type || hotel.relationship || "").toUpperCase();
  return (
    rel === "OWNED_BY" ||
    rel === "CONTROLLED_BY" ||
    rel === "ECONOMIC_OWNER" ||
    rel === "LEGAL_OWNER" ||
    /OWNED|CONTROLLED|ECONOMIC OWNER|LEGAL OWNER/i.test(rel)
  );
}

function isHistoricalHolding(hotel) {
  const t = String(hotel?.current_historical || hotel?.temporal_status || "CURRENT").toUpperCase();
  return t === "HISTORICAL" || t === "FORMER" || t === "DISPOSED" || t === "REALIZED";
}

function isProposedMarker(hotel) {
  const bucket = String(
    hotel?.verification_bucket || hotel?.verification_state || hotel?.preview_state || ""
  ).toUpperCase();
  if (bucket.includes("PROPOSED")) return true;
  if (String(hotel?.ownership_evidence_class || "").toUpperCase() === "PROPOSED") return true;
  if (hotel?.proposed === true) return true;
  return false;
}

/**
 * Classify ownership evidence. Portfolio website listings alone are PROPOSED,
 * not VERIFIED current legal/economic control.
 */
export function classifyPortfolioHotel(hotel) {
  if (!hotel) {
    return {
      class: PORTFOLIO_EVIDENCE.PROPOSED,
      label: "Proposed",
      pendingHpcMatch: true,
    };
  }
  const explicit = String(hotel.ownership_evidence_class || "").toUpperCase();
  if (explicit && Object.values(PORTFOLIO_EVIDENCE).includes(explicit)) {
    const pending =
      explicit === PORTFOLIO_EVIDENCE.UNRESOLVED_MATCH ||
      (!hotel.airtable_record_id && !hotel.id);
    return {
      class: explicit,
      label:
        explicit === PORTFOLIO_EVIDENCE.VERIFIED
          ? "Verified"
          : explicit === PORTFOLIO_EVIDENCE.HISTORICAL
            ? "Historical"
            : pending
              ? "Proposed · Pending HPC Match"
              : "Proposed",
      pendingHpcMatch: pending,
    };
  }

  if (isHistoricalHolding(hotel)) {
    return {
      class: PORTFOLIO_EVIDENCE.HISTORICAL,
      label: "Historical",
      pendingHpcMatch: false,
    };
  }

  const census = String(hotel.census_match_status || "").toUpperCase();
  const hasHpc = Boolean(String(hotel.airtable_record_id || hotel.id || "").trim());
  const unresolvedMatch =
    census === "UNRESOLVED_HOTEL_CANDIDATE" ||
    census === "UNRESOLVED" ||
    (!hasHpc && isProposedMarker(hotel));

  if (isProposedMarker(hotel)) {
    return {
      class: unresolvedMatch
        ? PORTFOLIO_EVIDENCE.UNRESOLVED_MATCH
        : PORTFOLIO_EVIDENCE.PROPOSED,
      label: unresolvedMatch ? "Proposed · Pending HPC Match" : "Proposed",
      pendingHpcMatch: unresolvedMatch || !hasHpc,
    };
  }

  const bucket = String(hotel.verification_bucket || hotel.confidence || "").toUpperCase();
  const verifiedTokens = ["VERIFIED", "CONFIRMED", "VALIDATED", "HIGH", "LIVE"];
  const looksVerified =
    hotel.ownership_verified === true ||
    (hotel.economic_owner_verified === true && verifiedTokens.includes(bucket)) ||
    verifiedTokens.includes(bucket);

  if (looksVerified) {
    return {
      class: PORTFOLIO_EVIDENCE.VERIFIED,
      label: "Verified",
      pendingHpcMatch: !hasHpc,
    };
  }

  if (unresolvedMatch || !hasHpc) {
    return {
      class: PORTFOLIO_EVIDENCE.UNRESOLVED_MATCH,
      label: "Proposed · Pending HPC Match",
      pendingHpcMatch: true,
    };
  }

  return {
    class: PORTFOLIO_EVIDENCE.PROPOSED,
    label: "Proposed",
    pendingHpcMatch: false,
  };
}

export function partitionPortfolioHotels(hotels = []) {
  const buckets = {
    verified: [],
    proposed: [],
    unresolved_match: [],
    historical: [],
    excluded_operated_only: [],
  };
  for (const raw of hotels || []) {
    if (!raw) continue;
    if (!isOwnedRow(raw)) {
      buckets.excluded_operated_only.push(raw);
      continue;
    }
    const cls = classifyPortfolioHotel(raw);
    if (cls.class === PORTFOLIO_EVIDENCE.HISTORICAL) buckets.historical.push(raw);
    else if (cls.class === PORTFOLIO_EVIDENCE.VERIFIED) buckets.verified.push(raw);
    else if (cls.class === PORTFOLIO_EVIDENCE.UNRESOLVED_MATCH) {
      buckets.unresolved_match.push(raw);
    } else buckets.proposed.push(raw);
  }
  return buckets;
}

/**
 * Apply scope to owned/controlled rows (excludes historical from both scopes).
 */
export function selectHotelsForScope(hotels = [], scope = PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED) {
  const normalized =
    scope === PORTFOLIO_SCOPE.VERIFIED_ONLY
      ? PORTFOLIO_SCOPE.VERIFIED_ONLY
      : PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED;
  const out = [];
  for (const raw of hotels || []) {
    if (!raw || !isOwnedRow(raw)) continue;
    const cls = classifyPortfolioHotel(raw);
    if (cls.class === PORTFOLIO_EVIDENCE.HISTORICAL) continue;
    if (normalized === PORTFOLIO_SCOPE.VERIFIED_ONLY) {
      if (cls.class === PORTFOLIO_EVIDENCE.VERIFIED) out.push(raw);
      continue;
    }
    if (
      cls.class === PORTFOLIO_EVIDENCE.VERIFIED ||
      cls.class === PORTFOLIO_EVIDENCE.PROPOSED ||
      cls.class === PORTFOLIO_EVIDENCE.UNRESOLVED_MATCH
    ) {
      out.push(raw);
    }
  }
  return out;
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
 * Deduplicate hotels by canonical property identity.
 * Prefer rows with airtable_record_id and known rooms.
 * Does not filter by verification class (caller applies scope).
 */
export function dedupePortfolioHotels(hotels = [], { currentOnly = true } = {}) {
  const map = new Map();
  for (const raw of hotels || []) {
    if (!raw) continue;
    if (!isOwnedRow(raw)) continue;
    if (currentOnly && isHistoricalHolding(raw)) continue;
    const key = hotelIdentityKey(raw);
    const rooms = parseRooms(raw.rooms_display != null ? raw.rooms_display : raw.rooms);
    const evidence = classifyPortfolioHotel(raw);
    const hpcId = String(raw.airtable_record_id || raw.id || "").trim() || null;
    const next = {
      ...raw,
      hotel_name: raw.hotel_name || raw.name || raw.hotel || "Hotel",
      name: raw.hotel_name || raw.name || raw.hotel || "Hotel",
      airtable_record_id: hpcId,
      rooms: rooms,
      rooms_known: rooms != null,
      brand_display: raw.brand_display || raw.brand || null,
      market_display: raw.market_display || raw.market || raw.city || null,
      city: raw.city || null,
      operator: raw.operator || raw.management_company || raw.operating_company || null,
      ownership_evidence_class: evidence.class,
      ownership_evidence_label: evidence.label,
      pending_hpc_match: evidence.pendingHpcMatch || !hpcId,
    };
    const prev = map.get(key);
    if (!prev) {
      map.set(key, next);
      continue;
    }
    const score = (h) =>
      (h.airtable_record_id ? 4 : 0) +
      (h.rooms_known ? 2 : 0) +
      (h.operator ? 1 : 0) +
      (h.ownership_evidence_class === PORTFOLIO_EVIDENCE.VERIFIED ? 3 : 0);
    map.set(key, score(next) >= score(prev) ? { ...prev, ...next } : prev);
  }
  return [...map.values()].sort((a, b) =>
    String(a.hotel_name).localeCompare(String(b.hotel_name))
  );
}

/** @deprecated Prefer dedupePortfolioHotels; kept for callers expecting owned-current filter. */
export function dedupeOwnedHotels(hotels = []) {
  return dedupePortfolioHotels(hotels, { currentOnly: true });
}

function sumRooms(hotels) {
  let known = 0;
  let unknownHotels = 0;
  let knownHotelCount = 0;
  for (const h of hotels) {
    if (h.rooms_known && h.rooms != null) {
      known += h.rooms;
      knownHotelCount += 1;
    } else unknownHotels += 1;
  }
  return {
    roomsKnown: known,
    roomsUnknownHotelCount: unknownHotels,
    roomsKnownHotelCount: knownHotelCount,
  };
}

function coverageLabelForScope(scope, opts = {}) {
  if (opts.coverageLabel) return opts.coverageLabel;
  if (scope === PORTFOLIO_SCOPE.VERIFIED_ONLY) return "Verified Portfolio Coverage";
  return "Selected Portfolio Coverage (verified + proposed)";
}

/**
 * Build the owner portfolio + operator grouping view for a selected scope.
 * @param {object[]} ownedHotels - ownership portfolio rows (owned/controlled only)
 * @param {{ ownerDisplayName?: string, coverageLabel?: string, completeness?: string, scope?: string, sharedHotelView?: object, focusHotelId?: string }} [opts]
 */
export function buildOwnerPortfolioOperatorView(ownedHotels = [], opts = {}) {
  const ownerDisplayName = opts.ownerDisplayName || "";
  const scope =
    opts.scope === PORTFOLIO_SCOPE.VERIFIED_ONLY
      ? PORTFOLIO_SCOPE.VERIFIED_ONLY
      : PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED;

  const partitioned = partitionPortfolioHotels(ownedHotels);
  const scopedSeed = selectHotelsForScope(ownedHotels, scope);
  let owned = dedupePortfolioHotels(scopedSeed, { currentOnly: true });

  // Hydrate focus (and optionally map-matched) rows from shared HPC/HI view model.
  // Mapping/rendering defect when rooms already exist on HPC but portfolio shows unresolved.
  if (opts.sharedHotelView && typeof opts.sharedHotelView === "object") {
    const focusHotelId = opts.focusHotelId || opts.sharedHotelView.hotelId || null;
    owned = owned.map((row) => {
      const rowId = String(row.airtable_record_id || row.id || "").trim();
      const isFocus =
        row.focus_hotel === true ||
        (focusHotelId && rowId && rowId === focusHotelId);
      if (!isFocus) return row;
      const shared = opts.sharedHotelView;
      const next = { ...row };
      if (shared.rooms != null && (next.rooms == null || next.rooms_known !== true)) {
        next.rooms = shared.rooms;
        next.rooms_known = true;
        next.rooms_display = String(shared.rooms);
        next.rooms_source = shared.sources?.rooms || "shared_hotel_view_model";
      }
      if (shared.brand && !(next.brand_display || next.brand)) {
        next.brand = shared.brand;
        next.brand_display = shared.brand;
      }
      if (shared.market && !(next.market_display || next.market)) {
        next.market = shared.market;
        next.market_display = shared.market;
      }
      if (shared.operator && !(next.operator || next.management_company)) {
        next.operator = shared.operator;
        next.management_company = shared.operator;
      }
      if (shared.name && (!next.hotel_name || next.hotel_name === rowId || next.name === rowId)) {
        next.hotel_name = shared.name;
        next.name = shared.name;
      }
      if (shared.status && !next.status) next.status = shared.status;
      next.pending_hpc_match = false;
      next.hydrated_from_shared_view = true;
      return next;
    });
  }

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
        roomsKnownHotelCount: roomSum.roomsKnownHotelCount,
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
  const verifiedCount = dedupePortfolioHotels(partitioned.verified).length;
  const proposedCount =
    dedupePortfolioHotels(partitioned.proposed).length +
    dedupePortfolioHotels(partitioned.unresolved_match).length;
  const historicalCount = partitioned.historical.length;
  const unresolvedMatchCount = dedupePortfolioHotels(partitioned.unresolved_match).length;

  return {
    version: OWNER_PORTFOLIO_OPERATOR_VIEW_VERSION,
    ownerDisplayName,
    scope,
    coverageLabel: coverageLabelForScope(scope, opts),
    completeness,
    partition: {
      verified: verifiedCount,
      proposed: proposedCount,
      unresolved_match: unresolvedMatchCount,
      historical: historicalCount,
    },
    ownedHotels: owned.map((h) => {
      const evidence = classifyPortfolioHotel(h);
      return {
        ...h,
        ownership_evidence_class: evidence.class,
        ownership_evidence_label: evidence.label,
        pending_hpc_match: evidence.pendingHpcMatch || !h.airtable_record_id,
        ownership_relationship_label: formatOwnershipRelationshipLabel(h),
        operator_relationship_label: formatOperatorRelationshipLabel(
          resolvePrimaryOperator(h, ownerDisplayName),
          h
        ),
      };
    }),
    totals: {
      hotels: owned.length,
      roomsKnown: ownedTotals.roomsKnown,
      roomsKnownHotelCount: ownedTotals.roomsKnownHotelCount,
      roomsUnknownHotelCount: ownedTotals.roomsUnknownHotelCount,
      labelHotels: "Selected portfolio hotels",
      labelRooms: "Known rooms (not a complete portfolio-room total)",
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
      hotelsVerifiedInUniverse: verifiedCount,
      hotelsProposedInUniverse: proposedCount,
      hotelsHistoricalExcluded: historicalCount,
    },
  };
}

/**
 * Filter a mixed portfolio (owned + operated) down to owned/controlled current holdings.
 */
export function selectOwnedControlledHotels(portfolio = []) {
  return dedupePortfolioHotels(portfolio, { currentOnly: true });
}
