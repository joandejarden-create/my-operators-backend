/**
 * Browser mirror of owner-portfolio-operator-view-v1 (owner-centric portfolio + operators).
 */
(function (global) {
  "use strict";

  var OPERATOR_NOT_VERIFIED_LABEL = "Operator Not Verified";
  var OWNER_OPERATED_LABEL = "Owner-Operated";

  function normName(v) {
    return String(v || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  function hotelIdentityKey(hotel) {
    var id = String((hotel && (hotel.airtable_record_id || hotel.id || hotel.hotel_id)) || "").trim();
    if (id) return "id:" + id;
    var name = normName(hotel && (hotel.hotel_name || hotel.name || hotel.hotel));
    var city = normName(hotel && (hotel.city || hotel.market));
    return "name:" + name + "|" + city;
  }

  function parseRooms(raw) {
    if (raw == null || raw === "" || raw === "—") return null;
    var n = Number(String(raw).replace(/,/g, "").trim());
    return Number.isFinite(n) && n > 0 ? n : null;
  }

  function isOwnedRow(hotel) {
    if (!hotel) return false;
    if (hotel.include_as_owned === true || hotel.economic_owner_verified === true) return true;
    var rel = String(hotel.relationship_type || hotel.relationship || "").toUpperCase();
    return (
      rel === "OWNED_BY" ||
      rel === "CONTROLLED_BY" ||
      rel === "ECONOMIC_OWNER" ||
      rel === "LEGAL_OWNER" ||
      /OWNED|CONTROLLED|ECONOMIC OWNER|LEGAL OWNER/i.test(rel)
    );
  }

  function isCurrentHolding(hotel) {
    var t = String(
      (hotel && (hotel.current_historical || hotel.temporal_status)) || "CURRENT"
    ).toUpperCase();
    return !(t === "HISTORICAL" || t === "FORMER" || t === "DISPOSED" || t === "REALIZED");
  }

  function resolvePrimaryOperator(hotel, ownerDisplayName) {
    var raw =
      (hotel &&
        (hotel.operator ||
          hotel.management_company ||
          hotel.operating_company ||
          hotel.operator_name)) ||
      null;
    var op = String(raw || "").trim();
    if (!op) {
      return {
        operatorKey: "__unverified__",
        operatorLabel: OPERATOR_NOT_VERIFIED_LABEL,
        ownerOperated: false,
        verified: false,
        ambiguous: Boolean(hotel && hotel.operator_ambiguous),
      };
    }
    var ownerKey = normName(ownerDisplayName);
    var opKey = normName(op);
    var ownerOperated =
      Boolean(hotel && hotel.owner_operated) ||
      (ownerKey &&
        opKey &&
        (opKey === ownerKey ||
          opKey.indexOf(ownerKey) >= 0 ||
          ownerKey.indexOf(opKey) >= 0 ||
          /owner.?operat/i.test(op)));
    if (ownerOperated) {
      return {
        operatorKey: "__owner_operated__",
        operatorLabel: OWNER_OPERATED_LABEL,
        ownerOperated: true,
        verified: true,
        displayOperatorName: op,
        ambiguous: false,
      };
    }
    return {
      operatorKey: opKey || op,
      operatorLabel: op,
      ownerOperated: false,
      verified: true,
      ambiguous: Boolean(hotel && hotel.operator_ambiguous),
    };
  }

  function formatOwnershipRelationshipLabel(hotel) {
    var raw = String((hotel && (hotel.relationship_type || hotel.relationship)) || "").toUpperCase();
    if (hotel && hotel.legal_owner && hotel.economic_owner) return "Economic control via PropCo";
    if (raw === "OWNED_BY" || raw === "LEGAL_OWNER") return "Legal / economic ownership";
    if (raw === "CONTROLLED_BY" || raw === "ECONOMIC_OWNER") return "Economic / controlling ownership";
    if (raw === "SPONSORED_BY") return "Economic sponsor interest";
    if (/OWNED|CONTROL/i.test(raw)) return "Owned / controlled";
    if (raw) {
      return raw
        .replace(/_/g, " ")
        .replace(/\b\w/g, function (c) {
          return c.toUpperCase();
        });
    }
    return "Owned / controlled";
  }

  function formatOperatorRelationshipLabel(resolved, hotel) {
    if (resolved.ownerOperated) {
      return (
        "Owner-operated" +
        (resolved.displayOperatorName ? " (" + resolved.displayOperatorName + ")" : "")
      );
    }
    if (!resolved.verified) return "Management company not verified";
    var brand = (hotel && (hotel.brand_display || hotel.brand)) || "";
    return "Managed by " + resolved.operatorLabel + (brand ? " · branded " + brand : "");
  }

  function dedupeOwnedHotels(hotels) {
    var map = {};
    var order = [];
    (hotels || []).forEach(function (raw) {
      if (!raw || !isOwnedRow(raw) || !isCurrentHolding(raw)) return;
      var key = hotelIdentityKey(raw);
      var rooms = parseRooms(raw.rooms_display != null ? raw.rooms_display : raw.rooms);
      var next = Object.assign({}, raw, {
        hotel_name: raw.hotel_name || raw.name || raw.hotel || "Hotel",
        name: raw.hotel_name || raw.name || raw.hotel || "Hotel",
        airtable_record_id: raw.airtable_record_id || raw.id || null,
        rooms: rooms,
        rooms_known: rooms != null,
        brand_display: raw.brand_display || raw.brand || null,
        market_display: raw.market_display || raw.market || raw.city || null,
        operator: raw.operator || raw.management_company || raw.operating_company || null,
      });
      var prev = map[key];
      if (!prev) {
        map[key] = next;
        order.push(key);
        return;
      }
      function score(h) {
        return (h.airtable_record_id ? 4 : 0) + (h.rooms_known ? 2 : 0) + (h.operator ? 1 : 0);
      }
      map[key] = score(next) >= score(prev) ? Object.assign({}, prev, next) : prev;
    });
    return order
      .map(function (k) {
        return map[k];
      })
      .sort(function (a, b) {
        return String(a.hotel_name).localeCompare(String(b.hotel_name));
      });
  }

  function sumRooms(hotels) {
    var known = 0;
    var unknownHotels = 0;
    (hotels || []).forEach(function (h) {
      if (h.rooms_known && h.rooms != null) known += h.rooms;
      else unknownHotels += 1;
    });
    return { roomsKnown: known, roomsUnknownHotelCount: unknownHotels };
  }

  function buildOwnerPortfolioOperatorView(ownedHotels, opts) {
    opts = opts || {};
    var ownerDisplayName = opts.ownerDisplayName || "";
    var owned = dedupeOwnedHotels(ownedHotels);
    var ownedTotals = sumRooms(owned);
    var groupMap = {};
    var groupOrder = [];
    var ambiguous = [];

    owned.forEach(function (hotel) {
      var resolved = resolvePrimaryOperator(hotel, ownerDisplayName);
      if (resolved.ambiguous) ambiguous.push(hotelIdentityKey(hotel));
      var key = resolved.operatorKey;
      if (!groupMap[key]) {
        groupMap[key] = {
          operatorKey: key,
          operatorName: resolved.operatorLabel,
          ownerOperated: resolved.ownerOperated,
          verified: resolved.verified,
          hotels: [],
        };
        groupOrder.push(key);
      }
      groupMap[key].hotels.push(
        Object.assign({}, hotel, {
          operator_relationship_label: formatOperatorRelationshipLabel(resolved, hotel),
          ownership_relationship_label: formatOwnershipRelationshipLabel(hotel),
        })
      );
    });

    var operatorGroups = groupOrder
      .map(function (k) {
        var g = groupMap[k];
        var roomSum = sumRooms(g.hotels);
        return Object.assign({}, g, {
          hotelCount: g.hotels.length,
          roomsKnown: roomSum.roomsKnown,
          roomsUnknownHotelCount: roomSum.roomsUnknownHotelCount,
          hotels: g.hotels.slice().sort(function (a, b) {
            return String(a.hotel_name).localeCompare(String(b.hotel_name));
          }),
        });
      })
      .sort(function (a, b) {
        if (a.ownerOperated !== b.ownerOperated) return a.ownerOperated ? -1 : 1;
        if (a.verified !== b.verified) return a.verified ? -1 : 1;
        return a.operatorName.localeCompare(b.operatorName);
      });

    var groupHotelSum = operatorGroups.reduce(function (s, g) {
      return s + g.hotelCount;
    }, 0);
    var groupRoomSum = operatorGroups.reduce(function (s, g) {
      return s + g.roomsKnown;
    }, 0);
    var groupUnknownSum = operatorGroups.reduce(function (s, g) {
      return s + g.roomsUnknownHotelCount;
    }, 0);

    return {
      version: "owner-portfolio-operator-view-v1",
      ownerDisplayName: ownerDisplayName,
      coverageLabel: opts.coverageLabel || "Verified Portfolio Coverage",
      ownedHotels: owned.map(function (h) {
        return Object.assign({}, h, {
          ownership_relationship_label: formatOwnershipRelationshipLabel(h),
          operator_relationship_label: formatOperatorRelationshipLabel(
            resolvePrimaryOperator(h, ownerDisplayName),
            h
          ),
        });
      }),
      totals: {
        hotels: owned.length,
        roomsKnown: ownedTotals.roomsKnown,
        roomsUnknownHotelCount: ownedTotals.roomsUnknownHotelCount,
      },
      operatorGroups: operatorGroups,
      reconciliation: {
        hotelsMatch: groupHotelSum === owned.length,
        roomsMatch:
          groupRoomSum === ownedTotals.roomsKnown &&
          groupUnknownSum === ownedTotals.roomsUnknownHotelCount,
        ownedHotels: owned.length,
        operatorGroupHotels: groupHotelSum,
        ownedRoomsKnown: ownedTotals.roomsKnown,
        operatorGroupRoomsKnown: groupRoomSum,
        ok:
          groupHotelSum === owned.length &&
          groupRoomSum === ownedTotals.roomsKnown &&
          groupUnknownSum === ownedTotals.roomsUnknownHotelCount &&
          !ambiguous.length,
      },
      stats: {
        hotelsOwnedControlled: owned.length,
        hotelsOwnerOperated: (operatorGroups.filter(function (g) {
          return g.ownerOperated;
        })[0] || { hotelCount: 0 }).hotelCount,
        hotelsThirdPartyManaged: operatorGroups
          .filter(function (g) {
            return g.verified && !g.ownerOperated;
          })
          .reduce(function (s, g) {
            return s + g.hotelCount;
          }, 0),
        hotelsOperatorUnresolved: (operatorGroups.filter(function (g) {
          return g.operatorKey === "__unverified__";
        })[0] || { hotelCount: 0 }).hotelCount,
      },
    };
  }

  global.HeOwnerPortfolioOperatorView = {
    OPERATOR_NOT_VERIFIED_LABEL: OPERATOR_NOT_VERIFIED_LABEL,
    OWNER_OPERATED_LABEL: OWNER_OPERATED_LABEL,
    buildOwnerPortfolioOperatorView: buildOwnerPortfolioOperatorView,
    dedupeOwnedHotels: dedupeOwnedHotels,
    resolvePrimaryOperator: resolvePrimaryOperator,
    formatOwnershipRelationshipLabel: formatOwnershipRelationshipLabel,
    selectOwnedControlledHotels: dedupeOwnedHotels,
  };
})(typeof window !== "undefined" ? window : globalThis);
