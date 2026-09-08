/**
 * Shared Hotel Explorer property-fundamentals merge (Packet 2.7-R3).
 * Field-aware: sparse / sentinel values never overwrite known good values.
 * Browser script (IIFE). Node tests load via vm.
 */
(function (global) {
  "use strict";

  var SENTINEL_RE =
    /^(unknown(\s+\w+)?|n\/?a|not available|tbd|null|undefined|—|-)$/i;

  function isBlank(v) {
    if (v == null) return true;
    if (typeof v === "number" && !Number.isFinite(v)) return true;
    return !String(v).trim();
  }

  function isSentinelText(v) {
    if (isBlank(v)) return true;
    return SENTINEL_RE.test(String(v).trim());
  }

  function isMissingRooms(v) {
    if (v == null || v === "") return true;
    var n = Number(v);
    if (!Number.isFinite(n)) return isSentinelText(v);
    return n <= 0;
  }

  function isMissingStatus(v) {
    return isSentinelText(v);
  }

  function preferText(preferred, fallback) {
    if (!isSentinelText(preferred)) return preferred;
    if (!isSentinelText(fallback)) return fallback;
    return preferred != null && preferred !== "" ? preferred : fallback;
  }

  function preferRooms(preferred, fallback) {
    if (!isMissingRooms(preferred)) return Number(preferred);
    if (!isMissingRooms(fallback)) return Number(fallback);
    return null;
  }

  function preferStatus(preferred, fallback) {
    if (!isMissingStatus(preferred)) return preferred;
    if (!isMissingStatus(fallback)) return fallback;
    return null;
  }

  function preferCoord(preferred, fallback) {
    var a = Number(preferred);
    var b = Number(fallback);
    if (Number.isFinite(a) && !(a === 0 && (!Number.isFinite(b) || b === 0))) return a;
    if (Number.isFinite(b)) return b;
    return null;
  }

  function stripSentinelDefaults(censusHotel) {
    if (!censusHotel || typeof censusHotel !== "object") return censusHotel;
    var h = Object.assign({}, censusHotel);
    if (isSentinelText(h.name)) h.name = null;
    if (isSentinelText(h.brand)) h.brand = null;
    if (isSentinelText(h.city)) h.city = null;
    if (isSentinelText(h.country)) h.country = null;
    if (isSentinelText(h.region)) h.region = null;
    if (isSentinelText(h.parentCompany)) h.parentCompany = null;
    if (isMissingStatus(h.status)) h.status = null;
    if (isMissingRooms(h.rooms)) h.rooms = null;
    if (h.telephone && !h.phone) h.phone = h.telephone;
    return h;
  }

  function mergePropertyFundamentals(base, overlay, opts) {
    opts = opts || {};
    base = base || {};
    overlay = overlay || {};
    var provenance = Object.assign({}, base._fieldProvenance || {});
    var hotel = Object.assign({}, base, overlay);

    function take(field, value, source) {
      if (value == null || value === "") return;
      hotel[field] = value;
      provenance[field] = {
        value: value,
        source: source || "merged",
        at: new Date().toISOString(),
      };
    }

    var name = preferText(overlay.name, base.name);
    if (!isSentinelText(name)) {
      take("name", name, !isSentinelText(overlay.name) ? "overlay" : "base");
    }

    var rooms = preferRooms(overlay.rooms, base.rooms);
    if (rooms != null) {
      take("rooms", rooms, !isMissingRooms(overlay.rooms) ? "overlay" : "base");
    } else {
      hotel.rooms = null;
      provenance.rooms = { value: null, source: "missing", at: new Date().toISOString() };
    }

    var status = preferStatus(
      overlay.status || overlay.hotelStatus || overlay.operating_status,
      base.status || base.hotelStatus || base.operating_status
    );
    if (status != null) {
      take("status", status, !isMissingStatus(overlay.status || overlay.hotelStatus) ? "overlay" : "base");
      hotel.hotelStatus = status;
    } else {
      hotel.status = null;
      hotel.hotelStatus = null;
    }

    [
      "city",
      "country",
      "market",
      "brand",
      "affiliation",
      "chainScale",
      "chain_scale",
      "parentCompany",
      "managementCompany",
      "address1",
      "address2",
      "state",
      "postalCode",
      "website",
      "phone",
      "telephone",
      "submarket",
      "region",
      "locationType",
    ].forEach(function (key) {
      var val = preferText(overlay[key], base[key]);
      if (!isSentinelText(val)) take(key, val, !isSentinelText(overlay[key]) ? "overlay" : "base");
      else if (isSentinelText(overlay[key]) && !isSentinelText(base[key])) take(key, base[key], "base");
    });

    var phone = preferText(overlay.phone || overlay.telephone, base.phone || base.telephone);
    if (!isSentinelText(phone)) {
      take("phone", phone, "merged");
      take("telephone", phone, "merged");
    }

    var lat = preferCoord(
      overlay.lat != null ? overlay.lat : overlay.latitude,
      base.lat != null ? base.lat : base.latitude
    );
    var lng = preferCoord(
      overlay.lng != null ? overlay.lng : overlay.longitude,
      base.lng != null ? base.lng : base.longitude
    );
    if (lat != null) {
      take("lat", lat, "merged");
      take("latitude", lat, "merged");
    }
    if (lng != null) {
      take("lng", lng, "merged");
      take("longitude", lng, "merged");
    }

    if (isSentinelText(hotel.name) && !isSentinelText(base.name)) hotel.name = base.name;
    if (isSentinelText(hotel.brand) && !isSentinelText(base.brand)) hotel.brand = base.brand;
    if (isSentinelText(hotel.city) && !isSentinelText(base.city)) hotel.city = base.city;
    if (isSentinelText(hotel.country) && !isSentinelText(base.country)) hotel.country = base.country;
    if (isMissingRooms(hotel.rooms) && !isMissingRooms(base.rooms)) hotel.rooms = Number(base.rooms);
    if (isMissingStatus(hotel.status) && !isMissingStatus(base.status)) hotel.status = base.status;

    hotel._fieldProvenance = provenance;

    if (opts.devWarn && typeof console !== "undefined" && console.warn) {
      if (isMissingRooms(hotel.rooms) && (base.id || hotel.id)) {
        console.warn("[HotelPropertyMerge] rooms missing after merge", hotel.id || hotel.recordId, hotel.name);
      }
      if (isMissingStatus(hotel.status) && (base.id || hotel.id)) {
        console.warn("[HotelPropertyMerge] status missing after merge", hotel.id || hotel.recordId, hotel.name);
      }
    }

    return { hotel: hotel, provenance: provenance };
  }

  var api = {
    isBlank: isBlank,
    isSentinelText: isSentinelText,
    isMissingRooms: isMissingRooms,
    isMissingStatus: isMissingStatus,
    preferText: preferText,
    preferRooms: preferRooms,
    preferStatus: preferStatus,
    mergePropertyFundamentals: mergePropertyFundamentals,
    stripSentinelDefaults: stripSentinelDefaults,
  };

  global.DealalityHotelPropertyMerge = api;
})(typeof globalThis !== "undefined" ? globalThis : this);
