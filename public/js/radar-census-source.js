/**
 * P8.6 / P8.7 — Radar census source opt-in.
 *
 * Production default follows server flag RADAR_HPC_V2 via /api/dealality-runtime-flags.
 * Overrides (for shadow / emergency):
 *   ?radarHpc=1 | ?radarHpc=0
 *   localStorage DEALALITY_RADAR_HPC=1 | 0
 *
 * Scout must not load this helper for its bulk fetch.
 */
(function (global) {
  "use strict";

  var STORAGE_KEY = "DEALALITY_RADAR_HPC";
  var _flags = null;
  var _flagsPromise = null;

  function queryParam(name) {
    try {
      return new URLSearchParams(global.location && global.location.search || "").get(name);
    } catch (err) {
      return null;
    }
  }

  function storageValue() {
    try {
      return global.localStorage ? global.localStorage.getItem(STORAGE_KEY) : null;
    } catch (err) {
      return null;
    }
  }

  function ensureFlags() {
    if (_flags) return Promise.resolve(_flags);
    if (_flagsPromise) return _flagsPromise;
    _flagsPromise = fetch("/api/dealality-runtime-flags", {
      headers: { Accept: "application/json", "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.ok ? r.json() : {};
      })
      .then(function (body) {
        _flags = {
          BRAND_PRESENCE_HPC_V2: !!(body && body.BRAND_PRESENCE_HPC_V2),
          RADAR_HPC_V2: !!(body && body.RADAR_HPC_V2),
        };
        try {
          global.DEALALITY_RUNTIME_FLAGS = _flags;
        } catch (err) {
          /* ignore */
        }
        return _flags;
      })
      .catch(function () {
        _flags = { BRAND_PRESENCE_HPC_V2: false, RADAR_HPC_V2: false };
        return _flags;
      });
    return _flagsPromise;
  }

  /**
   * Sync read after ensureFlags(); before that, only query/storage overrides apply.
   */
  function isRadarHpcOptInActive() {
    var radarHpc = queryParam("radarHpc");
    if (radarHpc === "0" || /^false$/i.test(String(radarHpc || ""))) return false;
    if (radarHpc === "1" || /^true$/i.test(String(radarHpc || ""))) return true;

    var stored = storageValue();
    if (stored === "0") return false;
    if (stored === "1") return true;

    if (_flags && _flags.RADAR_HPC_V2 === true) return true;
    return false;
  }

  function radarHpcOptInQuery() {
    return "censusSource=hpc&product=radar";
  }

  function brandPresenceUrl(limit) {
    var lim = limit == null ? 100000 : limit;
    var url = "/api/brand-presence?limit=" + encodeURIComponent(String(lim));
    if (isRadarHpcOptInActive()) {
      url += "&" + radarHpcOptInQuery();
    }
    return url;
  }

  function brandPresenceHeaders() {
    var headers = { "ngrok-skip-browser-warning": "true" };
    if (isRadarHpcOptInActive()) {
      headers["X-Dealality-Census-Source"] = "hpc";
      headers["X-Dealality-Product"] = "radar";
    }
    return headers;
  }

  function classifyPinIdentity(hotel) {
    if (!hotel || typeof hotel !== "object") {
      return { class: "NO_CANONICAL_HOTEL", dhl_: null, recordId: null };
    }
    var dhl = hotel.dhl_ || hotel.dealalityHotelId || null;
    var recordId = hotel.id || hotel.recordId || null;
    if (dhl && String(dhl).indexOf("dhl_") === 0) {
      return {
        class: hotel._bridgedFromLegacyId ? "BRIDGED_TO_DHL" : "DHL_DIRECT",
        dhl_: dhl,
        recordId: recordId,
      };
    }
    if (hotel._radarEntityType && hotel._radarEntityType !== "hotel") {
      return { class: "NON_HOTEL_RADAR_ENTITY", dhl_: null, recordId: recordId };
    }
    if (recordId && String(recordId).indexOf("rec") === 0) {
      return { class: "NO_CANONICAL_HOTEL", dhl_: null, recordId: recordId, note: "legacy_rec_without_dhl" };
    }
    return { class: "AMBIGUOUS", dhl_: null, recordId: recordId };
  }

  function annotateHotelIdentity(hotel) {
    if (!hotel || typeof hotel !== "object") return hotel;
    var id = classifyPinIdentity(hotel);
    hotel.pinIdentityClass = id.class;
    hotel.pinIdentityDhl = id.dhl_;
    return hotel;
  }

  function normalizeChainScaleUnknown(hotel) {
    var raw = hotel && (hotel.chainScale || hotel.propertyType);
    var s = String(raw || "").trim();
    if (!s || /^unknown$/i.test(s)) return "Unknown";
    return s.replace(/\s+Chain\s*$/i, "").trim() || "Unknown";
  }

  global.DealalityRadarCensusSource = {
    STORAGE_KEY: STORAGE_KEY,
    ensureFlags: ensureFlags,
    isRadarHpcOptInActive: isRadarHpcOptInActive,
    radarHpcOptInQuery: radarHpcOptInQuery,
    brandPresenceUrl: brandPresenceUrl,
    brandPresenceHeaders: brandPresenceHeaders,
    classifyPinIdentity: classifyPinIdentity,
    annotateHotelIdentity: annotateHotelIdentity,
    normalizeChainScaleUnknown: normalizeChainScaleUnknown,
  };

  try {
    global.DEALALITY_RADAR_HPC_ACTIVE = isRadarHpcOptInActive();
  } catch (err) {
    global.DEALALITY_RADAR_HPC_ACTIVE = false;
  }
})(typeof window !== "undefined" ? window : globalThis);
