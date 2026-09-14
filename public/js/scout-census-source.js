/**
 * P8.8 — Scout census source opt-in (shadow / test).
 *
 * Production default remains Legacy until SCOUT_HPC_V2 is enabled server-side
 * AND this client opts in (or production cutover P8.9 flips the default).
 *
 * Overrides:
 *   ?scoutHpc=1 | ?scoutHpc=0
 *   localStorage DEALALITY_SCOUT_HPC=1 | 0
 *
 * Does not affect Hotel Explorer or Radar.
 */
(function (global) {
  "use strict";

  var STORAGE_KEY = "DEALALITY_SCOUT_HPC";
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
          SCOUT_HPC_V2: !!(body && body.SCOUT_HPC_V2),
        };
        try {
          global.DEALALITY_RUNTIME_FLAGS = Object.assign({}, global.DEALALITY_RUNTIME_FLAGS || {}, _flags);
        } catch (err) {
          /* ignore */
        }
        return _flags;
      })
      .catch(function () {
        _flags = { BRAND_PRESENCE_HPC_V2: false, RADAR_HPC_V2: false, SCOUT_HPC_V2: false };
        return _flags;
      });
    return _flagsPromise;
  }

  /**
   * Sync after ensureFlags(). Query/storage overrides win.
   * Production default ON only when SCOUT_HPC_V2 server flag is true (P8.9).
   */
  function isScoutHpcOptInActive() {
    var scoutHpc = queryParam("scoutHpc");
    if (scoutHpc === "0" || /^false$/i.test(String(scoutHpc || ""))) return false;
    if (scoutHpc === "1" || /^true$/i.test(String(scoutHpc || ""))) return true;

    var stored = storageValue();
    if (stored === "0") return false;
    if (stored === "1") return true;

    if (_flags && _flags.SCOUT_HPC_V2 === true) return true;
    return false;
  }

  function scoutHpcOptInQuery() {
    return "censusSource=hpc&product=scout";
  }

  function appendScoutCensusParams(urlSearchParams) {
    if (!isScoutHpcOptInActive()) return urlSearchParams;
    urlSearchParams.set("censusSource", "hpc");
    urlSearchParams.set("product", "scout");
    return urlSearchParams;
  }

  function scoutCensusHeaders() {
    var headers = { "ngrok-skip-browser-warning": "true", Accept: "application/json" };
    if (isScoutHpcOptInActive()) {
      headers["X-Dealality-Census-Source"] = "hpc";
      headers["X-Dealality-Product"] = "scout";
    }
    return headers;
  }

  function classifyScoutEntity(hotel) {
    if (!hotel || typeof hotel !== "object") {
      return { class: "MISSING_CANONICAL_ID", dhl_: null };
    }
    if (hotel.markerType && hotel.markerType !== "hotel") {
      return { class: "RADAR/SCOUT_OPPORTUNITY_NON_HOTEL", dhl_: null, id: hotel.markerId || hotel.id };
    }
    var dhl = hotel.dhl_ || (String(hotel.id || "").indexOf("dhl_") === 0 ? hotel.id : null);
    if (dhl && String(dhl).indexOf("dhl_") === 0) {
      return {
        class: hotel._bridgedFromLegacyId ? "LEGACY_BRIDGED_TO_DHL" : "DHL_DIRECT",
        dhl_: dhl,
      };
    }
    if (hotel.status === "Pipeline" || hotel.isPipeline) {
      return { class: "PIPELINE_HOTEL", dhl_: null, id: hotel.id };
    }
    return { class: "MISSING_CANONICAL_ID", dhl_: null, id: hotel.id };
  }

  global.DealalityScoutCensusSource = {
    STORAGE_KEY: STORAGE_KEY,
    ensureFlags: ensureFlags,
    isScoutHpcOptInActive: isScoutHpcOptInActive,
    scoutHpcOptInQuery: scoutHpcOptInQuery,
    appendScoutCensusParams: appendScoutCensusParams,
    scoutCensusHeaders: scoutCensusHeaders,
    classifyScoutEntity: classifyScoutEntity,
  };
})(typeof window !== "undefined" ? window : globalThis);
