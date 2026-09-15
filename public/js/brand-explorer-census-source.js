/**
 * P8.10 — Brand Explorer census metrics HPC opt-in (shadow / test).
 *
 * Production default remains Legacy until BRAND_EXPLORER_HPC_V2 is enabled
 * AND this client opts in (or P8.11 flips production default).
 *
 * Overrides:
 *   ?beHpc=1 | ?beHpc=0
 *   localStorage DEALALITY_BE_HPC=1 | 0
 *
 * Does not affect HE / Radar / Scout / OE.
 */
(function (global) {
  "use strict";

  var STORAGE_KEY = "DEALALITY_BE_HPC";
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
          BRAND_EXPLORER_HPC_V2: !!(body && body.BRAND_EXPLORER_HPC_V2),
        };
        try {
          global.DEALALITY_RUNTIME_FLAGS = Object.assign({}, global.DEALALITY_RUNTIME_FLAGS || {}, _flags);
        } catch (err) {
          /* ignore */
        }
        return _flags;
      })
      .catch(function () {
        _flags = {
          BRAND_PRESENCE_HPC_V2: false,
          RADAR_HPC_V2: false,
          SCOUT_HPC_V2: false,
          BRAND_EXPLORER_HPC_V2: false,
        };
        return _flags;
      });
    return _flagsPromise;
  }

  function isBeHpcOptInActive() {
    var beHpc = queryParam("beHpc");
    if (beHpc === "0" || /^false$/i.test(String(beHpc || ""))) return false;
    if (beHpc === "1" || /^true$/i.test(String(beHpc || ""))) return true;

    var stored = storageValue();
    if (stored === "0") return false;
    if (stored === "1") return true;

    if (_flags && _flags.BRAND_EXPLORER_HPC_V2 === true) return true;
    return false;
  }

  function appendBeCensusParams(urlSearchParams) {
    if (!isBeHpcOptInActive()) return urlSearchParams;
    urlSearchParams.set("censusSource", "hpc");
    urlSearchParams.set("product", "brand-explorer");
    urlSearchParams.set("beHpc", "1");
    return urlSearchParams;
  }

  function beCensusHeaders() {
    var headers = { Accept: "application/json", "ngrok-skip-browser-warning": "true" };
    if (isBeHpcOptInActive()) {
      headers["X-Dealality-Census-Source"] = "hpc";
      headers["X-Dealality-Product"] = "brand-explorer";
    }
    return headers;
  }

  global.DealalityBrandExplorerCensusSource = {
    STORAGE_KEY: STORAGE_KEY,
    ensureFlags: ensureFlags,
    isBeHpcOptInActive: isBeHpcOptInActive,
    appendBeCensusParams: appendBeCensusParams,
    beCensusHeaders: beCensusHeaders,
  };
})(typeof window !== "undefined" ? window : globalThis);
