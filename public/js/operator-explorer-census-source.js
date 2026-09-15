/**
 * P8.12 — Operator Explorer census HPC opt-in (shadow / test).
 * Production default remains Legacy until OPERATOR_EXPLORER_HPC_V2 is enabled.
 *
 * ?oeHpc=1 | localStorage DEALALITY_OE_HPC=1
 */
(function (global) {
  "use strict";

  var STORAGE_KEY = "DEALALITY_OE_HPC";
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
          OPERATOR_EXPLORER_HPC_V2: !!(body && body.OPERATOR_EXPLORER_HPC_V2),
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
          OPERATOR_EXPLORER_HPC_V2: false,
        };
        return _flags;
      });
    return _flagsPromise;
  }

  function isOeHpcOptInActive() {
    var oeHpc = queryParam("oeHpc");
    if (oeHpc === "0" || /^false$/i.test(String(oeHpc || ""))) return false;
    if (oeHpc === "1" || /^true$/i.test(String(oeHpc || ""))) return true;
    var stored = storageValue();
    if (stored === "0") return false;
    if (stored === "1") return true;
    if (_flags && _flags.OPERATOR_EXPLORER_HPC_V2 === true) return true;
    return false;
  }

  function oeCensusHeaders() {
    var headers = { Accept: "application/json", "ngrok-skip-browser-warning": "true" };
    if (isOeHpcOptInActive()) {
      headers["X-Dealality-Census-Source"] = "hpc";
      headers["X-Dealality-Product"] = "operator-explorer";
    }
    return headers;
  }

  function appendOeCensusParams(url) {
    if (!isOeHpcOptInActive()) return url;
    var join = url.indexOf("?") >= 0 ? "&" : "?";
    return url + join + "censusSource=hpc&product=operator-explorer&oeHpc=1";
  }

  global.DealalityOperatorExplorerCensusSource = {
    STORAGE_KEY: STORAGE_KEY,
    ensureFlags: ensureFlags,
    isOeHpcOptInActive: isOeHpcOptInActive,
    oeCensusHeaders: oeCensusHeaders,
    appendOeCensusParams: appendOeCensusParams,
  };
})(typeof window !== "undefined" ? window : globalThis);
