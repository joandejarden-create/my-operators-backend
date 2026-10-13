/**
 * Opportunity Radar external share mode (pack=mexico, share=1).
 * Read-only: blocks write fetch methods; locks country; scopes brand-presence GETs.
 */
(function (global) {
  "use strict";

  function readParams() {
    try {
      return new URLSearchParams(global.location.search || "");
    } catch (_e) {
      return new URLSearchParams();
    }
  }

  function detectShareMode() {
    var params = readParams();
    if (params.get("share") === "1") return true;
    if (params.get("sharePack")) return true;
    if (global.__OR_SHARE_PACK__) return true;
    try {
      return /opportunity-radar-share\.html$/i.test(String(global.location.pathname || ""));
    } catch (_e2) {
      return false;
    }
  }

  function shareLocks() {
    var pack = global.__OR_SHARE_PACK__ || null;
    var params = readParams();
    return {
      country: String(
        (pack && pack.country) || params.get("country") || ""
      ).trim(),
      market: String((pack && pack.market) || params.get("market") || "").trim(),
      packKey: String((pack && pack.key) || params.get("sharePack") || params.get("pack") || "").trim(),
    };
  }

  var SHARE_MODE = detectShareMode();
  var LOCKS = shareLocks();

  function appendScopedQuery(url) {
    if (!SHARE_MODE || !LOCKS.country) return url;
    try {
      var abs = new URL(url, global.location.origin);
      if (!/\/api\/brand-presence(?:\/|$|\?)/i.test(abs.pathname)) return url;
      if (!abs.searchParams.get("country")) {
        abs.searchParams.set("country", LOCKS.country);
      }
      return abs.pathname + abs.search + abs.hash;
    } catch (_e) {
      return url;
    }
  }

  function installWriteGuard() {
    if (!SHARE_MODE || typeof global.fetch !== "function") return;
    if (global.__OR_SHARE_FETCH_GUARDED__) return;
    global.__OR_SHARE_FETCH_GUARDED__ = true;
    var orig = global.fetch.bind(global);
    global.fetch = function (input, init) {
      var opts = init || {};
      var method = String(opts.method || "GET").toUpperCase();
      if (method !== "GET" && method !== "HEAD" && method !== "OPTIONS") {
        return Promise.reject(
          new Error("Opportunity Radar share is read-only. Write actions are disabled.")
        );
      }
      var url = typeof input === "string" ? input : input && input.url;
      if (typeof url === "string") {
        var scoped = appendScopedQuery(url);
        if (scoped !== url) {
          if (typeof input === "string") return orig(scoped, opts);
          return orig(scoped, opts);
        }
      }
      return orig(input, opts);
    };
  }

  function applyCountryLock() {
    if (!SHARE_MODE || !LOCKS.country) return;
    if (typeof global.currentFilters === "object" && global.currentFilters) {
      global.currentFilters.country = LOCKS.country;
    }
    // brand-presence-mapping keeps currentFilters in module scope — set via helpers below
    var ids = ["countryFilter", "countryFilterDrawer"];
    ids.forEach(function (id) {
      var el = global.document && global.document.getElementById(id);
      if (!el) return;
      el.value = LOCKS.country;
      el.disabled = true;
      el.setAttribute("aria-readonly", "true");
      el.title = "Country locked for this shared preview";
    });
  }

  function applyShareChromeClass() {
    if (!SHARE_MODE || !global.document || !global.document.body) return;
    global.document.body.classList.add("or-share-mode");
  }

  global.OpportunityRadarShareMode = {
    enabled: SHARE_MODE,
    locks: LOCKS,
    applyCountryLock: applyCountryLock,
    appendScopedQuery: appendScopedQuery,
  };

  installWriteGuard();

  if (SHARE_MODE) {
    if (global.document && global.document.readyState === "loading") {
      global.document.addEventListener("DOMContentLoaded", function () {
        applyShareChromeClass();
        applyCountryLock();
      });
    } else {
      applyShareChromeClass();
      applyCountryLock();
    }
  }
})(typeof window !== "undefined" ? window : globalThis);
