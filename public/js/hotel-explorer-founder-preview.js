/**
 * Hotel Explorer Founder Preview — routes ownership/contacts fetches to
 * GET-only founder-preview APIs (Live Airtable vs Phase 3 Proposed).
 * Never writes. Does not change Radar Hotel Explorer behavior.
 */
(function (global) {
  "use strict";

  var DEFAULT_HOTEL = "recLuxvwwxID7U2B8";
  var STORAGE_KEY = "he_founder_preview_mode";
  var origFetch = global.fetch.bind(global);

  function params() {
    return new URLSearchParams(global.location.search || "");
  }

  function resolveMode() {
    var q = params();
    var fromUrl = String(q.get("previewMode") || q.get("mode") || "").trim().toLowerCase();
    if (fromUrl === "proposed" || fromUrl === "phase3") return "proposed";
    if (fromUrl === "live" || fromUrl === "airtable") return "live";
    try {
      var stored = String(global.sessionStorage.getItem(STORAGE_KEY) || "").toLowerCase();
      if (stored === "proposed" || stored === "live") return stored;
    } catch (_e) {}
    return "proposed";
  }

  function setMode(mode) {
    var next = mode === "live" ? "live" : "proposed";
    try {
      global.sessionStorage.setItem(STORAGE_KEY, next);
    } catch (_e) {}
    var u = new URL(global.location.href);
    u.searchParams.set("previewMode", next);
    if (!u.searchParams.get("hotelExplorer") && !u.searchParams.get("hotel")) {
      u.searchParams.set("hotelExplorer", DEFAULT_HOTEL);
    }
    global.location.replace(u.toString());
  }

  function rewriteOwnershipUrl(url) {
    var s = String(url || "");
    var m = s.match(/\/api\/golden-demo\/ownership\/hotel\/([^/?#]+)/);
    if (!m) return null;
    var id = decodeURIComponent(m[1]);
    return (
      "/api/hotel-explorer/founder-preview/" +
      encodeURIComponent(id) +
      "?mode=" +
      encodeURIComponent(resolveMode())
    );
  }

  function rewriteContactsUrl(url) {
    if (resolveMode() !== "proposed") return null;
    var s = String(url || "");
    var m = s.match(/\/api\/contact-intelligence\/hotels\/([^/?#]+)/);
    if (!m) return null;
    var id = decodeURIComponent(m[1]);
    return (
      "/api/hotel-explorer/founder-preview/" +
      encodeURIComponent(id) +
      "/contacts?mode=proposed"
    );
  }

  global.fetch = function (input, init) {
    var url = typeof input === "string" ? input : input && input.url;
    var next = rewriteOwnershipUrl(url) || rewriteContactsUrl(url);
    if (!next) return origFetch(input, init);
    if (typeof input === "string") return origFetch(next, init);
    try {
      return origFetch(new Request(next, input), init);
    } catch (_err) {
      return origFetch(next, init);
    }
  };

  function paintChrome() {
    var mode = resolveMode();
    var banner = document.getElementById("hefp-banner");
    var liveBtn = document.getElementById("hefp-mode-live");
    var propBtn = document.getElementById("hefp-mode-proposed");
    var modeLabel = document.getElementById("hefp-mode-label");
    if (liveBtn) liveBtn.setAttribute("aria-pressed", mode === "live" ? "true" : "false");
    if (propBtn) propBtn.setAttribute("aria-pressed", mode === "proposed" ? "true" : "false");
    if (modeLabel) {
      modeLabel.textContent = mode === "proposed" ? "Phase 3 Proposed Preview" : "Live Airtable";
    }
    if (banner) {
      banner.dataset.mode = mode;
      banner.querySelector("[data-hefp-copy]").textContent =
        mode === "proposed"
          ? "Phase 3 Proposed Preview — staged research overlay on live Airtable. Not persisted. Not customer-approved. GET-only (no writes)."
          : "Live Airtable — only information already persisted and retrievable from canonical sources. GET-only (no writes).";
    }
    document.documentElement.setAttribute("data-he-founder-preview", mode);
  }

  function bindChrome() {
    var liveBtn = document.getElementById("hefp-mode-live");
    var propBtn = document.getElementById("hefp-mode-proposed");
    if (liveBtn) {
      liveBtn.addEventListener("click", function () {
        setMode("live");
      });
    }
    if (propBtn) {
      propBtn.addEventListener("click", function () {
        setMode("proposed");
      });
    }
  }

  function resolveHotelId() {
    var Route = global.HotelIntelligenceGoldenDemoRoute;
    if (Route && Route.resolveRequestedHotelId) {
      return Route.resolveRequestedHotelId(params()) || DEFAULT_HOTEL;
    }
    return (
      String(params().get("hotelExplorer") || params().get("hotel") || "").trim() ||
      DEFAULT_HOTEL
    );
  }

  function enrichHotel(hotel) {
    var id = hotel && (hotel.id || hotel.recordId);
    if (!id || String(id).indexOf("rec") !== 0) return Promise.resolve(hotel);
    function preferSeed(seedVal, apiVal) {
      var s = String(seedVal || "").trim();
      var a = String(apiVal || "").trim();
      if (s && (!a || /^unknown/i.test(a))) return seedVal;
      if (apiVal == null || apiVal === "") return seedVal;
      return apiVal;
    }
    return origFetch("/api/brand-presence/hotel/" + encodeURIComponent(id), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (payload) {
        if (!(payload && payload.success && payload.hotel)) return hotel;
        var api = payload.hotel;
        return Object.assign({}, hotel, api, {
          id: hotel.id || api.id || id,
          recordId: hotel.recordId || api.recordId || id,
          name: preferSeed(hotel.name, api.name),
          city: preferSeed(hotel.city, api.city),
          market: preferSeed(hotel.market, api.market),
          country: preferSeed(hotel.country, api.country),
          brand: preferSeed(hotel.brand, api.brand || api.affiliation),
          rooms:
            api.rooms != null && Number(api.rooms) > 0
              ? api.rooms
              : hotel.rooms != null
                ? hotel.rooms
                : api.rooms,
          chainScale: preferSeed(hotel.chainScale || hotel.chain_scale, api.chainScale || api.chain_scale),
          chain_scale: preferSeed(hotel.chainScale || hotel.chain_scale, api.chainScale || api.chain_scale),
          website: hotel.website || api.website || "",
          latitude: hotel.latitude != null ? hotel.latitude : api.latitude,
          longitude: hotel.longitude != null ? hotel.longitude : api.longitude,
          status: preferSeed(hotel.status, api.status),
        });
      })
      .catch(function () {
        return hotel;
      });
  }

  function openExplorer() {
    var statusEl = document.getElementById("hefp-status");
    function setStatus(msg) {
      if (statusEl) statusEl.textContent = msg || "";
    }
    if (!global.HotelExplorer) {
      setStatus("Hotel Explorer failed to load.");
      return;
    }
    var Route = global.HotelIntelligenceGoldenDemoRoute;
    var hotelId = resolveHotelId();
    var seed =
      (Route && Route.seedForHotelId && Route.seedForHotelId(hotelId)) || {
        id: hotelId,
        recordId: hotelId,
        name: "Hotel " + hotelId,
        status: "Open",
      };
    var tabRaw = String(params().get("hexTab") || params().get("tab") || "hotel").trim();
    if (tabRaw === "overview" || tabRaw === "intelligence") tabRaw = "hotel";
    setStatus("Opening " + (seed.name || hotelId) + "…");
    enrichHotel(seed).then(function (hotel) {
      global.HotelExplorer.open(hotel, {
        tab: tabRaw || "hotel",
        requestedHotelId: hotelId,
      });
      var boot = document.getElementById("hefp-boot");
      if (boot) boot.hidden = true;
      setStatus("");
      // Soften Radar-oriented chrome when present
      try {
        var back = document.querySelector("[data-hex-back], .hex-back, a.hex-nav__back");
        if (back) {
          back.textContent = "Founder Preview";
          back.setAttribute("href", "#");
          back.addEventListener(
            "click",
            function (e) {
              e.preventDefault();
            },
            { once: true }
          );
        }
      } catch (_e) {}
    });
  }

  function boot() {
    global.__HOTEL_EXPLORER_SHARE_MODE = true;
    global.__HE_FOUNDER_PREVIEW = {
      active: true,
      mode: resolveMode,
      defaultHotelId: DEFAULT_HOTEL,
      writeOnGet: false,
    };
    paintChrome();
    bindChrome();
    openExplorer();
  }

  global.HotelExplorerFounderPreview = {
    resolveMode: resolveMode,
    setMode: setMode,
    defaultHotelId: DEFAULT_HOTEL,
    boot: boot,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})(typeof window !== "undefined" ? window : globalThis);
