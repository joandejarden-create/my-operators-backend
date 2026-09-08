/**
 * Golden Demo route resolver — hotelExplorer / hotel / demo slug → hotel seed.
 * Property-driven: no KGPV hardcode when an explicit hotel id is present.
 */
(function (global) {
  "use strict";

  var KGPV_ID = "recUNycnMwOVFX0hc";
  var CAMBRIDGE_ID = "recIwaP1etgx2g9nA";
  var CAMBRIDGE_DHL = "dhl_06G6TRD5N8Q1YVXKSFD1A5E2NT";

  var DEMO_SLUGS = {
    kgpv: KGPV_ID,
    "gsf-pv": KGPV_ID,
    gsf: KGPV_ID,
    "cambridge-beaches": CAMBRIDGE_ID,
    cambridge: CAMBRIDGE_ID,
    "golden-demo-2": CAMBRIDGE_ID,
  };

  var HOTEL_SEEDS = {};
  HOTEL_SEEDS[KGPV_ID] = {
    id: KGPV_ID,
    recordId: KGPV_ID,
    name: "Krystal Grand Puerto Vallarta",
    city: "Puerto Vallarta",
    market: "Puerto Vallarta",
    country: "Mexico",
    rooms: 451,
    chainScale: "Luxury Chain",
    chain_scale: "Luxury Chain",
    brand: "Krystal Grand",
    managementCompany: "Grupo Hotelero Santa Fe",
    status: "Open",
  };
  HOTEL_SEEDS[CAMBRIDGE_ID] = {
    id: CAMBRIDGE_ID,
    recordId: CAMBRIDGE_ID,
    hotel_id: CAMBRIDGE_DHL,
    name: "Cambridge Beaches Resort & Spa",
    city: "Sandys Parish",
    market: "Bermuda",
    country: "Bermuda",
    rooms: 86,
    chainScale: "Independent Luxury",
    chain_scale: "Independent Luxury",
    brand: "Independent",
    managementCompany: "",
    website: "https://www.cambridgebeaches.com",
    latitude: 32.309168,
    longitude: -64.867947,
    status: "Open",
  };
  HOTEL_SEEDS[CAMBRIDGE_DHL] = Object.assign({}, HOTEL_SEEDS[CAMBRIDGE_ID], {
    id: CAMBRIDGE_ID,
    recordId: CAMBRIDGE_ID,
  });

  function normalizeHexTab(tab) {
    var t = String(tab || "hotel").trim() || "hotel";
    if (t === "intelligence" || t === "overview") return "hotel";
    return t;
  }

  function resolveDemoSlug(slug) {
    if (!slug) return null;
    var key = String(slug).trim().toLowerCase();
    return DEMO_SLUGS[key] || null;
  }

  /**
   * Precedence:
   * 1. hotelExplorer
   * 2. hotel
   * 3. demo slug
   * 4. default KGPV (only when nothing specified)
   */
  function resolveRequestedHotelId(params) {
    params = params || new URLSearchParams();
    var explicit =
      String(params.get("hotelExplorer") || "").trim() ||
      String(params.get("hotel") || "").trim() ||
      "";
    if (explicit) return explicit;
    var fromDemo = resolveDemoSlug(params.get("demo"));
    if (fromDemo) return fromDemo;
    return KGPV_ID;
  }

  function seedForHotelId(hotelId) {
    var id = String(hotelId || "").trim();
    if (!id) return null;
    if (HOTEL_SEEDS[id]) return Object.assign({}, HOTEL_SEEDS[id]);
    return {
      id: id,
      recordId: id,
      name: "Hotel " + id,
      status: "Open",
    };
  }

  function resolveGoldenDemoLaunch(search) {
    var params =
      search instanceof URLSearchParams
        ? search
        : new URLSearchParams(String(search || "").replace(/^\?/, ""));
    var hotelId = resolveRequestedHotelId(params);
    var hotel = seedForHotelId(hotelId);
    var tab = normalizeHexTab(params.get("hexTab") || params.get("tab") || "hotel");
    return {
      hotelId: hotelId,
      hotel: hotel,
      tab: tab,
      demo: params.get("demo") || null,
      usedExplicitHotelParam: Boolean(
        String(params.get("hotelExplorer") || "").trim() ||
          String(params.get("hotel") || "").trim()
      ),
      usedDefaultKgpv:
        !String(params.get("hotelExplorer") || "").trim() &&
        !String(params.get("hotel") || "").trim() &&
        !resolveDemoSlug(params.get("demo")),
    };
  }

  function assertRequestedMatchesRendered(requestedId, renderedHotel) {
    var renderedId =
      (renderedHotel &&
        (renderedHotel.id || renderedHotel.recordId || renderedHotel.airtable_record_id)) ||
      "";
    var ok = String(requestedId || "") === String(renderedId || "");
    return {
      ok: ok,
      requested_hotel_id: String(requestedId || ""),
      rendered_hotel_id: String(renderedId || ""),
      rendered_hotel_name: (renderedHotel && renderedHotel.name) || "",
    };
  }

  global.HotelIntelligenceGoldenDemoRoute = {
    KGPV_ID: KGPV_ID,
    CAMBRIDGE_ID: CAMBRIDGE_ID,
    CAMBRIDGE_DHL: CAMBRIDGE_DHL,
    DEMO_SLUGS: DEMO_SLUGS,
    resolveDemoSlug: resolveDemoSlug,
    resolveRequestedHotelId: resolveRequestedHotelId,
    seedForHotelId: seedForHotelId,
    resolveGoldenDemoLaunch: resolveGoldenDemoLaunch,
    normalizeHexTab: normalizeHexTab,
    assertRequestedMatchesRendered: assertRequestedMatchesRendered,
  };
})(typeof window !== "undefined" ? window : globalThis);
