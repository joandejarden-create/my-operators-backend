/**
 * Hilton-inspired hotel detail panel for Dealality Radar map pins.
 * Layout: left = name, actions, description; right = amenities (icons from Amenities field).
 */
(function () {
  "use strict";

  var panelEl = null;
  var backdropEl = null;
  var layoutSyncBound = false;
  var lockedTabPanelsHeight = null;

  function syncPanelLayout(opts) {
    var updateSize = !opts || opts.updateSize !== false;
    var root = document.documentElement;
    var header = document.querySelector(".mapping-header");
    var headerBottom = 12;
    if (header) {
      headerBottom = Math.max(12, Math.round(header.getBoundingClientRect().bottom) + 8);
    }
    root.style.setProperty("--hdp-panel-top", headerBottom + "px");

    var mapEl = document.querySelector(".map-container");
    if (!mapEl) {
      root.style.setProperty("--hdp-panel-left", "50%");
      root.style.setProperty("--hdp-panel-top-center", "50%");
      if (panelEl) {
        panelEl.style.maxHeight = "calc(100vh - " + (headerBottom + 24) + "px)";
        if (isOpen && updateSize) {
          lockTabPanelsHeight(true);
        }
      }
      return;
    }

    var rect = mapEl.getBoundingClientRect();
    var centerX = rect.left + rect.width / 2;
    var minTop = headerBottom + 12;
    var maxBottom = window.innerHeight - 12;
    var availableHeight = Math.max(240, Math.min(rect.height - 24, maxBottom - minTop));
    var centerY = Math.min(
      Math.max(rect.top + rect.height / 2, minTop + availableHeight / 2),
      maxBottom - availableHeight / 2
    );

    root.style.setProperty("--hdp-backdrop-top", rect.top + "px");
    root.style.setProperty("--hdp-backdrop-left", rect.left + "px");
    root.style.setProperty("--hdp-backdrop-width", rect.width + "px");
    root.style.setProperty("--hdp-backdrop-height", rect.height + "px");
    root.style.setProperty("--hdp-panel-left", centerX + "px");
    root.style.setProperty("--hdp-panel-top-center", centerY + "px");

    if (panelEl && updateSize) {
      panelEl.style.maxHeight = availableHeight + "px";
      if (isOpen) {
        lockTabPanelsHeight(true);
      }
    }
  }

  function resetLockedPanelSize() {
    lockedTabPanelsHeight = null;
    if (!panelEl) return;
    panelEl.style.height = "";
    panelEl.style.minHeight = "";
  }

  function bindPanelLayoutSync() {
    if (layoutSyncBound) return;
    layoutSyncBound = true;
    window.addEventListener("resize", function () {
      syncPanelLayout({ updateSize: true });
    });
    window.addEventListener("scroll", function () {
      syncPanelLayout({ updateSize: false });
    }, { passive: true });
  }
  var mountEl = null;
  var isOpen = false;
  var embedMode = false;
  var presentationOverrides = null;
  var currentHotel = null;

  function applyPresentation(hotel) {
    if (!hotel || !presentationOverrides) return hotel;
    return Object.assign({}, hotel, presentationOverrides);
  }

  var CONTEXT_CONFIG = {
    nearbyLimit: 12,
    infraLimit: 12,
    competitiveLimit: 50,
    demandDriversLimit: 40,
    minAirports: 2,
  };

  /** Area Hotels scope — location-type radii + submarket + auto-expand (see docs). */
  /** Initial search radii (km). Founder defaults: Urban 5mi, Suburban/Airport 10mi, Resort 30mi, Rural 25mi. */
  var AREA_HOTELS_SCOPE = {
    minCompHotels: 5,
    listLimit: 50,
    maxRadiusKm: 80,
    byLocationType: {
      Urban: 8.05,
      Suburban: 16.09,
      Airport: 16.09,
      "Small Metro/Town": 40.23,
      Interstate: 40.23,
      Resort: 48.28,
      default: 16.09,
    },
    denseUrbanKm: 8.05,
    islandKm: 40.23,
    sparseCorridorKm: 48.28,
    denseUrbanCities: {
      "mexico city": true,
      "ciudad de mexico": true,
      bogota: true,
      bogotá: true,
      "sao paulo": true,
      "são paulo": true,
      "rio de janeiro": true,
      "buenos aires": true,
      santiago: true,
      lima: true,
      "panama city": true,
      "santo domingo": true,
      "san juan": true,
      medellin: true,
      medellín: true,
    },
    islandCountries: {
      aruba: true,
      barbados: true,
      bahamas: true,
      bermuda: true,
      "cayman islands": true,
      curaçao: true,
      curacao: true,
      dominica: true,
      grenada: true,
      jamaica: true,
      "puerto rico": true,
      "saint lucia": true,
      "st. lucia": true,
      "turks and caicos": true,
      "turks and caicos islands": true,
      "turks & caicos": true,
      anguilla: true,
      "antigua and barbuda": true,
      "saint kitts and nevis": true,
      "st. kitts and nevis": true,
      "saint vincent and the grenadines": true,
      "st. vincent and the grenadines": true,
      "saint-martin (french part)": true,
      "sint maarten (dutch part)": true,
      "bonaire, sint eustatius and saba": true,
      montserrat: true,
      "saint-barthélemy": true,
      "st. barts": true,
    },
    sparseCorridorKeys: {
      "miches / costa esmeralda": true,
      "samaná / las terrenas": true,
      "samana / las terrenas": true,
      "barahona / pedernales": true,
      "jarabacoa / constanza": true,
      "boca chica / juan dolio": true,
      "east coast / island access": true,
      "vieques / culebra": true,
      "southwest nature & beach corridor": true,
    },
    submarketScopeTypes: {
      Urban: true,
      Suburban: true,
    },
  };

  var INFRA_TYPE_ORDER = ["Airport", "Cruise Port", "Train Station", "Highway Access", "Convention Center", "Ferry Terminal", "Bus Terminal", "Port / Maritime"];

  /** Demand Anchors region values — hotel census uses different labels (e.g. CALA). */
  var DEMAND_ANCHOR_REGION_VALUES = {
    Caribbean: true,
    Mexico: true,
    "Central America": true,
    Colombia: true,
    "South America": true,
    CALA: true,
  };

  /* ── Inline SVG icons (Hilton-style line icons) ── */

  var ACTION_ICON_BASE = "/icons/hotel-actions/";
  var ACTION_ICON_FILES = {
    pin: "directions.png",
    building: "website.png",
    phone: "phone.png",
  };

  function actionIconHtml(iconKey) {
    var file = ACTION_ICON_FILES[iconKey];
    if (file) {
      return (
        '<img class="hdp-action-icon-img" src="' +
        ACTION_ICON_BASE +
        file +
        '" alt="" />'
      );
    }
    return ICONS[iconKey] || ICONS.default;
  }

  var ICONS = {
    wifi: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12.55a11 11 0 0 1 14.08 0M8.53 16.11a6 6 0 0 1 6.95 0M12 20h.01"/><path d="M2 8.82a15 15 0 0 1 20 0"/></svg>',
    pool: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 12h2M6 12h2M10 12h2M14 12h2M18 12h2M22 12h0"/><path d="M2 17c2-1 4 1 6 0s4-1 6 0 4 1 6 0M2 7c2-1 4 1 6 0s4-1 6 0 4 1 6 0"/></svg>',
    restaurant: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 2v7c0 1.1.9 2 2 2h0a2 2 0 0 0 2-2V2M7 2v20M21 15V2v0a5 5 0 0 0-5 5v6c0 1.1.9 2 2 2h3Zm0 0v7"/></svg>',
    spa: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2a7 7 0 0 0-4 12.7V22h8v-7.3A7 7 0 0 0 12 2z"/><path d="M8 22h8"/></svg>',
    fitness: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6.5 6.5 11 11M21 3 18 6M3 21l3-3M18 6l-3 3M6 18l3-3M3 3l3 3M15 15l3 3"/></svg>',
    parking: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M9 17V7h4a3 3 0 0 1 0 6H9"/></svg>',
    pet: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="4" r="2"/><circle cx="18" cy="8" r="2"/><circle cx="20" cy="16" r="2"/><circle cx="4" cy="16" r="2"/><path d="M9 10a5 5 0 0 0-2 8 5 5 0 0 0 8-2 5 5 0 0 0-6-6z"/></svg>',
    beach: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3 4 21h16L12 3z"/><path d="M2 21h20"/></svg>',
    golf: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 18V4l7 4-7 4"/><circle cx="12" cy="18" r="2"/></svg>',
    casino: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="2" y="4" width="20" height="16" rx="2"/><path d="M12 8v8M8 12h8"/></svg>',
    meeting: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="2" y="7" width="20" height="14" rx="2"/><path d="M16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2"/></svg>',
    ski: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m22 4-10 10M2 22l5-5"/><path d="m12 14 4 4M8 18l4-4"/></svg>',
    resort: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 21h18M5 21V7l7-4 7 4v14"/><path d="M9 21v-6h6v6"/></svg>',
    suite: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 7v11a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V7"/><path d="M21 10H3M7 7V5a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v2"/></svg>',
    smokeFree: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 12h10M18 12h4"/><circle cx="14" cy="12" r="2"/><path d="m4 4 16 16"/></svg>',
    ac: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2v20M2 12h20M4.93 4.93l14.14 14.14M19.07 4.93 4.93 19.07"/></svg>',
    bar: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 22h8M12 11v11M5 2l7 9 7-9"/></svg>',
    boutique: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 2 2 7h7l-5.5 4 2 7L12 16l-5.5 4 2-7L3 9h7l2-7z"/></svg>',
    concierge: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 8a6 6 0 0 1 12 0v3l2 2v5H4v-5l2-2V8z"/><path d="M10 21h4"/></svg>',
    crib: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 9v12M22 9v12M2 13h20"/><path d="M5 9V6a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v3"/><circle cx="8" cy="5" r="1"/><circle cx="16" cy="5" r="1"/></svg>',
    digitalKey: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="7" y="2" width="10" height="20" rx="2"/><circle cx="12" cy="18" r="1"/><path d="M9 6h6M10 10h4"/></svg>',
    allInclusive: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 11h18M12 3v18"/><circle cx="12" cy="12" r="9"/></svg>',
    childcare: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="8" r="4"/><path d="M6 21v-1a6 6 0 0 1 12 0v1"/></svg>',
    tennis: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M4 4c3 3 5 8 5 8s5-2 8-5"/></svg>',
    newHotel: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 21h18M5 21V8l7-4 7 4v13M9 21v-5h6v5"/><path d="M12 4v3M10.5 5.5h3"/></svg>',
    luxury: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 2 3 7h7l-3 13-3-13h7l-3-7z"/></svg>',
    noPets: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="4" r="2"/><circle cx="18" cy="8" r="2"/><path d="M9 10a5 5 0 0 0-2 8 5 5 0 0 0 8-2"/><path d="m4 4 16 16"/></svg>',
    connecting: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="2" y="7" width="8" height="10" rx="1"/><rect x="14" y="7" width="8" height="10" rx="1"/><path d="M10 12h4"/></svg>',
    elevator: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="2" width="14" height="20" rx="2"/><path d="M12 7v10M9 10l3-3 3 3M9 14l3 3 3-3"/></svg>',
    laundry: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="4" y="2" width="16" height="20" rx="2"/><circle cx="12" cy="13" r="5"/><path d="M8 6h8"/></svg>',
    business: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="2" y="7" width="20" height="14" rx="2"/><path d="M16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2"/></svg>',
    kitchen: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 3v8a4 4 0 0 0 8 0V3M12 3v8a4 4 0 0 0 8 0V3"/><path d="M4 21h16"/></svg>',
    balcony: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="10" width="18" height="11" rx="1"/><path d="M7 10V6h10v4M3 14h18"/></svg>',
    default: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/></svg>'
  };

  /** Exact / normalized label → icon (checked before regex rules). */
  var AMENITY_ICON_MAP = {
    "all inclusive": "allInclusive",
    "all-inclusive": "allInclusive",
    "beach access": "beach",
    "boutique": "boutique",
    "concierge": "concierge",
    "cribs available": "crib",
    "digital key": "digitalKey",
    "executive lounge": "bar",
    "fitness center": "fitness",
    "free parking": "parking",
    "free wifi": "wifi",
    "meeting rooms": "meeting",
    "meeting room": "meeting",
    "meeting event space": "meetingEvent",
    "wedding services": "wedding",
    "coffee tea in room": "coffeeTea",
    "new hotel": "newHotel",
    "non-smoking rooms": "smokeFree",
    "non smoking rooms": "smokeFree",
    "on-site restaurant": "restaurant",
    "onsite restaurant": "restaurant",
    "outdoor pool": "pool",
    "indoor pool": "pool",
    "pets not allowed": "noPets",
    "pet friendly": "pet",
    "pet-friendly": "pet",
    "pet friendly rooms": "pet",
    "resort": "resort",
    "room service": "restaurant",
    "spa": "spa",
    "childcare": "childcare",
    "tennis court": "tennis",
    "hotel residence": "suite",
    "luxury": "luxury",
    "connecting rooms": "connecting",
    "elevator": "elevator",
    "laundry": "laundry",
    "business center": "business",
    "kitchen": "kitchen",
    "balcony": "balcony",
    "valet parking": "parking",
    "airport shuttle": "pin",
    "free breakfast": "restaurant",
    "complimentary breakfast": "restaurant"
  };

  var AMENITY_ICON_RULES = [
    { match: /all.?inclusive/i, icon: "allInclusive" },
    { match: /boutique/i, icon: "boutique" },
    { match: /concierge/i, icon: "concierge" },
    { match: /crib/i, icon: "crib" },
    { match: /digital key/i, icon: "digitalKey" },
    { match: /executive lounge/i, icon: "bar" },
    { match: /new hotel/i, icon: "newHotel" },
    { match: /pets?\s+not\s+allowed|no pets/i, icon: "noPets" },
    { match: /childcare|child care|kids club/i, icon: "childcare" },
    { match: /tennis/i, icon: "tennis" },
    { match: /connecting room/i, icon: "connecting" },
    { match: /elevator|lift/i, icon: "elevator" },
    { match: /laundry/i, icon: "laundry" },
    { match: /business center/i, icon: "business" },
    { match: /kitchen|kitchenette/i, icon: "kitchen" },
    { match: /balcony|terrace/i, icon: "balcony" },
    { match: /luxury/i, icon: "luxury" },
    { match: /beach|oceanfront|waterfront/i, icon: "beach" },
    { match: /free parking|parking|valet/i, icon: "parking" },
    { match: /free wifi|wi-fi|wifi|internet|wireless/i, icon: "wifi" },
    { match: /outdoor pool|indoor pool|pool|swim/i, icon: "pool" },
    { match: /room service/i, icon: "restaurant" },
    { match: /on.?site restaurant|restaurant|dining|breakfast/i, icon: "restaurant" },
    { match: /wedding/i, icon: "wedding" },
    { match: /coffee\/tea|coffee tea|tea in room|coffee maker/i, icon: "coffeeTea" },
    { match: /meeting event space|event space/i, icon: "meetingEvent" },
    { match: /meeting room|meeting space|conference|convention|ballroom/i, icon: "meeting" },
    { match: /spa|wellness/i, icon: "spa" },
    { match: /fitness|gym|exercise/i, icon: "fitness" },
    { match: /pet.?friendly|pet friendly/i, icon: "pet" },
    { match: /\bpet/i, icon: "pet" },
    { match: /golf/i, icon: "golf" },
    { match: /casino|gaming/i, icon: "casino" },
    { match: /ski|snow/i, icon: "ski" },
    { match: /resort/i, icon: "resort" },
    { match: /suite|all suite/i, icon: "suite" },
    { match: /non.?smok|smoke.?free/i, icon: "smokeFree" },
    { match: /air.?cond|a\/c|climate/i, icon: "ac" },
    { match: /bar|lounge|cocktail/i, icon: "bar" },
    { match: /shuttle/i, icon: "pin" }
  ];

  function normalizeAmenityKey(name) {
    return String(name || "").toLowerCase().replace(/\s+/g, " ").trim();
  }

  function esc(value) {
    if (value == null || value === "") return "";
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function displayValue(value, fallback) {
    if (value == null || value === "") return fallback || "—";
    return String(value);
  }

  function formatParentCompanyDisplay(value, hotel) {
    if (value == null || value === "" || value === "Unknown") return "No Parent Company (Blank)";
    var semantic =
      hotel &&
      hotel.fieldProvenance &&
      hotel.fieldProvenance.parentCompany &&
      hotel.fieldProvenance.parentCompany.semantic;
    if (semantic === "brand_parent_company") {
      return String(value) + " (brand parent)";
    }
    return String(value);
  }

  function formatNumber(value) {
    var num = Number(value);
    if (!Number.isFinite(num)) return "—";
    return num.toLocaleString();
  }

  function formatPercent(value) {
    var num = Number(value);
    if (!Number.isFinite(num)) return "—";
    if (num <= 1) return Math.round(num * 100) + "%";
    return Math.round(num) + "%";
  }

  function formatCurrency(value) {
    var num = Number(value);
    if (!Number.isFinite(num)) return "—";
    return "$" + num.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }

  function formatDate(value) {
    if (!value) return "—";
    var date = new Date(value);
    if (Number.isNaN(date.getTime())) return displayValue(value);
    return date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
  }

  function statusBadgeClass(status) {
    var s = String(status || "").toLowerCase();
    if (s === "pipeline") return "hdp-status-pill--pipeline";
    if (s === "candidate") return "hdp-status-pill--candidate";
    return "";
  }

  function iconForAmenity(name) {
    var label = String(name || "");
    var key = normalizeAmenityKey(label);
    if (AMENITY_ICON_MAP[key]) return AMENITY_ICON_MAP[key];
    for (var i = 0; i < AMENITY_ICON_RULES.length; i++) {
      if (AMENITY_ICON_RULES[i].match.test(label)) return AMENITY_ICON_RULES[i].icon;
    }
    return "default";
  }

  /**
   * Parse amenities from the Airtable Amenities field only.
   * Supports comma, semicolon, pipe, and newline separators.
   */
  function parseAmenitiesField(raw) {
    if (!raw || !String(raw).trim()) return [];
    var text = String(raw).trim();
    var parts = text
      .split(/[,;|\n]+|(?:\s+and\s+)/i)
      .map(function (part) { return part.replace(/^[\s•\-–*]+/, "").trim(); })
      .filter(function (part) {
        return part.length > 0 && part.length < 80;
      });
    if (parts.length === 0 && text.length <= 80) return [text];
    if (parts.length === 1 && parts[0].length > 60) {
      return [parts[0].slice(0, 77) + "…"];
    }
    return parts.slice(0, 20);
  }

  function buildDescription(hotel) {
    var parts = [];
    var scale = hotel.chainScale || hotel.propertyType;
    var censusType = hotel.censusPropertyType;
    var locationType = hotel.locationType;

    if (censusType) {
      parts.push(censusType + " hotel");
    } else if (scale) {
      parts.push(scale + " hotel");
    } else {
      parts.push("Hotel property");
    }

    if (hotel.brand && hotel.brand !== "Unknown Brand") {
      parts.push("affiliated with " + hotel.brand);
    }

    if (hotel.parentCompany && hotel.parentCompany !== "Unknown") {
      parts.push("under " + hotel.parentCompany);
    }

    if (locationType) {
      parts.push("in a " + locationType.toLowerCase() + " location");
    }

    if (hotel.city && hotel.country) {
      parts.push("serving " + hotel.city + ", " + hotel.country);
    }

    if (hotel.rooms) {
      parts.push("with " + formatNumber(hotel.rooms) + " rooms");
    }

    var status = String(hotel.status || "");
    if (status === "Pipeline" && hotel.projectPhase) {
      parts.push("currently in " + hotel.projectPhase.toLowerCase() + " phase");
    } else if (status === "Pipeline" && hotel.projectedOpenDate) {
      parts.push("targeting opening " + formatDate(hotel.projectedOpenDate));
    } else if (status === "Open" && hotel.openDate) {
      parts.push("opened " + formatDate(hotel.openDate));
    }

    return parts.join(", ") + ".";
  }

  function isLowQualityGeneratedDescription(text, hotel) {
    var s = String(text || "").trim();
    if (!s) return true;
    if (/hotel\s+hotel/i.test(s)) return true;
    if (/signals\s*:/i.test(s)) return true;
    if (/_[a-z0-9]+_/i.test(s)) return true;
    if (/\brec[A-Za-z0-9]{10,}\b/.test(s)) return true;
    if (/\bdle_[A-Z0-9]+\b/i.test(s)) return true;
    if (/the physical product comprises/i.test(s)) return true;
    if (/on the physical-product side/i.test(s)) return true;
    if (/it sits in/i.test(s)) return true;
    if (/day-to-day operations are managed by/i.test(s)) return true;
    if (/\b\d+\s+keys\b/i.test(s) && s.length < 120) return true;
    if (/^(OPEN|PIPELINE|CLOSED)\b/i.test(s) && s.length < 80) return true;
    var commas = (s.match(/,/g) || []).length;
    if (commas >= 6 && s.split(".").length <= 2) return true;
    if (/^hotel property,/i.test(s) && !(hotel && hotel.brand && hotel.brand !== "Unknown Brand")) {
      return true;
    }
    var rich =
      (hotel && Number(hotel.rooms) > 0) ||
      (hotel && hotel.brand && hotel.brand !== "Unknown Brand") ||
      (hotel && Number(hotel.totalMeetingSpace) > 0);
    if (rich && s.length < 420) return true;
    var censusType = hotel && hotel.censusPropertyType ? String(hotel.censusPropertyType).trim() : "";
    if (censusType && new RegExp("^" + censusType.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\s+hotel,", "i").test(s)) {
      var hasBrand = hotel.brand && hotel.brand !== "Unknown Brand";
      var hasParent = hotel.parentCompany && hotel.parentCompany !== "Unknown";
      var hasRooms = Number(hotel.rooms) > 0;
      if (!hasBrand && !hasParent && !hasRooms) return true;
      if (!hasBrand && !hasParent && /serving\s+/i.test(s) && s.split(",").length <= 3) {
        return true;
      }
    }
    return false;
  }

  function formatDescriptionHtml(text) {
    var raw = String(text || "").trim();
    if (!raw) return "";
    return raw
      .split(/\n\n+/)
      .map(function (p) {
        return '<p class="hdp-description">' + esc(p.replace(/\s+/g, " ").trim()) + "</p>";
      })
      .join("");
  }

  function isLowQualityHeadline(text) {
    var s = String(text || "").trim();
    if (!s) return true;
    if (/signals\s*:/i.test(s)) return true;
    if (/_[a-z0-9]+/i.test(s)) return true;
    if (/serving\s+.+\s+—\s+signals/i.test(s)) return true;
    if (s.length < 24) return true;
    return false;
  }

  function buildCommercialOverviewNarrative(hotel) {
    hotel = hotel || {};
    var name = displayValue(hotel.name, "This hotel");
    var brand = hotel.brand && hotel.brand !== "Unknown Brand" ? hotel.brand : null;
    var scaleRaw = hotel.chainScale || hotel.propertyType || null;
    var scale = scaleRaw ? String(scaleRaw).replace(/\s+Chain$/i, "") : null;
    var scaleLc = scale ? scale.toLowerCase() : null;
    var city = hotel.city && hotel.city.indexOf("Unknown") !== 0 ? hotel.city : null;
    var market =
      hotel.market && String(hotel.market).indexOf("Unknown") !== 0 ? hotel.market : null;
    var submarket = resolveEffectiveSubmarket(hotel);
    var roomsN = Number(hotel.rooms) > 0 ? Number(hotel.rooms) : null;
    var rooms = roomsN ? formatNumber(roomsN) : null;
    var meetingSq =
      hotel.totalMeetingSpace != null && Number(hotel.totalMeetingSpace) > 0
        ? Number(hotel.totalMeetingSpace)
        : null;
    var meetingRooms =
      hotel.meetingRooms ||
      (hotel.meetingSpaceProvenance && hotel.meetingSpaceProvenance.meetingRooms) ||
      null;
    var largest =
      (hotel.meetingSpaceProvenance && hotel.meetingSpaceProvenance.largestRoom) || null;
    var operator =
      hotel.managementCompany && hotel.managementCompany !== "Unknown"
        ? hotel.managementCompany
        : null;
    var opType =
      hotel.operationType && String(hotel.operationType).indexOf("Unknown") !== 0
        ? hotel.operationType
        : null;
    var locationType = hotel.locationType || null;
    var demand = [];
    var rawDemand = hotel.demandGenerators || hotel.nearbyDemand || [];
    if (Array.isArray(rawDemand)) {
      rawDemand.forEach(function (d) {
        var label = typeof d === "string" ? d : d && (d.name || d.label);
        if (label && String(label).trim()) demand.push(String(label).trim());
      });
    }
    if (hotel.primaryDemandDriver) demand.push(String(hotel.primaryDemandDriver).trim());
    resolveAmenitiesDisplay(hotel).forEach(function (a) {
      var lab = a && a.label ? String(a.label) : "";
      if (/NIH|Metro|Airport|Convention|University|Hospital/i.test(lab) && demand.indexOf(lab) < 0) {
        demand.push(lab);
      }
    });
    demand = demand.filter(Boolean).slice(0, 5);
    var amenLabels = resolveAmenitiesDisplay(hotel)
      .map(function (a) {
        return a && a.label;
      })
      .filter(Boolean)
      .slice(0, 6);
    var ownershipCtx = hotel.ownershipContext || null;
    var economicOwner =
      (ownershipCtx && (ownershipCtx.economicOwnerName || ownershipCtx.economic_owner)) ||
      hotel.economicOwner ||
      null;
    var legalOwner =
      (ownershipCtx && (ownershipCtx.legalOwnerName || ownershipCtx.legal_owner)) ||
      hotel.legalOwner ||
      null;
    var ownershipProposed =
      ownershipCtx &&
      /PROPOSED|STAGED/i.test(String(ownershipCtx.preview_state || ownershipCtx.confidence || ""));
    var ownershipOk =
      ownershipCtx &&
      (ownershipProposed ||
        ownershipCtx.verified === true ||
        ownershipCtx.confidence === "HIGH" ||
        ownershipCtx.confidence === "MEDIUM" ||
        ownershipCtx.preview_state === "LIVE");
    var yearBuilt = resolveYearBuiltDisplay(hotel);
    var yearReno = resolveLastRenovatedDisplay(hotel);
    var paras = [];

    var p1 = name + " is";
    if (scaleLc) p1 += (/^[aeiou]/i.test(scaleLc) ? " an " : " a ") + scaleLc;
    else p1 += " a";
    p1 += " lodging asset";
    if (brand) p1 += " affiliated with " + brand;
    if (city) {
      p1 += " in " + city;
      var geoTail = [hotel.state, hotel.country]
        .filter(function (p) {
          return p && String(p).indexOf("Unknown") !== 0;
        })
        .filter(function (p) {
          return String(p).toLowerCase() !== String(city).toLowerCase();
        })
        .join(", ");
      if (geoTail) p1 += ", " + geoTail;
    }
    p1 += ".";
    if (rooms || brand || scaleLc) {
      p1 +=
        " From an owner and asset-management perspective, the hotel is positioned as" +
        (scaleLc ? (/^[aeiou]/i.test(scaleLc) ? " an " : " a ") + scaleLc : " a") +
        " product" +
        (rooms ? " with a " + rooms + "-key inventory" : "") +
        (brand ? " under the " + brand + " flag" : "") +
        ", oriented to a mix of transient and group demand rather than a narrowly specialized format.";
    }
    paras.push(p1);

    var placeBits = [];
    if (submarket) placeBits.push("the " + submarket + " corridor");
    else if (market) placeBits.push("the " + market + " market");
    if (locationType) placeBits.push(String(locationType).toLowerCase() + " setting");
    if (placeBits.length || demand.length) {
      var p2 = placeBits.length
        ? "Commercially, the hotel anchors " + placeBits.join(" within a ") + "."
        : "Location context is still being assembled for this asset.";
      if (demand.length) {
        p2 +=
          " Documented demand environments that are relevant to commercial diligence include " +
          (demand.length === 1
            ? demand[0]
            : demand.slice(0, -1).join(", ") + ", and " + demand[demand.length - 1]) +
          ". These are contextual drivers, not proof of captured market share or realized occupancy.";
      }
      paras.push(p2);
    }

    var productParts = [];
    if (rooms) productParts.push(rooms + " guestrooms");
    if (meetingSq) {
      var m = formatNumber(meetingSq) + " sq ft of meeting and event space";
      if (meetingRooms) m += " across " + formatNumber(meetingRooms) + " published rooms";
      if (largest && largest.name && largest.sqFt) {
        m +=
          ", anchored by " +
          largest.name +
          " (" +
          formatNumber(largest.sqFt) +
          " sq ft)";
      }
      productParts.push(m);
    }
    if (amenLabels.length) {
      productParts.push(
        "guest-facing amenities such as " +
          (amenLabels.length === 1
            ? amenLabels[0]
            : amenLabels.slice(0, -1).join(", ") + ", and " + amenLabels[amenLabels.length - 1])
      );
    }
    if (productParts.length) {
      paras.push(
        "Published inventory includes " +
          (productParts.length === 1
            ? productParts[0]
            : productParts.length === 2
              ? productParts[0] + " and " + productParts[1]
              : productParts.slice(0, -1).join("; ") +
                "; and " +
                productParts[productParts.length - 1]) +
          ". Together these attributes support both individual transient stays and structured group / meeting business, subject to competitive set and sales execution that are outside this summary."
      );
    }

    var ownBits = [];
    if (operator) {
      ownBits.push(
        operator +
          " provides hotel management" +
          (opType ? " under a " + String(opType).toLowerCase() : "") +
          (brand ? ", with brand standards and distribution tied to " + brand : "")
      );
    }
    if (ownershipOk && (economicOwner || legalOwner)) {
      if (ownershipProposed) {
        if (legalOwner && economicOwner && legalOwner !== economicOwner) {
          ownBits.push(
            "staged ownership research proposes " +
              legalOwner +
              " as legal property owner and " +
              economicOwner +
              " as economic sponsor (not yet Airtable-persisted; ultimate beneficial owner unresolved)"
          );
        } else if (economicOwner) {
          ownBits.push(
            "staged ownership research proposes " +
              economicOwner +
              " as economic sponsor (not yet Airtable-persisted; ultimate beneficial owner unresolved)"
          );
        }
      } else if (legalOwner && economicOwner && legalOwner !== economicOwner) {
        ownBits.push(
          "verified ownership intelligence associates legal title with " +
            legalOwner +
            " and economic sponsorship with " +
            economicOwner
        );
      } else if (economicOwner) {
        ownBits.push(
          "verified ownership intelligence associates economic sponsorship with " + economicOwner
        );
      }
    }
    if (ownBits.length) {
      paras.push(
        "From an ownership and operating standpoint, " +
          ownBits.join(". ") +
          ". Proposed or medium-confidence relationships are shown as research posture, not as confirmed closing-table facts."
      );
    }

    var p5 = "";
    if (yearBuilt && yearReno) {
      p5 += "Originally developed in " + yearBuilt + " and last renovated in " + yearReno + ". ";
    } else if (yearBuilt) {
      p5 += "Originally developed in " + yearBuilt + ". ";
    } else if (yearReno) {
      p5 += "Last renovated in " + yearReno + ". ";
    } else {
      p5 +=
        "Year built and last renovation are not yet independently verified in Dealality for this asset; capital-plan timing should not be inferred from branding alone. ";
    }
    var attrs = [];
    if (scaleLc) attrs.push(scaleLc + " positioning");
    if (rooms) attrs.push(rooms + "-key scale");
    if (meetingSq) attrs.push("material meeting inventory");
    if (operator || brand) attrs.push("branded / managed distribution");
    p5 +=
      "Commercially, the verified product attributes — " +
      (attrs.length ? attrs.join(", ") : "available inventory and affiliation facts") +
      " — frame a conventional business mix of corporate transient, group meetings, and leisure where the destination supports it. This overview does not estimate occupancy, ADR, RevPAR, or demand capture; those require performance data and competitive-set analysis beyond the verified property profile.";
    paras.push(p5);

    return paras.join("\n\n");
  }

  var DESCRIPTION_UNAVAILABLE = "Property description is not yet available.";

  function resolveHeadlineText(hotel) {
    var raw = hotel && hotel.hotelHeadline;
    if (!raw || isLowQualityHeadline(raw)) return "";
    return String(raw).trim();
  }

  function resolveDescriptionText(hotel) {
    var fromAirtable = hotel && (hotel.hotelDescription || hotel.description);
    if (fromAirtable && String(fromAirtable).trim() && !isLowQualityGeneratedDescription(fromAirtable, hotel)) {
      return String(fromAirtable).trim();
    }
    var positioning =
      hotel &&
      hotel.overviewEnrichment &&
      hotel.overviewEnrichment.positioning &&
      hotel.overviewEnrichment.positioning.text;
    if (
      positioning &&
      String(positioning).trim() &&
      !isLowQualityGeneratedDescription(positioning, hotel)
    ) {
      return String(positioning).trim();
    }
    var commercial = buildCommercialOverviewNarrative(hotel);
    if (commercial && String(commercial).trim().length >= 420) {
      return commercial;
    }
    if (commercial && !isLowQualityGeneratedDescription(commercial, hotel)) {
      return commercial;
    }
    var synthesized = buildDescription(hotel);
    if (isLowQualityGeneratedDescription(synthesized, hotel)) {
      return DESCRIPTION_UNAVAILABLE;
    }
    return synthesized;
  }

  function resolveYearBuiltDisplay(hotel) {
    var v =
      hotel.yearBuilt ||
      hotel.year_built ||
      hotel.YearBuilt ||
      (hotel.commercialProfile && hotel.commercialProfile.yearBuilt) ||
      hotel.openingDate ||
      hotel.openDate ||
      hotel.opening_date ||
      null;
    if (v == null || v === "" || v === "—") return null;
    var n = Number(v);
    if (Number.isFinite(n) && n >= 1800 && n <= 2100) return String(Math.round(n));
    var s = String(v).trim();
    var yearMatch = s.match(/(18|19|20)\d{2}/);
    if (yearMatch) return yearMatch[0];
    return s || null;
  }

  function resolveLastRenovatedDisplay(hotel) {
    var v =
      hotel.yearRenovated ||
      hotel.lastRenovated ||
      hotel.last_renovated ||
      hotel.YearRenovated ||
      (hotel.commercialProfile && hotel.commercialProfile.yearRenovated) ||
      null;
    if (v == null || v === "" || v === "—") {
      var events = hotel.renovationEvents || (hotel.fieldProvenance && hotel.fieldProvenance.lastRenovated && hotel.fieldProvenance.lastRenovated.events) || null;
      if (Array.isArray(events) && events.length) {
        var primary = events.slice().sort(function (a, b) {
          return (a.displayPriority || 99) - (b.displayPriority || 99);
        })[0];
        if (primary && primary.year) {
          var secondary = events.filter(function (e) {
            return e && e.year && e.year !== primary.year;
          });
          if (secondary.length) {
            return String(primary.year) + " (major); " + secondary.map(function (e) {
              return String(e.year);
            }).join(", ") + " milestone";
          }
          return String(primary.year);
        }
      }
      return null;
    }
    var n = Number(v);
    if (Number.isFinite(n) && n >= 1800 && n <= 2100) {
      var ev = hotel.renovationEvents || null;
      if (Array.isArray(ev) && ev.length > 1) {
        var others = ev.filter(function (e) {
          return e && e.year && Number(e.year) !== Math.round(n);
        });
        if (others.length) {
          return String(Math.round(n)) + " (major); " + others.map(function (e) {
            return String(e.year);
          }).join(", ") + " milestone";
        }
      }
      return String(Math.round(n));
    }
    var s = String(v).trim();
    return s || null;
  }

  function buildOverviewRows(hotel) {
    var meetingValue = "—";
    if (hotel.meetingSpaceDisplay) {
      meetingValue = String(hotel.meetingSpaceDisplay);
    } else if (hotel.totalMeetingSpace != null && Number(hotel.totalMeetingSpace) > 0) {
      meetingValue = formatNumber(hotel.totalMeetingSpace) + " sq ft";
    }

    var parentSemantic =
      hotel.fieldProvenance &&
      hotel.fieldProvenance.parentCompany &&
      hotel.fieldProvenance.parentCompany.semantic;
    var parentLabel =
      parentSemantic === "brand_parent_company"
        ? "Parent company (brand)"
        : "Parent company";
    var yearBuilt = resolveYearBuiltDisplay(hotel);
    var lastRenovated = resolveLastRenovatedDisplay(hotel);
    var rows = [
      { label: "Brand", value: displayValue(hotel.brand) },
      { label: parentLabel, value: formatParentCompanyDisplay(hotel.parentCompany, hotel) },
      { label: "Chain scale", value: displayValue(hotel.chainScale || hotel.propertyType) },
      { label: "Property type", value: displayValue(hotel.censusPropertyType) },
      { label: "Rooms", value: hotel.rooms ? formatNumber(hotel.rooms) : "—" },
      { label: "Year built", value: yearBuilt || "Not yet verified" },
      { label: "Last renovated", value: lastRenovated || "Not yet verified" },
      { label: "Meeting space", value: meetingValue },
      { label: "Operation type", value: displayValue(hotel.operationType) },
      { label: "Management company", value: displayValue(hotel.managementCompany) },
      { label: "Market", value: displayValue(hotel.market) },
      { label: "Submarket", value: displayValue(resolveEffectiveSubmarket(hotel)) },
      { label: "Location type", value: displayValue(hotel.locationType) },
      { label: "Project phase", value: displayValue(hotel.projectPhase) },
      { label: "Projected open", value: formatDate(hotel.projectedOpenDate) },
      { label: "Star rating", value: hotel.starRating != null ? String(hotel.starRating) + " / 5" : "—" },
      { label: "Data confidence", value: displayValue(hotel.dataConfidence) }
    ];

    return rows.filter(function (row) {
      if (row.label === "Year built" || row.label === "Last renovated") return true;
      return row.value && row.value !== "—" && row.value !== "Unknown" && row.value !== "Unknown Brand"
        && row.value !== "Unknown City" && row.value !== "Unknown Country" && row.value !== "Unknown Region";
    });
  }

  function buildAddressLine(hotel) {
    var parts = [hotel.address1, hotel.address2, hotel.city, hotel.state, hotel.postalCode, hotel.country]
      .filter(function (part) { return part && String(part).trim(); });
    return parts.join(", ");
  }

  function buildDirectionsUrl(hotel) {
    if (!Number.isFinite(Number(hotel.lat)) || !Number.isFinite(Number(hotel.lng))) return null;
    if (hotel.lat === 0 && hotel.lng === 0) return null;
    return "https://www.google.com/maps?q=" + encodeURIComponent(hotel.lat + "," + hotel.lng);
  }

  function normalizeWebsiteUrl(url) {
    if (!url) return null;
    var trimmed = String(url).trim();
    if (!trimmed) return null;
    if (/^https?:\/\//i.test(trimmed)) return trimmed;
    return "https://" + trimmed;
  }

  function renderActionLink(label, href, iconKey) {
    var icon = actionIconHtml(iconKey);
    if (!href) {
      return (
        '<span class="hdp-action is-disabled">' +
          '<span class="hdp-action__icon">' + icon + "</span>" +
          esc(label) +
        "</span>"
      );
    }
    var external = href.indexOf("http") === 0 ? ' target="_blank" rel="noopener noreferrer"' : "";
    return (
      '<a class="hdp-action" href="' + esc(href) + '"' + external + ">" +
        '<span class="hdp-action__icon">' + icon + "</span>" +
        esc(label) +
      "</a>"
    );
  }

  function toAmenityProperCase(label) {
    var smallWords = { a: 1, an: 1, and: 1, at: 1, by: 1, for: 1, in: 1, of: 1, on: 1, or: 1, the: 1, to: 1, not: 1 };
    var preserved = {
      wifi: "WiFi",
      "wi-fi": "Wi-Fi",
      ev: "EV",
      nih: "NIH",
      dc: "DC",
      "24hr": "24-Hour",
      "24-hr": "24-Hour",
    };
    var normalized = String(label || "")
      .trim()
      .replace(/_/g, " ")
      .replace(/\s+/g, " ");
    return normalized
      .split(/\s+/)
      .map(function (word, index) {
        if (!word) return word;
        return word.replace(/^[A-Za-z0-9]+(?:'[A-Za-z0-9]+)*/g, function (token) {
          var apostrophe = token.indexOf("'");
          var base = apostrophe >= 0 ? token.slice(0, apostrophe) : token;
          var suffix = apostrophe >= 0 ? token.slice(apostrophe) : "";
          var lower = base.toLowerCase();
          if (preserved[lower]) return preserved[lower] + suffix.toLowerCase();
          if (index > 0 && smallWords[lower]) return lower + suffix.toLowerCase();
          return base.charAt(0).toUpperCase() + base.slice(1).toLowerCase() + suffix.toLowerCase();
        });
      })
      .join(" ");
  }

  function amenityLabelKey(label) {
    return String(label || "")
      .toLowerCase()
      .replace(/_/g, " ")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  /** Collapse near-duplicate HPC vs ADP amenity labels for strip dedupe. */
  function amenityDedupeKey(label) {
    var s = String(label || "")
      .toLowerCase()
      .replace(/['’]/g, "")
      .replace(/&/g, " and ")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    s = s
      .replace(/\bwashington dc\b/g, "dc")
      .replace(/\bcomplimentary\b/g, "")
      .replace(/\bon[\s-]?site\b/g, "onsite")
      .replace(/\b24[\s-]?hour\b/g, "24hr");
    var tokens = s.split(/\s+/).filter(function (t) {
      return t && !/^(and|the|a|an|level|service|rooms)$/.test(t);
    });
    if (
      tokens.indexOf("coopers") >= 0 ||
      (tokens.indexOf("cooper") >= 0 && tokens.indexOf("mill") >= 0)
    ) {
      return "coopers_mill";
    }
    if (tokens.indexOf("peloton") >= 0) return "peloton";
    if (tokens.indexOf("fitness") >= 0 && tokens.indexOf("24hr") >= 0) return "fitness_24hr";
    if (tokens.indexOf("fitness") >= 0) return "fitness_center";
    if (tokens.indexOf("metro") >= 0 && tokens.indexOf("shuttle") >= 0) return "metro_shuttle";
    if (tokens.indexOf("metro") >= 0 && tokens.indexOf("access") >= 0) return "metro_access";
    if (tokens.indexOf("m") >= 0 && tokens.indexOf("club") >= 0) return "m_club";
    if (tokens.indexOf("ballroom") >= 0) return "ballroom";
    if (
      tokens.indexOf("meeting") >= 0 &&
      (tokens.indexOf("event") >= 0 ||
        tokens.indexOf("space") >= 0 ||
        tokens.indexOf("room") >= 0)
    ) {
      return "meeting_space";
    }
    if (tokens.indexOf("wifi") >= 0 || (tokens.indexOf("wi") >= 0 && tokens.indexOf("fi") >= 0)) {
      return "wifi";
    }
    if (tokens.indexOf("pet") >= 0 || tokens.indexOf("pets") >= 0) return "pets";
    if (tokens.indexOf("parking") >= 0) return "parking";
    if (tokens.indexOf("business") >= 0 && tokens.indexOf("center") >= 0) return "business_center";
    if (tokens.indexOf("room") >= 0 && tokens.indexOf("service") >= 0) return "room_service";
    if (
      tokens.indexOf("accessible") >= 0 ||
      tokens.indexOf("accessibility") >= 0 ||
      tokens.indexOf("ada") >= 0
    ) {
      return "accessibility";
    }
    if (tokens.indexOf("concierge") >= 0 && tokens.indexOf("club") < 0) return "concierge";
    return tokens.slice().sort().join("_");
  }

  /** Location / brand identity tokens that are not guest-facing amenities. */
  function isNonAmenityPresentationLabel(label) {
    var key = amenityLabelKey(label);
    if (!key) return true;
    if (
      /^(brand|loyalty|location|market|submarket|city|state|country|full service|full service hotel)$/.test(
        key
      )
    ) {
      return true;
    }
    if (/\b(location|brand|loyalty)\b$/.test(key)) return true;
    if (/^(bethesda md|pooks hill|montgomery county|marriott hotels|marriott bonvoy)\b/.test(key)) {
      return true;
    }
    if (/^(nih access|washington dc metro access|dc metro access)$/.test(key)) {
      // Keep access attributes — they are commercially useful amenities/access facts.
      return false;
    }
    return false;
  }

  function looksLikeMachineAmenityToken(label) {
    var s = String(label || "").trim();
    if (!s) return true;
    if (/_/.test(s)) return true;
    if (/signals\s*:/i.test(s)) return true;
    return false;
  }

  function resolveAmenityIdForLabel(label, idByLabelKey) {
    var key = amenityLabelKey(label);
    if (idByLabelKey && idByLabelKey[key]) return idByLabelKey[key];
    var icons = window.HiltonAmenityIcons;
    if (icons && typeof icons.amenityIdFromLabel === "function") {
      return icons.amenityIdFromLabel(label) || null;
    }
    return null;
  }

  var HE_AMENITIES_DISPLAY_MAX = 20;
  var HE_AMENITIES_DISPLAY_TARGET_MIN = 12;

  function amenityDisplayPriority(item) {
    var id = String((item && (item.id || item.attributeKey)) || "").trim();
    var priorityById = {
      meetingEventSpace: 100,
      meetingRooms: 98,
      ballroom: 96,
      dining: 90,
      spa: 88,
      outdoorPool: 86,
      indoorPool: 85,
      pool: 84,
      fitnessCenter: 82,
      peloton: 80,
      m_club: 78,
      executiveLounge: 78,
      concierge: 76,
      businessCenter: 74,
      freeParking: 72,
      valetParking: 71,
      airportShuttle: 70,
      dc_metro_access: 68,
      nih_access: 67,
      roomAccessibility: 66,
      beach: 64,
      roomService: 55,
      petsAllowed: 50,
      evCharging: 48,
    };
    if (id && priorityById[id] != null) return priorityById[id];
    var label = String((item && item.label) || "").toLowerCase();
    if (/meeting|event|ballroom/.test(label)) return 95;
    if (/restaurant|dining|bar|lounge|cooper/.test(label)) return 88;
    if (/spa/.test(label)) return 86;
    if (/pool/.test(label)) return 84;
    if (/fitness|peloton|gym/.test(label)) return 82;
    if (/club|concierge/.test(label)) return 76;
    if (/business/.test(label)) return 74;
    if (/park/.test(label)) return 70;
    if (/access|shuttle|metro|airport|nih/.test(label)) return 66;
    if (/accessib/.test(label)) return 64;
    return 30;
  }

  /**
   * Prefer verified / human-readable amenity labels (ADP / HPC).
   * Cap at 20 prioritized icons; never invent amenities to hit the 12 target.
   */
  function resolveAmenitiesDisplay(hotel) {
    function normalizeItem(item) {
      if (!item) return null;
      var label = toAmenityProperCase(item.label || item);
      if (!label || isNonAmenityPresentationLabel(label)) return null;
      var id = item.id || resolveAmenityIdForLabel(label, null) || null;
      return {
        id: id,
        label: label,
        verificationStatus: item.verificationStatus || item.factKind || null,
        sourceUrl: item.sourceUrl || null,
        lastVerified: item.lastVerified || item.lastVerifiedAt || null,
        attributeKey: item.attributeKey || id || null,
      };
    }

    var fromVerified = [];
    if (hotel && Array.isArray(hotel.verifiedAmenitiesDisplay)) {
      fromVerified = hotel.verifiedAmenitiesDisplay
        .filter(function (item) {
          return (
            item &&
            item.label &&
            item.factKind !== "UNSUPPORTED_CLAIM" &&
            item.factKind !== "INFERRED_SUITABILITY" &&
            item.factKind !== "REALITY_GAP_RECOGNITION" &&
            item.factKind !== "POSITIONING"
          );
        })
        .map(normalizeItem)
        .filter(Boolean);
    }

    var fromDisplay = [];
    if (hotel && Array.isArray(hotel.amenitiesDisplay)) {
      fromDisplay = hotel.amenitiesDisplay
        .filter(function (item) {
          return item && (item.id || item.label) && !looksLikeMachineAmenityToken(item.label);
        })
        .map(normalizeItem)
        .filter(Boolean);
    }

    var fromField = parseAmenitiesField(hotel && hotel.amenities)
      .map(function (label) {
        return normalizeItem({ label: label });
      })
      .filter(Boolean);

    // Union verified + HPC/display/text (verified first). Collapse near-duplicates.
    var merged = fromVerified.concat(fromDisplay).concat(fromField);

    var seen = {};
    var deduped = [];
    merged.forEach(function (item) {
      // Prefer canonicalized dedupe so "Meeting Space" does not duplicate
      // "Meeting & Event Space", while Peloton vs 24-Hour Fitness stay distinct.
      var key = amenityDedupeKey(item.label) || amenityLabelKey(item.label);
      if (!key || seen[key]) return;
      seen[key] = true;
      deduped.push(item);
    });
    deduped.sort(function (a, b) {
      return amenityDisplayPriority(b) - amenityDisplayPriority(a) || a.label.localeCompare(b.label);
    });
    var selected = deduped.slice(0, HE_AMENITIES_DISPLAY_MAX);
    var belowTarget = deduped.length < HE_AMENITIES_DISPLAY_TARGET_MIN;
    try {
      window.HOTEL_EXPLORER_AMENITIES_COVERAGE = {
        verifiedCount: deduped.length,
        displayedCount: selected.length,
        targetMin: HE_AMENITIES_DISPLAY_TARGET_MIN,
        max: HE_AMENITIES_DISPLAY_MAX,
        belowTargetMin: belowTarget,
        shortfall: Math.max(0, HE_AMENITIES_DISPLAY_TARGET_MIN - deduped.length),
        overflowCount: Math.max(0, deduped.length - selected.length),
        researchTrigger: belowTarget,
        researchStatus: belowTarget ? "RESEARCH_GAP" : "COVERAGE_OK",
        customerFacingCompletenessIndicator: false,
      };
    } catch (_err) {
      /* ignore */
    }
    return selected;
  }

  function amenityIconSvg(item) {
    var icons = window.HiltonAmenityIcons;
    if (icons) {
      if (item && item.label && typeof icons.iconSvgForAmenityLabel === "function") {
        var byLabel = icons.iconSvgForAmenityLabel(item.label);
        if (byLabel) return byLabel;
      }
      if (item && item.id && typeof icons.iconSvgForAmenityId === "function") {
        return icons.iconSvgForAmenityId(item.id);
      }
    }
    return ICONS[iconForAmenity(item && item.label)] || ICONS.default;
  }

  function formatAmenityLabelHtml(label) {
    var text = String(label || "").trim();
    if (!text) return "";
    var words = text.split(/\s+/);
    if (words.length <= 1) return esc(words[0]);
    var mid = Math.ceil(words.length / 2);
    return (
      esc(words.slice(0, mid).join(" ")) +
      "<br>" +
      esc(words.slice(mid).join(" "))
    );
  }

  function renderAmenityCard(item) {
    var label = item && item.label ? item.label : String(item || "");
    var icon = amenityIconSvg(item);
    return (
      '<div class="hdp-amenity-card" title="' + esc(label) + '">' +
        '<div class="hdp-amenity-card__icon">' + icon + "</div>" +
        '<div class="hdp-amenity-card__label">' + formatAmenityLabelHtml(label) + "</div>" +
      "</div>"
    );
  }

  function renderAmenitiesSection(amenities) {
    if (!amenities.length) {
      return '<p class="hdp-empty">No verified amenities are available for this property yet.</p>';
    }
    var cards = amenities.map(renderAmenityCard).join("");
    return (
      '<div class="hdp-amenities-wrap">' +
      '<button type="button" class="hdp-amenities-nav hdp-amenities-nav--prev" aria-label="Previous amenities" hidden>&lsaquo;</button>' +
      '<div class="hdp-amenities-carousel" role="list" aria-label="Hotel amenities" tabindex="0">' +
      cards +
      "</div>" +
      '<button type="button" class="hdp-amenities-nav hdp-amenities-nav--next" aria-label="Next amenities" hidden>&rsaquo;</button>' +
      "</div>"
    );
  }

  function hotelRecordId(hotel) {
    return hotel && (hotel.id || hotel.recordId) || null;
  }

  function isSameHotel(a, b) {
    var idA = hotelRecordId(a);
    var idB = hotelRecordId(b);
    if (idA && idB) return idA === idB;
    return a === b;
  }

  function haversineKm(lat1, lng1, lat2, lng2) {
    var toRad = Math.PI / 180;
    var dLat = (lat2 - lat1) * toRad;
    var dLng = (lng2 - lng1) * toRad;
    var a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2);
    return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  function itemCoords(item) {
    if (!item) return null;
    var lat = Number(item.lat != null ? item.lat : item.latitude);
    var lng = Number(item.lng != null ? item.lng : item.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) return null;
    return { lat: lat, lng: lng };
  }

  function hotelCoords(hotel) {
    if (!hotel) return null;
    var lat = Number(hotel.lat != null ? hotel.lat : hotel.latitude);
    var lng = Number(hotel.lng != null ? hotel.lng : hotel.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) return null;
    return { lat: lat, lng: lng };
  }

  function formatDistanceKm(km) {
    if (!Number.isFinite(km)) return "";
    if (window.HeMeasurementPrefs && typeof window.HeMeasurementPrefs.formatDistanceFromKm === "function") {
      return window.HeMeasurementPrefs.formatDistanceFromKm(km);
    }
    if (km < 1) return Math.round(km * 1000) + " m";
    if (km < 10) return km.toFixed(1) + " km";
    return Math.round(km) + " km";
  }

  function formatRadiusLabel(km) {
    if (!Number.isFinite(km)) return "";
    return formatDistanceKm(km);
  }

  function filterAndSortByDistance(items, hotel, options) {
    options = options || {};
    var origin = hotelCoords(hotel);
    if (!origin) return [];

    var radiusKm = options.radiusKm || 50;
    var limit = options.limit || 12;
    var typePriority = options.typePriority || null;

    var enriched = (items || []).map(function (item) {
      var coords = itemCoords(item);
      if (!coords) return null;
      var distanceKm = haversineKm(origin.lat, origin.lng, coords.lat, coords.lng);
      if (distanceKm > radiusKm) return null;
      return Object.assign({}, item, { distanceKm: distanceKm });
    }).filter(Boolean);

    enriched.sort(function (a, b) {
      if (typePriority) {
        var aPri = typePriority(a);
        var bPri = typePriority(b);
        if (aPri !== bPri) return aPri - bPri;
      }
      return a.distanceKm - b.distanceKm;
    });

    return enriched.slice(0, limit);
  }

  function enrichItemsWithDistance(items, hotel) {
    var origin = hotelCoords(hotel);
    if (!origin) return [];

    return (items || [])
      .map(function (item) {
        var coords = itemCoords(item);
        if (!coords) return null;
        var distanceKm = haversineKm(origin.lat, origin.lng, coords.lat, coords.lng);
        return Object.assign({}, item, { distanceKm: distanceKm });
      })
      .filter(Boolean);
  }

  function isAirportInfraItem(item) {
    var type = String(
      (item && (item.pointType || item.type || item.mapIconType)) || ""
    )
      .trim()
      .toLowerCase();
    return type.indexOf("airport") >= 0;
  }

  function infraItemKey(item) {
    var id = item && (item.id || item.recordId);
    if (id) return String(id);
    return [item && item.name, item && item.lat, item && item.lng].join("|");
  }

  function accessInfraTypePriority(item) {
    if (isAirportInfraItem(item)) return 0;
    var type = String((item && (item.pointType || item.type)) || "").toLowerCase();
    if (type.indexOf("cruise") >= 0) return 1;
    if (type.indexOf("highway") >= 0) return 2;
    return 3;
  }

  function sortAccessInfraItems(items) {
    return (items || []).slice().sort(function (a, b) {
      var aPri = accessInfraTypePriority(a);
      var bPri = accessInfraTypePriority(b);
      if (aPri !== bPri) return aPri - bPri;
      return (a.distanceKm || 0) - (b.distanceKm || 0);
    });
  }

  /** Always include the closest N airports, even beyond the tab search radius. */
  function mergeClosestAirportsIntoAccessItems(items, allItems, radiusKm, minAirports) {
    var targetCount = Math.max(1, Number(minAirports) || CONTEXT_CONFIG.minAirports || 2);
    var merged = (items || []).slice();
    var seen = {};

    merged.forEach(function (item) {
      seen[infraItemKey(item)] = true;
    });

    var airportsInList = merged.filter(isAirportInfraItem).length;
    if (airportsInList >= targetCount) {
      return sortAccessInfraItems(merged);
    }

    var airports = (allItems || [])
      .filter(isAirportInfraItem)
      .sort(function (a, b) {
        return (a.distanceKm || 0) - (b.distanceKm || 0);
      });

    for (var i = 0; i < airports.length && airportsInList < targetCount; i += 1) {
      var airport = airports[i];
      var key = infraItemKey(airport);
      if (seen[key]) continue;
      merged.push(
        Object.assign({}, airport, {
          outsideSearchRadius:
            radiusKm != null && Number.isFinite(airport.distanceKm) && airport.distanceKm > radiusKm,
        })
      );
      seen[key] = true;
      airportsInList += 1;
    }

    return sortAccessInfraItems(merged);
  }

  function filterHotelsByDistance(hotel, hotels, radiusKm) {
    var origin = hotelCoords(hotel);
    if (!origin) return [];

    return (hotels || [])
      .map(function (item) {
        var coords = itemCoords(item);
        if (!coords) return null;
        var distanceKm = haversineKm(origin.lat, origin.lng, coords.lat, coords.lng);
        if (distanceKm > radiusKm) return null;
        return Object.assign({}, item, { distanceKm: distanceKm });
      })
      .filter(Boolean)
      .sort(function (a, b) {
        return a.distanceKm - b.distanceKm;
      });
  }

  function normalizeAreaHotelsLocationType(hotel) {
    var raw = String((hotel && hotel.locationType) || "").trim();
    if (!raw) return "Unknown";
    var lower = raw.toLowerCase().replace(/\s+/g, " ");
    if (lower.indexOf("small metro") >= 0 || lower.indexOf("small town") >= 0) {
      return "Small Metro/Town";
    }
    if (lower.indexOf("interstate") >= 0 || lower.indexOf("highway") >= 0) {
      return "Interstate";
    }
    if (lower.indexOf("airport") >= 0) return "Airport";
    if (lower.indexOf("resort") >= 0) return "Resort";
    if (lower.indexOf("suburban") >= 0) return "Suburban";
    if (lower.indexOf("urban") >= 0) return "Urban";
    return raw;
  }

  function isIslandAreaHotelsCountry(country) {
    var key = String(country || "")
      .trim()
      .toLowerCase()
      .replace(/&/g, "and");
    return Boolean(AREA_HOTELS_SCOPE.islandCountries[key]);
  }

  function isDenseUrbanAreaHotel(hotel) {
    var loc = normalizeAreaHotelsLocationType(hotel);
    if (loc !== "Urban") return false;
    var city = String(hotel.city || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");
    return Boolean(AREA_HOTELS_SCOPE.denseUrbanCities[city]);
  }

  function isSparseAreaHotelsCorridor(hotel) {
    var corridor = resolveEffectiveSubmarket(hotel);
    if (!corridor) return false;
    var key = corridor.toLowerCase().replace(/\s+/g, " ");
    if (AREA_HOTELS_SCOPE.sparseCorridorKeys[key]) return true;
    var submarketHotels = filterHotelsBySubmarket(hotel, window.allHotels || []);
    return submarketHotels.length > 0 && submarketHotels.length < 12;
  }

  function getAreaHotelsBaseRadiusKm(hotel) {
    var loc = normalizeAreaHotelsLocationType(hotel);
    var base =
      AREA_HOTELS_SCOPE.byLocationType[loc] || AREA_HOTELS_SCOPE.byLocationType.default;

    if (isIslandAreaHotelsCountry(hotel.country)) {
      base = Math.min(base, AREA_HOTELS_SCOPE.islandKm);
    }
    if (isDenseUrbanAreaHotel(hotel)) {
      base = AREA_HOTELS_SCOPE.denseUrbanKm;
    }
    if (loc === "Resort" && isSparseAreaHotelsCorridor(hotel)) {
      base = Math.max(base, AREA_HOTELS_SCOPE.sparseCorridorKm);
    }

    return Math.min(base, AREA_HOTELS_SCOPE.maxRadiusKm);
  }

  function buildAreaHotelsRadiusLadder(baseKm) {
    var maxKm = AREA_HOTELS_SCOPE.maxRadiusKm;
    var candidates = [
      baseKm,
      Math.round(baseKm * 1.5),
      Math.round(baseKm * 2),
      Math.round(baseKm * 2.5),
      30,
      40,
      50,
      60,
      maxKm,
    ];
    var ladder = [];
    candidates.forEach(function (km) {
      km = Math.min(Math.max(km, baseKm), maxKm);
      if (!ladder.length || km > ladder[ladder.length - 1]) {
        ladder.push(km);
      }
    });
    return ladder;
  }

  function splitAreaHotelCandidates(hotel, candidates) {
    var existing = [];
    var pipeline = [];
    (candidates || []).forEach(function (h) {
      if (isSameHotel(h, hotel)) return;
      if (String(h.status || "").toLowerCase() === "pipeline") pipeline.push(h);
      else existing.push(h);
    });
    return { existing: existing, pipeline: pipeline };
  }

  function resolveAreaHotelsScope(hotel, allHotels) {
    var baseRadiusKm = getAreaHotelsBaseRadiusKm(hotel);
    var locationType = normalizeAreaHotelsLocationType(hotel);
    var corridor = resolveEffectiveSubmarket(hotel);
    var pool = (allHotels || []).filter(function (h) {
      return !isSameHotel(h, hotel);
    });
    var useSubmarket =
      Boolean(corridor) &&
      AREA_HOTELS_SCOPE.submarketScopeTypes[locationType] === true;

    if (useSubmarket) {
      var submarketPool = filterHotelsBySubmarket(hotel, pool).map(function (h) {
        var withDistance = filterHotelsByDistance(hotel, [h], AREA_HOTELS_SCOPE.maxRadiusKm)[0];
        return withDistance || null;
      }).filter(Boolean);
      var submarketSplit = splitAreaHotelCandidates(hotel, submarketPool);
      if (submarketSplit.existing.length >= AREA_HOTELS_SCOPE.minCompHotels) {
        var maxDist = submarketSplit.existing.reduce(function (max, h) {
          return Math.max(max, h.distanceKm || 0);
        }, 0);
        return {
          mode: "submarket",
          radiusKm: Math.min(Math.ceil(maxDist) || baseRadiusKm, AREA_HOTELS_SCOPE.maxRadiusKm),
          baseRadiusKm: baseRadiusKm,
          expanded: false,
          locationType: locationType,
          corridor: corridor,
          note: "",
          existing: submarketSplit.existing.slice(0, AREA_HOTELS_SCOPE.listLimit),
          pipeline: submarketSplit.pipeline.slice(0, AREA_HOTELS_SCOPE.listLimit),
        };
      }
    }

    // Empty census pool: keep the location-type default radius (do not expand to max).
    if (!pool.length) {
      return {
        mode: "radius",
        radiusKm: baseRadiusKm,
        baseRadiusKm: baseRadiusKm,
        expanded: false,
        locationType: locationType,
        corridor: corridor,
        note: "Nearby HPC hotel pool unavailable in this session — search radius kept at the location-type default. Market totals are not fabricated.",
        existing: [],
        pipeline: [],
      };
    }

    var ladder = buildAreaHotelsRadiusLadder(baseRadiusKm);
    var chosenRadius = ladder[ladder.length - 1];
    var chosenExisting = [];
    var chosenPipeline = [];
    var expanded = false;

    for (var i = 0; i < ladder.length; i += 1) {
      var radiusKm = ladder[i];
      var inRadius = filterHotelsByDistance(hotel, pool, radiusKm);
      var split = splitAreaHotelCandidates(hotel, inRadius);
      chosenRadius = radiusKm;
      chosenExisting = split.existing;
      chosenPipeline = split.pipeline;
      if (split.existing.length >= AREA_HOTELS_SCOPE.minCompHotels) {
        expanded = radiusKm > baseRadiusKm;
        break;
      }
      if (i === ladder.length - 1) {
        expanded = radiusKm > baseRadiusKm && chosenExisting.length > 0;
        if (!chosenExisting.length && !chosenPipeline.length) {
          chosenRadius = baseRadiusKm;
          expanded = false;
        }
      }
    }

    var note = "";
    if (expanded) {
      var atBase = filterHotelsByDistance(hotel, pool, baseRadiusKm);
      var atBaseCount = splitAreaHotelCandidates(hotel, atBase).existing.length;
      note =
        "Expanded to " +
        formatRadiusLabel(chosenRadius) +
        " — only " +
        atBaseCount +
        " open hotels within " +
        formatRadiusLabel(baseRadiusKm) +
        ".";
    }

    return {
      mode: "radius",
      radiusKm: chosenRadius,
      baseRadiusKm: baseRadiusKm,
      expanded: expanded,
      locationType: locationType,
      corridor: corridor,
      note: note,
      existing: chosenExisting.slice(0, AREA_HOTELS_SCOPE.listLimit),
      pipeline: chosenPipeline.slice(0, AREA_HOTELS_SCOPE.listLimit),
    };
  }

  function getPropertyContextScope(hotel) {
    var allHotels = window.allHotels || [];
    var locationType = normalizeAreaHotelsLocationType(hotel);
    var corridor = resolveEffectiveSubmarket(hotel);
    var baseRadiusKm = getAreaHotelsBaseRadiusKm(hotel);
    if (!hotelCoords(hotel)) {
      return {
        mode: "radius",
        radiusKm: baseRadiusKm,
        baseRadiusKm: baseRadiusKm,
        expanded: false,
        locationType: locationType,
        corridor: corridor,
        note: "",
      };
    }
    return resolveAreaHotelsScope(hotel, allHotels);
  }

  function formatContextScopeLabel(scope) {
    scope = scope || {};
    var parts = [];
    if (scope.locationType && scope.locationType !== "Unknown") {
      parts.push(scope.locationType);
    }
    if (scope.mode === "submarket" && scope.corridor) {
      parts.push("Submarket: " + scope.corridor);
    } else if (scope.radiusKm != null) {
      parts.push("Within " + formatRadiusLabel(scope.radiusKm));
    }
    return parts.join(" · ");
  }

  function renderContextScopeLine(scope, extraSuffix) {
    scope = scope || {};
    var label = formatContextScopeLabel(scope);
    if (extraSuffix) {
      label = label ? label + " · " + extraSuffix : extraSuffix;
    }
    var html =
      '<p class="hdp-snapshot-scope hdp-context-scope-line">' + esc(label) + "</p>";
    if (scope.note) {
      html +=
        '<p class="hdp-snapshot-meta hdp-area-hotels-scope-note">' + esc(scope.note) + "</p>";
    }
    return html;
  }

  function renderAreaHotelsScopeLine(scope) {
    return renderContextScopeLine(scope);
  }

  function getContextCategoryKey(item) {
    return String(item.pointType || item.mapIconType || item.type || "Other").trim();
  }

  function getInfraCategoryKey(item) {
    return String(item.pointType || item.type || "Other").trim();
  }

  function buildContextTypeCounts(items, categoryFn) {
    var counts = {};
    (items || []).forEach(function (item) {
      var key = categoryFn(item);
      counts[key] = (counts[key] || 0) + 1;
    });
    return counts;
  }

  function sortInfraCategoryKeys(types, counts) {
    return types.sort(function (a, b) {
      var aIdx = INFRA_TYPE_ORDER.indexOf(a);
      var bIdx = INFRA_TYPE_ORDER.indexOf(b);
      if (aIdx === -1 && bIdx === -1) return counts[b] - counts[a] || a.localeCompare(b);
      if (aIdx === -1) return 1;
      if (bIdx === -1) return -1;
      return aIdx - bIdx;
    });
  }

  function renderContextListItem(item, options) {
    options = options || {};
    var color = typeof options.colorFn === "function" ? options.colorFn(item) : "#6c72ff";
    var typeLabel = item.pointSubtype || item.pointType || item.mapIconType || item.type || "Point";
    var locationBits = [item.city, item.country].filter(Boolean).join(", ");
    var meta = [];
    if (item.distanceKm != null) meta.push(formatDistanceKm(item.distanceKm));
    if (locationBits) meta.push(locationBits);
    var filterAttrs = "";
    if (options.filterList) {
      var category = options.categoryKey || getContextCategoryKey(item);
      filterAttrs =
        ' class="hdp-context-list__item hdp-filter-list__item" data-filter-list-item data-filter-category="' +
        esc(category) +
        '"';
    } else {
      filterAttrs = ' class="hdp-context-list__item"';
    }

    return (
      "<li" + filterAttrs + ">" +
        '<span class="hdp-context-list__dot" style="background:' + esc(color) + '"></span>' +
        '<div class="hdp-context-list__body">' +
          '<div class="hdp-context-list__name">' + esc(item.name || "Unknown") + "</div>" +
          '<div class="hdp-context-list__type">' + esc(typeLabel) + "</div>" +
          (meta.length
            ? '<div class="hdp-context-list__meta">' + esc(meta.join(" · ")) + "</div>"
            : "") +
          (item.hotelDemandRationale
            ? '<div class="hdp-context-list__rationale">' + esc(item.hotelDemandRationale) + "</div>"
            : "") +
        "</div>" +
      "</li>"
    );
  }

  function renderContextList(items, options) {
    if (!items || !items.length) {
      return '<p class="hdp-empty">' + esc(options.emptyMessage || "No nearby points found.") + "</p>";
    }
    return (
      '<ul class="hdp-context-list">' +
        items.map(function (item) {
          return renderContextListItem(item, options);
        }).join("") +
      "</ul>"
    );
  }

  function renderLocationBlock(hotel) {
    var addressLine = buildAddressLine(hotel);
    var directionsUrl = buildDirectionsUrl(hotel);
    return (
      '<div class="hdp-location">' +
        (addressLine ? "<p><strong>Address:</strong> " + esc(addressLine) + "</p>" : "") +
        "<p><strong>City / country:</strong> " + esc(displayValue(hotel.city) + ", " + displayValue(hotel.country)) + "</p>" +
        (hotel.market ? "<p><strong>Market:</strong> " + esc(hotel.market) + "</p>" : "") +
        (directionsUrl
          ? "<p><strong>Coordinates:</strong> " + esc(Number(hotel.lat).toFixed(4) + ", " + Number(hotel.lng).toFixed(4)) + "</p>"
          : "") +
      "</div>"
    );
  }

  function renderLocationSection(hotel) {
    return (
      '<section class="hdp-section hdp-location-section">' +
        '<h3 class="hdp-section__title">Location</h3>' +
        renderLocationBlock(hotel) +
      "</section>"
    );
  }

  function renderDistancePrefsControlHtml() {
    if (
      !window.HeMeasurementPrefs ||
      typeof window.HeMeasurementPrefs.renderDistanceControlHtml !== "function"
    ) {
      return "";
    }
    return (
      '<div class="hdp-meas-prefs-slot" data-hdp-meas="distance">' +
      window.HeMeasurementPrefs.renderDistanceControlHtml("he-meas-prefs--tab") +
      "</div>"
    );
  }

  function bindDistancePrefsControls(container) {
    if (
      !container ||
      !window.HeMeasurementPrefs ||
      typeof window.HeMeasurementPrefs.bindControls !== "function"
    ) {
      return;
    }
    var slot = container.querySelector("[data-hdp-meas=\"distance\"]");
    if (slot) window.HeMeasurementPrefs.bindControls(slot);
  }

  function renderDemandDriversTabPanel(hotel, bodyHtml) {
    // mi/km toggle lives on Access & Connectivity when distances are shown (HE-QL-037).
    // Demand Drivers still format via HeMeasurementPrefs when set elsewhere.
    return (
      '<div class="hdp-demand-drivers-tab">' +
        '<div class="hdp-detail-row">' +
          '<section class="hdp-section hdp-demand-drivers-location">' +
            '<h3 class="hdp-section__title">Location</h3>' +
            renderLocationBlock(hotel) +
          "</section>" +
          '<section class="hdp-section hdp-demand-drivers-body">' +
            '<h3 class="hdp-section__title">Demand Drivers</h3>' +
            '<div class="hdp-demand-drivers__content">' + (bodyHtml || "") + "</div>" +
          "</section>" +
        "</div>" +
      "</div>"
    );
  }

  function buildDemandAnchorsFetchOpts(hotel) {
    var opts = {};
    if (hotel && hotel.country && hotel.country !== "Unknown Country") {
      opts.country = hotel.country;
    }
    var region = hotel && String(hotel.region || "").trim();
    if (region && DEMAND_ANCHOR_REGION_VALUES[region]) {
      opts.region = region;
    }
    return opts;
  }

  function buildInfrastructureFetchOpts(hotel) {
    var opts = {};
    if (hotel && hotel.country && hotel.country !== "Unknown Country") {
      opts.country = hotel.country;
    }
    return opts;
  }

  function isIndependentHotel(hotel) {
    var brand = String((hotel && hotel.brand) || "").trim().toLowerCase();
    return !brand || brand === "unknown brand" || brand === "independent" || brand === "unknown";
  }

  function normalizeChainScale(hotel) {
    var scale = (hotel && (hotel.chainScale || hotel.propertyType)) || "";
    scale = String(scale).trim();
    return scale && scale !== "Unknown" ? scale : "Unclassified";
  }

  var SUBMARKET_CHAIN_SCALE_ORDER = [
    "Luxury",
    "Upper Upscale",
    "Upscale",
    "Upper Midscale",
    "Midscale",
    "Economy",
    "Extended Stay",
    "Select Service",
    "Independant",
    "Independent",
    "Unclassified",
  ];

  function chainScaleSortIndex(label) {
    var raw = String(label || "").trim();
    var key = raw.replace(/\s+chain\s*$/i, "").trim() || raw;
    var k = key.toLowerCase();
    for (var i = 0; i < SUBMARKET_CHAIN_SCALE_ORDER.length; i++) {
      var o = SUBMARKET_CHAIN_SCALE_ORDER[i].toLowerCase();
      if (k === o || k.indexOf(o + " ") === 0) return i;
    }
    return SUBMARKET_CHAIN_SCALE_ORDER.length;
  }

  function sortChainScaleRows(rows) {
    return (rows || []).slice().sort(function (a, b) {
      var ai = chainScaleSortIndex(a.label);
      var bi = chainScaleSortIndex(b.label);
      if (ai !== bi) return ai - bi;
      return b.hotels - a.hotels || b.rooms - a.rooms || String(a.label).localeCompare(String(b.label));
    });
  }

  /**
   * Scout / fixture coverage may return count maps `{ "Brand": 3 }` or HDP bucket rows.
   * Normalize to the bucket-row shape used by footprint tables.
   */
  function normalizeCoverageBreakdownRows(raw) {
    if (!raw) return [];
    if (Array.isArray(raw)) {
      return raw
        .map(function (row) {
          if (!row || typeof row !== "object") return null;
          var hotels = fpMetricNum(row.hotels != null ? row.hotels : row.count);
          var rooms = fpMetricNum(row.rooms);
          var openHotels = fpMetricNum(row.openHotels);
          var openRooms = fpMetricNum(row.openRooms);
          var pipelineHotels = fpMetricNum(row.pipelineHotels);
          var pipelineRooms = fpMetricNum(row.pipelineRooms);
          if (!openHotels && !pipelineHotels && hotels) openHotels = hotels;
          if (!openRooms && !pipelineRooms && rooms) openRooms = rooms;
          return {
            label: row.label || row.name || row.key || "Unknown",
            hotels: hotels || openHotels + pipelineHotels,
            rooms: rooms || openRooms + pipelineRooms,
            openHotels: openHotels,
            openRooms: openRooms,
            pipelineHotels: pipelineHotels,
            pipelineRooms: pipelineRooms,
          };
        })
        .filter(Boolean);
    }
    if (typeof raw === "object") {
      return Object.keys(raw)
        .map(function (key) {
          var val = raw[key];
          if (val != null && typeof val === "object" && !Array.isArray(val)) {
            var hotels = fpMetricNum(val.hotels != null ? val.hotels : val.count);
            var rooms = fpMetricNum(val.rooms);
            var openHotels = fpMetricNum(val.openHotels);
            var openRooms = fpMetricNum(val.openRooms);
            var pipelineHotels = fpMetricNum(val.pipelineHotels);
            var pipelineRooms = fpMetricNum(val.pipelineRooms);
            if (!openHotels && !pipelineHotels && hotels) openHotels = hotels;
            if (!openRooms && !pipelineRooms && rooms) openRooms = rooms;
            return {
              label: val.label || key,
              hotels: hotels || openHotels + pipelineHotels,
              rooms: rooms || openRooms + pipelineRooms,
              openHotels: openHotels,
              openRooms: openRooms,
              pipelineHotels: pipelineHotels,
              pipelineRooms: pipelineRooms,
            };
          }
          var count = fpMetricNum(val);
          return {
            label: key,
            hotels: count,
            rooms: 0,
            openHotels: count,
            openRooms: 0,
            pipelineHotels: 0,
            pipelineRooms: 0,
          };
        })
        .sort(function (a, b) {
          return b.hotels - a.hotels || String(a.label).localeCompare(String(b.label));
        });
    }
    return [];
  }

  function normalizeCoverageBreakdowns(breakdowns) {
    breakdowns = breakdowns || {};
    return {
      byStatus: normalizeCoverageBreakdownRows(breakdowns.byStatus),
      byChainScale: normalizeCoverageBreakdownRows(breakdowns.byChainScale),
      byBrand: normalizeCoverageBreakdownRows(breakdowns.byBrand),
      byParentCompany: normalizeCoverageBreakdownRows(breakdowns.byParentCompany),
    };
  }

  function groupByField(items, fieldFn) {
    var groups = {};
    (items || []).forEach(function (item) {
      var key = fieldFn(item) || "Other";
      if (!groups[key]) groups[key] = [];
      groups[key].push(item);
    });
    return groups;
  }

  /**
   * Never invent drive times from straight-line distance.
   * Only surface verified Airtable Estimated Drive Time / travelTime fields.
   */
  function verifiedTravelTimeLabel(item) {
    if (!item || typeof item !== "object") return null;
    if (item.travelTimeVerified === false && item.estimatedDriveTime == null && !item.travelTime) {
      return null;
    }
    var raw =
      item.estimatedDriveTime != null
        ? item.estimatedDriveTime
        : item.travelTime != null
          ? item.travelTime
          : item.verifiedDriveMinutes != null
            ? item.verifiedDriveMinutes
            : null;
    if (raw == null || raw === "") return null;
    if (typeof raw === "number" && Number.isFinite(raw)) {
      return Math.round(raw) + " min drive (verified)";
    }
    var s = String(raw).trim();
    return s || null;
  }

  function renderMetricChips(chips) {
    return (
      '<div class="hdp-competitive-summary">' +
        chips
          .filter(function (chip) {
            return chip && chip.value != null && chip.value !== "";
          })
          .map(function (chip) {
            return (
              '<div class="hdp-competitive-chip">' +
                '<span class="hdp-competitive-chip__value">' + esc(String(chip.value)) + "</span>" +
                '<span class="hdp-competitive-chip__label">' + esc(chip.label) + "</span>" +
              "</div>"
            );
          })
          .join("") +
      "</div>"
    );
  }

  function setTabPanel(panelId, html, state) {
    if (!panelEl) return;
    var panel = panelEl.querySelector('[data-panel="' + panelId + '"]');
    if (!panel) return;
    if (state) panel.dataset.state = state;
    panel.innerHTML = html || "";
    if (
      panelId === "demand-drivers" ||
      panelId === "access-connectivity" ||
      panelId === "area-hotels"
    ) {
      bindFilterPills(panel);
    }
    if (panelId === "access-connectivity" || panelId === "area-hotels") {
      bindDistancePrefsControls(panel);
    }
  }

  function buildSubmarketCoverageUrl(hotel) {
    var params = ["includePipeline=1"];
    if (hotel.country && hotel.country !== "Unknown Country") {
      params.push("country=" + encodeURIComponent(hotel.country));
    }
    var submarketScope = resolveEffectiveSubmarket(hotel);
    if (submarketScope) {
      params.push("submarket=" + encodeURIComponent(submarketScope));
    } else if (hotel.market) {
      params.push("market=" + encodeURIComponent(hotel.market));
    }
    return "/api/scout/market-coverage?" + params.join("&");
  }

  function fetchSubmarketCoverage(hotel) {
    return fetch(buildSubmarketCoverageUrl(hotel), {
      headers: { "ngrok-skip-browser-warning": "true" },
    }).then(function (res) {
      return res.json();
    });
  }

  /** Map Scout / Hotel Census sample rows into HE hotel objects (read-only). */
  function mapCensusSampleToHotels(sample) {
    return (sample || [])
      .map(function (row) {
        if (!row) return null;
        var lat = Number(row.latitude != null ? row.latitude : row.lat);
        var lng = Number(row.longitude != null ? row.longitude : row.lng);
        return {
          id: row.id,
          recordId: row.id,
          name: row.name,
          brand: row.affiliation || row.brand || "",
          parentCompany: row.parentCompany || "",
          status: row.status || "",
          rooms: row.rooms,
          country: row.country || "",
          city: row.city || "",
          market: row.market || "",
          submarket: row.submarket || "",
          chainScale: row.chainScale || "",
          locationType: row.locationType || "",
          projectPhase: row.projectPhase || "",
          lat: Number.isFinite(lat) ? lat : null,
          lng: Number.isFinite(lng) ? lng : null,
          latitude: Number.isFinite(lat) ? lat : null,
          longitude: Number.isFinite(lng) ? lng : null,
        };
      })
      .filter(Boolean);
  }

  function dedupeHotelsByIdentity(hotels) {
    var seen = {};
    var out = [];
    (hotels || []).forEach(function (h) {
      var id = String(h.id || h.recordId || "").trim();
      var key =
        id ||
        [
          String(h.name || "")
            .toLowerCase()
            .trim(),
          String(h.city || "")
            .toLowerCase()
            .trim(),
          String(h.brand || "")
            .toLowerCase()
            .trim(),
        ].join("|");
      if (!key || seen[key]) return;
      seen[key] = true;
      out.push(h);
    });
    return out;
  }

  /**
   * Canonical HPC first; Radar map + Scout Hotel Census are supporting enrichment only.
   * A valid HPC hotel must never disappear because Scout/Radar pools are empty.
   * Never writes. Dedupes identities before aggregation.
   */
  function buildCityMarketCoverageUrl(hotel) {
    var params = [];
    if (hotel && hotel.country) params.push("country=" + encodeURIComponent(hotel.country));
    if (hotel && hotel.city) params.push("city=" + encodeURIComponent(hotel.city));
    else if (hotel && hotel.market) params.push("market=" + encodeURIComponent(hotel.market));
    params.push("includePipeline=1");
    return "/api/scout/market-coverage?" + params.join("&");
  }

  function fetchCoverageByUrl(url) {
    return fetch(url, { headers: { "ngrok-skip-browser-warning": "true" } }).then(function (res) {
      return res.json();
    });
  }

  function hotelRecordId(hotel) {
    return String((hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "").trim();
  }

  function fetchHpcSubmarketSnapshot(hotel) {
    var id = hotelRecordId(hotel);
    if (!/^rec[A-Za-z0-9]{14}$/.test(id)) {
      return Promise.resolve(null);
    }
    return fetch("/api/hotel-explorer/hotels/" + encodeURIComponent(id) + "/submarket-snapshot", {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (res) {
        return res.json().then(function (body) {
          return { ok: res.ok && body && body.success !== false && body.ok !== false, body: body };
        });
      })
      .catch(function () {
        return null;
      });
  }

  function ensureSelectedHotelInPool(hotel, hotels) {
    var list = dedupeHotelsByIdentity(hotels || []);
    var id = hotelRecordId(hotel);
    if (!hotel || !id) return list;
    var found = list.some(function (h) {
      return hotelRecordId(h) === id;
    });
    if (found) return list;
    return dedupeHotelsByIdentity(
      [
        {
          id: id,
          recordId: id,
          name: hotel.name,
          brand: hotel.brand || hotel.brand_display || "",
          parentCompany: hotel.parentCompany || "",
          status: hotel.status || hotel.radarStatus || "Open",
          rooms: hotel.rooms,
          country: hotel.country || "",
          city: hotel.city || "",
          market: hotel.market || "",
          submarket: hotel.submarket || "",
          chainScale: hotel.chainScale || hotel.chain_scale || "",
          locationType: hotel.locationType || "",
          lat: hotel.lat,
          lng: hotel.lng,
          source: "hpc_selected",
        },
      ].concat(list)
    );
  }

  function resolveSupportingCensusPool(hotel) {
    var fromMap = dedupeHotelsByIdentity(window.allHotels || []);
    if (fromMap.length) {
      return Promise.resolve({ hotels: fromMap, source: "radar_map", report: null });
    }
    return fetchSubmarketCoverage(hotel)
      .then(function (report) {
        var sample = report && report.success ? mapCensusSampleToHotels(report.recordsSample || []) : [];
        var metricsTotal =
          report && report.success && report.metrics
            ? fpMetricNum(report.metrics.totalHotels) ||
              fpMetricNum(report.metrics.openHotels) + fpMetricNum(report.metrics.pipelineHotels)
            : 0;
        if (sample.length || metricsTotal > 0) {
          return {
            hotels: dedupeHotelsByIdentity(sample),
            source: "scout_census",
            report: report,
          };
        }
        return fetchCoverageByUrl(buildCityMarketCoverageUrl(hotel)).then(function (cityReport) {
          var citySample =
            cityReport && cityReport.success
              ? mapCensusSampleToHotels(cityReport.recordsSample || [])
              : [];
          return {
            hotels: dedupeHotelsByIdentity(citySample),
            source: citySample.length || (cityReport && cityReport.success) ? "scout_census_city" : "none",
            report: cityReport && cityReport.success ? cityReport : report,
            coverageScope: "city_or_market_fallback",
          };
        });
      })
      .catch(function () {
        return { hotels: [], source: "none", report: null };
      });
  }

  function resolveCensusHotelPool(hotel) {
    return fetchHpcSubmarketSnapshot(hotel).then(function (hpcResult) {
      var hpcBody = hpcResult && hpcResult.ok ? hpcResult.body : null;
      var hpcHotels = hpcBody && Array.isArray(hpcBody.hotels) ? hpcBody.hotels : [];
      return resolveSupportingCensusPool(hotel).then(function (support) {
        var merged = dedupeHotelsByIdentity((hpcHotels || []).concat(support.hotels || []));
        merged = ensureSelectedHotelInPool(hotel, merged);
        if (hpcHotels.length || (hpcBody && hpcBody.selectedHotel)) {
          return {
            hotels: merged,
            source: "hpc",
            supportSource: support.source,
            report: support.report || null,
            hpc: hpcBody,
            coverage: hpcBody && hpcBody.coverage ? hpcBody.coverage : null,
            grouping: hpcBody && hpcBody.grouping ? hpcBody.grouping : null,
            metrics: hpcBody && hpcBody.metrics ? hpcBody.metrics : null,
          };
        }
        // HPC unavailable — fall back to supporting pools, still force-include selected hotel.
        return {
          hotels: ensureSelectedHotelInPool(hotel, support.hotels || []),
          source: support.source || "none",
          report: support.report || null,
          coverageScope: support.coverageScope || null,
          hpc: null,
        };
      });
    });
  }

  function isStrRegionalSubmarket(value) {
    return /\bregional$/i.test(String(value || "").trim());
  }

  var DC_DR_CITY_CORRIDOR = {
    "punta cana": "Punta Cana / Bávaro / Cap Cana",
    bavaro: "Punta Cana / Bávaro / Cap Cana",
    bávaro: "Punta Cana / Bávaro / Cap Cana",
    higuey: "Punta Cana / Bávaro / Cap Cana",
    macao: "Punta Cana / Bávaro / Cap Cana",
    "cap cana": "Punta Cana / Bávaro / Cap Cana",
    "uvero alto": "Punta Cana / Bávaro / Cap Cana",
    miches: "Miches / Costa Esmeralda",
    "la romana": "La Romana / Bayahibe",
    bayahibe: "La Romana / Bayahibe",
    "santo domingo": "Santo Domingo Metro",
    "puerto plata": "Puerto Plata / Sosúa / Cabarete",
    sosua: "Puerto Plata / Sosúa / Cabarete",
    sousa: "Puerto Plata / Sosúa / Cabarete",
    cabarete: "Puerto Plata / Sosúa / Cabarete",
    samana: "Samaná / Las Terrenas",
    samaná: "Samaná / Las Terrenas",
    "las terrenas": "Samaná / Las Terrenas",
    "las galeras": "Samaná / Las Terrenas",
    santiago: "Santiago / Cibao",
    "santiago de los caballeros": "Santiago / Cibao",
    jarabacoa: "Jarabacoa / Constanza",
    constanza: "Jarabacoa / Constanza",
    "boca chica": "Boca Chica / Juan Dolio",
    "juan dolio": "Boca Chica / Juan Dolio",
    "playa juan dolio": "Boca Chica / Juan Dolio",
    pedernales: "Barahona / Pedernales",
    barahona: "Barahona / Pedernales",
    bahoruco: "Barahona / Pedernales",
  };

  var DC_DR_CORRIDORS = [
    "Punta Cana / Bávaro / Cap Cana",
    "Santo Domingo Metro",
    "Puerto Plata / Sosúa / Cabarete",
    "La Romana / Bayahibe",
    "Samaná / Las Terrenas",
    "Santiago / Cibao",
    "Miches / Costa Esmeralda",
    "Barahona / Pedernales",
    "Boca Chica / Juan Dolio",
    "Jarabacoa / Constanza",
  ];

  function normalizeCorridorFromStrLabel(label, country) {
    var raw = String(label || "").trim();
    if (!raw || isStrRegionalSubmarket(raw)) return "";
    var lower = raw.toLowerCase();
    var corridors = country === "Dominican Republic" ? DC_DR_CORRIDORS : [];
    for (var i = 0; i < corridors.length; i++) {
      var corridor = corridors[i];
      var cLower = corridor.toLowerCase();
      if (cLower === lower || cLower.indexOf(lower) >= 0 || lower.indexOf(cLower) >= 0) {
        return corridor;
      }
    }
    return raw;
  }

  function resolveEffectiveSubmarket(hotel) {
    if (!hotel) return "";
    var sub = String(hotel.submarket || "").trim();
    if (sub && !isStrRegionalSubmarket(sub)) {
      var country = String(hotel.country || "").trim();
      var normalized = normalizeCorridorFromStrLabel(sub, country);
      return normalized || sub;
    }

    var country = String(hotel.country || "").trim();
    var city = String(hotel.city || "").trim().toLowerCase();
    if (country === "Dominican Republic" && DC_DR_CITY_CORRIDOR[city]) {
      return DC_DR_CITY_CORRIDOR[city];
    }

    return sub || "";
  }

  function filterHotelsBySubmarket(hotel, hotels) {
    var corridor = resolveEffectiveSubmarket(hotel);
    if (corridor) {
      return (hotels || []).filter(function (h) {
        if (hotel.country && h.country && h.country !== hotel.country) return false;
        return resolveEffectiveSubmarket(h) === corridor;
      });
    }

    var submarket = hotel && String(hotel.submarket || "").trim();
    if (!submarket || submarket === "Unknown") {
      return (hotels || []).filter(function (h) {
        if (hotel.market && h.market && h.market !== hotel.market) return false;
        if (hotel.country && h.country && h.country !== hotel.country) return false;
        return true;
      });
    }
    return (hotels || []).filter(function (h) {
      if (String(h.submarket || "").trim() !== submarket) return false;
      if (hotel.country && h.country && h.country !== hotel.country) return false;
      return true;
    });
  }

  function bumpSubmarketBucket(map, key, hotel) {
    var k = key || "Unknown";
    if (!map[k]) {
      map[k] = {
        label: k,
        hotels: 0,
        rooms: 0,
        openHotels: 0,
        openRooms: 0,
        pipelineHotels: 0,
        pipelineRooms: 0,
      };
    }
    map[k].hotels += 1;
    map[k].rooms += Number(hotel.rooms) || 0;
    var status = String(hotel.status || "").toLowerCase();
    if (status === "open") {
      map[k].openHotels += 1;
      map[k].openRooms += Number(hotel.rooms) || 0;
    } else if (status === "pipeline") {
      map[k].pipelineHotels += 1;
      map[k].pipelineRooms += Number(hotel.rooms) || 0;
    }
  }

  function breakdownMapToSortedRows(map, sortBy) {
    return Object.keys(map)
      .map(function (key) {
        return map[key];
      })
      .sort(function (a, b) {
        if (sortBy === "rooms") {
          return b.rooms - a.rooms || b.hotels - a.hotels || b.openRooms - a.openRooms;
        }
        return b.hotels - a.hotels || b.rooms - a.rooms || b.openRooms - a.openRooms;
      });
  }

  function knownRoomsValue(hotel) {
    var n = Number(hotel && hotel.rooms);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  function computeSubmarketMetrics(hotels) {
    var open = [];
    var pipeline = [];
    var candidate = [];
    (hotels || []).forEach(function (h) {
      var status = String(h.status || "").toLowerCase();
      if (status === "open") open.push(h);
      else if (status === "pipeline") pipeline.push(h);
      else if (status === "candidate") candidate.push(h);
    });
    var brandedOpen = open.filter(function (h) {
      return !isIndependentHotel(h);
    });
    var brands = {};
    var parents = {};
    open.forEach(function (h) {
      if (!isIndependentHotel(h) && h.brand) brands[h.brand] = true;
      if (h.parentCompany && h.parentCompany !== "Unknown") parents[h.parentCompany] = true;
    });
    var roomsKnownHotelCount = (hotels || []).filter(function (h) {
      return knownRoomsValue(h) > 0;
    }).length;
    return {
      totalHotels: (hotels || []).length,
      openHotels: open.length,
      // Sum only known positive room counts — never invent zero inventory for unknowns.
      openRooms: open.reduce(function (sum, h) {
        return sum + knownRoomsValue(h);
      }, 0),
      pipelineHotels: pipeline.length,
      pipelineRooms: pipeline.reduce(function (sum, h) {
        return sum + knownRoomsValue(h);
      }, 0),
      candidateHotels: candidate.length,
      candidateRooms: candidate.reduce(function (sum, h) {
        return sum + knownRoomsValue(h);
      }, 0),
      roomsKnownHotelCount: roomsKnownHotelCount,
      roomsUnknownHotelCount: (hotels || []).length - roomsKnownHotelCount,
      brandedOpen: brandedOpen.length,
      independentOpen: open.length - brandedOpen.length,
      brandCount: Object.keys(brands).length,
      parentCompanyCount: Object.keys(parents).length,
    };
  }

  function resolveSubmarketTotalRooms(metrics, submarketHotels) {
    metrics = metrics || {};
    var fromHotels = (submarketHotels || []).reduce(function (sum, h) {
      return sum + (Number(h.rooms) || 0);
    }, 0);
    if (fromHotels > 0) return fromHotels;
    if (fpMetricNum(metrics.totalRooms) > 0) return fpMetricNum(metrics.totalRooms);
    return (
      fpMetricNum(metrics.openRooms) +
      fpMetricNum(metrics.pipelineRooms) +
      fpMetricNum(metrics.candidateRooms)
    );
  }

  function computeSubmarketBreakdowns(hotels) {
    var byChainScale = {};
    var byBrand = {};
    var byStatus = {};
    var byParentCompany = {};
    (hotels || []).forEach(function (h) {
      bumpSubmarketBucket(byChainScale, normalizeChainScale(h), h);
      bumpSubmarketBucket(
        byBrand,
        isIndependentHotel(h) ? "Independent" : displayValue(h.brand, "Unknown Brand"),
        h
      );
      bumpSubmarketBucket(byStatus, displayValue(h.status, "Unknown"), h);
      if (!isIndependentHotel(h)) {
        bumpSubmarketBucket(byParentCompany, formatParentCompanyDisplay(h.parentCompany), h);
      }
    });
    return {
      byChainScale: breakdownMapToSortedRows(byChainScale),
      byBrand: breakdownMapToSortedRows(byBrand, "rooms"),
      byStatus: breakdownMapToSortedRows(byStatus),
      byParentCompany: breakdownMapToSortedRows(byParentCompany, "rooms"),
    };
  }

  function computeSubmarketPipelinePressure(hotel, sourceHotels, filters) {
    var RPP = window.RadarPipelinePressure || window.RadarBrandedPenetration;
    if (!RPP || !hotel) return null;
    var result = RPP.getPipelinePressureForHotel(
      hotel,
      sourceHotels || [],
      filters || (typeof RPP.resolveMapFilters === "function" ? RPP.resolveMapFilters() : {})
    );
    if (!result || !result.metrics || !result.metrics.sufficientSample) return null;
    return result.metrics;
  }

  function computeSubmarketPenetration(hotel, sourceHotels, filters) {
    var RBP = window.RadarBrandedPenetration;
    if (!RBP || !hotel) return null;
    var result = RBP.getPenetrationForHotel(
      hotel,
      sourceHotels || [],
      filters || (typeof RBP.resolveMapFilters === "function" ? RBP.resolveMapFilters() : {})
    );
    if (!result || !result.metrics || !result.metrics.sufficientSample) return null;
    return result.metrics.percentage;
  }

  function formatPipelinePressureMeta(metrics) {
    if (!metrics) return "";
    var headline =
      metrics.usesKeyWeighting && metrics.keyPercentage != null
        ? "Pipeline key pressure: " + metrics.keyPercentage + "%"
        : "Pipeline unit pressure: " + metrics.unitPercentage + "%";
    var detail =
      " (" +
      metrics.pipelineHotels +
      " pipeline / " +
      metrics.openHotels +
      " open hotels";
    if (metrics.usesKeyWeighting && metrics.openRooms > 0) {
      detail +=
        "; " + metrics.pipelineRooms + " / " + metrics.openRooms + " keys";
    }
    detail += ", current map filters)";
    return headline + detail;
  }

  function sortSubmarketHotels(hotels) {
    var statusOrder = { open: 0, pipeline: 1, candidate: 2 };
    return (hotels || []).slice().sort(function (a, b) {
      var aStatus = statusOrder[String(a.status || "").toLowerCase()];
      var bStatus = statusOrder[String(b.status || "").toLowerCase()];
      if (aStatus == null) aStatus = 3;
      if (bStatus == null) bStatus = 3;
      if (aStatus !== bStatus) return aStatus - bStatus;
      return (Number(b.rooms) || 0) - (Number(a.rooms) || 0);
    });
  }

  function renderSubmarketHotelRow(h) {
    var meta = [
      displayValue(h.status),
      normalizeChainScale(h),
      h.rooms ? formatNumber(h.rooms) + " rooms" : null,
      h.city || null,
    ].filter(Boolean);
    return (
      '<li class="hdp-context-list__item hdp-submarket-hotel">' +
        '<div class="hdp-context-list__body">' +
          '<div class="hdp-context-list__name">' + esc(displayValue(h.name, "Hotel")) + "</div>" +
          '<div class="hdp-context-list__type">' + esc(isIndependentHotel(h) ? "Independent" : displayValue(h.brand)) + "</div>" +
          '<div class="hdp-context-list__meta">' + esc(meta.join(" · ")) + "</div>" +
        "</div>" +
      "</li>"
    );
  }

  function fpMetricNum(value) {
    var n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  function fpMetricCell(value) {
    var n = fpMetricNum(value);
    return n > 0 ? esc(formatNumber(n)) : "—";
  }

  function fpMetricAvgKeys(hotels, rooms) {
    var h = fpMetricNum(hotels);
    var r = fpMetricNum(rooms);
    if (h <= 0) return "—";
    return esc(formatNumber(Math.round(r / h)));
  }

  function fpMetricPctCell(part, total) {
    var p = fpMetricNum(part);
    var t = fpMetricNum(total);
    if (t <= 0 || p <= 0) return "—";
    return esc(String(Math.round((p / t) * 100)) + "%");
  }

  function fpMetricRowHtml(label, openH, openR, pipeH, pipeR, totalH, totalR, isTotal, shareDenoms) {
    var totalHotels = totalH != null ? fpMetricNum(totalH) : fpMetricNum(openH) + fpMetricNum(pipeH);
    var totalRooms = totalR != null ? fpMetricNum(totalR) : fpMetricNum(openR) + fpMetricNum(pipeR);
    if (totalRooms <= 0) {
      var roomFallback = fpMetricNum(openR) + fpMetricNum(pipeR);
      if (roomFallback > 0) totalRooms = roomFallback;
    }
    var pctUnits = "—";
    var pctKeys = "—";
    if (shareDenoms) {
      if (isTotal && fpMetricNum(shareDenoms.hotels) > 0) {
        pctUnits = esc("100%");
        pctKeys = fpMetricNum(shareDenoms.rooms) > 0 ? esc("100%") : "—";
      } else {
        pctUnits = fpMetricPctCell(totalHotels, shareDenoms.hotels);
        pctKeys = fpMetricPctCell(totalRooms, shareDenoms.rooms);
      }
    }
    return (
      "<tr" + (isTotal ? ' class="hdp-ft-total-row"' : "") + ">" +
        '<th scope="row">' + esc(label) + "</th>" +
        "<td>" + fpMetricCell(openH) + "</td>" +
        "<td>" + fpMetricCell(openR) + "</td>" +
        "<td>" + fpMetricCell(pipeH) + "</td>" +
        "<td>" + fpMetricCell(pipeR) + "</td>" +
        "<td>" + fpMetricCell(totalHotels) + "</td>" +
        "<td>" + fpMetricCell(totalRooms) + "</td>" +
        "<td>" + fpMetricAvgKeys(totalHotels, totalRooms) + "</td>" +
        "<td>" + pctUnits + "</td>" +
        "<td>" + pctKeys + "</td>" +
      "</tr>"
    );
  }

  function footprintRowFromBucket(row, isTotal, shareDenoms) {
    return fpMetricRowHtml(
      row.label,
      row.openHotels,
      row.openRooms,
      row.pipelineHotels,
      row.pipelineRooms,
      row.hotels,
      row.rooms,
      isTotal,
      shareDenoms
    );
  }

  function footprintRowFromHotel(hotel, shareDenoms) {
    var status = String(hotel.status || "").toLowerCase();
    var rooms = fpMetricNum(hotel.rooms);
    var openH = status === "open" ? 1 : 0;
    var openR = status === "open" ? rooms : 0;
    var pipeH = status === "pipeline" ? 1 : 0;
    var pipeR = status === "pipeline" ? rooms : 0;
    var label = displayValue(hotel.name, "Hotel");
    if (!isIndependentHotel(hotel) && hotel.brand) {
      label += " · " + hotel.brand;
    }
    return fpMetricRowHtml(label, openH, openR, pipeH, pipeR, 1, rooms, false, shareDenoms);
  }

  var FP_METRIC_TABLE_HEAD =
    "<thead><tr>" +
    '<th scope="col">Category</th>' +
    '<th scope="col">Open Hotels</th>' +
    '<th scope="col">Open Rooms</th>' +
    '<th scope="col">Pipeline Hotels</th>' +
    '<th scope="col">Pipeline Rooms</th>' +
    '<th scope="col">Total Hotels</th>' +
    '<th scope="col">Total Rooms</th>' +
    '<th scope="col">Avg Keys</th>' +
    '<th scope="col">% Units</th>' +
    '<th scope="col">% Keys</th>' +
    "</tr></thead><tbody>";

  function renderFootprintTable(title, labelHeader, bodyRows, totalRow, wrapClass) {
    if (!bodyRows && !totalRow) return "";
    var wrapCls =
      "hdp-footprint-table-wrap ada-table-shell dl-table-wrap" +
      (wrapClass ? " " + wrapClass : "");
    return (
      '<section class="hdp-footprint-subsection">' +
        '<h4 class="hdp-footprint-table-title">' + esc(title) + "</h4>" +
        '<div class="' + wrapCls + '">' +
          '<table class="hdp-footprint-table ada-table dl-table">' +
            '<colgroup><col class="hdp-fp-col-label" /><col class="hdp-fp-col-metric" span="9" /></colgroup>' +
            FP_METRIC_TABLE_HEAD.replace("Category", esc(labelHeader)) +
            (bodyRows || "") +
            (totalRow || "") +
            "</tbody></table>" +
        "</div>" +
      "</section>"
    );
  }

  function renderFootprintTableFromBuckets(title, labelHeader, rows, includeTotal, shareDenoms) {
    if (!rows || !rows.length) return "";
    var body = rows.map(function (row) {
      return footprintRowFromBucket(row, false, shareDenoms);
    }).join("");
    var totalRow = "";
    if (includeTotal) {
      var sum = rows.reduce(
        function (acc, row) {
          acc.openHotels += fpMetricNum(row.openHotels);
          acc.openRooms += fpMetricNum(row.openRooms);
          acc.pipelineHotels += fpMetricNum(row.pipelineHotels);
          acc.pipelineRooms += fpMetricNum(row.pipelineRooms);
          acc.hotels += fpMetricNum(row.hotels);
          acc.rooms += fpMetricNum(row.rooms);
          return acc;
        },
        { openHotels: 0, openRooms: 0, pipelineHotels: 0, pipelineRooms: 0, hotels: 0, rooms: 0 }
      );
      totalRow = fpMetricRowHtml(
        "Total",
        sum.openHotels,
        sum.openRooms,
        sum.pipelineHotels,
        sum.pipelineRooms,
        sum.hotels,
        sum.rooms,
        true,
        shareDenoms
      );
    }
    return renderFootprintTable(title, labelHeader, body, totalRow);
  }

  function renderBreakdownSection(title, rows, valueFn) {
    if (!rows || !rows.length) return "";
    return (
      '<section class="hdp-subsection">' +
        '<h4 class="hdp-subsection__title">' + esc(title) + "</h4>" +
        '<div class="hdp-overview">' +
          rows.slice(0, 8).map(function (row) {
            return (
              '<div class="hdp-overview__row">' +
                '<div class="hdp-overview__label">' + esc(row.label || "—") + "</div>" +
                '<div class="hdp-overview__value">' + esc(valueFn(row)) + "</div>" +
              "</div>"
            );
          }).join("") +
        "</div>" +
      "</section>"
    );
  }

  function renderSubmarketSnapshotPanel(
    hotel,
    submarketHotels,
    metrics,
    breakdowns,
    penetrationPct,
    pipelineMetrics,
    opts
  ) {
    metrics = metrics || {};
    breakdowns = breakdowns || {};
    opts = opts || {};
    var corridor = resolveEffectiveSubmarket(hotel);
    var grouping = opts.grouping || {};
    var coverage = opts.coverage || {};
    var RBP = window.RadarBrandedPenetration;
    var geo = RBP && typeof RBP.geographyFromHotel === "function" ? RBP.geographyFromHotel(hotel) : null;
    var scopeLabel =
      grouping.label ||
      (geo && RBP.buildGeographyTitle ? RBP.buildGeographyTitle(geo) : null) ||
      [corridor, hotel.submarket, hotel.market, hotel.country].filter(Boolean).join(" · ");
    var strRegionalNote =
      isStrRegionalSubmarket(hotel.submarket) && corridor && corridor !== hotel.submarket
        ? '<p class="hdp-snapshot-meta">Submarket "' +
          esc(hotel.submarket) +
          '" is an STR country bucket — showing inferred corridor until census backfill runs.</p>'
        : "";
    var totalRooms = resolveSubmarketTotalRooms(metrics, submarketHotels);
    var shareDenoms = {
      hotels: fpMetricNum(metrics.totalHotels),
      rooms: totalRooms,
    };
    var incompletePctNote =
      coverage.partial || (metrics.roomsUnknownHotelCount && metrics.roomsUnknownHotelCount > 0)
        ? '<p class="hdp-snapshot-meta">Percentages reflect the matched HPC cohort only — not a complete market inventory. Unknown room counts are excluded from room totals (not treated as zero inventory). Market-share figures are never fabricated.</p>'
        : "";

    var cohorts = opts.cohorts || {};
    var geoCohort = cohorts.geographicSubmarketInventory || null;
    var geoMetrics = (geoCohort && geoCohort.metrics) || null;
    var cohortDisclosure =
      '<div class="hdp-snapshot-meta hdp-submarket-cohorts" data-he-submarket-coverage="disclosed">' +
      "<p><strong>Coverage cohorts (HPC only — legacy Hotel Census prohibited)</strong></p>" +
      "<ul>" +
      "<li>Verified matched HPC inventory: " +
      esc(
        String(
          (cohorts.matchedHpcInventory &&
            cohorts.matchedHpcInventory.hotels &&
            cohorts.matchedHpcInventory.hotels.length) ||
            (submarketHotels && submarketHotels.length) ||
            0
        )
      ) +
      " hotel(s)</li>" +
      "<li>Verified geographic market inventory: " +
      esc(
        String(
          (geoCohort && geoCohort.hotels && geoCohort.hotels.length) ||
            (coverage.cohorts && coverage.cohorts.geographicSubmarketInventory) ||
            0
        )
      ) +
      " hotel(s)" +
      (geoCohort && geoCohort.description ? " — " + esc(geoCohort.description) : "") +
      "</li>" +
      "<li>Property-relevant competitive set: starts from geographic HPC inventory; expand via Area Hotels research when authorized</li>" +
      "</ul>" +
      (coverage.researchNeeded
        ? "<p>Research needed to expand matched submarket coverage — one matched hotel is not the entire competitive market.</p>"
        : "") +
      "</div>";

    var summaryTable = renderFootprintTable(
      "Open vs. Pipeline (Submarket)",
      "Submarket",
      "",
      fpMetricRowHtml(
        "Total (matched HPC cohort)",
        metrics.openHotels,
        metrics.openRooms,
        metrics.pipelineHotels,
        metrics.pipelineRooms,
        metrics.totalHotels,
        totalRooms,
        true,
        shareDenoms
      )
    );

    var geoSummaryTable = "";
    if (geoMetrics && geoMetrics.totalHotels > (metrics.totalHotels || 0)) {
      var geoRooms = resolveSubmarketTotalRooms(geoMetrics, (geoCohort && geoCohort.hotels) || []);
      geoSummaryTable = renderFootprintTable(
        "Open vs. Pipeline (Geographic market inventory)",
        "Market",
        "",
        fpMetricRowHtml(
          "Total (verified geographic HPC cohort)",
          geoMetrics.openHotels,
          geoMetrics.openRooms,
          geoMetrics.pipelineHotels,
          geoMetrics.pipelineRooms,
          geoMetrics.totalHotels,
          geoRooms,
          true,
          { hotels: fpMetricNum(geoMetrics.totalHotels), rooms: geoRooms }
        )
      );
    }

    var hotelTable = "";
    if (submarketHotels && submarketHotels.length) {
      var tableTitle =
        opts.source === "hpc"
          ? "Matched HPC hotels (" +
            submarketHotels.length +
            ")" +
            (coverage.partial ? " · partial coverage" : "")
          : "All census hotels in submarket (" + submarketHotels.length + ")";
      hotelTable = renderFootprintTable(
        tableTitle,
        "Hotel",
        sortSubmarketHotels(submarketHotels).map(function (h) {
          return footprintRowFromHotel(h, shareDenoms);
        }).join(""),
        "",
        "hdp-submarket-hotel-table-wrap"
      );
    } else if (metrics && fpMetricNum(metrics.totalHotels) > 0) {
      hotelTable =
        '<p class="hdp-snapshot-meta">Hotel-level rows are not loaded in this session. Aggregate metrics above are incomplete and are not treated as a full market census.</p>';
    } else {
      hotelTable =
        '<p class="hdp-empty">No matched HPC hotels for this geography yet. The selected property should still appear when its canonical HPC record is available — empty Scout/Radar pools alone do not mean the hotel is missing.</p>';
    }

    var metaLine = penetrationPct != null
      ? '<p class="hdp-snapshot-meta">Brand penetration (open hotels, current map filters): ' +
        esc(String(penetrationPct)) +
        "%</p>"
      : "";
    var pipelineMetaLine = pipelineMetrics
      ? '<p class="hdp-snapshot-meta">' + esc(formatPipelinePressureMeta(pipelineMetrics)) + "</p>"
      : "";

    return (
      '<div class="hdp-submarket-snapshot hdp-footprint-metrics" data-hdp-submarket-source="' +
        esc(opts.source || "unknown") +
        '">' +
        '<p class="hdp-snapshot-scope">' + esc(scopeLabel || "Submarket scope unavailable") + "</p>" +
        strRegionalNote +
        incompletePctNote +
        cohortDisclosure +
        metaLine +
        pipelineMetaLine +
        geoSummaryTable +
        summaryTable +
        renderFootprintTableFromBuckets("Status mix", "Status", breakdowns.byStatus || [], false, shareDenoms) +
        renderFootprintTableFromBuckets(
          "Chain scale mix",
          "Chain scale",
          sortChainScaleRows(breakdowns.byChainScale || []),
          false,
          shareDenoms
        ) +
        renderFootprintTableFromBuckets("Brands", "Brand", breakdowns.byBrand || [], false, shareDenoms) +
        renderFootprintTableFromBuckets(
          "Parent companies",
          "Parent company",
          breakdowns.byParentCompany || [],
          false,
          shareDenoms
        ) +
        hotelTable +
      "</div>"
    );
  }

  function renderPipelineHotelItem(hotel, options) {
    options = options || {};
    var meta = [];
    if (hotel.distanceKm != null) meta.push(formatDistanceKm(hotel.distanceKm));
    meta.push(normalizeChainScale(hotel));
    if (hotel.rooms) meta.push(formatNumber(hotel.rooms) + " rooms");
    if (hotel.projectPhase) meta.push(hotel.projectPhase);

    var phaseKey = String(hotel.projectPhase || "Pipeline").trim();
    var extraClass = options.areaHotelsList ? " hdp-area-hotels-list__item" : "";
    var dataAttrs = options.areaHotelsList
      ? ' data-project-phase="' + esc(phaseKey) + '"'
      : "";

    return (
      '<li class="hdp-context-list__item' + extraClass + '"' + dataAttrs + ">" +
        '<span class="hdp-context-list__dot" style="background:#dc2626"></span>' +
        '<div class="hdp-context-list__body">' +
          '<div class="hdp-context-list__name">' + esc(displayValue(hotel.name, "Hotel")) + "</div>" +
          '<div class="hdp-context-list__type">' + esc(displayValue(hotel.brand, "Independent")) + "</div>" +
          '<div class="hdp-context-list__meta">' + esc(meta.join(" · ")) + "</div>" +
        "</div>" +
      "</li>"
    );
  }

  function sumHotelRooms(hotels) {
    return (hotels || []).reduce(function (sum, h) {
      var rooms = Number(h.rooms);
      return sum + (Number.isFinite(rooms) ? rooms : 0);
    }, 0);
  }

  function buildAreaHotelsFilterPills(title, groupId, counts, total, sortKeysFn) {
    if (!total) return "";
    var pills = [
      '<button type="button" class="hdp-filter-pill is-active" data-filter-value="__all__">All (' +
        esc(String(total)) +
        ")</button>",
    ];
    var keys = Object.keys(counts);
    if (typeof sortKeysFn === "function") {
      keys = sortKeysFn(keys.slice(), counts);
    } else {
      keys.sort(function (a, b) {
        return counts[b] - counts[a] || a.localeCompare(b);
      });
    }
    keys
      .forEach(function (key) {
        pills.push(
          '<button type="button" class="hdp-filter-pill" data-filter-value="' +
            esc(key) +
            '">' +
            esc(key) +
            ' <span class="hdp-filter-pill__count">(' +
            esc(String(counts[key])) +
            ")</span></button>"
        );
      });
    return (
      '<div class="hdp-context-filters hdp-area-hotels-filters" data-filter-group="' +
        esc(groupId) +
        '">' +
        '<h4 class="hdp-area-hotels-filters__title">' +
        esc(title) +
        "</h4>" +
        '<div class="hdp-filter-pills">' +
        pills.join("") +
        "</div>" +
      "</div>"
    );
  }

  function renderAreaHotelsExistingColumn(nearbyExisting, scope) {
    scope = scope || {};
    if (!nearbyExisting.length) {
      var emptyRadius =
        scope.mode === "submarket" && scope.corridor
          ? 'the "' + scope.corridor + '" submarket'
          : formatRadiusLabel(scope.radiusKm);
      return (
        '<p class="hdp-empty">No other open hotels found within ' + esc(String(emptyRadius)) + ".</p>"
      );
    }

    var branded = 0;
    var independent = 0;
    var scaleCounts = {};
    nearbyExisting.forEach(function (h) {
      if (isIndependentHotel(h)) independent += 1;
      else branded += 1;
      var scale = normalizeChainScale(h);
      scaleCounts[scale] = (scaleCounts[scale] || 0) + 1;
    });
    var totalRooms = sumHotelRooms(nearbyExisting);

    var kpiChips = [
      { value: nearbyExisting.length, label: "Hotels nearby" },
      { value: branded, label: "Branded" },
      { value: independent, label: "Independent" },
    ];
    if (totalRooms > 0) {
      kpiChips.push({ value: formatNumber(totalRooms), label: "Rooms (nearby)" });
    }

    return (
      renderMetricChips(kpiChips) +
      buildAreaHotelsFilterPills("Chain scale mix", "chain-scale", scaleCounts, nearbyExisting.length) +
      '<ul class="hdp-context-list hdp-competitive-list" data-area-hotels-list>' +
        nearbyExisting
          .map(function (h) {
            return renderCompetitiveHotelItem(h, { areaHotelsList: true });
          })
          .join("") +
      "</ul>"
    );
  }

  function renderAreaHotelsPipelineColumn(nearbyPipeline, nearbyExisting, scope) {
    scope = scope || {};
    if (!nearbyPipeline.length) {
      var emptyRadius =
        scope.mode === "submarket" && scope.corridor
          ? 'the "' + scope.corridor + '" submarket'
          : formatRadiusLabel(scope.radiusKm);
      return (
        '<p class="hdp-empty">No pipeline hotels within ' + esc(String(emptyRadius)) + ".</p>"
      );
    }

    var pipelineRooms = sumHotelRooms(nearbyPipeline);
    var openNearby = nearbyExisting.length;
    var pipelineRatio =
      openNearby > 0 ? Math.round((nearbyPipeline.length / openNearby) * 100) : null;

    var kpiChips = [
      { value: nearbyPipeline.length, label: "Pipeline nearby" },
      { value: formatNumber(pipelineRooms), label: "Pipeline rooms" },
      { value: openNearby, label: "Open nearby" },
    ];
    if (pipelineRatio != null) {
      kpiChips.push({ value: pipelineRatio + "%", label: "Pipeline / open" });
    }

    var phaseCounts = {};
    nearbyPipeline.forEach(function (h) {
      var phase = String(h.projectPhase || "Pipeline").trim();
      phaseCounts[phase] = (phaseCounts[phase] || 0) + 1;
    });

    return (
      renderMetricChips(kpiChips) +
      buildAreaHotelsFilterPills(
        "Project phase",
        "project-phase",
        phaseCounts,
        nearbyPipeline.length
      ) +
      '<ul class="hdp-context-list" data-area-hotels-list>' +
        nearbyPipeline
          .map(function (h) {
            return renderPipelineHotelItem(h, { areaHotelsList: true });
          })
          .join("") +
      "</ul>"
    );
  }

  function renderAreaHotelsTabPanel(hotel, scope) {
    scope = scope || {};
    var coordsMissing = !hotelCoords(hotel);
    var existingBody = coordsMissing
      ? '<p class="hdp-empty">Location coordinates are not available for area hotel search.</p>'
      : renderAreaHotelsExistingColumn(scope.existing || [], scope);
    var pipelineBody = coordsMissing
      ? '<p class="hdp-empty">Location coordinates are not available for pipeline search.</p>'
      : renderAreaHotelsPipelineColumn(scope.pipeline || [], scope.existing || [], scope);

    return (
      '<div class="hdp-area-hotels-tab">' +
        renderDistancePrefsControlHtml() +
        '<div class="hdp-detail-row">' +
          '<section class="hdp-section hdp-area-hotels-existing">' +
            '<h3 class="hdp-section__title">Existing Hotels</h3>' +
            renderAreaHotelsScopeLine(scope) +
            existingBody +
          "</section>" +
          '<section class="hdp-section hdp-area-hotels-pipeline">' +
            '<h3 class="hdp-section__title">Pipeline</h3>' +
            renderAreaHotelsScopeLine(scope) +
            pipelineBody +
          "</section>" +
        "</div>" +
      "</div>"
    );
  }

  function bindFilterPills(container) {
    if (!container) return;
    container.querySelectorAll(".hdp-context-filters, .hdp-area-hotels-filters").forEach(function (group) {
      if (group.dataset.filterBound === "1") return;
      group.dataset.filterBound = "1";

      var list = group.parentElement
        ? group.parentElement.querySelector("[data-filter-list], [data-area-hotels-list]")
        : null;
      if (!list) return;

      group.addEventListener("click", function (event) {
        var pill = event.target.closest(".hdp-filter-pill");
        if (!pill || !group.contains(pill)) return;

        group.querySelectorAll(".hdp-filter-pill").forEach(function (p) {
          p.classList.remove("is-active");
        });
        pill.classList.add("is-active");

        var value = pill.getAttribute("data-filter-value");
        var filterGroup = group.getAttribute("data-filter-group");
        var items = list.querySelectorAll("[data-filter-list-item], .hdp-area-hotels-list__item");
        items.forEach(function (item) {
          if (value === "__all__") {
            item.hidden = false;
            return;
          }
          if (filterGroup === "chain-scale") {
            item.hidden = item.getAttribute("data-chain-scale") !== value;
            return;
          }
          if (filterGroup === "project-phase") {
            item.hidden = item.getAttribute("data-project-phase") !== value;
            return;
          }
          item.hidden = item.getAttribute("data-filter-category") !== value;
        });
      });
    });
  }

  function renderDemandDriversPanel(hotel, items, nearbyApi, scope) {
    scope = scope || getPropertyContextScope(hotel);
    var radiusKm = scope.radiusKm;
    var scopeLabel = formatContextScopeLabel(scope);
    var bodyHtml;
    if (!items.length) {
      bodyHtml =
        '<p class="hdp-empty">No demand drivers within ' +
        esc(scopeLabel || formatRadiusLabel(radiusKm)) +
        " of this property.</p>";
    } else {
      var typeCounts = buildContextTypeCounts(items, getContextCategoryKey);
      var typeCount = Object.keys(typeCounts).length;
      var nearest =
        items[0] && items[0].distanceKm != null ? formatDistanceKm(items[0].distanceKm) : "—";
      var kpiChips = [
        { value: items.length, label: "Drivers nearby" },
        { value: typeCount, label: "Categories" },
        { value: nearest, label: "Nearest" },
        { value: formatRadiusLabel(radiusKm), label: "Search radius" },
      ];

      bodyHtml =
        '<div class="hdp-context-filterable-panel">' +
          renderContextScopeLine(scope, "of this property") +
          renderMetricChips(kpiChips) +
          buildAreaHotelsFilterPills("Driver category", "category", typeCounts, items.length) +
          '<ul class="hdp-context-list" data-filter-list>' +
            items
              .map(function (item) {
                return renderContextListItem(item, {
                  colorFn: nearbyApi.getAnchorColor,
                  filterList: true,
                  categoryKey: getContextCategoryKey(item),
                });
              })
              .join("") +
          "</ul>" +
        "</div>";
    }

    return renderDemandDriversTabPanel(hotel, bodyHtml);
  }

  function renderAccessItem(item, infraApi, options) {
    options = options || {};
    var typeLabel = item.pointSubtype || item.pointType || item.mapIconType || item.type || "Point";
    var meta = [];
    if (item.distanceKm != null) {
      meta.push(formatDistanceKm(item.distanceKm) + " straight-line");
      var verified = verifiedTravelTimeLabel(item);
      if (verified) meta.push(verified);
    }
    if (item.outsideSearchRadius) {
      meta.push("beyond search radius");
    }
    var locationBits = [item.city, item.country].filter(Boolean).join(", ");
    if (locationBits) meta.push(locationBits);
    var filterAttrs = "";
    if (options.filterList) {
      var category = options.categoryKey || getInfraCategoryKey(item);
      filterAttrs =
        ' class="hdp-context-list__item hdp-filter-list__item" data-filter-list-item data-filter-category="' +
        esc(category) +
        '"';
    } else {
      filterAttrs = ' class="hdp-context-list__item"';
    }
    var colorFn =
      infraApi && typeof infraApi.getInfraColor === "function"
        ? infraApi.getInfraColor
        : function () {
            return "#6c72ff";
          };

    return (
      "<li" + filterAttrs + ">" +
        '<span class="hdp-context-list__dot" style="background:' + esc(colorFn(item)) + '"></span>' +
        '<div class="hdp-context-list__body">' +
          '<div class="hdp-context-list__name">' + esc(item.name || "Unknown") + "</div>" +
          '<div class="hdp-context-list__type">' + esc(typeLabel) + "</div>" +
          (meta.length ? '<div class="hdp-context-list__meta">' + esc(meta.join(" · ")) + "</div>" : "") +
          (item.hotelDemandRationale
            ? '<div class="hdp-context-list__rationale">' + esc(item.hotelDemandRationale) + "</div>"
            : "") +
        "</div>" +
      "</li>"
    );
  }

  function renderAccessConnectivityPanel(items, infraApi, scope, hotel) {
    scope = scope || (hotel ? getPropertyContextScope(hotel) : {});
    var radiusKm = scope.radiusKm;
    var scopeLabel = formatContextScopeLabel(scope);
    if (!items.length) {
      return (
        '<div class="hdp-access-connectivity-tab">' +
          renderDistancePrefsControlHtml() +
          '<p class="hdp-empty">No airports or connectivity points within ' +
          esc(scopeLabel || formatRadiusLabel(radiusKm)) +
          ".</p>" +
        "</div>"
      );
    }

    var typeCounts = buildContextTypeCounts(items, getInfraCategoryKey);
    var typeCount = Object.keys(typeCounts).length;
    var airportCount = items.filter(isAirportInfraItem).length;
    var inRadiusCount = items.filter(function (item) {
      return !item.outsideSearchRadius;
    }).length;
    var kpiChips = [
      { value: inRadiusCount, label: "Points nearby" },
      { value: typeCount, label: "Types" },
      { value: airportCount, label: "Airports" },
      { value: formatRadiusLabel(radiusKm), label: "Search radius" },
    ];

    return (
      '<div class="hdp-access-connectivity-tab hdp-context-filterable-panel">' +
        renderDistancePrefsControlHtml() +
        renderContextScopeLine(
          scope,
          "Distances are straight-line (mi/km). Drive times appear only when verified in Airtable — never invented from distance. Closest " +
            CONTEXT_CONFIG.minAirports +
            " airports always included when available."
        ) +
        renderMetricChips(kpiChips) +
        buildAreaHotelsFilterPills(
          "Infrastructure type",
          "category",
          typeCounts,
          items.length,
          sortInfraCategoryKeys
        ) +
        '<ul class="hdp-context-list" data-filter-list>' +
          items
            .map(function (item) {
              return renderAccessItem(item, infraApi, {
                filterList: true,
                categoryKey: getInfraCategoryKey(item),
              });
            })
            .join("") +
        "</ul>" +
      "</div>"
    );
  }

  function paintSubmarketFromPool(hotel, censusHotels, report, source, poolMeta) {
    poolMeta = poolMeta || {};
    var RBP = window.RadarBrandedPenetration;
    var mapFilters =
      RBP && typeof RBP.resolveMapFilters === "function" ? RBP.resolveMapFilters() : {};
    var hpc = poolMeta.hpc || null;
    var submarketHotels;

    if (source === "hpc" && hpc && Array.isArray(hpc.hotels) && hpc.hotels.length) {
      // HPC cohort is authoritative for this tab; do not re-bucket away the selected hotel.
      submarketHotels = dedupeHotelsByIdentity(hpc.hotels);
      if (hotel && hotel.market == null && hpc.selectedHotel && hpc.selectedHotel.market) {
        hotel.market = hpc.selectedHotel.market;
      }
      if (hotel && hotel.submarket == null && hpc.selectedHotel && hpc.selectedHotel.submarket) {
        hotel.submarket = hpc.selectedHotel.submarket;
      }
      if (hotel && !hotel.city && hpc.selectedHotel && hpc.selectedHotel.city) {
        hotel.city = hpc.selectedHotel.city;
      }
      if (hotel && !hotel.country && hpc.selectedHotel && hpc.selectedHotel.country) {
        hotel.country = hpc.selectedHotel.country;
      }
    } else {
      submarketHotels =
        RBP && typeof RBP.hotelsInGeographyBucket === "function"
          ? RBP.hotelsInGeographyBucket(hotel, censusHotels)
          : filterHotelsBySubmarket(hotel, censusHotels);
      submarketHotels = ensureSelectedHotelInPool(hotel, submarketHotels);
    }
    submarketHotels = dedupeHotelsByIdentity(submarketHotels);

    var submarketMetrics =
      source === "hpc" && poolMeta.metrics
        ? Object.assign({}, computeSubmarketMetrics(submarketHotels), poolMeta.metrics)
        : computeSubmarketMetrics(submarketHotels);
    var submarketBreakdowns = computeSubmarketBreakdowns(submarketHotels);
    var penetrationPct = computeSubmarketPenetration(hotel, censusHotels, mapFilters);
    var pipelineMetrics = computeSubmarketPipelinePressure(hotel, censusHotels, mapFilters);
    var coverageNote = "";

    if (source === "hpc") {
      var cov = poolMeta.coverage || (hpc && hpc.coverage) || {};
      var grouping = poolMeta.grouping || (hpc && hpc.grouping) || {};
      coverageNote =
        '<p class="hdp-snapshot-meta" data-hdp-submarket-source="hpc">Source: Hotel Property Census (canonical). Scout/Radar are supporting only — empty Scout does not hide HPC hotels.</p>';
      if (cov.label) {
        coverageNote +=
          '<p class="hdp-snapshot-meta" data-hdp-submarket-coverage="1">' + esc(cov.label) + "</p>";
      }
      if (grouping.researchNeeded || grouping.mode === "selected_only") {
        coverageNote +=
          '<p class="hdp-snapshot-meta">Market/submarket research flagged — geography was not invented to fill the table.</p>';
      }
      if (poolMeta.supportSource && poolMeta.supportSource !== "none") {
        coverageNote +=
          '<p class="hdp-snapshot-meta">Supporting pool: ' +
          esc(String(poolMeta.supportSource)) +
          " (merged after HPC; duplicates removed).</p>";
      }
    } else if (!submarketHotels.length && report && report.success) {
      var metrics = report.metrics || {};
      var breakdowns = normalizeCoverageBreakdowns(report.breakdowns || {});
      var openHotels = fpMetricNum(metrics.openHotels);
      var openRooms = fpMetricNum(metrics.openRooms);
      var pipelineHotels = fpMetricNum(metrics.pipelineHotels);
      var pipelineRooms = fpMetricNum(metrics.pipelineRooms);
      var totalHotels = fpMetricNum(metrics.totalHotels) || openHotels + pipelineHotels;
      var totalRooms =
        fpMetricNum(metrics.totalRooms) ||
        openRooms + pipelineRooms + fpMetricNum(metrics.candidateRooms);
      var sampleHotels = mapCensusSampleToHotels(report.recordsSample || []);
      submarketHotels = ensureSelectedHotelInPool(hotel, sampleHotels);
      if (submarketHotels.length) {
        submarketMetrics = computeSubmarketMetrics(submarketHotels);
        submarketBreakdowns = computeSubmarketBreakdowns(submarketHotels);
      } else {
        submarketMetrics = {
          totalHotels: totalHotels,
          openHotels: openHotels,
          openRooms: openRooms,
          pipelineHotels: pipelineHotels,
          pipelineRooms: pipelineRooms,
          totalRooms: totalRooms,
          brandCount: metrics.brandCount,
          parentCompanyCount: metrics.parentCompanyCount,
        };
        submarketBreakdowns = breakdowns;
      }
      coverageNote =
        '<p class="hdp-snapshot-meta">Source: Hotel Census via Scout (read-only). HPC snapshot unavailable this session. Incomplete coverage is not treated as a complete market census.</p>';
    } else if (source === "scout_census" && submarketHotels.length) {
      coverageNote =
        '<p class="hdp-snapshot-meta">Source: Hotel Census sample (read-only). Sample size may be capped — not a full market census.</p>';
    }

    var coverageMeta = poolMeta.coverage || (hpc && hpc.coverage) || null;
    var status =
      submarketHotels.length || fpMetricNum(submarketMetrics.totalHotels) > 0
        ? submarketHotels.length
          ? coverageMeta && coverageMeta.partial
            ? "partial"
            : "success"
          : "partial"
        : "empty";

    setTabPanel(
      "submarket-snapshot",
      renderSubmarketSnapshotPanel(
        hotel,
        submarketHotels,
        submarketMetrics,
        submarketBreakdowns,
        penetrationPct,
        pipelineMetrics,
        {
          source: source,
          coverage: coverageMeta,
          grouping: poolMeta.grouping || (hpc && hpc.grouping) || null,
          cohorts: (hpc && hpc.cohorts) || poolMeta.cohorts || null,
        }
      ) + coverageNote,
      status
    );
  }

  function loadMarketIntelTabs(hotel) {
    setTabPanel(
      "submarket-snapshot",
      '<p class="hdp-empty hdp-context-state">Loading Hotel Property Census submarket intelligence…</p>',
      "loading"
    );

    resolveCensusHotelPool(hotel).then(function (pool) {
      if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
      try {
        paintSubmarketFromPool(hotel, pool.hotels || [], pool.report || null, pool.source, pool);
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[hdp] submarket snapshot render failed", err);
        }
        setTabPanel(
          "submarket-snapshot",
          '<p class="hdp-empty">Could not render Submarket Snapshot.</p>',
          "error"
        );
      }
    });
  }

  function hotelRecordIdForContext(hotel) {
    return String((hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "").trim();
  }

  /**
   * Hydrate lat/lng/country/market from canonical HPC when the open hotel payload is thin
   * (common in Founder Preview). Never invents coordinates.
   */
  function hydrateHotelFromHpc(hotel) {
    var id = hotelRecordIdForContext(hotel);
    if (!/^rec[A-Za-z0-9]{14}$/.test(id)) {
      return Promise.resolve(hotel);
    }
    if (hotelCoords(hotel) && hotel.country && hotel.country !== "Unknown Country") {
      return Promise.resolve(hotel);
    }
    return fetch("/api/brand-presence/hotel/" + encodeURIComponent(id), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (res) {
        return res.json();
      })
      .then(function (body) {
        var h = body && (body.hotel || body.data || body);
        if (!h || typeof h !== "object") return hotel;
        var next = Object.assign({}, hotel);
        ["lat", "lng", "latitude", "longitude", "city", "country", "region", "market", "submarket", "locationType", "brand", "name", "rooms", "chainScale", "status"].forEach(
          function (k) {
            if ((next[k] == null || next[k] === "" || next[k] === "Unknown Country" || next[k] === "Unknown City") && h[k] != null && h[k] !== "") {
              next[k] = h[k];
            }
          }
        );
        if (next.lat == null && h.lat != null) next.lat = h.lat;
        if (next.lng == null && h.lng != null) next.lng = h.lng;
        return next;
      })
      .catch(function () {
        return hotel;
      });
  }

  function fetchHeDemandDrivers(hotel, contextScope) {
    var id = hotelRecordIdForContext(hotel);
    var params = [];
    if (contextScope && contextScope.radiusKm != null) {
      params.push("radiusKm=" + encodeURIComponent(String(contextScope.radiusKm)));
    }
    params.push("limit=" + encodeURIComponent(String(CONTEXT_CONFIG.demandDriversLimit)));
    var url =
      "/api/hotel-explorer/hotels/" +
      encodeURIComponent(id) +
      "/demand-drivers" +
      (params.length ? "?" + params.join("&") : "");
    return fetch(url, { headers: { "ngrok-skip-browser-warning": "true" } }).then(function (res) {
      return res.json();
    });
  }

  function fetchHeAccessConnectivity(hotel, contextScope) {
    var id = hotelRecordIdForContext(hotel);
    var params = [];
    if (contextScope && contextScope.radiusKm != null) {
      params.push("radiusKm=" + encodeURIComponent(String(contextScope.radiusKm)));
    }
    params.push("limit=" + encodeURIComponent(String(CONTEXT_CONFIG.infraLimit * 2)));
    params.push("minAirports=" + encodeURIComponent(String(CONTEXT_CONFIG.minAirports)));
    var url =
      "/api/hotel-explorer/hotels/" +
      encodeURIComponent(id) +
      "/access-connectivity" +
      (params.length ? "?" + params.join("&") : "");
    return fetch(url, { headers: { "ngrok-skip-browser-warning": "true" } }).then(function (res) {
      return res.json();
    });
  }

  function mergeHotelFromHePayload(hotel, payloadHotel) {
    if (!payloadHotel || typeof payloadHotel !== "object") return hotel;
    var next = Object.assign({}, hotel);
    ["lat", "lng", "city", "country", "region", "market", "submarket", "locationType", "name", "brand"].forEach(
      function (k) {
        if ((next[k] == null || next[k] === "") && payloadHotel[k] != null && payloadHotel[k] !== "") {
          next[k] = payloadHotel[k];
        }
      }
    );
    return next;
  }

  function loadDemandAnchorTabs(hotel, contextScope) {
    var nearbyApi = window.DemandAnchorsRadar;
    contextScope = contextScope || getPropertyContextScope(hotel);
    var id = hotelRecordIdForContext(hotel);

    setTabPanel(
      "demand-drivers",
      renderDemandDriversTabPanel(
        hotel,
        '<p class="hdp-empty hdp-context-state" data-he-async="demand-drivers">Loading demand drivers…</p>'
      ),
      "loading"
    );

    var loadPromise =
      /^rec[A-Za-z0-9]{14}$/.test(id)
        ? fetchHeDemandDrivers(hotel, contextScope).then(function (payload) {
            if (payload && payload.success !== false && payload.ok !== false && Array.isArray(payload.items)) {
              return {
                mode: "he_api",
                hotel: mergeHotelFromHePayload(hotel, payload.hotel),
                drivers: payload.items,
                emptyMessage: payload.emptyMessage || null,
                sourceNote:
                  "Source: Demand Anchors (Airtable) · HPC hotel " +
                  id +
                  ". Straight-line distance only. Entities are reusable across hotels.",
              };
            }
            return null;
          })
        : Promise.resolve(null);

    loadPromise
      .then(function (heResult) {
        if (heResult) return heResult;
        // Fallback: legacy Radar Demand Anchors client (same Airtable store).
        if (!nearbyApi) {
          return {
            mode: "error",
            hotel: hotel,
            drivers: [],
            emptyMessage: "Demand Anchors modules are not loaded.",
          };
        }
        return nearbyApi.fetchDemandAnchors(buildDemandAnchorsFetchOpts(hotel)).then(function (data) {
          var allItems = nearbyApi.parseItems(data);
          var drivers = filterAndSortByDistance(allItems, hotel, {
            radiusKm: contextScope.radiusKm,
            limit: CONTEXT_CONFIG.demandDriversLimit,
          });
          return {
            mode: "legacy_radar",
            hotel: hotel,
            drivers: drivers,
            emptyMessage: drivers.length
              ? null
              : "No Demand Anchors within " +
                formatContextScopeLabel(contextScope) +
                ". Tab will populate when canonical Demand Anchors exist for this geography.",
            sourceNote: "Source: Demand Anchors via Radar API (read-only).",
          };
        });
      })
      .then(function (result) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (!result) return;
        if (result.mode === "error") {
          setTabPanel(
            "demand-drivers",
            renderDemandDriversTabPanel(result.hotel, '<p class="hdp-empty">' + esc(result.emptyMessage) + "</p>"),
            "error"
          );
          return;
        }
        var html = renderDemandDriversPanel(
          result.hotel,
          result.drivers,
          nearbyApi || {
            getAnchorColor: function () {
              return "#6c72ff";
            },
            getDemandTypeLabel: function (item) {
              return item.pointType || item.type || "Demand Anchor";
            },
          },
          contextScope
        );
        if (result.sourceNote) {
          html += '<p class="hdp-snapshot-meta" data-hdp-demand-source="1">' + esc(result.sourceNote) + "</p>";
        }
        if (!result.drivers.length && result.emptyMessage) {
          html = renderDemandDriversTabPanel(
            result.hotel,
            '<p class="hdp-empty">' +
              esc(result.emptyMessage) +
              "</p>" +
              (result.sourceNote
                ? '<p class="hdp-snapshot-meta">' + esc(result.sourceNote) + "</p>"
                : "")
          );
        }
        setTabPanel("demand-drivers", html, result.drivers.length ? "success" : "empty");
      })
      .catch(function (err) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[HotelDetailPanel] demand drivers fetch failed:", err);
        }
        setTabPanel(
          "demand-drivers",
          renderDemandDriversTabPanel(hotel, '<p class="hdp-empty">Could not load demand drivers.</p>'),
          "error"
        );
      });
  }

  function loadAccessConnectivityTab(hotel, contextScope) {
    var infraApi = window.TravelInfrastructureRadar;
    contextScope = contextScope || getPropertyContextScope(hotel);
    var id = hotelRecordIdForContext(hotel);

    setTabPanel(
      "access-connectivity",
      '<p class="hdp-empty hdp-context-state" data-he-async="access-connectivity">Loading access and connectivity…</p>',
      "loading"
    );

    var loadPromise =
      /^rec[A-Za-z0-9]{14}$/.test(id)
        ? fetchHeAccessConnectivity(hotel, contextScope).then(function (payload) {
            if (payload && payload.success !== false && payload.ok !== false && Array.isArray(payload.items)) {
              return {
                mode: "he_api",
                hotel: mergeHotelFromHePayload(hotel, payload.hotel),
                items: payload.items,
                emptyMessage: payload.emptyMessage || null,
                sourceNote:
                  "Source: Travel Infrastructure Data (Airtable) · HPC hotel " +
                  id +
                  ". Distances are straight-line (mi/km toggle). Drive times are shown only when verified in Airtable — not invented from distance.",
              };
            }
            return null;
          })
        : Promise.resolve(null);

    loadPromise
      .then(function (heResult) {
        if (heResult) return heResult;
        if (!infraApi) {
          return {
            mode: "error",
            items: [],
            emptyMessage: "Travel infrastructure modules are not loaded.",
          };
        }
        return infraApi.fetchInfrastructure(buildInfrastructureFetchOpts(hotel)).then(function (data) {
          var parsed = infraApi.parseItems(data);
          var allWithDistance = enrichItemsWithDistance(parsed, hotel);
          var items = filterAndSortByDistance(parsed, hotel, {
            radiusKm: contextScope.radiusKm,
            limit: CONTEXT_CONFIG.infraLimit * 2,
            typePriority: accessInfraTypePriority,
          });
          items = mergeClosestAirportsIntoAccessItems(
            items,
            allWithDistance,
            contextScope.radiusKm,
            CONTEXT_CONFIG.minAirports
          );
          return {
            mode: "legacy_radar",
            hotel: hotel,
            items: items,
            emptyMessage: items.length
              ? null
              : "No airports or connectivity points within " + formatContextScopeLabel(contextScope) + ".",
            sourceNote: "Source: Travel Infrastructure Data via Radar API (read-only).",
          };
        });
      })
      .then(function (result) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (!result) return;
        if (result.mode === "error") {
          setTabPanel(
            "access-connectivity",
            '<p class="hdp-empty">' + esc(result.emptyMessage) + "</p>",
            "error"
          );
          return;
        }
        var html = result.items.length
          ? renderAccessConnectivityPanel(
              result.items,
              infraApi || {
                getInfraColor: function () {
                  return "#6c72ff";
                },
              },
              contextScope,
              result.hotel || hotel
            )
          : '<p class="hdp-empty">' +
            esc(
              result.emptyMessage ||
                "No airports or connectivity points within " + formatContextScopeLabel(contextScope) + "."
            ) +
            "</p>";
        if (result.sourceNote) {
          html += '<p class="hdp-snapshot-meta" data-hdp-access-source="1">' + esc(result.sourceNote) + "</p>";
        }
        setTabPanel("access-connectivity", html, result.items.length ? "success" : "empty");
        bindDistancePrefsControls(panelEl && panelEl.querySelector('[data-panel="access-connectivity"]'));
      })
      .catch(function (err) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[HotelDetailPanel] access connectivity fetch failed:", err);
        }
        setTabPanel("access-connectivity", '<p class="hdp-empty">Could not load access and connectivity.</p>', "error");
      });
  }

  function renderCompetitiveHotelItem(hotel, options) {
    options = options || {};
    var brandLabel = isIndependentHotel(hotel)
      ? "Independent"
      : displayValue(hotel.brand, "Unknown Brand");
    var meta = [];
    if (hotel.distanceKm != null) meta.push(formatDistanceKm(hotel.distanceKm));
    meta.push(normalizeChainScale(hotel));
    if (hotel.rooms) meta.push(formatNumber(hotel.rooms) + " rooms");
    if (hotel.status) meta.push(hotel.status);

    var scaleKey = normalizeChainScale(hotel);
    var extraClass = options.areaHotelsList
      ? " hdp-competitive-list__item hdp-area-hotels-list__item"
      : " hdp-competitive-list__item";
    var dataAttrs = options.areaHotelsList
      ? ' data-chain-scale="' + esc(scaleKey) + '"'
      : "";

    return (
      '<li class="hdp-context-list__item' + extraClass + '"' + dataAttrs + ">" +
        '<span class="hdp-context-list__dot" style="background:#6c72ff"></span>' +
        '<div class="hdp-context-list__body">' +
          '<div class="hdp-context-list__name">' + esc(displayValue(hotel.name, "Hotel")) + "</div>" +
          '<div class="hdp-context-list__type">' + esc(brandLabel) + "</div>" +
          '<div class="hdp-context-list__meta">' + esc(meta.join(" · ")) + "</div>" +
          (hotel.parentCompany && !isIndependentHotel(hotel)
            ? '<div class="hdp-context-list__rationale">' + esc(formatParentCompanyDisplay(hotel.parentCompany)) + "</div>"
            : "") +
        "</div>" +
      "</li>"
    );
  }

  function normalizeCensusHotelRow(h) {
    if (!h || typeof h !== "object") return null;
    var lat = Number(h.lat != null ? h.lat : h.latitude);
    var lng = Number(h.lng != null ? h.lng : h.longitude);
    return Object.assign({}, h, {
      id: h.id || h.recordId || "",
      lat: Number.isFinite(lat) ? lat : 0,
      lng: Number.isFinite(lng) ? lng : 0,
      latitude: Number.isFinite(lat) ? lat : h.latitude,
      longitude: Number.isFinite(lng) ? lng : h.longitude,
    });
  }

  /**
   * Area Hotels needs a census pool. On Radar that is window.allHotels.
   * Outside Radar (e.g. golden demo), fetch a market/city slice from Brand Presence.
   *
   * Prefer city over Dealality market labels: many markets (e.g. "Pacific Central")
   * do not match name/city/country SEARCH and would return an empty set.
   */
  function areaHotelsCensusQueries(hotel) {
    var out = [];
    var seen = {};
    function push(raw) {
      var q = String(raw || "").trim();
      if (!q) return;
      if (/^unknown(\s|$)/i.test(q)) return;
      var key = q.toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push(q);
    }
    push(hotel && hotel.city);
    push(hotel && hotel.market);
    push(hotel && hotel.country);
    return out;
  }

  function fetchAreaHotelsCensusByQuery(q) {
    var url =
      "/api/brand-presence?search=" +
      encodeURIComponent(q) +
      "&limit=400";
    return fetch(url, { headers: { "ngrok-skip-browser-warning": "true" } })
      .then(function (r) {
        if (!r.ok) throw new Error("brand-presence HTTP " + r.status);
        return r.json();
      })
      .then(function (payload) {
        return ((payload && payload.hotels) || [])
          .map(normalizeCensusHotelRow)
          .filter(Boolean);
      });
  }

  function ensureAreaHotelsCensus(hotel) {
    var existing = dedupeHotelsByIdentity(window.allHotels || []);
    if (existing.length) return Promise.resolve(existing);

    var cacheKey = areaHotelsCensusCacheKey(hotel);
    if (
      window.__hexAreaHotelsCensusCache &&
      window.__hexAreaHotelsCensusCacheKey === cacheKey &&
      window.__hexAreaHotelsCensusCache.length
    ) {
      return Promise.resolve(window.__hexAreaHotelsCensusCache);
    }

    // HE 5.0: Area Hotels is HPC-native via Brand Presence. Do not query legacy Hotel Census.
    // Prefer HPC submarket snapshot hotels (coords required), then Brand Presence search.
    return fetchHpcSubmarketSnapshot(hotel)
      .then(function (hpcResult) {
        var hpcBody = hpcResult && hpcResult.ok ? hpcResult.body : null;
        var hpcHotels = ((hpcBody && hpcBody.hotels) || [])
          .map(normalizeCensusHotelRow)
          .filter(function (h) {
            return hotelCoords(h);
          });
        var withSelf = ensureSelectedHotelInPool(hotel, hpcHotels);
        if (withSelf.length > 1) {
          window.__hexAreaHotelsCensusCache = withSelf;
          window.__hexAreaHotelsCensusCacheKey = cacheKey;
          if (!(window.allHotels && window.allHotels.length)) {
            window.allHotels = withSelf;
          }
          return withSelf;
        }

        var queries = areaHotelsCensusQueries(hotel);
        if (!queries.length) return withSelf.length ? withSelf : [];

        function tryNext(i) {
          if (i >= queries.length) return Promise.resolve(withSelf.length ? withSelf : []);
          return fetchAreaHotelsCensusByQuery(queries[i]).then(function (rows) {
            var deduped = dedupeHotelsByIdentity(
              ensureSelectedHotelInPool(hotel, rows || [])
            ).filter(function (h) {
              return hotelCoords(h);
            });
            if (deduped.length > 1 || (deduped.length === 1 && !withSelf.length)) {
              window.__hexAreaHotelsCensusCache = deduped;
              window.__hexAreaHotelsCensusCacheKey = cacheKey;
              if (!(window.allHotels && window.allHotels.length)) {
                window.allHotels = deduped;
              }
              return deduped;
            }
            return tryNext(i + 1);
          });
        }
        return tryNext(0);
      })
      .catch(function (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[HotelDetailPanel] area hotels HPC pool failed:", err);
        }
        return [];
      });
  }

  function areaHotelsCensusCacheKey(hotel) {
    if (!hotel) return "";
    return [
      hotel.id || hotel.recordId || "",
      hotel.city || "",
      hotel.market || "",
      hotel.country || "",
    ].join("|");
  }

  function loadAreaHotelsTab(hotel) {
    if (!panelEl || !hotel) return;
    var panel = panelEl.querySelector('[data-panel="area-hotels"]');
    if (!panel) return;

    panel.dataset.state = "loading";
    panel.innerHTML =
      '<div class="hdp-area-hotels-tab">' +
      renderDistancePrefsControlHtml() +
      '<p class="hdp-empty hdp-context-state">Loading area hotels…</p></div>';
    bindDistancePrefsControls(panel);

    ensureAreaHotelsCensus(hotel).then(function (allHotels) {
      if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
      if (!allHotels.length) {
        panel.dataset.state = "empty";
        panel.innerHTML =
          '<div class="hdp-area-hotels-tab">' +
          renderDistancePrefsControlHtml() +
          '<p class="hdp-empty">No nearby hotels found for this city / market yet.</p></div>';
        bindDistancePrefsControls(panel);
        return;
      }

      var scope = hotelCoords(hotel)
        ? resolveAreaHotelsScope(hotel, allHotels)
        : {
            mode: "radius",
            radiusKm: getAreaHotelsBaseRadiusKm(hotel),
            baseRadiusKm: getAreaHotelsBaseRadiusKm(hotel),
            expanded: false,
            locationType: normalizeAreaHotelsLocationType(hotel),
            corridor: resolveEffectiveSubmarket(hotel),
            note: "",
            existing: [],
            pipeline: [],
          };

      var hasContent = scope.existing.length > 0 || scope.pipeline.length > 0;
      panel.dataset.state = hasContent ? "success" : "empty";
      panel.innerHTML = renderAreaHotelsTabPanel(hotel, scope);
      bindFilterPills(panel);
      bindDistancePrefsControls(panel);
    });
  }

  function loadContextTabData(hotel) {
    if (!panelEl || !hotel) return;

    loadMarketIntelTabs(hotel);

    ensureAreaHotelsCensus(hotel).then(function () {
      if (!isOpen || !isSameHotel(currentHotel, hotel)) return;

      loadAreaHotelsTab(hotel);

      return hydrateHotelFromHpc(hotel).then(function (hydrated) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (hydrated && hydrated !== hotel) {
          currentHotel = Object.assign({}, currentHotel || {}, hydrated);
          hotel = currentHotel;
        }
        var contextScope = getPropertyContextScope(hotel);
        // HE Demand / Access APIs resolve HPC coords server-side; still gate legacy path.
        loadDemandAnchorTabs(hotel, contextScope);
        loadAccessConnectivityTab(hotel, contextScope);
      });
    });
  }

  function bindPanelTabs(hotel) {
    if (!panelEl) return;
    var nav = panelEl.querySelector(".hdp-tabs-section");
    if (!nav) return;

    function activate(tabId) {
      nav.querySelectorAll(".section-nav-item").forEach(function (tab) {
        var isActive = tab.getAttribute("data-tab") === tabId;
        tab.classList.toggle("active", isActive);
        tab.setAttribute("aria-selected", isActive ? "true" : "false");
      });
      panelEl.querySelectorAll(".hdp-tab-panels .tab-panel").forEach(function (panel) {
        var isActive = panel.getAttribute("data-panel") === tabId;
        panel.classList.toggle("active", isActive);
      });
    }

    nav.addEventListener("click", function (event) {
      var tab = event.target.closest(".section-nav-item[data-tab]");
      if (!tab || !nav.contains(tab)) return;
      activate(tab.getAttribute("data-tab"));
    });

    activate("overview");
    loadContextTabData(hotel);
  }

  function bindAmenitiesCarousel() {
    if (!panelEl) return;
    var wrap = panelEl.querySelector(".hdp-amenities-wrap");
    if (!wrap) return;
    var track = wrap.querySelector(".hdp-amenities-carousel");
    var prevBtn = wrap.querySelector(".hdp-amenities-nav--prev");
    var nextBtn = wrap.querySelector(".hdp-amenities-nav--next");
    if (!track) return;

    function scrollByCards(direction) {
      var card = track.querySelector(".hdp-amenity-card");
      var step = card ? Math.max(card.offsetWidth * 3, 200) : 240;
      track.scrollBy({ left: direction * step, behavior: "smooth" });
    }

    function updateNav() {
      var overflow = track.scrollWidth > track.clientWidth + 4;
      var atStart = track.scrollLeft <= 2;
      var atEnd = track.scrollLeft + track.clientWidth >= track.scrollWidth - 2;
      if (prevBtn) {
        if (!overflow || atStart) prevBtn.setAttribute("hidden", "");
        else prevBtn.removeAttribute("hidden");
        prevBtn.disabled = !overflow || atStart;
      }
      if (nextBtn) {
        if (!overflow || atEnd) nextBtn.setAttribute("hidden", "");
        else nextBtn.removeAttribute("hidden");
        nextBtn.disabled = !overflow || atEnd;
      }
    }

    if (prevBtn && !prevBtn._hdpBound) {
      prevBtn._hdpBound = true;
      prevBtn.addEventListener("click", function () {
        scrollByCards(-1);
      });
    }
    if (nextBtn && !nextBtn._hdpBound) {
      nextBtn._hdpBound = true;
      nextBtn.addEventListener("click", function () {
        scrollByCards(1);
      });
    }
    if (!track._hdpBound) {
      track._hdpBound = true;
      track.addEventListener("scroll", updateNav, { passive: true });
      track.addEventListener("keydown", function (ev) {
        if (ev.key === "ArrowLeft") {
          ev.preventDefault();
          scrollByCards(-1);
        } else if (ev.key === "ArrowRight") {
          ev.preventDefault();
          scrollByCards(1);
        }
      });
      if (typeof ResizeObserver !== "undefined") {
        var ro = new ResizeObserver(updateNav);
        ro.observe(track);
      }
    }
    updateNav();
    setTimeout(updateNav, 50);
  }

  function renderPanelContent(hotel) {
    var amenities = resolveAmenitiesDisplay(hotel);
    var overviewRows = buildOverviewRows(hotel);
    var directionsUrl = buildDirectionsUrl(hotel);
    var websiteUrl = normalizeWebsiteUrl(hotel.website);
    var phoneHref = hotel.telephone ? "tel:" + String(hotel.telephone).replace(/[^\d+]/g, "") : null;
    var phoneLabel = hotel.telephone || "Phone";

    var amenitiesHtml = renderAmenitiesSection(amenities);

    var overviewHtml = overviewRows.map(function (row) {
      return (
        '<div class="hdp-overview__row">' +
          '<div class="hdp-overview__label">' + esc(row.label) + "</div>" +
          '<div class="hdp-overview__value">' + esc(row.value) + "</div>" +
        "</div>"
      );
    }).join("");

    var overviewPanelHtml =
      '<div class="hdp-overview-tab">' +
        '<div class="hdp-detail-row">' +
          '<section class="hdp-section hdp-description-section">' +
            '<h3 class="hdp-section__title">Description</h3>' +
            (resolveHeadlineText(hotel)
              ? '<p class="hdp-description hdp-description--headline">' +
                esc(resolveHeadlineText(hotel)) +
                "</p>"
              : "") +
            formatDescriptionHtml(resolveDescriptionText(hotel)) +
          "</section>" +
          '<section class="hdp-section hdp-overview-section">' +
            '<h3 class="hdp-section__title">Property Facts</h3>' +
            '<div class="hdp-overview">' + overviewHtml + "</div>" +
          "</section>" +
        "</div>" +
        '<section class="hdp-section hdp-amenities-section">' +
          '<h3 class="hdp-section__title">Amenities' +
            (amenities.length ? ' <span class="hdp-amenities-count">(' + amenities.length + ")</span>" : "") +
          "</h3>" +
          amenitiesHtml +
        "</section>" +
      "</div>";

    return (
      '<button type="button" class="hdp-panel__close" aria-label="Close hotel details">&times;</button>' +
      '<div class="hdp-panel__scroll">' +
        '<div class="hdp-header">' +
          '<div class="hdp-header__identity">' +
            '<div class="hdp-brand">' + esc(displayValue(hotel.brand, "Independent")) + "</div>" +
            '<h2 class="hdp-name">' + esc(displayValue(hotel.name, "Hotel")) + "</h2>" +
          "</div>" +
          '<div class="hdp-header__footer">' +
            '<div class="hdp-actions">' +
              renderActionLink("Directions", directionsUrl, "pin") +
              renderActionLink("Visit website", websiteUrl, "building") +
              renderActionLink(phoneLabel, phoneHref, "phone") +
            "</div>" +
            '<div class="hdp-status-pill-wrap">' +
              '<span class="hdp-status-pill ' + statusBadgeClass(hotel.status) + '">' +
                esc(displayValue(hotel.status, "Unknown")) +
              "</span>" +
              '<span class="hdp-status-pill__sublabel">Hotel Status</span>' +
            "</div>" +
          "</div>" +
        "</div>" +
        '<div class="hdp-context-band">' +
          '<nav class="tabs-section hdp-tabs-section" aria-label="Property sections">' +
            '<button type="button" class="section-nav-item active" data-tab="overview" aria-selected="true">' +
              '<div class="section-nav-icon" aria-hidden="true">' +
                '<svg viewBox="0 0 24 24"><path d="M3 9.5L12 3l9 6.5"/><path d="M5 10v10h14V10"/></svg>' +
              "</div>" +
              '<span class="section-nav-label">Hotel<br>Overview</span>' +
            "</button>" +
            '<button type="button" class="section-nav-item" data-tab="submarket-snapshot" aria-selected="false">' +
              '<div class="section-nav-icon" aria-hidden="true">' +
                '<svg viewBox="0 0 24 24"><path d="M4 19V5"/><path d="M8 17V9"/><path d="M12 17V3"/><path d="M16 17v-6"/><path d="M20 17v-9"/></svg>' +
              "</div>" +
              '<span class="section-nav-label">Submarket<br>Snapshot</span>' +
            "</button>" +
            '<button type="button" class="section-nav-item" data-tab="area-hotels" aria-selected="false">' +
              '<div class="section-nav-icon" aria-hidden="true">' +
                '<svg viewBox="0 0 24 24"><rect x="3" y="8" width="7" height="13"/><rect x="14" y="5" width="7" height="16"/><path d="M6 12h1M17 9h1"/></svg>' +
              "</div>" +
              '<span class="section-nav-label">Area<br>Hotels</span>' +
            "</button>" +
            '<button type="button" class="section-nav-item" data-tab="demand-drivers" aria-selected="false">' +
              '<div class="section-nav-icon" aria-hidden="true">' +
                '<svg viewBox="0 0 24 24"><path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>' +
              "</div>" +
              '<span class="section-nav-label">Demand<br>Drivers</span>' +
            "</button>" +
            '<button type="button" class="section-nav-item" data-tab="access-connectivity" aria-selected="false">' +
              '<div class="section-nav-icon" aria-hidden="true">' +
                '<svg viewBox="0 0 24 24"><path d="M2 16h20"/><path d="M5 16l2-8h10l2 8"/><path d="M12 8V4"/><path d="M9 4h6"/></svg>' +
              "</div>" +
              '<span class="section-nav-label">Access &amp;<br>Connectivity</span>' +
            "</button>" +
          "</nav>" +
          '<div class="hdp-tab-panels">' +
            '<section class="tab-panel active" data-panel="overview">' +
              overviewPanelHtml +
            "</section>" +
            '<section class="tab-panel" data-panel="submarket-snapshot" data-state="loading">' +
              '<p class="hdp-empty hdp-context-state">Loading submarket snapshot…</p>' +
            "</section>" +
            '<section class="tab-panel" data-panel="area-hotels" data-state="loading">' +
              '<p class="hdp-empty hdp-context-state">Loading area hotels…</p>' +
            "</section>" +
            '<section class="tab-panel" data-panel="demand-drivers" data-state="loading">' +
              '<p class="hdp-empty hdp-context-state">Loading demand drivers…</p>' +
            "</section>" +
            '<section class="tab-panel" data-panel="access-connectivity" data-state="loading">' +
              '<p class="hdp-empty hdp-context-state">Loading access and connectivity…</p>' +
            "</section>" +
          "</div>" +
        "</div>" +
      "</div>"
    );
  }

  function lockTabPanelsHeight(force) {
    if (embedMode) return;
    if (!panelEl) return;
    var panelsRoot = panelEl.querySelector(".hdp-tab-panels");
    var overviewPanel = panelEl.querySelector('[data-panel="overview"]');
    if (!panelsRoot || !overviewPanel) return;

    if (!force && lockedTabPanelsHeight != null) {
      panelsRoot.style.height = lockedTabPanelsHeight + "px";
      panelsRoot.style.minHeight = lockedTabPanelsHeight + "px";
      panelsRoot.style.maxHeight = lockedTabPanelsHeight + "px";
      return;
    }

    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        if (!panelEl || !isOpen) return;

        var headerEl = panelEl.querySelector(".hdp-header");
        var tabsEl = panelEl.querySelector(".hdp-tabs-section");
        var panelHeight = panelEl.clientHeight || 0;
        var headerHeight = headerEl ? headerEl.offsetHeight : 0;
        var tabsHeight = tabsEl ? tabsEl.offsetHeight : 0;
        var reserved = headerHeight + tabsHeight + 4;
        var available = Math.max(180, panelHeight - reserved);
        var contentHeight = overviewPanel.scrollHeight;
        var targetHeight = Math.min(contentHeight, available);

        if (lockedTabPanelsHeight != null && !force) {
          targetHeight = Math.max(targetHeight, lockedTabPanelsHeight);
        }

        if (targetHeight > 0) {
          lockedTabPanelsHeight = targetHeight;
          panelsRoot.style.height = targetHeight + "px";
          panelsRoot.style.minHeight = targetHeight + "px";
          panelsRoot.style.maxHeight = targetHeight + "px";
          panelEl.style.height = (reserved + targetHeight) + "px";
          panelEl.style.minHeight = panelEl.style.height;
        }
      });
    });
  }

  function renderLoadingContent(hotel) {
    return (
      '<button type="button" class="hdp-panel__close" aria-label="Close hotel details">&times;</button>' +
      '<div class="hdp-panel__scroll">' +
        '<div class="hdp-header" style="padding:16px;">' +
          '<p class="hdp-empty">Loading property details…</p>' +
          '<p class="hdp-loading-name">' + esc(displayValue(hotel.name, "Hotel")) + "</p>" +
        "</div>" +
      "</div>"
    );
  }

  function fetchHotelDetail(hotel) {
    var recordId = hotel && (hotel.id || hotel.recordId);
    if (!recordId || String(recordId).indexOf("rec") !== 0) {
      return Promise.resolve(hotel);
    }
    return fetch("/api/brand-presence/hotel/" + encodeURIComponent(recordId), {
      headers: { "ngrok-skip-browser-warning": "true" }
    })
      .then(function (res) { return res.json(); })
      .then(function (payload) {
        if (payload && payload.success && payload.hotel) {
          return Object.assign({}, hotel, payload.hotel);
        }
        return hotel;
      })
      .catch(function (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[HotelDetailPanel] detail fetch failed:", err);
        }
        return hotel;
      });
  }

  function mountPanelContent(html, hotel) {
    panelEl.innerHTML = html;
    var closeBtn = panelEl.querySelector(".hdp-panel__close");
    if (closeBtn) closeBtn.addEventListener("click", close);
    bindAmenitiesCarousel();
    if (hotel && panelEl.querySelector(".hdp-context-band")) {
      bindPanelTabs(hotel);
      resetLockedPanelSize();
      lockTabPanelsHeight(true);
    }
  }

  function ensureMounted() {
    if (panelEl && backdropEl) return;

    mountEl = document.body;

    backdropEl = document.createElement("div");
    backdropEl.className = "hdp-backdrop";
    backdropEl.setAttribute("aria-hidden", "true");

    panelEl = document.createElement("aside");
    panelEl.className = "hdp-panel";
    panelEl.id = "hotelDetailPanel";
    panelEl.setAttribute("role", "dialog");
    panelEl.setAttribute("aria-modal", "true");
    panelEl.setAttribute("aria-label", "Hotel details");
    panelEl.setAttribute("aria-hidden", "true");

    mountEl.appendChild(backdropEl);
    mountEl.appendChild(panelEl);

    bindPanelLayoutSync();
    syncPanelLayout();

    backdropEl.addEventListener("click", close);
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && isOpen) close();
    });
  }

  function showEmbeddedPanel(tabId) {
    if (!panelEl) return;
    var map = {
      hotel: "overview",
      overview: "overview",
      "submarket-snapshot": "submarket-snapshot",
      "area-hotels": "area-hotels",
      "demand-drivers": "demand-drivers",
      "access-connectivity": "access-connectivity",
    };
    var id = map[tabId] || tabId;
    panelEl.querySelectorAll(".hdp-tab-panels .tab-panel").forEach(function (panel) {
      var isActive = panel.getAttribute("data-panel") === id;
      panel.classList.toggle("active", isActive);
      panel.hidden = !isActive;
    });
  }

  /**
   * Mount Radar hotel tabs into a Hotel Explorer host (no small modal chrome).
   * Same data loaders as the compact panel — one source of truth.
   */
  function embed(hostEl, hotel) {
    if (!hostEl || !hotel) return Promise.resolve(null);
    embedMode = true;
    panelEl = hostEl;
    currentHotel = applyPresentation(hotel);
    isOpen = true;
    hostEl.classList.add("hdp-embed-host");
    hostEl.innerHTML =
      '<div class="hdp-tab-panels hdp-tab-panels--embed">' +
      '<section class="tab-panel active" data-panel="overview"></section>' +
      '<section class="tab-panel" data-panel="submarket-snapshot" data-state="loading">' +
      '<p class="hdp-empty hdp-context-state">Loading submarket snapshot…</p></section>' +
      '<section class="tab-panel" data-panel="area-hotels" data-state="loading">' +
      '<p class="hdp-empty hdp-context-state">Loading area hotels…</p></section>' +
      '<section class="tab-panel" data-panel="demand-drivers" data-state="loading">' +
      '<p class="hdp-empty hdp-context-state">Loading demand drivers…</p></section>' +
      '<section class="tab-panel" data-panel="access-connectivity" data-state="loading">' +
      '<p class="hdp-empty hdp-context-state">Loading access and connectivity…</p></section>' +
      "</div>";

    return fetchHotelDetail(hotel).then(function (fullHotel) {
      if (!embedMode) return null;
      currentHotel = applyPresentation(fullHotel);
      var amenities = resolveAmenitiesDisplay(currentHotel);
      var overviewRows = buildOverviewRows(currentHotel);
      var overviewHtml = overviewRows
        .map(function (row) {
          return (
            '<div class="hdp-overview__row">' +
            '<div class="hdp-overview__label">' +
            esc(row.label) +
            "</div>" +
            '<div class="hdp-overview__value">' +
            esc(row.value) +
            "</div></div>"
          );
        })
        .join("");
      var overviewPanel = hostEl.querySelector('[data-panel="overview"]');
      if (overviewPanel) {
        overviewPanel.innerHTML =
          '<div class="hdp-overview-tab">' +
          '<div class="hdp-detail-row">' +
          '<section class="hdp-section hdp-description-section">' +
          '<h3 class="hdp-section__title">Description</h3>' +
          (resolveHeadlineText(currentHotel)
            ? '<p class="hdp-description hdp-description--headline">' +
              esc(resolveHeadlineText(currentHotel)) +
              "</p>"
            : "") +
          formatDescriptionHtml(resolveDescriptionText(currentHotel)) +
          "</section>" +
          '<section class="hdp-section hdp-overview-section">' +
          '<h3 class="hdp-section__title">Property Facts</h3>' +
          '<div class="hdp-overview">' +
          overviewHtml +
          "</div></section></div>" +
          '<section class="hdp-section hdp-amenities-section">' +
          '<h3 class="hdp-section__title">Amenities' +
          (amenities.length
            ? ' <span class="hdp-amenities-count">(' + amenities.length + ")</span>"
            : "") +
          "</h3>" +
          renderAmenitiesSection(amenities) +
          "</section></div>";
        bindAmenitiesCarousel();
      }
      loadContextTabData(currentHotel);
      return currentHotel;
    });
  }

  function unembed() {
    embedMode = false;
    if (panelEl && panelEl.classList.contains("hdp-embed-host")) {
      panelEl.innerHTML = "";
      panelEl.classList.remove("hdp-embed-host");
    }
    panelEl = null;
    isOpen = false;
    currentHotel = null;
  }

  function openCompact(hotel) {
    if (!hotel) return;
    embedMode = false;
    ensureMounted();
    resetLockedPanelSize();
    syncPanelLayout({ updateSize: true });
    currentHotel = hotel;
    document.body.style.overflow = "hidden";

    mountPanelContent(renderLoadingContent(hotel));
    panelEl.classList.add("is-open");
    backdropEl.classList.add("is-open");
    panelEl.setAttribute("aria-hidden", "false");
    backdropEl.setAttribute("aria-hidden", "false");
    isOpen = true;

    fetchHotelDetail(hotel).then(function (fullHotel) {
      if (!isOpen) return;
      currentHotel = applyPresentation(fullHotel);
      mountPanelContent(renderPanelContent(currentHotel), currentHotel);
      syncPanelLayout({ updateSize: true });
    });
  }

  function open(hotel) {
    if (!hotel) return;
    if (window.__DEALALITY_HOTEL_EXPLORER_PRIMARY && window.HotelExplorer && typeof window.HotelExplorer.open === "function") {
      window.HotelExplorer.open(hotel);
      return;
    }
    openCompact(hotel);
  }

  function close() {
    if (embedMode) return;
    if (!panelEl || !backdropEl) return;
    panelEl.classList.remove("is-open");
    backdropEl.classList.remove("is-open");
    panelEl.setAttribute("aria-hidden", "true");
    backdropEl.setAttribute("aria-hidden", "true");
    isOpen = false;
    currentHotel = null;
    document.body.style.overflow = "";
    resetLockedPanelSize();
    var panelsRoot = panelEl.querySelector(".hdp-tab-panels");
    if (panelsRoot) {
      panelsRoot.style.height = "";
      panelsRoot.style.minHeight = "";
      panelsRoot.style.maxHeight = "";
    }
  }

  function createMinimalPopup(hotel) {
    return (
      '<div class="hdp-popup-mini">' +
        '<p class="hdp-popup-mini__name">' + esc(displayValue(hotel.name, "Hotel")) + "</p>" +
        '<div class="hdp-popup-mini__status">' + esc(displayValue(hotel.status, "—")) + "</div>" +
        '<div class="hdp-popup-mini__hint">Click for full property details</div>' +
      "</div>"
    );
  }

  function bindHotelMarker(marker, hotel, mapInstance) {
    if (!marker || !hotel) return marker;

    marker.bindTooltip(esc(displayValue(hotel.name, "Hotel")), {
      direction: "top",
      offset: [0, -6],
      opacity: 0.92
    });

    marker.on("click", function () {
      open(hotel);
      if (mapInstance && typeof mapInstance.closePopup === "function") {
        mapInstance.closePopup();
      }
    });

    return marker;
  }

  function reloadDistanceSensitiveTabs() {
    if (!isOpen || !currentHotel || !panelEl) return;
    loadAreaHotelsTab(currentHotel);
    var scope = getPropertyContextScope(currentHotel);
    if (hotelCoords(currentHotel)) {
      loadDemandAnchorTabs(currentHotel, scope);
      loadAccessConnectivityTab(currentHotel, scope);
    }
  }

  window.HotelDetailPanel = {
    open: open,
    close: close,
    createMinimalPopup: createMinimalPopup,
    bindHotelMarker: bindHotelMarker,
    embed: embed,
    unembed: unembed,
    openCompact: openCompact,
    showEmbeddedPanel: showEmbeddedPanel,
    reloadDistanceSensitiveTabs: reloadDistanceSensitiveTabs,
    setPresentationOverrides: function (overrides) {
      presentationOverrides = overrides || null;
    },
  };
})();
