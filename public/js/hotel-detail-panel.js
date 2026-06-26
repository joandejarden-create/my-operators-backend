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
  var currentHotel = null;

  var CONTEXT_CONFIG = {
    nearbyLimit: 12,
    infraLimit: 12,
    competitiveLimit: 50,
    demandDriversLimit: 40,
    minAirports: 2,
  };

  /** Area Hotels scope — location-type radii + submarket + auto-expand (see docs). */
  var AREA_HOTELS_SCOPE = {
    minCompHotels: 5,
    listLimit: 50,
    maxRadiusKm: 75,
    byLocationType: {
      Urban: 10,
      Suburban: 20,
      Airport: 15,
      "Small Metro/Town": 30,
      Interstate: 25,
      Resort: 50,
      default: 25,
    },
    denseUrbanKm: 8,
    islandKm: 25,
    sparseCorridorKm: 60,
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

  function formatParentCompanyDisplay(value) {
    if (value == null || value === "" || value === "Unknown") return "No Parent Company (Blank)";
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

  function resolveDescriptionText(hotel) {
    var fromAirtable = hotel && (hotel.hotelDescription || hotel.description);
    if (fromAirtable && String(fromAirtable).trim()) {
      return String(fromAirtable).trim();
    }
    return buildDescription(hotel);
  }

  function buildOverviewRows(hotel) {
    var rows = [
      { label: "Brand", value: displayValue(hotel.brand) },
      { label: "Parent company", value: formatParentCompanyDisplay(hotel.parentCompany) },
      { label: "Chain scale", value: displayValue(hotel.chainScale || hotel.propertyType) },
      { label: "Property type", value: displayValue(hotel.censusPropertyType) },
      { label: "Rooms", value: hotel.rooms ? formatNumber(hotel.rooms) : "—" },
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
    var preserved = { wifi: "WiFi", "wi-fi": "Wi-Fi", ev: "EV" };
    return String(label || "")
      .trim()
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
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
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

  /**
   * Prefer the Amenities text field when it lists more items than amenitiesDisplay.
   * amenitiesDisplay can be stale (older API builds only kept Hilton-mapped ids).
   */
  function resolveAmenitiesDisplay(hotel) {
    var fromField = parseAmenitiesField(hotel && hotel.amenities);
    var fromDisplay = [];
    if (hotel && Array.isArray(hotel.amenitiesDisplay)) {
      fromDisplay = hotel.amenitiesDisplay.filter(function (item) {
        return item && (item.id || item.label);
      }).map(function (item) {
        return {
          id: item.id || null,
          label: toAmenityProperCase(item.label),
        };
      });
    }

    var idByLabelKey = {};
    fromDisplay.forEach(function (item) {
      if (!item.id || !item.label) return;
      idByLabelKey[amenityLabelKey(item.label)] = item.id;
    });

    var sourceLabels = fromField.length >= fromDisplay.length ? fromField : [];
    if (sourceLabels.length) {
      return sourceLabels.map(function (label) {
        var proper = toAmenityProperCase(label);
        return {
          id: resolveAmenityIdForLabel(label, idByLabelKey) || resolveAmenityIdForLabel(proper, idByLabelKey),
          label: proper,
        };
      });
    }

    if (fromDisplay.length) return fromDisplay;

    return fromField.map(function (label) {
      return {
        id: resolveAmenityIdForLabel(label, idByLabelKey),
        label: toAmenityProperCase(label),
      };
    });
  }

  function amenityIconSvg(item) {
    var icons = window.HiltonAmenityIcons;
    if (icons) {
      if (item && item.id && typeof icons.iconSvgForAmenityId === "function") {
        return icons.iconSvgForAmenityId(item.id);
      }
      if (item && item.label && typeof icons.iconSvgForAmenityLabel === "function") {
        var byLabel = icons.iconSvgForAmenityLabel(item.label);
        if (byLabel) return byLabel;
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
      return '<p class="hdp-empty">No amenities recorded in the Amenities field for this property.</p>';
    }
    return (
      '<div class="hdp-amenities-wrap">' +
        '<button type="button" class="hdp-amenities-nav hdp-amenities-nav--prev" aria-label="Previous amenities">&lsaquo;</button>' +
        '<div class="hdp-amenities-carousel" tabindex="0">' +
          amenities.map(renderAmenityCard).join("") +
        "</div>" +
        '<button type="button" class="hdp-amenities-nav hdp-amenities-nav--next" aria-label="Next amenities">&rsaquo;</button>' +
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
    var lat = Number(hotel.lat);
    var lng = Number(hotel.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) return null;
    return { lat: lat, lng: lng };
  }

  function formatDistanceKm(km) {
    if (!Number.isFinite(km)) return "";
    if (km < 1) return Math.round(km * 1000) + " m";
    if (km < 10) return km.toFixed(1) + " km";
    return Math.round(km) + " km";
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
        expanded = radiusKm > baseRadiusKm;
      }
    }

    var note = "";
    if (expanded) {
      var atBase = filterHotelsByDistance(hotel, pool, baseRadiusKm);
      var atBaseCount = splitAreaHotelCandidates(hotel, atBase).existing.length;
      note =
        "Expanded to " +
        chosenRadius +
        " km — only " +
        atBaseCount +
        " open hotels within " +
        baseRadiusKm +
        " km.";
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
      parts.push("Within " + scope.radiusKm + " km");
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

  function renderDemandDriversTabPanel(hotel, bodyHtml) {
    return (
      '<div class="hdp-demand-drivers-tab">' +
        '<div class="hdp-detail-row">' +
          '<section class="hdp-section hdp-demand-drivers-location">' +
            '<h3 class="hdp-section__title">Location</h3>' +
            renderLocationBlock(hotel) +
          "</section>" +
          '<section class="hdp-section hdp-demand-drivers-body">' +
            '<h3 class="hdp-section__title">Demand drivers</h3>' +
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

  function groupByField(items, fieldFn) {
    var groups = {};
    (items || []).forEach(function (item) {
      var key = fieldFn(item) || "Other";
      if (!groups[key]) groups[key] = [];
      groups[key].push(item);
    });
    return groups;
  }

  function estimateDriveMinutes(distanceKm) {
    if (!Number.isFinite(distanceKm)) return null;
    return Math.max(1, Math.round((distanceKm / 55) * 60));
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
    return {
      totalHotels: (hotels || []).length,
      openHotels: open.length,
      openRooms: open.reduce(function (sum, h) {
        return sum + (Number(h.rooms) || 0);
      }, 0),
      pipelineHotels: pipeline.length,
      pipelineRooms: pipeline.reduce(function (sum, h) {
        return sum + (Number(h.rooms) || 0);
      }, 0),
      candidateHotels: candidate.length,
      candidateRooms: candidate.reduce(function (sum, h) {
        return sum + (Number(h.rooms) || 0);
      }, 0),
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
    var wrapCls = "hdp-footprint-table-wrap" + (wrapClass ? " " + wrapClass : "");
    return (
      '<section class="hdp-footprint-subsection">' +
        '<h4 class="hdp-footprint-table-title">' + esc(title) + "</h4>" +
        '<div class="' + wrapCls + '">' +
          '<table class="hdp-footprint-table">' +
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

  function renderSubmarketSnapshotPanel(hotel, submarketHotels, metrics, breakdowns, penetrationPct, pipelineMetrics) {
    metrics = metrics || {};
    breakdowns = breakdowns || {};
    var corridor = resolveEffectiveSubmarket(hotel);
    var RBP = window.RadarBrandedPenetration;
    var geo = RBP && typeof RBP.geographyFromHotel === "function" ? RBP.geographyFromHotel(hotel) : null;
    var scopeLabel =
      geo && RBP.buildGeographyTitle
        ? RBP.buildGeographyTitle(geo)
        : [corridor, hotel.market, hotel.country].filter(Boolean).join(" · ");
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

    var summaryTable = renderFootprintTable(
      "Open vs. Pipeline (Submarket)",
      "Submarket",
      "",
      fpMetricRowHtml(
        "Total (Submarket)",
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

    var hotelTable = "";
    if (submarketHotels && submarketHotels.length) {
      hotelTable = renderFootprintTable(
        "All census hotels in submarket (" + submarketHotels.length + ")",
        "Hotel",
        sortSubmarketHotels(submarketHotels).map(function (h) {
          return footprintRowFromHotel(h, shareDenoms);
        }).join(""),
        "",
        "hdp-submarket-hotel-table-wrap"
      );
    } else {
      hotelTable = '<p class="hdp-empty">No census hotels found for this submarket in the loaded map data.</p>';
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
      '<div class="hdp-submarket-snapshot hdp-footprint-metrics">' +
        '<p class="hdp-snapshot-scope">' + esc(scopeLabel || "Submarket scope unavailable") + "</p>" +
        strRegionalNote +
        metaLine +
        pipelineMetaLine +
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
          : scope.radiusKm + " km";
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
          : scope.radiusKm + " km";
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
        '<div class="hdp-detail-row">' +
          '<section class="hdp-section hdp-area-hotels-existing">' +
            '<h3 class="hdp-section__title">Existing hotels</h3>' +
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
        esc(scopeLabel || String(radiusKm) + " km") +
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
        { value: radiusKm + " km", label: "Search radius" },
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
      meta.push(formatDistanceKm(item.distanceKm));
      var mins = estimateDriveMinutes(item.distanceKm);
      if (mins) meta.push("~" + mins + " min drive");
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

    return (
      "<li" + filterAttrs + ">" +
        '<span class="hdp-context-list__dot" style="background:' + esc(infraApi.getInfraColor(item)) + '"></span>' +
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
        '<p class="hdp-empty">No airports or connectivity points within ' +
          esc(scopeLabel || String(radiusKm) + " km") +
          ".</p>"
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
      { value: radiusKm + " km", label: "Search radius" },
    ];

    return (
      '<div class="hdp-access-connectivity-tab hdp-context-filterable-panel">' +
        renderContextScopeLine(
          scope,
          "Drive times estimated from straight-line distance (~55 km/h average). Closest " +
            CONTEXT_CONFIG.minAirports +
            " airports always included."
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

  function loadMarketIntelTabs(hotel) {
    setTabPanel("submarket-snapshot", '<p class="hdp-empty hdp-context-state">Loading submarket snapshot…</p>', "loading");

    var RBP = window.RadarBrandedPenetration;
    var censusHotels = window.allHotels || [];
    var mapFilters =
      RBP && typeof RBP.resolveMapFilters === "function" ? RBP.resolveMapFilters() : {};
    var submarketHotels =
      RBP && typeof RBP.hotelsInGeographyBucket === "function"
        ? RBP.hotelsInGeographyBucket(hotel, censusHotels)
        : filterHotelsBySubmarket(hotel, censusHotels);
    var submarketMetrics = computeSubmarketMetrics(submarketHotels);
    var submarketBreakdowns = computeSubmarketBreakdowns(submarketHotels);
    var penetrationPct = computeSubmarketPenetration(hotel, censusHotels, mapFilters);
    var pipelineMetrics = computeSubmarketPipelinePressure(hotel, censusHotels, mapFilters);

    setTabPanel(
      "submarket-snapshot",
      renderSubmarketSnapshotPanel(
        hotel,
        submarketHotels,
        submarketMetrics,
        submarketBreakdowns,
        penetrationPct,
        pipelineMetrics
      ),
      submarketHotels.length ? "success" : "empty"
    );

    fetchSubmarketCoverage(hotel)
      .then(function (report) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (!report || !report.success || submarketHotels.length) return;
        var metrics = report.metrics || {};
        var breakdowns = report.breakdowns || {};
        setTabPanel(
          "submarket-snapshot",
          renderSubmarketSnapshotPanel(
            hotel,
            [],
            {
              totalHotels: metrics.openHotels + metrics.pipelineHotels,
              openHotels: metrics.openHotels,
              openRooms: metrics.openRooms,
              pipelineHotels: metrics.pipelineHotels,
              pipelineRooms: metrics.pipelineRooms,
              totalRooms:
                fpMetricNum(metrics.openRooms) +
                fpMetricNum(metrics.pipelineRooms) +
                fpMetricNum(metrics.candidateRooms),
              brandCount: metrics.brandCount,
              parentCompanyCount: metrics.parentCompanyCount,
            },
            breakdowns,
            null
          ) + '<p class="hdp-empty">Showing Scout submarket data; reload map census for full hotel list.</p>',
          "partial"
        );
      })
      .catch(function () {
        /* census-first snapshot already rendered */
      });
  }

  function loadDemandAnchorTabs(hotel, contextScope) {
    var nearbyApi = window.DemandAnchorsRadar;
    contextScope = contextScope || getPropertyContextScope(hotel);
    if (!nearbyApi) {
      setTabPanel(
        "demand-drivers",
        renderDemandDriversTabPanel(hotel, '<p class="hdp-empty">Demand anchor modules are not loaded.</p>'),
        "error"
      );
      return;
    }

    setTabPanel(
      "demand-drivers",
      renderDemandDriversTabPanel(hotel, '<p class="hdp-empty hdp-context-state">Loading demand drivers…</p>'),
      "loading"
    );

    nearbyApi
      .fetchDemandAnchors(buildDemandAnchorsFetchOpts(hotel))
      .then(function (data) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        var allItems = nearbyApi.parseItems(data);
        var drivers = filterAndSortByDistance(allItems, hotel, {
          radiusKm: contextScope.radiusKm,
          limit: CONTEXT_CONFIG.demandDriversLimit,
        });

        setTabPanel(
          "demand-drivers",
          renderDemandDriversPanel(hotel, drivers, nearbyApi, contextScope),
          drivers.length ? "success" : "empty"
        );
      })
      .catch(function (err) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[HotelDetailPanel] demand anchors fetch failed:", err);
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
    if (!infraApi) {
      setTabPanel("access-connectivity", '<p class="hdp-empty">Travel infrastructure modules are not loaded.</p>', "error");
      return;
    }

    setTabPanel("access-connectivity", '<p class="hdp-empty hdp-context-state">Loading access and connectivity…</p>', "loading");

    infraApi
      .fetchInfrastructure(buildInfrastructureFetchOpts(hotel))
      .then(function (data) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
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
        setTabPanel(
          "access-connectivity",
          renderAccessConnectivityPanel(items, infraApi, contextScope, hotel),
          items.length ? "success" : "empty"
        );
      })
      .catch(function (err) {
        if (!isOpen || !isSameHotel(currentHotel, hotel)) return;
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[HotelDetailPanel] infrastructure fetch failed:", err);
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

  function loadAreaHotelsTab(hotel) {
    if (!panelEl || !hotel) return;
    var panel = panelEl.querySelector('[data-panel="area-hotels"]');
    if (!panel) return;

    var allHotels = window.allHotels || [];
    if (!allHotels.length) {
      panel.dataset.state = "empty";
      panel.innerHTML =
        '<p class="hdp-empty">Hotel census data is not loaded yet. Wait for the map to finish loading and try again.</p>';
      return;
    }

    var scope = hotelCoords(hotel) ? getPropertyContextScope(hotel) : {
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
  }

  function loadContextTabData(hotel) {
    if (!panelEl || !hotel) return;

    loadMarketIntelTabs(hotel);
    var contextScope = getPropertyContextScope(hotel);
    loadAreaHotelsTab(hotel);

    if (!hotelCoords(hotel)) {
      setTabPanel(
        "demand-drivers",
        renderDemandDriversTabPanel(
          hotel,
          '<p class="hdp-empty">Location coordinates are not available for nearby demand driver search.</p>'
        ),
        "empty"
      );
      setTabPanel("access-connectivity", '<p class="hdp-empty">Location coordinates are not available.</p>', "empty");
      return;
    }

    loadDemandAnchorTabs(hotel, contextScope);
    loadAccessConnectivityTab(hotel, contextScope);
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

    if (prevBtn) {
      prevBtn.addEventListener("click", function () { scrollByCards(-1); });
    }
    if (nextBtn) {
      nextBtn.addEventListener("click", function () { scrollByCards(1); });
    }
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
            (hotel.hotelHeadline
              ? '<p class="hdp-description hdp-description--headline">' + esc(hotel.hotelHeadline) + "</p>"
              : "") +
            '<p class="hdp-description">' + esc(resolveDescriptionText(hotel)) + "</p>" +
          "</section>" +
          '<section class="hdp-section hdp-overview-section">' +
            '<h3 class="hdp-section__title">Overview</h3>' +
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

  function open(hotel) {
    if (!hotel) return;
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
      currentHotel = fullHotel;
      mountPanelContent(renderPanelContent(fullHotel), fullHotel);
      syncPanelLayout({ updateSize: true });
    });
  }

  function close() {
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

  window.HotelDetailPanel = {
    open: open,
    close: close,
    createMinimalPopup: createMinimalPopup,
    bindHotelMarker: bindHotelMarker
  };
})();
