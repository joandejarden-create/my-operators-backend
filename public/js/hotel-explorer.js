/**
 * Hotel Explorer — Brand Explorer–family full-screen overlay (Packet 2.6A).
 * Mounted same-app component (not iframe). Radar click → overlay → close restores Radar.
 */
(function () {
  "use strict";

  var TABS = [
    { id: "hotel", label: "Hotel<br>Overview" },
    { id: "ownership", label: "Ownership<br>Intelligence" },
    { id: "organization", label: "Organization<br>&amp; Portfolio" },
    { id: "relationships", label: "Relationships<br>&amp; Structure" },
    { id: "submarket", label: "Submarket<br>Snapshot" },
    { id: "area", label: "Area<br>Hotels" },
    { id: "demand", label: "Demand<br>Drivers" },
    { id: "access", label: "Access &amp;<br>Connectivity" },
    { id: "brand", label: "Brand /<br>Operator" },
    { id: "sources", label: "Sources &amp;<br>Evidence" },
  ];

  var TAB_ICONS = {
    hotel:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 9.5L12 3l9 6.5"/><path d="M5 10v10h14V10"/></svg>',
    ownership:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 12V7H5a2 2 0 0 1 0-4h14v2"/><path d="M3 7v10a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-3H7a2 2 0 0 1-2-2 2 2 0 0 1 2-2h16"/></svg>',
    organization:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
    relationships:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="7" cy="7" r="2.5"/><circle cx="17" cy="7" r="2.5"/><circle cx="12" cy="17" r="2.5"/><path d="M9 8.5 11 15M15 8.5 13 15M9.5 7h5"/></svg>',
    submarket:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 19V5"/><path d="M8 17V9"/><path d="M12 17V3"/><path d="M16 17v-6"/><path d="M20 17v-9"/></svg>',
    area:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="8" width="7" height="13"/><rect x="14" y="5" width="7" height="16"/><path d="M6 12h1M17 9h1"/></svg>',
    demand:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>',
    access:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 16h20"/><path d="M5 16l2-8h10l2 8"/><path d="M12 8V4"/><path d="M9 4h6"/></svg>',
    brand:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/></svg>',
    sources:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>',
  };

  var RADAR_TABS = {
    hotel: "overview",
    submarket: "submarket-snapshot",
    area: "area-hotels",
    demand: "demand-drivers",
    access: "access-connectivity",
  };

  var HI_TABS = {
    ownership: "ownership",
    organization: "organization",
    relationships: "relationships",
    brand: "brand",
    sources: "sources",
  };

  var state = {
    open: false,
    hotel: null,
    ownership: null,
    org: null,
    tab: "hotel",
    radarSnapshot: null,
    radarEmbedded: false,
    dossierOpen: false,
    dossier: null,
    dossierList: null,
    openGeneration: 0,
    requestedHotelId: null,
  };

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function recordId(hotel) {
    return (hotel && (hotel.id || hotel.recordId || hotel.airtable_record_id)) || "";
  }

  function snapshotRadar() {
    var map = window.map || window.radarMap || window.leafletMap;
    var snap = {
      scrollX: window.scrollX,
      scrollY: window.scrollY,
      hash: window.location.hash,
      search: window.location.search,
      country: (document.getElementById("countryFilter") || {}).value || null,
      market: (document.getElementById("marketFilter") || {}).value || null,
      mapCenter: null,
      mapZoom: null,
    };
    try {
      if (map && typeof map.getCenter === "function") {
        var c = map.getCenter();
        snap.mapCenter = { lat: c.lat, lng: c.lng };
        snap.mapZoom = typeof map.getZoom === "function" ? map.getZoom() : null;
      }
    } catch (err) {
      console.warn("[HotelExplorer] map snapshot failed", err);
    }
    return snap;
  }

  function restoreRadar(snap) {
    if (!snap) return;
    try {
      window.scrollTo(snap.scrollX || 0, snap.scrollY || 0);
    } catch (err) {
      console.warn("[HotelExplorer] restore scroll failed", err);
    }
    var map = window.map || window.radarMap || window.leafletMap;
    try {
      if (map && snap.mapCenter && typeof map.setView === "function") {
        map.setView([snap.mapCenter.lat, snap.mapCenter.lng], snap.mapZoom || map.getZoom());
      }
    } catch (err) {
      console.warn("[HotelExplorer] restore map failed", err);
    }
  }

  function canonicalBrand(hotel, ownership) {
    var hi = ownership && ownership.hotel;
    return (hi && hi.brand_display) || hotel.brand || hotel.affiliation || "";
  }

  function presentationOverrides(hotel, ownership) {
    var hi = (ownership && ownership.hotel) || {};
    var brand = canonicalBrand(hotel, ownership);
    var parent = hi.brand_parent_display || hi.parent_company || hotel.parentCompany || hotel.parent_company || "";
    var profile = propertyProfile(ownership) || {};
    var amenList = Array.isArray(profile.amenities) ? profile.amenities : [];
    var amenStr =
      (typeof hotel.amenities === "string" && hotel.amenities.trim()) ||
      (Array.isArray(hotel.amenities) && hotel.amenities.length ? hotel.amenities.join(", ") : "") ||
      (amenList.length ? amenList.join(", ") : "");
    var overrides = {
      brand: brand,
      affiliation: brand,
      brand_display: brand,
      brand_parent: parent,
      parentCompany: parent,
      hotelHeadline: hi.positioning_statement || hotel.hotelHeadline || profile.positioning || "",
    };
    // Data seals only — never change tab/section architecture.
    if (hotel.city && !isUnknownDisplayValue(hotel.city)) overrides.city = hotel.city;
    if (hotel.country && !isUnknownDisplayValue(hotel.country)) overrides.country = hotel.country;
    if (hotel.market && !isUnknownDisplayValue(hotel.market)) overrides.market = hotel.market;
    if (hotel.name && !isUnknownDisplayValue(hotel.name)) overrides.name = hotel.name;
    if (hotel.rooms != null && Number(hotel.rooms) > 0) overrides.rooms = hotel.rooms;
    if (amenStr) {
      overrides.amenities = amenStr;
      overrides.amenitiesDisplay = (hotel.amenitiesDisplay && hotel.amenitiesDisplay.length)
        ? hotel.amenitiesDisplay
        : amenList.map(function (a) { return { id: null, label: String(a) }; });
    }
    if (hotel.website) overrides.website = hotel.website;
    return overrides;
  }

  function orgSlug(ownership) {
    return (
      (ownership && ownership.organization && ownership.organization.slug) ||
      (ownership && ownership.organization_slug) ||
      (ownership && ownership.ownership_group_slug) ||
      (ownership && ownership.hotel && ownership.hotel.organization_slug) ||
      null
    );
  }

  function fetchOwnership(hotel) {
    var id = recordId(hotel);
    if (!id) return Promise.resolve(null);
    return fetch("/api/golden-demo/ownership/hotel/" + encodeURIComponent(id), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.json();
      })
      .catch(function (err) {
        console.warn("[HotelExplorer] ownership fetch failed", err);
        return null;
      });
  }

  function fetchOrg(slug) {
    if (!slug) return Promise.resolve(null);
    return fetch("/api/golden-demo/ownership/groups/" + encodeURIComponent(slug), {
      headers: { "ngrok-skip-browser-warning": "true" },
    })
      .then(function (r) {
        return r.json().then(function (body) {
          if (!r.ok || (body && body.success === false) || (body && body.ok === false)) {
            console.warn("[HotelExplorer] org group unavailable", slug, body && body.error);
            return null;
          }
          return body;
        });
      })
      .catch(function (err) {
        console.warn("[HotelExplorer] org fetch failed", err);
        return null;
      });
  }

  function shortOrg(name) {
    if (!name || name === "—") return "";
    return String(name).split("(")[0].split(",")[0].trim();
  }

  function shortScale(value) {
    return String(value || "")
      .replace(/\s*chain\s*$/i, "")
      .trim();
  }

  function intelligenceNarrative(hotel, ownership) {
    var ctrl = control(ownership);
    var econ = ctrl.economic_owner_or_group || {};
    var op = shortOrg((ctrl.operator && ctrl.operator.name) || hotel.managementCompany || "");
    var owner = econ.known && econ.name ? shortOrg(econ.name) : "";
    var brand = canonicalBrand(hotel, ownership);
    var chrono = (ownership && ownership.report && ownership.report.brand_chronology) || [];
    var parts = [];
    if (owner && op && owner === op) {
      parts.push(owner + " owns and operates this hotel.");
    } else if (owner && op) {
      parts.push("Economic owner " + owner + ". Operated by " + op + ".");
    } else if (op) {
      parts.push("Operated by " + op + ". Economic owner is not yet verified.");
    }
    if (hotel.rooms) {
      var scale = shortScale(hotel.chainScale || hotel.chain_scale);
      parts.push(
        hotel.rooms +
          "-room" +
          (scale ? " " + scale.toLowerCase() : "") +
          " property" +
          (hotel.city ? " in " + hotel.city : "") +
          "."
      );
    }
    if (brand) {
      var line = "Currently branded " + brand + ".";
      var hasFormer = chrono.some(function (b) {
        return /FORMER/i.test(b.status || "");
      });
      var hasAnnounced = chrono.some(function (b) {
        return /ANNOUNCED/i.test(b.status || "");
      });
      if (hasFormer) line += " Former brand identities are tracked separately.";
      if (hasAnnounced) line += " Announced brand relationships are not treated as current.";
      parts.push(line);
    }
    return parts.join(" ");
  }

  function fact(value, label) {
    if (!value || value === "—") return "";
    return (
      '<div class="hex-fact"><div class="label">' +
      esc(label) +
      '</div><div class="value">' +
      esc(value) +
      "</div></div>"
    );
  }

  function formatCount(n) {
    if (n == null || n === "" || n === "—") return "";
    var num = typeof n === "number" ? n : Number(String(n).replace(/,/g, ""));
    // Rooms/keys: never show 0 as a resolved KPI (0 is treated as missing).
    if (!Number.isFinite(num) || num <= 0) return "";
    return Math.round(num).toLocaleString("en-US");
  }

  function setShellExplorerOpen(on) {
    /* Stay inside the Radar iframe like Brand Explorer — do not hide parent left nav. */
    document.body.style.overflow = on ? "hidden" : "";
  }

  function meta(label, value) {
    return (
      '<div class="hex-meta-card"><div class="label">' +
      esc(label) +
      '</div><div class="value">' +
      esc(value || "—") +
      "</div></div>"
    );
  }

  function control(ownership) {
    return (ownership && ownership.report && ownership.report.ownership_and_control) || {};
  }

  function normalizeWebsiteUrl(url) {
    if (!url) return "";
    var trimmed = String(url).trim();
    if (!trimmed) return "";
    if (/^https?:\/\//i.test(trimmed)) return trimmed;
    return "https://" + trimmed;
  }

  function actionIcon(iconKey) {
    var files = { pin: "directions.png", building: "website.png", phone: "phone.png", search: "website.png" };
    var file = files[iconKey];
    if (!file) return "";
    return (
      '<img class="hex-action-icon-img" src="/icons/hotel-actions/' +
      file +
      '" alt="" />'
    );
  }

  /** Compact utility link — omitted entirely when href is unsupported (no disabled placeholders). */
  function utilityAction(label, href, iconKey) {
    if (!href) return "";
    var icon =
      '<span class="hex-util__icon" aria-hidden="true">' + actionIcon(iconKey) + "</span>";
    var external = href.indexOf("http") === 0 ? ' target="_blank" rel="noopener noreferrer"' : "";
    return (
      '<a class="hex-util" href="' +
      esc(href) +
      '"' +
      external +
      ' title="' +
      esc(label) +
      '">' +
      icon +
      '<span class="hex-util__label">' +
      esc(label) +
      "</span></a>"
    );
  }

  function actionButton(label, attrs, iconKey) {
    var icon =
      '<span class="hex-action__icon" aria-hidden="true">' + actionIcon(iconKey) + "</span>";
    return (
      '<button type="button" class="hex-action" ' +
      (attrs || "") +
      ">" +
      icon +
      esc(label) +
      "</button>"
    );
  }

  function actionLink(label, href, iconKey) {
    if (!href) return "";
    var icon =
      '<span class="hex-action__icon" aria-hidden="true">' + actionIcon(iconKey) + "</span>";
    var external = href.indexOf("http") === 0 ? ' target="_blank" rel="noopener noreferrer"' : "";
    return (
      '<a class="hex-action" href="' + esc(href) + '"' + external + ">" + icon + esc(label) + "</a>"
    );
  }

  function renderHero(hotel, ownership) {
    var ctrl = control(ownership);
    var econ = ctrl.economic_owner_or_group || {};
    var op = (ctrl.operator && ctrl.operator.name) || hotel.managementCompany || "";
    var brand = canonicalBrand(hotel, ownership) || "";
    var locParts = [hotel.city, hotel.country].filter(Boolean);
    var loc = locParts.join(", ");
    var statement =
      (ownership && ownership.hotel && ownership.hotel.positioning_statement) ||
      intelligenceNarrative(hotel, ownership);
    var website = normalizeWebsiteUrl(hotel.website || hotel.hotelWebsite || "");
    var lat = hotel.latitude != null ? hotel.latitude : hotel.lat;
    var lng = hotel.longitude != null ? hotel.longitude : hotel.lng;
    var maps =
      Number.isFinite(Number(lat)) &&
      Number.isFinite(Number(lng)) &&
      !(Number(lat) === 0 && Number(lng) === 0)
        ? "https://www.google.com/maps?q=" + encodeURIComponent(lat + "," + lng)
        : "";
    var phoneRaw = hotel.telephone || hotel.phone || "";
    var phoneHref = phoneRaw ? "tel:" + String(phoneRaw).replace(/[^\d+]/g, "") : "";
    var statusRaw = hotel.status || hotel.hotelStatus || hotel.operating_status || "";
    var status = "";
    if (statusRaw && !/^unknown(\s+\w+)?$/i.test(String(statusRaw).trim())) {
      status = statusRaw;
    }
    var facts =
      fact(formatCount(hotel.rooms), "Rooms") +
      fact(shortScale(hotel.chainScale || hotel.chain_scale) || "", "Chain scale") +
      fact(brand, "Current brand") +
      fact(shortOrg(op), "Operator") +
      fact(econ.known && econ.name ? shortOrg(econ.name) : "", "Economic owner") +
      fact(status, "Hotel status");
    var utils =
      utilityAction("Directions", maps, "pin") +
      utilityAction("Website", website, "building") +
      (phoneHref ? utilityAction("Phone", phoneHref, "phone") : "");
    return (
      '<header class="hex-hero">' +
      '<div class="hex-hero__top">' +
      '<div class="hex-hero__identity">' +
      '<p class="hex-kicker">Hotel Explorer</p>' +
      "<h1>" +
      esc(hotel.name || "Hotel") +
      "</h1>" +
      '<p class="hex-hero__tag">' +
      esc([brand, loc].filter(Boolean).join(" · ")) +
      "</p>" +
      "</div>" +
      '<div class="hex-hero__actions" aria-label="Hotel actions">' +
      '<div class="hex-hero__btn-row">' +
      '<button type="button" class="hex-btn hex-btn--primary" data-hex-deep-research>' +
      "Deep Research" +
      '<span class="hex-btn__badge" data-hex-research-badge hidden></span>' +
      "</button>" +
      "</div>" +
      (utils ? '<div class="hex-hero__utilities">' + utils + "</div>" : "") +
      "</div>" +
      "</div>" +
      (statement ? '<p class="hex-hero__statement">' + esc(statement) + "</p>" : "") +
      (facts ? '<div class="hex-facts">' + facts + "</div>" : "") +
      "</header>"
    );
  }

  function renderTabs() {
    return (
      '<nav class="tabs-section hex-tabs" role="tablist" aria-label="Hotel Explorer sections">' +
      TABS.map(function (t) {
        return (
          '<button type="button" class="section-nav-item' +
          (state.tab === t.id ? " active" : "") +
          '" role="tab" aria-selected="' +
          (state.tab === t.id ? "true" : "false") +
          '" data-hex-tab="' +
          t.id +
          '"><div class="section-nav-icon" aria-hidden="true">' +
          (TAB_ICONS[t.id] || "") +
          '</div><div class="section-nav-label">' +
          t.label +
          "</div></button>"
        );
      }).join("") +
      "</nav>"
    );
  }

  function isRadarTab(tab) {
    return Boolean(RADAR_TABS[tab]);
  }

  function marketIntel(ownership) {
    return (
      (ownership && ownership.hotel && ownership.hotel.market_intelligence) ||
      (ownership && ownership.deep_research && ownership.deep_research.market_intelligence) ||
      (ownership && ownership.report && ownership.report.market_intelligence) ||
      null
    );
  }

  function propertyProfile(ownership) {
    return (
      (ownership && ownership.hotel && ownership.hotel.property_profile) ||
      (ownership && ownership.deep_research && ownership.deep_research.property_profile) ||
      null
    );
  }

  /**
   * Data enrichment only — never changes tab/section architecture.
   * Maps ownership / research fields onto the shared hotel object consumed by HDP.
   */
  function enrichHotelFromOwnership(hotel, ownership) {
    if (!hotel) return hotel;
    var hi = (ownership && ownership.hotel) || {};
    var profile = propertyProfile(ownership) || {};
    var loc = (ownership && ownership.location) || hi.location || {};
    var next = Object.assign({}, hotel);
    var rooms = next.rooms || profile.rooms_suites || hi.rooms || hi.room_count;
    if (rooms != null && rooms !== "" && Number(rooms) !== 0) next.rooms = rooms;
    next.city = preferKnown(next.city, hi.city || loc.city || loc.parish);
    next.country = preferKnown(next.country, hi.country || loc.country);
    next.market = preferKnown(next.market, hi.market || loc.market || loc.island);
    var amenList = Array.isArray(profile.amenities) ? profile.amenities : [];
    if (amenList.length) {
      var amenStr = amenList.map(function (a) { return String(a); }).join(", ");
      var existingRich =
        (Array.isArray(next.amenities) && next.amenities.length) ||
        (typeof next.amenities === "string" && next.amenities.trim());
      if (!existingRich) next.amenities = amenStr;
      if (!next.amenitiesDisplay || !next.amenitiesDisplay.length) {
        next.amenitiesDisplay = amenList.map(function (a) {
          return { id: null, label: String(a) };
        });
      }
    }
    if (!next.address1 && (profile.address || next.address)) {
      next.address1 = profile.address || next.address;
    }
    if (!next.address && profile.address) next.address = profile.address;
    if (!next.website && profile.website) next.website = profile.website;
    if (!next.phone && profile.phone) next.phone = profile.phone;
    if (profile.latitude != null && (next.latitude == null || next.latitude === "")) next.latitude = profile.latitude;
    if (profile.longitude != null && (next.longitude == null || next.longitude === "")) next.longitude = profile.longitude;
    if (!next.hotelHeadline && (hi.positioning_statement || profile.positioning)) {
      next.hotelHeadline = hi.positioning_statement || profile.positioning;
    }
    return Object.assign(next, presentationOverrides(next, ownership));
  }

  function setTabRenderSource(source) {
    state.tabRenderSource = source || "UNKNOWN";
    try {
      window.HOTEL_EXPLORER_TAB_RENDER_SOURCE = {
        hotelId: recordId(state.hotel),
        tab: state.tab,
        source: state.tabRenderSource,
        at: new Date().toISOString(),
      };
    } catch (err) {
      /* ignore */
    }
  }

  function resolveHotelChainScale(hotel, ownership) {
    var hi = (ownership && ownership.hotel) || {};
    return (
      (hotel && (hotel.chainScale || hotel.chain_scale)) ||
      hi.chain_scale ||
      hi.chainScale ||
      ""
    );
  }

  function hotelExplorerThemeRoots() {
    var roots = [];
    var root = document.getElementById("hexRoot");
    if (root) roots.push(root);
    var hero = root && root.querySelector(".hex-hero");
    if (hero) roots.push(hero);
    var drawer = document.getElementById("hiResearchDrawer");
    if (drawer) roots.push(drawer);
    return roots;
  }

  function applyHotelExplorerChainScaleTheme() {
    var api = window.DealalityChainScaleTheme;
    if (!api || !api.applyThemeFromScale) return null;
    var scale = resolveHotelChainScale(state.hotel, state.ownership);
    return api.applyThemeFromScale(scale, hotelExplorerThemeRoots());
  }

  function clearHotelExplorerChainScaleTheme() {
    var api = window.DealalityChainScaleTheme;
    if (!api || !api.clearTheme) return;
    api.clearTheme(hotelExplorerThemeRoots());
  }

  function paint() {
    var root = document.getElementById("hexRoot");
    if (!root || !state.hotel) return;
    root.querySelector(".hex-hero-slot").innerHTML = renderHero(state.hotel, state.ownership);
    root.querySelector(".hex-tabs-slot").innerHTML = renderTabs();
    applyHotelExplorerChainScaleTheme();
    var hiHost = document.getElementById("hexHiHost");
    var radarHost = document.getElementById("hexRadarHost");
    var canEmbed =
      window.HotelDetailPanel &&
      typeof window.HotelDetailPanel.embed === "function" &&
      typeof window.HotelDetailPanel.showEmbeddedPanel === "function";

    // Packet 2.7-R1: ALL hotels use the same radar-tab renderer (HDP embed).
    // Market intelligence enriches data — it must NOT switch templates.
    if (isRadarTab(state.tab) && canEmbed) {
      hiHost.hidden = true;
      radarHost.hidden = false;
      setTabRenderSource("RADAR_EMBED");
      if (!state.radarEmbedded) {
        if (!radarHost.getAttribute("data-hex-embedding")) {
          radarHost.setAttribute("data-hex-embedding", "1");
          radarHost.innerHTML = '<p class="hex-card">Loading hotel location intelligence…</p>';
          embedRadarIfNeeded()
            .then(function () {
              radarHost.removeAttribute("data-hex-embedding");
              if (!state.open || !isRadarTab(state.tab)) return;
              setTabRenderSource("RADAR_EMBED");
              window.HotelDetailPanel.showEmbeddedPanel(RADAR_TABS[state.tab]);
            })
            .catch(function (err) {
              radarHost.removeAttribute("data-hex-embedding");
              console.warn("[HotelExplorer] radar embed failed", err);
              setTabRenderSource("HI_FALLBACK");
              paintHiFallback(hiHost, radarHost);
            });
        }
      } else {
        window.HotelDetailPanel.showEmbeddedPanel(RADAR_TABS[state.tab]);
      }
    } else if (isRadarTab(state.tab) && !canEmbed) {
      setTabRenderSource("HI_FALLBACK");
      paintHiFallback(hiHost, radarHost);
    } else {
      radarHost.hidden = true;
      hiHost.hidden = false;
      setTabRenderSource("CANONICAL_HI");
      var html = "";
      if (window.HotelIntelligenceWorkspace && window.HotelIntelligenceWorkspace.renderTabHtml) {
        html = window.HotelIntelligenceWorkspace.renderTabHtml(
          HI_TABS[state.tab] || state.tab,
          state.hotel,
          state.ownership,
          state.org
        );
      }
      hiHost.innerHTML = html || '<p class="hex-card">This section is not available yet.</p>';
    }
    writeDeepLink();
    refreshResearchBadge();
  }

  function paintHiFallback(hiHost, radarHost) {
    if (radarHost) radarHost.hidden = true;
    if (hiHost) hiHost.hidden = false;
    setTabRenderSource("HI_FALLBACK");
    var html = "";
    if (window.HotelIntelligenceWorkspace && window.HotelIntelligenceWorkspace.renderTabHtml) {
      html = window.HotelIntelligenceWorkspace.renderTabHtml(
        state.tab,
        state.hotel,
        state.ownership,
        state.org
      );
    }
    if (hiHost) {
      hiHost.innerHTML = html || '<p class="hex-card">This section is not available yet.</p>';
    }
  }

  function refreshResearchBadge() {
    var badge = document.querySelector("[data-hex-research-badge]");
    if (!badge || !state.hotel) return;
    var recordId = state.hotel.id || state.hotel.recordId;
    if (!recordId) return;
    fetch("/api/hotel-intelligence/hotels/" + encodeURIComponent(recordId) + "/research")
      .then(function (r) {
        return r.json();
      })
      .then(function (payload) {
        if (!badge.isConnected) return;
        if (payload && payload.badge && payload.badge.label) {
          badge.hidden = false;
          badge.textContent = payload.badge.label;
        } else {
          badge.hidden = true;
          badge.textContent = "";
        }
      })
      .catch(function () {
        /* ignore badge failures */
      });
  }

  function writeDeepLink() {
    try {
      var u = new URL(window.location.href);
      u.searchParams.set("hotelExplorer", recordId(state.hotel));
      u.searchParams.set("hexTab", state.tab);
      history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch (err) {
      console.warn("[HotelExplorer] deep link failed", err);
    }
  }

  function clearDeepLink() {
    try {
      var u = new URL(window.location.href);
      u.searchParams.delete("hotelExplorer");
      u.searchParams.delete("hexTab");
      history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch (err) {
      console.warn("[HotelExplorer] clear deep link failed", err);
    }
  }

  function ensureRoot() {
    var root = document.getElementById("hexRoot");
    if (root) return root;
    root = document.createElement("div");
    root.id = "hexRoot";
    root.className = "hex-root";
    root.setAttribute("role", "dialog");
    root.setAttribute("aria-modal", "true");
    root.setAttribute("aria-label", "Hotel Explorer");
    root.tabIndex = -1;
    root.innerHTML =
      '<div class="hex-backdrop" data-hex-back></div>' +
      '<div class="hex-shell">' +
      '<button type="button" class="hex-close" data-hex-back aria-label="Close">&times;</button>' +
      '<div class="hex-scroll">' +
      '<div class="hex-hero-slot"></div>' +
      '<div class="hex-tabs-slot"></div>' +
      '<div class="hex-main" id="hexMain">' +
      '<div id="hexHiHost"></div>' +
      '<div id="hexRadarHost" class="hex-radar-host" hidden></div>' +
      "</div></div>" +
      '<div class="hex-dossier-layer" id="hexDossierLayer" hidden>' +
      '<div class="hex-dossier-layer__body" id="hexDossierBody"></div></div>' +
      "</div>" +
      '<aside class="hex-drawer" id="hexDrawer" hidden></aside>';
    document.body.appendChild(root);
    root.addEventListener("click", onClick);
    return root;
  }

  function onClick(e) {
    if (e.target.closest("[data-hex-deep-research]")) {
      openDeepResearch();
      return;
    }
    if (e.target.closest("[data-hex-back]")) {
      if (state.dossierOpen) {
        closeDossierLayer();
        return;
      }
      close();
      return;
    }
    var tabBtn = e.target.closest("[data-hex-tab]");
    if (tabBtn) {
      setTab(tabBtn.getAttribute("data-hex-tab"));
      return;
    }
    var hiTab = e.target.closest("[data-hiw-tab]");
    if (hiTab) {
      var raw = hiTab.getAttribute("data-hiw-tab");
      var mapped = { overview: "hotel", market: "hotel", demand: "demand", intelligence: "hotel" }[raw] || raw;
      setTab(mapped);
      return;
    }
    if (e.target.closest("[data-hex-drawer-close]")) {
      closeDrawer();
      return;
    }
    var cite = e.target.closest("[data-hiw-cite]");
    if (cite) {
      openDrawer(cite.getAttribute("data-hiw-cite"));
      return;
    }
    var filt = e.target.closest("[data-hiw-rel-filter]");
    if (filt && window.HotelIntelligenceWorkspace) {
      window.HotelIntelligenceWorkspace.setRelFilter(filt.getAttribute("data-hiw-rel-filter"));
      paint();
      return;
    }
    if (e.target.closest("[data-hiw-org-network]") && window.HotelIntelligenceWorkspace) {
      window.HotelIntelligenceWorkspace.loadOrgNetwork();
    }
  }

  function closeDossierLayer() {
    state.dossierOpen = false;
    state.dossier = null;
    state.dossierList = null;
    var layer = document.getElementById("hexDossierLayer");
    var body = document.getElementById("hexDossierBody");
    var root = document.getElementById("hexRoot");
    if (root) root.classList.remove("hex-root--dossier-open");
    if (layer) {
      layer.hidden = true;
      layer.classList.remove("is-open");
    }
    if (body) body.innerHTML = "";
    closeDrawer();
  }

  function openDossierSource(src, num) {
    var drawer = document.getElementById("hexDrawer");
    if (!drawer) return;
    drawer.hidden = false;
    drawer.classList.add("is-open");
    if (!src) {
      drawer.innerHTML =
        '<button type="button" class="hex-drawer-close" data-hex-drawer-close aria-label="Close evidence">&times;</button>' +
        "<h4>Evidence</h4><p>Citation [" +
        esc(num || "") +
        "] is not available in this dossier bibliography.</p>";
      return;
    }
    drawer.innerHTML =
      '<button type="button" class="hex-drawer-close" data-hex-drawer-close aria-label="Close evidence">&times;</button>' +
      "<h4>Source [" +
      esc(String(src.number || num || "")) +
      "]</h4><p><strong>" +
      esc(src.title) +
      "</strong></p>" +
      (src.publisher ? "<p>Publisher: " + esc(src.publisher) + "</p>" : "") +
      (src.date ? "<p>Date: " + esc(src.date) + "</p>" : "") +
      (src.source_type ? "<p>Type: " + esc(src.source_type) + "</p>" : "") +
      (src.url
        ? '<p><a href="' + esc(src.url) + '" target="_blank" rel="noopener">Open source</a></p>'
        : "");
  }

  function renderDossierLoadError(message, diagnosticId) {
    var body = document.getElementById("hexDossierBody");
    if (!body) return;
    body.innerHTML =
      '<div class="hid-library-wrap"><button type="button" class="bas-btn bas-btn-secondary" data-hid-close>← Back to Hotel Explorer</button>' +
      '<div class="hid-runtime-error" role="alert" data-hid-customer-safe="1">' +
      "<h2>This report is not yet ready</h2>" +
      "<p>" +
      esc(message || "Report processing is incomplete. Please try again shortly.") +
      "</p>" +
      (diagnosticId
        ? '<p class="hex-dossier-diag" data-hid-diag="' + esc(diagnosticId) + '">Diagnostic: ' + esc(diagnosticId) + "</p>"
        : "") +
      "</div></div>";
    var back = body.querySelector("[data-hid-close]");
    if (back) back.addEventListener("click", closeDossierLayer);
  }

  function renderDossierReport(dossier) {
    var body = document.getElementById("hexDossierBody");
    var layer = document.getElementById("hexDossierLayer");
    var root = document.getElementById("hexRoot");
    if (!body || !layer) return;
    if (!window.HotelIntelligenceDossier) {
      console.error("[HotelExplorer] HotelIntelligenceDossier module missing");
      renderDossierLoadError("Report viewer failed to load. Refresh the page and try again.", "hid_module_missing");
      return;
    }
    try {
      var expectedHotelIds = [
        recordId(state.hotel),
        state.hotel && state.hotel.hotel_id,
        state.hotel && state.hotel.id,
        state.requestedHotelId,
      ].filter(Boolean);
      // Per-report contract resolution — never reuse prior report contract state.
      var contract =
        window.HotelIntelligenceDossier.resolveReportContract &&
        window.HotelIntelligenceDossier.resolveReportContract(dossier);
      var integrity =
        window.HotelIntelligenceDossier.assertLiveIntegrity &&
        window.HotelIntelligenceDossier.assertLiveIntegrity(dossier, {
          expectedHotelIds: expectedHotelIds,
        });
      if (integrity && !integrity.ok) {
        state.dossierOpen = true;
        if (root) root.classList.add("hex-root--dossier-open");
        layer.hidden = false;
        layer.classList.add("is-open");
        var founderDebug =
          typeof location !== "undefined" &&
          /[?&](founderDebug|hidDebug)=1/.test(String(location.search || ""));
        if (founderDebug) {
          body.innerHTML =
            '<div class="hid-library-wrap"><button type="button" class="bas-btn bas-btn-secondary" data-hid-close>← Back to Hotel Explorer</button>' +
            '<div class="hid-runtime-error" role="alert"><h2>Report validation failed (founder debug)</h2><p>Contract: ' +
            esc((contract && contract.contract_id) || "unknown") +
            "</p><ul>" +
            (integrity.errors || [])
              .map(function (e) {
                return "<li>" + esc(e) + "</li>";
              })
              .join("") +
            "</ul></div></div>";
          var errClose = body.querySelector("[data-hid-close]");
          if (errClose) errClose.addEventListener("click", closeDossierLayer);
        } else {
          renderDossierLoadError("This report is not yet ready.", "hid_integrity_" + String(Date.now()).slice(-6));
        }
        console.error("[HotelExplorer] dossier integrity failed", {
          contract: contract,
          errors: integrity.errors,
        });
        return;
      }
      state.dossier = dossier;
      state.dossierOpen = true;
      if (root) root.classList.add("hex-root--dossier-open");
      layer.hidden = false;
      layer.classList.add("is-open");
      body.innerHTML = window.HotelIntelligenceDossier.buildHtml(dossier, {
        embed: true,
        closeLabel: "← Back to Hotel Explorer",
        expectedHotelIds: expectedHotelIds,
      });
      window.HotelIntelligenceDossier.bind(body, dossier, {
        onClose: closeDossierLayer,
        onCite: openDossierSource,
      });
      var focusTarget = body.querySelector("[data-hid-close]") || body.querySelector("[data-hid-root]");
      if (focusTarget && typeof focusTarget.focus === "function") {
        try {
          focusTarget.focus();
        } catch (_) {}
      }
    } catch (err) {
      console.error("[HotelExplorer] dossier render failed", err);
      renderDossierLoadError(
        "Report viewer encountered an error. Please try again.",
        "hid_render_" + String(Date.now()).slice(-6)
      );
    }
  }

  function openDeepResearch() {
    var hotel = state.hotel;
    if (!hotel) return;
    var recordId = hotel.id || hotel.recordId;
    if (window.HotelIntelligenceResearchCenter && typeof window.HotelIntelligenceResearchCenter.open === "function") {
      window.HotelIntelligenceResearchCenter.open({
        hotelId: recordId,
        hotelName: hotel.name || hotel.hotel_name || "",
      });
      return;
    }
    // Fallback if Research Center script failed to load — do not lose access to dossier.
    openDossierForHotel();
  }

  function openDossierById(dossierId) {
    var id = String(dossierId || "").trim();
    if (!id) return;
    var body = document.getElementById("hexDossierBody");
    var layer = document.getElementById("hexDossierLayer");
    if (!body || !layer) return;
    state.dossierOpen = true;
    layer.hidden = false;
    layer.classList.add("is-open");
    var rootEl = document.getElementById("hexRoot");
    if (rootEl) rootEl.classList.add("hex-root--dossier-open");
    body.innerHTML = '<p class="hex-dossier-loading">Loading Intelligence Dossier…</p>';
    fetch("/api/hotel-intelligence/dossiers/" + encodeURIComponent(id))
      .then(function (r) {
        return r.json();
      })
      .then(function (res) {
        if (!state.dossierOpen) return;
        if (res && res.dossier) renderDossierReport(res.dossier);
        else {
          body.innerHTML =
            '<div class="hid-library-wrap"><button type="button" class="bas-btn bas-btn-secondary" data-hid-close>← Back to Hotel Explorer</button>' +
            '<p class="hex-dossier-loading">Report not available.</p></div>';
          var back = body.querySelector("[data-hid-close]");
          if (back) back.addEventListener("click", closeDossierLayer);
        }
      })
      .catch(function (err) {
        console.warn("[HotelExplorer] dossier fetch failed", err);
        body.innerHTML =
          '<div class="hid-library-wrap"><button type="button" class="bas-btn bas-btn-secondary" data-hid-close>← Back to Hotel Explorer</button>' +
          '<p class="hex-dossier-loading">Could not load report.</p></div>';
        var failClose = body.querySelector("[data-hid-close]");
        if (failClose) failClose.addEventListener("click", closeDossierLayer);
      });
  }

  function openDossierForHotel() {
    var hotel = state.hotel;
    if (!hotel) return;
    var recordId = hotel.id || hotel.recordId;
    var body = document.getElementById("hexDossierBody");
    var layer = document.getElementById("hexDossierLayer");
    if (!body || !layer) return;
    state.dossierOpen = true;
    layer.hidden = false;
    layer.classList.add("is-open");
    var rootEl = document.getElementById("hexRoot");
    if (rootEl) rootEl.classList.add("hex-root--dossier-open");
    body.innerHTML = '<p class="hex-dossier-loading">Loading Intelligence Dossier…</p>';
    if (!recordId || String(recordId).indexOf("rec") !== 0) {
      bindDossierLibrary(body, [], {
        status: "NOT_STARTED",
        title: "Full Hotel Intelligence Investigation",
        description:
          "Conduct a comprehensive investigation of the property's ownership, corporate structure, operator and brand relationships, history, key people, portfolio connections, material changes and development implications.",
      });
      return;
    }
    fetch("/api/hotel-intelligence/hotels/" + encodeURIComponent(recordId) + "/dossiers")
      .then(function (r) {
        return r.json();
      })
      .then(function (payload) {
        if (!state.dossierOpen) return;
        state.dossierList = (payload && payload.dossiers) || [];
        var count = Number((payload && payload.count) || state.dossierList.length || 0);
        if (payload && payload.dossier) {
          renderDossierReport(payload.dossier);
          return;
        }
        if (count === 1 && state.dossierList[0] && state.dossierList[0].dossier_id) {
          fetch(
            "/api/hotel-intelligence/dossiers/" +
              encodeURIComponent(state.dossierList[0].dossier_id)
          )
            .then(function (r) {
              return r.json();
            })
            .then(function (res) {
              if (!state.dossierOpen) return;
              if (res && res.dossier) renderDossierReport(res.dossier);
              else bindDossierLibrary(body, state.dossierList, (payload && payload.empty_state) || undefined);
            })
            .catch(function (err) {
              console.warn("[HotelExplorer] dossier fetch failed", err);
              bindDossierLibrary(body, state.dossierList, (payload && payload.empty_state) || undefined);
            });
          return;
        }
        bindDossierLibrary(body, state.dossierList, (payload && payload.empty_state) || undefined);
      })
      .catch(function (err) {
        console.warn("[HotelExplorer] dossier list failed", err);
        body.innerHTML =
          '<div class="hid-library-wrap"><button type="button" class="bas-btn bas-btn-secondary" data-hid-close>← Back to Hotel Explorer</button>' +
          '<p class="hex-dossier-loading">Could not load dossiers. Retry Deep Research.</p></div>';
        var failClose = body.querySelector("[data-hid-close]");
        if (failClose) failClose.addEventListener("click", closeDossierLayer);
      });
  }

  function bindDossierLibrary(body, list, emptyState) {
    if (!window.HotelIntelligenceDossier) {
      body.innerHTML = '<p class="hex-dossier-loading">Dossier renderer failed to load.</p>';
      return;
    }
    body.innerHTML =
      '<div class="hid-library-wrap"><button type="button" class="bas-btn bas-btn-secondary hid-library-back" data-hid-close>← Back to Hotel Explorer</button>' +
      window.HotelIntelligenceDossier.renderLibrary(list || [], { emptyState: emptyState }) +
      "</div>";
    var back = body.querySelector("[data-hid-close]");
    if (back) back.addEventListener("click", closeDossierLayer);
    body.querySelectorAll("[data-hid-open]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var id = btn.getAttribute("data-hid-open");
        fetch("/api/hotel-intelligence/dossiers/" + encodeURIComponent(id))
          .then(function (r) {
            return r.json();
          })
          .then(function (res) {
            if (res && res.dossier) renderDossierReport(res.dossier);
          })
          .catch(function (err) {
            console.warn("[HotelExplorer] dossier fetch failed", err);
          });
      });
    });
  }

  function closeDrawer() {
    var drawer = document.getElementById("hexDrawer");
    if (!drawer) return;
    drawer.hidden = true;
    drawer.classList.remove("is-open");
    drawer.innerHTML = "";
  }

  function openDrawer(idx) {
    var cites =
      (window.HotelIntelligenceWorkspace && window.HotelIntelligenceWorkspace.getCitations()) || [];
    var item = cites[Number(idx) - 1] || cites[0];
    var drawer = document.getElementById("hexDrawer");
    if (!drawer || !item) return;
    drawer.hidden = false;
    drawer.classList.add("is-open");
    drawer.innerHTML =
      '<button type="button" class="hex-drawer-close" data-hex-drawer-close aria-label="Close evidence">&times;</button>' +
      "<h4>Evidence</h4><p><strong>" +
      esc(item.title) +
      "</strong></p>" +
      (item.provider ? "<p>Publisher: " + esc(item.provider) + "</p>" : "") +
      (item.observed_date ? "<p>Date: " + esc(item.observed_date) + "</p>" : "") +
      (item.excerpt ? "<p>" + esc(item.excerpt) + "</p>" : "") +
      (item.claim ? "<p>Claim supported: " + esc(item.claim) + "</p>" : "") +
      (item.url ? '<p><a href="' + esc(item.url) + '" target="_blank" rel="noopener">Open source</a></p>' : "");
  }

  function setTab(tabId) {
    if (!TABS.some(function (t) {
      return t.id === tabId;
    }))
      return;
    state.tab = tabId;
    paint();
    var scroll = document.querySelector("#hexRoot .hex-scroll");
    if (scroll) scroll.scrollTop = 0;
  }

  function embedRadarIfNeeded() {
    if (state.radarEmbedded) return Promise.resolve();
    if (!window.HotelDetailPanel || typeof window.HotelDetailPanel.embed !== "function") {
      return Promise.resolve();
    }
    var enriched = enrichHotelFromOwnership(state.hotel, state.ownership);
    if (typeof window.HotelDetailPanel.setPresentationOverrides === "function") {
      window.HotelDetailPanel.setPresentationOverrides(presentationOverrides(enriched, state.ownership));
    }
    var host = document.getElementById("hexRadarHost");
    if (!host) return Promise.resolve();
    return window.HotelDetailPanel.embed(host, enriched).then(function (fullHotel) {
      state.radarEmbedded = true;
      if (fullHotel) {
        var censusClean = sealCensusOverlay(fullHotel);
        var prior = enrichHotelFromOwnership(state.hotel, state.ownership);
        var merged = mergeHotelState(prior, censusClean);
        merged = mergeHotelState(merged, presentationOverrides(merged, state.ownership));
        var scale =
          resolveHotelChainScale(merged, state.ownership) ||
          merged.chainScale ||
          merged.chain_scale ||
          "";
        merged.chainScale = scale;
        merged.chain_scale = scale;
        merged.id = state.hotel.id || state.hotel.recordId || merged.id;
        merged.recordId = state.hotel.recordId || state.hotel.id || merged.recordId;
        // Brand: prefer ownership canonical display over census Unknown Brand.
        merged.brand = preferKnown(
          canonicalBrand(state.hotel, state.ownership) || state.hotel.brand,
          merged.brand
        );
        state.hotel = merged;
      }
    });
  }

  function resetExplorerContentHosts() {
    var hiHost = document.getElementById("hexHiHost");
    var radarHost = document.getElementById("hexRadarHost");
    if (hiHost) hiHost.innerHTML = "";
    if (radarHost) {
      radarHost.innerHTML = "";
      radarHost.removeAttribute("data-hex-embedding");
    }
    if (window.HotelDetailPanel && typeof window.HotelDetailPanel.unembed === "function") {
      try {
        window.HotelDetailPanel.unembed();
      } catch (err) {
        console.warn("[HotelExplorer] unembed on reset failed", err);
      }
    }
    window.__hexAreaHotelsCensusCache = null;
    window.__hexAreaHotelsCensusCacheKey = "";
    state.radarEmbedded = false;
  }

  function assertRenderedHotelMatchesRequest(context) {
    var requested = state.requestedHotelId;
    var rendered = recordId(state.hotel);
    if (!requested || !rendered) return true;
    if (String(requested) === String(rendered)) return true;
    // Allow Cambridge dhl alias → census id.
    if (
      window.HotelIntelligenceGoldenDemoRoute &&
      requested === window.HotelIntelligenceGoldenDemoRoute.CAMBRIDGE_DHL &&
      rendered === window.HotelIntelligenceGoldenDemoRoute.CAMBRIDGE_ID
    ) {
      return true;
    }
    console.error("[HotelExplorer] REQUESTED_HOTEL_ID ≠ RENDERED_HOTEL_ID", {
      context: context || "paint",
      requested_hotel_id: requested,
      rendered_hotel_id: rendered,
      rendered_name: state.hotel && state.hotel.name,
    });
    return false;
  }

  function isUnknownDisplayValue(v) {
    var merge = window.DealalityHotelPropertyMerge;
    if (merge && merge.isSentinelText) return merge.isSentinelText(v);
    var s = String(v == null ? "" : v).trim();
    if (!s) return true;
    return /^unknown(\s+hotel)?$/i.test(s) || /^unknown(\s+\w+)?$/i.test(s);
  }

  function preferKnown(seedVal, incoming) {
    var merge = window.DealalityHotelPropertyMerge;
    if (merge && merge.preferText) return merge.preferText(incoming, seedVal) || merge.preferText(seedVal, incoming);
    if (!isUnknownDisplayValue(seedVal) && isUnknownDisplayValue(incoming)) return seedVal;
    if (incoming == null || incoming === "") return seedVal;
    return incoming;
  }

  function mergeHotelState(base, overlay) {
    var merge = window.DealalityHotelPropertyMerge;
    if (merge && merge.mergePropertyFundamentals) {
      return merge.mergePropertyFundamentals(base || {}, overlay || {}, { devWarn: true }).hotel;
    }
    return Object.assign({}, base || {}, overlay || {});
  }

  function sealCensusOverlay(censusHotel) {
    var merge = window.DealalityHotelPropertyMerge;
    if (merge && merge.stripSentinelDefaults) return merge.stripSentinelDefaults(censusHotel);
    return censusHotel;
  }

  function open(hotel, opts) {
    opts = opts || {};
    if (!hotel) return;
    var generation = ++state.openGeneration;
    var requestedId = opts.requestedHotelId || recordId(hotel);
    state.requestedHotelId = requestedId;

    // Drop prior hotel surface before hydrating the requested property.
    closeDossierLayer();
    closeDrawer();
    if (window.HotelIntelligenceResearchCenter && window.HotelIntelligenceResearchCenter.close) {
      window.HotelIntelligenceResearchCenter.close();
    }
    resetExplorerContentHosts();
    clearHotelExplorerChainScaleTheme();

    state.radarSnapshot = snapshotRadar();
    state.hotel = hotel;
    var tab = opts.tab || "hotel";
    if (tab === "intelligence" || tab === "overview") tab = "hotel";
    state.tab = tab;
    state.ownership = null;
    state.org = null;
    state.dossier = null;
    state.dossierList = null;
    state.dossierOpen = false;
    state.open = true;
    state.radarEmbedded = false;
    var root = ensureRoot();
    root.classList.add("is-open");
    root.setAttribute("data-hex-requested-hotel", requestedId || "");
    root.setAttribute("data-hex-rendered-hotel", recordId(hotel) || "");
    setShellExplorerOpen(true);
    try {
      root.focus();
    } catch (err) {
      /* ignore */
    }
    assertRenderedHotelMatchesRequest("open-initial");
    paint();

    fetchOwnership(hotel).then(function (ownership) {
      if (!state.open || generation !== state.openGeneration) return;
      if (recordId(state.hotel) && recordId(state.hotel) !== recordId(hotel)) return;
      state.ownership = ownership;
      var hi = (ownership && ownership.hotel) || {};
      var fromOwnership = {
        rooms: hi.rooms != null && Number(hi.rooms) > 0 ? hi.rooms : null,
        city: hi.city || null,
        market: hi.market || null,
        country: hi.country || null,
        chainScale: hi.chain_scale || hi.chainScale || null,
        chain_scale: hi.chain_scale || hi.chainScale || null,
        website: hi.website || null,
        latitude: hi.latitude != null ? hi.latitude : null,
        longitude: hi.longitude != null ? hi.longitude : null,
        managementCompany: hi.operator || hi.management_company || null,
        name: hi.name || hi.canonical_trading_name || hi.hotel_name || null,
        brand: hi.brand_display || hi.brand || null,
        status: hi.operating_status || hi.status || null,
      };
      var nextHotel = enrichHotelFromOwnership(mergeHotelState(hotel, fromOwnership), ownership);
      // Never let ownership payload swap the census/record identity.
      nextHotel.id = hotel.id || hotel.recordId || nextHotel.id;
      nextHotel.recordId = hotel.recordId || hotel.id || nextHotel.recordId;
      nextHotel = mergeHotelState(hotel, nextHotel);
      state.hotel = nextHotel;
      if (root) root.setAttribute("data-hex-rendered-hotel", recordId(state.hotel) || "");
      if (!assertRenderedHotelMatchesRequest("after-ownership")) return;
      if (window.HotelIntelligenceWorkspace && window.HotelIntelligenceWorkspace.renderTabHtml) {
        window.HotelIntelligenceWorkspace.renderTabHtml("overview", state.hotel, state.ownership, state.org);
      }
      paint();
      return fetchOrg(orgSlug(ownership)).then(function (org) {
        if (!state.open || generation !== state.openGeneration) return;
        state.org = org;
        paint();
        return embedRadarIfNeeded().then(function () {
          if (!state.open || generation !== state.openGeneration) return;
          if (!assertRenderedHotelMatchesRequest("after-embed")) return;
          paint();
        });
      });
    });
  }

  function close() {
    closeDossierLayer();
    closeDrawer();
    clearHotelExplorerChainScaleTheme();
    if (window.HotelIntelligenceResearchCenter && window.HotelIntelligenceResearchCenter.close) {
      window.HotelIntelligenceResearchCenter.close();
    }
    var root = document.getElementById("hexRoot");
    if (root) root.classList.remove("is-open");
    state.open = false;
    setShellExplorerOpen(false);
    if (window.HotelDetailPanel && typeof window.HotelDetailPanel.unembed === "function") {
      window.HotelDetailPanel.unembed();
    }
    state.radarEmbedded = false;
    clearDeepLink();
    restoreRadar(state.radarSnapshot);
  }

  function onKeydown(e) {
    if (e.key !== "Escape" || !state.open) return;
    var drawer = document.getElementById("hexDrawer");
    if (drawer && drawer.classList.contains("is-open")) {
      closeDrawer();
      return;
    }
    if (state.dossierOpen) {
      closeDossierLayer();
      return;
    }
    close();
  }

  document.addEventListener("keydown", onKeydown);

  window.HotelExplorer = {
    open: open,
    close: close,
    setTab: setTab,
    openDossierById: openDossierById,
    openDossierForHotel: openDossierForHotel,
    openDeepResearch: openDeepResearch,
    resolveHotelChainScale: resolveHotelChainScale,
    applyChainScaleTheme: applyHotelExplorerChainScaleTheme,
    getRequestedHotelId: function () {
      return state.requestedHotelId;
    },
    getRenderedHotelId: function () {
      return recordId(state.hotel);
    },
  };

  window.__hexOpenDossierById = openDossierById;

  window.__DEALALITY_HOTEL_EXPLORER_PRIMARY = true;
})();
