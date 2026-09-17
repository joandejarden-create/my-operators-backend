(function () {
  "use strict";

  var UI = window.DealalityGdiUi;
  if (!UI) throw new Error("DealalityGdiUi missing");

  // Pilot default hotelId only (HOTEL_SPECIFIC_DATA). Display names come from profile/API.
  var DEFAULT_HOTEL_ID = "recLuxvwwxID7U2B8";
  var root = document.getElementById("gdiRoot");
  var loading = document.getElementById("gdiLoading");
  var drawer = document.getElementById("gdiDrawer");
  var drawerTitle = document.getElementById("gdiDrawerTitle");
  var drawerBody = document.getElementById("gdiDrawerBody");
  var drawerClose = document.getElementById("gdiDrawerClose");

  var STORAGE_PREFIX = "gdi-auth-";
  function resolveInitialHotelId() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var fromQuery = String(params.get("hotelId") || "").trim();
      if (fromQuery) return fromQuery;
      var fromStore = String(localStorage.getItem(STORAGE_PREFIX + "hotelId") || "").trim();
      if (fromStore) return fromStore;
    } catch (e) {
      /* ignore */
    }
    return DEFAULT_HOTEL_ID;
  }
  var state = {
    hotelId: resolveInitialHotelId(),
    hotels: [],
    hotelProfile: null,
    summary: null,
    opportunities: [],
    runs: [],
    tab: "opportunities",
    sortKey: "priority",
    sortDir: 1,
    viewMode: "tiles",
    filters: { priority: "", segment: "", booking: "", territory: "" },
    isAdmin: false,
    flag: null,
  };

  function readPersistedBrowse() {
    try {
      var v = sessionStorage.getItem(STORAGE_PREFIX + "viewMode");
      if (v === "list" || v === "tiles") state.viewMode = v;
      var s = sessionStorage.getItem(STORAGE_PREFIX + "sortKey");
      if (s) state.sortKey = s;
    } catch (e) {
      /* ignore */
    }
  }

  function persistBrowse() {
    try {
      sessionStorage.setItem(STORAGE_PREFIX + "viewMode", state.viewMode);
      sessionStorage.setItem(STORAGE_PREFIX + "sortKey", state.sortKey);
    } catch (e) {
      /* ignore */
    }
  }

  function resetBrowseView() {
    state.filters = { priority: "", segment: "", booking: "", territory: "" };
    state.sortKey = "priority";
    state.sortDir = 1;
    state.viewMode = "tiles";
    try {
      sessionStorage.removeItem(STORAGE_PREFIX + "viewMode");
      sessionStorage.removeItem(STORAGE_PREFIX + "sortKey");
    } catch (e) {
      /* ignore */
    }
    render();
  }

  function propertyBarActionsHtml() {
    var resetBtn =
      '<button type="button" class="btn-clear" id="gdiResetViewBtn">Reset View</button>';
    if (memberFetch) {
      return (
        '<button type="button" class="aiv-btn-apply" id="gdiRunBtn">Run Research</button>' +
        resetBtn
      );
    }
    return (
      '<span class="gdi-muted gdi-property-bar__hint">Sign in for research writes</span>' +
      resetBtn
    );
  }

  readPersistedBrowse();

  // Prefer plain fetch for GDI pilot reads (PILOT_READ allows unauth).
  // Memberstack is used only when a session exists and for admin actions.
  function plainFetch(url, opts) {
    return fetch(url, opts);
  }

  var memberFetch =
    window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.fetchMyDealsApi
      ? window.DealalityMemberstackAuth.fetchMyDealsApi.bind(window.DealalityMemberstackAuth)
      : null;

  function fetchFn(url, opts) {
    opts = opts || {};
    var method = String(opts.method || "GET").toUpperCase();
    var needsAuthWrite = method !== "GET" && method !== "HEAD";
    if (needsAuthWrite && memberFetch) return memberFetch(url, opts);
    return plainFetch(url, opts).then(function (r) {
      if (r.status === 401 && memberFetch) return memberFetch(url, opts);
      return r;
    });
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtVal(v) {
    if (v == null || v === "" || v === "UNKNOWN") return "—";
    return v;
  }

  function formatDate(iso) {
    if (!iso) return "Never";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    return d.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function applyEmbedMode() {
    var params = new URLSearchParams(window.location.search || "");
    var inFrame = false;
    try {
      inFrame = window !== window.top;
    } catch (e) {
      inFrame = true;
    }
    if (inFrame || params.get("embed") === "1") {
      document.body.classList.add("embed-mode");
    }
  }

  function showError(msg) {
    if (loading) loading.style.display = "none";
    root.innerHTML =
      '<div class="gdi-error" role="alert"><p>' +
      esc(msg) +
      "</p><p>Local servers enable pilot read automatically. On production set GROUP_DEMAND_INTELLIGENCE_V1=1. Admin actions still require sign-in.</p></div>";
  }

  function priorityPill(p) {
    return UI.priorityPill(p);
  }

  function countEnteringWindow() {
    return state.opportunities.filter(function (o) {
      return (
        o.bookingWindowStatus === "CONTACT_NOW" ||
        o.bookingWindowStatus === "QUALIFY_NOW" ||
        o.bookingWindowStatus === "RESEARCH_FURTHER" ||
        (o.labels || []).indexOf("entering_booking_window") >= 0
      );
    }).length;
  }

  function qualifiedCount() {
    var s = state.summary || {};
    if (s.qualifiedCount != null) return s.qualifiedCount;
    return state.opportunities.length;
  }

  function oppById(id) {
    for (var i = 0; i < state.opportunities.length; i++) {
      if (state.opportunities[i].id === id) return state.opportunities[i];
    }
    return null;
  }

  function contactLabel(contact) {
    if (!contact) return "—";
    if (contact.name && contact.name !== "UNKNOWN") {
      return contact.name + (contact.role ? " (" + contact.role + ")" : "");
    }
    if (contact.email) return contact.email;
    return "—";
  }

  function whoShouldSalesContactHtml(o) {
    var c = o.primaryContact;
    if (!c) {
      return "<p>No usable contact resolved yet.</p>";
    }
    var backups = (o.backupContacts || [])
      .map(function (b) {
        return (
          "<li><strong>" +
          esc(b.name || "—") +
          "</strong> · " +
          esc(b.role || "") +
          " · " +
          esc(b.email || "") +
          (b.phone ? " · " + esc(b.phone) : "") +
          (b.whyThisContact ? "<br/><em>" + esc(b.whyThisContact) + "</em>" : "") +
          "</li>"
        );
      })
      .join("");
    return (
      "<p><strong>" +
      esc(c.name || "—") +
      "</strong></p>" +
      "<p>Title: " +
      esc(c.title || c.role || "—") +
      "</p>" +
      "<p>Relationship to event: " +
      esc(c.relationshipToEvent || o.relationshipToEvent || "—") +
      "</p>" +
      "<p><strong>Why this contact:</strong> " +
      esc(c.whyThisContact || o.whyThisContact || "—") +
      "</p>" +
      "<p>Email: " +
      esc(c.email || "—") +
      " · " +
      esc(c.emailVerificationStatusLabel || c.emailVerificationStatus || "") +
      "</p>" +
      "<p>Phone: " +
      esc(c.phone || "—") +
      (c.phoneTypeLabel || c.phoneType
        ? " (" + esc(c.phoneTypeLabel || c.phoneType) + ")"
        : "") +
      "</p>" +
      "<p>Contact Quality: <strong>" +
      esc(o.contactGradeLabel || c.contactGradeLabel || o.contactQualityLabel || "—") +
      "</strong> · Contact Confidence: <strong>" +
      esc(o.contactConfidence ?? c.contactConfidence ?? "—") +
      "</strong></p>" +
      "<p>Last verified: " +
      esc(c.lastVerifiedAt || "—") +
      "</p>" +
      (backups
        ? "<h4>Backup contacts</h4><ul>" + backups + "</ul>"
        : "")
    );
  }

  function trunc(s, n) {
    s = s == null ? "" : String(s);
    if (s.length <= n) return s;
    return s.slice(0, n) + "…";
  }

  function api(path, opts) {
    return fetchFn(path, opts).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          var err = new Error(data.message || data.error || "Request failed");
          err.status = r.status;
          err.data = data;
          throw err;
        }
        return data;
      });
    });
  }

  function clearHotelScopedState() {
    state.hotelProfile = null;
    state.summary = null;
    state.opportunities = [];
    state.runs = [];
    state.filters = { priority: "", segment: "", booking: "", territory: "" };
    state.sortKey = "priority";
    state.sortDir = 1;
    state.viewMode = "tiles";
    state.tab = "opportunities";
    if (typeof drawer !== "undefined" && drawer) {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    }
  }

  function selectHotel(hotelId) {
    var next = String(hotelId || "").trim();
    if (!next || next === state.hotelId) return Promise.resolve();
    state.hotelId = next;
    try {
      localStorage.setItem(STORAGE_PREFIX + "hotelId", next);
      var url = new URL(window.location.href);
      url.searchParams.set("hotelId", next);
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    } catch (e) {
      /* ignore */
    }
    clearHotelScopedState();
    if (loading) loading.style.display = "";
    return loadAll();
  }

  function loadHotelCatalog() {
    return api("/api/group-demand-intelligence/hotels")
      .then(function (payload) {
        state.hotels = Array.isArray(payload.hotels) ? payload.hotels : [];
        var ids = state.hotels.map(function (h) {
          return h.hotelId;
        });
        if (ids.length && ids.indexOf(state.hotelId) === -1) {
          state.hotelId = ids[0];
        }
        return state.hotels;
      })
      .catch(function () {
        state.hotels = [];
        return state.hotels;
      });
  }

  function loadAll() {
    var id = encodeURIComponent(state.hotelId);
    // Critical path: hotel catalog + flag + summary + opportunities + profile.
    // Research-runs are audit-tab only — load after first paint.
    return loadHotelCatalog().then(function () {
      id = encodeURIComponent(state.hotelId);
      return Promise.all([
        api("/api/group-demand-intelligence/flag"),
        api("/api/group-demand-intelligence/hotels/" + id + "/summary"),
        api("/api/group-demand-intelligence/hotels/" + id + "/opportunities"),
        api("/api/group-demand-intelligence/hotels/" + id + "/profile").catch(function () {
          return { ok: false, profile: null };
        }),
      ]).then(function (parts) {
        state.flag = parts[0].flag;
        state.summary = parts[1].summary;
        state.opportunities = parts[2].opportunities || [];
        state.hotelProfile = (parts[3] && parts[3].profile) || null;
        render();
        return api("/api/group-demand-intelligence/hotels/" + id + "/research-runs")
          .then(function (runsPayload) {
            state.runs = runsPayload.runs || [];
            if (state.tab === "audit") render();
          })
          .catch(function () {
            state.runs = [];
          });
      });
    });
  }

  function runResearch() {
    if (
      !window.confirm(
        "Run Group Demand research for this hotel? This does not auto-spend Webhound unless configured in the run payload."
      )
    ) {
      return;
    }
    if (loading) {
      loading.style.display = "block";
      loading.textContent = "Running research…";
    }
    api("/api/group-demand-intelligence/hotels/" + encodeURIComponent(state.hotelId) + "/research/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    })
      .then(function () {
        return loadAll();
      })
      .catch(function (err) {
        showError(err.message || "Research run failed");
      });
  }

  function filteredSorted() {
    return UI.sortOpportunities(
      UI.filterOpportunities(state.opportunities, state.filters),
      state.sortKey,
      state.sortDir
    );
  }

  function applyTilePairFilter(priority, booking, e) {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    state.filters.priority = priority || "";
    state.filters.booking = booking || "";
    render();
  }

  function wireBrowseControls() {
    root.querySelectorAll("[data-gdi-priority]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.priority = btn.getAttribute("data-gdi-priority") || "";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "priority"
        );
        render();
      });
    });
    root.querySelectorAll("[data-gdi-booking]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.booking = btn.getAttribute("data-gdi-booking") || "";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "booking"
        );
        render();
      });
    });
    root.querySelectorAll("[data-gdi-tile-priority][data-gdi-tile-booking]").forEach(
      function (btn) {
        btn.addEventListener("click", function (e) {
          applyTilePairFilter(
            btn.getAttribute("data-gdi-tile-priority"),
            btn.getAttribute("data-gdi-tile-booking"),
            e
          );
        });
      }
    );
    var sortSel = document.getElementById("gdiSortSelect");
    if (sortSel) {
      sortSel.addEventListener("change", function () {
        state.sortKey = sortSel.value || "priority";
        state.sortDir = 1;
        persistBrowse();
        render();
      });
    }
    var sortDirBtn = document.getElementById("gdiSortDirBtn");
    if (sortDirBtn) {
      sortDirBtn.addEventListener("click", function () {
        state.sortDir = state.sortDir === 1 ? -1 : 1;
        render();
      });
    }
    root.querySelectorAll("[data-gdi-view]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.viewMode = btn.getAttribute("data-gdi-view") === "tiles" ? "tiles" : "list";
        persistBrowse();
        render();
      });
    });
  }

  function renderOpportunityCards(rows) {
    if (!rows.length) {
      return '<div class="gdi-empty">No opportunities match the current filters. Run research or clear filters.</div>';
    }
    return UI.opportunityCardsGridHtml(rows, state.viewMode, state.filters);
  }

  function renderBrowseBody(opts) {
    opts = opts || {};
    var rows = filteredSorted();
    var total = state.opportunities.length;
    var chrome = UI.opportunityBrowseChromeHtml({
      activePriority: state.filters.priority,
      priorityCounts: UI.facetCountByPriority(state.opportunities, state.filters),
      activeBooking: state.filters.booking,
      actionCounts: UI.facetCountByActionStatus(state.opportunities, state.filters),
      shown: rows.length,
      total: total,
      sort: state.sortKey,
      viewMode: state.viewMode,
      noun: opts.noun || "Opportunities",
    });
    return chrome + (opts.lede || "") + renderOpportunityCards(rows);
  }

  function humanizeToken(value) {
    if (!value) return "";
    return String(value)
      .replace(/_/g, " ")
      .toLowerCase()
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
  }

  function formatShortDate(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return "";
      return d.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    } catch (e) {
      return "";
    }
  }

  function eventStamp(ev) {
    if (!ev) return "";
    return (
      ev.validatedAt ||
      ev.reportedAt ||
      ev.completedAt ||
      ev.actionDate ||
      ev.outcomeDate ||
      ev.createdAt ||
      ""
    );
  }

  /** Prefer latest action/outcome label over raw lifecycle enum for customer UI. */
  function deriveDisplayStage(current) {
    if (!current) return null;
    if (current.latestOutcome && current.latestOutcome.outcomeType) {
      return humanizeToken(current.latestOutcome.outcomeType);
    }
    if (current.latestAction && current.latestAction.actionType) {
      return humanizeToken(current.latestAction.actionType);
    }
    if (current.decisionLifecycleStage) {
      return humanizeToken(current.decisionLifecycleStage);
    }
    return null;
  }

  function renderLifecycleSummary(current) {
    if (!current) return "";
    var stage = deriveDisplayStage(current);
    var bits = [];
    if (stage) {
      bits.push(
        '<p class="gdi-lifecycle-stage">Current stage: <strong>' +
          esc(stage) +
          "</strong></p>"
      );
    }
    var hist = [];
    if (current.latestAction && current.latestAction.actionType) {
      var actionLine =
        "Last action: " + humanizeToken(current.latestAction.actionType);
      var ad = formatShortDate(eventStamp(current.latestAction));
      if (ad) actionLine += " · " + ad;
      hist.push(actionLine);
    }
    if (current.latestOutcome && current.latestOutcome.outcomeType) {
      hist.push(
        "Latest outcome: " + humanizeToken(current.latestOutcome.outcomeType)
      );
    } else if (current.latestAction) {
      hist.push("Latest outcome: Pending");
    }
    if (hist.length) {
      bits.push(
        '<p class="gdi-lifecycle-history">' +
          hist.map(function (line) {
            return esc(line);
          }).join("<br>") +
          "</p>"
      );
    }
    if (!bits.length) return "";
    return '<div class="gdi-lifecycle-summary" id="gdiLifecycleSummary">' + bits.join("") + "</div>";
  }

  function updateLifecycleSummary(current) {
    var el = document.getElementById("gdiLifecycleSummary");
    if (!el) return;
    var html = renderLifecycleSummary(current);
    if (!html) return;
    var tmp = document.createElement("div");
    tmp.innerHTML = html;
    var next = tmp.firstChild;
    if (next) el.replaceWith(next);
  }

  function openDetail(id) {
    var oppUrl =
      "/api/group-demand-intelligence/hotels/" +
      encodeURIComponent(state.hotelId) +
      "/opportunities/" +
      encodeURIComponent(id);
    var decisionUrl =
      "/api/hotels/" +
      encodeURIComponent(state.hotelId) +
      "/subjects/" +
      encodeURIComponent(id) +
      "/decision?module=GDI";

    Promise.all([
      api(oppUrl),
      api(decisionUrl).catch(function () {
        return null;
      }),
    ]).then(function (results) {
      var data = results[0];
      var decisionBundle = results[1];
      var o = data.opportunity;
      var current =
        (decisionBundle && decisionBundle.current) ||
        (decisionBundle &&
          decisionBundle.decision &&
          decisionBundle.current) ||
        null;
      drawerTitle.textContent = o.title;
      drawerBody.innerHTML = renderDetail(o, data.scoreAudit, current);
      wireDetailLifecycleControls();
      if (typeof drawer.showModal === "function") drawer.showModal();
      else drawer.setAttribute("open", "open");
    });
  }

  function wireDetailLifecycleControls() {
    var outcomeEl = document.getElementById("gdiDoOutcome");
    var lossWrap = document.getElementById("gdiDoLossWrap");
    function syncLossVisibility() {
      if (!lossWrap) return;
      var show = outcomeEl && outcomeEl.value === "LOST";
      lossWrap.hidden = !show;
      if (!show) {
        var lossEl = document.getElementById("gdiDoLoss");
        if (lossEl) lossEl.value = "";
      }
    }
    if (outcomeEl) {
      outcomeEl.addEventListener("change", syncLossVisibility);
      syncLossVisibility();
    }
  }

  function claimLabel(kind) {
    if (kind === "FACT") return "VERIFIED";
    if (kind === "INFERENCE") return "INFERRED";
    if (kind === "ESTIMATED") return "ESTIMATED";
    return kind || "UNKNOWN";
  }

  function fitLabel(key) {
    var map = {
      physicalFit: "Physical Fit",
      geographyFit: "Demand Territory Fit",
      timing: "Timing / Winnability",
      commercialValue: "Commercial Potential",
      historicalFit: "Historical Hotel / Brand Fit",
      competitiveAccessibility: "Competitive Accessibility",
      contactability: "Contactability",
    };
    return map[key] || key;
  }

  function incrementalBadge(o) {
    var s = o.incrementalValueStatus || "";
    if (s === "NEW_TO_HOTEL" || s === "NEW_OPPORTUNITY") {
      return '<span class="gdi-pill gdi-pill-high">NEW TO HOTEL</span>';
    }
    if (s.indexOf("ALREADY_KNOWN") === 0) {
      return '<span class="gdi-pill gdi-pill-med">ALREADY KNOWN</span>';
    }
    if ((o.sourcingStatus || "").indexOf("HOTEL_CONFIRMED_ALREADY") === 0) {
      return '<span class="gdi-pill gdi-pill-watch">ALREADY SOURCED</span>';
    }
    return "";
  }

  function renderDetail(o, audit, decisionCurrent) {
    var hist = (o.meetingHistory || [])
      .map(function (h) {
        return "<li>" + esc(h.year) + " — " + esc(h.city) + " (" + esc(claimLabel(h.claimKind)) + ")</li>";
      })
      .join("");
    var comps = (audit && audit.hotelFit && audit.hotelFit.components) || {};
    var labels = (o.hotelFitComponentLabels) || {};
    var sources = (o.sources || [])
      .map(function (s) {
        var link = s.url
          ? '<a href="' + esc(s.url) + '" target="_blank" rel="noopener">' + esc(s.name) + "</a>"
          : esc(s.name);
        return (
          "<li>" +
          link +
          (s.sourceType ? " · " + esc(s.sourceType) : "") +
          (s.supportsFact ? " — supports " + esc(s.supportsFact) : "") +
          (s.date ? " · " + esc(s.date) : "") +
          "</li>"
        );
      })
      .join("");
    var verified = ((o.knownVsEstimated && o.knownVsEstimated.verified) || [])
      .map(function (x) {
        return "<li><strong>" + esc(x.field) + ":</strong> " + esc(x.value) + " <em>(" + esc(x.status) + ")</em></li>";
      })
      .join("");
    var estimated = ((o.knownVsEstimated && o.knownVsEstimated.estimated) || [])
      .map(function (x) {
        return "<li><strong>" + esc(x.field) + ":</strong> " + esc(x.value) + " <em>(" + esc(x.status) + ")</em></li>";
      })
      .join("");
    var inferred = ((o.knownVsEstimated && o.knownVsEstimated.inferred) || [])
      .map(function (x) {
        return "<li><strong>" + esc(x.field) + ":</strong> " + esc(x.value) + " <em>(" + esc(x.status) + ")</em></li>";
      })
      .join("");
    var compsList = (o.competitors || [])
      .map(function (c) {
        return (
          "<li><strong>" +
          esc(c.name) +
          "</strong> · " +
          esc(c.classLabel || c.class) +
          "<br/><span class='gdi-meta-value'>" +
          esc(c.reason || "") +
          "</span></li>"
        );
      })
      .join("");

    return (
      '<div class="gdi-section"><h3>1. Opportunity Summary</h3><p>' +
      esc(o.summaryWhat) +
      "</p><p>" +
      esc(o.organizationName) +
      " · " +
      esc(o.segment) +
      " · " +
      esc(o.eventStartDate || "TBD") +
      " → " +
      esc(o.eventEndDate || "TBD") +
      "</p></div>" +
      '<div class="gdi-section"><h3>2. Opportunity Type</h3><p><strong>' +
      esc(o.opportunityTypeLabel || o.opportunityType || "—") +
      "</strong></p></div>" +
      '<div class="gdi-section"><h3>3. Venue / Sourcing Status</h3><p><strong>' +
      esc(o.venueSourcingStatusLabel || o.venueSourcingStatus || "—") +
      "</strong></p><p>" +
      esc(o.venueSourcingRationale || "") +
      "</p><p>Venue note: " +
      esc(o.venueStatus || "—") +
      "</p></div>" +
      '<div class="gdi-section"><h3>4. Hotel Opportunity Thesis</h3><p>' +
      esc(o.hotelOpportunityThesis || o.bethesdaWinThesis || "—") +
      "</p></div>" +
      '<div class="gdi-section"><h3>5. Room Demand</h3><p><strong>' +
      esc(o.roomDemandStatusLabel || o.roomDemandStatus || "—") +
      "</strong>" +
      (o.roomDemandConfidence != null ? " · confidence " + esc(o.roomDemandConfidence) : "") +
      "</p><p>" +
      esc(o.roomDemandRationale || "") +
      "</p><p>Attendees: " +
      esc(fmtVal(o.estimatedAttendance)) +
      " · Peak rooms: " +
      esc(fmtVal(o.estimatedPeakRooms)) +
      " · Published peak: " +
      esc(fmtVal(o.publishedPeakRooms)) +
      "</p></div>" +
      '<div class="gdi-section"><h3>6. Why This Hotel?</h3><p>' +
      esc(o.summaryWhyHotel || o.fitExplanation) +
      "</p>" +
      (o.hotelOpportunityThesis || o.bethesdaWinThesis
        ? "<p><strong>Hotel opportunity thesis:</strong> " +
          esc(o.hotelOpportunityThesis || o.bethesdaWinThesis) +
          "</p>"
        : "") +
      "</div>" +
      '<div class="gdi-section"><h3>7. Why Now?</h3><p><strong>' +
      esc(o.bookingWindowLabel || o.bookingWindowStatus) +
      "</strong></p><p>" +
      esc(o.whyNow) +
      "</p></div>" +
      '<div class="gdi-section"><h3>8. Hotel Fit</h3>' +
      '<div class="gdi-score-grid">' +
      '<div class="gdi-score-cell"><div class="n">' +
      esc(o.hotelFitScore) +
      '</div><div class="l">Hotel Fit</div></div>' +
      Object.keys(comps)
        .map(function (k) {
          return (
            '<div class="gdi-score-cell"><div class="n">' +
            esc(comps[k]) +
            '</div><div class="l">' +
            esc(labels[k] || fitLabel(k)) +
            "</div></div>"
          );
        })
        .join("") +
      "</div><p class='gdi-lede'>" +
      esc((audit && audit.hotelFit && audit.hotelFit.explanation) || o.fitExplanation || "") +
      "</p><p class='gdi-lede'>Hotel Fit answers whether the hotel could serve the business — not whether the opportunity is still open.</p></div>" +
      '<div class="gdi-section"><h3>9. Opportunity Qualification</h3><p><strong>' +
      esc(o.opportunityQualificationLabel || o.opportunityQualification || "—") +
      "</strong></p><p>" +
      esc(o.qualificationNotes || "") +
      "</p>" +
      (o.qualificationFailureReasonLabel
        ? "<p>Failure reason: " + esc(o.qualificationFailureReasonLabel) + "</p>"
        : "") +
      "</div>" +
      '<div class="gdi-section"><h3>10. Evidence Confidence <span class="gdi-help" title="' +
      esc(o.evidenceConfidenceTooltip || "How confident Dealality is in the facts underlying the opportunity based on source quality, recency, corroboration and how much is verified versus inferred.") +
      '">?</span></h3><div class="gdi-score-grid"><div class="gdi-score-cell"><div class="n">' +
      esc(o.evidenceConfidence) +
      '</div><div class="l">Evidence Confidence</div></div></div><p>' +
      esc(o.evidenceConfidenceExplanation || (audit && audit.evidenceConfidenceExplanation) || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>11. Event Location</h3><p><strong>' +
      esc(o.eventLocationStatusLabel || o.eventLocationStatus || "—") +
      "</strong></p><p>" +
      esc(o.eventLocationSummary || o.destinationStatus || "—") +
      "</p><p>Demand Territory: " +
      esc(o.demandTerritoryFitLabel || "—") +
      "</p><p>" +
      esc(o.demandTerritoryRationale || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>12. Meeting / Venue History</h3><ul>' +
      (hist || "<li>None captured</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>13. Reactivation Signals</h3><p><strong>' +
      esc(o.reactivationSignalLabel || o.reactivationSignal || "Unknown") +
      "</strong></p><p>" +
      esc(o.reactivationThesis || "No reactivation thesis.") +
      "</p></div>" +
      '<div class="gdi-section"><h3>14. Competitive Context</h3><p><strong>Likely STR competitor:</strong> ' +
      esc(o.likelyStrCompetitor || "—") +
      "</p><p><strong>Likely group alternative:</strong> " +
      esc(o.likelyGroupCompetitor || "—") +
      "</p><p>" +
      esc(o.competitorRationale || "") +
      "</p><ul>" +
      (compsList || "<li>See hotel STR set and group alternatives in configuration.</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>15. Who Should Sales Contact?</h3>' +
      whoShouldSalesContactHtml(o) +
      "</div>" +
      '<div class="gdi-section"><h3>16. Sources</h3><ul>' +
      (sources || "<li>No sources listed.</li>") +
      "</ul><h4>What we know</h4><ul>" +
      (verified || "<li>See evidence table for verified fields.</li>") +
      "</ul><h4>Estimated</h4><ul>" +
      (estimated || "<li>No estimated fields highlighted.</li>") +
      "</ul><h4>Inferred</h4><ul>" +
      (inferred || "<li>No inferred fields highlighted.</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>17. Recommended Action</h3><p>' +
      esc(o.recommendedAction) +
      "</p><p>" +
      esc(o.summaryWhyMatters || "") +
      "</p></div>" +
      '<div class="gdi-section gdi-section--validation gdi-validation-panel gdi-hotel-feedback">' +
      "<h3>Hotel Validation</h3>" +
      renderLifecycleSummary(decisionCurrent) +
      '<p class="gdi-lede">Your feedback helps track this opportunity and improve future recommendations.</p>' +
      '<div class="gdi-lifecycle-step" data-gdi-validation-mode="AUTH_FULL">' +
      "<h4>Validation</h4>" +
      "<p class=\"gdi-lifecycle-q\">Is this opportunity useful to your hotel?</p>" +
      '<div class="gdi-feedback">' +
      '<label>Familiarity<select id="gdiFbFamiliarity"><option value="">Select…</option><option>Never Seen</option><option>Familiar</option><option>Already Received</option><option>Already Pursuing</option></select></label>' +
      '<label>Commercial Status<select id="gdiFbCommercial"><option value="">Select…</option><option>Worth Pursuing</option><option>Not a Fit</option><option>Already Lost</option><option>Already Won</option><option>Already Booked Elsewhere</option></select></label>' +
      '<label>Value<select id="gdiFbValue"><option value="">Select…</option><option>Excellent</option><option>Useful</option><option>Marginal</option><option>No Incremental Value</option></select></label>' +
      '<label>Incremental?<select id="gdiFbIncremental"><option value="">Select…</option><option value="NEW_TO_HOTEL">New to hotel</option><option value="ALREADY_KNOWN_USEFUL_ADDITIONAL">Already known / useful additional</option><option value="ALREADY_KNOWN_NO_INCREMENTAL">Already known / no incremental value</option><option value="DUPLICATE_OF_EXISTING_SALES_LEAD">Duplicate of existing sales lead</option><option value="NEW_TIMING_CONTACT_COMPETITIVE_INTEL">New timing / contact / competitive intel</option></select></label>' +
      '<label>Qualification failure<select id="gdiFbFailReason"><option value="">Select…</option><option value="VENUE_ALREADY_SELECTED">Venue already selected</option><option value="HOTEL_ALREADY_SELECTED">Hotel already selected</option><option value="NO_OVERFLOW_OPPORTUNITY">No overflow opportunity</option><option value="EVENT_LOCATION_POOR_FIT">Event location poor fit</option><option value="ROOM_DEMAND_TOO_SMALL">Room demand too small</option><option value="MOST_ATTENDEES_LOCAL">Most attendees local</option><option value="WRONG_EVENT_CYCLE">Wrong event cycle</option><option value="CONTACT_NOT_RELEVANT">Contact not relevant</option><option value="DUPLICATE">Duplicate</option><option value="TOO_EARLY">Too early</option><option value="TOO_LATE">Too late</option><option value="WEAK_EVIDENCE">Weak evidence</option><option value="NOT_HOTEL_DEMAND">Not hotel demand</option><option value="OTHER">Other</option></select></label>' +
      '<textarea id="gdiFbComment" placeholder="Optional comment"></textarea>' +
      '<div class="gdi-validation-actions">' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiFbSave" data-id="' +
      esc(o.id) +
      '">Save Hotel Validation</button>' +
      '<span class="gdi-validation-status" id="gdiFbSaveStatus" hidden></span>' +
      "</div></div></div>" +
      '<div class="gdi-lifecycle-step">' +
      "<h4>Action</h4>" +
      "<p class=\"gdi-lifecycle-q\">What did the team do?</p>" +
      '<div class="gdi-feedback">' +
      '<label>Action<select id="gdiDoAction"><option value="">Select action...</option>' +
      '<option value="NOT_CONTACTED">Not Contacted</option>' +
      '<option value="PLANNED_TO_CONTACT">Planned To Contact</option>' +
      '<option value="CONTACTED">Contacted</option>' +
      '<option value="FOLLOW_UP_REQUIRED">Follow Up Required</option>' +
      '<option value="RFP_REQUESTED">RFP Requested</option>' +
      '<option value="RFP_RECEIVED">RFP Received</option>' +
      '<option value="SITE_VISIT_REQUESTED">Site Visit Requested</option>' +
      '<option value="SITE_VISIT_COMPLETED">Site Visit Completed</option>' +
      '<option value="PROPOSAL_SUBMITTED">Proposal Submitted</option>' +
      '<option value="NEGOTIATING">Negotiating</option>' +
      '<option value="NO_ACTION">No Action</option></select></label>' +
      '<p class="gdi-lede">Add when there is an update.</p>' +
      '<div class="gdi-validation-actions">' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiDoActionSave" data-id="' +
      esc(o.id) +
      '">Save Action</button>' +
      '<span class="gdi-validation-status" id="gdiDoActionSaveStatus" hidden></span>' +
      "</div></div></div>" +
      '<div class="gdi-lifecycle-step">' +
      "<h4>Outcome</h4>" +
      "<p class=\"gdi-lifecycle-q\">What happened?</p>" +
      '<div class="gdi-feedback">' +
      '<label>Outcome<select id="gdiDoOutcome"><option value="">Select outcome...</option>' +
      '<option value="WON">Won</option>' +
      '<option value="LOST">Lost</option>' +
      '<option value="BOOKED">Booked</option>' +
      '<option value="NO_RESPONSE">No Response</option>' +
      '<option value="NOT_QUALIFIED">Not Qualified</option>' +
      '<option value="OPPORTUNITY_CLOSED">Opportunity Closed</option>' +
      '<option value="DEFERRED">Deferred</option>' +
      '<option value="UNKNOWN">Unknown</option></select></label>' +
      '<label id="gdiDoLossWrap" hidden>Loss reason<select id="gdiDoLoss"><option value="">Select loss reason...</option>' +
      '<option value="RATE">Rate</option>' +
      '<option value="AVAILABILITY">Availability</option>' +
      '<option value="LOCATION">Location</option>' +
      '<option value="MEETING_SPACE">Meeting Space</option>' +
      '<option value="BRAND">Brand</option>' +
      '<option value="COMPETITOR">Competitor</option>' +
      '<option value="ROOM_BLOCK">Room Block</option>' +
      '<option value="DATES">Dates</option>' +
      '<option value="NO_RESPONSE">No Response</option>' +
      '<option value="OTHER">Other</option>' +
      '<option value="UNKNOWN">Unknown</option></select></label>' +
      '<div class="gdi-validation-actions">' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiDoOutcomeSave" data-id="' +
      esc(o.id) +
      '">Save Outcome</button>' +
      '<span class="gdi-validation-status" id="gdiDoOutcomeSaveStatus" hidden></span>' +
      "</div></div></div></div>"
    );
  }

  function renderAudit() {
    var latest = state.runs[0];
    if (!latest) {
      return '<div class="gdi-empty">No research runs yet.</div>';
    }
    var cost = latest.cost || {};
    return (
      '<div class="gdi-audit">' +
      "<p>Run <strong>" +
      esc(latest.id) +
      "</strong> · " +
      esc(latest.status) +
      "</p>" +
      "<p>Total cost $" +
      esc(cost.totalUsd || 0) +
      " · Webhound $" +
      esc(cost.webhoundUsd || 0) +
      " / hard cap $" +
      esc(cost.webhoundHardCapUsd || 5) +
      "</p><pre>" +
      esc(JSON.stringify(latest, null, 2)) +
      "</pre></div>"
    );
  }

  function render() {
    if (loading) loading.style.display = "none";
    var s = state.summary || {};

    var body =
      state.tab === "audit"
        ? renderAudit()
        : renderBrowseBody({ noun: "Opportunities" });

    var identity = (state.hotelProfile && state.hotelProfile.identity) || {};
    var hotelName = identity.hotelName || "Hotel";
    var hotel = {
      hotelName: hotelName,
      city: identity.city || null,
      state: identity.state || null,
    };
    root.className = "gdi-shell dashboard-container aiv-dashboard";
    root.innerHTML =
      UI.hotelShellHtml({
        mode: "auth",
        badges: { pilot: true },
      }) +
      UI.contentTabsHtml(state.tab, [
        UI.GDI_MAIN_TAB,
        { id: "audit", label: "Research Audit" },
      ]) +
      UI.propertyBarHtml({
        mode: "auth",
        hotel: hotel,
        hotelId: state.hotelId,
        hotels: state.hotels,
        lastResearch: formatDate(s.lastResearchAt),
        runStatus: s.runStatus || "NEVER_RUN",
        actionHtml: propertyBarActionsHtml(),
      }) +
      '<div id="gdiTabBody">' +
      body +
      "</div>";

    var hotelSelect = document.getElementById("gdiHotel");
    if (hotelSelect) {
      hotelSelect.addEventListener("change", function () {
        selectHotel(hotelSelect.value).catch(function (err) {
          showError(err.message || "Failed to switch hotel");
        });
      });
    }

    var runBtn = document.getElementById("gdiRunBtn");
    if (runBtn) runBtn.addEventListener("click", runResearch);
    var resetBtn = document.getElementById("gdiResetViewBtn");
    if (resetBtn) resetBtn.addEventListener("click", resetBrowseView);

    root.querySelectorAll(".section-nav-item[data-tab]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.tab = btn.getAttribute("data-tab");
        render();
      });
    });

    wireBrowseControls();

    root.querySelectorAll("[data-open]").forEach(function (el) {
      el.addEventListener("click", function (e) {
        var innermost = e.target.closest("[data-open]");
        if (innermost !== el) return;
        openDetail(el.getAttribute("data-open"));
      });
      if (el.tagName === "ARTICLE") {
        el.addEventListener("keydown", function (e) {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            openDetail(el.getAttribute("data-open"));
          }
        });
      }
    });
  }

  applyEmbedMode();

  if (drawerClose) {
    drawerClose.addEventListener("click", function () {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    });
  }

  drawerBody &&
    drawerBody.addEventListener("click", function (e) {
      var t = e.target;

      function showSaveOk(statusId, message) {
        var el = document.getElementById(statusId);
        if (!el) return;
        el.hidden = false;
        el.textContent = message;
      }

      if (t && t.id === "gdiFbSave") {
        var id = t.getAttribute("data-id");
        var familiarityEl = document.getElementById("gdiFbFamiliarity");
        var commercialEl = document.getElementById("gdiFbCommercial");
        var valueEl = document.getElementById("gdiFbValue");
        var incrementalEl = document.getElementById("gdiFbIncremental");
        var failReasonEl = document.getElementById("gdiFbFailReason");
        var comment = document.getElementById("gdiFbComment").value;
        var familiarity = familiarityEl ? familiarityEl.value : "";
        var commercialStatus = commercialEl ? commercialEl.value : "";
        var value = valueEl ? valueEl.value : "";
        var incrementalValueStatus = incrementalEl ? incrementalEl.value : "";
        var qualificationFailureReason = failReasonEl ? failReasonEl.value : "";
        t.disabled = true;
        api(
          "/api/group-demand-intelligence/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/opportunities/" +
            encodeURIComponent(id) +
            "/feedback",
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              familiarity: familiarity || null,
              commercialStatus: commercialStatus || null,
              value: value || null,
              quality: value || null,
              salesOutcome: commercialStatus || "Not Reviewed",
              incrementalValueStatus: incrementalValueStatus || null,
              qualificationFailureReason: qualificationFailureReason || null,
              comment: comment,
            }),
          }
        )
          .then(function () {
            t.disabled = false;
            showSaveOk("gdiFbSaveStatus", "✓ Validation saved");
            // Refresh projection for stage strip (best-effort)
            return api(
              "/api/hotels/" +
                encodeURIComponent(state.hotelId) +
                "/subjects/" +
                encodeURIComponent(id) +
                "/decision?module=GDI"
            ).catch(function () {
              return null;
            });
          })
          .then(function (bundle) {
            if (bundle && bundle.current) updateLifecycleSummary(bundle.current);
          })
          .catch(function (err) {
            t.disabled = false;
            showSaveOk("gdiFbSaveStatus", err.message || "Failed");
          });
      }

      function ensureThenRecord(opportunityId, kind, payload, btn, statusId, okMessage) {
        var opp =
          (state.opportunities || []).find(function (o) {
            return o.id === opportunityId;
          }) || {
            id: opportunityId,
            title: opportunityId,
            priority: "MEDIUM_PRIORITY",
            recommendedAction: "Recorded from GDI detail",
          };
        btn.disabled = true;
        api("/api/hotels/" + encodeURIComponent(state.hotelId) + "/decisions/ensure-gdi", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ opportunity: opp }),
        })
          .then(function (ensured) {
            var decisionId =
              (ensured.decision && ensured.decision.decisionId) ||
              (ensured.decision &&
                ensured.decision.decision &&
                ensured.decision.decision.decisionId);
            if (!decisionId) throw new Error("decision_missing");
            var path =
              kind === "action"
                ? "/api/decisions/" + encodeURIComponent(decisionId) + "/actions"
                : "/api/decisions/" + encodeURIComponent(decisionId) + "/outcomes";
            return api(path, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(Object.assign({ hotelId: state.hotelId }, payload)),
            });
          })
          .then(function (result) {
            btn.disabled = false;
            showSaveOk(statusId, okMessage);
            if (result && result.current) updateLifecycleSummary(result.current);
          })
          .catch(function (err) {
            btn.disabled = false;
            showSaveOk(statusId, err.message || "Failed");
          });
      }

      if (t && t.id === "gdiDoActionSave") {
        var actionEl = document.getElementById("gdiDoAction");
        var actionType = actionEl ? actionEl.value : "";
        if (!actionType) {
          showSaveOk("gdiDoActionSaveStatus", "Select action...");
          return;
        }
        ensureThenRecord(
          t.getAttribute("data-id"),
          "action",
          { actionType: actionType },
          t,
          "gdiDoActionSaveStatus",
          "✓ Action saved"
        );
      }
      if (t && t.id === "gdiDoOutcomeSave") {
        var outcomeEl = document.getElementById("gdiDoOutcome");
        var lossEl = document.getElementById("gdiDoLoss");
        var outcomeType = outcomeEl ? outcomeEl.value : "";
        if (!outcomeType) {
          showSaveOk("gdiDoOutcomeSaveStatus", "Select outcome...");
          return;
        }
        ensureThenRecord(
          t.getAttribute("data-id"),
          "outcome",
          {
            outcomeType: outcomeType,
            outcomeReason:
              outcomeType === "LOST" && lossEl && lossEl.value ? lossEl.value : null,
          },
          t,
          "gdiDoOutcomeSaveStatus",
          "✓ Outcome saved"
        );
      }
    });

  loadAll().catch(function (err) {
    showError(err.message || "Failed to load Group Demand Intelligence");
  });
})();
