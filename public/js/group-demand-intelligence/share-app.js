(function () {
  "use strict";

  var root = document.getElementById("gdiShareRoot");
  var loading = document.getElementById("gdiShareLoading");
  var drawer = document.getElementById("gdiShareDrawer");
  var drawerTitle = document.getElementById("gdiShareDrawerTitle");
  var drawerBody = document.getElementById("gdiShareDrawerBody");
  var drawerClose = document.getElementById("gdiShareDrawerClose");
  var params = new URLSearchParams(window.location.search || "");
  var share = params.get("share") || "";

  var UI = window.DealalityGdiUi;
  if (!UI) throw new Error("DealalityGdiUi missing");
  var esc = UI.esc;
  var fmtVal = UI.fmtVal;
  var trunc = UI.trunc;

  var STORAGE_PREFIX = "gdi-share-";
  var state = {
    hotelId: null,
    resolve: null,
    opportunities: [],
    validationEnums: null,
    tab: "opportunities",
    sortKey: "priority",
    sortDir: 1,
    viewMode: "list",
    filters: { priority: "", segment: "", booking: "", territory: "" },
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
    state.viewMode = "list";
    try {
      sessionStorage.removeItem(STORAGE_PREFIX + "viewMode");
      sessionStorage.removeItem(STORAGE_PREFIX + "sortKey");
    } catch (e) {
      /* ignore */
    }
    render();
  }

  readPersistedBrowse();

  function showError(msg) {
    if (loading) loading.style.display = "none";
    root.innerHTML = '<div class="gdi-error" role="alert"><p>' + esc(msg) + "</p></div>";
  }

  function formatDate(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    return d.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function api(path) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return fetch(path + sep + "share=" + encodeURIComponent(share)).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          throw new Error(data.message || data.error || "Request failed");
        }
        return data;
      });
    });
  }

  function apiPost(path, body) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return fetch(path + sep + "share=" + encodeURIComponent(share), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          throw new Error(data.message || data.error || "Request failed");
        }
        return data;
      });
    });
  }

  function enumOptions(keys, labels, selected) {
    return (
      '<option value="">—</option>' +
      (keys || [])
        .map(function (k) {
          return (
            '<option value="' +
            esc(k) +
            '"' +
            (selected === k ? " selected" : "") +
            ">" +
            esc((labels && labels[k]) || k) +
            "</option>"
          );
        })
        .join("")
    );
  }

  function nextUnvalidatedId(currentId) {
    var rows = state.opportunities || [];
    var start = -1;
    for (var i = 0; i < rows.length; i++) {
      if (rows[i].id === currentId) {
        start = i;
        break;
      }
    }
    for (var j = start + 1; j < rows.length; j++) {
      if (!rows[j].shareValidation) return rows[j].id;
    }
    for (var k = 0; k < start; k++) {
      if (!rows[k].shareValidation) return rows[k].id;
    }
    return null;
  }

  function validationFormHtml(o) {
    var v = o.shareValidation || {};
    var e = state.validationEnums || {};
    return (
      '<p class="gdi-lede">Fast review for sales. Stored separately — does not overwrite research.</p>' +
      '<div class="gdi-feedback" id="gdiShareValidation">' +
      "<label>Familiarity / Status<select id=\"gdiSvFamiliarity\">" +
      enumOptions(e.familiarityStatus, e.familiarityLabels, v.familiarityStatus) +
      "</select></label>" +
      "<label>Commercial Value<select id=\"gdiSvCommercial\">" +
      enumOptions(e.commercialValue, e.commercialValueLabels, v.commercialValue) +
      "</select></label>" +
      "<label>Contact Person<select id=\"gdiSvPerson\">" +
      enumOptions(e.contactPerson, e.contactPersonLabels, v.contactPersonAssessment) +
      "</select></label>" +
      "<label>Email Quality<select id=\"gdiSvEmail\">" +
      enumOptions(e.emailAssessment, e.emailAssessmentLabels, v.emailAssessment) +
      "</select></label>" +
      "<label>Phone Quality<select id=\"gdiSvPhone\">" +
      enumOptions(e.phoneAssessment, e.phoneAssessmentLabels, v.phoneAssessment) +
      "</select></label>" +
      '<textarea id="gdiSvNote" placeholder="Optional note" rows="2">' +
      esc(v.note || "") +
      "</textarea>" +
      '<div class="gdi-validation-actions">' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiSvSave" data-id="' +
      esc(o.id) +
      '">Save</button>' +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="gdiSvSaveNext" data-id="' +
      esc(o.id) +
      '">Save + Next</button>' +
      '<span class="gdi-validation-status" id="gdiSvStatus"></span></div></div>'
    );
  }

  function wireValidationForm(opportunityId) {
    function collect() {
      return {
        familiarityStatus: (document.getElementById("gdiSvFamiliarity") || {}).value || null,
        commercialValue: (document.getElementById("gdiSvCommercial") || {}).value || null,
        contactPersonAssessment: (document.getElementById("gdiSvPerson") || {}).value || null,
        emailAssessment: (document.getElementById("gdiSvEmail") || {}).value || null,
        phoneAssessment: (document.getElementById("gdiSvPhone") || {}).value || null,
        note: (document.getElementById("gdiSvNote") || {}).value || "",
        validator: "SHARE_REVIEWER",
      };
    }
    function save(thenNext) {
      var statusEl = document.getElementById("gdiSvStatus");
      if (statusEl) statusEl.textContent = "Saving…";
      var payload = collect();
      if (!payload.familiarityStatus && !payload.commercialValue) {
        if (statusEl) statusEl.textContent = "Pick familiarity or commercial value.";
        return;
      }
      apiPost(
        "/api/group-demand-intelligence/share/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/opportunities/" +
          encodeURIComponent(opportunityId) +
          "/validation",
        payload
      )
        .then(function (data) {
          for (var i = 0; i < state.opportunities.length; i++) {
            if (state.opportunities[i].id === opportunityId) {
              state.opportunities[i].shareValidation = data.validation;
              break;
            }
          }
          if (state.resolve && data.summary) {
            state.resolve.summary = Object.assign({}, state.resolve.summary, {
              validated: data.summary.validated,
              pendingValidation: data.summary.pendingValidation,
              confirmedNew: data.summary.confirmedNew,
              alreadyKnown: data.summary.alreadyKnown,
              worthPursuingNow: data.summary.worthPursuingNow,
              notRelevant: data.summary.notRelevant,
            });
            state.resolve.validationSummary = data.summary;
          }
          if (statusEl) statusEl.textContent = "Saved";
          if (thenNext) {
            var nid = nextUnvalidatedId(opportunityId);
            if (nid) openDetail(nid);
            else if (statusEl) statusEl.textContent = "Saved — all reviewed";
          }
        })
        .catch(function (err) {
          if (statusEl) statusEl.textContent = err.message || "Save failed";
        });
    }
    var saveBtn = document.getElementById("gdiSvSave");
    var nextBtn = document.getElementById("gdiSvSaveNext");
    if (saveBtn) saveBtn.addEventListener("click", function () { save(false); });
    if (nextBtn) nextBtn.addEventListener("click", function () { save(true); });
  }

  function priorityPill(p) {
    return UI.priorityPill(p);
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
    if (!c) return "<p>No usable contact resolved yet.</p>";
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
      (backups ? "<h4>Backup contacts</h4><ul>" + backups + "</ul>" : "")
    );
  }

  function oppById(id) {
    for (var i = 0; i < state.opportunities.length; i++) {
      if (state.opportunities[i].id === id) return state.opportunities[i];
    }
    return null;
  }

  function entityName() {
    return (state.resolve && state.resolve.hotelName) || "Hotel";
  }

  function entityMeta() {
    if (!state.resolve) return "";
    var parts = [];
    if (state.resolve.city) parts.push(state.resolve.city);
    if (state.resolve.state) parts.push(state.resolve.state);
    return parts.join(", ");
  }

  function qualifiedCount() {
    var s = (state.resolve && state.resolve.summary) || {};
    if (s.qualifiedCount != null) return s.qualifiedCount;
    return state.opportunities.length;
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

  function openDetail(id) {
    api(
      "/api/group-demand-intelligence/share/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/opportunities/" +
        encodeURIComponent(id)
    ).then(function (data) {
      var o = data.opportunity;
      var audit = data.scoreAudit || {};
      drawerTitle.textContent = o.title;
      drawerBody.innerHTML = UI.intelligenceDetailHtml(o, audit, {
        validationHtml: validationFormHtml(o),
        whoContactHtml: whoShouldSalesContactHtml,
      });
      if (typeof drawer.showModal === "function") drawer.showModal();
      else drawer.setAttribute("open", "open");
      wireValidationForm(o.id);
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

  function renderOpps() {
    if (!state.opportunities.length) {
      return '<div class="gdi-empty">No opportunities.</div>';
    }
    var rows = filteredSorted();
    var chrome = UI.opportunityBrowseChromeHtml({
      activePriority: state.filters.priority,
      priorityCounts: UI.facetCountByPriority(state.opportunities, state.filters),
      activeBooking: state.filters.booking,
      actionCounts: UI.facetCountByActionStatus(state.opportunities, state.filters),
      shown: rows.length,
      total: state.opportunities.length,
      sort: state.sortKey,
      viewMode: state.viewMode,
      noun: "Opportunities",
    });
    if (!rows.length) {
      return (
        chrome +
        '<div class="gdi-empty">No opportunities match the current filters.</div>'
      );
    }
    return chrome + UI.opportunityCardsGridHtml(rows, state.viewMode, state.filters);
  }

  function render() {
    if (loading) loading.style.display = "none";
    var s = (state.resolve && state.resolve.summary) || {};
    var hotel = {
      hotelName: entityName(),
      city: state.resolve && state.resolve.city,
      state: state.resolve && state.resolve.state,
    };
    root.className = "gdi-shell dashboard-container aiv-dashboard";
    root.innerHTML =
      UI.hotelShellHtml({
        mode: "share",
        badges: { pilot: true, readOnly: true },
      }) +
      UI.contentTabsHtml(state.tab, [UI.GDI_MAIN_TAB]) +
      UI.propertyBarHtml({
        mode: "share",
        hotel: hotel,
        lastResearch: formatDate(s.lastResearchAt),
        runStatus: s.runStatus || "—",
        actionHtml:
          '<button type="button" class="btn-clear" id="gdiResetViewBtn">Reset View</button>',
      }) +
      '<div id="gdiTabBody">' +
      renderOpps() +
      "</div>" +
      '<div class="gdi-disclaimer">Pilot brief for review only. Findings are research-assisted and should be validated by the hotel sales team before outreach.</div>';

    wireBrowseControls();

    var resetBtn = document.getElementById("gdiResetViewBtn");
    if (resetBtn) resetBtn.addEventListener("click", resetBrowseView);

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

  if (!share) {
    showError("A valid share link is required.");
    return;
  }

  if (drawerClose) {
    drawerClose.addEventListener("click", function () {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    });
  }

  api("/api/group-demand-intelligence/share/resolve")
    .then(function (resolve) {
      state.resolve = resolve;
      state.hotelId = resolve.hotelId;
      return api(
        "/api/group-demand-intelligence/share/hotels/" +
          encodeURIComponent(resolve.hotelId) +
          "/opportunities"
      );
    })
    .then(function (data) {
      state.opportunities = data.opportunities || [];
      state.validationEnums = data.validationEnums || null;
      render();
    })
    .catch(function (err) {
      showError(err.message || "Unable to open share link.");
    });
})();
