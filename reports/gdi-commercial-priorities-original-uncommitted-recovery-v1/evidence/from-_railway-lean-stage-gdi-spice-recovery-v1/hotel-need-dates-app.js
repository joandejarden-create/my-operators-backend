(function () {
  "use strict";

  var DEFAULT_HOTEL_ID = "recgMYovrrZDJMqzX";
  var STORAGE_KEY = "gdi-need-dates-hotelId";
  var root = document.getElementById("hndRoot");
  var loading = document.getElementById("hndLoading");
  var drawer = document.getElementById("hndDrawer");
  var drawerTitle = document.getElementById("hndDrawerTitle");
  var drawerBody = document.getElementById("hndDrawerBody");
  var drawerClose = document.getElementById("hndDrawerClose");
  var addDialog = document.getElementById("hndAddDialog");
  var addBody = document.getElementById("hndAddBody");
  var addClose = document.getElementById("hndAddClose");

  var memberFetch =
    window.DealalityMemberstackAuth &&
    typeof window.DealalityMemberstackAuth.fetchMyDealsApi === "function"
      ? window.DealalityMemberstackAuth.fetchMyDealsApi.bind(
          window.DealalityMemberstackAuth,
        )
      : null;

  function resolveHotelId() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var q = String(params.get("hotelId") || "").trim();
      if (q) return q;
      var s = String(localStorage.getItem(STORAGE_KEY) || "").trim();
      if (s) return s;
    } catch (e) {}
    return DEFAULT_HOTEL_ID;
  }

  function resolveIsolation() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var airtable = String(params.get("airtable") || "").trim();
      if (airtable === "1" || airtable === "true") return null;
      var i = String(params.get("isolation") || "").trim();
      if (i === "portal_v1_test" || i === "1") return "portal_v1_test";
      if (i === "0" || i === "none") return null;
      // Explicit airtable=0 keeps local shadow only when requested.
      if (airtable === "0" || airtable === "false") return "portal_v1_test";
    } catch (e) {}
    // V4 default: Airtable-backed production portal (AI Demand Positioning).
    return null;
  }

  function useAirtableMode() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var airtable = String(params.get("airtable") || "").trim();
      if (airtable === "0" || airtable === "false") return false;
      if (airtable === "1" || airtable === "true") return true;
      // Default Airtable when not in explicit isolation namespace.
      return resolveIsolation() == null;
    } catch (e) {
      return true;
    }
  }

  function resolveYear() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var y = Number(params.get("year"));
      if (y >= 2024 && y <= 2100) return y;
    } catch (e) {}
    return 2027;
  }

  var state = {
    hotelId: resolveHotelId(),
    isolation: resolveIsolation(),
    year: resolveYear(),
    dashboard: null,
    hotels: [],
    error: null,
    busy: false,
    selectedMonth: null,
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setLoading(on, msg) {
    if (!loading) return;
    loading.style.display = on ? "flex" : "none";
    loading.setAttribute("aria-busy", on ? "true" : "false");
    if (on) loading.classList.add("show");
    else loading.classList.remove("show");
    var m = document.getElementById("hndLoadingMessage");
    if (m && msg) m.textContent = msg;
  }

  async function api(path, opts) {
    opts = opts || {};
    var method = String(opts.method || "GET").toUpperCase();
    var headers = Object.assign({ Accept: "application/json" }, opts.headers || {});
    if (opts.body && !headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }
    var init = {
      method: method,
      headers: headers,
      credentials: "same-origin",
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    };
    var res;
    if (method !== "GET" && method !== "HEAD" && memberFetch) {
      res = await memberFetch(path, init);
    } else {
      res = await fetch(path, init);
      if (res.status === 401 && memberFetch) {
        res = await memberFetch(path, init);
      }
    }
    var data = null;
    try {
      data = await res.json();
    } catch (e) {
      data = { ok: false, error: "invalid_json" };
    }
    if (!res.ok) {
      var err = new Error(data.message || data.error || "request_failed");
      err.status = res.status;
      err.payload = data;
      throw err;
    }
    return data;
  }

  function isolationQuery() {
    var parts = [];
    if (state.isolation) {
      parts.push("isolation=" + encodeURIComponent(state.isolation));
    }
    if (useAirtableMode()) parts.push("airtable=1");
    return parts.join("&");
  }

  async function loadHotels() {
    try {
      var data = await api("/api/group-demand-intelligence/hotels");
      state.hotels = data.hotels || data.items || [];
    } catch (e) {
      state.hotels = [];
    }
  }

  async function loadDashboard() {
    var qs = [isolationQuery(), "year=" + state.year].filter(Boolean).join("&");
    state.dashboard = await api(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/need-dates" +
        (qs ? "?" + qs : ""),
    );
  }

  function selectHotel(hotelId) {
    state.hotelId = hotelId;
    try {
      localStorage.setItem(STORAGE_KEY, hotelId);
      sessionStorage.setItem("dealality.gdi.hotelId", hotelId);
      var url = new URL(window.location.href);
      url.searchParams.set("hotelId", hotelId);
      if (state.isolation) {
        url.searchParams.set("isolation", state.isolation);
        url.searchParams.delete("airtable");
      } else {
        url.searchParams.delete("isolation");
        url.searchParams.set("airtable", "1");
      }
      url.searchParams.set("year", String(state.year));
      history.replaceState({}, "", url.toString());
    } catch (e) {}
    return refresh();
  }

  function priorityClass(p) {
    var x = String(p || "").toUpperCase();
    if (x === "HIGH") return "priority-high";
    if (x === "LOW") return "priority-low";
    return "priority-medium";
  }

  function statusClass(s) {
    var x = String(s || "");
    if (x === "TEST_SCENARIO") return "status-test";
    if (/partial/i.test(x)) return "status-partial";
    if (/filled|closed|cancel|replaced/i.test(x)) return "status-filled";
    return "status-active";
  }

  function revenueLabels(arr) {
    var map = {};
    ((state.dashboard && state.dashboard.enums && state.dashboard.enums.revenueTypes) || []).forEach(
      function (r) {
        map[r.id] = r.label;
      },
    );
    return (arr || [])
      .map(function (id) {
        return map[id] || id;
      })
      .join(", ");
  }

  function filteredPeriods() {
    var periods = (state.dashboard && state.dashboard.periods) || [];
    if (!state.selectedMonth) return periods;
    var m = state.selectedMonth;
    return periods.filter(function (p) {
      return (
        p.startDate &&
        p.endDate &&
        p.startDate <= m.endDate &&
        p.endDate >= m.startDate
      );
    });
  }

  function render() {
    if (!root) return;
    var d = state.dashboard;
    if (state.error) {
      root.innerHTML =
        '<div class="hnd-banner error" role="alert">' +
        esc(state.error) +
        '</div><button type="button" class="gdi-btn gdi-btn-primary" id="hndRetry">Retry</button>';
      return;
    }
    if (!d) {
      root.innerHTML = '<div class="hnd-empty">No data loaded.</div>';
      return;
    }

    var hotelOptions = (state.hotels || [])
      .map(function (h) {
        var id = h.hotelId || h.id || "";
        var name = h.displayName || h.name || id;
        return (
          '<option value="' +
          esc(id) +
          '"' +
          (id === state.hotelId ? " selected" : "") +
          ">" +
          esc(name) +
          "</option>"
        );
      })
      .join("");

    var months = (d.monthlyOverview || [])
      .map(function (m) {
        var active = m.hasActiveNeed ? " is-active" : "";
        var meta = m.hasActiveNeed
          ? (m.periods || [])
              .map(function (p) {
                return (p.priority || "") + " · " + (p.status || "");
              })
              .slice(0, 2)
              .join("; ")
          : "No need set";
        return (
          '<button type="button" class="hnd-month' +
          active +
          '" data-hnd-month="' +
          m.month +
          '"><div class="m-name">' +
          esc(m.label) +
          '</div><div class="m-meta">' +
          esc(meta) +
          "</div></button>"
        );
      })
      .join("");

    var cards = filteredPeriods()
      .map(function (p) {
        return (
          '<article class="hnd-card" data-period-id="' +
          esc(p.needPeriodId) +
          '"><div class="hnd-card-head"><h3>' +
          esc(p.periodLabel || p.needPeriodId) +
          '</h3><div class="hnd-pills">' +
          '<span class="hnd-pill ' +
          priorityClass(p.priority) +
          '">' +
          esc(p.priority || "MEDIUM") +
          "</span>" +
          '<span class="hnd-pill ' +
          statusClass(p.status) +
          '">' +
          esc(p.status) +
          "</span>" +
          '<span class="hnd-pill">' +
          esc(p.researchStateLabel || p.researchState || "") +
          "</span></div></div>" +
          '<div class="hnd-card-meta">' +
          "<div><strong>Dates</strong><br/>" +
          esc(p.startDate) +
          " → " +
          esc(p.endDate) +
          "</div>" +
          "<div><strong>Revenue focus</strong><br/>" +
          esc(revenueLabels(p.revenueNeed) || "—") +
          "</div>" +
          "<div><strong>Matched opportunities</strong><br/>" +
          esc(String(p.matchedOpportunityCount || 0)) +
          "</div>" +
          "<div><strong>Last updated</strong><br/>" +
          esc((p.updatedAt || "").slice(0, 19).replace("T", " ") || "—") +
          "</div></div>" +
          renderMatches(p) +
          '<div class="hnd-card-actions">' +
          '<button type="button" class="gdi-btn gdi-btn-primary" data-hnd-edit="' +
          esc(p.needPeriodId) +
          '">Edit</button>' +
          '<button type="button" class="gdi-btn gdi-btn-ghost" data-hnd-history="' +
          esc(p.needPeriodId) +
          '">History</button>' +
          '<a class="hnd-nav-link" href="/group-demand-intelligence.html?hotelId=' +
          encodeURIComponent(state.hotelId) +
          '">Open GDI</a>' +
          "</div></article>"
        );
      })
      .join("");

    if (!cards) {
      cards =
        '<div class="hnd-empty">No need periods yet. Add a month to start matching existing GDI opportunities.</div>';
    }

    var isolationBanner = d.isolation
      ? '<div class="hnd-banner warn" role="status">Isolated portal test data — not written to customer-visible Airtable inventory. Mode: ' +
        esc(d.dataMode) +
        ".</div>"
      : d.dataMode === "AIRTABLE_AI_DEMAND_POSITIONING"
        ? '<div class="hnd-banner" role="status">Persisted in Airtable <strong>AI Demand Positioning</strong> · GDI Hotel Need Periods. Paid Exa remains blocked. Mode: ' +
          esc(d.dataMode) +
          ".</div>"
        : "";

    root.innerHTML =
      isolationBanner +
      '<header class="hnd-header"><div><h1>Hotel Need Dates</h1><p class="hnd-sub">' +
      esc((d.hotel && d.hotel.name) || state.hotelId) +
      " · Calendar " +
      esc(String(d.year)) +
      " · Updated " +
      esc((d.lastUpdated || "—").slice(0, 19).replace("T", " ")) +
      '</p><p class="hnd-sub"><a class="hnd-nav-link" href="/group-demand-intelligence.html?hotelId=' +
      encodeURIComponent(state.hotelId) +
      '">← Back to Group Demand Intelligence</a></p></div>' +
      '<div class="hnd-header-actions">' +
      (hotelOptions
        ? '<label class="gdi-muted">Hotel <select id="hndHotelSelect">' +
          hotelOptions +
          "</select></label>"
        : "") +
      '<button type="button" class="gdi-btn gdi-btn-primary" id="hndAddBtn">Add Need Period</button>' +
      "</div></header>" +
      '<section class="hnd-summary" aria-label="Summary">' +
      stat("Active need periods", d.summary.activeNeedPeriods) +
      stat("Matched GDI opportunities", d.summary.matchedGdiOpportunities) +
      stat(
        "New need-date research opps",
        d.summary.newNeedDateResearchOpportunities,
      ) +
      stat("Research status", d.summary.researchStatus) +
      "</section>" +
      '<section class="hnd-section"><h2>Monthly overview</h2><div class="hnd-month-grid">' +
      months +
      "</div></section>" +
      '<section class="hnd-section"><h2>Need periods' +
      (state.selectedMonth
        ? " · " + esc(state.selectedMonth.label)
        : "") +
      '</h2><div class="hnd-cards">' +
      cards +
      "</div></section>";
  }

  function stat(label, value) {
    return (
      '<div class="hnd-stat"><span class="label">' +
      esc(label) +
      '</span><span class="value">' +
      esc(String(value)) +
      "</span></div>"
    );
  }

  function renderMatches(p) {
    var m = p.matches || {};
    var rows = []
      .concat(m.existingMatch || [])
      .concat(m.existingEnriched || [])
      .concat(m.newOpportunity || [])
      .concat(m.researchContinuation || []);
    if (!rows.length) {
      return '<p class="gdi-muted" style="margin:0 0 0.75rem;font-size:0.85rem">No verified month-specific opportunity matches yet. Existing inventory was inspected; research gaps remain.</p>';
    }
    return (
      '<ul class="hnd-match-list">' +
      rows
        .slice(0, 8)
        .map(function (r) {
          return (
            "<li><strong>" +
            esc(r.bucket.replace(/_/g, " ")) +
            "</strong> — " +
            esc(r.title || r.opportunityId) +
            " · " +
            esc(r.timingOverlapStatus || "timing n/a") +
            (r.confirmedRevenueTiming ? " · verified demand timing" : "") +
            "</li>"
          );
        })
        .join("") +
      "</ul>"
    );
  }

  function openAddForm() {
    var revenue = ((state.dashboard && state.dashboard.enums && state.dashboard.enums.revenueTypes) || []);
    var year = state.year;
    addBody.innerHTML =
      '<form class="hnd-form" id="hndAddForm">' +
      "<p class=\"gdi-muted\">Select one or more months. Nonconsecutive months become independent need periods unless you choose a continuous range.</p>" +
      '<div class="hnd-month-checks" id="hndMonthChecks">' +
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        .map(function (m) {
          var label = new Date(Date.UTC(year, m - 1, 1)).toLocaleString("en-US", {
            month: "short",
            timeZone: "UTC",
          });
          return (
            '<label><input type="checkbox" name="month" value="' +
            m +
            '"/> ' +
            label +
            "</label>"
          );
        })
        .join("") +
      "</div>" +
      '<label>Priority<select name="priority"><option value="HIGH">High</option><option value="MEDIUM" selected>Medium</option><option value="LOW">Low</option></select></label>' +
      '<fieldset><legend class="gdi-muted">Revenue focus</legend><div class="hnd-check-grid">' +
      revenue
        .map(function (r) {
          var checked =
            r.id === "GROUP_GUESTROOMS" ||
            r.id === "MEETING_SPACE" ||
            r.id === "BANQUETS" ||
            r.id === "LOCAL_CATERING"
              ? " checked"
              : "";
          return (
            '<label><input type="checkbox" name="revenue" value="' +
            esc(r.id) +
            '"' +
            checked +
            "/> " +
            esc(r.label) +
            "</label>"
          );
        })
        .join("") +
      "</div></fieldset>" +
      '<label><input type="checkbox" name="continuousRange" value="1"/> Treat selected months as one continuous date range</label>' +
      '<label>Target room nights (optional)<input type="number" name="targetRoomNights" min="0" placeholder="Leave blank — do not invent"/></label>' +
      '<label>Target revenue (optional)<input type="number" name="targetRevenue" min="0" step="0.01" placeholder="Leave blank"/></label>' +
      '<label>Restrictions<textarea name="restrictions" placeholder="Blackout dates, space limits, etc."></textarea></label>' +
      '<label>Sales notes<textarea name="salesNotes"></textarea></label>' +
      '<div class="hnd-card-actions"><button type="submit" class="gdi-btn gdi-btn-primary">Save need periods</button>' +
      '<button type="button" class="gdi-btn gdi-btn-ghost" id="hndAddCancel">Cancel</button></div>' +
      '<p class="gdi-muted" id="hndAddError" role="alert"></p></form>';
    if (typeof addDialog.showModal === "function") addDialog.showModal();
    else addDialog.setAttribute("open", "open");
  }

  async function submitAdd(ev) {
    ev.preventDefault();
    var form = ev.target;
    var errEl = document.getElementById("hndAddError");
    var months = Array.prototype.map
      .call(form.querySelectorAll('input[name="month"]:checked'), function (el) {
        return { year: state.year, month: Number(el.value) };
      });
    var revenueNeed = Array.prototype.map.call(
      form.querySelectorAll('input[name="revenue"]:checked'),
      function (el) {
        return el.value;
      },
    );
    if (!months.length) {
      errEl.textContent = "Select at least one month.";
      return;
    }
    if (!revenueNeed.length) {
      errEl.textContent = "Select at least one revenue type.";
      return;
    }
    var continuous = form.continuousRange && form.continuousRange.checked;
    var body = {
      months: months,
      revenueNeed: revenueNeed,
      priority: form.priority.value,
      continuousRange: continuous,
      restrictions: form.restrictions.value || null,
      salesNotes: form.salesNotes.value || null,
      isTestScenario: state.isolation === "portal_v1_test",
      isolation: state.isolation,
      airtable: useAirtableMode(),
    };
    if (form.targetRoomNights.value) {
      body.targetRoomNights = Number(form.targetRoomNights.value);
    }
    if (form.targetRevenue.value) {
      body.targetRevenue = Number(form.targetRevenue.value);
    }
    if (continuous && months.length) {
      months.sort(function (a, b) {
        return a.month - b.month;
      });
      var first = months[0];
      var last = months[months.length - 1];
      body.startDate =
        state.year +
        "-" +
        String(first.month).padStart(2, "0") +
        "-01";
      var endDay = new Date(Date.UTC(state.year, last.month, 0)).getUTCDate();
      body.endDate =
        state.year +
        "-" +
        String(last.month).padStart(2, "0") +
        "-" +
        String(endDay).padStart(2, "0");
    }
    try {
      state.busy = true;
      await api(
        "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/need-periods",
        { method: "POST", body: body },
      );
      addDialog.close();
      await refresh();
    } catch (e) {
      errEl.textContent = e.message || "Save failed";
    } finally {
      state.busy = false;
    }
  }

  async function openEdit(needPeriodId) {
    var p = ((state.dashboard && state.dashboard.periods) || []).find(
      function (x) {
        return x.needPeriodId === needPeriodId;
      },
    );
    if (!p) return;
    drawerTitle.textContent = p.periodLabel || "Edit need period";
    drawerBody.innerHTML =
      '<form class="hnd-form" id="hndEditForm" data-id="' +
      esc(p.needPeriodId) +
      '" data-version="' +
      esc(String(p.currentVersion || 1)) +
      '">' +
      "<p class=\"gdi-muted\">Version " +
      esc(String(p.currentVersion || 1)) +
      " · Changes are versioned. Closing a period does not delete GDI opportunities.</p>" +
      '<label>Priority<select name="priority">' +
      ["HIGH", "MEDIUM", "LOW"]
        .map(function (x) {
          return (
            '<option value="' +
            x +
            '"' +
            (p.priority === x ? " selected" : "") +
            ">" +
            x +
            "</option>"
          );
        })
        .join("") +
      "</select></label>" +
      '<label>Status change<select name="changeType">' +
      '<option value="MODIFY">Update fields</option>' +
      '<option value="PARTIALLY_FILLED">Mark partially filled</option>' +
      '<option value="FILLED">Mark filled</option>' +
      '<option value="CANCEL">Withdraw (not filled)</option>' +
      '<option value="REOPEN">Reopen</option>' +
      '<option value="REPLACE">Replace with another period</option>' +
      '<option value="PAUSED">Pause</option>' +
      "</select></label>" +
      '<label>Reason (required for reopen / replace)<textarea name="reason" placeholder="e.g. Group cancellation"></textarea></label>' +
      '<div id="hndReplaceFields" hidden>' +
      '<label>Replacement month (1–12)<input type="number" name="replaceMonth" min="1" max="12" value="11"/></label>' +
      '<label>Replacement year<input type="number" name="replaceYear" value="' +
      esc(String(state.year)) +
      '"/></label></div>' +
      '<label>Sales notes<textarea name="salesNotes">' +
      esc(p.hotelSalesNotes || "") +
      "</textarea></label>" +
      '<label>Restrictions<textarea name="restrictions">' +
      esc(p.commercialRestrictions || "") +
      "</textarea></label>" +
      '<div class="hnd-card-actions"><button type="submit" class="gdi-btn gdi-btn-primary">Save changes</button>' +
      '<button type="button" class="gdi-btn gdi-btn-ghost" id="hndEditCancel">Cancel</button></div>' +
      '<p class="gdi-muted" id="hndEditError" role="alert"></p></form>';
    if (typeof drawer.showModal === "function") drawer.showModal();
    else drawer.setAttribute("open", "open");
  }

  async function submitEdit(ev) {
    ev.preventDefault();
    var form = ev.target;
    var errEl = document.getElementById("hndEditError");
    var changeType = form.changeType.value;
    if (
      (changeType === "REOPEN" || changeType === "REPLACE") &&
      !String(form.reason.value || "").trim()
    ) {
      errEl.textContent = "Reason is required for reopen or replace.";
      return;
    }
    var body = {
      changeType: changeType,
      priority: form.priority.value,
      reason: form.reason.value || null,
      salesNotes: form.salesNotes.value,
      restrictions: form.restrictions.value,
      expectedVersion: Number(form.getAttribute("data-version")),
      isolation: state.isolation,
      airtable: useAirtableMode(),
    };
    if (changeType === "REPLACE") {
      var y = Number(form.replaceYear.value);
      var m = Number(form.replaceMonth.value);
      var start =
        y + "-" + String(m).padStart(2, "0") + "-01";
      var endDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
      var end =
        y +
        "-" +
        String(m).padStart(2, "0") +
        "-" +
        String(endDay).padStart(2, "0");
      body.successorPeriod = {
        hotelId: state.hotelId,
        periodLabel:
          new Date(Date.UTC(y, m - 1, 1)).toLocaleString("en-US", {
            month: "long",
            timeZone: "UTC",
          }) +
          " " +
          y,
        startDate: start,
        endDate: end,
        revenueNeed: ["GROUP_GUESTROOMS", "MEETING_SPACE"],
        status: state.isolation ? "TEST_SCENARIO" : "Active",
        isTestScenario: Boolean(state.isolation),
        source: "hotel_portal",
      };
    }
    try {
      await api(
        "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/need-periods/" +
          encodeURIComponent(form.getAttribute("data-id")),
        { method: "PATCH", body: body },
      );
      drawer.close();
      await refresh();
    } catch (e) {
      if (e.status === 409) {
        errEl.textContent =
          "Someone else updated this period. Reload and try again. (version conflict)";
      } else {
        errEl.textContent = e.message || "Save failed";
      }
    }
  }

  async function openHistory(needPeriodId) {
    try {
      var qs = isolationQuery();
      var data = await api(
        "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/need-periods/" +
          encodeURIComponent(needPeriodId) +
          (qs ? "?" + qs : ""),
      );
      drawerTitle.textContent = "Change history";
      var changes = data.changes || [];
      drawerBody.innerHTML =
        '<div class="hnd-history"><ol>' +
        (changes.length
          ? changes
              .slice()
              .reverse()
              .map(function (c) {
                return (
                  "<li><strong>" +
                  esc(c.changeType) +
                  "</strong> · " +
                  esc((c.timestamp || "").slice(0, 19).replace("T", " ")) +
                  " · " +
                  esc(c.actorSource || "") +
                  (c.actorUser && c.actorUser.email
                    ? " (" + esc(c.actorUser.email) + ")"
                    : "") +
                  "<br/>" +
                  esc(c.reason || "") +
                  "</li>"
                );
              })
              .join("")
          : "<li>No history yet.</li>") +
        "</ol></div>";
      if (typeof drawer.showModal === "function") drawer.showModal();
    } catch (e) {
      state.error = e.message;
      render();
    }
  }

  async function refresh() {
    setLoading(true, "Loading Hotel Need Dates…");
    state.error = null;
    try {
      await loadHotels();
      await loadDashboard();
      render();
    } catch (e) {
      state.error = e.message || "Failed to load need dates";
      render();
    } finally {
      setLoading(false);
    }
  }

  root.addEventListener("click", function (ev) {
    var t = ev.target;
    if (!t) return;
    if (t.id === "hndRetry") return refresh();
    if (t.id === "hndAddBtn") return openAddForm();
    if (t.id === "hndAddCancel") return addDialog.close();
    if (t.id === "hndEditCancel") return drawer.close();
    if (t.getAttribute && t.getAttribute("data-hnd-month")) {
      var monthNum = Number(t.getAttribute("data-hnd-month"));
      var hit = ((state.dashboard && state.dashboard.monthlyOverview) || []).find(
        function (m) {
          return m.month === monthNum;
        },
      );
      state.selectedMonth =
        state.selectedMonth && state.selectedMonth.month === monthNum
          ? null
          : hit || null;
      render();
      return;
    }
    if (t.getAttribute && t.getAttribute("data-hnd-edit")) {
      return openEdit(t.getAttribute("data-hnd-edit"));
    }
    if (t.getAttribute && t.getAttribute("data-hnd-history")) {
      return openHistory(t.getAttribute("data-hnd-history"));
    }
  });

  root.addEventListener("change", function (ev) {
    if (ev.target && ev.target.id === "hndHotelSelect") {
      selectHotel(ev.target.value);
    }
  });

  addBody.addEventListener("submit", function (ev) {
    if (ev.target && ev.target.id === "hndAddForm") submitAdd(ev);
  });
  drawerBody.addEventListener("submit", function (ev) {
    if (ev.target && ev.target.id === "hndEditForm") submitEdit(ev);
  });
  drawerBody.addEventListener("change", function (ev) {
    if (ev.target && ev.target.name === "changeType") {
      var box = document.getElementById("hndReplaceFields");
      if (box) box.hidden = ev.target.value !== "REPLACE";
    }
  });
  drawerClose.addEventListener("click", function () {
    drawer.close();
  });
  addClose.addEventListener("click", function () {
    addDialog.close();
  });

  refresh();
})();
