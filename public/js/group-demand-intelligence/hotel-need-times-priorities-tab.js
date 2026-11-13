/**
 * GDI Tab 2 — Hotel Need Times & Commercial Priorities
 *
 * ORIGINAL UI recovered (not reconstructed): Hotel Commercial Priorities V3
 * from worktree feat/gdi-universal-need-date-intelligence-v1
 * (staged in index, never committed) + CSS from Railway lean stage twin.
 * Evidence: reports/gdi-commercial-priorities-original-uncommitted-recovery-v1/
 *
 * Mount/keep-alive API from PR #215. Optional specific-days from PR #216.
 * Supervisor soft-boosts remain server-side.
 */
(function (global) {
  "use strict";

  function createTabController(opts) {
    opts = opts || {};
    var root = opts.root;
    if (!root) throw new Error("need_times_tab_root_required");
    var loading = opts.loading || null;
    var optsHotelId = String(opts.hotelId || "").trim() || null;
    var optsIsolation = opts.isolation || null;
    var DEFAULT_HOTEL_ID = "recgMYovrrZDJMqzX";
    var STORAGE_KEY = "gdi-need-dates-hotelId";

  /** Page sections — continuous scroll; no wizard navigation. */
  var SECTIONS = [
    {
      id: "revenue",
      title: "Revenue Priorities",
      subtitle: "Select the revenue streams that matter most for your hotel.",
    },
    {
      id: "need-dates",
      title: "Need Dates",
      subtitle:
        "Select months and periods where additional demand would be most valuable.",
    },
    {
      id: "preferred",
      title: "Preferred Business",
      subtitle: "Group size, meeting needs, booking windows, and stay preferences.",
    },
    {
      id: "customers",
      title: "Target Demand Segments",
      subtitle: "Choose preferred demand segments — no typing required.",
    },
    {
      id: "local-market",
      title: "Local Market Intelligence",
      subtitle:
        "Demand generators, local organizations, and existing account relationships.",
    },
    {
      id: "fit",
      title: "Hotel Fit & Constraints",
      subtitle: "Property advantages, meeting-space considerations, and restrictions.",
    },
    {
      id: "team",
      title: "Hotel Team",
      subtitle: "Sales, revenue, and executive contacts for this property.",
    },
  ];

  var memberFetch =
    window.DealalityMemberstackAuth &&
    typeof window.DealalityMemberstackAuth.fetchMyDealsApi === "function"
      ? window.DealalityMemberstackAuth.fetchMyDealsApi.bind(
          window.DealalityMemberstackAuth,
        )
      : null;

  function qs() {
    return new URLSearchParams(window.location.search || "");
  }

  function resolveHotelId() {
    if (optsHotelId) return optsHotelId;
    try {
      var q = String(qs().get("hotelId") || "").trim();
      if (q) return q;
      var s = String(localStorage.getItem(STORAGE_KEY) || "").trim();
      if (s) return s;
    } catch (e) {}
    return DEFAULT_HOTEL_ID;
  }

  function useAirtableMode() {
    if (optsIsolation === "portal_v1_test") return false;
    try {
      var airtable = String(qs().get("airtable") || "").trim();
      if (airtable === "0" || airtable === "false") return false;
      var iso = String(qs().get("isolation") || "").trim();
      if (iso === "portal_v1_test" || iso === "1") return false;
      return true;
    } catch (e) {
      return true;
    }
  }

  function resolveYear() {
    try {
      var y = Number(qs().get("year"));
      if (y >= 2024 && y <= 2100) return y;
    } catch (e) {}
    return 2027;
  }

  var state = {
    hotelId: resolveHotelId(),
    year: resolveYear(),
    airtable: useAirtableMode(),
    dashboard: null,
    profile: null,
    draft: null,
    dirty: false,
    selectedMonths: {},
    monthDraft: null,
    drawerMonth: null,
    saveMessage: null,
    error: null,
    suggestions: null,
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
    var m =
      document.getElementById("gdiLoadingMessage") ||
      document.getElementById("hndLoadingMessage");
    if (m && msg) m.textContent = msg;
  }

  async function api(path, opts) {
    opts = opts || {};
    var init = {
      method: opts.method || "GET",
      headers: Object.assign({ Accept: "application/json" }, opts.headers || {}),
    };
    if (opts.body != null) {
      init.headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(opts.body);
    }
    var res;
    var method = String(init.method || "GET").toUpperCase();
    if (method !== "GET" && method !== "HEAD" && memberFetch) {
      res = await memberFetch(path, init);
    } else {
      res = await fetch(path, init);
      if (res.status === 401 && memberFetch) res = await memberFetch(path, init);
    }
    var data = await res.json().catch(function () {
      return { ok: false, error: "invalid_json" };
    });
    if (!res.ok) {
      var msg = data.message || data.error || "request_failed";
      if (
        res.status === 404 &&
        (data.error === "API route not found" || msg === "API route not found")
      ) {
        msg =
          "API route not found — restart the local Dealality server (npm start) so Hotel Commercial Priorities routes register.";
      }
      var err = new Error(msg);
      err.status = res.status;
      err.payload = data;
      throw err;
    }
    return data;
  }

  function airtableQs() {
    return state.airtable ? "airtable=1" : "isolation=portal_v1_test";
  }

  function emptyDraft() {
    return {
      revenuePriorities: [],
      peakRoomsPreset: null,
      bookingWindow: null,
      meetingSpaceNeed: null,
      stayDuration: null,
      dayPreference: null,
      specificDays: [],
      segmentMode: "NO_PREFERENCE",
      prioritySegments: [],
      excludedSegments: [],
      lowerPrioritySegments: [],
      demandGeneratorCategories: [],
      knownAccounts: [],
      businessToAvoid: [],
      hotelConsiderations: [],
      restrictionNotes: "",
      commercialTeam: {},
      currentVersion: 1,
      isSyntheticTest: false,
      provenance: "Hotel Confirmed",
    };
  }

  function profileToDraft(p) {
    p = p || {};
    return {
      revenuePriorities: (p.revenuePriorities || []).map(function (x) {
        return typeof x === "string" ? x : x.id;
      }),
      peakRoomsPreset: p.peakRoomsPreset || null,
      bookingWindow: p.bookingWindow || null,
      meetingSpaceNeed: p.meetingSpaceNeed || null,
      stayDuration: p.stayDuration || null,
      dayPreference: p.dayPreference || null,
      specificDays: p.specificDays || [],
      segmentMode: p.segmentMode || "NO_PREFERENCE",
      prioritySegments: p.prioritySegments || [],
      excludedSegments: p.excludedSegments || [],
      lowerPrioritySegments: p.lowerPrioritySegments || [],
      demandGeneratorCategories: p.demandGeneratorCategories || [],
      knownAccounts: p.knownAccounts || [],
      businessToAvoid: p.businessToAvoid || [],
      hotelConsiderations: p.hotelConsiderations || [],
      restrictionNotes: p.restrictionNotes || "",
      commercialTeam: p.commercialTeam || {},
      currentVersion: p.currentVersion || 1,
      isSyntheticTest: Boolean(p.isSyntheticTest),
      provenance: p.provenance || "Hotel Confirmed",
    };
  }

  function markDirty() {
    state.dirty = true;
    var hint = root && root.querySelector(".hcp-save-hint");
    if (hint) hint.textContent = "Unsaved changes";
    state.saveMessage = null;
  }

  function toggleArr(obj, key, value, max) {
    var arr = obj[key] || [];
    var i = arr.indexOf(value);
    if (i >= 0) arr.splice(i, 1);
    else {
      if (max && arr.length >= max) return;
      arr.push(value);
    }
    obj[key] = arr;
  }

  function chip(opts) {
    return (
      '<button type="button" class="hcp-chip' +
      (opts.selected ? " is-selected" : "") +
      '" data-chip-group="' +
      esc(opts.group) +
      '" data-chip-value="' +
      esc(opts.value) +
      '"' +
      (opts.multi ? ' data-multi="1"' : "") +
      ">" +
      esc(opts.label) +
      "</button>"
    );
  }

  function chipGroup(group, options, selected, multi) {
    var html = '<div class="hcp-chip-row">';
    (options || []).forEach(function (o) {
      var id = o.id || o;
      var label = o.label || o;
      var isSel = multi
        ? (selected || []).indexOf(id) >= 0
        : selected === id;
      html += chip({
        group: group,
        value: id,
        label: label,
        selected: isSel,
        multi: multi,
      });
    });
    return html + "</div>";
  }

  async function refresh() {
    setLoading(true, "Loading Hotel Commercial Priorities…");
    state.error = null;
    var loadWatch = setTimeout(function () {
      setLoading(false);
    }, 25000);
    try {
      var dash = await api(
        "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/need-dates?" +
          airtableQs() +
          "&year=" +
          state.year,
      );
      state.dashboard = dash;
      state.profile = dash.commercialProfile || null;
      state.draft = profileToDraft(state.profile);
      state.dirty = false;
      // Restore hotel-keyed unsaved draft after profile load (never cross-hotel).
      if (
        opts.restoredDraft &&
        opts.restoredDraft.dirty === true &&
        opts.restoredDraft.draft &&
        typeof opts.restoredDraft.draft === "object"
      ) {
        state.draft = Object.assign({}, state.draft, opts.restoredDraft.draft);
        state.dirty = true;
        // Consume once so a later refresh does not re-apply a stale stash.
        opts.restoredDraft = null;
      }
      if (dash.commercialProfile && dash.commercialProfile.suggestions) {
        state.suggestions = dash.commercialProfile.suggestions;
      }
      try {
        sessionStorage.setItem("dealality.gdi.hotelId", state.hotelId);
        localStorage.setItem(STORAGE_KEY, state.hotelId);
      } catch (e) {}
    } catch (err) {
      state.error = err.message || String(err);
    } finally {
      clearTimeout(loadWatch);
      setLoading(false);
      render();
    }
  }

  function monthOverviewMap() {
    var overview = (state.dashboard && state.dashboard.monthlyOverview) || [];
    var map = {};
    overview.forEach(function (m) {
      var key = Number(m.month);
      if (!Number.isFinite(key)) key = m.month;
      map[key] = m;
      map[String(key)] = m;
    });
    return map;
  }

  function monthTiles() {
    var names = [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
    ];
    var byMonth = monthOverviewMap();
    var activeMonths =
      (state.dashboard &&
        state.dashboard.summary &&
        state.dashboard.summary.activeNeedMonths) ||
      0;
    var html =
      '<p class="form-label" id="hcpActiveMonthCount">Active need months: <strong>' +
      esc(String(activeMonths)) +
      "</strong></p>" +
      '<p class="form-label"><span class="help-text">Historical / replaced months stay visible but are not counted as active.</span></p>' +
      '<div class="hcp-month-tiles" role="group" aria-label="Commercial need calendar">';
    for (var i = 1; i <= 12; i++) {
      var m = byMonth[i] || {};
      var hasActive = m.hasActiveNeed === true;
      var hasAny = (m.periods || []).length > 0;
      var statusKey = String(m.visualStatus || "NO_CURRENT_NEED")
        .toLowerCase()
        .replace(/_/g, "-");
      var classes =
        "hcp-month-tile status-" +
        statusKey +
        (hasActive ? " has-active-need" : "") +
        (hasAny && !hasActive ? " has-historical" : "") +
        (state.selectedMonths[i] ? " is-selected" : "") +
        (state.drawerMonth === i ? " is-drawer-open" : "");
      html +=
        '<button type="button" class="' +
        classes +
        '" data-month="' +
        i +
        '"><span class="hcp-month-name">' +
        names[i - 1] +
        "</span>";
      if (hasAny) {
        html +=
          '<span class="hcp-month-status"><span class="hcp-status-icon">' +
          esc(m.visualStatusIcon || "○") +
          "</span> " +
          esc(m.visualStatusLabel || "") +
          "</span>";
        if (hasActive && m.headlinePriority) {
          html +=
            '<span class="hcp-month-priority pri-' +
            esc(String(m.headlinePriority).toLowerCase()) +
            '">' +
            esc(m.headlinePriority) +
            "</span>";
        }
        if (m.changeIndicatorLabel) {
          html +=
            '<span class="hcp-change-chip">' +
            esc(m.changeIndicatorLabel) +
            "</span>";
        }
        if (m.multipleNeeds) {
          html +=
            '<span class="hcp-multi-chip">' +
            esc(String((m.periods || []).length)) +
            " needs</span>";
        }
      } else {
        html += '<span class="hcp-month-status muted">No need</span>';
      }
      html += "</button>";
    }
    return html + "</div>";
  }

  function monthDrawer() {
    if (!state.drawerMonth) return "";
    var m = monthOverviewMap()[state.drawerMonth];
    if (!m) return "";
    var periodIds = (m.periods || []).map(function (p) {
      return p.needPeriodId;
    });
    var fullPeriods = ((state.dashboard && state.dashboard.periods) || []).filter(
      function (p) {
        return periodIds.indexOf(p.needPeriodId) >= 0;
      },
    );
    var hist = (
      (state.dashboard &&
        state.dashboard.changeHistory &&
        state.dashboard.changeHistory.changes) ||
      []
    ).filter(function (c) {
      return periodIds.indexOf(c.needPeriodId) >= 0;
    });
    if (!state.monthDraft) {
      state.monthDraft = {
        revenueNeed: ["GROUP_GUESTROOMS", "MEETING_SPACE"],
        priority: "HIGH",
        days: "ANY",
        peakRoomsOverride: null,
      };
    }
    var html =
      '<div class="hcp-drawer" id="hcpMonthDrawer" role="dialog" aria-label="Month need details">' +
      '<div class="hcp-drawer-head"><h3 class="form-section-title" style="font-size:1.1rem;margin:0">' +
      esc(m.label) +
      " " +
      esc(String(state.year)) +
      '</h3><button type="button" class="btn-secondary" id="hcpDrawerClose">Close</button></div>' +
      '<p class="form-section-subtitle" style="margin-bottom:12px">' +
      esc(m.visualStatusLabel || "No current need") +
      (m.headlinePriority ? " · Priority " + esc(m.headlinePriority) : "") +
      "</p>";

    html +=
      '<div class="form-group full-width"><label class="form-label">What do you need?</label>' +
      chipGroup(
        "monthNeed",
        [
          { id: "GROUP_GUESTROOMS", label: "Guestrooms / Groups" },
          { id: "MEETING_SPACE", label: "Meetings" },
          { id: "LOCAL_CATERING", label: "Catering" },
          { id: "BANQUETS", label: "Banquets" },
          { id: "PRIVATE_DINING", label: "Private Dining" },
          { id: "EXTENDED_STAY", label: "Extended Stay" },
        ],
        state.monthDraft.revenueNeed,
        true,
      ) +
      '</div><div class="form-group full-width"><label class="form-label">How important?</label>' +
      chipGroup(
        "monthPriority",
        [
          { id: "CRITICAL", label: "Critical" },
          { id: "HIGH", label: "High" },
          { id: "MEDIUM", label: "Medium" },
        ],
        state.monthDraft.priority,
        false,
      ) +
      '</div><div class="form-group full-width"><label class="form-label">Which days?</label>' +
      chipGroup(
        "monthDays",
        [
          { id: "ANY", label: "Any" },
          { id: "WEEKDAYS", label: "Weekdays" },
          { id: "WEEKENDS", label: "Weekends" },
        ],
        state.monthDraft.days,
        false,
      ) +
      '</div><div class="form-group full-width"><label class="form-label">This month only — peak rooms override<span class="help-text">Does not change hotel-wide preference.</span></label>' +
      chipGroup(
        "monthPeakOverride",
        [
          { id: "UNDER_10", label: "Under 10" },
          { id: "10_25", label: "10–25" },
          { id: "26_50", label: "26–50" },
          { id: "51_100", label: "51–100" },
          { id: "FLEXIBLE", label: "Flexible" },
        ],
        state.monthDraft.peakRoomsOverride,
        false,
      ) +
      '</div><div class="form-actions" style="margin-top:16px;padding-top:16px"><button type="button" class="btn-primary" id="hcpSaveMonths">Save selected months</button></div>';

    fullPeriods.forEach(function (p) {
      html +=
        '<div class="hcp-drawer-period"><h4 class="form-label">' +
        esc(p.periodLabel || p.needPeriodId) +
        "</h4>" +
        '<p class="form-label"><span class="help-text">Status: ' +
        esc(p.status) +
        " · Priority: " +
        esc(p.priority || "—") +
        " · Research: " +
        esc(p.researchStateLabel || p.researchState || "—") +
        " · Matches: " +
        esc(
          String(
            (p.matches && p.matches.existingMatch && p.matches.existingMatch.length) ||
              0,
          ),
        ) +
        "</span></p>" +
        '<div class="hcp-drawer-actions">' +
        '<button type="button" class="btn-secondary" data-quick-action="PRIORITY_CHANGED" data-period="' +
        esc(p.needPeriodId) +
        '" data-priority="HIGH">Increase Priority</button>' +
        '<button type="button" class="btn-secondary" data-quick-action="PARTIALLY_FILLED" data-period="' +
        esc(p.needPeriodId) +
        '">Partially Filled</button>' +
        '<button type="button" class="btn-secondary" data-quick-action="FILLED" data-period="' +
        esc(p.needPeriodId) +
        '">Mark Filled</button>' +
        '<button type="button" class="btn-secondary" data-quick-action="REOPEN" data-period="' +
        esc(p.needPeriodId) +
        '">Reopen</button>' +
        '<button type="button" class="btn-secondary" data-quick-action="CANCEL" data-period="' +
        esc(p.needPeriodId) +
        '">Withdraw</button></div></div>';
    });

    html += '<h4 class="form-label" style="margin-top:16px">Change history</h4>';
    if (!hist.length) {
      html +=
        '<p class="form-label"><span class="help-text">No saved Airtable change events for these periods yet.</span></p>';
    } else {
      html += '<ol class="hcp-timeline">';
      hist.slice(0, 10).forEach(function (c) {
        html +=
          "<li><strong>" +
          esc(String(c.timestamp || "").slice(0, 10)) +
          "</strong> — " +
          esc(c.changeTypeLabel || c.changeType) +
          (c.reason ? " · " + esc(c.reason) : "") +
          "</li>";
      });
      html += "</ol>";
    }
    return html + "</div>";
  }

  function renderKnownAccountSuggestions() {
    var hints =
      (state.profile &&
        state.profile.suggestions &&
        state.profile.suggestions.inventoryAccountHints) ||
      (state.suggestions && state.suggestions.inventoryAccountHints) ||
      [];
    if (!hints.length) {
      return '<p class="form-label"><span class="help-text">No suggested accounts yet — skip for now.</span></p>';
    }
    var selected = (state.draft.knownAccounts || []).map(function (a) {
      return a.organizationName;
    });
    var html = '<div class="hcp-chip-row">';
    hints.slice(0, 8).forEach(function (h) {
      html += chip({
        group: "knownAccount",
        value: h.organizationName,
        label: h.organizationName,
        selected: selected.indexOf(h.organizationName) >= 0,
        multi: true,
      });
    });
    return html + "</div>";
  }

  function sectionBody(step) {
    var draft = state.draft || emptyDraft();
    if (step.id === "need-dates") {
      return (
        monthTiles() +
        monthDrawer() +
        '<p class="form-label"><span class="help-text">Tap months to select and open details. Each month saves independently.</span></p>'
      );
    }
    if (step.id === "local-market") {
      return (
        '<div class="form-group full-width"><label class="form-label">Local demand generator categories<span class="help-text">Suggestions are not hotel-confirmed until you select them.</span></label>' +
        chipGroup(
          "generators",
          [
            { id: "HOSPITALS_MEDICAL", label: "Hospitals / Medical" },
            { id: "UNIVERSITIES", label: "Universities" },
            { id: "CORPORATE_OFFICES", label: "Corporate Offices" },
            { id: "GOVERNMENT_AGENCIES", label: "Government Agencies" },
            { id: "CONVENTION_CENTERS", label: "Convention Centers" },
            { id: "WEDDING_VENUES", label: "Wedding Venues" },
            { id: "SPORTS_FACILITIES", label: "Sports Facilities" },
            { id: "ENTERTAINMENT_VENUES", label: "Entertainment Venues" },
            { id: "BUSINESS_PARKS", label: "Business Parks" },
            { id: "AIRPORTS_TRANSPORT", label: "Airports / Transportation" },
            { id: "OTHER", label: "Other" },
          ],
          draft.demandGeneratorCategories,
          true,
        ) +
        '</div><div class="form-group full-width" style="margin-top:22px"><label class="form-label">Known accounts<span class="help-text">Select from suggested organizations already in your intelligence.</span></label>' +
        renderKnownAccountSuggestions() +
        "</div>"
      );
    }
    if (step.id === "revenue") {
      return (
        '<div class="form-group full-width"><label class="form-label">Select up to five<span class="help-text">Structured choices only — no written Top Five required.</span></label>' +
        chipGroup(
          "revenue",
          [
            { id: "GROUP_ROOM_NIGHTS", label: "Group Room Nights" },
            { id: "CORPORATE_TRANSIENT", label: "Corporate Transient" },
            { id: "MEETINGS_CONFERENCES", label: "Meetings & Conferences" },
            { id: "LOCAL_CATERING", label: "Local Catering" },
            { id: "BANQUETS_CELEBRATIONS", label: "Banquets & Celebrations" },
            { id: "WEDDINGS_SOCIAL", label: "Weddings & Social Groups" },
            { id: "EXTENDED_STAY", label: "Extended Stay" },
            { id: "SPORTS_GROUPS", label: "Sports Groups" },
            { id: "ASSOCIATION_BUSINESS", label: "Association Business" },
            { id: "GOVERNMENT_BUSINESS", label: "Government Business" },
            { id: "LEISURE_GROUPS", label: "Leisure Groups" },
          ],
          draft.revenuePriorities,
          true,
        ) +
        "</div>"
      );
    }
    if (step.id === "preferred") {
      return (
        '<div class="form-grid">' +
        '<div class="form-group"><label class="form-label">Preferred peak rooms</label>' +
        chipGroup(
          "peakRooms",
          [
            { id: "UNDER_10", label: "Under 10" },
            { id: "10_25", label: "10–25" },
            { id: "26_50", label: "26–50" },
            { id: "51_100", label: "51–100" },
            { id: "101_200", label: "101–200" },
            { id: "200_PLUS", label: "200+" },
            { id: "FLEXIBLE", label: "Flexible" },
          ],
          draft.peakRoomsPreset,
          false,
        ) +
        "</div>" +
        '<div class="form-group"><label class="form-label">Preferred booking window</label>' +
        chipGroup(
          "bookingWindow",
          [
            { id: "UNDER_30_DAYS", label: "Under 30 days" },
            { id: "1_3_MONTHS", label: "1–3 months" },
            { id: "3_6_MONTHS", label: "3–6 months" },
            { id: "6_12_MONTHS", label: "6–12 months" },
            { id: "12_PLUS_MONTHS", label: "12+ months" },
            { id: "FLEXIBLE", label: "Flexible" },
          ],
          draft.bookingWindow,
          false,
        ) +
        "</div>" +
        '<div class="form-group"><label class="form-label">Meeting space requirement</label>' +
        chipGroup(
          "meetingSpace",
          [
            { id: "ROOMS_ONLY", label: "Rooms Only" },
            { id: "ROOMS_AND_MEETINGS", label: "Rooms + Meetings" },
            { id: "MEETINGS_CATERING_ONLY", label: "Meetings / Catering Only" },
            { id: "ANY", label: "Any" },
          ],
          draft.meetingSpaceNeed,
          false,
        ) +
        "</div>" +
        '<div class="form-group"><label class="form-label">Preferred duration</label>' +
        chipGroup(
          "stayDuration",
          [
            { id: "1_NIGHT", label: "1 night" },
            { id: "2_3_NIGHTS", label: "2–3 nights" },
            { id: "4_7_NIGHTS", label: "4–7 nights" },
            { id: "7_PLUS_NIGHTS", label: "7+ nights" },
            { id: "FLEXIBLE", label: "Flexible" },
          ],
          draft.stayDuration,
          false,
        ) +
        "</div>" +
        '<div class="form-group full-width"><label class="form-label">Preferred days</label>' +
        chipGroup(
          "dayPreference",
          [
            { id: "WEEKDAYS", label: "Weekdays" },
            { id: "WEEKENDS", label: "Weekends" },
            { id: "BOTH", label: "Both" },
          ],
          draft.dayPreference,
          false,
        ) +
        '</div><div class="form-group full-width"><label class="form-label">Specific days<span class="help-text">Optional Mon–Sun targeting (retained from PR #216).</span></label>' +
        chipGroup(
          "specificDays",
          [
            { id: "Mon", label: "Mon" },
            { id: "Tue", label: "Tue" },
            { id: "Wed", label: "Wed" },
            { id: "Thu", label: "Thu" },
            { id: "Fri", label: "Fri" },
            { id: "Sat", label: "Sat" },
            { id: "Sun", label: "Sun" },
          ],
          draft.specificDays || [],
          true,
        ) +
        "</div></div>"
      );
    }
    if (step.id === "customers") {
      return (
        '<div class="form-group full-width"><label class="form-label">Selection mode</label>' +
        chipGroup(
          "segmentMode",
          [
            { id: "ALL_RELEVANT", label: "All Relevant" },
            { id: "SELECT_SPECIFIC", label: "Select Specific" },
            { id: "NO_PREFERENCE", label: "No Preference" },
          ],
          draft.segmentMode,
          false,
        ) +
        "</div>" +
        (draft.segmentMode === "SELECT_SPECIFIC"
          ? '<div class="form-group full-width"><label class="form-label">Customer types</label>' +
            chipGroup(
              "segments",
              [
                { id: "association_events", label: "Associations & Conferences" },
                { id: "scaling_companies", label: "Corporate Accounts" },
                { id: "meetings_incentives", label: "Corporate Meetings" },
                { id: "government_public_sector", label: "Government" },
                { id: "sports_entertainment", label: "Sports & Entertainment" },
                { id: "education_scientific", label: "Education & Universities" },
                { id: "projects_infrastructure", label: "Projects & Construction" },
                { id: "social_special_events", label: "Weddings & Social Groups" },
                { id: "local_catering_day_meetings", label: "Local Catering" },
                { id: "corporate_celebrations", label: "Corporate Celebrations" },
                {
                  id: "travel_trade_group_intermediaries",
                  label: "Travel Trade",
                },
                {
                  id: "relocation_extended_stay",
                  label: "Relocation & Extended Stay",
                },
                {
                  id: "preferred_lodging_partnerships",
                  label: "Venue Partnerships",
                },
                { id: "professional_event_planners", label: "Event Planners" },
              ],
              draft.prioritySegments,
              true,
            ) +
            '</div><div class="form-group full-width"><label class="form-label">Lower priority<span class="help-text">Retained from PR #216.</span></label>' +
            chipGroup(
              "lowerPrioritySegments",
              [
                { id: "association_events", label: "Associations & Conferences" },
                { id: "scaling_companies", label: "Corporate Accounts" },
                { id: "meetings_incentives", label: "Corporate Meetings" },
                { id: "government_public_sector", label: "Government" },
                { id: "sports_entertainment", label: "Sports & Entertainment" },
                { id: "education_scientific", label: "Education & Universities" },
                { id: "projects_infrastructure", label: "Projects & Construction" },
                { id: "social_special_events", label: "Weddings & Social Groups" },
                { id: "local_catering_day_meetings", label: "Local Catering" },
                { id: "corporate_celebrations", label: "Corporate Celebrations" },
                { id: "travel_trade_group_intermediaries", label: "Travel Trade" },
                { id: "relocation_extended_stay", label: "Relocation & Extended Stay" },
                { id: "preferred_lodging_partnerships", label: "Venue Partnerships" },
                { id: "professional_event_planners", label: "Event Planners" },
              ],
              draft.lowerPrioritySegments || [],
              true,
            ) +
            '</div><div class="form-group full-width"><label class="form-label">Excluded segments<span class="help-text">Retained from PR #216.</span></label>' +
            chipGroup(
              "excludedSegments",
              [
                { id: "association_events", label: "Associations & Conferences" },
                { id: "scaling_companies", label: "Corporate Accounts" },
                { id: "meetings_incentives", label: "Corporate Meetings" },
                { id: "government_public_sector", label: "Government" },
                { id: "sports_entertainment", label: "Sports & Entertainment" },
                { id: "education_scientific", label: "Education & Universities" },
                { id: "projects_infrastructure", label: "Projects & Construction" },
                { id: "social_special_events", label: "Weddings & Social Groups" },
                { id: "local_catering_day_meetings", label: "Local Catering" },
                { id: "corporate_celebrations", label: "Corporate Celebrations" },
                { id: "travel_trade_group_intermediaries", label: "Travel Trade" },
                { id: "relocation_extended_stay", label: "Relocation & Extended Stay" },
                { id: "preferred_lodging_partnerships", label: "Venue Partnerships" },
                { id: "professional_event_planners", label: "Event Planners" },
              ],
              draft.excludedSegments || [],
              true,
            ) +
            "</div>"
          : "") +
        '<div class="form-group full-width" style="margin-top:16px"><label class="form-label">Restriction notes<span class="help-text">Optional free text (PR #216).</span></label>' +
        '<textarea class="form-input" id="hcpRestrictionNotes" rows="2" placeholder="e.g. Prefer midweek groups">' +
        esc(draft.restrictionNotes || "") +
        "</textarea></div>"
      );
    }
    if (step.id === "fit") {
      return (
        '<div class="form-grid"><div class="form-group"><label class="form-label">Business to avoid</label>' +
        chipGroup(
          "avoid",
          [
            { id: "LARGE_ROOM_BLOCKS", label: "Large room blocks" },
            { id: "SMALL_GROUPS", label: "Small groups" },
            { id: "LATE_NIGHT_EVENTS", label: "Late-night events" },
            { id: "WEEKEND_HEAVY", label: "Weekend-heavy groups" },
            { id: "LONG_STAY", label: "Long-stay groups" },
            { id: "MEETING_INTENSIVE", label: "Meeting-space intensive" },
            { id: "NO_RESTRICTIONS", label: "No restrictions" },
          ],
          draft.businessToAvoid,
          true,
        ) +
        '</div><div class="form-group"><label class="form-label">Hotel considerations</label>' +
        chipGroup(
          "considerations",
          [
            { id: "LIMITED_MEETING_SPACE", label: "Limited meeting space" },
            { id: "LIMITED_COACH_PARKING", label: "Limited coach/bus parking" },
            { id: "LIMITED_GROUP_INVENTORY", label: "Limited group inventory" },
            { id: "SEASONAL_RESTRICTIONS", label: "Seasonal restrictions" },
            { id: "NOISE_RESTRICTIONS", label: "Venue/event noise restrictions" },
            { id: "ACCESSIBILITY", label: "Accessibility considerations" },
            { id: "RENOVATION_PERIODS", label: "Renovation periods" },
            { id: "NO_SPECIAL", label: "No special restrictions" },
          ],
          draft.hotelConsiderations,
          true,
        ) +
        "</div></div>"
      );
    }
    if (step.id === "team") {
      return (
        '<div class="form-group full-width"><label class="form-label">Role selectors<span class="help-text">Optional. Use only if contacts are not already on your Dealality account.</span></label>' +
        chipGroup(
          "teamRoles",
          [
            { id: "SALES_LEAD", label: "Sales Lead" },
            { id: "REVENUE_CONTACT", label: "Revenue Contact" },
            { id: "GM_SPONSOR", label: "GM / Executive Sponsor" },
            { id: "MARKETING_DIGITAL", label: "Marketing / Digital" },
            { id: "HOTEL_COORDINATOR", label: "Hotel Coordinator" },
          ],
          Object.keys(draft.commercialTeam || {}),
          true,
        ) +
        "</div>"
      );
    }
    return "";
  }

  function render() {
    if (!root) return;
    try {
      renderInner();
    } catch (err) {
      state.error =
        "Render error: " + (err && err.message ? err.message : String(err));
      root.innerHTML =
        '<div class="intake-shell"><div class="intake-container"><div class="intake-banner warn">' +
        esc(state.error) +
        '</div><button type="button" class="btn-secondary" id="hcpRetry">Retry</button></div></div>';
      var retryBtn = document.getElementById("hcpRetry");
      if (retryBtn) retryBtn.addEventListener("click", refresh);
      setLoading(false);
    }
  }

  function renderInner() {
    if (state.error && !state.dashboard) {
      root.innerHTML =
        '<div class="intake-shell"><div class="intake-container"><div class="intake-banner warn">' +
        esc(state.error) +
        '</div><button type="button" class="btn-secondary" id="hcpRetry">Retry</button></div></div>';
      bind();
      return;
    }

    var d = state.dashboard || {};
    var hotelName = (d.hotel && d.hotel.name) || state.hotelId;
    var banner =
      d.dataMode === "AIRTABLE_AI_DEMAND_POSITIONING"
        ? '<div class="intake-banner">Saved to Airtable · AI Demand Positioning. Research does not start automatically.</div>'
        : '<div class="intake-banner warn">Local draft mode</div>';
    if (state.dirty) {
      banner +=
        '<div class="intake-banner warn">Unsaved commercial profile changes</div>';
    } else if (state.saveMessage) {
      banner +=
        '<div class="intake-banner ok">' + esc(state.saveMessage) + "</div>";
    }
    if (state.error) {
      banner +=
        '<div class="intake-banner warn">' + esc(state.error) + "</div>";
    }

    var sectionsHtml = SECTIONS.map(function (step, idx) {
      return (
        '<section class="form-section hcp-page-section active" id="hcp-section-' +
        esc(step.id) +
        '" data-section="' +
        esc(step.id) +
        '">' +
        (idx > 0 ? '<hr class="hcp-section-divider" />' : "") +
        '<h2 class="form-section-title">' +
        esc(step.title) +
        "</h2>" +
        '<p class="form-section-subtitle">' +
        esc(step.subtitle) +
        "</p>" +
        sectionBody(step) +
        "</section>"
      );
    }).join("");

    var html =
      '<div class="intake-shell hcp-intake hcp-intake--single-page">' +
      '<div class="intake-container">' +
      banner +
      '<header class="intake-header"><div class="intake-title-container"><h1>Commercial Priorities</h1></div>' +
      "<p>Help us find the right business for your hotel. Select your priorities and the dates where additional demand would be most valuable.</p>" +
      "<p>" +
      esc(hotelName) +
      " · " +
      esc(String(state.year)) +
      "</p></header>" +
      '<div class="form-sections-wrap hcp-single-page-sections">' +
      sectionsHtml +
      '<div class="form-actions hcp-sticky-save">' +
      '<span class="hcp-save-hint">' +
      (state.dirty ? "Unsaved changes" : "All changes saved") +
      "</span>" +
      '<button type="button" class="btn-primary" id="hcpSaveProfile">Save Changes</button>' +
      "</div></div></div></div>";

    root.innerHTML = html;
    bind();
  }

  function handleUiClick(e) {
    var t =
      e.target && e.target.closest
        ? e.target.closest(
            "[data-month], [data-quick-action], .hcp-chip, #hcpDrawerClose, #hcpSaveMonths, #hcpSaveProfile, #hcpRetry",
          )
        : null;
    if (!t || !root || !root.contains(t)) return;

    if (t.id === "hcpRetry") {
      refresh();
      return;
    }
    if (t.id === "hcpDrawerClose") {
      state.drawerMonth = null;
      render();
      return;
    }
    if (t.id === "hcpSaveMonths") {
      saveSelectedMonths();
      return;
    }
    if (t.id === "hcpSaveProfile") {
      saveProfileFn();
      return;
    }
    if (t.hasAttribute("data-month")) {
      var m = Number(t.getAttribute("data-month"));
      if (!Number.isFinite(m) || m < 1 || m > 12) return;
      state.selectedMonths[m] = true;
      state.drawerMonth = m;
      render();
      return;
    }
    if (t.hasAttribute("data-quick-action")) {
      quickLifecycle(
        t.getAttribute("data-period"),
        t.getAttribute("data-quick-action"),
        t.getAttribute("data-priority"),
      );
      return;
    }
    if (t.classList.contains("hcp-chip")) {
      var group = t.getAttribute("data-chip-group");
      var value = t.getAttribute("data-chip-value");
      if (!state.draft) state.draft = emptyDraft();
      if (!state.monthDraft) {
        state.monthDraft = {
          revenueNeed: [],
          priority: "HIGH",
          days: "ANY",
          peakRoomsOverride: null,
        };
      }
      if (group === "monthNeed") toggleArr(state.monthDraft, "revenueNeed", value);
      else if (group === "monthPriority") state.monthDraft.priority = value;
      else if (group === "monthDays") state.monthDraft.days = value;
      else if (group === "monthPeakOverride") {
        state.monthDraft.peakRoomsOverride =
          state.monthDraft.peakRoomsOverride === value ? null : value;
      } else if (group === "revenue") {
        toggleArr(state.draft, "revenuePriorities", value, 5);
        markDirty();
      } else if (group === "peakRooms") {
        state.draft.peakRoomsPreset = value;
        markDirty();
      } else if (group === "bookingWindow") {
        state.draft.bookingWindow = value;
        markDirty();
      } else if (group === "meetingSpace") {
        state.draft.meetingSpaceNeed = value;
        markDirty();
      } else if (group === "stayDuration") {
        state.draft.stayDuration = value;
        markDirty();
      } else if (group === "dayPreference") {
        state.draft.dayPreference = value;
        markDirty();
      } else if (group === "specificDays") {
        toggleArr(state.draft, "specificDays", value);
        markDirty();
      } else if (group === "segmentMode") {
        state.draft.segmentMode = value;
        markDirty();
      } else if (group === "segments") {
        toggleArr(state.draft, "prioritySegments", value);
        markDirty();
      } else if (group === "lowerPrioritySegments") {
        toggleArr(state.draft, "lowerPrioritySegments", value);
        markDirty();
      } else if (group === "excludedSegments") {
        toggleArr(state.draft, "excludedSegments", value);
        markDirty();
      } else if (group === "generators") {
        toggleArr(state.draft, "demandGeneratorCategories", value);
        markDirty();
      } else if (group === "avoid") {
        toggleArr(state.draft, "businessToAvoid", value);
        markDirty();
      } else if (group === "considerations") {
        toggleArr(state.draft, "hotelConsiderations", value);
        markDirty();
      } else if (group === "teamRoles") {
        if (!state.draft.commercialTeam) state.draft.commercialTeam = {};
        if (state.draft.commercialTeam[value])
          delete state.draft.commercialTeam[value];
        else
          state.draft.commercialTeam[value] = {
            role: value,
            source: "hotel_selected",
          };
        markDirty();
      } else if (group === "knownAccount") {
        var accounts = state.draft.knownAccounts || [];
        var idx = accounts.findIndex(function (a) {
          return a.organizationName === value;
        });
        if (idx >= 0) accounts.splice(idx, 1);
        else
          accounts.push({
            organizationName: value,
            status: "Actively Pursuing",
            provenance: "Suggested From Research",
          });
        state.draft.knownAccounts = accounts;
        markDirty();
      }
      render();
    }
  }

  function bind() {
    if (!root) return;
    // Listener on #hndRoot survives innerHTML re-renders (element identity preserved).
    if (!root.__hcpUiBound) {
      root.__hcpUiBound = true;
      root.addEventListener("click", handleUiClick);
      root.addEventListener("input", function (ev) {
        if (ev.target && ev.target.id === "hcpRestrictionNotes") {
          if (!state.draft) state.draft = emptyDraft();
          state.draft.restrictionNotes = String(ev.target.value || "");
          markDirty();
        }
      });
    }
    window.__hcpOpenMonth = function (month) {
      var m = Number(month);
      if (!Number.isFinite(m) || m < 1 || m > 12) return false;
      state.selectedMonths[m] = true;
      state.drawerMonth = m;
      render();
      return !!document.getElementById("hcpMonthDrawer");
    };
  }

  async function quickLifecycle(periodId, changeType, priority) {
    if (!periodId || !changeType) return;
    var period = ((state.dashboard && state.dashboard.periods) || []).find(
      function (p) {
        return p.needPeriodId === periodId;
      },
    );
    if (!period) return;
    setLoading(true, "Updating need period…");
    try {
      var patch = {};
      if (changeType === "PRIORITY_CHANGED" && priority) patch.priority = priority;
      await api(
        "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/need-periods/" +
          encodeURIComponent(periodId),
        {
          method: "PATCH",
          body: {
            airtable: state.airtable,
            changeType: changeType,
            expectedVersion: period.currentVersion,
            patch: patch,
            reason: "Commercial Priorities calendar quick action: " + changeType,
          },
        },
      );
      state.saveMessage = "Need period updated. History appended.";
      await refresh();
    } catch (err) {
      state.error = err.message || String(err);
      setLoading(false);
      render();
    }
  }

  async function saveSelectedMonths() {
    var months = Object.keys(state.selectedMonths)
      .filter(function (k) {
        return state.selectedMonths[k];
      })
      .map(Number)
      .sort(function (a, b) {
        return a - b;
      });
    if (!months.length && state.drawerMonth) months = [state.drawerMonth];
    if (!months.length) return;
    if (!state.monthDraft) {
      state.monthDraft = {
        revenueNeed: ["GROUP_GUESTROOMS"],
        priority: "HIGH",
        days: "ANY",
      };
    }
    setLoading(true, "Saving need months…");
    try {
      var existing = (state.dashboard.periods || []).map(function (p) {
        return Number(String(p.startDate || "").slice(5, 7));
      });
      var toCreate = months.filter(function (m) {
        return existing.indexOf(m) < 0;
      });
      if (toCreate.length) {
        await api(
          "/api/group-demand-intelligence/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/need-periods",
          {
            method: "POST",
            body: {
              airtable: state.airtable,
              months: toCreate.map(function (m) {
                return { year: state.year, month: m };
              }),
              revenueNeed: state.monthDraft.revenueNeed,
              priority: state.monthDraft.priority,
              preferredDays: state.monthDraft.days,
              preferredWeekpart: state.monthDraft.days,
              preferenceOverrides: state.monthDraft.peakRoomsOverride
                ? { peakRoomsPreset: state.monthDraft.peakRoomsOverride }
                : null,
              isTestScenario: true,
              status: "TEST_SCENARIO",
              source: "hotel_commercial_priorities_v2",
              salesNotes: "Hotel Commercial Priorities My Deals UI save.",
            },
          },
        );
      }
      for (var i = 0; i < months.length; i++) {
        var m = months[i];
        var period = (state.dashboard.periods || []).find(function (p) {
          return Number(String(p.startDate || "").slice(5, 7)) === m;
        });
        if (!period) continue;
        var patchBody = {
          revenueNeed: state.monthDraft.revenueNeed,
          priority: state.monthDraft.priority,
          preferredWeekpart: state.monthDraft.days,
        };
        if (state.monthDraft.peakRoomsOverride) {
          patchBody.preferenceOverrides = Object.assign(
            {},
            period.preferenceOverrides || {},
            { peakRoomsPreset: state.monthDraft.peakRoomsOverride },
          );
        }
        await api(
          "/api/group-demand-intelligence/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/need-periods/" +
            encodeURIComponent(period.needPeriodId),
          {
            method: "PATCH",
            body: {
              airtable: state.airtable,
              changeType: "MODIFY",
              expectedVersion: period.currentVersion,
              patch: patchBody,
              reason: "Commercial Priorities month quick-edit",
            },
          },
        );
      }
      state.selectedMonths = {};
      state.saveMessage = "Need months saved.";
      await refresh();
    } catch (err) {
      state.error = err.message || String(err);
      setLoading(false);
      render();
    }
  }

  async function saveProfileFn() {
    setLoading(true, "Saving commercial priorities…");
    try {
      var body = {
        airtable: state.airtable,
        year: state.year,
        expectedVersion: state.draft.currentVersion,
        changeReason: "Hotel Commercial Priorities My Deals UI save",
        profile: Object.assign({}, state.draft, {
          revenuePriorities: (state.draft.revenuePriorities || []).map(
            function (id) {
              return { id: id };
            },
          ),
        }),
      };
      var res = await api(
        "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/commercial-profile?" +
          airtableQs(),
        { method: "PUT", body: body },
      );
      state.profile = res.profile;
      state.draft = profileToDraft(res.profile);
      state.dirty = false;
      state.saveMessage =
        "Commercial priorities saved. Research was not started.";
      if (res.dashboard) state.dashboard = res.dashboard;
      setLoading(false);
      render();
    } catch (err) {
      state.error = err.message || String(err);
      setLoading(false);
      render();
    }
  }

  if (!window.__hcpBeforeUnloadBound) {
    window.__hcpBeforeUnloadBound = true;
    window.addEventListener("beforeunload", function (e) {
      if (!state.dirty) return;
      e.preventDefault();
      e.returnValue = "";
    });
  }

    function destroy() {
      try {
        if (root && root.__hcpUiBound) {
          root.removeEventListener("click", handleUiClick);
          root.__hcpUiBound = false;
        }
      } catch (e) {}
      try {
        root.innerHTML = "";
      } catch (e2) {}
    }

    function getState() {
      return {
        hotelId: state.hotelId,
        year: state.year,
        dirty: state.dirty,
        airtable: state.airtable,
      };
    }

    function exportDraft() {
      try {
        return state.draft ? JSON.parse(JSON.stringify(state.draft)) : null;
      } catch (e) {
        return state.draft || null;
      }
    }

    function setHotelId(hotelId) {
      var next = String(hotelId || "").trim();
      if (!next || next === state.hotelId) return Promise.resolve();
      // Caller (mount) is responsible for stashing; clear local UI only.
      state.hotelId = next;
      state.selectedMonths = {};
      state.monthDraft = null;
      state.drawerMonth = null;
      state.saveMessage = null;
      state.error = null;
      state.dirty = false;
      state.draft = null;
      state.profile = null;
      state.dashboard = null;
      return refresh();
    }

    return {
      refresh: refresh,
      destroy: destroy,
      getState: getState,
      exportDraft: exportDraft,
      setHotelId: setHotelId,
      root: root,
    };
  }

  var api = {
    _active: null,
    /** Hotel-keyed unsaved drafts — never merge across hotels. */
    _draftsByHotelId: Object.create(null),
    _stashActiveDraft: function () {
      if (!api._active || !api._active.getState) return;
      try {
        var st = api._active.getState();
        var hid = String(st.hotelId || "").trim();
        if (!hid) return;
        if (st.dirty === true && api._active.exportDraft) {
          api._draftsByHotelId[hid] = {
            dirty: true,
            draft: api._active.exportDraft(),
            stashedAt: Date.now(),
          };
        } else if (st.dirty !== true) {
          delete api._draftsByHotelId[hid];
        }
      } catch (e) {}
    },
    _closeSharedDialogs: function () {
      try {
        var d = document.getElementById("hndDrawer");
        if (d && d.open) d.close();
      } catch (e) {}
      try {
        var a = document.getElementById("hndAddDialog");
        if (a && a.open) a.close();
      } catch (e2) {}
      try {
        var h = document.getElementById("hcpDrawer");
        if (h && h.open) h.close();
      } catch (e3) {}
    },
    mount: function (container, options) {
      options = options || {};
      var hotelId = String(options.hotelId || "").trim();
      if (!hotelId) throw new Error("need_times_tab_hotelId_required");
      if (
        api._active &&
        api._active.getState &&
        api._active.getState().hotelId === hotelId &&
        api._active.root === container
      ) {
        return api._active;
      }
      if (
        api._active &&
        api._active.getState &&
        api._active.getState().hotelId === hotelId &&
        container &&
        api._active.root !== container
      ) {
        try {
          var prev = api._active.root;
          if (prev && prev !== container && prev.childNodes.length) {
            while (prev.firstChild) container.appendChild(prev.firstChild);
          }
          api._active.root = container;
        } catch (e) {}
        if (api._active.root === container) return api._active;
      }
      if (api._active) {
        var prevHotel =
          api._active.getState && api._active.getState().hotelId
            ? String(api._active.getState().hotelId)
            : "";
        if (prevHotel && prevHotel !== hotelId) {
          api._stashActiveDraft();
          api._closeSharedDialogs();
        }
        try {
          api._active.destroy();
        } catch (e) {}
        api._active = null;
      }
      var ctrl = createTabController({
        root: container,
        loading: options.loading || null,
        hotelId: hotelId,
        year: options.year,
        isolation: options.isolation,
        restoredDraft: api._draftsByHotelId[hotelId] || null,
      });
      ctrl.root = container;
      api._active = ctrl;
      ctrl.refresh();
      return ctrl;
    },
    unmount: function () {
      if (api._active) {
        api._stashActiveDraft();
        api._closeSharedDialogs();
        try {
          api._active.destroy();
        } catch (e) {}
        api._active = null;
      }
    },
    getDraftForHotel: function (hotelId) {
      return api._draftsByHotelId[String(hotelId || "").trim()] || null;
    },
    clearDraftForHotel: function (hotelId) {
      delete api._draftsByHotelId[String(hotelId || "").trim()];
    },
  };

  global.DealalityGdiNeedTimesPrioritiesTab = api;
})(typeof window !== "undefined" ? window : globalThis);
