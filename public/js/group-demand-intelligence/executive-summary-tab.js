/**
 * GDI Executive Summary tab — Phase 1 foundation.
 * Auth: FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127
 * Opportunities remains the default landing tab (wired in app.js).
 */
(function (global) {
  "use strict";

  var API_BASE = "/api/group-demand-intelligence";

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtNum(v) {
    if (v == null || !Number.isFinite(Number(v))) return "—";
    return String(Number(v));
  }

  function metricValue(m) {
    if (!m) return "—";
    if (m.value == null) return "—";
    return fmtNum(m.value);
  }

  function availabilityNote(m) {
    if (!m) return "";
    if (m.value != null) return "";
    var a = String(m.availability || "");
    if (a === "TELEMETRY_REQUIRED") return "Telemetry pending";
    if (a === "HOTEL_INPUT_REQUIRED") return "Hotel input required";
    if (a === "UNAVAILABLE") return "Unavailable";
    if (a === "PARTIAL_COVERAGE") return "Partial";
    if (a === "NOT_CURRENTLY_SUPPORTED") return "Not yet supported";
    return m.reason ? String(m.reason) : "Unavailable";
  }

  async function apiGet(path, apiFn) {
    if (typeof apiFn === "function") {
      return apiFn(path);
    }
    var memberAuth = global.DealalityMemberstackAuth;
    if (memberAuth && typeof memberAuth.fetchMyDealsApi === "function") {
      var res = await memberAuth.fetchMyDealsApi(path);
      // fetchMyDealsApi may return a Response or already-parsed JSON.
      if (res && typeof res.json === "function") return res.json();
      return res;
    }
    var r = await fetch(path, { credentials: "same-origin" });
    return r.json();
  }

  function dataStateBanner(dataState) {
    if (dataState === "CURRENT_AVAILABLE") return "";
    if (dataState === "STAGED_ONLY_NOT_CUSTOMER") {
      return (
        '<div class="gdi-exec-banner gdi-exec-banner--warn" role="status">' +
        "<strong>Publication not customer-authorized.</strong> " +
        "STAGED inventory exists but CURRENT is required for customer-facing metrics. " +
        "No fabricated statistics are shown." +
        "</div>"
      );
    }
    if (dataState === "FOUNDER_PREVIEW") {
      return (
        '<div class="gdi-exec-banner gdi-exec-banner--info" role="status">' +
        "<strong>Founder preview.</strong> Not a customer-facing publication view." +
        "</div>"
      );
    }
    return (
      '<div class="gdi-exec-banner gdi-exec-banner--muted" role="status">' +
      "<strong>No eligible CURRENT publication</strong> for this hotel. " +
      "Executive Summary metrics stay unavailable until authorized publication." +
      "</div>"
    );
  }

  function renderSnapshot(section) {
    var s = section || {};
    return (
      '<section class="gdi-exec-section" data-section="snapshot">' +
      "<header><h2>Executive Commercial Snapshot</h2>" +
      "<p>Commercial outcomes and actionable intelligence — not an Opportunities list reprint.</p></header>" +
      (s.unavailableReason
        ? '<p class="gdi-exec-muted">' + esc(s.unavailableReason) + "</p>"
        : "") +
      '<div class="gdi-exec-kpis">' +
      kpi("Qualified opportunities", s.totalQualifiedOpportunities) +
      kpi("New this period", s.newOpportunitiesSincePriorPeriod) +
      kpi("Materially changed", s.materiallyChangedOpportunities) +
      kpi("Actionable contacts", s.actionableOpportunities) +
      kpi("Need-period aligned", s.opportunitiesAlignedWithNeedPeriods) +
      "</div>" +
      '<div class="gdi-exec-meta">' +
      "<span>Reporting period: " +
      esc((s.reportingPeriod && s.reportingPeriod.label) || "Latest published") +
      "</span>" +
      "<span>Last research: " +
      esc(s.lastResearchCompletionDate || "—") +
      "</span></div>" +
      (s.priorityDemandSegments && s.priorityDemandSegments.length
        ? '<ul class="gdi-exec-segments">' +
          s.priorityDemandSegments
            .map(function (seg) {
              return (
                "<li><strong>" +
                esc(seg.name) +
                "</strong> · " +
                esc(seg.count) +
                "</li>"
              );
            })
            .join("") +
          "</ul>"
        : "") +
      "</section>"
    );
  }

  function kpi(label, value) {
    return (
      '<div class="gdi-exec-kpi"><div class="gdi-exec-kpi-value">' +
      esc(fmtNum(value)) +
      '</div><div class="gdi-exec-kpi-label">' +
      esc(label) +
      "</div></div>"
    );
  }

  function renderReceipt(receipt) {
    var metrics = (receipt && receipt.metrics) || [];
    var show = metrics.filter(function (m) {
      return (
        m.customerFacing !== false &&
        (m.value != null ||
          m.availability === "TELEMETRY_REQUIRED" ||
          m.availability === "PARTIAL_COVERAGE" ||
          m.availability === "AVAILABLE_DERIVED" ||
          m.availability === "AVAILABLE_EXISTING")
      );
    });
    return (
      '<section class="gdi-exec-section" data-section="receipt">' +
      "<header><h2>Dealality Research Receipt</h2>" +
      "<p>The work Dealality performed — hotel-attributable where evidenced. Synthetic counts are never shown.</p></header>" +
      '<div class="gdi-exec-receipt-grid">' +
      show
        .map(function (m) {
          var note = availabilityNote(m);
          return (
            '<div class="gdi-exec-receipt-card' +
            (m.value == null ? " is-unavailable" : "") +
            '"><div class="gdi-exec-kpi-value">' +
            esc(metricValue(m)) +
            '</div><div class="gdi-exec-kpi-label">' +
            esc(m.label) +
            "</div>" +
            (note
              ? '<div class="gdi-exec-receipt-note">' + esc(note) + "</div>"
              : "") +
            "</div>"
          );
        })
        .join("") +
      "</div></section>"
    );
  }

  function renderPriorities(section, hotelId) {
    var items = (section && section.items) || [];
    if (!items.length) {
      return (
        '<section class="gdi-exec-section" data-section="priorities">' +
        "<header><h2>Priority Commercial Opportunities</h2></header>" +
        '<p class="gdi-exec-muted">No priority opportunities available for this publication state.</p></section>'
      );
    }
    return (
      '<section class="gdi-exec-section" data-section="priorities">' +
      "<header><h2>Priority Commercial Opportunities</h2>" +
      "<p>Opportunity of the Week plus up to five priority opportunities using existing qualification signals — not discovery date alone.</p></header>" +
      '<div class="gdi-exec-priority-list">' +
      items
        .map(function (item, i) {
          var href =
            "?hotelId=" +
            encodeURIComponent(hotelId || "") +
            "&tab=opportunities&" +
            (item.deepLinkQuery || "");
          var ootw =
            item.opportunityOfTheWeek === true || i === 0
              ? '<span class="gdi-exec-ootw-badge">Opportunity of the Week</span> '
              : "";
          return (
            '<article class="gdi-exec-priority-card' +
            (item.opportunityOfTheWeek || i === 0
              ? " gdi-exec-priority-card--ootw"
              : "") +
            '">' +
            '<div class="gdi-exec-priority-rank">' +
            (i + 1) +
            "</div>" +
            "<div><h3>" +
            ootw +
            esc(item.name) +
            "</h3>" +
            '<div class="gdi-exec-priority-meta">' +
            esc(item.segment || "Segment TBD") +
            " · " +
            esc(item.demandTiming || "Timing TBD") +
            " · " +
            esc(item.contactActionabilityTier || "") +
            "</div>" +
            (item.hotelRationale
              ? '<p class="gdi-exec-rationale">' +
                esc(item.hotelRationale).slice(0, 280) +
                "</p>"
              : "") +
            (item.recommendedNextAction
              ? "<p><strong>Next:</strong> " +
                esc(item.recommendedNextAction).slice(0, 160) +
                "</p>"
              : "") +
            '<a class="gdi-exec-deep-link" href="' +
            esc(href) +
            '" data-opportunity-id="' +
            esc(item.opportunityId) +
            '">Open in Opportunities</a>' +
            "</div></article>"
          );
        })
        .join("") +
      "</div></section>"
    );
  }

  function renderEngagement(section) {
    var s = section || {};
    function row(label, m) {
      var val =
        m && m.value != null
          ? fmtNum(m.value)
          : m && m.zeroMeansNoActivity
            ? "0"
            : "—";
      var note =
        m && m.value == null && !m.zeroMeansNoActivity
          ? " <span class=\"gdi-exec-receipt-note\">(" +
            esc(availabilityNote(m) || "Unavailable") +
            ")</span>"
          : "";
      return (
        "<li><span>" +
        esc(label) +
        '</span><strong>' +
        esc(val) +
        "</strong>" +
        note +
        "</li>"
      );
    }
    return (
      '<section class="gdi-exec-section" data-section="engagement">' +
      "<header><h2>Commercial Engagement &amp; Outcomes</h2>" +
      "<p>Read-only from existing pursuit and feedback records. Zero activity is distinct from unavailable data.</p></header>" +
      '<ul class="gdi-exec-engagement-list">' +
      row("Opportunities pursued", s.opportunitiesPursued) +
      row("Marked useful", s.opportunitiesMarkedUseful) +
      row("RFP / proposal activity", s.rfpOrProposalActivity) +
      row("Won", s.opportunitiesWon) +
      row("Lost", s.opportunitiesLost) +
      row("Reported room nights", s.reportedRoomNights) +
      "</ul></section>"
    );
  }

  function renderInvestment(section) {
    return (
      '<section class="gdi-exec-section" data-section="investment">' +
      "<header><h2>Investment Recovery</h2>" +
      "<p>Contract established for a future phase. No synthetic ADR or unverified financials in Phase 1.</p></header>" +
      '<p class="gdi-exec-muted">Financial performance metrics are hidden until verified hotel inputs and Dealality investment terms are authorized.</p>' +
      '<ul class="gdi-exec-investment-keys">' +
      ((section && section.metrics) || [])
        .map(function (m) {
          return "<li>" + esc(m.key) + " · unavailable</li>";
        })
        .join("") +
      "</ul></section>"
    );
  }

  function renderPayload(payload, hotelId) {
    var sections = (payload && payload.sections) || {};
    return (
      '<div class="gdi-exec-page">' +
      '<header class="gdi-exec-hero">' +
      "<h1>Executive Summary</h1>" +
      "<p>We do the hard work so hotels can focus on winning business.</p>" +
      (payload.hotelName
        ? '<div class="gdi-exec-hotel">' + esc(payload.hotelName) + "</div>"
        : "") +
      "</header>" +
      dataStateBanner(payload.dataState) +
      renderSnapshot(sections.executiveCommercialSnapshot) +
      renderReceipt(sections.researchReceipt) +
      renderPriorities(sections.priorityCommercialOpportunities, hotelId) +
      renderEngagement(sections.commercialEngagementAndOutcomes) +
      renderInvestment(sections.investmentRecovery) +
      "</div>"
    );
  }

  function createTabController(opts) {
    opts = opts || {};
    var root = opts.root;
    if (!root) throw new Error("executive_summary_root_required");
    var hotelId = String(opts.hotelId || "").trim();
    var active = true;

    async function load() {
      if (!active) return;
      root.innerHTML =
        '<div class="gdi-exec-loading" role="status">Loading executive summary…</div>';
      if (!hotelId) {
        root.innerHTML =
          '<div class="gdi-exec-banner gdi-exec-banner--muted">Select a hotel to view the Executive Summary.</div>';
        return;
      }
      try {
        var q = new URLSearchParams(global.location.search || "");
        var preview = q.get("preview") || "";
        var path =
          API_BASE +
          "/hotels/" +
          encodeURIComponent(hotelId) +
          "/executive-summary" +
          (preview ? "?preview=" + encodeURIComponent(preview) : "");
        var data = await apiGet(path, opts.api);
        if (!active) return;
        if (!data || data.ok === false) {
          root.innerHTML =
            '<div class="gdi-exec-banner gdi-exec-banner--warn">' +
            esc((data && (data.message || data.error)) || "Unable to load Executive Summary") +
            "</div>";
          return;
        }
        root.innerHTML = renderPayload(data, hotelId);
        root.querySelectorAll("a.gdi-exec-deep-link").forEach(function (a) {
          a.addEventListener("click", function (ev) {
            if (typeof opts.onOpenOpportunity === "function") {
              ev.preventDefault();
              opts.onOpenOpportunity(a.getAttribute("data-opportunity-id"));
            }
          });
        });
      } catch (err) {
        if (!active) return;
        root.innerHTML =
          '<div class="gdi-exec-banner gdi-exec-banner--warn">Executive Summary failed to load.</div>';
        if (typeof console !== "undefined") console.error(err);
      }
    }

    function setHotelId(next) {
      hotelId = String(next || "").trim();
      return load();
    }

    function destroy() {
      active = false;
    }

    load();
    return { load: load, setHotelId: setHotelId, destroy: destroy };
  }

  function mount(opts) {
    var ctrl = createTabController(opts);
    global.DealalityGdiExecutiveSummaryTab._active = true;
    global.DealalityGdiExecutiveSummaryTab._controller = ctrl;
    return ctrl;
  }

  global.DealalityGdiExecutiveSummaryTab = {
    mount: mount,
    createTabController: createTabController,
    _active: false,
    _controller: null,
  };
})(typeof window !== "undefined" ? window : globalThis);
