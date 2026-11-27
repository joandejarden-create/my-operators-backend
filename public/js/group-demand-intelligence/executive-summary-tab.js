/**
 * GDI Executive Summary tab — consolidated customer commercial dashboard.
 *
 * Founder direction (simplification): no secondary perspective tabs.
 * Sales Performance + Research Intelligence live under Admin Resources.
 * Priority Commercial Opportunities reuse DealalityGdiUi.opportunityTileHtml
 * (exact same component as Group & Demand Opportunities Tiles View).
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
    if (m == null) return "—";
    if (typeof m === "object") {
      if (m.value == null) return "—";
      return fmtNum(m.value);
    }
    return fmtNum(m);
  }

  function availabilityNote(m) {
    if (!m || typeof m !== "object") return "";
    if (m.value != null) return "";
    var a = String(m.availability || "");
    if (a === "TELEMETRY_REQUIRED") return "Telemetry pending";
    if (a === "HOTEL_INPUT_REQUIRED") return "Hotel input required";
    if (a === "UNAVAILABLE") return "Unavailable";
    if (a === "PARTIAL_COVERAGE") return "Partial";
    if (a === "NOT_CURRENTLY_SUPPORTED") return "Not yet supported";
    return m.reason || m.note ? String(m.reason || m.note) : "Unavailable";
  }

  function withTimeout(promise, ms, label) {
    return Promise.race([
      promise,
      new Promise(function (_, reject) {
        setTimeout(function () {
          var err = new Error(label || "timeout");
          err.code = "TIMEOUT";
          reject(err);
        }, ms);
      }),
    ]);
  }

  async function apiGet(path, apiFn) {
    if (typeof apiFn === "function") {
      try {
        return await withTimeout(apiFn(path), 8000, "executive_summary_api_timeout");
      } catch (err) {
        if (err && err.code === "TIMEOUT") {
          return {
            ok: false,
            error: "timeout",
            message:
              "Executive Summary request timed out. Retry after signing in.",
          };
        }
        return {
          ok: false,
          error: (err && err.code) || "request_failed",
          message: (err && err.message) || "Unable to load Executive Summary",
          status: err && err.status,
        };
      }
    }
    var memberAuth = global.DealalityMemberstackAuth;
    if (memberAuth && typeof memberAuth.fetchMyDealsApi === "function") {
      var res = await withTimeout(
        memberAuth.fetchMyDealsApi(path),
        8000,
        "executive_summary_api_timeout",
      );
      if (res && typeof res.json === "function") return res.json();
      return res;
    }
    var r = await fetch(path, { credentials: "same-origin" });
    return r.json();
  }

  function stagedChip(payload) {
    if (payload && payload.dataState === "FOUNDER_PREVIEW") {
      return '<span class="gdi-es-status-chip gdi-es-status-chip--staged">Staged</span>';
    }
    if (payload && payload.dataState === "CURRENT_AVAILABLE") {
      return '<span class="gdi-es-status-chip gdi-es-status-chip--current">Current</span>';
    }
    return "";
  }

  function kpiCard(label, value, note) {
    return (
      '<div class="gdi-es-kpi">' +
      '<div class="gdi-es-kpi__value">' +
      esc(value == null ? "—" : fmtNum(value)) +
      "</div>" +
      '<div class="gdi-es-kpi__label">' +
      esc(label) +
      "</div>" +
      (note
        ? '<div class="gdi-es-kpi__note">' + esc(note) + "</div>"
        : "") +
      "</div>"
    );
  }

  function kpiFromObj(k) {
    var note = "";
    if (k && k.value == null) {
      note = availabilityNote(k) || k.note || "Unavailable";
    }
    return kpiCard(k.label || k.key, k ? k.value : null, note);
  }

  function renderBarChart(composition) {
    var rows = (composition && composition.rows) || [];
    if (!rows.length) {
      return '<p class="gdi-es-muted">Demand composition is unavailable for this inventory.</p>';
    }
    var max = Math.max.apply(
      null,
      rows.map(function (r) {
        return r.count || 0;
      }),
    );
    return (
      '<div class="gdi-es-bars" role="img" aria-label="Demand composition chart">' +
      rows
        .map(function (r) {
          var pct = max ? Math.round((r.count / max) * 100) : 0;
          return (
            '<div class="gdi-es-bar-row">' +
            '<div class="gdi-es-bar-label">' +
            esc(r.name) +
            "</div>" +
            '<div class="gdi-es-bar-track"><div class="gdi-es-bar-fill" style="width:' +
            pct +
            '%"></div></div>' +
            '<div class="gdi-es-bar-count">' +
            esc(r.count) +
            "</div></div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function renderOutlook(outlook) {
    var timeline = (outlook && outlook.timeline) || [];
    var buckets = (outlook && outlook.buckets) || {};
    return (
      '<div class="gdi-es-outlook">' +
      '<div class="gdi-es-outlook__buckets">' +
      kpiCard("Established Dates", buckets.establishedEventDates) +
      kpiCard("Estimated Periods", buckets.estimatedDemandPeriods) +
      kpiCard("Ongoing Accounts", buckets.ongoingAccountRelationships) +
      kpiCard("Timing Not Established", buckets.timingNotEstablished) +
      "</div>" +
      (timeline.length
        ? '<div class="gdi-es-bars gdi-es-bars--compact">' +
          timeline
            .map(function (t) {
              return (
                '<div class="gdi-es-bar-row"><div class="gdi-es-bar-label">' +
                esc(t.month) +
                '</div><div class="gdi-es-bar-track"><div class="gdi-es-bar-fill" style="width:' +
                Math.min(100, t.count * 18) +
                '%"></div></div><div class="gdi-es-bar-count">' +
                esc(t.count) +
                "</div></div>"
              );
            })
            .join("") +
          "</div>"
        : '<p class="gdi-es-muted">' +
          esc(
            (outlook && outlook.note) ||
              "No established event months — dates are not invented.",
          ) +
          "</p>") +
      "</div>"
    );
  }

  function renderAssessment(a) {
    a = a || {};
    var rows = [
      ["Market Opportunity", a.marketOpportunity],
      ["Strongest Demand Segments", a.strongestCommercialDemandSegments],
      ["Immediate Commercial Focus", a.immediateCommercialFocus],
      ["Competitive Capture Potential", a.competitiveCapturePotential],
      ["Material Changes", a.materialChangesSinceLastResearch],
      ["Business Impact", a.businessImpact],
    ];
    return (
      '<div class="gdi-es-assessment">' +
      rows
        .map(function (r) {
          return (
            '<div class="gdi-es-assessment__item"><h3>' +
            esc(r[0]) +
            "</h3><p>" +
            esc(r[1] || "—") +
            "</p></div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function renderReceipt(receipt) {
    var metrics = (receipt && receipt.metrics) || [];
    return (
      '<div class="gdi-es-receipt-grid">' +
      metrics
        .map(function (m) {
          return (
            '<div class="gdi-es-receipt-card' +
            (m.value == null ? " is-unavailable" : "") +
            '"><div class="gdi-es-kpi__value">' +
            esc(metricValue(m)) +
            '</div><div class="gdi-es-kpi__label">' +
            esc(m.label || m.key) +
            "</div>" +
            (availabilityNote(m)
              ? '<div class="gdi-es-kpi__note">' +
                esc(availabilityNote(m)) +
                "</div>"
              : "") +
            "</div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function fmtProductivityValue(m) {
    if (!m || m.value == null) return "—";
    if (m.currency === "USD") {
      return "$" + fmtNum(m.value);
    }
    return fmtNum(m.value);
  }

  function renderResearchProductivity(section) {
    section = section || {};
    var metrics = section.metrics || [];
    return (
      '<section class="gdi-es-section gdi-es-productivity">' +
      '<header class="gdi-es-section__head">' +
      "<h2>" +
      esc(section.headline || "Your Time. Better Invested.") +
      "</h2>" +
      "<p>" +
      esc(
        section.lede ||
          "Less time searching for business. More time building relationships, pursuing opportunities and converting demand into bookings.",
      ) +
      "</p></header>" +
      '<div class="gdi-es-kpi-ribbon gdi-es-productivity__kpis">' +
      metrics
        .map(function (m) {
          var note =
            m.value == null
              ? availabilityNote(m) || m.note || "Unavailable"
              : m.estimateClass === "PROVISIONAL_ESTIMATE"
                ? "Provisional estimate"
                : "";
          return (
            '<div class="gdi-es-kpi' +
            (m.value == null ? " is-unavailable" : "") +
            '"><div class="gdi-es-kpi__value">' +
            esc(fmtProductivityValue(m)) +
            '</div><div class="gdi-es-kpi__label">' +
            esc(m.label || m.key) +
            "</div>" +
            (note
              ? '<div class="gdi-es-kpi__note">' + esc(note) + "</div>"
              : "") +
            "</div>"
          );
        })
        .join("") +
      "</div>" +
      '<details class="gdi-es-methodology">' +
      "<summary>How This Estimate Is Calculated</summary>" +
      "<p>" +
      esc(
        section.methodologySummary ||
          "Verified research activity volumes × provisional manual effort benchmarks, minus residual hotel review time.",
      ) +
      "</p>" +
      "<p>" +
      esc(
        section.disclaimer ||
          "Estimated research productivity — not payroll savings and not booked revenue.",
      ) +
      "</p></details></section>"
    );
  }

  function renderSalesTimeSaved(section) {
    if (!section || !section.breakdown || !section.breakdown.length) {
      return "";
    }
    return (
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>' +
      esc(section.headline || "Where Research Time Was Saved") +
      "</h2><p>Measured research volumes with provisional benchmark assumptions.</p></header>" +
      '<div class="gdi-es-receipt-grid">' +
      section.breakdown
        .map(function (b) {
          return (
            '<div class="gdi-es-receipt-card"><div class="gdi-es-kpi__value">' +
            esc(b.estimatedHoursBase == null ? "—" : fmtNum(b.estimatedHoursBase)) +
            '</div><div class="gdi-es-kpi__label">' +
            esc(b.label) +
            "</div>" +
            '<div class="gdi-es-kpi__note">Est. hours · volume ' +
            esc(b.volume == null ? "—" : b.volume) +
            "</div></div>"
          );
        })
        .join("") +
      "</div>" +
      '<p class="gdi-es-muted">' +
      esc(
        section.disclaimer ||
          "Provisional estimates. Ordinary commercial pursuit is excluded.",
      ) +
      "</p></section>"
    );
  }

  /**
   * Priority Commercial Opportunities — exact Opportunities Tiles View component.
   * OOTW badge is outside the tile so the tile markup stays identical.
   */
  function renderPrioritySharedTiles(section, hotelId) {
    var UI = global.DealalityGdiUi;
    var rows =
      (section && section.opportunityRows) ||
      (section && section.items
        ? section.items
            .map(function () {
              return null;
            })
            .filter(Boolean)
        : []);
    if (!section || (!rows.length && !(section.items && section.items.length))) {
      return '<p class="gdi-es-muted">No priority opportunities available for this publication state.</p>';
    }
    if (!UI || typeof UI.opportunityCardsGridHtml !== "function") {
      return (
        '<p class="gdi-es-banner gdi-es-banner--warn" role="alert">' +
        "Canonical opportunity tile component failed to load." +
        "</p>"
      );
    }
    var tileRows = Array.isArray(section.opportunityRows)
      ? section.opportunityRows.slice(0, 4)
      : [];
    if (!tileRows.length) {
      return '<p class="gdi-es-muted">Priority opportunity rows are unavailable for shared tiles.</p>';
    }
    var ootwId =
      (section.opportunityOfTheWeek &&
        section.opportunityOfTheWeek.opportunityId) ||
      (tileRows[0] &&
        (tileRows[0].opportunityId ||
          tileRows[0].id ||
          (tileRows[0].identity && tileRows[0].identity.opportunityId))) ||
      "";
    var gridHtml = UI.opportunityCardsGridHtml(tileRows, "tiles", null, {
      sharedSurface: "executive-summary",
    });
    var viewAllHref =
      "?hotelId=" +
      encodeURIComponent(hotelId || "") +
      "&tab=opportunities";
    return (
      '<div class="gdi-es-priority-shared" data-shared-tile-component="DealalityGdiUi.opportunityTileHtml">' +
      (ootwId
        ? '<div class="gdi-es-ootw-outside" data-ootw-opportunity-id="' +
          esc(String(ootwId)) +
          '"><span class="gdi-es-ootw">Opportunity of the Week</span></div>'
        : "") +
      gridHtml +
      '<p class="gdi-es-view-all"><a class="gdi-es-deep-link gdi-es-view-all__link" href="' +
      esc(viewAllHref) +
      '" data-view-all-opportunities="1">View All Opportunities</a></p>' +
      "</div>"
    );
  }

  function renderOutcomes(section) {
    var s = section || {};
    function row(label, m) {
      var val = m && m.value != null ? fmtNum(m.value) : "—";
      var note =
        m && m.value == null
          ? " <span class=\"gdi-es-kpi__note\">(" +
            esc(availabilityNote(m) || "Unavailable") +
            ")</span>"
          : "";
      return (
        "<li><span>" +
        esc(label) +
        "</span><strong>" +
        esc(val) +
        "</strong>" +
        note +
        "</li>"
      );
    }
    return (
      '<ul class="gdi-es-outcomes">' +
      row("Opportunities Pursued", s.opportunitiesPursued) +
      row("RFP / Proposal", s.rfpOrProposalActivity) +
      row("Business Won", s.opportunitiesWon) +
      row("Business Lost", s.opportunitiesLost) +
      row("Reported Room Nights", s.reportedRoomNights) +
      "</ul>" +
      '<p class="gdi-es-muted">Estimated opportunity potential is never mixed with realized business.</p>'
    );
  }

  function renderCustomerExecutiveSummary(payload, hotelId) {
    var ov = (payload.sections && payload.sections.executiveOverview) || {};
    var snap =
      (payload.sections && payload.sections.executiveCommercialSnapshot) || {};
    var period = (snap.reportingPeriod && snap.reportingPeriod.label) || "—";
    var priorities =
      (ov && ov.priorityCommercialOpportunities) ||
      (payload.sections && payload.sections.priorityCommercialOpportunities) ||
      {};
    return (
      '<section class="gdi-es-section">' +
      '<header class="gdi-es-section__head"><h2>Commercial Snapshot</h2>' +
      "<p>Reporting period: " +
      esc(period) +
      (snap.lastResearchCompletionDate
        ? " · As of " + esc(String(snap.lastResearchCompletionDate).slice(0, 10))
        : "") +
      "</p></header>" +
      '<div class="gdi-es-kpi-ribbon">' +
      (ov.commercialKpiRibbon || [])
        .map(function (k) {
          return kpiFromObj(k);
        })
        .join("") +
      "</div></section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Executive Commercial Assessment</h2></header>' +
      renderAssessment(ov.executiveAssessment) +
      "</section>" +
      '<div class="gdi-es-split">' +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Demand Composition</h2><p>Qualified inventory by canonical demand segment.</p></header>' +
      renderBarChart(ov.demandComposition) +
      "</section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Commercial Demand Outlook</h2><p>Evidenced future demand windows only.</p></header>' +
      renderOutlook(ov.commercialDemandOutlook) +
      "</section></div>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Dealality Research Receipt</h2><p>Commercially meaningful research metrics for this hotel.</p></header>' +
      renderReceipt(ov.customerResearchReceipt) +
      "</section>" +
      renderResearchProductivity(
        ov.researchProductivity ||
          (ov.customerResearchReceipt &&
            ov.customerResearchReceipt.researchProductivity),
      ) +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Priority Commercial Opportunities</h2>' +
      "<p>Same opportunity tiles as Group &amp; Demand Opportunities.</p></header>" +
      renderPrioritySharedTiles(priorities, hotelId) +
      "</section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Commercial Outcomes &amp; Investment Recovery</h2></header>' +
      renderOutcomes(ov.commercialOutcomes) +
      '<div class="gdi-es-invest-split">' +
      '<div class="gdi-es-invest-card"><h3>A. Estimated Research Productivity Value</h3><p>' +
      esc(
        (ov.investmentRecovery &&
          ov.investmentRecovery.categories &&
          ov.investmentRecovery.categories.estimatedResearchProductivityValue &&
          ov.investmentRecovery.categories.estimatedResearchProductivityValue
            .note) ||
          "Redeployable research time estimate — not actual payroll savings.",
      ) +
      "</p></div>" +
      '<div class="gdi-es-invest-card"><h3>B. Verified Incremental Commercial Value</h3><p>' +
      esc(
        (ov.investmentRecovery &&
          ov.investmentRecovery.categories &&
          ov.investmentRecovery.categories.verifiedIncrementalCommercialValue &&
          ov.investmentRecovery.categories.verifiedIncrementalCommercialValue
            .note) ||
          "Hotel-reported or otherwise verified commercial outcomes only.",
      ) +
      "</p></div></div>" +
      '<p class="gdi-es-muted">' +
      esc(
        (ov.investmentRecovery && ov.investmentRecovery.customerCopy) ||
          "Category A and Category B are never combined into a single realized ROI claim.",
      ) +
      "</p></section>"
    );
  }

  function renderSalesPerformance(payload) {
    var sp = (payload.sections && payload.sections.salesPerformance) || {};
    var funnel = sp.funnel || {};
    var focus = sp.commercialFocus || {};
    return (
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>GDI Sales Performance</h2><p>Internal commercial performance for Dealality founder and admin users.</p></header>' +
      '<div class="gdi-es-kpi-ribbon">' +
      (sp.kpis || [])
        .map(function (k) {
          return kpiFromObj(k);
        })
        .join("") +
      "</div></section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Commercial Conversion Funnel</h2>' +
      "<p>" +
      esc(funnel.note || "") +
      "</p></header>" +
      '<div class="gdi-es-funnel">' +
      (funnel.stages || [])
        .map(function (st) {
          return (
            '<div class="gdi-es-funnel__stage"><div class="gdi-es-kpi__value">' +
            esc(st.value == null ? "—" : fmtNum(st.value)) +
            '</div><div class="gdi-es-kpi__label">' +
            esc(st.label) +
            "</div></div>"
          );
        })
        .join("") +
      "</div></section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Commercial Focus</h2></header>' +
      '<div class="gdi-es-focus-grid">' +
      '<div class="gdi-es-focus-card"><h3>Immediate Pursuit</h3><ul>' +
      ((focus.immediatePursuit || [])
        .map(function (r) {
          return (
            "<li>" +
            esc(r.title) +
            (r.salesAction ? " · " + esc(r.salesAction) : "") +
            "</li>"
          );
        })
        .join("") || '<li class="gdi-es-muted">None queued</li>') +
      "</ul></div>" +
      '<div class="gdi-es-focus-card"><h3>Upcoming Booking Windows</h3><ul>' +
      ((focus.upcomingBookingWindows || [])
        .map(function (t) {
          return (
            "<li>" + esc(t.month) + " · " + esc(t.count) + " opportunities</li>"
          );
        })
        .join("") ||
        '<li class="gdi-es-muted">No established event months</li>') +
      "</ul></div>" +
      '<div class="gdi-es-focus-card"><h3>Account &amp; Competitive</h3><ul>' +
      "<li>Corporate account actions: " +
      esc(
        focus.corporateAccountDevelopment == null
          ? "—"
          : focus.corporateAccountDevelopment,
      ) +
      "</li>" +
      "<li>Competitive capture: " +
      esc(focus.competitiveCapture == null ? "—" : focus.competitiveCapture) +
      "</li>" +
      "<li>Need-period opportunities: " +
      esc(
        focus.needPeriodOpportunities == null
          ? "—"
          : focus.needPeriodOpportunities,
      ) +
      "</li></ul></div></div></section>" +
      renderSalesTimeSaved(sp.researchTimeSaved)
    );
  }

  function renderResearchIntelligence(payload) {
    var ri = payload.sections && payload.sections.researchIntelligence;
    if (!ri) {
      return (
        '<section class="gdi-es-section"><div class="gdi-es-banner gdi-es-banner--warn">' +
        "Research Intelligence is restricted to Dealality founder and admin sessions." +
        "</div></section>"
      );
    }
    return (
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>GDI Research Intelligence</h2>' +
      "<p>Internal operational metrics for qualified inventory grain. Raw discovery volume is separate.</p></header>" +
      '<div class="gdi-es-kpi-ribbon">' +
      (ri.metrics || [])
        .map(function (m) {
          return kpiCard(
            m.label,
            typeof m.value === "number" ? m.value : m.value,
            m.availability === "UNAVAILABLE" ? m.note || "Unavailable" : m.note || "",
          );
        })
        .join("") +
      "</div>" +
      '<h3 class="gdi-es-subhead">Demand Segment Coverage</h3>' +
      renderBarChart(ri.demandComposition) +
      (ri.researchProductivity
        ? '<p class="gdi-es-muted">Benchmark status: ' +
          esc(
            (ri.researchProductivity.internalMetrics &&
              ri.researchProductivity.internalMetrics.benchmarkValidationStatus) ||
              ri.researchProductivity.estimateClass ||
              "PROVISIONAL",
          ) +
          ". AI elapsed time is not counted as human labor.</p>"
        : "") +
      "</section>"
    );
  }

  function renderPayload(payload, hotelId) {
    return (
      '<div class="gdi-es-page" data-ui-version="v2" data-es-layout="consolidated-customer">' +
      '<header class="gdi-es-hero">' +
      '<div class="gdi-es-hero__title"><h1>Executive Summary</h1>' +
      stagedChip(payload) +
      "</div>" +
      (payload.hotelName
        ? '<div class="gdi-es-hotel">' + esc(payload.hotelName) + "</div>"
        : "") +
      '<p class="gdi-es-hero__lede">Commercial intelligence for leadership — not a technical validation page.</p>' +
      "</header>" +
      '<div class="gdi-es-body">' +
      renderCustomerExecutiveSummary(payload, hotelId) +
      "</div></div>"
    );
  }

  function createTabController(opts) {
    opts = opts || {};
    var root = opts.root;
    if (!root) throw new Error("executive_summary_root_required");
    var hotelId = String(opts.hotelId || "").trim();
    var active = true;
    var lastPayload = null;

    function bindInteractions() {
      root.querySelectorAll("[data-open]").forEach(function (el) {
        el.addEventListener("click", function (ev) {
          var innermost = ev.target.closest("[data-open]");
          if (innermost !== el) return;
          var id = el.getAttribute("data-open");
          if (!id) return;
          if (typeof opts.onOpenOpportunity === "function") {
            ev.preventDefault();
            opts.onOpenOpportunity(id);
          }
        });
      });
      root.querySelectorAll("a.gdi-es-deep-link").forEach(function (a) {
        a.addEventListener("click", function (ev) {
          if (a.getAttribute("data-view-all-opportunities") === "1") {
            if (typeof opts.onOpenOpportunity === "function") {
              ev.preventDefault();
              opts.onOpenOpportunity(null);
            }
            return;
          }
          if (typeof opts.onOpenOpportunity === "function") {
            ev.preventDefault();
            opts.onOpenOpportunity(a.getAttribute("data-opportunity-id"));
          }
        });
      });
    }

    async function load() {
      if (!active) return;
      root.innerHTML =
        '<div class="gdi-es-loading" role="status">Loading executive summary…</div>';
      if (!hotelId) {
        root.innerHTML =
          '<div class="gdi-es-banner gdi-es-banner--muted">Select a hotel to view the Executive Summary.</div>';
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
            '<div class="gdi-es-banner gdi-es-banner--warn">' +
            esc(
              (data && (data.message || data.error)) ||
                "Unable to load Executive Summary",
            ) +
            "</div>";
          return;
        }
        lastPayload = data;
        root.innerHTML = renderPayload(data, hotelId);
        bindInteractions();
      } catch (err) {
        if (!active) return;
        root.innerHTML =
          '<div class="gdi-es-banner gdi-es-banner--warn">Executive Summary failed to load.</div>';
        if (typeof console !== "undefined") console.error(err);
      }
    }

    function setHotelId(next) {
      hotelId = String(next || "").trim();
      lastPayload = null;
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
    renderCustomerExecutiveSummary: renderCustomerExecutiveSummary,
    renderSalesPerformance: renderSalesPerformance,
    renderResearchIntelligence: renderResearchIntelligence,
    renderPrioritySharedTiles: renderPrioritySharedTiles,
    _active: false,
  };
})(typeof window !== "undefined" ? window : globalThis);
