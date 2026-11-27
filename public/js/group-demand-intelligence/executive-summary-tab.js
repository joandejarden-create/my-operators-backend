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

  function infoTip(text) {
    if (!text) return "";
    return (
      '<button type="button" class="gdi-es-info" title="' +
      esc(text) +
      '" aria-label="' +
      esc(text) +
      '">i</button>'
    );
  }

  function kpiCard(label, value, note, tooltip) {
    var unavailable = value == null;
    return (
      '<div class="gdi-es-kpi' +
      (unavailable ? " is-unavailable" : "") +
      '">' +
      '<div class="gdi-es-kpi__label-row">' +
      '<div class="gdi-es-kpi__label">' +
      esc(label) +
      "</div>" +
      infoTip(tooltip) +
      "</div>" +
      '<div class="gdi-es-kpi__value">' +
      esc(unavailable ? "—" : fmtNum(value)) +
      "</div>" +
      (note
        ? '<div class="gdi-es-kpi__note">' + esc(note) + "</div>"
        : "") +
      "</div>"
    );
  }

  function kpiFromObj(k) {
    if (!k) return "";
    // Skip measured-zero optional KPIs that are actually unsupported.
    if (
      k.value == null &&
      (k.availability === "NOT_CURRENTLY_SUPPORTED" ||
        k.omitWhenUnavailable === true)
    ) {
      return "";
    }
    var note = "";
    if (k.value == null) {
      note = availabilityNote(k) || k.note || "Unavailable";
    } else if (k.estimateClass === "PROVISIONAL_ESTIMATE") {
      note = "Provisional estimate";
    }
    return kpiCard(k.label || k.key, k.value, note, k.tooltip || k.note);
  }

  function renderBarChart(composition) {
    var rows = (composition && composition.rows) || [];
    var total = Number(composition && composition.total) || 0;
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
      '<div class="gdi-es-bars" role="img" aria-label="Demand composition by segment">' +
      rows
        .map(function (r) {
          var pctMax = max ? Math.round((r.count / max) * 100) : 0;
          var share = total ? Math.round((r.count / total) * 100) : 0;
          return (
            '<div class="gdi-es-bar-row">' +
            '<div class="gdi-es-bar-label" title="' +
            esc(r.name) +
            '">' +
            esc(r.name) +
            "</div>" +
            '<div class="gdi-es-bar-track"><div class="gdi-es-bar-fill" style="width:' +
            pctMax +
            '%"></div></div>' +
            '<div class="gdi-es-bar-count">' +
            esc(r.count) +
            ' <span class="gdi-es-bar-share">(' +
            esc(share) +
            "%)</span></div></div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function renderOutlook(outlook) {
    var timeline = (outlook && outlook.timeline) || [];
    var buckets = (outlook && outlook.buckets) || {};
    var max = Math.max.apply(
      null,
      [1].concat(
        timeline.map(function (t) {
          return t.count || 0;
        }),
      ),
    );
    return (
      '<div class="gdi-es-outlook">' +
      '<div class="gdi-es-outlook__buckets">' +
      kpiCard(
        "Established Event Dates",
        buckets.establishedEventDates,
        null,
        "Opportunities with evidenced calendar event dates.",
      ) +
      kpiCard(
        "Estimated Demand Periods",
        buckets.estimatedDemandPeriods,
        null,
        "Estimated windows without a confirmed event start date.",
      ) +
      kpiCard(
        "Ongoing Accounts",
        buckets.ongoingAccountRelationships,
        null,
        "Account relationships without a single event month.",
      ) +
      kpiCard(
        "Timing Not Established",
        buckets.timingNotEstablished,
        null,
        "Qualified opportunities without evidenced timing yet.",
      ) +
      "</div>" +
      (timeline.length
        ? '<div class="gdi-es-bars gdi-es-bars--compact" role="img" aria-label="Upcoming demand by month">' +
          timeline
            .map(function (t) {
              var width = Math.round(((t.count || 0) / max) * 100);
              return (
                '<div class="gdi-es-bar-row"><div class="gdi-es-bar-label">' +
                esc(t.monthLabel || t.month) +
                '</div><div class="gdi-es-bar-track"><div class="gdi-es-bar-fill" style="width:' +
                width +
                '%"></div></div><div class="gdi-es-bar-count">' +
                esc(t.count) +
                "</div></div>"
              );
            })
            .join("") +
          "</div>" +
          (outlook.note
            ? '<p class="gdi-es-muted gdi-es-outlook__note">' +
              esc(outlook.note) +
              "</p>"
            : "")
        : '<p class="gdi-es-muted">' +
          esc(
            (outlook && outlook.note) ||
              "No evidenced future event months. Dates are never invented.",
          ) +
          "</p>") +
      "</div>"
    );
  }

  function renderAssessment(a) {
    a = a || {};
    var cards =
      Array.isArray(a.primaryCards) && a.primaryCards.length
        ? a.primaryCards.concat(a.optionalCards || [])
        : [
            { title: "Market Opportunity", body: a.marketOpportunity },
            {
              title: "Recommended Commercial Focus",
              body: a.recommendedCommercialFocus || a.immediateCommercialFocus,
            },
            {
              title: "Forward Demand Outlook",
              body: a.forwardDemandOutlook || a.businessImpact,
            },
          ].concat(
            a.competitiveCapture || a.competitiveCapturePotential
              ? [
                  {
                    title: "Competitive Capture",
                    body: a.competitiveCapture || a.competitiveCapturePotential,
                  },
                ]
              : [],
          );
    return (
      '<div class="gdi-es-assessment gdi-es-assessment--primary">' +
      cards
        .filter(function (c) {
          return c && c.body;
        })
        .map(function (c) {
          return (
            '<article class="gdi-es-assessment__item"><h3>' +
            esc(c.title) +
            "</h3><p>" +
            esc(c.body) +
            "</p></article>"
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

  function renderConsolidatedReceipt(receipt, productivity) {
    receipt = receipt || {};
    productivity = productivity || {};
    var metrics = (receipt.metrics || []).filter(function (m) {
      return m && m.value != null;
    });
    var prodMetrics = (productivity.metrics || []).filter(function (m) {
      // Hide labor value unless authorized value is present.
      if (m.key === "estimatedResearchLaborValue" && m.value == null) return false;
      return true;
    });
    return (
      '<section class="gdi-es-section gdi-es-receipt">' +
      '<header class="gdi-es-section__head">' +
      "<h2>Dealality Research Receipt</h2>" +
      "<p>" +
      esc(
        productivity.lede ||
          "Your Time. Better Invested. — less time searching for demand, more time converting opportunities.",
      ) +
      "</p></header>" +
      (prodMetrics.length
        ? '<div class="gdi-es-kpi-ribbon gdi-es-receipt__productivity">' +
          prodMetrics
            .map(function (m) {
              var note =
                m.value == null
                  ? availabilityNote(m) || "Unavailable"
                  : m.estimateClass === "PROVISIONAL_ESTIMATE"
                    ? "Provisional estimate"
                    : "";
              return kpiCard(
                m.label || m.key,
                m.value,
                note,
                m.note || m.tooltip,
              );
            })
            .join("") +
          "</div>"
        : "") +
      (metrics.length
        ? '<div class="gdi-es-receipt-grid">' +
          metrics
            .map(function (m) {
              return kpiCard(
                m.label || m.key,
                m.value,
                availabilityNote(m),
                m.note,
              );
            })
            .join("") +
          "</div>"
        : "") +
      '<details class="gdi-es-methodology">' +
      "<summary>How Research Hours Are Estimated</summary>" +
      "<p>" +
      esc(
        productivity.methodologySummary ||
          "Verified research activity volumes × provisional benchmarks − residual hotel review time.",
      ) +
      "</p>" +
      "<p>" +
      esc(
        productivity.disclaimer ||
          "Provisional estimate — not payroll savings and not booked revenue.",
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
    var items = [
      { label: "Opportunities Pursued", m: s.opportunitiesPursued },
      { label: "RFPs & Proposals", m: s.rfpOrProposalActivity },
      { label: "Business Won", m: s.opportunitiesWon },
      { label: "Business Lost", m: s.opportunitiesLost },
      { label: "Booked Room Nights", m: s.reportedRoomNights },
      { label: "Attributed Revenue", m: s.attributedRevenue },
    ];
    var anyValue = items.some(function (it) {
      return it.m && it.m.value != null;
    });
    if (!anyValue) {
      return (
        '<p class="gdi-es-muted">' +
        "Commercial outcomes appear here when the hotel reports pursuits, proposals, wins, or booked room nights. Research-time estimates stay separate and are never shown as realized revenue." +
        "</p>"
      );
    }
    return (
      '<div class="gdi-es-kpi-ribbon gdi-es-outcomes-ribbon">' +
      items
        .map(function (it) {
          return kpiFromObj({
            label: it.label,
            value: it.m && it.m.value,
            availability: it.m && it.m.availability,
            note: it.m && it.m.note,
          });
        })
        .join("") +
      "</div>"
    );
  }

  function renderCustomerExecutiveSummary(payload, hotelId) {
    var ov = (payload.sections && payload.sections.executiveOverview) || {};
    var priorities =
      (ov && ov.priorityCommercialOpportunities) ||
      (payload.sections && payload.sections.priorityCommercialOpportunities) ||
      {};
    var productivity =
      ov.researchProductivity ||
      (ov.customerResearchReceipt &&
        ov.customerResearchReceipt.researchProductivity) ||
      {};
    return (
      '<section class="gdi-es-section">' +
      '<header class="gdi-es-section__head"><h2>Commercial Snapshot</h2>' +
      "<p>Key commercial metrics for this hotel’s qualified demand.</p></header>" +
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
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Demand Composition</h2><p>Qualified opportunities by demand segment.</p></header>' +
      renderBarChart(ov.demandComposition) +
      "</section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Commercial Demand Outlook</h2><p>Evidenced future demand windows only.</p></header>' +
      renderOutlook(ov.commercialDemandOutlook) +
      "</section></div>" +
      renderConsolidatedReceipt(ov.customerResearchReceipt, productivity) +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Priority Commercial Opportunities</h2></header>' +
      renderPrioritySharedTiles(priorities, hotelId) +
      "</section>" +
      '<section class="gdi-es-section"><header class="gdi-es-section__head"><h2>Commercial Outcomes</h2>' +
      "<p>Verified hotel-reported results — separate from research-time estimates.</p></header>" +
      renderOutcomes(ov.commercialOutcomes) +
      "</section>"
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

  function renderContactCoverageScorecard(scorecard) {
    if (!scorecard || scorecard.customerFacing === true) return "";
    var opp = scorecard.opportunityLevel || {};
    var person = scorecard.uniquePersonLevel || {};
    var defects = scorecard.recoveryAndDefects || {};
    var rows = [
      ["Qualified opportunities", opp.totalQualifiedOpportunities],
      ["Opportunities with verified named contacts", opp.withVerifiedNamedContacts],
      ["Unique people (verified full name)", person.uniquePeople],
      ["Contacts with verified professional titles", person.withVerifiedProfessionalTitle],
      ["Contacts with verified email", person.withVerifiedEmail],
      ["Contacts with verified telephone", person.withVerifiedTelephone],
      ["Contacts with verified personal LinkedIn", person.withVerifiedPersonalLinkedIn],
      ["Contacts with email or telephone", person.withEmailOrTelephone],
      [
        "Contacts with usable professional outreach route",
        person.withUsableProfessionalOutreachRoute,
      ],
      ["Contacts with completed Stage 4 research", person.withCompletedStage4Research],
      ["Contacts still requiring Stage 4 research", person.stillRequiringStage4Research],
      [
        "LinkedIn fields hydrated on contacts",
        defects.linkedInFieldsHydratedOnContacts,
      ],
    ];
    return (
      '<h3 class="gdi-es-subhead">Contact Coverage Scorecard</h3>' +
      '<p class="gdi-es-muted">Internal only — opportunity grain vs unique-person / channel grain. Not shown on customer Research Receipt.</p>' +
      '<div class="gdi-es-kpi-ribbon gdi-es-kpi-ribbon--contact-coverage">' +
      rows
        .map(function (row) {
          return kpiCard(row[0], row[1] == null ? "—" : row[1], "");
        })
        .join("") +
      "</div>"
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
      renderContactCoverageScorecard(ri.contactCoverageScorecard) +
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
    var snap =
      (payload.sections && payload.sections.executiveCommercialSnapshot) || {};
    var period = snap.reportingPeriod || {};
    var asOf =
      period.asOfDay ||
      (snap.lastResearchCompletionDay
        ? String(snap.lastResearchCompletionDay).slice(0, 10)
        : snap.lastResearchCompletionDate
          ? String(snap.lastResearchCompletionDate).slice(0, 10)
          : "");
    var metaParts = [];
    if (payload.hotelName) metaParts.push(esc(payload.hotelName));
    if (asOf) metaParts.push("Updated " + esc(asOf));
    if (period.customerLabel || period.label) {
      var periodLabel = String(period.customerLabel || period.label || "");
      // Avoid duplicating the same date line when label already embeds it.
      if (periodLabel && (!asOf || periodLabel.indexOf(asOf) === -1)) {
        metaParts.push(esc(periodLabel));
      }
    }
    return (
      '<div class="gdi-es-page" data-ui-version="v2" data-es-layout="premium-customer">' +
      '<header class="gdi-es-hero">' +
      '<div class="gdi-es-hero__title"><h1>Executive Summary</h1>' +
      stagedChip(payload) +
      "</div>" +
      (metaParts.length
        ? '<div class="gdi-es-hero__meta">' + metaParts.join(" · ") + "</div>"
        : "") +
      '<p class="gdi-es-hero__lede">Commercial briefing for hotel leadership and regional commercial teams.</p>' +
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
