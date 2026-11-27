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

  function infoIconSvg() {
    return (
      '<svg class="gdi-es-info__svg" width="14" height="14" viewBox="0 0 16 16" aria-hidden="true" focusable="false">' +
      '<circle cx="8" cy="8" r="7" fill="none" stroke="currentColor" stroke-width="1.25"/>' +
      '<circle cx="8" cy="4.6" r="0.9" fill="currentColor"/>' +
      '<path d="M8 7.1v4.4" stroke="currentColor" stroke-width="1.35" stroke-linecap="round"/>' +
      "</svg>"
    );
  }

  function infoTip(text, label) {
    if (!text) return "";
    var tipId =
      "gdi-es-tip-" +
      String(label || "metric")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "") +
      "-" +
      Math.random().toString(36).slice(2, 7);
    return (
      '<span class="gdi-es-info-tooltip">' +
      '<button type="button" class="gdi-es-info" aria-expanded="false" aria-controls="' +
      esc(tipId) +
      '" aria-label="About ' +
      esc(label || "this metric") +
      '">' +
      infoIconSvg() +
      "</button>" +
      '<span class="gdi-es-info-panel" id="' +
      esc(tipId) +
      '" role="tooltip" hidden>' +
      '<span class="gdi-es-info-panel__title">' +
      esc(label || "Metric") +
      "</span>" +
      '<span class="gdi-es-info-panel__body">' +
      esc(text) +
      "</span></span></span>"
    );
  }

  function assessmentIcon(kind) {
    var paths = {
      market:
        '<path d="M2 12.5L5.5 8l2.5 2.5L14 4" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><path d="M10 4h4v4" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>',
      focus:
        '<circle cx="8" cy="8" r="2.2" fill="none" stroke="currentColor" stroke-width="1.35"/><path d="M8 2.5v2.2M8 11.3v2.2M2.5 8h2.2M11.3 8h2.2" stroke="currentColor" stroke-width="1.35" stroke-linecap="round"/>',
      outlook:
        '<rect x="3" y="8" width="2.2" height="5" rx="0.4" fill="currentColor"/><rect x="6.9" y="5.5" width="2.2" height="7.5" rx="0.4" fill="currentColor"/><rect x="10.8" y="3.5" width="2.2" height="9.5" rx="0.4" fill="currentColor"/>',
      competitive:
        '<path d="M3 12.5l3-3 2.2 2.2 4.5-5.2" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><circle cx="12.2" cy="4.2" r="1.2" fill="currentColor"/>',
      changes:
        '<path d="M3.5 8a4.5 4.5 0 0 1 7.6-3.2M12.5 8a4.5 4.5 0 0 1-7.6 3.2" fill="none" stroke="currentColor" stroke-width="1.35" stroke-linecap="round"/><path d="M11.2 2.8v2.6h-2.6M4.8 13.2v-2.6h2.6" fill="none" stroke="currentColor" stroke-width="1.35" stroke-linecap="round" stroke-linejoin="round"/>',
    };
    return (
      '<span class="gdi-es-assessment__icon" aria-hidden="true">' +
      '<svg width="18" height="18" viewBox="0 0 16 16" focusable="false">' +
      (paths[kind] || paths.market) +
      "</svg></span>"
    );
  }

  function kpiCard(label, value, note, tooltip, opts) {
    opts = opts || {};
    var unavailable = value == null;
    var unit = opts.unit || "";
    var valueHtml;
    if (unavailable) {
      valueHtml = "—";
    } else if (unit) {
      valueHtml =
        '<span class="gdi-es-kpi__num">' +
        esc(fmtNum(value)) +
        '</span><span class="gdi-es-kpi__unit"> ' +
        esc(unit) +
        "</span>";
    } else {
      valueHtml = esc(fmtNum(value));
    }
    return (
      '<div class="gdi-es-kpi' +
      (unavailable ? " is-unavailable" : "") +
      (opts.prominent ? " gdi-es-kpi--prominent" : "") +
      (opts.compact ? " gdi-es-kpi--compact" : "") +
      '">' +
      '<div class="gdi-es-kpi__label-row">' +
      '<div class="gdi-es-kpi__label">' +
      esc(label) +
      "</div>" +
      infoTip(tooltip, label) +
      "</div>" +
      '<div class="gdi-es-kpi__value">' +
      valueHtml +
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
        k.omitWhenUnavailable === true ||
        k.availability === "UNAVAILABLE")
    ) {
      if (k.omitWhenUnavailable === true || k.availability === "UNAVAILABLE") {
        return "";
      }
    }
    var note = "";
    if (k.value == null) {
      note = availabilityNote(k) || k.note || "Unavailable";
    } else if (
      k.customerEstimateLabel === "Estimated" ||
      k.estimateClass === "PROVISIONAL_ESTIMATE"
    ) {
      note = k.customerEstimateLabel || "Estimated";
    }
    return kpiCard(k.label || k.key, k.value, note, k.tooltip || k.note, {
      unit: k.displayUnit || "",
      prominent: k.prominent === true,
      compact: k.compact === true,
    });
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

  function outlookMonthParts(t) {
    var months = [
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
    var key = String((t && t.month) || "");
    var m = key.match(/^(\d{4})-(\d{2})$/);
    if (m) {
      var idx = Number(m[2]) - 1;
      return {
        month: months[idx] || m[2],
        year: m[1],
      };
    }
    var label = String((t && t.monthLabel) || key || "");
    var parts = label.trim().split(/\s+/);
    if (parts.length >= 2 && /^\d{4}$/.test(parts[parts.length - 1])) {
      return {
        month: parts.slice(0, -1).join(" "),
        year: parts[parts.length - 1],
      };
    }
    return { month: label || "—", year: "" };
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
    var colCount = Math.max(timeline.length, 1);
    var compactOpts = { compact: true };
    var bucketsHtml =
      '<div class="gdi-es-outlook__buckets gdi-es-outlook__buckets--compact">' +
      kpiCard(
        "Established Event Dates",
        buckets.establishedEventDates,
        null,
        "Opportunities with evidenced calendar event dates.",
        compactOpts,
      ) +
      kpiCard(
        "Estimated Demand Periods",
        buckets.estimatedDemandPeriods,
        null,
        "Estimated windows without a confirmed event start date.",
        compactOpts,
      ) +
      kpiCard(
        "Ongoing Accounts",
        buckets.ongoingAccountRelationships,
        null,
        "Account relationships without a single event month.",
        compactOpts,
      ) +
      kpiCard(
        "Timing Not Established",
        buckets.timingNotEstablished,
        null,
        "Qualified opportunities without evidenced timing yet.",
        compactOpts,
      ) +
      "</div>";
    var chartHtml = timeline.length
      ? '<div class="gdi-es-columns gdi-es-columns--outlook" role="img" aria-label="Upcoming demand by month" style="grid-template-columns:repeat(' +
        colCount +
        ',minmax(0,1fr))">' +
        timeline
          .map(function (t) {
            var count = t.count || 0;
            var height = Math.round((count / max) * 100);
            var isZero = count === 0;
            var parts = outlookMonthParts(t);
            return (
              '<div class="gdi-es-columns__item' +
              (isZero ? " is-zero" : "") +
              '">' +
              '<div class="gdi-es-columns__value">' +
              esc(count) +
              "</div>" +
              '<div class="gdi-es-columns__track"><div class="gdi-es-columns__fill" style="height:' +
              height +
              '%"></div></div>' +
              '<div class="gdi-es-columns__label" title="' +
              esc(t.monthLabel || t.month || "") +
              '">' +
              '<span class="gdi-es-columns__month">' +
              esc(parts.month) +
              "</span>" +
              '<span class="gdi-es-columns__year">' +
              esc(parts.year) +
              "</span></div></div>"
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
        "</p>";
    return (
      '<div class="gdi-es-outlook">' +
      '<div class="gdi-es-outlook__top">' +
      '<header class="gdi-es-section__head gdi-es-outlook__head">' +
      "<h2>Commercial Demand Outlook</h2>" +
      "<p>Evidenced future demand windows only.</p>" +
      "</header>" +
      bucketsHtml +
      "</div>" +
      chartHtml +
      "</div>"
    );
  }

  function renderAssessment(a) {
    a = a || {};
    var cards =
      Array.isArray(a.primaryCards) && a.primaryCards.length
        ? a.primaryCards.concat(a.optionalCards || [])
        : [
            {
              title: "Market Opportunity",
              icon: "market",
              body: a.marketOpportunity,
            },
            {
              title: "Recommended Commercial Focus",
              icon: "focus",
              body: a.recommendedCommercialFocus || a.immediateCommercialFocus,
            },
            {
              title: "Forward Demand Outlook",
              icon: "outlook",
              body: a.forwardDemandOutlook || a.businessImpact,
            },
          ].concat(
            a.competitiveCapture || a.competitiveCapturePotential
              ? [
                  {
                    title: "Competitive Capture",
                    icon: "competitive",
                    body: a.competitiveCapture || a.competitiveCapturePotential,
                  },
                ]
              : [],
          );
    var visible = cards.filter(function (c) {
      return c && c.body;
    });
    return (
      '<div class="gdi-es-assessment gdi-es-assessment--primary" data-card-count="' +
      esc(String(visible.length)) +
      '">' +
      visible
        .map(function (c) {
          return (
            '<article class="gdi-es-assessment__item">' +
            '<div class="gdi-es-assessment__head">' +
            assessmentIcon(c.icon || "market") +
            "<h3>" +
            esc(c.title) +
            "</h3></div><p>" +
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
    // Prefer receipt-wrapped productivity (includes displayUnit + Estimated label).
    productivity =
      (receipt.researchProductivity && receipt.researchProductivity.metrics
        ? receipt.researchProductivity
        : null) ||
      productivity ||
      {};
    var funnel =
      (receipt.researchFunnel && receipt.researchFunnel.stages) ||
      [];
    var supporting =
      receipt.supportingMetrics ||
      (receipt.metrics || []).filter(function (m) {
        return (
          m &&
          m.key !== "potentialOpportunitiesIdentified" &&
          m.key !== "opportunitiesResearched" &&
          m.key !== "opportunitiesValidated" &&
          m.key !== "qualifiedOpportunitiesDelivered"
        );
      });
    var prodMetrics = (productivity.metrics || []).filter(function (m) {
      if (m.key === "estimatedResearchLaborValue" && m.value == null) return false;
      return true;
    });
    return (
      '<section class="gdi-es-section gdi-es-receipt" data-gdi-tour-target="es-receipt">' +
      '<header class="gdi-es-section__head">' +
      "<h2>Dealality Research Receipt</h2>" +
      "<p>" +
      esc(
        productivity.lede ||
          "Your time. Better invested — less time searching for demand, more time converting opportunities.",
      ) +
      "</p></header>" +
      (prodMetrics.length
        ? '<div class="gdi-es-kpi-ribbon gdi-es-receipt__productivity">' +
          prodMetrics
            .map(function (m) {
              var note =
                m.value == null
                  ? availabilityNote(m) || "Unavailable"
                  : m.customerEstimateLabel ||
                    (m.estimateClass === "PROVISIONAL_ESTIMATE"
                      ? "Estimated"
                      : "");
              return kpiCard(
                m.label || m.key,
                m.value,
                note,
                m.tooltip || m.note,
                {
                  unit: m.displayUnit || "",
                  prominent: true,
                },
              );
            })
            .join("") +
          "</div>"
        : "") +
      (funnel.length
        ? '<div class="gdi-es-receipt__funnel-block">' +
          '<h3 class="gdi-es-subhead">Research Funnel</h3>' +
          '<p class="gdi-es-muted">' +
          esc(
            (receipt.researchFunnel && receipt.researchFunnel.lede) ||
              "Research volume leading to commercially qualified opportunities.",
          ) +
          "</p>" +
          '<div class="gdi-es-funnel gdi-es-funnel--research" data-stage-count="' +
          esc(String(funnel.length)) +
          '">' +
          funnel
            .map(function (m, idx) {
              return (
                '<div class="gdi-es-funnel__stage">' +
                (idx > 0
                  ? '<span class="gdi-es-funnel__connector" aria-hidden="true"></span>'
                  : "") +
                '<div class="gdi-es-kpi__label-row"><div class="gdi-es-kpi__label">' +
                esc(m.label || m.key) +
                "</div>" +
                infoTip(m.tooltip || m.note, m.label || m.key) +
                '</div><div class="gdi-es-kpi__value">' +
                esc(m.value == null ? "—" : fmtNum(m.value)) +
                "</div></div>"
              );
            })
            .join("") +
          "</div></div>"
        : "") +
      (supporting.length
        ? '<div class="gdi-es-receipt__supporting">' +
          '<h3 class="gdi-es-subhead">Supporting Research Metrics</h3>' +
          '<div class="gdi-es-kpi-ribbon gdi-es-kpi-ribbon--compact">' +
          supporting
            .map(function (m) {
              return kpiFromObj(
                Object.assign({}, m, {
                  compact: true,
                  omitWhenUnavailable: m.omitWhenUnavailable !== false,
                }),
              );
            })
            .join("") +
          "</div></div>"
        : "") +
      renderResearchHoursMethodology(productivity) +
      "</section>"
    );
  }

  /**
   * Expandable customer methodology — activity ranges come from the live
   * benchmark contract via methodologyExplanation (never hardcoded minutes).
   */
  function renderResearchHoursMethodology(productivity) {
    var expl =
      (productivity && productivity.methodologyExplanation) || null;
    var title = "How Research Hours Are Estimated";
    if (!expl || !Array.isArray(expl.activities) || !expl.activities.length) {
      return (
        '<details class="gdi-es-methodology">' +
        "<summary>" +
        esc(title) +
        "</summary>" +
        "<p>" +
        esc(
          (productivity && productivity.methodologySummary) ||
            "Dealality estimates the time your sales team would otherwise spend manually identifying, researching and qualifying business opportunities.",
        ) +
        "</p>" +
        "<p>" +
        esc(
          (productivity && productivity.disclaimer) ||
            "These are estimated productivity benefits, not actual payroll savings or revenue generated.",
        ) +
        "</p></details>"
      );
    }
    var list =
      '<ul class="gdi-es-methodology__list">' +
      expl.activities
        .map(function (a) {
          return (
            "<li><strong>" +
            esc(a.label) +
            ":</strong> " +
            esc(a.rangeLabel) +
            " " +
            esc(a.perUnitPhrase || "per opportunity") +
            ".</li>"
          );
        })
        .join("") +
      "</ul>";
    return (
      '<details class="gdi-es-methodology">' +
      "<summary>" +
      esc(expl.summaryTitle || title) +
      "</summary>" +
      "<p>" +
      esc(expl.intro || "") +
      "</p>" +
      "<p>" +
      esc(expl.activitiesIntro || "") +
      "</p>" +
      list +
      "<p>" +
      esc(expl.calculationNote || "") +
      "</p>" +
      "<p>" +
      esc(expl.workingDayNote || "") +
      "</p>" +
      "<p>" +
      esc(expl.disclaimer || (productivity && productivity.disclaimer) || "") +
      "</p></details>"
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
          "Estimated research productivity. Ordinary commercial pursuit is excluded.",
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
      '<section class="gdi-es-section" data-gdi-tour-target="es-snapshot">' +
      '<header class="gdi-es-section__head"><h2>Commercial Snapshot</h2>' +
      "<p>Key commercial metrics for this hotel’s qualified demand.</p></header>" +
      '<div class="gdi-es-kpi-ribbon">' +
      (ov.commercialKpiRibbon || [])
        .map(function (k) {
          return kpiFromObj(k);
        })
        .join("") +
      "</div></section>" +
      '<section class="gdi-es-section" data-gdi-tour-target="es-assessment"><header class="gdi-es-section__head"><h2>Executive Commercial Assessment</h2></header>' +
      renderAssessment(ov.executiveAssessment) +
      "</section>" +
      '<div class="gdi-es-split">' +
      '<section class="gdi-es-section" data-gdi-tour-target="es-composition"><header class="gdi-es-section__head"><h2>Demand Composition</h2><p>Qualified opportunities by demand segment.</p></header>' +
      renderBarChart(ov.demandComposition) +
      "</section>" +
      '<section class="gdi-es-section gdi-es-outlook-section" data-gdi-tour-target="es-outlook">' +
      renderOutlook(ov.commercialDemandOutlook) +
      "</section></div>" +
      renderConsolidatedReceipt(ov.customerResearchReceipt, productivity) +
      '<section class="gdi-es-section" data-gdi-tour-target="es-priority-opps"><header class="gdi-es-section__head"><h2>Priority Commercial Opportunities</h2></header>' +
      renderPrioritySharedTiles(priorities, hotelId) +
      "</section>" +
      '<section class="gdi-es-section" data-gdi-tour-target="es-outcomes"><header class="gdi-es-section__head"><h2>Commercial Outcomes</h2>' +
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
    var asOfLabel =
      period.asOfLabel ||
      period.customerLabel ||
      period.label ||
      "";
    var asOfDay =
      period.asOfDay ||
      (snap.lastResearchCompletionDay
        ? String(snap.lastResearchCompletionDay).slice(0, 10)
        : snap.lastResearchCompletionDate
          ? String(snap.lastResearchCompletionDate).slice(0, 10)
          : "");
    var metaParts = [];
    if (payload.hotelName) metaParts.push(esc(payload.hotelName));
    if (asOfLabel && /Research updated/i.test(asOfLabel)) {
      metaParts.push(esc(asOfLabel));
    } else if (asOfLabel) {
      metaParts.push("Updated " + esc(asOfLabel));
    } else if (asOfDay) {
      metaParts.push("Updated " + esc(asOfDay));
    }
    return (
      '<div class="gdi-es-page" data-ui-version="v2" data-es-layout="premium-customer">' +
      '<header class="gdi-es-hero">' +
      '<div class="gdi-es-hero__title"><h1>Executive Summary</h1></div>' +
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

  function bindInfoTooltips(root) {
    if (!root) return;
    function closeAllInfoTips(exceptBtn) {
      root.querySelectorAll(".gdi-es-info-tooltip.is-open").forEach(function (wrap) {
        if (exceptBtn && wrap.contains(exceptBtn)) return;
        wrap.classList.remove("is-open");
        var btn = wrap.querySelector(".gdi-es-info");
        var panel = wrap.querySelector(".gdi-es-info-panel");
        if (btn) btn.setAttribute("aria-expanded", "false");
        if (panel) {
          panel.hidden = true;
          panel.classList.remove(
            "gdi-es-info-panel--fixed",
            "gdi-es-info-panel--flip",
            "gdi-es-info-panel--align-end",
          );
          panel.style.top = "";
          panel.style.left = "";
        }
      });
    }

    function positionInfoPanel(btn, panel) {
      panel.classList.add("gdi-es-info-panel--fixed");
      panel.hidden = false;
      var rect = btn.getBoundingClientRect();
      var width = panel.offsetWidth || 260;
      var height = panel.offsetHeight || 120;
      var left = rect.right - width;
      if (left < 12) left = 12;
      if (left + width > window.innerWidth - 12) {
        left = Math.max(12, window.innerWidth - width - 12);
        panel.classList.add("gdi-es-info-panel--align-end");
      }
      var top = rect.bottom + 8;
      if (top + height > window.innerHeight - 12) {
        top = Math.max(12, rect.top - height - 8);
        panel.classList.add("gdi-es-info-panel--flip");
      }
      panel.style.left = Math.round(left) + "px";
      panel.style.top = Math.round(top) + "px";
    }

    function openInfoTip(btn) {
      var wrap = btn.closest(".gdi-es-info-tooltip");
      if (!wrap) return;
      closeAllInfoTips(btn);
      var panel = wrap.querySelector(".gdi-es-info-panel");
      if (!panel) return;
      wrap.classList.add("is-open");
      btn.setAttribute("aria-expanded", "true");
      positionInfoPanel(btn, panel);
    }

    root.querySelectorAll(".gdi-es-info-tooltip .gdi-es-info").forEach(function (btn) {
      if (btn.getAttribute("data-bound") === "1") return;
      btn.setAttribute("data-bound", "1");
      btn.addEventListener("click", function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        var wrap = btn.closest(".gdi-es-info-tooltip");
        if (wrap && wrap.classList.contains("is-open")) {
          closeAllInfoTips();
        } else {
          openInfoTip(btn);
        }
      });
      btn.addEventListener("mouseenter", function () {
        openInfoTip(btn);
      });
      btn.addEventListener("focus", function () {
        openInfoTip(btn);
      });
      btn.addEventListener("keydown", function (ev) {
        if (ev.key === "Escape") {
          closeAllInfoTips();
          btn.blur();
        }
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          openInfoTip(btn);
        }
      });
    });
    if (root.getAttribute("data-es-tip-doc-bound") === "1") return;
    root.setAttribute("data-es-tip-doc-bound", "1");
    document.addEventListener("click", function (ev) {
      if (!ev.target.closest || !ev.target.closest(".gdi-es-info-tooltip")) {
        closeAllInfoTips();
      }
    });
    document.addEventListener("keydown", function (ev) {
      if (ev.key === "Escape") closeAllInfoTips();
    });
  }

  function createTabController(opts) {
    opts = opts || {};
    var root = opts.root;
    if (!root) throw new Error("executive_summary_root_required");
    var hotelId = String(opts.hotelId || "").trim();
    var active = true;
    var lastPayload = null;

    function bindInteractions() {
      bindInfoTooltips(root);
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

    function executiveSummaryLoadingHtml() {
      var label = "Loading Executive Summary\u2026";
      var wave = global.DealalityWaveLoader;
      if (wave && typeof wave.toastHtml === "function") {
        return (
          '<div class="gdi-es-loading-wave" data-gdi-es-wave-loading="1">' +
          wave.toastHtml(label) +
          "</div>"
        );
      }
      if (wave && typeof wave.compactHtml === "function") {
        return (
          '<div class="gdi-es-loading-wave" data-gdi-es-wave-loading="1">' +
          wave.compactHtml(label) +
          "</div>"
        );
      }
      if (wave && typeof wave.html === "function") {
        return (
          '<div class="gdi-es-loading-wave" data-gdi-es-wave-loading="1">' +
          wave.html(label, { compact: true, fixed: false, progress: false }) +
          "</div>"
        );
      }
      // Script should be present on GDI shell; keep a11y status without inventing a new spinner.
      return (
        '<div class="gdi-es-loading-wave" data-gdi-es-wave-loading="1" role="status" aria-live="polite" aria-busy="true">' +
        esc(label) +
        "</div>"
      );
    }

    async function load() {
      if (!active) return;
      root.innerHTML = executiveSummaryLoadingHtml();
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
    bindInfoTooltips: bindInfoTooltips,
    _active: false,
  };
})(typeof window !== "undefined" ? window : globalThis);
