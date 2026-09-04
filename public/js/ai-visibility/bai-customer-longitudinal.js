/**
 * BAI customer longitudinal surfaces — Executive Read, Prior Run KPIs, Trends.
 * Consumes customerLongitudinal from executive-summary (post-promotion) or
 * /api/ai-visibility/brand/customer-promotion-preview (internal preview only).
 *
 * Visual grammar: ADP-family Executive Read + Wave 1–4 customer KPI/card tokens.
 * Measurement / period identity is payload-owned — this module only presents.
 */
(function (global) {
  var charts = {};

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function $(id) {
    return document.getElementById(id);
  }

  function fmtPct(n) {
    return n == null || !isFinite(Number(n)) ? "n/a" : Number(n).toFixed(1) + "%";
  }

  function destroyCharts() {
    Object.keys(charts).forEach(function (k) {
      try {
        charts[k].destroy();
      } catch (e) {}
      delete charts[k];
    });
  }

  function pickParent(payload, parentKey) {
    var parents = (payload && payload.parents) || [];
    if (!parents.length) return null;
    if (!parentKey || parentKey === "all") return parents[0];
    return (
      parents.find(function (p) {
        return p.parentCompanyKey === parentKey;
      }) || parents[0]
    );
  }

  function chipKindFromLabel(label) {
    var l = String(label || "").toLowerCase();
    if (l.indexOf("improv") >= 0) return "improved";
    if (l.indexOf("declin") >= 0) return "declined";
    if (l.indexOf("weaken") >= 0) return "weakened";
    if (l.indexOf("stable") >= 0) return "stable";
    return "neutral";
  }

  function statusChip(label) {
    var kind = chipKindFromLabel(label);
    return (
      '<span class="bai-w4-chip bai-w4-chip--' +
      esc(kind) +
      '">' +
      esc(label || "—") +
      "</span>"
    );
  }

  function formatShortDate(iso) {
    var s = String(iso || "").slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return s || "—";
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
    var m = Number(s.slice(5, 7)) - 1;
    var d = Number(s.slice(8, 10));
    return months[m] + " " + d;
  }

  function formatDateRange(priorIso, currentIso) {
    return formatShortDate(priorIso) + " → " + formatShortDate(currentIso);
  }

  function setDisclosureItem(itemId, bodyId, text) {
    var item = $(itemId);
    var body = $(bodyId);
    var t = String(text || "").trim();
    if (body) body.textContent = t;
    if (item) item.hidden = !t;
    return !!t;
  }

  function syncDisclosureStrip() {
    var strip = $("aivCustLongDisclosures");
    if (!strip) return;
    var cohort = $("aivCustLongCohortNote");
    var intent = $("aivCustLongIntentNote");
    strip.hidden = !(
      (cohort && !cohort.hidden) ||
      (intent && !intent.hidden)
    );
  }

  function buildWhatChangedItems(pv) {
    var items = [];
    var port = pv.portfolio || {};
    var provider = pv.provider || {};

    if (port.strongestPositiveMover) {
      items.push({
        label: "Strongest gain",
        value:
          port.strongestPositiveMover.brandName +
          " " +
          (port.strongestPositiveMover.deltaDisplay || ""),
      });
    } else if (port.noBrandsImproved) {
      items.push({
        label: "Strongest gain",
        value: port.mostStableBrand
          ? "None — most stable: " +
            port.mostStableBrand.brandName +
            " (" +
            (port.mostStableBrand.deltaDisplay || "—") +
            ")"
          : "None — all brands declined or held",
      });
    }

    if (port.largestVisibilityLoss) {
      items.push({
        label: "Largest decline",
        value:
          port.largestVisibilityLoss.brandName +
          " " +
          (port.largestVisibilityLoss.deltaDisplay || ""),
      });
    }

    var strongest = provider.strongestProvider;
    if (!strongest && provider.rows && provider.rows.length) {
      strongest = provider.rows.reduce(function (best, row) {
        if (!best) return row;
        return Number(row.currentPresence) > Number(best.currentPresence)
          ? row
          : best;
      }, null);
    }
    if (strongest) {
      items.push({
        label: "Strongest current provider",
        value:
          (strongest.providerLabel || strongest.provider || "Provider") +
          " " +
          fmtPct(strongest.currentPresence),
      });
    }

    return items;
  }

  function renderExecutiveRead(pv, cl) {
    var position = $("aivCustLongErPosition");
    var changed = $("aivCustLongErChanged");
    if (!position || !changed) return;

    var port = pv.portfolio || {};
    var priorIso = cl.priorDate || pv.priorDate || "";
    var currentIso = cl.currentDate || pv.currentDate || "";
    var monitoring = formatDateRange(priorIso, currentIso);

    position.innerHTML =
      '<div class="bai-er-position" data-bai-er="position">' +
      '<p class="aiv-er-summary-box__label">Current Portfolio Position</p>' +
      '<p class="bai-er-position__value" data-bai-er-value="current">' +
      esc(fmtPct(port.currentPresence)) +
      "</p>" +
      '<p class="bai-er-position__delta" data-bai-er-value="delta">' +
      esc(port.deltaDisplay || "—") +
      " <span class=\"bai-er-position__delta-suffix\">vs Prior Run</span></p>" +
      '<div class="bai-er-absrel" data-bai-er="abs-rel">' +
      '<div class="bai-er-absrel__cell">' +
      '<span class="bai-er-absrel__label">Absolute</span>' +
      statusChip(port.absoluteLabel) +
      "</div>" +
      '<div class="bai-er-absrel__cell">' +
      '<span class="bai-er-absrel__label">Relative</span>' +
      statusChip(port.relativeLabel) +
      "</div>" +
      "</div>" +
      '<dl class="bai-er-position__meta">' +
      "<div><dt>Prior Run</dt><dd>" +
      esc(fmtPct(port.priorPresence)) +
      "</dd></div>" +
      "<div><dt>Monitoring</dt><dd>" +
      esc(monitoring) +
      "</dd></div>" +
      "</dl>" +
      "</div>";

    var items = buildWhatChangedItems(pv);
    var listHtml = items
      .map(function (it) {
        return (
          '<li class="bai-er-changed__item">' +
          '<span class="bai-er-changed__label">' +
          esc(it.label) +
          "</span>" +
          '<span class="bai-er-changed__value">' +
          esc(it.value) +
          "</span>" +
          "</li>"
        );
      })
      .join("");

    changed.innerHTML =
      '<p class="aiv-executive-read__main-title">What Changed</p>' +
      (listHtml
        ? '<ul class="bai-er-changed__list">' + listHtml + "</ul>"
        : '<p class="aiv-executive-read__narrative">Portfolio movement is available in Brand Movement below.</p>') +
      '<p class="bai-er-changed__next">Next to inspect: <strong>Brand Movement</strong> → <strong>Competitive Movement</strong></p>';
  }

  /**
   * Duplicate Current/Prior/Change/Dates KPI row removed.
   * Executive Read remains the single primary summary surface.
   * Gate: BAI_LONGITUDINAL_NO_DUPLICATE_SUMMARY_KPIS
   */
  function clearDuplicateSummaryKpis() {
    var kpis = $("aivCustLongKpis");
    if (!kpis) return;
    kpis.innerHTML = "";
    kpis.hidden = true;
    kpis.setAttribute("data-bai-kpi-row", "removed");
    kpis.setAttribute("aria-hidden", "true");
  }

  function renderTrend(trend) {
    var wrap = $("aivCustLongTrendWrap");
    var empty = $("aivCustLongTrendEmpty");
    var canvas = $("aivCustLongTrendChart");
    if (!canvas || !trend) return;
    if (!trend.points || trend.points.length < 2) {
      if (empty) {
        empty.hidden = false;
        empty.textContent = "Comparable trend requires two monitoring runs.";
      }
      if (wrap) wrap.hidden = true;
      return;
    }
    if (empty) empty.hidden = true;
    if (wrap) wrap.hidden = false;
    if (charts.trend) {
      try {
        charts.trend.destroy();
      } catch (e) {}
    }
    if (!global.Chart) return;
    charts.trend = new global.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: trend.points.map(function (p) {
          return p.label;
        }),
        datasets: [
          {
            label: "AI Presence",
            data: trend.points.map(function (p) {
              return p.value;
            }),
            borderColor: "#5b8def",
            backgroundColor: "rgba(91,141,239,0.15)",
            tension: 0.2,
            pointRadius: 4,
            pointHitRadius: 8,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: { autoPadding: true, padding: { top: 8, right: 10, bottom: 4, left: 4 } },
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: { color: "#9aa6c7", font: { size: 11 } },
            grid: { display: false },
          },
          y: {
            beginAtZero: true,
            suggestedMax: 100,
            ticks: {
              color: "#9aa6c7",
              font: { size: 11 },
              callback: function (v) {
                return v + "%";
              },
            },
            grid: { color: "rgba(55,68,107,0.35)" },
          },
        },
      },
    });
  }

  function renderProvider(pv) {
    var note = $("aivCustLongProviderNote");
    var thead = $("aivCustLongProviderHead");
    var body = $("aivCustLongProviderBody");
    var disclosure =
      (pv.provider && pv.provider.customerDisclosure) ||
      (pv.disclosures && pv.disclosures.provider) ||
      "Change vs Prior Run is not comparable for this monitoring pair.";
    setDisclosureItem(
      "aivCustLongProviderDisclosure",
      "aivCustLongProviderNote",
      disclosure
    );
    if (note && !note.closest(".bai-long-disclosure")) {
      note.textContent = disclosure;
    }
    if (thead) {
      thead.innerHTML =
        '<tr><th scope="col">Provider</th><th class="num" scope="col">Current</th></tr>';
    }
    if (!body) return;
    body.innerHTML = ((pv.provider && pv.provider.rows) || [])
      .map(function (r) {
        return (
          "<tr><td>" +
          esc(r.providerLabel) +
          '</td><td class="num bai-w4-primary-num">' +
          esc(fmtPct(r.currentPresence)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderBrandTable(pv) {
    var body = $("aivCustLongBrandBody");
    if (!body) return;
    body.innerHTML = ((pv.brandMovement && pv.brandMovement.rows) || [])
      .map(function (b) {
        return (
          "<tr><td>" +
          esc(b.brandName) +
          '</td><td class="num bai-w4-primary-num">' +
          esc(fmtPct(b.currentPresence)) +
          '</td><td class="num bai-w4-secondary-num">' +
          esc(fmtPct(b.priorPresence)) +
          '</td><td class="num">' +
          esc(b.deltaDisplay) +
          "</td><td>" +
          esc(b.rankDisplay || "") +
          '</td><td class="bai-cust-absrel-cell">' +
          '<span class="bai-cust-absrel-pair">' +
          '<span class="bai-er-absrel__label">Abs</span> ' +
          statusChip(b.absoluteLabel) +
          "</span> " +
          '<span class="bai-cust-absrel-pair">' +
          '<span class="bai-er-absrel__label">Rel</span> ' +
          statusChip(b.relativeLabel) +
          "</span>" +
          "</td></tr>"
        );
      })
      .join("");
  }

  function render(payload, opts) {
    opts = opts || {};
    var root = $("aivCustomerLongitudinal");
    if (!root) return;
    var cl = payload && (payload.customerLongitudinal || payload);
    if (!cl || !cl.available) {
      root.hidden = true;
      destroyCharts();
      return;
    }
    var pv = pickParent(cl, opts.parentKey);
    if (!pv) {
      root.hidden = true;
      return;
    }
    root.hidden = false;
    root.setAttribute("data-bai-customer-visual", "cleanup-v2");

    var banner = $("aivCustLongPreviewBanner");
    if (banner) {
      banner.hidden = !opts.previewMode;
    }

    var title = $("aivCustLongTitle");
    if (title) {
      title.textContent =
        (pv.parentCompanyName || "Portfolio") + " · Prior Run & Trends";
    }

    // Dates live in Executive Read Monitoring; keep help line as SR-only meta.
    var datesHelp = $("aivCustLongDates");
    if (datesHelp) {
      datesHelp.textContent =
        "Monitoring pair: " +
        formatDateRange(
          cl.priorDate || pv.priorDate,
          cl.currentDate || pv.currentDate
        );
      datesHelp.classList.add("bai-cust-dates-help");
    }

    renderExecutiveRead(pv, cl);
    clearDuplicateSummaryKpis();

    setDisclosureItem(
      "aivCustLongCohortNote",
      "aivCustLongCohortNoteBody",
      (pv.disclosures && pv.disclosures.cohortChange) ||
        (pv.competitive &&
          pv.competitive.story &&
          pv.competitive.story.cohortChangeDisclosure) ||
        ""
    );
    setDisclosureItem(
      "aivCustLongIntentNote",
      "aivCustLongIntentNoteBody",
      (pv.ownerIntent && pv.ownerIntent.presentation) ||
        (pv.disclosures && pv.disclosures.intent) ||
        ""
    );
    syncDisclosureStrip();

    var competitive = $("aivCustLongCompetitive");
    if (competitive) {
      competitive.textContent =
        (pv.competitive &&
          pv.competitive.story &&
          pv.competitive.story.narrative) ||
        "";
    }

    renderTrend(pv.trend);
    renderProvider(pv);
    renderBrandTable(pv);
  }

  function isPromotionPreviewUrl() {
    try {
      var q = new URLSearchParams(global.location.search || "");
      if (q.get("share")) return false;
      return q.get("baiPromotionPreview") === "1";
    } catch (e) {
      return false;
    }
  }

  function previewParentKey() {
    try {
      return (
        new URLSearchParams(global.location.search || "").get("parent") ||
        "marriott"
      );
    } catch (e) {
      return "marriott";
    }
  }

  global.BaiCustomerLongitudinal = {
    render: render,
    isPromotionPreviewUrl: isPromotionPreviewUrl,
    previewParentKey: previewParentKey,
    destroyCharts: destroyCharts,
  };
})(window);
