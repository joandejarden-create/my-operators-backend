/**
 * BAI customer longitudinal surfaces — Prior Run, Trends, abs/rel, disclosures.
 * Consumes customerLongitudinal from executive-summary (post-promotion) or
 * /api/ai-visibility/brand/customer-promotion-preview (internal preview only).
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

  function chip(kind, label) {
    return (
      '<span class="bai-w4-chip bai-w4-chip-kind--' +
      esc(kind || "") +
      '">' +
      esc(label || "") +
      "</span>"
    );
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
        layout: { autoPadding: true, padding: { top: 12, right: 12, bottom: 8, left: 8 } },
        plugins: { legend: { display: false } },
        scales: {
          y: {
            beginAtZero: true,
            suggestedMax: 100,
            ticks: {
              callback: function (v) {
                return v + "%";
              },
            },
          },
        },
      },
    });
  }

  function renderProvider(pv) {
    var note = $("aivCustLongProviderNote");
    var thead = $("aivCustLongProviderHead");
    var body = $("aivCustLongProviderBody");
    if (note) {
      note.textContent =
        (pv.provider && pv.provider.customerDisclosure) ||
        (pv.disclosures && pv.disclosures.provider) ||
        "Change vs Prior Run is not comparable for this monitoring pair.";
    }
    if (thead) {
      thead.innerHTML =
        "<tr><th>Provider</th><th class=\"num\">Current</th></tr>";
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
          "</td><td>" +
          chip("abs", b.absoluteLabel) +
          " / " +
          chip("rel", b.relativeLabel) +
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
    var banner = $("aivCustLongPreviewBanner");
    if (banner) {
      banner.hidden = !opts.previewMode;
    }
    var title = $("aivCustLongTitle");
    if (title) {
      title.textContent =
        (pv.parentCompanyName || "Portfolio") + " · Prior Run & Trends";
    }
    var dates = $("aivCustLongDates");
    if (dates) {
      dates.textContent =
        "Current " +
        (cl.currentDate || pv.currentDate || "") +
        " · Prior Run " +
        (cl.priorDate || pv.priorDate || "");
    }
    var exec = $("aivCustLongExec");
    if (exec) {
      exec.textContent =
        (pv.executiveRead && pv.executiveRead.narrative) || "";
    }
    var kpis = $("aivCustLongKpis");
    if (kpis && pv.portfolio) {
      kpis.innerHTML =
        '<div class="bai-w4-kpi-grid" data-bai-w4-layout="kpi">' +
        '<article class="bai-w4-kpi bai-w4-kpi--primary"><div class="bai-w4-kpi__label">Current</div><div class="bai-w4-kpi__value">' +
        esc(fmtPct(pv.portfolio.currentPresence)) +
        '</div></article>' +
        '<article class="bai-w4-kpi"><div class="bai-w4-kpi__label">Prior Run</div><div class="bai-w4-kpi__value bai-w4-kpi__value--secondary">' +
        esc(fmtPct(pv.portfolio.priorPresence)) +
        '</div></article>' +
        '<article class="bai-w4-kpi"><div class="bai-w4-kpi__label">Change</div><div class="bai-w4-kpi__value bai-w4-kpi__value--tertiary">' +
        esc(pv.portfolio.deltaDisplay || "") +
        "</div><div class=\"bai-w4-kpi__meta\">" +
        chip("abs", "Abs " + (pv.portfolio.absoluteLabel || "")) +
        " " +
        chip("rel", "Rel " + (pv.portfolio.relativeLabel || "")) +
        "</div></article></div>";
    }
    var cohortNote = $("aivCustLongCohortNote");
    if (cohortNote) {
      cohortNote.textContent =
        (pv.disclosures && pv.disclosures.cohortChange) ||
        (pv.competitive &&
          pv.competitive.story &&
          pv.competitive.story.cohortChangeDisclosure) ||
        "";
      cohortNote.hidden = !cohortNote.textContent;
    }
    var intentNote = $("aivCustLongIntentNote");
    if (intentNote) {
      intentNote.textContent =
        (pv.ownerIntent && pv.ownerIntent.presentation) ||
        (pv.disclosures && pv.disclosures.intent) ||
        "";
    }
    var competitive = $("aivCustLongCompetitive");
    if (competitive) {
      competitive.textContent =
        (pv.competitive && pv.competitive.story && pv.competitive.story.narrative) ||
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
