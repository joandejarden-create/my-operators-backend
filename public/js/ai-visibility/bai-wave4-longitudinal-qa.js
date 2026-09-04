/**
 * BAI Wave 4 — Internal Longitudinal QA UI (candidate Period 2 only).
 * Consumes /api/ai-visibility/brand/internal-longitudinal-qa wave4 payload.
 */
(function () {
  var charts = {};
  var state = { data: null, parentKey: "marriott", sort: "CURRENT_POSITION" };

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
    return n == null || !isFinite(Number(n)) ? "—" : Number(n).toFixed(1) + "%";
  }

  function destroyCharts() {
    Object.keys(charts).forEach(function (k) {
      try {
        charts[k].destroy();
      } catch (e) {}
      delete charts[k];
    });
  }

  function parentView() {
    var parents = (state.data && state.data.wave4 && state.data.wave4.parents) || [];
    return (
      parents.find(function (p) {
        return p.parentCompanyKey === state.parentKey;
      }) || parents[0]
    );
  }

  function statusChip(kind, label) {
    var tone = String(label || "")
      .toLowerCase()
      .replace(/\s+/g, "");
    var cls =
      "bai-w4-chip bai-w4-chip--" +
      (tone || "na") +
      (kind ? " bai-w4-chip-kind--" + kind : "");
    return '<span class="' + cls + '">' + esc(label || "—") + "</span>";
  }

  function renderHierarchyCard(portfolio) {
    return (
      '<div class="bai-w4-kpi-grid" data-bai-w4-layout="kpi">' +
      '<article class="bai-w4-kpi bai-w4-kpi--primary" data-bai-priority="current">' +
      '<div class="bai-w4-kpi__label">Current position</div>' +
      '<div class="bai-w4-kpi__value">' +
      esc(fmtPct(portfolio.currentPresence)) +
      "</div>" +
      '<div class="bai-w4-kpi__meta">AI Presence · portfolio</div></article>' +
      '<article class="bai-w4-kpi" data-bai-priority="prior">' +
      '<div class="bai-w4-kpi__label">Prior comparable</div>' +
      '<div class="bai-w4-kpi__value bai-w4-kpi__value--secondary">' +
      esc(fmtPct(portfolio.priorPresence)) +
      "</div>" +
      '<div class="bai-w4-kpi__meta">Secondary</div></article>' +
      '<article class="bai-w4-kpi" data-bai-priority="delta">' +
      '<div class="bai-w4-kpi__label">Change vs prior</div>' +
      '<div class="bai-w4-kpi__value bai-w4-kpi__value--tertiary">' +
      esc(portfolio.deltaDisplay || "—") +
      "</div>" +
      '<div class="bai-w4-kpi__meta">' +
      statusChip("abs", "Abs " + (portfolio.absoluteLabel || "—")) +
      " " +
      statusChip("rel", "Rel " + (portfolio.relativeLabel || "—")) +
      "</div></article>" +
      '<article class="bai-w4-kpi">' +
      '<div class="bai-w4-kpi__label">Brand mix</div>' +
      '<div class="bai-w4-kpi__value bai-w4-kpi__value--secondary">' +
      esc(
        (portfolio.brandsImproving || 0) +
          "↑ · " +
          (portfolio.brandsDeclining || 0) +
          "↓ · " +
          (portfolio.brandsStable || 0) +
          "→"
      ) +
      "</div>" +
      '<div class="bai-w4-kpi__meta">Improving / declining / stable</div></article>' +
      "</div>"
    );
  }

  function renderTrend(trend) {
    var wrap = $("baiW4TrendChartWrap");
    var empty = $("baiW4TrendEmpty");
    var canvas = $("baiW4TrendChart");
    if (!trend || !trend.points || trend.points.length === 0) {
      if (wrap) wrap.hidden = true;
      if (empty) {
        empty.hidden = false;
        empty.textContent = "No comparable trend points.";
      }
      return;
    }
    if (trend.chartMode === "SINGLE_POINT") {
      if (wrap) wrap.hidden = true;
      if (empty) {
        empty.hidden = false;
        empty.textContent =
          "One comparable period only — single point shown in KPIs (no fake line).";
      }
      return;
    }
    if (empty) empty.hidden = true;
    if (wrap) wrap.hidden = false;
    if (!canvas || typeof window.Chart !== "function") return;

    if (charts.trend) {
      try {
        charts.trend.destroy();
      } catch (e) {}
    }
    var labels = trend.points.map(function (p) {
      return p.label || String(p.date || "").slice(0, 10);
    });
    var values = trend.points.map(function (p) {
      return p.value;
    });
    charts.trend = new window.Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Portfolio AI Presence",
            data: values,
            borderColor: "#7dd3fc",
            backgroundColor: "rgba(125, 211, 252, 0.12)",
            pointBackgroundColor: "#e0f2fe",
            pointRadius: 4,
            pointHoverRadius: 5,
            tension: 0.2,
            fill: true,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: {
          autoPadding: true,
          padding: { top: 12, right: 12, bottom: 8, left: 8 },
        },
        plugins: {
          legend: {
            display: true,
            position: "bottom",
            labels: { color: "#c7d0e8", boxWidth: 10, font: { size: 11 } },
          },
          tooltip: {
            callbacks: {
              label: function (ctx) {
                var v = ctx.parsed.y;
                return v == null ? "—" : Number(v).toFixed(1) + "%";
              },
            },
          },
        },
        scales: {
          x: {
            ticks: { color: "#9aa6c7", maxRotation: 0, autoSkip: true },
            grid: { color: "rgba(55,68,107,0.35)" },
          },
          y: {
            beginAtZero: true,
            suggestedMax: 100,
            ticks: {
              color: "#9aa6c7",
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

  function renderProvider(provider) {
    var note = $("baiW4ProviderNote");
    var body = $("baiW4ProviderBody");
    var wrap = $("baiW4ProviderChartWrap");
    var canvas = $("baiW4ProviderChart");
    if (note) note.textContent = provider.note || "";
    if (!provider.rows || !provider.rows.length) {
      if (body) body.innerHTML = '<tr><td colspan="4">No provider rows.</td></tr>';
      return;
    }
    if (body) {
      body.innerHTML = provider.rows
        .map(function (r) {
          return (
            "<tr><td>" +
            esc(r.providerLabel) +
            '</td><td class="num bai-w4-primary-num">' +
            esc(fmtPct(r.currentPresence)) +
            '</td><td class="num bai-w4-secondary-num">' +
            esc(fmtPct(r.priorPresence)) +
            '</td><td class="num">' +
            esc(r.deltaDisplay) +
            " <span class='bai-w4-muted'>(" +
            esc(r.comparabilityState) +
            ")</span></td></tr>"
          );
        })
        .join("");
    }
    if (!canvas || typeof window.Chart !== "function") return;
    if (wrap) wrap.hidden = false;
    if (charts.provider) {
      try {
        charts.provider.destroy();
      } catch (e) {}
    }
    charts.provider = new window.Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels: provider.rows.map(function (r) {
          return r.providerLabel;
        }),
        datasets: [
          {
            label: "Current",
            data: provider.rows.map(function (r) {
              return r.currentPresence;
            }),
            backgroundColor: "rgba(125, 211, 252, 0.75)",
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: {
          autoPadding: true,
          padding: { top: 12, right: 8, bottom: 4, left: 8 },
        },
        plugins: {
          legend: {
            display: true,
            position: "bottom",
            labels: { color: "#c7d0e8", boxWidth: 10, font: { size: 11 } },
          },
        },
        scales: {
          x: {
            ticks: { color: "#9aa6c7" },
            grid: { display: false },
          },
          y: {
            beginAtZero: true,
            suggestedMax: 100,
            ticks: {
              color: "#9aa6c7",
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

  function renderBrandTable(pv) {
    var rows =
      (pv.brandMovement.bySort && pv.brandMovement.bySort[state.sort]) ||
      pv.brandMovement.rows ||
      [];
    var body = $("baiW4BrandBody");
    if (!body) return;
    body.innerHTML = rows
      .map(function (b) {
        return (
          "<tr>" +
          "<td>" +
          esc(b.brandName) +
          '</td><td class="num bai-w4-primary-num">' +
          esc(fmtPct(b.currentPresence)) +
          '</td><td class="num bai-w4-secondary-num">' +
          esc(fmtPct(b.priorPresence)) +
          '</td><td class="num">' +
          esc(b.deltaDisplay) +
          "</td><td>" +
          esc(b.rankDisplay || "—") +
          "</td><td>" +
          statusChip("abs", b.absoluteLabel) +
          " / " +
          statusChip("rel", b.relativeLabel) +
          "</td><td>" +
          esc(b.membershipState) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderCompetitive(pv) {
    var story = $("baiW4CompetitiveStory");
    var body = $("baiW4CompetitiveBody");
    if (story) story.textContent = (pv.competitive.story && pv.competitive.story.narrative) || "";
    if (!body) return;
    body.innerHTML = (pv.competitive.rows || [])
      .map(function (r) {
        return (
          "<tr><td>" +
          esc(r.brandName) +
          '</td><td class="num bai-w4-primary-num">' +
          esc(r.currentRank != null ? "#" + r.currentRank : "—") +
          '</td><td class="num bai-w4-secondary-num">' +
          esc(r.priorRank != null ? "#" + r.priorRank : "—") +
          "</td><td>" +
          esc(r.rankDisplay || r.movementState) +
          '</td><td class="num">' +
          esc(fmtPct(r.currentPresence)) +
          '</td><td class="num">' +
          esc(r.deltaDisplay) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderParent() {
    var pv = parentView();
    if (!pv) return;
    $("baiW4ParentTitle").textContent = pv.parentCompanyName || state.parentKey;
    $("baiW4Exec").textContent = (pv.executiveRead && pv.executiveRead.narrative) || "";
    $("baiW4Kpis").innerHTML = renderHierarchyCard(pv.portfolio || {});
    $("baiW4Intent").textContent =
      (pv.ownerIntent && pv.ownerIntent.presentation) ||
      "Intent-level change is not yet comparable for this monitoring pair.";
    $("baiW4IntentDetail").textContent =
      (pv.ownerIntent && pv.ownerIntent.detail) || "";
    renderTrend(pv.trend);
    renderBrandTable(pv);
    renderProvider(pv.provider || { rows: [] });
    renderCompetitive(pv);
  }

  function wireControls(data) {
    var select = $("baiW4ParentSelect");
    var sort = $("baiW4SortSelect");
    var parents = (data.wave4 && data.wave4.parents) || [];
    if (select) {
      select.innerHTML = parents
        .map(function (p) {
          return (
            '<option value="' +
            esc(p.parentCompanyKey) +
            '">' +
            esc(p.parentCompanyName) +
            "</option>"
          );
        })
        .join("");
      select.value = state.parentKey;
      select.onchange = function () {
        state.parentKey = select.value;
        destroyCharts();
        renderParent();
      };
    }
    if (sort) {
      sort.value = state.sort;
      sort.onchange = function () {
        state.sort = sort.value;
        renderBrandTable(parentView());
      };
    }
  }

    async function load() {
      var status = $("baiQaStatus");
      try {
        var url =
          "/api/ai-visibility/brand/internal-longitudinal-qa?scope=full_cohort&geography=CALA";
        var headers = { Accept: "application/json" };
        var fetchFn =
          (window.DealalityMemberstackAuth &&
            window.DealalityMemberstackAuth.authFetch) ||
          fetch;
        var res;
        try {
          res = await fetchFn(url, { headers: headers });
        } catch (authErr) {
          res = await fetch(url, { headers: headers });
        }
        if (res && res.status === 401) {
          res = await fetch(url, { headers: headers });
        }
        var data = await res.json();
        if (!res.ok || !data.ok || !data.wave4) {
          status.textContent =
            "Blocked or failed: " + (data.message || data.error || res.status);
          return;
        }
      state.data = data;
      status.textContent =
        "Wave 4 loaded · candidate " +
        ((data.periodResolve && data.periodResolve.currentPeriodId) || "—") +
        " · prior " +
        ((data.periodResolve && data.periodResolve.priorPeriodId) || "—") +
        " · " +
        data.PERIOD_2_PUBLICATION_STATE +
        " · LIVE_PROVIDER_CALLS=" +
        (data.LIVE_PROVIDER_CALLS || 0);
      $("baiW4Shell").hidden = false;
      wireControls(data);
      renderParent();
    } catch (err) {
      status.textContent =
        "Error: " + (err && err.message ? err.message : String(err));
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", load);
  } else {
    load();
  }
})();
