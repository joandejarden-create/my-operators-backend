/**
 * Admin — Surfe Usage dashboard client.
 * Never renders or requests personal email/phone values.
 */
(function () {
  "use strict";

  var summary = null;
  var activeTrend = "daily";

  function authFetch(url, opts) {
    opts = opts || {};
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, opts);
    }
    return fetch(url, Object.assign({ credentials: "same-origin" }, opts));
  }

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function money(n) {
    var x = Number(n);
    if (!Number.isFinite(x)) return "—";
    return "$" + x.toFixed(2);
  }

  function num(n) {
    var x = Number(n);
    if (!Number.isFinite(x)) return "—";
    return String(x);
  }

  function setStatus(text) {
    var el = document.getElementById("asuStatus");
    if (el) el.textContent = text || "";
  }

  function queryParams() {
    var from = document.getElementById("asuFrom").value;
    var to = document.getElementById("asuTo").value;
    var hotelId = document.getElementById("asuHotel").value;
    var portfolioId = document.getElementById("asuPortfolio").value.trim();
    var q = [];
    if (from) q.push("from=" + encodeURIComponent(from));
    if (to) q.push("to=" + encodeURIComponent(to + "T23:59:59.999Z"));
    if (hotelId) q.push("hotelId=" + encodeURIComponent(hotelId));
    if (portfolioId) q.push("portfolioId=" + encodeURIComponent(portfolioId));
    return q.length ? "?" + q.join("&") : "";
  }

  function renderKpis(totals) {
    var items = [
      { label: "Total lookups", value: num(totals.totalLookups) },
      { label: "Successful (+ partial)", value: num((totals.successfulLookups || 0) + (totals.partialLookups || 0)) },
      { label: "Success rate", value: num(totals.successRate) + "%" },
      { label: "Credits consumed", value: num(totals.creditsConsumed) },
      { label: "Est. enrichment cost", value: money(totals.totalCostEstimateUsd) },
      { label: "Avg cost / success", value: money(totals.avgCostPerSuccessfulLookupUsd) },
    ];
    var root = document.getElementById("asuKpis");
    root.innerHTML = items
      .map(function (it) {
        return (
          '<div class="asu-kpi"><div class="asu-kpi__label">' +
          esc(it.label) +
          '</div><div class="asu-kpi__value">' +
          esc(it.value) +
          "</div></div>"
        );
      })
      .join("");
  }

  function renderTrends() {
    var rows = (summary && summary.trends && summary.trends[activeTrend]) || [];
    var tbody = document.querySelector("#asuTrendsTable tbody");
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="6">No tracked events in this range.</td></tr>';
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        return (
          "<tr><td>" +
          esc(r.key) +
          "</td><td>" +
          esc(r.lookups) +
          "</td><td>" +
          esc(r.success) +
          "</td><td>" +
          esc(r.unsuccessful) +
          "</td><td>" +
          esc(r.credits) +
          "</td><td>" +
          esc(money(r.costUsd)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function fillHotelFilter(hotels) {
    var sel = document.getElementById("asuHotel");
    var prev = sel.value;
    sel.innerHTML = '<option value="">All hotels</option>';
    (hotels || []).forEach(function (h) {
      var opt = document.createElement("option");
      opt.value = h.hotelId;
      opt.textContent = h.hotelName || h.hotelId;
      sel.appendChild(opt);
    });
    if (prev) sel.value = prev;
  }

  function renderHotels(hotels) {
    var tbody = document.querySelector("#asuHotelsTable tbody");
    if (!hotels || !hotels.length) {
      tbody.innerHTML = '<tr><td colspan="9">No hotel-level usage yet.</td></tr>';
      return;
    }
    tbody.innerHTML = hotels
      .map(function (h) {
        return (
          "<tr><td>" +
          esc(h.hotelName || h.hotelId) +
          "</td><td>" +
          esc(h.portfolioId || "—") +
          "</td><td>" +
          esc(h.lookups) +
          "</td><td>" +
          esc(h.emailSearches) +
          "</td><td>" +
          esc(h.phoneSearches) +
          "</td><td>" +
          esc(h.bothSearches) +
          "</td><td>" +
          esc(h.successfulLookups) +
          "</td><td>" +
          esc(h.creditsConsumed) +
          "</td><td>" +
          esc(money(h.costEstimateUsd)) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function statusPill(status) {
    var s = String(status || "unknown");
    return '<span class="asu-pill asu-pill--' + esc(s) + '">' + esc(s) + "</span>";
  }

  function yn(v) {
    return v ? "Yes" : "No";
  }

  function renderLatest(rows) {
    var tbody = document.querySelector("#asuLatestTable tbody");
    if (!rows || !rows.length) {
      tbody.innerHTML = '<tr><td colspan="9">No lookup activity recorded yet.</td></tr>';
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        return (
          "<tr><td>" +
          esc(r.at) +
          "</td><td>" +
          esc(r.hotelName || r.hotelId) +
          "</td><td>" +
          esc(r.opportunityId || "—") +
          "</td><td>" +
          esc(r.lookupType || "—") +
          "</td><td>" +
          statusPill(r.status) +
          "</td><td>" +
          esc(yn(r.emailFound)) +
          "</td><td>" +
          esc(yn(r.phoneFound)) +
          "</td><td>" +
          esc(r.creditsCharged == null ? "—" : r.creditsCharged) +
          "</td><td>" +
          esc(r.providerLatencyMs == null ? "—" : r.providerLatencyMs + " ms") +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderOps(summaryData) {
    var t = summaryData.totals || {};
    var list = document.getElementById("asuOpsList");
    list.innerHTML = [
      "No-match frequency: <strong>" + esc(num(summaryData.noMatchFrequency)) + "%</strong>",
      "Error lookups: <strong>" + esc(num(t.errorLookups)) + "</strong>",
      "Avg provider latency: <strong>" +
        esc(t.avgLatencyMs == null ? "—" : t.avgLatencyMs + " ms") +
        "</strong>",
      esc(t.duplicateSuppressedNote || "Duplicate in-flight lookups are blocked server-side."),
      "Personal contact details are not stored in this ledger.",
    ]
      .map(function (html) {
        return "<li>" + html + "</li>";
      })
      .join("");

    var errors = summaryData.recentErrors || [];
    var tbody = document.querySelector("#asuErrorsTable tbody");
    if (!errors.length) {
      tbody.innerHTML = '<tr><td colspan="4">No recent provider errors.</td></tr>';
      return;
    }
    tbody.innerHTML = errors
      .map(function (e) {
        return (
          "<tr><td>" +
          esc(e.at) +
          "</td><td>" +
          esc(e.hotelId || "—") +
          "</td><td>" +
          esc(e.errorCategory || "—") +
          "</td><td>" +
          esc(e.opportunityId || "—") +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderRemaining(credits) {
    var el = document.getElementById("asuRemainingCredits");
    if (!credits) {
      el.textContent = "Live credit balance unavailable.";
      return;
    }
    el.innerHTML =
      "Email credits: <strong>" +
      esc(credits.totalEmail) +
      "</strong> · Mobile credits: <strong>" +
      esc(credits.totalMobile) +
      "</strong>";
  }

  async function loadSummary() {
    setStatus("Loading…");
    var res = await authFetch("/api/admin/surfe-usage/summary" + queryParams(), {
      cache: "no-store",
    });
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok || !data.ok) {
      setStatus((data && (data.message || data.error)) || "Failed to load usage");
      return;
    }
    summary = data;
    if (data.trackingStartedNote) {
      document.getElementById("asuHistoryNote").textContent = data.trackingStartedNote;
    }
    renderKpis(data.totals || {});
    fillHotelFilter(data.hotels || []);
    renderTrends();
    renderHotels(data.hotels || []);
    renderLatest(data.latestActivity || []);
    renderOps(data);
    renderRemaining(data.remainingCredits);
    setStatus("Updated " + (data.generatedAt || ""));
  }

  function bind() {
    document.getElementById("asuApplyFilters").addEventListener("click", function () {
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.getElementById("asuRefresh").addEventListener("click", function () {
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.querySelectorAll(".asu-tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        document.querySelectorAll(".asu-tab").forEach(function (b) {
          b.classList.remove("active");
        });
        btn.classList.add("active");
        activeTrend = btn.getAttribute("data-trend") || "daily";
        renderTrends();
      });
    });
  }

  async function boot() {
    var gate = window.SupportAdminGate;
    if (!gate || typeof gate.requireAdmin !== "function") {
      document.getElementById("supportGateLoading").hidden = true;
      document.getElementById("supportGateDenied").hidden = false;
      return;
    }
    var ok = await gate.requireAdmin({
      loadingEl: document.getElementById("supportGateLoading"),
      deniedEl: document.getElementById("supportGateDenied"),
      contentEl: document.getElementById("supportGateContent"),
    });
    if (!ok) return;
    bind();
    await loadSummary();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
