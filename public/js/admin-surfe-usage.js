/**
 * Admin — Surfe Usage dashboard (Dealality Admin Resources shell).
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
    var outcome = document.getElementById("asuOutcome").value;
    var q = [];
    if (from) q.push("from=" + encodeURIComponent(from));
    if (to) q.push("to=" + encodeURIComponent(to + "T23:59:59.999Z"));
    if (hotelId) q.push("hotelId=" + encodeURIComponent(hotelId));
    if (portfolioId) q.push("portfolioId=" + encodeURIComponent(portfolioId));
    if (outcome) q.push("status=" + encodeURIComponent(outcome));
    return q.length ? "?" + q.join("&") : "";
  }

  function clearFilters() {
    document.getElementById("asuFrom").value = "";
    document.getElementById("asuTo").value = "";
    document.getElementById("asuHotel").value = "";
    document.getElementById("asuPortfolio").value = "";
    document.getElementById("asuOutcome").value = "";
  }

  function renderKpis(totals, remainingCredits) {
    var remainingLabel = "—";
    if (remainingCredits) {
      remainingLabel =
        "E " +
        num(remainingCredits.totalEmail) +
        " · M " +
        num(remainingCredits.totalMobile);
    }
    var successLike =
      (totals.successfulLookups || 0) + (totals.partialLookups || 0);
    var creditsLabel =
      "C " +
      num(totals.creditsConfirmed) +
      " · E " +
      num(totals.creditsEstimated) +
      " · U " +
      num(totals.creditsUnknownCount);
    var items = [
      { label: "Total Lookups", value: num(totals.totalLookups) },
      { label: "Successful Lookups", value: num(successLike) },
      { label: "Success Rate", value: num(totals.successRate) + "%" },
      { label: "Email Contacts Found", value: num(totals.emailFoundCount) },
      { label: "Phone Contacts Found", value: num(totals.phoneFoundCount) },
      { label: "Credits (confirmed · estimated · unknown)", value: creditsLabel },
      {
        label: "Estimated Cost (known only)",
        value: money(totals.totalCostEstimateUsd),
      },
      { label: "Remaining Surfe Credits", value: remainingLabel },
      {
        label: "Accounting failures",
        value: num(totals.accountingFailureCount),
      },
    ];
    var root = document.getElementById("asuKpis");
    root.innerHTML = items
      .map(function (it) {
        return (
          '<div class="adr-count"><span class="adr-count__n">' +
          esc(it.value) +
          '</span><span class="adr-count__l">' +
          esc(it.label) +
          "</span></div>"
        );
      })
      .join("");
  }

  function emptyRow(colspan, message) {
    return (
      '<tr><td colspan="' +
      colspan +
      '"><div class="asu-empty">' +
      esc(message) +
      "</div></td></tr>"
    );
  }

  function renderTrends() {
    var rows = (summary && summary.trends && summary.trends[activeTrend]) || [];
    var tbody = document.querySelector("#asuTrendsTable tbody");
    if (!rows.length) {
      tbody.innerHTML = emptyRow(
        6,
        "No tracked Surfe usage in this range yet.",
      );
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
      tbody.innerHTML = emptyRow(
        8,
        "No hotel-level Surfe usage recorded for these filters.",
      );
      return;
    }
    tbody.innerHTML = hotels
      .map(function (h) {
        var phoneSearches =
          Number(h.phoneSearches || 0) + Number(h.bothSearches || 0);
        var emailSearches =
          Number(h.emailSearches || 0) + Number(h.bothSearches || 0);
        return (
          "<tr><td>" +
          esc(h.hotelName || h.hotelId) +
          "</td><td>" +
          esc(h.portfolioId || "—") +
          "</td><td>" +
          esc(h.lookups) +
          "</td><td>" +
          esc(emailSearches) +
          "</td><td>" +
          esc(phoneSearches) +
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

  function certaintyPill(certainty) {
    var c = String(certainty || "unknown");
    return (
      '<span class="asu-pill asu-pill--certainty-' +
      esc(c) +
      '">' +
      esc(c) +
      "</span>"
    );
  }

  function renderLatest(rows) {
    var tbody = document.querySelector("#asuLatestTable tbody");
    if (!rows || !rows.length) {
      tbody.innerHTML = emptyRow(
        8,
        "No lookup activity recorded yet for these filters.",
      );
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        var creditsDisplay =
          r.creditsCharged == null
            ? "—"
            : String(r.creditsCharged) +
              (r.creditCertainty ? " (" + r.creditCertainty + ")" : "");
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
          esc(creditsDisplay) +
          "</td><td>" +
          certaintyPill(r.creditCertainty) +
          "</td><td>" +
          esc(
            r.costEstimateUsd == null
              ? "—"
              : money(r.costEstimateUsd) +
                  (r.costCertainty ? " (" + r.costCertainty + ")" : ""),
          ) +
          "</td></tr>"
        );
      })
      .join("");
  }

  function renderOps(summaryData) {
    var t = summaryData.totals || {};
    var durability = summaryData.durability || {};
    var list = document.getElementById("asuOpsList");
    list.innerHTML = [
      "No-match frequency: <strong>" +
        esc(num(summaryData.noMatchFrequency)) +
        "%</strong>",
      "Error lookups: <strong>" + esc(num(t.errorLookups)) + "</strong>",
      "Avg provider latency: <strong>" +
        esc(t.avgLatencyMs == null ? "—" : t.avgLatencyMs + " ms") +
        "</strong>",
      "Accounting complete: <strong>" +
        esc(t.accountingComplete === false ? "No" : "Yes") +
        "</strong> (" +
        esc(num(t.accountingFailureCount)) +
        " failures)",
      "Credits unknown rows: <strong>" +
        esc(num(t.creditsUnknownCount)) +
        "</strong>",
      esc(
        t.creditAccountingNote ||
          "Confirmed / estimated / unknown credits are reported separately.",
      ),
      esc(
        t.duplicateSuppressedNote ||
          "Transport retries dedupe on clientRequestId; legitimate repeats are counted.",
      ),
      "Persistence: <strong>" +
        esc(durability.store || "filesystem") +
        "</strong> · " +
        esc(durability.mechanism || "lock + atomic write"),
      "Personal contact details are not stored in this ledger.",
    ]
      .map(function (html) {
        return "<li>" + html + "</li>";
      })
      .join("");

    var errors = summaryData.recentErrors || [];
    var tbody = document.querySelector("#asuErrorsTable tbody");
    if (!errors.length) {
      tbody.innerHTML = emptyRow(4, "No recent provider errors.");
    } else {
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

    var failures = summaryData.recentAccountingFailures || [];
    var fbody = document.querySelector("#asuAccountingFailures tbody");
    if (!fbody) return;
    if (!failures.length) {
      fbody.innerHTML = emptyRow(4, "No accounting write failures recorded.");
      return;
    }
    fbody.innerHTML = failures
      .map(function (e) {
        return (
          "<tr><td>" +
          esc(e.at) +
          "</td><td>" +
          esc(e.category || "—") +
          "</td><td>" +
          esc(e.hotelId || "—") +
          "</td><td>" +
          esc(e.opportunityId || "—") +
          "</td></tr>"
        );
      })
      .join("");
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
      document.getElementById("asuHistoryNote").textContent =
        data.trackingStartedNote;
    }
    renderKpis(data.totals || {}, data.remainingCredits);
    fillHotelFilter(data.hotels || []);
    renderTrends();
    renderHotels(data.hotels || []);
    renderLatest(data.latestActivity || []);
    renderOps(data);
    setStatus("Updated " + (data.generatedAt || ""));
  }

  function bind() {
    document.getElementById("asuApplyFilters").addEventListener("click", function () {
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.getElementById("asuClearFilters").addEventListener("click", function () {
      clearFilters();
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.getElementById("asuRefresh").addEventListener("click", function () {
      loadSummary().catch(function (err) {
        setStatus(err && err.message ? err.message : "Load failed");
      });
    });
    document.querySelectorAll(".asu-trend-tab").forEach(function (btn) {
      btn.addEventListener("click", function () {
        document.querySelectorAll(".asu-trend-tab").forEach(function (b) {
          b.classList.remove("active");
          b.setAttribute("aria-selected", "false");
        });
        btn.classList.add("active");
        btn.setAttribute("aria-selected", "true");
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
