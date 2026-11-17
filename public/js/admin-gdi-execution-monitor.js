/**
 * AI Demand Admin — GDI Full E2E Execution Monitor tab.
 * Founder/admin only. Observability; never triggers paid research.
 */
(function () {
  "use strict";

  var hotels = [];
  var runs = [];
  var selectedHotelId = "";
  var selectedRunId = "";
  var dashboard = null;
  var loaded = false;

  function authFetch(url, opts) {
    opts = opts || {};
    if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.authFetch) {
      return window.DealalityMemberstackAuth.authFetch(url, opts);
    }
    return fetch(url, Object.assign({ credentials: "same-origin" }, opts));
  }

  function $(id) {
    return document.getElementById(id);
  }

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setStatus(msg) {
    var el = $("gemStatusLine");
    if (el) el.textContent = msg || "";
  }

  function pillClass(status) {
    var s = String(status || "").toUpperCase();
    if (/COMPLETE|EXECUTED|OK|ENABLED/.test(s)) return "gem-pill gem-pill--ok";
    if (/PARTIAL|UNKNOWN|CACHE|SKIP|BLOCK|PLANNED|PENDING/.test(s))
      return "gem-pill gem-pill--warn";
    if (/FAIL|ERROR/.test(s)) return "gem-pill gem-pill--bad";
    return "gem-pill gem-pill--muted";
  }

  function renderHotelSelect() {
    var sel = $("gemHotelSelect");
    if (!sel) return;
    var prev = selectedHotelId || sel.value || "";
    sel.innerHTML =
      '<option value="">Select hotel…</option>' +
      hotels
        .map(function (h) {
          var id = h.hotelId || "";
          return (
            '<option value="' +
            esc(id) +
            '">' +
            esc(h.displayName || id) +
            (h.runCount ? " (" + h.runCount + " runs)" : "") +
            "</option>"
          );
        })
        .join("");
    if (prev) sel.value = prev;
  }

  function renderRunSelect() {
    var sel = $("gemRunSelect");
    if (!sel) return;
    sel.innerHTML =
      '<option value="">Select run…</option>' +
      runs
        .map(function (r) {
          var id = r.runId || "";
          var label =
            (r.label || id) +
            (r.historical ? " [historical]" : "") +
            (r.verdict ? " — " + r.verdict : "");
          return '<option value="' + esc(id) + '">' + esc(label) + "</option>";
        })
        .join("");
    if (selectedRunId) sel.value = selectedRunId;
  }

  function renderSummary(d) {
    var s = (d && d.summary) || {};
    var cert = (d && d.certificate) || {};
    if ($("gemCardSegments"))
      $("gemCardSegments").textContent = String(s.segmentsCompletedOf14 || 0) + " / 14";
    if ($("gemCardProviders"))
      $("gemCardProviders").textContent = String(
        (s.providersInvoked && s.providersInvoked.length) || 0
      );
    if ($("gemCardCalls"))
      $("gemCardCalls").textContent = String(s.providerCalls || 0);
    if ($("gemCardSpend"))
      $("gemCardSpend").textContent =
        "$" + Number(s.totalSpendUsd || 0).toFixed(2);
    if ($("gemCardVerdict"))
      $("gemCardVerdict").textContent = cert.verdict || d.run?.runStatus || "—";
  }

  function renderSegmentMatrix(d) {
    var host = $("gemSegmentMatrix");
    if (!host) return;
    var rows = (d && d.segmentMatrix) || [];
    var html =
      '<table class="gem-table"><thead><tr>' +
      "<th>Segment</th><th>Discovery</th><th>Deep Research</th><th>Providers</th>" +
      "<th>Requests</th><th>Cost</th><th>Candidates</th><th>Qualified</th>" +
      "<th>Visible</th><th>Reason</th></tr></thead><tbody>";
    rows.forEach(function (r, idx) {
      html +=
        '<tr data-expand="' +
        idx +
        '"><td>' +
        esc(r.displayTitle || r.segmentId) +
        '</td><td><span class="' +
        pillClass(r.discoveryStatus) +
        '">' +
        esc(r.discoveryStatus) +
        '</span></td><td><span class="' +
        pillClass(r.deepResearchStatus) +
        '">' +
        esc(r.deepResearchStatus) +
        "</span></td><td>" +
        esc((r.providersUsed || []).join(", ") || "—") +
        "</td><td>" +
        esc(r.requestCount) +
        "</td><td>$" +
        Number(r.costUsd || 0).toFixed(2) +
        "</td><td>" +
        esc(r.candidatesFound) +
        "</td><td>" +
        esc(r.qualifiedOpportunities) +
        "</td><td>" +
        esc(r.customerVisibleAdditions) +
        "</td><td>" +
        esc(r.errorOrSkipReason || "—") +
        "</td></tr>";
      var calls = r.providerCalls || [];
      html +=
        '<tr class="gem-detail-row" data-detail="' +
        idx +
        '" hidden><td colspan="10"><div class="gem-detail">' +
        esc(
          calls.length
            ? JSON.stringify(calls, null, 2)
            : "No provider calls recorded for this segment."
        ) +
        "</div></td></tr>";
    });
    html += "</tbody></table>";
    host.innerHTML = html;
    host.querySelectorAll("tr[data-expand]").forEach(function (tr) {
      tr.addEventListener("click", function () {
        var i = tr.getAttribute("data-expand");
        var detail = host.querySelector('tr[data-detail="' + i + '"]');
        if (!detail) return;
        detail.hidden = !detail.hidden;
        tr.classList.toggle("gem-expanded", !detail.hidden);
      });
    });
  }

  function renderProviderMatrix(d) {
    var host = $("gemProviderMatrix");
    if (!host) return;
    var rows = (d && d.providerMatrix) || [];
    var html =
      '<table class="gem-table"><thead><tr>' +
      "<th>Provider</th><th>Config</th><th>Enabled</th><th>Submitted</th>" +
      "<th>Completed</th><th>Failed</th><th>Cache</th><th>Spend</th>" +
      "<th>Segments</th><th>Last</th><th>Reason</th></tr></thead><tbody>";
    rows.forEach(function (r) {
      html +=
        "<tr><td>" +
        esc(r.name || r.providerId) +
        '</td><td><span class="' +
        pillClass(r.configurationState) +
        '">' +
        esc(r.configurationState) +
        "</span></td><td>" +
        (r.enabled ? "yes" : "no") +
        "</td><td>" +
        esc(r.callsSubmitted) +
        "</td><td>" +
        esc(r.callsCompleted) +
        "</td><td>" +
        esc(r.callsFailed) +
        "</td><td>" +
        esc(r.cacheHits) +
        "</td><td>$" +
        Number(r.actualSpendUsd || 0).toFixed(2) +
        "</td><td>" +
        esc((r.segmentsServed || []).join(", ") || "—") +
        "</td><td>" +
        esc(r.lastExecution || "—") +
        "</td><td>" +
        esc(r.failureOrSkipReason || "—") +
        "</td></tr>";
    });
    html += "</tbody></table>";
    host.innerHTML = html;
  }

  function renderApify(d) {
    var host = $("gemApifyLifecycle");
    if (!host) return;
    var rows = (d && d.apifyLifecycle) || [];
    if (!rows.length) {
      host.innerHTML =
        '<p class="gem-status">No Apify lifecycle events in this run (configured ≠ executed).</p>';
      return;
    }
    var html =
      '<table class="gem-table"><thead><tr><th>When</th><th>State</th><th>Segment</th><th>Actor</th><th>Job</th><th>Items</th><th>Cache</th><th>Reason</th></tr></thead><tbody>';
    rows.forEach(function (r) {
      html +=
        "<tr><td>" +
        esc(r.at) +
        '</td><td><span class="' +
        pillClass(r.state) +
        '">' +
        esc(r.state) +
        "</span></td><td>" +
        esc(r.segmentId || "—") +
        "</td><td>" +
        esc(r.actorId || "—") +
        "</td><td>" +
        esc(r.jobId || "—") +
        "</td><td>" +
        esc(r.itemCount == null ? "—" : r.itemCount) +
        "</td><td>" +
        (r.cacheHit ? "yes" : "no") +
        "</td><td>" +
        esc(r.reason || "—") +
        "</td></tr>";
    });
    html += "</tbody></table>";
    host.innerHTML = html;
  }

  function renderStages(d) {
    var host = $("gemStageTimeline");
    if (!host) return;
    var rows = (d && d.stageTimeline) || [];
    var html =
      '<table class="gem-table"><thead><tr><th>Stage</th><th>Status</th><th>Started</th><th>Completed</th><th>Error</th></tr></thead><tbody>';
    rows.forEach(function (r) {
      html +=
        "<tr><td>" +
        esc((r.letter ? r.letter + ". " : "") + (r.title || r.stageId)) +
        '</td><td><span class="' +
        pillClass(r.status) +
        '">' +
        esc(r.status) +
        "</span></td><td>" +
        esc(r.startedAt || "—") +
        "</td><td>" +
        esc(r.completedAt || "—") +
        "</td><td>" +
        esc(r.error || "—") +
        "</td></tr>";
    });
    html += "</tbody></table>";
    host.innerHTML = html;
  }

  function renderSkips(d) {
    var host = $("gemSilentSkips");
    if (!host) return;
    var rows = (d && d.silentSkips) || [];
    if (!rows.length) {
      host.innerHTML = '<p class="gem-status">No silent-skip findings.</p>';
      return;
    }
    var html =
      '<table class="gem-table"><thead><tr><th>Segment</th><th>Kind</th><th>Reason</th></tr></thead><tbody>';
    rows.forEach(function (r) {
      html +=
        "<tr><td>" +
        esc(r.segmentId) +
        '</td><td><span class="' +
        pillClass(r.kind) +
        '">' +
        esc(r.kind) +
        "</span></td><td>" +
        esc(r.reason) +
        "</td></tr>";
    });
    html += "</tbody></table>";
    host.innerHTML = html;
  }

  function renderCertificate(d) {
    var el = $("gemCertificate");
    if (!el) return;
    el.textContent = JSON.stringify((d && d.certificate) || {}, null, 2);
  }

  function renderAll(d) {
    dashboard = d;
    renderSummary(d);
    renderSegmentMatrix(d);
    renderProviderMatrix(d);
    renderApify(d);
    renderStages(d);
    renderSkips(d);
    renderCertificate(d);
  }

  async function loadHotels() {
    setStatus("Loading hotels…");
    var res = await authFetch(
      "/api/group-demand-intelligence/execution-monitor/hotels"
    );
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error(json.message || json.error || "hotels_failed");
    hotels = json.hotels || [];
    renderHotelSelect();
    setStatus("Select a hotel.");
  }

  async function loadRuns(hotelId) {
    selectedHotelId = hotelId;
    selectedRunId = "";
    dashboard = null;
    if (!hotelId) {
      runs = [];
      renderRunSelect();
      return;
    }
    setStatus("Loading runs…");
    var res = await authFetch(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(hotelId) +
        "/execution-monitor/runs"
    );
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error(json.message || json.error || "runs_failed");
    runs = json.runs || [];
    renderRunSelect();
    if (runs[0] && runs[0].runId) {
      selectedRunId = runs[0].runId;
      $("gemRunSelect").value = selectedRunId;
      await loadRun(hotelId, selectedRunId);
    } else {
      setStatus("No execution runs for this hotel yet. Reconcile historical or run Full E2E with observability context.");
    }
  }

  async function loadRun(hotelId, runId) {
    selectedRunId = runId;
    if (!hotelId || !runId) return;
    setStatus("Loading run " + runId + "…");
    var res = await authFetch(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(hotelId) +
        "/execution-monitor/runs/" +
        encodeURIComponent(runId)
    );
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error(json.message || json.error || "run_failed");
    renderAll(json.dashboard);
    setStatus(
      "Loaded " +
        runId +
        (json.dashboard && json.dashboard.run && json.dashboard.run.historical
          ? " (historical reconciliation)"
          : "")
    );
  }

  async function reconcileHistorical() {
    setStatus("Reconciling historical YOTEL / Radisson / Bethesda…");
    var res = await authFetch(
      "/api/group-demand-intelligence/execution-monitor/reconcile-historical",
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" }
    );
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error(json.message || json.error || "reconcile_failed");
    setStatus("Reconciled " + json.count + " hotels. Refreshing…");
    await loadHotels();
    if (selectedHotelId) await loadRuns(selectedHotelId);
  }

  function wire() {
    var hotelSel = $("gemHotelSelect");
    var runSel = $("gemRunSelect");
    if (hotelSel) {
      hotelSel.addEventListener("change", function () {
        loadRuns(hotelSel.value).catch(function (e) {
          setStatus(String(e.message || e));
        });
      });
    }
    if (runSel) {
      runSel.addEventListener("change", function () {
        loadRun(selectedHotelId, runSel.value).catch(function (e) {
          setStatus(String(e.message || e));
        });
      });
    }
    var refresh = $("gemRefreshBtn");
    if (refresh) {
      refresh.addEventListener("click", function () {
        if (selectedHotelId && selectedRunId) {
          loadRun(selectedHotelId, selectedRunId).catch(function (e) {
            setStatus(String(e.message || e));
          });
        } else {
          loadHotels().catch(function (e) {
            setStatus(String(e.message || e));
          });
        }
      });
    }
    var recon = $("gemReconcileBtn");
    if (recon) {
      recon.addEventListener("click", function () {
        reconcileHistorical().catch(function (e) {
          setStatus(String(e.message || e));
        });
      });
    }
  }

  function onTab(ev) {
    var tab = ev && ev.detail && ev.detail.tab;
    if (tab !== "gdi-execution-monitor") return;
    if (!loaded) {
      loaded = true;
      wire();
      loadHotels().catch(function (e) {
        setStatus(String(e.message || e));
      });
    }
  }

  window.addEventListener("dealality-adp-admin-tab", onTab);
  window.addEventListener("dealality-adp-admin-ready", function () {
    try {
      var params = new URLSearchParams(window.location.search || "");
      if (params.get("tab") === "gdi-execution-monitor") {
        onTab({ detail: { tab: "gdi-execution-monitor" } });
      }
    } catch (_e) {}
  });
})();
