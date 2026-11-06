/**
 * AI Demand Admin — GDI Research Audit tab.
 * Relocated from customer GDI. Reuses existing research-runs APIs (admin-only).
 */
(function () {
  "use strict";

  var hotels = [];
  var runs = [];
  var selectedHotelId = "";
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
    var el = $("graStatusLine");
    if (el) el.textContent = msg || "";
  }

  function fmtTs(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso);
      return d.toLocaleString();
    } catch (_e) {
      return String(iso);
    }
  }

  function runIdOf(run) {
    return (run && (run.id || run.runId)) || "";
  }

  function runCostUsd(run) {
    if (!run) return 0;
    if (run.cost && run.cost.totalUsd != null) return Number(run.cost.totalUsd) || 0;
    if (run.webhoundSpentUsd != null || run.contactResearchCostUsd != null) {
      return (Number(run.webhoundSpentUsd) || 0) + (Number(run.contactResearchCostUsd) || 0);
    }
    return 0;
  }

  function qualifiedCount(run) {
    if (!run) return "—";
    if (run.metrics && run.metrics.opportunitiesQualified != null) {
      return String(run.metrics.opportunitiesQualified);
    }
    if (run.opportunityCount != null) return String(run.opportunityCount);
    return "—";
  }

  function updateCounts() {
    var hotelsWithRuns = hotels.filter(function (h) {
      return Number(h.runCountHint || 0) > 0;
    }).length;
    if ($("graCardHotels")) {
      $("graCardHotels").textContent = String(
        hotelsWithRuns || (selectedHotelId && runs.length ? 1 : hotels.length || "—")
      );
    }
    if ($("graCardRuns")) $("graCardRuns").textContent = String(runs.length || 0);
    if ($("graCardCost")) {
      var latest = runs[0];
      $("graCardCost").textContent = latest ? "$" + runCostUsd(latest).toFixed(2) : "—";
    }
  }

  function renderHotelSelect() {
    var sel = $("graHotelSelect");
    if (!sel) return;
    var prev = selectedHotelId || sel.value || "";
    sel.innerHTML =
      '<option value="">Select hotel…</option>' +
      hotels
        .map(function (h) {
          var id = h.hotelId || h.id || "";
          var name = h.displayName || h.hotelName || id;
          return (
            '<option value="' +
            esc(id) +
            '"' +
            (id === prev ? " selected" : "") +
            ">" +
            esc(name) +
            (id ? " (" + esc(id) + ")" : "") +
            "</option>"
          );
        })
        .join("");
    if (prev) sel.value = prev;
  }

  function renderRunsTable() {
    var body = $("graTableBody");
    if (!body) return;
    if (!selectedHotelId) {
      body.innerHTML =
        '<tr><td colspan="7">Select a hotel to load research runs.</td></tr>';
      return;
    }
    if (!runs.length) {
      body.innerHTML =
        '<tr><td colspan="7">No research runs found for this hotel.</td></tr>';
      return;
    }
    body.innerHTML = runs
      .map(function (run) {
        var id = runIdOf(run);
        return (
          "<tr>" +
          "<td><code>" +
          esc(id) +
          "</code></td>" +
          "<td>" +
          esc(run.status || run.kind || "—") +
          "</td>" +
          "<td>" +
          esc(fmtTs(run.startedAt || run.createdAt)) +
          "</td>" +
          "<td>" +
          esc(fmtTs(run.completedAt)) +
          "</td>" +
          "<td>" +
          esc(qualifiedCount(run)) +
          "</td>" +
          "<td>$" +
          esc(runCostUsd(run).toFixed(2)) +
          "</td>" +
          '<td><button type="button" class="adr-btn adr-btn--secondary gra-open-run" data-run-id="' +
          esc(id) +
          '">Details</button></td>' +
          "</tr>"
        );
      })
      .join("");

    body.querySelectorAll(".gra-open-run").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openRunDetail(btn.getAttribute("data-run-id"));
      });
    });
  }

  function openRunDetail(runId) {
    var run = runs.find(function (r) {
      return runIdOf(r) === runId;
    });
    var drawer = $("graDrawer");
    var title = $("graDrawerTitle");
    var body = $("graDrawerBody");
    if (!drawer || !body) return;
    if (!run) {
      body.innerHTML = "<p>Run not found in loaded list.</p>";
      drawer.hidden = false;
      return;
    }
    if (title) title.textContent = "Research run · " + runIdOf(run);
    var cost = run.cost || {};
    var metrics = run.metrics || {};
    var categories = Array.isArray(run.researchMethods)
      ? run.researchMethods
      : Array.isArray(run.categories)
        ? run.categories
        : [];
    var warnings =
      (run.warnings && run.warnings.length && run.warnings) ||
      (run.failures && run.failures.length && run.failures) ||
      (cost.escalationLog && cost.escalationLog.filter
        ? cost.escalationLog.filter(function (e) {
            return e && /fail|error|warn/i.test(String(e.result || e.level || ""));
          })
        : []);

    body.innerHTML =
      "<dl class='gra-run-dl'>" +
      "<dt>Run ID</dt><dd><code>" +
      esc(runIdOf(run)) +
      "</code></dd>" +
      "<dt>Hotel</dt><dd><code>" +
      esc(run.hotelId || selectedHotelId) +
      "</code></dd>" +
      "<dt>Status</dt><dd>" +
      esc(run.status || run.kind || "—") +
      "</dd>" +
      "<dt>Started</dt><dd>" +
      esc(fmtTs(run.startedAt || run.createdAt)) +
      "</dd>" +
      "<dt>Completed</dt><dd>" +
      esc(fmtTs(run.completedAt)) +
      "</dd>" +
      "<dt>Trigger</dt><dd>" +
      esc(run.trigger || "—") +
      "</dd>" +
      "<dt>Discovery mode</dt><dd>" +
      esc(run.discoveryMode || "—") +
      "</dd>" +
      "<dt>Research categories / methods</dt><dd>" +
      esc(categories.length ? categories.join(", ") : "—") +
      "</dd>" +
      "<dt>Candidates researched</dt><dd>" +
      esc(metrics.candidatesResearched != null ? metrics.candidatesResearched : "—") +
      "</dd>" +
      "<dt>Opportunities qualified</dt><dd>" +
      esc(qualifiedCount(run)) +
      "</dd>" +
      "<dt>High / Medium / Watch</dt><dd>" +
      esc(
        [
          metrics.highPriorityCount != null ? metrics.highPriorityCount : "—",
          metrics.mediumPriorityCount != null ? metrics.mediumPriorityCount : "—",
          metrics.watchlistCount != null ? metrics.watchlistCount : "—",
        ].join(" / ")
      ) +
      "</dd>" +
      "<dt>Total cost</dt><dd>$" +
      esc(runCostUsd(run).toFixed(2)) +
      "</dd>" +
      "<dt>Webhound / hard cap</dt><dd>$" +
      esc(Number(cost.webhoundUsd || 0).toFixed(2)) +
      " / $" +
      esc(
        Number(
          cost.webhoundHardCapUsd ||
            (run.configuration && run.configuration.webhoundHardCapUsd) ||
            0
        ).toFixed(2)
      ) +
      "</dd>" +
      "<dt>Warnings / failures</dt><dd>" +
      (warnings && warnings.length
        ? "<pre>" + esc(JSON.stringify(warnings, null, 2)) + "</pre>"
        : "None recorded") +
      "</dd>" +
      "</dl>" +
      "<details><summary>Full run JSON (admin)</summary><pre class='gra-run-json'>" +
      esc(JSON.stringify(run, null, 2)) +
      "</pre></details>";

    drawer.hidden = false;

    // Prefer server detail when available (same contract, may include fuller payload).
    authFetch(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(selectedHotelId) +
        "/research-runs/" +
        encodeURIComponent(runIdOf(run))
    )
      .then(function (res) {
        return res.json().then(function (payload) {
          return { res: res, payload: payload };
        });
      })
      .then(function (out) {
        if (!out.res.ok || !out.payload || !out.payload.run) return;
        var detail = out.payload.run;
        var idx = runs.findIndex(function (r) {
          return runIdOf(r) === runIdOf(detail);
        });
        if (idx >= 0) runs[idx] = detail;
        // Re-render drawer from freshest detail without recursion.
        runs = runs.slice();
        var keepId = runIdOf(detail);
        var drawerBody = $("graDrawerBody");
        if (!drawerBody) return;
        drawerBody.querySelectorAll("details").forEach(function () {});
        // Replace full JSON block if present.
        var pre = drawerBody.querySelector(".gra-run-json");
        if (pre) pre.textContent = JSON.stringify(detail, null, 2);
        void keepId;
      })
      .catch(function () {
        /* list payload remains usable */
      });
  }

  function loadRunsForHotel(hotelId) {
    selectedHotelId = String(hotelId || "").trim();
    if (!selectedHotelId) {
      runs = [];
      updateCounts();
      renderRunsTable();
      return Promise.resolve();
    }
    setStatus("Loading research runs…");
    return authFetch(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(selectedHotelId) +
        "/research-runs"
    )
      .then(function (res) {
        return res.json().then(function (payload) {
          return { res: res, payload: payload };
        });
      })
      .then(function (out) {
        if (out.res.status === 401 || out.res.status === 403) {
          runs = [];
          setStatus("Admin authorization required to view research runs.");
          updateCounts();
          renderRunsTable();
          return;
        }
        if (!out.res.ok) {
          runs = [];
          setStatus(
            "Failed to load runs (" +
              out.res.status +
              "): " +
              ((out.payload && out.payload.error) || "error")
          );
          updateCounts();
          renderRunsTable();
          return;
        }
        runs = (out.payload && out.payload.runs) || [];
        setStatus(
          runs.length
            ? "Loaded " + runs.length + " research run(s)."
            : "No research runs for this hotel."
        );
        updateCounts();
        renderRunsTable();
      })
      .catch(function (err) {
        runs = [];
        setStatus(String((err && err.message) || err || "load_failed"));
        updateCounts();
        renderRunsTable();
      });
  }

  function loadHotelCatalog() {
    setStatus("Loading hotel catalog…");
    return authFetch("/api/admin/group-demand-intelligence/reports")
      .then(function (res) {
        return res.json().then(function (payload) {
          return { res: res, payload: payload };
        });
      })
      .then(function (out) {
        if (!out.res.ok) {
          // Fallback: GDI selectable hotels (admin session).
          return authFetch("/api/group-demand-intelligence/hotels").then(function (res2) {
            return res2.json().then(function (payload2) {
              hotels = (payload2.hotels || []).map(function (h) {
                return {
                  hotelId: h.hotelId || h.id,
                  displayName: h.displayName || h.hotelName || h.hotelId || h.id,
                };
              });
              renderHotelSelect();
              setStatus(
                hotels.length
                  ? "Catalog loaded (" + hotels.length + " hotels)."
                  : "No hotels available."
              );
              updateCounts();
            });
          });
        }
        var rows =
          (out.payload && (out.payload.hotels || out.payload.rows || out.payload.items)) ||
          [];
        hotels = rows.map(function (h) {
          return {
            hotelId: h.hotelId || h.id,
            displayName: h.displayName || h.hotelName || h.hotelId || h.id,
            runCountHint: h.runCount || h.researchRunCount || 0,
          };
        });
        renderHotelSelect();
        setStatus(
          hotels.length
            ? "Catalog loaded (" + hotels.length + " hotels)."
            : "No hotels in GDI reports catalog."
        );
        updateCounts();
      })
      .catch(function (err) {
        setStatus(String((err && err.message) || err || "catalog_failed"));
      });
  }

  function wire() {
    var sel = $("graHotelSelect");
    if (sel) {
      sel.addEventListener("change", function () {
        loadRunsForHotel(sel.value);
      });
    }
    var refresh = $("graRefresh");
    if (refresh) {
      refresh.addEventListener("click", function () {
        if (selectedHotelId) loadRunsForHotel(selectedHotelId);
        else loadHotelCatalog();
      });
    }
    var close = $("graDrawerClose");
    if (close) {
      close.addEventListener("click", function () {
        var drawer = $("graDrawer");
        if (drawer) drawer.hidden = true;
      });
    }
  }

  function onTab(ev) {
    var tab = ev && ev.detail && ev.detail.tab;
    if (tab !== "gdi-research-audit") return;
    if (!loaded) {
      loaded = true;
      loadHotelCatalog().then(function () {
        if (selectedHotelId) return loadRunsForHotel(selectedHotelId);
      });
    }
  }

  function boot() {
    wire();
    window.addEventListener("dealality-adp-admin-tab", onTab);
    window.addEventListener("dealality-adp-admin-ready", function () {
      var params = new URLSearchParams(window.location.search || "");
      if (params.get("tab") === "gdi-research-audit") {
        onTab({ detail: { tab: "gdi-research-audit" } });
      }
    });
  }

  boot();
})();
