/**
 * Validation Scorecard UI — Data Trust / Monitoring Coverage / Monitoring Operations.
 * No composite accuracy or trust score.
 */
(function () {
  "use strict";

  var inventoryRows = [];

  function apiBase() {
    if (window.DEALALITY_API_BASE) return String(window.DEALALITY_API_BASE).replace(/\/$/, "");
    return "";
  }

  async function getJwt() {
    if (window.__dealalityMemberstackJwt) return window.__dealalityMemberstackJwt;
    if (window.DealalityMemberstackAuth?.getMemberstackJwtWhenReady) {
      return window.DealalityMemberstackAuth.getMemberstackJwtWhenReady(2000);
    }
    return null;
  }

  async function fetchJson(path) {
    var jwt = await getJwt();
    var headers = { Accept: "application/json" };
    if (jwt) headers.Authorization = "Bearer " + jwt;
    var res;
    try {
      res = await fetch(apiBase() + path, { headers: headers });
    } catch (err) {
      return {
        status: 0,
        data: {
          ok: false,
          code: "SERVER_ERROR",
          message: err && err.message ? err.message : "Network error",
        },
      };
    }
    var data = await res.json().catch(function () {
      return null;
    });
    return { status: res.status, data: data };
  }

  function esc(s) {
    return window.AiVisibilityUi && AiVisibilityUi.escapeHtml
      ? AiVisibilityUi.escapeHtml(String(s == null ? "" : s))
      : String(s == null ? "" : s)
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;");
  }

  function na(v) {
    if (v == null || v === "") return "Not Available";
    return v;
  }

  function fmtCost(v) {
    if (v == null) return "Not Available";
    return "Estimated $" + (Math.round(Number(v) * 10000) / 10000);
  }

  function pct(v) {
    if (v == null) return "—";
    return Math.round(Number(v) * 1000) / 10 + "%";
  }

  function setState(msg, isError) {
    var el = document.getElementById("aivValState");
    if (!el) return;
    el.hidden = false;
    el.textContent = msg;
    el.className = "aiv-val-state " + (isError ? "error" : "loading");
  }

  function classifySummaryFailure(status, data) {
    if (status === 401) {
      return {
        code: "AUTH_REQUIRED",
        message: "Sign in required to view Validation Scorecard. (AUTH_REQUIRED)",
      };
    }
    if (status === 403) {
      return {
        code: "ACCESS_DENIED",
        message:
          "Access denied. Validation Scorecard is for founder/admin governance only. (ACCESS_DENIED)",
      };
    }
    if (status === 404) {
      var code404 = (data && (data.code || data.error)) || "VALIDATION_SUMMARY_ROUTE_MISSING";
      if (code404 === "VALIDATION_REPORT_MISSING" || code404 === "VALIDATION_NOT_RUN") {
        return {
          code: "VALIDATION_NOT_RUN",
          message:
            "Validation has not been run yet, or the report file is missing. Run npm run ai-intelligence:validate. (VALIDATION_NOT_RUN)",
        };
      }
      return {
        code: "VALIDATION_SUMMARY_ROUTE_MISSING",
        message:
          "Validation summary API route not found (404). Restart the Dealality server from C:\\Dev\\deal-capture-proxy so routes register. (ROUTE_MISSING)",
      };
    }
    if (status === 500) {
      var code500 = (data && (data.code || data.error)) || "SERVER_ERROR";
      if (code500 === "VALIDATION_REPORT_INVALID") {
        return {
          code: "VALIDATION_REPORT_INVALID",
          message: "Validation report exists but could not be parsed. (VALIDATION_REPORT_INVALID)",
        };
      }
      return {
        code: "SERVER_ERROR",
        message:
          (data && data.message) ||
          "Validation Scorecard server error while loading summary. (SERVER_ERROR)",
      };
    }
    if (!data || data.ok !== true) {
      return {
        code: "SERVER_ERROR",
        message: "Failed to load validation summary (unexpected response).",
      };
    }
    return null;
  }

  function show(id) {
    var el = document.getElementById(id);
    if (el) el.hidden = false;
  }

  function renderCards(hostId, cards) {
    var host = document.getElementById(hostId);
    if (!host || !cards) return;
    var html = "";
    if (Array.isArray(cards)) {
      html = cards
        .map(function (c) {
          return (
            '<div class="aiv-val-card"><h3>' +
            esc(c.label) +
            '</h3><div class="val">' +
            esc(c.value) +
            '</div><div class="detail">' +
            esc(c.detail || "") +
            "</div></div>"
          );
        })
        .join("");
    } else {
      html = Object.keys(cards)
        .map(function (k) {
          var c = cards[k];
          return (
            '<div class="aiv-val-card"><h3>' +
            esc(c.label) +
            '</h3><div class="val">' +
            esc(c.value) +
            '</div><div class="detail">' +
            esc(c.detail || "") +
            "</div></div>"
          );
        })
        .join("");
    }
    host.innerHTML = html;
  }

  function renderTopSummary(top) {
    if (!top) return;
    var cards = [
      {
        label: "DATA TRUST — Metric Reconciliation",
        value: (top.DATA_TRUST && top.DATA_TRUST.metricReconciliation && top.DATA_TRUST.metricReconciliation.value) || "—",
        detail: "Classification: " + ((top.DATA_TRUST && top.DATA_TRUST.classificationStatus) || "—"),
      },
      {
        label: "DATA TRUST — Evidence Traceability",
        value: (top.DATA_TRUST && top.DATA_TRUST.evidenceTraceability && top.DATA_TRUST.evidenceTraceability.value) || "—",
        detail: "Validation failures: " + ((top.DATA_TRUST && top.DATA_TRUST.validationFailures) || 0),
      },
      {
        label: "MONITORING COVERAGE — Providers",
        value: ((top.MONITORING_COVERAGE && top.MONITORING_COVERAGE.providersMonitored) || []).join(", ") || "—",
        detail:
          "Languages: " +
          (((top.MONITORING_COVERAGE && top.MONITORING_COVERAGE.languagesMonitored) || []).join(", ") || "—") +
          " · Publishable batches: " +
          ((top.MONITORING_COVERAGE && top.MONITORING_COVERAGE.publishableBatches) || 0),
      },
      {
        label: "MONITORING OPERATIONS — Successful Prompts",
        value: (top.MONITORING_OPERATIONS && top.MONITORING_OPERATIONS.successfulPrompts) || 0,
        detail:
          "Latest monitoring: " +
          na(top.MONITORING_OPERATIONS && top.MONITORING_OPERATIONS.latestMonitoring) +
          " · Est. spend: " +
          fmtCost(top.MONITORING_OPERATIONS && top.MONITORING_OPERATIONS.estimatedSpend) +
          " · Failed calls: " +
          ((top.MONITORING_OPERATIONS && top.MONITORING_OPERATIONS.failedCalls) || 0),
      },
    ];
    renderCards("aivValTopCards", cards);
  }

  function renderGolden(g, threshold) {
    var host = document.getElementById("aivValGolden");
    if (!host) return;
    if (!g || !g.CASE_COUNT) {
      host.innerHTML =
        '<p class="aiv-val-muted">Golden Set Not Yet Run. Sample size: 0. Threshold Not Yet Governed.</p>';
      return;
    }
    var cov = g.coverage || {};
    host.innerHTML =
      "<p><strong>Golden Set Version:</strong> " +
      esc(g.GOLDEN_SET_VERSION) +
      " · <strong>Human-labelled cases:</strong> " +
      esc(g.CASE_COUNT) +
      " · <strong>Threshold status:</strong> " +
      esc((threshold && threshold.THRESHOLD_STATUS) || g.threshold || "—") +
      " · <strong>Governance:</strong> " +
      esc((threshold && threshold.THRESHOLD_GOVERNANCE) || "PROVISIONAL") +
      "</p>" +
      "<p class=\"aiv-val-muted\">Provider coverage: " +
      esc(JSON.stringify(cov.PROVIDER || {})) +
      " · Language: " +
      esc(JSON.stringify(cov.LANGUAGE || {})) +
      " · Geography: " +
      esc(JSON.stringify(cov.GEOGRAPHY || {})) +
      " · Hard cases: " +
      esc(cov.HARD_CASE_COUNT || 0) +
      " · Last review: " +
      esc(g.lastValidatedAt || "—") +
      "</p>" +
      '<table class="aiv-val-table"><thead><tr><th>Measure</th><th>Value</th><th>n</th></tr></thead><tbody>' +
      "<tr><td>Entity Resolution Precision</td><td>" +
      esc(pct(g.ENTITY_RESOLUTION_PRECISION)) +
      "</td><td>" +
      esc(g.CASE_COUNT) +
      "</td></tr>" +
      "<tr><td>Entity Resolution Recall</td><td>" +
      esc(pct(g.ENTITY_RESOLUTION_RECALL)) +
      "</td><td>" +
      esc(g.CASE_COUNT) +
      "</td></tr>" +
      "<tr><td>Recommendation Classification Accuracy</td><td>" +
      esc(pct(g.RECOMMENDATION_CLASSIFICATION_ACCURACY)) +
      "</td><td>" +
      esc(g.CASE_COUNT) +
      "</td></tr>" +
      "<tr><td>Recommendation Precision</td><td>" +
      esc(pct(g.RECOMMENDATION_PRECISION)) +
      "</td><td>—</td></tr>" +
      "<tr><td>Recommendation Recall</td><td>" +
      esc(pct(g.RECOMMENDATION_RECALL)) +
      "</td><td>—</td></tr>" +
      "<tr><td>First Recommendation Accuracy</td><td>" +
      esc(pct(g.FIRST_RECOMMENDATION_ACCURACY)) +
      "</td><td>—</td></tr>" +
      "<tr><td>Question Status Accuracy</td><td>" +
      esc(pct(g.QUESTION_STATUS_ACCURACY)) +
      "</td><td>" +
      esc(g.QUESTION_STATUS_LABEL_COUNT || 0) +
      "</td></tr>" +
      "<tr><td>Citation Association Precision</td><td>" +
      esc(pct(g.CITATION_ASSOCIATION_PRECISION)) +
      "</td><td>" +
      esc(g.CITATION_ASSOCIATION_LABEL_COUNT || 0) +
      "</td></tr>" +
      "<tr><td>Citation Association Recall</td><td>" +
      esc(pct(g.CITATION_ASSOCIATION_RECALL)) +
      "</td><td>" +
      esc(g.CITATION_ASSOCIATION_LABEL_COUNT || 0) +
      "</td></tr>" +
      "</tbody></table>";
  }

  function fillTable(tableId, rowsHtml, colspan) {
    var tbody = document.querySelector("#" + tableId + " tbody");
    if (!tbody) return;
    tbody.innerHTML = rowsHtml || '<tr><td colspan="' + (colspan || 8) + '">No data</td></tr>';
  }

  function renderFreshness(summaryData, ops) {
    var host = document.getElementById("aivValFreshness");
    if (!host) return;
    var f = (summaryData && summaryData.freshness) || {};
    var m = (ops && ops.freshness) || f.monitoring || {};
    host.innerHTML =
      "<p class=\"aiv-val-muted\">" +
      "<strong>Last Validation Run:</strong> " +
      esc(f.lastValidationRun || summaryData.generatedAt || "—") +
      " · <strong>Latest Monitoring:</strong> " +
      esc(m.LATEST_MONITORING_DATE || "—") +
      " · <strong>Latest Publishable Batch:</strong> " +
      esc(m.LATEST_PUBLISHABLE_BATCH_DATE || "—") +
      " · <strong>Last OpenAI:</strong> " +
      esc(m.LAST_OPENAI_RUN || "—") +
      " · <strong>Last Gemini:</strong> " +
      esc(m.LAST_GEMINI_RUN || "—") +
      " · <strong>Last Perplexity:</strong> " +
      esc(m.LAST_PERPLEXITY_RUN || "—") +
      " · <strong>Last Claude:</strong> " +
      esc(m.LAST_CLAUDE_RUN || "—") +
      " · <strong>Last EN:</strong> " +
      esc(m.LAST_ENGLISH_RUN || "—") +
      " · <strong>Last ES:</strong> " +
      esc(m.LAST_SPANISH_RUN || "—") +
      "</p>";
  }

  function renderCoverage(ops) {
    if (!ops || !ops.coverage) return;
    var c = ops.coverage;
    renderCards("aivValCoverageCards", [
      { label: "Total Batches", value: c.TOTAL_MONITORING_BATCHES, detail: "Validated: " + c.TOTAL_VALIDATED_BATCHES },
      { label: "Total Runs", value: c.TOTAL_MONITORING_RUNS, detail: "Responses stored: " + c.TOTAL_RESPONSES_STORED },
      { label: "Prompts Attempted", value: c.TOTAL_PROMPTS_ATTEMPTED, detail: "Success: " + c.TOTAL_PROMPTS_SUCCESSFUL + " · Fail: " + c.TOTAL_PROMPTS_FAILED },
      { label: "Entities / Brands", value: c.TOTAL_CANONICAL_ENTITIES_COVERED + " / " + c.TOTAL_BRANDS_COVERED, detail: "Citations: " + c.TOTAL_CITATIONS_CAPTURED },
      { label: "Evidence Records", value: c.TOTAL_EVIDENCE_RECORDS, detail: "Publishable batches: " + c.TOTAL_PUBLISHABLE_BATCHES },
    ]);

    fillTable(
      "aivValProviderLangTable",
      (ops.providerLanguageMatrix || [])
        .map(function (r) {
          return (
            "<tr><td>" +
            esc(r.PROVIDER) +
            "</td><td>" +
            esc(r.ENGLISH_PROMPTS) +
            "</td><td>" +
            esc(r.SPANISH_PROMPTS) +
            "</td><td>" +
            esc(r.ENGLISH_SUCCESSFUL) +
            "</td><td>" +
            esc(r.SPANISH_SUCCESSFUL) +
            "</td><td>" +
            esc(na(r.LATEST_ENGLISH_RUN)) +
            "</td><td>" +
            esc(na(r.LATEST_SPANISH_RUN)) +
            "</td><td>" +
            esc(fmtCost(r.ESTIMATED_ENGLISH_COST)) +
            "</td><td>" +
            esc(fmtCost(r.ESTIMATED_SPANISH_COST)) +
            "</td></tr>"
          );
        })
        .join(""),
      9
    );

    fillTable(
      "aivValGeoTable",
      Object.keys(ops.geographyOps || {})
        .map(function (k) {
          var g = ops.geographyOps[k];
          return (
            "<tr><td>" +
            esc(k) +
            "</td><td>" +
            esc(g.BATCH_COUNT) +
            "</td><td>" +
            esc(g.PROMPTS_ATTEMPTED) +
            "</td><td>" +
            esc(g.PROMPTS_SUCCESSFUL) +
            "</td><td>" +
            esc((g.PROVIDERS || []).join(", ")) +
            "</td><td>" +
            esc((g.LANGUAGES || []).join(", ")) +
            "</td><td>" +
            esc(g.BRANDS_COVERED) +
            "</td><td>" +
            esc(na(g.FIRST_RUN_AT)) +
            "</td><td>" +
            esc(na(g.LATEST_RUN_AT)) +
            "</td><td>" +
            esc(fmtCost(g.ESTIMATED_COST)) +
            "</td></tr>"
          );
        })
        .join(""),
      10
    );
  }

  function renderOperations(ops) {
    if (!ops) return;
    var cost = ops.cost || {};
    var rel = ops.reliability || {};
    var yld = ops.yield || {};
    renderCards("aivValOpsCards", [
      {
        label: "Estimated Monitoring Cost",
        value: fmtCost(cost.TOTAL_ESTIMATED_MONITORING_COST),
        detail: "Source: " + (cost.COST_SOURCE || "—") + " · " + (cost.LABEL || "Estimated Cost"),
      },
      {
        label: "Call Success Rate",
        value: pct(rel.SUCCESS_RATE),
        detail: "Attempted " + (rel.CALLS_ATTEMPTED || 0) + " · Failed " + (rel.CALLS_FAILED || 0) + " · Retries " + (rel.RETRIES || 0),
      },
      {
        label: "Citation Yield",
        value: pct(yld.CITATION_YIELD),
        detail: "Operational ratio — not quality. Entity mention yield: " + pct(yld.ENTITY_MENTION_YIELD),
      },
      {
        label: "Batches Without Cost",
        value: cost.BATCHES_WITHOUT_COST != null ? cost.BATCHES_WITHOUT_COST : "—",
        detail: "With cost: " + (cost.BATCHES_WITH_COST || 0) + " · Missing never shown as $0",
      },
    ]);

    fillTable(
      "aivValProviderOpsTable",
      Object.keys(ops.providerOps || {})
        .map(function (k) {
          var p = ops.providerOps[k];
          return (
            "<tr><td>" +
            esc(k) +
            "</td><td>" +
            esc(p.BATCH_COUNT) +
            "</td><td>" +
            esc(p.RUN_COUNT) +
            "</td><td>" +
            esc(p.PROMPTS_ATTEMPTED) +
            "</td><td>" +
            esc(p.PROMPTS_SUCCESSFUL) +
            "</td><td>" +
            esc(p.PROMPTS_FAILED) +
            "</td><td>" +
            esc(pct(p.SUCCESS_RATE)) +
            "</td><td>" +
            esc(na(p.TOKENS_TOTAL)) +
            "</td><td>" +
            esc(fmtCost(p.ESTIMATED_COST)) +
            "</td><td>" +
            esc(na(p.LATEST_RUN_AT)) +
            "</td></tr>"
          );
        })
        .join(""),
      10
    );

    fillTable(
      "aivValLangOpsTable",
      Object.keys(ops.languageOps || {})
        .map(function (k) {
          var p = ops.languageOps[k];
          return (
            "<tr><td>" +
            esc(k) +
            "</td><td>" +
            esc(p.PROMPTS_ATTEMPTED) +
            "</td><td>" +
            esc(p.PROMPTS_SUCCESSFUL) +
            "</td><td>" +
            esc(p.PROMPTS_FAILED) +
            "</td><td>" +
            esc(p.RUN_COUNT) +
            "</td><td>" +
            esc(p.BATCH_COUNT) +
            "</td><td>" +
            esc((p.PROVIDERS || []).join(", ")) +
            "</td><td>" +
            esc((p.GEOGRAPHIES || []).join(", ")) +
            "</td><td>" +
            esc(fmtCost(p.ESTIMATED_COST)) +
            "</td><td>" +
            esc(na(p.LATEST_RUN_AT)) +
            "</td></tr>"
          );
        })
        .join(""),
      10
    );

    inventoryRows = ops.inventory || [];
    populateInventoryFilters(inventoryRows);
    renderInventory();
  }

  function populateInventoryFilters(rows) {
    function fill(selId, values, label) {
      var sel = document.getElementById(selId);
      if (!sel) return;
      var cur = sel.value;
      sel.innerHTML = '<option value="">' + label + ": All</option>";
      values.forEach(function (v) {
        if (!v) return;
        var opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        sel.appendChild(opt);
      });
      sel.value = cur || "";
      sel.onchange = renderInventory;
    }
    var providers = {};
    var geos = {};
    var langs = {};
    var statuses = {};
    rows.forEach(function (r) {
      if (r.PROVIDER) providers[r.PROVIDER] = 1;
      if (r.GEOGRAPHY) geos[r.GEOGRAPHY] = 1;
      if (r.LANGUAGE) langs[r.LANGUAGE] = 1;
      if (r.VALIDATION_STATUS) statuses[r.VALIDATION_STATUS] = 1;
    });
    fill("aivFiltProvider", Object.keys(providers).sort(), "Provider");
    fill("aivFiltGeo", Object.keys(geos).sort(), "Geography");
    fill("aivFiltLang", Object.keys(langs).sort(), "Language");
    fill("aivFiltStatus", Object.keys(statuses).sort(), "Validation");
  }

  function renderInventory() {
    var p = (document.getElementById("aivFiltProvider") || {}).value || "";
    var g = (document.getElementById("aivFiltGeo") || {}).value || "";
    var l = (document.getElementById("aivFiltLang") || {}).value || "";
    var s = (document.getElementById("aivFiltStatus") || {}).value || "";
    var filtered = inventoryRows.filter(function (r) {
      if (p && r.PROVIDER !== p) return false;
      if (g && r.GEOGRAPHY !== g) return false;
      if (l && r.LANGUAGE !== l) return false;
      if (s && r.VALIDATION_STATUS !== s) return false;
      return true;
    });
    fillTable(
      "aivValInventoryTable",
      filtered
        .map(function (r) {
          return (
            "<tr><td><button type=\"button\" class=\"aiv-val-link\" data-batch=\"" +
            esc(r.BATCH_ID) +
            "\">" +
            esc(r.BATCH_ID) +
            "</button></td><td>" +
            esc(r.PROVIDER) +
            "</td><td>" +
            esc(na(r.MODEL)) +
            "</td><td>" +
            esc(na(r.GEOGRAPHY)) +
            "</td><td>" +
            esc(na(r.LANGUAGE)) +
            "</td><td>" +
            esc(r.PROMPT_COUNT) +
            "</td><td>" +
            esc(r.SUCCESS_COUNT) +
            "</td><td>" +
            esc(r.FAIL_COUNT) +
            "</td><td>" +
            esc(r.CITATIONS) +
            "</td><td>" +
            esc(fmtCost(r.ESTIMATED_COST)) +
            "</td><td>" +
            esc(na(r.EXECUTED_AT)) +
            "</td><td>" +
            esc(na(r.VALIDATED_AT)) +
            "</td><td>" +
            esc(r.VALIDATION_STATUS) +
            "</td><td>" +
            esc(r.PUBLISHABLE) +
            "</td></tr>"
          );
        })
        .join(""),
      14
    );
    var tbody = document.querySelector("#aivValInventoryTable tbody");
    if (tbody) {
      tbody.querySelectorAll("[data-batch]").forEach(function (btn) {
        btn.addEventListener("click", function () {
          openBatchDetail(btn.getAttribute("data-batch"));
        });
      });
    }
  }

  async function openBatchDetail(batchId) {
    var host = document.getElementById("aivValBatchDetail");
    if (!host) return;
    host.textContent = "Loading " + batchId + "…";
    var res = await fetchJson("/api/ai-intelligence/validation/batches/" + encodeURIComponent(batchId));
    if (!res.data || res.data.ok !== true) {
      host.textContent = "Could not load batch detail.";
      return;
    }
    var b = res.data.batch || {};
    var inv = b.inventory || {};
    host.innerHTML =
      "<pre style=\"white-space:pre-wrap;font-size:12px;color:#c5d0f0;\">" +
      esc(
        JSON.stringify(
          {
            BATCH_ID: b.BATCH_ID || inv.BATCH_ID,
            PROVIDER: b.PROVIDER || inv.PROVIDER,
            MODEL: inv.MODEL,
            GEOGRAPHY: b.GEOGRAPHY || inv.GEOGRAPHY,
            LANGUAGE: b.LANGUAGE || inv.LANGUAGE,
            PROMPT_COUNT: inv.PROMPT_COUNT,
            SUCCESS_COUNT: inv.SUCCESS_COUNT,
            FAIL_COUNT: inv.FAIL_COUNT,
            ESTIMATED_COST: inv.ESTIMATED_COST,
            COST_LABEL: inv.ESTIMATED_COST != null ? "Estimated Cost" : "Not Available",
            EXECUTED_AT: inv.EXECUTED_AT,
            VALIDATED_AT: inv.VALIDATED_AT || b.validatedAt,
            VALIDATION_STATUS: b.VALIDATION_STATUS || inv.VALIDATION_STATUS,
            PUBLISHABLE: b.PUBLISHABLE != null ? b.PUBLISHABLE : inv.PUBLISHABLE,
            note: "Raw provider responses are not exposed by default.",
          },
          null,
          2
        )
      ) +
      "</pre>";
  }

  async function boot() {
    setState("Loading validation results…", false);
    try {
      var summaryRes = await fetchJson("/api/ai-intelligence/validation/summary?refresh=1");
      var fail = classifySummaryFailure(summaryRes.status, summaryRes.data);
      if (fail) {
        setState(fail.message, true);
        return;
      }

      var gatesRes = await fetchJson("/api/ai-intelligence/validation/gates");
      var classRes = await fetchJson("/api/ai-intelligence/validation/classification");
      var batchesRes = await fetchJson("/api/ai-intelligence/validation/batches");
      var issuesRes = await fetchJson("/api/ai-intelligence/validation/issues");
      var varRes = await fetchJson("/api/ai-intelligence/validation/variability");
      var opsRes = await fetchJson("/api/ai-intelligence/validation/operations");

      document.getElementById("aivValState").hidden = true;
      show("aivValTopSummary");
      show("aivValDataTrust");
      show("aivValCoverage");
      show("aivValOperations");
      show("aivValFreshness");
      show("aivValBatches");
      show("aivValManual");
      show("aivValVariability");
      show("aivValMethod");

      renderTopSummary(summaryRes.data.topSummary);
      renderFreshness(summaryRes.data, opsRes.data && opsRes.data.monitoringOperations);
      renderCards("aivValCards", summaryRes.data.systemCards);
      renderGolden(
        classRes.data && classRes.data.goldenSet,
        (classRes.data && classRes.data.classificationThreshold) ||
          summaryRes.data.classificationThreshold
      );

      if (opsRes.data && opsRes.data.ok && opsRes.data.monitoringOperations) {
        renderCoverage(opsRes.data.monitoringOperations);
        renderOperations(opsRes.data.monitoringOperations);
      }

      fillTable(
        "aivValGatesTable",
        ((gatesRes.data && gatesRes.data.gates) || [])
          .map(function (g) {
            return (
              "<tr><td>" +
              esc(g.name) +
              "</td><td>" +
              esc(g.status) +
              "</td><td>" +
              esc(g.measuredResult) +
              "</td><td>" +
              esc(g.requirement) +
              "</td><td>" +
              esc(g.failures) +
              "</td></tr>"
            );
          })
          .join("")
      );

      fillTable(
        "aivValBatchesTable",
        ((batchesRes.data && batchesRes.data.batches) || [])
          .map(function (b) {
            return (
              "<tr><td>" +
              esc(b.BATCH_ID) +
              "</td><td>" +
              esc(b.PROVIDER) +
              "</td><td>" +
              esc(b.GEOGRAPHY) +
              "</td><td>" +
              esc(b.LANGUAGE) +
              "</td><td>" +
              esc(b.VALIDATION_STATUS) +
              "</td><td>" +
              esc(b.PUBLISHABLE) +
              "</td><td>" +
              esc(b.ISSUES) +
              "</td></tr>"
            );
          })
          .join("")
      );

      var issues = (issuesRes.data && issuesRes.data.issues) || [];
      fillTable(
        "aivValIssuesTable",
        issues.length
          ? issues
              .slice(0, 200)
              .map(function (i) {
                return (
                  "<tr><td>" +
                  esc(i.type) +
                  "</td><td>" +
                  esc(i.batchId) +
                  "</td><td>" +
                  esc((i.metricId || "") + " " + (i.entityId || "")) +
                  "</td><td>" +
                  esc(i.expected) +
                  "</td><td>" +
                  esc(i.actual) +
                  "</td></tr>"
                );
              })
              .join("")
          : '<tr><td colspan="5">No open validation issues in the latest run.</td></tr>',
        5
      );

      var manual = (varRes.data && varRes.data.manualReview) || {};
      var hr = (summaryRes.data && summaryRes.data.humanReview) || (varRes.data && varRes.data.humanReview) || {};
      var hrProg = hr.progress || {};
      document.getElementById("aivValManualBody").innerHTML =
        '<p class="aiv-val-muted">' +
        esc(manual.status || "MANUAL_SPOT_CHECK_PENDING") +
        " · Spot-check sample: " +
        esc(manual.reviewSampleSize || 0) +
        " · Reviewed: " +
        esc(manual.reviewed || 0) +
        "</p>" +
        "<p><strong>Golden Set Human Review</strong><br>" +
        "Candidates: " +
        esc(hrProg.TOTAL != null ? hrProg.TOTAL : "—") +
        " · Reviewed: " +
        esc(hrProg.REVIEWED != null ? hrProg.REVIEWED : 0) +
        " · Confirmed: " +
        esc(hrProg.CONFIRMED != null ? hrProg.CONFIRMED : 0) +
        " · Corrected: " +
        esc(hrProg.CORRECTED != null ? hrProg.CORRECTED : 0) +
        " · Deferred: " +
        esc(hrProg.DEFERRED != null ? hrProg.DEFERRED : 0) +
        " · Remaining: " +
        esc(hrProg.REMAINING != null ? hrProg.REMAINING : "—") +
        "<br>v1 size: " +
        esc(hr.v1Size != null ? hr.v1Size : 65) +
        " · v2 size: " +
        esc(hr.v2Size != null ? hr.v2Size : 0) +
        " · " +
        esc(hr.note || "Unreviewed ≠ approved.") +
        "</p>";

      var variability = (varRes.data && varRes.data.variability) || {};
      document.getElementById("aivValVariabilityBody").innerHTML =
        '<p class="aiv-val-muted">' +
        esc(variability.note || variability.status || "Insufficient comparable runs") +
        "</p>";

      document.getElementById("aivValMethodologyNote").textContent =
        summaryRes.data.methodologyNote || "";
      document.getElementById("aivValOpsMethodologyNote").textContent =
        summaryRes.data.operationalMethodologyNote ||
        (opsRes.data && opsRes.data.operationalMethodologyNote) ||
        "";
    } catch (err) {
      setState("SERVER_ERROR: " + (err && err.message ? err.message : String(err)), true);
      if (window.console && console.error) console.error("[ValidationScorecard]", err);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
