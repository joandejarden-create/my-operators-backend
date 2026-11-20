/**
 * Brand Explorer Batch Review — Factory 2.0 portfolio UI.
 */
(function () {
  "use strict";

  var state = {
    rows: [],
    summary: null,
    selected: {},
  };

  function $(id) {
    return document.getElementById(id);
  }

  function toast(msg, kind) {
    var el = $("bebrToast");
    if (!el) return;
    el.hidden = false;
    el.className = "bebr-toast" + (kind ? " " + kind : "");
    el.textContent = msg;
  }

  function shortHash(h) {
    if (!h) return "—";
    return String(h).slice(0, 12) + "…";
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderSummary(summary) {
    var el = $("bebrSummary");
    if (!el || !summary) return;
    var items = [
      ["Queued", summary.totalQueued],
      ["Ready", summary.readyForReview],
      ["Remediation", summary.needsRemediation],
      ["In progress", summary.researchInProgress],
      ["Approved", summary.approved],
      ["Published", summary.published],
      ["Blocked", summary.blocked],
      ["Protected", summary.skippedProtected],
      ["Spend $", summary.researchSpendUsdTotal],
      ["Exp. writes", summary.expectedPublicationWritesTotal],
    ];
    el.innerHTML = items
      .map(function (pair) {
        return (
          '<div class="bebr-stat"><strong>' +
          escapeHtml(pair[1]) +
          "</strong><span>" +
          escapeHtml(pair[0]) +
          "</span></div>"
        );
      })
      .join("");
  }

  function updateApproveButton() {
    var btn = $("bebrApprove");
    if (!btn) return;
    var n = Object.keys(state.selected).filter(function (k) {
      return state.selected[k];
    }).length;
    btn.disabled = n === 0;
    btn.textContent =
      n === 0 ? "Record batch approval" : "Record batch approval (" + n + ")";
  }

  function renderTable(rows) {
    var body = $("bebrBody");
    if (!body) return;
    if (!rows.length) {
      body.innerHTML =
        '<tr><td colspan="11">No brands match this filter. Build/process the Factory 2.0 queue first.</td></tr>';
      return;
    }
    body.innerHTML = rows
      .map(function (r) {
        var canSelect = r.factoryStatus === "READY_FOR_REVIEW" && r.candidateHash;
        var checked = state.selected[r.brandId] ? " checked" : "";
        var gaps = Array.isArray(r.researchGaps) ? r.researchGaps.slice(0, 3).join("; ") : "";
        return (
          "<tr>" +
          "<td>" +
          (canSelect
            ? '<input type="checkbox" data-bebr-id="' +
              escapeHtml(r.brandId) +
              '" data-bebr-hash="' +
              escapeHtml(r.candidateHash || "") +
              '"' +
              checked +
              " />"
            : "") +
          "</td>" +
          "<td><strong>" +
          escapeHtml(r.brandName) +
          "</strong><br><code>" +
          escapeHtml(r.brandId) +
          "</code></td>" +
          "<td>" +
          escapeHtml(r.parentCompany || "—") +
          "</td>" +
          '<td><span class="bebr-status ' +
          escapeHtml(r.factoryStatus || "") +
          '">' +
          escapeHtml(r.factoryStatus || "—") +
          "</span></td>" +
          "<td>" +
          escapeHtml(r.tenTabQualityScore == null ? "—" : r.tenTabQualityScore) +
          "</td>" +
          '<td class="bebr-gaps">' +
          escapeHtml(gaps || "—") +
          "</td>" +
          "<td>" +
          escapeHtml(r.dataFreshness || "—") +
          "</td>" +
          "<td><code title=\"" +
          escapeHtml(r.candidateHash || "") +
          '">' +
          escapeHtml(shortHash(r.candidateHash)) +
          "</code></td>" +
          "<td>" +
          escapeHtml(r.expectedAirtableWrites == null ? "—" : r.expectedAirtableWrites) +
          "</td>" +
          "<td>" +
          escapeHtml(r.researchSpendUsd == null ? "0" : r.researchSpendUsd) +
          "</td>" +
          '<td class="bebr-links">' +
          '<a href="' +
          escapeHtml(r.compareStudioUrl || "#") +
          '" target="_blank" rel="noopener">Compare</a>' +
          '<a href="' +
          escapeHtml(r.proposedProductUrl || "#") +
          '" target="_blank" rel="noopener">Proposed</a>' +
          "</td>" +
          "</tr>"
        );
      })
      .join("");

    body.querySelectorAll("input[type=checkbox][data-bebr-id]").forEach(function (cb) {
      cb.addEventListener("change", function () {
        var id = cb.getAttribute("data-bebr-id");
        var hash = cb.getAttribute("data-bebr-hash");
        if (cb.checked) state.selected[id] = hash;
        else delete state.selected[id];
        updateApproveButton();
      });
    });
  }

  async function loadPortfolio() {
    var filter = ($("bebrFilter") && $("bebrFilter").value) || "";
    var url =
      "/api/brand-explorer-batch-review/portfolio" +
      (filter ? "?status=" + encodeURIComponent(filter) : "");
    var res = await fetch(url, { credentials: "include" });
    var json = await res.json().catch(function () {
      return {};
    });
    if (!res.ok || !json.ok) {
      toast(json.error || "Failed to load portfolio", "error");
      return;
    }
    state.rows = json.rows || [];
    state.summary = json.summary;
    renderSummary(state.summary);
    renderTable(state.rows);
    updateApproveButton();
    toast(
      "Loaded " +
        state.rows.length +
        " brands · ready " +
        (state.summary && state.summary.readyForReview) +
        " · remediation " +
        (state.summary && state.summary.needsRemediation),
      "ok"
    );
  }

  async function approveSelected() {
    var selections = Object.keys(state.selected).map(function (brandId) {
      return { brandId: brandId, candidateHash: state.selected[brandId] };
    });
    if (!selections.length) return;
    if (
      !window.confirm(
        "Record approval for " +
          selections.length +
          " brand(s)?\n\nThis binds per-candidate tokens only.\nProduction Airtable Presentation will NOT be written."
      )
    ) {
      return;
    }
    var res = await fetch("/api/brand-explorer-batch-review/approve", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ selections: selections }),
    });
    var json = await res.json().catch(function () {
      return {};
    });
    if (!res.ok || json.failedCount > 0) {
      toast(
        "Approval partial/failed: approved=" +
          (json.approvedCount || 0) +
          " failed=" +
          (json.failedCount || "?") +
          " — " +
          (json.error || JSON.stringify(json.results || []).slice(0, 180)),
        "error"
      );
    } else {
      toast(
        "Recorded approval " +
          (json.approvalId || "") +
          " for " +
          json.approvedCount +
          " brand(s). Production Presentation unchanged.",
        "ok"
      );
      state.selected = {};
    }
    await loadPortfolio();
  }

  function wire() {
    $("bebrRefresh") && $("bebrRefresh").addEventListener("click", loadPortfolio);
    $("bebrFilter") && $("bebrFilter").addEventListener("change", loadPortfolio);
    $("bebrApprove") && $("bebrApprove").addEventListener("click", approveSelected);
    $("bebrClear") &&
      $("bebrClear").addEventListener("click", function () {
        state.selected = {};
        renderTable(state.rows);
        updateApproveButton();
      });
    $("bebrSelectReady") &&
      $("bebrSelectReady").addEventListener("click", function () {
        state.selected = {};
        (state.rows || []).forEach(function (r) {
          if (r.factoryStatus === "READY_FOR_REVIEW" && r.candidateHash) {
            state.selected[r.brandId] = r.candidateHash;
          }
        });
        renderTable(state.rows);
        updateApproveButton();
      });
  }

  async function boot() {
    wire();
    await loadPortfolio();
  }

  window.BrandExplorerBatchReviewBoot = boot;

  async function initAccess() {
    var loadingEl = $("supportGateLoading");
    var contentEl = $("supportGateContent");
    var deniedEl = $("supportGateDenied");

    if (window.SupportAdminGate) {
      window.SupportAdminGate.showPageLoading("supportGateLoading", "Verifying admin access…");
    }

    if (!window.SupportAdminGate) {
      if (loadingEl) loadingEl.hidden = true;
      if (contentEl) contentEl.hidden = true;
      if (deniedEl) {
        deniedEl.hidden = false;
        var msg = deniedEl.querySelector("[data-gate-message]");
        if (msg) msg.textContent = "Unable to verify admin access. Refresh and try again.";
      }
      return;
    }

    var allowed = await window.SupportAdminGate.requireInternalRunbook({
      contentId: "supportGateContent",
      onAllowed: function () {},
    });
    if (!allowed) return;
    if (contentEl) contentEl.hidden = false;
    window.SupportAdminGate.hidePageLoading(loadingEl);
    await boot();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAccess);
  } else {
    initAccess();
  }
})();
