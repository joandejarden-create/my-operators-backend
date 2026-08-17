/**
 * Presence validation review UI — keyboard Y/N/I/D + filter-aware export.
 * ChatGPT export drafts are assistance only — never auto-label.
 */
(function () {
  "use strict";

  var cases = [];
  var idx = 0;
  var summary = null;
  var exportLimit = "25";

  function apiBase() {
    if (window.DEALALITY_API_BASE) return String(window.DEALALITY_API_BASE).replace(/\/$/, "");
    return "";
  }

  async function getJwt() {
    if (window.__dealalityMemberstackJwt) return window.__dealalityMemberstackJwt;
    if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.getMemberstackJwtWhenReady) {
      return window.DealalityMemberstackAuth.getMemberstackJwtWhenReady(2000);
    }
    return null;
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setState(msg, isError) {
    var el = document.getElementById("state");
    if (!el) return;
    el.hidden = !msg;
    el.textContent = msg || "";
    el.className = "state" + (isError ? " error" : "");
  }

  async function fetchJson(url, opts) {
    var jwt = await getJwt();
    var headers = { Accept: "application/json" };
    if (jwt) headers.Authorization = "Bearer " + jwt;
    if (opts && opts.body && !(opts.headers && opts.headers["Content-Type"])) {
      headers["Content-Type"] = "application/json";
    }
    if (opts && opts.headers) {
      Object.keys(opts.headers).forEach(function (k) {
        headers[k] = opts.headers[k];
      });
    }
    var res;
    try {
      res = await fetch(apiBase() + url, Object.assign({}, opts || {}, {
        credentials: "same-origin",
        headers: headers,
      }));
    } catch (err) {
      return {
        status: 0,
        data: {
          ok: false,
          message: err && err.message ? err.message : "Network error",
        },
      };
    }
    var data = null;
    try {
      data = await res.json();
    } catch (e) {
      data = { ok: false, message: "Invalid JSON" };
    }
    if (res.status === 401) {
      data = data || {};
      data.ok = false;
      data.message =
        data.message ||
        "Send Authorization: Bearer <Memberstack member JWT>. Log in to Dealality, then reload this page.";
    }
    return { status: res.status, data: data };
  }

  function reviewer() {
    var el = document.getElementById("reviewer");
    var v = el && el.value ? el.value.trim() : "";
    if (!v) {
      try {
        v = localStorage.getItem("presval_reviewer") || "";
      } catch (e) {}
    }
    return v;
  }

  function rememberReviewer() {
    var v = reviewer();
    if (!v) return;
    try {
      localStorage.setItem("presval_reviewer", v);
    } catch (e) {}
  }

  function filterParams(extra) {
    var status = document.getElementById("fStatus").value || "pending";
    var provider = document.getElementById("fProvider").value;
    var language = document.getElementById("fLanguage").value;
    var geography = document.getElementById("fGeo").value;
    var candidateType = document.getElementById("fType").value;
    var assisted = document.getElementById("fAssisted")
      ? document.getElementById("fAssisted").value
      : "all";
    var q = new URLSearchParams();
    q.set("status", status === "all" ? "all" : status);
    if (provider) q.set("provider", provider);
    if (language) q.set("language", language);
    if (geography) q.set("geography", geography);
    if (candidateType) q.set("candidateType", candidateType);
    var primary = document.getElementById("fPrimary").value;
    q.set("primary", primary === "0" ? "0" : "1");
    if (assisted && assisted !== "all") q.set("assisted", assisted);
    if (extra) {
      Object.keys(extra).forEach(function (k) {
        if (extra[k] != null) q.set(k, String(extra[k]));
      });
    }
    return q.toString();
  }

  function query() {
    return filterParams();
  }

  function renderProgress() {
    var el = document.getElementById("progress");
    if (!el || !summary) return;
    el.innerHTML =
      "<span>Reviewed pairs <strong>" +
      esc(summary.ReviewedCandidatePairs != null ? summary.ReviewedCandidatePairs : summary.Reviewed) +
      "</strong></span>" +
      "<span>Unique responses reviewed <strong>" +
      esc(summary.UniqueResponsesReviewed != null ? summary.UniqueResponsesReviewed : "—") +
      "</strong></span>" +
      "<span>Present <strong>" +
      esc(summary.Present) +
      "</strong></span>" +
      "<span>Not Present <strong>" +
      esc(summary.NotPresent) +
      "</strong></span>" +
      "<span>Invalid <strong>" +
      esc(summary.Invalid) +
      "</strong></span>" +
      "<span>Deferred <strong>" +
      esc(summary.Deferred) +
      "</strong></span>" +
      "<span>Remaining <strong>" +
      esc(summary.Remaining) +
      "</strong></span>" +
      "<span>Assisted <strong>" +
      esc(summary.TOTAL_ASSISTED != null ? summary.TOTAL_ASSISTED : 0) +
      "</strong></span>" +
      "<span>Accepted <strong>" +
      esc(summary.ACCEPTED != null ? summary.ACCEPTED : 0) +
      "</strong></span>" +
      "<span>Changed <strong>" +
      esc(summary.CHANGED != null ? summary.CHANGED : 0) +
      "</strong></span>" +
      "<span>Assisted remaining <strong>" +
      esc(summary.REMAINING_ASSISTED != null ? summary.REMAINING_ASSISTED : 0) +
      "</strong></span>";
  }

  function fillFilterOptions(list) {
    function uniq(key) {
      var s = {};
      list.forEach(function (c) {
        if (c[key]) s[c[key]] = true;
      });
      return Object.keys(s).sort();
    }
    function fill(selId, values, label) {
      var sel = document.getElementById(selId);
      var cur = sel.value;
      sel.innerHTML = "<option value=\"\">" + label + "</option>";
      values.forEach(function (v) {
        var o = document.createElement("option");
        o.value = v;
        o.textContent = v;
        sel.appendChild(o);
      });
      sel.value = cur;
    }
    fill("fProvider", uniq("provider"), "Provider");
    fill("fLanguage", uniq("language"), "Language");
    fill("fGeo", uniq("geography"), "Geography");
  }

  function renderCase() {
    var root = document.getElementById("card");
    if (!cases.length) {
      root.innerHTML = '<div class="empty">No cases in this filter. Change filters or finish review.</div>';
      return;
    }
    if (idx >= cases.length) idx = 0;
    var c = cases[idx];
    var prop = c.assistedProposal;
    var assistedHtml = "";
    if (prop) {
      assistedHtml =
        '<div class="assisted-box' +
        (c.assistedDisagreesWithSystem ? " disagree" : "") +
        '">' +
        '<div class="badge">ASSISTED PROPOSAL — NOT HUMAN GROUND TRUTH</div>' +
        "<div><strong>ChatGPT proposed decision:</strong> " +
        esc(prop.proposedDecision) +
        "</div>" +
        "<div><strong>Proposed notes:</strong> " +
        esc(prop.proposedNotes || "(none)") +
        "</div>" +
        '<div class="triptych">' +
        "<div><strong>SYSTEM SUGGESTION</strong>" +
        esc(c.assistance && c.assistance.SYSTEM_PRESENCE_SUGGESTION) +
        " → " +
        esc(c.systemSuggestionMapped || "—") +
        "</div>" +
        "<div><strong>CHATGPT PROPOSAL</strong>" +
        esc(prop.proposedDecision) +
        "</div>" +
        "<div><strong>HUMAN FINAL</strong>" +
        esc((c.humanReview && c.humanReview.action) || "(pending)") +
        "</div>" +
        "</div>" +
        "</div>";
    }

    var actionsHtml =
      '<div class="actions">' +
      (prop
        ? '<button class="primary" data-act="ACCEPT">A · Accept proposal</button>'
        : "") +
      '<button data-act="PRESENT">Y · Present</button>' +
      '<button data-act="NOT_PRESENT">N · Not Present</button>' +
      '<button class="warn" data-act="INVALID">I · Invalid</button>' +
      '<button data-act="DEFER">D · Defer</button>' +
      "</div>";

    root.innerHTML =
      '<div class="case">' +
      '<div class="meta">' +
      '<div class="entity">' +
      esc(c.canonicalEntityName) +
      "</div>" +
      '<div class="row">' +
      esc(c.provider) +
      " · " +
      esc(c.language) +
      " · " +
      esc(c.geography) +
      " · " +
      esc(c.candidateType) +
      " · pairs/response " +
      esc(c.sourceResponseCandidateCount != null ? c.sourceResponseCandidateCount : "—") +
      "</div>" +
      '<div class="row">' +
      esc(c.caseId) +
      " · resp " +
      esc(c.sourceResponseId || c.responseId) +
      " · " +
      (idx + 1) +
      "/" +
      cases.length +
      "</div>" +
      "</div>" +
      '<div class="prompt"><strong>Prompt</strong><br>' +
      esc(c.promptText) +
      "</div>" +
      '<pre class="response">' +
      esc(c.rawText) +
      "</pre>" +
      assistedHtml +
      '<div class="guidance"><strong>Does this specific canonical entity actually appear?</strong>' +
      "<ul><li>A = ACCEPT PROPOSAL</li><li>Y = PRESENT</li><li>N = NOT PRESENT</li><li>I = INVALID SUBJECT</li><li>D = DEFER</li></ul></div>" +
      actionsHtml +
      '<div class="assist">System suggestion (assistance only): ' +
      esc(c.assistance && c.assistance.SYSTEM_PRESENCE_SUGGESTION) +
      " — " +
      esc(c.assistance && c.assistance.rationale) +
      "</div>" +
      "</div>";

    root.querySelectorAll("button[data-act]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var act = btn.getAttribute("data-act");
        if (act === "ACCEPT") acceptProposal();
        else decide(act);
      });
    });
  }

  function setExportMeta(caseCount, uniqueCount) {
    var el = document.getElementById("exportMeta");
    if (!el) return;
    el.textContent =
      "Cases in export: " +
      (caseCount != null ? caseCount : "—") +
      " · Unique responses: " +
      (uniqueCount != null ? uniqueCount : "—");
  }

  async function refreshExportPreview(mode) {
    var m = mode || "filter";
    var r = await fetchJson(
      "/api/ai-intelligence/presence-validation-review/export/preview?" +
        filterParams({ mode: m, limit: exportLimit })
    );
    if (!r.data || !r.data.ok) {
      setExportMeta("—", "—");
      return;
    }
    setExportMeta(r.data.caseCount, r.data.uniqueResponseCount);
  }

  function downloadBlob(filename, text, mime) {
    var blob = new Blob([text], { type: mime || "text/plain;charset=utf-8" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 1000);
  }

  async function runExport(mode, format) {
    setState("Preparing export…");
    var qs = filterParams({
      mode: mode,
      limit: exportLimit,
      format: format,
      download: "0",
    });
    var r = await fetchJson("/api/ai-intelligence/presence-validation-review/export?" + qs);
    if (!r.data || !r.data.ok) {
      setState((r.data && r.data.message) || "Export failed (" + r.status + ")", true);
      return;
    }
    var stamp = new Date().toISOString().replace(/[:.]/g, "-");
    if (format === "md" || format === "markdown") {
      var md = r.data.markdown || "";
      downloadBlob("presence-validation-review-" + stamp + ".md", md, "text/markdown;charset=utf-8");
      setExportMeta(r.data.caseCount, r.data.uniqueResponseCount);
      setState("Exported " + (r.data.caseCount || 0) + " cases (Markdown)");
      return;
    }
    var payload = r.data.export || r.data;
    downloadBlob(
      "presence-validation-review-" + stamp + ".json",
      JSON.stringify(payload, null, 2) + "\n",
      "application/json;charset=utf-8"
    );
    setExportMeta(payload.caseCount, payload.uniqueResponseCount);
    setState("Exported " + (payload.caseCount || 0) + " cases (JSON)");
  }

  async function loadQueue() {
    setState("Loading…");
    var r = await fetchJson("/api/ai-intelligence/presence-validation-review/queue?" + query());
    if (!r.data || !r.data.ok) {
      setState((r.data && r.data.message) || "Failed to load", true);
      return;
    }
    cases = r.data.cases || [];
    summary = r.data.summary;
    idx = 0;
    fillFilterOptions(cases.length ? cases : []);
    if (
      !document.getElementById("fProvider").options.length ||
      document.getElementById("fProvider").options.length <= 1
    ) {
      var all = await fetchJson(
        "/api/ai-intelligence/presence-validation-review/queue?status=all&primary=0"
      );
      if (all.data && all.data.ok) fillFilterOptions(all.data.cases || []);
    }
    renderProgress();
    renderCase();
    setState("");
    refreshExportPreview("filter");
  }

  async function decide(action, opts) {
    rememberReviewer();
    var rev = reviewer();
    if (!rev) {
      setState("Enter reviewer email first", true);
      document.getElementById("reviewer").focus();
      return;
    }
    if (!cases[idx]) return;
    var caseId = cases[idx].caseId;
    var prop = cases[idx].assistedProposal;
    setState("Saving…");
    var body = {
      caseId: caseId,
      action: action,
      reviewer: rev,
      notes: (opts && opts.notes) || (prop && prop.proposedNotes) || null,
      acceptAssisted: !!(opts && opts.acceptAssisted),
    };
    if (opts && opts.humanAction) body.humanAction = opts.humanAction;
    var r = await fetchJson("/api/ai-intelligence/presence-validation-review/decide", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.data || !r.data.ok) {
      setState((r.data && r.data.message) || "Save failed", true);
      return;
    }
    summary = r.data.summary;
    renderProgress();
    cases.splice(idx, 1);
    if (idx >= cases.length) idx = 0;
    renderCase();
    setState("Saved " + action + (opts && opts.acceptAssisted ? " (accepted proposal)" : ""));
    refreshExportPreview("filter");
  }

  async function acceptProposal() {
    var c = cases[idx];
    if (!c || !c.assistedProposal) {
      setState("No assisted proposal on this case", true);
      return;
    }
    await decide(c.assistedProposal.proposedDecision, {
      acceptAssisted: true,
      humanAction: "ACCEPTED_ASSISTED_PROPOSAL",
      notes: c.assistedProposal.proposedNotes || null,
    });
  }

  document.addEventListener("keydown", function (e) {
    if (
      e.target &&
      (e.target.tagName === "INPUT" || e.target.tagName === "SELECT" || e.target.tagName === "TEXTAREA")
    ) {
      return;
    }
    var k = e.key.toLowerCase();
    if (k === "a") acceptProposal();
    else if (k === "y") decide("PRESENT");
    else if (k === "n") decide("NOT_PRESENT");
    else if (k === "i") decide("INVALID");
    else if (k === "d") decide("DEFER");
  });

  ["fStatus", "fProvider", "fLanguage", "fGeo", "fType", "fPrimary", "fAssisted"].forEach(function (id) {
    var el = document.getElementById(id);
    if (el) el.addEventListener("change", loadQueue);
  });

  document.querySelectorAll(".limitBtn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      exportLimit = btn.getAttribute("data-limit") || "25";
      document.querySelectorAll(".limitBtn").forEach(function (b) {
        b.classList.toggle("selected", b === btn);
      });
      refreshExportPreview("filter");
    });
  });

  document.getElementById("btnExportPendingMd").addEventListener("click", function () {
    runExport("pending", "md");
  });
  document.getElementById("btnExportPendingJson").addEventListener("click", function () {
    runExport("pending", "json");
  });
  document.getElementById("btnExportFilterMd").addEventListener("click", function () {
    runExport("filter", "md");
  });
  document.getElementById("btnExportFilterJson").addEventListener("click", function () {
    runExport("filter", "json");
  });

  var bulkClassification = null;

  function renderBulkSummary(cls) {
    var el = document.getElementById("bulkSummary");
    if (!el || !cls) return;
    el.innerHTML =
      "Bulk-approval eligible: <strong>" +
      esc(cls.BULK_APPROVAL_ELIGIBLE) +
      "</strong> · Manual review required: <strong>" +
      esc(cls.MANUAL_REVIEW_REQUIRED) +
      "</strong> · Already reviewed (in scope): <strong>" +
      esc(cls.ALREADY_REVIEWED) +
      "</strong> · Assisted remaining (summary): <strong>" +
      esc(summary && summary.REMAINING_ASSISTED != null ? summary.REMAINING_ASSISTED : "—") +
      "</strong>" +
      (cls.scope && cls.scope.SCOPE_NOTE
        ? "<br><span style=\"opacity:.8\">Scope: " + esc(cls.scope.SCOPE_NOTE) + "</span>"
        : "");
  }

  function renderBulkDetail(cls) {
    var el = document.getElementById("bulkDetail");
    if (!el || !cls) return;
    el.hidden = false;
    el.innerHTML =
      "Eligible set: " +
      esc(cls.BULK_APPROVAL_ELIGIBLE) +
      " cases · Present " +
      esc(cls.ELIGIBLE_PRESENT) +
      " · Not Present " +
      esc(cls.ELIGIBLE_NOT_PRESENT) +
      " · Unique responses " +
      esc(cls.UNIQUE_RESPONSES) +
      "<br>Providers: " +
      esc(JSON.stringify(cls.providerBreakdown || {})) +
      "<br>Languages: " +
      esc(JSON.stringify(cls.languageBreakdown || {})) +
      "<br>Geographies: " +
      esc(JSON.stringify(cls.geographyBreakdown || {})) +
      "<br>Manual case IDs: " +
      esc((cls.MANUAL_CASE_IDS || []).join(", ") || "(none)");
  }

  async function loadBulkClassification() {
    var r = await fetchJson(
      "/api/ai-intelligence/presence-validation-review/bulk-approval/preview"
    );
    if (!r.data || !r.data.ok) {
      var el = document.getElementById("bulkSummary");
      if (el) el.textContent = (r.data && r.data.message) || "Bulk classification failed";
      return;
    }
    bulkClassification = r.data;
    renderBulkSummary(r.data);
  }

  async function confirmBulkApproval() {
    rememberReviewer();
    var rev = reviewer();
    if (!rev) {
      setState("Enter reviewer email first", true);
      document.getElementById("reviewer").focus();
      return;
    }
    if (!bulkClassification || !bulkClassification.BULK_APPROVAL_ELIGIBLE) {
      setState("No eligible cases to approve", true);
      return;
    }
    setState("Applying bulk approval…");
    var r = await fetchJson(
      "/api/ai-intelligence/presence-validation-review/bulk-approval/apply",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          reviewer: rev,
          confirmToken: "CONFIRM_BULK_APPROVAL",
          caseIds: bulkClassification.ELIGIBLE_CASE_IDS || null,
        }),
      }
    );
    document.getElementById("bulkConfirm").hidden = true;
    if (!r.data || !r.data.ok) {
      setState((r.data && r.data.message) || "Bulk approval failed", true);
      return;
    }
    summary = r.data.summary;
    renderProgress();
    setState(
      "Bulk approved " +
        (r.data.appliedCount || 0) +
        " cases. Manual remaining: " +
        ((r.data.MANUAL_CASE_IDS && r.data.MANUAL_CASE_IDS.length) || 0)
    );
    document.getElementById("fAssisted").value = "manual";
    document.getElementById("fStatus").value = "pending";
    await loadBulkClassification();
    await loadQueue();
  }

  document.getElementById("btnReviewBulkSet").addEventListener("click", async function () {
    if (!bulkClassification) await loadBulkClassification();
    renderBulkDetail(bulkClassification);
  });

  document.getElementById("btnDownloadEligibleIds").addEventListener("click", async function () {
    if (!bulkClassification) await loadBulkClassification();
    var ids = (bulkClassification && bulkClassification.ELIGIBLE_CASE_IDS) || [];
    downloadBlob(
      "presence-bulk-eligible-case-ids.json",
      JSON.stringify(
        {
          caseCount: ids.length,
          caseIds: ids,
          Present: bulkClassification.ELIGIBLE_PRESENT,
          NotPresent: bulkClassification.ELIGIBLE_NOT_PRESENT,
        },
        null,
        2
      ) + "\n",
      "application/json;charset=utf-8"
    );
  });

  document.getElementById("btnApproveEligible").addEventListener("click", async function () {
    if (!bulkClassification) await loadBulkClassification();
    var n = bulkClassification && bulkClassification.BULK_APPROVAL_ELIGIBLE;
    if (!n) {
      setState("No bulk-approval eligible cases", true);
      return;
    }
    document.getElementById("bulkConfirmText").textContent =
      "Approve " + n + " assisted Presence proposals as human-reviewed final labels?";
    document.getElementById("bulkConfirm").hidden = false;
    renderBulkDetail(bulkClassification);
  });

  document.getElementById("btnConfirmBulk").addEventListener("click", function () {
    confirmBulkApproval();
  });
  document.getElementById("btnCancelBulk").addEventListener("click", function () {
    document.getElementById("bulkConfirm").hidden = true;
    setState("Bulk approval cancelled");
  });

  try {
    var saved = localStorage.getItem("presval_reviewer");
    if (saved) document.getElementById("reviewer").value = saved;
  } catch (e) {}

  loadBulkClassification();
  loadQueue();
})();
