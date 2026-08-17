/**
 * Recommendation Taxonomy Review UI — 52 DEV cases.
 * Decisions are local until explicit Apply. No auto-apply.
 */
(function () {
  "use strict";

  var queueCases = [];
  var selectedIdx = -1;
  var previewOk = false;
  var taxonomy = [];
  var summary = null;

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setState(msg, isError) {
    var el = document.getElementById("trxState");
    if (!el) return;
    el.hidden = false;
    el.textContent = msg;
    el.className = "trx-state" + (isError ? " error" : "");
  }

  async function fetchJson(url, opts) {
    var res = await fetch(url, Object.assign({ credentials: "same-origin" }, opts || {}));
    var data = null;
    try {
      data = await res.json();
    } catch (e) {
      data = { ok: false, message: "Invalid JSON response" };
    }
    return { status: res.status, data: data };
  }

  function reviewer() {
    var el = document.getElementById("trxReviewer");
    return (el && el.value && el.value.trim()) || "";
  }

  function renderProgress(s) {
    var host = document.getElementById("trxProgress");
    if (!host || !s) return;
    host.hidden = false;
    summary = s;
    host.innerHTML = [
      ["Total", s.TOTAL],
      ["Proposed Keep", s.PROPOSED_KEEP],
      ["Proposed Amend", s.PROPOSED_AMEND],
      ["Unreviewed", s.UNREVIEWED],
      ["Decided Keep", s.DECIDED_KEEP],
      ["Accept Proposal", s.DECIDED_ACCEPT_PROPOSAL],
      ["Edited", s.DECIDED_EDIT],
      ["Deferred", s.DEFERRED],
    ]
      .map(function (row) {
        return (
          '<div class="trx-card"><h3>' +
          esc(row[0]) +
          '</h3><div class="val">' +
          esc(row[1]) +
          "</div></div>"
        );
      })
      .join("");
  }

  function renderHelp(tree, notes) {
    var host = document.getElementById("trxHelpBody");
    if (!host) return;
    var ol =
      "<ol>" +
      (tree || [])
        .map(function (t) {
          return "<li><code>" + esc(t.role) + "</code> — " + esc(t.when) + "</li>";
        })
        .join("") +
      "</ol>";
    var n =
      "<p><strong>Boundaries:</strong></p><ul>" +
      (notes || [])
        .map(function (x) {
          return "<li>" + esc(x) + "</li>";
        })
        .join("") +
      "</ul>";
    host.innerHTML = ol + n;
  }

  function badgeFor(c) {
    if (c.humanDecision === "DEFER") return '<span class="badge defer">DEFER</span>';
    if (c.humanDecision) return '<span class="badge done">DECIDED</span>';
    if (c.isAmendProposal) return '<span class="badge amend">AMEND?</span>';
    return '<span class="badge keep">KEEP?</span>';
  }

  function renderQueue() {
    var host = document.getElementById("trxQueue");
    if (!host) return;
    host.innerHTML = queueCases
      .map(function (c, i) {
        return (
          '<button type="button" data-i="' +
          i +
          '" class="' +
          (i === selectedIdx ? "active" : "") +
          '"><div><strong>' +
          esc(c.ENTITY) +
          "</strong>" +
          badgeFor(c) +
          '</div><div class="meta">' +
          esc(c.CASE_ID) +
          " · " +
          esc(c.CURRENT_HUMAN_LABEL) +
          " → " +
          esc(c.TAXONOMY_DECISION_PROPOSED) +
          "</div></button>"
        );
      })
      .join("");
    host.querySelectorAll("button").forEach(function (btn) {
      btn.onclick = function () {
        openCase(Number(btn.getAttribute("data-i")));
      };
    });
  }

  function evFlag(on, label) {
    return '<span class="' + (on ? "on" : "off") + '">' + (on ? "✓" : "·") + " " + esc(label) + "</span>";
  }

  function openCase(i) {
    selectedIdx = i;
    renderQueue();
    var c = queueCases[i];
    var host = document.getElementById("trxPanel");
    if (!host || !c) return;
    var ev = c.EVIDENCE || {};
    var editOpts = (taxonomy || [])
      .map(function (r) {
        return (
          '<option value="' +
          esc(r) +
          '"' +
          (r === (c.editedLabel || c.TAXONOMY_DECISION_PROPOSED) ? " selected" : "") +
          ">" +
          esc(r) +
          "</option>"
        );
      })
      .join("");

    host.innerHTML =
      "<h2>" +
      esc(c.ENTITY) +
      "</h2>" +
      '<div class="trx-meta">' +
      ["PROVIDER", "LANGUAGE", "GEOGRAPHY", "PROMPT_FAMILY", "CASE_ID"]
        .map(function (k) {
          return "<div>" + esc(k) + "<strong>" + esc(c[k] || "—") + "</strong></div>";
        })
        .join("") +
      "</div>" +
      '<div class="trx-box"><h3>FULL RELEVANT RESPONSE CONTEXT</h3><div class="trx-excerpt">' +
      esc(ev.LOCAL_ENTITY_CONTEXT || ev.LINE || "") +
      "</div></div>" +
      '<div class="trx-box human"><h3>CURRENT HUMAN LABEL</h3><div style="font-size:18px;color:#ffe9a8">' +
      esc(c.CURRENT_HUMAN_LABEL) +
      "</div></div>" +
      '<div class="trx-box proposed"><h3>CLARIFIED TAXONOMY PROPOSAL</h3><div style="font-size:18px;color:#a8ffd0">' +
      esc(c.TAXONOMY_DECISION_PROPOSED) +
      "</div><p style=\"margin:8px 0 0;font-size:12px;color:#9facd6\">" +
      esc(c.REASON || "") +
      "</p></div>" +
      '<div class="trx-box classifier"><h3>CLASSIFIER OUTPUT</h3><div style="font-size:16px;color:#cfe0ff">' +
      esc(c.CLASSIFIER_OUTPUT) +
      "</div></div>" +
      '<div class="trx-box"><h3>DECISION EVIDENCE</h3><div class="trx-ev">' +
      evFlag(ev.TRUE_RANKING, "true ranking") +
      evFlag(ev.SECTION_NUMBER_ONLY, "section number only") +
      evFlag(ev.HAS_DIRECT_POSITIVE_CUE, "direct positive cue") +
      evFlag(ev.HAS_CONSIDERATION_SET_CUE, "consideration-set cue") +
      evFlag(ev.HAS_ONLY_DESCRIPTION, "neutral description only") +
      evFlag(!!ev.SECTION_PARENT_HEADING, "section: " + (ev.SECTION_PARENT_HEADING || "none")) +
      "</div><p style=\"margin:8px 0 0;font-size:12px;color:#7f8db3\">Line: " +
      esc(ev.LINE || "") +
      "</p></div>" +
      '<label class="trx-edit">Edit label (for EDIT LABEL action)<select id="trxEditLabel">' +
      editOpts +
      "</select></label>" +
      '<div class="trx-actions">' +
      '<button type="button" class="keep" id="trxKeep">KEEP HUMAN LABEL</button>' +
      '<button type="button" class="accept" id="trxAccept">ACCEPT TAXONOMY PROPOSAL</button>' +
      '<button type="button" id="trxEdit">EDIT LABEL</button>' +
      '<button type="button" class="defer" id="trxDefer">DEFER</button>' +
      "</div>" +
      (c.humanDecision
        ? '<p style="margin-top:10px;font-size:12px;color:#a8ffd0">Current decision: ' +
          esc(c.humanDecision) +
          (c.editedLabel ? " → " + esc(c.editedLabel) : "") +
          "</p>"
        : "");

    document.getElementById("trxKeep").onclick = function () {
      decide("KEEP_HUMAN_LABEL");
    };
    document.getElementById("trxAccept").onclick = function () {
      decide("ACCEPT_TAXONOMY_PROPOSAL");
    };
    document.getElementById("trxEdit").onclick = function () {
      var sel = document.getElementById("trxEditLabel");
      decide("EDIT_LABEL", sel && sel.value);
    };
    document.getElementById("trxDefer").onclick = function () {
      decide("DEFER");
    };
  }

  async function decide(action, editedLabel) {
    var c = queueCases[selectedIdx];
    if (!c) return;
    var rev = reviewer();
    if (!rev) {
      setState("Enter reviewer name/email before deciding.", true);
      return;
    }
    previewOk = false;
    document.getElementById("trxApply").disabled = true;
    var res = await fetchJson("/api/ai-intelligence/recommendation-taxonomy-review/decision", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        caseId: c.CASE_ID,
        action: action,
        editedLabel: editedLabel || null,
        reviewer: rev,
      }),
    });
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Decision failed", true);
      return;
    }
    setState("Saved decision: " + action + " for " + c.CASE_ID, false);
    await loadQueue(false);
    if (selectedIdx >= 0 && selectedIdx < queueCases.length) openCase(selectedIdx);
    else if (queueCases.length) openCase(0);
  }

  async function loadQueue(selectFirst) {
    var filt = document.getElementById("trxFilt");
    var q = filt && filt.value ? "?filter=" + encodeURIComponent(filt.value) : "";
    var res = await fetchJson("/api/ai-intelligence/recommendation-taxonomy-review/queue" + q);
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Failed to load taxonomy review queue", true);
      return false;
    }
    if (res.data.validation && res.data.validation.HOLDOUT_CASES > 0) {
      setState("BLOCKED: holdout cases present in taxonomy review artifact", true);
      return false;
    }
    queueCases = res.data.cases || [];
    taxonomy = res.data.recommendationTaxonomy || [];
    renderHelp(res.data.decisionTree, res.data.boundaryNotes);
    renderProgress(res.data.summary);
    document.getElementById("trxFilters").hidden = false;
    document.getElementById("trxToolbar").hidden = false;
    document.getElementById("trxMain").hidden = false;
    document.getElementById("trxState").hidden = true;
    renderQueue();
    if (selectFirst !== false && queueCases.length) openCase(0);
    return true;
  }

  async function acceptAll() {
    var rev = reviewer();
    if (!rev) {
      setState("Enter reviewer before accepting all proposals.", true);
      return;
    }
    if (
      !window.confirm(
        "Seed ACCEPT TAXONOMY PROPOSAL for all amend cases (and KEEP for identical)?\n\nThis does NOT write Golden Set yet — you must Preview + Apply."
      )
    ) {
      return;
    }
    previewOk = false;
    document.getElementById("trxApply").disabled = true;
    var res = await fetchJson(
      "/api/ai-intelligence/recommendation-taxonomy-review/accept-all-proposals",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reviewer: rev, includeKeep: true }),
      }
    );
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Accept-all failed", true);
      return;
    }
    setState("Seeded " + res.data.seeded + " decisions. Preview + Apply still required.", false);
    await loadQueue(true);
  }

  async function previewApply() {
    var box = document.getElementById("trxPreviewBox");
    var res = await fetchJson("/api/ai-intelligence/recommendation-taxonomy-review/preview-apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Preview failed", true);
      previewOk = false;
      document.getElementById("trxApply").disabled = true;
      return;
    }
    var p = res.data;
    box.style.display = "block";
    box.textContent = JSON.stringify(
      {
        TOTAL: p.TOTAL,
        KEEP_PROPOSED: p.KEEP_PROPOSED,
        AMEND_PROPOSED: p.AMEND_PROPOSED,
        DEFER_PROPOSED: p.DEFER_PROPOSED,
        UNREVIEWED: p.UNREVIEWED,
        BREAKDOWN_BY_CURRENT_TO_PROPOSED: p.BREAKDOWN_BY_CURRENT_TO_PROPOSED,
        CAN_APPLY: p.CAN_APPLY,
        BLOCKERS: p.BLOCKERS,
        note: "Explicit Apply click is the human authorization event.",
      },
      null,
      2
    );
    previewOk = p.CAN_APPLY === true;
    document.getElementById("trxApply").disabled = !previewOk;
    setState(
      previewOk
        ? "Preview OK — click Apply to write Golden Set amendments."
        : "Preview blocked: " + (p.BLOCKERS || []).join(", "),
      !previewOk
    );
  }

  async function applyNow() {
    if (!previewOk) {
      setState("Run Preview Apply first.", true);
      return;
    }
    var rev = reviewer();
    if (!rev) {
      setState("Enter reviewer before Apply.", true);
      return;
    }
    if (
      !window.confirm(
        "APPLY taxonomy review to Golden Set v2?\n\nThis will amend human labels via governed amendment API.\nHoldout remains untouched.\n\nType intent confirmed by clicking OK."
      )
    ) {
      return;
    }
    var res = await fetchJson("/api/ai-intelligence/recommendation-taxonomy-review/apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        explicitApply: true,
        confirmToken: "APPLY_TAXONOMY_REVIEW",
        reviewer: rev,
      }),
    });
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Apply failed", true);
      return;
    }
    previewOk = false;
    document.getElementById("trxApply").disabled = true;
    setState(
      "Applied. KEPT=" +
        res.data.KEPT +
        " AMENDED=" +
        res.data.AMENDED +
        " DEFERRED=" +
        res.data.DEFERRED,
      false
    );
    document.getElementById("trxPreviewBox").style.display = "block";
    document.getElementById("trxPreviewBox").textContent = JSON.stringify(res.data, null, 2);
  }

  async function init() {
    var ready = await fetchJson("/api/ai-intelligence/recommendation-taxonomy-review/ready");
    if (!ready.data || ready.data.ok !== true) {
      setState((ready.data && ready.data.message) || "Taxonomy review not ready", true);
      return;
    }
    if (ready.data.HOLDOUT_CASES > 0) {
      setState("BLOCKED: HOLDOUT_CASES=" + ready.data.HOLDOUT_CASES, true);
      return;
    }
    if (ready.data.READY_FOR_HUMAN_APPLY !== "YES") {
      setState(
        "Artifact validation failed. VALID=" +
          ready.data.VALID +
          " INVALID=" +
          ready.data.INVALID,
        true
      );
      return;
    }
    document.getElementById("trxFilt").onchange = function () {
      loadQueue(true);
    };
    document.getElementById("trxAcceptAll").onclick = acceptAll;
    document.getElementById("trxPreview").onclick = previewApply;
    document.getElementById("trxApply").onclick = applyNow;
    await loadQueue(true);
  }

  init();
})();
