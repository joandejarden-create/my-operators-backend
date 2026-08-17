/**
 * Golden Set Review UI — packets, guidance, diffs, learning dashboard.
 * Never auto-approves. External assistance is NOT ground truth.
 */
(function () {
  "use strict";

  var queueCases = [];
  var currentIndex = 0;
  var taxonomies = {};
  var taxonomyHelp = {};
  var busy = false;
  var currentDetail = null;
  var currentSystemLabels = null;

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

  async function fetchJson(path, opts) {
    var jwt = await getJwt();
    var headers = { Accept: "application/json" };
    if (jwt) headers.Authorization = "Bearer " + jwt;
    if (opts && opts.body) headers["Content-Type"] = "application/json";
    var res = await fetch(apiBase() + path, {
      method: (opts && opts.method) || "GET",
      headers: headers,
      body: opts && opts.body ? JSON.stringify(opts.body) : undefined,
    });
    var data = await res.json().catch(function () {
      return null;
    });
    return { status: res.status, data: data };
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setState(msg, isError) {
    var el = document.getElementById("gsrState");
    if (!el) return;
    el.hidden = false;
    el.textContent = msg;
    el.className = "gsr-state" + (isError ? " error" : "");
  }

  function toast(msg) {
    var el = document.getElementById("gsrToast");
    if (!el) return;
    el.textContent = msg;
    el.style.display = "block";
    setTimeout(function () {
      el.style.display = "none";
    }, 2200);
  }

  async function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    var ta = document.createElement("textarea");
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  }

  function renderProgress(p) {
    var host = document.getElementById("gsrProgress");
    if (!host || !p) return;
    host.hidden = false;
    host.innerHTML = [
      ["Outstanding Review", p.OUTSTANDING_REVIEW != null ? p.OUTSTANDING_REVIEW : p.REMAINING],
      ["Completed Review", p.COMPLETED_REVIEW != null ? p.COMPLETED_REVIEW : p.REVIEWED],
      ["Invalidated", p.INVALIDATED != null ? p.INVALIDATED : 0],
      ["Superseded", p.SUPERSEDED != null ? p.SUPERSEDED : 0],
      ["Total Historical", p.TOTAL_HISTORICAL != null ? p.TOTAL_HISTORICAL : p.TOTAL],
      ["Promotable (completed)", p.PROMOTABLE],
    ]
      .map(function (row) {
        return (
          '<div class="gsr-card"><h3>' +
          esc(row[0]) +
          '</h3><div class="val">' +
          esc(row[1]) +
          "</div></div>"
        );
      })
      .join("");
  }

  function renderLearning(learning) {
    var host = document.getElementById("gsrLearn");
    if (!host) return;
    host.hidden = false;
    var L = learning || {};
    host.innerHTML = [
      ["Corrections", L.CORRECTIONS || 0],
      ["Most Common Disagreement", L.MOST_COMMON_DISAGREEMENT || "—"],
      ["Rec. Corrections", L.RECOMMENDATION_CLASSIFICATION_CORRECTIONS || 0],
      ["First-Rec Corrections", L.FIRST_RECOMMENDATION_CORRECTIONS || 0],
      ["Question Status Corrections", L.QUESTION_STATUS_CORRECTIONS || 0],
      ["Citation Corrections", L.CITATION_CORRECTIONS || 0],
      ["Parent/Brand Corrections", L.PARENT_BRAND_CORRECTIONS || 0],
      ["Improvement Candidates", L.CLASSIFIER_IMPROVEMENT_CANDIDATES || 0],
    ]
      .map(function (row) {
        return (
          '<div class="gsr-card"><h3>' +
          esc(row[0]) +
          '</h3><div class="val" style="font-size:14px">' +
          esc(row[1]) +
          "</div></div>"
        );
      })
      .join("");
  }

  function renderNeeded(needed) {
    var host = document.getElementById("gsrNeeded");
    if (!host) return;
    host.hidden = false;
    var items = (needed && needed.needed) || [];
    if (!items.length) {
      host.textContent =
        "COVERAGE NEEDED FOR GOVERNANCE: minimum dimension hints currently satisfied (still need ≥150 labelled).";
      return;
    }
    host.textContent =
      "COVERAGE NEEDED FOR GOVERNANCE: " +
      items
        .map(function (n) {
          return n.dimension + "=" + n.key + " (have " + n.have + ", hint ≥" + n.targetHint + ")";
        })
        .join(" · ");
  }

  function renderHowTo() {
    var host = document.getElementById("gsrHowToBody");
    var wrap = document.getElementById("gsrHowTo");
    if (!host || !wrap) return;
    wrap.hidden = false;
    var rows = (taxonomyHelp && taxonomyHelp.howToLabel) || [];
    if (!rows.length) {
      host.innerHTML =
        "<p>ENTITY PRESENT = entity appears. RECOMMENDED = positively suggested. FIRST = leading recommendation. DISCUSSION ONLY = mentioned not recommended. NEGATIVE = discouraged. MISSING = absent. CITATION ASSOCIATED = governed association. UNKNOWN/N/A = cannot determine.</p>";
      return;
    }
    host.innerHTML =
      "<dl>" +
      rows
        .map(function (r) {
          return "<dt>" + esc(r.title) + "</dt><dd>" + esc(r.text) + "</dd>";
        })
        .join("") +
      "</dl>";
  }

  function tip(fieldKey, optionKey) {
    var map = taxonomyHelp[fieldKey] || {};
    var text = map[optionKey] || "";
    if (!text) return "";
    return (
      '<span class="gsr-tip" title="' +
      esc(text) +
      '">[?]</span>'
    );
  }

  function filterQuery() {
    var q = [];
    var st = document.getElementById("gsrFiltState");
    var p = document.getElementById("gsrFiltProvider");
    var l = document.getElementById("gsrFiltLang");
    var g = document.getElementById("gsrFiltGeo");
    var s = document.getElementById("gsrFiltStatus");
    var h = document.getElementById("gsrHardOnly");
    var a = document.getElementById("gsrFiltAssisted");
    // Default Active when state control missing
    var stateVal = st && st.value ? st.value : "active";
    q.push("state=" + encodeURIComponent(stateVal));
    if (p && p.value) q.push("provider=" + encodeURIComponent(p.value));
    if (l && l.value) q.push("language=" + encodeURIComponent(l.value));
    if (g && g.value) q.push("geography=" + encodeURIComponent(g.value));
    if (s && s.value) q.push("reviewStatus=" + encodeURIComponent(s.value));
    if (h && h.checked) q.push("hardCasesOnly=1");
    if (a && a.value === "available") q.push("assistedProposalAvailable=1");
    if (a && a.value === "differs") q.push("assistedProposalDiffersFromSystem=1");
    if (a && a.value === "needs") q.push("needsHumanDecision=1");
    return q.length ? "?" + q.join("&") : "";
  }

  function updateStateTabs(progress) {
    var b = (progress && progress.stateBuckets) || progress || {};
    var map = {
      gsrTabActive: b.ACTIVE_REVIEW != null ? b.ACTIVE_REVIEW : progress && progress.OUTSTANDING_REVIEW,
      gsrTabCompleted: b.COMPLETED != null ? b.COMPLETED : progress && progress.COMPLETED_REVIEW,
      gsrTabInvalidated: b.INVALIDATED != null ? b.INVALIDATED : progress && progress.INVALIDATED,
      gsrTabAll: b.TOTAL_HISTORICAL != null ? b.TOTAL_HISTORICAL : progress && progress.TOTAL_HISTORICAL,
    };
    Object.keys(map).forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      var base = el.getAttribute("data-label") || el.textContent.split(" (")[0];
      el.setAttribute("data-label", base);
      var n = map[id];
      el.textContent = base + (n != null ? " (" + n + ")" : "");
    });
  }

  function fillFilterOptions(cases) {
    function fill(id, values, prefix) {
      var sel = document.getElementById(id);
      if (!sel) return;
      var cur = sel.value;
      sel.innerHTML = "";
      var o = document.createElement("option");
      o.value = "";
      o.textContent = prefix + ": All";
      sel.appendChild(o);
      Object.keys(values)
        .sort()
        .forEach(function (v) {
          if (!v) return;
          var opt = document.createElement("option");
          opt.value = v;
          opt.textContent = v;
          sel.appendChild(opt);
        });
      sel.value = cur || "";
    }
    var providers = {};
    var langs = {};
    var geos = {};
    (cases || []).forEach(function (c) {
      if (c.provider) providers[c.provider] = 1;
      if (c.language) langs[c.language] = 1;
      if (c.geography) geos[c.geography] = 1;
    });
    fill("gsrFiltProvider", providers, "Provider");
    fill("gsrFiltLang", langs, "Language");
    fill("gsrFiltGeo", geos, "Geography");
  }

  function renderQueue() {
    var host = document.getElementById("gsrQueue");
    if (!host) return;
    host.innerHTML = queueCases
      .map(function (c, i) {
        return (
          '<button type="button" data-idx="' +
          i +
          '" class="' +
          (i === currentIndex ? "active" : "") +
          '"><div>' +
          esc(c.caseId) +
          " · " +
          esc(c.reviewStatus) +
          '</div><div class="meta">' +
          esc(c.provider) +
          " · " +
          esc(c.language) +
          " · " +
          esc(c.geography) +
          (c.hardCase ? " · HARD" : "") +
          "</div></button>"
        );
      })
      .join("");
    host.querySelectorAll("button[data-idx]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        openCase(Number(btn.getAttribute("data-idx")));
      });
    });
  }

  function optionList(values, selected, helpKey) {
    return (values || [])
      .map(function (v) {
        var title = ((taxonomyHelp[helpKey] || {})[v]) || "";
        return (
          '<option value="' +
          esc(v) +
          '" title="' +
          esc(title) +
          '"' +
          (String(selected) === String(v) ? " selected" : "") +
          ">" +
          esc(v) +
          "</option>"
        );
      })
      .join("");
  }

  function fmtLabel(v) {
    if (v === true) return "YES";
    if (v === false) return "NO";
    if (v == null || v === "") return "—";
    return String(v);
  }

  function computeLocalDiff(labels) {
    if (!currentSystemLabels) return { matches: true, suggestedAction: "CONFIRM", differences: [] };
    var fields = [
      "entityPresent",
      "recommendationStatus",
      "firstRecommendation",
      "questionStatus",
      "citationAssociation",
      "canonicalEntityId",
      "canonicalEntityName",
    ];
    var differences = [];
    fields.forEach(function (f) {
      if (String(labels[f]) !== String(currentSystemLabels[f])) {
        differences.push({ field: f, system: currentSystemLabels[f], human: labels[f] });
      }
    });
    return {
      matches: differences.length === 0,
      suggestedAction: differences.length === 0 ? "CONFIRM" : "CORRECT",
      differences: differences,
    };
  }

  function updateDecisionPanels() {
    var labels = readLabels();
    var diff = computeLocalDiff(labels);
    var save = document.getElementById("gsrSavePreview");
    var diffEl = document.getElementById("gsrDiffPreview");
    var confirmBtn = document.getElementById("gsrConfirm");
    var correctBtn = document.getElementById("gsrCorrect");
    if (save) {
      save.innerHTML =
        "<strong>HUMAN LABELS TO SAVE</strong><br>" +
        "Entity present: " +
        esc(fmtLabel(labels.entityPresent)) +
        "<br>Recommendation: " +
        esc(fmtLabel(labels.recommendationStatus)) +
        "<br>First recommendation: " +
        esc(fmtLabel(labels.firstRecommendation)) +
        "<br>Question status: " +
        esc(fmtLabel(labels.questionStatus)) +
        "<br>Citation: " +
        esc(fmtLabel(labels.citationAssociation)) +
        "<br>Canonical entity: " +
        esc(labels.canonicalEntityName || labels.canonicalEntityId || "—");
    }
    if (diffEl) {
      if (!diff.differences.length) {
        diffEl.innerHTML =
          "<strong>Suggested action: CONFIRM</strong><br>No differences from SYSTEM SUGGESTION (still requires explicit click).";
      } else {
        diffEl.innerHTML =
          "<strong>Suggested action: CORRECT</strong><br><strong>DIFFERENCES FROM SYSTEM SUGGESTION</strong><br>" +
          diff.differences
            .map(function (d) {
              return (
                esc(d.field) +
                ": System = " +
                esc(fmtLabel(d.system)) +
                " → Human = " +
                esc(fmtLabel(d.human))
              );
            })
            .join("<br>");
      }
    }
    if (confirmBtn) {
      confirmBtn.style.outline = diff.suggestedAction === "CONFIRM" ? "2px solid #4caf7a" : "none";
    }
    if (correctBtn) {
      correctBtn.style.outline = diff.suggestedAction === "CORRECT" ? "2px solid #d4a017" : "none";
    }
  }

  async function openCase(idx) {
    if (!queueCases[idx]) return;
    currentIndex = idx;
    renderQueue();
    var panel = document.getElementById("gsrPanel");
    panel.innerHTML = '<p class="gsr-warn">Loading case…</p>';
    var cMeta = queueCases[idx];
    var res = await fetchJson(
      "/api/ai-intelligence/golden-set-review/cases/" + encodeURIComponent(cMeta.caseId)
    );
    if (!res.data || res.data.ok !== true) {
      panel.innerHTML =
        '<p class="gsr-warn">Failed to load case: ' +
        esc((res.data && res.data.message) || res.status) +
        "</p>";
      return;
    }
    currentDetail = res.data.case;
    currentSystemLabels = res.data.systemAsLabels || null;
    if (res.data.taxonomyHelp) taxonomyHelp = res.data.taxonomyHelp;
    var detail = currentDetail;
    var cand = detail.candidate || {};
    if (!cand.candidateEntity || !cand.canonicalEntityId) {
      panel.innerHTML =
        '<p class="gsr-state error">INVALID_REVIEW_CASE_MISSING_SUBJECT — this case has no evaluated canonical entity and cannot be Confirmed/Corrected. It should not appear in the review queue.</p>';
      return;
    }
    var sug = detail.systemSuggestion || cand.systemSuggestion || {};
    var prior = (detail.review && detail.review.humanLabels) || {};
    var assisted = res.data.assistedProposal || null;
    var firstVal =
      prior.firstRecommendation === true
        ? "YES"
        : prior.firstRecommendation === false
          ? "NO"
          : prior.firstRecommendation === "NOT_APPLICABLE"
            ? "NOT_APPLICABLE"
            : "";

    var historyHtml = "";
    if (detail.review) {
      historyHtml =
        '<div class="gsr-history"><strong>Case history</strong><br>SYSTEM SUGGESTION → HUMAN LABEL → ' +
        esc(detail.review.reviewStatus) +
        " · reviewer " +
        esc(detail.review.reviewer || "—") +
        " · " +
        esc(detail.review.reviewedAt || "—") +
        (detail.review.fieldsChanged && detail.review.fieldsChanged.length
          ? "<br>Changed: " + esc(detail.review.fieldsChanged.join(", "))
          : "") +
        "</div>";
    }

    var assistedHtml = "";
    if (assisted) {
      assistedHtml =
        '<div class="gsr-box" style="border-color:#5a3a6a;background:#160c1a"><h3>ASSISTED PROPOSAL (not ground truth)</h3>' +
        "<pre style=\"white-space:pre-wrap;font-size:12px;color:#e0c8f0;margin:0;\">" +
        esc(JSON.stringify(assisted, null, 2)) +
        "</pre>" +
        '<div class="gsr-actions" style="margin-top:8px">' +
        '<button type="button" id="gsrAcceptAssistedOne">Accept Assisted</button>' +
        '<button type="button" id="gsrUseAssistedForm">Load Assisted Into Form</button>' +
        "</div></div>";
    }

    panel.innerHTML =
      "<h2>" +
      esc(cand.caseId) +
      " · " +
      esc(cMeta.reviewStatus) +
      "</h2>" +
      '<div class="gsr-toolbar">' +
      '<button type="button" id="gsrCopyPacket">Copy for Review</button>' +
      '<button type="button" id="gsrCopyResponse">Copy Response Only</button>' +
      "</div>" +
      '<div class="gsr-meta">' +
      "<div><strong>Provider</strong><br>" +
      esc(cand.provider) +
      "</div>" +
      "<div><strong>Model</strong><br>" +
      esc(cand.model || "—") +
      "</div>" +
      "<div><strong>Geography</strong><br>" +
      esc(cand.geography) +
      "</div>" +
      "<div><strong>Language</strong><br>" +
      esc(cand.language) +
      "</div>" +
      "<div><strong>Prompt Family</strong><br>" +
      esc(cand.promptFamily || cand.promptIntentTerritory || "—") +
      "</div>" +
      "<div><strong>Entity</strong><br>" +
      esc(cand.candidateEntity || "—") +
      "</div>" +
      "<div><strong>Canonical ID</strong><br>" +
      esc(cand.canonicalEntityId || "—") +
      "</div>" +
      "</div>" +
      '<div class="gsr-box"><h3>Prompt</h3><div class="gsr-excerpt">' +
      esc(detail.promptText || cand.promptText || "—") +
      "</div></div>" +
      '<div class="gsr-box"><h3>Stored response' +
      (detail.evidenceRef && detail.evidenceRef.fullAvailable ? " (expanded)" : " (excerpt)") +
      '</h3><div class="gsr-excerpt">' +
      esc(detail.rawResponseText || cand.rawResponseExcerpt || "—") +
      "</div></div>" +
      '<div class="gsr-box sys"><h3>SYSTEM SUGGESTION (not ground truth)</h3>' +
      "<pre style=\"white-space:pre-wrap;font-size:12px;color:#e8d9a8;margin:0;\">" +
      esc(JSON.stringify(sug, null, 2)) +
      "</pre></div>" +
      assistedHtml +
      historyHtml +
      '<div class="gsr-box human"><h3>HUMAN REVIEW (final authority)</h3><div class="gsr-fields">' +
      "<div><label>Entity present " +
      tip("entityPresent", "YES") +
      '</label><select id="gsrEntityPresent"><option value="">— select —</option>' +
      '<option value="YES"' +
      (prior.entityPresent === true ? " selected" : "") +
      ">YES</option>" +
      '<option value="NO"' +
      (prior.entityPresent === false ? " selected" : "") +
      ">NO</option></select></div>" +
      '<div><label>Canonical entity ID</label><input id="gsrCanonicalId" value="' +
      esc(prior.canonicalEntityId || cand.canonicalEntityId || "") +
      '"></div>' +
      '<div><label>Canonical entity name</label><input id="gsrCanonicalName" value="' +
      esc(prior.canonicalEntityName || cand.candidateEntity || "") +
      '"></div>' +
      "<div><label>Recommendation status " +
      tip("recommendationStatus", "discussed") +
      '</label><select id="gsrRecStatus"><option value="">— select —</option>' +
      optionList(taxonomies.recommendationStatus || [], prior.recommendationStatus || "", "recommendationStatus") +
      "</select></div>" +
      "<div><label>First recommendation " +
      tip("firstRecommendation", "YES") +
      '</label><select id="gsrFirst"><option value="">— select —</option>' +
      '<option value="YES"' +
      (firstVal === "YES" ? " selected" : "") +
      ">YES</option>" +
      '<option value="NO"' +
      (firstVal === "NO" ? " selected" : "") +
      ">NO</option>" +
      '<option value="NOT_APPLICABLE"' +
      (firstVal === "NOT_APPLICABLE" ? " selected" : "") +
      ">NOT_APPLICABLE</option></select></div>" +
      "<div><label>Question status " +
      tip("questionStatus", "DISCUSSION_ONLY") +
      '</label><select id="gsrQStatus"><option value="">— select —</option>' +
      optionList(taxonomies.questionStatus || [], prior.questionStatus || "", "questionStatus") +
      "</select></div>" +
      "<div><label>Citation association " +
      tip("citationAssociation", "ASSOCIATED") +
      '</label><select id="gsrCite"><option value="">— select —</option>' +
      optionList(taxonomies.citationAssociation || [], prior.citationAssociation || "", "citationAssociation") +
      "</select></div>" +
      '<div><label>Parent vs brand note</label><input id="gsrParentNote" value="' +
      esc(prior.parentVsBrandNote || "") +
      '"></div>' +
      '<div style="grid-column:1/-1"><label>Notes</label><textarea id="gsrNotes" rows="2">' +
      esc((detail.review && detail.review.notes) || "") +
      "</textarea></div>" +
      '<div><label><input type="checkbox" id="gsrSecond"> Flag SECOND_REVIEW_REQUIRED</label></div>' +
      "</div>" +
      '<div class="gsr-save" id="gsrSavePreview"></div>' +
      '<div class="gsr-diff" id="gsrDiffPreview"></div>' +
      '<div class="gsr-actions">' +
      '<button type="button" id="gsrPrev">Previous</button>' +
      '<button type="button" id="gsrNext">Next</button>' +
      '<button type="button" class="confirm" id="gsrConfirm">Confirm &amp; Next (C)</button>' +
      '<button type="button" class="correct" id="gsrCorrect">Correct &amp; Next</button>' +
      '<button type="button" class="defer" id="gsrDefer">Defer &amp; Next (D)</button>' +
      "</div>" +
      '<p class="gsr-keys">Suggested CONFIRM/CORRECT updates from form vs system suggestion — you must still click. Shortcuts: C confirm · D defer · ←/→ navigate. No auto-submit.</p>' +
      "</div>";

    document.getElementById("gsrPrev").onclick = function () {
      openCase(Math.max(0, currentIndex - 1));
    };
    document.getElementById("gsrNext").onclick = function () {
      openCase(Math.min(queueCases.length - 1, currentIndex + 1));
    };
    document.getElementById("gsrConfirm").onclick = function () {
      submitReview("CONFIRMED");
    };
    document.getElementById("gsrCorrect").onclick = function () {
      submitReview("CORRECTED");
    };
    document.getElementById("gsrDefer").onclick = function () {
      submitReview("DEFERRED");
    };
    document.getElementById("gsrCopyPacket").onclick = function () {
      copyPacket(cand.caseId, "full");
    };
    document.getElementById("gsrCopyResponse").onclick = function () {
      copyPacket(cand.caseId, "response");
    };

    var loadAssistedBtn = document.getElementById("gsrUseAssistedForm");
    if (loadAssistedBtn && assisted) {
      loadAssistedBtn.onclick = function () {
        applyAssistedToForm(assisted);
        updateDecisionPanels();
        toast("Assisted proposal loaded into form — still must Confirm/Correct");
      };
    }
    var acceptOne = document.getElementById("gsrAcceptAssistedOne");
    if (acceptOne) {
      acceptOne.onclick = async function () {
        if (!window.confirm("Accept assisted proposal as HUMAN FINAL for " + cand.caseId + "?")) {
          return;
        }
        var resA = await fetchJson(
          "/api/ai-intelligence/golden-set-review/import/accept-assisted",
          { method: "POST", body: { apply: true, caseIds: [cand.caseId] } }
        );
        if (!resA.data || resA.data.ok !== true) {
          setState((resA.data && resA.data.message) || "Accept failed", true);
          return;
        }
        renderProgress(resA.data.progress);
        await loadQueue(false);
        await openCase(currentIndex);
      };
    }

    ["gsrEntityPresent", "gsrCanonicalId", "gsrCanonicalName", "gsrRecStatus", "gsrFirst", "gsrQStatus", "gsrCite", "gsrParentNote"].forEach(
      function (id) {
        var el = document.getElementById(id);
        if (el) el.addEventListener("change", updateDecisionPanels);
        if (el) el.addEventListener("input", updateDecisionPanels);
      }
    );
    updateDecisionPanels();
  }

  function applyAssistedToForm(assisted) {
    if (!assisted) return;
    var ep = document.getElementById("gsrEntityPresent");
    if (assisted.entityPresent === true || assisted.entityPresent === "YES") ep.value = "YES";
    if (assisted.entityPresent === false || assisted.entityPresent === "NO") ep.value = "NO";
    if (assisted.canonicalEntityId) {
      document.getElementById("gsrCanonicalId").value = assisted.canonicalEntityId;
    }
    if (assisted.canonicalEntityName) {
      document.getElementById("gsrCanonicalName").value = assisted.canonicalEntityName;
    }
    if (assisted.recommendationStatus) {
      document.getElementById("gsrRecStatus").value = assisted.recommendationStatus;
    }
    var first = document.getElementById("gsrFirst");
    if (assisted.firstRecommendation === true || assisted.firstRecommendation === "YES") {
      first.value = "YES";
    } else if (assisted.firstRecommendation === false || assisted.firstRecommendation === "NO") {
      first.value = "NO";
    } else if (assisted.firstRecommendation === "NOT_APPLICABLE") {
      first.value = "NOT_APPLICABLE";
    }
    if (assisted.questionStatus) document.getElementById("gsrQStatus").value = assisted.questionStatus;
    if (assisted.citationAssociation) document.getElementById("gsrCite").value = assisted.citationAssociation;
    if (assisted.parentBrandNote) {
      document.getElementById("gsrParentNote").value = assisted.parentBrandNote;
    }
    if (assisted.reason) {
      var notes = document.getElementById("gsrNotes");
      if (notes && !notes.value) notes.value = assisted.reason;
    }
  }

  function readLabels() {
    var ep = document.getElementById("gsrEntityPresent").value;
    var first = document.getElementById("gsrFirst").value;
    return {
      entityPresent: ep === "YES" ? true : ep === "NO" ? false : null,
      canonicalEntityId: document.getElementById("gsrCanonicalId").value.trim() || null,
      canonicalEntityName: document.getElementById("gsrCanonicalName").value.trim() || null,
      recommendationStatus: document.getElementById("gsrRecStatus").value || null,
      firstRecommendation:
        first === "YES" ? true : first === "NO" ? false : first === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : null,
      questionStatus: document.getElementById("gsrQStatus").value || null,
      citationAssociation: document.getElementById("gsrCite").value || null,
      parentVsBrandNote: document.getElementById("gsrParentNote").value.trim() || null,
    };
  }

  function prefillFromSuggestionIfEmpty() {
    var sug = queueCases[currentIndex] && queueCases[currentIndex].systemSuggestion;
    if (!sug) return;
    var ep = document.getElementById("gsrEntityPresent");
    if (!ep.value && queueCases[currentIndex].candidateEntity) ep.value = "YES";
    var rec = document.getElementById("gsrRecStatus");
    if (!rec.value && sug.expectedRecommendationClass) rec.value = sug.expectedRecommendationClass;
    var first = document.getElementById("gsrFirst");
    if (!first.value && sug.expectedFirstRecommendation != null) {
      first.value = sug.expectedFirstRecommendation ? "YES" : "NO";
    }
    var q = document.getElementById("gsrQStatus");
    if (!q.value && sug.expectedQuestionStatus) q.value = sug.expectedQuestionStatus;
    var cite = document.getElementById("gsrCite");
    if (!cite.value) {
      if (sug.expectedCitationAssociation === "citations_present_unreviewed") cite.value = "UNKNOWN";
      else if (sug.expectedCitationAssociation) cite.value = sug.expectedCitationAssociation;
      else cite.value = "NOT_APPLICABLE";
    }
  }

  async function copyPacket(caseId, mode) {
    var res = await fetchJson(
      "/api/ai-intelligence/golden-set-review/cases/" +
        encodeURIComponent(caseId) +
        "/packet" +
        (mode === "response" ? "?mode=response" : "")
    );
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Could not build review packet", true);
      return;
    }
    var text =
      mode === "response"
        ? (res.data.packet && res.data.packet.text) || ""
        : (res.data.packet && res.data.packet.packetText) || "";
    await copyText(text);
    toast(mode === "response" ? "Response copied" : "Review packet copied");
  }

  async function copyNextFive() {
    var ids = queueCases
      .filter(function (c) {
        return c.reviewStatus === "UNREVIEWED";
      })
      .slice(0, 5)
      .map(function (c) {
        return c.caseId;
      });
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/packets/batch", {
      method: "POST",
      body: { caseIds: ids },
    });
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Could not build batch packets", true);
      return;
    }
    await copyText((res.data.batch && res.data.batch.combinedText) || "");
    toast("Next " + ((res.data.batch && res.data.batch.count) || 0) + " review packets copied");
  }

  async function downloadJson(filename, obj) {
    var blob = new Blob([JSON.stringify(obj, null, 2)], { type: "application/json" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function downloadCsv(filename, text) {
    var blob = new Blob([text], { type: "text/csv; charset=utf-8" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function exportBulk(mode, format) {
    var qs = "mode=" + encodeURIComponent(mode || "ALL") + "&format=" + encodeURIComponent(format || "json");
    if (mode === "FILTERED_CURRENT_VIEW") {
      var fq = filterQuery();
      if (fq) qs += "&" + fq.slice(1);
    }
    var path =
      mode === "FILTERED_CURRENT_VIEW"
        ? "/api/ai-intelligence/golden-set-review/export/filtered?" + qs
        : "/api/ai-intelligence/golden-set-review/export/all?" + qs;
    if (format === "csv") {
      var jwt = await getJwt();
      var headers = { Accept: "text/csv" };
      if (jwt) headers.Authorization = "Bearer " + jwt;
      var res = await fetch(apiBase() + path, { headers: headers });
      if (!res.ok) {
        setState("Export failed (" + res.status + ")", true);
        return;
      }
      var text = await res.text();
      await downloadCsv(
        "golden-set-review-candidates-" + String(mode).toLowerCase() + ".csv",
        text
      );
      toast("CSV export downloaded");
      return;
    }
    var resJ = await fetchJson(path);
    if (!resJ.data || resJ.data.ok !== true) {
      setState((resJ.data && resJ.data.message) || "Export failed", true);
      return;
    }
    var exp = resJ.data.export || {};
    await downloadJson(
      "golden-set-review-candidates-" + String(mode).toLowerCase() + ".json",
      exp
    );
    toast("Exported " + (exp.totalCandidates || 0) + " candidates");
  }

  async function exportPackets() {
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/export/packets?mode=ALL");
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Packet export failed", true);
      return;
    }
    await downloadJson("golden-set-review-packets-all.json", res.data.export || {});
    toast("Review packets JSON exported");
  }

  async function exportTemplate() {
    var res = await fetchJson(
      "/api/ai-intelligence/golden-set-review/export/assistance-template?mode=ALL"
    );
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Template export failed", true);
      return;
    }
    await downloadJson(
      "golden-set-review-assistance-template.json",
      res.data.template || {}
    );
    toast("Assistance template downloaded");
  }

  var pendingImportDoc = null;

  function formatImportPreviewError(res) {
    var status = res && res.status;
    var data = (res && res.data) || {};
    var code = data.code || data.error || null;
    if (status === 413 || code === "IMPORT_PAYLOAD_TOO_LARGE") {
      return "IMPORT_PAYLOAD_TOO_LARGE: Import JSON exceeds server body limit. Restart server after the 5mb import route fix.";
    }
    if (code === "IMPORT_FILE_INVALID") return "IMPORT_FILE_INVALID: " + (data.message || "Invalid import file");
    if (code === "IMPORT_SCHEMA_UNSUPPORTED") {
      return "IMPORT_SCHEMA_UNSUPPORTED: " + (data.message || "Unrecognized import schema");
    }
    if (code === "CANDIDATE_VERSION_MISMATCH") {
      return "CANDIDATE_VERSION_MISMATCH: " + (data.message || "Candidate version does not match");
    }
    if (code === "IMPORT_DOCUMENT_REQUIRED") {
      return "IMPORT_FILE_INVALID: " + (data.message || "cases[] required");
    }
    if (!data || data.ok !== true) {
      if (status === 401) return "AUTH_REQUIRED: Sign in required for import preview.";
      if (status === 403) return "ACCESS_DENIED: Founder/admin validation access required.";
      if (status >= 500) return "SERVER_ERROR: " + (data.message || ("HTTP " + status));
      if (!data) return "SERVER_ERROR: Empty or non-JSON response (HTTP " + status + ")";
      return (code || "SERVER_ERROR") + ": " + (data.message || "Import preview failed");
    }
    return null;
  }

  function renderImportPreview(preview) {
    var host = document.getElementById("gsrImportPreview");
    if (!host || !preview) return;
    host.style.display = "block";
    var isAssisted = preview.kind === "ASSISTED";
    var lines = [
      "<h3>Import Preview (dry run — nothing saved yet)</h3>",
      "<p>Kind: <strong>" +
        esc(preview.kind) +
        "</strong> · reviewVersion: <code>" +
        esc(preview.reviewVersion || "—") +
        "</code></p>",
      "<p>Proposals: <strong>" +
        esc(preview.PROPOSALS_TOTAL != null ? preview.PROPOSALS_TOTAL : preview.TOTAL_ROWS) +
        "</strong> · Matched active: <strong>" +
        esc(preview.MATCHED_ACTIVE_CASES != null ? preview.MATCHED_ACTIVE_CASES : preview.MATCHED_CASES) +
        "</strong> · Unknown: " +
        esc((preview.UNKNOWN_CASE_IDS || preview.INVALID_CASE_IDS || []).length) +
        " · Superseded/inactive: " +
        esc(preview.SUPERSEDED != null ? preview.SUPERSEDED : (preview.CANDIDATE_NOT_ACTIVE || []).length) +
        " · Invalid: " +
        esc(preview.INVALID_CASES != null ? preview.INVALID_CASES : (preview.INVALID_ENUMS || []).length) +
        "</p>",
    ];
    if (isAssisted) {
      lines.push(
        '<div class="gsr-box" style="margin:10px 0;">' +
          "<p><strong>Assisted vs System</strong></p>" +
          "<p>Same as system: <strong>" +
          esc(preview.SYSTEM_MATCHES) +
          "</strong> · Different from system: <strong>" +
          esc(preview.SYSTEM_DIFFERENCES) +
          "</strong></p>" +
          "<p>Recommendation diffs: " +
          esc(preview.RECOMMENDATION_DIFFS) +
          " · First-rec diffs: " +
          esc(preview.FIRST_RECOMMENDATION_DIFFS) +
          " · Question-status diffs: " +
          esc(preview.QUESTION_STATUS_DIFFS) +
          " · Citation diffs: " +
          esc(preview.CITATION_DIFFS) +
          " · Entity-present diffs: " +
          esc(preview.ENTITY_PRESENT_DIFFS) +
          " · Canonical diffs: " +
          esc(preview.CANONICAL_ENTITY_DIFFS) +
          "</p>" +
          "<p style=\"color:#9ad7ff\">Assisted proposals are NOT ground truth. Apply stores proposals only; cases stay UNREVIEWED until Accept All / Accept Assisted.</p>" +
          "</div>"
      );
      if (preview.DISTRIBUTION) {
        lines.push(
          "<p>Providers: " +
            esc(JSON.stringify(preview.DISTRIBUTION.provider || {})) +
            " · Languages: " +
            esc(JSON.stringify(preview.DISTRIBUTION.language || {})) +
            " · Geography: " +
            esc(JSON.stringify(preview.DISTRIBUTION.geography || {})) +
            "</p>"
        );
      }
    } else {
      lines.push(
        "<p>Confirmed: " +
          esc(preview.CONFIRMED) +
          " · Corrected: " +
          esc(preview.CORRECTED) +
          " · Deferred: " +
          esc(preview.DEFERRED) +
          "</p>"
      );
    }
    lines.push(
      "<p>Invalid enums: " +
        esc((preview.INVALID_ENUMS || []).length) +
        " · Canonical mismatches: " +
        esc((preview.CANONICAL_ENTITY_MISMATCHES || []).length) +
        " · Missing fields: " +
        esc((preview.MISSING_REQUIRED_FIELDS || []).length) +
        " · Duplicates: " +
        esc((preview.DUPLICATES || []).length) +
        " · Version mismatches: " +
        esc((preview.VERSION_MISMATCHES || []).length) +
        "</p>"
    );
    if (preview.errorCode) {
      lines.push('<p class="gsr-warn">errorCode: ' + esc(preview.errorCode) + "</p>");
    }
    if (preview.canApply) {
      var applyLabel = isAssisted
        ? "Apply Assisted Proposals (store only — not ground truth)"
        : "Apply Human Review Import";
      lines.push(
        '<div class="gsr-actions"><button type="button" class="confirm" id="gsrImportApply">' +
          applyLabel +
          "</button>" +
          '<button type="button" id="gsrImportCancel">Cancel</button></div>'
      );
      if (isAssisted) {
        lines.push(
          "<p style=\"opacity:0.85\">After Apply, use <strong>Accept All Valid Assisted Proposals</strong> as the explicit human authorization to create HUMAN FINAL labels.</p>"
        );
      }
    } else {
      lines.push('<p class="gsr-warn">Cannot apply — fix validation errors first.</p>');
      lines.push('<button type="button" id="gsrImportCancel">Dismiss</button>');
    }
    host.innerHTML = lines.join("");
    var applyBtn = document.getElementById("gsrImportApply");
    if (applyBtn) {
      applyBtn.onclick = function () {
        applyImport();
      };
    }
    var cancel = document.getElementById("gsrImportCancel");
    if (cancel) {
      cancel.onclick = function () {
        pendingImportDoc = null;
        host.style.display = "none";
        host.innerHTML = "";
      };
    }
  }

  async function previewImportFile(file) {
    var text = await file.text();
    var doc;
    try {
      doc = JSON.parse(text);
    } catch (err) {
      setState("IMPORT_FILE_INVALID: Import file is not valid JSON", true);
      return;
    }
    pendingImportDoc = doc;
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/import/preview", {
      method: "POST",
      body: { document: doc },
    });
    var errMsg = formatImportPreviewError(res);
    if (errMsg) {
      setState(errMsg, true);
      return;
    }
    if (!res.data || res.data.ok !== true || !res.data.preview) {
      setState("SERVER_ERROR: Unexpected import preview response", true);
      return;
    }
    renderImportPreview(res.data.preview);
    setState(
      "Import preview ready — review differences before Apply. No reviews written yet.",
      false
    );
  }

  async function applyImport() {
    if (!pendingImportDoc) return;
    if (
      !window.confirm(
        "Apply import? This writes human review decisions (or stores assisted proposals). Not auto-promotion."
      )
    ) {
      return;
    }
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/import/apply", {
      method: "POST",
      body: {
        apply: true,
        document: pendingImportDoc,
        externalAssistanceUsed: true,
      },
    });
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Import apply failed", true);
      return;
    }
    pendingImportDoc = null;
    var host = document.getElementById("gsrImportPreview");
    if (host) {
      host.style.display = "none";
      host.innerHTML = "";
    }
    renderProgress(res.data.progress);
    toast("Import applied (" + (res.data.resultCount || 0) + " cases)");
    setState(
      "Import applied. Kind=" +
        esc(res.data.kind) +
        ". Assisted proposals alone are not Golden Set ground truth.",
      false
    );
    await loadQueue(true);
  }

  async function acceptAllAssisted() {
    var ids = queueCases
      .filter(function (c) {
        return c.assistedProposalAvailable && c.reviewStatus === "UNREVIEWED";
      })
      .map(function (c) {
        return c.caseId;
      });
    if (!ids.length) {
      toast("No assisted proposals to accept");
      return;
    }
    if (
      !window.confirm(
        "Accept " +
          ids.length +
          " assisted proposals as HUMAN FINAL labels? This is an explicit human action."
      )
    ) {
      return;
    }
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/import/accept-assisted", {
      method: "POST",
      body: { apply: true, caseIds: ids },
    });
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Accept assisted failed", true);
      return;
    }
    renderProgress(res.data.progress);
    toast("Accepted " + ids.length + " assisted proposals as human labels");
    await loadQueue(true);
  }

  async function exportReviewed() {
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/export?format=json");
    if (!res.data || res.data.ok !== true) {
      setState((res.data && res.data.message) || "Export failed", true);
      return;
    }
    var blob = new Blob([JSON.stringify(res.data.export, null, 2)], {
      type: "application/json",
    });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = "golden-set-reviewed-cases.json";
    a.click();
    URL.revokeObjectURL(url);
    toast("Reviewed cases exported");
  }

  async function submitReview(status) {
    if (busy) return;
    var c = queueCases[currentIndex];
    if (!c) return;
    if (status === "CONFIRMED" || status === "CORRECTED") {
      prefillFromSuggestionIfEmpty();
      updateDecisionPanels();
    }
    var labels = readLabels();
    busy = true;
    setState("Saving " + status + "…", false);
    try {
      var res = await fetchJson(
        "/api/ai-intelligence/golden-set-review/cases/" + encodeURIComponent(c.caseId),
        {
          method: "POST",
          body: {
            reviewStatus: status,
            humanLabels: labels,
            notes: (document.getElementById("gsrNotes") || {}).value || null,
            secondReviewRequired: !!(document.getElementById("gsrSecond") || {}).checked,
          },
        }
      );
      if (!res.data || res.data.ok !== true) {
        var msg = (res.data && res.data.message) || "Save failed";
        if (res.data && res.data.failures && res.data.failures.length) {
          msg += " — " + res.data.failures.join("; ");
        }
        setState(msg, true);
        busy = false;
        return;
      }
      renderProgress(res.data.progress);
      if (res.data.learning) renderLearning(res.data.learning);
      setState("Saved " + status + " for " + c.caseId + ". External assistance was not auto-applied.", false);
      await loadQueue(false);
      var next = Math.min(currentIndex + 1, queueCases.length - 1);
      for (var i = currentIndex + 1; i < queueCases.length; i++) {
        if (queueCases[i].reviewStatus === "UNREVIEWED") {
          next = i;
          break;
        }
      }
      busy = false;
      await openCase(next);
    } catch (err) {
      busy = false;
      setState("SERVER_ERROR: " + (err && err.message ? err.message : String(err)), true);
    }
  }

  async function loadQueue(selectFirst) {
    var res = await fetchJson("/api/ai-intelligence/golden-set-review/queue" + filterQuery());
    if (res.status === 0) {
      setState("SERVER_ERROR: network failure contacting Golden Set Review API.", true);
      return false;
    }
    if (res.status === 401) {
      setState("Sign in required to load Golden Set Review queue. (AUTH_REQUIRED)", true);
      return false;
    }
    if (res.status === 403) {
      setState(
        "Access denied. Golden Set Review is for founder/admin validation only. (ACCESS_DENIED)",
        true
      );
      return false;
    }
    if (res.status === 404) {
      var code404 = (res.data && (res.data.code || res.data.error)) || "";
      if (code404 === "GOLDEN_SET_CANDIDATES_MISSING") {
        setState("Candidate file missing. (CANDIDATE_FILE_MISSING)", true);
        return false;
      }
      setState(
        "Golden Set Review queue API route not found (404). Restart Dealality from C:\\Dev\\deal-capture-proxy. (QUEUE_ROUTE_MISSING)",
        true
      );
      return false;
    }
    if (res.status >= 500) {
      var code500 = (res.data && (res.data.code || res.data.error)) || "SERVER_ERROR";
      if (code500 === "GOLDEN_SET_CANDIDATES_MISSING") {
        setState("Candidate file missing. (CANDIDATE_FILE_MISSING)", true);
        return false;
      }
      if (code500 === "GOLDEN_SET_CANDIDATES_INVALID" || code500 === "CANDIDATE_FILE_INVALID") {
        setState(
          "Candidate file could not be parsed or failed schema checks. (CANDIDATE_FILE_INVALID)",
          true
        );
        return false;
      }
      setState(((res.data && res.data.message) || "Server error") + " (SERVER_ERROR)", true);
      return false;
    }
    if (!res.data || res.data.ok !== true) {
      setState(((res.data && res.data.message) || "Unexpected queue response") + " (SERVER_ERROR)", true);
      return false;
    }
    taxonomies = res.data.taxonomies || {};
    taxonomyHelp = res.data.taxonomyHelp || taxonomyHelp;
    queueCases = (res.data.queue && res.data.queue.cases) || [];
    renderProgress(res.data.progress);
    updateStateTabs(res.data.progress);
    renderLearning(res.data.learning);
    renderNeeded(res.data.coverageNeeded);
    renderHowTo();
    document.getElementById("gsrFilters").hidden = false;
    document.getElementById("gsrToolbar").hidden = false;
    document.getElementById("gsrDisclosure").hidden = false;
    if (!queueCases.length) {
      var st = document.getElementById("gsrFiltState");
      var isActive = !st || st.value === "active" || !st.value;
      if (isActive && res.data.progress && res.data.progress.ZERO_ACTIVE_COMPLETE_STATE) {
        setState(
          "All Golden Set review work is complete.\nCompleted and invalidated cases remain available for audit.",
          false
        );
      } else {
        setState("No cases in this review-state bucket. (QUEUE_EMPTY)", false);
      }
      document.getElementById("gsrMain").hidden = true;
      document.getElementById("gsrState").hidden = false;
      return true;
    }
    fillFilterOptions(queueCases);
    document.getElementById("gsrMain").hidden = false;
    document.getElementById("gsrState").hidden = true;
    var accAll = document.getElementById("gsrAcceptAssistedAll");
    if (accAll) {
      var hasAssisted = queueCases.some(function (c) {
        return c.assistedProposalAvailable && c.reviewStatus === "UNREVIEWED";
      });
      accAll.hidden = !hasAssisted;
    }
    renderQueue();
    if (selectFirst !== false && queueCases.length) await openCase(0);
    return true;
  }

  function bindFilters() {
    ["gsrFiltState", "gsrFiltProvider", "gsrFiltLang", "gsrFiltGeo", "gsrFiltStatus", "gsrHardOnly", "gsrFiltAssisted"].forEach(
      function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        el.addEventListener("change", function () {
          loadQueue(true);
        });
      }
    );
    var five = document.getElementById("gsrCopyFive");
    if (five) five.onclick = copyNextFive;
    var exp = document.getElementById("gsrExport");
    if (exp) exp.onclick = exportReviewed;
    var expAll = document.getElementById("gsrExportAll");
    if (expAll) {
      expAll.onclick = function () {
        exportBulk("ALL", "json");
      };
    }
    var exp10 = document.getElementById("gsrExportNext10");
    if (exp10) {
      exp10.onclick = function () {
        exportBulk("NEXT_10", "json");
      };
    }
    var expFilt = document.getElementById("gsrExportFiltered");
    if (expFilt) {
      expFilt.onclick = function () {
        exportBulk("FILTERED_CURRENT_VIEW", "json");
      };
    }
    var expPkt = document.getElementById("gsrExportPackets");
    if (expPkt) expPkt.onclick = exportPackets;
    var expTpl = document.getElementById("gsrExportTemplate");
    if (expTpl) expTpl.onclick = exportTemplate;
    var impBtn = document.getElementById("gsrImportBtn");
    var impFile = document.getElementById("gsrImportFile");
    if (impBtn && impFile) {
      impBtn.onclick = function () {
        impFile.click();
      };
      impFile.onchange = function () {
        var f = impFile.files && impFile.files[0];
        if (f) previewImportFile(f);
        impFile.value = "";
      };
    }
    var acc = document.getElementById("gsrAcceptAssistedAll");
    if (acc) acc.onclick = acceptAllAssisted;
  }

  document.addEventListener("keydown", function (e) {
    if (!queueCases.length || busy) return;
    var tag = (e.target && e.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
    if (e.key === "c" || e.key === "C") {
      e.preventDefault();
      submitReview("CONFIRMED");
    } else if (e.key === "d" || e.key === "D") {
      e.preventDefault();
      submitReview("DEFERRED");
    } else if (e.key === "ArrowLeft") {
      e.preventDefault();
      openCase(Math.max(0, currentIndex - 1));
    } else if (e.key === "ArrowRight") {
      e.preventDefault();
      openCase(Math.min(queueCases.length - 1, currentIndex + 1));
    }
  });

  async function boot() {
    bindFilters();
    await loadQueue(true);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
