/**
 * Operator AI Intelligence customer UI V2.
 * Mirrors Brand AI layout: rich insight tiles, provider comparison, competitive context.
 * Certified Presence + Questions Missing + client-promoted Competitive Gaps only.
 */
(function () {
  var STORAGE_OPERATOR = "aiv_op_selected_operator";
  var STORAGE_PROVIDER = "aiv_op_provider";
  var STORAGE_TAB = "aiv_op_tab";

  var state = {
    tab: "executive",
    operatorId: null,
    provider: "all",
    universe: null,
    payload: null,
    requestGeneration: 0,
    loadAbort: null,
    infoCopy: {},
  };

  function $(id) {
    return document.getElementById(id);
  }

  function esc(str) {
    return window.AiVisibilityUi ? window.AiVisibilityUi.escapeHtml(String(str || "")) : String(str || "");
  }

  function infoTooltip(label, copy) {
    if (!copy) return "";
    return (
      '<span class="info-tooltip aiv-col-info">' +
      '<span class="info-icon" role="button" tabindex="0" aria-label="About ' +
      esc(label) + '">' +
      '<svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg>' +
      "</span>" +
      '<div class="tooltip-content" hidden><strong>' +
      esc(label) + "</strong><br>" + esc(copy) +
      "</div></span>"
    );
  }

  function showState(which) {
    ["aivOpStateLoading", "aivOpStateEmpty", "aivOpStateError", "aivOpStateSuccess"].forEach(function (id) {
      var el = $(id);
      if (el) el.hidden = id !== which;
    });
  }

  function setActiveTab(tab) {
    state.tab = tab;
    try { localStorage.setItem(STORAGE_TAB, tab); } catch (_) {}
    var execTab = $("aivOpTabExecutive");
    var detailTab = $("aivOpTabDetail");
    if (execTab) {
      execTab.classList.toggle("active", tab === "executive");
      execTab.setAttribute("aria-selected", tab === "executive" ? "true" : "false");
    }
    if (detailTab) {
      detailTab.classList.toggle("active", tab === "detail");
      detailTab.setAttribute("aria-selected", tab === "detail" ? "true" : "false");
    }
    var execPanel = $("aivOpExecutivePanel");
    var detailPanel = $("aivOpDetailPanel");
    if (execPanel) execPanel.hidden = tab !== "executive";
    if (detailPanel) detailPanel.hidden = tab !== "detail";
  }

  function fillOperatorSelect(operators) {
    var sel = $("aivOpOperator");
    if (!sel) return;
    sel.innerHTML = (operators || [])
      .map(function (op) {
        return '<option value="' + esc(op.operatorId) + '">' + esc(op.name) + "</option>";
      })
      .join("");
    if (state.operatorId) sel.value = state.operatorId;
  }

  // --- KPI RENDERING (with helper text + status indicator) ---

  function presenceStatus(val) {
    if (!val || val === "—") return "neutral";
    var num = parseFloat(val);
    if (isNaN(num)) return "neutral";
    if (num >= 50) return "good";
    if (num >= 25) return "warning";
    return "critical";
  }

  function kpiStatus(label, value) {
    if (label === "AI Presence") return presenceStatus(value);
    if (label === "Competitive Gaps") {
      var n = parseInt(value, 10);
      if (n === 0) return "good";
      if (n <= 2) return "warning";
      return "critical";
    }
    if (label === "Questions Missing") {
      var m = parseInt(value, 10);
      if (m === 0) return "good";
      if (m <= 3) return "warning";
      return "critical";
    }
    return "neutral";
  }

  function renderKpis(containerId, kpis, infoCopy) {
    var el = $(containerId);
    if (!el || !kpis) return;
    var items = [
      { label: "AI Presence", value: kpis.aiPresence?.display || "—", info: infoCopy.aiPresence },
      { label: "Owner Intents Monitored", value: kpis.ownerIntentsMonitored?.display || "—", info: infoCopy.ownerIntent },
      { label: "Questions Missing", value: kpis.questionsMissing?.display || "—", info: infoCopy.questionsMissing },
      { label: "Competitive Gaps", value: kpis.competitiveGaps?.display || "—", info: infoCopy.competitiveGap },
      { label: "Provider Agreement", value: kpis.providerAgreement?.display || "—", info: infoCopy.providerDisagreement },
    ];
    el.innerHTML = items
      .map(function (item) {
        var status = kpiStatus(item.label, item.value);
        return (
          '<article class="aiv-kpi aiv-kpi--' + status + '">' +
          '<h3>' + esc(item.label) + infoTooltip(item.label, item.info) + '</h3>' +
          '<div class="aiv-value">' + esc(item.value) + '</div>' +
          (item.info ? '<div class="aiv-meta">' + esc(item.info) + '</div>' : '') +
          "</article>"
        );
      })
      .join("");
  }

  // --- HEADER ---

  function renderHeader(payload) {
    var el = $("aivOpHeader");
    if (!el || !payload?.operator) return;
    var op = payload.operator;
    el.innerHTML =
      '<div class="aiv-op-header__main">' +
      "<h2>" + esc(op.name) + "</h2>" +
      '<div class="aiv-op-header__meta">' +
      (op.operatorModel ? '<span class="aiv-chip">' + esc(op.operatorModel) + "</span>" : "") +
      (op.monitoredScope ? '<span class="aiv-chip">' + esc(op.monitoredScope) + "</span>" : "") +
      '<span class="aiv-chip">' + esc(payload.providerLabel || "") + "</span>" +
      "</div>" +
      (op.insufficientOperatorEvidence
        ? '<div class="aiv-op-insufficient-banner"><p>' + esc(op.insufficientEvidenceCopy || "") + "</p></div>"
        : "") +
      "</div>";
  }

  // --- EXECUTIVE FINDINGS (rich insight tiles) ---

  var CATEGORY_STYLES = {
    COMPETITIVE_VISIBILITY_GAP: { icon: "⚠", color: "#ef4444", label: "Competitive Visibility Gap" },
    PROVIDER_DISAGREEMENT: { icon: "⇄", color: "#f59e0b", label: "Provider Disagreement" },
    OWNER_DECISION_COVERAGE: { icon: "◎", color: "#8b5cf6", label: "Owner Decision Coverage" },
    PRESENCE: { icon: "✓", color: "#10b981", label: "Presence" },
  };

  function renderExecutiveFindings(payload) {
    var section = $("aivOpExecutiveFindings");
    if (!section) return;
    var findings = payload.executive?.findings || [];
    if (!findings.length) {
      section.innerHTML = '<p class="aiv-empty">No executive findings for the selected scope.</p>';
      return;
    }
    section.innerHTML =
      '<h2 class="aiv-section-title">Intelligence Findings</h2>' +
      '<div class="aiv-insights-row">' +
      findings.map(function (f) {
        var cat = CATEGORY_STYLES[f.category] || { icon: "•", color: "#64748b", label: f.category };
        return (
          '<article class="aiv-insight-box aiv-insight-box--operator">' +
          '<div class="aiv-insight-box__badge" style="border-left: 3px solid ' + cat.color + '">' +
          '<span class="aiv-insight-box__icon">' + cat.icon + '</span>' +
          '<span class="aiv-insight-box__category">' + esc(cat.label) + '</span>' +
          '</div>' +
          '<p class="aiv-insight-finding">' + esc(f.finding || "") + '</p>' +
          (f.evidence
            ? '<p class="aiv-insight-meta"><span class="aiv-insight-evidence-label">Evidence:</span> ' + esc(f.evidence) + '</p>'
            : '') +
          '</article>'
        );
      }).join("") +
      '</div>';
  }

  // --- PROVIDER COMPARISON SUMMARY ---

  function renderProviderSummary(payload) {
    var section = $("aivOpProviderSummary");
    if (!section) return;
    var rows = payload.detail?.ownerIntentRows || [];
    if (!rows.length) { section.innerHTML = ""; return; }

    var providerStats = {};
    rows.forEach(function (row) {
      (row.providerCoverage || []).forEach(function (pc) {
        if (!providerStats[pc.providerLabel]) {
          providerStats[pc.providerLabel] = { present: 0, absent: 0, total: 0 };
        }
        providerStats[pc.providerLabel].total += 1;
        if (pc.present) providerStats[pc.providerLabel].present += 1;
        else providerStats[pc.providerLabel].absent += 1;
      });
    });

    var providers = Object.keys(providerStats);
    if (!providers.length) { section.innerHTML = ""; return; }

    section.innerHTML =
      '<h2 class="aiv-section-title">Provider Presence Overview</h2>' +
      '<div class="aiv-op-provider-grid">' +
      providers.map(function (name) {
        var s = providerStats[name];
        var rate = s.total ? Math.round((s.present / s.total) * 100) : 0;
        var status = rate >= 50 ? "good" : rate >= 25 ? "warning" : "critical";
        return (
          '<div class="aiv-op-provider-card aiv-op-provider-card--' + status + '">' +
          '<div class="aiv-op-provider-card__name">' + esc(name) + '</div>' +
          '<div class="aiv-op-provider-card__rate">' + rate + '% present</div>' +
          '<div class="aiv-op-provider-card__detail">' +
          esc(s.present + " of " + s.total + " scenarios") +
          '</div>' +
          '</div>'
        );
      }).join("") +
      '</div>';
  }

  // --- COMPETITIVE POSITION SUMMARY ---

  function renderCompetitiveSummary(payload) {
    var section = $("aivOpCompetitiveSummary");
    if (!section) return;
    var rows = payload.detail?.ownerIntentRows || [];
    var promoted = rows.filter(function (r) { return r.competitiveGap?.clientPromoted; });

    if (!promoted.length) {
      section.innerHTML =
        '<div class="aiv-op-competitive-summary__empty">' +
        '<span class="aiv-op-competitive-good">✓</span> ' +
        'No certified competitive gaps in the selected provider scope.' +
        '</div>';
      return;
    }

    var competitorNames = [];
    promoted.forEach(function (r) {
      (r.relevantComparableOperators || []).forEach(function (name) {
        if (competitorNames.indexOf(name) === -1) competitorNames.push(name);
      });
    });

    section.innerHTML =
      '<h2 class="aiv-section-title">Your Competitive Position</h2>' +
      '<div class="aiv-op-competitive-alert">' +
      '<div class="aiv-op-competitive-alert__count">' +
      '<span class="aiv-op-competitive-alert__num">' + promoted.length + '</span>' +
      ' certified competitive gap' + (promoted.length > 1 ? 's' : '') +
      '</div>' +
      '<p class="aiv-op-competitive-alert__desc">' +
      'In ' + promoted.length + ' owner-decision context' + (promoted.length > 1 ? 's' : '') +
      ', comparable operators appear while you are absent.' +
      '</p>' +
      (competitorNames.length
        ? '<p class="aiv-op-competitive-alert__who">' +
          '<strong>Operators appearing:</strong> ' + esc(competitorNames.join(", ")) +
          '</p>'
        : '') +
      '<button type="button" class="aiv-btn-text" id="aivOpGotoDetail">View in Detailed View →</button>' +
      '</div>';

    $("aivOpGotoDetail")?.addEventListener("click", function () {
      setActiveTab("detail");
    });
  }

  // --- EXECUTIVE TAB ASSEMBLY ---

  function renderExecutive(payload) {
    renderKpis("aivOpKpiRow", payload.kpis, payload.infoCopy || state.infoCopy);
    renderExecutiveFindings(payload);
    renderProviderSummary(payload);
    renderCompetitiveSummary(payload);
  }

  // --- DETAILED VIEW: INTENT TABLE (enhanced) ---

  function presencePill(display) {
    if (display === "Present") return '<span class="aiv-pill aiv-pill--present">Present</span>';
    if (display === "Absent") return '<span class="aiv-pill aiv-pill--absent">Absent</span>';
    return '<span class="aiv-pill aiv-pill--na">' + esc(display || "—") + '</span>';
  }

  function gapBadge(gap) {
    if (!gap) return "—";
    if (gap.clientPromoted) return '<span class="aiv-badge aiv-badge--gap">Certified Gap</span>';
    if (gap.display === "No certified gap") return '<span class="aiv-badge aiv-badge--clear">No certified gap</span>';
    if (gap.display === "Not applicable") return '<span class="aiv-badge aiv-badge--na">N/A</span>';
    return '<span class="aiv-badge aiv-badge--context">' + esc(gap.display || "—") + '</span>';
  }

  function providerMiniPills(coverage) {
    if (!coverage || !coverage.length) return "";
    return '<div class="aiv-provider-pills">' +
      coverage.map(function (p) {
        var cls = p.present ? "aiv-mpill--present" : "aiv-mpill--absent";
        return '<span class="aiv-mpill ' + cls + '" title="' + esc(p.providerLabel) + ': ' + esc(p.display) + '">' +
          esc(p.providerLabel.substring(0, 3)) + '</span>';
      }).join("") +
      '</div>';
  }

  function peerChips(operators) {
    if (!operators || !operators.length) return "";
    return '<div class="aiv-peer-chips">' +
      operators.map(function (name) {
        return '<span class="aiv-chip aiv-chip--peer">' + esc(name) + '</span>';
      }).join("") +
      '</div>';
  }

  function renderExpandedDetail(row, infoCopy) {
    var parts = [];
    if (row.decisionContext) {
      parts.push('<p class="aiv-owner-intent-context__line"><strong>Decision Context:</strong> ' + esc(row.decisionContext) + '</p>');
    }
    if (row.competitiveGap?.clientPromoted) {
      parts.push('<p class="aiv-owner-intent-context__line aiv-owner-intent-context__line--alert">Your operator was absent while one or more directly comparable operators appeared in the same owner-decision context.</p>');
    }
    if ((row.relevantComparableOperators || []).length) {
      parts.push(
        '<p class="aiv-owner-intent-context__line"><strong>Relevant Comparable Operators:</strong> ' +
        esc(row.relevantComparableOperators.join(", ")) +
        infoTooltip("Relevant Comparable Operators", infoCopy.relevantComparableOperators) + '</p>'
      );
    }
    if ((row.observedCompetitors || []).length) {
      parts.push(
        '<p class="aiv-owner-intent-context__line"><strong>Observed Competitors:</strong> ' +
        esc(row.observedCompetitors.map(function (c) { return c.name; }).join(", ")) +
        ' <em>(observed context — not all are direct competitors)</em></p>'
      );
    }
    if ((row.providerCoverage || []).length) {
      parts.push(
        '<p class="aiv-owner-intent-context__line"><strong>Provider Presence:</strong> ' +
        esc(row.providerCoverage.map(function (p) { return p.providerLabel + ": " + p.display; }).join(" · ")) + '</p>'
      );
    }
    if (row.detailNotes) {
      parts.push('<p class="aiv-owner-intent-context__line aiv-owner-intent-context__line--note">' + esc(row.detailNotes) + '</p>');
    }
    parts.push('<p class="aiv-owner-intent-context__line"><strong>Evidence count:</strong> ' + esc(String(row.evidenceCount || 0)) + '</p>');
    return (
      '<tr class="aiv-op-expand-row" id="' + esc(row._expandId) + '" hidden>' +
      '<td colspan="7"><div class="aiv-owner-intent-context aiv-owner-intent-context--expanded">' +
      parts.join("") + '</div></td></tr>'
    );
  }

  function rowStatusClass(row) {
    if (row.competitiveGap?.clientPromoted) return "aiv-op-row--gap";
    if (row.missing?.display === "Missing") return "aiv-op-row--missing";
    if (row.yourPresence?.display === "Present") return "aiv-op-row--present";
    if (row.yourPresence?.display === "Not applicable") return "aiv-op-row--na";
    return "";
  }

  function renderIntentTable(payload) {
    var body = $("aivOpIntentBody");
    if (!body) return;
    var rows = payload.detail?.ownerIntentRows || [];
    var infoCopy = payload.infoCopy || state.infoCopy;
    body.innerHTML = rows
      .map(function (row, idx) {
        var expandId = "aiv-op-expand-" + idx;
        row._expandId = expandId;
        return (
          '<tr class="' + rowStatusClass(row) + '">' +
          '<td><strong>' + esc(row.ownerIntent) + '</strong>' + peerChips(row.relevantComparableOperators) + '</td>' +
          '<td>' + presencePill(row.yourPresence?.display) + '</td>' +
          '<td>' + esc(row.missing?.display || "—") + '</td>' +
          '<td>' + esc(row.peerPresentGaps?.display ?? "0") + '</td>' +
          '<td>' + gapBadge(row.competitiveGap) + '</td>' +
          '<td>' + esc(row.providerDisagreement?.display || "—") +
          providerMiniPills(row.providerCoverage) + '</td>' +
          '<td><button type="button" class="aiv-btn-text aiv-op-expand-btn" aria-expanded="false" aria-controls="' +
          esc(expandId) + '" data-expand="' + esc(expandId) + '">Details</button></td>' +
          '</tr>' +
          renderExpandedDetail(row, infoCopy)
        );
      })
      .join("");

    body.querySelectorAll(".aiv-op-expand-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var targetId = btn.getAttribute("data-expand");
        var row = document.getElementById(targetId);
        if (!row) return;
        var open = btn.getAttribute("aria-expanded") === "true";
        row.hidden = open;
        btn.setAttribute("aria-expanded", open ? "false" : "true");
        btn.textContent = open ? "Details" : "Hide";
      });
    });
  }

  // --- PROVIDER DISAGREEMENT PANEL ---

  function renderDisagreementPanel(payload) {
    var section = $("aivOpDisagreementPanel");
    var body = $("aivOpDisagreementBody");
    if (!section || !body) return;
    var rows = (payload.detail?.ownerIntentRows || []).filter(function (r) {
      return r.providerDisagreement?.hasDisagreement;
    });
    if (!rows.length) {
      section.hidden = true;
      return;
    }
    section.hidden = false;
    body.innerHTML =
      '<p class="aiv-section-intro">AI providers disagree on your visibility in these owner-decision contexts. This means some providers show you while others do not.</p>' +
      '<div class="aiv-op-disagreement-cards">' +
      rows.map(function (row) {
        var present = (row.providerCoverage || []).filter(function (p) { return p.present; });
        var absent = (row.providerCoverage || []).filter(function (p) { return !p.present; });
        return (
          '<div class="aiv-op-disagreement-card">' +
          '<h3>' + esc(row.ownerIntent) + '</h3>' +
          '<div class="aiv-op-disagreement-card__providers">' +
          (present.length ? '<span class="aiv-op-dg-present">Present on: ' + esc(present.map(function (p) { return p.providerLabel; }).join(", ")) + '</span>' : '') +
          (absent.length ? '<span class="aiv-op-dg-absent">Absent on: ' + esc(absent.map(function (p) { return p.providerLabel; }).join(", ")) + '</span>' : '') +
          '</div>' +
          '</div>'
        );
      }).join("") +
      '</div>';
  }

  // --- QUESTIONS MISSING (enhanced with opportunity framing) ---

  function renderWatchlist(payload) {
    var body = $("aivOpWatchlistBody");
    if (!body) return;
    var rows = payload.detail?.questionsMissingWatchlist || [];
    if (!rows.length) {
      body.innerHTML = '<p class="aiv-empty">No Questions Missing for this scope — your operator appears across all comparable monitored scenarios.</p>';
      return;
    }
    body.innerHTML =
      '<p class="aiv-section-intro">These are owner-decision scenarios where you are absent across all monitored providers. Each represents a potential visibility opportunity.</p>' +
      '<div class="aiv-op-qm-cards">' +
      rows.map(function (row) {
        var severity = (row.missingProviders || []).length >= 3 ? "critical" : "warning";
        return (
          '<div class="aiv-op-qm-card aiv-op-qm-card--' + severity + '">' +
          '<div class="aiv-op-qm-card__header">' +
          '<h3>' + esc(row.ownerIntent) + '</h3>' +
          '<span class="aiv-op-qm-severity aiv-op-qm-severity--' + severity + '">' +
          (severity === "critical" ? "All providers" : "Multiple providers") +
          '</span>' +
          '</div>' +
          '<p class="aiv-op-qm-card__context">' + esc(row.decisionContext) + '</p>' +
          '<div class="aiv-op-qm-card__detail">' +
          '<span><strong>Missing on:</strong> ' + esc((row.missingProviders || []).join(", ") || "—") + '</span>' +
          (row.relevantOperatorsPresent?.length
            ? '<span><strong>Operators present:</strong> ' + esc(row.relevantOperatorsPresent.join(", ")) + '</span>'
            : '') +
          '</div>' +
          '</div>'
        );
      }).join("") +
      '</div>';
  }

  // --- COMPETITIVE CONTEXT PANEL ---

  function renderCompetitiveContext(payload) {
    var section = $("aivOpCompetitiveContext");
    var body = $("aivOpCompetitiveContextBody");
    if (!section || !body) return;
    var rows = payload.detail?.ownerIntentRows || [];
    var competitors = {};
    rows.forEach(function (row) {
      (row.observedCompetitors || []).forEach(function (c) {
        if (!competitors[c.name]) competitors[c.name] = { count: 0, scenarios: [] };
        competitors[c.name].count += 1;
        if (competitors[c.name].scenarios.length < 3) {
          competitors[c.name].scenarios.push(row.ownerIntent);
        }
      });
    });
    var sorted = Object.keys(competitors).sort(function (a, b) {
      return competitors[b].count - competitors[a].count;
    });
    if (!sorted.length) {
      section.hidden = true;
      return;
    }
    section.hidden = false;
    body.innerHTML =
      '<p class="aiv-section-intro">These operators and companies appeared in owner-decision contexts during monitoring. Not all are direct competitors — this shows the competitive landscape owners encounter.</p>' +
      '<div class="aiv-op-competitor-list">' +
      sorted.slice(0, 10).map(function (name) {
        var info = competitors[name];
        return (
          '<div class="aiv-op-competitor-item">' +
          '<div class="aiv-op-competitor-item__name">' + esc(name) + '</div>' +
          '<div class="aiv-op-competitor-item__count">Observed in ' + info.count + ' context' + (info.count > 1 ? 's' : '') + '</div>' +
          '<div class="aiv-op-competitor-item__scenarios">' +
          esc(info.scenarios.join(", ")) +
          (info.count > 3 ? " + more" : "") +
          '</div>' +
          '</div>'
        );
      }).join("") +
      '</div>';
  }

  // --- DETAIL TAB ASSEMBLY ---

  function renderDetail(payload) {
    renderKpis("aivOpDetailKpiRow", payload.kpis, payload.infoCopy || state.infoCopy);
    renderIntentTable(payload);
    renderDisagreementPanel(payload);
    renderWatchlist(payload);
    renderCompetitiveContext(payload);
  }

  // --- PAINT ---

  function paintPayload(payload) {
    state.payload = payload;
    state.infoCopy = payload.infoCopy || {};
    renderHeader(payload);
    renderExecutive(payload);
    renderDetail(payload);
    showState("aivOpStateSuccess");
    setActiveTab(state.tab);
  }

  // --- AUTH ---

  async function authFetchOp(url, opts) {
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      try {
        return await auth.authFetch(url, Object.assign({ waitForLogin: true, maxWaitMs: 15000 }, opts || {}));
      } catch (authErr) {
        if (authErr && /abort/i.test(authErr.message)) throw authErr;
        throw new Error(authErr?.message || "Please sign in to access Operator AI Intelligence.");
      }
    }
    var headers = { Accept: "application/json" };
    try {
      var ws = localStorage.getItem("dealality_active_workspace");
      if (ws) headers["X-Dealality-Active-Workspace"] = ws;
    } catch (_) {}
    return fetch(url, Object.assign({ headers: headers, credentials: "include" }, opts || {}));
  }

  // --- DATA LOADING ---

  async function loadUniverse() {
    var res = await authFetchOp("/api/ai-visibility/operator/universe");
    var data = await res.json().catch(function () { return {}; });
    if (!res.ok || data.success === false) {
      if (res.status === 401 || res.status === 403) {
        throw new Error("Please sign in to access Operator AI Intelligence.");
      }
      throw new Error("Unable to load Operator AI Intelligence. Please try again.");
    }
    state.universe = data;
    fillOperatorSelect(data.operators || []);
    if (!state.operatorId && data.operators?.length) {
      state.operatorId = data.operators[0].operatorId;
    }
    return data;
  }

  async function loadCustomerPayload() {
    if (state.loadAbort && typeof state.loadAbort.abort === "function") {
      try { state.loadAbort.abort(); } catch (_) {}
    }
    state.requestGeneration += 1;
    var generation = state.requestGeneration;
    var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    state.loadAbort = controller;

    showState("aivOpStateLoading");
    var url =
      "/api/ai-visibility/operator/" +
      encodeURIComponent(state.operatorId || "") +
      "/customer?provider=" +
      encodeURIComponent(state.provider || "all");
    var res = await authFetchOp(url, { signal: controller ? controller.signal : undefined });
    if (generation !== state.requestGeneration) return;
    var data = await res.json().catch(function () { return {}; });
    if (!res.ok || data.success === false) {
      var msg = "Unable to load Operator AI Intelligence. Please try again.";
      if (res.status === 401 || res.status === 403) msg = "Session expired. Please sign in again.";
      else if (res.status === 404) msg = "Operator data not available.";
      $("aivOpErrorMessage").textContent = msg;
      showState("aivOpStateError");
      return;
    }
    paintPayload(data);
  }

  // --- BOOTSTRAP ---

  async function bootstrap() {
    showState("aivOpStateLoading");
    try {
      await loadUniverse();
      await loadCustomerPayload();
    } catch (err) {
      var errMsg = (err && err.message) || "";
      if (/log in|sign in/i.test(errMsg)) {
        $("aivOpErrorMessage").textContent = errMsg;
      } else if (/abort/i.test(errMsg)) {
        return;
      } else {
        $("aivOpErrorMessage").textContent = "Unable to load Operator AI Intelligence. Please try again.";
      }
      showState("aivOpStateError");
    }
  }

  function bindControls() {
    $("aivOpTabExecutive")?.addEventListener("click", function () { setActiveTab("executive"); });
    $("aivOpTabDetail")?.addEventListener("click", function () { setActiveTab("detail"); });
    $("aivOpApply")?.addEventListener("click", function () {
      state.operatorId = $("aivOpOperator")?.value || state.operatorId;
      state.provider = $("aivOpProvider")?.value || "all";
      try {
        localStorage.setItem(STORAGE_OPERATOR, state.operatorId || "");
        localStorage.setItem(STORAGE_PROVIDER, state.provider || "all");
      } catch (_) {}
      loadCustomerPayload();
    });
    $("aivOpReset")?.addEventListener("click", function () {
      state.provider = "all";
      state.tab = "executive";
      if ($("aivOpProvider")) $("aivOpProvider").value = "all";
      if (state.universe?.operators?.length) {
        state.operatorId = state.universe.operators[0].operatorId;
        if ($("aivOpOperator")) $("aivOpOperator").value = state.operatorId;
      }
      setActiveTab("executive");
      loadCustomerPayload();
    });
    $("aivOpRetry")?.addEventListener("click", bootstrap);
  }

  document.addEventListener("DOMContentLoaded", function () {
    try {
      state.operatorId = localStorage.getItem(STORAGE_OPERATOR) || null;
      state.provider = localStorage.getItem(STORAGE_PROVIDER) || "all";
      state.tab = localStorage.getItem(STORAGE_TAB) || "executive";
    } catch (_) {}
    if ($("aivOpProvider")) $("aivOpProvider").value = state.provider;
    bindControls();
    bootstrap();
  });
})();
