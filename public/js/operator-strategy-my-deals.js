/**
 * My Deals — Operator Strategy tab (cross-deal pipeline table).
 * API calls use DealalityMemberstackAuth.fetchMyDealsApi (owner JWT required — Batch 1 security).
 */
(function (global) {
  "use strict";

  var MAX_DEALS_PER_LOAD = 40;
  var FETCH_CONCURRENCY = 4;

  var BANNED_PHRASES = [
    "recommended operators",
    "best operators",
    "top operators",
    "preferred operators",
    "dealality recommends",
    "the owner should select",
    "no recommended operators",
  ];

  var state = {
    allRows: [],
    filteredRows: [],
    loading: false,
    loadedOnce: false,
    pendingReload: false,
    lastDealSig: "",
    authError: false,
    partialErrors: 0,
    fatalError: null,
    search: "",
    pipelineFilter: "all",
    dealFilterId: "",
    dealFilterName: "",
    sortColumn: "",
    sortDirection: "asc",
    selectedRowKeys: Object.create(null),
    operatorRequestByKey: Object.create(null),
  };

  var options = {};
  var els = {};
  var moreMenuEl = null;
  /** @type {Promise<void> | null} */
  var pipelineLoadPromise = null;

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fetchApi(url) {
    var fn =
      global.DealalityMemberstackAuth &&
      global.DealalityMemberstackAuth.fetchMyDealsApi
        ? global.DealalityMemberstackAuth.fetchMyDealsApi.bind(
            global.DealalityMemberstackAuth
          )
        : function (u, opts) {
            return fetch(u, opts);
          };
    return fn(url, { method: "GET" }).then(function (r) {
      return r.json().then(function (data) {
        return { ok: r.ok, status: r.status, data: data };
      });
    });
  }

  function postAuthApi(path, body) {
    var fn =
      global.DealalityMemberstackAuth &&
      global.DealalityMemberstackAuth.fetchMyDealsApi
        ? global.DealalityMemberstackAuth.fetchMyDealsApi.bind(
            global.DealalityMemberstackAuth
          )
        : function (u, opts) {
            return fetch(u, opts);
          };
    return fn(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.json().then(function (data) {
        return { ok: r.ok, status: r.status, data: data };
      });
    });
  }

  function normalizeCompanyKey(name) {
    return String(name || "")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  }

  /** CSS modifier for Outreach Status column (Phase 3A). */
  function outreachStatusModifier(row) {
    var label = String((row && row.outreachStatusLabel) || "Not contacted").toLowerCase();
    if (label === "not contacted") return "operator-strategy-outreach-status--none";
    if (label === "request sent") return "operator-strategy-outreach-status--sent";
    if (label === "viewed") return "operator-strategy-outreach-status--viewed";
    if (label === "more info requested") return "operator-strategy-outreach-status--more-info";
    if (label === "accepted") return "operator-strategy-outreach-status--accepted";
    if (label === "declined") return "operator-strategy-outreach-status--declined";
    if (label === "archived") return "operator-strategy-outreach-status--archived";
    if (row && row.odrActive) return "operator-strategy-outreach-status--progress";
    return "";
  }

  function indexOperatorRequests(contacted) {
    state.operatorRequestByKey = Object.create(null);
    (contacted || []).forEach(function (c) {
      if (!c || !c.dealId) return;
      if (c.operatorSetupId) {
        state.operatorRequestByKey[c.dealId + "|op|" + c.operatorSetupId] = c;
      }
      var nameKey = normalizeCompanyKey(c.operatingCompanyName);
      if (nameKey) {
        state.operatorRequestByKey[c.dealId + "|name|" + nameKey] = c;
      }
    });
  }

  function lookupOperatorRequest(row) {
    if (!row || !row.dealId) return null;
    if (row.operatorId && state.operatorRequestByKey[row.dealId + "|op|" + row.operatorId]) {
      return state.operatorRequestByKey[row.dealId + "|op|" + row.operatorId];
    }
    var nameKey = normalizeCompanyKey(row.companyName);
    if (nameKey && state.operatorRequestByKey[row.dealId + "|name|" + nameKey]) {
      return state.operatorRequestByKey[row.dealId + "|name|" + nameKey];
    }
    return null;
  }

  function mergeOperatorRequestIntoRow(row) {
    var odr = lookupOperatorRequest(row);
    if (!odr) {
      row.odrRequestId = null;
      row.odrStatus = null;
      row.outreachStatusLabel = "Not contacted";
      row.odrActive = false;
      return row;
    }
    row.odrRequestId = odr.id || null;
    row.odrStatus = odr.status || null;
    row.outreachStatusLabel = odr.outreachStatusLabel || odr.status || "Request sent";
    row.odrActive = odr.isActiveRequest !== false;
    return row;
  }

  function fetchOperatorRequestsForDealIds(dealIds) {
    if (!dealIds || !dealIds.length) {
      indexOperatorRequests([]);
      return Promise.resolve();
    }
    return postAuthApi("/api/my-deals/operator-requests/by-deals", {
      dealIds: dealIds,
    }).then(function (res) {
      if (!res.ok || !res.data || !res.data.success) {
        return;
      }
      indexOperatorRequests(res.data.contacted || []);
    }).catch(function () {
      /* non-blocking */
    });
  }

  function toast(message, success) {
    if (typeof global.showMyDealsToast === "function") {
      global.showMyDealsToast(message, success);
      return;
    }
    if (typeof global.alert === "function") global.alert(message);
  }

  function findRowByIds(dealId, operatorId) {
    return (state.allRows || []).find(function (r) {
      return r.dealId === dealId && r.operatorId === operatorId;
    });
  }

  function refreshOperatorRequestsAndRender(dealIds) {
    var ids = dealIds && dealIds.length ? dealIds : (state.allRows || []).map(function (r) {
      return r.dealId;
    }).filter(function (id, i, arr) {
      return id && arr.indexOf(id) === i;
    });
    return fetchOperatorRequestsForDealIds(ids.slice(0, 40)).then(function () {
      (state.allRows || []).forEach(mergeOperatorRequestIntoRow);
      applyFiltersAndRender();
    });
  }

  function submitOperatorOutreach(row, ownerNotes) {
    if (!row || !row.dealId || !row.operatorId) {
      toast("Missing deal or operator for outreach.", false);
      return Promise.resolve(false);
    }
    if (row.odrActive) {
      toast("An active operator request already exists for this company.", false);
      return Promise.resolve(false);
    }
    var path =
      "/api/my-deals/" + encodeURIComponent(row.dealId) + "/operator-requests";
    var payload = {
      operatorSetupId: row.operatorId,
      operatingCompanyName: row.companyName,
      alignmentScore: row.alignmentScoreOptional,
      alignmentBand: row.alignmentBand,
      dataConfidence: row.dataConfidence,
      ownerNotes: ownerNotes || "",
    };
    return postAuthApi(path, payload).then(function (res) {
      var data = res.data || {};
      if (res.status === 403) {
        toast(data.message || "You cannot create operator requests for this account.", false);
        return false;
      }
      if (!res.ok && !data.alreadyExists) {
        toast(data.message || data.error || "Could not create operator request.", false);
        return false;
      }
      if (data.alreadyExists) {
        toast("Operator request already active for " + (row.companyName || "this company") + ".", true);
      } else {
        toast("Operator outreach recorded for " + (row.companyName || "this company") + ".", true);
      }
      return refreshOperatorRequestsAndRender([row.dealId]).then(function () {
        return true;
      });
    }).catch(function (err) {
      toast(err.message || "Network error creating operator request.", false);
      return false;
    });
  }

  function promptContactOperator(row) {
    if (!row) return;
    if (row.odrActive) {
      toast("Request already sent — status: " + (row.outreachStatusLabel || row.odrStatus) + ".", true);
      return;
    }
    var company = row.companyName || "this operating company";
    var ok = global.confirm(
      "Send operator outreach request to " + company + "?\n\nThis creates an Operator Deal Request visible in their My Operator Deals workspace."
    );
    if (!ok) return;
    var notes = "";
    if (typeof global.prompt === "function") {
      var entered = global.prompt("Optional notes for the operator team (leave blank to skip):", "");
      if (entered != null) notes = String(entered).trim();
    }
    submitOperatorOutreach(row, notes);
  }

  /** Operator alignment badge class — bands from lib/operator-alignment-scoring-weight-config.js via DcOperatorMatchScoreUi. */
  function getAlignmentScoreClass(score) {
    if (global.DcOperatorMatchScoreUi && typeof global.DcOperatorMatchScoreUi.getAlignmentScoreClass === "function") {
      return global.DcOperatorMatchScoreUi.getAlignmentScoreClass(score);
    }
    if (score == null || score === "") return "";
    var n = Number(score);
    if (!Number.isFinite(n)) return "";
    if (n >= 80) return "match-score-high";
    if (n >= 50) return "match-score-medium";
    if (n >= 25) return "match-score-weak";
    return "match-score-poor";
  }

  function renderScoreCell(row) {
    if (!row || row.alignmentScoreOptional == null || row.alignmentScoreOptional === "") {
      return '<div class="match-score-cell match-score-cell--compact">—</div>';
    }
    var n = Number(row.alignmentScoreOptional);
    if (!Number.isFinite(n)) {
      return '<div class="match-score-cell match-score-cell--compact">—</div>';
    }
    var scoreClass = getAlignmentScoreClass(n);
    var canBreakdown = isLiveOperatorId(row.operatorId) && isLiveOperatorId(row.dealId);
    if (!canBreakdown) {
      return (
        '<div class="match-score-cell match-score-cell--compact">' +
        '<span class="match-score-badge ' +
        esc(scoreClass) +
        '">' +
        esc(n.toFixed(1)) +
        "</span></div>"
      );
    }
    return (
      '<div class="match-score-cell match-score-cell--compact">' +
      '<button type="button" class="match-score-badge operator-strategy-score-btn ' +
      esc(scoreClass) +
      '" data-os-action="score-breakdown" data-deal-id="' +
      esc(row.dealId) +
      '" data-operator-id="' +
      esc(row.operatorId) +
      '" title="View operator alignment score breakdown" aria-label="View operator alignment score breakdown for ' +
      esc(row.companyName || "this operator") +
      '">' +
      esc(n.toFixed(1)) +
      "</button></div>"
    );
  }

  function getBreakdownScoreClass(score) {
    if (global.DcOperatorMatchScoreUi && typeof global.DcOperatorMatchScoreUi.getBreakdownScoreClass === "function") {
      return global.DcOperatorMatchScoreUi.getBreakdownScoreClass(score);
    }
    if (score == null || score === "") return "medium";
    var n = Number(score);
    if (!Number.isFinite(n)) return "medium";
    if (n >= 80) return "high";
    if (n >= 50) return "medium";
    if (n >= 25) return "low";
    return "poor";
  }

  function getOperatorScoreBreakdownModalParts() {
    var modal = document.getElementById("matchedBrandsScoreNewModal");
    var content = document.getElementById("matchedBrandsScoreNewModalContent");
    var titleEl = modal ? modal.querySelector("h2") : null;
    return { modal: modal, content: content, titleEl: titleEl };
  }

  function buildOperatorAlignmentScoreSummary(score, operatorName, details) {
    var op = operatorName ? String(operatorName).trim() : null;
    var opPhrase = op ? op + "'s" : "this operator's";
    var withOp = op ? " with " + op : "";
    var ui = global.DcOperatorMatchScoreUi;
    var factorStrongMin =
      ui && typeof ui.getFactorStrongMin === "function" ? ui.getFactorStrongMin() : 80;
    var factorWeakBelow =
      ui && typeof ui.getFactorWeakBelow === "function" ? ui.getFactorWeakBelow() : 50;
    var tier =
      ui && typeof ui.getNarrativeTier === "function" ? ui.getNarrativeTier(score) : null;
    if (!tier) {
      if (score >= 80) tier = "strong";
      else if (score >= 50) tier = "moderate";
      else if (score >= 25) tier = "weak";
      else tier = "poor";
    }
    var strong = [];
    var weak = [];
    if (details && typeof details === "object") {
      Object.keys(details).forEach(function (k) {
        var d = details[k];
        var s = d && d.score != null && d.score !== "—" ? Number(d.score) : null;
        var lbl = d && d.label ? d.label : k;
        if (s == null || Number.isNaN(s)) return;
        if (s >= factorStrongMin) strong.push(lbl);
        else if (s < factorWeakBelow) weak.push(lbl);
      });
    }
    var strongStr = strong.length ? strong.slice(0, 3).join(", ") : null;
    var weakStr = weak.length ? weak.slice(0, 3).join(", ") : null;
    if (tier === "strong") {
      var p =
        "With a score of " +
        score.toFixed(0) +
        ", your project shows strong alignment signals against " +
        opPhrase +
        " Operator Setup profile across most scored factors. ";
      if (strongStr) {
        p +=
          "The strongest areas are " +
          strongStr +
          "—these indicate overlap between your deal inputs and what this operating company documents in Operator Setup. ";
      }
      p +=
        "This is a promising fit signal for your review set; validate open items before any outreach or term discussions.";
      return p;
    }
    if (tier === "moderate") {
      var p =
        "At " + score.toFixed(0) + ", you have moderate alignment signals" + withOp + ". ";
      if (strongStr) p += "Strengths include " + strongStr + ". ";
      if (weakStr) p += "Main gaps to validate are " + weakStr + ". ";
      p +=
        "Proceed only after confirming geography, scale, structure, and commercial assumptions with the operator team.";
      return p;
    }
    if (tier === "weak") {
      var p =
        "A score of " +
        score.toFixed(0) +
        " indicates notable gaps between your project and " +
        opPhrase +
        " documented scope. ";
      if (weakStr) p += "The largest gaps are in " + weakStr + ". ";
      if (strongStr) p += "Some positives remain (" + strongStr + "), but weak areas may be harder to bridge. ";
      p += "Clarify whether those gaps can be addressed before investing more review time on this company.";
      return p;
    }
    var p =
      "With a score of " +
      score.toFixed(0) +
      ", alignment signals are limited relative to " +
      opPhrase +
      " documented profile. ";
    if (weakStr) p += "Major gaps include " + weakStr + ". ";
    p +=
      "Consider other operating companies in your strategy table where deal inputs and Operator Setup data overlap more clearly.";
    return p;
  }

  function renderOperatorBreakdownHtml(score, operatorName, details) {
    var scoreDisplay = score != null ? Number(score).toFixed(1) : "—";
    var subTitle = operatorName
      ? ' for <span style="color: var(--accent--primary-1);">' + esc(operatorName) + "</span>"
      : "";
    var colorNote =
      ' Score bands: <span style="color: var(--system--green-400);">80–100 = strong</span>, <span style="color: var(--system--orange-400);">50–79 = moderate</span>, <span style="color: var(--system--red-400);">25–49 = weak</span>, <span style="color: #6B2D2D;">0–24 = poor</span>.';
    var html =
      '<div class="modal-section"><h3>Overall Operator Alignment Score: <span style="color: var(--accent--primary-1);">' +
      esc(scoreDisplay) +
      "/100</span>" +
      subTitle +
      "</h3>" +
      '<p style="color: var(--neutral--400); font-size: 14px; line-height: 1.5; margin: 10px 0 0 0;">This score compares what this operating company supports with what your project offers. It weighs nine factors across geography and markets, chain scale, asset and stage fit, deal structure, commercial terms, services, systems and reporting, owner relations, and portfolio relevance.' +
      colorNote +
      " The breakdown below shows how each factor scored and why. Alignment scores highlight fit signals and data gaps—they do not indicate operator approval, availability, or commercial terms.</p></div>";

    if (details && typeof details === "object" && Object.keys(details).length > 0) {
      html += '<div class="modal-section"><h3>Quantitative Breakdown</h3><div class="match-score-breakdown">';
      Object.keys(details).forEach(function (factorKey) {
        var d = details[factorKey];
        if (!d) return;
        var label = d.label || factorKey;
        var weight = d.weight != null ? d.weight : 0;
        var sc =
          d.score != null && d.score !== "—" ? getBreakdownScoreClass(Number(d.score)) : "low";
        var scorePct =
          d.score != null && d.score !== "—"
            ? Math.min(100, Math.max(0, Number(d.score)))
            : 0;
        var setupValue = d.operatorValue || d.brandValue || "—";
        html +=
          '<div class="score-category"><div class="score-category-label">' +
          '<div class="score-factor-heading">' +
          esc(label) +
          "</div>" +
          (weight ? '<div class="score-factor-weight">(Weight: ' + esc(String(weight)) + "%)</div>" : "") +
          "</div>" +
          '<div class="score-category-value"><div class="score-bar"><div class="score-bar-fill ' +
          esc(sc) +
          '" style="width: ' +
          esc(String(scorePct)) +
          '%"></div></div>' +
          '<span class="score-number">' +
          (d.score != null && d.score !== "—" ? esc(String(d.score)) : "—") +
          "</span></div>";
        if (setupValue || d.dealValue || d.note) {
          html +=
            '<div class="score-factor-details">' +
            '<div><strong style="color: var(--neutral--300);">Operator setup:</strong> ' +
            esc(setupValue) +
            "</div>" +
            '<div style="margin-top: 4px;"><strong style="color: var(--neutral--300);">Deal setup:</strong> ' +
            esc(d.dealValue || "—") +
            "</div>" +
            (d.note
              ? '<div style="margin-top: 4px;"><strong style="color: var(--neutral--300);">How match works:</strong> ' +
                esc(d.note) +
                "</div>"
              : "") +
            "</div>";
        }
        html += "</div>";
      });
      html += "</div></div>";
    } else {
      html +=
        '<div class="modal-section"><h3>Quantitative Breakdown</h3><p style="color: var(--neutral--400);">Breakdown details are not available for this operator. This usually means Operator Setup data is incomplete or the deal is missing required intake fields.</p></div>';
    }

    if (score != null && !Number.isNaN(Number(score))) {
      var summary = buildOperatorAlignmentScoreSummary(Number(score), operatorName, details);
      if (summary) {
        html +=
          '<div class="modal-section"><h3>What This Score Means For You</h3><p class="match-score-summary" style="color: var(--neutral--300); font-size: 15px; line-height: 1.6; margin: 0;">' +
          esc(summary) +
          "</p></div>";
      }
    }
    return html;
  }

  function showOperatorMatchScoreBreakdown(dealId, operatorId, companyName) {
    var parts = getOperatorScoreBreakdownModalParts();
    if (!parts.modal || !parts.content) {
      toast("Score breakdown modal is not available on this page.", false);
      return;
    }
    if (!dealId || !operatorId) {
      toast("Missing deal or operator for score breakdown.", false);
      return;
    }
    if (parts.titleEl) parts.titleEl.textContent = "Operator Alignment Score Breakdown";
    parts.content.innerHTML =
      '<div class="modal-section"><p style="color: var(--neutral--400);">Loading breakdown…</p></div>';
    parts.modal.style.display = "block";

    var url =
      "/api/my-deals/" +
      encodeURIComponent(dealId) +
      "/operator-match-score-breakdown?operatorId=" +
      encodeURIComponent(operatorId);
    fetchApi(url)
      .then(function (res) {
        if (res.status === 401 || res.status === 403) {
          parts.content.innerHTML =
            '<div class="modal-section"><p style="color: var(--neutral--400);">Sign in required to view operator alignment breakdown.</p></div>';
          return;
        }
        if (!res.ok || !res.data || !res.data.success) {
          var msg =
            (res.data && (res.data.error || res.data.message)) ||
            "Could not load operator alignment breakdown.";
          parts.content.innerHTML =
            '<div class="modal-section"><p style="color: var(--neutral--400);">' + esc(msg) + "</p></div>";
          return;
        }
        var operatorName = res.data.operatorName || companyName || "Selected operator";
        parts.content.innerHTML = renderOperatorBreakdownHtml(
          res.data.operatorScore,
          operatorName,
          res.data.operatorBreakdownDetails || {}
        );
      })
      .catch(function () {
        parts.content.innerHTML =
          '<div class="modal-section"><p style="color: var(--neutral--400);">An error occurred while loading the breakdown. Please try again.</p></div>';
      });
  }

  function isLiveOperatorId(id) {
    return id && String(id).indexOf("rec") === 0;
  }

  function getDealMeta(dealId) {
    if (typeof options.getDealMeta === "function") {
      return options.getDealMeta(dealId) || { projectName: "", location: "" };
    }
    return { projectName: "", location: "" };
  }

  function flattenCompaniesForDeal(dealId, pack, dealMeta) {
    var rows = [];
    if (!pack || !pack.companiesAvailable) return rows;
    var companies = pack.companiesForConsideration || [];
    companies.forEach(function (c) {
      rows.push({
        dealId: dealId,
        projectName: dealMeta.projectName || "—",
        location: dealMeta.location || "",
        operatorId: c.operatorId || "",
        companyName: c.companyName || c.operatorName || "—",
        parentCompany: c.parentCompany || "",
        alignmentBand: c.alignmentBand || "Insufficient Data",
        alignmentScoreOptional: c.alignmentScoreOptional,
        reviewStatus: c.reviewStatusLabel || "—",
        keyConsideration: c.keyConsideration || "—",
        dataConfidence: c.dataConfidenceLevel || "Not provided",
      });
    });
    return rows;
  }

  function fetchCompaniesForDeal(dealId) {
    var url =
      "/api/operator-alignment-snapshot/" + encodeURIComponent(dealId) + "/companies";
    return fetchApi(url).then(function (pack) {
      if (pack.status === 401 || pack.status === 403) {
        var err = new Error("auth");
        err.auth = true;
        throw err;
      }
      if (!pack.ok || !pack.data || !pack.data.success) {
        return { dealId: dealId, error: true, rows: [] };
      }
      var meta = getDealMeta(dealId);
      if (!meta.projectName && pack.data.dealContext && pack.data.dealContext.dealName) {
        meta.projectName = pack.data.dealContext.dealName;
      }
      return {
        dealId: dealId,
        error: false,
        rows: flattenCompaniesForDeal(dealId, pack.data, meta),
      };
    }).catch(function (err) {
      if (err && err.auth) throw err;
      return { dealId: dealId, error: true, rows: [] };
    });
  }

  function runPool(dealIds, concurrency, worker) {
    var index = 0;
    var results = [];
    var active = 0;

    return new Promise(function (resolve, reject) {
      function next() {
        if (index >= dealIds.length && active === 0) {
          resolve(results);
          return;
        }
        while (active < concurrency && index < dealIds.length) {
          var i = index++;
          var id = dealIds[i];
          active++;
          worker(id)
            .then(function (r) {
              results.push(r);
            })
            .catch(reject)
            .finally(function () {
              active--;
              next();
            });
        }
      }
      next();
    });
  }

  function resolveDealsForPipeline(dealsOverride) {
    if (Array.isArray(dealsOverride) && dealsOverride.length) return dealsOverride;
    return typeof options.getAllDeals === "function" ? options.getAllDeals() || [] : [];
  }

  function dealIdsSignature(deals) {
    return (deals || [])
      .map(function (d) {
        return d && d.id ? String(d.id) : "";
      })
      .filter(function (id) {
        return id.indexOf("rec") === 0;
      })
      .sort()
      .join(",");
  }

  function isPanelVisible() {
    return !!(els.panel && els.panel.style.display !== "none");
  }

  function loadPipeline(force, dealsOverride) {
    if (state.loading) {
      if (force) state.pendingReload = true;
      return pipelineLoadPromise || Promise.resolve();
    }
    if (state.loadedOnce && !force) {
      applyFiltersAndRender();
      return Promise.resolve();
    }

    var deals = resolveDealsForPipeline(dealsOverride);
    var dealIds = deals
      .map(function (d) {
        return d && d.id ? String(d.id) : "";
      })
      .filter(function (id) {
        return id.indexOf("rec") === 0;
      })
      .slice(0, MAX_DEALS_PER_LOAD);

    if (!dealIds.length) {
      state.allRows = [];
      state.loadedOnce = true;
      state.fatalError = null;
      applyFiltersAndRender();
      pipelineLoadPromise = Promise.resolve();
      return pipelineLoadPromise;
    }

    state.loading = true;
    state.pendingReload = false;
    state.fatalError = null;
    state.partialErrors = 0;
    state.authError = false;
    clearRowSelection();
    closeBulkDropdown();
    updateTabCount();
    setVisibility();

    pipelineLoadPromise = runPool(dealIds, FETCH_CONCURRENCY, fetchCompaniesForDeal)
      .then(function (parts) {
        var merged = [];
        parts.forEach(function (p) {
          if (p.error) state.partialErrors += 1;
          merged = merged.concat(p.rows || []);
        });
        state.allRows = merged;
        return fetchOperatorRequestsForDealIds(dealIds);
      })
      .then(function () {
        (state.allRows || []).forEach(mergeOperatorRequestIntoRow);
        state.loadedOnce = true;
        state.lastDealSig = dealIdsSignature(deals);
      })
      .catch(function (err) {
        state.allRows = [];
        state.loadedOnce = true;
        state.lastDealSig = dealIdsSignature(deals);
        if (err && err.auth) {
          state.authError = true;
          state.fatalError =
            "Operator strategy could not be loaded. Please confirm access to the selected deals.";
        } else {
          state.fatalError =
            err.message || "Operator strategy could not be loaded.";
        }
      })
      .finally(function () {
        state.loading = false;
        applyFiltersAndRender();
        if (state.pendingReload) {
          state.pendingReload = false;
          state.loadedOnce = false;
          loadPipeline(true);
        }
      });
    return pipelineLoadPromise;
  }

  function matchesAlignmentFilter(row) {
    var mode = state.pipelineFilter || "all";
    if (mode === "all") return true;
    var band = String(row.alignmentBand || "").toLowerCase();
    if (mode === "strong") return band.indexOf("strong") !== -1;
    if (mode === "moderate") return band.indexOf("moderate") !== -1;
    if (mode === "conditional") return band.indexOf("conditional") !== -1;
    if (mode === "limited") return band.indexOf("limited") !== -1;
    if (mode === "insufficient") return band.indexOf("insufficient") !== -1;
    return true;
  }

  function rowKey(row) {
    return String(row.dealId || "") + "|" + String(row.operatorId || "");
  }

  function isRowSelected(row) {
    return !!state.selectedRowKeys[rowKey(row)];
  }

  function clearRowSelection() {
    state.selectedRowKeys = Object.create(null);
  }

  function getVisibleRowCheckboxes() {
    if (!els.tableBody) return [];
    return Array.prototype.slice.call(
      els.tableBody.querySelectorAll(".operator-strategy-row-checkbox")
    );
  }

  function closeBulkDropdown() {
    if (els.bulkDropdown) els.bulkDropdown.classList.remove("open");
    if (els.bulkActionsBtn) {
      els.bulkActionsBtn.setAttribute("aria-expanded", "false");
    }
  }

  function updateBulkActionsState() {
    var checkboxes = getVisibleRowCheckboxes();
    var checkedCount = 0;
    checkboxes.forEach(function (cb) {
      if (cb.checked) checkedCount += 1;
    });
    if (els.bulkActionsBtn) {
      els.bulkActionsBtn.disabled = checkedCount === 0;
    }
    if (els.selectAllCheckbox && checkboxes.length) {
      els.selectAllCheckbox.checked =
        checkedCount > 0 && checkedCount === checkboxes.length;
      els.selectAllCheckbox.indeterminate =
        checkedCount > 0 && checkedCount < checkboxes.length;
    } else if (els.selectAllCheckbox) {
      els.selectAllCheckbox.checked = false;
      els.selectAllCheckbox.indeterminate = false;
    }
    if (checkedCount === 0) closeBulkDropdown();
  }

  function syncSelectionFromCheckbox(cb) {
    var dealId = cb.getAttribute("data-deal-id") || "";
    var operatorId = cb.getAttribute("data-operator-id") || "";
    var key = dealId + "|" + operatorId;
    if (!dealId && !operatorId) return;
    if (cb.checked) state.selectedRowKeys[key] = true;
    else delete state.selectedRowKeys[key];
  }

  function formatDataConfidence(val) {
    var s = String(val == null ? "" : val).trim();
    if (!s || /^not provided$/i.test(s)) return "Not provided";
    return s;
  }

  function applyFilters() {
    var q = (state.search || "").trim().toLowerCase();
    state.filteredRows = (state.allRows || []).filter(function (row) {
      if (state.dealFilterId && row.dealId !== state.dealFilterId) return false;
      if (!matchesAlignmentFilter(row)) return false;
      if (!q) return true;
      var hay = [
        row.projectName,
        row.location,
        row.companyName,
        row.parentCompany,
        row.alignmentBand,
        row.reviewStatus,
        row.keyConsideration,
        row.outreachStatusLabel,
      ]
        .join(" ")
        .toLowerCase();
      return hay.indexOf(q) !== -1;
    });
  }

  function compareStrings(a, b, dir) {
    var aVal = String(a == null ? "" : a)
      .trim()
      .toLowerCase();
    var bVal = String(b == null ? "" : b)
      .trim()
      .toLowerCase();
    if (aVal < bVal) return dir < 0 ? 1 : -1;
    if (aVal > bVal) return dir < 0 ? -1 : 1;
    return 0;
  }

  function compareNullableNumbers(a, b, dir) {
    var an = a == null || a === "" ? NaN : Number(a);
    var bn = b == null || b === "" ? NaN : Number(b);
    if (!Number.isFinite(an) && !Number.isFinite(bn)) return 0;
    if (!Number.isFinite(an)) return 1;
    if (!Number.isFinite(bn)) return -1;
    if (an < bn) return dir < 0 ? 1 : -1;
    if (an > bn) return dir < 0 ? -1 : 1;
    return 0;
  }

  function getSortValue(row, column) {
    if (!row) return "";
    if (column === "companyName") {
      return [row.companyName, row.parentCompany].filter(Boolean).join(" ");
    }
    if (column === "dataConfidence") {
      return formatDataConfidence(row.dataConfidence);
    }
    return row[column];
  }

  function sortFilteredRows() {
    var column = state.sortColumn;
    if (!column) return;
    var dir = state.sortDirection === "desc" ? -1 : 1;
    state.filteredRows.sort(function (a, b) {
      if (column === "alignmentScoreOptional") {
        return compareNullableNumbers(
          a.alignmentScoreOptional,
          b.alignmentScoreOptional,
          dir
        );
      }
      var cmp = compareStrings(getSortValue(a, column), getSortValue(b, column), dir);
      if (cmp !== 0) return cmp;
      return compareStrings(a.companyName, b.companyName, dir);
    });
  }

  function updateSortHeaderUI() {
    if (!els.table) return;
    var headers = els.table.querySelectorAll("th[data-sort]");
    headers.forEach(function (th) {
      th.classList.remove("sort-asc", "sort-desc");
      if (th.getAttribute("data-sort") === state.sortColumn) {
        th.classList.add(state.sortDirection === "asc" ? "sort-asc" : "sort-desc");
      }
    });
  }

  function handleSort(column) {
    if (!column) return;
    if (state.sortColumn === column) {
      state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
    } else {
      state.sortColumn = column;
      state.sortDirection = "asc";
    }
    applyFiltersAndRender();
  }

  function countDistinctDeals(rows) {
    var set = Object.create(null);
    (rows || []).forEach(function (r) {
      if (r.dealId) set[r.dealId] = true;
    });
    return Object.keys(set).length;
  }

  function iconSvg(name) {
    if (name === "alignment") {
      return '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><rect x="8" y="2" width="8" height="4" rx="1"/><path d="M9 12h6"/><path d="M9 16h6"/><path d="M9 8h6"/></svg>';
    }
    if (name === "capability") {
      return '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v4"/><path d="M12 18v4"/><path d="M4.93 4.93l2.83 2.83"/><path d="M16.24 16.24l2.83 2.83"/><path d="M2 12h4"/><path d="M18 12h4"/><path d="M4.93 19.07l2.83-2.83"/><path d="M16.24 7.76l2.83-2.83"/><circle cx="12" cy="12" r="3"/></svg>';
    }
    if (name === "profile") {
      return '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>';
    }
    if (name === "review") {
      return '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/><line x1="12" y1="7" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>';
    }
    if (name === "outreach") {
      return '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg>';
    }
    if (name === "more") {
      return '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="12" cy="19" r="1"/></svg>';
    }
    return "";
  }

  function closeOperatorStrategyMoreMenu() {
    if (moreMenuEl && moreMenuEl.parentNode) {
      moreMenuEl.parentNode.removeChild(moreMenuEl);
    }
    moreMenuEl = null;
  }

  function showOperatorStrategyMoreMenu(anchorBtn) {
    closeOperatorStrategyMoreMenu();
    if (!anchorBtn) return;

    var dealId = anchorBtn.getAttribute("data-deal-id") || "";
    var operatorId = anchorBtn.getAttribute("data-operator-id") || "";
    var row = findRowByIds(dealId, operatorId);

    var menu = document.createElement("div");
    menu.className = "operator-strategy-more-menu";
    menu.setAttribute("role", "menu");

    var rect = anchorBtn.getBoundingClientRect();
    menu.style.top = Math.min(global.innerHeight - 120, rect.bottom + 6) + "px";
    menu.style.left = Math.min(global.innerWidth - 240, rect.left) + "px";

    function addDisabledMenuItem(label, iconName, title) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className =
        "operator-strategy-more-menu__item operator-strategy-more-menu__item--disabled";
      btn.disabled = true;
      btn.setAttribute("role", "menuitem");
      btn.setAttribute("aria-disabled", "true");
      btn.title = title;
      btn.setAttribute("aria-label", label);
      btn.innerHTML =
        '<span class="operator-strategy-more-menu__icon" aria-hidden="true">' +
        iconSvg(iconName) +
        '</span><span class="operator-strategy-more-menu__label">' +
        esc(label) +
        "</span>";
      menu.appendChild(btn);
    }

    function addActionMenuItem(label, iconName, title, onClick) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "operator-strategy-more-menu__item";
      btn.setAttribute("role", "menuitem");
      btn.title = title;
      btn.setAttribute("aria-label", label);
      btn.innerHTML =
        '<span class="operator-strategy-more-menu__icon" aria-hidden="true">' +
        iconSvg(iconName) +
        '</span><span class="operator-strategy-more-menu__label">' +
        esc(label) +
        "</span>";
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        closeOperatorStrategyMoreMenu();
        onClick();
      });
      menu.appendChild(btn);
    }

    addDisabledMenuItem(
      "Add to Operator Review",
      "review",
      "Operator review set workflow coming soon"
    );

    if (row && row.odrActive) {
      addDisabledMenuItem(
        "Prepare Outreach",
        "outreach",
        "Active request: " + (row.outreachStatusLabel || row.odrStatus || "sent")
      );
    } else if (row && isLiveOperatorId(row.operatorId)) {
      addActionMenuItem(
        "Contact Operator",
        "outreach",
        "Create Operator Deal Request for this company",
        function () {
          promptContactOperator(row);
        }
      );
    } else {
      addDisabledMenuItem(
        "Contact Operator",
        "outreach",
        "Operator profile id required to send outreach"
      );
    }

    global.document.body.appendChild(menu);
    moreMenuEl = menu;

    global.setTimeout(function () {
      var dismiss = function (evt) {
        if (!moreMenuEl) return;
        if (moreMenuEl.contains(evt.target) || evt.target === anchorBtn) return;
        closeOperatorStrategyMoreMenu();
        global.document.removeEventListener("click", dismiss, true);
      };
      global.document.addEventListener("click", dismiss, true);
    }, 0);
  }

  function renderTableRows() {
    if (!els.tableBody) return;
    var rows = state.filteredRows || [];
    if (!rows.length) {
      els.tableBody.innerHTML = "";
      updateBulkActionsState();
      return;
    }

    els.tableBody.innerHTML = rows
      .map(function (row) {
        var dealId = row.dealId || "";
        var opId = row.operatorId || "";
        var checked = isRowSelected(row);
        var checkboxCell =
          '<td class="cell-checkbox"><input type="checkbox" class="operator-strategy-row-checkbox"' +
          (checked ? " checked" : "") +
          ' data-deal-id="' +
          esc(dealId) +
          '" data-operator-id="' +
          esc(opId) +
          '" title="Select row"></td>';
        var parent = row.parentCompany
          ? '<span class="operator-strategy-company-parent">' + esc(row.parentCompany) + "</span>"
          : "";
        var locationCell = row.location
          ? '<span class="operator-strategy-location">' + esc(row.location) + "</span>"
          : "—";
        var score = renderScoreCell(row);
        var profileDisabled = !isLiveOperatorId(opId);
        var outreachLabel = row.outreachStatusLabel || "Not contacted";
        var statusMod = outreachStatusModifier(row);
        var statusCell =
          '<td class="col-status">' +
          '<span class="operator-strategy-outreach-status' +
          (statusMod ? " " + statusMod : "") +
          (row.odrActive ? " operator-strategy-outreach-status--active" : "") +
          '">' +
          esc(outreachLabel) +
          "</span>" +
          (row.odrActive && row.reviewStatus && row.reviewStatus !== "—"
            ? '<span class="operator-strategy-review-sub">' + esc(row.reviewStatus) + "</span>"
            : "") +
          "</td>";

        var ctas =
          '<div class="action-icons">' +
          '<button type="button" class="action-icon" title="View Operator Alignment Snapshot" aria-label="View Operator Alignment Snapshot" data-os-action="view-oas" data-deal-id="' +
          esc(dealId) +
          '" data-operator-id="' +
          esc(opId) +
          '">' +
          iconSvg("alignment") +
          "</button>" +
          '<button type="button" class="action-icon" title="View Operator Capability Snapshot" aria-label="View Operator Capability Snapshot" data-os-action="view-ocs" data-deal-id="' +
          esc(dealId) +
          '">' +
          iconSvg("capability") +
          "</button>" +
          '<button type="button" class="action-icon' +
          (profileDisabled ? " action-icon--disabled" : "") +
          '" title="Open Operator Profile" aria-label="Open Operator Profile"' +
          (profileDisabled ? " disabled" : "") +
          ' data-os-action="open-profile" data-operator-id="' +
          esc(opId) +
          '" data-deal-id="' +
          esc(dealId) +
          '">' +
          iconSvg("profile") +
          "</button>" +
          '<button type="button" class="action-icon" title="More actions" aria-label="More actions" data-os-action="more" data-deal-id="' +
          esc(dealId) +
          '" data-operator-id="' +
          esc(opId) +
          '">' +
          iconSvg("more") +
          "</button>" +
          "</div>";

        return (
          "<tr>" +
          checkboxCell +
          '<td class="col-deal"><button type="button" class="operator-strategy-deal-link" data-os-action="open-deal" data-deal-id="' +
          esc(dealId) +
          '" title="Open deal">' +
          '<span class="operator-strategy-deal-name">' +
          esc(row.projectName) +
          "</span>" +
          "</button></td>" +
          '<td class="col-company"><span class="operator-strategy-company-name">' +
          esc(row.companyName) +
          "</span>" +
          parent +
          "</td>" +
          '<td class="col-location">' +
          locationCell +
          "</td>" +
          '<td class="col-score">' +
          score +
          "</td>" +
          statusCell +
          '<td class="col-consideration">' +
          esc(row.keyConsideration) +
          "</td>" +
          '<td class="col-confidence">' +
          esc(formatDataConfidence(row.dataConfidence)) +
          "</td>" +
          '<td class="col-cta cell-call-to-action">' +
          ctas +
          "</td></tr>"
        );
      })
      .join("");
    updateBulkActionsState();
  }

  function updateResultsCount() {
    if (!els.resultsCount) return;
    var n = (state.filteredRows || []).length;
    var deals = countDistinctDeals(state.filteredRows);
    if (state.loading) {
      els.resultsCount.textContent = "Loading operator strategy…";
      return;
    }
    if (state.fatalError) {
      els.resultsCount.textContent = state.fatalError;
      return;
    }
    if (state.dealFilterId && deals <= 1) {
      els.resultsCount.innerHTML =
        "Showing <strong>" + n + "</strong> operator-company row" + (n === 1 ? "" : "s");
      return;
    }
    els.resultsCount.innerHTML =
      "Showing <strong>" + n + "</strong> operator-company row" + (n === 1 ? "" : "s");
  }

  function updateTabCount() {
    var badge = document.getElementById("tabCountOperatorStrategy");
    if (!badge) return;
    if (state.loading) {
      badge.textContent = "…";
      return;
    }
    var n = (state.allRows || []).length;
    badge.textContent = n > 0 ? String(n) : "—";
  }

  function updateFilterCountBadge() {
    if (!els.filterBadge) return;
    var count = 0;
    if ((state.search || "").trim()) count += 1;
    if (state.pipelineFilter && state.pipelineFilter !== "all") count += 1;
    if (state.dealFilterId) count += 1;
    if (count > 0) {
      els.filterBadge.textContent = String(count);
      els.filterBadge.style.display = "inline-flex";
    } else {
      els.filterBadge.style.display = "none";
    }
  }

  function clearFilters() {
    state.search = "";
    state.pipelineFilter = "all";
    state.dealFilterId = "";
    state.dealFilterName = "";
    clearRowSelection();
    closeBulkDropdown();
    if (els.search) els.search.value = "";
    if (els.alignment) els.alignment.value = "all";
    applyFiltersAndRender();
  }

  function renderDealFilterChip() {
    if (!els.dealFilterChip) return;
    if (!state.dealFilterId) {
      els.dealFilterChip.style.display = "none";
      els.dealFilterChip.innerHTML = "";
      return;
    }
    var name = state.dealFilterName || state.dealFilterId;
    els.dealFilterChip.style.display = "flex";
    els.dealFilterChip.innerHTML =
      '<span class="operator-strategy-filter-chip__label">Filtered to:</span> ' +
      '<span class="operator-strategy-filter-chip__name">' +
      esc(name) +
      "</span>" +
      '<button type="button" class="btn-clear operator-strategy-filter-chip__clear" id="operatorStrategyClearDealFilter">Clear filter</button>';
  }

  function setVisibility() {
    var showPanelLoading = state.loading && isPanelVisible();
    if (els.loading) els.loading.style.display = showPanelLoading ? "block" : "none";
    if (els.partial) {
      els.partial.style.display =
        !state.loading && state.partialErrors > 0 && !state.fatalError ? "block" : "none";
    }
    if (els.fatal) {
      els.fatal.style.display = state.fatalError && !state.loading ? "block" : "none";
      if (state.fatalError) els.fatal.textContent = state.fatalError;
      els.fatal.classList.toggle("operator-strategy-state--error", !!state.authError);
    }
    var hasRows = (state.filteredRows || []).length > 0;
    if (els.table) els.table.style.display = hasRows && !state.loading ? "table" : "none";
    if (els.empty) {
      els.empty.style.display =
        !state.loading && !state.fatalError && !hasRows && state.loadedOnce ? "block" : "none";
    }
    if (els.tableWrap) {
      els.tableWrap.style.display = state.loading ? "none" : "block";
    }
  }

  function applyFiltersAndRender() {
    applyFilters();
    sortFilteredRows();
    renderTableRows();
    updateSortHeaderUI();
    updateResultsCount();
    updateTabCount();
    updateFilterCountBadge();
    renderDealFilterChip();
    setVisibility();
  }

  function setDealFilter(dealId) {
    var id = String(dealId || "").trim();
    if (!id || id.indexOf("rec") !== 0) {
      state.dealFilterId = "";
      state.dealFilterName = "";
    } else {
      state.dealFilterId = id;
      var meta = getDealMeta(id);
      state.dealFilterName = meta.projectName || "";
      if (!state.dealFilterName && state.allRows.length) {
        for (var i = 0; i < state.allRows.length; i++) {
          if (state.allRows[i].dealId === id) {
            state.dealFilterName = state.allRows[i].projectName || "";
            break;
          }
        }
      }
    }
    applyFiltersAndRender();
  }

  function openIframeModal(title, url) {
    if (typeof global.openMyDealsIframeModal === "function") {
      global.openMyDealsIframeModal(title, url);
      return true;
    }
    var modal = document.getElementById("submitProposalModal");
    var iframe = document.getElementById("submitProposalIframe");
    var titleEl = document.getElementById("submitProposalModalTitle");
    if (!modal || !iframe) return false;
    iframe.src = url;
    if (titleEl) titleEl.textContent = title || "View";
    modal.style.display = "block";
    return true;
  }

  function onTableChange(e) {
    var cb = e.target;
    if (
      !cb ||
      !cb.classList ||
      !cb.classList.contains("operator-strategy-row-checkbox")
    ) {
      return;
    }
    syncSelectionFromCheckbox(cb);
    updateBulkActionsState();
    var soleDeal = "";
    var checked = getVisibleRowCheckboxes().filter(function (box) {
      return box.checked;
    });
    if (checked.length === 1) {
      soleDeal = checked[0].getAttribute("data-deal-id") || "";
    }
    if (
      soleDeal &&
      soleDeal.indexOf("rec") === 0 &&
      typeof global.myDealsSetWorkspaceActiveDealId === "function"
    ) {
      global.myDealsSetWorkspaceActiveDealId(soleDeal);
    }
  }

  function onTableClick(e) {
    if (e.target && e.target.classList && e.target.classList.contains("operator-strategy-row-checkbox")) {
      return;
    }
    var btn = e.target.closest("[data-os-action]");
    if (!btn || btn.disabled) return;
    var action = btn.getAttribute("data-os-action");
    if (action === "more") {
      e.preventDefault();
      e.stopPropagation();
      showOperatorStrategyMoreMenu(btn);
      return;
    }
    e.preventDefault();
    var dealId = btn.getAttribute("data-deal-id") || "";
    var operatorId = btn.getAttribute("data-operator-id") || "";

    if (action === "score-breakdown" && dealId && operatorId) {
      var scoreRow = findRowByIds(dealId, operatorId);
      showOperatorMatchScoreBreakdown(
        dealId,
        operatorId,
        scoreRow ? scoreRow.companyName : ""
      );
      return;
    }
    if (action === "open-deal" && dealId) {
      if (typeof options.openDealView === "function") {
        options.openDealView(dealId);
      } else if (typeof global.openMyDealsDealBriefForDeal === "function") {
        global.openMyDealsDealBriefForDeal(dealId);
      }
      return;
    }
    if (action === "view-oas" && dealId) {
      if (typeof global.openMyDealsOperatorAlignmentForDeal === "function") {
        global.openMyDealsOperatorAlignmentForDeal(dealId);
      } else {
        global.location.href =
          "/operator-alignment-snapshot.html?dealId=" +
          encodeURIComponent(dealId) +
          "&embed=1";
      }
      return;
    }
    if (action === "view-ocs" && dealId) {
      if (typeof global.openMyDealsOperatorCapabilityForDeal === "function") {
        global.openMyDealsOperatorCapabilityForDeal(dealId);
      } else {
        global.location.href =
          "/operator-capability-snapshot.html?dealId=" +
          encodeURIComponent(dealId) +
          "&embed=1";
      }
      return;
    }
    if (action === "open-profile" && operatorId) {
      if (typeof global.openMyDealsOperatorProfileForDeal === "function") {
        global.openMyDealsOperatorProfileForDeal(operatorId, dealId);
      } else if (
        !openIframeModal(
          "Operator Profile",
          "/operator-explorer-gold-mock.html?id=" +
            encodeURIComponent(operatorId) +
            "&embed=1" +
            (dealId ? "&dealId=" + encodeURIComponent(dealId) : "")
        )
      ) {
        global.location.href =
          "/operator-explorer-gold-mock.html?id=" +
          encodeURIComponent(operatorId) +
          "&embed=1" +
          (dealId ? "&dealId=" + encodeURIComponent(dealId) : "");
      }
    }
  }

  function cacheElements() {
    els.panel = document.getElementById("sectionOperatorStrategy");
    els.search = document.getElementById("operatorStrategySearchInput");
    els.alignment = document.getElementById("operatorStrategyAlignmentFilter");
    els.clearFiltersBtn = document.getElementById("operatorStrategyClearFiltersBtn");
    els.filterBadge = document.getElementById("operatorStrategyFilterCountBadge");
    els.resultsCount = document.getElementById("operatorStrategyResultsCount");
    els.dealFilterChip = document.getElementById("operatorStrategyDealFilterChip");
    els.tableBody = document.getElementById("operatorStrategyTableBody");
    els.table = document.getElementById("operatorStrategyTable");
    els.tableWrap = document.getElementById("operatorStrategyTableWrap");
    els.loading = document.getElementById("operatorStrategyLoadingState");
    els.partial = document.getElementById("operatorStrategyPartialError");
    els.fatal = document.getElementById("operatorStrategyFatalError");
    els.empty = document.getElementById("operatorStrategyEmptyState");
    els.selectAllCheckbox = document.getElementById("operatorStrategySelectAllCheckbox");
    els.bulkActionsBtn = document.getElementById("operatorStrategyBulkActionsBtn");
    els.bulkDropdown = document.getElementById("operatorStrategyBulkDropdown");
  }

  function bindEvents() {
    if (els.search) {
      els.search.addEventListener("input", function () {
        state.search = els.search.value || "";
        applyFiltersAndRender();
      });
    }
    if (els.alignment) {
      els.alignment.addEventListener("change", function () {
        state.pipelineFilter = els.alignment.value || "all";
        applyFiltersAndRender();
      });
    }
    if (els.clearFiltersBtn) {
      els.clearFiltersBtn.addEventListener("click", function () {
        clearFilters();
      });
    }
    if (els.tableBody) {
      els.tableBody.addEventListener("click", onTableClick);
      els.tableBody.addEventListener("change", onTableChange);
    }
    if (els.selectAllCheckbox) {
      els.selectAllCheckbox.addEventListener("change", function () {
        var checked = !!els.selectAllCheckbox.checked;
        getVisibleRowCheckboxes().forEach(function (cb) {
          cb.checked = checked;
          syncSelectionFromCheckbox(cb);
        });
        updateBulkActionsState();
      });
    }
    if (els.bulkActionsBtn && els.bulkDropdown) {
      els.bulkActionsBtn.addEventListener("click", function (e) {
        e.stopPropagation();
        if (els.bulkActionsBtn.disabled) return;
        var open = els.bulkDropdown.classList.toggle("open");
        els.bulkActionsBtn.setAttribute("aria-expanded", open ? "true" : "false");
      });
      els.bulkDropdown.addEventListener("click", function (e) {
        e.stopPropagation();
      });
      global.document.addEventListener("click", function (e) {
        if (
          e.target.closest("#operatorStrategyBulkActionsBtn") ||
          e.target.closest("#operatorStrategyBulkDropdown")
        ) {
          return;
        }
        closeBulkDropdown();
      });
    }
    if (els.table) {
      var sortHeaders = els.table.querySelectorAll("th[data-sort]");
      sortHeaders.forEach(function (th) {
        th.addEventListener("click", function () {
          handleSort(th.getAttribute("data-sort"));
        });
      });
    }
    if (els.dealFilterChip) {
      els.dealFilterChip.addEventListener("click", function (e) {
        if (e.target && e.target.id === "operatorStrategyClearDealFilter") {
          state.dealFilterId = "";
          state.dealFilterName = "";
          applyFiltersAndRender();
        }
      });
    }
  }

  function init(initOptions) {
    options = initOptions || {};
    cacheElements();
    if (!els.panel) return;

    if (initOptions.initialDealFilterId) {
      setDealFilter(initOptions.initialDealFilterId);
    }

    bindEvents();
    setVisibility();
  }

  function onDealsReset() {
    state.loadedOnce = false;
    state.lastDealSig = "";
    state.fatalError = null;
    state.partialErrors = 0;
    pipelineLoadPromise = null;
    updateTabCount();
  }

  function isPipelineReady() {
    return state.loadedOnce && !state.loading;
  }

  /** Preload operator-company rows for all deals (My Deals page load / refresh). */
  function preloadFromDeals(deals) {
    var list = resolveDealsForPipeline(deals);
    var sig = dealIdsSignature(list);
    if (state.loading && pipelineLoadPromise) {
      return pipelineLoadPromise;
    }
    if (state.loadedOnce && state.lastDealSig === sig) {
      return Promise.resolve(true);
    }
    state.loadedOnce = false;
    return loadPipeline(true, list);
  }

  function onDealsLoaded(deals) {
    return preloadFromDeals(deals);
  }

  function onTabActivated() {
    setVisibility();
    if (state.loadedOnce && !state.loading) {
      applyFiltersAndRender();
      return Promise.resolve();
    }
    if (pipelineLoadPromise) {
      return pipelineLoadPromise.then(function () {
        applyFiltersAndRender();
      });
    }
    return loadPipeline(true, resolveDealsForPipeline());
  }

  function validateDomCopy() {
    var panel = document.getElementById("sectionOperatorStrategy");
    if (!panel) return ["missing #sectionOperatorStrategy"];
    var text = panel.innerText.toLowerCase();
    var issues = [];
    BANNED_PHRASES.forEach(function (phrase) {
      if (text.indexOf(phrase) !== -1) issues.push("banned phrase: " + phrase);
    });
    var forbidden = [
      "switch deal",
      "deal actions",
      "operating pathways to validate",
      "select a deal from my deals",
      "operatorstrategyswitchdeal",
      "operatorstrategynodealstate",
      "operatorstrategysummarygrid",
      "operatorstrategytopactions",
      "operatorstrategyquickfilters",
      "data-os-quick-filter",
    ];
    forbidden.forEach(function (s) {
      if (text.indexOf(s) !== -1) issues.push("forbidden UI: " + s);
    });
    var required = [
      "Project / Deal",
      "Operating Company",
      "Project Location",
      "Review Status",
      "Key Consideration",
      "Data Confidence",
      "Call to Action",
      "operatorStrategyTableBody",
      "operatorStrategyClearDealFilter",
    ];
    required.forEach(function (s) {
      if (!panel.innerText.includes(s) && !document.getElementById(s)) {
        if (panel.innerHTML.indexOf(s) === -1) issues.push("missing: " + s);
      }
    });
    var js = "";
    try {
      /* validation script checks JS separately */
    } catch (e) {}
    return issues;
  }

  global.OperatorStrategyMyDeals = {
    init: init,
    onDealsReset: onDealsReset,
    onDealsLoaded: onDealsLoaded,
    preloadFromDeals: preloadFromDeals,
    onTabActivated: onTabActivated,
    isPipelineReady: isPipelineReady,
    loadPipeline: loadPipeline,
    setDealFilter: setDealFilter,
    validateDomCopy: validateDomCopy,
    showOperatorMatchScoreBreakdown: showOperatorMatchScoreBreakdown,
  };
})(typeof window !== "undefined" ? window : global);
