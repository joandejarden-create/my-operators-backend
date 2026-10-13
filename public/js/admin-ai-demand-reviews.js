/**
 * Admin Resources — AI Demand Reviews control center (client).
 */
(function () {
  "use strict";

  var API = "/api/admin/ai-demand-positioning/monthly-reviews";
  var state = {
    reviews: [],
    properties: [],
    coverageMode: false,
    counts: {},
    selectedId: null,
    sortKey: "propertyName",
    sortDir: 1,
    generatePropertyId: "adp_cambridge_beaches_bermuda",
  };

  function authFetch(url, opts) {
    opts = opts || {};
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, opts);
    }
    return fetch(url, Object.assign({ credentials: "same-origin" }, opts));
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtDate(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toISOString().slice(0, 10);
  }

  function fmtDateTime(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toISOString().replace("T", " ").slice(0, 16) + " UTC";
  }

  function statusBadge(kind, label) {
    var cls = "adr-badge";
    if (kind === "ok") cls += " adr-badge--ok";
    if (kind === "warn") cls += " adr-badge--warn";
    if (kind === "danger") cls += " adr-badge--danger";
    return '<span class="' + cls + '">' + esc(label) + "</span>";
  }

  function reviewStatusKind(s) {
    if (s === "CLIENT_ISSUED" || s === "APPROVED") return "ok";
    if (s === "GENERATION_FAILED" || s === "ARCHIVED") return "danger";
    if (s === "SUPERSEDED") return "warn";
    return "warn";
  }

  function showRenderError(message) {
    var loadingEl = document.getElementById("supportGateLoading");
    var deniedEl = document.getElementById("supportGateDenied");
    var contentEl = document.getElementById("supportGateContent");
    if (window.SupportAdminGate && loadingEl) {
      window.SupportAdminGate.hidePageLoading(loadingEl);
    } else if (loadingEl) {
      loadingEl.hidden = true;
    }
    if (deniedEl) deniedEl.hidden = true;
    if (contentEl) {
      contentEl.hidden = false;
      var host = document.getElementById("adrRenderError");
      if (!host) {
        host = document.createElement("div");
        host.id = "adrRenderError";
        host.className = "adr-subtitle";
        host.style.cssText =
          "margin:12px 0;padding:12px;border:1px solid #c53030;background:#fff5f5;color:#9b2c2c;border-radius:6px";
        contentEl.insertBefore(host, contentEl.firstChild);
      }
      host.hidden = false;
      host.textContent =
        "AI Demand Admin failed to load reviews (render error — not an access denial): " +
        String(message || "unknown");
    }
    console.error("[AI Demand Reviews] RENDER_RUNTIME_ERROR", message);
  }

  function showDenied(message) {
    var loadingEl = document.getElementById("supportGateLoading");
    var deniedEl = document.getElementById("supportGateDenied");
    var contentEl = document.getElementById("supportGateContent");
    if (window.SupportAdminGate) {
      window.SupportAdminGate.hidePageLoading(loadingEl);
    } else if (loadingEl) {
      loadingEl.hidden = true;
    }
    if (contentEl) contentEl.hidden = true;
    if (deniedEl) {
      deniedEl.hidden = false;
      var msg = deniedEl.querySelector("[data-gate-message]");
      if (msg && message) msg.textContent = message;
    }
  }

  function queryFromFilters() {
    var qs = new URLSearchParams();
    function val(id) {
      var el = document.getElementById(id);
      return el ? String(el.value || "").trim() : "";
    }
    var q = val("adrSearch");
    var propertyId = val("adrFilterProperty");
    var reviewStatus = val("adrFilterReviewStatus");
    var generationStatus = val("adrFilterGenStatus");
    var issued = val("adrFilterIssued");
    var hasPdf = val("adrFilterPdf");
    var pattern = val("adrFilterPattern");
    var includeArchivedEl = document.getElementById("adrIncludeArchived");
    if (q) qs.set("q", q);
    if (propertyId) qs.set("propertyId", propertyId);
    if (reviewStatus) qs.set("reviewStatus", reviewStatus);
    if (generationStatus) qs.set("generationStatus", generationStatus);
    if (issued) qs.set("clientIssued", issued);
    if (hasPdf) qs.set("hasPdf", hasPdf);
    if (pattern) qs.set("actionPatternId", pattern);
    if (includeArchivedEl && includeArchivedEl.checked) qs.set("includeArchived", "true");
    return qs.toString();
  }

  function renderCounts(counts) {
    var el = document.getElementById("adrCounts");
    if (!el) return;
    var items = state.coverageMode
      ? [
          ["Published", counts.published || 0],
          ["Review ready", counts.reviewReady || 0],
          ["Needs rebuild", counts.missing || 0],
          ["Blocked", counts.blocked || 0],
        ]
      : [
          ["Draft", counts.DRAFT || 0],
          ["Ready for Review", counts.READY_FOR_REVIEW || 0],
          ["Approved", counts.APPROVED || 0],
          ["Client Issued", counts.CLIENT_ISSUED || 0],
        ];
    el.innerHTML = items
      .map(function (it) {
        return (
          '<div class="adr-count"><span class="adr-count__n">' +
          it[1] +
          '</span><span class="adr-count__l">' +
          esc(it[0]) +
          "</span></div>"
        );
      })
      .join("");
  }

  function coverageKind(status) {
    if (status === "READY") return "ok";
    if (status === "BLOCKED") return "danger";
    return "warn";
  }

  function sortedRows() {
    var rows = state.coverageMode ? state.properties.slice() : state.reviews.slice();
    var key = state.sortKey;
    var dir = state.sortDir;
    rows.sort(function (a, b) {
      var av = a[key];
      var bv = b[key];
      if (key === "clientIssued") {
        av = a.clientIssued ? 1 : 0;
        bv = b.clientIssued ? 1 : 0;
      }
      if (av == null) av = "";
      if (bv == null) bv = "";
      if (av < bv) return -1 * dir;
      if (av > bv) return 1 * dir;
      return 0;
    });
    return rows;
  }

  function coveragePdfCell(row) {
    // Canonical artifact = AI Demand Performance Review PDF for this reviewId.
    // Never current-report web-print / window.print / Admin DOM.
    if (row.hasPdf && row.reviewId) {
      return (
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="view-pdf" data-id="' +
        esc(row.reviewId) +
        '" data-property="' +
        esc(row.propertyId) +
        '">View PDF</button>' +
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="download-pdf" data-id="' +
        esc(row.reviewId) +
        '" data-property="' +
        esc(row.propertyId) +
        '">Download</button>'
      );
    }
    if (
      row.pdfUnavailableReason ===
        "performance_review_pdf_stale_vs_published_period" ||
      row.coverageReason ===
        "performance_review_pdf_stale_vs_published_period"
    ) {
      return (
        '<span class="adr-badge adr-badge--warn">PDF NOT AVAILABLE</span>' +
        '<div class="adr-subtitle">Performance Review PDF does not match current published period — prior-month artifact withheld</div>' +
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="draft" data-property="' +
        esc(row.propertyId) +
        '">Generate PDF</button>'
      );
    }
    if (row.pdfStatus === "PENDING_PDF" || row.coverageStatus === "NEEDS_REBUILD") {
      return (
        '<span class="adr-badge adr-badge--warn">PDF NOT AVAILABLE</span>' +
        (row.propertyId
          ? '<button type="button" class="adr-btn adr-btn--tiny" data-act="draft" data-property="' +
            esc(row.propertyId) +
            '">Generate PDF</button>'
          : '<span class="adr-badge adr-badge--warn">' +
            esc(row.pdfStatus || "Generating") +
            "</span>")
      );
    }
    if (row.coverageStatus === "BLOCKED") {
      return '<span class="adr-badge adr-badge--danger">Blocked</span>';
    }
    return '<span class="adr-badge adr-badge--warn">PDF NOT AVAILABLE</span>';
  }

  function coverageActionPlanCell(row) {
    var n = Number(row.actionCount || 0);
    var label = n === 1 ? "1 Action" : n + " Actions";
    return (
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="goto-action-plan" data-property="' +
      esc(row.propertyId) +
      '">' +
      esc(label) +
      "</button>"
    );
  }

  function coverageAdpClientCell(row) {
    var available =
      row &&
      row.adpClient &&
      row.adpClient.available === true;
    if (!available) {
      return '<span class="adr-subtitle" title="No valid ADP external client share">—</span>';
    }
    return (
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="open-adp-client" data-property="' +
      esc(row.propertyId) +
      '">Open</button>' +
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="copy-adp-client" data-property="' +
      esc(row.propertyId) +
      '">Copy URL</button>' +
      '<span class="adr-subtitle adr-copy-toast" data-adp-copy-toast hidden style="margin-left:6px;color:#276749">Copied</span>'
    );
  }

  function showAdpClientCopiedToast(btn) {
    var cell = btn && btn.closest ? btn.closest("td") : null;
    var toast = cell ? cell.querySelector("[data-adp-copy-toast]") : null;
    if (!toast) return;
    toast.hidden = false;
    toast.textContent = "Copied";
    clearTimeout(toast.__hideTimer);
    toast.__hideTimer = setTimeout(function () {
      toast.hidden = true;
    }, 1600);
  }

  function coverageMoreActions(row) {
    var html = "";
    if (row.reviewId) {
      html +=
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="open" data-id="' +
        esc(row.reviewId) +
        '">Open Review</button>';
      html +=
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="meeting" data-id="' +
        esc(row.reviewId) +
        '">Meeting Mode</button>';
      html +=
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="regenerate" data-id="' +
        esc(row.reviewId) +
        '">Regenerate</button>';
    }
    if (row.coverageStatus !== "BLOCKED") {
      html +=
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="draft" data-property="' +
        esc(row.propertyId) +
        '">Generate New Draft</button>';
    }
    html +=
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="open-archive" data-property="' +
      esc(row.propertyId) +
      '" data-hotel="' +
      esc(row.censusRecordId || row.hotelId || "") +
      '">Archive</button>';
    return html || "—";
  }

  function openReviewUrl(row, mode) {
    var u =
      "/owner-adp-monthly-review.html?adminPreview=1&propertyId=" +
      encodeURIComponent(row.propertyId) +
      "&reviewId=" +
      encodeURIComponent(row.reviewId) +
      "&source=archive";
    if (mode) u += "&mode=" + encodeURIComponent(mode);
    return u;
  }

  function actionButtons(row) {
    var actions = row.actions || [];
    function btn(label, attrs) {
      if (actions.indexOf(label) === -1) return "";
      return (
        '<button type="button" class="adr-btn adr-btn--tiny" ' +
        attrs +
        ">" +
        esc(label) +
        "</button>"
      );
    }
    return (
      btn("Open Review", 'data-act="open" data-id="' + esc(row.reviewId) + '"') +
      btn("Meeting Mode", 'data-act="meeting" data-id="' + esc(row.reviewId) + '"') +
      btn("Open PDF", 'data-act="pdf" data-id="' + esc(row.reviewId) + '"') +
      btn("View Payload", 'data-act="payload" data-id="' + esc(row.reviewId) + '"') +
      btn("View Actions", 'data-act="actions" data-id="' + esc(row.reviewId) + '"') +
      btn("Regenerate", 'data-act="regenerate" data-id="' + esc(row.reviewId) + '"') +
      btn(
        "Generate New Draft",
        'data-act="draft" data-property="' + esc(row.propertyId) + '"'
      ) +
      btn("Compare Versions", 'data-act="compare" data-id="' + esc(row.reviewId) + '"') +
      btn("Archive", 'data-act="archive" data-id="' + esc(row.reviewId) + '"')
    );
  }

  function renderTable() {
    var tbody = document.getElementById("adrTableBody");
    if (!tbody) return;
    var rows = sortedRows();
    if (state.coverageMode) {
      var covFilter = document.getElementById("adrFilterCoverage");
      var covVal = covFilter ? covFilter.value : "";
      if (covVal) {
        rows = rows.filter(function (r) {
          return r.coverageStatus === covVal;
        });
      }
      if (!rows.length) {
        tbody.innerHTML =
          '<tr><td colspan="15">No published hotels match the current filters.</td></tr>';
        return;
      }
      tbody.innerHTML = rows
        .map(function (r) {
          return (
            '<tr data-property="' +
            esc(r.propertyId) +
            '"' +
            (r.reviewId ? ' data-id="' + esc(r.reviewId) + '"' : "") +
            ">" +
            "<td>" +
            esc(r.propertyName) +
            "</td>" +
            "<td>" +
            esc(r.market || "—") +
            "</td>" +
            "<td>" +
            esc(r.reportingMonth || "—") +
            "</td>" +
            "<td>" +
            esc(fmtDate(r.currentMonitoringDate) || r.currentPeriodId || "—") +
            "</td>" +
            "<td>" +
            esc(fmtDate(r.priorRunDate) || "—") +
            "</td>" +
            "<td>" +
            esc(r.versionLabel || "—") +
            (r.isCurrent ? " · Current" : "") +
            "</td>" +
            "<td>" +
            statusBadge(
              reviewStatusKind(r.reviewStatus),
              r.reviewStatusLabel || r.reviewStatus || "—"
            ) +
            "</td>" +
            "<td>" +
            statusBadge(
              r.generationStatus === "GENERATION_FAILED" ? "danger" : "ok",
              r.generationStatusLabel || r.generationStatus || "—"
            ) +
            "</td>" +
            "<td>" +
            statusBadge(coverageKind(r.coverageStatus), r.coverageStatus) +
            (r.coverageReason
              ? '<div class="adr-subtitle">' + esc(r.coverageReason) + "</div>"
              : "") +
            "</td>" +
            "<td>" +
            (r.clientIssued ? "Yes" : "No") +
            "</td>" +
            '<td class="adr-col-pdf">' +
            coveragePdfCell(r) +
            "</td>" +
            '<td style="white-space:nowrap">' +
            coverageAdpClientCell(r) +
            "</td>" +
            '<td class="adr-col-action-plan">' +
            coverageActionPlanCell(r) +
            "</td>" +
            "<td>" +
            esc(fmtDateTime(r.generatedAt)) +
            "</td>" +
            '<td class="adr-actions">' +
            coverageMoreActions(r) +
            "</td>" +
            "</tr>"
          );
        })
        .join("");
      return;
    }
    if (!rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="15">No reviews match the current filters.</td></tr>';
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        return (
          '<tr data-id="' +
          esc(r.reviewId) +
          '"' +
          (state.selectedId === r.reviewId ? ' class="is-selected"' : "") +
          ">" +
          "<td>" +
          esc(r.propertyName) +
          "</td>" +
          "<td>" +
          esc(r.reportingMonth) +
          "</td>" +
          "<td>" +
          esc(fmtDate(r.currentMonitoringDate)) +
          "</td>" +
          "<td>" +
          esc(fmtDate(r.priorRunDate)) +
          "</td>" +
          "<td>" +
          esc(r.versionLabel) +
          (r.isCurrent ? " · Current" : "") +
          "</td>" +
          "<td>" +
          statusBadge(reviewStatusKind(r.reviewStatus), r.reviewStatusLabel || r.reviewStatus) +
          "</td>" +
          "<td>" +
          statusBadge(
            r.generationStatus === "GENERATION_FAILED" ? "danger" : "ok",
            r.generationStatusLabel || r.generationStatus
          ) +
          "</td>" +
          "<td>" +
          esc(r.actionCount) +
          "</td>" +
          "<td>" +
          esc(r.pdfStatus) +
          "</td>" +
          "<td>" +
          esc(fmtDateTime(r.generatedAt)) +
          "</td>" +
          "<td>" +
          (r.clientIssued ? "Yes" : "No") +
          "</td>" +
          "<td>" +
          actionButtons(r) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadList() {
    var qs = queryFromFilters();
    var res = await authFetch(API + (qs ? "?" + qs : ""));
    var json = await res.json();
    if (!res.ok || !json.ok) {
      throw new Error(json.message || json.error || "list_failed");
    }
    state.reviews = json.reviews || [];
    state.properties = json.properties || [];
    state.coverageMode = Array.isArray(json.properties) && json.properties.length > 0;
    state.counts = json.counts || {};
    renderCounts(state.counts);
    renderTable();
  }

  function findRow(id) {
    return state.reviews.find(function (r) {
      return r.reviewId === id;
    });
  }

  async function openDetails(reviewId, view) {
    state.selectedId = reviewId;
    renderTable();
    var res = await authFetch(API + "/" + encodeURIComponent(reviewId));
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error(json.error || "load_failed");
    var drawer = document.getElementById("adrDrawer");
    var body = document.getElementById("adrDrawerBody");
    var title = document.getElementById("adrDrawerTitle");
    var meta = json.meta || {};
    title.textContent = meta.propertyName + " · " + (meta.versionLabel || "");

    if (view === "payload") {
      body.innerHTML = renderPayloadInspection(json.review, json);
    } else if (view === "actions") {
      body.innerHTML = renderActionIntelligence(json.review, json);
    } else {
      body.innerHTML =
        "<dl>" +
        [
          ["Property", meta.propertyName],
          ["Reporting Month", meta.reportingMonth],
          ["Source Current Period", meta.sourceCurrentPeriodId],
          ["Source Prior Period", meta.sourcePriorPeriodId],
          ["Generated", fmtDateTime(meta.generatedAt)],
          ["Review Version", meta.versionLabel],
          ["Builder Version", meta.builderVersion],
          ["Action Library Version", meta.actionLibraryVersion],
          ["Status", meta.reviewStatusLabel || meta.reviewStatus],
          ["Generation", meta.generationStatusLabel || meta.generationStatus],
          ["Actions Count", meta.actionCount],
          ["PDF", meta.pdfStatus],
          ["Content Fingerprint", meta.contentFingerprint],
          ["PDF Fingerprint", meta.pdfFingerprint || "—"],
          [
            "Quality",
            meta.qualityGates && meta.qualityGates.summary
              ? meta.qualityGates.summary
              : "—",
          ],
        ]
          .map(function (row) {
            return "<dt>" + esc(row[0]) + "</dt><dd>" + esc(row[1]) + "</dd>";
          })
          .join("") +
        "</dl>" +
        '<div style="margin-top:12px">' +
        actionButtons(meta) +
        "</div>" +
        '<h3 style="margin:16px 0 8px;color:#fff;font-size:13px">Generation Log</h3>' +
        '<pre class="adr-pre">' +
        esc(JSON.stringify(json.generationLog || {}, null, 2)) +
        "</pre>";
    }
    drawer.hidden = false;
  }

  function renderPayloadInspection(review, pack) {
    var sections = [
      ["Review Identity", { schema: review.schema, productTitle: review.productTitle }],
      ["Property Identity", review.property],
      ["Current Period", {
        reportingMonth: review.reporting && review.reporting.reportingMonth,
        currentMonitoringDate: review.reporting && review.reporting.currentMonitoringDate,
        currentPeriodId: review.reporting && review.reporting.currentPeriodId,
      }],
      ["Prior Period", {
        priorRunDate: review.reporting && review.reporting.priorRunDate,
        priorPeriodId: review.reporting && review.reporting.priorPeriodId,
        comparability: review.reporting && review.reporting.comparability,
      }],
      ["KPI Values", review.kpis],
      ["Executive Assessment", review.executiveAssessment],
      ["Material Movements", review.materialMovements],
      ["Evidence", review.evidenceReview],
      ["Action Agenda", review.actionAgenda],
      ["Prior Actions", review.priorActionReview],
      ["Decisions Required", review.decisionsRequired],
      ["Next Monitoring Priorities", review.nextMonitoringPriorities],
    ];
    return (
      '<p class="adr-subtitle">Canonical Review Payload · gate ADP_MONTHLY_REVIEW_ADMIN_PAYLOAD_INSPECTION</p>' +
      '<label class="adr-check"><input type="checkbox" id="adrRawJsonToggle" /> Raw JSON</label>' +
      '<div id="adrPayloadHuman">' +
      sections
        .map(function (s) {
          return (
            '<section class="adr-payload-section"><h3>' +
            esc(s[0]) +
            '</h3><pre class="adr-pre">' +
            esc(JSON.stringify(s[1], null, 2)) +
            "</pre></section>"
          );
        })
        .join("") +
      '</div><pre class="adr-pre" id="adrPayloadRaw" hidden>' +
      esc(JSON.stringify(review, null, 2)) +
      "</pre>"
    );
  }

  function renderActionIntelligence(review) {
    var actions = review.actionAgenda || [];
    if (!actions.length) return "<p>No actions on this review.</p>";
    return (
      '<p class="adr-subtitle">Action Intelligence</p>' +
      actions
        .map(function (a, idx) {
          return (
            '<section class="adr-payload-section">' +
            "<h3>" +
            esc(a.actionTitle || a.title) +
            "</h3>" +
            "<dl>" +
            [
              ["Action Pattern ID", a.actionPatternId],
              ["Observed Issue", a.observedIssue],
              ["Evidence Trigger", a.evidenceTrigger || a.evidence],
              [
                "Target Sources",
                (a.targetSources || [])
                  .map(function (s) {
                    return typeof s === "string" ? s : s.label || s.url || JSON.stringify(s);
                  })
                  .join("; "),
              ],
              [
                "Implementation Steps",
                (a.implementationSteps || []).join(" → "),
              ],
              ["Accountable Owner", a.accountableOwnerRole || a.owner],
              [
                "Supporting Team",
                Array.isArray(a.supportingTeam)
                  ? a.supportingTeam.join(", ")
                  : a.supportingTeam,
              ],
              [
                "Definition of Done",
                Array.isArray(a.definitionOfDone)
                  ? a.definitionOfDone.join("; ")
                  : a.definitionOfDone,
              ],
              ["Expected Signal", a.expectedSignal],
              ["Status", a.status],
              ["Governance State", a.governanceState || a.lifecycle || "—"],
              [
                "Learning Record State",
                a.learningRecordState ||
                  (a.learningKeys ? "structure_present" : "—"),
              ],
            ]
              .map(function (row) {
                return (
                  "<dt>" +
                  esc(row[0]) +
                  "</dt><dd>" +
                  esc(row[1] == null ? "—" : row[1]) +
                  "</dd>"
                );
              })
              .join("") +
            "</dl>" +
            '<div class="adr-feedback" data-action-idx="' +
            idx +
            '">' +
            '<button type="button" class="adr-btn adr-btn--tiny" data-fb="KEEP" data-pattern="' +
            esc(a.actionPatternId || "") +
            '" data-instance="' +
            esc(a.actionId || a.actionPatternId || idx) +
            '">Keep</button>' +
            '<button type="button" class="adr-btn adr-btn--tiny" data-fb="NEEDS_IMPROVEMENT" data-pattern="' +
            esc(a.actionPatternId || "") +
            '" data-instance="' +
            esc(a.actionId || a.actionPatternId || idx) +
            '">Needs Improvement</button>' +
            '<button type="button" class="adr-btn adr-btn--tiny" data-fb="REJECT" data-pattern="' +
            esc(a.actionPatternId || "") +
            '" data-instance="' +
            esc(a.actionId || a.actionPatternId || idx) +
            '">Reject</button>' +
            "</div>" +
            "</section>"
          );
        })
        .join("") +
      '<section class="adr-payload-section"><h3>Report Section Feedback</h3>' +
      ["Executive Assessment", "Material Movements", "Evidence", "Actions", "PDF Layout", "Meeting Mode"]
        .map(function (sec) {
          return (
            '<div style="margin-bottom:8px"><strong>' +
            esc(sec) +
            '</strong> ' +
            '<button type="button" class="adr-btn adr-btn--tiny" data-sec-fb="PASS" data-section="' +
            esc(sec) +
            '">Pass</button> ' +
            '<button type="button" class="adr-btn adr-btn--tiny" data-sec-fb="NEEDS_WORK" data-section="' +
            esc(sec) +
            '">Needs Work</button></div>'
          );
        })
        .join("") +
      "</section>"
    );
  }

  async function showGenerateModal(propertyId) {
    state.generatePropertyId = propertyId;
    var res = await authFetch(API + "/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ propertyId: propertyId, confirm: false }),
    });
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error(json.error || "preview_failed");
    var m = json.modal || {};
    document.getElementById("adrModalTitle").textContent = m.title || "Generate AI Demand Performance Review";
    document.getElementById("adrModalBody").innerHTML =
      "<dl>" +
      [
        ["Property", m.property],
        ["Last Measurement", m.currentMonitoring],
        ["Prior Run", m.priorRun],
        ["Review Builder", m.reviewBuilder],
        ["Action Library", m.actionLibrary],
        ["Output", (m.outputs || []).join(", ")],
        ["Live Provider Calls", String(m.liveProviderCalls != null ? m.liveProviderCalls : 0)],
      ]
        .map(function (row) {
          return "<dt>" + esc(row[0]) + "</dt><dd>" + esc(row[1]) + "</dd>";
        })
        .join("") +
      "</dl>";
    document.getElementById("adrModal").hidden = false;
  }

  async function confirmGenerate() {
    var res = await authFetch(API + "/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        propertyId: state.generatePropertyId,
        confirm: true,
      }),
    });
    var json = await res.json();
    document.getElementById("adrModal").hidden = true;
    if (!res.ok || !json.ok) {
      alert(
        "Generation failed: " +
          ((json.failure && json.failure.message) || json.error || res.status)
      );
      return;
    }
    await loadList();
    if (json.newReviewId) openDetails(json.newReviewId);
  }

  function parseContentDispositionFilename(cd) {
    var header = String(cd || "");
    var m =
      header.match(/filename\*=UTF-8''([^;]+)/i) ||
      header.match(/filename=\"([^\"]+)\"/i) ||
      header.match(/filename=([^;]+)/i);
    if (!m || !m[1]) return "";
    try {
      return decodeURIComponent(String(m[1]).trim().replace(/^["']|["']$/g, ""));
    } catch (_e) {
      return String(m[1]).trim().replace(/^["']|["']$/g, "");
    }
  }

  function openPdfBlobWithCanonicalName(blob, filenameHint) {
    var filename = String(filenameHint || "Dealality - AI Demand Performance Review.pdf");
    if (!/\.pdf$/i.test(filename)) filename += ".pdf";
    if (/\.tmp$/i.test(filename)) {
      filename = filename.replace(/\.tmp$/i, ".pdf");
    }
    // Named File objects preserve the canonical .pdf name in browser Save As
    // (raw Blob object URLs often save as UUID.tmp).
    var file =
      typeof File === "function"
        ? new File([blob], filename, { type: "application/pdf" })
        : blob;
    var url = URL.createObjectURL(file);
    window.open(url, "_blank");
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 120000);
  }

  async function fetchPerformanceReviewPdf(reviewId, download) {
    var url =
      API +
      "/" +
      encodeURIComponent(reviewId) +
      "/pdf" +
      (download ? "?download=1" : "");
    var res = await authFetch(url);
    if (!res.ok) {
      return { ok: false, res: res };
    }
    var cdName = parseContentDispositionFilename(
      res.headers.get("Content-Disposition")
    );
    var hdrName = res.headers.get("X-ADP-PDF-Filename") || "";
    var blob = await res.blob();
    return {
      ok: true,
      blob: blob,
      filename:
        cdName || hdrName || "Dealality - AI Demand Performance Review.pdf",
      contentType: res.headers.get("Content-Type") || "application/pdf",
    };
  }

  function triggerPdfDownload(blob, filenameHint) {
    var filename = String(filenameHint || "Dealality - AI Demand Performance Review.pdf");
    if (!/\.pdf$/i.test(filename)) filename += ".pdf";
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () {
      URL.revokeObjectURL(a.href);
    }, 2000);
  }

  async function resolveCoverageReviewId(id, propertyId) {
    var reviewId = id;
    if (!reviewId && propertyId) {
      var cov = (state.properties || []).find(function (p) {
        return p.propertyId === propertyId;
      });
      reviewId = cov && cov.reviewId;
    }
    return reviewId || null;
  }

  async function onAction(act, id, propertyId, sourceBtn) {
    if (act === "goto-action-plan" && propertyId) {
      try {
        sessionStorage.setItem("dealality_adp_admin_property", propertyId);
      } catch (_e) {}
      if (typeof window.__ADP_ADMIN_SET_TAB__ === "function") {
        window.__ADP_ADMIN_SET_TAB__("action-plan");
      }
      return;
    }
    // Canonical Performance Review PDF (View / Download / Review alias).
    // Never current-report web-print, never window.print(), never Admin DOM.
    if (
      (act === "view-pdf" || act === "download-pdf" || act === "pdf") &&
      (id || propertyId)
    ) {
      var reviewId = await resolveCoverageReviewId(id, propertyId);
      if (!reviewId) {
        alert("PDF NOT AVAILABLE — AI Demand Performance Review not generated yet.");
        return;
      }
      // Coverage current row View/Download: refuse stale Sep PDF when published period advanced.
      // Historical Open PDF (act=pdf) always opens that exact reviewId artifact.
      if (
        (act === "view-pdf" || act === "download-pdf") &&
        propertyId &&
        state.coverageMode
      ) {
        var covRow = (state.properties || []).find(function (p) {
          return p.propertyId === propertyId;
        });
        if (
          covRow &&
          (covRow.pdfUnavailableReason ===
            "performance_review_pdf_stale_vs_published_period" ||
            covRow.coverageReason ===
              "performance_review_pdf_stale_vs_published_period" ||
            covRow.hasPdf === false)
        ) {
          alert(
            "PDF NOT AVAILABLE — current published ADP has no matching Performance Review PDF. Generate a new review for the current period (do not open a prior-month artifact)."
          );
          return;
        }
      }
      var wantDownload = act === "download-pdf";
      var pdfPack = await fetchPerformanceReviewPdf(reviewId, wantDownload);
      if (!pdfPack.ok) {
        alert("PDF NOT AVAILABLE — AI Demand Performance Review PDF missing for this version.");
        return;
      }
      if (wantDownload) {
        triggerPdfDownload(pdfPack.blob, pdfPack.filename);
      } else {
        openPdfBlobWithCanonicalName(pdfPack.blob, pdfPack.filename);
      }
      return;
    }
    if (act === "view-current-pdf" || act === "download-current-pdf") {
      // Hard refuse: current-report web-print is NOT the Admin Reviews artifact.
      alert(
        "PDF NOT AVAILABLE — Admin Reviews uses the AI Demand Performance Review PDF only (not the live ADP web print)."
      );
      return;
    }
    if (
      (act === "open-adp-client" || act === "copy-adp-client") &&
      propertyId
    ) {
      try {
        var linkRes = await authFetch(
          "/api/admin/ai-demand/hotels/" +
            encodeURIComponent(propertyId) +
            "/external-client-links"
        );
        var linkJson = await linkRes.json();
        var adpPack = linkJson && linkJson.adp ? linkJson.adp : null;
        var adpUrl =
          adpPack && adpPack.available && adpPack.url ? adpPack.url : null;
        if (!adpUrl) {
          alert(
            (adpPack && adpPack.reason) ||
              "ADP client URL not available for this hotel."
          );
          return;
        }
        if (/localhost|127\.0\.0\.1/i.test(adpUrl)) {
          alert("ADP client URL is not an external production client link.");
          return;
        }
        if (act === "copy-adp-client") {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(adpUrl);
            showAdpClientCopiedToast(sourceBtn);
          } else {
            prompt("Copy ADP client URL", adpUrl);
          }
        } else {
          window.open(adpUrl, "_blank");
        }
      } catch (_err) {
        alert("Could not resolve ADP client URL.");
      }
      return;
    }
    if (act === "open-archive") {
      if (typeof window.__ADP_ADMIN_SET_TAB__ === "function") {
        window.__ADP_ADMIN_SET_TAB__("report-archive");
      }
      return;
    }
    var row = id ? findRow(id) : null;
    if (act === "open" && row) {
      window.open(openReviewUrl(row, "live"), "_blank");
      return;
    }
    if (act === "meeting" && row) {
      window.open(openReviewUrl(row, "meeting"), "_blank");
      return;
    }
    if (act === "payload" && id) {
      await openDetails(id, "payload");
      // wire raw toggle without inline script
      var t = document.getElementById("adrRawJsonToggle");
      if (t) {
        t.addEventListener("change", function () {
          document.getElementById("adrPayloadHuman").hidden = t.checked;
          document.getElementById("adrPayloadRaw").hidden = !t.checked;
        });
      }
      return;
    }
    if (act === "actions" && id) {
      await openDetails(id, "actions");
      return;
    }
    if (act === "regenerate" && id) {
      if (!confirm("Regenerate creates a NEW immutable version from the same certified periods. Continue?")) {
        return;
      }
      var rRes = await authFetch(API + "/" + encodeURIComponent(id) + "/regenerate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      var rJson = await rRes.json();
      if (!rRes.ok || !rJson.ok) {
        alert("Regenerate failed: " + (rJson.error || rRes.status));
        return;
      }
      alert("Created " + (rJson.reviewId || rJson.newReviewId || "new version"));
      await loadList();
      if (rJson.newReviewId) openDetails(rJson.newReviewId);
      return;
    }
    if (act === "draft") {
      await showGenerateModal(propertyId || state.generatePropertyId);
      return;
    }
    if (act === "compare" && id) {
      var siblings = state.reviews.filter(function (r) {
        return row && r.propertyId === row.propertyId && r.reviewId !== id;
      });
      if (!siblings.length) {
        alert("No other version for this property to compare.");
        return;
      }
      var other = siblings[0].reviewId;
      var cRes = await authFetch(
        API +
          "/" +
          encodeURIComponent(id) +
          "/compare?other=" +
          encodeURIComponent(other)
      );
      var cJson = await cRes.json();
      if (!cRes.ok || !cJson.ok) {
        alert("Compare failed: " + (cJson.error || cRes.status));
        return;
      }
      var drawer = document.getElementById("adrDrawer");
      document.getElementById("adrDrawerTitle").textContent = "Compare Versions";
      document.getElementById("adrDrawerBody").innerHTML =
        '<pre class="adr-pre">' + esc(JSON.stringify(cJson.diff, null, 2)) + "</pre>";
      drawer.hidden = false;
      return;
    }
    if (act === "archive" && id) {
      if (!confirm("Archive removes this review from default views but does not delete artifacts.")) {
        return;
      }
      var aRes = await authFetch(API + "/" + encodeURIComponent(id) + "/archive", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      var aJson = await aRes.json();
      if (!aRes.ok || !aJson.ok) {
        alert("Archive failed: " + (aJson.error || aRes.status));
        return;
      }
      await loadList();
      return;
    }
    if (id) await openDetails(id);
  }

  async function init() {
    async function start() {
      var loadingEl = document.getElementById("supportGateLoading");
      if (window.SupportAdminGate && loadingEl) {
        window.SupportAdminGate.setPageLoadingMessage(
          loadingEl,
          "Loading AI Demand Reviews…"
        );
      }
      try {
        await loadList();
        if (window.SupportAdminGate && loadingEl) {
          window.SupportAdminGate.hidePageLoading(loadingEl);
        }
        var content = document.getElementById("supportGateContent");
        if (content) content.hidden = false;
      } catch (err) {
        showRenderError(
          err && err.message
            ? err.message
            : "Unable to load AI Demand Reviews."
        );
        return;
      }

      var applyBtn = document.getElementById("adrApplyFilters");
      if (applyBtn) {
        applyBtn.addEventListener("click", function () {
          loadList().catch(function (e) {
            alert(e.message);
          });
        });
      }
      var genBtn = document.getElementById("adrGenerateDraft");
      if (genBtn) {
        genBtn.addEventListener("click", function () {
          var propEl = document.getElementById("adrFilterProperty");
          showGenerateModal(
            (propEl && propEl.value) ||
              (state.properties[0] && state.properties[0].propertyId) ||
              "adp_cambridge_beaches_bermuda"
          ).catch(function (e) {
            alert(e.message);
          });
        });
      }
      var modalCancel = document.getElementById("adrModalCancel");
      if (modalCancel) {
        modalCancel.addEventListener("click", function () {
          document.getElementById("adrModal").hidden = true;
        });
      }
      var modalConfirm = document.getElementById("adrModalConfirm");
      if (modalConfirm) {
        modalConfirm.addEventListener("click", function () {
          confirmGenerate().catch(function (e) {
            alert(e.message);
          });
        });
      }
      var drawerClose = document.getElementById("adrDrawerClose");
      if (drawerClose) {
        drawerClose.addEventListener("click", function () {
          document.getElementById("adrDrawer").hidden = true;
        });
      }

      var table = document.getElementById("adrTable");
      if (table) {
        table.addEventListener("click", function (ev) {
          var th = ev.target.closest("th[data-sort]");
          if (th) {
            var key = th.getAttribute("data-sort");
            if (state.sortKey === key) state.sortDir *= -1;
            else {
              state.sortKey = key;
              state.sortDir = 1;
            }
            renderTable();
            return;
          }
          var btn = ev.target.closest("button[data-act]");
          if (btn) {
            onAction(
              btn.getAttribute("data-act"),
              btn.getAttribute("data-id"),
              btn.getAttribute("data-property"),
              btn
            ).catch(function (e) {
              alert(e.message);
            });
            return;
          }
          var tr = ev.target.closest("tr[data-id]");
          if (tr) {
            openDetails(tr.getAttribute("data-id")).catch(function (e) {
              alert(e.message);
            });
          }
        });
      }

      var drawerBody = document.getElementById("adrDrawerBody");
      if (drawerBody) {
        drawerBody.addEventListener("click", function (ev) {
          var fb = ev.target.closest("button[data-fb]");
          if (fb && state.selectedId) {
            var note = window.prompt("Optional note (founder feedback only):", "") || "";
            authFetch(
              API + "/" + encodeURIComponent(state.selectedId) + "/action-feedback",
              {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  actionInstanceId: fb.getAttribute("data-instance"),
                  actionPatternId: fb.getAttribute("data-pattern"),
                  rating: fb.getAttribute("data-fb"),
                  note: note,
                }),
              }
            )
              .then(function (r) {
                return r.json();
              })
              .then(function (j) {
                alert(j.ok ? "Feedback saved." : j.error || "Failed");
              });
            return;
          }
          var sec = ev.target.closest("button[data-sec-fb]");
          if (sec && state.selectedId) {
            var note2 = window.prompt("Optional note:", "") || "";
            authFetch(
              API + "/" + encodeURIComponent(state.selectedId) + "/report-feedback",
              {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  section: sec.getAttribute("data-section"),
                  rating: sec.getAttribute("data-sec-fb"),
                  note: note2,
                }),
              }
            )
              .then(function (r) {
                return r.json();
              })
              .then(function (j) {
                alert(j.ok ? "Section feedback saved." : j.error || "Failed");
              });
          }
        });
      }
    }

    if (window.__ADP_ADMIN_WORKSPACE_AUTH__) {
      await start();
      return;
    }
    if (document.getElementById("adaTabReviews")) {
      window.addEventListener(
        "dealality-adp-admin-ready",
        function () {
          start();
        },
        { once: true }
      );
      return;
    }
    if (!window.SupportAdminGate) {
      showDenied("Unable to load admin gate.");
      return;
    }
    var allowed = await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
      contentId: "supportGateContent",
    });
    if (!allowed) return;
    await start();
  }

  init();
})();
