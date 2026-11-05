/**
 * AI Demand Admin — AI Visibility Audit & Action Plan tab.
 * Matches AI Demand Reviews table / drawer patterns.
 * Staged management only; never auto-publishes customer reports.
 */
(function () {
  "use strict";

  var API = "/api/admin/ai-visibility-audit";
  var T = window.DealalityAiAdminTable;
  var state = {
    hotels: [],
    loaded: false,
    selectedId: null,
    drawerFocus: null,
  };
  var selection = T && T.createSelectionStore ? T.createSelectionStore() : null;

  var STATUS_LABELS = {
    NOT_STARTED: "Not Started",
    BASELINE_READY: "Not Started",
    PDF_STAGED: "Draft Available",
    ACTION_PLAN_STAGED: "Draft Available",
    VALIDATION_REQUIRED: "Needs Review",
    AUDIT_VALIDATED: "Validated",
    FOUNDER_REVIEWED_STAGED: "Founder Reviewed",
    FOUNDER_APPROVED: "Founder Reviewed",
    FOUNDER_APPROVED_FOR_CUSTOMER: "Founder Approved",
    CUSTOMER_READY: "Customer Ready",
    PUBLISHED: "Published",
    BLOCKED: "Blocked",
  };

  var VALIDATED_OR_LATER = {
    AUDIT_VALIDATED: true,
    FOUNDER_REVIEWED_STAGED: true,
    FOUNDER_APPROVED: true,
    FOUNDER_APPROVED_FOR_CUSTOMER: true,
    CUSTOMER_READY: true,
    PUBLISHED: true,
  };

  var CUSTOMER_APPROVED = {
    FOUNDER_APPROVED_FOR_CUSTOMER: true,
    CUSTOMER_READY: true,
    PUBLISHED: true,
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
    return T && T.esc
      ? T.esc(s)
      : String(s == null ? "" : s)
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;");
  }

  function statusLabel(status) {
    var raw = String(status || "NOT_STARTED");
    return STATUS_LABELS[raw] || raw.replace(/_/g, " ");
  }

  function statusKind(status) {
    var s = String(status || "");
    if (s === "BLOCKED") return "danger";
    if (CUSTOMER_APPROVED[s] || s === "AUDIT_VALIDATED") return "ok";
    if (
      s === "PDF_STAGED" ||
      s === "ACTION_PLAN_STAGED" ||
      s === "VALIDATION_REQUIRED" ||
      s === "FOUNDER_REVIEWED_STAGED"
    ) {
      return "warn";
    }
    return "";
  }

  function statusBadge(kind, label) {
    if (T && T.statusBadge) return T.statusBadge(kind, label);
    var cls = "adr-badge";
    if (kind === "ok") cls += " adr-badge--ok";
    if (kind === "warn") cls += " adr-badge--warn";
    if (kind === "danger") cls += " adr-badge--danger";
    return '<span class="' + cls + '">' + esc(label) + "</span>";
  }

  function hasAudit(h) {
    return !!(h && (h.latestVersionId || (h.pdfStatus && h.pdfStatus !== "MISSING")));
  }

  function hasPdf(h) {
    var pdf = String((h && h.pdfStatus) || "");
    return pdf === "STAGED" || pdf === "AVAILABLE" || pdf === "READY";
  }

  function isCustomerApproved(h) {
    if (!h) return false;
    if (h.founderApprovedForCustomer === true) return true;
    return !!CUSTOMER_APPROVED[h.auditStatus];
  }

  function isValidatedOrLater(h) {
    return !!(h && VALIDATED_OR_LATER[h.auditStatus]);
  }

  function formatEvidenceSnippet(action) {
    if (!action) return "";
    if (action.evidenceSummary) return String(action.evidenceSummary);
    var ev = action.evidence;
    if (!ev) return "";
    if (typeof ev === "string") return ev;
    if (Array.isArray(ev)) {
      return ev
        .map(function (item) {
          if (!item) return "";
          if (typeof item === "string") return item;
          return (
            item.summary ||
            item.label ||
            item.title ||
            item.claim ||
            item.sourceUrl ||
            item.url ||
            ""
          );
        })
        .filter(Boolean)
        .slice(0, 3)
        .join(" · ");
    }
    if (typeof ev === "object") {
      return (
        ev.summary ||
        ev.label ||
        ev.title ||
        ev.claim ||
        ev.sourceUrl ||
        ""
      );
    }
    return "";
  }

  function formatBaselineMonth(periodId) {
    var id = String(periodId || "");
    if (!id) return "—";
    var m = id.match(/(?:^|_)(20\d{2})(\d{2})(\d{2})/);
    if (!m) return "Certified";
    var months = [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
    ];
    var monthIdx = Number(m[2]) - 1;
    if (monthIdx < 0 || monthIdx > 11) return "Certified";
    return months[monthIdx] + " " + m[1];
  }

  function hotelById(propertyId) {
    propertyId = String(propertyId || "");
    for (var i = 0; i < state.hotels.length; i++) {
      if (String(state.hotels[i].propertyId) === propertyId) return state.hotels[i];
    }
    return null;
  }

  function appliedFilters() {
    return {
      q: String((document.getElementById("avaSearch") || {}).value || "")
        .trim()
        .toLowerCase(),
      status: String((document.getElementById("avaFilterStatus") || {}).value || ""),
      pdf: String((document.getElementById("avaFilterPdf") || {}).value || ""),
    };
  }

  function filterHotels() {
    var f = appliedFilters();
    return state.hotels.filter(function (h) {
      if (f.status) {
        if (f.status === "BASELINE_READY") {
          if (h.auditStatus !== "BASELINE_READY" && h.auditStatus !== "NOT_STARTED") {
            return false;
          }
        } else if (h.auditStatus !== f.status) {
          return false;
        }
      }
      if (f.pdf === "AVAILABLE" && !hasPdf(h)) return false;
      if (f.pdf === "MISSING" && hasPdf(h)) return false;
      if (!f.q) return true;
      return (
        String(h.name || "")
          .toLowerCase()
          .indexOf(f.q) >= 0 ||
        String(h.propertyId || "")
          .toLowerCase()
          .indexOf(f.q) >= 0 ||
        String(h.market || "")
          .toLowerCase()
          .indexOf(f.q) >= 0
      );
    });
  }

  function computeSummary(hotels) {
    var eligible = hotels.length;
    var created = 0;
    var validated = 0;
    var approved = 0;
    hotels.forEach(function (h) {
      if (hasAudit(h)) created += 1;
      if (isValidatedOrLater(h)) validated += 1;
      if (isCustomerApproved(h)) approved += 1;
    });
    return {
      eligible: eligible,
      created: created,
      validated: validated,
      approved: approved,
    };
  }

  function renderSummary() {
    var s = computeSummary(state.hotels);
    var map = {
      avaCardEligible: s.eligible,
      avaCardCreated: s.created,
      avaCardValidated: s.validated,
      avaCardApproved: s.approved,
    };
    Object.keys(map).forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = String(map[id]);
    });
  }

  function renderResultsRow(shown, total) {
    var el = document.getElementById("avaResultsRow");
    if (!el) return;
    if (T && T.resultsCountHtml) {
      el.innerHTML = T.resultsCountHtml(shown, total, "hotels");
      return;
    }
    el.textContent =
      "Showing " + shown + " of " + total + " hotel" + (total === 1 ? "" : "s");
  }

  function pdfUrl(propertyId, opts) {
    opts = opts || {};
    var url = API + "/" + encodeURIComponent(propertyId) + "/pdf";
    var qs = [];
    if (opts.download) qs.push("download=1");
    if (opts.channel) qs.push("channel=" + encodeURIComponent(opts.channel));
    if (opts.versionId) qs.push("versionId=" + encodeURIComponent(opts.versionId));
    return qs.length ? url + "?" + qs.join("&") : url;
  }

  function parseContentDispositionFilename(cd) {
    if (!cd) return "";
    var star = /filename\*\s*=\s*UTF-8''([^;]+)/i.exec(cd);
    if (star && star[1]) {
      try {
        return decodeURIComponent(star[1].trim().replace(/"/g, ""));
      } catch (_e) {
        /* fall through */
      }
    }
    var plain = /filename\s*=\s*"([^"]+)"/i.exec(cd) || /filename\s*=\s*([^;]+)/i.exec(cd);
    return plain && plain[1] ? plain[1].trim().replace(/"/g, "") : "";
  }

  function triggerPdfDownload(blob, filenameHint) {
    var filename = String(
      filenameHint || "Dealality - AI Visibility Audit & Action Plan.pdf"
    );
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

  function openPdfBlobPreview(blob, filenameHint) {
    var filename = String(
      filenameHint || "Dealality - AI Visibility Audit & Action Plan.pdf"
    );
    var file =
      typeof File !== "undefined"
        ? new File([blob], filename, { type: "application/pdf" })
        : blob;
    var url = URL.createObjectURL(file);
    window.open(url, "_blank", "noopener");
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 120000);
  }

  async function fetchAvaPdf(propertyId, opts) {
    opts = opts || {};
    var res = await authFetch(pdfUrl(propertyId, opts));
    if (!res.ok) {
      var errBody = null;
      try {
        errBody = await res.json();
      } catch (_e) {
        /* ignore */
      }
      return {
        ok: false,
        status: res.status,
        res: res,
        error: (errBody && errBody.error) || null,
        channel: (errBody && errBody.channel) || opts.channel || null,
      };
    }
    var cdName = parseContentDispositionFilename(
      res.headers.get("Content-Disposition")
    );
    var hdrName = res.headers.get("X-AVA-PDF-Filename") || "";
    var blob = await res.blob();
    return {
      ok: true,
      blob: blob,
      filename:
        cdName ||
        hdrName ||
        "Dealality - AI Visibility Audit & Action Plan.pdf",
      sha256: res.headers.get("X-AVA-PDF-Sha256") || "",
      channel: res.headers.get("X-AVA-PDF-Channel") || opts.channel || "",
      versionId: res.headers.get("X-AVA-PDF-Version-Id") || "",
      created: res.headers.get("X-AVA-PDF-Created") || "",
      contentType: res.headers.get("Content-Type") || "application/pdf",
    };
  }

  async function deliverAvaPdf(propertyId, act, versionId, channel) {
    var h = hotelById(propertyId);
    var ch = channel || (versionId ? "history" : "draft");
    if (ch === "draft" && !versionId && !hasPdf(h)) {
      window.alert("No New Draft PDF is staged for this hotel yet.");
      return;
    }
    var wantDownload =
      act === "download-pdf" ||
      act === "download-draft-pdf" ||
      act === "download-published-pdf";
    var pack = await fetchAvaPdf(propertyId, {
      download: wantDownload,
      channel: ch,
      versionId: versionId || null,
    });
    if (!pack.ok) {
      var missingMsg =
        ch === "published"
          ? "PUBLISHED PDF NOT AVAILABLE — refusing to fall back to draft."
          : ch === "history"
            ? "HISTORICAL PDF NOT AVAILABLE — refusing to fall back to another version."
            : "NEW DRAFT PDF NOT AVAILABLE — refusing to fall back to published.";
      window.alert(
        pack.status === 404
          ? missingMsg + (pack.error ? " (" + pack.error + ")" : "")
          : "PDF request failed (" + (pack.status || "error") + ")."
      );
      return;
    }
    if (wantDownload) triggerPdfDownload(pack.blob, pack.filename);
    else openPdfBlobPreview(pack.blob, pack.filename);
  }

  function shortSha(sha) {
    return sha ? String(sha).slice(0, 12) + "…" : "—";
  }

  function formatPdfChannelCard(title, channel, info, propertyId) {
    info = info || {};
    var file = info.file || null;
    var available = !!info.available && !!file;
    var qa =
      '<div class="ava-muted">Version: ' +
      esc((info.draftVersionId || info.publishedVersionId || (file && file.versionId) || "—")) +
      " · SHA: " +
      esc(shortSha((file && file.sha256) || info.draftPdfSha256 || info.publishedPdfSha256)) +
      " · Updated: " +
      esc((file && file.mtime) || info.draftUpdatedAt || info.publishedFrozenAt || "—") +
      "</div>";
    if (!available) {
      return (
        '<div class="ava-pdf-channel">' +
        "<p><strong>" +
        esc(title) +
        "</strong> " +
        statusBadge("warn", "Not available") +
        "</p>" +
        '<p class="ava-muted">' +
        esc(info.error || "Missing artifact") +
        " — Admin will not substitute another channel.</p>" +
        "</div>"
      );
    }
    return (
      '<div class="ava-pdf-channel">' +
      "<p><strong>" +
      esc(title) +
      "</strong> " +
      statusBadge("ok", "Available") +
      "</p>" +
      qa +
      '<div class="ava-drawer-actions">' +
      '<button type="button" class="adr-btn ava-pdf-channel-btn" data-channel="' +
      esc(channel) +
      '" data-act="view-pdf" data-property-id="' +
      esc(propertyId) +
      '">Preview ' +
      esc(title) +
      "</button>" +
      '<button type="button" class="adr-btn adr-btn--secondary ava-pdf-channel-btn" data-channel="' +
      esc(channel) +
      '" data-act="download-pdf" data-property-id="' +
      esc(propertyId) +
      '">Download ' +
      esc(title) +
      "</button>" +
      "</div></div>"
    );
  }

  function buildActions(h) {
    if (!T || !T.actionsHtml) {
      return (
        '<button type="button" class="adr-btn adr-btn--secondary" data-act="open" data-property-id="' +
        esc(h.propertyId) +
        '">Open</button>'
      );
    }
    if (!hasAudit(h)) {
      return T.actionsHtml(
        {
          primary: [
            {
              label: "Generate Audit",
              act: "generate",
              title: "Generate staged audit PDF",
              icon: "list",
            },
          ],
          more: [
            {
              label: "Open details",
              act: "open",
              title: "Open hotel drawer",
            },
          ],
        },
        { "property-id": h.propertyId }
      );
    }
    var primary = [];
    if (hasPdf(h)) {
      primary.push({
        label: "Preview New Draft",
        act: "view-draft-pdf",
        title: "Preview New Draft (staged) PDF",
        icon: "file",
      });
      primary.push({
        label: "Download New Draft",
        act: "download-draft-pdf",
        title: "Download New Draft (staged) PDF",
        icon: "download",
      });
    }
    primary.push({
      label: "Open Audit",
      act: "open",
      title: "Open audit management",
      icon: "external",
    });
    var more = [
      { label: "View audit details", act: "open", title: "Open overview" },
      {
        label: "Preview New Draft",
        act: "view-draft-pdf",
        title: hasPdf(h) ? "Preview New Draft PDF" : "No draft PDF staged yet",
        disabled: !hasPdf(h),
      },
      {
        label: "Download New Draft",
        act: "download-draft-pdf",
        title: hasPdf(h) ? "Download New Draft PDF" : "No draft PDF staged yet",
        disabled: !hasPdf(h),
      },
      {
        label: "Preview Published",
        act: "view-published-pdf",
        title: "Preview published (customer-visible) PDF",
      },
      {
        label: "Download Published",
        act: "download-published-pdf",
        title: "Download published (customer-visible) PDF",
      },
      {
        label: "Review recommendations",
        act: "recs",
        title: "Jump to recommendations",
      },
      {
        label: "Generate new staged version",
        act: "generate",
        title: "Generate staged PDF (no publish)",
      },
      {
        label: "View version history",
        act: "history",
        title: "Jump to version history",
      },
      {
        label: "Review evidence",
        act: "evidence",
        title: "Jump to research & evidence",
      },
    ];
    if (
      h.auditStatus === "PDF_STAGED" ||
      h.auditStatus === "ACTION_PLAN_STAGED" ||
      h.auditStatus === "VALIDATION_REQUIRED"
    ) {
      more.push({
        label: "Validate audit",
        act: "validate",
        title: "Mark AUDIT_VALIDATED (internal quality gate)",
      });
    }
    if (
      (h.auditStatus === "AUDIT_VALIDATED" ||
        h.auditStatus === "FOUNDER_REVIEWED_STAGED" ||
        h.auditStatus === "FOUNDER_APPROVED") &&
      !isCustomerApproved(h)
    ) {
      more.push({
        label: "Record founder approval",
        act: "founder-approve",
        title: "Explicit FOUNDER_APPROVED_FOR_CUSTOMER (does not publish)",
      });
    }
    return T.actionsHtml({ primary: primary, more: more }, { "property-id": h.propertyId });
  }

  function renderTable() {
    var body = document.getElementById("avaTableBody");
    if (!body) return;
    renderSummary();
    var rows = filterHotels();
    renderResultsRow(rows.length, state.hotels.length);
    if (selection) {
      selection.retain(rows.map(function (h) {
        return h.propertyId;
      }));
      syncSelectPage(rows);
    }
    if (!rows.length) {
      body.innerHTML =
        '<tr><td colspan="9">' +
        (state.hotels.length
          ? "No hotels match the current filters."
          : "No eligible hotels found.") +
        "</td></tr>";
      return;
    }
    body.innerHTML = rows
      .map(function (h) {
        var selected =
          selection && selection.has(h.propertyId)
            ? ' class="is-selected"'
            : state.selectedId === h.propertyId
              ? ' class="is-selected"'
              : "";
        var check =
          T && T.checkboxCell
            ? T.checkboxCell(h.propertyId, selection && selection.has(h.propertyId))
            : "<td></td>";
        var hotelCell =
          T && T.hotelLinkCell
            ? T.hotelLinkCell(h.name, null, h.propertyId)
            : '<td class="ada-cell-hotel"><strong>' +
              esc(h.name) +
              '</strong><div class="ada-subtitle ada-hotel-id">' +
              esc(h.propertyId) +
              "</div></td>";
        var pdfCell = hasPdf(h)
          ? statusBadge("ok", "Draft + Published")
          : statusBadge("warn", "Missing");
        var approvalCell = isCustomerApproved(h)
          ? statusBadge("ok", "Approved")
          : statusBadge("warn", "Not Authorized");
        var recCount =
          h.recommendationCount != null
            ? h.recommendationCount
            : h.acceptedRecommendationCount || 0;
        return (
          '<tr data-property-id="' +
          esc(h.propertyId) +
          '"' +
          selected +
          ">" +
          check +
          hotelCell +
          '<td class="ada-cell-loc">' +
          esc(h.market || "—") +
          "</td>" +
          '<td class="ava-col-baseline">' +
          (h.adpBaselineOk
            ? esc(formatBaselineMonth(h.adpBaselinePeriodId))
            : statusBadge("danger", "Missing")) +
          "</td>" +
          '<td class="ava-col-status">' +
          statusBadge(statusKind(h.auditStatus), statusLabel(h.auditStatus)) +
          "</td>" +
          '<td class="ada-cell-num ava-col-recs">' +
          esc(recCount) +
          "</td>" +
          '<td class="ava-col-pdf">' +
          pdfCell +
          "</td>" +
          '<td class="ava-col-approval">' +
          approvalCell +
          "</td>" +
          '<td class="ada-col-actions">' +
          buildActions(h) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  function syncSelectPage(rows) {
    var el = document.getElementById("avaSelectPage");
    if (!el || !selection) return;
    var ids = (rows || []).map(function (h) {
      return h.propertyId;
    });
    var selected = 0;
    ids.forEach(function (id) {
      if (selection.has(id)) selected += 1;
    });
    el.checked = ids.length > 0 && selected === ids.length;
    el.indeterminate = selected > 0 && selected < ids.length;
  }

  function drawerSection(id, title, html) {
    return (
      '<section class="ava-drawer-section" id="' +
      esc(id) +
      '"><h3>' +
      esc(title) +
      "</h3>" +
      html +
      "</section>"
    );
  }

  function dlRow(label, valueHtml) {
    return "<dt>" + esc(label) + "</dt><dd>" + valueHtml + "</dd>";
  }

  async function openDrawer(propertyId, focus) {
    var drawer = document.getElementById("avaDrawer");
    var title = document.getElementById("avaDrawerTitle");
    var body = document.getElementById("avaDrawerBody");
    if (!drawer || !body) return;
    state.selectedId = propertyId;
    state.drawerFocus = focus || null;
    drawer.hidden = false;
    if (title) title.textContent = propertyId;
    body.innerHTML = "<p>Loading…</p>";
    renderTable();
    try {
      var res = await authFetch(API + "/" + encodeURIComponent(propertyId));
      var json = await res.json();
      if (!res.ok || !json.ok) {
        body.innerHTML = "<p>Could not load audit details.</p>";
        return;
      }
      var meta = json.meta || {};
      var baseline = json.baseline || {};
      var latest = json.latestVersion;
      if (title) title.textContent = baseline.name || meta.name || propertyId;

      var recRes = await authFetch(
        API + "/" + encodeURIComponent(propertyId) + "/recommendations"
      );
      var recJson = await recRes.json().catch(function () {
        return { ok: false, actions: [] };
      });
      var actions = (recJson && recJson.actions) || [];

      var status = meta.status || "NOT_STARTED";
      var approved =
        meta.founderApprovedForCustomer === true || !!CUSTOMER_APPROVED[status];
      var versions = Array.isArray(meta.versions) ? meta.versions.slice().reverse() : [];

      var overviewHtml =
        '<dl class="ava-drawer-grid">' +
        dlRow("Hotel", esc(baseline.name || meta.name || propertyId)) +
        dlRow("Property ID", "<code>" + esc(propertyId) + "</code>") +
        dlRow("Location", esc(baseline.market || meta.market || "—")) +
        dlRow(
          "Website",
          baseline.website
            ? '<a href="' +
                esc(baseline.website) +
                '" target="_blank" rel="noopener">' +
                esc(baseline.website) +
                "</a>"
            : "—"
        ) +
        dlRow(
          "Certified ADP baseline",
          esc(baseline.periodId || meta.adpBaselinePeriodId || "—") +
            '<div class="ava-muted">' +
            esc(formatBaselineMonth(baseline.periodId || meta.adpBaselinePeriodId)) +
            "</div>"
        ) +
        dlRow(
          "Current audit version",
          esc((latest && latest.versionId) || "None yet")
        ) +
        dlRow(
          "Audit status",
          statusBadge(statusKind(status), statusLabel(status)) +
            '<div class="ava-muted">Internal: ' +
            esc(status) +
            "</div>"
        ) +
        dlRow(
          "Founder approval",
          approved
            ? statusBadge("ok", "Approved for Customer")
            : statusBadge("warn", "Not Authorized")
        ) +
        "</dl>";

      var channels = json.pdfChannels || {};
      var draftInfo = channels.draft || {};
      var publishedInfo = channels.published || {};
      var pdfHtml =
        '<p class="ava-gate-note">Published = customer-visible approved PDF. New Draft = latest staged report awaiting founder approval. Channels never silently substitute.</p>' +
        formatPdfChannelCard(
          "Published PDF",
          "published",
          publishedInfo,
          propertyId
        ) +
        formatPdfChannelCard(
          "New Draft PDF",
          "draft",
          draftInfo,
          propertyId
        ) +
        (draftInfo.differsFromPublished === false
          ? '<p class="ava-muted">Note: draft bytes currently match published — confirm regeneration if a newer draft was expected.</p>'
          : "") +
        '<div class="ava-drawer-actions">' +
        '<button type="button" class="adr-btn adr-btn--secondary" id="avaGeneratePdf">Generate new staged version</button>' +
        "</div>" +
        '<p class="ava-gate-note">Internal download is for founder review only. It does not publish, rotate share tokens, or email the customer.</p>';

      var recsHtml = actions.length
        ? actions
            .map(function (a) {
              var decision =
                (a.adminDecision && a.adminDecision.decision) || "PENDING";
              return (
                '<div class="ava-rec-card">' +
                '<div class="ava-rec-card__title">' +
                esc(a.headline || a.actionId) +
                "</div>" +
                '<div class="ava-rec-card__meta">' +
                esc(a.priority || "—") +
                " · " +
                esc(a.claimType || "—") +
                " · Decision: " +
                esc(decision) +
                (a.accountableOwner
                  ? " · Owner: " + esc(a.accountableOwner)
                  : "") +
                (a.suggestedTimeframe || a.timing
                  ? " · Timing: " + esc(a.suggestedTimeframe || a.timing)
                  : "") +
                "</div>" +
                (a.whatWeFound
                  ? '<p class="ava-muted">' + esc(a.whatWeFound) + "</p>"
                  : "") +
                (formatEvidenceSnippet(a)
                  ? '<p class="ava-muted">Evidence: ' +
                    esc(formatEvidenceSnippet(a)) +
                    "</p>"
                  : "") +
                '<div class="ava-rec-card__actions">' +
                '<button type="button" class="adr-btn adr-btn--secondary ava-decide" data-action="' +
                esc(a.actionId) +
                '" data-decision="APPROVE">Approve</button>' +
                '<button type="button" class="adr-btn adr-btn--secondary ava-decide" data-action="' +
                esc(a.actionId) +
                '" data-decision="REJECT">Reject</button>' +
                '<button type="button" class="adr-btn adr-btn--secondary ava-decide" data-action="' +
                esc(a.actionId) +
                '" data-decision="DEFER">Defer</button>' +
                "</div></div>"
              );
            })
            .join("")
        : "<p>No staged recommendations yet. Generate an audit to create the action plan.</p>";

      var evCert = json.evidenceHyperlinkCertification || {};
      var evSummary = evCert.summary || evCert.lastRun || {};
      var autoEv = json.autonomousEvidenceResolution || {};
      var autoSum = autoEv.summary || {};
      var researchHtml =
        '<dl class="ava-drawer-grid">' +
        dlRow(
          "Evidence review",
          esc(meta.evidenceReviewStatus || "NOT_STARTED")
        ) +
        dlRow(
          "Research spend",
          "$" + Number(meta.researchCostUsd || 0).toFixed(2) + " (paid research blocked)"
        ) +
        dlRow(
          "Outstanding confirmations",
          esc(
            (meta.outstandingConfirmations && meta.outstandingConfirmations.length) ||
              "None recorded"
          )
        ) +
        "</dl>" +
        '<p class="ava-gate-note">Verified findings and source registers live with each staged version payload.</p>';

      var evidenceValidationHtml =
        '<dl class="ava-drawer-grid">' +
        dlRow(
          "Pipeline",
          esc(autoEv.pipelineStatus || meta.evidencePipelineStatus || "EVIDENCE_VALIDATION_PENDING")
        ) +
        dlRow(
          "Total sources checked",
          String(
            autoSum.totalSourcesChecked != null
              ? autoSum.totalSourcesChecked
              : evSummary.totalEvidenceLinks != null
                ? evSummary.totalEvidenceLinks
                : "—"
          )
        ) +
        dlRow(
          "Automatically verified",
          String(
            autoSum.automaticallyVerified != null
              ? autoSum.automaticallyVerified
              : evSummary.verifiedLinks != null
                ? evSummary.verifiedLinks
                : "—"
          )
        ) +
        dlRow(
          "Automatically repaired",
          String(autoSum.automaticallyRepaired != null ? autoSum.automaticallyRepaired : "0")
        ) +
        dlRow(
          "Claims revised/removed",
          String(
            autoSum.claimsRevisedOrRemoved != null ? autoSum.claimsRevisedOrRemoved : "0"
          )
        ) +
        dlRow(
          "Remaining unresolved",
          String(
            autoSum.remainingUnresolved != null
              ? autoSum.remainingUnresolved
              : evSummary.linksRequiringManualReview != null
                ? evSummary.linksRequiringManualReview
                : "—"
          )
        ) +
        dlRow(
          "Last verification",
          esc(
            autoSum.lastVerificationDate ||
              evSummary.lastValidationDate ||
              "Not run"
          )
        ) +
        dlRow(
          "Certification",
          (autoSum.certificationStatus === "PASS" || evSummary.gatePass === true)
            ? statusBadge("ok", "PASS")
            : (autoSum.certificationStatus === "FAIL" || evSummary.gatePass === false)
              ? statusBadge("warn", "FAIL")
              : statusBadge("muted", "NOT RUN")
        ) +
        "</dl>" +
        '<div class="ava-drawer-actions">' +
        '<button type="button" class="adr-btn adr-btn--secondary" id="avaRunEvidenceValidation">Run autonomous validation</button>' +
        "</div>" +
        '<details class="ava-evidence-resolution-log"><summary>Evidence Resolution Log</summary>' +
        '<div id="avaEvidenceResolutionLog" class="ava-evidence-failures"></div></details>' +
        '<div id="avaEvidenceFailures" class="ava-evidence-failures"></div>' +
        '<p class="ava-gate-note">Autonomous remediation retries, browser-verifies, replaces, or qualifies evidence before asking for founder attention. Founder approval and customer publication remain separate gates.</p>';

      var cqa = json.customerQualityAssurance || {};
      var cqaGates = cqa.gates || {};
      var cqaHtml =
        '<dl class="ava-drawer-grid">' +
        dlRow(
          "Overall customer-quality",
          cqa.customerQualityCertified === true
            ? statusBadge("ok", "CERTIFIED")
            : cqa.assessedAt
              ? statusBadge("warn", "NOT CERTIFIED")
              : statusBadge("muted", "NOT RUN")
        ) +
        dlRow(
          "Research quality",
          cqaGates.researchQuality === true
            ? statusBadge("ok", "PASS")
            : cqaGates.researchQuality === false
              ? statusBadge("warn", "FAIL")
              : "—"
        ) +
        dlRow(
          "Evidence quality",
          cqaGates.evidenceQuality === true
            ? statusBadge("ok", "PASS")
            : cqaGates.evidenceQuality === false
              ? statusBadge("warn", "FAIL")
              : "—"
        ) +
        dlRow(
          "Communication quality",
          cqaGates.communicationQuality === true
            ? statusBadge("ok", "PASS")
            : cqaGates.communicationQuality === false
              ? statusBadge("warn", "FAIL")
              : "—"
        ) +
        dlRow(
          "PDF presentation",
          cqaGates.pdfPresentationQuality === true
            ? statusBadge("ok", "PASS")
            : cqaGates.pdfPresentationQuality === false
              ? statusBadge("warn", "FAIL")
              : "—"
        ) +
        dlRow(
          "Auto-corrections",
          String(cqa.totalRepairs != null ? cqa.totalRepairs : "—")
        ) +
        dlRow(
          "Unresolved material issues",
          String(
            (cqa.unresolvedMaterialIssues &&
              cqa.unresolvedMaterialIssues.length) ||
              0
          )
        ) +
        dlRow("Last certification", esc(cqa.assessedAt || "Not run")) +
        "</dl>" +
        '<details class="ava-cqa-log"><summary>Customer QA log</summary><div class="ava-evidence-failures">' +
        ((cqa.qaLog || [])
          .slice(0, 40)
          .map(function (row) {
            return (
              "<div><strong>" +
              esc(row.defect || row.severity || "issue") +
              "</strong> · " +
              esc(row.section || "") +
              "<div class='ava-muted'>Before: " +
              esc(String(row.originalText || "").slice(0, 160)) +
              "</div><div class='ava-muted'>After: " +
              esc(String(row.correctedText || "").slice(0, 160)) +
              "</div><div class='ava-muted'>" +
              esc(row.reason || "") +
              "</div></div>"
            );
          })
          .join("") ||
          "<p>No QA corrections recorded yet.</p>") +
        "</div></details>" +
        '<p class="ava-gate-note">Customer-quality certification never grants founder approval or customer publication. Founder review should focus on commercial judgment.</p>';

      var historyHtml = versions.length
        ? '<ul class="ava-version-list">' +
          versions
            .map(function (v) {
              var canPdf = !!(v.pdfRelativePath || meta.pdfRelativePath);
              return (
                "<li><strong>" +
                esc(v.versionId) +
                "</strong>" +
                '<div class="ava-muted">' +
                esc(v.savedAt || "—") +
                " · " +
                esc(v.status || "—") +
                (canPdf ? " · PDF staged" : " · No PDF") +
                (v.actionCount != null ? " · " + v.actionCount + " recs" : "") +
                "</div>" +
                (canPdf
                  ? '<div class="ava-drawer-actions" style="margin-top:6px">' +
                    '<button type="button" class="adr-btn adr-btn--secondary ava-hist-preview" data-version="' +
                    esc(v.versionId) +
                    '">Preview</button>' +
                    '<button type="button" class="adr-btn adr-btn--secondary ava-hist-download" data-version="' +
                    esc(v.versionId) +
                    '">Download</button>' +
                    "</div>"
                  : "") +
                "</li>"
              );
            })
            .join("") +
          "</ul>"
        : "<p>No versions yet.</p>";

      var canValidate =
        status === "PDF_STAGED" ||
        status === "ACTION_PLAN_STAGED" ||
        status === "VALIDATION_REQUIRED";
      var canFounderApprove =
        (status === "AUDIT_VALIDATED" ||
          status === "FOUNDER_REVIEWED_STAGED" ||
          status === "FOUNDER_APPROVED") &&
        !approved;

      var founderHtml =
        '<dl class="ava-drawer-grid">' +
        dlRow(
          "Authorization state",
          approved
            ? statusBadge("ok", "FOUNDER_APPROVED_FOR_CUSTOMER")
            : statusBadge("warn", "Not authorized")
        ) +
        dlRow(
          "Customer readiness",
          esc(meta.customerReadinessStatus || "NOT_CUSTOMER_READY")
        ) +
        dlRow(
          "Publication",
          esc(meta.publicationAuthorizationStatus || "NOT_AUTHORIZED")
        ) +
        "</dl>" +
        '<div class="ava-drawer-actions">' +
        (canValidate
          ? '<button type="button" class="adr-btn adr-btn--secondary" id="avaMarkValidated">Validate audit</button>'
          : "") +
        (canFounderApprove
          ? '<button type="button" class="adr-btn adr-btn--secondary" id="avaFounderApprove">Record founder approval</button>'
          : "") +
        "</div>" +
        '<p class="ava-gate-note">AUDIT_VALIDATED is an internal quality gate only. FOUNDER_APPROVED_FOR_CUSTOMER requires an explicit action and never publishes or emails the customer. Customer publication stays separately authorized.</p>';

      body.innerHTML =
        drawerSection("avaSecOverview", "Overview", overviewHtml) +
        drawerSection("avaSecPdf", "PDF Report", pdfHtml) +
        drawerSection("avaSecRecs", "Recommendations", recsHtml) +
        drawerSection("avaSecEvidence", "Research & Evidence", researchHtml) +
        drawerSection(
          "avaSecEvidenceValidation",
          "Evidence Validation",
          evidenceValidationHtml
        ) +
        drawerSection(
          "avaSecCustomerQuality",
          "Customer Quality Assurance",
          cqaHtml
        ) +
        drawerSection("avaSecHistory", "Version History", historyHtml) +
        drawerSection("avaSecFounder", "Founder Approval", founderHtml);

      body.querySelectorAll(".ava-pdf-channel-btn").forEach(function (btn) {
        btn.addEventListener("click", function () {
          deliverAvaPdf(
            propertyId,
            btn.getAttribute("data-act") || "view-pdf",
            null,
            btn.getAttribute("data-channel") || "draft"
          );
        });
      });
      var genBtn = document.getElementById("avaGeneratePdf");
      if (genBtn) {
        genBtn.addEventListener("click", function () {
          generatePdf(propertyId);
        });
      }
      body.querySelectorAll(".ava-hist-preview").forEach(function (btn) {
        btn.addEventListener("click", function () {
          deliverAvaPdf(
            propertyId,
            "view-pdf",
            btn.getAttribute("data-version"),
            "history"
          );
        });
      });
      body.querySelectorAll(".ava-hist-download").forEach(function (btn) {
        btn.addEventListener("click", function () {
          deliverAvaPdf(
            propertyId,
            "download-pdf",
            btn.getAttribute("data-version"),
            "history"
          );
        });
      });
      var valBtn = document.getElementById("avaMarkValidated");
      if (valBtn) {
        valBtn.addEventListener("click", function () {
          setStatus(propertyId, "AUDIT_VALIDATED", {
            note: "Internal quality certification (evidence/hyperlink gate enforced)",
          });
        });
      }
      var runEvBtn = document.getElementById("avaRunEvidenceValidation");
      if (runEvBtn) {
        runEvBtn.addEventListener("click", function () {
          runEvidenceValidation(propertyId);
        });
      }
      renderEvidenceFailures(
        (evSummary && evSummary.reasons) ||
          (meta.evidenceHyperlinkCertification &&
            meta.evidenceHyperlinkCertification.lastFullReport &&
            meta.evidenceHyperlinkCertification.lastFullReport.gate &&
            meta.evidenceHyperlinkCertification.lastFullReport.gate.reasons) ||
          [],
        (meta.evidenceHyperlinkCertification &&
          meta.evidenceHyperlinkCertification.lastFullReport &&
          meta.evidenceHyperlinkCertification.lastFullReport.evidence) ||
          [],
        propertyId
      );
      renderEvidenceResolutionLog(
        autoEv.resolutionLog ||
          (meta.autonomousEvidenceResolution &&
            meta.autonomousEvidenceResolution.lastRun &&
            meta.autonomousEvidenceResolution.lastRun.resolutionLog) ||
          []
      );
      var founderBtn = document.getElementById("avaFounderApprove");
      if (founderBtn) {
        founderBtn.addEventListener("click", function () {
          if (
            !window.confirm(
              "Authorize FOUNDER_APPROVED_FOR_CUSTOMER for this report version? This does not publish or email the customer."
            )
          ) {
            return;
          }
          setStatus(propertyId, "FOUNDER_APPROVED_FOR_CUSTOMER", {
            note: "Explicit founder authorization for customer distribution",
            founderApprovedForCustomer: true,
          });
        });
      }
      body.querySelectorAll(".ava-decide").forEach(function (btn) {
        btn.addEventListener("click", function () {
          decideAction(
            propertyId,
            btn.getAttribute("data-action"),
            btn.getAttribute("data-decision")
          );
        });
      });

      scrollDrawerFocus(focus);
    } catch (_err) {
      body.innerHTML = "<p>Could not load audit details.</p>";
    }
  }

  function scrollDrawerFocus(focus) {
    if (!focus) return;
    var map = {
      recs: "avaSecRecs",
      history: "avaSecHistory",
      evidence: "avaSecEvidence",
      evidenceValidation: "avaSecEvidenceValidation",
      founder: "avaSecFounder",
      pdf: "avaSecPdf",
    };
    var id = map[focus];
    if (!id) return;
    var el = document.getElementById(id);
    if (el && typeof el.scrollIntoView === "function") {
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  function renderEvidenceResolutionLog(logRows) {
    var host = document.getElementById("avaEvidenceResolutionLog");
    if (!host) return;
    var rows = logRows || [];
    if (!rows.length) {
      host.innerHTML =
        '<p class="ava-muted">No autonomous remediation events recorded yet.</p>';
      return;
    }
    var html = '<ul class="ava-evidence-fail-list">';
    rows.slice(0, 40).forEach(function (row) {
      html +=
        "<li>" +
        "<strong>" +
        esc(row.outcome || "EVENT") +
        "</strong>" +
        (row.originalUrl
          ? ' · <code>' + esc(row.originalUrl) + "</code>"
          : "") +
        (row.finalUrl ? " → <code>" + esc(row.finalUrl) + "</code>" : "") +
        '<div class="ava-muted">' +
        esc(row.failure || "") +
        (row.claimAffected ? " · claim " + esc(row.claimAffected) : "") +
        "</div>" +
        (row.strategiesAttempted && row.strategiesAttempted.length
          ? '<div class="ava-muted">Tried: ' +
            esc(row.strategiesAttempted.join(", ")) +
            "</div>"
          : "") +
        (row.supportingEvidence
          ? '<div class="ava-muted">' + esc(row.supportingEvidence) + "</div>"
          : "") +
        "</li>";
    });
    html += "</ul>";
    host.innerHTML = html;
  }

  function renderEvidenceFailures(reasons, evidenceRows, propertyId) {
    var host = document.getElementById("avaEvidenceFailures");
    if (!host) return;
    var blocking = (evidenceRows || []).filter(function (e) {
      return e && e.gateBlocking;
    });
    if ((!reasons || !reasons.length) && !blocking.length) {
      host.innerHTML = "";
      return;
    }
    var html = '<ul class="ava-evidence-fail-list">';
    blocking.slice(0, 12).forEach(function (e) {
      html +=
        "<li>" +
        "<strong>" +
        esc(e.evidenceClassification || "ISSUE") +
        "</strong> · " +
        esc(e.label || e.associatedClaimOrActionId || "Evidence") +
        '<div class="ava-muted">' +
        esc(e.observation || "") +
        "</div>" +
        (e.sourceUrl
          ? '<div class="ava-drawer-actions" style="margin-top:4px">' +
            '<a class="adr-btn adr-btn--secondary" href="' +
            esc(e.sourceUrl) +
            '" target="_blank" rel="noopener noreferrer">Open source</a>' +
            '<button type="button" class="adr-btn adr-btn--secondary ava-manual-verify" data-url="' +
            esc(e.sourceUrl) +
            '" data-action="' +
            esc(e.associatedClaimOrActionId || "") +
            '">Record manual verify</button>' +
            "</div>"
          : "") +
        "</li>";
    });
    if (!blocking.length && reasons && reasons.length) {
      reasons.slice(0, 8).forEach(function (r) {
        html += "<li>" + esc(r) + "</li>";
      });
    }
    html += "</ul>";
    host.innerHTML = html;
    host.querySelectorAll(".ava-manual-verify").forEach(function (btn) {
      btn.addEventListener("click", function () {
        recordManualEvidenceVerification(
          propertyId,
          btn.getAttribute("data-url"),
          btn.getAttribute("data-action")
        );
      });
    });
  }

  async function runEvidenceValidation(propertyId) {
    var host = document.getElementById("avaEvidenceFailures");
    if (host) host.innerHTML = "<p>Running evidence &amp; hyperlink validation…</p>";
    try {
      var res = await authFetch(
        API + "/" + encodeURIComponent(propertyId) + "/evidence-validation",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        if (host) {
          host.innerHTML =
            "<p>Validation failed to run: " +
            esc((json && (json.message || json.error)) || res.status) +
            "</p>";
        }
        return;
      }
      await openDrawer(propertyId, "evidenceValidation");
      if (json.gatePass === false) {
        window.alert(
          "Evidence gate FAILED. Status may move to VALIDATION_REQUIRED. Review failures in Evidence Validation."
        );
      }
    } catch (_err) {
      if (host) host.innerHTML = "<p>Validation request failed.</p>";
    }
  }

  async function recordManualEvidenceVerification(propertyId, url, actionId) {
    var explanation = window.prompt(
      "Manual verification explanation (required). Describe how you confirmed the source supports the claim:"
    );
    if (!explanation || String(explanation).trim().length < 12) {
      window.alert("Explanation required (at least 12 characters). No override recorded.");
      return;
    }
    var decision = window.confirm(
      "Mark this source as VERIFIED after browser review?\n\nCancel = REJECT instead."
    )
      ? "VERIFIED"
      : "REJECTED";
    try {
      var res = await authFetch(
        API +
          "/" +
          encodeURIComponent(propertyId) +
          "/evidence-validation/manual",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            url: url,
            decision: decision,
            explanation: String(explanation).trim(),
            associatedClaimOrActionId: actionId || null,
          }),
        }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        window.alert(
          (json && (json.message || json.error)) || "Manual verification failed"
        );
        return;
      }
      await runEvidenceValidation(propertyId);
    } catch (_err) {
      window.alert("Manual verification request failed");
    }
  }

  async function generatePdf(propertyId) {
    var body = document.getElementById("avaDrawerBody");
    try {
      if (body && !document.getElementById("avaDrawer").hidden) {
        body.innerHTML = "<p>Generating staged PDF…</p>";
      }
      var res = await authFetch(
        API + "/" + encodeURIComponent(propertyId) + "/generate-pdf",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        window.alert(
          "Generate failed: " +
            ((json && (json.error || json.message)) || res.status)
        );
        if (!document.getElementById("avaDrawer").hidden) {
          openDrawer(propertyId);
        }
        return;
      }
      await loadHotels();
      openDrawer(propertyId, "pdf");
    } catch (_err) {
      window.alert("Generate failed.");
    }
  }

  async function decideAction(propertyId, actionId, decision) {
    try {
      var res = await authFetch(
        API +
          "/" +
          encodeURIComponent(propertyId) +
          "/actions/" +
          encodeURIComponent(actionId) +
          "/decision",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ decision: decision }),
        }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        window.alert("Decision failed: " + ((json && json.error) || res.status));
        return;
      }
      openDrawer(propertyId, "recs");
    } catch (_err) {
      window.alert("Decision failed.");
    }
  }

  async function setStatus(propertyId, status, extra) {
    try {
      var payload = Object.assign({ status: status }, extra || {});
      var res = await authFetch(
        API + "/" + encodeURIComponent(propertyId) + "/status",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        window.alert(
          "Status change failed: " +
            ((json && (json.error || json.message)) || res.status)
        );
        return;
      }
      await loadHotels();
      openDrawer(propertyId, status === "FOUNDER_APPROVED_FOR_CUSTOMER" ? "founder" : null);
    } catch (_err) {
      window.alert("Status change failed.");
    }
  }

  function hotelListErrorMessage(res, json) {
    var status = res && res.status;
    if (status === 401) return "Session expired. Please sign in again.";
    if (status === 403) return "You do not have permission to access this feature.";
    if (status === 404) return "AI Visibility Audit service unavailable.";
    if (status >= 500) return "Unable to load hotel data. Please try again.";
    if (json && json.message) return String(json.message);
    return "Unable to load hotel data. Please try again.";
  }

  async function loadHotels() {
    var body = document.getElementById("avaTableBody");
    if (body) body.innerHTML = '<tr><td colspan="9">Loading…</td></tr>';
    try {
      var res = await authFetch(API + "/hotels");
      var json = {};
      try {
        json = await res.json();
      } catch (_parseErr) {
        json = {};
      }
      if (!res.ok || !json.ok) {
        if (body) {
          body.innerHTML =
            '<tr><td colspan="9">' +
            esc(hotelListErrorMessage(res, json)) +
            "</td></tr>";
        }
        return;
      }
      state.hotels = json.hotels || [];
      state.loaded = true;
      renderTable();
    } catch (_err) {
      if (body) {
        body.innerHTML =
          '<tr><td colspan="9">Unable to connect to the server.</td></tr>';
      }
    }
  }

  function resetView() {
    var search = document.getElementById("avaSearch");
    var status = document.getElementById("avaFilterStatus");
    var pdf = document.getElementById("avaFilterPdf");
    if (search) search.value = "";
    if (status) status.value = "";
    if (pdf) pdf.value = "";
    if (selection) selection.clear();
    renderTable();
  }

  function handleAct(act, propertyId) {
    if (!propertyId) return;
    if (act === "open") {
      openDrawer(propertyId);
      return;
    }
    if (
      act === "view-pdf" ||
      act === "pdf" ||
      act === "download-pdf" ||
      act === "view-draft-pdf" ||
      act === "download-draft-pdf"
    ) {
      deliverAvaPdf(
        propertyId,
        act.indexOf("download") >= 0 ? "download-pdf" : "view-pdf",
        null,
        "draft"
      );
      return;
    }
    if (act === "view-published-pdf" || act === "download-published-pdf") {
      deliverAvaPdf(
        propertyId,
        act.indexOf("download") >= 0 ? "download-pdf" : "view-pdf",
        null,
        "published"
      );
      return;
    }
    if (act === "generate") {
      generatePdf(propertyId);
      return;
    }
    if (act === "recs") {
      openDrawer(propertyId, "recs");
      return;
    }
    if (act === "history") {
      openDrawer(propertyId, "history");
      return;
    }
    if (act === "evidence") {
      openDrawer(propertyId, "evidence");
      return;
    }
    if (act === "validate") {
      setStatus(propertyId, "AUDIT_VALIDATED", {
        note: "Internal quality certification",
      });
      return;
    }
    if (act === "founder-approve") {
      if (
        !window.confirm(
          "Authorize FOUNDER_APPROVED_FOR_CUSTOMER for this report version? This does not publish or email the customer."
        )
      ) {
        return;
      }
      setStatus(propertyId, "FOUNDER_APPROVED_FOR_CUSTOMER", {
        note: "Explicit founder authorization for customer distribution",
        founderApprovedForCustomer: true,
      });
    }
  }

  function wire() {
    if (wire._done) return;
    wire._done = true;

    var refresh = document.getElementById("avaRefresh");
    var apply = document.getElementById("avaApplyFilters");
    var reset = document.getElementById("avaResetView");
    var close = document.getElementById("avaDrawerClose");
    var table = document.getElementById("avaTable");
    var selectPage = document.getElementById("avaSelectPage");

    if (refresh) refresh.addEventListener("click", loadHotels);
    if (apply) apply.addEventListener("click", renderTable);
    if (reset) reset.addEventListener("click", resetView);
    if (close) {
      close.addEventListener("click", function () {
        var drawer = document.getElementById("avaDrawer");
        if (drawer) drawer.hidden = true;
        state.selectedId = null;
        renderTable();
      });
    }

    var search = document.getElementById("avaSearch");
    if (search) {
      search.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") {
          ev.preventDefault();
          renderTable();
        }
      });
    }

    if (selectPage && selection) {
      selectPage.addEventListener("change", function () {
        var rows = filterHotels();
        selection.setPage(
          rows.map(function (h) {
            return h.propertyId;
          }),
          selectPage.checked
        );
        renderTable();
      });
    }

    var panel = document.getElementById("adaPanelVisibilityAudit");
    if (panel) {
      panel.addEventListener("change", function (ev) {
        var check = ev.target.closest(".ada-row-check");
        if (!check || !panel.contains(check) || !selection) return;
        var id = check.getAttribute("data-id");
        if (!id) return;
        if (check.checked) selection.add(id);
        else selection.remove(id);
        renderTable();
      });

      panel.addEventListener(
        "click",
        function (ev) {
          var btn = ev.target.closest("button[data-act]");
          if (!btn || !panel.contains(btn)) return;
          if (btn.classList.contains("ada-more-trigger")) return;
          var wrap = btn.closest(".ada-row-actions");
          if (!wrap && !btn.getAttribute("data-property-id")) return;
          var propertyId =
            (wrap && wrap.getAttribute("data-property-id")) ||
            btn.getAttribute("data-property-id") ||
            (btn.closest("tr") &&
              btn.closest("tr").getAttribute("data-property-id"));
          if (!propertyId) return;
          ev.preventDefault();
          ev.stopPropagation();
          handleAct(btn.getAttribute("data-act"), propertyId);
        },
        true
      );
    }
  }

  window.__AVA_OPEN_DRAWER__ = openDrawer;
  window.__AVA_HANDLE_ACT__ = handleAct;
  window.__AVA_RENDER__ = renderTable;
  window.__AVA_LOAD__ = loadHotels;

  function onReady() {
    if (!window.__ADP_ADMIN_WORKSPACE_AUTH__) return;
    wire();
    if (!state.loaded) loadHotels();
  }

  function onTab(ev) {
    if (ev && ev.detail && ev.detail.tab === "visibility-audit") {
      onReady();
      if (state.loaded) renderTable();
      else loadHotels();
    }
  }

  window.addEventListener("dealality-adp-admin-ready", onReady);
  window.addEventListener("dealality-adp-admin-tab", onTab);
})();
