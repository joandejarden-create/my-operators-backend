/**
 * Admin — AI Demand Leak Audits list + actions.
 */
(function () {
  "use strict";

  var API = "/api/admin/adp-leak-audits";
  var promoteRequestId = null;
  var promoteConfirmationText = "";

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

  function renderCounts(counts) {
    var el = document.getElementById("alaCounts");
    if (!el) return;
    var keys = ["requested", "approved", "completed", "sent", "converted", "error"];
    el.innerHTML = keys
      .map(function (k) {
        return (
          '<div class="adr-count"><span class="adr-count__n">' +
          esc(String(counts[k] || 0)) +
          '</span><span class="adr-count__l">' +
          esc(k) +
          "</span></div>"
        );
      })
      .join("");
  }

  function actionButtons(row) {
    var a = row.actions || {};
    var parts = [];
    if (a.approve) {
      parts.push(
        '<button type="button" class="adr-btn adr-btn--secondary ala-act" data-act="approve" data-id="' +
          esc(row.id) +
          '">Approve</button>'
      );
    }
    if (a.runAudit) {
      parts.push(
        '<button type="button" class="adr-btn ala-act" data-act="run" data-id="' +
          esc(row.id) +
          '">Run Audit</button>'
      );
    }
    if (a.viewReport && row.reportId) {
      parts.push(
        '<a class="adr-btn adr-btn--secondary" href="/adp-leak-audit/' +
          esc(row.reportId) +
          '" target="_blank" rel="noopener">View Report</a>'
      );
    }
    if (a.markSent) {
      parts.push(
        '<button type="button" class="adr-btn adr-btn--secondary ala-act" data-act="sent" data-id="' +
          esc(row.id) +
          '">Mark Sent</button>'
      );
    }
    if (a.promoteToPilot) {
      parts.push(
        '<button type="button" class="adr-btn ala-act" data-act="promote" data-id="' +
          esc(row.id) +
          '">Promote to Paid ADP Pilot</button>'
      );
    }
    return parts.join(" ");
  }

  function renderRows(rows) {
    var body = document.getElementById("alaTableBody");
    if (!rows.length) {
      body.innerHTML =
        '<tr><td colspan="8">No leak audit requests yet. Create one to get started.</td></tr>';
      return;
    }
    body.innerHTML = rows
      .map(function (r) {
        return (
          "<tr>" +
          "<td>" +
          esc(r.hotelName) +
          "</td>" +
          "<td>" +
          esc(r.contactName || "—") +
          (r.contactEmail ? "<br><small>" + esc(r.contactEmail) + "</small>" : "") +
          "</td>" +
          "<td>" +
          esc(r.companyName || "—") +
          "</td>" +
          "<td>" +
          esc(r.demandSegmentOfInterest || "—") +
          "</td>" +
          "<td>" +
          esc(r.source || "—") +
          "</td>" +
          "<td><span class=\"adr-badge\">" +
          esc(r.status) +
          "</span></td>" +
          "<td>" +
          esc(fmtDate(r.createdAt)) +
          "</td>" +
          '<td class="ala-actions">' +
          actionButtons(r) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadList() {
    var qs = new URLSearchParams();
    var q = document.getElementById("alaSearch").value.trim();
    var status = document.getElementById("alaFilterStatus").value;
    if (q) qs.set("q", q);
    if (status) qs.set("status", status);
    var res = await authFetch(API + (qs.toString() ? "?" + qs.toString() : ""));
    var data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error((data && data.message) || "Failed to load audits");
    }
    renderCounts(data.counts || {});
    renderRows(data.requests || []);
  }

  async function postAction(path) {
    var res = await authFetch(API + path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    var data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error((data && (data.message || data.error)) || "Action failed");
    }
    return data;
  }

  function openPromoteModal(id) {
    promoteRequestId = id;
    var modal = document.getElementById("alaPromoteModal");
    var body = document.getElementById("alaPromoteBody");
    body.textContent =
      promoteConfirmationText ||
      "This will create or connect this hotel to a paid ADP monitoring workflow. Free audit records will remain separate. No production records will be overwritten.";
    modal.hidden = false;
  }

  function closePromoteModal() {
    promoteRequestId = null;
    document.getElementById("alaPromoteModal").hidden = true;
  }

  async function confirmPromote() {
    if (!promoteRequestId) return;
    var res = await authFetch(API + "/" + promoteRequestId + "/promote", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirmed: true }),
    });
    var data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error((data && (data.message || data.error)) || "Promote failed");
    }
    closePromoteModal();
    await loadList();
  }

  async function onActionClick(ev) {
    var btn = ev.target.closest(".ala-act");
    if (!btn) return;
    var id = btn.getAttribute("data-id");
    var act = btn.getAttribute("data-act");
    try {
      if (act === "approve") await postAction("/" + id + "/approve");
      if (act === "run") await postAction("/" + id + "/run");
      if (act === "sent") await postAction("/" + id + "/mark-sent");
      if (act === "promote") {
        openPromoteModal(id);
        return;
      }
      await loadList();
    } catch (err) {
      alert(err.message || String(err));
    }
  }

  async function init() {
    if (!window.SupportAdminGate) {
      showDenied("Unable to load admin gate.");
      return;
    }
    var allowed = await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
      contentId: "supportGateContent",
    });
    if (!allowed) return;

    var loadingEl = document.getElementById("supportGateLoading");
    window.SupportAdminGate.setPageLoadingMessage(
      loadingEl,
      "Loading AI Demand Leak Audits…"
    );

    try {
      var metaRes = await authFetch(API + "/meta");
      var meta = await metaRes.json();
      if (meta && meta.promoteConfirmationText) {
        promoteConfirmationText = meta.promoteConfirmationText;
      }
      await loadList();
      window.SupportAdminGate.hidePageLoading(loadingEl);
      document.getElementById("supportGateContent").hidden = false;
    } catch (err) {
      showDenied(err && err.message ? err.message : "Unable to load leak audits.");
      return;
    }

    document.getElementById("alaApplyFilters").addEventListener("click", function () {
      loadList().catch(function (e) {
        alert(e.message);
      });
    });
    document.getElementById("alaTableBody").addEventListener("click", onActionClick);
    document.getElementById("alaPromoteCancel").addEventListener("click", closePromoteModal);
    document.getElementById("alaPromoteConfirm").addEventListener("click", function () {
      confirmPromote().catch(function (e) {
        alert(e.message);
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
