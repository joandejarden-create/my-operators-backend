/**
 * Admin — New AI Demand Leak Audit intake form.
 */
(function () {
  "use strict";

  var API = "/api/admin/adp-leak-audits";

  function authFetch(url, opts) {
    opts = opts || {};
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, opts);
    }
    return fetch(url, Object.assign({ credentials: "same-origin" }, opts));
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

  function formPayload(form) {
    var fd = new FormData(form);
    var out = {};
    fd.forEach(function (value, key) {
      out[key] = String(value || "").trim();
    });
    if (out.openToPaidPilot) {
      var pilotLine = "Open to paid monthly pilot: " + out.openToPaidPilot;
      out.notes = out.notes ? out.notes + "\n" + pilotLine : pilotLine;
      delete out.openToPaidPilot;
    }
    return out;
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
    window.SupportAdminGate.hidePageLoading(loadingEl);
    document.getElementById("supportGateContent").hidden = false;

    var form = document.getElementById("alaIntakeForm");
    var errEl = document.getElementById("alaFormError");
    var okEl = document.getElementById("alaFormSuccess");

    form.addEventListener("submit", async function (ev) {
      ev.preventDefault();
      errEl.hidden = true;
      okEl.hidden = true;
      var payload = formPayload(form);
      if (!payload.hotelName) {
        errEl.hidden = false;
        errEl.textContent = "Hotel name is required.";
        return;
      }
      try {
        var res = await authFetch(API, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        var data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error((data && (data.message || data.error)) || "Create failed");
        }
        okEl.hidden = false;
        okEl.textContent =
          "Request created (" +
          data.request.id +
          ") with status requested. Production hotel records were not touched.";
        form.reset();
        setTimeout(function () {
          window.location.href = "/admin/adp-leak-audits/reports";
        }, 900);
      } catch (err) {
        errEl.hidden = false;
        errEl.textContent = err.message || String(err);
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
