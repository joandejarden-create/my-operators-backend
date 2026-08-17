/**
 * Webflow signup — track Terms & Privacy checkbox into Dealality API/Airtable.
 * Expects window.DEALALITY_API_BASE (set in Webflow site head).
 */
(function (global) {
  "use strict";

  var TERMS_VERSION = "2026-07-27";
  var FORM_ID = "wf-form-Signup-Form";
  var CHECKBOX_ID = "Agree-with-Terms";

  function apiBase() {
    return String(global.DEALALITY_API_BASE || "")
      .trim()
      .replace(/\/$/, "");
  }

  function val(selector) {
    var el = document.querySelector(selector);
    return el && typeof el.value === "string" ? el.value.trim() : "";
  }

  function isAgreed() {
    var cb = document.getElementById(CHECKBOX_ID);
    return !!(cb && cb.checked);
  }

  function buildPayload(memberstackId) {
    return {
      email: val('input[data-ms-member="email"], input[type="email"], #Email'),
      firstName: val(
        'input[data-ms-member="first-name"], input[data-ms-member="firstName"], #First-Name, #First-name'
      ),
      lastName: val(
        'input[data-ms-member="last-name"], input[data-ms-member="lastName"], #Last-Name, #Last-name'
      ),
      companyName: val(
        'input[data-ms-member="company-name"], #Company-Name, #Company-name'
      ),
      companyType: val("select#Company-Type, #Company-Type, select[name='Company Type']"),
      agreeWithTerms: true,
      termsVersion: TERMS_VERSION,
      termsAcceptedAt: new Date().toISOString(),
      memberstackId: memberstackId || "webflow-signup",
    };
  }

  function postAcceptance(payload) {
    var base = apiBase();
    if (!base) {
      console.error("[dc-signup-terms-track] DEALALITY_API_BASE missing");
      return Promise.resolve({ ok: false, skipped: true });
    }
    return fetch(base + "/api/signup-terms-acceptance", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      mode: "cors",
      credentials: "omit",
    })
      .then(function (res) {
        return res.json().then(function (data) {
          return { httpStatus: res.status, data: data };
        });
      })
      .catch(function (err) {
        console.error("[dc-signup-terms-track] network error:", err);
        return { ok: false, error: err && err.message };
      });
  }

  function guardSubmit(event) {
    var form = event.target;
    if (!form || form.id !== FORM_ID) return;
    if (!isAgreed()) {
      event.preventDefault();
      event.stopPropagation();
      if (typeof global.alert === "function") {
        global.alert(
          "Please agree to the Terms of Service and Privacy Policy to continue."
        );
      }
      return false;
    }
    // Fire-and-forget; Memberstack continues signup
    var payload = buildPayload();
    if (!payload.email) {
      console.warn("[dc-signup-terms-track] no email yet; will retry after signup");
      return;
    }
    postAcceptance(payload).then(function (result) {
      if (result && result.httpStatus && result.httpStatus >= 400) {
        console.error("[dc-signup-terms-track] API error:", result);
      }
    });
  }

  function trackAfterMember(member) {
    if (!isAgreed()) return;
    var id =
      (member && member.data && (member.data.id || member.data.auth?.id)) ||
      "";
    var email =
      (member && member.data && member.data.auth?.email) ||
      (member && member.data && member.data.email) ||
      "";
    var payload = buildPayload(id);
    if (email) payload.email = String(email).trim().toLowerCase();
    if (!payload.email) return;
    postAcceptance(payload);
  }

  function bind() {
    document.addEventListener("submit", guardSubmit, true);

    var ms = global.$memberstackDom;
    if (ms && typeof ms.on === "function") {
      try {
        ms.on("member.signup", trackAfterMember);
      } catch (err) {
        console.warn("[dc-signup-terms-track] member.signup bind failed:", err);
      }
    }

    // Fallback: after Memberstack sets current member on this page
    if (ms && typeof ms.getCurrentMember === "function") {
      setTimeout(function () {
        ms.getCurrentMember()
          .then(function (member) {
            if (member && member.data && isAgreed()) trackAfterMember(member);
          })
          .catch(function () {});
      }, 2500);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }

  global.DealalitySignupTermsTrack = {
    postAcceptance: postAcceptance,
    buildPayload: buildPayload,
    TERMS_VERSION: TERMS_VERSION,
  };
})(window);
