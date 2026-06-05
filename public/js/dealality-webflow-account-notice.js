/**
 * Webflow: pending-approval banner + suppress misleading "No brands assigned" toast.
 * Works with inline loadUserContext even if footer was not updated to call .apply().
 */
(function (global) {
  "use strict";

  var STYLE_ID = "dealality-account-notice-style";
  var BANNER_ID = "dealality-account-pending-banner";
  var TOAST_PATCHED = false;
  var CONTEXT_POLL_MS = 250;
  var CONTEXT_POLL_MAX_MS = 30000;

  function shouldSuppressBrandToast() {
    if (global.__dealalitySuppressBrandToast) return true;
    var data = global.__dealalityUserContext;
    if (!data || data.success !== true) return false;
    var access = data.accountAccess;
    if (access && access.pendingApproval) return true;
    var d = data.dealality;
    if (d && (d.isOwner || d.isAdmin)) return true;
    return false;
  }

  function patchBrandAssignmentToast() {
    if (TOAST_PATCHED || !global.$ || typeof global.$.toast !== "function") return false;
    var original = global.$.toast;
    global.$.toast = function (options) {
      var text = options && options.text ? String(options.text) : "";
      if (/no brands assigned/i.test(text) && shouldSuppressBrandToast()) {
        return;
      }
      return original.apply(this, arguments);
    };
    TOAST_PATCHED = true;
    return true;
  }

  function ensureToastPatch() {
    if (patchBrandAssignmentToast()) return;
    var attempts = 0;
    var timer = global.setInterval(function () {
      attempts += 1;
      if (patchBrandAssignmentToast() || attempts >= 80) {
        global.clearInterval(timer);
      }
    }, CONTEXT_POLL_MS);
  }

  function injectStyles() {
    var doc = global.document;
    if (!doc || doc.getElementById(STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = STYLE_ID;
    style.textContent =
      "#" +
      BANNER_ID +
      "{box-sizing:border-box;max-width:720px;margin:24px auto;padding:20px 24px;" +
      "border-radius:12px;border:1px solid rgba(100,180,255,.35);" +
      "background:linear-gradient(135deg,rgba(8,20,48,.95),rgba(12,32,64,.92));" +
      "color:#e8eef8;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;" +
      "box-shadow:0 8px 32px rgba(0,0,0,.35);}" +
      "#" +
      BANNER_ID +
      " h2{margin:0 0 8px;font-size:1.125rem;font-weight:600;color:#fff;}" +
      "#" +
      BANNER_ID +
      " p{margin:0;font-size:.9375rem;line-height:1.55;color:#c5d4ea;}" +
      "#" +
      BANNER_ID +
      " .dealality-account-notice__hint{margin-top:12px;font-size:.8125rem;color:#8fa3c4;}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function findBannerHost() {
    var doc = global.document;
    if (!doc || !doc.body) return null;
    return (
      doc.querySelector("main") ||
      doc.querySelector('[class*="main-content"]') ||
      doc.querySelector('[class*="page-wrapper"]') ||
      doc.querySelector(".page-wrapper") ||
      doc.body
    );
  }

  function removeBanner() {
    var existing = global.document && global.document.getElementById(BANNER_ID);
    if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
  }

  function showPendingBanner(access) {
    var doc = global.document;
    if (!doc || !doc.body || !access || !access.pendingApproval) return;

    injectStyles();
    removeBanner();

    var host = findBannerHost();
    if (!host) return;

    var banner = doc.createElement("section");
    banner.id = BANNER_ID;
    banner.setAttribute("role", "status");
    banner.setAttribute("aria-live", "polite");
    banner.innerHTML =
      "<h2>" +
      escapeHtml(access.userTitle || "Account pending approval") +
      "</h2>" +
      "<p>" +
      escapeHtml(
        access.userMessage ||
          "Thanks for signing up. Our team will enable your platform access after review."
      ) +
      "</p>" +
      '<p class="dealality-account-notice__hint">Questions? Contact support@dealality.com</p>';

    host.insertBefore(banner, host.firstChild || null);
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function applyFromMe(data) {
    if (!data || data.success !== true) return;
    var access = data.accountAccess;
    if (!access) return;

    if (access.suppressBrandAssignmentToast || access.pendingApproval) {
      global.__dealalitySuppressBrandToast = true;
      showPendingBanner(access);
      return;
    }

    global.__dealalitySuppressBrandToast = false;
    removeBanner();
  }

  function watchUserContext() {
    var started = Date.now();
    var timer = global.setInterval(function () {
      if (global.__dealalityUserContext && global.__dealalityUserContext.success === true) {
        applyFromMe(global.__dealalityUserContext);
      }
      if (Date.now() - started >= CONTEXT_POLL_MAX_MS) {
        global.clearInterval(timer);
      }
    }, CONTEXT_POLL_MS);
  }

  if (global.document) {
    ensureToastPatch();
    watchUserContext();

    global.document.addEventListener("dealality-me-ready", function (ev) {
      if (ev && ev.detail && ev.detail.data) applyFromMe(ev.detail.data);
    });

    global.document.addEventListener("DOMContentLoaded", function () {
      ensureToastPatch();
      if (global.__dealalityUserContext) applyFromMe(global.__dealalityUserContext);
    });

    if (global.__dealalityUserContext) applyFromMe(global.__dealalityUserContext);
  }

  global.DealalityWebflowAccountNotice = {
    apply: applyFromMe,
    shouldSuppressBrandToast: shouldSuppressBrandToast,
  };
})(typeof window !== "undefined" ? window : global);
