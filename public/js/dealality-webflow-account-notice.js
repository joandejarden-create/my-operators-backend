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
      "{box-sizing:border-box;position:fixed;top:20px;left:50%;transform:translateX(-50%);" +
      "width:min(720px,calc(100vw - 32px));padding:20px 24px;z-index:2147483000;" +
      "border-radius:12px;border:1px solid rgba(120,190,255,.55);" +
      "background:#0b1a3a;color:#f0f4fc;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;" +
      "box-shadow:0 12px 40px rgba(0,0,0,.55);filter:none!important;-webkit-filter:none!important;" +
      "backdrop-filter:none!important;-webkit-backdrop-filter:none!important;isolation:isolate;}" +
      "#" +
      BANNER_ID +
      " h2{margin:0 0 8px;font-size:1.125rem;font-weight:600;color:#fff;}" +
      "#" +
      BANNER_ID +
      " p{margin:0;font-size:.9375rem;line-height:1.55;color:#dbe6f8;}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function clearPageLoaderOverlays(doc) {
    if (!doc || !doc.body) return;
    doc.body.classList.remove("overflow-hidden");
    try {
      if (global.$) {
        global.$(".page-loader").remove();
      }
    } catch (_) {}
    doc.querySelectorAll(".page-loader").forEach(function (el) {
      if (el && el.parentNode) el.parentNode.removeChild(el);
    });
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
    clearPageLoaderOverlays(doc);

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
      "</p>";

    doc.body.appendChild(banner);
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
