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
    doc.body.classList.remove("h-100-vh");
    doc.documentElement.style.overflow = "visible";
    doc.documentElement.style.height = "auto";
    doc.body.style.overflow = "visible";
    doc.body.style.height = "auto";
    doc.body.style.minHeight = "100vh";
    try {
      if (global.$) {
        global.$(".page-loader").remove();
      }
    } catch (_) {}
    doc.querySelectorAll(".page-loader").forEach(function (el) {
      if (el && el.parentNode) el.parentNode.removeChild(el);
    });
    doc.querySelectorAll(".loading-bar-wrapper").forEach(function (el) {
      el.style.display = "none";
    });
  }

  function injectLandingScrollbarStyles(doc) {
    if (!doc || doc.getElementById("dl-landing-shell-scrollbar")) return;
    var style = doc.createElement("style");
    style.id = "dl-landing-shell-scrollbar";
    style.textContent =
      "html,body{scrollbar-width:thin!important;scrollbar-color:#6c72ff #080f25!important}" +
      "html::-webkit-scrollbar,body::-webkit-scrollbar{width:8px!important}" +
      "html::-webkit-scrollbar-track,body::-webkit-scrollbar-track{background:#080f25!important;border-left:1px solid rgba(87,195,255,.35)!important}" +
      "html::-webkit-scrollbar-thumb,body::-webkit-scrollbar-thumb{background:#6c72ff!important}" +
      "html::-webkit-scrollbar-thumb:hover,body::-webkit-scrollbar-thumb:hover{background:#8b90ff!important}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function bootstrapHomeNewLandingShell(doc) {
    if (!doc || !doc.getElementById("dealality-landing-embed")) return;
    injectLandingScrollbarStyles(doc);
    clearPageLoaderOverlays(doc);
    var nav =
      doc.querySelector(".navbar-2.w-nav") ||
      doc.querySelector('[role="banner"].w-nav') ||
      doc.querySelector(".w-nav");
    if (nav) {
      nav.querySelectorAll(".navbar_content").forEach(function (el) {
        el.style.minHeight = "52px";
        el.style.paddingTop = "4px";
        el.style.paddingBottom = "4px";
      });
      nav.querySelectorAll(".navbar_logo").forEach(function (el) {
        el.style.height = "28px";
        el.style.maxHeight = "28px";
      });
    }
    var offset = nav ? Math.ceil(nav.getBoundingClientRect().height) : 48;
    var shell = doc.querySelector(".dl-landing-embed-full");
    var wrap = doc.querySelector(".dl-landing-embed-wrap");
    if (shell) {
      shell.style.paddingTop = "0";
      shell.style.overflow = "visible";
      shell.style.width = "100%";
      shell.style.background = "#080f25";
    }
    if (wrap) {
      wrap.style.paddingTop = offset + "px";
      wrap.style.overflow = "visible";
      wrap.style.minHeight = "0";
      wrap.style.width = "100%";
      wrap.style.background = "#080f25";
    }
    var iframe = doc.getElementById("dealality-landing-embed");
    if (iframe) {
      iframe.style.display = "block";
      iframe.style.width = "100%";
      iframe.style.minHeight = "80vh";
      iframe.style.height = Math.max(Number(iframe.offsetHeight) || 0, 3200) + "px";
      iframe.style.visibility = "visible";
      iframe.style.opacity = "1";
    }
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

  function resolveRailwayScriptBase() {
    var base = (global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || "")
      .trim()
      .replace(/\/+$/, "");
    return base;
  }

  function ensureLandingNavbarBridge(doc) {
    if (!doc || global.__dealalityLandingNavInstalled || global.__dealalityLandingParentLoading) {
      return;
    }
    if (!doc.querySelector(".navbar-2.w-nav, .w-nav")) return;

    var base = resolveRailwayScriptBase();
    if (!base) return;

    global.__dealalityLandingParentLoading = true;
    var script = doc.createElement("script");
    script.src = base + "/marketing/dealality-landing-webflow-parent.js";
    script.defer = true;
    script.onload = function () {
      global.__dealalityLandingParentLoading = false;
    };
    script.onerror = function () {
      global.__dealalityLandingParentLoading = false;
      if (global.console && global.console.warn) {
        global.console.warn(
          "[DealalityWebflowAccountNotice] Could not load landing navbar bridge:",
          script.src
        );
      }
    };
    (doc.head || doc.documentElement).appendChild(script);
  }

  if (global.document) {
    ensureToastPatch();
    ensureLandingNavbarBridge(global.document);
    watchUserContext();

    global.document.addEventListener("dealality-me-ready", function (ev) {
      if (ev && ev.detail && ev.detail.data) applyFromMe(ev.detail.data);
    });

    global.document.addEventListener("DOMContentLoaded", function () {
      ensureToastPatch();
      ensureLandingNavbarBridge(global.document);
      bootstrapHomeNewLandingShell(global.document);
      if (global.__dealalityUserContext) applyFromMe(global.__dealalityUserContext);
    });

    global.addEventListener("load", function () {
      bootstrapHomeNewLandingShell(global.document);
    });
    global.setTimeout(function () {
      bootstrapHomeNewLandingShell(global.document);
    }, 1200);
    global.addEventListener("resize", function () {
      bootstrapHomeNewLandingShell(global.document);
    });

    if (global.__dealalityUserContext) applyFromMe(global.__dealalityUserContext);
    bootstrapHomeNewLandingShell(global.document);
  }

  global.DealalityWebflowAccountNotice = {
    apply: applyFromMe,
    shouldSuppressBrandToast: shouldSuppressBrandToast,
  };
})(typeof window !== "undefined" ? window : global);
