/**
 * Webflow: pending-approval banner + suppress misleading "No brands assigned" toast.
 * Works with inline loadUserContext even if footer was not updated to call .apply().
 */
(function (global) {
  "use strict";

  var STYLE_ID = "dealality-account-notice-style";
  var AUTH_BG_STYLE_ID = "dl-auth-landing-bg-style";
  var AUTH_MOBILE_LAYOUT_STYLE_ID = "dl-auth-mobile-layout-style";
  var AUTH_BG_EARLY_STYLE_ID = "dl-auth-landing-bg-early";
  var PLATFORM_EARLY_STYLE_ID = "dl-platform-shell-early";
  var PLATFORM_SCROLLBAR_STYLE_ID = "dl-platform-shell-scrollbar";
  var AUTH_BG_ROOT_ID = "dl-auth-bg-root";
  var BANNER_ID = "dealality-account-pending-banner";
  var LANDING_BG = "#080f25";
  var WEBFLOW_BLOB_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/68108c2a063eeb5d1bd7b088_sales-home-bg-blob-small-dashdark-webflow-template.svg";
  var TOAST_PATCHED = false;
  var CONTEXT_POLL_MS = 250;
  var CONTEXT_POLL_MAX_MS = 30000;
  var AUTH_NAV_LABEL_TO_ID = {
    "for owners": "owners",
    "for brands": "brands",
    "for brands & operators": "brands",
    "for partners": "partners",
    "how it works": "how",
    "the platform": "how",
    "see the platform": "how",
    faqs: "faq",
    faq: "faq",
  };
  var LANDING_SECTION_ALIASES = { platform: "how", persona: "owners" };
  var LANDING_PARENT_SOURCE = "dealality-landing-parent";
  var LANDING_SECTION_IDS = ["owners", "brands", "partners", "how", "faq", "hero", "persona", "platform"];
  var NAVBAR_SPACING_STYLE_ID = "dl-navbar-spacing-lock";
  var NAVBAR_LOGO_RELEASE_STYLE_ID = "dl-navbar-logo-release";
  var NAVBAR_LOGO_HEIGHT_PX = 50;
  var NAVBAR_CONTENT_MIN_HEIGHT_PX = 58;
  var NAVBAR_SHELL_HEIGHT_PX = 66;
  var HERO_GAP_BELOW_NAV_PX = 36;
  var EMBED_TOP_PADDING_PX = NAVBAR_SHELL_HEIGHT_PX + HERO_GAP_BELOW_NAV_PX;

  function stripEmbedNavbarLogoOverrides(doc) {
    if (!doc) return;
    doc.querySelectorAll(".dl-landing-embed-full style, .w-embed style").forEach(function (styleEl) {
      var css = styleEl.textContent || "";
      if (css.indexOf("navbar_logo") === -1 && css.indexOf("navbar_content") === -1) return;
      styleEl.textContent = css
        .replace(/\.navbar-2\s+\.navbar_logo-link\s*\{[^}]*\}/gi, "")
        .replace(/\.navbar-2\s+\.navbar_content\s*\{[^}]*\}/gi, "")
        .replace(/\.navbar-2\s+\.navbar_logo\s*\{[^}]*\}/gi, "");
    });
  }

  function applyNavbarLogoSize(doc) {
    if (!doc) return;
    wireNavbarLogoHome(doc);
    stripEmbedNavbarLogoOverrides(doc);
    var style = doc.getElementById(NAVBAR_LOGO_RELEASE_STYLE_ID);
    if (!style) {
      style = doc.createElement("style");
      style.id = NAVBAR_LOGO_RELEASE_STYLE_ID;
      (doc.body || doc.head || doc.documentElement).appendChild(style);
    }
    style.textContent =
      ".navbar-2.w-nav{height:" +
      NAVBAR_SHELL_HEIGHT_PX +
      "px!important;min-height:" +
      NAVBAR_SHELL_HEIGHT_PX +
      "px!important;max-height:" +
      NAVBAR_SHELL_HEIGHT_PX +
      "px!important;box-sizing:border-box!important;overflow:visible!important}" +
      ".navbar-2 .navbar_content{height:" +
      NAVBAR_CONTENT_MIN_HEIGHT_PX +
      "px!important;min-height:" +
      NAVBAR_CONTENT_MIN_HEIGHT_PX +
      "px!important;max-height:" +
      NAVBAR_CONTENT_MIN_HEIGHT_PX +
      "px!important;padding-top:4px!important;padding-bottom:4px!important;padding-right:16px!important;box-sizing:border-box!important;overflow:visible!important}" +
      ".navbar-2 .navbar_logo-link{height:auto!important;min-height:0!important;max-height:" +
      NAVBAR_LOGO_HEIGHT_PX +
      "px!important}" +
      ".navbar-2 .navbar_logo,.navbar-2 img.navbar_logo{height:" +
      NAVBAR_LOGO_HEIGHT_PX +
      "px!important;max-height:" +
      NAVBAR_LOGO_HEIGHT_PX +
      "px!important;width:auto!important}" +
      ".navbar-2 .nav_links,.navbar-2 a.nav_links,.navbar-2 .nav_link,.navbar-2 a.nav_link,.navbar-2 .w-nav-link,.navbar-2 .w-nav-link.w--current{padding:.35rem .5rem!important;line-height:1.2!important;border:0!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important;font-weight:400!important}" +
      ".navbar-2 .nav-compact,.navbar-2 a.nav-compact{padding:.4rem .85rem!important;line-height:1.2!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important;font-weight:400!important}";
    var nav = doc.querySelector(".navbar-2.w-nav") || doc.querySelector(".w-nav");
    if (nav) {
      nav.querySelectorAll(".navbar_content").forEach(function (el) {
        el.style.minHeight = NAVBAR_CONTENT_MIN_HEIGHT_PX + "px";
        el.style.paddingTop = "4px";
        el.style.paddingBottom = "4px";
        el.style.paddingRight = "16px";
      });
      nav.querySelectorAll(".navbar_logo").forEach(function (el) {
        el.style.height = NAVBAR_LOGO_HEIGHT_PX + "px";
        el.style.maxHeight = NAVBAR_LOGO_HEIGHT_PX + "px";
        el.style.width = "auto";
      });
    }
    doc.querySelectorAll(".navbar-2 img.navbar_logo").forEach(function (img) {
      img.removeAttribute("height");
      img.style.setProperty("height", NAVBAR_LOGO_HEIGHT_PX + "px", "important");
      img.style.setProperty("max-height", NAVBAR_LOGO_HEIGHT_PX + "px", "important");
      img.style.setProperty("width", "auto", "important");
    });
  }

  function scheduleNavbarLogoFix(doc) {
    if (!doc) return;
    applyNavbarLogoSize(doc);
    [100, 500, 1500, 3000].forEach(function (ms) {
      global.setTimeout(function () {
        applyNavbarLogoSize(doc);
      }, ms);
    });
    if (typeof global.MutationObserver === "function" && doc.body && !global.__dealalityNavbarLogoObserver) {
      global.__dealalityNavbarLogoObserver = true;
      var logoFixTimer = null;
      var observer = new global.MutationObserver(function () {
        if (logoFixTimer) return;
        logoFixTimer = global.setTimeout(function () {
          logoFixTimer = null;
          applyNavbarLogoSize(doc);
        }, 400);
      });
      var navRoot = doc.querySelector(".navbar-2.w-nav") || doc.querySelector(".w-nav");
      if (navRoot) {
        observer.observe(navRoot, { childList: true, subtree: true, attributes: true, attributeFilter: ["class", "style", "height"] });
      }
    }
  }

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

  function injectGlobalNavbarSpacingStyles(doc) {
    if (!doc || doc.getElementById(NAVBAR_SPACING_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = NAVBAR_SPACING_STYLE_ID;
    style.textContent =
      ".navbar-2 .nav_links,.navbar-2 a.nav_links,.navbar-2 .nav_link,.navbar-2 a.nav_link,.navbar-2 .w-nav-link,.navbar-2 .w-nav-link.w--current{padding:.35rem .5rem!important;margin-left:0!important;margin-right:0!important;font-weight:400!important}" +
      ".navbar-2 .nav-compact,.navbar-2 a.nav-compact{padding:.4rem .85rem!important;line-height:1.2!important;margin-left:0!important;margin-right:0!important;font-weight:400!important}" +
      ".navbar-2 .w-nav-menu{display:flex!important;align-items:center!important;column-gap:0!important;row-gap:0!important}" +
      ".navbar-2 .navbar_content{padding-right:16px!important}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function applyGlobalNavbarSpacing(doc) {
    if (!doc) return;
    injectGlobalNavbarSpacingStyles(doc);
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

  function isAuthMarketingPage() {
    try {
      var path = (global.location.pathname || "").replace(/\/+$/, "").toLowerCase();
      return (
        path === "/signup" ||
        path === "/signup-new" ||
        path === "/log-in" ||
        path === "/login" ||
        path === "/join" ||
        path.indexOf("/signup") === 0 ||
        path.indexOf("/log-in") === 0
      );
    } catch (err) {
      return false;
    }
  }

  function normalizeLandingHomePath(path) {
    var raw = String(path || "/").trim();
    if (!raw || raw === "/") return "/";
    return raw.replace(/\/+$/, "");
  }

  var LANDING_HOME_PATH = normalizeLandingHomePath(global.DEALALITY_LANDING_HOME_PATH);

  function isLandingHomePage() {
    try {
      return normalizeLandingHomePath(global.location.pathname || "/") === LANDING_HOME_PATH;
    } catch (err) {
      return false;
    }
  }

  function isPlatformDashboardPage() {
    if (isAuthMarketingPage() || isLandingHomePage()) return false;
    try {
      var path = (global.location.pathname || "").replace(/\/+$/, "").toLowerCase();
      if (!path) return false;
      return /^\/(hotel-owner|brand|member|asset-manager|user-management|my-brands-v2)(\/|$)/.test(path);
    } catch (err) {
      return false;
    }
  }

  function landingHomeHref(sectionId) {
    var hash = sectionId ? "#" + sectionId : "";
    if (LANDING_HOME_PATH === "/") return "/" + hash;
    return LANDING_HOME_PATH + hash;
  }

  function resolveRailwayScriptBase() {
    return (global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || "")
      .trim()
      .replace(/\/+$/, "");
  }

  function hasNativeAuthBlob(doc) {
    return !!(
      doc &&
      doc.querySelector &&
      doc.querySelector(".signup-page-bg .coming-soon-img, .signup-page-bg img")
    );
  }

  function applyAuthPageEarlySkin() {
    if (!isAuthMarketingPage() || !global.document) return;
    var doc = global.document;
    doc.documentElement.classList.add("dl-auth-landing-skin");
    if (doc.getElementById(AUTH_BG_EARLY_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = AUTH_BG_EARLY_STYLE_ID;
    style.textContent =
      "html,body{background:" +
      LANDING_BG +
      "!important;color:rgba(255,255,255,.62)!important}" +
      "html.dl-auth-landing-skin .page-wrapper,html.dl-auth-landing-skin .dashboard-main-section," +
      "html.dl-auth-landing-skin .dashboard-content,html.dl-auth-landing-skin .dashboard-main-content{background:transparent!important;background-image:none!important}" +
      "html.dl-auth-landing-skin .signup-page-bg{position:fixed!important;inset:0!important;z-index:0!important;" +
      "pointer-events:none!important;background:" +
      LANDING_BG +
      "!important}" +
      "html.dl-auth-landing-skin .signup-page-bg .coming-soon-img{position:absolute;top:-10%;right:-8%;left:auto;" +
      "width:min(720px,55vw);max-width:none;opacity:.55;height:auto}" +
      "html.dl-auth-landing-skin .coming-soon-overlay,html.dl-auth-landing-skin .loading-bar-wrapper," +
      "html.dl-auth-landing-skin .page-loader{display:none!important}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  applyAuthPageEarlySkin();

  function landingShellScrollbarCss() {
    return (
      "html,body{scrollbar-width:thin!important;scrollbar-color:#6c72ff #080f25!important}" +
      "html::-webkit-scrollbar,body::-webkit-scrollbar{width:8px!important}" +
      "html::-webkit-scrollbar-track,body::-webkit-scrollbar-track{background:#080f25!important;border-left:1px solid rgba(87,195,255,.35)!important}" +
      "html::-webkit-scrollbar-thumb,body::-webkit-scrollbar-thumb{background:#6c72ff!important}" +
      "html::-webkit-scrollbar-thumb:hover,body::-webkit-scrollbar-thumb:hover{background:#8b90ff!important}"
    );
  }

  function injectLandingScrollbarStyles(doc) {
    if (!doc || doc.getElementById("dl-landing-shell-scrollbar")) return;
    var style = doc.createElement("style");
    style.id = "dl-landing-shell-scrollbar";
    style.textContent = landingShellScrollbarCss();
    (doc.head || doc.documentElement).appendChild(style);
  }

  /** Match app.css / Railway embed scrollbars (6px, neutral thumb — not landing purple). */
  function platformShellScrollbarCss() {
    return (
      "html.dl-platform-shell,html.dl-platform-shell body{scrollbar-gutter:stable!important;scrollbar-width:thin!important;scrollbar-color:#37446b #080f25!important}" +
      "html.dl-platform-shell::-webkit-scrollbar,html.dl-platform-shell body::-webkit-scrollbar{width:6px!important;height:6px!important}" +
      "html.dl-platform-shell::-webkit-scrollbar-track,html.dl-platform-shell body::-webkit-scrollbar-track{background:#080f25!important;border-left:1px solid rgba(87,195,255,.35)!important}" +
      "html.dl-platform-shell::-webkit-scrollbar-thumb,html.dl-platform-shell body::-webkit-scrollbar-thumb{background:#37446b!important;border-radius:0!important}" +
      "html.dl-platform-shell::-webkit-scrollbar-thumb:hover,html.dl-platform-shell body::-webkit-scrollbar-thumb:hover{background:#7e89ac!important}" +
      "html.dl-platform-shell::-webkit-scrollbar-corner,html.dl-platform-shell body::-webkit-scrollbar-corner{background:#080f25!important}"
    );
  }

  function injectPlatformScrollbarStyles(doc) {
    if (!doc || doc.getElementById(PLATFORM_SCROLLBAR_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = PLATFORM_SCROLLBAR_STYLE_ID;
    style.textContent = platformShellScrollbarCss();
    (doc.head || doc.documentElement).appendChild(style);
  }

  function applyPlatformPageEarlySkin() {
    if (!isPlatformDashboardPage() || !global.document) return;
    var doc = global.document;
    doc.documentElement.classList.add("dl-platform-shell");
    injectPlatformScrollbarStyles(doc);
    if (doc.getElementById(PLATFORM_EARLY_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = PLATFORM_EARLY_STYLE_ID;
    style.textContent =
      "html,body{background:" +
      LANDING_BG +
      "!important}" +
      "html.dl-platform-shell .page-wrapper,html.dl-platform-shell .dashboard-main-section," +
      "html.dl-platform-shell .dashboard-content,html.dl-platform-shell .dashboard-main-content{background:" +
      LANDING_BG +
      "!important;background-image:none!important}" +
      "html.dl-platform-shell .dashboard-main-section iframe,html.dl-platform-shell .dashboard-content iframe{background:" +
      LANDING_BG +
      "!important}" +
      "html.dl-platform-shell .loading-bar-wrapper,html.dl-platform-shell .page-loader," +
      "html.dl-platform-shell .coming-soon-overlay{display:none!important}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  applyPlatformPageEarlySkin();

  function applyPlatformPageShell(doc) {
    if (!doc || !isPlatformDashboardPage()) return;
    applyPlatformPageEarlySkin();
    clearPageLoaderOverlays(doc);
    doc.querySelectorAll(".dashboard-main-section iframe, .dashboard-content iframe").forEach(function (frame) {
      frame.setAttribute("loading", "eager");
      frame.style.background = LANDING_BG;
    });
  }

  function isNavbarLink(link) {
    return !!(
      link &&
      link.closest &&
      link.closest(
        ".navbar-2, .w-nav, .navbar_content, .nav_links, .nav_link, .nav-compact, .w-nav-menu, .w-nav-link, nav, header"
      )
    );
  }

  function normalizeLandingSectionId(id) {
    if (!id) return id;
    var key = String(id).toLowerCase();
    return LANDING_SECTION_ALIASES[key] || key;
  }

  function postScrollToLandingEmbed(id) {
    var iframe = global.document && global.document.getElementById("dealality-landing-embed");
    if (!iframe || !iframe.contentWindow) return false;
    id = normalizeLandingSectionId(id);
    var msg = { source: LANDING_PARENT_SOURCE, type: "scrollTo", id: id };
    iframe.contentWindow.postMessage(msg, "*");
    global.setTimeout(function () {
      if (iframe.contentWindow) iframe.contentWindow.postMessage(msg, "*");
    }, 420);
    return true;
  }

  function isNavbarLogoLink(link) {
    if (!link) return false;
    if (link.closest && link.closest(".navbar_logo-link, .w-nav-brand")) return true;
    if (link.classList && (link.classList.contains("navbar_logo-link") || link.classList.contains("w-nav-brand"))) {
      return true;
    }
    return !!(link.querySelector && link.querySelector(".navbar_logo, img.navbar_logo"));
  }

  function wireNavbarLogoHome(doc) {
    if (!doc) return;
    var homeHref = landingHomeHref();
    doc.querySelectorAll(".navbar_logo-link, .w-nav-brand, a.navbar_logo-link").forEach(function (link) {
      link.setAttribute("href", homeHref);
      link.setAttribute("aria-label", "Dealality home");
    });
  }

  function handleNavbarLogoClick(event) {
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link || !isNavbarLogoLink(link)) return;
    wireNavbarLogoHome(global.document);
    if (isLandingHomePage() && global.document.getElementById("dealality-landing-embed")) {
      event.preventDefault();
      event.stopPropagation();
      global.history.replaceState(null, "", "/");
      global.scrollTo({ top: 0, behavior: "auto" });
      postScrollToLandingEmbed("hero");
      return;
    }
    if (!isLandingHomePage()) {
      event.preventDefault();
      event.stopPropagation();
      global.location.href = landingHomeHref();
    }
  }

  function scrollLandingHomeHashOnLoad() {
    if (!isLandingHomePage() || !global.document.getElementById("dealality-landing-embed")) return;
    var hash = (global.location.hash || "").replace(/^#/, "");
    if (!hash) return;
    hash = normalizeLandingSectionId(hash);
    postScrollToLandingEmbed(hash);
    global.setTimeout(function () {
      postScrollToLandingEmbed(hash);
    }, 700);
  }

  function resolveAuthNavId(link) {
    if (!link) return null;
    var href = (link.getAttribute("href") || "").trim();
    if (href) {
      if (href.charAt(0) === "#" && href.length > 1) return href.slice(1);
      try {
        var url = new URL(href, global.location.href);
        if (url.hash && url.hash.length > 1) return url.hash.slice(1);
      } catch (err) {
        if (global.console && global.console.warn) {
          global.console.warn("[DealalityWebflowAccountNotice] resolveAuthNavId failed", err);
        }
      }
    }
    var text = (link.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
    return AUTH_NAV_LABEL_TO_ID[text] || null;
  }

  function handleAuthPageNavbarClick(event) {
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link || !isNavbarLink(link)) return;
    var id = resolveAuthNavId(link);
    if (!id) return;
    id = normalizeLandingSectionId(id);

    if (isAuthMarketingPage()) {
      event.preventDefault();
      event.stopPropagation();
      global.location.href = landingHomeHref(id);
      return;
    }

    if (global.document.getElementById("dealality-landing-embed") && isLandingHomePage()) {
      event.preventDefault();
      event.stopPropagation();
      postScrollToLandingEmbed(id);
      if (global.history && global.history.replaceState) {
        global.history.replaceState(null, "", "#" + id);
      }
      return;
    }

    if (!global.document.getElementById("dealality-landing-embed") && !isLandingHomePage()) {
      event.preventDefault();
      global.location.href = landingHomeHref(id);
    }
  }

  function redirectAuthPageHashOnLoad() {
    if (!isAuthMarketingPage()) return;
    var hash = (global.location.hash || "").replace(/^#/, "").toLowerCase();
    if (!hash || LANDING_SECTION_IDS.indexOf(hash) < 0) return;
    global.location.replace(landingHomeHref(hash));
  }

  function installAuthPageNavBridge() {
    if (!global.document || global.__dealalityAuthNavInstalled) return;
    global.__dealalityAuthNavInstalled = true;
    global.document.addEventListener("click", handleNavbarLogoClick, true);
    global.document.addEventListener("click", handleAuthPageNavbarClick, true);
    redirectAuthPageHashOnLoad();
    global.addEventListener("hashchange", function () {
      if (!isLandingHomePage() || !global.document.getElementById("dealality-landing-embed")) return;
      var hash = (global.location.hash || "").replace(/^#/, "");
      if (!hash) return;
      postScrollToLandingEmbed(normalizeLandingSectionId(hash));
    });
    if (global.document.readyState === "loading") {
      global.document.addEventListener("DOMContentLoaded", scrollLandingHomeHashOnLoad);
    } else {
      scrollLandingHomeHashOnLoad();
    }
    global.addEventListener("load", scrollLandingHomeHashOnLoad);
  }

  function injectAuthLandingBackgroundStyles(doc) {
    if (!doc || doc.getElementById(AUTH_BG_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = AUTH_BG_STYLE_ID;
    style.textContent =
      "html.dl-auth-landing-skin,html.dl-auth-landing-skin body{background:" +
      LANDING_BG +
      "!important;color:rgba(255,255,255,.62)!important}" +
      "html.dl-auth-landing-skin .page-wrapper,html.dl-auth-landing-skin .dashboard-main-section," +
      "html.dl-auth-landing-skin .dashboard-content,html.dl-auth-landing-skin .dashboard-main-content{background:transparent!important;background-image:none!important}" +
      "html.dl-auth-landing-skin .dashboard-footer-wrapper{background:" +
      LANDING_BG +
      "!important;background-image:none!important}" +
      "html.dl-auth-landing-skin .signup-page-bg{position:fixed!important;inset:0!important;z-index:0!important;" +
      "pointer-events:none!important;background:" +
      LANDING_BG +
      "!important}" +
      "html.dl-auth-landing-skin .signup-page-bg .coming-soon-ss," +
      "html.dl-auth-landing-skin .signup-page-bg .coming-soon-img{display:block!important}" +
      "html.dl-auth-landing-skin .signup-page-bg .coming-soon-img{position:absolute;top:-10%;right:-8%;left:auto;" +
      "width:min(720px,55vw);max-width:none;opacity:.55;height:auto}" +
      "html.dl-auth-landing-skin .signup-wrapper,html.dl-auth-landing-skin .signup-left," +
      "html.dl-auth-landing-skin .signup-right,html.dl-auth-landing-skin .login-wrapper," +
      "html.dl-auth-landing-skin .login-left,html.dl-auth-landing-skin .login-right," +
      "html.dl-auth-landing-skin .memberstack-form{background:transparent!important;background-image:none!important}" +
      "html.dl-auth-landing-skin .dashboard-main-content.sales-page{position:relative;overflow:hidden;min-height:90vh}" +
      "html.dl-auth-landing-skin .page-wrapper,html.dl-auth-landing-skin .dashboard-main-section," +
      "html.dl-auth-landing-skin .dashboard-content,html.dl-auth-landing-skin .w-nav," +
      "html.dl-auth-landing-skin .navbar-2,html.dl-auth-landing-skin main{position:relative;z-index:1}" +
      "#" +
      AUTH_BG_ROOT_ID +
      "{position:fixed;inset:0;z-index:0;pointer-events:none;overflow:hidden;background:" +
      LANDING_BG +
      "}" +
      "#" +
      AUTH_BG_ROOT_ID +
      " .dl-auth-bg-grid{position:absolute;inset:0;background-image:" +
      "linear-gradient(45deg,rgba(108,114,255,.025) 1px,transparent 1px)," +
      "linear-gradient(-45deg,rgba(108,114,255,.025) 1px,transparent 1px);background-size:56px 56px}" +
      "#" +
      AUTH_BG_ROOT_ID +
      " .dl-auth-bg-blob{position:absolute;top:-10%;right:-8%;left:auto;width:min(720px,55vw);max-width:none;opacity:.55;height:auto}" +
      "html.dl-auth-landing-skin .coming-soon-overlay,html.dl-auth-landing-skin .loading-bar-wrapper," +
      "html.dl-auth-landing-skin .page-loader{display:none!important}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function injectAuthMobileLayoutStyles(doc) {
    if (!doc || doc.getElementById(AUTH_MOBILE_LAYOUT_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = AUTH_MOBILE_LAYOUT_STYLE_ID;
    style.textContent =
      "@media(max-width:767px){" +
      "html.dl-auth-landing-skin,html.dl-auth-landing-skin body{overflow-x:hidden!important}" +
      "html.dl-auth-landing-skin .dashboard-content,html.dl-auth-landing-skin .dashboard-main-content.sales-page{width:100%!important;max-width:100%!important;padding-left:20px!important;padding-right:20px!important;box-sizing:border-box!important;overflow-x:hidden!important}" +
      "html.dl-auth-landing-skin .form-block,html.dl-auth-landing-skin .signup-left .form-block{width:100%!important;max-width:min(440px,100%)!important;margin-left:auto!important;margin-right:auto!important;padding-left:0!important;padding-right:0!important;display:flex!important;flex-direction:column!important;align-items:center!important;text-align:center!important;box-sizing:border-box!important}" +
      "html.dl-auth-landing-skin .logo-holder{width:100%!important;max-width:100%!important;margin:0 auto 20px!important;padding:0!important;text-align:center!important;display:flex!important;justify-content:center!important}" +
      "html.dl-auth-landing-skin .logo-holder img,html.dl-auth-landing-skin .logo-holder .image-14{display:block!important;width:min(68vw,260px)!important;max-width:100%!important;height:auto!important;margin:0 auto!important;object-fit:contain!important}" +
      "html.dl-auth-landing-skin .login-form-block,html.dl-auth-landing-skin .login-form-block.w-form,html.dl-auth-landing-skin .signup-left .login-form-block{width:100%!important;max-width:100%!important}" +
      "html.dl-auth-landing-skin .login-form-block form,html.dl-auth-landing-skin .login-form-block .form-field-wrap,html.dl-auth-landing-skin .signup-left .login-form-block form{width:100%!important;max-width:100%!important;box-sizing:border-box!important}" +
      "html.dl-auth-landing-skin .input.w-input,html.dl-auth-landing-skin textarea.w-input{width:100%!important;max-width:100%!important;box-sizing:border-box!important}" +
      "html.dl-auth-landing-skin .login-form-block input[type=submit],html.dl-auth-landing-skin .login-form-block .w-button,html.dl-auth-landing-skin .signup-left input[type=submit]{width:100%!important;max-width:100%!important;margin-left:auto!important;margin-right:auto!important}" +
      "html.dl-auth-landing-skin h3.display-4,html.dl-auth-landing-skin .paragraph-large,html.dl-auth-landing-skin .form-block h3,html.dl-auth-landing-skin .form-block p{width:100%!important;text-align:center!important}" +
      "}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function mountAuthLandingBackgroundLayers(doc) {
    if (!doc || !doc.body || doc.getElementById(AUTH_BG_ROOT_ID)) return;

    var root = doc.createElement("div");
    root.id = AUTH_BG_ROOT_ID;
    root.setAttribute("aria-hidden", "true");

    var grid = doc.createElement("div");
    grid.className = "dl-auth-bg-grid";
    root.appendChild(grid);

    if (!hasNativeAuthBlob(doc)) {
      var base = resolveRailwayScriptBase();
      var blob = doc.createElement("img");
      blob.className = "dl-auth-bg-blob";
      blob.src = base
        ? base + "/marketing/assets/sales-home-bg-blob.svg"
        : WEBFLOW_BLOB_URL;
      blob.alt = "";
      blob.setAttribute("aria-hidden", "true");
      blob.decoding = "async";
      blob.loading = "eager";
      root.appendChild(blob);
    }

    doc.body.insertBefore(root, doc.body.firstChild);
  }

  function applyAuthPageLandingSkin(doc) {
    if (!doc || !isAuthMarketingPage()) return;
    doc.documentElement.classList.add("dl-auth-landing-skin");
    if (doc.body) {
      doc.body.classList.add("signup-page");
    }
    injectAuthLandingBackgroundStyles(doc);
    injectAuthMobileLayoutStyles(doc);
    injectLandingScrollbarStyles(doc);
    clearPageLoaderOverlays(doc);
    mountAuthLandingBackgroundLayers(doc);
  }

  function applyLandingEmbedTopPadding(doc) {
    if (!doc) return;
    applyNavbarLogoSize(doc);
    applyGlobalNavbarSpacing(doc);
    var shell = doc.querySelector(".dl-landing-embed-full");
    var wrap = doc.querySelector(".dl-landing-embed-wrap");
    if (shell) {
      shell.style.paddingTop = "0";
      shell.style.overflow = "visible";
      shell.style.width = "100%";
      shell.style.background = "#080f25";
    }
    if (wrap) {
      wrap.style.paddingTop = EMBED_TOP_PADDING_PX + "px";
      wrap.style.overflow = "visible";
      wrap.style.minHeight = "0";
      wrap.style.width = "100%";
      wrap.style.background = "#080f25";
    }
  }

  function bootstrapHomeNewLandingShell(doc) {
    if (!doc || !doc.getElementById("dealality-landing-embed")) return;
    injectLandingScrollbarStyles(doc);
    clearPageLoaderOverlays(doc);
    applyLandingEmbedTopPadding(doc);
    var iframe = doc.getElementById("dealality-landing-embed");
    if (iframe) {
      iframe.style.display = "block";
      iframe.style.width = "100%";
      iframe.style.overflow = "hidden";
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
    applyGlobalNavbarSpacing(global.document);
    scheduleNavbarLogoFix(global.document);
    installAuthPageNavBridge();
    ensureToastPatch();
    applyAuthPageLandingSkin(global.document);
    applyPlatformPageShell(global.document);
    ensureLandingNavbarBridge(global.document);
    watchUserContext();

    global.document.addEventListener("dealality-me-ready", function (ev) {
      if (ev && ev.detail && ev.detail.data) applyFromMe(ev.detail.data);
    });

    global.document.addEventListener("DOMContentLoaded", function () {
      ensureToastPatch();
      applyAuthPageLandingSkin(global.document);
      applyPlatformPageShell(global.document);
      ensureLandingNavbarBridge(global.document);
      scheduleNavbarLogoFix(global.document);
      bootstrapHomeNewLandingShell(global.document);
      if (global.__dealalityUserContext) applyFromMe(global.__dealalityUserContext);
    });

    global.addEventListener("load", function () {
      applyAuthPageLandingSkin(global.document);
      applyPlatformPageShell(global.document);
      scheduleNavbarLogoFix(global.document);
      bootstrapHomeNewLandingShell(global.document);
    });
    global.setTimeout(function () {
      bootstrapHomeNewLandingShell(global.document);
    }, 1200);
    global.addEventListener("resize", function () {
      applyLandingEmbedTopPadding(global.document);
    });

    if (global.__dealalityUserContext) applyFromMe(global.__dealalityUserContext);
    bootstrapHomeNewLandingShell(global.document);
  }

  global.DealalityWebflowAccountNotice = {
    apply: applyFromMe,
    shouldSuppressBrandToast: shouldSuppressBrandToast,
    applyAuthPageLandingSkin: applyAuthPageLandingSkin,
    applyPlatformPageShell: function () {
      applyPlatformPageShell(global.document);
    },
    applyNavbarLogoSize: function () {
      applyNavbarLogoSize(global.document);
    },
  };
})(typeof window !== "undefined" ? window : global);
