/**
 * Webflow parent page — embed full Railway landing body below site navbar.
 * Intercepts in-page #hash nav links and scrolls the embedded iframe.
 */
(function (global) {
  "use strict";

  var IFRAME_ID = "dealality-landing-embed";
  var PARENT_SOURCE = "dealality-landing-parent";
  var CHILD_SOURCE = "dealality-landing-embed";
  var NAVBAR_SHELL_HEIGHT_PX = 66;
  var NAV_OFFSET_FALLBACK_PX = NAVBAR_SHELL_HEIGHT_PX;
  var HERO_GAP_BELOW_NAV_PX = 36;
  var SECTION_SCROLL_BUFFER_PX = 12;
  var EMBED_TOP_PADDING_PX = NAVBAR_SHELL_HEIGHT_PX + HERO_GAP_BELOW_NAV_PX;
  var pendingParentScrollTimer = null;
  var lastParentScrollKey = "";
  var lastParentScrollAt = 0;
  var SCROLLBAR_STYLE_ID = "dl-landing-shell-scrollbar";

  var MIN_IFRAME_HEIGHT_PX = 2400;

  var NAV_LABEL_TO_ID = {
    "for owners": "owners",
    "for brands": "brands",
    "for brands & operators": "brands",
    "for partners": "partners",
    "how it works": "how",
    "the platform": "how",
    "see the platform": "how",
    "faqs": "faq",
    "faq": "faq",
  };

  var SECTION_ID_ALIASES = { platform: "how" };

  function normalizeLandingHomePath(path) {
    var raw = String(path || "/").trim();
    if (!raw || raw === "/") return "/";
    return raw.replace(/\/+$/, "");
  }

  var LANDING_HOME_PATH = normalizeLandingHomePath(global.DEALALITY_LANDING_HOME_PATH);

  function isLandingHomePage() {
    try {
      var path = normalizeLandingHomePath(global.location.pathname || "/");
      return path === LANDING_HOME_PATH;
    } catch (err) {
      return false;
    }
  }

  function landingHomeHref(sectionId) {
    var hash = sectionId ? "#" + sectionId : "";
    if (LANDING_HOME_PATH === "/") return "/" + hash;
    return LANDING_HOME_PATH + hash;
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

  function injectScrollbarStyles() {
    var doc = global.document;
    if (!doc || doc.getElementById(SCROLLBAR_STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = SCROLLBAR_STYLE_ID;
    style.textContent =
      "html,body{scrollbar-width:thin;scrollbar-color:#6c72ff #080f25 !important}" +
      "html::-webkit-scrollbar,body::-webkit-scrollbar{width:8px}" +
      "html::-webkit-scrollbar-track,body::-webkit-scrollbar-track{background:#080f25;border-left:1px solid rgba(87,195,255,.35)}" +
      "html::-webkit-scrollbar-thumb,body::-webkit-scrollbar-thumb{background:#6c72ff;border-radius:0}" +
      "html::-webkit-scrollbar-thumb:hover,body::-webkit-scrollbar-thumb:hover{background:#8b90ff}";
    (doc.head || doc.documentElement).appendChild(style);
  }

  function getIframe() {
    return global.document.getElementById(IFRAME_ID);
  }

  function getNavbarHeight() {
    return NAVBAR_SHELL_HEIGHT_PX;
  }

  function lockNavbarShell() {
    if (global.DealalityWebflowAccountNotice && typeof global.DealalityWebflowAccountNotice.applyNavbarLogoSize === "function") {
      global.DealalityWebflowAccountNotice.applyNavbarLogoSize();
    }
  }

  function unlockPageScroll() {
    var doc = global.document;
    if (!doc || !doc.body) return;
    doc.body.classList.remove("overflow-hidden");
    doc.body.classList.remove("h-100-vh");
    doc.documentElement.style.overflow = "visible";
    doc.documentElement.style.height = "auto";
    doc.body.style.overflow = "visible";
    doc.body.style.height = "auto";
    doc.body.style.minHeight = "100vh";
  }

  function hideLoadingBar() {
    global.document.querySelectorAll(".loading-bar-wrapper").forEach(function (el) {
      el.style.display = "none";
    });
  }

  function compactNavbar() {
    var nav = global.document.querySelector(".navbar-2.w-nav") || global.document.querySelector(".w-nav");
    if (!nav) return;
    nav.querySelectorAll(".nav_links").forEach(function (el) {
      el.style.padding = "0.35rem 0.5rem";
      el.style.marginLeft = "0";
      el.style.marginRight = "0";
    });
    nav.querySelectorAll(".nav-compact").forEach(function (el) {
      el.style.padding = "0.4rem 0.85rem";
      el.style.lineHeight = "1.2";
      el.style.marginLeft = "0";
      el.style.marginRight = "0";
    });
  }

  function applyEmbedOffset() {
    lockNavbarShell();
    compactNavbar();
    var wrap = global.document.querySelector(".dl-landing-embed-wrap");
    var embedShell = global.document.querySelector(".dl-landing-embed-full");
    if (embedShell) {
      embedShell.style.paddingTop = "0";
      embedShell.style.position = "relative";
      embedShell.style.zIndex = "1";
      embedShell.style.overflow = "visible";
      embedShell.style.width = "100%";
    }
    if (wrap) {
      wrap.style.marginTop = "0";
      wrap.style.paddingTop = EMBED_TOP_PADDING_PX + "px";
      wrap.style.overflow = "visible";
      wrap.style.width = "100%";
      wrap.style.minHeight = "0";
    }
  }

  function configureIframe() {
    var iframe = getIframe();
    if (!iframe) return;
    iframe.setAttribute("scrolling", "no");
    iframe.setAttribute("allowfullscreen", "");
    iframe.setAttribute("webkitallowfullscreen", "");
    iframe.setAttribute("allow", "autoplay; fullscreen; encrypted-media; picture-in-picture");
    iframe.style.overflow = "hidden";
    iframe.style.display = "block";
    iframe.style.width = "100%";
    iframe.style.visibility = "visible";
    iframe.style.opacity = "1";
    iframe.style.border = "0";
  }

  function applyIframeHeight(px, fromChild) {
    var iframe = getIframe();
    if (!iframe) return;
    var height = Math.max(Number(px) || 0, 0);
    if (!fromChild && height < MIN_IFRAME_HEIGHT_PX) {
      height = MIN_IFRAME_HEIGHT_PX;
    }
    iframe.style.setProperty("height", height + "px", "important");
    iframe.style.setProperty(
      "min-height",
      fromChild ? "0" : height + "px",
      "important"
    );
  }

  function scrollParentToIframeOffset(offsetTop, id) {
    var iframe = getIframe();
    if (!iframe) return;
    var pageY = global.pageYOffset || global.document.documentElement.scrollTop || 0;
    var iframeTop = iframe.getBoundingClientRect().top + pageY;
    var target =
      iframeTop + Number(offsetTop || 0) - getNavbarHeight() - SECTION_SCROLL_BUFFER_PX;
    target = Math.max(0, target);
    var scrollKey = String(id || "") + ":" + Math.round(target);
    var now = Date.now();
    if (scrollKey === lastParentScrollKey && now - lastParentScrollAt < 900) return;
    lastParentScrollKey = scrollKey;
    lastParentScrollAt = now;
    if (pendingParentScrollTimer) global.clearTimeout(pendingParentScrollTimer);
    pendingParentScrollTimer = global.setTimeout(function () {
      pendingParentScrollTimer = null;
      global.scrollTo({ top: target, behavior: "auto" });
    }, 40);
  }

  function postScroll(id) {
    var iframe = getIframe();
    if (!iframe || !iframe.contentWindow) return;
    if (SECTION_ID_ALIASES[id]) id = SECTION_ID_ALIASES[id];
    var msg = { source: PARENT_SOURCE, type: "scrollTo", id: id };
    iframe.contentWindow.postMessage(msg, "*");
    global.setTimeout(function () {
      if (iframe.contentWindow) iframe.contentWindow.postMessage(msg, "*");
    }, 420);
  }

  function resolveNavId(link) {
    if (!link) return null;
    var href = (link.getAttribute("href") || "").trim();
    if (href) {
      if (href.charAt(0) === "#" && href.length > 1) {
        var hashId = href.slice(1);
        return SECTION_ID_ALIASES[hashId] || hashId;
      }
      try {
        var url = new URL(href, global.location.href);
        if (url.hash && url.hash.length > 1) {
          var urlHashId = url.hash.slice(1);
          return SECTION_ID_ALIASES[urlHashId] || urlHashId;
        }
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[dealality-landing-parent] resolveNavId href parse failed", err);
        }
      }
    }
    var text = (link.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
    return NAV_LABEL_TO_ID[text] || null;
  }

  function applyNavbarLogoFromShell() {
    if (global.DealalityWebflowAccountNotice && typeof global.DealalityWebflowAccountNotice.applyNavbarLogoSize === "function") {
      global.DealalityWebflowAccountNotice.applyNavbarLogoSize();
    }
  }

  function wireNavbarLogoHome() {
    var homeHref = "/";
    try {
      if (LANDING_HOME_PATH && LANDING_HOME_PATH !== "/") homeHref = LANDING_HOME_PATH;
    } catch (err) {
      /* use default */
    }
    global.document.querySelectorAll(".navbar_logo-link, .w-nav-brand, a.navbar_logo-link").forEach(function (link) {
      link.setAttribute("href", homeHref);
      link.setAttribute("aria-label", "Dealality home");
    });
  }

  function bootstrapShell() {
    injectScrollbarStyles();
    unlockPageScroll();
    hideLoadingBar();
    applyEmbedOffset();
    configureIframe();
    applyIframeHeight(MIN_IFRAME_HEIGHT_PX);
    applyNavbarLogoFromShell();
    wireNavbarLogoHome();
  }

  global.addEventListener("message", function (event) {
    var data = event.data;
    if (!data || data.source !== CHILD_SOURCE) return;
    if (data.type === "resize" && typeof data.height === "number") {
      applyIframeHeight(data.height, true);
    }
    if (data.type === "scrollToOffset" && typeof data.top === "number") {
      scrollParentToIframeOffset(data.top, data.id);
      if (data.id && global.history && global.history.replaceState) {
        global.history.replaceState(null, "", "#" + data.id);
      }
    }
  });

  function handleNavbarSectionClick(event) {
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link || !isNavbarLink(link)) return;
    var id = resolveNavId(link);
    if (!id) return;

    var iframe = getIframe();
    if (iframe) {
      event.preventDefault();
      postScroll(id);
      if (global.history && global.history.replaceState) {
        global.history.replaceState(null, "", "#" + id);
      }
      return;
    }

    if (!isLandingHomePage()) {
      event.preventDefault();
      global.location.href = landingHomeHref(id);
    }
  }

  if (!global.__dealalityLandingNavInstalled) {
    global.__dealalityLandingNavInstalled = true;
    global.document.addEventListener("click", handleNavbarSectionClick, true);
  }

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", bootstrapShell);
  } else {
    bootstrapShell();
  }

  function scrollFromLocationHash() {
    var hash = (global.location.hash || "").replace(/^#/, "");
    if (!hash) return;
    if (SECTION_ID_ALIASES[hash]) hash = SECTION_ID_ALIASES[hash];
    postScroll(hash);
  }

  global.addEventListener("load", function () {
    bootstrapShell();
    scrollFromLocationHash();
  });

  global.addEventListener("hashchange", scrollFromLocationHash);

  global.setTimeout(bootstrapShell, 1200);
  global.addEventListener("resize", applyEmbedOffset);
})(window);
