/**
 * Webflow parent page — embed full Railway landing body below site navbar.
 * Intercepts in-page #hash nav links and scrolls the embedded iframe.
 */
(function (global) {
  "use strict";

  var IFRAME_ID = "dealality-landing-embed";
  var PARENT_SOURCE = "dealality-landing-parent";
  var CHILD_SOURCE = "dealality-landing-embed";
  var NAV_OFFSET_FALLBACK_PX = 48;
  var SCROLLBAR_STYLE_ID = "dl-landing-shell-scrollbar";

  var MIN_IFRAME_HEIGHT_PX = 2400;

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
    var nav =
      global.document.querySelector(".navbar-2.w-nav") ||
      global.document.querySelector('[role="banner"].w-nav') ||
      global.document.querySelector(".w-nav");
    if (!nav) return NAV_OFFSET_FALLBACK_PX;
    var rect = nav.getBoundingClientRect();
    return Math.max(Math.ceil(rect.height || 0), NAV_OFFSET_FALLBACK_PX);
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
    nav.querySelectorAll(".navbar_content").forEach(function (el) {
      el.style.minHeight = "52px";
      el.style.paddingTop = "4px";
      el.style.paddingBottom = "4px";
    });
    nav.querySelectorAll(".navbar_logo").forEach(function (el) {
      el.style.height = "28px";
      el.style.maxHeight = "28px";
      el.style.width = "auto";
    });
    nav.querySelectorAll(".navbar_logo-link").forEach(function (el) {
      el.style.height = "auto";
      el.style.minHeight = "0";
    });
  }

  function applyEmbedOffset() {
    compactNavbar();
    var offset = getNavbarHeight();
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
      wrap.style.paddingTop = offset + "px";
      wrap.style.overflow = "visible";
      wrap.style.width = "100%";
      wrap.style.minHeight = "0";
    }
  }

  function applyIframeHeight(px) {
    var iframe = getIframe();
    if (!iframe) return;
    var height = Math.max(Number(px) || 0, MIN_IFRAME_HEIGHT_PX);
    iframe.style.height = height + "px";
    iframe.style.minHeight = "80vh";
    iframe.style.display = "block";
    iframe.style.width = "100%";
    iframe.style.visibility = "visible";
    iframe.style.opacity = "1";
  }

  function postScroll(id) {
    var iframe = getIframe();
    if (!iframe || !iframe.contentWindow) return;
    iframe.contentWindow.postMessage(
      { source: PARENT_SOURCE, type: "scrollTo", id: id },
      "*"
    );
  }

  function bootstrapShell() {
    injectScrollbarStyles();
    unlockPageScroll();
    hideLoadingBar();
    applyEmbedOffset();
    applyIframeHeight(MIN_IFRAME_HEIGHT_PX);
  }

  global.addEventListener("message", function (event) {
    var data = event.data;
    if (!data || data.source !== CHILD_SOURCE) return;
    if (data.type === "resize" && typeof data.height === "number") {
      applyIframeHeight(data.height);
    }
  });

  global.document.addEventListener("click", function (event) {
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link) return;
    var href = (link.getAttribute("href") || "").trim();
    if (!href || href.charAt(0) !== "#" || href.length < 2) return;
    var id = href.slice(1);
    if (!getIframe()) return;
    event.preventDefault();
    postScroll(id);
    if (global.history && global.history.replaceState) {
      global.history.replaceState(null, "", href);
    }
  });

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", bootstrapShell);
  } else {
    bootstrapShell();
  }

  global.addEventListener("load", function () {
    bootstrapShell();
    var hash = (global.location.hash || "").replace(/^#/, "");
    if (hash) global.setTimeout(function () { postScroll(hash); }, 800);
  });

  global.setTimeout(bootstrapShell, 1200);
  global.addEventListener("resize", applyEmbedOffset);
})(window);
