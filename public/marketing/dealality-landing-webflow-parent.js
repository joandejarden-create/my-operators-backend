/**
 * Webflow parent page — embed full Railway landing body below site navbar.
 * Intercepts in-page #hash nav links and scrolls the embedded iframe.
 */
(function (global) {
  "use strict";

  var IFRAME_ID = "dealality-landing-embed";
  var PARENT_SOURCE = "dealality-landing-parent";
  var CHILD_SOURCE = "dealality-landing-embed";

  var MIN_IFRAME_HEIGHT_PX = 2400;

  function getIframe() {
    return global.document.getElementById(IFRAME_ID);
  }

  function applyIframeHeight(px) {
    var iframe = getIframe();
    if (!iframe) return;
    var height = Math.max(Number(px) || 0, MIN_IFRAME_HEIGHT_PX);
    iframe.style.height = height + "px";
    iframe.style.minHeight = "80vh";
    iframe.style.display = "block";
    iframe.style.width = "100%";
  }

  function postScroll(id) {
    var iframe = getIframe();
    if (!iframe || !iframe.contentWindow) return;
    iframe.contentWindow.postMessage(
      { source: PARENT_SOURCE, type: "scrollTo", id: id },
      "*"
    );
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

  global.addEventListener("load", function () {
    applyIframeHeight(MIN_IFRAME_HEIGHT_PX);
    var hash = (global.location.hash || "").replace(/^#/, "");
    if (hash) global.setTimeout(function () { postScroll(hash); }, 800);
  });

  global.setTimeout(function () { applyIframeHeight(MIN_IFRAME_HEIGHT_PX); }, 1200);
})(window);
