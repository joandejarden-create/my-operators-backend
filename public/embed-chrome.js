/**
 * Optional footer for pages loaded as direct iframes (e.g. Webflow) with ?embed=1.
 * Skips when the page is inside the Dealality app shell (same-origin parent #frameContainer)
 * so we do not duplicate the shell footer.
 */
(function () {
  function hasEmbedParam() {
    return /(?:\?|&)embed=1(?:&|$)/.test(window.location.search || "");
  }

  if (!hasEmbedParam()) return;

  /** Host shells (e.g. My Deals) embed child pages that render their own footer chrome. */
  try {
    var path = (window.location.pathname || "").replace(/\/+$/, "");
    if (/\/my-deals$/i.test(path) || /\/my-deals\.html$/i.test(path)) return;
  } catch (_pathErr) {
    /* ignore */
  }

  try {
    if (window.parent !== window) {
      var parentPath = (window.parent.location.pathname || "").replace(/\/+$/, "");
      if (/\/my-deals$/i.test(parentPath) || /\/my-deals\.html$/i.test(parentPath)) return;
    }
  } catch (_parentPathErr) {
    /* cross-origin parent — show chrome */
  }

  try {
    if (
      window.parent !== window &&
      window.parent.document &&
      window.parent.document.getElementById("frameContainer")
    ) {
      return;
    }
  } catch (_e) {
    /* cross-origin parent (typical Webflow embed) — show chrome */
  }

  if (document.getElementById("platform-embed-chrome-footer")) return;

  var year = String(new Date().getFullYear());
  var style = document.createElement("style");
  style.setAttribute("data-platform-embed-chrome", "1");
  style.textContent =
    "body.has-platform-embed-chrome-footer{padding-bottom:48px;box-sizing:border-box;}" +
    "#platform-embed-chrome-footer{position:fixed;left:0;right:0;bottom:0;z-index:9999;" +
    "padding:10px 16px 12px;text-align:center;font-size:12px;font-weight:500;font-family:Inter,system-ui,sans-serif;" +
    "color:#7e89ac;background:#080f25;border-top:1px solid #37446b;}";
  (document.head || document.documentElement).appendChild(style);

  var footer = document.createElement("footer");
  footer.id = "platform-embed-chrome-footer";
  footer.setAttribute("role", "contentinfo");
  footer.textContent = year + " Copyright \u00A9 Dealality";

  document.body.appendChild(footer);
  document.body.classList.add("has-platform-embed-chrome-footer");
})();
