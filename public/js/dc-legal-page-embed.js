/**
 * Injects full Terms/Privacy HTML from DEALALITY_API_BASE into #dc-legal-content.
 * Used on Webflow /terms and /privacy until those pages host native copy.
 */
(function (global) {
  "use strict";

  function apiBase() {
    return String(global.DEALALITY_API_BASE || "https://my-operators-backend-staging.up.railway.app")
      .trim()
      .replace(/\/$/, "");
  }

  function pathHint() {
    var p = (global.location.pathname || "").toLowerCase();
    if (p.indexOf("privacy") !== -1) return "/privacy";
    return "/terms";
  }

  function paint(html) {
    var root = document.getElementById("dc-legal-content");
    if (!root) return;
    try {
      var doc = new DOMParser().parseFromString(html, "text/html");
      var styleEl = doc.querySelector("style");
      if (styleEl) {
        var s = document.createElement("style");
        s.textContent = styleEl.textContent;
        document.head.appendChild(s);
      }
      var wrap = doc.querySelector(".wrap") || doc.body;
      if (!wrap) {
        root.innerHTML = html;
        return;
      }
      // Webflow shell already shows one Back link + one page title.
      var clone = wrap.cloneNode(true);
      var back = clone.querySelector("a.back, .back");
      if (back) back.remove();
      var h1 = clone.querySelector("h1");
      if (h1) h1.remove();
      var updated = clone.querySelector(".updated");
      if (updated) updated.remove();
      root.innerHTML = clone.innerHTML;
    } catch (err) {
      console.error("[dc-legal-page-embed]", err);
      root.innerHTML =
        "<p>Unable to render the document. Open it directly: <a href=\"" +
        apiBase() +
        pathHint() +
        "\">" +
        apiBase() +
        pathHint() +
        "</a></p>";
    }
  }

  function load() {
    var root = document.getElementById("dc-legal-content");
    if (!root) return;
    var url = apiBase() + pathHint();
    fetch(url, { mode: "cors", credentials: "omit", cache: "no-store" })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.text();
      })
      .then(paint)
      .catch(function (err) {
        console.error("[dc-legal-page-embed] fetch failed:", err);
        root.innerHTML =
          "<p>We could not load the latest legal document. Please email <a href=\"mailto:hello@aohospitalityadvisors.com\">hello@aohospitalityadvisors.com</a> or try again shortly.</p>" +
          "<p><a href=\"" +
          url +
          "\" target=\"_blank\" rel=\"noopener noreferrer\">Open document on Dealality app host</a></p>";
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", load);
  } else {
    load();
  }
})(window);
