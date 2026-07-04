/**
 * Mount live Dealality app iframes for marketing embeds.
 * Usage: <div class="dl-iframe" data-dealality-embed="termComparison" data-app-base="https://your-app.up.railway.app"></div>
 */
(function (global) {
  "use strict";

  var FALLBACK_PATHS = {
    heroDashboard: "/app/home.html?embed=1&appShell=1",
    brandExplorer: "/brand-education-atelier-north.html?embed=1",
    operatorExplorer: "/operator-explorer-gold-mock.html?embed=1",
    marketIntelligence: "/market-alerts.html?embed=1",
    termComparison: "/deal-compare.html?embed=1&dealId=recqGVET08a8faagy",
    dealRoom: "/deal-room-owner.html?embed=1&dealId=recqGVET08a8faagy&marketingEmbed=1",
    loiHandoff: "/deal-setup.html?embed=1&id=recqGVET08a8faagy&edit=1&marketingEmbed=1",
  };

  function trimBase(base) {
    return String(base || "").trim().replace(/\/$/, "");
  }

  function resolveBase(el) {
    var fromEl = el && el.getAttribute("data-app-base");
    if (fromEl) return trimBase(fromEl);
    if (global.DEALALITY_APP_BASE) return trimBase(global.DEALALITY_APP_BASE);
    return trimBase(global.location && global.location.origin);
  }

  function buildSrc(key, base, embeds) {
    if (embeds && embeds[key]) return embeds[key];
    var path = FALLBACK_PATHS[key];
    if (!path) return "";
    return base + path;
  }

  function mount(el, src, title) {
    if (!src || el.getAttribute("data-dl-mounted") === "1") return;
    el.setAttribute("data-dl-mounted", "1");
    var frameWrap = el.querySelector(".dl-iframe__frame");
    if (!frameWrap) {
      frameWrap = document.createElement("div");
      frameWrap.className = "dl-iframe__frame";
      el.appendChild(frameWrap);
    }
    var iframe = document.createElement("iframe");
    iframe.src = src;
    iframe.title = title || "Dealality platform preview";
    iframe.loading = "lazy";
    iframe.setAttribute("referrerpolicy", "strict-origin-when-cross-origin");
    frameWrap.innerHTML = "";
    frameWrap.appendChild(iframe);
  }

  function init() {
    var nodes = document.querySelectorAll("[data-dealality-embed]");
    if (!nodes.length) return;

    var base = resolveBase(nodes[0]);
    var configUrl = base + "/api/marketing/demo-embeds";

    fetch(configUrl)
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        var embeds = data && data.embeds ? data.embeds : null;
        var appBase = (data && data.baseUrl) || base;
        nodes.forEach(function (el) {
          var key = el.getAttribute("data-dealality-embed");
          var title = el.getAttribute("data-embed-title") || "";
          mount(el, buildSrc(key, appBase, embeds), title);
        });
      })
      .catch(function () {
        nodes.forEach(function (el) {
          var key = el.getAttribute("data-dealality-embed");
          var title = el.getAttribute("data-embed-title") || "";
          mount(el, buildSrc(key, base, null), title);
        });
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  global.DealalityLiveEmbed = { init: init };
})(window);
