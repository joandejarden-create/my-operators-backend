/**
 * Old Home — Features 6 equal tiles 3×2 (v20260730b)
 * Path-gated to /old-home. Rebuilds #platform-features-grid; kills x1/x2 row-span.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohPlatformFeaturesTiles >= 202607302) return;
    window.__ohPlatformFeaturesTiles = 202607302;

    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css";

    var LEAD =
      "Dealality capabilities that help owners evaluate brands, operators, market context, and readiness — with confidentiality and owner control throughout.";

    var TILES = [
      {
        id: 1,
        icon: "OR",
        title: "Opportunity Review",
        body:
          "Submit a confidential hotel opportunity for structured review. Owners control when and how counterparties are engaged.",
      },
      {
        id: 2,
        icon: "DR",
        title: "Deal Readiness",
        body:
          "Surface gaps, priorities, and readiness signals before inviting brands, operators, or capital partners into the process.",
      },
      {
        id: 3,
        icon: "SM",
        title: "Smart Matching",
        body:
          "Brands and operators are evaluated based on fit, strategy, and project alignment.",
      },
      {
        id: 4,
        icon: "BE",
        title: "Brand Explorer",
        body:
          "Review brand positioning, alignment signals, and owner-value context before outreach so conversations start from clearer fit questions.",
      },
      {
        id: 5,
        icon: "OE",
        title: "Operator Explorer",
        body:
          "Compare operator capabilities, operating models, and partnership signals with structured profiles built for owner diligence.",
      },
      {
        id: 6,
        icon: "MI",
        title: "Market Intelligence",
        body:
          "Map market and travel-infrastructure context around the opportunity so geography and demand anchors inform path selection.",
      },
    ];

    function ensureCss() {
      var links = document.querySelectorAll(
        'link[href*="dealality-old-home-platform-features"]'
      );
      var i;
      for (i = 0; i < links.length; i++) {
        links[i].setAttribute("href", CSS_HREF);
        links[i].setAttribute("data-oh-pf", "30b");
      }
      if (document.querySelector('link[data-oh-pf="30b"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-pf", "30b");
      (document.head || document.documentElement).appendChild(link);
    }

    function esc(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function cardHtml(t) {
      var n = t.id;
      return (
        '<article id="pf-card-' +
        n +
        '">' +
        '<div id="pf-card-' +
        n +
        '-visual" aria-hidden="true"><span>' +
        esc(t.icon) +
        "</span></div>" +
        '<div id="pf-card-' +
        n +
        '-body">' +
        '<h3 id="pf-card-' +
        n +
        '-h">' +
        esc(t.title) +
        "</h3>" +
        '<p id="pf-card-' +
        n +
        '-p">' +
        esc(t.body) +
        "</p>" +
        "</div></article>"
      );
    }

    function apply() {
      ensureCss();
      var lead = document.getElementById("platform-features-lead");
      if (lead) lead.textContent = LEAD;
      var grid = document.getElementById("platform-features-grid");
      if (!grid) return;
      if (grid.getAttribute("data-oh-pf-tiles") === "30b") return;
      grid.innerHTML = TILES.map(cardHtml).join("");
      grid.setAttribute("data-oh-pf-tiles", "30b");
    }

    ensureCss();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", apply);
    } else {
      apply();
    }
    window.addEventListener("load", function () {
      try {
        apply();
      } catch (err) {
        console.error("[oh-platform-features]", err);
      }
    });
  } catch (err) {
    console.error("[oh-platform-features]", err);
  }
})();
