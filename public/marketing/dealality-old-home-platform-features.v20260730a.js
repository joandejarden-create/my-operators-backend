/**
 * Old Home — Features 9 equal tiles (v20260730a)
 * Path-gated to /old-home. Rebuilds #platform-features-grid as equal 3×3 cards.
 * Does not touch How-we-do-it Process timeline, FOUC, hero-fit, Problem, or quote tiles.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohPlatformFeaturesTiles >= 202607301) return;
    window.__ohPlatformFeaturesTiles = 202607301;

    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a8745b901cef08fef6b14_dealality-old-home-platform-features.v20260730a.css";

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
      {
        id: 7,
        icon: "OC",
        title: "Owner-Controlled Process",
        body:
          "Owners decide who participates, when information is shared, and how the process moves forward.",
      },
      {
        id: 8,
        icon: "SR",
        title: "Structured Deal Responses",
        body:
          "Owners receive more organized responses that are easier to compare across participants.",
      },
      {
        id: 9,
        icon: "+",
        title: "And More…",
        body:
          "The platform continues to expand with new capabilities, coverage, and workflows — built for how owners evaluate hotel opportunities.",
      },
    ];

    function ensureCss() {
      var existing = document.querySelector(
        'link[href*="dealality-old-home-platform-features"]'
      );
      if (existing) {
        if (existing.getAttribute("href") !== CSS_HREF) {
          existing.setAttribute("href", CSS_HREF);
        }
        existing.setAttribute("data-oh-pf", "30a");
        return;
      }
      if (document.querySelector('link[data-oh-pf="30a"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-pf", "30a");
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
      if (grid.getAttribute("data-oh-pf-tiles") === "30a") return;
      grid.innerHTML = TILES.map(cardHtml).join("");
      grid.setAttribute("data-oh-pf-tiles", "30a");
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
