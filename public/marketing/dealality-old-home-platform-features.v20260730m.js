/**
 * Old Home — Features image-overlay hover tiles (v20260730m)
 * Path-gated to /old-home.
 * Image tiles: full-bleed screenshot; blue caption with title only,
 * expands upward on hover to show title + 3 lines.
 * Opportunity Review, Smart Matching, Brand Explorer, Operator Explorer, and Structured Deal Responses use cropped app screenshots.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohPlatformFeaturesTiles >= 202607312) return;
    window.__ohPlatformFeaturesTiles = 202607312;

    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b6441b592bc308fa5ca9e_dealality-old-home-platform-features.v20260730i.css";

    var IMG_RADAR =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b368b0b3eb941d61d056d_dealality-old-home-feature-radar.v20260730d.png";
    var IMG_FEE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b368cc1507579a4d6b3b6_dealality-old-home-feature-fee-estimator.v20260730d.png";
    var IMG_COMPARE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b6323ab470bca92bc1577_dealality-old-home-feature-deal-compare.v20260730h.png";
    var IMG_OPERATOR =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b650591a1d82d913e3ea1_dealality-old-home-feature-operator-explorer.v20260730i.png";
    var IMG_OPPORTUNITY =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b66e3b2a036492744c65a_dealality-old-home-feature-opportunity-review.v20260730k.png";
    var IMG_BRAND =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b68f279195823136433b7_dealality-old-home-feature-brand-explorer.v20260730l.png";
    var IMG_SMART =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b6a031a27c3d545828a6c_dealality-old-home-feature-smart-matching.v20260730m.png";

    var H2 =
      "Advanced Features, Simple Experience. Built for Speed and Greater Value.";

    var LEAD =
      "Dealality capabilities that help owners evaluate brands, operators, market context, and readiness — with confidentiality and owner control throughout.";

    var TILES = [
      {
        id: 1,
        icon: "OR",
        title: "Opportunity Review",
        body:
          "Submit a confidential hotel opportunity for structured review. Owners control when and how counterparties are engaged.",
        image: IMG_OPPORTUNITY,
        imageAlt:
          "Deal Brief opportunity review with project summary, property details, and strategic priorities",
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
        image: IMG_SMART,
        imageAlt:
          "Smart Matching results table with preferred brands and match scores for hotel development deals",
      },
      {
        id: 4,
        icon: "BE",
        title: "Brand Explorer",
        body:
          "Review brand positioning, alignment signals, and owner-value context before outreach so conversations start from clearer fit questions.",
        image: IMG_BRAND,
        imageAlt:
          "Brand Explorer overview with brand positioning, audience, and navigation across brand diligence tabs",
      },
      {
        id: 5,
        icon: "OE",
        title: "Operator Explorer",
        body:
          "Compare operator capabilities, operating models, and partnership signals with structured profiles built for owner diligence.",
        image: IMG_OPERATOR,
        imageAlt:
          "Operator Explorer profile for Cenote Azul Operadores with positioning and quick facts",
      },
      {
        id: 6,
        icon: "MI",
        title: "Market Intelligence",
        body:
          "Use Dealality Radar to see where brands operate, where they are growing, and where untapped opportunities remain around your market.",
        image: IMG_RADAR,
        imageAlt: "Dealality Radar map of hotel brands, pipeline, and market clusters",
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
        image: IMG_COMPARE,
        imageAlt:
          "Side-by-side structured deal response comparison across chain scale, fees, and terms",
      },
      {
        id: 9,
        icon: "+",
        title: "And More…",
        body:
          "Including Fee Estimator for franchise and management fee projections — plus more tools as the platform expands.",
        image: IMG_FEE,
        imageAlt: "Dealality Fee Estimator projecting franchise fees over a multi-year period",
      },
    ];

    function ensureCss() {
      var links = document.querySelectorAll(
        'link[href*="dealality-old-home-platform-features"]'
      );
      var i;
      for (i = 0; i < links.length; i++) {
        links[i].setAttribute("href", CSS_HREF);
        links[i].setAttribute("data-oh-pf", "30m");
      }
      if (document.querySelector('link[data-oh-pf="30i"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-pf", "30m");
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
      var visual = t.image
        ? '<img src="' +
          esc(t.image) +
          '" alt="' +
          esc(t.imageAlt || "") +
          '" width="640" height="360" loading="lazy" decoding="async">'
        : "<span>" + esc(t.icon) + "</span>";
      return (
        '<article id="pf-card-' +
        n +
        '" class="' +
        (t.image ? "is-image-tile" : "is-icon-tile") +
        '"' +
        (t.image ? ' tabindex="0"' : "") +
        ">" +
        '<div id="pf-card-' +
        n +
        '-visual"' +
        (t.image ? "" : ' aria-hidden="true"') +
        ">" +
        visual +
        "</div>" +
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
      var h2 = document.getElementById("platform-features-h2");
      if (h2) h2.textContent = H2;
      var lead = document.getElementById("platform-features-lead");
      if (lead) lead.textContent = LEAD;
      var grid = document.getElementById("platform-features-grid");
      if (!grid) return;
      if (grid.getAttribute("data-oh-pf-tiles") === "30m") return;
      grid.innerHTML = TILES.map(cardHtml).join("");
      grid.setAttribute("data-oh-pf-tiles", "30m");
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
