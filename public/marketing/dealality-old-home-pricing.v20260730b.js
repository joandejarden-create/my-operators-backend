/**
 * Old Home — Pricing enhance (v20260730b)
 * Path-gated to /old-home.
 * - Injects modules-style SVG icons into pricing tiles
 * - Ensures Advisors tile exists
 * - Dedupes subscription disclaimer lines
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohPricingEnhance >= 202607302) return;
    window.__ohPricingEnhance = 202607302;

    var ICONS = {
      "pricing-owners-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M10 28V14l10-5 10 5v14" stroke="#9B8AFB" stroke-width="1.5" stroke-linejoin="round"/><path d="M10 20h20M20 9v19" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="2 2" opacity=".4"/><circle cx="15" cy="17" r="2" fill="#9B8AFB" opacity=".6"/><circle cx="25" cy="17" r="2" fill="#9B8AFB" opacity=".6"/><circle cx="15" cy="24" r="2" fill="#9B8AFB" opacity=".4"/><circle cx="25" cy="24" r="2" fill="#9B8AFB" opacity=".4"/><path d="M7 28h26" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/></svg>',
      "pricing-brands-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="20" cy="20" r="12" stroke="#9B8AFB" stroke-width="1.5"/><path d="M20 12v8l5.5 5.5" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M14 8l-3-3M26 8l3-3" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round"/><circle cx="20" cy="20" r="2" fill="#9B8AFB"/></svg>',
      "pricing-operators-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="6" y="10" width="12" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="22" y="10" width="12" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M10 15h4M10 19h4M10 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><path d="M26 15h4M26 19h4M26 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><path d="M18 17l4 0M18 23l4 0" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="1.5 1.5" opacity=".35"/><circle cx="28" cy="27" r="1.5" fill="#9B8AFB"/><path d="M12 26l2 2 3-4" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
      "pricing-advisors-icon":
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="15" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="27" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M21 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M19 16h2" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-dasharray="1.5 1.5" opacity=".4"/><path d="M18 11l2-3M24 11l-2-3" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><circle cx="21" cy="7" r="1.5" fill="#9B8AFB" opacity=".5"/></svg>',
    };

    function applyIcons() {
      Object.keys(ICONS).forEach(function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        if (el.querySelector("svg") && el.getAttribute("data-oh-pricing-svg") === "30b") return;
        el.setAttribute("data-oh-pricing-svg", "30b");
        el.innerHTML = ICONS[id];
      });
    }

    function ensureAdvisorsCard() {
      var grid = document.getElementById("pricing-grid");
      if (!grid) return;
      if (document.getElementById("pricing-card-advisors")) return;

      var article = document.createElement("article");
      article.id = "pricing-card-advisors";
      article.innerHTML = [
        '<div id="pricing-advisors-icon" aria-hidden="true"></div>',
        '<h3 id="pricing-advisors-title">Advisors</h3>',
        '<p id="pricing-advisors-desc">Counsel and transaction advisors</p>',
        '<p id="pricing-advisors-price"><strong>Annual Access</strong>',
        "<span>Subscription access for advisory teams supporting owner-led brand, operator, and strategic decisions.</span></p>",
        '<ul id="pricing-advisors-features" role="list">',
        "<li>Early involvement in owner-led processes</li>",
        "<li>Structured decision and asset context</li>",
        "<li>Client opportunity visibility</li>",
        "<li>Permission-based deal-room participation</li>",
        "<li>Advisory notes within the workflow</li>",
        "<li>Clear decision record for counsel</li>",
        "</ul>",
        '<a id="pricing-advisors-cta" href="mailto:joan@dealality.com?subject=Dealality%20Advisor%20Access">Request Advisor Access</a>',
      ].join("");
      grid.appendChild(article);
    }

    function normalizeTerms() {
      var terms = document.getElementById("pricing-terms");
      if (!terms) return;
      var brands = document.getElementById("pricing-term-brands");
      var operators = document.getElementById("pricing-term-operators");
      var advisors = document.getElementById("pricing-term-advisors");
      var sharedText =
        "Subscription access does not guarantee a minimum volume of opportunities.";

      // Collapse duplicate brand/operator lines into one shared note
      if (brands && operators) {
        brands.id = "pricing-term-subscription";
        brands.textContent = sharedText;
        if (operators.parentNode) operators.parentNode.removeChild(operators);
      } else if (!document.getElementById("pricing-term-subscription") && (brands || operators)) {
        var existing = brands || operators;
        existing.id = "pricing-term-subscription";
        existing.textContent = sharedText;
      }

      if (advisors && advisors.parentNode) advisors.parentNode.removeChild(advisors);
    }

    function enhance() {
      ensureAdvisorsCard();
      applyIcons();
      normalizeTerms();
    }

    function boot() {
      enhance();
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    setTimeout(enhance, 0);
    setTimeout(enhance, 400);
    setTimeout(enhance, 1200);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-pricing]", err);
    }
  }
})();
