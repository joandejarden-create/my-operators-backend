/**
 * Old Home — Pricing enhance (v20260730c)
 * Path-gated to /old-home.
 * Slim role cards (unique one-liner each) + comparison matrix for differentiation.
 * Modules-style SVG icons; Advisors tile; deduped terms.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohPricingEnhance >= 202607303) return;
    window.__ohPricingEnhance = 202607303;

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

    var SLIM = {
      owners: {
        desc: "Asset-side decision makers",
        model: "Success-Based",
        blurb: "No platform fee until the opportunity hits the milestone you agree up front.",
        cta: "Explore Your Opportunity",
        href: "https://www.dealality.com/opportunity-review",
      },
      brands: {
        desc: "Franchise and development teams",
        model: "Annual Access",
        blurb: "Pursue owner-led opportunities in the markets you cover.",
        cta: "Request Brand Access",
        href: "mailto:joan@dealality.com?subject=Dealality%20Brand%20Access",
      },
      operators: {
        desc: "Hotel management companies",
        model: "Annual Access",
        blurb: "Compete for mandates with structured owner and asset context.",
        cta: "Request Operator Access",
        href: "mailto:joan@dealality.com?subject=Dealality%20Operator%20Access",
      },
      advisors: {
        desc: "Counsel and transaction advisors",
        model: "Annual Access",
        blurb: "Enter early enough to shape outcomes — with a clear decision record for clients.",
        cta: "Request Advisor Access",
        href: "mailto:joan@dealality.com?subject=Dealality%20Advisor%20Access",
      },
    };

    var MATRIX_ROWS = [
      {
        label: "Fee model",
        owners: "Success-based at agreed milestone",
        brands: "Annual subscription",
        operators: "Annual subscription",
        advisors: "Annual subscription",
        model: true,
      },
      {
        label: "Primary job",
        owners: "Compare paths and choose partners",
        brands: "Win owner-led brand opportunities",
        operators: "Win quality management mandates",
        advisors: "Guide clients before the path is fixed",
      },
      {
        label: "What you access",
        owners: "Confidential review + packaged outreach",
        brands: "Opportunity criteria + proposal workflow",
        operators: "Asset context + term submission",
        advisors: "Client visibility + advisory notes",
      },
      {
        label: "Deal-room role",
        owners: "You control participation",
        brands: "Permission-based contributor",
        operators: "Permission-based contributor",
        advisors: "Counsel with visibility",
      },
      {
        label: "Scoped by",
        owners: "Opportunity size and complexity",
        brands: "Markets, users, participation needs",
        operators: "Footprint, markets, users",
        advisors: "Advisory seats and client scope",
      },
      {
        label: "Volume guarantee",
        owners: "N/A — scoped per opportunity",
        brands: "None",
        operators: "None",
        advisors: "None",
      },
    ];

    function applyIcons() {
      Object.keys(ICONS).forEach(function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        if (el.querySelector("svg") && el.getAttribute("data-oh-pricing-svg") === "30c") return;
        el.setAttribute("data-oh-pricing-svg", "30c");
        el.innerHTML = ICONS[id];
      });
    }

    function ensureAdvisorsCard() {
      var grid = document.getElementById("pricing-grid");
      if (!grid || document.getElementById("pricing-card-advisors")) return;
      var article = document.createElement("article");
      article.id = "pricing-card-advisors";
      article.innerHTML = [
        '<div id="pricing-advisors-icon" aria-hidden="true"></div>',
        '<h3 id="pricing-advisors-title">Advisors</h3>',
        '<p id="pricing-advisors-desc"></p>',
        '<p id="pricing-advisors-price"><strong></strong><span></span></p>',
        '<ul id="pricing-advisors-features" role="list"></ul>',
        '<a id="pricing-advisors-cta" href="#"></a>',
      ].join("");
      grid.appendChild(article);
    }

    function slimCards() {
      ["owners", "brands", "operators", "advisors"].forEach(function (role) {
        var conf = SLIM[role];
        var desc = document.getElementById("pricing-" + role + "-desc");
        var price = document.getElementById("pricing-" + role + "-price");
        var cta = document.getElementById("pricing-" + role + "-cta");
        if (desc) desc.textContent = conf.desc;
        if (price) {
          var strong = price.querySelector("strong");
          var span = price.querySelector("span");
          if (!strong) {
            strong = document.createElement("strong");
            price.appendChild(strong);
          }
          if (!span) {
            span = document.createElement("span");
            price.appendChild(span);
          }
          strong.textContent = conf.model;
          span.textContent = conf.blurb;
        }
        if (cta) {
          cta.textContent = conf.cta;
          cta.setAttribute("href", conf.href);
        }
      });
    }

    function ensureLead() {
      var h2 = document.getElementById("pricing-h2");
      if (!h2) return;
      var lead = document.getElementById("pricing-lead");
      if (!lead) {
        lead = document.createElement("p");
        lead.id = "pricing-lead";
        h2.insertAdjacentElement("afterend", lead);
      }
      lead.textContent =
        "Owners run opportunity-by-opportunity. Brands, operators, and advisors subscribe for ongoing access.";
    }

    function ensureMatrix() {
      var grid = document.getElementById("pricing-grid");
      if (!grid) return;
      var wrap = document.getElementById("pricing-matrix-wrap");
      if (!wrap) {
        wrap = document.createElement("div");
        wrap.id = "pricing-matrix-wrap";
        grid.insertAdjacentElement("afterend", wrap);
      }
      if (wrap.getAttribute("data-oh-matrix") === "30c") return;

      var rowsHtml = MATRIX_ROWS.map(function (row) {
        function cell(role, text) {
          var cls = [];
          if (role === "owners") cls.push("oh-px-owners");
          if (row.model) cls.push("oh-px-model");
          var attr = cls.length ? ' class="' + cls.join(" ") + '"' : "";
          return "<td" + attr + ">" + text + "</td>";
        }
        return (
          "<tr>" +
          '<th scope="row">' +
          row.label +
          "</th>" +
          cell("owners", row.owners) +
          cell("brands", row.brands) +
          cell("operators", row.operators) +
          cell("advisors", row.advisors) +
          "</tr>"
        );
      }).join("");

      wrap.innerHTML = [
        '<h3 id="pricing-matrix-h3">How the paths differ</h3>',
        '<div id="pricing-matrix-scroll">',
        '<table id="pricing-matrix">',
        "<thead><tr>",
        '<th scope="col">Compare</th>',
        '<th scope="col" class="oh-px-owners">Owners</th>',
        '<th scope="col">Brands</th>',
        '<th scope="col">Operators</th>',
        '<th scope="col">Advisors</th>',
        "</tr></thead>",
        "<tbody>" + rowsHtml + "</tbody>",
        "</table></div>",
      ].join("");
      wrap.setAttribute("data-oh-matrix", "30c");
    }

    function normalizeTerms() {
      var terms = document.getElementById("pricing-terms");
      if (!terms) return;

      var owners = document.getElementById("pricing-term-owners");
      if (owners) {
        owners.textContent =
          "Owner pricing varies by opportunity size, complexity, and scope.";
      }

      var sub = document.getElementById("pricing-term-subscription");
      if (!sub) {
        sub = document.createElement("p");
        sub.id = "pricing-term-subscription";
        terms.appendChild(sub);
      }
      sub.textContent =
        "Subscription access does not guarantee a minimum volume of opportunities.";

      ["pricing-term-brands", "pricing-term-operators", "pricing-term-advisors"].forEach(
        function (id) {
          var el = document.getElementById(id);
          if (el && el.parentNode) el.parentNode.removeChild(el);
        }
      );
    }

    function enhance() {
      ensureAdvisorsCard();
      slimCards();
      applyIcons();
      ensureLead();
      ensureMatrix();
      normalizeTerms();
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", enhance);
    } else {
      enhance();
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
