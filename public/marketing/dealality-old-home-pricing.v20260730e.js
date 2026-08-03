/**
 * Old Home — Pricing enhance (v20260730e)
 * Path-gated to /old-home.
 * One composition: role headers + matrix with perspective-specific benefits
 * (process, opportunities, communication) — not identical bullets per role.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohPricingEnhance >= 202607305) return;
    window.__ohPricingEnhance = 202607305;

    var ROLES = ["owners", "brands", "operators", "advisors"];

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
        title: "Owners",
        desc: "Asset-side decision makers",
        model: "Success-Based",
        cta: "Explore Your Opportunity",
        href: "https://www.dealality.com/opportunity-review",
      },
      brands: {
        title: "Brands",
        desc: "Franchise and development teams",
        model: "Annual Access",
        cta: "Request Brand Access",
        href: "mailto:joan@dealality.com?subject=Dealality%20Brand%20Access",
      },
      operators: {
        title: "Operators",
        desc: "Hotel management companies",
        model: "Annual Access",
        cta: "Request Operator Access",
        href: "mailto:joan@dealality.com?subject=Dealality%20Operator%20Access",
      },
      advisors: {
        title: "Advisors",
        desc: "Counsel and transaction advisors",
        model: "Annual Access",
        cta: "Request Advisor Access",
        href: "mailto:joan@dealality.com?subject=Dealality%20Advisor%20Access",
      },
    };

    var MATRIX_ROWS = [
      {
        label: "Primary job",
        owners: "Compare paths and choose partners",
        brands: "Win owner-led brand opportunities",
        operators: "Win quality management mandates",
        advisors: "Guide clients before the path is fixed",
      },
      {
        label: "Process",
        owners: "A less fragmented path — one controlled process instead of scattered outreach",
        brands: "Pursue opportunities inside one structured, owner-led workflow",
        operators: "Clear submission process with asset context already packaged",
        advisors: "Stay inside the process early — not after the path is locked",
        benefit: true,
      },
      {
        label: "Opportunities",
        owners: "Relevant partners engaged against your criteria — not a cold blast",
        brands: "Owner-qualified opportunities matched to the markets you cover",
        operators: "Compete for mandates that already carry owner intent",
        advisors: "Opportunities with a decision record you can stand behind for clients",
        benefit: true,
      },
      {
        label: "Communication",
        owners: "You decide who is in the room and when",
        brands: "Permission-based engagement with the owner and other parties",
        operators: "Coordinated term discussion without side-channel noise",
        advisors: "Shared visibility so counsel isn't reconstructing threads later",
        benefit: true,
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
        if (el.querySelector("svg") && el.getAttribute("data-oh-pricing-svg") === "30e") return;
        el.setAttribute("data-oh-pricing-svg", "30e");
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
      ROLES.forEach(function (role) {
        var conf = SLIM[role];
        var title = document.getElementById("pricing-" + role + "-title");
        var desc = document.getElementById("pricing-" + role + "-desc");
        var price = document.getElementById("pricing-" + role + "-price");
        var cta = document.getElementById("pricing-" + role + "-cta");
        if (title) title.textContent = conf.title;
        if (desc) desc.textContent = conf.desc;
        if (price) {
          var strong = price.querySelector("strong");
          var span = price.querySelector("span");
          if (strong) strong.textContent = conf.model;
          if (span) span.textContent = "";
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

    function headCellHtml(role) {
      var conf = SLIM[role];
      var ownersCls = role === "owners" ? " oh-px-owners" : "";
      return [
        '<th scope="col" class="oh-px-head-cell' + ownersCls + '" id="pricing-head-' + role + '">',
        '<div class="oh-px-head">',
        '<div class="oh-px-icon-slot"></div>',
        '<div class="oh-px-role">' + conf.title + "</div>",
        '<div class="oh-px-audience">' + conf.desc + "</div>",
        '<div class="oh-px-model">' + conf.model + "</div>",
        '<div class="oh-px-cta-slot"></div>',
        "</div></th>",
      ].join("");
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
      if (wrap.getAttribute("data-oh-matrix") === "30e") {
        grid.setAttribute("hidden", "");
        grid.setAttribute("data-oh-pricing-hidden", "1");
        return;
      }

      var stash = document.createElement("div");
      stash.setAttribute("hidden", "");
      stash.id = "pricing-node-stash";
      document.body.appendChild(stash);
      ROLES.forEach(function (role) {
        var icon = document.getElementById("pricing-" + role + "-icon");
        var cta = document.getElementById("pricing-" + role + "-cta");
        if (icon) stash.appendChild(icon);
        if (cta) stash.appendChild(cta);
      });

      var rowsHtml = MATRIX_ROWS.map(function (row) {
        function cell(role, text) {
          var cls = [];
          if (role === "owners") cls.push("oh-px-owners");
          if (row.benefit) cls.push("oh-px-benefit");
          var attr = cls.length ? ' class="' + cls.join(" ") + '"' : "";
          return "<td" + attr + ">" + text + "</td>";
        }
        return (
          "<tr" +
          (row.benefit ? ' class="oh-px-benefit-row"' : "") +
          ">" +
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
        '<p id="pricing-matrix-hint">Swipe to compare paths</p>',
        '<div id="pricing-matrix-scroll">',
        '<table id="pricing-matrix">',
        "<thead><tr>",
        '<th scope="col" class="oh-px-corner"><span>Compare</span></th>',
        headCellHtml("owners"),
        headCellHtml("brands"),
        headCellHtml("operators"),
        headCellHtml("advisors"),
        "</tr></thead>",
        "<tbody>" + rowsHtml + "</tbody>",
        "</table></div>",
      ].join("");

      ROLES.forEach(function (role) {
        var head = document.getElementById("pricing-head-" + role);
        if (!head) return;
        var iconSlot = head.querySelector(".oh-px-icon-slot");
        var ctaSlot = head.querySelector(".oh-px-cta-slot");
        var icon = document.getElementById("pricing-" + role + "-icon");
        var cta = document.getElementById("pricing-" + role + "-cta");
        if (icon && iconSlot) iconSlot.appendChild(icon);
        if (cta && ctaSlot) ctaSlot.appendChild(cta);
      });
      if (stash.parentNode) stash.parentNode.removeChild(stash);

      wrap.setAttribute("data-oh-matrix", "30e");
      grid.setAttribute("hidden", "");
      grid.setAttribute("data-oh-pricing-hidden", "1");
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
