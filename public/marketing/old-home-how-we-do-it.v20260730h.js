/**
 * Old Home — How We Do It document-flow process story (v20260730h)
 * Path-gated to /old-home. Sticky nav + stacked visible steps (no empty runway).
 * Adds a light 6-tile process overview above the detailed steps.
 * Eyebrow matches Problem/Features dual-pill pattern.
 * Namespace: dealality-process_*
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b38e30f1c5be3a79b319f_dealality-old-home-how-we-do-it.v20260730h.css";
    if (window.__ohHowWeDoIt >= 202607311) return;
    window.__ohHowWeDoIt = 202607311;
    var IMG_BRAND =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer.png";
    var IMG_COMPARE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679519982910d5c314bf1f_deal-compare.png";

    var STEPS = [
      {
        n: "01",
        title: "Define the Opportunity",
        primary:
          "Clarify what the owner wants the asset to achieve before the first available relationship begins shaping the direction.",
        support:
          "Bring the hotel, market context, objectives, constraints, commercial priorities, and decision criteria into one structured opportunity brief.",
        labels: [
          "Asset and Market Context",
          "Owner Objectives",
          "Commercial Priorities",
          "Control Preferences",
          "Timing and Constraints",
          "Decision Criteria",
        ],
        visual: "define",
      },
      {
        n: "02",
        title: "Explore Credible Paths",
        primary: "See more than the first available option.",
        support:
          "Identify the brand, operator, conversion, positioning, operating-model, capital, and strategic-partner paths that may be worth evaluating.",
        labels: [
          "Brand Paths",
          "Operator Paths",
          "Conversion Options",
          "Operating Models",
          "Capital Partners",
          "Strategic Alternatives",
        ],
        visual: "paths",
        img: IMG_BRAND,
        alt: "Exploring credible brand and operator paths for one hotel opportunity",
      },
      {
        n: "03",
        title: "Prepare and Engage",
        primary: "Present the opportunity consistently to the right participants.",
        support:
          "Prepare the opportunity, select relevant participants, manage confidentiality, coordinate outreach, and capture questions and responses in one place.",
        labels: [
          "Opportunity Brief",
          "Selected Participants",
          "Confidentiality",
          "Outreach Status",
          "Information Requests",
          "Response Tracking",
        ],
        visual: "engage",
      },
      {
        n: "04",
        title: "Compare What Matters",
        primary: "Turn different proposals into a meaningful comparison.",
        support:
          "Compare economics, control, requirements, support, timing, missing terms, and material trade-offs on a shared basis.",
        labels: [
          "Fees and Economics",
          "Owner Control",
          "Capital Support",
          "Brand Requirements",
          "Operating Terms",
          "Missing Information",
        ],
        visual: "compare",
        img: IMG_COMPARE,
        alt: "Side-by-side proposal comparison across fees, control, and missing terms",
      },
      {
        n: "05",
        title: "Pursue the Selected Direction",
        primary: "Move the preferred path forward with clarity and leverage.",
        support:
          "Identify negotiation priorities, resolve remaining gaps, preserve the decision record, and support the owner through the agreed milestone.",
        labels: [
          "Preferred Direction",
          "Negotiation Priorities",
          "Remaining Conditions",
          "Decision Rationale",
          "Next Actions",
          "Decision Record",
        ],
        visual: "pursue",
      },
    ];

    var OVERVIEW = [
      {
        title: "Review the Opportunity",
        body: "Bring the hotel, owner goals, market context, constraints, and key questions into one place.",
        step: 0,
        icon:
          '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="9" y="6" width="22" height="28" rx="2.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M14 13h12M14 18h12M14 23h8" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><circle cx="28" cy="28" r="6" stroke="#9B8AFB" stroke-width="1.5"/><path d="M32 32l3.5 3.5" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/></svg>',
      },
      {
        title: "Explore the Possible Paths",
        body: "Identify the brands, operators, structures, conversions, and capital options worth considering.",
        step: 1,
        icon:
          '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="8" cy="20" r="3" fill="#9B8AFB"/><path d="M11 20h5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="12" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><circle cx="20" cy="20" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><circle cx="20" cy="28" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><path d="M16 20l1-5.5M16 20l1 5.5" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M23 12h5M23 20h5M23 28h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" stroke-dasharray="2 2" opacity=".4"/><circle cx="32" cy="12" r="2.5" fill="#9B8AFB" opacity=".4"/><circle cx="32" cy="20" r="2.5" fill="#9B8AFB" opacity=".4"/><circle cx="32" cy="28" r="2.5" fill="#9B8AFB" opacity=".4"/></svg>',
      },
      {
        title: "Research the Right Partners",
        body: "Understand who may fit, why they may fit, and what still needs to be confirmed.",
        step: 1,
        icon:
          '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="15" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="27" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M21 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M19 16h2" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-dasharray="1.5 1.5" opacity=".4"/><path d="M18 11l2-3M24 11l-2-3" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><circle cx="21" cy="7" r="1.5" fill="#9B8AFB" opacity=".5"/></svg>',
      },
      {
        title: "Prepare and Manage Outreach",
        body: "Present the opportunity clearly and manage confidential conversations with selected parties.",
        step: 2,
        icon:
          '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="6" y="12" width="20" height="16" rx="2.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M6 16l10 6 10-6" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><rect x="22" y="8" width="12" height="9" rx="2" stroke="#9B8AFB" stroke-width="1.2" opacity=".6"/><path d="M25 12h6M25 14h4" stroke="#9B8AFB" stroke-width="1" stroke-linecap="round" opacity=".4"/><circle cx="30" cy="26" r="5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M30 23v3l2 1.5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round"/></svg>',
      },
      {
        title: "Compare Proposals",
        body: "Review fees, requirements, support, control, timing, and important differences side by side.",
        step: 3,
        icon:
          '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="5" y="8" width="13" height="24" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="22" y="8" width="13" height="24" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 14h5M9 18h5M9 22h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><path d="M26 14h5M26 18h5M26 22h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><path d="M18 16l4 0M18 20l4 0" stroke="#9B8AFB" stroke-width="1.3" stroke-dasharray="1.5 1.5" opacity=".35"/><path d="M9 27l2 2 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><path d="M26 27l2 2 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
      },
      {
        title: "Support the Negotiation and Decision",
        body: "Track missing terms, negotiation priorities, open questions, and the reasons behind the final choice.",
        step: 4,
        icon:
          '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M8 22c0 0 4-8 12-8s12 8 12 8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><path d="M8 22c0 0 4 8 12 8s12-8 12-8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="20" cy="22" r="5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="22" r="2" fill="#9B8AFB"/><path d="M17 8h6M20 6v4" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" opacity=".5"/><path d="M31 15l2-2M9 15l-2-2" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/></svg>',
      },
    ];

    function ensureCss() {
      var existing = document.querySelector(
        'link[href*="dealality-old-home-how-we-do-it"]'
      );
      if (existing) {
        existing.setAttribute("href", CSS_HREF);
        existing.setAttribute("data-oh-how", "30h");
        return;
      }
      if (document.querySelector('link[data-oh-how="30h"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-how", "30h");
      (document.head || document.documentElement).appendChild(link);
    }

    function esc(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function labelsHtml(labels) {
      return (
        '<ul class="dealality-process_labels">' +
        labels
          .map(function (l) {
            return "<li>" + esc(l) + "</li>";
          })
          .join("") +
        "</ul>"
      );
    }

    function visualHtml(step) {
      if (step.visual === "define") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Opportunity brief panel">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Opportunity Review · Brief</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Opportunity Brief</span>' +
          '<span class="dealality-process_meta">Demo · Coastal Boutique Hotel</span>' +
          "</div>" +
          '<div class="dealality-process_hero-stat"><strong>118 keys</strong><span>Upscale boutique · Greater Santo Domingo</span></div>' +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Opportunity Type</strong><span>Brand + operator review</span></div>' +
          '<div class="dealality-process_row"><strong>Owner Objectives</strong><span>Stabilize NOI · Preserve control</span></div>' +
          '<div class="dealality-process_row"><strong>Constraints</strong><span>Capex limited · 18-month horizon</span></div>' +
          '<div class="dealality-process_row"><strong>Decision Criteria</strong><span>Control · fees · timeline · support</span></div>' +
          '<div class="dealality-process_row"><strong>Current Stage</strong><span>Define opportunity</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "paths") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Credible strategic paths">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Brand Explorer · Path Map</em></div>' +
          '<div class="dealality-process_shot">' +
          (step.img
            ? '<img src="' +
              esc(step.img) +
              '" alt="' +
              esc(step.alt) +
              '" loading="lazy" width="960" height="540">'
            : "") +
          "</div>" +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Credible Paths</span>' +
          '<span class="dealality-process_meta">One hotel · several futures</span>' +
          "</div>" +
          '<div class="dealality-process_paths">' +
          '<div class="dealality-process_path"><strong>Soft Brand</strong><span>Fit rationale · Capex moderate · Open questions on pipeline</span></div>' +
          '<div class="dealality-process_path"><strong>Operator Partner</strong><span>Operating model lift · Control trade-offs · Shortlist</span></div>' +
          '<div class="dealality-process_path"><strong>Conversion</strong><span>Positioning shift · Evidence pack · Review pending</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "engage") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Prepare and engage workspace">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Controlled Outreach · Workspace</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Controlled Outreach</span>' +
          '<span class="dealality-process_meta">One opportunity · one consistent story</span>' +
          "</div>" +
          '<div class="dealality-process_progress-bar" aria-hidden="true"><i style="width:62%"></i></div>' +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Selected Participants</strong><span>3 brands · 2 operators</span></div>' +
          '<div class="dealality-process_row"><strong>Confidentiality</strong><span>NDA active · gated materials</span></div>' +
          '<div class="dealality-process_row"><strong>Outreach Status</strong><span>2 contacted · 1 ready</span></div>' +
          '<div class="dealality-process_row"><strong>Questions Received</strong><span>Capex · brand standards</span></div>' +
          '<div class="dealality-process_row"><strong>Responses Pending</strong><span>Commercial terms due</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "compare") {
        return (
          '<div class="dealality-process_visual is-compare is-large" aria-label="Proposal comparison">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Shared Comparison · Strongest View</em></div>' +
          '<div class="dealality-process_shot is-compare-shot">' +
          (step.img
            ? '<img src="' +
              esc(step.img) +
              '" alt="' +
              esc(step.alt) +
              '" loading="lazy" width="960" height="540">'
            : "") +
          "</div>" +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Shared Comparison</span>' +
          '<span class="dealality-process_meta">Economics · control · gaps</span>' +
          "</div>" +
          '<div class="dealality-process_compare" role="table" aria-label="Proposal differences">' +
          '<div class="is-head" role="columnheader">Category</div><div class="is-head" role="columnheader">Path A</div><div class="is-head" role="columnheader">Path B</div><div class="is-head" role="columnheader">Path C</div>' +
          "<div>Fees</div><div>4.5% + 2%</div><div>5.0% + 1%</div><div>3.8% + 3%</div>" +
          "<div>Owner Control</div><div>High</div><div>Medium</div><div>Shared</div>" +
          "<div>Capital Support</div><div>Key money</div><div>—</div><div>PIP credit</div>" +
          '<div>Missing Terms</div><div class="is-gap">Capex TBD</div><div>—</div><div class="is-gap">Exit TBD</div>' +
          "<div>Timing</div><div>9–12 mo</div><div>6–9 mo</div><div>12–18 mo</div>" +
          "</div></div></div>"
        );
      }
      return (
        '<div class="dealality-process_visual is-large" aria-label="Selected direction workspace">' +
        '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Preferred Direction · Decision Record</em></div>' +
        '<div class="dealality-process_panel">' +
        '<div class="dealality-process_panel-head">' +
        '<span class="dealality-process_chip">Preferred Direction</span>' +
        '<span class="dealality-process_meta">Owner remains decision-maker</span>' +
        "</div>" +
        '<div class="dealality-process_rows">' +
        '<div class="dealality-process_row"><strong>Paths Considered</strong><span>Soft brand · Operator · Conversion</span></div>' +
        '<div class="dealality-process_row"><strong>Preferred Direction</strong><span>Soft brand + operator support</span></div>' +
        '<div class="dealality-process_row"><strong>Key Reasons</strong><span>Control · brand lift · timeline</span></div>' +
        '<div class="dealality-process_row"><strong>Remaining Conditions</strong><span>Capex schedule · PIP scope</span></div>' +
        '<div class="dealality-process_row"><strong>Negotiation Priorities</strong><span>Fees · termination · support</span></div>' +
        '<div class="dealality-process_row"><strong>Next Action</strong><span>Owner review · counsel align</span></div>' +
        "</div></div></div>"
      );
    }

    function stepHtml(step, i) {
      return (
        '<article class="dealality-process_step' +
        (i === 0 ? " is-active" : "") +
        '" id="dealality-process-step-' +
        (i + 1) +
        '" data-dealality-process-step="' +
        i +
        '">' +
        '<p class="dealality-process_kicker">Step ' +
        esc(step.n) +
        "</p>" +
        '<h3 class="dealality-process_step-title">' +
        esc(step.title) +
        "</h3>" +
        '<p class="dealality-process_primary">' +
        esc(step.primary) +
        "</p>" +
        '<p class="dealality-process_support">' +
        esc(step.support) +
        "</p>" +
        visualHtml(step) +
        labelsHtml(step.labels) +
        "</article>"
      );
    }

    function buildSection() {
      var nav = STEPS.map(function (step, i) {
        return (
          '<button type="button" class="dealality-process_nav-item' +
          (i === 0 ? " is-active" : "") +
          '" data-dealality-process-nav="' +
          i +
          '" aria-controls="dealality-process-step-' +
          (i + 1) +
          '" aria-current="' +
          (i === 0 ? "step" : "false") +
          '">' +
          '<span class="dealality-process_nav-num" aria-hidden="true">' +
          esc(step.n) +
          "</span>" +
          '<span class="dealality-process_nav-label">' +
          esc(step.title) +
          "</span>" +
          "</button>"
        );
      }).join("");

      var overview = OVERVIEW.map(function (tile, i) {
        var num =
          String(i + 1).length > 1 ? String(i + 1) : "0" + (i + 1);
        return (
          '<div class="dealality-process_overview-card" role="button" tabindex="0" data-dealality-process-overview="' +
          tile.step +
          '" aria-label="' +
          esc(tile.title) +
          ' — jump to process detail">' +
          '<span class="dealality-process_overview-icon" aria-hidden="true">' +
          tile.icon +
          "</span>" +
          '<span class="dealality-process_overview-num" aria-hidden="true">' +
          num +
          "</span>" +
          '<strong class="dealality-process_overview-title">' +
          esc(tile.title) +
          "</strong>" +
          '<span class="dealality-process_overview-body">' +
          esc(tile.body) +
          "</span>" +
          "</div>"
        );
      }).join("");

      return (
        '<section id="oh-how-we-do-it" class="dealality-process_section oh-how" aria-labelledby="dealality-process-h2" style="--oh-how-steps:' +
        STEPS.length +
        '" data-oh-how="30h">' +
        '<div class="dealality-process_glow" aria-hidden="true"></div>' +
        '<div class="dealality-process_inner">' +
        '<header class="dealality-process_intro">' +
        '<div class="dealality-process_eyebrow">' +
        '<span class="dealality-process_eyebrow-pill">How Dealality Works</span>' +
        '<span class="dealality-process_eyebrow-right">From Possibility to Pursuit.</span>' +
        "</div>" +
        '<h2 class="dealality-process_h2" id="dealality-process-h2">Turn One Hotel Opportunity Into a Structured Decision Process.</h2>' +
        '<p class="dealality-process_lead">Dealality brings the opportunity, credible strategic paths, market engagement, responses, and decision criteria into one confidential process—helping owners explore more possibilities without losing control or momentum.</p>' +
        "</header>" +
        '<div class="dealality-process_overview" aria-label="Process overview">' +
        overview +
        "</div>" +
        '<div class="dealality-process_runway" id="dealality-process-runway">' +
        '<div class="dealality-process_layout">' +
        '<div class="dealality-process_nav-col">' +
        '<nav class="dealality-process_nav" aria-label="Dealality process steps">' +
        '<div class="dealality-process_progress" aria-hidden="true"><span id="dealality-process-progress"></span></div>' +
        nav +
        "</nav></div>" +
        '<div class="dealality-process_content" id="dealality-process-content">' +
        STEPS.map(stepHtml).join("") +
        "</div>" +
        "</div></div>" +
        '<div class="dealality-process_cta">' +
        "<h3>One Opportunity. One Connected Process.</h3>" +
        "<p>From the first question to the selected direction, Dealality keeps the opportunity, participants, proposals, and decision criteria connected.</p>" +
        '<div class="dealality-process_cta-row">' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-primary" data-dealality-process-cta="explore">Explore Your Opportunity</button>' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-secondary" data-dealality-process-cta="video">See Dealality in Action</button>' +
        "</div></div>" +
        '<p class="dealality-process_sr" id="dealality-process-live" aria-live="polite"></p>' +
        "</div></section>"
      );
    }

    function track(eventName, payload) {
      try {
        if (window.dataLayer && Array.isArray(window.dataLayer)) {
          window.dataLayer.push(
            Object.assign({ event: eventName }, payload || {})
          );
        } else if (typeof window.gtag === "function") {
          window.gtag("event", eventName, payload || {});
        }
      } catch (_e) {}
    }

    function mount() {
      var existing = document.getElementById("oh-how-we-do-it");
      var html = buildSection();
      if (existing) {
        var wrap = document.createElement("div");
        wrap.innerHTML = html;
        existing.parentNode.replaceChild(wrap.firstChild, existing);
        return true;
      }
      // Prefer insert before Features (#platform-features). Legacy #features
      // Process leftover was removed; keep a soft fallback if it still exists.
      var anchor =
        document.getElementById("platform-features") ||
        document.getElementById("features") ||
        document.getElementById("modules");
      if (!anchor || !anchor.parentNode) return false;
      var holder = document.createElement("div");
      holder.innerHTML = html;
      if (anchor.id === "features") {
        anchor.parentNode.insertBefore(holder.firstChild, anchor.nextSibling);
      } else {
        anchor.parentNode.insertBefore(holder.firstChild, anchor);
      }
      return true;
    }

    function bind(section) {
      if (!section || section.getAttribute("data-dealality-process-bound") === "1")
        return;
      section.setAttribute("data-dealality-process-bound", "1");

      var navItems = Array.prototype.slice.call(
        section.querySelectorAll("[data-dealality-process-nav]")
      );
      var steps = Array.prototype.slice.call(
        section.querySelectorAll("[data-dealality-process-step]")
      );
      var progress = section.querySelector("#dealality-process-progress");
      var live = section.querySelector("#dealality-process-live");
      var active = 0;
      var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
      var viewed = {};
      var scrollingTo = -1;
      var scrollClearTimer = 0;

      function setActive(index, fromUser) {
        if (index < 0 || index >= STEPS.length) return;
        active = index;
        navItems.forEach(function (btn, i) {
          var on = i === active;
          btn.classList.toggle("is-active", on);
          btn.classList.toggle("is-done", i < active);
          btn.setAttribute("aria-current", on ? "step" : "false");
        });
        steps.forEach(function (el, i) {
          el.classList.toggle("is-active", i === active);
          el.removeAttribute("hidden");
        });
        if (progress) {
          var pct =
            STEPS.length <= 1
              ? 100
              : Math.round((active / (STEPS.length - 1)) * 100);
          progress.style.height = pct + "%";
        }
        if (live) {
          live.textContent =
            "Step " + STEPS[active].n + ": " + STEPS[active].title;
        }
        if (!viewed[active]) {
          viewed[active] = 1;
          track("process_step_view", {
            step: STEPS[active].n,
            title: STEPS[active].title,
          });
        }
        if (fromUser) {
          track("process_step_click", {
            step: STEPS[active].n,
            title: STEPS[active].title,
          });
        }
      }

      function scrollToStep(index) {
        var el = steps[index];
        if (!el) return;
        scrollingTo = index;
        setActive(index, true);
        if (typeof el.scrollIntoView === "function") {
          el.scrollIntoView({
            behavior: reduceMotion.matches ? "auto" : "smooth",
            block: "start",
          });
        }
        if (scrollClearTimer) window.clearTimeout(scrollClearTimer);
        scrollClearTimer = window.setTimeout(function () {
          scrollingTo = -1;
        }, 900);
      }

      navItems.forEach(function (btn) {
        btn.addEventListener("click", function () {
          var idx = parseInt(btn.getAttribute("data-dealality-process-nav"), 10);
          if (!isNaN(idx)) scrollToStep(idx);
        });
      });

      Array.prototype.forEach.call(
        section.querySelectorAll("[data-dealality-process-overview]"),
        function (btn) {
          function go() {
            var idx = parseInt(
              btn.getAttribute("data-dealality-process-overview"),
              10
            );
            if (!isNaN(idx)) {
              track("process_overview_click", {
                step: STEPS[idx] && STEPS[idx].n,
                title: STEPS[idx] && STEPS[idx].title,
              });
              scrollToStep(idx);
            }
          }
          btn.addEventListener("click", go);
          btn.addEventListener("keydown", function (e) {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              go();
            }
          });
        }
      );

      var explore = section.querySelector('[data-dealality-process-cta="explore"]');
      if (explore) {
        explore.addEventListener("click", function () {
          track("process_cta_click", { cta: "explore" });
          if (typeof window.ohOpenOpportunityReview === "function") {
            window.ohOpenOpportunityReview();
            return;
          }
          var orBtn =
            document.querySelector("[data-oh-or-open]") ||
            document.querySelector('a[href*="opportunity"]');
          if (orBtn) orBtn.click();
        });
      }
      var videoCta = section.querySelector('[data-dealality-process-cta="video"]');
      if (videoCta) {
        videoCta.addEventListener("click", function () {
          track("process_cta_click", { cta: "video" });
          var launcher =
            document.querySelector("[data-oh-video-open]") ||
            document.querySelector("#oh-platform-video-launcher") ||
            document.querySelector(".oh-video-launcher");
          if (launcher) launcher.click();
        });
      }

      if (typeof IntersectionObserver === "function") {
        var ratios = {};
        var io = new IntersectionObserver(
          function (entries) {
            if (scrollingTo >= 0) return;
            entries.forEach(function (entry) {
              var idx = parseInt(
                entry.target.getAttribute("data-dealality-process-step"),
                10
              );
              if (isNaN(idx)) return;
              ratios[idx] = entry.isIntersecting ? entry.intersectionRatio : 0;
            });
            var best = active;
            var bestRatio = -1;
            Object.keys(ratios).forEach(function (k) {
              var i = parseInt(k, 10);
              if (ratios[i] > bestRatio) {
                bestRatio = ratios[i];
                best = i;
              }
            });
            if (bestRatio > 0 && best !== active) setActive(best, false);
          },
          {
            root: null,
            rootMargin: "-20% 0px -45% 0px",
            threshold: [0.15, 0.35, 0.55, 0.75],
          }
        );
        steps.forEach(function (el) {
          io.observe(el);
        });
      }

      setActive(0, false);
      track("process_section_view", { section: "how-we-do-it" });
    }

    function run() {
      ensureCss();
      if (!mount()) return;
      bind(document.getElementById("oh-how-we-do-it"));
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
    window.addEventListener("load", run);
    setTimeout(run, 900);
    setTimeout(run, 2200);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-how-we-do-it]", err);
    }
  }
})();
