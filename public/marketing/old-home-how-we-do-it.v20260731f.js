/**
 * Old Home — How We Do It compact one-screen process (v20260731f)
 * Path-gated to /old-home.
 * Click left nav to swap the right step in place. No scroll runway.
 * Namespace: dealality-process_*
 * 8 steps: 5 platform + 3 offline (Meet, Collaborate, Hand Off).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6c678ad8e5949e1d264b56_dealality-old-home-how-we-do-it.v20260731c.css";
    if (window.__ohHowWeDoIt >= 202607316) return;
    window.__ohHowWeDoIt = 202607316;
    var IMG_BRAND =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer.png";
    var IMG_COMPARE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679519982910d5c314bf1f_deal-compare.png";
    var STEPS = [
      {
        n: "01",
        mode: "platform",
        title: "Define the Opportunity",
        primary:
          "Clarify what the owner wants before any relationship starts shaping the answer.",
        support:
          "Bring the hotel, market context, objectives, constraints, and decision criteria into one brief—so later conversations start from clarity.",
        bridge: "Prepares the first offline conversations with a shared opportunity story.",
        labels: [
          "Asset Context",
          "Owner Objectives",
          "Commercial Priorities",
          "Control Preferences",
          "Timing",
          "Decision Criteria",
        ],
        visual: "define",
      },
      {
        n: "02",
        mode: "platform",
        title: "Explore Credible Paths",
        primary: "See more than the first available option.",
        support:
          "Identify the brand, operator, conversion, and partner paths worth evaluating—so the owner chooses who to meet with intention.",
        bridge: "Shortlists who is worth meeting before outreach begins.",
        labels: [
          "Brand Paths",
          "Operator Paths",
          "Conversion",
          "Operating Models",
          "Capital",
          "Alternatives",
        ],
        visual: "paths",
        img: IMG_BRAND,
        alt: "Exploring credible brand and operator paths for one hotel opportunity",
      },
      {
        n: "03",
        mode: "platform",
        title: "Prepare & Engage",
        primary: "Open the right conversations—without losing control of the story.",
        support:
          "Select participants, manage confidentiality, coordinate outreach, and capture responses so introduction calls start ready—not cold.",
        bridge: "Hands a clean brief and status into the first relationship step.",
        labels: [
          "Opportunity Brief",
          "Participants",
          "Confidentiality",
          "Outreach",
          "Questions",
          "Responses",
        ],
        visual: "engage",
      },
      {
        n: "04",
        mode: "offline",
        title: "Meet & Align",
        primary: "Put people in the room—where trust actually begins.",
        support:
          "Introduction and clarification happen offline between people. Dealality prepares both sides and tracks outcomes so the conversation stays part of the process—not a side channel.",
        bridge: "Met offline. Facilitated by Dealality · tracked in Dealality.",
        labels: [
          "Introduction Call",
          "Clarification Call",
          "Fit Discussion",
          "Open Questions",
          "Tracked Outcome",
        ],
        visual: "meet",
      },
      {
        n: "05",
        mode: "platform",
        title: "Compare What Matters",
        primary: "Give every proposal a shared basis—so the next conversation can go deeper.",
        support:
          "Compare economics, control, requirements, support, timing, and gaps side by side. The numbers do not replace judgment; they prepare it.",
        bridge: "Creates a shared comparison for facilitated offline diligence.",
        labels: [
          "Fees",
          "Owner Control",
          "Capital Support",
          "Requirements",
          "Timing",
          "Gaps",
        ],
        visual: "compare",
        img: IMG_COMPARE,
        alt: "Side-by-side proposal comparison across fees, control, and missing terms",
      },
      {
        n: "06",
        mode: "offline",
        title: "Collaborate & Review",
        primary:
          "Take the comparison into the real world—calls, walkthroughs, and technical review.",
        support:
          "Collaboration calls and site or tech review happen offline. Dealality facilitates scheduling and context, then tracks what was covered so judgment stays connected to the deal record.",
        bridge: "Met offline. Facilitated by Dealality · tracked in Dealality.",
        labels: [
          "Collaboration Calls",
          "Site Visit",
          "Tech Review",
          "Trade-offs",
          "Tracked Notes",
        ],
        visual: "diligence",
      },
      {
        n: "07",
        mode: "platform",
        title: "Pursue the Selected Direction",
        primary:
          "Hold finalists, materials, and feasibility in one place while conversations continue.",
        support:
          "Confirm finalists, open the Deal Room, and sync feasibility—so momentum does not disappear between facilitated offline alignment moments.",
        bridge: "Keeps the record ready for final alignment—still tracked in Dealality.",
        labels: [
          "Finalists",
          "Deal Room",
          "Feasibility",
          "Conditions",
          "Decision Record",
        ],
        visual: "pursue",
      },
      {
        n: "08",
        mode: "offline",
        title: "Align & Hand Off",
        primary:
          "Close alignment between people—then hand into legal with a clean, tracked exit.",
        support:
          "Final alignment on vision, cost, and ops stays human and offline. Dealality tracks readiness through LOI intent, then supports a clean handoff into legal drafting.",
        bridge: "Aligned offline. Tracked in Dealality through handoff.",
        labels: [
          "Final Alignment Call",
          "LOI Intent",
          "Legal Start",
          "MA / FA Drafting",
          "Tracked Handoff",
        ],
        visual: "handoff",
      },
    ];
    function ensureCss() {
      var existing = document.querySelector(
        'link[href*="dealality-old-home-how-we-do-it"]'
      );
      if (existing) {
        existing.setAttribute("href", CSS_HREF);
        existing.setAttribute("data-oh-how", "31f");
        return;
      }
      if (document.querySelector('link[data-oh-how="31f"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-how", "31f");
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
          '<span class="dealality-process_meta">Clarity before the first conversation</span>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Opportunity Type</strong><span>Brand + operator review</span></div>' +
          '<div class="dealality-process_row"><strong>Owner Objectives</strong><span>Stabilize NOI · Preserve control</span></div>' +
          '<div class="dealality-process_row"><strong>Constraints</strong><span>Capex limited · 18-month horizon</span></div>' +
          '<div class="dealality-process_row"><strong>Decision Criteria</strong><span>Control · fees · timeline · support</span></div>' +
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
          '<span class="dealality-process_meta">Choose who is worth meeting</span>' +
          "</div>" +
          '<div class="dealality-process_paths">' +
          '<div class="dealality-process_path"><strong>Soft Brand</strong><span>Fit rationale · Capex moderate</span></div>' +
          '<div class="dealality-process_path"><strong>Operator Partner</strong><span>Operating model lift · Control trade-offs</span></div>' +
          '<div class="dealality-process_path"><strong>Conversion</strong><span>Positioning shift · Evidence pack</span></div>' +
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
          '<span class="dealality-process_meta">Ready for the first meeting</span>' +
          "</div>" +
          '<div class="dealality-process_progress-bar" aria-hidden="true"><i style="width:62%"></i></div>' +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Selected Participants</strong><span>3 brands · 2 operators</span></div>' +
          '<div class="dealality-process_row"><strong>Confidentiality</strong><span>NDA active · gated materials</span></div>' +
          '<div class="dealality-process_row"><strong>Outreach Status</strong><span>2 contacted · 1 ready</span></div>' +
          '<div class="dealality-process_row"><strong>Next Step</strong><span>Introduction call · offline</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "meet") {
        return (
          '<div class="dealality-process_visual is-large is-offline-visual" aria-label="Offline relationship moments">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Relationship Moments</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Meet &amp; Align</span>' +
          '<span class="dealality-process_meta">Where trust begins</span>' +
          "</div>" +
          '<div class="dealality-process_moments">' +
          '<div class="dealality-process_moment"><strong>Introduction Call</strong><p>Owner and brand/operator meet. Fit, intent, and chemistry—not a form submission.</p></div>' +
          '<div class="dealality-process_moment"><strong>Clarification Call</strong><p>Questions get answered live before commercial terms harden.</p></div>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row is-human"><strong>People meet</strong><span>Calls · video · in person</span></div>' +
          '<div class="dealality-process_row"><strong>Dealality role</strong><span>Prepared · facilitated · outcome tracked</span></div>' +
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
          '<span class="dealality-process_meta">Prepares the next offline diligence</span>' +
          "</div>" +
          '<div class="dealality-process_compare" role="table" aria-label="Proposal differences">' +
          '<div class="is-head" role="columnheader">Category</div><div class="is-head" role="columnheader">Path A</div><div class="is-head" role="columnheader">Path B</div><div class="is-head" role="columnheader">Path C</div>' +
          "<div>Fees</div><div>4.5% + 2%</div><div>5.0% + 1%</div><div>3.8% + 3%</div>" +
          "<div>Owner Control</div><div>High</div><div>Medium</div><div>Shared</div>" +
          '<div>Missing Terms</div><div class="is-gap">Capex TBD</div><div>—</div><div class="is-gap">Exit TBD</div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "diligence") {
        return (
          '<div class="dealality-process_visual is-large is-offline-visual" aria-label="Offline human diligence">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Human Diligence</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Collaborate &amp; Review</span>' +
          '<span class="dealality-process_meta">Where judgment happens</span>' +
          "</div>" +
          '<div class="dealality-process_moments">' +
          '<div class="dealality-process_moment"><strong>Collaboration Calls</strong><p>Walk economics, support models, and strategic fit together—live.</p></div>' +
          '<div class="dealality-process_moment"><strong>Site / Tech Review</strong><p>Property walkthrough or remote technical review. Human diligence.</p></div>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row is-human"><strong>People meet</strong><span>Calls · site visit · tech review</span></div>' +
          '<div class="dealality-process_row"><strong>Dealality role</strong><span>Context ready · coverage tracked</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "pursue") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Selected direction workspace">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Preferred Direction · Decision Record</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Preferred Direction</span>' +
          '<span class="dealality-process_meta">Momentum between conversations</span>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Preferred Direction</strong><span>Soft brand + operator support</span></div>' +
          '<div class="dealality-process_row"><strong>Deal Room</strong><span>Open for finalists</span></div>' +
          '<div class="dealality-process_row"><strong>Feasibility Sync</strong><span>Docs + checklist tracked</span></div>' +
          '<div class="dealality-process_row"><strong>Next Step</strong><span>Final alignment call · offline</span></div>' +
          "</div></div></div>"
        );
      }
      return (
        '<div class="dealality-process_visual is-large is-offline-visual" aria-label="Offline align and hand off">' +
        '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Align &amp; Platform Exit</em></div>' +
        '<div class="dealality-process_panel">' +
        '<div class="dealality-process_panel-head">' +
        '<span class="dealality-process_chip">Align &amp; Hand Off</span>' +
        '<span class="dealality-process_meta">Where the deal closes</span>' +
        "</div>" +
        '<div class="dealality-process_moments">' +
        '<div class="dealality-process_moment"><strong>Final Alignment Call</strong><p>Vision, cost, and ops confirmed between people—last strategic discussion.</p></div>' +
        '<div class="dealality-process_moment"><strong>Legal Start</strong><p>MA / FA drafting and signature move offline. Clean platform exit.</p></div>' +
        "</div>" +
        '<div class="dealality-process_rows">' +
        '<div class="dealality-process_row is-human"><strong>People meet</strong><span>Alignment · counsel · signing path</span></div>' +
        '<div class="dealality-process_row"><strong>Dealality role</strong><span>LOI readiness tracked · clean handoff</span></div>' +
        "</div></div></div>"
      );
    }
    function stepHtml(step, i) {
      var offline = step.mode === "offline";
      return (
        '<article class="dealality-process_step' +
        (offline ? " is-offline-step" : "") +
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
        '<div class="dealality-process_mode-pill ' +
        (offline ? "is-offline" : "is-platform") +
        '"><i aria-hidden="true"></i>' +
        (offline
          ? "Offline · Facilitated by Dealality"
          : "In Platform · Structure") +
        "</div>" +
        '<p class="dealality-process_primary">' +
        esc(step.primary) +
        "</p>" +
        '<p class="dealality-process_support">' +
        esc(step.support) +
        "</p>" +
        '<p class="dealality-process_bridge"><strong>' +
        (offline ? "Where people meet:" : "Leads into:") +
        "</strong> " +
        esc(step.bridge) +
        "</p>" +
        visualHtml(step) +
        labelsHtml(step.labels) +
        "</article>"
      );
    }
    function buildSection() {
      var nav = STEPS.map(function (step, i) {
        var offline = step.mode === "offline";
        return (
          '<button type="button" class="dealality-process_nav-item' +
          (i === 0 ? " is-active" : "") +
          (offline ? " is-offline" : "") +
          '" data-dealality-process-nav="' +
          i +
          '" aria-controls="dealality-process-step-' +
          (i + 1) +
          '" aria-current="' +
          (i === 0 ? "step" : "false") +
          '" aria-label="' +
          esc(step.n) +
          " " +
          esc(step.title) +
          ", " +
          (offline ? "Offline" : "Platform") +
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
      return (
        '<section id="oh-how-we-do-it" class="dealality-process_section oh-how" aria-labelledby="dealality-process-h2" style="--oh-how-steps:' +
        STEPS.length +
        '" data-oh-how="31f">' +
        '<div class="dealality-process_glow" aria-hidden="true"></div>' +
        '<div class="dealality-process_inner">' +
        '<header class="dealality-process_intro">' +
        '<div class="dealality-process_eyebrow">' +
        '<span class="dealality-process_eyebrow-pill">How Dealality Works</span>' +
        '<span class="dealality-process_eyebrow-right">A Connected Process From Opportunity to Agreement.</span>' +
        "</div>" +
        '<h2 class="dealality-process_h2" id="dealality-process-h2">Turn One Hotel Opportunity Into a Structured Decision Process.</h2>' +
        '<p class="dealality-process_lead">Dealality organizes the opportunity on platform—and <em>facilitates the offline conversations that actually close deals</em>. People meet in person or on a call; Dealality keeps those moments prepared, tracked, and connected to the process.</p>' +
        '<div class="dealality-process_principle">' +
        '<span class="dealality-process_principle-mark" aria-hidden="true"></span>' +
        "<p><strong>People meet offline. Dealality stays with them.</strong> Introduction, diligence, and final alignment happen between people—prepared, facilitated, and tracked in the platform so nothing falls out of the process.</p>" +
        "</div>" +
        "</header>" +
        '<div class="dealality-process_stage-track" id="dealality-process-runway">' +
        '<div class="dealality-process_stage-pin">' +
        '<div class="dealality-process_layout">' +
        '<div class="dealality-process_nav-col">' +
        '<nav class="dealality-process_nav" aria-label="Dealality process steps">' +
        '<div class="dealality-process_nav-legend" aria-label="Step type legend">' +
        '<span class="dealality-process_nav-legend-item"><span class="dealality-process_nav-legend-swatch is-platform" aria-hidden="true"></span>Platform</span>' +
        '<span class="dealality-process_nav-legend-item"><span class="dealality-process_nav-legend-swatch is-offline" aria-hidden="true"></span>Offline</span>' +
        "</div>" +
        '<div class="dealality-process_nav-rail">' +
        '<div class="dealality-process_progress" aria-hidden="true"><span id="dealality-process-progress"></span></div>' +
        nav +
        "</div></nav></div>" +
        '<div class="dealality-process_content" id="dealality-process-content">' +
        STEPS.map(stepHtml).join("") +
        "</div>" +
        "</div></div></div>" +
        '<div class="dealality-process_cta">' +
        "<div>" +
        "<h3>One Opportunity. One Connected Process.</h3>" +
        "<p>From the first question to the selected direction, Dealality keeps the opportunity, participants, proposals, and decision criteria connected.</p>" +
        "</div>" +
        '<div class="dealality-process_cta-row">' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-primary" data-dealality-process-cta="explore">Explore Your Opportunity</button>' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-secondary" data-dealality-process-cta="demo">See Dealality in Action</button>' +
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
      var viewed = {};
      function setActive(index, fromUser) {
        if (index < 0 || index >= STEPS.length) return;
        active = index;
        navItems.forEach(function (btn, i) {
          var on = i === active;
          var offline = STEPS[i].mode === "offline";
          btn.classList.toggle("is-active", on);
          btn.classList.toggle("is-done", i < active);
          btn.classList.toggle("is-offline", offline);
          btn.setAttribute("aria-current", on ? "step" : "false");
        });
        steps.forEach(function (el, i) {
          var on = i === active;
          el.classList.toggle("is-active", on);
          if (on) el.removeAttribute("hidden");
          else el.setAttribute("hidden", "");
        });
        if (progress) {
          var pct =
            STEPS.length <= 1
              ? 100
              : Math.round(((active + 1) / STEPS.length) * 100);
          progress.style.height = pct + "%";
        }
        if (live) {
          live.textContent =
            "Step " +
            STEPS[active].n +
            ": " +
            STEPS[active].title +
            " (" +
            (STEPS[active].mode === "offline" ? "Offline" : "Platform") +
            ")";
        }
        if (!viewed[active]) {
          viewed[active] = 1;
          track("process_step_view", {
            step: STEPS[active].n,
            title: STEPS[active].title,
            mode: STEPS[active].mode,
          });
        }
        if (fromUser) {
          track("process_step_click", { step: STEPS[active].n, title: STEPS[active].title, mode: STEPS[active].mode, });
        }
      }
      navItems.forEach(function (btn) {
        btn.addEventListener("click", function () {
          var idx = parseInt(btn.getAttribute("data-dealality-process-nav"), 10);
          if (!isNaN(idx)) setActive(idx, true);
        });
      });
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
      var demoCta = section.querySelector('[data-dealality-process-cta="demo"]');
      if (demoCta) {
        demoCta.addEventListener("click", function () {
          track("process_cta_click", { cta: "demo" });
          if (typeof window.ohOpenRequestDemo === "function") {
            window.ohOpenRequestDemo();
            return;
          }
          var demoBtn =
            document.querySelector("[data-dealality-demo-open]") ||
            document.querySelector('a[href="#request-demo"]') ||
            document.querySelector("#fsw-demo-link");
          if (demoBtn) demoBtn.click();
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