/**
 * Old Home — How We Do It scroll-led process story (v20260730b)
 * Path-gated to /old-home. Replaces/upgrades 30a Gobiz timeline with brief copy.
 * Namespace: dealality-process_*
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHowWeDoIt >= 202607302) return;
    window.__ohHowWeDoIt = 202607302;

    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a96eaa0215af88895a6d2_dealality-old-home-how-we-do-it.v20260730b.css";
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

    function ensureCss() {
      var existing = document.querySelector(
        'link[href*="dealality-old-home-how-we-do-it"]'
      );
      if (existing) {
        existing.setAttribute("href", CSS_HREF);
        existing.setAttribute("data-oh-how", "30b");
        return;
      }
      if (document.querySelector('link[data-oh-how="30b"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-how", "30b");
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
          '<div class="dealality-process_visual" aria-label="Opportunity brief panel">' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Opportunity Brief</span>' +
          '<span class="dealality-process_meta">Demo · Coastal Boutique Hotel</span>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Hotel Summary</strong><span>118 keys · Upscale boutique</span></div>' +
          '<div class="dealality-process_row"><strong>Location</strong><span>Greater Santo Domingo</span></div>' +
          '<div class="dealality-process_row"><strong>Opportunity Type</strong><span>Brand + operator review</span></div>' +
          '<div class="dealality-process_row"><strong>Owner Objectives</strong><span>Stabilize NOI · Preserve control</span></div>' +
          '<div class="dealality-process_row"><strong>Constraints</strong><span>Capex limited · 18-month horizon</span></div>' +
          '<div class="dealality-process_row"><strong>Current Stage</strong><span>Define opportunity</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "paths") {
        return (
          '<div class="dealality-process_visual" aria-label="Credible strategic paths">' +
          (step.img
            ? '<img src="' +
              esc(step.img) +
              '" alt="' +
              esc(step.alt) +
              '" loading="lazy" width="960" height="540">'
            : "") +
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
          '<div class="dealality-process_visual" aria-label="Prepare and engage workspace">' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Controlled Outreach</span>' +
          '<span class="dealality-process_meta">One opportunity · one consistent story</span>' +
          "</div>" +
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
          '<div class="dealality-process_visual is-compare" aria-label="Proposal comparison">' +
          (step.img
            ? '<img src="' +
              esc(step.img) +
              '" alt="' +
              esc(step.alt) +
              '" loading="lazy" width="960" height="540">'
            : "") +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Shared Comparison</span>' +
          '<span class="dealality-process_meta">Economics · control · gaps</span>' +
          "</div>" +
          '<div class="dealality-process_compare" role="table" aria-label="Proposal differences">' +
          '<div class="is-head" role="columnheader">Category</div><div class="is-head" role="columnheader">Path A</div><div class="is-head" role="columnheader">Path B</div><div class="is-head" role="columnheader">Path C</div>' +
          "<div>Fees</div><div>4.5% + 2%</div><div>5.0% + 1%</div><div>3.8% + 3%</div>" +
          "<div>Owner Control</div><div>High</div><div>Medium</div><div>Shared</div>" +
          '<div>Missing Terms</div><div class="is-gap">Capex TBD</div><div>—</div><div class="is-gap">Exit TBD</div>' +
          "<div>Timing</div><div>9–12 mo</div><div>6–9 mo</div><div>12–18 mo</div>" +
          "</div></div></div>"
        );
      }
      return (
        '<div class="dealality-process_visual" aria-label="Selected direction workspace">' +
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
        '"' +
        (i === 0 ? "" : ' hidden') +
        ">" +
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

      return (
        '<section id="oh-how-we-do-it" class="dealality-process_section oh-how" aria-labelledby="dealality-process-h2" style="--oh-how-steps:' +
        STEPS.length +
        '" data-oh-how="30b">' +
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
        '<div class="dealality-process_runway" id="dealality-process-runway">' +
        '<div class="dealality-process_layout">' +
        '<nav class="dealality-process_nav" aria-label="Dealality process steps">' +
        '<div class="dealality-process_progress" aria-hidden="true"><span id="dealality-process-progress"></span></div>' +
        nav +
        "</nav>" +
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
      var features = document.getElementById("features");
      if (!features || !features.parentNode) return false;
      var holder = document.createElement("div");
      holder.innerHTML = html;
      features.parentNode.insertBefore(holder.firstChild, features.nextSibling);
      return true;
    }

    function bind(section) {
      if (!section || section.getAttribute("data-dealality-process-bound") === "1")
        return;
      section.setAttribute("data-dealality-process-bound", "1");

      var runway = section.querySelector("#dealality-process-runway");
      var navItems = Array.prototype.slice.call(
        section.querySelectorAll("[data-dealality-process-nav]")
      );
      var steps = Array.prototype.slice.call(
        section.querySelectorAll("[data-dealality-process-step]")
      );
      var progress = section.querySelector("#dealality-process-progress");
      var live = section.querySelector("#dealality-process-live");
      var active = 0;
      var mq = window.matchMedia("(max-width: 960px)");
      var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
      var viewed = {};

      function setActive(index, fromUser) {
        if (index < 0 || index >= STEPS.length) return;
        active = index;
        navItems.forEach(function (btn, i) {
          var on = i === active;
          btn.classList.toggle("is-active", on);
          btn.classList.toggle("is-done", i < active);
          btn.setAttribute("aria-current", on ? "step" : "false");
        });
        if (mq.matches || reduceMotion.matches) {
          steps.forEach(function (el) {
            el.classList.add("is-active");
            el.removeAttribute("hidden");
          });
        } else {
          steps.forEach(function (el, i) {
            var on = i === active;
            el.classList.toggle("is-active", on);
            if (on) el.removeAttribute("hidden");
            else el.setAttribute("hidden", "");
          });
        }
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
        if (!runway || mq.matches || reduceMotion.matches) {
          setActive(index, true);
          var el = steps[index];
          if (el && typeof el.scrollIntoView === "function") {
            el.scrollIntoView({ behavior: "smooth", block: "start" });
          }
          return;
        }
        var rect = runway.getBoundingClientRect();
        var top = window.pageYOffset + rect.top;
        var usable = Math.max(1, runway.offsetHeight - window.innerHeight * 0.35);
        var target = top + (index / Math.max(1, STEPS.length - 1)) * usable;
        window.scrollTo({ top: target, behavior: "smooth" });
        setActive(index, true);
      }

      navItems.forEach(function (btn) {
        btn.addEventListener("click", function () {
          var idx = parseInt(btn.getAttribute("data-dealality-process-nav"), 10);
          if (!isNaN(idx)) scrollToStep(idx);
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

      function syncFromScroll() {
        if (mq.matches || reduceMotion.matches) {
          setActive(active, false);
          return;
        }
        if (!runway) return;
        var rect = runway.getBoundingClientRect();
        var vh = window.innerHeight || 800;
        var total = Math.max(1, runway.offsetHeight - vh * 0.35);
        var scrolled = Math.min(total, Math.max(0, -rect.top + vh * 0.2));
        var ratio = scrolled / total;
        var idx = Math.min(
          STEPS.length - 1,
          Math.max(0, Math.round(ratio * (STEPS.length - 1)))
        );
        if (progress) progress.style.height = Math.round(ratio * 100) + "%";
        if (idx !== active) setActive(idx, false);
      }

      var raf = 0;
      function onScroll() {
        if (raf) return;
        raf = window.requestAnimationFrame(function () {
          raf = 0;
          syncFromScroll();
        });
      }

      window.addEventListener("scroll", onScroll, { passive: true });
      window.addEventListener("resize", onScroll);
      if (typeof mq.addEventListener === "function") {
        mq.addEventListener("change", function () {
          setActive(active, false);
          syncFromScroll();
        });
      }

      setActive(0, false);
      syncFromScroll();
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
