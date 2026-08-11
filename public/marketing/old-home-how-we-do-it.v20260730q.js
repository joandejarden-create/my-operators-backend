/**
 * Old Home — How We Do It compact one-screen process (v20260730q)
 * Path-gated to /old-home.
 * Click left nav to swap the right step in place. No scroll runway.
 * Locks right panel height (CSS var) so CTA stays put. Does not alter mockup images.
 * Namespace: dealality-process_*
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b5d5b9f2451383a2633f8_dealality-old-home-how-we-do-it.v20260730q.css";
    if (window.__ohHowWeDoIt >= 202607319) return;
    window.__ohHowWeDoIt = 202607319;
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

    function ensureCss(onReady) {
      function ready() {
        if (typeof onReady === "function") onReady();
      }
      var existing = document.querySelector(
        'link[href*="dealality-old-home-how-we-do-it"]'
      );
      if (existing) {
        var same = (existing.getAttribute("href") || "").indexOf("v20260730q.css") !== -1;
        existing.setAttribute("href", CSS_HREF);
        existing.setAttribute("data-oh-how", "30q");
        if (same) ready();
        else {
          existing.addEventListener("load", ready, { once: true });
          existing.addEventListener("error", ready, { once: true });
          // Fallback if cached stylesheet does not fire load
          setTimeout(ready, 50);
        }
        return;
      }
      if (document.querySelector('link[data-oh-how="30q"]')) {
        ready();
        return;
      }
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-how", "30q");
      link.addEventListener("load", ready, { once: true });
      link.addEventListener("error", ready, { once: true });
      (document.head || document.documentElement).appendChild(link);
      setTimeout(ready, 120);
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
              '" loading="eager" width="960" height="96">'
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
              '" loading="eager" width="960" height="110">'
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

      return (
        '<section id="oh-how-we-do-it" class="dealality-process_section oh-how" aria-labelledby="dealality-process-h2" style="--oh-how-steps:' +
        STEPS.length +
        '" data-oh-how="30q">' +
        '<div class="dealality-process_glow" aria-hidden="true"></div>' +
        '<div class="dealality-process_inner">' +
        '<header class="dealality-process_intro">' +
        '<div class="dealality-process_eyebrow">' +
        '<span class="dealality-process_eyebrow-pill">How Dealality Works</span>' +
        '<span class="dealality-process_eyebrow-right">A Connected Process From Opportunity to Agreement.</span>' +
        "</div>" +
        '<h2 class="dealality-process_h2" id="dealality-process-h2">Turn One Hotel Opportunity Into a Structured Decision Process.</h2>' +
        '<p class="dealality-process_lead">Dealality brings the opportunity, credible strategic paths, market engagement, responses, and decision criteria into one confidential process—helping owners explore more possibilities without losing control or momentum.</p>' +
        "</header>" +
        '<div class="dealality-process_stage-track" id="dealality-process-runway">' +
        '<div class="dealality-process_stage-pin">' +
        '<div class="dealality-process_layout">' +
        '<div class="dealality-process_nav-col">' +
        '<nav class="dealality-process_nav" aria-label="Dealality process steps">' +
        '<div class="dealality-process_progress" aria-hidden="true"><span id="dealality-process-progress"></span></div>' +
        nav +
        "</nav></div>" +
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
      if (existing && existing.getAttribute("data-oh-how") === "30q") return true;
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

      var content = section.querySelector("#dealality-process-content");
      var heightLockPending = false;

      function lockPanelHeight() {
        if (!content || !steps.length) return;
        var prevActive = active;
        // Measure natural tallest step only — no extra empty room below it
        content.style.removeProperty("--oh-process-content-h");
        content.classList.add("is-measuring");
        var pad =
          (parseFloat(getComputedStyle(content).paddingTop) || 0) +
          (parseFloat(getComputedStyle(content).paddingBottom) || 0);
        var max = 0;
        for (var i = 0; i < steps.length; i++) {
          steps.forEach(function (el, j) {
            var on = j === i;
            el.classList.toggle("is-active", on);
            if (on) el.removeAttribute("hidden");
            else el.setAttribute("hidden", "");
          });
          // Use the active step's box + content padding (avoids inflated scrollHeight)
          var stepEl = steps[i];
          var h = Math.ceil((stepEl ? stepEl.offsetHeight : 0) + pad);
          if (!h) h = content.scrollHeight;
          if (h > max) max = h;
        }
        steps.forEach(function (el, j) {
          var on = j === prevActive;
          el.classList.toggle("is-active", on);
          if (on) el.removeAttribute("hidden");
          else el.setAttribute("hidden", "");
        });
        active = prevActive;
        content.classList.remove("is-measuring");
        if (max > 0) {
          content.style.setProperty("--oh-process-content-h", max + "px");
        }
      }

      function scheduleHeightLock() {
        if (heightLockPending) return;
        heightLockPending = true;
        requestAnimationFrame(function () {
          requestAnimationFrame(function () {
            heightLockPending = false;
            lockPanelHeight();
          });
        });
      }

      setActive(0, false);
      scheduleHeightLock();
      Array.prototype.forEach.call(content ? content.querySelectorAll("img") : [], function (img) {
        if (img.complete) return;
        img.addEventListener("load", scheduleHeightLock, { once: true });
        img.addEventListener("error", scheduleHeightLock, { once: true });
      });
      // Remeasure after CSS + late layout so we do not keep oversized empty space
      ensureCss(scheduleHeightLock);
      setTimeout(scheduleHeightLock, 200);
      setTimeout(scheduleHeightLock, 600);
      window.addEventListener("resize", function () {
        clearTimeout(section._ohHeightTimer);
        section._ohHeightTimer = setTimeout(scheduleHeightLock, 120);
      });
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
