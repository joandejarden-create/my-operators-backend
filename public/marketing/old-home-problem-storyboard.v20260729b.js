/**
 * Old Home Problem — synchronized animated storyboard (v20260729a)
 * Path-gated to /old-home. Replaces old-home-problem-v2 visual.
 */
(function () {
  try {
    var PATH = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (PATH !== "/old-home") return;
    if (window.__ohProblemStoryboard >= 202607292) return;
    window.__ohProblemStoryboard = 202607292;

    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a774df6520a2f78161f69_old-home-problem-storyboard.v20260729b.css";
    var ACCENT = "#6C72FF";
    var SESSION_KEY = "oh_problem_storyboard_played_v1";
    var TOTAL_MS = 8200;
    var FRAME_AT = [0, 1100, 2300, 3700, 5300, 6700];
    var PANEL_BY_FRAME = [0, 1, 1, 2, 3, 3];

    var icons = {
      1:
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="12" cy="14" r="3.5" stroke="' +
        ACCENT +
        '" stroke-width="1.5"/><circle cx="28" cy="14" r="3.5" stroke="' +
        ACCENT +
        '" stroke-width="1.5"/><circle cx="20" cy="28" r="3.5" stroke="' +
        ACCENT +
        '" stroke-width="1.5"/><path d="M15 15.5l8-1M25.5 16.5l-4.5 8M14.5 16.5l4.5 8" stroke="' +
        ACCENT +
        '" stroke-width="1.2" stroke-linecap="round" opacity=".55"/></svg>',
      2:
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="6" y="10" width="11" height="20" rx="2" stroke="' +
        ACCENT +
        '" stroke-width="1.5"/><rect x="23" y="10" width="11" height="20" rx="2" stroke="' +
        ACCENT +
        '" stroke-width="1.5"/><path d="M9 15h5M9 19h5M9 23h4M26 15h5M26 19h3M26 23h5" stroke="' +
        ACCENT +
        '" stroke-width="1.2" stroke-linecap="round" opacity=".55"/></svg>',
      3:
        '<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="20" cy="20" r="12" stroke="' +
        ACCENT +
        '" stroke-width="1.5" stroke-dasharray="3 3" opacity=".55"/><circle cx="20" cy="20" r="3" fill="' +
        ACCENT +
        '" opacity=".55"/><path d="M27 11l5-5M32 6v4M32 6h-4" stroke="' +
        ACCENT +
        '" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    };

    var cards = [
      {
        id: "about-point-1",
        title: "Fragmented outreach",
        body: "The same hotel story is repeated across separate brand, operator, advisor, and capital conversations.",
        icon: icons[1],
      },
      {
        id: "about-point-2",
        title: "Slower comparison",
        body: "Responses return in different formats, with different assumptions and missing information.",
        icon: icons[2],
      },
      {
        id: "about-point-3",
        title: "Missed upside",
        body: "The first path gains momentum while other credible ways to create value remain untested.",
        icon: icons[3],
      },
    ];

    var SR_SUMMARY =
      "Most hotel owners do not lack options. They lack a good way to compare them. " +
      "One hotel opportunity is evaluated through separate brand, operator, advisor, and capital conversations. " +
      "Artefacts scatter across emails, decks, spreadsheets, calls, notes, and PDFs. " +
      "Different information, assumptions, terminology, and formats make options hard to compare fairly. " +
      "The first path starts shaping the decision while other paths remain partial, unconfirmed, or not yet tested. " +
      "Potential value stays hidden — not because the hotel lacked options, but because the options were never evaluated in one shared process.";

    var timers = [];
    var observer = null;
    var played = false;
    var running = false;
    var root = null;
    var stage = null;
    var panels = [];
    var reduced =
      typeof matchMedia === "function" &&
      matchMedia("(prefers-reduced-motion: reduce)").matches;

    function track(name) {
      try {
        window.dataLayer = window.dataLayer || [];
        window.dataLayer.push({ event: name });
      } catch (e) {}
      try {
        if (typeof window.gtag === "function") {
          window.gtag("event", name, { event_category: "old_home_problem" });
        }
      } catch (e2) {}
    }

    function pinCss() {
      if (document.querySelector('link[data-oh-problem-sb-css="1"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-problem-sb-css", "1");
      document.head.appendChild(link);
    }

    function clearTimers() {
      timers.forEach(function (id) {
        clearTimeout(id);
      });
      timers = [];
    }

    function setPanel(activeIdx) {
      panels.forEach(function (el, i) {
        var n = i + 1;
        el.classList.toggle("is-active", n === activeIdx);
        el.classList.toggle("is-done", activeIdx > 0 && n < activeIdx);
        if (!activeIdx) {
          el.classList.remove("is-active", "is-done");
        }
      });
    }

    function setFrame(n) {
      if (!stage) return;
      stage.setAttribute("data-frame", String(n));
      setPanel(PANEL_BY_FRAME[n - 1] || 0);
    }

    function markComplete() {
      if (!stage) return;
      stage.classList.add("is-complete");
      running = false;
      played = true;
      try {
        sessionStorage.setItem(SESSION_KEY, "1");
      } catch (e) {}
      track("problem_storyboard_complete");
    }

    function runTimeline(fromReplay) {
      if (!stage || running) return;
      running = true;
      stage.classList.remove("is-complete", "is-reduced");
      clearTimers();
      setFrame(1);
      if (fromReplay) track("problem_storyboard_replay");

      if (reduced) {
        stage.classList.add("is-reduced", "is-complete");
        setFrame(6);
        setPanel(3);
        panels.forEach(function (el) {
          el.classList.add("is-done");
        });
        panels[2] && panels[2].classList.add("is-active");
        running = false;
        played = true;
        track("problem_storyboard_complete");
        return;
      }

      FRAME_AT.forEach(function (ms, idx) {
        var frame = idx + 1;
        timers.push(
          setTimeout(function () {
            setFrame(frame);
          }, ms)
        );
      });
      timers.push(
        setTimeout(function () {
          markComplete();
        }, TOTAL_MS)
      );
    }

    function buildStoryboardHtml() {
      return (
        '<div class="oh-problem-sb" data-frame="1" role="img" aria-label="Animated storyboard of fragmented hotel opportunity evaluation">' +
        '<p class="oh-problem-sr">' +
        SR_SUMMARY +
        "</p>" +
        '<p class="oh-problem-sb-eyebrow">How it usually happens today</p>' +
        '<div class="oh-problem-sb-stage">' +
        '<div class="oh-problem-sb-layer oh-problem-sb-l1">' +
        '<div class="oh-problem-node">One hotel opportunity</div>' +
        '<div class="oh-problem-meta" aria-hidden="true">' +
        "<span>Owner objectives</span><span>Asset information</span><span>Market context</span><span>Decision criteria</span>" +
        "</div></div>" +
        '<div class="oh-problem-sb-layer oh-problem-sb-l2"><div class="oh-problem-conv" aria-hidden="true">' +
        '<div class="oh-problem-node">One hotel opportunity</div>' +
        '<span class="oh-problem-chip is-channel oh-problem-c1">Brand conversation</span>' +
        '<span class="oh-problem-chip is-channel oh-problem-c2">Operator introduction</span>' +
        '<span class="oh-problem-chip is-channel oh-problem-c3">Advisor recommendation</span>' +
        '<span class="oh-problem-chip is-channel oh-problem-c4">Capital discussion</span>' +
        "</div></div>" +
        '<div class="oh-problem-sb-layer oh-problem-sb-l3"><div class="oh-problem-conv" aria-hidden="true">' +
        '<div class="oh-problem-node">One hotel opportunity</div>' +
        '<span class="oh-problem-chip is-channel oh-problem-c1">Brand conversation</span>' +
        '<span class="oh-problem-chip is-channel oh-problem-c2">Operator introduction</span>' +
        '<span class="oh-problem-chip is-channel oh-problem-c3">Advisor recommendation</span>' +
        '<span class="oh-problem-chip is-channel oh-problem-c4">Capital discussion</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a1">Proposal in email</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a2">Slide deck</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a3">Terms in spreadsheet</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a4">Questions in calls</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a5">Advisor notes</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a6">Documents in PDFs</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a7">Follow-up requests</span>' +
        '<span class="oh-problem-chip is-artefact oh-problem-a8">Revised attachments</span>' +
        "</div></div>" +
        '<div class="oh-problem-sb-layer oh-problem-sb-l4">' +
        '<p class="oh-problem-fail-title">Hard to compare fairly</p>' +
        '<div class="oh-problem-fail-tags" aria-hidden="true">' +
        "<span>Different information</span><span>Different assumptions</span><span>Different terminology</span><span>Different formats</span><span>Missing fields</span>" +
        "</div>" +
        '<div class="oh-problem-grid" aria-hidden="true">' +
        '<div class="oh-problem-cell"><strong>Fees</strong><em>Mixed</em></div>' +
        '<div class="oh-problem-cell is-mismatch"><strong>Term</strong><em>Unaligned</em></div>' +
        '<div class="oh-problem-cell is-miss"><strong>Capital support</strong><em>Incomplete</em></div>' +
        '<div class="oh-problem-cell is-mismatch"><strong>Owner control</strong><em>Unclear</em></div>' +
        '<div class="oh-problem-cell"><strong>Standards</strong><em>Partial</em></div>' +
        '<div class="oh-problem-cell is-miss"><strong>Timing</strong><em>Missing</em></div>' +
        "</div></div>" +
        '<div class="oh-problem-sb-layer oh-problem-sb-l5">' +
        '<p class="oh-problem-mom-lead">The first path starts shaping the decision.</p>' +
        '<div class="oh-problem-paths" aria-hidden="true">' +
        '<div class="oh-problem-path is-lead"><strong>First available path</strong><span>Gaining momentum</span></div>' +
        '<div class="oh-problem-path"><strong>Alternative structure</strong><span>Partial</span></div>' +
        '<div class="oh-problem-path"><strong>Second brand option</strong><span>Unconfirmed</span></div>' +
        '<div class="oh-problem-path"><strong>Value-creation path</strong><span>Not yet tested</span></div>' +
        "</div></div>" +
        '<div class="oh-problem-sb-layer oh-problem-sb-l6">' +
        '<p class="oh-problem-final-title">Potential value stays hidden.</p>' +
        '<p class="oh-problem-final-body">Not because the hotel lacked options — because the options were never evaluated in one shared process.</p>' +
        "</div>" +
        "</div>" +
        '<button type="button" class="oh-problem-replay" aria-label="Replay problem storyboard">' +
        '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M7.5 7.5A6.5 6.5 0 1 1 6 12" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/><path d="M7.5 4.5v4h4" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>' +
        "</button></div>"
      );
    }

    function applyCopy() {
      var h2 = document.getElementById("about-h2");
      if (h2) {
        h2.innerHTML =
          "Most hotel owners do not lack options.<br>They lack a good way to compare them.";
      }
      var lead = document.getElementById("about-lead");
      if (lead) {
        lead.textContent =
          "Hotel opportunities are still evaluated across emails, slide decks, spreadsheets, calls, and separate advisor conversations. Different parties receive different information, respond in different formats, and use different assumptions. That makes the process slower, the options harder to compare, and the full potential of the asset easier to miss.";
      }
      var lead2 = document.getElementById("about-lead-2");
      if (lead2) lead2.setAttribute("hidden", "");
      var close = document.getElementById("about-close");
      if (close) close.setAttribute("hidden", "");

      cards.forEach(function (card) {
        var li = document.getElementById(card.id);
        if (!li) return;
        li.classList.add("oh-about-point", "oh-problem-panel");
        var iconId = card.id + "-icon";
        var existing = document.getElementById(iconId);
        if (!existing) {
          existing = document.createElement("div");
          existing.id = iconId;
          existing.className = "about-point-icon mod-icon";
          existing.setAttribute("aria-hidden", "true");
          li.insertBefore(existing, li.firstChild);
        }
        existing.classList.add("about-point-icon", "mod-icon");
        existing.innerHTML = card.icon;
        var strong = li.querySelector("strong");
        var span = li.querySelector("span");
        if (strong) strong.textContent = card.title;
        if (span) span.textContent = card.body;
      });
    }

    function init() {
      root = document.getElementById("about");
      if (!root) return;
      if (root.getAttribute("data-oh-problem-sb") === "1") return;
      root.setAttribute("data-oh-problem-sb", "1");
      root.setAttribute("data-oh-problem-v2", "1");
      root.classList.add("oh-problem-ready");

      pinCss();
      applyCopy();

      var visual = document.getElementById("about-visual");
      if (!visual) return;
      visual.setAttribute(
        "aria-label",
        "Fragmented evaluation storyboard"
      );
      visual.innerHTML = buildStoryboardHtml();
      stage = visual.querySelector(".oh-problem-sb");
      panels = cards
        .map(function (c) {
          return document.getElementById(c.id);
        })
        .filter(Boolean);

      var replay = visual.querySelector(".oh-problem-replay");
      if (replay) {
        replay.addEventListener("click", function () {
          clearTimers();
          running = false;
          runTimeline(true);
        });
      }

      var already = false;
      try {
        already = sessionStorage.getItem(SESSION_KEY) === "1";
      } catch (e) {}

      if (reduced) {
        stage.classList.add("is-reduced", "is-complete");
        setFrame(6);
        setPanel(3);
        panels.forEach(function (el) {
          el.classList.add("is-done");
        });
        if (panels[2]) panels[2].classList.add("is-active");
        track("problem_storyboard_view");
        track("problem_storyboard_complete");
        played = true;
        return;
      }

      if (already) {
        setFrame(6);
        setPanel(3);
        panels.forEach(function (el) {
          el.classList.add("is-done");
        });
        if (panels[2]) panels[2].classList.add("is-active");
        stage.classList.add("is-complete");
        played = true;
        return;
      }

      if (typeof IntersectionObserver === "undefined") {
        track("problem_storyboard_view");
        runTimeline(false);
        return;
      }

      observer = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (!entry.isIntersecting || played || running) return;
            if (entry.intersectionRatio < 0.35) return;
            track("problem_storyboard_view");
            runTimeline(false);
            if (observer) {
              observer.disconnect();
              observer = null;
            }
          });
        },
        { threshold: [0.35, 0.5, 0.65] }
      );
      observer.observe(root);

      window.addEventListener(
        "pagehide",
        function () {
          clearTimers();
          if (observer) {
            try {
              observer.disconnect();
            } catch (e) {}
            observer = null;
          }
        },
        { once: true }
      );
    }

    pinCss();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", init, { once: true });
    } else {
      init();
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-problem-storyboard]", err);
    }
  }
})();
