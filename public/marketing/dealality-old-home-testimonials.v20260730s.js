/**
 * Old Home testimonials carousel (v20260730s)
 * Path-gated to /old-home.
 * - One auto-advance pass, then manual only
 * - 2 tiles visible per slide (SoT relies on #oh-tt)
 * - Four Joan Dejarden quotes; Owners quote focuses on What Owners Gain
 * - Labels top-left (top:24px), no punch-out; no name-row gradient on the label text
 * - Mid rule under label matches founder-name accent (soft line + purple glow)
 * - Name/title attribution centered under each quote
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohTestimonials >= 202607318) return;
  } catch (ePath) {
    return;
  }

  var JOAN =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518bded1ae16ef415eb_founder-joan-dejarden.png";
  var JOAN_ALT = "Joan Dejarden, Founder of Dealality";

  /* Slide 0 = Owners + Brands; slide 1 = Operators + Advisors */
  var CARDS = [
    {
      audience: "Owners",
      label: "For Owners",
      quote:
        "I built Dealality so owners can centralize the opportunity, engage the right participants, compare responses side by side, and keep credible alternatives in view — then move toward agreement without losing control.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      audience: "Brands",
      label: "For Brands",
      quote:
        "When owners organize criteria and project context first, brand teams stop drowning in incomplete outreach. Dealality brings structured opportunities — so development conversations start from fit, not cleanup.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      audience: "Operators",
      label: "For Operators",
      quote:
        "Owners needed operators evaluating fit from more than scattered teasers. Dealality's permission-based deal rooms and structured briefs help operators judge operating fit faster — while owners keep the process confidential and controlled.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      audience: "Advisors",
      label: "For Advisors",
      quote:
        "Owners gain more when counsel shapes the path before it is half-chosen. Dealality brings advisors into the workflow early enough to change the outcome — with optionality and a clear decision record still intact.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
  ];

  var ABOUT_LEAD =
    "Dealality was developed from the realities of hotel brand, operator, development, conversion, and strategic-partner decisions, informed by work with major global brands on growth strategies and the hospitality perspective of senior hospitality leaders.";

  var started = false;
  var retries = 0;

  function injectCss() {
    var prev = document.getElementById("oh-tt-audience");
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    var css = document.createElement("style");
    css.id = "oh-tt-audience";
    css.textContent = [
      "#testimonials-viewport,#testimonials-viewport>div[data-slide],#testimonials-viewport>div[data-slide]>div{",
      "overflow:visible!important}",
      "#testimonials-viewport article[data-oh-audience]{",
      "position:relative!important;overflow:visible!important;padding-top:64px!important;",
      "align-items:stretch!important;text-align:left!important}",
      "#testimonials-viewport .oh-tt-mid-rule{",
      "position:absolute!important;left:0!important;right:0!important;top:48px!important;",
      "height:0!important;margin:0!important;padding:0!important;border:0!important;",
      "background:transparent!important;pointer-events:none!important;z-index:4!important}",
      "#testimonials-viewport .oh-tt-mid-rule::before{",
      "content:\"\"!important;display:block!important;position:absolute!important;",
      "left:6%!important;right:6%!important;top:0!important;height:1px!important;",
      "background-image:linear-gradient(90deg,transparent,rgba(255,255,255,.28) 50%,transparent)!important;",
      "opacity:1!important}",
      "#testimonials-viewport .oh-tt-mid-rule::after{",
      "content:\"\"!important;display:block!important;position:absolute!important;",
      "left:50%!important;top:-1.5px!important;transform:translateX(-50%)!important;",
      "width:90px!important;height:4px!important;border-radius:70px!important;",
      "background-image:linear-gradient(90deg,transparent,#6C72FF 50%,transparent)!important;",
      "filter:blur(1.5px)!important;opacity:1!important}",
      "#testimonials-viewport .oh-tt-audience{",
      "position:absolute!important;top:24px!important;left:24px!important;right:auto!important;",
      "display:inline-block!important;visibility:visible!important;opacity:1!important;",
      "width:auto!important;max-width:calc(100% - 48px)!important;",
      "box-sizing:border-box!important;z-index:5!important;margin:0!important;",
      "padding:0!important;border:0!important;background:transparent!important;",
      "color:#D78E2C!important;font-size:12px!important;font-weight:700!important;letter-spacing:.08em!important;",
      "text-transform:uppercase!important;line-height:1.2!important;white-space:nowrap!important;",
      "text-align:left!important;flex-shrink:0!important;pointer-events:none!important;",
      "overflow:visible!important;text-overflow:clip!important}",
      "#testimonials-viewport .oh-tt-audience::before,",
      "#testimonials-viewport .oh-tt-audience::after{",
      "content:none!important;display:none!important;opacity:0!important;",
      "background:none!important;background-image:none!important;filter:none!important;",
      "box-shadow:none!important;width:0!important;height:0!important}",
      "#testimonials-viewport article img{margin:0 auto 10px!important;align-self:center!important}",
      "#testimonials-viewport blockquote{",
      "max-height:8.6em!important;-webkit-line-clamp:5!important;font-size:.9rem!important;",
      "text-align:left!important;align-self:stretch!important;width:100%!important}",
      "#testimonials-viewport article p:not(.oh-tt-audience){text-align:center!important;align-self:center!important;width:100%!important}",
      "#testimonials-dots button.oh-testimonial-dot{",
      "appearance:none;border:0;width:8px;height:8px;border-radius:999px;margin:0 4px;padding:0;",
      "background:rgba(255,255,255,.28);cursor:pointer}",
      "#testimonials-dots button.oh-testimonial-dot.is-active{background:#D78E2C}",
      "@media(max-width:900px){#testimonials-viewport blockquote{max-height:none!important;-webkit-line-clamp:unset!important;display:block!important}}",
    ].join("");
    document.head.appendChild(css);
  }

  function ensureAudienceLabel(article, card) {
    var label = article.querySelector(".oh-tt-audience");
    if (!label) {
      label = document.createElement("p");
      label.className = "oh-tt-audience";
      article.insertBefore(label, article.firstChild);
    }
    label.textContent = card.label;
    label.setAttribute("aria-hidden", "true");
    var mid = article.querySelector(".oh-tt-mid-rule");
    if (!mid) {
      mid = document.createElement("span");
      mid.className = "oh-tt-mid-rule";
      mid.setAttribute("aria-hidden", "true");
      if (label.nextSibling) article.insertBefore(mid, label.nextSibling);
      else article.appendChild(mid);
    }
    article.setAttribute("data-oh-audience", card.audience.toLowerCase());
    article.setAttribute("aria-label", card.label + " — quote from " + card.name);
  }

  function applyLead() {
    var lead = document.getElementById("testimonials-lead");
    if (lead) lead.textContent = ABOUT_LEAD;
  }

  function applyCopy(root) {
    applyLead();
    var articles = root.querySelectorAll("#testimonials-viewport article");
    articles.forEach(function (article, i) {
      var card = CARDS[i];
      if (!card) return;
      ensureAudienceLabel(article, card);
      var bq = article.querySelector("blockquote");
      var p = article.querySelector("p:not(.oh-tt-audience)");
      var img = article.querySelector("img");
      if (bq) bq.textContent = '"' + card.quote + '"';
      if (p) {
        p.innerHTML = "<strong>" + card.name + "</strong> — " + card.title;
      }
      if (img) {
        img.src = card.img;
        img.alt = card.alt || "";
      }
    });
  }

  function start(root, viewport, slides, dotsWrap) {
    if (started) return;
    started = true;
    window.__ohTestimonials = 202607318;

    var index = 0;
    var timer = null;
    var autoplayDone = false;
    var seen = Object.create(null);
    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function stopAutoplay() {
      if (timer) window.clearInterval(timer);
      timer = null;
      autoplayDone = true;
    }

    function markSeen(i) {
      seen[i] = true;
    }

    function allSlidesSeen() {
      return Object.keys(seen).length >= slides.length;
    }

    function setSlide(next) {
      index = (next + slides.length) % slides.length;
      markSeen(index);
      slides.forEach(function (slide, i) {
        slide.classList.toggle("is-active", i === index);
        slide.setAttribute("aria-hidden", i === index ? "false" : "true");
      });
      dotsWrap.querySelectorAll("button").forEach(function (dot, i) {
        dot.classList.toggle("is-active", i === index);
        dot.setAttribute("aria-selected", i === index ? "true" : "false");
      });
      /* Re-assert labels after slide changes (Webflow/interactions can clobber DOM). */
      applyCopy(root);
    }

    if (!dotsWrap.querySelector("button")) {
      var slideLabels = ["Owners & Brands", "Operators & Advisors"];
      slides.forEach(function (_, i) {
        var dot = document.createElement("button");
        dot.type = "button";
        dot.className = "oh-testimonial-dot" + (i === 0 ? " is-active" : "");
        dot.setAttribute(
          "aria-label",
          "Show " + (slideLabels[i] || "testimonial slide " + (i + 1))
        );
        dot.setAttribute("aria-selected", i === 0 ? "true" : "false");
        dot.addEventListener("click", function () {
          stopAutoplay();
          setSlide(i);
        });
        dotsWrap.appendChild(dot);
      });
    } else {
      dotsWrap.querySelectorAll("button").forEach(function (dot, i) {
        dot.addEventListener("click", function () {
          stopAutoplay();
          setSlide(i);
        });
      });
    }

    function startAutoplayOnce() {
      if (timer) window.clearInterval(timer);
      timer = null;
      if (reduced || autoplayDone || slides.length < 2) return;
      markSeen(index);
      if (allSlidesSeen()) {
        autoplayDone = true;
        return;
      }
      timer = window.setInterval(function () {
        setSlide(index + 1);
        if (allSlidesSeen()) stopAutoplay();
      }, 7000);
    }

    injectCss();
    applyCopy(root);
    setSlide(0);
    startAutoplayOnce();

    /* Re-apply after paint / late Webflow CSS so labels are not lost. */
    window.requestAnimationFrame(function () {
      injectCss();
      applyCopy(root);
    });
    window.setTimeout(function () {
      injectCss();
      applyCopy(root);
    }, 120);
    window.setTimeout(function () {
      injectCss();
      applyCopy(root);
    }, 600);
  }

  function tryBoot() {
    if (started || window.__ohTestimonials >= 202607318) return;
    var root = document.getElementById("testimonials") || document.getElementById("trust");
    if (!root) {
      scheduleRetry();
      return;
    }
    var viewport = root.querySelector("#testimonials-viewport");
    var slides = root.querySelectorAll("#testimonials-viewport > div[data-slide]");
    var dotsWrap = root.querySelector("#testimonials-dots");
    if (!viewport || !slides.length || !dotsWrap) {
      scheduleRetry();
      return;
    }
    start(root, viewport, slides, dotsWrap);
  }

  function scheduleRetry() {
    if (retries >= 40) return;
    retries += 1;
    window.setTimeout(tryBoot, 50);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", tryBoot, { once: true });
  } else {
    tryBoot();
  }
  /* Boot guard may inject this script before body exists; keep retrying. */
  scheduleRetry();
})();
