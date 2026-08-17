/**
 * Old Home testimonials carousel (v20260801a)
 * Path-gated to / and /old-home (homepage cutover).
 * - First time section enters viewport: auto-advance twice, then manual only
 * - 2 tiles visible per slide (SoT relies on #oh-tt)
 * - Joan + Justin Boutwell + Gustavo Sarago (Operators) + Joan (Advisors)
 * - No "For Owners / For Brands / …" audience labels
 * - Name/title attribution centered under each quote
 * - 01a: scroll-triggered autoplay ×2; root #trust|#testimonials
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home") return;
    if (window.__ohTestimonials >= 202608011) return;
  } catch (ePath) {
    return;
  }

  var JOAN =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518bded1ae16ef415eb_founder-joan-dejarden.png";
  var JOAN_ALT = "Joan Dejarden, Founder of Dealality";
  var JUSTIN =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cabdcddb9933257d89126_justin-boutwell-avatar.v20260731a.jpg";
  var JUSTIN_ALT = "Justin Boutwell, Lead Asset Manager";
  var GUSTAVO =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d0a1ffa5aa34cdd6103cc_gustavo-sarago-avatar.v20260731k.png";
  var GUSTAVO_ALT = "Gustavo Sarago, Developer and Operator";

  var ABOUT_H2 =
    "Technology Built Around How Hotel Decisions Actually Happen.";
  var ABOUT_LEAD =
    "Dealality was developed from the realities of hotel brand, operator, development, conversion, and strategic-partner decisions, informed by work with major global brands on growth strategies and the hospitality perspective of senior hospitality leaders.";

  /* Slide 0 = Joan + Justin; slide 1 = Gustavo + Advisors */
  var CARDS = [
    {
      quote:
        "I built Dealality so owners can centralize the opportunity, engage the right participants, compare responses side by side, and keep credible alternatives in view — then move toward agreement without losing control.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      quote:
        "Dealality is the perfect tool to centralize and streamline the information, communications, tools, and deal participants I need for evaluating an opportunity. It creates better visibility into the alternatives, the trade-offs, and the issues that deserve closer attention.",
      name: "Justin Boutwell",
      title: "Lead Asset Manager",
      img: JUSTIN,
      alt: JUSTIN_ALT,
    },
    {
      quote:
        "Development and operating decisions move faster when the right people are working from the same opportunity. Dealality helps bring those conversations together, reduce friction, and keep a project moving toward a workable outcome.",
      name: "Gustavo Sarago",
      title: "Developer and Operator",
      img: GUSTAVO,
      alt: GUSTAVO_ALT,
    },
    {
      quote:
        "Owners gain more when counsel shapes the path before it is half-chosen. Dealality brings advisors into the workflow early enough to change the outcome — with optionality and a clear decision record still intact.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
  ];

  var AUTO_ADVANCES = 2;
  var AUTO_MS = 7000;
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
      "#testimonials-viewport article{",
      "position:relative!important;overflow:visible!important;padding-top:28px!important;",
      "align-items:stretch!important;text-align:left!important}",
      "#testimonials-viewport .oh-tt-audience,",
      "#testimonials-viewport .oh-tt-mid-rule{",
      "display:none!important;visibility:hidden!important;opacity:0!important;",
      "height:0!important;width:0!important;margin:0!important;padding:0!important;",
      "overflow:hidden!important;pointer-events:none!important}",
      "#testimonials-viewport article img{margin:0 auto 10px!important;align-self:center!important;object-fit:cover!important}",
      "#testimonials-viewport article img.oh-tt-avatar-face{",
      "object-position:center 18%!important",
      "}",
      "#testimonials-viewport article img.oh-tt-avatar-gustavo{",
      "object-fit:cover!important;object-position:center 42%!important",
      "}",
      "#testimonials-viewport blockquote{",
      "max-height:8.6em!important;-webkit-line-clamp:5!important;font-size:.9rem!important;",
      "text-align:left!important;align-self:stretch!important;width:100%!important}",
      "#testimonials-viewport article p{text-align:center!important;align-self:center!important;width:100%!important}",
      "#testimonials-dots button.oh-testimonial-dot{",
      "appearance:none;border:0;width:8px;height:8px;border-radius:999px;margin:0 4px;padding:0;",
      "background:rgba(255,255,255,.28);cursor:pointer}",
      "#testimonials-dots button.oh-testimonial-dot.is-active{background:#D78E2C}",
      "@media(max-width:900px){#testimonials-viewport blockquote{max-height:none!important;-webkit-line-clamp:unset!important;display:block!important}}",
    ].join("");
    document.head.appendChild(css);
  }

  function stripAudienceChrome(article) {
    article.removeAttribute("data-oh-audience");
    article.querySelectorAll(".oh-tt-audience,.oh-tt-mid-rule").forEach(function (el) {
      if (el && el.parentNode) el.parentNode.removeChild(el);
    });
  }

  function applyHeader() {
    var h2 = document.getElementById("testimonials-h2");
    if (h2 && !(h2.textContent || "").trim()) h2.textContent = ABOUT_H2;
    var lead = document.getElementById("testimonials-lead");
    if (lead) lead.textContent = ABOUT_LEAD;
  }

  function applyCopy(root) {
    applyHeader();
    var articles = root.querySelectorAll("#testimonials-viewport article");
    articles.forEach(function (article, i) {
      var card = CARDS[i];
      if (!card) return;
      stripAudienceChrome(article);
      article.setAttribute("aria-label", "Quote from " + card.name);
      var bq = article.querySelector("blockquote");
      var p = article.querySelector("p");
      var img = article.querySelector("img");
      if (bq) bq.textContent = '"' + card.quote + '"';
      if (p) {
        p.innerHTML = "<strong>" + card.name + "</strong> — " + card.title;
      }
      if (img) {
        img.src = card.img;
        img.alt = card.alt || "";
        img.loading = "lazy";
        img.classList.remove("oh-tt-avatar-face");
        img.classList.remove("oh-tt-avatar-gustavo");
        img.style.removeProperty("object-position");
        if (card.name === "Justin Boutwell") {
          img.classList.add("oh-tt-avatar-face");
          img.style.objectPosition = "center 18%";
        } else if (card.name === "Gustavo Sarago") {
          img.classList.add("oh-tt-avatar-gustavo");
          img.style.objectPosition = "center 42%";
        }
      }
    });
  }

  function start(root, viewport, slides, dotsWrap) {
    if (started) return;
    started = true;
    window.__ohTestimonials = 202608011;

    var index = 0;
    var timer = null;
    var autoplayDone = false;
    var advancesLeft = 0;
    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    function stopAutoplay() {
      if (timer) window.clearInterval(timer);
      timer = null;
      advancesLeft = 0;
      autoplayDone = true;
    }

    function setSlide(next) {
      index = (next + slides.length) % slides.length;
      slides.forEach(function (slide, i) {
        slide.classList.toggle("is-active", i === index);
        slide.setAttribute("aria-hidden", i === index ? "false" : "true");
      });
      dotsWrap.querySelectorAll("button").forEach(function (dot, i) {
        dot.classList.toggle("is-active", i === index);
        dot.setAttribute("aria-selected", i === index ? "true" : "false");
      });
      applyCopy(root);
    }

    if (!dotsWrap.querySelector("button")) {
      var slideLabels = ["Testimonials slide 1", "Testimonials slide 2"];
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

    function startAutoplayTwice() {
      if (timer) window.clearInterval(timer);
      timer = null;
      if (reduced || autoplayDone || slides.length < 2) return;
      advancesLeft = AUTO_ADVANCES;
      timer = window.setInterval(function () {
        if (advancesLeft <= 0) {
          stopAutoplay();
          return;
        }
        advancesLeft -= 1;
        setSlide(index + 1);
        if (advancesLeft <= 0) stopAutoplay();
      }, AUTO_MS);
    }

    function armScrollAutoplay() {
      if (reduced || autoplayDone || slides.length < 2) return;
      var target = root;
      function maybeStart() {
        if (autoplayDone || timer) return;
        startAutoplayTwice();
      }
      if (typeof IntersectionObserver !== "function") {
        maybeStart();
        return;
      }
      var io = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (!entry.isIntersecting) return;
            maybeStart();
            io.disconnect();
          });
        },
        { root: null, rootMargin: "0px 0px -10% 0px", threshold: 0.2 }
      );
      io.observe(target);
      var rect = target.getBoundingClientRect();
      var vh = window.innerHeight || 800;
      if (rect.top < vh * 0.85 && rect.bottom > vh * 0.15) {
        maybeStart();
        io.disconnect();
      }
    }

    injectCss();
    applyCopy(root);
    setSlide(0);
    armScrollAutoplay();
    window.requestAnimationFrame(function () {
      applyCopy(root);
    });
  }

  function tryBoot() {
    if (started || window.__ohTestimonials >= 202608011) return;
    var root = document.getElementById("trust") || document.getElementById("testimonials");
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
    if (retries >= 3) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[oh-testimonials] markup not ready after 3 retries");
      }
      return;
    }
    retries += 1;
    window.setTimeout(tryBoot, 100 * retries);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", tryBoot, { once: true });
  } else {
    tryBoot();
  }
})();
