/**
 * Old Home testimonials carousel (v20260730e)
 * Path-gated to /old-home.
 * - One auto-advance pass, then manual only
 * - 2 tiles visible per slide (SoT relies on #oh-tt)
 * - Four Joan Dejarden quotes with explicit stakeholder labels
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
    if (window.__ohTestimonials >= 202607305) return;
    window.__ohTestimonials = 202607305;
  } catch (ePath) {
    return;
  }

  var root = document.getElementById("testimonials") || document.getElementById("trust");
  if (!root) return;

  var viewport = root.querySelector("#testimonials-viewport");
  var slides = root.querySelectorAll("#testimonials-viewport > div[data-slide]");
  var dotsWrap = root.querySelector("#testimonials-dots");
  if (!viewport || !slides.length || !dotsWrap) return;

  var JOAN =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518bded1ae16ef415eb_founder-joan-dejarden.png";
  var JOAN_ALT = "Joan Dejarden, Founder of Dealality";

  /* Slide 0 = Owners + Brands; slide 1 = Operators + Advisors */
  var CARDS = [
    {
      audience: "Owners",
      label: "For Owners",
      quote:
        "Owners had options, but no clear way to compare them. I built Dealality to give a confidential, structured process for understanding what an asset could become — before committing.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      audience: "Brands",
      label: "For Brands",
      quote:
        "Brands were drowning in incomplete outreach and spending capacity on projects that were never ready. Dealality gives development teams structured owner criteria and better-fit opportunities — so conversations start from alignment, not cleanup.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      audience: "Operators",
      label: "For Operators",
      quote:
        "Operators were asked to evaluate management opportunities from scattered teasers and incomplete files. Dealality brings permission-based deal rooms and structured briefs, so operators can judge operating fit faster — without chasing fragments.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
    {
      audience: "Advisors",
      label: "For Advisors",
      quote:
        "Advisors were often brought in after the path was already half-chosen — and asked to advise on a process they never shaped. Dealality brings counsel, brokers, and capital partners into the workflow when their input can still change the outcome.",
      name: "Joan Dejarden",
      title: "Founder",
      img: JOAN,
      alt: JOAN_ALT,
    },
  ];

  function injectCss() {
    var prev = document.getElementById("oh-tt-audience");
    if (prev && prev.parentNode) prev.parentNode.removeChild(prev);
    var css = document.createElement("style");
    css.id = "oh-tt-audience";
    css.textContent = [
      "#testimonials-viewport article[data-oh-audience]{padding-top:22px!important}",
      "#testimonials-viewport .oh-tt-audience{",
      "display:inline-flex!important;align-items:center!important;justify-content:center!important;",
      "margin:0 0 10px!important;padding:0!important;border:0!important;background:transparent!important;",
      "color:#F4D03F!important;font-size:12px!important;font-weight:700!important;letter-spacing:.08em!important;",
      "text-transform:uppercase!important;line-height:1.2!important;white-space:nowrap!important;",
      "flex-shrink:0!important}",
      "#testimonials-viewport article img{margin-top:0!important;margin-bottom:10px!important}",
      "#testimonials-viewport blockquote{max-height:8.6em!important;-webkit-line-clamp:5!important;font-size:.9rem!important}",
      "#testimonials-dots button.oh-testimonial-dot{",
      "appearance:none;border:0;width:8px;height:8px;border-radius:999px;margin:0 4px;padding:0;",
      "background:rgba(255,255,255,.28);cursor:pointer}",
      "#testimonials-dots button.oh-testimonial-dot.is-active{background:#F4D03F}",
      "@media(max-width:900px){#testimonials-viewport blockquote{max-height:none!important;-webkit-line-clamp:unset!important;display:block!important}}",
    ].join("");
    document.head.appendChild(css);
  }

  function ensureAudienceLabel(article, card) {
    var label = article.querySelector(".oh-tt-audience");
    if (!label) {
      label = document.createElement("p");
      label.className = "oh-tt-audience";
      var img = article.querySelector("img");
      if (img && img.parentNode === article) {
        article.insertBefore(label, img);
      } else {
        article.insertBefore(label, article.firstChild);
      }
    }
    label.textContent = card.label;
    article.setAttribute("data-oh-audience", card.audience.toLowerCase());
    article.setAttribute("aria-label", card.label + " — quote from " + card.name);
  }

  function applyCopy() {
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
  applyCopy();
  setSlide(0);
  startAutoplayOnce();
})();
