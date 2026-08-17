/**
 * Old Home testimonials carousel (v20260730b)
 * Path-gated to /old-home.
 * - One auto-advance pass, then manual only (keeps v20260730a behavior)
 * - 2 tiles visible per slide (SoT relies on #oh-tt / w22 — does not inject w16)
 * - Applies compact equal card copy + avatar alignment for owner / brand / operator set
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
    if (path !== "/old-home") return;
  } catch (ePath) {
    return;
  }

  var root = document.getElementById("testimonials") || document.getElementById("trust");
  if (!root) return;

  var viewport = root.querySelector("#testimonials-viewport");
  var slides = root.querySelectorAll("#testimonials-viewport > div[data-slide]");
  var dotsWrap = root.querySelector("#testimonials-dots");
  if (!viewport || !slides.length || !dotsWrap) return;

  var AVATAR = {
    joan: "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518bded1ae16ef415eb_founder-joan-dejarden.png",
    natalie:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69ad14c6a7716b55405234_testimonial-avatar-natalie.jpg",
    sarah:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69ad21a26d598adc5289cd_testimonial-avatar-sarah.jpg",
  };

  /* Slide 0 = owners; slide 1 = brand + operator. Compact copy locked to equal card height. */
  var CARDS = [
    {
      quote:
        "Owners had options, but no clear way to compare them. I built Dealality to give a confidential, structured process for understanding what an asset could become — before committing.",
      name: "Joan Dejarden",
      title: "Founder",
      img: AVATAR.joan,
      alt: "Joan Dejarden, Founder of Dealality",
    },
    {
      quote:
        "Confidential, organized, and owner-first — exactly what we needed before entering brand and operator discussions.",
      name: "Sarah Mitchell",
      title: "Asset Manager",
      img: AVATAR.sarah,
      alt: "Sarah Mitchell, Asset Manager",
    },
    {
      quote:
        "Owners arrive with clearer criteria and structured project context, so our development team spends less time on incomplete leads and more time on opportunities that fit.",
      name: "Natalie Brooks",
      title: "Brand Development",
      img: AVATAR.natalie,
      alt: "Natalie Brooks, Brand Development",
    },
    {
      quote:
        "Permission-based deal rooms and structured owner briefs help us evaluate fit faster — without chasing fragmented outreach or incomplete project files.",
      name: "Elena Vargas",
      title: "Managing Director",
      img: AVATAR.sarah,
      alt: "Elena Vargas, Managing Director",
    },
  ];

  function applyCopy() {
    var articles = root.querySelectorAll("#testimonials-viewport article");
    articles.forEach(function (article, i) {
      var card = CARDS[i];
      if (!card) return;
      var bq = article.querySelector("blockquote");
      var p = article.querySelector("p");
      var img = article.querySelector("img");
      if (bq) bq.textContent = '"' + card.quote + '"';
      if (p) {
        p.innerHTML =
          "<strong>" +
          card.name +
          "</strong> — " +
          card.title;
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
    slides.forEach(function (_, i) {
      var dot = document.createElement("button");
      dot.type = "button";
      dot.className = "oh-testimonial-dot" + (i === 0 ? " is-active" : "");
      dot.setAttribute("aria-label", "Show testimonial slide " + (i + 1));
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

  applyCopy();
  setSlide(0);
  startAutoplayOnce();
})();
