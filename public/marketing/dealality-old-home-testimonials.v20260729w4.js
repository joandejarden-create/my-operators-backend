(function () {
  "use strict";

  var root = document.getElementById("testimonials") || document.getElementById("trust");
  if (!root) return;

  var viewport = root.querySelector("#testimonials-viewport");
  var slides = root.querySelectorAll("#testimonials-viewport > div[data-slide]");
  var dotsWrap = root.querySelector("#testimonials-dots");
  if (!viewport || !slides.length || !dotsWrap) return;

  var index = 0;
  var timer = null;
  var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

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
  }

  slides.forEach(function (_, i) {
    var dot = document.createElement("button");
    dot.type = "button";
    dot.className = "oh-testimonial-dot" + (i === 0 ? " is-active" : "");
    dot.setAttribute("aria-label", "Show testimonial slide " + (i + 1));
    dot.setAttribute("aria-selected", i === 0 ? "true" : "false");
    dot.addEventListener("click", function () {
      setSlide(i);
      restart();
    });
    dotsWrap.appendChild(dot);
  });

  function restart() {
    if (timer) window.clearInterval(timer);
    if (reduced || slides.length < 2) return;
    timer = window.setInterval(function () {
      setSlide(index + 1);
    }, 7000);
  }

  /* Wrap quote text in ASCII quotation marks when missing */
  root.querySelectorAll("#testimonials-viewport blockquote").forEach(function (bq) {
    var text = (bq.textContent || "").trim();
    if (!text) return;
    var first = text.charAt(0);
    var last = text.charAt(text.length - 1);
    var hasOpen = first === '"' || first === "\u201C" || first === "\u201D";
    var hasClose = last === '"' || last === "\u201D" || last === "\u201C";
    if (!hasOpen || !hasClose) {
      var core = text.replace(/^[\s\u201C\u201D"]+|[\s\u201C\u201D"]+$/g, "");
      bq.textContent = '"' + core + '"';
    }
  });

  /* Normalize attribution to "Name — Title" */
  root.querySelectorAll("#testimonials-viewport article p").forEach(function (p) {
    var html = p.innerHTML;
    if (!html) return;
    var next = html.replace(/\s*[–—−-]\s*/g, " — ");
    if (next !== html) p.innerHTML = next;
  });

  setSlide(0);
  restart();
})();
