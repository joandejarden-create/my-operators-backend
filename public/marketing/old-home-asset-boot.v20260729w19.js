(function () {
  var p = "ht" + "tps:";
  var b = p + "//cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
  var fonts = document.createElement("link");
  fonts.rel = "stylesheet";
  fonts.href =
    p +
    "//fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap";
  document.head.appendChild(fonts);

  [
    "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css",
    "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css",
    "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css",
    "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css",
    "6a6a7cb895b20766ec5595f9_dealality-old-home-freeform-head.v20260729w22.css",
    "6a69b8c44c1a006da8e12d60_dealality-old-home-platform-features.v20260729x1.css",
    "6a69e594ecaa34a1d14f852e_dealality-old-home-pricing.v20260729a.css",
  ].forEach(function (f) {
    var l = document.createElement("link");
    l.rel = "stylesheet";
    l.href = b + f;
    document.head.appendChild(l);
  });

  [
    "6a6a5bdfff4fad2bc2596d50_dealality-old-home-hero-globe-bg.v202607307.js",
    "6a6a3c5a3eb739f6cf4f747e_dealality-old-home-platform-video-launcher.v20260729i.js",
    "6a6a8739c83f9c69c9343dfe_dealality-old-home-testimonials.v20260730a.js",
  ].forEach(function (f) {
    var s = document.createElement("script");
    s.defer = true;
    s.src = b + f;
    document.head.appendChild(s);
  });

  function applyModules() {
    var copy = {
      "mod-1-p":
        "Explore the brand, operator, conversion, capital, and strategic paths most relevant to your hotel and goals.",
      "mod-2-p":
        "Organize the owner's goals, the property story, the economics, and the information partners need to evaluate the opportunity.",
      "mod-3-p":
        "Focus outreach on the brands, operators, investors, and advisors best positioned to respond to the opportunity.",
      "mod-4-p":
        "Avoid letting a single conversation or relationship set the pace, options, or outcome for the entire process.",
      "mod-5-p":
        "See the differences in fees, control, requirements, timing, risk, and long-term value across proposals.",
      "mod-6-p":
        "Choose what to pursue with clearer information, stronger priorities, and greater confidence in the path forward.",
      "modp-1-p":
        "Bring the hotel, owner goals, market context, constraints, and key questions into one structured review workspace.",
      "modp-2-p":
        "Identify the brands, operators, structures, conversions, and capital options worth considering for the property.",
      "modp-3-p":
        "Understand who may fit the opportunity, why they may fit, and what still needs to be confirmed.",
      "modp-4-p":
        "Present the opportunity clearly and manage confidential conversations with the parties selected for outreach.",
      "modp-5-p":
        "Review fees, requirements, support, control, timing, and important differences side by side across proposals.",
      "modp-6-p":
        "Track open questions, missing terms, negotiation priorities, and the reasons behind the final decision.",
    };
    Object.keys(copy).forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.textContent = copy[id];
    });
    ["modules-dot-1", "modules-dot-2"].forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.classList.add("modules-dot");
      if (el.getAttribute("aria-selected") === "true") el.classList.add("is-active");
    });
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", applyModules);
  } else {
    applyModules();
  }
})();
