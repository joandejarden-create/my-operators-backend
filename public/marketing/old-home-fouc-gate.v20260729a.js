/**
 * Old Home FOUC gate (v20260729a)
 * Path-gated to /old-home. Hides #dc-page until critical CSS applies,
 * injects critical stylesheets early, then marks html.oh-ready.
 * Does not touch globe pin-dim or hero-fit spacing rules — only loads existing CDN builds.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohFoucGate && window.__ohFoucGate >= 202607291) return;
    window.__ohFoucGate = 202607291;

    var d = document;
    var html = d.documentElement;
    html.classList.add("oh-boot");

    // Inline critical hide rules in case site freeform style is missing
    if (!d.getElementById("oh-fouc-gate-style")) {
      var st = d.createElement("style");
      st.id = "oh-fouc-gate-style";
      st.textContent =
        "html.oh-boot{background:#080F25!important}" +
        "html.oh-boot:not(.oh-ready) #dc-page," +
        "html.oh-boot:not(.oh-ready) .oh-page{visibility:hidden!important}" +
        "html.oh-boot #hero-globe-list{display:none!important;visibility:hidden!important;height:0!important;overflow:hidden!important}" +
        "html.oh-boot:not(.oh-ready) #rotator > *{opacity:0!important}" +
        "html.oh-boot:not(.oh-ready) #rotator > *.is-on," +
        "html.oh-boot:not(.oh-ready) #rotator > *.oh-hrword-on," +
        "html.oh-boot:not(.oh-ready) #rotator > *[aria-hidden='false']{opacity:1!important}";
      (d.head || html).appendChild(st);
    }

    var p = "ht" + "tps:";
    var b = p + "//cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";

    function ensureLink(href, dataKey) {
      var sel = 'link[data-oh-fouc="' + dataKey + '"]';
      var existing = d.querySelector(sel);
      if (existing) return existing;
      // Reuse if already present from BootGuard / other boots
      var byHref = d.querySelector('link[rel="stylesheet"][href="' + href + '"]');
      if (byHref) {
        byHref.setAttribute("data-oh-fouc", dataKey);
        return byHref;
      }
      var l = d.createElement("link");
      l.rel = "stylesheet";
      l.href = href;
      l.setAttribute("data-oh-fouc", dataKey);
      (d.head || html).appendChild(l);
      return l;
    }

    // Fonts
    ensureLink(
      p +
        "//fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap",
      "fonts"
    );

    // Critical CSS — versions pinned to current live CDN builds
    var critical = [
      ["dark", b + "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css"],
      ["freeform", b + "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css"],
      ["benefits", b + "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css"],
      ["perspectives", b + "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css"],
      // w21 — includes #hero-globe-list{display:none} + rotator layout
      ["head", b + "6a6a629414ad57d94a7f3c87_dealality-old-home-freeform-head.v20260729w21.css"],
      ["platform", b + "6a6a1aa77e93121d8fecb49d_dealality-old-home-platform-features.v20260729x2.css"],
      ["pricing", b + "6a69e594ecaa34a1d14f852e_dealality-old-home-pricing.v20260729a.css"],
      // hero-fit — min-height 100vh so mid-page sections don't peek into first paint
      ["herofit", b + "6a6a5be1b239faf73c7e267f_dealality-old-home-hero-fit.v20260729c.css"],
    ];

    var pending = 0;
    var revealed = false;
    function reveal() {
      if (revealed) return;
      revealed = true;
      html.classList.add("oh-ready");
    }

    function track(link) {
      pending++;
      var done = false;
      function finish() {
        if (done) return;
        done = true;
        pending--;
        if (pending <= 0) reveal();
      }
      // Already loaded (cached)
      if (link.sheet) {
        finish();
        return;
      }
      link.addEventListener("load", finish);
      link.addEventListener("error", finish);
    }

    // Must-wait sheets: dark + freeform-head + hero-fit
    var mustWait = { dark: 1, head: 1, herofit: 1 };
    critical.forEach(function (pair) {
      var key = pair[0];
      var href = pair[1];
      var link = ensureLink(href, key);
      if (mustWait[key]) track(link);
    });

    // Failsafe — never leave page invisible
    setTimeout(reveal, 1400);

    // Soft failsafe if nothing to track
    if (pending === 0) reveal();
  } catch (err) {
    try {
      document.documentElement.classList.add("oh-ready");
    } catch (_e) {}
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-fouc-gate]", err);
    }
  }
})();
