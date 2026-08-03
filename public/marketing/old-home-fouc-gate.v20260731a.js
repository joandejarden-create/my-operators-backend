/**
 * Old Home FOUC gate (v20260731a)
 * Path-gated to /old-home. Hides #dc-page until critical CSS applies,
 * early-injects Request a Demo shell, then marks html.oh-ready.
 *
 * Fixes:
 * - Signals: freeform-head w23 + hero-fit 29f bake final quiet sizes
 *   (no post-reveal quiet-w21 font jump).
 * - Demo CTA: create #fsw-demo-link + critical orange styles before reveal
 *   so the button does not pop in after first paint.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohFoucGate && window.__ohFoucGate >= 202607311) return;
    window.__ohFoucGate = 202607311;

    var d = document;
    var html = d.documentElement;
    html.classList.add("oh-boot");

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
        "html.oh-boot:not(.oh-ready) #rotator > *[aria-hidden='false']{opacity:1!important}" +
        "#fsw-secondary-wrap,#fsw-secondary-wrap.oh-fsw-secondary-wrap{" +
        "display:none!important;visibility:hidden!important;pointer-events:none!important}" +
        '#fsw-secondary-wrap[data-oh-visible="1"]{' +
        "display:block!important;visibility:visible!important;pointer-events:auto!important}" +
        /* Early demo CTA chrome — same look as request-demo 30l, before that script runs */
        "#fsw-field-wrap,#fsw-email,#fsw-glow,#fsw-bg1,#fsw-bg2,#fsw-glow-rotate,#fsw-submit-hit{display:none!important}" +
        "#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{position:relative!important;inset:auto!important;display:inline-flex!important;flex-direction:row!important;flex-wrap:nowrap!important;align-items:stretch!important;justify-content:flex-start!important;gap:8px!important;width:auto!important;height:auto!important;margin:0!important;overflow:visible!important}" +
        "#fsw-btn-wrap .oh-fsw-btn,#fsw-btn-wrap #fsw-btn{position:relative!important;flex:0 0 auto!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;min-height:2.55rem!important;padding:.62rem 1.1rem!important;margin:0!important;border-radius:999px!important;box-sizing:border-box!important}" +
        "#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link{appearance:none!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;flex:0 0 auto!important;min-height:2.55rem!important;padding:.62rem 1.1rem!important;margin:0!important;box-sizing:border-box!important;border-radius:999px!important;border:1px solid #d78e2c!important;background:#d78e2c!important;color:#0b1220!important;font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;font-size:16px!important;font-weight:600!important;letter-spacing:-.01em!important;line-height:1.2!important;white-space:nowrap!important;text-decoration:none!important;cursor:pointer!important;box-shadow:0 8px 18px rgba(215,142,44,.28)!important}";
      (d.head || html).appendChild(st);
    }

    var p = "ht" + "tps:";
    var b = p + "//cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";

    function ensureLink(href, dataKey) {
      var sel = 'link[data-oh-fouc="' + dataKey + '"]';
      var existing = d.querySelector(sel);
      if (existing) {
        if (dataKey === "herofit") existing.setAttribute("data-oh-hero-fit", "1");
        return existing;
      }
      var byHref = d.querySelector('link[rel="stylesheet"][href="' + href + '"]');
      if (byHref) {
        byHref.setAttribute("data-oh-fouc", dataKey);
        if (dataKey === "herofit") byHref.setAttribute("data-oh-hero-fit", "1");
        return byHref;
      }
      var l = d.createElement("link");
      l.rel = "stylesheet";
      l.href = href;
      l.setAttribute("data-oh-fouc", dataKey);
      if (dataKey === "herofit") l.setAttribute("data-oh-hero-fit", "1");
      (d.head || html).appendChild(l);
      return l;
    }

    ensureLink(
      p +
        "//fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap",
      "fonts"
    );

    var HEAD_CSS = "6a6c919f39ff30b40ba1ce1c_dealality-old-home-freeform-head.v20260729w23.css";
    var HEROFIT_CSS = "6a6c919f1183b11fc03668f6_dealality-old-home-hero-fit.v20260729f.css";

    var critical = [
      ["dark", b + "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css"],
      ["freeform", b + "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css"],
      ["benefits", b + "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css"],
      ["perspectives", b + "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css"],
      ["head", b + HEAD_CSS],
      ["platform", b + "6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css"],
      ["pricing", b + "6a6bd91e6302c929aa4de707_dealality-old-home-pricing.v20260730f.css"],
      ["herofit", b + HEROFIT_CSS],
    ];

    var cssReady = false;
    var shellReady = false;
    var revealed = false;

    function reveal() {
      if (revealed) return;
      revealed = true;
      html.classList.add("oh-ready");
    }

    function maybeReveal() {
      if (cssReady && shellReady) reveal();
    }

    function ensureDemoShell() {
      var btnWrap = d.getElementById("fsw-btn-wrap");
      var fswBtn = d.getElementById("fsw-btn");
      if (!btnWrap || !fswBtn) return false;
      var demoBtn = d.getElementById("fsw-demo-link");
      if (!demoBtn) {
        demoBtn = d.createElement("button");
        demoBtn.type = "button";
        demoBtn.id = "fsw-demo-link";
        demoBtn.className = "oh-fsw-demo-btn";
        demoBtn.textContent = "Request a Demo";
        demoBtn.setAttribute("aria-haspopup", "dialog");
        demoBtn.setAttribute("data-oh-fouc-demo", "1");
      } else {
        demoBtn.className = "oh-fsw-demo-btn";
        if (!demoBtn.textContent) demoBtn.textContent = "Request a Demo";
        demoBtn.setAttribute("aria-haspopup", "dialog");
      }
      if (fswBtn.parentNode !== btnWrap) btnWrap.appendChild(fswBtn);
      if (demoBtn.parentNode !== btnWrap) btnWrap.appendChild(demoBtn);
      if (btnWrap.firstElementChild !== fswBtn) btnWrap.insertBefore(fswBtn, btnWrap.firstElementChild);
      if (fswBtn.nextSibling !== demoBtn) btnWrap.insertBefore(demoBtn, fswBtn.nextSibling);
      return true;
    }

    function markShellReady() {
      ensureDemoShell();
      shellReady = true;
      maybeReveal();
    }

    var pending = 0;
    function track(link) {
      pending++;
      var done = false;
      function finish() {
        if (done) return;
        done = true;
        pending--;
        if (pending <= 0) {
          cssReady = true;
          maybeReveal();
        }
      }
      if (link.sheet) {
        finish();
        return;
      }
      link.addEventListener("load", finish);
      link.addEventListener("error", finish);
    }

    var mustWait = { dark: 1, head: 1, herofit: 1 };
    critical.forEach(function (pair) {
      var key = pair[0];
      var href = pair[1];
      var link = ensureLink(href, key);
      if (mustWait[key]) track(link);
    });
    if (pending === 0) cssReady = true;

    if (d.readyState === "loading") {
      d.addEventListener("DOMContentLoaded", markShellReady);
    } else {
      markShellReady();
    }

    // If CTA wrap appears slightly after DOMContentLoaded, still seed it before failsafe.
    var tries = 0;
    var poll = setInterval(function () {
      tries++;
      if (ensureDemoShell() || tries > 20) {
        clearInterval(poll);
        shellReady = true;
        maybeReveal();
      }
    }, 50);

    // Failsafe — never leave page invisible
    setTimeout(function () {
      ensureDemoShell();
      cssReady = true;
      shellReady = true;
      reveal();
    }, 1600);
  } catch (err) {
    try {
      document.documentElement.classList.add("oh-ready");
    } catch (_e) {}
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-fouc-gate]", err);
    }
  }
})();
