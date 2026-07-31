/**
 * Old Home FOUC gate (v20260731e)
 * Path-gated to /old-home. Hides #dc-page until critical CSS applies,
 * early-injects Request a Demo shell, then marks html.oh-ready.
 *
 * Fixes:
 * - Signals: freeform-head w26 + hero-fit 29h bake final quiet sizes.
 * - CTA size lock: Explore + Request a Demo stay 2.55rem (no tall-viewport grow).
 * - Demo CTA: create #fsw-demo-link before reveal.
 * - Hide scroll-cue until html.oh-ready to prevent double enter flash.
 * - 31c: cue reveal uses oh-ready gate (hide had !important that never lifted).
 * - 31c: bake form-subscribe margins so CTA cannot flash lower→up.
 * - 31d: bake pure-white nav link color (Webflow ships #ffffff9e gray).
 * - 31d: rebuild nav to white-order before reveal so FAQ/Insights do not swap.
 * - 31e: nav label "How It Works" → #oh-how-we-do-it (How Dealality Works).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohFoucGate && window.__ohFoucGate >= 202607315) return;
    window.__ohFoucGate = 202607315;

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
        /* Pure white nav — override Webflow .oh-nav-link #ffffff9e before first paint */
        "#nav-links.oh-nav-links a,#nav-links.oh-nav-links a.oh-nav-link,.oh-nav-links a,.oh-nav-link{" +
        "color:#fff!important;opacity:1!important;font-weight:500!important;letter-spacing:-.01em!important;text-decoration:none!important}" +
        "#nav-links.oh-nav-links a:hover,.oh-nav-links a:hover,.oh-nav-link:hover{color:#fff!important;opacity:.88!important}" +
        "#mnav.oh-mnav a.oh-mnav-link,.oh-mnav-link{color:#fff!important;opacity:1!important;font-weight:500!important}" +
        "#mnav.oh-mnav a.oh-mnav-link:hover,.oh-mnav-link:hover{color:#fff!important;opacity:.88!important}" +
        /* Early demo CTA chrome — same look as request-demo 30l, before that script runs */
        "#fsw-field-wrap,#fsw-email,#fsw-glow,#fsw-bg1,#fsw-bg2,#fsw-glow-rotate,#fsw-submit-hit{display:none!important}" +
        "#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{position:relative!important;inset:auto!important;display:inline-flex!important;flex-direction:row!important;flex-wrap:nowrap!important;align-items:stretch!important;justify-content:flex-start!important;gap:8px!important;width:auto!important;height:auto!important;margin:0!important;overflow:visible!important}" +
        "#fsw-btn-wrap .oh-fsw-btn,#fsw-btn-wrap #fsw-btn,#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link{position:relative!important;flex:0 0 auto!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;min-height:2.55rem!important;height:2.55rem!important;max-height:2.55rem!important;padding:.62rem 1.1rem!important;margin:0!important;border-radius:999px!important;box-sizing:border-box!important;font-size:16px!important;line-height:1.2!important}" +
        "html.oh-boot:not(.oh-ready) #oh-hero-scroll-cue,html.oh-boot:not(.oh-ready) #oh-hero-scroll-cue.oh-hero-scroll-cue{opacity:0!important;visibility:hidden!important;pointer-events:none!important;animation:none!important;transition:none!important}" +
        "#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:.75rem!important}" +
        "@media(max-height:900px) and (min-width:961px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:.55rem!important}}" +
        "@media(max-height:800px) and (min-width:961px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:.4rem!important}}" +
        "@media(min-width:1200px) and (min-height:901px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:clamp(1.65rem,3.2vh,2.35rem)!important}}" +
        "@media(min-width:1440px) and (min-height:901px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:clamp(2.1rem,3.8vh,2.85rem)!important}}" +
        "@media(min-width:1600px) and (min-height:950px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:clamp(2.45rem,4.2vh,3.25rem)!important}}" +
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

    // HEAD_CSS pin filled after upload (placeholder replaced by upload step)
    var HEAD_CSS = "6a6ca035bc58079a7c5af558_dealality-old-home-freeform-head.v20260729w26.css";
    var HEROFIT_CSS = "6a6c9e1362368e3a654cf326_dealality-old-home-hero-fit.v20260729h.css";

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

    var NAV_ORDER = [
      { href: "#about", label: "About" },
      { href: "#oh-how-we-do-it", label: "How It Works" },
      { href: "#modules", label: "Benefits" },
      { href: "#pricing", label: "Pricing" },
      { href: "#faq", label: "FAQ" },
      { href: "#insights", label: "Insights" },
    ];

    function ensureNavOrder() {
      // Same contract as nav-cleanup 30b — mark done so footer script does not rebuild/flash.
      // Requires site footer to load nav-cleanup 30b (not 30a), since 30a only skips on exact === 301.
      if (window.__ohNavCleanup && window.__ohNavCleanup >= 202607302) return true;
      var host = d.getElementById("nav-links");
      if (!host) return false;
      host.innerHTML = "";
      for (var i = 0; i < NAV_ORDER.length; i++) {
        var a = d.createElement("a");
        a.href = NAV_ORDER[i].href;
        a.className = "oh-nav-link";
        a.textContent = NAV_ORDER[i].label;
        host.appendChild(a);
      }
      var mnav = d.getElementById("mnav");
      if (mnav) {
        var extras = [];
        var kids = mnav.querySelectorAll("a");
        for (var j = 0; j < kids.length; j++) {
          var href = kids[j].getAttribute("href") || "";
          var text = (kids[j].textContent || "").trim();
          if (href.indexOf("#") === 0) continue;
          if (/^about$|^how we do it$|^how it works$|^benefits$|^pricing$|^faq$|^insights$/i.test(text)) continue;
          extras.push({
            href: href,
            label: text,
            className: kids[j].className || "oh-mnav-link",
          });
        }
        mnav.innerHTML = "";
        for (var k = 0; k < NAV_ORDER.length; k++) {
          var ma = d.createElement("a");
          ma.href = NAV_ORDER[k].href;
          ma.className = "oh-mnav-link";
          ma.textContent = NAV_ORDER[k].label;
          mnav.appendChild(ma);
        }
        for (var x = 0; x < extras.length; x++) {
          var ea = d.createElement("a");
          ea.href = extras[x].href;
          ea.className = extras[x].className || "oh-mnav-link";
          ea.textContent = extras[x].label;
          mnav.appendChild(ea);
        }
      }
      window.__ohNavCleanup = 202607302;
      return true;
    }

    var cssReady = false;
    var shellReady = false;
    var revealed = false;

    function reveal() {
      if (revealed) return;
      ensureNavOrder();
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
      ensureNavOrder();
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
      var demoOk = ensureDemoShell();
      var navOk = ensureNavOrder();
      if ((demoOk && navOk) || tries > 20) {
        clearInterval(poll);
        shellReady = true;
        maybeReveal();
      }
    }, 50);

    // Failsafe — never leave page invisible
    setTimeout(function () {
      ensureDemoShell();
      ensureNavOrder();
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
