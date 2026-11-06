/**
 * Old Home FOUC gate (v20260802b)
 * Path-gated to /, /es, and /old-home (homepage + Spanish locale).
 * 02b: /es — #hstatic "Find the right" → "Encuentra tu" before reveal.
 * 02a: unlock `/es` so Spanish home gets FOUC/oh-ready + hero chrome.
 * Hides #dc-page until critical CSS applies,
 * early-injects Request a Demo shell, then marks html.oh-ready.
 *
 * 01j: pin freeform-head 01e + width-bands 01a (1120 content / 1320 learn·CTA·footer).
 * 01i: pin freeform-head 01d + hero-fit 01b (tall --hr-lh matches first paint;
 *      rotator must not crush --hr-lh — see hero-rotator 01c).
 * 01h: wide+tall bake hero-inner flex + CTA margin-top:auto (buttons near fold);
 *      keep eyebrow→lead spacing; no CTA high→drop flash after reveal.
 * 01g: bake final wide-screen stack (no freeform 2.75rem → FOUC crush flash);
 *      CTA wrap align-items:center + Explore height:40px hard lock (no stretch twitch);
 *      Demo hover transform:none; re-assert locks after reveal/demo CSS.
 * 01f: wait for document.fonts before reveal; bake eyebrow final chrome;
 *      beat site-head dual-CTA 2.55rem lock (Explore stays 40px / 10px);
 *      failsafe does not fake rotator-ready until 2.4s (rotator 01b owns measure).
 * 01e: bake FINAL nav order (incl. Benefits → #modules) before reveal; stamp
 *      __ohNavCleanup >= nav-cleanup 01a so late rebuild cannot flash.
 * 01c: on reveal, re-append Explore CTA size lock so freeform/demo cannot
 * flash tall pill → Connected Process size after oh-ready.
 * 01b: Explore CTA chrome matches Connected Process primary (10px / 40px / #6C72FF).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home" && path !== "/es") return;
    if (window.__ohFoucGate && window.__ohFoucGate >= 202608022) return;
    window.__ohFoucGate = 202608022;

    var d = document;
    var html = d.documentElement;
    html.classList.add("oh-boot");

    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    function applyEsHeroStatic() {
      if (!isEs) return;
      var hs = d.getElementById("hstatic");
      if (!hs) return;
      var cur = (hs.textContent || "").replace(/\s+/g, " ").trim();
      if (cur === "Find the right" || cur === "Find the Right") {
        hs.textContent = "Encuentra tu";
      }
    }
    applyEsHeroStatic();

    if (!d.getElementById("oh-fouc-gate-style")) {
      var st = d.createElement("style");
      st.id = "oh-fouc-gate-style";
      st.textContent =
        "html.oh-boot{background:#080F25!important}" +
        "html.oh-boot:not(.oh-ready) #dc-page," +
        "html.oh-boot:not(.oh-ready) .oh-page{" +
        "visibility:hidden!important;opacity:0!important}" +
        "html.oh-boot.oh-ready #dc-page,html.oh-boot.oh-ready .oh-page{opacity:1!important}" +
        "html.oh-boot #hero-globe-list{display:none!important;visibility:hidden!important;height:0!important;overflow:hidden!important}" +
        /* Hero rotator FOUC — clip stack + flex row before reveal; no 640px width jump */
        "#h1wrap.oh-h1wrap,#h1wrap{" +
        "display:flex!important;flex-direction:row!important;flex-wrap:nowrap!important;" +
        "align-items:center!important;justify-content:flex-start!important;" +
        "gap:.55em!important;overflow:visible!important;" +
        "--hr-slots:5!important;--hr-center:2!important;" +
        "--hr-lh:clamp(40px,5.2vw,70px)!important}" +
        "#hrwrap,.oh-hrwrap{" +
        "display:block!important;position:relative!important;z-index:1!important;" +
        "height:calc(var(--hr-lh)*var(--hr-slots))!important;" +
        "min-height:calc(var(--hr-lh)*var(--hr-slots))!important;" +
        "width:var(--hr-w,max-content)!important;min-width:0!important;" +
        "overflow:hidden!important;flex-shrink:0!important;" +
        "-webkit-mask-image:linear-gradient(180deg,transparent 0%,#000 12%,#000 88%,transparent 100%)!important;" +
        "mask-image:linear-gradient(180deg,transparent 0%,#000 12%,#000 88%,transparent 100%)!important}" +
        "#rotator,.oh-rotator{" +
        "display:flex!important;flex-direction:column!important;align-items:flex-start!important;" +
        "width:max-content!important;min-width:100%!important}" +
        "#rotator > *,.oh-hrword{" +
        "display:flex!important;align-items:center!important;box-sizing:border-box!important;" +
        "font-family:Lora,Georgia,serif!important;font-style:italic!important;font-weight:400!important;" +
        "font-size:clamp(32px,4.5vw,58px)!important;" +
        "line-height:var(--hr-lh)!important;height:var(--hr-lh)!important;" +
        "min-height:var(--hr-lh)!important;max-height:var(--hr-lh)!important;" +
        "color:#8B90FF!important;white-space:nowrap!important;opacity:.12!important;" +
        "padding:0!important;margin:0!important;pointer-events:none!important}" +
        "#rotator > *.on,#rotator > *.oh-hrword-on,.oh-hrword-on{" +
        "font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;" +
        "font-style:normal!important;font-weight:800!important;letter-spacing:-.035em!important;" +
        "color:#D78E2C!important;opacity:1!important}" +
        "html.oh-boot:not(.oh-rotator-ready) #rotator > *:nth-child(3){" +
        "font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;" +
        "font-style:normal!important;font-weight:800!important;letter-spacing:-.035em!important;" +
        "color:#D78E2C!important;opacity:1!important}" +
        "html.oh-boot:not(.oh-rotator-ready) #hrwrap,html.oh-boot:not(.oh-rotator-ready) .oh-hrwrap{" +
        "visibility:hidden!important}" +
        "@media (max-width:960px){" +
        "#h1wrap.oh-h1wrap,#h1wrap{flex-direction:column!important;align-items:flex-start!important;" +
        "--hr-slots:3!important;--hr-center:1!important;--hr-lh:clamp(34px,8vw,52px)!important}" +
        "#hstatic,.oh-hstatic,#rotator > *,.oh-hrword,.oh-hrword-on{font-size:clamp(26px,7.2vw,40px)!important}}" +
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
        "#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{position:relative!important;inset:auto!important;display:inline-flex!important;flex-direction:row!important;flex-wrap:nowrap!important;align-items:center!important;justify-content:flex-start!important;gap:8px!important;width:auto!important;height:auto!important;margin:0!important;overflow:visible!important}" +
        /* Explore CTA = Connected Process primary; hard 40px (never stretch to Demo) */
        "#hero #fsw-btn,#hero #fsw-btn.oh-fsw-btn,#fsw-btn-wrap .oh-fsw-btn,#fsw-btn-wrap #fsw-btn," +
        "#fsw-btn,#fsw-btn.oh-fsw-btn{position:relative!important;flex:0 0 auto!important;align-self:center!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;min-height:40px!important;height:40px!important;max-height:40px!important;padding:0 14px!important;margin:0!important;border-radius:10px!important;box-sizing:border-box!important;font-size:.86rem!important;font-weight:700!important;line-height:1.2!important;background:#6C72FF!important;background-image:none!important;color:#fff!important;box-shadow:none!important;border:0!important;transform:none!important;font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important}" +
        "#hero #fsw-btn:hover,#fsw-btn:hover,#fsw-btn.oh-fsw-btn:hover{transform:none!important;filter:none!important}" +
        "#hero #fsw-demo-link,#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link{position:relative!important;flex:0 0 auto!important;align-self:center!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;min-height:2.55rem!important;height:2.55rem!important;max-height:2.55rem!important;padding:.62rem 1.1rem!important;margin:0!important;border-radius:999px!important;box-sizing:border-box!important;font-size:16px!important;line-height:1.2!important;transform:none!important}" +
        "#hero #fsw-demo-link:hover,#fsw-demo-link.oh-fsw-demo-btn:hover,#fsw-demo-link:hover{transform:none!important}" +
        "html.oh-boot:not(.oh-ready) #oh-hero-scroll-cue,html.oh-boot:not(.oh-ready) #oh-hero-scroll-cue.oh-hero-scroll-cue{opacity:0!important;visibility:hidden!important;pointer-events:none!important;animation:none!important;transition:none!important}" +
        /* Final eyebrow (FAQ pill) — bake before reveal; beat freeform 2.25rem margin */
        "#section-subtitle,.oh-sst-wrap,#hero #section-subtitle{display:flex!important;justify-content:flex-start!important;align-items:center!important;width:100%!important;margin:0 0 .4rem!important;text-align:left!important}" +
        "#sst-inner,.oh-sst-inner{display:inline-flex!important;align-items:center!important;overflow:hidden!important;border-radius:999px!important;border:1px solid rgba(255,255,255,.14)!important;background:rgba(8,15,37,.92)!important;padding:5px 15px 5px 5px!important;box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;margin:0!important}" +
        "#sst-pill,.oh-sst-pill{display:inline-flex!important;align-items:center!important;padding:0 10px!important;height:32px!important;border-radius:10px!important;background:#343259!important;color:#fff!important;font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important}" +
        "#sst-text,.oh-sst-text{display:inline-flex!important;align-items:center!important;margin-left:15px!important;color:#fff!important;font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;white-space:nowrap!important}" +
        "#sst-glow,#sst-bg1,#sst-bg2,#sst-rotate,#sst-rotate-inner,.oh-sst-glow,.oh-sst-bg1,.oh-sst-bg2,.oh-sst-rotate,.oh-sst-rotate-inner{display:none!important}" +
        /* Bake hero-fit 29h stack so first paint matches final CTA Y (no lower→up flash) */
        "#hero.oh-hero,#hero{padding-top:clamp(2.35rem,4.8vh,3.55rem)!important;padding-bottom:clamp(1.75rem,4.5vh,2.75rem)!important}" +
        "#hero #hero-inner{padding-top:0!important;margin-top:0!important;transform:translateY(-0.35rem)!important}" +
        "#hero #h1wrap,#hero .oh-h1wrap{margin:0 0 .55rem!important;gap:.4em!important;column-gap:.4em!important}" +
        "#hero #hero-lead,#hero .oh-hero-lead{margin:0 0 .6rem!important;max-width:34rem!important}" +
        "#hero #hero-signals{margin:0 0 .1rem!important}" +
        "@media(max-height:900px) and (min-width:961px){#hero.oh-hero,#hero{padding-top:2.15rem!important;padding-bottom:1.85rem!important}#hero #hero-inner{transform:translateY(-0.55rem)!important}#hero #h1wrap,#hero .oh-h1wrap{margin-bottom:.45rem!important}#hero #hero-lead,#hero .oh-hero-lead{margin-bottom:.45rem!important}}" +
        "@media(max-height:800px) and (min-width:961px){#hero.oh-hero,#hero{padding-top:1.75rem!important;padding-bottom:1.4rem!important}#hero #hero-inner{transform:translateY(-0.75rem)!important}#hero #h1wrap,#hero .oh-h1wrap{margin-bottom:.35rem!important}#hero #hero-lead,#hero .oh-hero-lead{margin-bottom:.35rem!important}}" +
        /* Wide/tall: more open stack (final first paint — matches freeform-head 01c) */
        "@media(min-width:1200px) and (min-height:901px){#hero.oh-hero,#hero{padding-top:clamp(2.85rem,5.8vh,4.1rem)!important;padding-bottom:clamp(1.35rem,3.2vh,2.1rem)!important}#hero #hero-inner{display:flex!important;flex-direction:column!important;align-items:flex-start!important;flex:1 1 auto!important;min-height:0!important;transform:none!important}#hero #section-subtitle,#section-subtitle{margin:0 0 .85rem!important}#hero #h1wrap,#hero .oh-h1wrap{margin:0 0 1.35rem!important}#hero #hero-lead,#hero .oh-hero-lead{margin:0 0 1.15rem!important}#hero #hero-signals{margin:0 0 .5rem!important}}" +
        "@media(min-width:1440px) and (min-height:901px){#hero.oh-hero,#hero{padding-top:clamp(3.15rem,6.4vh,4.65rem)!important;padding-bottom:clamp(1.5rem,3.5vh,2.35rem)!important}#hero #section-subtitle,#section-subtitle{margin:0 0 .95rem!important}#hero #h1wrap,#hero .oh-h1wrap{margin:0 0 1.55rem!important}#hero #hero-lead,#hero .oh-hero-lead{margin:0 0 1.35rem!important}}" +
        "#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:.75rem!important}" +
        "@media(max-height:900px) and (min-width:961px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:.55rem!important}}" +
        "@media(max-height:800px) and (min-width:961px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:.4rem!important}}" +
        "@media(min-width:1200px) and (min-height:901px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:auto!important;margin-bottom:0!important;padding-bottom:clamp(4.5rem,9.5vh,6.75rem)!important}}" +
        "@media(min-width:1440px) and (min-height:901px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:auto!important;padding-bottom:clamp(5rem,10vh,7.25rem)!important}}" +
        "@media(min-width:1600px) and (min-height:950px){#hero #form-subscribe-wrap,#hero .oh-fsw-wrap,#hero #fsw-cta{margin-top:auto!important;padding-bottom:clamp(5.25rem,10.5vh,7.5rem)!important}}" +
        "#fsw-demo-link.oh-fsw-demo-btn,#fsw-demo-link,#hero #fsw-demo-link{appearance:none!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;flex:0 0 auto!important;min-height:2.55rem!important;padding:.62rem 1.1rem!important;margin:0!important;box-sizing:border-box!important;border-radius:999px!important;border:1px solid #d78e2c!important;background:#d78e2c!important;color:#0b1220!important;font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;font-size:16px!important;font-weight:600!important;letter-spacing:-.01em!important;line-height:1.2!important;white-space:nowrap!important;text-decoration:none!important;cursor:pointer!important;box-shadow:0 8px 18px rgba(215,142,44,.28)!important;transform:none!important;transition:background .2s ease,border-color .2s ease,filter .15s ease!important}";
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

    // HEAD_CSS / WIDTH_BANDS pins filled after upload
    var HEAD_CSS = "6a6e13b2eb6dc0e3344cd21f_dealality-old-home-freeform-head.v20260801e.css";
    var HEROFIT_CSS = "6a6e0f42f5f1ad337d225404_dealality-old-home-hero-fit.v20260801b.css";
    var WIDTH_BANDS_CSS = "6a6e13b2de9a5a2f589890b9_dealality-old-home-width-bands.v20260801a.css";

    var critical = [
      ["dark", b + "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css"],
      ["freeform", b + "6a69c1a5f31a1ddd5b6b2158_dealality-old-home-freeform.v20260729benefits2.css"],
      ["benefits", b + "6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css"],
      ["perspectives", b + "6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css"],
      ["head", b + HEAD_CSS],
      ["platform", b + "6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css"],
      ["pricing", b + "6a6e13b3cf83f720d5d74b39_dealality-old-home-pricing.v20260801a.css"],
      ["herofit", b + HEROFIT_CSS],
      ["widthbands", b + WIDTH_BANDS_CSS],
    ];

    // FINAL nav SoT — must match dealality-old-home-nav-cleanup.v20260801a.js
    var NAV_ORDER = [
      { href: "#oh-how-we-do-it", label: "How It Works" },
      { href: "#modules", label: "Benefits" },
      { href: "#many-futures", label: "Platform" },
      { href: "#faq", label: "FAQs" },
      { href: "#insights", label: "Insights" },
    ];

    function ensureNavOrder() {
      // Stamp matches nav-cleanup 01a so footer/page cleanup no-ops (no second rebuild).
      if (window.__ohNavCleanup && window.__ohNavCleanup >= 202608011) return true;
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
          if (/^about$|^how we do it$|^how it works$|^benefits$|^platform$|^pricing$|^faq$|^faqs$|^insights$/i.test(text)) continue;
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
      window.__ohNavCleanup = 202608011;
      return true;
    }

    var cssReady = false;
    var shellReady = false;
    var fontsReady = false;
    var rotatorReady = html.classList.contains("oh-rotator-ready");
    var revealed = false;

    function reassertHeroLocks() {
      try {
        var exploreLock = d.getElementById("oh-fouc-gate-style");
        if (exploreLock && exploreLock.parentNode) {
          exploreLock.parentNode.appendChild(exploreLock);
        }
        var exploreCta = d.getElementById("oh-explore-cta-01c") || d.getElementById("oh-explore-cta-01b");
        if (exploreCta && exploreCta.parentNode) {
          exploreCta.parentNode.appendChild(exploreCta);
        }
      } catch (_re) {}
    }

    function reveal() {
      if (revealed) return;
      ensureNavOrder();
      applyEsHeroStatic();
      revealed = true;
      // Re-assert Explore/eyebrow locks at reveal so late freeform/demo/site-head cannot flash.
      reassertHeroLocks();
      html.classList.add("oh-ready");
      // Demo CSS (#oh-demo-css) often lands after oh-ready — re-pin locks.
      try {
        [0, 50, 160, 400].forEach(function (ms) {
          setTimeout(reassertHeroLocks, ms);
        });
      } catch (_t) {}
    }

    function maybeReveal() {
      rotatorReady = rotatorReady || html.classList.contains("oh-rotator-ready");
      if (cssReady && shellReady && fontsReady && rotatorReady) reveal();
    }

    if (d.fonts && d.fonts.ready) {
      d.fonts.ready
        .then(function () {
          fontsReady = true;
          maybeReveal();
        })
        .catch(function () {
          fontsReady = true;
          maybeReveal();
        });
    } else {
      fontsReady = true;
    }

    // Rotator (header/footer) marks oh-rotator-ready after first measure+paint.
    var rotMo = null;
    try {
      rotMo = new MutationObserver(function () {
        if (html.classList.contains("oh-rotator-ready")) {
          rotatorReady = true;
          maybeReveal();
          if (rotMo) rotMo.disconnect();
        }
      });
      rotMo.observe(html, { attributes: true, attributeFilter: ["class"] });
    } catch (_mo) {}

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
      applyEsHeroStatic();
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

    // Failsafe — never leave page invisible (give rotator/fonts time before faking ready)
    setTimeout(function () {
      ensureDemoShell();
      ensureNavOrder();
      cssReady = true;
      shellReady = true;
      fontsReady = true;
      rotatorReady = true;
      try {
        html.classList.add("oh-rotator-ready");
      } catch (_e) {}
      reveal();
    }, 2400);
  } catch (err) {
    try {
      document.documentElement.classList.add("oh-ready");
    } catch (_e) {}
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-fouc-gate]", err);
    }
  }
})();
