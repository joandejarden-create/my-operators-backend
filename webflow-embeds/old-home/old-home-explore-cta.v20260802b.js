/** v20260802b: rewrite /es hrefs after DOM ready; force locale OR URL into iframe opener. */
/**
 * Old Home — unify Explore / Opportunity Review CTAs (v20260802a)
 * 01d: also runs on /who-its-for (role page).
 * Reference: How We Do It “One Opportunity. One Connected Process.”
 *   .dealality-process_btn.dealality-process_btn-primary
 * Shape: 10px radius, height 40px hard lock, solid #6C72FF, no pill/shadow.
 * Click: ohOpenOpportunityReview(url, label) → same as Connected Process.
 * 01c: height:40px (not auto) so stretch/demo cannot twitch size.
 * 01b: re-append styles after #oh-demo-css so hero Explore stays 10px.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && path !== "/old-home" && path !== "/who-its-for" && !isEs) return;
    if (window.__ohExploreCta >= 202608021) return;
    window.__ohExploreCta = 202608021;

    var STYLE_ID = "oh-explore-cta-02a";
    var OR_URL = isEs ? "/es/opportunity-review" : "/opportunity-review";
    var LABEL = isEs ? "Explora Tu Oportunidad" : "Explore Your Opportunity";

    // Locale-correct absolute EN links on /es (nav + footer CTAs). Must run after DOM exists.
    function rewriteLocaleHrefs() {
      if (!isEs) return;
      [
        ["#nav-cta", "/es/signup"],
        ["#nav-signin", "/es/login"],
        ['a[href*="/signup"]', "/es/signup"],
        ['a[href*="/login"]', "/es/login"],
        ['a[href*="opportunity-review"]', OR_URL],
      ].forEach(function (pair) {
        document.querySelectorAll(pair[0]).forEach(function (a) {
          try {
            var href = a.getAttribute("href") || "";
            if (!href || href.charAt(0) === "#") return;
            if (/opportunity-review/i.test(pair[0]) || /opportunity-review/i.test(href)) {
              a.setAttribute("href", OR_URL);
              return;
            }
            if (/\/signup/i.test(href) && pair[1].indexOf("signup") !== -1) a.setAttribute("href", "/es/signup");
            if (/\/login/i.test(href) && pair[1].indexOf("login") !== -1) a.setAttribute("href", "/es/login");
          } catch (err) {}
        });
      });
    }

    var SELECTORS = [
      "#fsw-btn",
      "#pricing-owners-cta",
      "#cta-band-btn",
      'a[href*="opportunity-review"].oh-mnav-link',
      '#mnav a[href*="opportunity-review"]',
      '[data-dealality-process-cta="explore"]',
    ];

    var CSS = [
      /* Match Connected Process primary explore button — beat #oh-demo-css pills */
      "#hero #fsw-btn,",
      "#hero #fsw-btn.oh-fsw-btn,",
      "#fsw-btn,",
      "#fsw-btn.oh-fsw-btn,",
      "#fsw-btn.fsw-demo-cta,",
      "#fsw-btn-wrap #fsw-btn,",
      "#fsw-btn-wrap .oh-fsw-btn,",
      "#fsw-btn-wrap #fsw-btn.oh-fsw-btn,",
      "#pricing-owners-cta,",
      "#cta-band-btn,",
      "#cta-band-btn.oh-cta-band-btn,",
      '#mnav a[href*="opportunity-review"],',
      ".dealality-process_btn.dealality-process_btn-primary[data-dealality-process-cta=\"explore\"],",
      "#oh-how-we-do-it .dealality-process_btn-primary[data-dealality-process-cta=\"explore\"]{",
      "display:inline-flex!important;",
      "align-items:center!important;",
      "justify-content:center!important;",
      "align-self:center!important;",
      "box-sizing:border-box!important;",
      "min-height:40px!important;",
      "height:40px!important;",
      "max-height:40px!important;",
      "padding:0 14px!important;",
      "margin:0!important;",
      "border:0!important;",
      "border-radius:10px!important;",
      "background:#6C72FF!important;",
      "background-image:none!important;",
      "color:#fff!important;",
      "font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;",
      "font-size:.86rem!important;",
      "font-weight:700!important;",
      "letter-spacing:normal!important;",
      "line-height:1.2!important;",
      "text-decoration:none!important;",
      "box-shadow:none!important;",
      "white-space:nowrap!important;",
      "cursor:pointer!important;",
      "transform:none!important;",
      "filter:none!important;",
      "overflow:visible!important;",
      "}",
      "#hero #fsw-btn:hover,",
      "#fsw-btn:hover,#fsw-btn.oh-fsw-btn:hover,",
      "#fsw-btn-wrap #fsw-btn:hover,#fsw-btn-wrap .oh-fsw-btn:hover,",
      "#pricing-owners-cta:hover,",
      "#cta-band-btn:hover,#cta-band-btn.oh-cta-band-btn:hover,",
      '#mnav a[href*="opportunity-review"]:hover,',
      ".dealality-process_btn.dealality-process_btn-primary[data-dealality-process-cta=\"explore\"]:hover,",
      "#oh-how-we-do-it .dealality-process_btn-primary[data-dealality-process-cta=\"explore\"]:hover{",
      "background:#7B80FF!important;",
      "color:#fff!important;",
      "filter:none!important;",
      "transform:none!important;",
      "box-shadow:none!important;",
      "}",
      "#fsw-btn-text,#cta-band-btn-text{",
      "color:#fff!important;",
      "font:inherit!important;",
      "}",
      "#fsw-btn-grad{display:none!important}",
      "#fsw-btn-wrap.oh-fsw-btn-wrap,#fsw-btn-wrap{",
      "align-items:center!important;",
      "}",
      "#pricing-owners-cta{",
      "width:100%!important;",
      "white-space:normal!important;",
      "text-align:center!important;",
      "height:auto!important;",
      "max-height:none!important;",
      "min-height:40px!important;",
      "}",
    ].join("");

    function ensureStyle() {
      var head = document.head || document.documentElement;
      var st = document.getElementById(STYLE_ID);
      if (!st) {
        st = document.createElement("style");
        st.id = STYLE_ID;
        st.textContent = CSS;
      }
      /* Always move to end so we win over late #oh-demo-css / freeform pills */
      head.appendChild(st);
      var oldC = document.getElementById("oh-explore-cta-01c");
      if (oldC && oldC.parentNode) oldC.parentNode.removeChild(oldC);
      var oldB = document.getElementById("oh-explore-cta-01b");
      if (oldB && oldB.parentNode) oldB.parentNode.removeChild(oldB);
      var oldA = document.getElementById("oh-explore-cta-01a");
      if (oldA && oldA.parentNode) oldA.parentNode.removeChild(oldA);
    }

    function openExplore(url, label) {
      var href = url || OR_URL;
      // Never open the English absolute OR URL from /es — keep iframe locale-correct.
      if (isEs && /opportunity-review/i.test(href)) href = OR_URL;
      var lab = label || LABEL;
      if (typeof window.ohOpenOpportunityReview === "function") {
        window.ohOpenOpportunityReview(href, lab);
        return;
      }
      window.location.href = href;
    }

    function labelFor(el) {
      if (!el) return LABEL;
      var t = (el.textContent || "").replace(/\s+/g, " ").trim();
      return t || LABEL;
    }

    function bindOne(el) {
      if (!el || el.getAttribute("data-oh-explore-bound") === "1") return;
      el.setAttribute("data-oh-explore-bound", "1");
      el.classList.add("oh-explore-cta");
      el.addEventListener(
        "click",
        function (e) {
          var href = el.getAttribute("href") || OR_URL;
          if (el.tagName === "A" && href && href.indexOf("opportunity-review") === -1) {
            return;
          }
          e.preventDefault();
          e.stopPropagation();
          openExplore(
            href.indexOf("http") === 0 ? href : OR_URL,
            labelFor(el)
          );
        },
        true
      );
    }

    function bindAll() {
      SELECTORS.forEach(function (sel) {
        try {
          document.querySelectorAll(sel).forEach(bindOne);
        } catch (_e) {}
      });
      document.querySelectorAll('a[href*="opportunity-review"]').forEach(function (a) {
        var t = (a.textContent || "").replace(/\s+/g, " ").trim();
        if (
          /^Explore Your (Hotel )?Opportunity$/i.test(t) ||
          /^Start an Opportunity Review$/i.test(t)
        ) {
          bindOne(a);
        }
      });
    }

    function boot() {
      rewriteLocaleHrefs();
      ensureStyle();
      bindAll();
    }

    ensureStyle();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    [0, 400, 1200, 2500].forEach(function (ms) {
      window.setTimeout(boot, ms);
    });

    /* If Request-a-Demo CSS injects later, re-assert Explore chrome */
    try {
      var mo = new MutationObserver(function (muts) {
        for (var i = 0; i < muts.length; i++) {
          var nodes = muts[i].addedNodes || [];
          for (var j = 0; j < nodes.length; j++) {
            var n = nodes[j];
            if (n && n.id === "oh-demo-css") {
              ensureStyle();
              return;
            }
          }
        }
      });
      mo.observe(document.documentElement, { childList: true, subtree: true });
      window.setTimeout(function () {
        try {
          mo.disconnect();
        } catch (_e) {}
      }, 8000);
    } catch (_e2) {}
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-explore-cta]", err);
    }
  }
})();
