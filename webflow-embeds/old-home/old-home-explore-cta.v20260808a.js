/** v20260808a: break /es MutationObserver loop — only set href when changed; reentrancy lock; narrow rewrite triggers. */
/**
 * Old Home — unify Explore / Opportunity Review CTAs (v20260802a)
 * 01d: also runs on /who-its-for (role page).
 * Reference: How We Do It “One Opportunity. One Connected Process.”
 *   .dealality-process_btn.dealality-process_btn-primary
 * Shape: 10px radius, height 40px hard lock, solid #6C72FF, no pill/shadow.
 * Click: ohOpenOpportunityReview(url, label) → same as Connected Process.
 * 01c: height:40px (not auto) so stretch/demo cannot twitch size.
 * 01b: re-append styles after #oh-demo-css so hero Explore stays 10px.
 * 02d: rewrite /es hrefs after DOM ready; force locale OR URL into iframe opener.
 * 02e: /es blank-page fix — MO was setAttribute-looping on every href rewrite.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && path !== "/old-home" && path !== "/who-its-for" && !isEs) return;
    if (window.__ohExploreCta >= 2026080801) return;
    window.__ohExploreCta = 2026080801;

    var STYLE_ID = "oh-explore-cta-02a";
    var OR_URL = isEs ? "/es/opportunity-review" : "/opportunity-review";
    var LABEL = isEs ? "Explora Tu Oportunidad" : "Explore your opportunity";
    var LABEL_HOTEL = isEs ? "Explora Tu Oportunidad Hotelera" : "Explore your hotel opportunity";
    var ARIA_FORM = isEs ? "Explora tu oportunidad hotelera" : "Explore your hotel opportunity";
    var rewriting = false;

    function setHrefIfChanged(a, next) {
      if (!a || !next) return false;
      var cur = a.getAttribute("href") || "";
      if (cur === next) return false;
      a.setAttribute("href", next);
      return true;
    }

    function rewriteExploreLabels() {
      if (!isEs) return;
      var pairs = [
        ["#fsw-btn-text", LABEL_HOTEL],
        ["#pricing-owners-cta", LABEL],
        ['a.oh-mnav-link[href*="opportunity-review"]', LABEL],
        ['#mnav a[href*="opportunity-review"]', LABEL],
        ['[data-dealality-process-cta="explore"]', LABEL],
      ];
      pairs.forEach(function (pair) {
        try {
          document.querySelectorAll(pair[0]).forEach(function (el) {
            var cur = (el.textContent || "").replace(/\s+/g, " ").trim();
            if (
              !cur ||
              /^Explore Your/i.test(cur) ||
              /^Start an Opportunity Review$/i.test(cur) ||
              /^Explora Tu Oportunidad/i.test(cur)
            ) {
              if (cur !== pair[1]) el.textContent = pair[1];
            }
          });
        } catch (_e) {}
      });
      try {
        document.querySelectorAll("[aria-label]").forEach(function (el) {
          var a = el.getAttribute("aria-label") || "";
          if (/^Explore your hotel opportunity$/i.test(a) || /^Explore your hotel opportunity$/i.test(a)) {
            el.setAttribute("aria-label", ARIA_FORM);
          }
        });
      } catch (_e2) {}
    }


    // Locale-correct absolute EN links on /es (nav + footer CTAs). Must run after DOM exists.
    function rewriteLocaleHrefs() {
      if (!isEs || rewriting) return;
      rewriting = true;
      try {
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
              // Explicit id targets always win (auth chrome may restore absolute EN URLs).
              if (pair[0] === "#nav-cta") {
                setHrefIfChanged(a, "/es/signup");
                return;
              }
              if (pair[0] === "#nav-signin") {
                setHrefIfChanged(a, "/es/login");
                return;
              }
              if (/opportunity-review/i.test(pair[0]) || /opportunity-review/i.test(href)) {
                setHrefIfChanged(a, OR_URL);
                return;
              }
              if (/\/signup/i.test(href) && pair[1].indexOf("signup") !== -1) {
                setHrefIfChanged(a, "/es/signup");
              }
              if (/\/login/i.test(href) && pair[1].indexOf("login") !== -1) {
                setHrefIfChanged(a, "/es/login");
              }
            } catch (err) {}
          });
        });
      } finally {
        rewriting = false;
      }
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
      if (isEs) {
        if (/hotel/i.test(t) || el.id === "fsw-btn" || (el.querySelector && el.querySelector("#fsw-btn-text"))) {
          return LABEL_HOTEL;
        }
        if (/^Explore Your/i.test(t) || /^Start an Opportunity Review$/i.test(t) || !t) {
          return LABEL;
        }
      }
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
      rewriteExploreLabels();
      ensureStyle();
      bindAll();
    }

    function nodeNeedsLocaleRewrite(n) {
      if (!n || n.nodeType !== 1) return false;
      if (n.id === "nav-cta" || n.id === "nav-signin") return true;
      if (n.classList && n.classList.contains("cta-button")) return true;
      var tag = (n.tagName || "").toLowerCase();
      if (tag === "a") {
        var href = n.getAttribute("href") || "";
        return /signup|login|opportunity-review/i.test(href);
      }
      try {
        return !!(
          n.querySelector &&
          n.querySelector(
            '#nav-cta, #nav-signin, a[href*="signup"], a[href*="login"], a[href*="opportunity-review"]'
          )
        );
      } catch (_e) {
        return false;
      }
    }

    ensureStyle();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    [0, 400, 1200, 2500, 5000, 8000].forEach(function (ms) {
      window.setTimeout(boot, ms);
    });

    /* Re-assert Explore chrome + locale hrefs when auth/demo scripts mutate DOM */
    try {
      var mo = new MutationObserver(function (muts) {
        if (rewriting) return;
        var needStyle = false;
        var needRewrite = false;
        for (var i = 0; i < muts.length; i++) {
          var m = muts[i];
          if (m.type === "attributes" && m.attributeName === "href") {
            var t = m.target;
            if (
              t &&
              (t.id === "nav-cta" ||
                t.id === "nav-signin" ||
                (t.classList && t.classList.contains("cta-button")))
            ) {
              // Only rewrite when auth chrome restored a non-/es href.
              var h = t.getAttribute("href") || "";
              if (isEs && h && h.indexOf("/es/") !== 0 && h !== "/es") {
                needRewrite = true;
              }
            }
          }
          var nodes = m.addedNodes || [];
          for (var j = 0; j < nodes.length; j++) {
            var n = nodes[j];
            if (n && n.id === "oh-demo-css") needStyle = true;
            if (nodeNeedsLocaleRewrite(n)) needRewrite = true;
          }
        }
        if (needStyle) ensureStyle();
        if (needRewrite) rewriteLocaleHrefs();
      });
      mo.observe(document.documentElement, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["href"],
      });
      window.setTimeout(function () {
        try {
          mo.disconnect();
        } catch (_e) {}
      }, 12000);
    } catch (_e2) {}
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-explore-cta]", err);
    }
  }
})();
