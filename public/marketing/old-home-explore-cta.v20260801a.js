/**
 * Old Home — unify Explore / Opportunity Review CTAs (v20260801a)
 * Reference: How We Do It “One Opportunity. One Connected Process.”
 *   .dealality-process_btn.dealality-process_btn-primary
 * Shape: 10px radius, min-height 40px, solid #6C72FF, no pill/shadow.
 * Click: ohOpenOpportunityReview(url, label) → same as Connected Process.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohExploreCta >= 202608011) return;
    window.__ohExploreCta = 202608011;

    var STYLE_ID = "oh-explore-cta-01a";
    var OR_URL = "https://www.dealality.com/opportunity-review";
    var LABEL = "Explore Your Opportunity";

    var SELECTORS = [
      "#fsw-btn",
      "#pricing-owners-cta",
      "#cta-band-btn",
      'a[href*="opportunity-review"].oh-mnav-link',
      "#mnav a[href*=\"opportunity-review\"]",
      '[data-dealality-process-cta="explore"]',
    ];

    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        /* Match Connected Process primary explore button */
        "#fsw-btn,#fsw-btn.oh-fsw-btn,#fsw-btn.fsw-demo-cta,",
        "#pricing-owners-cta,",
        "#cta-band-btn,#cta-band-btn.oh-cta-band-btn,",
        '#mnav a[href*="opportunity-review"],',
        "#oh-how-we-do-it .dealality-process_btn-primary[data-dealality-process-cta=\"explore\"]{",
        "display:inline-flex!important;",
        "align-items:center!important;",
        "justify-content:center!important;",
        "box-sizing:border-box!important;",
        "min-height:40px!important;",
        "height:auto!important;",
        "max-height:none!important;",
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
        "#fsw-btn:hover,#fsw-btn.oh-fsw-btn:hover,",
        "#pricing-owners-cta:hover,",
        "#cta-band-btn:hover,#cta-band-btn.oh-cta-band-btn:hover,",
        '#mnav a[href*="opportunity-review"]:hover,',
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
        /* Pricing owners: keep full column width but same chrome */
        "#pricing-owners-cta{",
        "width:100%!important;",
        "white-space:normal!important;",
        "text-align:center!important;",
        "}",
        /* FOUC / hero wrap: stop forcing pill height on Explore */
        "#fsw-btn-wrap .oh-fsw-btn,#fsw-btn-wrap #fsw-btn{",
        "min-height:40px!important;",
        "height:auto!important;",
        "max-height:none!important;",
        "border-radius:10px!important;",
        "padding:0 14px!important;",
        "font-size:.86rem!important;",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    function openExplore(url, label) {
      var href = url || OR_URL;
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
      /* Also catch plain Explore copy on opportunity-review anchors */
      document.querySelectorAll('a[href*="opportunity-review"]').forEach(function (a) {
        var t = (a.textContent || "").replace(/\s+/g, " ").trim();
        if (/^Explore Your (Hotel )?Opportunity$/i.test(t) || /^Start an Opportunity Review$/i.test(t)) {
          bindOne(a);
        }
      });
    }

    function boot() {
      bindAll();
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    window.setTimeout(boot, 0);
    window.setTimeout(boot, 400);
    window.setTimeout(boot, 1200);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-explore-cta]", err);
    }
  }
})();
