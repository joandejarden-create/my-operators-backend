/**
 * Dealality — Who It's For page mount (v20260801a)
 * Path-gated to /who-its-for. Mounts the role section formerly on /old-home#pricing.
 */
(function () {
  "use strict";
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/who-its-for") return;
    if (window.__ohWhoItsFor >= 202608011) return;
    window.__ohWhoItsFor = 202608011;

    var html = document.documentElement;
    html.classList.add("oh-wif", "dl-public-no-loader", "oh-boot", "oh-ready");

    var CDN =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/";
    var HOME = "https://www.dealality.com/old-home";
    var WORDMARK =
      CDN + "6a67ed8bb2f335717dc3b820_dealality-wordmark-nav.png";

    var CSS = [
      CDN + "6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css",
      CDN + "6a6dfebd6d7424a20895415b_dealality-old-home-freeform-head.v20260801c.css",
      CDN + "6a6dc763d2843e12335fae98_dealality-old-home-pricing.v20260730g.css",
      CDN + "6a6d4d297aebfdd31871d780_dealality-old-home-section-type.v20260801a.css",
    ];

    function ensureLink(href) {
      if (document.querySelector('link[href="' + href + '"]')) return;
      var l = document.createElement("link");
      l.rel = "stylesheet";
      l.href = href;
      (document.head || html).appendChild(l);
    }

    function ensureFonts() {
      if (document.querySelector('link[data-oh-wif-fonts="1"]')) return;
      var fl = document.createElement("link");
      fl.rel = "stylesheet";
      fl.setAttribute("data-oh-wif-fonts", "1");
      fl.href =
        "https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap";
      (document.head || html).appendChild(fl);
    }

    function ensureBootCss() {
      if (document.getElementById("oh-wif-boot-css")) return;
      var st = document.createElement("style");
      st.id = "oh-wif-boot-css";
      st.textContent =
        "html.oh-wif,html.oh-wif body{background:#080F25!important;color:#aeb9e1!important;margin:0!important}" +
        "html.oh-wif .page-loader,html.oh-wif .loading-bar-wrapper,html.oh-wif .coming-soon-overlay,html.oh-wif .coming-soon-ss{display:none!important}" +
        "#oh-wif-page{min-height:100vh;background:#080F25}" +
        "#oh-wif-nav{position:sticky;top:0;z-index:40;display:flex;align-items:center;justify-content:space-between;gap:1rem;padding:1rem clamp(1.25rem,4vw,2.5rem);background:rgba(8,15,37,.92);border-bottom:1px solid rgba(255,255,255,.08);backdrop-filter:blur(10px)}" +
        "#oh-wif-logo img{display:block;height:28px;width:auto}" +
        "#oh-wif-nav-links{display:flex;align-items:center;gap:1rem;flex-wrap:wrap}" +
        "#oh-wif-nav-links a{color:#fff!important;text-decoration:none;font:500 .92rem/1 \"Inter Tight\",system-ui,sans-serif;opacity:.92}" +
        "#oh-wif-nav-links a:hover{opacity:1}" +
        "#oh-wif-main{padding-top:1rem}" +
        "#oh-wif-page #pricing{display:block!important;visibility:visible!important;height:auto!important;overflow:visible!important}" +
        "#oh-wif-page #footer-new a[href^=\"#\"]{pointer-events:auto}";
      (document.head || html).appendChild(st);
    }

    var PRICING_HTML =
      '<section id="pricing" aria-labelledby="pricing-h2"><div id="pricing-inner">' +
      '<div id="pricing-badges"><div id="pricing-badge">' +
      '<span id="pricing-badge-left">Choose Your Starting Point</span>' +
      '<span id="pricing-badge-right">We Will Confirm the Scope Before You Commit.</span>' +
      "</div></div>" +
      '<h2 id="pricing-h2">Start with the role that fits you.</h2>' +
      '<div id="pricing-grid">' +
      '<article id="pricing-card-owners"><div id="pricing-owners-icon" aria-hidden="true"></div>' +
      '<h3 id="pricing-owners-title">Owners &amp; Developers</h3>' +
      '<p id="pricing-owners-desc">Asset-side decision makers</p>' +
      '<p id="pricing-owners-price"><strong>Success-Based</strong><span>No upfront platform fee. A fee applies only when the opportunity reaches the success milestone agreed in advance.</span></p>' +
      '<ul id="pricing-owners-features" role="list"><li>Confidential opportunity review</li><li>Strategic path and partner research</li><li>Opportunity packaging and selected outreach</li><li>Proposal comparison</li><li>Negotiation and decision support</li><li>Support through the agreed milestone</li></ul>' +
      '<a id="pricing-owners-cta" href="https://www.dealality.com/opportunity-review">Explore Your Opportunity</a></article>' +
      '<article id="pricing-card-brands"><div id="pricing-brands-icon" aria-hidden="true"></div>' +
      "<h3 id=\"pricing-brands-title\">Brands</h3>" +
      '<p id="pricing-brands-desc">Franchise and development teams</p>' +
      '<p id="pricing-brands-price"><strong>Annual Access</strong><span>Subscription access based on market coverage, users, and participation needs.</span></p>' +
      '<ul id="pricing-brands-features" role="list"><li>Brand profile and opportunity criteria</li><li>Relevant owner-led opportunity review</li><li>Structured project context</li><li>Permission-based deal-room participation</li><li>Proposal submission workflow</li><li>Response and pipeline tracking</li></ul>' +
      '<a id="pricing-brands-cta" href="mailto:joan@dealality.com?subject=Dealality%20Brand%20Access">Request Brand Access</a></article>' +
      '<article id="pricing-card-operators"><div id="pricing-operators-icon" aria-hidden="true"></div>' +
      "<h3 id=\"pricing-operators-title\">Operators</h3>" +
      '<p id="pricing-operators-desc">Hotel management companies</p>' +
      '<p id="pricing-operators-price"><strong>Annual Access</strong><span>Subscription access based on operating footprint, markets served, users, and participation needs.</span></p>' +
      '<ul id="pricing-operators-features" role="list"><li>Operator profile and opportunity criteria</li><li>Relevant owner-led opportunity review</li><li>Structured owner and asset context</li><li>Permission-based deal-room participation</li><li>Management-term submission workflow</li><li>Response and pipeline tracking</li></ul>' +
      '<a id="pricing-operators-cta" href="mailto:joan@dealality.com?subject=Dealality%20Operator%20Access">Request Operator Access</a></article>' +
      "</div>" +
      '<div id="pricing-terms">' +
      '<p id="pricing-term-owners">Access and engagement terms vary by opportunity size, complexity, and scope.</p>' +
      '<p id="pricing-term-brands">Subscription access does not guarantee a minimum volume of opportunities.</p>' +
      '<p id="pricing-term-operators">Subscription access does not guarantee a minimum volume of opportunities.</p>' +
      "</div></div></section>";

    var FOOTER_HTML =
      '<footer id="footer-new"><div id="footer-inner-new"><div id="footer-grid-new">' +
      '<div id="footer-col-company"><a id="footer-logo" aria-label="Dealality home" href="' +
      HOME +
      '" class="w-inline-block"><img src="' +
      WORDMARK +
      '" loading="lazy" alt="Dealality"/></a>' +
      '<p id="footer-tagline">ONE HOTEL.<br/>BETTER DECISIONS.<br/>MORE VALUE.</p>' +
      '<p id="footer-blurb">Helping hotel owners discover the strategic paths that create the greatest value before they commit.</p>' +
      "</div>" +
      '<div id="footer-col-products"><h3 id="footer-h-products">Platform</h3>' +
      '<a href="https://www.dealality.com/opportunity-review">Opportunity Assessment</a>' +
      '<a href="' +
      HOME +
      '#oh-how-we-do-it">Strategic Paths</a>' +
      '<a href="' +
      HOME +
      '#modules">Brand Intelligence</a>' +
      '<a href="' +
      HOME +
      '#modules">Operator Intelligence</a></div>' +
      '<div id="footer-col-resources"><h3 id="footer-h-resources">Learn</h3>' +
      '<a href="' +
      HOME +
      '#insights">Insights</a>' +
      '<a href="/who-its-for" aria-current="page">Who It\'s For</a>' +
      '<a href="' +
      HOME +
      '#faq">FAQs</a>' +
      '<a href="mailto:joan@dealality.com?subject=Dealality%20for%20Advisors">For Advisors</a></div>' +
      '<div id="footer-col-links"><h3 id="footer-h-links">Company</h3>' +
      '<a href="' +
      HOME +
      '#about">About</a>' +
      '<a href="' +
      HOME +
      '#oh-how-we-do-it">How It Works</a>' +
      '<a href="mailto:joan@dealality.com">Contact</a>' +
      '<a href="https://www.dealality.com/privacy">Privacy</a>' +
      '<a href="https://www.dealality.com/terms">Terms</a></div>' +
      "</div>" +
      '<p id="footer-copy-new">© 2026 Dealality. A platform of AO Hospitality Advisors, LLC.</p>' +
      "</div></footer>";

    function mount() {
      if (document.getElementById("oh-wif-page")) return;
      ensureFonts();
      ensureBootCss();
      CSS.forEach(ensureLink);

      var host = document.createElement("div");
      host.id = "oh-wif-page";
      host.innerHTML =
        '<header id="oh-wif-nav">' +
        '<a id="oh-wif-logo" href="' +
        HOME +
        '" aria-label="Dealality home"><img src="' +
        WORDMARK +
        '" alt="Dealality"/></a>' +
        '<div id="oh-wif-nav-links">' +
        '<a href="' +
        HOME +
        '">Home</a>' +
        '<a href="' +
        HOME +
        '#oh-how-we-do-it">How It Works</a>' +
        '<a href="https://www.dealality.com/opportunity-review">Opportunity Review</a>' +
        '<a href="https://www.dealality.com/signup">Request Access</a>' +
        "</div></header>" +
        '<main id="oh-wif-main">' +
        PRICING_HTML +
        "</main>" +
        FOOTER_HTML;

      var body = document.body || html;
      body.insertBefore(host, body.firstChild);

      // Hide leftover Webflow canvas chrome on this blank page
      Array.prototype.forEach.call(body.children, function (el) {
        if (el && el.id !== "oh-wif-page") {
          el.setAttribute("data-oh-wif-hide", "1");
          el.style.display = "none";
        }
      });
    }

    function loadPricingEnhance() {
      if (document.querySelector('script[data-oh-wif-pricing="1"]')) return;
      var s = document.createElement("script");
      s.src = CDN + "6a6e011a7be4790556f2607b_dealality-old-home-pricing.v20260801a.js";
      s.defer = true;
      s.setAttribute("data-oh-wif-pricing", "1");
      (document.body || html).appendChild(s);
    }

    function loadExploreCta() {
      if (document.querySelector('script[data-oh-wif-explore="1"]')) return;
      var s = document.createElement("script");
      s.src = CDN + "6a6e011a78d1ce1e62697d4b_old-home-explore-cta.v20260801d.js";
      s.defer = true;
      s.setAttribute("data-oh-wif-explore", "1");
      (document.body || html).appendChild(s);
    }

    mount();
    // Pricing enhance is path-gated; load patched build after upload (placeholder swapped).
    loadPricingEnhance();
    loadExploreCta();
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-who-its-for]", err);
    }
  }
})();
