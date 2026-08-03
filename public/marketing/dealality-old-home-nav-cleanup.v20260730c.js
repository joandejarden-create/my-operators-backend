/**
 * Old Home nav cleanup (v20260730c)
 * Path-gated to /old-home.
 * - Pure white nav link color
 * - Fix Pricing (missing class + dead href)
 * - Order: How It Works → Benefits → About → FAQ → Insights → Pricing
 * - About → #trust (testimonials / about Dealality)
 * - How It Works → #oh-how-we-do-it (How Dealality Works section)
 */
(function () {
  try {
    var path = (window.location && window.location.pathname) || "";
    if (path !== "/old-home") return;
    if (window.__ohNavCleanup && window.__ohNavCleanup >= 202607303) return;
    window.__ohNavCleanup = 202607303;

    var ORDER = [
      { href: "#oh-how-we-do-it", label: "How It Works" },
      { href: "#modules", label: "Benefits" },
      { href: "#trust", label: "About" },
      { href: "#faq", label: "FAQ" },
      { href: "#insights", label: "Insights" },
      { href: "#pricing", label: "Pricing" },
    ];

    function injectStyles() {
      if (document.getElementById("oh-nav-cleanup-30c")) return;
      var style = document.createElement("style");
      style.id = "oh-nav-cleanup-30c";
      style.textContent = [
        "#nav-links.oh-nav-links a,",
        "#nav-links.oh-nav-links a.oh-nav-link,",
        ".oh-nav-links a,",
        ".oh-nav-link{",
        "color:#fff!important;",
        "opacity:1!important;",
        "font-weight:500!important;",
        "letter-spacing:-.01em!important;",
        "text-decoration:none!important;",
        "}",
        "#nav-links.oh-nav-links a:hover,",
        ".oh-nav-links a:hover,",
        ".oh-nav-link:hover{color:#fff!important;opacity:.88!important}",
        "#mnav.oh-mnav a.oh-mnav-link,",
        ".oh-mnav-link{",
        "color:#fff!important;",
        "opacity:1!important;",
        "font-weight:500!important;",
        "}",
        "#mnav.oh-mnav a.oh-mnav-link:hover,",
        ".oh-mnav-link:hover{color:#fff!important;opacity:.88!important}",
      ].join("");
      (document.head || document.documentElement).appendChild(style);
    }

    function rebuildDesktop() {
      var host = document.getElementById("nav-links");
      if (!host) return false;
      host.innerHTML = "";
      for (var i = 0; i < ORDER.length; i++) {
        var a = document.createElement("a");
        a.href = ORDER[i].href;
        a.className = "oh-nav-link";
        a.textContent = ORDER[i].label;
        host.appendChild(a);
      }
      return true;
    }

    function rebuildMobile() {
      var host = document.getElementById("mnav");
      if (!host) return false;
      var extras = [];
      var kids = host.querySelectorAll("a");
      for (var i = 0; i < kids.length; i++) {
        var href = kids[i].getAttribute("href") || "";
        var text = (kids[i].textContent || "").trim();
        if (href.indexOf("#") === 0) continue;
        if (/^about$|^how we do it$|^how it works$|^benefits$|^pricing$|^faq$|^insights$/i.test(text)) continue;
        extras.push({ href: href, label: text, className: kids[i].className || "oh-mnav-link" });
      }
      host.innerHTML = "";
      for (var j = 0; j < ORDER.length; j++) {
        var a = document.createElement("a");
        a.href = ORDER[j].href;
        a.className = "oh-mnav-link";
        a.textContent = ORDER[j].label;
        host.appendChild(a);
      }
      for (var k = 0; k < extras.length; k++) {
        var x = document.createElement("a");
        x.href = extras[k].href;
        x.className = extras[k].className || "oh-mnav-link";
        x.textContent = extras[k].label;
        host.appendChild(x);
      }
      return true;
    }

    function run() {
      injectStyles();
      rebuildDesktop();
      rebuildMobile();
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-nav-cleanup]", err);
    }
  }
})();
