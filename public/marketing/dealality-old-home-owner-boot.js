/**
 * Dealality Old Home — owner-first inject for Webflow /old-home.
 * Hides legacy multi-audience sections and injects the owner narrative.
 * Does not alter the live `/` Railway iframe homepage.
 */
(function () {
  "use strict";

  var API_BASE =
    (window.DEALALITY_API_BASE ||
      "https://my-operators-backend-production.up.railway.app").replace(/\/$/, "");
  var FRAGMENT_URL =
    API_BASE + "/marketing/dealality-old-home-owner.html?v=20260728a";
  var CSS_URL =
    API_BASE + "/marketing/dealality-old-home-owner.css?v=20260728a";
  var OPP_URL = "https://www.dealality.com/opportunity-review";
  var EVENTS_URL = API_BASE + "/api/marketing/landing-events";
  var ROOT_ID = "dc-owner-root";
  var LEGACY_IDS = [
    "nav",
    "mnav",
    "hero",
    "hero-overview-wrap",
    "proofbar",
    "problem",
    "how",
    "audiences",
    "why",
    "faq",
    "cta",
    "footer",
  ];

  function loadCss() {
    if (document.getElementById("dc-owner-css")) return;
    var link = document.createElement("link");
    link.id = "dc-owner-css";
    link.rel = "stylesheet";
    link.href = CSS_URL;
    document.head.appendChild(link);
  }

  function hideLegacy() {
    LEGACY_IDS.forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      el.setAttribute("data-dc-legacy-hidden", "1");
      el.style.display = "none";
      el.setAttribute("aria-hidden", "true");
    });
    // Hide leftover visual embeds that are not section-rooted
    document.querySelectorAll("#dc-page > .w-embed").forEach(function (el) {
      if (el.querySelector && el.querySelector("#" + ROOT_ID)) return;
      // Keep first style-only embeds; hide mesh/video-only embeds after inject
    });
  }

  function track(event, extra) {
    try {
      var payload = Object.assign(
        {
          event: event,
          surface: "old_home_owner",
          path: location.pathname + location.search,
          device:
            window.innerWidth < 768
              ? "mobile"
              : window.innerWidth < 1024
                ? "tablet"
                : "desktop",
          referrer: document.referrer || "",
          landingVersion: "old-home-owner-v1",
        },
        extra || {}
      );
      var body = JSON.stringify(payload);
      if (navigator.sendBeacon) {
        navigator.sendBeacon(
          EVENTS_URL,
          new Blob([body], { type: "application/json" })
        );
      } else {
        fetch(EVENTS_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: body,
          credentials: "omit",
          keepalive: true,
        }).catch(function () {});
      }
    } catch (_e) {}
  }

  function rewriteOppLinks(root) {
    root.querySelectorAll('a[href="/opportunity-review"], a[href*="opportunity-review"]').forEach(function (a) {
      a.setAttribute("href", OPP_URL);
    });
  }

  function bindUi(root) {
    var nmenu = root.querySelector("#nmenu");
    var mnav = root.querySelector("#mnav");
    function setMnav(open) {
      if (!mnav || !nmenu) return;
      if (open) mnav.removeAttribute("hidden");
      else mnav.setAttribute("hidden", "");
      nmenu.setAttribute("aria-expanded", open ? "true" : "false");
      document.body.style.overflow = open ? "hidden" : "";
    }
    if (nmenu) {
      nmenu.addEventListener("click", function () {
        setMnav(mnav.hasAttribute("hidden"));
        track("mobile_nav_open", { section: "nav" });
      });
    }
    if (mnav) {
      mnav.querySelectorAll("a").forEach(function (a) {
        a.addEventListener("click", function () {
          setMnav(false);
        });
      });
    }

    root.querySelectorAll(".dc-faq-q").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var item = btn.closest(".dc-faq-item");
        var open = item.classList.toggle("is-open");
        btn.setAttribute("aria-expanded", open ? "true" : "false");
        if (open) {
          var q = (btn.textContent || "").trim().slice(0, 64);
          track("homepage_faq_open", { label: q, section: "faq" });
          track("faq_open", { label: q, section: "faq" });
        }
      });
      btn.setAttribute("aria-expanded", "false");
    });

    root.addEventListener("click", function (ev) {
      var t = ev.target && ev.target.closest && ev.target.closest("a,button");
      if (!t) return;
      var cta = t.getAttribute("data-dc-cta");
      if (cta === "primary") {
        track("homepage_primary_cta_click", {
          location: t.getAttribute("data-dc-loc") || "unknown",
          section: "cta",
        });
        track("cta_click", {
          location: t.getAttribute("data-dc-loc") || "unknown",
          label: "discuss_your_hotel_opportunity",
        });
      } else if (cta === "secondary") {
        track("homepage_secondary_cta_click", {
          location: t.getAttribute("data-dc-loc") || "unknown",
        });
      }
      if (t.getAttribute("data-dc-track") === "signin") {
        track("signin_click", { location: "nav" });
      }
      if (t.getAttribute("data-dc-track") === "secondary_audience") {
        track("secondary_audience_click", {
          label: (t.textContent || "").trim().slice(0, 64),
        });
      }
    });

    if ("IntersectionObserver" in window) {
      var how = root.querySelector("#how-it-works");
      if (how) {
        var seen = false;
        var io = new IntersectionObserver(
          function (entries) {
            entries.forEach(function (e) {
              if (!e.isIntersecting || seen) return;
              seen = true;
              track("how_it_works_view", { section: "how-it-works" });
            });
          },
          { threshold: 0.35 }
        );
        io.observe(how);
      }
    }

    root.querySelectorAll('a[href^="#"]').forEach(function (a) {
      a.addEventListener("click", function (e) {
        var id = (a.getAttribute("href") || "").slice(1);
        var el = id && root.querySelector("#" + CSS.escape(id));
        if (!el) return;
        e.preventDefault();
        el.scrollIntoView({ behavior: "smooth", block: "start" });
        try {
          history.pushState(null, "", "#" + id);
        } catch (_e) {}
      });
    });
  }

  function mount(html) {
    var page = document.getElementById("dc-page") || document.body;
    hideLegacy();
    var existing = document.getElementById(ROOT_ID);
    if (existing) existing.remove();
    var root = document.createElement("div");
    root.id = ROOT_ID;
    root.className = "dc-owner-root";
    root.innerHTML = html;
    rewriteOppLinks(root);
    page.insertBefore(root, page.firstChild);
    bindUi(root);
    track("homepage_view", { section: "hero" });
    track("page_land", { section: "hero" });
    document.documentElement.classList.add("dc-owner-home");
  }

  function boot() {
    loadCss();
    fetch(FRAGMENT_URL, { credentials: "omit" })
      .then(function (res) {
        if (!res.ok) throw new Error("fragment_fetch_failed");
        return res.text();
      })
      .then(mount)
      .catch(function (err) {
        console.error("[dc-old-home-owner] inject failed:", err && err.message);
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
