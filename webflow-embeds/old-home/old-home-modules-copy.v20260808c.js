/**
 * Old Home — Benefits outcomes copy (v20260808c)
 * Path-gated to / and /old-home. Locks Clear-Not-Clever approved copy.
 * Titles/lead/h2 live in Webflow; this script reinforces so older CDN cannot win.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home") return;
    if (window.__ohModulesCopy >= 2026080803) return;
    window.__ohModulesCopy = 2026080803;

    var STYLE_ID = "oh-modules-copy-08c";
    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "#mod-1-p,#mod-2-p,#mod-3-p,#mod-4-p,#mod-5-p,#mod-6-p,",
        "#mod-1 p,#mod-2 p,#mod-3 p,#mod-4 p,#mod-5 p,#mod-6 p{",
        "min-height:calc(1.55em * 3)!important;",
        "line-height:1.55!important;",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    var H2 =
      "Keep the Deal in One Place. Compare Offers Side by Side. Decide with Confidence.";
    var LEAD =
      "Dealality helps hotel owners organize the opportunity, talk to the right people, compare responses the same way, and keep good backup choices open before they commit.";

    var TITLES = {
      "mod-1-h": "Keep the Deal in One Place",
      "mod-2-h": "See the Real Options",
      "mod-3-h": "Talk to the Right People",
      "mod-4-h": "Get to a Clear Yes or No Faster",
      "mod-5-h": "Compare Offers the Same Way",
      "mod-6-h": "Keep Backup Choices Open",
    };

    var COPY = {
      "mod-1-p":
        "Asset story, goals, people, documents, questions, and responses stay in one confidential process.",
      "mod-2-p":
        "Review brand, operator, conversion, positioning, capital, and partner paths before you narrow the choice.",
      "mod-3-p":
        "Give brands, operators, and partners the context they need so they can respond clearly.",
      "mod-4-p":
        "Outreach, questions, responses, and next steps without scattered email threads.",
      "mod-5-p":
        "Fees, requirements, support, timing, open questions, and missing terms in one shared view.",
      "mod-6-p":
        "Stay ready for negotiation with real alternatives still in view before you pick one path.",
    };

    function setText(id, text, mark) {
      var el = document.getElementById(id);
      if (!el) return false;
      if (el.getAttribute("data-oh-copy") === mark) return true;
      el.textContent = text;
      el.setAttribute("data-oh-copy", mark);
      return true;
    }

    function apply() {
      var mark = "08c";
      var n = 0;
      if (setText("modules-h2", H2, mark)) n += 1;
      if (setText("modules-lead", LEAD, mark)) n += 1;
      Object.keys(TITLES).forEach(function (id) {
        if (setText(id, TITLES[id], mark)) n += 1;
      });
      Object.keys(COPY).forEach(function (id) {
        if (setText(id, COPY[id], mark)) n += 1;
      });
      return n;
    }

    function boot() {
      if (apply() >= 8) return;
      window.setTimeout(apply, 50);
      window.setTimeout(apply, 250);
      window.setTimeout(apply, 800);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    window.addEventListener("load", apply);
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-modules-copy]", err);
    }
  }
})();
