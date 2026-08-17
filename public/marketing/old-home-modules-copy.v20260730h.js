/**
 * Old Home — Benefits outcomes copy (v20260730h)
 * Path-gated to /old-home. Keeps card body min-height + locks approved copy.
 * Titles/lead/h2 live in Webflow; this script only reinforces paragraph copy
 * so older CDN overrides cannot win.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohModulesCopy >= 202607308) return;
    window.__ohModulesCopy = 202607308;

    var STYLE_ID = "oh-modules-copy-30h";
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
      "Centralize the Process. Compare the Options. Move Toward Agreement Faster.";
    var LEAD =
      "Dealality helps hotel owners organize the opportunity, engage relevant participants, compare responses on a consistent basis, and keep credible alternatives in view as the preferred path moves toward agreement.";

    var TITLES = {
      "mod-1-h": "Centralize the Opportunity",
      "mod-2-h": "Explore Credible Paths",
      "mod-3-h": "Engage the Right Participants",
      "mod-4-h": "Accelerate the Path to Agreement",
      "mod-5-h": "Compare Responses Consistently",
      "mod-6-h": "Preserve Optionality",
    };

    var COPY = {
      "mod-1-p":
        "Keep the asset story, owner objectives, participants, documents, questions, and responses connected in one confidential process.",
      "mod-2-p":
        "Review relevant brand, operator, conversion, positioning, capital, and strategic-partner paths before narrowing the opportunity.",
      "mod-3-p":
        "Give relevant brands, operators, and partners the context they need to evaluate the opportunity and respond efficiently.",
      "mod-4-p":
        "Coordinate outreach, information requests, responses, follow-ups, and next actions without relying on scattered emails and separate trackers.",
      "mod-5-p":
        "Bring economics, requirements, support, timing, open questions, and missing terms into one shared comparison.",
      "mod-6-p":
        "Keep credible alternatives visible and enter negotiations better prepared before committing to one direction.",
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
      var mark = "30h";
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
