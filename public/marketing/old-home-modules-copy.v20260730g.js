/**
 * Old Home — modules card description copy (v20260730g)
 * Path-gated to /old-home. Restores 3-line descriptions for
 * What Owners Gain + How Dealality Works tiles.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohModulesCopy >= 202607307) return;
    window.__ohModulesCopy = 202607307;

    var STYLE_ID = "oh-modules-copy-30g";
    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "#mod-1-p,#mod-2-p,#mod-3-p,#mod-4-p,#mod-5-p,#mod-6-p,",
        "#modp-1-p,#modp-2-p,#modp-3-p,#modp-4-p,#modp-5-p,#modp-6-p,",
        "#mod-1 p,#mod-2 p,#mod-3 p,#mod-4 p,#mod-5 p,#mod-6 p,",
        "#modp-1 p,#modp-2 p,#modp-3 p,#modp-4 p,#modp-5 p,#modp-6 p{",
        "min-height:calc(1.55em * 3)!important;",
        "line-height:1.55!important;",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    var COPY = {
      "mod-1-p":
        "Explore the brand, operator, conversion, capital, and strategic paths most relevant to your hotel and goals.",
      "mod-2-p":
        "Organize the owner's goals, the property story, the economics, and the information partners need to evaluate the opportunity.",
      "mod-3-p":
        "Focus outreach on the brands, operators, investors, and advisors best positioned to respond to the opportunity.",
      "mod-4-p":
        "Avoid letting a single conversation or relationship set the pace, options, or outcome for the entire process.",
      "mod-5-p":
        "See the differences in fees, control, requirements, timing, risk, and long-term value across proposals.",
      "mod-6-p":
        "Choose what to pursue with clearer information, stronger priorities, and greater confidence in the path forward.",
      "modp-1-p":
        "Bring the hotel, owner goals, market context, constraints, and key questions into one structured review workspace.",
      "modp-2-p":
        "Identify the brands, operators, structures, conversions, and capital options worth considering for the property.",
      "modp-3-p":
        "Understand who may fit the opportunity, why they may fit, and what still needs to be confirmed.",
      "modp-4-p":
        "Present the opportunity clearly and manage confidential conversations with the parties selected for outreach.",
      "modp-5-p":
        "Review fees, requirements, support, control, timing, and important differences side by side across proposals.",
      "modp-6-p":
        "Track open questions, missing terms, negotiation priorities, and the reasons behind the final decision.",
    };

    function apply() {
      var n = 0;
      Object.keys(COPY).forEach(function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        if (el.getAttribute("data-oh-copy") === "30g") return;
        el.textContent = COPY[id];
        el.setAttribute("data-oh-copy", "30g");
        n += 1;
      });
      return n;
    }

    function boot() {
      if (apply() >= 6) return;
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
