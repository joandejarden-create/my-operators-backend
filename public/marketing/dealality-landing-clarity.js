/**
 * Optional Microsoft Clarity loader for Railway landing iframe.
 * Fetches project id from /api/marketing/landing-config (CLARITY_PROJECT_ID env).
 * Must load before dealality-landing-analytics.js so custom events fire.
 */
(function (global) {
  "use strict";

  function loadClarity(projectId) {
    if (!projectId || typeof global.clarity === "function") return;
    (function (c, l, a, r, i, t, y) {
      c[a] =
        c[a] ||
        function () {
          (c[a].q = c[a].q || []).push(arguments);
        };
      t = l.createElement(r);
      t.async = 1;
      t.src = "https://www.clarity.ms/tag/" + i;
      y = l.getElementsByTagName(r)[0];
      y.parentNode.insertBefore(t, y);
    })(global, document, "clarity", "script", projectId);
  }

  function boot() {
    var inline = global.DEALALITY_CLARITY_PROJECT_ID;
    if (inline) {
      loadClarity(String(inline).trim());
      return;
    }
    fetch("/api/marketing/landing-config", { credentials: "omit" })
      .then(function (res) {
        return res.ok ? res.json() : null;
      })
      .then(function (cfg) {
        if (cfg && cfg.clarityProjectId) loadClarity(cfg.clarityProjectId);
      })
      .catch(function () {});
  }

  boot();
})(window);
