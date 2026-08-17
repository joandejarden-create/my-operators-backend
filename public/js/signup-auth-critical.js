/**
 * Auth pages (Webflow head): dark background + hide loaders before footer scripts run.
 * Add to Webflow Project Settings → Custom Code → Head on /signup, /signup-new, /log-in.
 */
(function (global) {
  "use strict";
  var doc = global.document;
  if (!doc || !doc.documentElement) return;

  var root = doc.documentElement;
  root.classList.add("dl-auth-landing-skin", "signup-page-paint");
  root.style.backgroundColor = "#080f25";

  if (doc.getElementById("signup-auth-critical-css")) return;

  var base = (global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || "")
    .trim()
    .replace(/\/+$/, "");
  if (!base) {
    base = "https://my-operators-backend-staging.up.railway.app";
  }

  var link = doc.createElement("link");
  link.id = "signup-auth-critical-css";
  link.rel = "stylesheet";
  link.href = base + "/css/signup-auth-critical.css";
  (doc.head || root).appendChild(link);
})(typeof window !== "undefined" ? window : global);
