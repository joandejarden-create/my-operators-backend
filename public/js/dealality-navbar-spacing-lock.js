/**
 * Site-wide Webflow navbar link spacing only.
 * Logo size is controlled in Webflow Designer (navbar_logo) — do not override height here.
 */
(function (global) {
  "use strict";

  var STYLE_ID = "dl-navbar-spacing-lock";

  function injectStyles(doc) {
    if (!doc || doc.getElementById(STYLE_ID)) return;
    var style = doc.createElement("style");
    style.id = STYLE_ID;
    style.textContent =
      ".navbar-2 .nav_links,.navbar-2 a.nav_links{padding:.35rem .5rem!important;margin-left:0!important;margin-right:0!important}" +
      ".navbar-2 .nav-compact,.navbar-2 a.nav-compact{padding:.4rem .85rem!important;line-height:1.2!important;margin-left:0!important;margin-right:0!important}" +
      ".navbar-2 .w-nav-menu{display:flex!important;align-items:center!important;column-gap:0!important;row-gap:0!important}";
    (doc.head || doc.documentElement).appendChild(style);
    if (global.DealalityWebflowAccountNotice && typeof global.DealalityWebflowAccountNotice.applyNavbarLogoSize === "function") {
      global.DealalityWebflowAccountNotice.applyNavbarLogoSize();
    }
  }

  if (global.document) {
    if (global.document.readyState === "loading") {
      global.document.addEventListener("DOMContentLoaded", function () {
        injectStyles(global.document);
      });
    } else {
      injectStyles(global.document);
    }
  }
})(typeof window !== "undefined" ? window : global);
