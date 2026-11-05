/**
 * Browser renderer for AI Visibility Audit & Action Plan PDF.
 * Markup is pre-built server-side (build-audit-pdf-html-v1.js) and injected
 * via route fulfill or ?htmlPath — this module only mounts + marks ready.
 */
(function (global) {
  "use strict";

  var TEMPLATE_ID = "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1";
  var RENDERER_ID = "AdpAiVisibilityAuditPdfV1";
  var REPORT_FAMILY = "dealality-report-system-v1";

  function mount(html, meta) {
    meta = meta || {};
    var boot = document.getElementById("ava-pdf-boot");
    var old = document.getElementById("ava-pdf-host");
    if (old) old.remove();
    var host = document.createElement("div");
    host.id = "ava-pdf-host";
    /* adp-mr-pdf-host applies the exact ADP Performance Review cover chrome rules */
    host.className = "ava-pdf-host adp-mr-pdf-host hid-print-host--pdf-export";
    host.setAttribute("data-ava-pdf-ready", "1");
    host.setAttribute("data-ava-property-id", meta.propertyId || "");
    host.setAttribute("data-ava-property-name", meta.propertyName || "");
    host.setAttribute("data-ava-report-family", REPORT_FAMILY);
    host.setAttribute("data-ava-template-id", TEMPLATE_ID);
    host.setAttribute("data-ava-renderer", RENDERER_ID);
    host.innerHTML = html;
    host.querySelectorAll("[data-dealality-report-footer]").forEach(function (el) {
      el.remove();
    });
    document.body.appendChild(host);
    if (boot) boot.hidden = true;
    document.title =
      "Dealality — AI Visibility Audit & Action Plan — " +
      (meta.propertyName || "Property");
    return host;
  }

  global.AdpAiVisibilityAuditPdfV1 = {
    TEMPLATE_ID: TEMPLATE_ID,
    RENDERER_ID: RENDERER_ID,
    REPORT_FAMILY: REPORT_FAMILY,
    mount: mount,
  };
})(typeof window !== "undefined" ? window : globalThis);
