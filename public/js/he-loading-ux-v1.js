/**
 * Hotel Explorer — shared loading UX adapter (HE-QL-028).
 * Reuses canonical DealalityWaveLoader toast / wave / compact variants.
 * Never invents a parallel spinner system.
 *
 * Modes:
 *   toast  — short subsection fetches (centered compact toast)
 *   wave   — sustained section loads (inline centered wave card)
 *   inline — action-local (Surfe, OCN expand, button refresh)
 */
(function (global) {
  "use strict";

  function api() {
    return global.DealalityWaveLoader || null;
  }

  function escapeHtml(text) {
    return String(text == null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /** Compact centered toast — default for short HE async fetches. */
  function toastHtml(mainText, opts) {
    opts = opts || {};
    var loader = api();
    var text = mainText || "Loading…";
    if (loader && typeof loader.toastHtml === "function") {
      return loader.toastHtml(text, {
        subText: opts.subText || "",
        hostClass: opts.hostClass || "dealality-loading-toast-host he-loading-toast-host",
        label: opts.label || text,
      });
    }
    if (loader && typeof loader.compactHtml === "function") {
      return (
        '<div class="dealality-loading-toast-host he-loading-toast-host" data-dealality-loading-toast="1">' +
        loader.compactHtml(text, { subText: opts.subText || "", progress: false }) +
        "</div>"
      );
    }
    return (
      '<div class="dealality-loading-toast-host he-loading-toast-host" role="status" aria-live="polite">' +
      '<div class="dealality-wave-loader dealality-wave-loader--inline dealality-wave-loader--compact dealality-wave-loader--toast">' +
      '<div class="loading-text-main">' +
      escapeHtml(text) +
      "</div></div></div>"
    );
  }

  /** Sustained section wave — still centered, not full-width banner. */
  function waveHtml(mainText, opts) {
    opts = opts || {};
    var loader = api();
    var text = mainText || "Loading…";
    if (loader && typeof loader.compactHtml === "function") {
      return (
        '<div class="he-loading-wave-host" data-he-loading-wave="1">' +
        loader.compactHtml(text, {
          subText: opts.subText || "",
          progress: opts.progress === true,
          size: opts.size || "sm",
        }) +
        "</div>"
      );
    }
    return toastHtml(text, opts);
  }

  /** Action-local indicator (Surfe / expand / retry). */
  function inlineHtml(mainText, opts) {
    opts = opts || {};
    var loader = api();
    var text = mainText || "Loading…";
    if (loader && typeof loader.compactHtml === "function") {
      return loader.compactHtml(text, {
        subText: opts.subText || "",
        progress: false,
        size: "sm",
      });
    }
    return (
      '<div class="dealality-wave-loader dealality-wave-loader--inline dealality-wave-loader--compact dealality-wave-loader--sm" role="status" aria-live="polite">' +
      '<div class="loading-text-main">' +
      escapeHtml(text) +
      "</div></div>"
    );
  }

  function errorHtml(message, retryAttr) {
    if (global.HeAsyncSection && typeof global.HeAsyncSection.errorHtml === "function") {
      return global.HeAsyncSection.errorHtml(message, retryAttr);
    }
    var msg = escapeHtml(message || "This information is temporarily unavailable.");
    var retry = retryAttr
      ? '<button type="button" class="hiw-btn hiw-btn--ghost hiw-btn--sm" data-he-async-retry="' +
        escapeHtml(retryAttr) +
        '">Retry</button>'
      : "";
    return (
      '<div class="he-async-error" role="status"><p class="hiw-prose hiw-prose--quiet">' +
      msg +
      "</p>" +
      retry +
      "</div>"
    );
  }

  var HeLoadingUx = {
    toastHtml: toastHtml,
    waveHtml: waveHtml,
    inlineHtml: inlineHtml,
    errorHtml: errorHtml,
    /** Prefer toast for short loads; wave when opts.mode === 'wave'. */
    sectionHtml: function (mainText, opts) {
      opts = opts || {};
      if (opts.mode === "wave") return waveHtml(mainText, opts);
      if (opts.mode === "inline") return inlineHtml(mainText, opts);
      return toastHtml(mainText, opts);
    },
  };

  global.HeLoadingUx = HeLoadingUx;
})(typeof window !== "undefined" ? window : globalThis);
