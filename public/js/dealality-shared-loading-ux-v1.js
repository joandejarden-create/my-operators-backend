/**
 * Dealality shared loading UX — cross-product adapter over DealalityWaveLoader.
 *
 * Contract: DEALALITY_SHARED_LOADING_COMPONENT_V1
 * Canonical presentation: animated Dealality wave + compact black toast + message.
 * Only the contextual message varies by product/view.
 *
 * Lifecycle rules:
 *   - Show only while an async operation is pending.
 *   - Do not show for cached / immediately renderable tab switches.
 *   - Hide on READY / EMPTY / ERROR; never leave indefinite loaders.
 */
(function (global) {
  "use strict";

  var CONTRACT = "DEALALITY_SHARED_LOADING_COMPONENT_V1";

  var MESSAGES = Object.freeze({
    gdiPage: "Loading Group Demand Intelligence…",
    gdiOpportunity: "Loading opportunity…",
    gdiExecutiveSummary: "Loading Executive Summary…",
    gdiPortfolio: "Loading Portfolio Demand Intelligence…",
    gdiCommercialPriorities: "Loading Hotel Commercial Priorities…",
    adpPage: "Loading AI Demand Intelligence…",
    beBrands: "Loading Brands…",
    beBrand: "Loading Brand…",
    beBrandProfile: "Loading Brand Profile…",
    beBrandIntelligence: "Loading Brand Intelligence…",
    heContacts: "Loading Contacts…",
    heOwnership: "Loading ownership intelligence…",
    heOwnershipNetwork: "Loading ownership network…",
    heHotelNews: "Loading hotel news…",
    hePortfolio: "Loading Portfolio…",
    heHotelDetails: "Loading Hotel Details…",
    generic: "Loading…",
  });

  function wave() {
    return global.DealalityWaveLoader || null;
  }

  function escapeHtml(text) {
    return String(text == null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function resolveMessage(keyOrText) {
    if (keyOrText == null || keyOrText === "") return MESSAGES.generic;
    if (Object.prototype.hasOwnProperty.call(MESSAGES, keyOrText)) {
      return MESSAGES[keyOrText];
    }
    return String(keyOrText);
  }

  /** Compact centered section toast (default async subsection). */
  function toastHtml(mainText, opts) {
    opts = opts || {};
    var text = resolveMessage(mainText);
    var api = wave();
    if (api && typeof api.toastHtml === "function") {
      return api.toastHtml(text, {
        subText: opts.subText || "",
        hostClass:
          opts.hostClass ||
          "dealality-loading-toast-host dealality-shared-loading-toast-host",
        label: opts.label || text,
      });
    }
    return (
      '<div class="dealality-loading-toast-host" data-dealality-loading-toast="1" role="status" aria-live="polite">' +
      '<div class="loading dealality-wave-loader dealality-wave-loader--inline dealality-wave-loader--compact dealality-wave-loader--toast dealality-wave-loader--sm">' +
      '<div class="loading-content"><div class="loading-text-main">' +
      escapeHtml(text) +
      "</div></div></div></div>"
    );
  }

  function inlineHtml(mainText, opts) {
    opts = opts || {};
    var text = resolveMessage(mainText);
    var api = wave();
    if (api && typeof api.compactHtml === "function") {
      return api.compactHtml(text, {
        subText: opts.subText || "",
        progress: false,
        size: "sm",
      });
    }
    return toastHtml(text, opts);
  }

  function sectionHtml(mainText, opts) {
    opts = opts || {};
    if (opts.mode === "inline") return inlineHtml(mainText, opts);
    if (opts.mode === "wave") {
      var api = wave();
      var text = resolveMessage(mainText);
      if (api && typeof api.compactHtml === "function") {
        return (
          '<div class="dealality-shared-loading-wave-host" data-dealality-loading-wave="1">' +
          api.compactHtml(text, {
            subText: opts.subText || "",
            progress: opts.progress === true,
            size: opts.size || "sm",
          }) +
          "</div>"
        );
      }
    }
    return toastHtml(mainText, opts);
  }

  /**
   * Corner / page toast markup using the same wave orb primitives as DealalityWaveLoader.
   * Positioning remains product CSS (.bdd-toast) — chrome must stay black toast.
   */
  function pageToastInnerHtml(mainText) {
    var text = resolveMessage(mainText);
    var api = wave();
    var orb =
      api && typeof api.waveParticlesHtml === "function"
        ? '<div class="toast-wave-container" aria-hidden="true"><div class="wave-container">' +
          '<div class="wave wave-1"></div><div class="wave wave-2"></div><div class="wave wave-3"></div>' +
          api.waveParticlesHtml() +
          "</div></div>"
        : '<div class="toast-wave-container" aria-hidden="true"><div class="wave-container">' +
          '<div class="wave wave-1"></div><div class="wave wave-2"></div><div class="wave wave-3"></div>' +
          '<div class="wave-particles"><div class="particle"></div><div class="particle"></div>' +
          '<div class="particle"></div><div class="particle"></div></div></div></div>';
    return (
      orb +
      '<div class="toast-message loading-text-main" data-dealality-loading-message="1">' +
      escapeHtml(text) +
      "</div>"
    );
  }

  /**
   * Show/hide an existing page toast element. No-op when el missing.
   * Does not invent a second animation — relies on shared wave CSS.
   */
  function setPageToastBusy(el, on, message) {
    if (!el) return { shown: false, reason: "missing_el" };
    var msgEl =
      el.querySelector("[data-dealality-loading-message]") ||
      el.querySelector(".toast-message") ||
      (el.id ? global.document && global.document.getElementById(el.id + "Message") : null);
    if (on) {
      if (msgEl) msgEl.textContent = resolveMessage(message || MESSAGES.generic);
      el.hidden = false;
      if (el.style) el.style.display = "flex";
      el.setAttribute("aria-busy", "true");
      el.classList.add("show");
      return { shown: true, reason: "async_pending" };
    }
    el.classList.remove("show");
    el.setAttribute("aria-busy", "false");
    if (el.style) el.style.display = "none";
    el.hidden = true;
    return { shown: false, reason: "idle" };
  }

  /**
   * Decide whether a loader should appear for a load attempt.
   * Cached / immediate payloads must not flicker the toast.
   */
  function shouldShowLoader(meta) {
    meta = meta || {};
    if (meta.asyncPending === true) return true;
    if (meta.fromCache === true) return false;
    if (meta.immediate === true) return false;
    if (meta.force === true) return true;
    return meta.asyncPending === true;
  }

  var api = {
    CONTRACT: CONTRACT,
    MESSAGES: MESSAGES,
    resolveMessage: resolveMessage,
    toastHtml: toastHtml,
    inlineHtml: inlineHtml,
    sectionHtml: sectionHtml,
    pageToastInnerHtml: pageToastInnerHtml,
    setPageToastBusy: setPageToastBusy,
    shouldShowLoader: shouldShowLoader,
    wave: wave,
  };

  global.DealalitySharedLoadingUx = api;

  // Back-compat: HE adapter surface when HeLoadingUx not yet loaded.
  if (!global.HeLoadingUx) {
    global.HeLoadingUx = {
      toastHtml: toastHtml,
      inlineHtml: inlineHtml,
      sectionHtml: sectionHtml,
      waveHtml: function (mainText, opts) {
        return sectionHtml(mainText, Object.assign({}, opts || {}, { mode: "wave" }));
      },
      errorHtml: function (message, retryAttr) {
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
      },
    };
  }
})(typeof window !== "undefined" ? window : globalThis);
