/**
 * Client mirror of HE Guest Sentiment founder feature flag.
 * Default OFF. Explicit window override may enable UI only when server/env also allow;
 * publication-rights gates remain authoritative on the API.
 */
(function (global) {
  "use strict";

  var TAB_ID = "sentiment";
  var DISABLED_BY_FOUNDER = "DISABLED_BY_FOUNDER";
  /** Keep aligned with server default HE_GUEST_SENTIMENT_ENABLED=false */
  var ENV_DEFAULT_ENABLED = false;

  function isEnabled() {
    try {
      if (global.__DEALALITY_HE_GUEST_SENTIMENT_ENABLED__ === true) return true;
      if (global.__DEALALITY_HE_GUEST_SENTIMENT_ENABLED__ === false) return false;
    } catch (err) {
      /* ignore */
    }
    return ENV_DEFAULT_ENABLED === true;
  }

  function status() {
    return isEnabled() ? "CONFIG_ENABLED_RIGHTS_GATED" : DISABLED_BY_FOUNDER;
  }

  function filterActiveTabs(tabs) {
    var list = Array.isArray(tabs) ? tabs : [];
    if (isEnabled()) return list.slice();
    return list.filter(function (t) {
      var id = t && typeof t === "object" ? t.id : t;
      return id !== TAB_ID && id !== "brand";
    });
  }

  function normalizeTabOrFallback(tabId, fallback) {
    var raw = String(tabId || "").trim();
    var fb = fallback || "hotel";
    if (!isEnabled() && (raw === TAB_ID || raw === "brand" || raw === "guest-sentiment" || raw === "guest" || raw === "brand-operator")) {
      return fb;
    }
    return raw || fb;
  }

  function unavailableHtml() {
    return (
      '<section class="hex-card hiw-section" data-he-guest-sentiment-disabled="1" data-status="' +
      DISABLED_BY_FOUNDER +
      '">' +
      "<h2>Guest Sentiment unavailable</h2>" +
      '<p class="hiw-prose">Guest Sentiment is temporarily disabled pending licensing and redistribution rights review. Other Hotel Explorer sections remain available.</p>' +
      "</section>"
    );
  }

  global.HeGuestSentimentFeatureFlag = {
    TAB_ID: TAB_ID,
    DISABLED_BY_FOUNDER: DISABLED_BY_FOUNDER,
    isEnabled: isEnabled,
    status: status,
    filterActiveTabs: filterActiveTabs,
    normalizeTabOrFallback: normalizeTabOrFallback,
    unavailableHtml: unavailableHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
