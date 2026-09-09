/**
 * Dealality shared chain-scale accent theme.
 * Authoritative mapping previously lived in brand-explorer-gold-detail.js
 * (chainStripeColor). Brand Explorer and Hotel Explorer must both use this module.
 */
(function (global) {
  "use strict";

  /** CSS default when scale is null/unmapped — same as Brand Explorer :root fallback. */
  var FALLBACK_ACCENT = "#6c72ff";

  var THEME_VAR_NAMES = [
    "--accent--primary-1",
    "--accent",
    "--hero-stripe-bg",
    "--hero-tag",
    "--accent-soft",
    "--accent-line",
  ];

  /**
   * Canonical Brand Explorer scale → accent hex.
   * Order matters: "upper upscale" before "upscale"; "upper midscale" before "midscale".
   */
  function chainStripeColor(scale) {
    if (!scale) return null;
    var s = String(scale).toLowerCase();
    if (s.indexOf("luxury") !== -1) return "#d4af37";
    if (s.indexOf("upper upscale") !== -1) return "#9b59b6";
    if (s.indexOf("upscale") !== -1 && s.indexOf("upper") === -1) return "#3498db";
    if (s.indexOf("upper midscale") !== -1) return "#2ecc71";
    if (s.indexOf("midscale") !== -1) return "#1abc9c";
    if (s.indexOf("economy") !== -1) return "#e67e22";
    return null;
  }

  function normalizeChainScaleLabel(scale) {
    if (!scale) return "";
    var s = String(scale).toLowerCase();
    if (s.indexOf("luxury") !== -1) return "Luxury";
    if (s.indexOf("upper upscale") !== -1) return "Upper Upscale";
    if (s.indexOf("upscale") !== -1 && s.indexOf("upper") === -1) return "Upscale";
    if (s.indexOf("upper midscale") !== -1) return "Upper Midscale";
    if (s.indexOf("midscale") !== -1) return "Midscale";
    if (s.indexOf("economy") !== -1) return "Economy";
    return String(scale).trim();
  }

  function parseHexRgb(hex) {
    var h = String(hex || "")
      .trim()
      .replace(/^#/, "");
    if (h.length === 3) {
      h = h
        .split("")
        .map(function (c) {
          return c + c;
        })
        .join("");
    }
    if (!/^[0-9a-fA-F]{6}$/.test(h)) return null;
    return {
      r: parseInt(h.slice(0, 2), 16),
      g: parseInt(h.slice(2, 4), 16),
      b: parseInt(h.slice(4, 6), 16),
    };
  }

  function getChainScaleTheme(chainScale) {
    var hex = chainStripeColor(chainScale);
    var rgb = hex ? parseHexRgb(hex) : null;
    return {
      scale_raw: chainScale == null ? "" : String(chainScale),
      scale_canonical: normalizeChainScaleLabel(chainScale),
      hex: hex,
      accent: hex || FALLBACK_ACCENT,
      uses_fallback: !hex,
      soft: rgb ? "rgba(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ", 0.14)" : "rgba(108, 114, 255, 0.14)",
      line: rgb ? "rgba(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ", 0.45)" : "rgba(108, 114, 255, 0.45)",
    };
  }

  function applyChainScaleThemeVars(el, hex) {
    if (!el) return;
    if (!hex) {
      THEME_VAR_NAMES.forEach(function (name) {
        el.style.removeProperty(name);
      });
      el.removeAttribute("data-be-chain-scale-theme");
      el.removeAttribute("data-chain-scale-theme");
      return;
    }
    var rgb = parseHexRgb(hex);
    if (!rgb) return;
    var accent = hex.indexOf("#") === 0 ? hex : "#" + hex;
    el.style.setProperty("--accent--primary-1", accent);
    el.style.setProperty("--accent", accent);
    el.style.setProperty("--hero-stripe-bg", accent);
    el.style.setProperty("--hero-tag", accent);
    el.style.setProperty(
      "--accent-soft",
      "rgba(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ", 0.14)"
    );
    el.style.setProperty(
      "--accent-line",
      "rgba(" + rgb.r + ", " + rgb.g + ", " + rgb.b + ", 0.45)"
    );
    el.setAttribute("data-be-chain-scale-theme", "1");
    el.setAttribute("data-chain-scale-theme", "1");
  }

  function applyThemeFromScale(chainScale, roots) {
    var hex = chainStripeColor(chainScale);
    var list = Array.isArray(roots) ? roots : roots ? [roots] : [];
    list.forEach(function (el) {
      applyChainScaleThemeVars(el, hex);
    });
    return getChainScaleTheme(chainScale);
  }

  function clearTheme(roots) {
    var list = Array.isArray(roots) ? roots : roots ? [roots] : [];
    list.forEach(function (el) {
      applyChainScaleThemeVars(el, null);
    });
  }

  global.DealalityChainScaleTheme = {
    FALLBACK_ACCENT: FALLBACK_ACCENT,
    THEME_VAR_NAMES: THEME_VAR_NAMES,
    chainStripeColor: chainStripeColor,
    normalizeChainScaleLabel: normalizeChainScaleLabel,
    getChainScaleTheme: getChainScaleTheme,
    applyChainScaleThemeVars: applyChainScaleThemeVars,
    applyThemeFromScale: applyThemeFromScale,
    clearTheme: clearTheme,
    parseHexRgb: parseHexRgb,
  };
})(typeof window !== "undefined" ? window : globalThis);
