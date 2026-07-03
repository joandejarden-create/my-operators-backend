/**
 * Operator match score UI — reads bands from window.DcOperatorMatchScoreConfig
 * (served by /js/generated/operator-match-scoring-config.js).
 */
(function (global) {
  "use strict";

  var FALLBACK_BANDS = [
    { min: 80, label: "Strong alignment signals", uiClass: "match-score-high" },
    { min: 50, label: "Moderate alignment — review gaps", uiClass: "match-score-medium" },
    { min: 25, label: "Weak alignment — significant gaps", uiClass: "match-score-weak" },
    { min: 0, label: "Very limited alignment", uiClass: "match-score-poor" },
  ];

  var BREAKDOWN_CLASS_BY_UI = {
    "match-score-high": "high",
    "match-score-medium": "medium",
    "match-score-weak": "low",
    "match-score-poor": "poor",
  };

  function getPayload() {
    return global.DcOperatorMatchScoreConfig || null;
  }

  function getBands() {
    var payload = getPayload();
    var bands = payload && payload.bands;
    return Array.isArray(bands) && bands.length ? bands : FALLBACK_BANDS;
  }

  function sortBandsDesc(bands) {
    return bands.slice().sort(function (a, b) {
      return b.min - a.min;
    });
  }

  function getBandForScore(score) {
    var n = Number(score);
    if (!Number.isFinite(n)) return null;
    var sorted = sortBandsDesc(getBands());
    for (var i = 0; i < sorted.length; i++) {
      if (n >= sorted[i].min) return sorted[i];
    }
    return sorted[sorted.length - 1] || null;
  }

  function getAlignmentScoreClass(score) {
    var band = getBandForScore(score);
    return band ? band.uiClass : "";
  }

  function getBreakdownScoreClass(score) {
    var uiClass = getAlignmentScoreClass(score);
    return BREAKDOWN_CLASS_BY_UI[uiClass] || "medium";
  }

  function getNarrativeTier(score) {
    var n = Number(score);
    if (!Number.isFinite(n)) return "poor";
    var sorted = sortBandsDesc(getBands());
    for (var i = 0; i < sorted.length; i++) {
      if (n >= sorted[i].min) {
        if (i === 0) return "strong";
        if (i === 1) return "moderate";
        if (i === 2) return "weak";
        return "poor";
      }
    }
    return "poor";
  }

  function getFactorStrongMin() {
    var payload = getPayload();
    if (payload && typeof payload.factorStrongMin === "number") return payload.factorStrongMin;
    var sorted = sortBandsDesc(getBands());
    return sorted[0] ? sorted[0].min : 80;
  }

  function getFactorWeakBelow() {
    var payload = getPayload();
    if (payload && typeof payload.factorWeakBelow === "number") return payload.factorWeakBelow;
    var sorted = sortBandsDesc(getBands());
    return sorted[1] ? sorted[1].min : 50;
  }

  function isHighFitScore(score) {
    var band = getBandForScore(score);
    var sorted = sortBandsDesc(getBands());
    return !!(band && sorted[0] && band.min === sorted[0].min);
  }

  global.DcOperatorMatchScoreUi = {
    getBands: getBands,
    getBandForScore: getBandForScore,
    getAlignmentScoreClass: getAlignmentScoreClass,
    getBreakdownScoreClass: getBreakdownScoreClass,
    getNarrativeTier: getNarrativeTier,
    getFactorStrongMin: getFactorStrongMin,
    getFactorWeakBelow: getFactorWeakBelow,
    isHighFitScore: isHighFitScore,
  };
})(window);
