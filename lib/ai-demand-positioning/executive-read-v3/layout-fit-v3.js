/**
 * ADP Executive Read V3 — layout fit contract (presentation only).
 * LAYOUT_FIT_GOVERNS_PRESENTATION; WORD_COUNT_GUIDES_COMPOSITION.
 * No methodology change.
 */

export const ADP_EXECUTIVE_READ_LAYOUT_FIT_V3 = "ADP_EXECUTIVE_READ_LAYOUT_FIT_V3";
export const ADP_EXECUTIVE_WORD_COUNT_GUIDANCE_NOT_TRUNCATION =
  "ADP_EXECUTIVE_WORD_COUNT_GUIDANCE_NOT_TRUNCATION";
export const ADP_EXECUTIVE_V3_NO_ARBITRARY_WORD_TRUNCATION =
  "ADP_EXECUTIVE_V3_NO_ARBITRARY_WORD_TRUNCATION";
export const ADP_EXECUTIVE_NO_MICROTYPE_OVERFLOW_FIX =
  "ADP_EXECUTIVE_NO_MICROTYPE_OVERFLOW_FIX";
export const ADP_EXECUTIVE_MOBILE_READABILITY_V3 = "ADP_EXECUTIVE_MOBILE_READABILITY_V3";
export const LAYOUT_FIT_GOVERNS_PRESENTATION_WORD_COUNT_GUIDES_COMPOSITION =
  "LAYOUT_FIT_GOVERNS_PRESENTATION; WORD_COUNT_GUIDES_COMPOSITION.";

export const LAYOUT_FIT_STATUS = Object.freeze({
  FIT: "FIT",
  FIT_WITHIN_TOLERANCE: "FIT_WITHIN_TOLERANCE",
  COMPRESSION_REQUIRED: "COMPRESSION_REQUIRED",
  LAYOUT_REVIEW_REQUIRED: "LAYOUT_REVIEW_REQUIRED",
});

/** Preferred visual envelope — not a rigid equal height. */
export const LAYOUT_ENVELOPE_V3 = Object.freeze({
  desktop: Object.freeze({
    width: 1280,
    preferredHeightPx: 420,
    acceptableMaxPx: 580,
    reviewMaxPx: 720,
    fontSizePx: 14,
    lineHeight: 1.65,
    charsPerLine: 72,
  }),
  tablet: Object.freeze({
    width: 768,
    preferredHeightPx: 480,
    acceptableMaxPx: 680,
    reviewMaxPx: 860,
    fontSizePx: 14,
    lineHeight: 1.65,
    charsPerLine: 58,
  }),
  mobile: Object.freeze({
    width: 390,
    preferredHeightPx: null, // natural growth
    acceptableMaxPx: null,
    reviewMaxPx: null,
    fontSizePx: 14,
    lineHeight: 1.65,
    charsPerLine: 42,
    naturalGrowth: true,
  }),
  print: Object.freeze({
    width: 720,
    preferredHeightPx: 420,
    acceptableMaxPx: 640,
    reviewMaxPx: 820,
    fontSizePx: 11,
    lineHeight: 1.5,
    charsPerLine: 85,
  }),
});

export const WORD_COUNT_GUIDANCE_V3 = Object.freeze({
  preferredMin: 170,
  preferredMax: 260,
  softWarning: 280,
  reviewThreshold: 320,
  truncateAt: null, // NEVER
});

function countWords(text) {
  return String(text || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

export function measureExecutiveReadWordCountV3(sections) {
  const parts = [
    sections?.headline,
    sections?.keyInsight,
    sections?.whyItMatters,
    sections?.focusNow,
    sections?.watch,
    sections?.whatToReview,
  ].filter((t) => t != null && String(t).trim());
  const text = parts.join(" ");
  const wordCount = countWords(text);
  let guidance = "PREFERRED";
  if (wordCount > WORD_COUNT_GUIDANCE_V3.reviewThreshold) guidance = "REVIEW";
  else if (wordCount > WORD_COUNT_GUIDANCE_V3.softWarning) guidance = "SOFT_WARNING";
  else if (wordCount < WORD_COUNT_GUIDANCE_V3.preferredMin) guidance = "BELOW_PREFERRED";
  return {
    gate: ADP_EXECUTIVE_WORD_COUNT_GUIDANCE_NOT_TRUNCATION,
    wordCount,
    guidance,
    truncateApplied: false,
    rule: "diagnostic_only",
  };
}

/**
 * Estimate rendered height from section text (DOM-independent diagnostic).
 * Real DOM measurement may refine via Playwright QA.
 */
export function estimateRenderedHeightV3(sections, surface = "desktop") {
  const env = LAYOUT_ENVELOPE_V3[surface] || LAYOUT_ENVELOPE_V3.desktop;
  const linePx = env.fontSizePx * env.lineHeight;
  const sectionKeys = ["headline", "keyInsight", "whyItMatters", "focusNow", "watch", "whatToReview"];
  let lineCount = 0;
  let sectionCount = 0;
  for (const key of sectionKeys) {
    const text = sections?.[key];
    if (text == null || !String(text).trim()) continue;
    if (key === "watch" && !String(text).trim()) continue;
    sectionCount += 1;
    const chars = String(text).length;
    const lines = Math.max(1, Math.ceil(chars / env.charsPerLine));
    // label + gap
    lineCount += lines + (key === "headline" ? 0.5 : 1.2);
  }
  const renderedHeight = Math.round(lineCount * linePx + 24); // padding
  return {
    surface,
    availableHeight: env.preferredHeightPx,
    preferredHeight: env.preferredHeightPx,
    acceptableMax: env.acceptableMaxPx,
    reviewMax: env.reviewMaxPx,
    renderedHeight,
    overflowPx:
      env.acceptableMaxPx != null ? Math.max(0, renderedHeight - env.acceptableMaxPx) : 0,
    fitRatio:
      env.preferredHeightPx != null && env.preferredHeightPx > 0
        ? Math.round((renderedHeight / env.preferredHeightPx) * 100) / 100
        : null,
    lineCount: Math.round(lineCount * 10) / 10,
    sectionCount,
    naturalGrowth: env.naturalGrowth === true,
  };
}

export function evaluateLayoutFitV3(sections, surface = "desktop") {
  const measure = estimateRenderedHeightV3(sections, surface);
  const words = measureExecutiveReadWordCountV3(sections);

  if (measure.naturalGrowth) {
    return {
      gate: ADP_EXECUTIVE_READ_LAYOUT_FIT_V3,
      status: LAYOUT_FIT_STATUS.FIT,
      measure,
      words,
      doctrine: LAYOUT_FIT_GOVERNS_PRESENTATION_WORD_COUNT_GUIDES_COMPOSITION,
      mobileRule: ADP_EXECUTIVE_MOBILE_READABILITY_V3,
      note: "Mobile uses natural vertical growth; desktop height parity not required.",
    };
  }

  let status = LAYOUT_FIT_STATUS.FIT;
  if (measure.renderedHeight <= measure.preferredHeight) {
    status = LAYOUT_FIT_STATUS.FIT;
  } else if (measure.renderedHeight <= measure.acceptableMax) {
    status = LAYOUT_FIT_STATUS.FIT_WITHIN_TOLERANCE;
  } else if (measure.renderedHeight <= measure.reviewMax) {
    status = LAYOUT_FIT_STATUS.COMPRESSION_REQUIRED;
  } else {
    status = LAYOUT_FIT_STATUS.LAYOUT_REVIEW_REQUIRED;
  }

  return {
    gate: ADP_EXECUTIVE_READ_LAYOUT_FIT_V3,
    status,
    measure,
    words,
    doctrine: LAYOUT_FIT_GOVERNS_PRESENTATION_WORD_COUNT_GUIDES_COMPOSITION,
    noArbitraryTruncate: ADP_EXECUTIVE_V3_NO_ARBITRARY_WORD_TRUNCATION,
    noMicrotype: ADP_EXECUTIVE_NO_MICROTYPE_OVERFLOW_FIX,
  };
}

export function evaluateAllSurfacesLayoutFitV3(sections) {
  const surfaces = ["desktop", "tablet", "mobile", "print"];
  const bySurface = {};
  for (const s of surfaces) bySurface[s] = evaluateLayoutFitV3(sections, s);
  const needsCompression = ["desktop", "tablet", "print"].some(
    (s) => bySurface[s].status === LAYOUT_FIT_STATUS.COMPRESSION_REQUIRED
  );
  const needsReview = Object.values(bySurface).some(
    (r) => r.status === LAYOUT_FIT_STATUS.LAYOUT_REVIEW_REQUIRED
  );
  return {
    bySurface,
    needsCompression,
    needsReview,
    pass: !needsReview,
  };
}
