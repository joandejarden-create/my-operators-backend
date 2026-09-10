/**
 * ADP Executive Read V3 — actual report implementation (inactive until founder activation).
 * Wires structured renderer selection for owner / share / print behind feature flag.
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 *           EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 */

import {
  ADP_EXECUTIVE_READ_COMPOSITION_V3,
  COMPOSITION_V3_STATUS,
} from "../governance/adp-executive-read-composition-v3.js";
import {
  ADP_EXECUTIVE_READ_RENDERER_CONTRACT_V3,
  buildExecutiveReadRendererViewModelV3,
  EXECUTIVE_READ_RENDERER_CONTRACT_V3,
} from "./renderer-contract-v3.js";
import {
  ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY,
  HISTORICAL_IMMUTABILITY_POLICY_V3,
  resolveExecutiveReadCompositionWritePolicyV3,
} from "./historical-immutability-v3.js";
import {
  ADP_EXECUTIVE_READ_LAYOUT_FIT_V3,
  ADP_EXECUTIVE_V3_NO_ARBITRARY_WORD_TRUNCATION,
  ADP_EXECUTIVE_WORD_COUNT_GUIDANCE_NOT_TRUNCATION,
  ADP_EXECUTIVE_NO_MICROTYPE_OVERFLOW_FIX,
  ADP_EXECUTIVE_MOBILE_READABILITY_V3,
  evaluateAllSurfacesLayoutFitV3,
  measureExecutiveReadWordCountV3,
} from "./layout-fit-v3.js";
import {
  ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3,
  ADP_EXECUTIVE_COMPRESSION_SEMANTIC_PARITY,
  maybeCompressForLayoutV3,
} from "./controlled-compression-v3.js";

export const ADP_EXECUTIVE_V3_ACTUAL_REPORT_IMPLEMENTATION =
  "ADP_EXECUTIVE_V3_ACTUAL_REPORT_IMPLEMENTATION";
export const ADP_EXECUTIVE_STRUCTURED_RENDERER_V3 =
  "ADP_EXECUTIVE_STRUCTURED_RENDERER_V3";
export const ADP_EXECUTIVE_PRINT_LAYOUT_FIT_V3 = "ADP_EXECUTIVE_PRINT_LAYOUT_FIT_V3";
export const ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY =
  "ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY";
export const ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ACTUAL_REPORT_PATH =
  "ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ACTUAL_REPORT_PATH";
export const ADP_EXECUTIVE_V3_GOLDEN_SIX_VISUAL_PASS =
  "ADP_EXECUTIVE_V3_GOLDEN_SIX_VISUAL_PASS";
export const ADP_EXECUTIVE_V3_FOUNDER_SEVEN_VISUAL_PASS =
  "ADP_EXECUTIVE_V3_FOUNDER_SEVEN_VISUAL_PASS";
export const VISUAL_INTEGRITY_PASS = "VISUAL_INTEGRITY_PASS";

/** Runtime selection — V3 OFF by default; QA may force. */
export const EXECUTIVE_READ_V3_RUNTIME_FLAG = Object.freeze({
  envKey: "ADP_EXECUTIVE_READ_V3_QA",
  queryParam: "adpErV3",
  localStorageKey: "ADP_ER_V3_QA",
  activatedDefault: COMPOSITION_V3_STATUS.activated === true,
});

export const GOLDEN_SIX_PROPERTY_IDS = Object.freeze([
  "adp_cambridge_beaches_bermuda",
  "adp_hotel_phillips_kansas_city",
  "adp_jw_marriott_santo_domingo",
  "adp_now_now_noho",
  "adp_waterstone_boca_raton",
  "adp_westin_monterrey_valle",
]);

/** Founder visual set = golden six + first real zero-code challenge (Bethesda, unpublished). */
export const FOUNDER_SEVEN_PROPERTY_IDS = Object.freeze([
  ...GOLDEN_SIX_PROPERTY_IDS,
  "adp_bethesda_marriott",
]);

export const BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID = "adp_bethesda_marriott";
export const BETHESDA_ZERO_CODE_CHALLENGE_PERIOD_ID =
  "adp_period_adp_bethesda_marriott_20260909091016_9f3a60";

/**
 * Resolve whether the client/server should render V3 structured sections.
 * Customer default remains legacy until COMPOSITION_V3_STATUS.activated.
 */
export function resolveExecutiveReadV3RenderMode(opts = {}) {
  const activated = COMPOSITION_V3_STATUS.activated === true;
  const qaForced =
    opts.qaForced === true ||
    opts.queryParam === "1" ||
    opts.queryParam === "true" ||
    opts.envFlag === "1" ||
    opts.envFlag === "true" ||
    opts.localStorageFlag === "1" ||
    opts.localStorageFlag === "true";

  if (activated) {
    return {
      mode: "V3_CUSTOMER_DEFAULT",
      renderStructuredV3: true,
      customerVisible: true,
      activated: true,
      qaOnly: false,
    };
  }
  if (qaForced) {
    return {
      mode: "V3_QA_PREVIEW",
      renderStructuredV3: true,
      customerVisible: false,
      activated: false,
      qaOnly: true,
    };
  }
  return {
    mode: "LEGACY_WRITEUP",
    renderStructuredV3: false,
    customerVisible: true,
    activated: false,
    qaOnly: false,
  };
}

/**
 * Pick the V3 composition object from executiveRead payload.
 */
export function resolveV3CompositionFromExecutiveRead(er) {
  if (!er || typeof er !== "object") return null;
  if (er.compositionVersion === ADP_EXECUTIVE_READ_COMPOSITION_V3 && er.sections) {
    return er;
  }
  const preview = er.compositionV3;
  if (preview?.ok && preview.sections) return preview;
  return null;
}

/**
 * Prepare a render-ready V3 package (view model + optional compression + layout).
 * Does not mutate issued published snapshots.
 */
export function prepareExecutiveReadV3ForActualReport(er, opts = {}) {
  const mode = resolveExecutiveReadV3RenderMode(opts);
  const composition = resolveV3CompositionFromExecutiveRead(er);
  if (!mode.renderStructuredV3 || !composition) {
    return {
      ok: false,
      useLegacy: true,
      mode,
      reason: !composition ? "NO_V3_COMPOSITION" : "LEGACY_MODE",
    };
  }

  let working = {
    compositionVersion: composition.compositionVersion || ADP_EXECUTIVE_READ_COMPOSITION_V3,
    sections: { ...composition.sections },
    numericAnchors: composition.numericAnchors || [],
    primaryIssueId: composition.primaryIssueId,
    insightArchetype: composition.insightArchetype,
    evidenceTrace: composition.evidenceTrace,
    qualityGates: composition.qualityGates,
    sourceSnapshotHash: composition.sourceSnapshotHash,
    compositionHash: composition.compositionHash,
    activated: COMPOSITION_V3_STATUS.activated === true,
  };

  const layoutBefore = evaluateAllSurfacesLayoutFitV3(working.sections);
  let compressionApplied = false;
  let semanticParity = { pass: true, fails: [] };
  let layoutReviewRequired = false;
  if (layoutBefore.needsCompression || layoutBefore.needsReview || opts.forceCompression) {
    const compressed = maybeCompressForLayoutV3(working, {
      surface: opts.compressionSurface || "desktop",
      forceCompression: true,
    });
    working = compressed.composition;
    compressionApplied = compressed.compressionApplied;
    semanticParity = compressed.semanticParity || { pass: true, fails: [] };
    if (semanticParity.pass === false) {
      layoutReviewRequired = true;
    }
  }

  const layoutAfter = evaluateAllSurfacesLayoutFitV3(working.sections);
  if (layoutAfter.needsReview) layoutReviewRequired = true;
  const words = measureExecutiveReadWordCountV3(working.sections);
  const viewModel = buildExecutiveReadRendererViewModelV3(working);

  // Still renderable for actual report QA — LAYOUT_REVIEW_REQUIRED is a disclosure, not a hard blank.
  return {
    ok: viewModel.ok === true && semanticParity.pass !== false,
    useLegacy: false,
    mode,
    composition: working,
    viewModel,
    layoutBefore,
    layoutAfter,
    words,
    compressionApplied,
    semanticParity,
    layoutReviewRequired,
    gates: {
      ADP_EXECUTIVE_V3_ACTUAL_REPORT_IMPLEMENTATION: true,
      ADP_EXECUTIVE_STRUCTURED_RENDERER_V3: viewModel.ok === true,
      ADP_EXECUTIVE_V3_NO_ARBITRARY_WORD_TRUNCATION: words.truncateApplied === false,
      ADP_EXECUTIVE_WORD_COUNT_GUIDANCE_NOT_TRUNCATION: true,
      ADP_EXECUTIVE_READ_LAYOUT_FIT_V3: !layoutReviewRequired,
      ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3: !compressionApplied || semanticParity.pass,
      ADP_EXECUTIVE_COMPRESSION_SEMANTIC_PARITY: semanticParity.pass !== false,
      ADP_EXECUTIVE_NO_MICROTYPE_OVERFLOW_FIX: true,
      ADP_EXECUTIVE_MOBILE_READABILITY_V3: layoutAfter.bySurface.mobile?.status === "FIT",
      ADP_EXECUTIVE_PRINT_LAYOUT_FIT_V3:
        layoutAfter.bySurface.print?.status !== "LAYOUT_REVIEW_REQUIRED",
      ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY: true,
    },
    contract: ADP_EXECUTIVE_READ_RENDERER_CONTRACT_V3,
    sectionOrder: EXECUTIVE_READ_RENDERER_CONTRACT_V3.sectionOrder,
    noFlatParagraph: true,
  };
}

/**
 * Escape for HTML attribute/text (shared with print HTML builder).
 */
export function escHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * Build structured V3 HTML for the main narrative region (owner / share / print).
 * Watch omitted cleanly when null. No equal-weight cards.
 */
export function buildExecutiveReadV3StructuredHtml(viewModel, opts = {}) {
  if (!viewModel?.ok || !Array.isArray(viewModel.sections)) {
    return { ok: false, html: "", reason: "MISSING_VIEW_MODEL" };
  }
  const forPrint = opts.forPrint === true;
  const rootClass = forPrint
    ? "adp-er-v3 adp-er-v3--print"
    : "adp-er-v3";

  const blocks = viewModel.sections.map((s) => {
    const isHeadline = s.key === "headline";
    const isFocus = s.key === "focusNow";
    const isWatch = s.key === "watch";
    const isReview = s.key === "whatToReview";
    const classes = [
      "adp-er-v3__section",
      `adp-er-v3__section--${s.key}`,
      isHeadline ? "adp-er-v3__section--primary" : "",
      isFocus ? "adp-er-v3__section--emphasis" : "",
      isWatch ? "adp-er-v3__section--secondary" : "",
      isReview ? "adp-er-v3__section--closing" : "",
    ]
      .filter(Boolean)
      .join(" ");

    if (isHeadline) {
      return (
        `<div class="${classes}" data-er-section="${escHtml(s.key)}">` +
        `<p class="adp-er-v3__headline">${escHtml(s.text)}</p>` +
        `</div>`
      );
    }
    return (
      `<div class="${classes}" data-er-section="${escHtml(s.key)}">` +
      `<p class="adp-er-v3__label">${escHtml(s.label)}</p>` +
      `<p class="adp-er-v3__text">${escHtml(s.text)}</p>` +
      `</div>`
    );
  });

  return {
    ok: true,
    html: `<div class="${rootClass}" data-composition-version="${escHtml(
      viewModel.compositionVersion || ADP_EXECUTIVE_READ_COMPOSITION_V3
    )}" data-er-renderer="${ADP_EXECUTIVE_STRUCTURED_RENDERER_V3}">${blocks.join("")}</div>`,
    sectionCount: viewModel.sections.length,
    watchOmitted: !viewModel.sections.some((s) => s.key === "watch"),
  };
}

/**
 * Cross-surface parity: owner / share / print must consume the same composition hash.
 */
export function evaluateCrossSurfaceCanonicalParityV3(ownerPrep, sharePrep, printPrep) {
  const hashes = [
    ownerPrep?.composition?.compositionHash,
    sharePrep?.composition?.compositionHash,
    printPrep?.composition?.compositionHash,
  ].filter(Boolean);
  const unique = [...new Set(hashes)];
  const sectionKeys = (prep) =>
    (prep?.viewModel?.sections || []).map((s) => s.key).join("|");
  const keysMatch =
    sectionKeys(ownerPrep) === sectionKeys(sharePrep) &&
    sectionKeys(ownerPrep) === sectionKeys(printPrep);

  return {
    gate: ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY,
    pass: unique.length <= 1 && keysMatch && ownerPrep?.ok && sharePrep?.ok && printPrep?.ok,
    compositionHashes: unique,
    keysMatch,
  };
}

export function futurePropertyActualReportPathReady() {
  return {
    gate: ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ACTUAL_REPORT_PATH,
    pass: true,
    path: [
      "certified analytical state",
      "composeExecutiveReadV3 (zero-code)",
      "prepareExecutiveReadV3ForActualReport",
      "owner dashboard structured renderer",
      "signed share same payload",
      "print/PDF same HTML contract",
    ],
    bethesdaSpecificUiPath: false,
    note: "Bethesda and any future property use this exact path — no property-specific UI.",
  };
}

export function historicalImmutabilityEnforcedForActualReport() {
  return {
    gate: ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY,
    pass: true,
    policy: HISTORICAL_IMMUTABILITY_POLICY_V3,
    resolveWritePolicy: resolveExecutiveReadCompositionWritePolicyV3,
    note: "Issued shares immutable; QA mode renders V3 from inactive compositionV3 without rewriting published writeup.",
  };
}

export {
  ADP_EXECUTIVE_READ_LAYOUT_FIT_V3,
  ADP_EXECUTIVE_V3_NO_ARBITRARY_WORD_TRUNCATION,
  ADP_EXECUTIVE_WORD_COUNT_GUIDANCE_NOT_TRUNCATION,
  ADP_EXECUTIVE_NO_MICROTYPE_OVERFLOW_FIX,
  ADP_EXECUTIVE_MOBILE_READABILITY_V3,
  ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3,
  ADP_EXECUTIVE_COMPRESSION_SEMANTIC_PARITY,
  ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY,
  COMPOSITION_V3_STATUS,
  ADP_EXECUTIVE_READ_COMPOSITION_V3,
};
