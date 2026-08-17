/**
 * v41 — Brand Explorer OS canonical state machine.
 * Single canonical state per brand. Live API/render beats report-only readiness.
 */
export const V41_OS_VERSION = "v41";

export const BRAND_EXPLORER_OS_STATES = Object.freeze([
  "not_started",
  "sources_seeded",
  "knowledge_ready",
  "asset_ready",
  "draft_ready",
  "draft_applied_with_defects",
  "internal_preview_blocked",
  "internal_preview_ready",
  "founder_review_ready",
  "active_release_ready",
  "active_profile_ready",
  "state_conflict",
]);

/**
 * Operational release cohort — NOT the Brand Explorer active universe.
 * Active universe = Brand Basics Brand Status Active/Live (see brand-explorer-active-universe.js).
 * OS release-readiness defaults to this subset; pass --brands or active-universe
 * tooling when evaluating the full 24.
 */
export const PRIMARY_RELEASE_SLUGS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  "design-hotels",
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

/** Empty after v43 fourth-batch graduation (Indigo / MGallery / SLH). */
export const INCOMPLETE_CONTROL_SLUGS = Object.freeze([]);

/**
 * Historical incomplete lifestyle cohort — graduated to primary via v43 after v42A-R2.
 * Kept for historical batch scripts (v46/v47/v42A-R2) that must not follow live incomplete list.
 */
export const GRADUATED_LIFESTYLE_COHORT_SLUGS = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

/** Original golden four before lifestyle graduation (for historical protect lists). */
export const ORIGINAL_GOLDEN_RELEASE_SLUGS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  "design-hotels",
]);

export const DEFAULT_OS_BRANDS = Object.freeze([
  ...PRIMARY_RELEASE_SLUGS,
  ...INCOMPLETE_CONTROL_SLUGS,
]);

/**
 * Resolve one canonical OS state from evaluated gate bundle.
 * @param {object} input
 */
export function resolveCanonicalBrandState(input = {}) {
  const {
    brandExists = false,
    factoryConfigExists = false,
    sourceCoverageReady = false,
    galleryReady = false,
    propertyExamplesReady = false,
    visualAssetPackReady = false,
    liveInternalPreviewClean = false,
    residualPresentationDirty = false,
    externalQualityLockPass = false,
    externalFullProfileRendered = false,
    founderVisualReviewPassed = false,
    activeReleaseApproved = false,
    reportReadyButLiveBlocked = false,
    companyValidatedClaimWithoutEvidence = false,
    brandSpecificSourceValidationPass = true,
    renderedFieldCompletenessPass = true,
    goldenContentQualityPass = true,
    tabFactoryAuditPass = true,
    sourceProvenanceByTabPass = true,
    noEmptyRenderedComponentsPass = true,
    imageDistinctivenessPass = true,
    cohort = "primary",
  } = input;

  const conflicts = [];
  if (reportReadyButLiveBlocked) conflicts.push("report_ready_but_live_blocked");
  if (companyValidatedClaimWithoutEvidence) conflicts.push("company_validated_claim_without_evidence");
  if (externalFullProfileRendered && !activeReleaseApproved) {
    conflicts.push("external_full_profile_without_active_approval");
  }

  if (!brandExists) {
    return { canonicalState: "not_started", conflicts, rationale: "Brand record missing." };
  }
  if (!factoryConfigExists && cohort === "incomplete") {
    // discovery config may still exist
  }
  if (!factoryConfigExists && !sourceCoverageReady && !galleryReady) {
    return {
      canonicalState: "not_started",
      conflicts,
      rationale: "No factory config and no source/visual readiness.",
    };
  }

  if (conflicts.includes("external_full_profile_without_active_approval")) {
    return {
      canonicalState: "state_conflict",
      conflicts,
      rationale: "External full profile rendered without active release approval.",
    };
  }

  if (reportReadyButLiveBlocked) {
    return {
      canonicalState: "state_conflict",
      conflicts,
      rationale: "Report-only readiness conflicts with live API/render blockers.",
    };
  }

  // Residual Presentation dirty → defects remain even if renderer scrub hides some tokens.
  if (residualPresentationDirty) {
    return {
      canonicalState: "draft_applied_with_defects",
      conflicts,
      rationale:
        "Presentation residual owner-copy still dirty (v40C patches pending). Do not treat as founder-ready until apply.",
    };
  }

  if (!liveInternalPreviewClean) {
    return {
      canonicalState: "internal_preview_blocked",
      conflicts,
      rationale: "Live internal preview still contains forbidden owner-copy.",
    };
  }

  if (!galleryReady || !propertyExamplesReady || !visualAssetPackReady) {
    if (sourceCoverageReady && !galleryReady) {
      return {
        canonicalState: "sources_seeded",
        conflicts,
        rationale: "Sources present but gallery/property visuals incomplete.",
      };
    }
    if (galleryReady || propertyExamplesReady) {
      return {
        canonicalState: "asset_ready",
        conflicts,
        rationale: "Partial visual readiness; not all imageUrl gates pass.",
      };
    }
    return {
      canonicalState: "draft_applied_with_defects",
      conflicts,
      rationale: "Draft/visual defects block founder path.",
    };
  }

  // Mandatory content/source gates — block founder_review_ready and active_profile_ready
  if (
    !brandSpecificSourceValidationPass ||
    !renderedFieldCompletenessPass ||
    !goldenContentQualityPass ||
    !tabFactoryAuditPass ||
    !sourceProvenanceByTabPass ||
    !noEmptyRenderedComponentsPass ||
    !imageDistinctivenessPass
  ) {
    return {
      canonicalState: "draft_applied_with_defects",
      conflicts,
      rationale:
        "Mandatory Tab Factory release gates failed: source provenance by tab, tab-factory audit, rendered field completeness (auditPass requires failFindings=0), no-empty rendered components, image distinctiveness, and/or golden content quality. Remediate before founder_review_ready.",
    };
  }

  if (!externalQualityLockPass && externalFullProfileRendered) {
    return {
      canonicalState: "state_conflict",
      conflicts: [...conflicts, "external_lock_leak"],
      rationale: "External quality lock failed while full profile appears rendered.",
    };
  }

  // Visuals + internal preview clean + residual clean + mandatory quality gates
  if (!founderVisualReviewPassed) {
    return {
      canonicalState: "founder_review_ready",
      conflicts,
      rationale: "Internal preview owner-copy clean; founder visual review is the next gate.",
    };
  }

  if (!activeReleaseApproved) {
    return {
      canonicalState: "active_release_ready",
      conflicts,
      rationale: "Founder visual review passed; active release approval not set.",
    };
  }

  return {
    canonicalState: "active_profile_ready",
    conflicts,
    rationale: "Active release approval set; profile eligible for owner-ready path.",
  };
}

export function isTerminalUnlockState(state) {
  return state === "active_profile_ready";
}
