/**
 * v41 — Brand Explorer OS action router.
 * Exactly one allowed next action per brand.
 */
import { PRIMARY_RELEASE_SLUGS, INCOMPLETE_CONTROL_SLUGS } from "./brand-explorer-os-state-machine.js";
import { buildV40CApplyDesign } from "./brand-explorer-economics-chrome-remediation.js";

export const OS_ACTIONS = Object.freeze([
  "no_action",
  "source_capture",
  "image_remediation",
  "apply_source_seed",
  "apply_draft",
  "apply_remediation",
  "internal_preview_review",
  "founder_visual_review",
  "apply_active_release",
  "investigate_state_conflict",
]);

/**
 * @param {object} ctx
 */
export function routeBrandExplorerOsAction(ctx = {}) {
  const {
    brandSlug,
    canonicalState,
    metrics = {},
    failedGates = [],
    stateConflicts = [],
    residualPatchCount = 0,
    v40cApplyAllowed = false,
  } = ctx;

  const blockedActions = new Set();

  // Always block unlock paths without prerequisites
  if (!metrics.founderVisualReviewPassed) blockedActions.add("apply_active_release");
  if (!metrics.liveInternalPreviewClean || metrics.residualPresentationDirty) {
    blockedActions.add("apply_active_release");
    blockedActions.add("founder_visual_review");
  }
  if (
    metrics.brandSpecificSourceValidationPass === false ||
    metrics.renderedFieldCompletenessPass === false ||
    metrics.goldenContentQualityPass === false ||
    metrics.tabFactoryAuditPass === false ||
    metrics.sourceProvenanceByTabPass === false ||
    metrics.noEmptyRenderedComponentsPass === false ||
    metrics.imageDistinctivenessPass === false ||
    metrics.imageRoleMatchPass === false
  ) {
    blockedActions.add("apply_active_release");
    blockedActions.add("founder_visual_review");
  }
  if (!metrics.galleryReady || !metrics.propertyExamplesReady) {
    blockedActions.add("apply_draft");
    blockedActions.add("founder_visual_review");
    blockedActions.add("apply_active_release");
  }
  if (!metrics.activeReleaseApproved) {
    blockedActions.add("external_full_render");
  }

  let allowedNextAction = "no_action";
  let exactNextCommand = null;
  let rationale = "";

  if (canonicalState === "state_conflict" || (stateConflicts || []).length > 0) {
    allowedNextAction = "investigate_state_conflict";
    rationale = "Conflicting readiness signals — investigate before any apply.";
    exactNextCommand = `npm run brand-explorer-os -- --brands ${brandSlug} --stage release-readiness --dry-run`;
  } else if (canonicalState === "not_started") {
    allowedNextAction = "source_capture";
    rationale = "Brand not seeded.";
    exactNextCommand = `npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands ${brandSlug} --dry-run`;
  } else if (
    failedGates.includes("gallery_six_imageurl") ||
    failedGates.includes("property_examples_three_imageurl") ||
    failedGates.includes("visual_asset_pack_ready") ||
    failedGates.includes("image_distinctiveness") ||
    failedGates.includes("image_role_match")
  ) {
    allowedNextAction = "image_remediation";
    rationale =
      "Live gallery/property images incomplete, not distinct, or caption/role mismatch — run uniqueness + role-match audits then remediate.";
    exactNextCommand = failedGates.includes("image_role_match")
      ? `npm run brand-explorer-image-role-match-audit -- --brands ${brandSlug} --dry-run`
      : `npm run brand-explorer-image-uniqueness-audit -- --brands ${brandSlug} --dry-run`;
  } else if (
    failedGates.includes("source_coverage_ready") &&
    INCOMPLETE_CONTROL_SLUGS.includes(brandSlug)
  ) {
    allowedNextAction = "apply_source_seed";
    rationale = "Source coverage incomplete for incomplete-control brand.";
    exactNextCommand = `npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands ${brandSlug} --dry-run`;
  } else if (metrics.residualPresentationDirty || residualPatchCount > 0) {
    // Residual Presentation patches pending — do not advance to founder review yet
    allowedNextAction = "apply_remediation";
    rationale =
      brandSlug === "design-hotels"
        ? "v45 Design Hotels OS remediation pending. Affiliation/curation residual Presentation patches must apply before founder review."
        : "v40C residual Presentation patches pending. Internal preview may look clean via renderer scrub, but Airtable copy still needs apply.";
    if (brandSlug === "design-hotels") {
      exactNextCommand =
        "npm run brand-explorer-v45-design-hotels-os-remediation -- --brand design-hotels --dry-run";
    } else {
      exactNextCommand = v40cApplyAllowed
        ? buildV40CApplyDesign([brandSlug]).command
        : `npm run brand-explorer-v40c-economics-chrome-remediation -- --brands ${brandSlug} --dry-run`;
    }
    blockedActions.add("founder_visual_review");
    blockedActions.add("apply_active_release");
  } else if (
    metrics.brandSpecificSourceValidationPass === false ||
    metrics.renderedFieldCompletenessPass === false ||
    metrics.goldenContentQualityPass === false ||
    metrics.tabFactoryAuditPass === false ||
    metrics.sourceProvenanceByTabPass === false ||
    metrics.noEmptyRenderedComponentsPass === false ||
    metrics.imageDistinctivenessPass === false ||
    metrics.imageRoleMatchPass === false
  ) {
    allowedNextAction = "apply_remediation";
    const parts = [];
    if (metrics.tabFactoryAuditPass === false) parts.push("tab-factory-audit");
    if (metrics.sourceProvenanceByTabPass === false) parts.push("source-provenance-by-tab");
    if (metrics.noEmptyRenderedComponentsPass === false) parts.push("no-empty-rendered-components");
    if (metrics.brandSpecificSourceValidationPass === false) {
      parts.push("brand-specific-source-validation");
    }
    if (metrics.renderedFieldCompletenessPass === false) {
      parts.push(
        `rendered-field-completeness (failFindings=${metrics.renderedFieldCompletenessFailFindings || "?"})`
      );
    }
    if (metrics.goldenContentQualityPass === false) parts.push("golden-content-quality");
    if (metrics.imageDistinctivenessPass === false) parts.push("image-distinctiveness");
    if (metrics.imageRoleMatchPass === false) parts.push("image-role-match");
    rationale = `Mandatory Tab Factory release gates failed: ${parts.join(", ")}. Remediate before founder_review_ready.`;
    exactNextCommand =
      metrics.imageRoleMatchPass === false
        ? `npm run brand-explorer-image-role-match-remediation -- --brands ${brandSlug} --dry-run`
        : `npm run brand-explorer-tab-factory-remediation -- --brands ${brandSlug} --dry-run`;
    blockedActions.add("founder_visual_review");
    blockedActions.add("apply_active_release");
  } else if (!metrics.liveInternalPreviewClean) {
    allowedNextAction = "internal_preview_review";
    rationale = "Live internal preview still has forbidden owner-copy.";
    exactNextCommand = `npm run test:brand-explorer-internal-preview-owner-copy -- --brands ${brandSlug} --no-project-residual`;
  } else if (!metrics.founderVisualReviewPassed) {
    allowedNextAction = "founder_visual_review";
    rationale = "Internal preview + residual clean; founder visual review is next.";
    exactNextCommand = `npm run brand-explorer-v42-founder-visual-review -- --brands ${brandSlug} --dry-run`;
  } else if (!metrics.activeReleaseApproved) {
    allowedNextAction = "apply_active_release";
    rationale = "Founder review passed; active release apply is next (not executed by OS).";
    exactNextCommand = `npm run brand-explorer-v43-active-release-apply -- --brands ${brandSlug} --dry-run`;
  } else {
    allowedNextAction = "no_action";
    rationale = "Active profile path complete from OS perspective.";
    exactNextCommand = null;
  }

  // Cohort soft-overrides for incomplete brands that somehow have residual clean
  if (INCOMPLETE_CONTROL_SLUGS.includes(brandSlug) && allowedNextAction === "apply_active_release") {
    allowedNextAction = "image_remediation";
    rationale = "Incomplete-control brand must not enter active release.";
    blockedActions.add("apply_active_release");
    blockedActions.add("founder_visual_review");
  }

  if (PRIMARY_RELEASE_SLUGS.includes(brandSlug) && allowedNextAction === "apply_active_release") {
    // OS never auto-unlocks; keep command as audit-only
    blockedActions.add("external_unlock");
  }

  return {
    allowedNextAction,
    blockedActions: [...blockedActions],
    exactNextCommand,
    rationale,
    founderReviewAllowed: allowedNextAction === "founder_visual_review",
    activeReleaseAllowed: allowedNextAction === "apply_active_release" && metrics.founderVisualReviewPassed,
    remediationRequired:
      allowedNextAction === "apply_remediation" ||
      allowedNextAction === "image_remediation" ||
      allowedNextAction === "internal_preview_review",
  };
}
