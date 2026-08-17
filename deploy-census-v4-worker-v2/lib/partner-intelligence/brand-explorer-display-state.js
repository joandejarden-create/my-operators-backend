/**
 * Brand Explorer external display state — canonical v38 contract.
 *
 * Source Library seeding alone must never surface a brand as externally owner-ready.
 * Previously approved/finished brands must not be silently hidden solely because they
 * are outside the latest PRIMARY_RELEASE_SLUGS cohort — they route through legacy migration.
 */
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { isLegacyVisibilityUnlockHeld } from "./brand-explorer-public-restore-registry.js";

export const BRAND_EXPLORER_DISPLAY_STATES = Object.freeze({
  hidden_incomplete: "hidden_incomplete",
  internal_preview_only: "internal_preview_only",
  draft_applied_with_defects: "draft_applied_with_defects",
  founder_review_ready: "founder_review_ready",
  external_owner_ready: "external_owner_ready",
  active_profile_ready: "active_profile_ready",
  legacy_approved_pending_migration: "legacy_approved_pending_migration",
  /**
   * Internal-only factory candidate preview (never public-full).
   * Assigned by Factory Preview Mode client/API meta — not by resolveBrandExplorerDisplayState.
   */
  factory_preview_internal: "factory_preview_internal",
});

/** States that may render the full external Brand Explorer profile. */
export const FULL_PROFILE_DISPLAY_STATES = Object.freeze([
  BRAND_EXPLORER_DISPLAY_STATES.external_owner_ready,
  BRAND_EXPLORER_DISPLAY_STATES.active_profile_ready,
]);

const GALLERY_MIN = 6;
const PROPERTY_EXAMPLE_MIN = 3;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function presentationBlocks(brand, options = {}) {
  if (Array.isArray(options.presentationRows) && options.presentationRows.length) {
    return options.presentationRows;
  }
  return Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
}

function countGalleryWithImageUrl(blocks) {
  return blocks.filter(
    (b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)
  ).length;
}

function countPropertyExamplesWithImageUrl(blocks) {
  return blocks.filter((b) => b.slotKey === "footprint.openings" && nz(b.imageUrl)).length;
}

function hasScenarioPresentationRows(blocks) {
  return [1, 2, 3].every((i) => blocks.some((b) => nz(b.slotKey) === `overview.scenario.${i}`));
}

function readCompanyValidated(brand, options = {}) {
  if (brand?.governance?.companyValidated === true) return true;
  const basics = options.brandBasics?.fields || options.brandBasics || {};
  return basics["Company Validated"] === true || basics.company_validated === true;
}

function readActiveProfileApproved(brand, options = {}) {
  if (brand?.readyForActiveProfile === true || brand?.activeProfileApproved === true) return true;
  const basics = options.brandBasics?.fields || options.brandBasics || {};
  return (
    basics["Ready for Active Profile"] === true ||
    basics["Active Profile Approved"] === true ||
    basics["Active Profile Approved Date"] != null
  );
}

function readFounderVisualReviewPass(brand, options = {}) {
  if (brand?.founderVisualReviewPass === true) return true;
  const basics = options.brandBasics?.fields || options.brandBasics || {};
  return (
    basics["Founder Visual Review Pass"] === true ||
    basics["Founder Visual Review"] === "Pass"
  );
}

function readHistoricalApproved(brand, options = {}) {
  if (options.legacyHistoricalApproved === true || brand?.legacyHistoricalApproved === true) {
    return true;
  }
  if (brand?.legacyApprovedProfileMigrated === true || options.legacyApprovedProfileMigrated === true) {
    return true;
  }
  const basics = options.brandBasics?.fields || options.brandBasics || {};
  return (
    basics["Complete Build"] === true ||
    basics["QA Ready"] === true ||
    basics["QA ready"] === true ||
    /^(pass|passed|complete|completed|ready|approved)$/i.test(nz(basics["Build Status"])) ||
    /^(complete|completed|ready|approved)$/i.test(nz(basics["Profile Status"]))
  );
}

function resolveReadinessGates(options = {}) {
  const g = options.readinessGates || options.gates || {};
  return {
    source_ready: g.source_ready !== false,
    knowledge_pack_ready: g.knowledge_pack_ready !== false,
    visual_asset_pack_ready: g.visual_asset_pack_ready !== false,
    render_ready: g.render_ready !== false,
    presentation_plan_ready: g.presentation_plan_ready !== false,
    copy_ready: g.copy_ready !== false,
    external_owner_ready: g.external_owner_ready !== false,
    founder_visual_review_passed: g.founder_visual_review_passed !== false,
    active_profile_approval_passed: g.active_profile_approval_passed !== false,
  };
}

/**
 * @param {object} brand
 * @param {object} [options]
 */
export function resolveBrandExplorerDisplayState(brand = {}, options = {}) {
  const blocks = presentationBlocks(brand, options);
  const hasScenarioRows = hasScenarioPresentationRows(blocks);
  const galleryCount = countGalleryWithImageUrl(blocks);
  const openingsCount = countPropertyExamplesWithImageUrl(blocks);
  const imageUniqueness =
    options.imageUniqueness ||
    evaluateImageUniqueness({ brand, presentationRows: blocks, brandSlug: brand?.slug });
  const imageRoleMatch =
    options.imageRoleMatch ||
    evaluateBrandImageRoleMatch({ presentationRows: blocks, brandSlug: brand?.slug });
  const visualsCountReady = galleryCount >= GALLERY_MIN && openingsCount >= PROPERTY_EXAMPLE_MIN;
  const visualsReady =
    visualsCountReady &&
    imageUniqueness.galleryDistinctCount >= GALLERY_MIN &&
    imageUniqueness.propertyExampleDistinctCount >= PROPERTY_EXAMPLE_MIN &&
    imageRoleMatch.pass === true;
  const hasPresentationRows = blocks.length > 0;
  const externalOwnerRule = evaluateExternalOwnerReadinessRule(blocks);
  const companyValidated = readCompanyValidated(brand, options);
  const activeProfileApproved = readActiveProfileApproved(brand, options);
  const founderVisualReviewPass = readFounderVisualReviewPass(brand, options);
  const historicalApproved = readHistoricalApproved(brand, options);
  const renderContractPass = options.renderContract?.pass === true;
  const fullTabContractPass = options.fullTabContract?.pass === true;
  const sourceLibrarySeeded = options.sourceLibrarySeeded === true;
  const gates = resolveReadinessGates(options);

  const blockers = [];
  if (!hasPresentationRows) blockers.push("missing_presentation_rows");
  if (!hasScenarioRows) blockers.push("missing_scenario_rows");
  if (galleryCount < GALLERY_MIN) blockers.push(`gallery_images_${galleryCount}_of_${GALLERY_MIN}`);
  if (openingsCount < PROPERTY_EXAMPLE_MIN) {
    blockers.push(`property_examples_${openingsCount}_of_${PROPERTY_EXAMPLE_MIN}`);
  }
  if (imageUniqueness.pass !== true) blockers.push("image_uniqueness_fail");
  if (imageRoleMatch.pass !== true) blockers.push("image_role_match_fail");
  if (!externalOwnerRule.pass) blockers.push("external_owner_copy_fail");
  if (!companyValidated) blockers.push("not_company_validated");
  if (!founderVisualReviewPass) blockers.push("founder_visual_review_not_passed");
  if (!activeProfileApproved) blockers.push("active_profile_not_approved");
  if (historicalApproved && !activeProfileApproved) {
    blockers.push("legacy_approved_pending_migration");
  }
  if (options.renderContract && !renderContractPass) blockers.push("render_contract_fail");
  if (options.fullTabContract && !fullTabContractPass) blockers.push("full_tab_contract_fail");

  if (options.readinessGates || options.gates) {
    if (!gates.source_ready) blockers.push("source_not_ready");
    if (!gates.knowledge_pack_ready) blockers.push("knowledge_pack_not_ready");
    if (!gates.visual_asset_pack_ready) blockers.push("visual_asset_pack_not_ready");
    if (!gates.render_ready) blockers.push("render_not_ready");
    if (!gates.presentation_plan_ready) blockers.push("presentation_plan_not_ready");
    if (!gates.copy_ready) blockers.push("copy_not_ready");
    if (!gates.external_owner_ready) blockers.push("external_owner_score_not_ready");
  }

  const stagingFallbackRisk =
    !hasScenarioRows || galleryCount < GALLERY_MIN || openingsCount < PROPERTY_EXAMPLE_MIN;

  const allExternalOwnerGatesPass =
    hasPresentationRows &&
    hasScenarioRows &&
    visualsReady &&
    externalOwnerRule.pass &&
    companyValidated &&
    founderVisualReviewPass &&
    activeProfileApproved &&
    (!options.renderContract || renderContractPass) &&
    (!options.fullTabContract || fullTabContractPass) &&
    (!options.readinessGates && !options.gates
      ? true
      : gates.source_ready &&
        gates.knowledge_pack_ready &&
        gates.visual_asset_pack_ready &&
        gates.render_ready &&
        gates.presentation_plan_ready &&
        gates.copy_ready &&
        gates.external_owner_ready &&
        gates.founder_visual_review_passed &&
        gates.active_profile_approval_passed);

  const activeProfileGatesPass =
    hasPresentationRows &&
    hasScenarioRows &&
    visualsReady &&
    externalOwnerRule.pass &&
    activeProfileApproved &&
    (!options.renderContract || renderContractPass);

  let brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.hidden_incomplete;

  if (allExternalOwnerGatesPass) {
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.external_owner_ready;
  } else if (activeProfileGatesPass && activeProfileApproved) {
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.active_profile_ready;
  } else if (
    hasPresentationRows &&
    visualsReady &&
    externalOwnerRule.pass &&
    activeProfileApproved &&
    founderVisualReviewPass
  ) {
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.founder_review_ready;
  } else if (historicalApproved && hasPresentationRows && !activeProfileApproved) {
    // Historically approved/finished profiles must not fall through to silent
    // hidden_incomplete merely because they are outside the latest release cohort.
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.legacy_approved_pending_migration;
  } else if (hasPresentationRows && (visualsCountReady || hasScenarioRows)) {
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.draft_applied_with_defects;
  } else if (hasPresentationRows || sourceLibrarySeeded) {
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.internal_preview_only;
  }

  if (
    stagingFallbackRisk &&
    !historicalApproved &&
    brandExplorerDisplayState !== BRAND_EXPLORER_DISPLAY_STATES.external_owner_ready &&
    brandExplorerDisplayState !== BRAND_EXPLORER_DISPLAY_STATES.active_profile_ready &&
    brandExplorerDisplayState !== BRAND_EXPLORER_DISPLAY_STATES.legacy_approved_pending_migration
  ) {
    brandExplorerDisplayState = BRAND_EXPLORER_DISPLAY_STATES.hidden_incomplete;
  }

  // Historically approved profiles with presentation + gallery/opening counts + distinct
  // images must not stay locked solely because they are outside PRIMARY_RELEASE_SLUGS or
  // lack new Active Profile Approved fields / newer role-match gates.
  // Image uniqueness fail → stay locked (image_remediation). Role-match is not required
  // for this legacy visibility unlock (global role-match remediation is a separate pass).
  // Accidental legacy unlock for country/suburban/woodspring is held until intentional
  // public-restore governance registry membership (founder-approved).
  const brandSlug = nz(brand?.slug || options.brandSlug || options.slug);
  const legacyUnlockHeld = brandSlug ? isLegacyVisibilityUnlockHeld(brandSlug) : false;
  const legacyVisibilityUnlock =
    historicalApproved &&
    hasPresentationRows &&
    visualsCountReady &&
    imageUniqueness.pass === true &&
    !legacyUnlockHeld;

  const shouldRenderFullProfile =
    FULL_PROFILE_DISPLAY_STATES.includes(brandExplorerDisplayState) || legacyVisibilityUnlock;
  const shouldSuppressIncompleteExternalSections = !shouldRenderFullProfile;

  return {
    brandExplorerDisplayState,
    shouldRenderFullProfile,
    shouldSuppressIncompleteExternalSections,
    shouldHideExternalProfile: !shouldRenderFullProfile,
    completeness: {
      hasPresentationRows,
      hasScenarioRows,
      galleryCount,
      galleryRequired: GALLERY_MIN,
      galleryDistinctCount: imageUniqueness.galleryDistinctCount,
      openingsCount,
      openingsRequired: PROPERTY_EXAMPLE_MIN,
      propertyExampleDistinctCount: imageUniqueness.propertyExampleDistinctCount,
      visualsReady,
      visualsCountReady,
      imageUniquenessPass: imageUniqueness.pass === true,
      imageRoleMatchPass: imageRoleMatch.pass === true,
      unresolvedRoleMismatchCount: imageRoleMatch.unresolvedRoleMismatchCount,
      stagingFallbackRisk,
      companyValidated,
      activeProfileApproved,
      founderVisualReviewPass,
      historicalApproved,
      legacyVisibilityUnlockHeld: legacyUnlockHeld,
      legacyVisibilityUnlock,
      externalOwnerRulePass: externalOwnerRule.pass,
      renderContractPass: options.renderContract ? renderContractPass : null,
      fullTabContractPass: options.fullTabContract ? fullTabContractPass : null,
      readinessGates: options.readinessGates || options.gates || null,
    },
    blockers,
    imageUniqueness,
    imageRoleMatch,
    readyForActiveApproval: false,
  };
}

export function shouldRenderFullBrandExplorerProfile(brand = {}) {
  if (brand.shouldRenderFullProfile === true) return true;
  if (brand.shouldRenderFullProfile === false) return false;
  const state = nz(brand.brandExplorerDisplayState);
  if (FULL_PROFILE_DISPLAY_STATES.includes(state)) return true;
  if (state === BRAND_EXPLORER_DISPLAY_STATES.legacy_approved_pending_migration) {
    const c = brand.brandExplorerDisplayCompleteness || {};
    const slug = nz(brand?.slug);
    if (
      c.historicalApproved === true &&
      c.visualsCountReady === true &&
      c.imageUniquenessPass === true &&
      !(slug && isLegacyVisibilityUnlockHeld(slug))
    ) {
      return true;
    }
    // Prefer live re-resolve when completeness payload is absent.
    return resolveBrandExplorerDisplayState(brand).shouldRenderFullProfile === true;
  }
  if (brand.legacyHistoricalApproved === true || brand.legacyApprovedProfileMigrated === true) {
    return resolveBrandExplorerDisplayState(brand, {
      legacyHistoricalApproved: true,
    }).shouldRenderFullProfile === true;
  }
  return false;
}

export function isDisplayStateIncomplete(state) {
  return (
    !FULL_PROFILE_DISPLAY_STATES.includes(state) &&
    state !== BRAND_EXPLORER_DISPLAY_STATES.legacy_approved_pending_migration
  );
}
