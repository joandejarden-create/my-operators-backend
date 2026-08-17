/**
 * v39 — Brand Explorer Active Release Gate Inventory.
 *
 * Live API / Presentation imageUrl is source of truth for external unlock.
 * Report-only readiness is never sufficient for release.
 */
import {
  FULL_PROFILE_DISPLAY_STATES,
  resolveBrandExplorerDisplayState,
} from "./brand-explorer-display-state.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";

export const V39_RELEASE_GATE_VERSION = "v39";

export const GALLERY_MIN = 6;
export const PROPERTY_EXAMPLE_MIN = 3;

export const RELEASE_OUTCOMES = Object.freeze({
  safe_to_unlock_after_active_approval: "safe_to_unlock_after_active_approval",
  release_remediation_required: "release_remediation_required",
  false_blocker_due_to_mapping: "false_blocker_due_to_mapping",
  not_owner_ready: "not_owner_ready",
});

export const NEXT_ACTIONS = Object.freeze({
  no_action: "no_action",
  release_remediation_required: "release_remediation_required",
  founder_visual_review_required: "founder_visual_review_required",
  safe_to_apply_active_release: "safe_to_apply_active_release",
  mapping_fix_required: "mapping_fix_required",
  not_owner_ready: "not_owner_ready",
});

export const MISMATCH_CLASSES = Object.freeze([
  "report_ready_but_api_locked",
  "api_ready_but_report_blocked",
  "registry_ready_but_render_locked",
  "active_approval_missing",
  "visual_materialization_missing",
  "stale_report",
  "legacy_ready_signal_not_connected",
  "true_owner_visible_issue",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function blocksFrom(brandApi, presentationRows) {
  if (Array.isArray(presentationRows) && presentationRows.length) return presentationRows;
  return Array.isArray(brandApi?.brandExplorer?.blocks) ? brandApi.brandExplorer.blocks : [];
}

function countGallery(blocks) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countOpenings(blocks) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
}

function countScenarios(blocks) {
  return [1, 2, 3].filter((i) => blocks.some((b) => nz(b.slotKey) === `overview.scenario.${i}`)).length;
}

function scenariosHavePlaceholders(blocks) {
  return [1, 2, 3].some((i) => {
    const row = blocks.find((b) => nz(b.slotKey) === `overview.scenario.${i}`);
    if (!row) return true;
    const body = nz(row.body);
    const img = nz(row.imageUrl);
    return !body || !img || /placeholder|IMAGE/i.test(img);
  });
}

function readBasicsField(brandBasics, ...keys) {
  const fields = brandBasics?.fields || brandBasics || {};
  for (const k of keys) {
    if (fields[k] != null && fields[k] !== "") return fields[k];
  }
  return null;
}

function gateResult({
  name,
  pass,
  expected,
  actual,
  sourceOfTruth,
  rootCause = null,
  issueType = "true_issue",
  remediation = null,
  airtableWriteRequired = false,
  codeChangeRequired = false,
}) {
  return {
    name,
    pass: Boolean(pass),
    expected,
    actual,
    sourceOfTruth,
    rootCause: pass ? null : rootCause,
    issueType: pass ? null : issueType,
    remediation: pass ? null : remediation,
    airtableWriteRequired: pass ? false : Boolean(airtableWriteRequired),
    codeChangeRequired: pass ? false : Boolean(codeChangeRequired),
  };
}

/**
 * Inventory all live release gates for a brand.
 */
export function inventoryReleaseGates({
  brandSlug,
  brandApi = null,
  brandConfig = null,
  brandBasics = null,
  presentationRows = null,
  registryAssets = [],
  assetPack = null,
  renderContract = null,
  fullTabContract = null,
  qualityLock = null,
  displayMeta = null,
} = {}) {
  const blocks = blocksFrom(brandApi, presentationRows);
  const galleryCount = countGallery(blocks);
  const openingsCount = countOpenings(blocks);
  const scenarioCount = countScenarios(blocks);
  const externalOwner = evaluateExternalOwnerReadinessRule(blocks);
  const display =
    displayMeta ||
    resolveBrandExplorerDisplayState(brandApi || {}, {
      presentationRows: blocks,
      brandBasics,
      renderContract,
      fullTabContract,
    });

  const activeApproved =
    brandApi?.readyForActiveProfile === true ||
    brandApi?.activeProfileApproved === true ||
    readBasicsField(brandBasics, "Ready for Active Profile", "Active Profile Approved") === true ||
    readBasicsField(brandBasics, "Active Profile Approved Date") != null;

  const founderPass =
    brandApi?.founderVisualReviewPass === true ||
    readBasicsField(brandBasics, "Founder Visual Review Pass") === true ||
    nz(readBasicsField(brandBasics, "Founder Visual Review")) === "Pass";

  const approvedRegistryCount = (registryAssets || []).filter(
    (r) =>
      r.explorerUsePermission === "Approved For Explorer" ||
      r.fields?.["Approved For Explorer Use"] === "Yes"
  ).length;

  const sourceCoverageOk = approvedRegistryCount >= 3 || (assetPack?.summary && galleryCount >= 0);
  // Prefer Source Library / registry evidence; fall back to presentation presence for gate reporting.
  const knowledgeReady = Boolean(brandBasics) || Boolean(brandApi?.name);
  const visualPackReady =
    Boolean(assetPack?.summary) &&
    !(nz(assetPack?.summary?.status).includes("blocked") && galleryCount < GALLERY_MIN);

  const gates = [
    gateResult({
      name: "brand_record_exists",
      pass: Boolean(brandApi?.id || brandConfig?.recordId),
      expected: "Airtable Brand Basics record",
      actual: brandApi?.id || brandConfig?.recordId || null,
      sourceOfTruth: "Brand Library API / ACTIVE_PROFILE_BRAND_CONFIGS",
      rootCause: "brand_missing",
      remediation: "Register brand in Brand Basics and factory config",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "factory_config_exists",
      pass: Boolean(brandConfig?.slug || brandConfig?.recordId),
      expected: "ACTIVE_PROFILE_BRAND_CONFIGS entry",
      actual: brandConfig?.slug || null,
      sourceOfTruth: "brand-explorer-active-profile-brand-config.js / discovery config",
      rootCause: "missing_factory_config",
      remediation: "Add brand to ACTIVE_PROFILE_BRAND_CONFIGS or discovery config",
      codeChangeRequired: true,
    }),
    gateResult({
      name: "source_library_coverage_ready",
      pass: approvedRegistryCount >= 3 || (blocks.length > 20 && galleryCount >= 3),
      expected: ">=3 approved registry/source-backed assets OR mature presentation set",
      actual: { approvedRegistryCount, presentationBlockCount: blocks.length },
      sourceOfTruth: "Brand Asset Registry / Presentation",
      rootCause: "insufficient_sources",
      remediation: "Seed/approve Source Library + Registry assets",
      airtableWriteRequired: true,
      issueType: approvedRegistryCount < 3 && blocks.length > 40 ? "state_mapping_issue" : "true_issue",
    }),
    gateResult({
      name: "brand_knowledge_pack_ready",
      pass: knowledgeReady,
      expected: "Brand Basics loaded",
      actual: Boolean(brandBasics),
      sourceOfTruth: "Brand Basics",
      rootCause: "knowledge_pack_missing",
      remediation: "Ensure Brand Basics record loads",
    }),
    gateResult({
      name: "visual_asset_pack_ready",
      pass: visualPackReady || (galleryCount >= GALLERY_MIN && openingsCount >= PROPERTY_EXAMPLE_MIN),
      expected: "asset-pack dry-run consumable OR live 6/6 + 3/3 imageUrl",
      actual: {
        assetPackSummary: assetPack?.summary || null,
        galleryCount,
        openingsCount,
      },
      sourceOfTruth: "asset-pack builder + live Presentation imageUrl",
      rootCause: "visual_pack_incomplete",
      remediation: "Materialize gallery/property images into Presentation Image fields",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "gallery_six_imageurl",
      pass: galleryCount >= GALLERY_MIN,
      expected: `${GALLERY_MIN}/6 materials.gallery.* with imageUrl`,
      actual: `${galleryCount}/${GALLERY_MIN}`,
      sourceOfTruth: "Brand Library API brandExplorer.blocks imageUrl",
      rootCause: "gallery_imageurl_missing",
      remediation: "Write Image attachments on materials.gallery.1–6 Presentation rows",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "property_examples_three_imageurl",
      pass: openingsCount >= PROPERTY_EXAMPLE_MIN,
      expected: `${PROPERTY_EXAMPLE_MIN}/3 footprint.openings with imageUrl`,
      actual: `${openingsCount}/${PROPERTY_EXAMPLE_MIN}`,
      sourceOfTruth: "Brand Library API brandExplorer.blocks imageUrl",
      rootCause: "property_example_imageurl_missing",
      remediation: "Write Image attachments on footprint.openings Presentation rows",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "scenario_cards_no_placeholders",
      pass: scenarioCount >= 3 && !scenariosHavePlaceholders(blocks),
      expected: "overview.scenario.1–3 with body + imageUrl, no placeholders",
      actual: { scenarioCount, hasPlaceholders: scenariosHavePlaceholders(blocks) },
      sourceOfTruth: "Presentation overview.scenario.*",
      rootCause: "scenario_placeholders",
      remediation: "Complete scenario presentation rows with hotel images",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "full_tab_content_contract",
      pass: fullTabContract?.pass !== false && (fullTabContract == null || fullTabContract.pass === true),
      expected: "full tab contract pass",
      actual: fullTabContract?.pass ?? "not_evaluated",
      sourceOfTruth: "brand-explorer-full-tab-content-contract / factory rules",
      rootCause: "full_tab_contract_fail",
      remediation: "Populate underpopulated presentation tabs",
      airtableWriteRequired: true,
      issueType: fullTabContract == null ? "state_mapping_issue" : "true_issue",
    }),
    gateResult({
      name: "external_owner_copy_rules",
      pass: externalOwner.pass === true,
      expected: "external_owner_readiness pass",
      actual: { pass: externalOwner.pass, blockers: externalOwner.blockers || [] },
      sourceOfTruth: "evaluateExternalOwnerReadinessRule(presentation)",
      rootCause: "external_owner_copy_fail",
      remediation: "Remove URLs / governance language / empty cards from owner-visible rows",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "external_dom_no_forbidden_strings",
      pass: (qualityLock?.forbiddenStringsFound ?? 0) === 0 || qualityLock?.externalQualityLockPass === true,
      expected: "0 forbidden strings in rendered external DOM",
      actual: {
        forbiddenStringsFound: qualityLock?.forbiddenStringsFound ?? null,
        externalQualityLockPass: qualityLock?.externalQualityLockPass ?? null,
      },
      sourceOfTruth: "v38 external quality lock DOM scan",
      rootCause: "forbidden_strings_in_dom",
      remediation: "Keep quality lock OR remediate copy before unlock",
      codeChangeRequired: false,
    }),
    gateResult({
      name: "external_dom_no_empty_cards",
      pass: (qualityLock?.emptyCardsFound ?? 0) === 0 || qualityLock?.externalQualityLockPass === true,
      expected: "0 empty owner-facing cards when unlocked",
      actual: qualityLock?.emptyCardsFound ?? null,
      sourceOfTruth: "v38 DOM scan",
      rootCause: "empty_cards_visible",
      remediation: "Fill or hide empty cards before unlock",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "external_dom_no_internal_notes",
      pass: qualityLock?.internalNotesFound !== true || qualityLock?.externalQualityLockPass === true,
      expected: "no Output Note / internal review language externally",
      actual: qualityLock?.internalNotesFound ?? null,
      sourceOfTruth: "v38 DOM scan",
      rootCause: "internal_notes_visible",
      remediation: "Suppress internal notes in external renderer",
      codeChangeRequired: true,
    }),
    gateResult({
      name: "founder_visual_review_passed",
      pass: founderPass,
      expected: "Founder Visual Review Pass = true / Pass",
      actual: {
        founderVisualReviewPass: brandApi?.founderVisualReviewPass ?? null,
        basics: readBasicsField(brandBasics, "Founder Visual Review Pass", "Founder Visual Review"),
      },
      sourceOfTruth: "Brand Basics Founder Visual Review fields",
      rootCause: "founder_visual_review_not_passed",
      remediation: "Run founder visual review and record Pass",
      airtableWriteRequired: true,
      issueType: "true_issue",
    }),
    gateResult({
      name: "active_profile_approval_set",
      pass: activeApproved,
      expected: "Ready for Active Profile / Active Profile Approved = true",
      actual: {
        readyForActiveProfile: brandApi?.readyForActiveProfile ?? null,
        activeProfileApproved: brandApi?.activeProfileApproved ?? null,
        basicsReady: readBasicsField(brandBasics, "Ready for Active Profile"),
        basicsApproved: readBasicsField(brandBasics, "Active Profile Approved"),
      },
      sourceOfTruth: "Brand Basics active-profile approval fields",
      rootCause: "active_profile_not_approved",
      remediation: "Gated active-release apply after founder + DOM gates pass",
      airtableWriteRequired: true,
    }),
    gateResult({
      name: "should_render_full_profile",
      pass: display.shouldRenderFullProfile === true || brandApi?.shouldRenderFullProfile === true,
      expected: true,
      actual: brandApi?.shouldRenderFullProfile ?? display.shouldRenderFullProfile,
      sourceOfTruth: "Brand Library API shouldRenderFullProfile",
      rootCause: "display_state_locked",
      remediation: "Satisfy active_profile_ready / external_owner_ready gates",
    }),
    gateResult({
      name: "display_state_release_ready",
      pass: FULL_PROFILE_DISPLAY_STATES.includes(
        brandApi?.brandExplorerDisplayState || display.brandExplorerDisplayState
      ),
      expected: "active_profile_ready | external_owner_ready",
      actual: brandApi?.brandExplorerDisplayState || display.brandExplorerDisplayState,
      sourceOfTruth: "resolveBrandExplorerDisplayState",
      rootCause: "display_state_not_release_ready",
      remediation: "Clear failed gates so display state advances",
    }),
    gateResult({
      name: "render_contract_pass",
      pass: renderContract?.pass === true,
      expected: true,
      actual: renderContract?.pass ?? "not_evaluated",
      sourceOfTruth: "extendAssetPackWithRenderReadiness",
      rootCause: "render_contract_fail",
      remediation: "Ensure Presentation imageUrl matches registry/render readiness",
      airtableWriteRequired: true,
    }),
  ];

  const failed = gates.filter((g) => !g.pass);
  return {
    version: V39_RELEASE_GATE_VERSION,
    brandSlug,
    galleryCount,
    openingsCount,
    scenarioCount,
    displayState: brandApi?.brandExplorerDisplayState || display.brandExplorerDisplayState,
    shouldRenderFullProfile: Boolean(
      brandApi?.shouldRenderFullProfile ?? display.shouldRenderFullProfile
    ),
    gates,
    failedGates: failed.map((g) => g.name),
    failedGateDetails: failed,
    passCount: gates.filter((g) => g.pass).length,
    failCount: failed.length,
    allRequiredPass: failed.length === 0,
  };
}

/**
 * Reconcile report signals vs live API/render contract.
 */
export function reconcileReadinessSignals({
  brandSlug,
  gateInventory,
  completeBuildReport = null,
  finalQaReport = null,
  visualDefectReport = null,
  v36cReport = null,
  v38BrandResult = null,
} = {}) {
  const mismatches = [];
  const reportReady =
    completeBuildReport?.readyForActiveProfile === true ||
    finalQaReport?.readyForActiveProfile === true ||
    finalQaReport?.pass === true;

  const apiLocked = gateInventory.shouldRenderFullProfile !== true;
  const apiReady = gateInventory.shouldRenderFullProfile === true;

  if (reportReady && apiLocked) {
    mismatches.push({
      class: "report_ready_but_api_locked",
      detail: "complete-build/final-QA report says ready but live API shouldRenderFullProfile is false",
      failedGates: gateInventory.failedGates,
    });
  }
  if (apiReady && reportReady === false && completeBuildReport) {
    mismatches.push({
      class: "api_ready_but_report_blocked",
      detail: "API ready but report still blocked — possible stale report",
    });
  }

  const missingVisual = gateInventory.failedGates.includes("gallery_six_imageurl") ||
    gateInventory.failedGates.includes("property_examples_three_imageurl");
  if (missingVisual) {
    mismatches.push({
      class: "visual_materialization_missing",
      detail: `Live imageUrl counts gallery=${gateInventory.galleryCount} openings=${gateInventory.openingsCount}`,
    });
  }

  if (gateInventory.failedGates.includes("active_profile_approval_set")) {
    mismatches.push({
      class: "active_approval_missing",
      detail: "Brand Basics active-profile approval fields not set",
    });
  }

  if (
    reportReady &&
    !gateInventory.failedGates.includes("gallery_six_imageurl") &&
    !gateInventory.failedGates.includes("property_examples_three_imageurl") &&
    gateInventory.failedGates.includes("active_profile_approval_set")
  ) {
    mismatches.push({
      class: "legacy_ready_signal_not_connected",
      detail: "Report readyForActiveProfile not wired to live Active Profile Approved field",
    });
  }

  if (
    gateInventory.failedGates.includes("external_owner_copy_rules") ||
    gateInventory.failedGates.includes("external_dom_no_forbidden_strings")
  ) {
    mismatches.push({
      class: "true_owner_visible_issue",
      detail: "Owner-visible copy or DOM defects remain",
    });
  }

  const registryOnlyButRenderFail =
    gateInventory.failedGates.includes("render_contract_pass") &&
    !gateInventory.failedGates.includes("gallery_six_imageurl");
  if (registryOnlyButRenderFail) {
    mismatches.push({
      class: "registry_ready_but_render_locked",
      detail: "Render contract fails despite gallery imageUrls present",
    });
  }

  if (completeBuildReport?.generatedAt) {
    const ageMs = Date.now() - new Date(completeBuildReport.generatedAt).getTime();
    if (Number.isFinite(ageMs) && ageMs > 1000 * 60 * 60 * 24 * 14) {
      mismatches.push({
        class: "stale_report",
        detail: `complete-build report age >14d (${completeBuildReport.generatedAt})`,
      });
    }
  }

  return {
    brandSlug,
    reportReady: Boolean(reportReady),
    apiReady,
    liveIsSourceOfTruth: true,
    mismatches,
    mismatchClasses: [...new Set(mismatches.map((m) => m.class))],
    v38DisplayState: v38BrandResult?.displayState || null,
    v36cExternalOwner:
      v36cReport?.brandResults?.find?.((b) => b.brandSlug === brandSlug)?.externalOwnerReady ??
      v36cReport?.externalOwnerReady ??
      null,
    visualDefectSummary: visualDefectReport
      ? {
          defectCount: visualDefectReport.defects?.length ?? visualDefectReport.defectCount ?? null,
          status: visualDefectReport.status || visualDefectReport.band || null,
        }
      : null,
  };
}

/**
 * Classify primary release candidate outcome (no unlock).
 */
export function classifyReleaseCandidate({ gateInventory, reconciliation } = {}) {
  const failed = new Set(gateInventory?.failedGates || []);
  const hasVisualFail =
    failed.has("gallery_six_imageurl") || failed.has("property_examples_three_imageurl");
  const hasCopyFail =
    failed.has("external_owner_copy_rules") ||
    failed.has("external_dom_no_forbidden_strings") ||
    failed.has("external_dom_no_empty_cards");
  const onlyApprovalMissing =
    failed.has("active_profile_approval_set") &&
    !hasVisualFail &&
    !hasCopyFail &&
    !failed.has("scenario_cards_no_placeholders") &&
    gateInventory.galleryCount >= GALLERY_MIN &&
    gateInventory.openingsCount >= PROPERTY_EXAMPLE_MIN;

  const mappingOnly =
    (reconciliation?.mismatchClasses || []).includes("legacy_ready_signal_not_connected") &&
    onlyApprovalMissing &&
    !hasCopyFail;

  if (gateInventory?.allRequiredPass) {
    return {
      outcome: RELEASE_OUTCOMES.safe_to_unlock_after_active_approval,
      reason: "All live gates already pass — release apply would only confirm approval fields",
    };
  }

  if (mappingOnly || (onlyApprovalMissing && failed.has("founder_visual_review_passed") && !hasCopyFail && !hasVisualFail)) {
    // founder missing + approval missing but content/visuals OK
    if (!hasVisualFail && !hasCopyFail && gateInventory.galleryCount >= GALLERY_MIN) {
      if (failed.has("founder_visual_review_passed") && !onlyApprovalMissing) {
        return {
          outcome: RELEASE_OUTCOMES.safe_to_unlock_after_active_approval,
          reason: "Content/visuals live-ready; founder review + active approval required before unlock",
        };
      }
      if (onlyApprovalMissing) {
        return {
          outcome: mappingOnly
            ? RELEASE_OUTCOMES.false_blocker_due_to_mapping
            : RELEASE_OUTCOMES.safe_to_unlock_after_active_approval,
          reason: mappingOnly
            ? "Report ready signal not connected to live Active Profile Approved — mapping/approval write only"
            : "Live visuals and copy ready; active-profile approval missing",
        };
      }
    }
  }

  if (!hasVisualFail && !hasCopyFail && gateInventory.galleryCount >= GALLERY_MIN) {
    if (failed.has("founder_visual_review_passed") || failed.has("active_profile_approval_set")) {
      return {
        outcome: RELEASE_OUTCOMES.safe_to_unlock_after_active_approval,
        reason: "Owner-facing visuals/copy appear live-ready; approval/founder gates remain",
      };
    }
  }

  if (hasVisualFail || hasCopyFail || failed.has("scenario_cards_no_placeholders")) {
    return {
      outcome: RELEASE_OUTCOMES.release_remediation_required,
      reason: "True owner-visible or materialization gaps remain before unlock",
    };
  }

  if (failed.size > 0) {
    return {
      outcome: RELEASE_OUTCOMES.not_owner_ready,
      reason: `Remaining failed gates: ${[...failed].join(", ")}`,
    };
  }

  return {
    outcome: RELEASE_OUTCOMES.not_owner_ready,
    reason: "Unable to confirm owner-ready from live gates",
  };
}

/**
 * Design (do not execute) active release apply command shape.
 */
export function buildActiveReleaseApplyCommandDesign(brandSlugs = []) {
  const brands = (brandSlugs || []).join(",");
  return {
    command: [
      "npm run brand-explorer-active-release-apply --",
      `--brands ${brands || "<brand-slugs>"}`,
      "--apply",
      "--approve-brand-explorer-active-release",
      "--confirm-founder-visual-review-passed",
      "--confirm-external-quality-lock-passed",
      "--confirm-six-gallery-imageurls",
      "--confirm-three-property-example-imageurls",
      "--confirm-no-company-validation-claim",
      "--confirm-no-forbidden-owner-copy",
      "--confirm-brand-only",
    ].join(" "),
    writesAllowed: [
      "Ready for Active Profile",
      "Active Profile Approved",
      "Active Profile Approved Date",
      "Founder Visual Review Pass (if separately confirmed)",
    ],
    writesForbidden: [
      "Company Validated",
      "Company Validation Date",
      "Source Library",
      "Brand Asset Registry (unless separately gated)",
      "Presentation copy/images (unless separately gated)",
    ],
    preconditions: [
      "external quality lock pass for unlocked DOM",
      "6/6 gallery imageUrl",
      "3/3 property example imageUrl",
      "no forbidden owner copy",
      "founder visual review passed",
    ],
    status: "designed_not_executed",
  };
}

export { evaluateBrandExternalQualityLock };
