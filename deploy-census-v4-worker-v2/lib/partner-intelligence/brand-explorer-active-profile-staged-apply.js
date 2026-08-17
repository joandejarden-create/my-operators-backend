/**
 * Brand Explorer Active Profile Staged Apply v34D.
 *
 * Stage 1: apply-draft — presentation/registry/copy writes without active-profile approval.
 * Stage 2: founder-review — live API visual + copy gate (dry-run).
 * Stage 3: apply-approved — active-profile approval only after founder visual review passes.
 */
import { FACTORY_GUARD_FLAGS, ACTIVE_PROFILE_GALLERY_MINIMUM } from "./brand-explorer-active-profile-factory-rules.js";

export const STAGED_APPLY_VERSION = "v34D";

const DRAFT_APPLY_REQUIRED_FLAGS = Object.freeze([
  "approveBrandExplorerActiveProfileDraft",
  "founderVisualReview",
  "confirmNoCompanyValidationClaim",
  "confirmNoSummaryUrlField",
  "confirmBrandOnly",
  "confirmOfficialSourceImagesOnly",
  "confirmMinimumSixVisibleGalleryImages",
  "confirmPropertyExamplesHaveHotelImages",
  "confirmNoLogoLifestylePropertyImages",
  "confirmStandardDetailGovernanceReviewed",
]);

const ACTIVE_APPROVAL_REQUIRED_FLAGS = Object.freeze([
  "approveBrandExplorerActiveProfile",
  "confirmFounderVisualReviewPassed",
  "confirmNoCompanyValidationClaim",
  "confirmBrandOnly",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function companyValidatedValue(brandBasics = {}) {
  return Boolean(brandBasics?.["Company Validated"] ?? brandBasics?.companyValidated);
}

export function buildDraftApplyCommand(brandSlug) {
  return [
    `npm run brand-explorer-active-profile-apply-draft --`,
    `--brand ${brandSlug}`,
    "--apply",
    FACTORY_GUARD_FLAGS.approveDraft,
    FACTORY_GUARD_FLAGS.approveCopyGovernance,
    FACTORY_GUARD_FLAGS.founderVisualReview,
    FACTORY_GUARD_FLAGS.noCompanyValidation,
    FACTORY_GUARD_FLAGS.noSummaryUrl,
    FACTORY_GUARD_FLAGS.brandOnly,
    FACTORY_GUARD_FLAGS.officialImagesOnly,
    FACTORY_GUARD_FLAGS.minimumSixGallery,
    FACTORY_GUARD_FLAGS.propertyExamplesHaveHotelImages,
    FACTORY_GUARD_FLAGS.noLogoLifestylePropertyImages,
    FACTORY_GUARD_FLAGS.standardDetailGovernance,
  ].join(" ");
}

export function buildActiveApprovalCommand(brandSlug) {
  return [
    `npm run brand-explorer-active-profile-apply-approved --`,
    `--brand ${brandSlug}`,
    "--apply",
    FACTORY_GUARD_FLAGS.approveActiveProfile,
    FACTORY_GUARD_FLAGS.confirmFounderVisualReviewPassed,
    FACTORY_GUARD_FLAGS.noCompanyValidation,
    FACTORY_GUARD_FLAGS.brandOnly,
  ].join(" ");
}

export function validateDraftApplyRequest({
  guardFlags = {},
  copyGovernancePlan = null,
  draftPlan = null,
  apply = false,
} = {}) {
  const blockers = [];

  if (apply && guardFlags.approveBrandExplorerActiveProfile) {
    blockers.push("draft_apply_cannot_use_approve_brand_explorer_active_profile");
  }
  if (apply && !guardFlags.approveBrandExplorerActiveProfileDraft) {
    blockers.push("missing_approve_brand_explorer_active_profile_draft");
  }
  for (const key of DRAFT_APPLY_REQUIRED_FLAGS) {
    if (apply && !guardFlags[key]) {
      blockers.push(`missing_guard:${key}`);
    }
  }
  if (apply && copyGovernancePlan && guardFlags.approveCopyGovernance) {
    if ((copyGovernancePlan.founderReviewQueue || []).length > 0) {
      blockers.push("copy_governance_founder_queue_not_empty");
    }
  }
  if (apply && !guardFlags.approveCopyGovernance && copyGovernancePlan?.repairs?.length) {
    blockers.push("copy_governance_repairs_pending_without_copy_approval_flag");
  }
  if (apply && draftPlan && !draftPlan.presentationPatches?.length && !copyGovernancePlan?.repairs?.length) {
    blockers.push("no_draft_or_copy_patches_to_apply");
  }

  const missingFlagsForApply = [
    ...DRAFT_APPLY_REQUIRED_FLAGS.filter((k) => !guardFlags[k]),
    ...(!guardFlags.approveCopyGovernance && copyGovernancePlan?.repairs?.length
      ? ["approveCopyGovernance"]
      : []),
  ];

  return {
    stage: "apply-draft",
    allowed: blockers.length === 0,
    blockers,
    missingFlagsForApply,
    requiredFlags: DRAFT_APPLY_REQUIRED_FLAGS,
    writesActiveProfileApproval: false,
    setsReadyForActiveProfile: false,
  };
}

export function validateActiveApprovalRequest({
  guardFlags = {},
  founderVisualReview = null,
  apply = false,
} = {}) {
  const blockers = [];

  if (apply && guardFlags.approveBrandExplorerActiveProfileDraft && !guardFlags.approveBrandExplorerActiveProfile) {
    blockers.push("active_approval_requires_approve_active_profile_not_draft_only");
  }
  if (apply && !guardFlags.approveBrandExplorerActiveProfile) {
    blockers.push("missing_approve_brand_explorer_active_profile");
  }
  if (apply && !guardFlags.confirmFounderVisualReviewPassed) {
    blockers.push("missing_confirm_founder_visual_review_passed");
  }
  for (const key of ACTIVE_APPROVAL_REQUIRED_FLAGS) {
    if (apply && !guardFlags[key]) {
      blockers.push(`missing_guard:${key}`);
    }
  }
  if (apply && founderVisualReview && !founderVisualReview.pass) {
    blockers.push(...(founderVisualReview.blockers || []));
  }

  const missingFlagsForApply = ACTIVE_APPROVAL_REQUIRED_FLAGS.filter((k) => !guardFlags[k]);

  return {
    stage: "apply-approved",
    allowed: blockers.length === 0,
    blockers,
    missingFlagsForApply,
    requiredFlags: ACTIVE_APPROVAL_REQUIRED_FLAGS,
    writesDraftContent: false,
  };
}

export function evaluateFounderVisualReview({
  factoryRules = null,
  brandBasics = null,
  companyValidatedBefore = null,
} = {}) {
  const fr = factoryRules || { pass: false, rules: {}, blockers: [] };
  const checks = [
    {
      id: "gallery_six_visible",
      label: `${ACTIVE_PROFILE_GALLERY_MINIMUM} gallery images visible with imageUrl`,
      pass: Boolean(fr.rules?.gallery?.pass),
      detail: `${fr.rules?.gallery?.withImageUrl ?? 0}/${ACTIVE_PROFILE_GALLERY_MINIMUM}`,
    },
    {
      id: "property_examples_hotel_images",
      label: "3 property examples visible with hotel images",
      pass: Boolean(fr.rules?.propertyExamples?.pass),
      detail: `${fr.rules?.propertyExamples?.visibleOpeningCards ?? 0} cards`,
    },
    {
      id: "no_logo_lifestyle_property_images",
      label: "No logo/lifestyle/generic property images",
      pass:
        (fr.rules?.gallery?.logoOrGenericSlots || []).length === 0 &&
        (fr.rules?.propertyExamples?.defects || []).filter((d) =>
          /logo|generic|lifestyle/.test(d.issue)
        ).length === 0,
      detail: "gallery + property scan",
    },
    {
      id: "scenario_no_placeholders",
      label: "No IMAGE placeholders on scenario cards",
      pass: Boolean(fr.rules?.scenarioImages?.pass),
      detail: (fr.rules?.scenarioImages?.placeholderSlots || []).join(", ") || "ok",
    },
    {
      id: "external_owner_readiness",
      label: "No visible source URLs, governance language, or modal placeholders in owner copy",
      pass: Boolean(fr.rules?.externalOwnerReadiness?.pass),
      detail: `${fr.rules?.externalOwnerReadiness?.blockers?.length ?? 0} blockers`,
    },
    {
      id: "copy_safety",
      label: "No FDD / Item 19 / ADR / RevPAR / net contribution language",
      pass: Boolean(fr.rules?.copySafety?.pass),
      detail: `${fr.rules?.copySafety?.highCount ?? 0} high findings`,
    },
    {
      id: "standard_detail_governance",
      label: "Standard detail governance visible and safe",
      pass: Boolean(fr.rules?.standardDetail?.pass),
      detail: fr.rules?.standardDetail?.sectionStatus || "unknown",
    },
    {
      id: "company_validated_untouched",
      label: "Company Validated untouched",
      pass:
        companyValidatedBefore == null
          ? true
          : companyValidatedValue(brandBasics) === companyValidatedBefore,
      detail:
        companyValidatedBefore == null
          ? "not checked"
          : `before=${companyValidatedBefore} after=${companyValidatedValue(brandBasics)}`,
    },
    {
      id: "registry_traceability",
      label: "Registry traceability for visual slots",
      pass: Boolean(fr.rules?.registryTraceability?.pass),
      detail: `${fr.rules?.registryTraceability?.gaps?.length ?? 0} gaps`,
    },
    {
      id: "ui_fallback_risk",
      label: "No stale UI fallback titles",
      pass: Boolean(fr.rules?.uiFallback?.pass),
      detail: `${fr.rules?.uiFallback?.risks?.length ?? 0} risks`,
    },
  ];

  const failedChecks = checks.filter((c) => !c.pass);
  const criticalBlockers = (fr.blockers || []).filter((b) =>
    /risky_copy:|missing_imageUrl:|logo_or_generic:|scenario_image_placeholder:|external_copy:|governance_language:|visible_source_urls:|modal_placeholders:/i.test(
      b
    )
  );

  const pass = failedChecks.length === 0 && criticalBlockers.length === 0;

  return {
    stagedApplyVersion: STAGED_APPLY_VERSION,
    pass,
    checks,
    failedChecks,
    blockers: [
      ...failedChecks.map((c) => `founder_visual_check_failed:${c.id}`),
      ...criticalBlockers,
    ],
    highBlockerCount: (fr.rules?.copySafety?.highCount ?? 0) + criticalBlockers.length,
  };
}

export function buildFounderVisualReviewMarkdown({
  brand,
  founderVisualReview,
  draftApply = null,
  activeApproval = null,
  postDraftApply = null,
} = {}) {
  const lines = [];
  lines.push(`# Founder Visual Review ${STAGED_APPLY_VERSION}`);
  lines.push("");
  lines.push(`- Brand: **${brand?.name || "unknown"}** (\`${brand?.slug || "unknown"}\`)`);
  lines.push(`- Generated: ${new Date().toISOString()}`);
  lines.push(`- Overall: **${founderVisualReview?.pass ? "PASS" : "FAIL"}**`);
  lines.push("");

  if (postDraftApply) {
    lines.push("## Post-draft apply status");
    lines.push(`- Draft applied: **${postDraftApply.applied ? "yes" : "no"}**`);
    lines.push(`- Company Validated unchanged: **${postDraftApply.companyValidatedUnchanged ? "yes" : "no"}**`);
    lines.push(`- Active profile approved: **no** (draft stage only)`);
    lines.push(`- readyForActiveProfile set: **no**`);
    lines.push("");
  }

  lines.push("## Live Brand Explorer checks");
  for (const check of founderVisualReview?.checks || []) {
    lines.push(`- **${check.pass ? "PASS" : "FAIL"}** — ${check.label} (${check.detail})`);
  }
  lines.push("");

  if (founderVisualReview?.failedChecks?.length) {
    lines.push("## Failed checks");
    for (const c of founderVisualReview.failedChecks) {
      lines.push(`- \`${c.id}\`: ${c.label}`);
    }
    lines.push("");
  }

  if (draftApply) {
    lines.push("## Stage 1 — Draft apply");
    lines.push(`- Allowed (apply mode): **${draftApply.allowed ? "yes" : "no"}**`);
    if (draftApply.missingFlagsForApply?.length) {
      lines.push(`- Flags required for apply: ${draftApply.missingFlagsForApply.join(", ")}`);
    }
    if (draftApply.blockers?.length) {
      for (const b of draftApply.blockers) lines.push(`- blocker: ${b}`);
    } else {
      lines.push("```bash");
      lines.push(buildDraftApplyCommand(brand?.slug || "suburban-studios"));
      lines.push("```");
    }
    lines.push("");
  }

  lines.push("## Stage 2 — Founder visual review (this report)");
  lines.push(
    founderVisualReview?.pass
      ? "All checks passed. Founder may proceed to Stage 3 active approval."
      : "Blocked. Fix issues or re-run draft apply, then re-run founder visual review."
  );
  lines.push("");

  lines.push("## Stage 3 — Active approval");
  const activeAllowed = Boolean(activeApproval?.allowed && founderVisualReview?.pass);
  if (activeApproval) {
    lines.push(`- Allowed: **${activeAllowed ? "yes" : "no"}**`);
    if (!founderVisualReview?.pass) {
      lines.push("- blocker: founder_visual_review_not_passed");
    }
    if (activeApproval.blockers?.length) {
      for (const b of activeApproval.blockers) lines.push(`- blocker: ${b}`);
    }
    if (activeApproval.missingFlagsForApply?.length) {
      lines.push(`- Flags required for apply: ${activeApproval.missingFlagsForApply.join(", ")}`);
    }
    if (activeAllowed) {
      lines.push("```bash");
      lines.push(buildActiveApprovalCommand(brand?.slug || "suburban-studios"));
      lines.push("```");
    }
  }
  lines.push("");
  lines.push("## Workflow separation");
  lines.push("1. **apply-draft** — writes gallery, property examples, scenarios, copy governance, visibility; does **not** approve active profile.");
  lines.push("2. **founder-review** — live rendered checks (this report).");
  lines.push("3. **apply-approved** — records active-profile approval only after visual review passes.");

  return lines.join("\n");
}

export function buildPostDraftApplySummary({
  applyResult = null,
  companyValidatedBefore = null,
  companyValidatedAfter = null,
  founderVisualReview = null,
} = {}) {
  const draftApplied = Boolean(applyResult?.draft?.applied);
  const copyApplied = Boolean(applyResult?.copyGovernance?.applied);
  return {
    stagedApplyVersion: STAGED_APPLY_VERSION,
    applied: draftApplied || copyApplied,
    draftApplied,
    copyApplied,
    companyValidatedUnchanged: companyValidatedBefore === companyValidatedAfter,
    activeProfileApproved: false,
    readyForActiveProfileSet: false,
    founderVisualReview,
    nextStep: founderVisualReview?.pass
      ? "run_founder_review_then_active_approval"
      : "run_founder_visual_review_dry_run",
  };
}
