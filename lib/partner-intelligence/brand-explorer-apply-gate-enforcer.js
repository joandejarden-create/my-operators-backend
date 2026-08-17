/**
 * Brand Explorer v36D — Apply Gate Enforcer.
 *
 * Dry-run by default. Never writes unless explicit gated apply flags are present.
 * Never changes Company Validated. Never approves active profile unless apply-approved.
 */
import { FACTORY_GUARD_FLAGS } from "./brand-explorer-active-profile-factory-rules.js";
import { buildDraftApplyCommand, buildActiveApprovalCommand } from "./brand-explorer-active-profile-staged-apply.js";

export const APPLY_GATE_ENFORCER_VERSION = "v36D";

export const V36D_APPLY_MODES = Object.freeze({
  DRY_RUN: "dry-run",
  APPLY_DRAFT: "apply-draft",
  APPLY_REMEDIATION: "apply-remediation",
  APPLY_APPROVED: "apply-approved",
});

export const V36D_REQUIRED_FLAGS = Object.freeze({
  common: [
    "confirmNoCompanyValidationClaim",
    "confirmNoSummaryUrlField",
    "confirmExternalOwnerCopyClean",
    "confirmRenderReadinessContract",
    "confirmBrandOnly",
  ],
  applyDraft: ["confirmNoActiveProfileApproval", "approveBrandExplorerActiveProfileDraft"],
  applyRemediation: ["confirmNoActiveProfileApproval", "approveBrandExplorerActiveProfileRemediation"],
  applyApproved: ["approveBrandExplorerActiveProfile", "confirmFounderVisualReviewPassed"],
});

export const V36D_CLI_FLAGS = Object.freeze({
  confirmNoCompanyValidationClaim: "--confirm-no-company-validation-claim",
  confirmNoSummaryUrlField: "--confirm-no-summary-url-field",
  confirmExternalOwnerCopyClean: "--confirm-external-owner-copy-clean",
  confirmRenderReadinessContract: "--confirm-render-readiness-contract",
  confirmBrandOnly: "--confirm-brand-only",
  confirmNoActiveProfileApproval: "--confirm-no-active-profile-approval",
  approveBrandExplorerActiveProfileDraft: FACTORY_GUARD_FLAGS.approveDraft,
  approveBrandExplorerActiveProfileRemediation: "--approve-brand-explorer-active-profile-remediation",
  approveBrandExplorerActiveProfile: FACTORY_GUARD_FLAGS.approveActiveProfile,
  confirmFounderVisualReviewPassed: FACTORY_GUARD_FLAGS.confirmFounderVisualReviewPassed,
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseV36DApplyMode(argv = []) {
  if (argv.includes("--apply-approved")) return V36D_APPLY_MODES.APPLY_APPROVED;
  if (argv.includes("--apply-remediation")) return V36D_APPLY_MODES.APPLY_REMEDIATION;
  if (argv.includes("--apply-draft")) return V36D_APPLY_MODES.APPLY_DRAFT;
  return V36D_APPLY_MODES.DRY_RUN;
}

export function parseV36DGuardFlags(argv = []) {
  const flags = {};
  for (const [key, cli] of Object.entries(V36D_CLI_FLAGS)) {
    flags[key] = argv.includes(cli);
  }
  return flags;
}

export function buildRemediationApplyCommand(brandSlug) {
  return [
    `npm run brand-explorer-v36d-action-router --`,
    `--brands ${brandSlug}`,
    "--apply-remediation",
    V36D_CLI_FLAGS.approveBrandExplorerActiveProfileRemediation,
    V36D_CLI_FLAGS.confirmNoCompanyValidationClaim,
    V36D_CLI_FLAGS.confirmNoActiveProfileApproval,
    V36D_CLI_FLAGS.confirmNoSummaryUrlField,
    V36D_CLI_FLAGS.confirmExternalOwnerCopyClean,
    V36D_CLI_FLAGS.confirmRenderReadinessContract,
    V36D_CLI_FLAGS.confirmBrandOnly,
  ].join(" ");
}

export function buildAllowedCommandForAction(action, brandSlug) {
  switch (action) {
    case "apply_draft":
      return buildDraftApplyCommand(brandSlug);
    case "remediation_apply":
      return buildRemediationApplyCommand(brandSlug);
    case "apply_approved":
      return buildActiveApprovalCommand(brandSlug);
    case "founder_review":
      return `npm run brand-explorer-active-profile-founder-review -- --brand ${brandSlug} --dry-run`;
    case "investigate_exception":
      return `npm run brand-explorer-v36d-action-router -- --brands ${brandSlug} --dry-run`;
    default:
      return null;
  }
}

export function buildBlockedCommandsForAction(action) {
  const all = ["apply-draft", "apply-remediation", "apply-approved", "founder-review"];
  const map = {
    no_action: all,
    apply_draft: ["apply-remediation", "apply-approved"],
    remediation_apply: ["apply-draft", "apply-approved"],
    founder_review: ["apply-draft", "apply-remediation", "apply-approved"],
    apply_approved: ["apply-draft", "apply-remediation"],
    investigate_exception: all,
  };
  return map[action] || all;
}

/**
 * Validate whether writes are allowed for the requested mode.
 * v36D default = dry-run → writesBlocked=true always unless mode + all flags pass.
 */
export function enforceApplyGate({
  mode = V36D_APPLY_MODES.DRY_RUN,
  guardFlags = {},
  recommendedAction = "no_action",
  companyValidatedTouchAttempted = false,
  activeProfileApprovalAttempted = false,
} = {}) {
  const blockers = [];
  const writesBlocked = mode === V36D_APPLY_MODES.DRY_RUN;

  if (companyValidatedTouchAttempted) {
    blockers.push("company_validated_touch_forbidden");
  }

  if (mode === V36D_APPLY_MODES.DRY_RUN) {
    return {
      version: APPLY_GATE_ENFORCER_VERSION,
      mode,
      allowed: true,
      writesBlocked: true,
      blockers: [],
      note: "dry-run — plans generated, no Airtable writes",
    };
  }

  if (mode === V36D_APPLY_MODES.APPLY_DRAFT) {
    if (recommendedAction !== "apply_draft") blockers.push(`action_mismatch:${recommendedAction}`);
    for (const f of [...V36D_REQUIRED_FLAGS.common, ...V36D_REQUIRED_FLAGS.applyDraft]) {
      if (!guardFlags[f]) blockers.push(`missing_flag:${V36D_CLI_FLAGS[f] || f}`);
    }
    if (activeProfileApprovalAttempted) blockers.push("active_profile_approval_not_allowed_in_apply_draft");
  }

  if (mode === V36D_APPLY_MODES.APPLY_REMEDIATION) {
    if (recommendedAction !== "remediation_apply") blockers.push(`action_mismatch:${recommendedAction}`);
    for (const f of [...V36D_REQUIRED_FLAGS.common, ...V36D_REQUIRED_FLAGS.applyRemediation]) {
      if (!guardFlags[f]) blockers.push(`missing_flag:${V36D_CLI_FLAGS[f] || f}`);
    }
    if (activeProfileApprovalAttempted) blockers.push("active_profile_approval_not_allowed_in_apply_remediation");
  }

  if (mode === V36D_APPLY_MODES.APPLY_APPROVED) {
    if (recommendedAction !== "apply_approved") blockers.push(`action_mismatch:${recommendedAction}`);
    for (const f of [...V36D_REQUIRED_FLAGS.common.filter((x) => x !== "confirmNoActiveProfileApproval"), ...V36D_REQUIRED_FLAGS.applyApproved]) {
      if (!guardFlags[f]) blockers.push(`missing_flag:${V36D_CLI_FLAGS[f] || f}`);
    }
  }

  return {
    version: APPLY_GATE_ENFORCER_VERSION,
    mode,
    allowed: blockers.length === 0,
    writesBlocked: blockers.length > 0 || writesBlocked,
    blockers,
    note:
      blockers.length === 0
        ? `${mode} validated — executor may write (still not invoked by v36D default)`
        : `${mode} blocked — ${blockers.join("; ")}`,
  };
}

export function assertNoCompanyValidatedMutation(fields = {}) {
  const forbidden = ["Company Validated", "Company Validation Date"];
  const hits = Object.keys(fields || {}).filter((k) => forbidden.includes(k));
  return {
    pass: hits.length === 0,
    hits,
  };
}
