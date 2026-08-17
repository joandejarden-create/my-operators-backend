/**
 * v39 — Brand Explorer Active Release Plan generator (read-only).
 */
import {
  NEXT_ACTIONS,
  RELEASE_OUTCOMES,
  buildActiveReleaseApplyCommandDesign,
} from "./brand-explorer-active-release-gate.js";

export const V39_RELEASE_PLAN_VERSION = "v39";

const PRIMARY_RELEASE_SLUGS = new Set([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

const INCOMPLETE_CONTROL_SLUGS = new Set([
  "hotel-indigo",
  "mgallery-collection",
  "design-hotels",
  "small-luxury-hotels-of-the-world",
]);

function mapOutcomeToNextAction(outcome, gateInventory) {
  if (INCOMPLETE_CONTROL_SLUGS.has(gateInventory?.brandSlug)) {
    return NEXT_ACTIONS.no_action;
  }
  switch (outcome) {
    case RELEASE_OUTCOMES.safe_to_unlock_after_active_approval:
      if (gateInventory?.failedGates?.includes("founder_visual_review_passed")) {
        return NEXT_ACTIONS.founder_visual_review_required;
      }
      return NEXT_ACTIONS.safe_to_apply_active_release;
    case RELEASE_OUTCOMES.false_blocker_due_to_mapping:
      return NEXT_ACTIONS.mapping_fix_required;
    case RELEASE_OUTCOMES.release_remediation_required:
      return NEXT_ACTIONS.release_remediation_required;
    case RELEASE_OUTCOMES.not_owner_ready:
    default:
      return NEXT_ACTIONS.not_owner_ready;
  }
}

/**
 * Build per-brand release plan (no writes).
 */
export function buildActiveReleasePlan({
  brandSlug,
  gateInventory,
  reconciliation,
  classification,
  qualityLock = null,
  cohort = "other",
} = {}) {
  const failed = gateInventory?.failedGateDetails || [];
  const outcome = classification?.outcome || RELEASE_OUTCOMES.not_owner_ready;
  const allowedNextAction = mapOutcomeToNextAction(outcome, { ...gateInventory, brandSlug });

  const remediationItems = failed.map((g) => ({
    gate: g.name,
    remediation: g.remediation,
    airtableWriteRequired: g.airtableWriteRequired,
    codeChangeRequired: g.codeChangeRequired,
    issueType: g.issueType,
  }));

  const requiredWrites = [
    ...new Set(
      failed
        .filter((g) => g.airtableWriteRequired)
        .flatMap((g) => {
          if (g.name === "active_profile_approval_set") {
            return ["Ready for Active Profile", "Active Profile Approved"];
          }
          if (g.name === "founder_visual_review_passed") {
            return ["Founder Visual Review Pass"];
          }
          if (g.name === "gallery_six_imageurl" || g.name === "property_examples_three_imageurl") {
            return ["Brand Explorer Presentation.Image"];
          }
          if (g.name === "external_owner_copy_rules" || g.name === "scenario_cards_no_placeholders") {
            return ["Brand Explorer Presentation Title/Body/Image"];
          }
          return [];
        })
    ),
  ];

  const founderConfirmationRequired =
    failed.some((g) => g.name === "founder_visual_review_passed") ||
    allowedNextAction === NEXT_ACTIONS.founder_visual_review_required ||
    allowedNextAction === NEXT_ACTIONS.safe_to_apply_active_release;

  const genericReleaseApplyCanHandle =
    outcome === RELEASE_OUTCOMES.safe_to_unlock_after_active_approval ||
    outcome === RELEASE_OUTCOMES.false_blocker_due_to_mapping;

  const brandSpecificPatchNeeded =
    outcome === RELEASE_OUTCOMES.release_remediation_required ||
    failed.some((g) => g.codeChangeRequired) ||
    failed.some((g) =>
      ["gallery_six_imageurl", "property_examples_three_imageurl", "external_owner_copy_rules"].includes(
        g.name
      )
    );

  const applyDesign = buildActiveReleaseApplyCommandDesign([brandSlug]);

  const blockedCommands = [
    "brand-explorer-active-release-apply (until gates pass)",
    "any Company Validated write",
    "Source Library writes from this pipeline",
  ];
  if (cohort === "incomplete" || INCOMPLETE_CONTROL_SLUGS.has(brandSlug)) {
    blockedCommands.push("any active-profile unlock for incomplete cohort");
  }
  if (!genericReleaseApplyCanHandle) {
    blockedCommands.push(applyDesign.command);
  }

  let exactAllowedNextCommand = "no_action — remain locked";
  if (allowedNextAction === NEXT_ACTIONS.safe_to_apply_active_release) {
    exactAllowedNextCommand = `${applyDesign.command}  # DESIGN ONLY — do not run until founder confirms`;
  } else if (allowedNextAction === NEXT_ACTIONS.founder_visual_review_required) {
    exactAllowedNextCommand = `npm run brand-explorer-active-profile-founder-review -- --brand ${brandSlug} --dry-run`;
  } else if (allowedNextAction === NEXT_ACTIONS.release_remediation_required) {
    exactAllowedNextCommand = `npm run brand-explorer-v36c-remediation-planner -- --brands ${brandSlug} --dry-run`;
  } else if (allowedNextAction === NEXT_ACTIONS.mapping_fix_required) {
    exactAllowedNextCommand =
      "Code/mapping fix: connect complete-build readyForActiveProfile to live Active Profile Approved fields — then re-audit";
  }

  return {
    version: V39_RELEASE_PLAN_VERSION,
    brandSlug,
    cohort,
    currentDisplayState: gateInventory?.displayState || null,
    shouldRenderFullProfile: gateInventory?.shouldRenderFullProfile === true,
    releaseOutcome: outcome,
    releaseOutcomeReason: classification?.reason || null,
    failedGates: gateInventory?.failedGates || [],
    remediationItems,
    requiredWrites,
    founderConfirmationRequired,
    genericReleaseApplyCanHandle,
    brandSpecificPatchNeeded,
    allowedNextAction,
    exactAllowedNextCommand,
    blockedCommands,
    activeReleaseApplyDesign: applyDesign,
    mismatchClasses: reconciliation?.mismatchClasses || [],
    qualityLockPass: qualityLock?.externalQualityLockPass ?? null,
    readyForActiveApproval: false,
    companyValidatedUntouched: true,
  };
}

export function buildIncompleteBrandControlCheck(brandResults = []) {
  const incomplete = brandResults.filter(
    (b) => INCOMPLETE_CONTROL_SLUGS.has(b.brandSlug) || b.cohort === "incomplete"
  );
  const rows = incomplete.map((b) => {
    const locked =
      b.gateInventory?.shouldRenderFullProfile !== true &&
      !["external_owner_ready", "active_profile_ready"].includes(b.gateInventory?.displayState);
    const prepOk =
      b.qualityLock?.profileInPreparationRendered === true ||
      b.qualityLock?.externalQualityLockPass === true;
    const noForbidden = (b.qualityLock?.forbiddenStringsFound ?? 0) === 0;
    const noAccidentalUnlock =
      b.classification?.outcome !== RELEASE_OUTCOMES.safe_to_unlock_after_active_approval ||
      b.releasePlan?.allowedNextAction === NEXT_ACTIONS.no_action ||
      INCOMPLETE_CONTROL_SLUGS.has(b.brandSlug);

    // Incomplete brands must never get safe_to_apply
    const pathToApprovalBlocked =
      b.releasePlan?.allowedNextAction === NEXT_ACTIONS.no_action ||
      b.releasePlan?.allowedNextAction === NEXT_ACTIONS.not_owner_ready ||
      b.releasePlan?.allowedNextAction === NEXT_ACTIONS.release_remediation_required;

    return {
      brandSlug: b.brandSlug,
      shouldRenderFullProfile: false,
      shouldRenderFullProfileActual: b.gateInventory?.shouldRenderFullProfile === true,
      fullTabsSuppressed: locked,
      profileInPreparationRenders: prepOk,
      noForbiddenStrings: noForbidden,
      noPathToActiveApproval: pathToApprovalBlocked && !b.gateInventory?.shouldRenderFullProfile,
      noAccidentalUnlock: !b.gateInventory?.shouldRenderFullProfile,
      controlPass:
        !b.gateInventory?.shouldRenderFullProfile &&
        locked &&
        noForbidden &&
        pathToApprovalBlocked,
    };
  });

  return {
    version: V39_RELEASE_PLAN_VERSION,
    brandsChecked: rows.map((r) => r.brandSlug),
    allControlPass: rows.every((r) => r.controlPass),
    rows,
  };
}

export function renderReleasePlanMarkdown(plan) {
  const lines = [
    `# v39 Release Plan — ${plan.brandSlug}`,
    "",
    `- Cohort: ${plan.cohort}`,
    `- Current display state: \`${plan.currentDisplayState}\``,
    `- shouldRenderFullProfile: **${plan.shouldRenderFullProfile}**`,
    `- Release outcome: **${plan.releaseOutcome}**`,
    `- Reason: ${plan.releaseOutcomeReason || "—"}`,
    `- Allowed next action: **${plan.allowedNextAction}**`,
    `- Founder confirmation required: ${plan.founderConfirmationRequired ? "yes" : "no"}`,
    `- Generic release apply can handle: ${plan.genericReleaseApplyCanHandle ? "yes" : "no"}`,
    `- Brand-specific patch needed: ${plan.brandSpecificPatchNeeded ? "yes" : "no"}`,
    `- Ready for active approval: **no** (audit only)`,
    "",
    "## Failed gates",
  ];
  if (!plan.failedGates?.length) {
    lines.push("- none");
  } else {
    for (const g of plan.failedGates) lines.push(`- \`${g}\``);
  }
  lines.push("", "## Remediation items");
  if (!plan.remediationItems?.length) {
    lines.push("- none");
  } else {
    for (const r of plan.remediationItems) {
      lines.push(
        `- **${r.gate}**: ${r.remediation} (Airtable write: ${r.airtableWriteRequired ? "yes" : "no"}; code: ${r.codeChangeRequired ? "yes" : "no"}; type: ${r.issueType})`
      );
    }
  }
  lines.push("", "## Required writes (if later gated apply)");
  if (!plan.requiredWrites?.length) lines.push("- none for unlock-only path");
  else for (const w of plan.requiredWrites) lines.push(`- ${w}`);

  lines.push("", "## Exact allowed next command");
  lines.push("```");
  lines.push(plan.exactAllowedNextCommand);
  lines.push("```");
  lines.push("", "## Blocked commands");
  for (const c of plan.blockedCommands || []) lines.push(`- ${c}`);
  lines.push("", "## Designed release apply (NOT executed)");
  lines.push("```");
  lines.push(plan.activeReleaseApplyDesign?.command || "");
  lines.push("```");
  lines.push("", "Guardrails: no Company Validated changes; no Source Library writes; no blind unlock.");
  return lines.join("\n");
}

export function renderIncompleteControlMarkdown(control) {
  const lines = [
    "# v39 Incomplete Brand Control Check",
    "",
    `All control pass: **${control.allControlPass ? "yes" : "no"}**`,
    "",
  ];
  for (const r of control.rows || []) {
    lines.push(`## ${r.brandSlug}`);
    lines.push(`- shouldRenderFullProfile expected false / actual: **${r.shouldRenderFullProfileActual}**`);
    lines.push(`- full tabs suppressed: ${r.fullTabsSuppressed}`);
    lines.push(`- Profile in Preparation: ${r.profileInPreparationRenders}`);
    lines.push(`- no forbidden strings: ${r.noForbiddenStrings}`);
    lines.push(`- no path to active approval: ${r.noPathToActiveApproval}`);
    lines.push(`- no accidental unlock: ${r.noAccidentalUnlock}`);
    lines.push(`- control pass: **${r.controlPass ? "PASS" : "FAIL"}**`);
    lines.push("");
  }
  return lines.join("\n");
}

export { PRIMARY_RELEASE_SLUGS, INCOMPLETE_CONTROL_SLUGS };
