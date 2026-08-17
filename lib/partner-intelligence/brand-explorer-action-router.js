/**
 * Brand Explorer v36D — Action Router.
 *
 * Maps v36C draft state + apply gate + regression checks → recommended action.
 */
import {
  buildAllowedCommandForAction,
  buildBlockedCommandsForAction,
} from "./brand-explorer-apply-gate-enforcer.js";

export const ACTION_ROUTER_VERSION = "v36D";

export const ACTIONS = Object.freeze([
  "no_action",
  "apply_draft",
  "remediation_apply",
  "founder_review",
  "apply_approved",
  "investigate_exception",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * @param {object} brandResult — v36C brand result
 * @param {object} opts
 * @param {object|null} opts.regressionCheck — from v36C regressionChecks
 */
export function routeBrandAction(brandResult, { regressionCheck = null } = {}) {
  const brandSlug = brandResult.brandSlug;
  const primaryState = brandResult.draftState?.primaryState || "not_started";
  const gate = brandResult.applyGate?.recommendation || "no_apply_allowed";
  const completeReady = brandResult.completeBuildReady === true;
  const enforcementPass = brandResult.enforcement?.pass === true;
  const factoryPass = brandResult.factoryRules?.pass === true || brandResult.factoryReportSummary?.factoryPass === true;
  const score = brandResult.enforcement?.numericScore ?? 0;
  const itemCount = brandResult.remediationPlan?.itemCount ?? 0;
  const ownerVisible = brandResult.remediationPlan?.ownerVisibleCount ?? 0;

  let recommendedAction = "no_action";
  const reasons = [];

  // Exception investigation wins when complete-build ready but v36C still blocks
  if (
    (regressionCheck?.verdict === "investigate" || regressionCheck?.legacyExceptionNeeded) &&
    completeReady &&
    (ownerVisible > 0 || itemCount > 0)
  ) {
    recommendedAction = "investigate_exception";
    reasons.push("complete-build ready but v36C owner-visible blockers remain");
  } else if (primaryState === "draft_not_applied" && gate === "apply-draft_allowed") {
    recommendedAction = "apply_draft";
    reasons.push("draft_not_applied + apply-draft_allowed");
  } else if (
    (primaryState === "draft_applied_with_defects" || primaryState === "draft_applied") &&
    (gate === "remediation_apply_required" || itemCount > 0)
  ) {
    recommendedAction = "remediation_apply";
    reasons.push("draft applied with defects — remediation before re-apply or approval");
  } else if (
    (primaryState === "founder_visual_review_ready" || gate === "founder-review_allowed") &&
    factoryPass &&
    !enforcementPass
  ) {
    recommendedAction = "founder_review";
    reasons.push("factory pass but calibrated external-owner score incomplete");
  } else if (
    gate === "apply-approved_allowed" &&
    enforcementPass &&
    factoryPass &&
    score >= 85
  ) {
    // Still never auto-approve without explicit gates; route only
    recommendedAction = "apply_approved";
    reasons.push("all gates clear — apply-approved may be considered after founder sign-off");
  } else if (primaryState === "active_profile_ready" && completeReady) {
    recommendedAction = "no_action";
    reasons.push("already active-profile ready");
  } else if (gate === "apply-draft_allowed") {
    recommendedAction = "apply_draft";
    reasons.push("apply gate says apply-draft_allowed");
  } else if (gate === "remediation_apply_required") {
    recommendedAction = "remediation_apply";
    reasons.push("apply gate says remediation_apply_required");
  } else {
    recommendedAction = "no_action";
    reasons.push(`no clear action for state=${primaryState} gate=${gate}`);
  }

  // Hard stops for apply_approved
  if (recommendedAction === "apply_approved") {
    if (!enforcementPass || score < 85 || !factoryPass) {
      recommendedAction = "founder_review";
      reasons.push("downgraded apply_approved — unresolved visual/copy gates");
    }
  }

  const blockersRemaining = [
    ...(brandResult.enforcement?.blockers || []).slice(0, 12),
    ...(brandResult.applyGate?.reasons || []).slice(0, 5),
  ];

  const items = brandResult.remediationPlan?.items || [];
  const genericFixable = items.filter((i) => i.genericFactoryCanFix).length;
  const brandSpecific = items.filter((i) => i.brandSpecificConfigNeeded).length;
  const codePatch = items.filter((i) => i.codeRenderPatchRequired).length;
  const founderRequired =
    brandResult.enforcement?.band === "founder_review_required" ||
    items.some((i) => i.founderJudgmentNeeded) ||
    recommendedAction === "investigate_exception" ||
    recommendedAction === "founder_review";

  const safeToApplyNow =
    (recommendedAction === "apply_draft" && primaryState === "draft_not_applied") ||
    (recommendedAction === "remediation_apply" &&
      genericFixable > 0 &&
      brandSpecific === 0 &&
      codePatch === 0);

  return {
    version: ACTION_ROUTER_VERSION,
    brand: brandResult.brandName || brandSlug,
    brandSlug,
    currentState: primaryState,
    applyGateFromV36C: gate,
    recommendedAction,
    reasons,
    allowedCommand: buildAllowedCommandForAction(recommendedAction, brandSlug),
    blockedCommand: buildBlockedCommandsForAction(recommendedAction),
    blockersRemaining,
    founderReviewRequired: founderRequired,
    estimatedGenericFixCoverage:
      items.length === 0 ? 0 : Math.round((genericFixable / items.length) * 100),
    requiresBrandSpecificConfig: brandSpecific > 0,
    requiresCodePatch: codePatch > 0,
    safeToApplyNow: Boolean(safeToApplyNow) && recommendedAction !== "investigate_exception",
    metrics: {
      calibratedScore: score,
      remediationItemCount: itemCount,
      ownerVisibleCount: ownerVisible,
      genericFixable,
      brandSpecific,
      codePatch,
      completeBuildReady: completeReady,
      draftPatchCount: brandResult.factoryReportSummary?.draftPatchCount ?? null,
    },
  };
}

export function routeBatchActions(v36cReport) {
  const regressionBySlug = new Map(
    (v36cReport.regressionChecks || []).map((r) => [r.brandSlug, r])
  );
  return (v36cReport.brandResults || []).map((br) =>
    routeBrandAction(br, { regressionCheck: regressionBySlug.get(br.brandSlug) || null })
  );
}
