/**
 * Brand Explorer v36D — Remediation Executor (plan generation only by default).
 *
 * Reads v36C remediation items + action router output → patch plans.
 * Does not write Airtable unless a future apply gate explicitly allows it.
 */
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  buildPatchPlanItem,
  expandDesignHotelsPatchPlan,
  summarizePatchPlan,
  REMEDIATION_PATCH_BUILDER_VERSION,
} from "./brand-explorer-remediation-patch-builder.js";
import { buildDraftApplyCommand } from "./brand-explorer-active-profile-staged-apply.js";
import { buildRemediationApplyCommand } from "./brand-explorer-apply-gate-enforcer.js";

export const REMEDIATION_EXECUTOR_VERSION = "v36D";

function buildApplyDraftPlanFromV36C(brandResult, route) {
  const brandSlug = brandResult.brandSlug;
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  const draftPatchCount = brandResult.factoryReportSummary?.draftPatchCount ?? 0;
  const items = (brandResult.remediationPlan?.items || []).map((i) =>
    buildPatchPlanItem(i, { brandConfig })
  );

  return {
    brandSlug,
    brandName: brandResult.brandName,
    action: "apply_draft",
    mode: "dry-run",
    draftPlanSummary: {
      presentationPatchCount: draftPatchCount,
      expectedPresentationPatches: 12,
      expectedRegistryCreates: brandSlug === "small-luxury-hotels-of-the-world" ? 9 : null,
      contractValidAssumption: draftPatchCount > 0,
      liveApiBlockedUntilMaterialization: true,
      postDraftFounderVisualReviewRequired: true,
      companyValidatedUntouched: true,
      activeApprovalNotWritten: true,
    },
    confirmations: [
      "12 presentation patches must pass Presentation Plan Row Contract before apply",
      "9 registry creates (SLH) must be official property images only",
      "external owner copy passes affiliation sanitizer — no franchise/parent-flag language",
      "no Company Validated change",
      "no active-profile approval",
      "live API remains gallery-blocked until Image materialization completes",
      "post-draft founder visual review required",
    ],
    patchPlan: {
      version: REMEDIATION_PATCH_BUILDER_VERSION,
      summary: summarizePatchPlan(items),
      items,
    },
    allowedCommand: route.allowedCommand || buildDraftApplyCommand(brandSlug),
    applyBlockedInV36D: true,
    note: "v36D generates plan only — execute with --apply-draft + confirm flags later",
  };
}

function buildRemediationPlanForBrand(brandResult, route, { expandDesignHotels = false } = {}) {
  const brandSlug = brandResult.brandSlug;
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  const rawItems = brandResult.remediationPlan?.items || [];

  const patchItems =
    expandDesignHotels && brandSlug === "design-hotels"
      ? expandDesignHotelsPatchPlan(rawItems, brandConfig)
      : rawItems.map((i) => buildPatchPlanItem(i, { brandConfig }));

  return {
    brandSlug,
    brandName: brandResult.brandName,
    action: "remediation_apply",
    mode: "dry-run",
    draftState: brandResult.draftState?.primaryState,
    calibratedScore: brandResult.enforcement?.numericScore,
    activeApprovalRecommended: false,
    patchPlan: {
      version: REMEDIATION_PATCH_BUILDER_VERSION,
      summary: summarizePatchPlan(patchItems),
      items: patchItems,
    },
    addressChecklist: patchItems.map((p) => ({
      issueId: p.issueId,
      patchType: p.patchType,
      stage: p.stage,
      safeForGenericApply: p.safeForGenericApply,
      requiresFounderApproval: p.requiresFounderApproval,
      requiresCodePatch: p.requiresCodePatch,
    })),
    allowedCommand: route.allowedCommand || buildRemediationApplyCommand(brandSlug),
    applyBlockedInV36D: true,
    note: "Remediation apply plan only — no Airtable writes in v36D dry-run",
  };
}

function buildEverhomeExceptionInvestigation(brandResult, route, regressionCheck) {
  const items = brandResult.remediationPlan?.items || [];
  const ownerVisible = items.filter((i) => i.ownerVisible);
  const codeOnly = items.filter((i) => i.codeRenderPatchRequired);
  const fallbackish = items.filter((i) =>
    /fallback|commercial_static|loyalty_demand|ui_fallback|proof_fallback/.test(i.issueType)
  );
  const trueOwnerGaps = ownerVisible.filter(
    (i) => !/fallback|commercial_static|loyalty_demand/.test(i.issueType)
  );

  let recommendation = "no_action";
  let rationale = "";

  if (trueOwnerGaps.length === 0 && fallbackish.length > 0) {
    recommendation = "contract_adjustment";
    rationale =
      "Most blockers are UI fallback / demand-matrix masks; complete-build already ready — contract may be too strict for franchise brands with populated tabs";
  } else if (trueOwnerGaps.length > 3) {
    recommendation = "remediation_apply";
    rationale = "Owner-visible gaps appear genuine — remediation apply warranted despite prior readyForActiveProfile";
  } else {
    recommendation = "legacy_exception";
    rationale =
      "Mixed signal: complete-build ready; v36C flags mostly cosmetic/fallback — legacy exception review before remediating";
  }

  return {
    brandSlug: brandResult.brandSlug,
    brandName: brandResult.brandName,
    action: "investigate_exception",
    previousReadyForActiveProfile: brandResult.completeBuildReady === true,
    v36cCalibratedScore: brandResult.enforcement?.numericScore,
    enforcementBand: brandResult.enforcement?.band,
    regressionVerdict: regressionCheck?.verdict || null,
    blockers: {
      total: items.length,
      ownerVisible: ownerVisible.length,
      codePatch: codeOnly.length,
      fallbackLike: fallbackish.length,
      trueOwnerGaps: trueOwnerGaps.length,
      samples: ownerVisible.slice(0, 12).map((i) => ({
        issueId: i.issueId,
        issueType: i.issueType,
        severity: i.severity,
        rootCause: i.rootCause,
      })),
    },
    determination: {
      trueOwnerVisibleIssues: trueOwnerGaps.length > 0,
      contractPossiblyTooStrict: fallbackish.length >= Math.max(3, Math.floor(ownerVisible.length * 0.5)),
      legacyExceptionWarranted: recommendation === "legacy_exception" || recommendation === "contract_adjustment",
    },
    recommendation,
    rationale,
    recommendedNext:
      recommendation === "remediation_apply"
        ? "Run remediation_apply after founder confirms gaps are real"
        : recommendation === "contract_adjustment"
          ? "Adjust v36C fallback penalties for franchise brands with populated loyalty/commercial slots"
          : "No automatic patch — founder exception review",
    applyBlockedInV36D: true,
    allowedCommand: route.allowedCommand,
  };
}

function buildTributeBenchmarkNotes(brandResult, route, patchPlan) {
  const items = patchPlan.patchPlan?.items || [];
  const generic = items.filter((p) => p.safeForGenericApply);
  const founder = items.filter((p) => p.requiresFounderApproval);
  const code = items.filter((p) => p.requiresCodePatch);

  return {
    ...patchPlan,
    benchmark: {
      remainsBestLifestyleBenchmark: true,
      reason:
        "Soft-brand collection with strong prior presentation coverage; closest model for Design Hotels / SLH remediation patterns",
      genericFixes: generic.length,
      founderJudgment: founder.length,
      codePatches: code.length,
      executeBeforeDesignOrSlh:
        generic.length > founder.length && brandResult.enforcement?.numericScore != null
          ? false
          : "Prefer Design Hotels remediation packages first; Tribute stays benchmark for copy shape",
      canExecuteBeforeDesignHotels: generic.length > 0 && founder.length === 0,
      canExecuteBeforeSlh: false,
    },
  };
}

function buildWoodSpringNotes(brandResult, route, patchPlan) {
  return {
    ...patchPlan,
    woodspringPriorIssues: [
      "property/gallery image render readiness",
      "visual/valueOwners blockers",
      "generic internal/fallback copy",
      "six-gallery rule",
      "owner-facing copy quality",
    ],
    note: "Do not treat prior Final QA / required-section scores as sufficient — v36C calibrated score gates apply",
  };
}

/**
 * Execute (plan-only) remediation routing for one brand.
 */
export function executeBrandRemediationPlan(brandResult, route, { regressionCheck = null } = {}) {
  const action = route.recommendedAction;

  if (action === "apply_draft") {
    return buildApplyDraftPlanFromV36C(brandResult, route);
  }

  if (action === "investigate_exception") {
    return buildEverhomeExceptionInvestigation(brandResult, route, regressionCheck);
  }

  if (action === "remediation_apply") {
    const expandDh = brandResult.brandSlug === "design-hotels";
    let plan = buildRemediationPlanForBrand(brandResult, route, { expandDesignHotels: expandDh });
    if (brandResult.brandSlug === "tribute-portfolio") {
      plan = buildTributeBenchmarkNotes(brandResult, route, plan);
    }
    if (brandResult.brandSlug === "woodspring-suites") {
      plan = buildWoodSpringNotes(brandResult, route, plan);
    }
    return plan;
  }

  if (action === "founder_review" || action === "apply_approved") {
    return {
      brandSlug: brandResult.brandSlug,
      action,
      mode: "dry-run",
      note: "No remediation patches — proceed to founder visual review / apply-approved gates",
      applyBlockedInV36D: action === "apply_approved",
      allowedCommand: route.allowedCommand,
    };
  }

  return {
    brandSlug: brandResult.brandSlug,
    action: "no_action",
    mode: "dry-run",
    note: "No action recommended",
    applyBlockedInV36D: true,
  };
}

export function executeBatchRemediationPlans(v36cReport, routes) {
  const regressionBySlug = new Map(
    (v36cReport.regressionChecks || []).map((r) => [r.brandSlug, r])
  );
  const routeBySlug = new Map(routes.map((r) => [r.brandSlug, r]));

  return (v36cReport.brandResults || []).map((br) => {
    const route = routeBySlug.get(br.brandSlug);
    return executeBrandRemediationPlan(br, route, {
      regressionCheck: regressionBySlug.get(br.brandSlug) || null,
    });
  });
}
