/**
 * Brand Explorer v36C — remediation planner (read-only).
 */
import { tabFromSlot } from "./brand-explorer-contract-enforcement.js";

export const REMEDIATION_PLANNER_VERSION = "v36C";

export const REMEDIATION_STAGES = Object.freeze([
  "source_capture",
  "asset_pack",
  "draft_build",
  "copy_governance",
  "external_owner_cleanup",
  "visual_asset_materialization",
  "registry_traceability",
  "standard_detail_governance",
  "renderer_fix",
  "founder_exception_review",
  "active_approval",
]);

const VIOLATION_STAGE_MAP = Object.freeze({
  visible_source_url: "external_owner_cleanup",
  sources_block_visible: "external_owner_cleanup",
  governance_language: "copy_governance",
  empty_card_body: "copy_governance",
  modal_placeholder: "visual_asset_materialization",
  gallery_render_not_ready: "visual_asset_materialization",
  property_example_render_not_ready: "visual_asset_materialization",
  registry_only_images: "registry_traceability",
  underpopulated_tabs: "draft_build",
  tab_coverage_gap: "draft_build",
  wrong_model_language: "copy_governance",
  scenario_fallback_risk: "renderer_fix",
  proof_fallback_risk: "renderer_fix",
  ui_fallback_risk: "renderer_fix",
  commercial_static_demand: "renderer_fix",
  loyalty_demand_matrix: "renderer_fix",
  insufficient_sources: "source_capture",
});

const VIOLATION_FIX_MAP = Object.freeze({
  visible_source_url: "Strip HTTP URLs from external body; keep trace in internalEvidenceRefs only",
  sources_block_visible: "Remove Sources: block from Body; map to sourceRecordIds in plan row",
  governance_language: "Run affiliation copy sanitizer; remove FDD/LOI/brand-verified phrasing",
  empty_card_body: "Populate Body or hide row via External Display Status",
  modal_placeholder: "Fill Case Summary columns or expand Body to 5+ paragraphs for footprint.openings parser",
  gallery_render_not_ready: "Materialize Presentation Image attachment from asset pack; verify API imageUrl",
  property_example_render_not_ready: "Match footprint.openings row by property name; attach hotel image per catalog entry",
  registry_only_images: "Draft apply must write Presentation Image — registry alone does not render",
  tab_coverage_gap: "Add presentation rows for tab via brand config packages or factory draft",
  wrong_model_language: "Rewrite with affiliation_curation_platform framing; block franchise flag language",
  scenario_fallback_risk: "Ensure overview.scenario.N has imageUrl in API before founder review",
  proof_fallback_risk: "Populate overview.proof.1–6 with brand-specific proof cards",
  ui_fallback_risk: "Populate slots that trigger atelier hardcoded fallbacks",
  commercial_static_demand: "Populate commercial.demand slots to suppress COMM_STATIC",
  loyalty_demand_matrix: "Populate loyalty KPI/proof rows to suppress LOY_DEMAND matrix",
  insufficient_sources: "Capture and approve Source Library rows (v35C tagging)",
});

function remediationItemFromViolation(v, brandSlug, brandName) {
  const stage = VIOLATION_STAGE_MAP[v.id] || "founder_exception_review";
  const proposedFix = VIOLATION_FIX_MAP[v.id] || "Founder review required";
  const genericFactory =
    [
      "gallery_render_not_ready",
      "property_example_render_not_ready",
      "registry_only_images",
      "scenario_fallback_risk",
      "ui_fallback_risk",
    ].includes(v.id) && brandSlug !== "design-hotels";
  const brandSpecific =
    v.id === "tab_coverage_gap" ||
    v.id === "wrong_model_language" ||
    brandSlug === "design-hotels";
  const founderJudgment =
    v.id === "modal_placeholder" ||
    stage === "standard_detail_governance" ||
    stage === "founder_exception_review";
  const airtableWriteLater = !["renderer_fix"].includes(stage);
  const codeRenderPatch = stage === "renderer_fix";

  return {
    issueId: `${brandSlug}:${v.id}:${v.slotKey || v.tab || v.recordId || "global"}`,
    severity: v.severity || "medium",
    brand: brandName || brandSlug,
    brandSlug,
    tab: v.tab || tabFromSlot(v.slotKey || ""),
    slot: v.slotKey || null,
    recordId: v.recordId || null,
    issueType: v.id,
    ownerVisible: v.ownerVisible !== false,
    rootCause: v.detail || v.id,
    proposedFix,
    requiredSystemStage: stage,
    genericFactoryCanFix: genericFactory,
    brandSpecificConfigNeeded: brandSpecific,
    founderJudgmentNeeded: founderJudgment,
    airtableWriteRequiredLater: airtableWriteLater,
    codeRenderPatchRequired: codeRenderPatch,
  };
}

function factoryBlockerItems(factoryRules, brandSlug, brandName) {
  if (factoryRules?.pass) return [];
  const items = [];
  for (const blocker of factoryRules?.blockers || []) {
    items.push({
      issueId: `${brandSlug}:factory:${blocker}`,
      severity: "high",
      brand: brandName || brandSlug,
      brandSlug,
      tab: "Cross-tab",
      slot: null,
      recordId: null,
      issueType: "factory_rule_blocker",
      ownerVisible: /gallery|scenario|property|fallback/i.test(blocker),
      rootCause: blocker,
      proposedFix: "Resolve via mapped factory stage before apply or approval",
      requiredSystemStage: /gallery|image|materialization/i.test(blocker)
        ? "visual_asset_materialization"
        : /governance|standard/i.test(blocker)
          ? "standard_detail_governance"
          : /copy|governance/i.test(blocker)
            ? "copy_governance"
            : "draft_build",
      genericFactoryCanFix: !brandSlug.includes("design-hotels"),
      brandSpecificConfigNeeded: brandSlug === "design-hotels",
      founderJudgmentNeeded: /governance|founder/i.test(blocker),
      airtableWriteRequiredLater: true,
      codeRenderPatchRequired: /fallback/i.test(blocker),
    });
  }
  return items;
}

export function buildRemediationPlan(ctx = {}) {
  const {
    brandSlug,
    brandName,
    enforcement,
    draftState,
    factoryRules,
    renderContract,
    presentationPlan,
  } = ctx;

  const items = [];

  for (const v of enforcement?.violations || []) {
    items.push(remediationItemFromViolation(v, brandSlug, brandName));
  }

  items.push(...factoryBlockerItems(factoryRules, brandSlug, brandName));

  if (draftState?.primaryState === "draft_applied_with_defects") {
    items.push({
      issueId: `${brandSlug}:draft_applied_with_defects`,
      severity: "high",
      brand: brandName || brandSlug,
      brandSlug,
      tab: "Cross-tab",
      slot: null,
      recordId: null,
      issueType: "draft_state",
      ownerVisible: true,
      rootCause: "Prior draft apply materialized rows but founder-visible defects remain",
      proposedFix: "Run remediation writers before re-apply or founder review — do not apply-draft again blindly",
      requiredSystemStage: "visual_asset_materialization",
      genericFactoryCanFix: false,
      brandSpecificConfigNeeded: true,
      founderJudgmentNeeded: true,
      airtableWriteRequiredLater: true,
      codeRenderPatchRequired: false,
    });
  }

  if (presentationPlan && !presentationPlan.pass && presentationPlan.summary?.total > 0) {
    items.push({
      issueId: `${brandSlug}:presentation_plan_invalid`,
      severity: "medium",
      brand: brandName || brandSlug,
      brandSlug,
      tab: "Cross-tab",
      slot: null,
      recordId: null,
      issueType: "presentation_plan_contract",
      ownerVisible: false,
      rootCause: `${presentationPlan.summary.total - presentationPlan.summary.externalOwnerReady} plan rows fail contract`,
      proposedFix: "Fix plan row externalBody, renderReadiness, and model-fit before apply-draft",
      requiredSystemStage: "draft_build",
      genericFactoryCanFix: true,
      brandSpecificConfigNeeded: false,
      founderJudgmentNeeded: false,
      airtableWriteRequiredLater: true,
      codeRenderPatchRequired: false,
    });
  }

  const byStage = {};
  for (const stage of REMEDIATION_STAGES) byStage[stage] = [];
  for (const item of items) {
    byStage[item.requiredSystemStage]?.push(item.issueId);
  }

  const severityOrder = { critical: 0, high: 1, medium: 2, low: 3 };
  items.sort((a, b) => (severityOrder[a.severity] ?? 9) - (severityOrder[b.severity] ?? 9));

  return {
    version: REMEDIATION_PLANNER_VERSION,
    brandSlug,
    brandName: brandName || brandSlug,
    itemCount: items.length,
    ownerVisibleCount: items.filter((i) => i.ownerVisible).length,
    items,
    byStage,
    summary: {
      critical: items.filter((i) => i.severity === "critical").length,
      high: items.filter((i) => i.severity === "high").length,
      medium: items.filter((i) => i.severity === "medium").length,
      stagesTouched: REMEDIATION_STAGES.filter((s) => byStage[s]?.length),
    },
    renderContractPass: renderContract?.pass ?? false,
    activeProfileReady: draftState?.signals?.readyForActiveProfile ?? false,
  };
}

export function recommendApplyGate(ctx = {}) {
  const { draftState, enforcement, factoryRules, renderContract, remediationPlan } = ctx;

  const readyForActive = draftState?.signals?.readyForActiveProfile;
  const factoryPass = factoryRules?.pass === true;
  const renderPass = renderContract?.pass === true;
  const enforcementPass = enforcement?.pass === true;
  const hasCritical = (remediationPlan?.items || []).some((i) => i.severity === "critical");
  const draftApplied = ["draft_applied", "draft_applied_with_defects"].includes(draftState?.primaryState);
  const draftNotApplied = draftState?.primaryState === "draft_not_applied";

  let recommendation = "no_apply_allowed";
  const reasons = [];

  if (readyForActive && enforcementPass && factoryPass) {
    recommendation = "apply-approved_allowed";
    reasons.push("active_profile_ready signals present — still requires explicit founder approval gate");
  } else if (draftApplied && draftState?.primaryState === "founder_visual_review_ready" && renderPass) {
    recommendation = "founder-review_allowed";
    reasons.push("Draft applied; factory rules pass; proceed to founder visual review");
  } else if (draftApplied && remediationPlan?.itemCount > 0) {
    recommendation = "remediation_apply_required";
    reasons.push("Draft applied with defects — remediation before re-apply or founder review");
  } else if (draftNotApplied && !hasCritical && draftState?.readyForApplyDraft) {
    recommendation = "apply-draft_allowed";
    reasons.push("Draft plan ready; live API not materialized; first apply-draft permitted");
  } else if (draftApplied && !renderPass) {
    recommendation = "remediation_apply_required";
    reasons.push("Do not re-apply draft until visual materialization remediation completes");
  } else {
    recommendation = "no_apply_allowed";
    reasons.push("Sources, copy, or contract blockers must clear first");
  }

  return {
    recommendation,
    reasons,
    applyDraftAllowed: recommendation === "apply-draft_allowed",
    remediationApplyRequired: recommendation === "remediation_apply_required",
    founderReviewAllowed: recommendation === "founder-review_allowed",
    applyApprovedAllowed: recommendation === "apply-approved_allowed" && false,
    note: "apply-approved_allowed is informational only — v36C never executes apply",
  };
}

export function buildDesignHotelsRemediationPlan(remediationPlan, enforcement, draftState) {
  const lines = [];
  lines.push("# Design Hotels Remediation Plan (v36C)");
  lines.push("");
  lines.push(`- Draft state: **${draftState?.primaryState}**`);
  lines.push(`- Calibrated score: **${enforcement?.numericScore}/100** (${enforcement?.band})`);
  lines.push(`- Active approval: **NOT RECOMMENDED**`);
  lines.push("");
  lines.push("## Known issue areas");
  const focus = (remediationPlan?.items || []).filter((i) =>
    [
      "property_example_render_not_ready",
      "modal_placeholder",
      "tab_coverage_gap",
      "wrong_model_language",
      "governance_language",
      "visible_source_url",
      "scenario_fallback_risk",
      "proof_fallback_risk",
      "gallery_render_not_ready",
    ].includes(i.issueType)
  );
  for (const item of focus.slice(0, 20)) {
    lines.push(`### ${item.issueId}`);
    lines.push(`- Severity: ${item.severity}`);
    lines.push(`- Tab: ${item.tab} | Slot: ${item.slot || "—"} | Row: ${item.recordId || "—"}`);
    lines.push(`- Root cause: ${item.rootCause}`);
    lines.push(`- Fix: ${item.proposedFix}`);
    lines.push(`- Stage: ${item.requiredSystemStage}`);
    lines.push("");
  }
  lines.push("## Before active approval");
  lines.push("- Clear modal placeholders on footprint.openings");
  lines.push("- Property example row-level image match (3/3 render-ready)");
  lines.push("- Standards table owner-ready with affiliation-safe governance");
  lines.push("- Loyalty KPI/proof/watchouts coverage");
  lines.push("- Economics/fee affiliation fit (no FDD templates)");
  lines.push("- Pass founder visual review after remediation apply");
  return lines.join("\n");
}

export function buildSlhNextAction(ctx = {}) {
  const { draftState, enforcement, remediationPlan, factoryRules, draftPlan } = ctx;
  const patchCount = draftPlan?.presentationPatches?.length || 0;

  let nextStep = "remediation_before_draft";
  const reasons = [];

  if (draftState?.primaryState === "draft_not_applied" && patchCount >= 10) {
    if ((enforcement?.categories || []).includes("blocked_by_copy")) {
      nextStep = "copy_governance_issue";
      reasons.push("Copy governance blockers must clear before apply-draft");
    } else if ((enforcement?.categories || []).includes("blocked_by_sources")) {
      nextStep = "source_asset_issue";
      reasons.push("Insufficient approved sources");
    } else {
      nextStep = "apply-draft";
      reasons.push(`${patchCount} presentation patches ready; live API 0/6 gallery until materialization`);
    }
  } else if (draftState?.signals?.draftLikelyApplied) {
    nextStep = "remediation_before_draft";
    reasons.push("Draft already applied — do not apply-draft again");
  } else if ((remediationPlan?.itemCount || 0) > 5) {
    nextStep = "remediation_before_draft";
    reasons.push("Multiple contract violations block draft apply");
  }

  return {
    nextStep,
    reasons,
    applyDraftPermitted: nextStep === "apply-draft",
    activeApprovalBlocked: true,
    postDraftRequirement: "founder visual review after apply-draft",
    factoryPass: factoryRules?.pass,
  };
}

export function buildTributeBenchmark(ctx = {}) {
  const { enforcement, draftState, renderContract, remediationPlan, factoryRules } = ctx;
  const close =
    renderContract?.pass &&
    (enforcement?.numericScore || 0) >= 70 &&
    (remediationPlan?.itemCount || 0) < 8;

  return {
    isClose: close,
    canServeAsLifestyleBenchmark: close && draftState?.primaryState !== "draft_not_applied",
    passes: [
      renderContract?.pass ? "render_ready" : null,
      (enforcement?.numericScore || 0) >= 70 ? "calibrated_score_ok" : null,
      factoryRules?.pass ? "factory_rules_pass" : null,
    ].filter(Boolean),
    blocksActiveApproval: [
      !factoryRules?.pass ? "factory_rules" : null,
      enforcement?.band === "founder_review_required" ? "founder_review_required" : null,
      !draftState?.signals?.readyForActiveProfile ? "not_ready_for_active_profile" : null,
    ].filter(Boolean),
    genericRemediationViable: (remediationPlan?.items || []).filter((i) => i.genericFactoryCanFix).length >
      (remediationPlan?.items || []).filter((i) => i.brandSpecificConfigNeeded).length,
    fasterThanDesignSlh: close,
  };
}

export function buildRegressionCheck(brandSlug, ctx = {}) {
  const { enforcement, draftState, completeBuildReady, remediationPlan } = ctx;
  const ownerVisibleItems = (remediationPlan?.items || []).filter((i) => i.ownerVisible);
  const cosmeticOnly =
    ownerVisibleItems.length === 0 &&
    (remediationPlan?.items || []).every((i) =>
      ["ui_fallback_risk", "proof_fallback_risk", "commercial_static_demand", "loyalty_demand_matrix"].includes(
        i.issueType
      )
    );

  let verdict = "pass";
  let explanation = "Stricter contracts align with prior complete-build readiness";

  if (completeBuildReady && ownerVisibleItems.length > 0) {
    verdict = "investigate";
    explanation = "Complete-build marked ready but v36C reports owner-visible blockers — verify false positive";
  } else if (completeBuildReady && cosmeticOnly) {
    verdict = "legacy_exception";
    explanation = "Complete-build ready; v36C flags fallback/cosmetic risks only — legacy exception may apply";
  } else if (completeBuildReady && !ownerVisibleItems.length) {
    verdict = "pass";
    explanation = "Regression check pass — no owner-visible false blockers";
  } else if (!completeBuildReady && ownerVisibleItems.length > 0) {
    verdict = "true_issue";
    explanation = "Brand has genuine open owner-visible defects";
  } else {
    verdict = "pass";
    explanation = "Both complete-build and v36C agree not ready or only non-visible gaps";
  }

  return {
    brandSlug,
    completeBuildReady,
    v36cBlocked: ownerVisibleItems.length > 0,
    ownerVisibleRemediationCount: ownerVisibleItems.length,
    enforcementBand: enforcement?.band,
    draftState: draftState?.primaryState,
    verdict,
    explanation,
    legacyExceptionNeeded: verdict === "legacy_exception" || verdict === "investigate",
    v24dCosmeticOnly: brandSlug === "woodspring-suites" && cosmeticOnly,
  };
}

export function remediationPlanMarkdown(plan, title) {
  const lines = [`# ${title}`, "", `Items: ${plan.itemCount} (${plan.ownerVisibleCount} owner-visible)`, ""];
  for (const item of plan.items.slice(0, 25)) {
    lines.push(`- **[${item.severity}]** ${item.issueType} — ${item.tab}/${item.slot || "—"} — ${item.proposedFix}`);
  }
  return lines.join("\n");
}
