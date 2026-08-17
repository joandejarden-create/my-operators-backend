/**
 * Brand Explorer v36D — Remediation Patch Builder.
 *
 * Converts v36C remediation items into schema-safe patch plan rows (read-only).
 */
export const REMEDIATION_PATCH_BUILDER_VERSION = "v36D";

export const PATCH_TYPES = Object.freeze([
  "patch_presentation_row",
  "create_presentation_row",
  "hide_duplicate_row",
  "materialize_image",
  "promote_registry_asset",
  "rewrite_external_copy",
  "populate_modal_fields",
  "suppress_empty_field",
  "renderer_patch_required",
  "no_write_investigation",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REGISTRY_TABLE = "Brand Asset Registry";

const ISSUE_TO_PATCH_TYPE = Object.freeze({
  property_example_render_not_ready: "materialize_image",
  gallery_render_not_ready: "materialize_image",
  registry_only_images: "promote_registry_asset",
  modal_placeholder: "populate_modal_fields",
  wrong_model_language: "rewrite_external_copy",
  governance_language: "rewrite_external_copy",
  visible_source_url: "rewrite_external_copy",
  sources_block_visible: "rewrite_external_copy",
  empty_card_body: "suppress_empty_field",
  tab_coverage_gap: "create_presentation_row",
  underpopulated_tabs: "create_presentation_row",
  scenario_fallback_risk: "materialize_image",
  proof_fallback_risk: "create_presentation_row",
  ui_fallback_risk: "create_presentation_row",
  commercial_static_demand: "create_presentation_row",
  loyalty_demand_matrix: "create_presentation_row",
  factory_rule_blocker: "no_write_investigation",
  draft_state: "no_write_investigation",
  presentation_plan_contract: "patch_presentation_row",
  insufficient_sources: "no_write_investigation",
});

/** Design Hotels known modal/property targets from founder visual review. */
export const DESIGN_HOTELS_MODAL_TARGETS = Object.freeze([
  {
    propertyName: "Wake BioHotel",
    recordId: null,
    slotKey: "footprint.openings",
    note: "Modal placeholders — Case Summary columns empty",
  },
  {
    propertyName: "Condesa DF",
    recordId: null,
    slotKey: "footprint.openings",
    note: "Modal placeholders — Case Summary columns empty",
  },
  {
    propertyName: "Carlota",
    recordId: null,
    slotKey: "footprint.openings",
    note: "Modal placeholders — Case Summary columns empty",
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function blankChecks({ pass = false, detail = "" } = {}) {
  return { pass, detail };
}

function fieldsForPatchType(patchType, item) {
  switch (patchType) {
    case "materialize_image":
      return ["Image", "Title", "External Display Status"];
    case "promote_registry_asset":
      return ["Image", "Brand Asset Registry"];
    case "populate_modal_fields":
      return [
        "Case Summary Overview",
        "Case Summary Brand Relevance",
        "Case Summary Owner Objective",
        "Case Summary Interpretation",
        "Case Summary Tags",
        "Body",
      ];
    case "rewrite_external_copy":
      return ["Title", "Body"];
    case "suppress_empty_field":
      return ["Body", "External Display Status"];
    case "create_presentation_row":
      return ["Slot Key", "Title", "Body", "Sort Order", "Active", "Brand", "Brand Name"];
    case "hide_duplicate_row":
      return ["External Display Status", "Active"];
    case "patch_presentation_row":
      return ["Title", "Body"];
    default:
      return [];
  }
}

function expectedQAImpact(patchType, item) {
  const map = {
    materialize_image: "Improves render-readiness + gallery/property image QA",
    promote_registry_asset: "Registry→Presentation Image materialization; gallery_six_visible",
    populate_modal_fields: "Clears footprint.openings modal placeholders",
    rewrite_external_copy: "Clears model-fit / visible URL / governance copy hits",
    suppress_empty_field: "Removes empty card bodies from external-owner score",
    create_presentation_row: "Populates underfilled tabs; suppresses UI fallbacks",
    hide_duplicate_row: "Removes duplicate openings from owner view",
    renderer_patch_required: "No Airtable fix — atelier fallback surfaces need code change",
    no_write_investigation: "Investigation only — may adjust contract or warrant exception",
    patch_presentation_row: "Plan contract compliance before apply",
  };
  return map[patchType] || "Unknown QA impact";
}

/**
 * Build one patch plan item from a v36C remediation item.
 */
export function buildPatchPlanItem(item, { brandConfig = null, presentationRows = [] } = {}) {
  let patchType = ISSUE_TO_PATCH_TYPE[item.issueType] || "no_write_investigation";
  if (item.codeRenderPatchRequired) patchType = "renderer_patch_required";

  const matchedRow =
    (item.recordId && presentationRows.find((r) => r.recordId === item.recordId)) ||
    (item.slot &&
      presentationRows.find(
        (r) =>
          r.slotKey === item.slot &&
          (!item.rootCause || !nz(r.title) || true)
      )) ||
    null;

  const safeForGenericApply =
    Boolean(item.genericFactoryCanFix) &&
    !item.founderJudgmentNeeded &&
    !item.codeRenderPatchRequired &&
    patchType !== "no_write_investigation" &&
    patchType !== "renderer_patch_required";

  const brandModelFitCheck = blankChecks({
    pass: item.issueType !== "wrong_model_language",
    detail:
      item.issueType === "wrong_model_language"
        ? `Must rewrite with ${brandConfig?.brandModelType || brandConfig?.copyGovernanceMode || "affiliation"} framing`
        : "model fit assumed ok until rewrite applied",
  });

  const externalOwnerCopyCheck = blankChecks({
    pass: !["visible_source_url", "sources_block_visible", "governance_language", "wrong_model_language"].includes(
      item.issueType
    ),
    detail: item.proposedFix,
  });

  const renderReadinessCheck = blankChecks({
    pass: !["property_example_render_not_ready", "gallery_render_not_ready", "registry_only_images", "scenario_fallback_risk"].includes(
      item.issueType
    ),
    detail: item.rootCause || item.issueType,
  });

  return {
    brand: item.brand,
    brandSlug: item.brandSlug,
    issueId: item.issueId,
    severity: item.severity,
    stage: item.requiredSystemStage,
    targetTable: patchType === "promote_registry_asset" ? REGISTRY_TABLE : PRESENTATION_TABLE,
    recordId: item.recordId || matchedRow?.recordId || null,
    slotKey: item.slot || matchedRow?.slotKey || null,
    patchType,
    fieldsToPatch: fieldsForPatchType(patchType, item),
    proposedBefore: {
      title: matchedRow?.title || null,
      bodyPreview: matchedRow?.body ? String(matchedRow.body).slice(0, 160) : null,
      imageUrl: matchedRow?.imageUrl || null,
      rootCause: item.rootCause,
    },
    proposedAfter: {
      action: item.proposedFix,
      note: "Dry-run proposal only — values require apply gate + founder/config packages",
    },
    sourceSupport: {
      brandConfigPresent: Boolean(brandConfig),
      officialDomains: brandConfig?.officialSourceDomains || [],
      airtableWriteRequiredLater: item.airtableWriteRequiredLater !== false,
    },
    brandModelFitCheck,
    externalOwnerCopyCheck,
    renderReadinessCheck,
    expectedQAImpact: expectedQAImpact(patchType, item),
    requiresFounderApproval: Boolean(item.founderJudgmentNeeded),
    requiresCodePatch: Boolean(item.codeRenderPatchRequired) || patchType === "renderer_patch_required",
    safeForGenericApply,
    ownerVisible: item.ownerVisible !== false,
    tab: item.tab,
  };
}

/**
 * Expand Design Hotels remediation with named modal targets if missing from v36C items.
 */
export function expandDesignHotelsPatchPlan(items, brandConfig) {
  const existing = items.map((i) => buildPatchPlanItem(i, { brandConfig }));
  const hasModal = existing.some((p) => p.patchType === "populate_modal_fields");
  const extras = [];

  if (!hasModal) {
    for (const target of DESIGN_HOTELS_MODAL_TARGETS) {
      extras.push(
        buildPatchPlanItem(
          {
            issueId: `design-hotels:modal_placeholder:${target.propertyName}`,
            severity: "high",
            brand: "Design Hotels",
            brandSlug: "design-hotels",
            tab: "Footprint & Growth",
            slot: target.slotKey,
            recordId: target.recordId,
            issueType: "modal_placeholder",
            ownerVisible: true,
            rootCause: target.note,
            proposedFix:
              "Populate Case Summary Overview/Brand Relevance/Owner Objective/Interpretation or expand Body to 5+ paragraphs",
            requiredSystemStage: "visual_asset_materialization",
            genericFactoryCanFix: false,
            brandSpecificConfigNeeded: true,
            founderJudgmentNeeded: true,
            airtableWriteRequiredLater: true,
            codeRenderPatchRequired: false,
          },
          { brandConfig }
        )
      );
    }
  }

  // Standards / loyalty / economics coverage as brand-specific create rows if not present
  for (const slotGroup of [
    { issueType: "tab_coverage_gap", tab: "Owner Considerations", slot: "standards.requirement", label: "standards table owner-readiness" },
    { issueType: "tab_coverage_gap", tab: "Loyalty Program", slot: "loyalty.owner_lens", label: "loyalty tab coverage" },
    { issueType: "tab_coverage_gap", tab: "Economics & Obligations", slot: "economics.intro", label: "economics/fee affiliation fit" },
  ]) {
    const already = existing.some(
      (p) => p.tab === slotGroup.tab || (p.slotKey && p.slotKey.startsWith(slotGroup.slot.split(".")[0]))
    );
    if (!already) {
      extras.push(
        buildPatchPlanItem(
          {
            issueId: `design-hotels:tab_coverage_gap:${slotGroup.slot}`,
            severity: "high",
            brand: "Design Hotels",
            brandSlug: "design-hotels",
            tab: slotGroup.tab,
            slot: slotGroup.slot,
            recordId: null,
            issueType: "tab_coverage_gap",
            ownerVisible: true,
            rootCause: slotGroup.label,
            proposedFix: `Populate ${slotGroup.slot} with affiliation-safe, brand-specific copy (no FDD)`,
            requiredSystemStage: "draft_build",
            genericFactoryCanFix: false,
            brandSpecificConfigNeeded: true,
            founderJudgmentNeeded: true,
            airtableWriteRequiredLater: true,
            codeRenderPatchRequired: false,
          },
          { brandConfig }
        )
      );
    }
  }

  return [...existing, ...extras];
}

export function summarizePatchPlan(patchItems = []) {
  const byType = {};
  for (const p of patchItems) {
    byType[p.patchType] = (byType[p.patchType] || 0) + 1;
  }
  return {
    total: patchItems.length,
    safeForGenericApply: patchItems.filter((p) => p.safeForGenericApply).length,
    requiresFounderApproval: patchItems.filter((p) => p.requiresFounderApproval).length,
    requiresCodePatch: patchItems.filter((p) => p.requiresCodePatch).length,
    ownerVisible: patchItems.filter((p) => p.ownerVisible).length,
    byType,
  };
}
