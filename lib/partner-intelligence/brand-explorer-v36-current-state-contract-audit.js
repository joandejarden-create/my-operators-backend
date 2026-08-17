/**
 * Brand Explorer v36A Current-State Contract Audit (read-only).
 *
 * Validates proposed v36 content contract against codebase, renderers,
 * factory modules, schema assumptions, and latest brand reports.
 *
 * @see docs/data-intelligence/brand-explorer-v36-current-state-contract-audit.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const AUDIT_VERSION = "v36A";
export const REPORT_JSON_NAME = "brand-explorer-v36-current-state-contract-audit.json";
export const REPORT_MD_NAME = "brand-explorer-v36-current-state-contract-audit.md";
export const DOC_MD_NAME = "brand-explorer-v36-current-state-contract-audit.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const REFERENCE_BRANDS = [
  { key: "design-hotels", name: "Design Hotels", slug: "design-hotels", recordId: "rec02zPClpWUTCyXM" },
  { key: "slh", name: "Small Luxury Hotels of the World", slug: "small-luxury-hotels-of-the-world", recordId: "recjjSnY2opb8P4DG" },
  { key: "tribute-portfolio", name: "Tribute Portfolio", slug: "tribute-portfolio", recordId: "recCvV0PuZOi8c3hC" },
  { key: "woodspring-suites", name: "WoodSpring Suites", slug: "woodspring-suites", recordId: "recsOd51NzRPYsMko" },
  { key: "everhome-suites", name: "Everhome Suites", slug: "everhome-suites", recordId: "recqkkrsevi4r9ibj" },
];

const V36_COMPONENTS = [
  "Brand Knowledge Pack",
  "Claim Ledger",
  "Visual Asset Pack",
  "Brand Model Spec",
  "Full Tab Content Contract",
  "External Owner Copy Rules",
  "Presentation Plan Row Contract",
  "External Owner Readiness Score",
  "Batch Queue Status",
  "Exception Review Report",
];

const FACTORY_MODULES = [
  { path: "lib/partner-intelligence/brand-explorer-active-profile-factory.js", version: "v34D", role: "orchestrator" },
  { path: "lib/partner-intelligence/brand-explorer-active-profile-factory-rules.js", version: "v34D", role: "gate_rules" },
  { path: "lib/partner-intelligence/brand-explorer-active-profile-brand-config.js", version: "v35B", role: "brand_model_config" },
  { path: "lib/partner-intelligence/brand-explorer-active-profile-asset-pack-builder.js", version: "v34B", role: "visual_asset_pack" },
  { path: "lib/partner-intelligence/brand-explorer-active-profile-draft-builder.js", version: "v34B", role: "draft_apply" },
  { path: "lib/partner-intelligence/brand-explorer-active-profile-copy-governance-builder.js", version: "v34C/v35B", role: "copy_governance" },
  { path: "lib/partner-intelligence/brand-explorer-active-profile-staged-apply.js", version: "v34D", role: "staged_apply_gates" },
  { path: "lib/partner-intelligence/brand-explorer-lifestyle-affiliation-brand-config.js", version: "v35B", role: "lifestyle_brand_config" },
  { path: "lib/partner-intelligence/brand-explorer-lifestyle-affiliation-property-catalog.js", version: "v35C", role: "property_catalog" },
  { path: "lib/partner-intelligence/brand-explorer-design-hotels-content-packages-v35F.js", version: "v35F", role: "design_hotels_content" },
  { path: "lib/partner-intelligence/brand-explorer-final-qa-auditor.js", version: "v1", role: "final_qa" },
  { path: "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js", version: "v2", role: "complete_build_batch" },
  { path: "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js", version: "v24", role: "visual_defect_audit" },
  { path: "lib/partner-intelligence/brand-explorer-external-owner-readiness-rules.js", version: "—", role: "external_owner_readiness" },
  { path: "lib/partner-intelligence/brand-explorer-external-owner-content-governance.js", version: "—", role: "external_owner_copy_rules" },
  { path: "lib/partner-intelligence/brand-explorer-required-section-population-contract.js", version: "v27B", role: "required_section_contract" },
  { path: "lib/partner-intelligence/brand-explorer-slot-standard-manifest.js", version: "v18", role: "slot_manifest" },
];

const SLOT_FAMILIES = [
  {
    prefix: "overview.",
    tab: "Overview",
    renderer: "renderAtelierOverview (+ hero in brand-explorer-gold-detail.js)",
    repeatable: "mixed",
    activeProfileRequired: true,
    examples: {
      "design-hotels": ["overview.why_value", "overview.scenario.1", "overview.proof.2"],
      "woodspring-suites": ["overview.typical_use_case", "overview.scenario.1"],
      tribute: ["overview.typical_use_case", "overview.scenario.1", "overview.portfolio_context"],
      "everhome-suites": ["overview.typical_use_case", "overview.scenario.1"],
      slh: ["overview.why_value", "overview.scenario.1"],
    },
  },
  {
    prefix: "overview.scenario.",
    tab: "Overview",
    renderer: "renderAtelierOverview scenario strip",
    repeatable: true,
    activeProfileRequired: true,
    expectedFields: ["Title", "Body", "Image→imageUrl", "Sort Order", "Active"],
  },
  {
    prefix: "overview.proof.",
    tab: "Overview",
    renderer: "renderAtelierOverview proof grid",
    repeatable: true,
    activeProfileRequired: false,
    notes: "Code renders overview.proof.1–6; docs only mention overview.proof_operator",
  },
  {
    prefix: "overview.differentiators.",
    tab: "Overview",
    renderer: "renderAtelierOverview Key Differentiators",
    repeatable: false,
    activeProfileRequired: true,
  },
  {
    prefix: "overview.bestAt.",
    tab: "Overview",
    renderer: "renderAtelierOverview Best At cards",
    repeatable: true,
    activeProfileRequired: false,
  },
  {
    prefix: "valueOwners.",
    tab: "Value to Owners",
    renderer: "renderValueToOwners",
    repeatable: "mixed",
    activeProfileRequired: true,
  },
  {
    prefix: "operations.",
    tab: "Operating Model",
    renderer: "renderOperationsStandards",
    repeatable: "mixed",
    activeProfileRequired: true,
    notes: "operations.flexibility.* requires canonical bar labels; operations.model.* has no Brand Setup fallback",
  },
  {
    prefix: "standards.",
    tab: "Owner Considerations",
    renderer: "renderStandardsOwnerConsiderations",
    repeatable: "standards.requirement multi-row",
    activeProfileRequired: true,
    notes: "Rendered in code but under-documented in presentation-slots.md",
  },
  {
    prefix: "commercial.",
    tab: "Commercial Engine",
    renderer: "renderCommercialEngine",
    repeatable: "mixed",
    activeProfileRequired: true,
    notes: "Heavy static fallbacks when slots empty (COMM_STATIC, LOY_DEMAND analog)",
  },
  {
    prefix: "economics.",
    tab: "Economics & Obligations",
    renderer: "renderAtelierEconomicsObligations",
    repeatable: "mixed",
    activeProfileRequired: true,
    notes: "economics.checklist/diligence/opening.financials documented but NOT wired in UI",
  },
  {
    prefix: "loyalty.",
    tab: "Loyalty Program",
    renderer: "renderLoyaltyProgram",
    repeatable: "mixed",
    activeProfileRequired: true,
    notes: "loyalty.kpi.* + loyalty.proof/elite/implications; demand matrix is hardcoded",
  },
  {
    prefix: "footprint.",
    tab: "Footprint & Growth",
    renderer: "renderFootprintGrowth + renderMomentumSection",
    repeatable: "footprint.openings/momentum/mix multi-row",
    activeProfileRequired: true,
    notes: "footprint.openings requires 5+ body paragraphs for full card; momentum needs press URL for linked label",
  },
  {
    prefix: "materials.gallery.",
    tab: "Brand Materials",
    renderer: "renderBrandMaterials gallery tiles",
    repeatable: true,
    activeProfileRequired: true,
    expectedFields: ["Image→imageUrl required for visible gallery QA"],
  },
  {
    prefix: "materials.file",
    tab: "Brand Materials",
    renderer: "renderBrandMaterials file cards",
    repeatable: true,
    activeProfileRequired: false,
  },
  {
    prefix: "insight.",
    tab: "Dealality Insight",
    renderer: "renderDealalityInsight",
    repeatable: "insight.similar multi-row",
    activeProfileRequired: true,
  },
];

const SCHEMA_INVENTORY = {
  tables: [
    {
      name: "Brand Setup - Brand Explorer Presentation",
      exists: "documented + used",
      readBy: ["api/brand-library.js", "factory modules", "final-qa", "complete-build", "all apply writers"],
      writtenBy: ["draft-builder", "copy-governance-builder", "brand-specific writers"],
      fieldsConfirmed: [
        "Slot Key",
        "Title",
        "Body",
        "Sort Order",
        "Active",
        "Brand (link)",
        "Brand Name",
        "Image / Images / Scenario Image / Attachments",
        "Case Summary Overview",
        "Case Summary Owner Objective",
        "Case Summary Brand Relevance",
        "Case Summary Interpretation",
        "Case Summary Tags",
        "Summary URL / View Summary URL / Case summary URL",
        "External Display Status",
      ],
      fieldsAssumedNotInSchema: [
        "Brand Asset Registry (link on Presentation row — used in factory rules; may be missing in some bases)",
        "Source IDs / Source Trace (no dedicated trace columns — trace lives in reports only)",
        "Ready for Active Profile (referenced in blocked fields; not factory-written)",
        "Company Validated (read-only guard; never auto-written)",
      ],
      apiBlockShape: [
        "recordId",
        "slotKey",
        "title",
        "body",
        "sort",
        "imageUrl",
        "summaryUrl",
        "caseSummaryOverview",
        "caseSummaryOwnerObjective",
        "caseSummaryBrandRelevance",
        "caseSummaryInterpretation",
        "caseSummaryTags",
      ],
      internalOnlyFields: [
        "Source footnotes in Body (stripped by external-owner governance)",
        "External Display Status = Internal Only / Do Not Display (filtered server-side)",
      ],
    },
    {
      name: "Brand Setup - Brand Basics",
      exists: "documented + used",
      readBy: ["brand-library.js", "final-qa", "factory"],
      writtenBy: ["none in active-profile factory stack"],
      fieldsUsed: ["Company Validated", "Company Validation Date", "Parent Company", "Brand Name", "feeStructure", "footprint", "loyaltyCommercial", "brandStandards"],
    },
    {
      name: "Source Library",
      exists: "documented + used",
      readBy: ["copy-governance", "final-qa", "orchestrator"],
      writtenBy: ["source capture scripts only — not factory draft apply"],
      fieldsUsed: ["Approved for Explorer Use", "Source URL", "Source Title", "Source Type", "Notes"],
    },
    {
      name: "Partner Intelligence - Brand Asset Registry",
      exists: "documented + used",
      readBy: ["asset-pack-builder", "factory rules", "visual audits"],
      writtenBy: ["draft-builder creates Candidate Only stubs"],
      fieldsUsed: [
        "Asset Name",
        "Brand",
        "Source URL",
        "Source Page URL",
        "Explorer Use Permission",
        "Usage Review Status",
        "Recommended Explorer Slot",
        "Asset Status",
      ],
      gap: "Registry linkage to Presentation Image field is indirect — draft apply materializes Image attachment from registry URL, not a persistent link field on Presentation",
    },
    {
      name: "Partner Facts",
      exists: "documented + used",
      readBy: ["final-qa", "complete-build orchestrator"],
      writtenBy: ["not in active-profile factory"],
      usage: "Governance / Claim Ledger analog — not wired to presentation row generation in factory v34D",
    },
  ],
};

const V36_COMPONENT_CLASSIFICATION = [
  {
    component: "Brand Knowledge Pack",
    classification: "partially_exists",
    existingArtifacts: [
      "ACTIVE_PROFILE_BRAND_CONFIGS (brand-explorer-active-profile-brand-config.js)",
      "brand-explorer-lifestyle-affiliation-brand-config.js",
      "approved Sources + property catalogs",
      "overviewScenarioCopy / momentumSourceUrls per brand",
    ],
    gaps: ["No unified versioned JSON schema", "No single export consumed by all writers", "Facts not integrated into pack"],
    conflicts: [],
    recommendation: "safe_to_implement_now",
    notes: "Formalize as read-only aggregate over existing config + sources + catalogs; do not duplicate v35F content packages",
  },
  {
    component: "Claim Ledger",
    classification: "partially_exists",
    existingArtifacts: ["Partner Facts table", "Source Library", "final-qa sourceGovernanceScore"],
    gaps: ["No claim-to-slot mapping object", "No approval state per claim row in factory pipeline", "Facts not required for active-profile draft apply today"],
    conflicts: ["Design Hotels has 0 approved facts but 90+ presentation rows"],
    recommendation: "should_be_deferred",
    notes: "Defer until fact approval workflow is mandatory for affiliation brands; use Source Library as interim ledger",
  },
  {
    component: "Visual Asset Pack",
    classification: "already_exists",
    existingArtifacts: ["buildActiveProfileAssetPack (v34B)", "gallery/property/scenario/proofSupport outputs", "readinessBand"],
    gaps: ["Registry-only assets can pass traceability QA but fail render until Image materialized"],
    conflicts: [],
    recommendation: "safe_to_implement_now",
    notes: "Promote to first-class v36 contract type; add render-readiness flag separate from registry-readiness",
  },
  {
    component: "Brand Model Spec",
    classification: "partially_exists",
    existingArtifacts: [
      "ACTIVE_PROFILE_BRAND_CONFIGS modelType/copyGovernanceMode",
      "lifestyle-affiliation brandModelType",
      "required-section-population-contract section minimums",
    ],
    gaps: ["No single spec drives both renderer fallbacks and writer validation", "Affiliation vs franchise rules scattered"],
    conflicts: ["Commercial/economics static fallbacks ignore brand model spec"],
    recommendation: "safe_to_implement_now",
    notes: "Extract from brand-config + required-section-contract; wire into v36 validation layer",
  },
  {
    component: "Full Tab Content Contract",
    classification: "partially_exists",
    existingArtifacts: [
      "docs/brand-explorer-presentation-slots.md",
      "brand-explorer-slot-standard-manifest.js",
      "brand-explorer-required-section-population-contract.js",
    ],
    gaps: [
      "Docs/code drift: materials.caseStudy, economics.checklist unwired",
      "standards.* rendered but not in slots doc",
      "overview.proof.1–6 rendered but not documented",
    ],
    conflicts: ["Renderer expects fields contract does not document"],
    recommendation: "needs_migration",
    notes: "Regenerate canonical contract from code + slot manifest before v36 writers",
  },
  {
    component: "External Owner Copy Rules",
    classification: "already_exists",
    existingArtifacts: [
      "brand-explorer-external-owner-content-governance.js",
      "brand-explorer-external-owner-readiness-rules.js",
      "COPY_SAFETY_PATTERNS in factory-rules",
    ],
    gaps: ["footprint.momentum URL exception recently added — not in all writers", "No numeric score"],
    conflicts: ["Momentum/openings URLs allowed in body but other slots strip URLs"],
    recommendation: "safe_to_implement_now",
    notes: "Centralize slot-aware URL policy in v36 contract",
  },
  {
    component: "Presentation Plan Row Contract",
    classification: "partially_exists",
    existingArtifacts: ["draft-builder presentationPatches shape", "factory listPresentationRows normalized row"],
    gaps: ["No JSON schema validation before apply", "Case summary + imageUrl rules not enforced uniformly"],
    conflicts: [],
    recommendation: "safe_to_implement_now",
    notes: "Codify patch shape { recordId, slotKey, fields, reason, sourceIds } as v36 contract",
  },
  {
    component: "External Owner Readiness Score",
    classification: "partially_exists",
    existingArtifacts: ["evaluateExternalOwnerReadinessRule → pass/blockers", "founder visual review external_owner_readiness check"],
    gaps: ["Not numeric; not in final-qa overallNumeric", "Modal placeholder detection incomplete in Final QA"],
    conflicts: [],
    recommendation: "safe_to_implement_now",
    notes: "Add weighted score to v36; promote modal field completeness from visual audit",
  },
  {
    component: "Batch Queue Status",
    classification: "already_exists",
    existingArtifacts: ["complete-build-orchestrator buildBatchAggregate", "expansion_backlog wave metadata"],
    gaps: ["No persistent queue table", "Per-brand halt reasons not unified UI"],
    conflicts: [],
    recommendation: "safe_to_implement_now",
    notes: "Expose orchestrator batch output as v36 Batch Queue Status artifact",
  },
  {
    component: "Exception Review Report",
    classification: "partially_exists",
    existingArtifacts: [
      "founderReviewQueue in copy-governance",
      "founderQueueResolution",
      "visual defect audit defectsBySeverity",
      "complete-build blockers[]",
    ],
    gaps: ["No single exception report schema", "Founder screenshot issues captured ad hoc in writer reports"],
    conflicts: [],
    recommendation: "safe_to_implement_now",
    notes: "Merge founderReviewQueue + visual defects + external-owner blockers into v36 Exception Review Report",
  },
];

const IMPLEMENTATION_RISKS = [
  {
    id: "source_urls_in_external_copy",
    severity: "high",
    detail: "footprint.openings and footprint.momentum allow trailing URLs; other slots strip URLs via sanitizeAffiliationExternalCopy. Writers without slot-aware policy reintroduce governance hits.",
    mitigation: "v36 External Owner Copy Rules must declare per-slot URL policy",
  },
  {
    id: "source_traceability_fields_missing",
    severity: "medium",
    detail: "No Presentation columns for sourceIds/sourceFootnote; trace lives in apply reports only",
    mitigation: "Keep internal trace in v36 plan rows; optional Source Library link field TODO",
  },
  {
    id: "registry_link_missing_on_presentation",
    severity: "high",
    detail: "Brand Asset Registry link may be missing on Presentation table; factory infers traceability via slot+URL matching",
    mitigation: "v36 Presentation Plan should include registryCandidateId when available; do not assume Airtable link field",
  },
  {
    id: "gallery_requires_presentation_image",
    severity: "critical",
    detail: "UI reads block.imageUrl from Presentation Image attachment only; registry-only assets do not render",
    mitigation: "Visual Asset Pack must distinguish registry-readiness vs render-readiness; draft apply must materialize Image",
  },
  {
    id: "registry_passes_qa_not_render",
    severity: "high",
    detail: "Factory registry traceability rule can pass while gallery_six_visible fails",
    mitigation: "v36 QA must require API imageUrl not just registry row",
  },
  {
    id: "hardcoded_ui_fallbacks",
    severity: "medium",
    detail: "Commercial, loyalty, economics, proof grids use static fallbacks masking empty slots",
    mitigation: "v36 contract should flag fallback-active sections as incomplete for active-profile",
  },
  {
    id: "empty_modal_fields",
    severity: "high",
    detail: "footprint.openings modals need Case Summary columns or 5+ paragraph Body; Final QA weak on modal placeholders",
    mitigation: "Promote evaluateExternalOwnerReadinessRule modal check into Final QA score",
  },
  {
    id: "economics_fee_template_language",
    severity: "medium",
    detail: "Affiliation brands still inherit FDD-oriented economics templates and fee bucket defaults",
    mitigation: "Brand Model Spec must branch economics.intro and fee copy for affiliation_curation_platform",
  },
  {
    id: "company_validated_wording",
    severity: "critical",
    detail: "All factory apply paths block Company Validated changes; copy must never imply brand-verified",
    mitigation: "v36 External Owner Copy Rules — enforce across all generated copy",
  },
  {
    id: "draft_vs_active_apply_separation",
    severity: "high",
    detail: "apply-approved does not write readyForActiveProfile; founders must not conflate draft materialization with active approval",
    mitigation: "v36 Batch Queue Status must show stage: draft-applied vs founder-approved vs active-ready",
  },
  {
    id: "materials_casestudy_unwired",
    severity: "medium",
    detail: "materials.caseStudy parsed in modal JS but not rendered — data can exist with zero UI",
    mitigation: "Either wire renderer or exclude from v36 Full Tab Contract until wired",
  },
  {
    id: "openings_body_shape",
    severity: "high",
    detail: "4-paragraph footprint.openings bodies lose scenario/meta/teaser in parser",
    mitigation: "v36 Presentation Plan Row Contract must enforce 5+ block or Case Summary columns",
  },
];

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function findFactoryReport(brand) {
  const directKey = readJsonIfExists(`reports/brand-explorer-active-profile-factory-${brand.key}.json`);
  if (directKey) return directKey;

  const reportsDir = path.join(ROOT, "reports");
  if (!fs.existsSync(reportsDir)) return null;

  const versionedPrefixes = [
    `brand-explorer-active-profile-factory-${brand.key}-v`,
    `brand-explorer-active-profile-factory-${brand.slug}-v`,
  ];
  const versionedCandidates = fs
    .readdirSync(reportsDir)
    .filter((name) => name.endsWith(".json") && versionedPrefixes.some((prefix) => name.startsWith(prefix)))
    .sort()
    .reverse();
  for (const name of versionedCandidates) {
    const parsed = readJsonIfExists(`reports/${name}`);
    if (parsed) return parsed;
  }

  const slugAggregate = readJsonIfExists(`reports/brand-explorer-active-profile-factory-${brand.slug}.json`);
  if (slugAggregate && !slugAggregate.stage) return slugAggregate;

  const stageSuffixes = ["-preflight", "-asset-pack", "-build-draft", "-copy-governance", "-founder-review", "-apply-draft", "-apply-approved"];
  const stageCandidates = fs
    .readdirSync(reportsDir)
    .filter(
      (name) =>
        name.endsWith(".json") &&
        name.startsWith(`brand-explorer-active-profile-factory-${brand.slug}`) &&
        stageSuffixes.some((suffix) => name.includes(suffix))
    )
    .sort()
    .reverse();
  for (const name of stageCandidates) {
    const parsed = readJsonIfExists(`reports/${name}`);
    if (parsed) return parsed;
  }

  return slugAggregate;
}

function extractFactorySnapshot(factoryReport) {
  if (!factoryReport) return null;

  const brandBundle = factoryReport.brand?.factoryReports ? factoryReport.brand : factoryReport;
  const factoryReports = brandBundle.factoryReports || {};
  const founderReviewSummary = brandBundle.founderReview;

  const nested =
    factoryReports["founder-review"] ||
    factoryReports.preflight ||
    factoryReports["copy-governance"] ||
    Object.values(factoryReports).find((r) => r?.founderVisualReview);

  const lastRun = factoryReport.runs?.[factoryReport.runs.length - 1] || nested || factoryReport;
  const founderReview = lastRun.founderVisualReview || nested?.founderVisualReview || factoryReport.founderVisualReview;
  const preflight = lastRun.stageResults?.find?.((s) => s.stage === "preflight")?.summary;
  const copyGov =
    lastRun.stageResults?.find?.((s) => s.stage === "copy-governance")?.summary ||
    factoryReports["copy-governance"];

  const factoryBlockers = founderReviewSummary?.blockers || nested?.blockers || lastRun.blockers || [];
  const blockers = [
    ...factoryBlockers.map((b) => (typeof b === "string" ? { section: "factory", message: b } : b)),
    ...(copyGov?.blockers || []),
    ...(founderReview?.checks?.filter((c) => !c.pass).map((c) => ({ section: c.id, message: c.detail || c.label })) || []),
  ].slice(0, 12);

  return {
    source: "factory_report",
    version: factoryReport.version || nested?.factoryVersion || null,
    readiness: brandBundle.readiness || null,
    readyForActiveProfile:
      preflight?.readyForActiveProfile ??
      nested?.readyForActiveProfile ??
      lastRun.readyForActiveProfile ??
      false,
    founderVisualPass: founderReview?.pass ?? (founderReviewSummary ? founderReviewSummary.galleryPass && founderReviewSummary.propertyExamplesPass : null),
    founderVisualFailedChecks:
      founderReview?.failedChecks?.map((c) => c.id) ||
      founderReview?.checks?.filter((c) => !c.pass).map((c) => c.id) ||
      [],
    blockers,
    externalOwnerBlockers: copyGov?.blockers?.length || 0,
    visibleGalleryCount: nested?.visibleGalleryCount ?? nested?.factoryRules?.gallery?.withImageUrl ?? null,
    wouldApplyPresentationPatches: founderReviewSummary?.wouldApplyPresentationPatches ?? null,
  };
}

function fileExists(relPath) {
  return fs.existsSync(path.join(ROOT, relPath));
}

function summarizeBrandReport(brand) {
  const completeBuild = readJsonIfExists(`reports/brand-explorer-complete-build-${brand.slug}.json`);
  const factoryReport = findFactoryReport(brand);
  const finalQa =
    readJsonIfExists(`reports/brand-explorer-active-profile-factory-${brand.key}-final-qa.json`) ||
    readJsonIfExists(`reports/brand-explorer-active-profile-factory-${brand.slug}-final-qa.json`);

  return {
    brand: brand.name,
    slug: brand.slug,
    recordId: brand.recordId,
    reportsFound: {
      completeBuild: Boolean(completeBuild),
      factory: Boolean(factoryReport),
      finalQa: Boolean(finalQa),
    },
    completeBuild: completeBuild
      ? {
          halted: completeBuild.halted,
          haltReason: completeBuild.haltReason,
          readyForActiveProfile: completeBuild.readyForActiveProfile,
          readinessBand: completeBuild.readinessBand,
          blockers: (completeBuild.blockers || []).slice(0, 12),
          finalQaSummary: completeBuild.stageResults?.find((s) => s.stage === "final_qa_auditor")?.summary || null,
          visualDefectSummary: completeBuild.stageResults?.find((s) => s.stage === "visual_defect_audit")?.summary || null,
          requiredSectionScore: completeBuild.stageResults?.find((s) => s.stage === "required_section_contract")?.summary || null,
        }
      : null,
    factorySnapshot: !completeBuild ? extractFactorySnapshot(factoryReport) : null,
    factoryStage: factoryReport?.stage || factoryReport?.mode || factoryReport?.version || null,
    founderVisualPass: factoryReport?.founderVisualReview?.pass ?? extractFactorySnapshot(factoryReport)?.founderVisualPass ?? null,
  };
}

function qaGapAnalysis() {
  return {
    commonFalsePositives: [
      "Final QA presentationQualityScore can be moderate while UI still shows static commercial/loyalty fallbacks",
      "Registry traceability pass does not guarantee gallery imageUrl in API",
      "external_owner_readiness pass after momentum URL exceptions may miss other slot URL leaks",
      "required_section_contract 100% while visual_defect_audit still has high-severity items (WoodSpring)",
    ],
    gapsNotCaughtByFinalQa: [
      "Hardcoded UI fallback cards (commercial.demand, loyalty.proof, economics.negotiable_items)",
      "materials.caseStudy rows present in Airtable but never rendered",
      "footprint.openings 4-block legacy body shape (thin cards)",
      "Momentum rows with directory URLs instead of press URLs (caught only by founder review)",
      "Non-hotel photography passing property example rules in some factory runs",
    ],
    founderScreenshotOnlyIssues: [
      "Loyalty KPI strip empty or generic for affiliation brands",
      "Standard Detail table placeholder/governance language",
      "Recent Momentum date column vs headline column parse inversion",
      "Gallery fewer than 6 visible despite registry candidates",
      "Watchouts bullet count / empty li elements",
    ],
    recommendedExternalOwnerChecks: [
      "visible_source_urls per slot policy",
      "modal_placeholders on footprint.openings",
      "empty_visible_cards on titled rows",
      "governance_language (FDD, LOI, brand-verified, Sources: blocks)",
      "fallback_active detection for commercial/loyalty/economics sections",
    ],
  };
}

function recommendedV36Plan() {
  return {
    implementImmediately: [
      "Visual Asset Pack contract (formalize v34B output + render-readiness flag)",
      "External Owner Copy Rules (centralize slot URL policy + affiliation sanitizers)",
      "Presentation Plan Row Contract (validate before apply)",
      "Batch Queue Status (surface complete-build-orchestrator aggregate)",
      "Exception Review Report (merge founder queue + visual defects + blockers)",
      "Brand Model Spec (extract from ACTIVE_PROFILE_BRAND_CONFIGS + lifestyle config)",
    ],
    schemaSafeApproach: [
      "Read-only v36 contract modules consuming existing API block shape",
      "No new Airtable fields in v36 phase 1",
      "Internal source trace in plan JSON only — not in Presentation Body",
      "Draft apply continues to materialize Image attachments; no registry link field required in phase 1",
    ],
    requiresFieldAdditions: [
      "Optional: Brand Asset Registry link on Presentation (nice-to-have, not blocking)",
      "Optional: Source IDs multi-link on Presentation for dev traceability",
      "Case Summary columns already exist — ensure ensure-brand-explorer-presentation-case-summary-fields run on all bases",
    ],
    rendererChangesNeeded: [
      "Wire materials.caseStudy OR remove from contract",
      "Wire economics.checklist/diligence OR remove from docs",
      "Detect and surface fallback-active state in UI (dev banner or QA only)",
      "Design Hotels momentum label default already fixed — generalize via Brand Model Spec",
    ],
    qaRuleAdditions: [
      "render_readiness: API imageUrl present for gallery/openings/scenarios",
      "fallback_active: section uses static fallback while slots empty",
      "momentum_announcement_url: press/trade URL pattern required",
      "openings_body_shape: 5+ paragraphs OR case summary columns complete",
      "modal_field_completeness: promote external-owner rule into final-qa numeric",
    ],
    migrationStrategyDesignHotels: [
      "1. Run v36 contract audit against live API (this report)",
      "2. Fix remaining blockers: openings complete rows, standards table, loyalty fact-backed KPIs",
      "3. Do NOT re-run full v35F content — patch via Presentation Plan Row Contract",
      "4. Materialize gallery Images from asset pack before founder visual review",
      "5. Keep Company Validated untouched; external-owner cleanup already applied",
    ],
    batchStrategySlhTribute: [
      "Tribute: presentation mature (contract 100%); batch focus on fact approval + governance publish — not content rebuild",
      "SLH: mirror Design Hotels affiliation pipeline; use lifestyle-affiliation config; asset pack from property catalog",
      "Both: use Batch Queue Status to track haltReason separately from readyForActiveProfile",
    ],
    nextImplementationPrompt:
      "Implement v36B Brand Knowledge Pack + Presentation Plan Row Contract as read-only lib modules that load ACTIVE_PROFILE_BRAND_CONFIGS, approved sources, and API blocks; emit validation report per brand without Airtable writes. Add render-readiness and fallback-active flags to Visual Asset Pack output. Wire into factory preflight stage before build-draft.",
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer v36A Current-State Contract Audit");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Audit version: ${report.auditVersion}`);
  lines.push(`Mode: **read-only** — no Airtable writes`);
  lines.push("");
  lines.push("## Executive summary");
  lines.push("");
  lines.push(
    "The codebase already implements most v36 concepts under different names (v34D factory, v34B asset pack, external-owner governance, complete-build batch queue). The primary gap is a **unified, code-derived Full Tab Content Contract** aligned with renderers — not greenfield infrastructure."
  );
  lines.push("");
  lines.push("## v36 component classification");
  lines.push("");
  for (const row of report.v36ComponentClassification) {
    lines.push(`### ${row.component}`);
    lines.push(`- **Status:** ${row.classification}`);
    lines.push(`- **Recommendation:** ${row.recommendation}`);
    if (row.gaps?.length) lines.push(`- **Gaps:** ${row.gaps.join("; ")}`);
    if (row.conflicts?.length) lines.push(`- **Conflicts:** ${row.conflicts.join("; ")}`);
    lines.push("");
  }
  lines.push("## Reference brand QA snapshot");
  lines.push("");
  for (const b of report.referenceBrandReports) {
    lines.push(`### ${b.brand} (\`${b.slug}\`)`);
    if (b.completeBuild) {
      lines.push(`- Halted: ${b.completeBuild.halted} (${b.completeBuild.haltReason || "—"})`);
      lines.push(`- readyForActiveProfile: ${b.completeBuild.readyForActiveProfile}`);
      lines.push(`- Required section score: ${b.completeBuild.requiredSectionScore?.readinessScore ?? "—"}%`);
      if (b.completeBuild.blockers?.length) {
        lines.push("- Top blockers:");
        for (const bl of b.completeBuild.blockers.slice(0, 5)) {
          lines.push(`  - ${bl.section || bl.type}: ${bl.message || bl.severity || ""}`);
        }
      }
    } else if (b.factorySnapshot) {
      lines.push(`- Source: factory report (${b.factorySnapshot.version || b.factoryStage || "—"})`);
      if (b.factorySnapshot.readiness) lines.push(`- Readiness: ${b.factorySnapshot.readiness}`);
      lines.push(`- readyForActiveProfile: ${b.factorySnapshot.readyForActiveProfile}`);
      lines.push(`- Founder visual review: ${b.factorySnapshot.founderVisualPass === true ? "PASS" : b.factorySnapshot.founderVisualPass === false ? "FAIL" : "—"}`);
      if (b.factorySnapshot.founderVisualFailedChecks?.length) {
        lines.push(`- Failed visual checks: ${b.factorySnapshot.founderVisualFailedChecks.join(", ")}`);
      }
      if (b.factorySnapshot.blockers?.length) {
        lines.push("- Top blockers:");
        for (const bl of b.factorySnapshot.blockers.slice(0, 5)) {
          lines.push(`  - ${bl.section || bl.type}: ${bl.message || bl.severity || ""}`);
        }
      }
    } else {
      lines.push("- Complete-build report not found");
      lines.push("- Factory report not found");
    }
    lines.push("");
  }
  lines.push("## Implementation risks");
  lines.push("");
  for (const r of report.implementationRisks) {
    lines.push(`- **${r.id}** (${r.severity}): ${r.detail}`);
  }
  lines.push("");
  lines.push("## Recommended v36 implementation plan");
  lines.push("");
  lines.push("### Implement immediately");
  for (const item of report.recommendedV36Plan.implementImmediately) lines.push(`- ${item}`);
  lines.push("");
  lines.push("### Next implementation prompt");
  lines.push("");
  lines.push(`> ${report.recommendedV36Plan.nextImplementationPrompt}`);
  lines.push("");
  lines.push("## Validation tests");
  lines.push("");
  for (const t of report.validationTests) {
    lines.push(`- \`${t.script}\`: **${t.status}**${t.exitCode != null ? ` (exit ${t.exitCode})` : ""}`);
  }
  return lines.join("\n");
}

export async function buildBrandExplorerV36CurrentStateContractAuditReport(options = {}) {
  const factoryModules = FACTORY_MODULES.map((m) => ({
    ...m,
    exists: fileExists(m.path),
  }));

  const slotInventory = {
    source: "docs/brand-explorer-presentation-slots.md + brand-explorer-slot-standard-manifest.js + atelier-from-api.js",
    families: SLOT_FAMILIES,
    canonicalManifestReport: fileExists("reports/brand-explorer-slot-standard-manifest.json")
      ? "reports/brand-explorer-slot-standard-manifest.json"
      : null,
    requiredSectionMinimums: "brand-explorer-required-section-population-contract.js REQUIRED_SECTION_MINIMUMS",
  };

  const referenceBrandReports = REFERENCE_BRANDS.map(summarizeBrandReport);

  const report = {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "read-only",
    airtableModified: false,
    purpose: "Validate proposed v36 content contract against existing codebase before v36 AI Build OS Core implementation",
    factoryPipeline: {
      stages: [
        "preflight",
        "asset-pack",
        "build-draft",
        "copy-governance",
        "apply-draft",
        "founder-review",
        "apply-approved",
        "final-qa",
      ],
      modules: factoryModules,
      blockedFieldsGlobal: [
        "Company Validated",
        "Company Validation Date",
        "Summary URL",
        "View Summary URL",
        "Ready for Active Profile (not written by factory apply-approved)",
      ],
    },
    frontendRenderers: {
      primary: "public/js/brand-explorer-atelier-from-api.js",
      api: "api/brand-library.js normalizeBrandExplorerPresentationRecords",
      hero: "public/js/brand-explorer-gold-detail.js",
      tabs: [
        "Overview",
        "Value to Owners",
        "Operating Model",
        "Owner Considerations",
        "Commercial Engine",
        "Economics & Obligations",
        "Loyalty Program",
        "Footprint & Growth",
        "Brand Materials",
        "Dealality Insight",
      ],
      docsRendererDrift: [
        "materials.caseStudy — modal wired, cards not rendered",
        "economics.checklist / economics.diligence / economics.opening.financials — documented, not in atelier JS",
        "standards.* — rendered, under-documented in slots doc",
        "overview.proof.1–6 — rendered, not in slots doc",
      ],
    },
    schemaInventory: SCHEMA_INVENTORY,
    slotInventory,
    referenceBrandReports,
    qaGapAnalysis: qaGapAnalysis(),
    v36ComponentClassification: V36_COMPONENT_CLASSIFICATION,
    implementationRisks: IMPLEMENTATION_RISKS,
    recommendedV36Plan: recommendedV36Plan(),
    validationTests: options.validationTests || [],
    filesRead: [
      ...FACTORY_MODULES.map((m) => m.path),
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "api/brand-library.js",
      "docs/brand-explorer-presentation-slots.md",
      "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
      "reports/brand-explorer-slot-standard-manifest.json",
      ...REFERENCE_BRANDS.map((b) => `reports/brand-explorer-complete-build-${b.slug}.json`),
    ],
    changeImpact: "Low — read-only audit report generation only",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function writeBrandExplorerV36AuditReports(report, root = ROOT) {
  const jsonPath = path.join(root, "reports", REPORT_JSON_NAME);
  const mdPath = path.join(root, "reports", REPORT_MD_NAME);
  const docPath = path.join(root, "docs", "data-intelligence", DOC_MD_NAME);
  fs.mkdirSync(path.dirname(jsonPath), { recursive: true });
  fs.mkdirSync(path.dirname(docPath), { recursive: true });
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, `${report.markdown}\n`);
  fs.writeFileSync(
    docPath,
    `# Brand Explorer v36A Current-State Contract Audit\n\nRead-only audit before v36 AI Build OS Core.\n\nSee report: \`reports/${REPORT_MD_NAME}\`\n`
  );
  return { jsonPath, mdPath, docPath };
}
