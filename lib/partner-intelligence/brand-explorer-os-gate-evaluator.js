/**
 * v41 — Brand Explorer OS single gate evaluator.
 * Live API + Presentation + internal preview + external DOM are source of truth.
 */
import { inventoryReleaseGates, GALLERY_MIN, PROPERTY_EXAMPLE_MIN } from "./brand-explorer-active-release-gate.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { scanInternalPreviewOwnerCopy } from "./brand-explorer-economics-chrome-remediation.js";
import { buildResidualOwnerCopyPatchPlan } from "./brand-explorer-residual-owner-copy-remediation.js";
import { evaluateBrandSpecificSourceValidation } from "./brand-explorer-brand-specific-source-validation.js";
import { evaluateRenderedFieldCompletenessFromPayload } from "./brand-explorer-rendered-field-completeness-evaluate.js";
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { scanNoEmptyRenderedComponents } from "./brand-explorer-no-empty-rendered-components.js";
import { evaluateSourceProvenanceByTab } from "./brand-explorer-source-provenance-by-tab.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { scanOwnerFacingForbiddenLanguage } from "./brand-explorer-public-visibility-quality-lock.js";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function stripHtmlForCopyScan(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function osGate({
  name,
  pass,
  sourceOfTruth,
  evidence = {},
  affectedRows = [],
  affectedTabs = [],
  requiredRemediation = null,
  founderJudgmentNeeded = false,
  codePatchNeeded = false,
  airtablePatchNeeded = false,
}) {
  return {
    name,
    pass: Boolean(pass),
    sourceOfTruth,
    evidence,
    affectedRows,
    affectedTabs,
    requiredRemediation: pass ? null : requiredRemediation,
    founderJudgmentNeeded: pass ? false : Boolean(founderJudgmentNeeded),
    codePatchNeeded: pass ? false : Boolean(codePatchNeeded),
    airtablePatchNeeded: pass ? false : Boolean(airtablePatchNeeded),
  };
}

function countGallery(blocks = []) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countOpenings(blocks = []) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
}

function isOwnerFacingPresentationRow(row = {}) {
  if (row.visible === false || row.active === false) return false;
  const ext = nz(row.externalDisplayStatus || row.external_display_status);
  if (/^do not display$/i.test(ext) || /^internal only$/i.test(ext)) return false;
  return true;
}

/** Collect trailing announcement URLs allowed on openings/momentum rows (PVQL contract). */
function collectAllowedAnnouncementUrls(rows = []) {
  const urls = new Set();
  for (const r of rows || []) {
    const slot = nz(r.slotKey);
    if (slot !== "footprint.momentum" && slot !== "footprint.openings") continue;
    const body = nz(r.body);
    if (!body) continue;
    const lines = body.split("\n").map((l) => l.trim()).filter(Boolean);
    const last = lines[lines.length - 1] || "";
    if (/^https?:\/\//i.test(last)) urls.add(last.replace(/[),.;]+$/g, ""));
    for (const m of body.matchAll(/https?:\/\/[^\s)]+/gi)) {
      // Only treat as allowed when the match is the trailing announcement line.
      if (last.startsWith(m[0])) urls.add(m[0].replace(/[),.;]+$/g, ""));
    }
  }
  return urls;
}

function filterAnnouncementUrlHits(hits = [], allowedUrls = new Set()) {
  if (!allowedUrls.size) return hits || [];
  return (hits || []).filter((h) => {
    if (h?.id !== "raw_url") return true;
    const snip = nz(h.snippet);
    if (!snip) return true;
    for (const u of allowedUrls) {
      if (u.includes(snip) || snip.includes(u) || u.startsWith(snip) || snip.startsWith(u.slice(0, 40))) {
        return false;
      }
    }
    return true;
  });
}

/**
 * Evaluate all OS gates for one brand.
 */
export function evaluateBrandExplorerOsGates({
  brandSlug,
  brandApi = null,
  brandConfig = null,
  brandBasics = null,
  presentationRows = [],
  registryAssets = [],
  assetPack = null,
  renderContract = null,
  completeBuildReport = null,
} = {}) {
  const blocks = presentationRows?.length
    ? presentationRows
    : brandApi?.brandExplorer?.blocks || [];

  const releaseGates = inventoryReleaseGates({
    brandSlug,
    brandApi,
    brandConfig,
    brandBasics,
    presentationRows: blocks,
    registryAssets,
    assetPack,
    renderContract,
    qualityLock: null,
    displayMeta: null,
  });

  const externalHtml = renderBrandExplorerHtmlForTest(brandApi || {}, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brandApi || {}, externalHtml, {
    brandSlug,
    brandBasics,
    renderContract,
  });

  // Live internal preview (no Presentation projection) — current shipped renderer
  const liveInternalHtml = renderBrandExplorerHtmlForTest(brandApi || {}, {
    allPanels: true,
    internalPreview: true,
  });
  const ownerFacingRows = (blocks || []).filter(isOwnerFacingPresentationRow);
  const allowedAnnouncementUrls = collectAllowedAnnouncementUrls(ownerFacingRows);
  const liveInternalHits = filterAnnouncementUrlHits(
    scanInternalPreviewOwnerCopy(stripHtmlForCopyScan(liveInternalHtml)),
    allowedAnnouncementUrls
  );

  const residualPlan = buildResidualOwnerCopyPatchPlan({
    brandSlug,
    presentationRows: ownerFacingRows,
  });
  // Slot-aware scan: trailing https on footprint.momentum / openings are required, not defects.
  const presentationForbidden = scanOwnerFacingForbiddenLanguage(ownerFacingRows);

  // Mandatory Tab Factory quality gates (every Brand Explorer setup before founder/active)
  const tabFactory = evaluateTabFactoryFromPayload({
    brand: brandApi || {},
    rows: blocks,
    html: externalHtml,
    brandSlug,
    brandConfig,
    registryAssets,
  });
  const sourceValidation = tabFactory.provenance ||
    evaluateBrandSpecificSourceValidation({
      brandSlug,
      brandConfig,
      registryAssets,
      presentationRows: blocks,
      brandApi,
    });
  const completeness = tabFactory.completeness ||
    evaluateRenderedFieldCompletenessFromPayload(brandApi || {}, blocks, externalHtml, brandSlug);
  const goldenQuality = tabFactory.golden ||
    evaluateGoldenContentQuality(brandApi || {}, blocks, externalHtml, { brandSlug });
  const emptyScan = tabFactory.emptyScan || scanNoEmptyRenderedComponents(externalHtml, { brandSlug });
  const provenance = tabFactory.provenance ||
    evaluateSourceProvenanceByTab({
      brandSlug,
      brandConfig,
      registryAssets,
      presentationRows: blocks,
      brandApi,
    });
  // Remediation / tab-factory gate: pass only when failFindings === 0
  const remediationPass = tabFactory.auditPass === true;
  const imageDistinctivenessPass = tabFactory.gates?.image_distinctiveness === true;
  const imageRoleMatchPass = tabFactory.gates?.image_role_match === true;
  const imageUniqueness = tabFactory.imageUniqueness || null;
  const imageRoleMatch = tabFactory.imageRoleMatch || null;

  const galleryCount = countGallery(blocks);
  const openingsCount = countOpenings(blocks);
  const brandExists = Boolean(brandApi?.id || brandApi?.name);
  const factoryConfigExists = Boolean(brandConfig);
  const sourceCoverageReady =
    (registryAssets || []).filter((a) => /approved|ready/i.test(nz(a.status || a.approvalStatus))).length >=
      3 ||
    (releaseGates.gates || []).find((g) => g.name === "source_library_coverage_ready")?.pass === true;

  const visualPackReady =
    ((releaseGates.gates || []).find((g) => g.name === "visual_asset_pack_ready")?.pass === true ||
      (galleryCount >= GALLERY_MIN && openingsCount >= PROPERTY_EXAMPLE_MIN)) &&
    imageDistinctivenessPass === true &&
    imageRoleMatchPass === true;

  const founderPass =
    brandApi?.founderVisualReviewPass === true ||
    brandBasics?.fields?.["Founder Visual Review Pass"] === true ||
    brandBasics?.["Founder Visual Review Pass"] === true;

  const activeApproved =
    brandApi?.readyForActiveProfile === true ||
    brandApi?.activeProfileApproved === true ||
    brandBasics?.fields?.["Active Profile Approved"] === true ||
    brandBasics?.["Active Profile Approved"] === true;

  const companyValidated =
    brandApi?.governance?.companyValidated === true ||
    brandBasics?.fields?.["Company Validated"] === true ||
    brandBasics?.["Company Validated"] === true;

  const reportReady =
    completeBuildReport?.summary?.readyForActiveProfile === true ||
    completeBuildReport?.readyForActiveProfile === true ||
    completeBuildReport?.overallStatus === "ready";

  const chromeForbidden = liveInternalHits.filter((h) =>
    ["fdd", "loi", "item_7", "item_19", "fee_stack", "net_contribution", "franchise_disclosure", "disclosure_document"].includes(
      h.id
    )
  );

  const gates = [
    osGate({
      name: "brand_record_exists",
      pass: brandExists,
      sourceOfTruth: "Brand Library API",
      evidence: { id: brandApi?.id || null, name: brandApi?.name || null },
      requiredRemediation: "Create or link Brand Basics record",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "factory_config_exists",
      pass: factoryConfigExists,
      sourceOfTruth: "active-profile / discovery brand config",
      evidence: { configSlug: brandConfig?.slug || brandConfig?.brandSlug || null },
      requiredRemediation: "Register brand in factory config",
      codePatchNeeded: true,
    }),
    osGate({
      name: "source_coverage_ready",
      pass: sourceCoverageReady,
      sourceOfTruth: "Source Library + Registry approval counts / release gate",
      evidence: { registryCount: (registryAssets || []).length },
      requiredRemediation: "Seed/approve Source Library + Registry assets",
      airtablePatchNeeded: true,
      affectedTabs: ["materials"],
    }),
    osGate({
      name: "visual_asset_pack_ready",
      pass: visualPackReady,
      sourceOfTruth: "asset-pack builder + live Presentation imageUrl + uniqueness",
      evidence: {
        galleryCount,
        openingsCount,
        galleryDistinctCount: imageUniqueness?.galleryDistinctCount ?? null,
        imageDistinctivenessPass,
      },
      requiredRemediation: "Materialize distinct gallery/property images into Presentation",
      airtablePatchNeeded: true,
      affectedTabs: ["materials", "footprint"],
    }),
    osGate({
      name: "gallery_six_imageurl",
      pass: galleryCount >= GALLERY_MIN && (imageUniqueness?.galleryDistinctCount ?? 0) >= GALLERY_MIN,
      sourceOfTruth: "Brand Library API brandExplorer.blocks imageUrl + uniqueness",
      evidence: {
        galleryCount,
        galleryDistinctCount: imageUniqueness?.galleryDistinctCount ?? null,
        required: GALLERY_MIN,
      },
      affectedRows: blocks
        .filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)))
        .map((b) => ({ recordId: b.recordId, slotKey: b.slotKey, hasImage: Boolean(nz(b.imageUrl)) })),
      affectedTabs: ["materials"],
      requiredRemediation: "Write 6 distinct Image URLs on materials.gallery.1–6 (no crop repeats)",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "property_examples_three_imageurl",
      pass:
        openingsCount >= PROPERTY_EXAMPLE_MIN &&
        (imageUniqueness?.propertyExampleDistinctCount ?? 0) >= PROPERTY_EXAMPLE_MIN,
      sourceOfTruth: "Brand Library API footprint.openings imageUrl + uniqueness",
      evidence: {
        openingsCount,
        propertyExampleDistinctCount: imageUniqueness?.propertyExampleDistinctCount ?? null,
        required: PROPERTY_EXAMPLE_MIN,
      },
      affectedRows: blocks
        .filter((b) => nz(b.slotKey) === "footprint.openings")
        .map((b) => ({ recordId: b.recordId, slotKey: b.slotKey, hasImage: Boolean(nz(b.imageUrl)) })),
      affectedTabs: ["footprint"],
      requiredRemediation: "Ensure ≥3 footprint.openings with distinct property imageUrls",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "required_tab_contract",
      pass: (releaseGates.gates || []).find((g) => g.name === "render_contract_pass")?.pass !== false ||
        (renderContract?.summary?.pass !== false && galleryCount >= 3),
      sourceOfTruth: "render readiness contract",
      evidence: { renderContractStatus: renderContract?.summary?.status || null },
      requiredRemediation: "Fix render contract gaps",
      airtablePatchNeeded: true,
      codePatchNeeded: false,
    }),
    osGate({
      name: "no_fallback_sections",
      pass: !externalQl.helperTextFound && !/Scenario cards will appear|Slots materials\.gallery/i.test(externalHtml),
      sourceOfTruth: "external DOM quality lock",
      evidence: { helperTextFound: externalQl.helperTextFound === true },
      affectedTabs: ["overview", "materials"],
      requiredRemediation: "Suppress incomplete fallbacks / complete Presentation",
      codePatchNeeded: true,
    }),
    osGate({
      name: "no_empty_owner_facing_cards",
      pass: (externalQl.emptyCardsFound || 0) === 0 || externalQl.profileInPreparationRendered === true,
      sourceOfTruth: "external DOM scan",
      evidence: { emptyCardsFound: externalQl.emptyCardsFound || 0 },
      requiredRemediation: "Fill or hide empty owner-facing cards",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "no_visible_source_urls",
      pass: !liveInternalHits.some((h) => h.id === "raw_url") && !presentationForbidden.some((h) => h.id === "raw_url"),
      sourceOfTruth: "internal preview DOM + Presentation corpus",
      evidence: {
        liveUrlHits: liveInternalHits.filter((h) => h.id === "raw_url").length,
        presentationUrlHits: presentationForbidden.filter((h) => h.id === "raw_url").length,
      },
      requiredRemediation: "Strip raw URLs from Presentation Body / footnotes",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "no_forbidden_copy_presentation",
      pass: presentationForbidden.length === 0,
      sourceOfTruth: "Presentation Title/Body/Case Summary fields",
      evidence: { hits: presentationForbidden.slice(0, 20) },
      affectedRows: residualPlan.patches.slice(0, 30).map((p) => ({
        recordId: p.recordId,
        slotKey: p.slotKey,
        field: p.field,
      })),
      requiredRemediation: "Apply v40C residual Presentation scrub",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "internal_preview_owner_copy_live",
      pass: liveInternalHits.length === 0,
      sourceOfTruth: "internal preview renderer (?beInternalPreview=1) live DOM",
      evidence: { hits: liveInternalHits },
      requiredRemediation: "Fix renderer chrome and/or Presentation residual",
      codePatchNeeded: chromeForbidden.length > 0,
      airtablePatchNeeded: residualPlan.summary.patchCount > 0,
    }),
    osGate({
      name: "external_quality_lock",
      pass: externalQl.profileInPreparationRendered === true || externalQl.externalQualityLockPass === true,
      sourceOfTruth: "external DOM quality lock (Profile in Preparation when not approved)",
      evidence: {
        profileInPreparationRendered: externalQl.profileInPreparationRendered === true,
        tabsRendered: (externalQl.tabsRenderedExternally || []).length,
        forbiddenStringsFound: externalQl.forbiddenStringsFound,
      },
      requiredRemediation: "Keep incomplete/unapproved brands locked externally",
      codePatchNeeded: true,
    }),
    osGate({
      name: "renderer_chrome_clean",
      pass: chromeForbidden.length === 0,
      sourceOfTruth: "internal preview economics/loyalty chrome DOM",
      evidence: { chromeHits: chromeForbidden },
      affectedTabs: ["economics", "loyalty", "standards"],
      requiredRemediation: "Patch atelier economics chrome",
      codePatchNeeded: true,
    }),
    osGate({
      name: "residual_presentation_clean",
      pass: residualPlan.summary.patchCount === 0,
      sourceOfTruth: "v40C residual scrubber vs live Presentation rows",
      evidence: { patchCount: residualPlan.summary.patchCount, unsafeCount: residualPlan.summary.unsafeCount },
      requiredRemediation: "Apply v40C residual Presentation patches",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "brand_specific_source_validation",
      pass: sourceValidation.pass === true,
      sourceOfTruth: "brand-specific official domains + source hierarchy",
      evidence: {
        requiredBrandDomains: sourceValidation.requiredBrandDomains,
        missingRequiredBrandDomains: sourceValidation.missingRequiredBrandDomains,
        classificationCounts: sourceValidation.classificationCounts,
        failures: sourceValidation.failures,
      },
      requiredRemediation:
        "Add canonical brand-specific official sources; do not rely on parent-company umbrella pages for brand copy",
      airtablePatchNeeded: true,
      codePatchNeeded: true,
    }),
    osGate({
      name: "source_provenance_by_tab",
      pass: provenance.pass === true,
      sourceOfTruth: "source provenance by tab (brand-specific vs parent classification)",
      evidence: {
        pass: provenance.pass === true,
        failures: provenance.failures || [],
        overall: provenance.overall || null,
      },
      requiredRemediation: `npm run brand-explorer-source-provenance-by-tab -- --brands ${brandSlug} --dry-run`,
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "tab_factory_audit",
      pass: tabFactory.auditPass === true,
      sourceOfTruth: "tab factory field-by-field audit (live rendered payload)",
      evidence: {
        auditComplete: tabFactory.auditComplete,
        patchPlanComplete: tabFactory.patchPlanComplete,
        auditPass: tabFactory.auditPass,
        failFindings: tabFactory.failFindings,
        emptyRenderFailFindings: tabFactory.emptyRenderFailFindings,
      },
      requiredRemediation: `npm run brand-explorer-tab-factory-remediation -- --brands ${brandSlug} --dry-run`,
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "rendered_field_completeness_audit",
      pass: completeness.auditComplete === true && completeness.auditPass === true,
      sourceOfTruth: "rendered field-by-field completeness audit (live payload + atelier HTML)",
      evidence: {
        auditComplete: completeness.auditComplete,
        patchPlanComplete: completeness.patchPlanComplete,
        auditPass: completeness.auditPass,
        failFindings: completeness.failFindings,
        releaseQualityDecision: completeness.releaseQualityDecision,
      },
      requiredRemediation: "Run tab-factory / rendered-field remediation until failFindings = 0",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "rendered_field_completeness_remediation",
      pass: remediationPass,
      sourceOfTruth: "post-remediation rendered payload (auditPass = failFindings === 0)",
      evidence: {
        auditPass: tabFactory.auditPass,
        patchPlanComplete: tabFactory.patchPlanComplete,
        failFindings: tabFactory.failFindings,
      },
      requiredRemediation: `npm run brand-explorer-tab-factory-remediation -- --brands ${brandSlug} --dry-run`,
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "no_empty_rendered_components",
      pass: emptyScan.pass === true,
      sourceOfTruth: "rendered HTML empty-shell scanner",
      evidence: {
        failFindings: emptyScan.failFindings,
        findings: (emptyScan.findings || []).slice(0, 20),
      },
      requiredRemediation: "Fill or suppress empty cards/bars/phases/chips before founder review",
      airtablePatchNeeded: true,
      codePatchNeeded: true,
    }),
    osGate({
      name: "image_distinctiveness",
      pass: imageDistinctivenessPass,
      sourceOfTruth: "image uniqueness (gallery≥6 distinct, scenario≥3, property≥3, no near-dupes)",
      evidence: {
        pass: imageDistinctivenessPass,
        galleryDistinctCount: imageUniqueness?.galleryDistinctCount ?? null,
        scenarioDistinctCount: imageUniqueness?.scenarioDistinctCount ?? null,
        propertyExampleDistinctCount: imageUniqueness?.propertyExampleDistinctCount ?? null,
        duplicateGroups: (imageUniqueness?.duplicateGroups || []).length,
      },
      requiredRemediation:
        "npm run brand-explorer-image-uniqueness-audit -- --brands <slug> --dry-run then image remediation",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "image_role_match",
      pass: imageRoleMatchPass,
      sourceOfTruth: "image role-match (captions must match Accor/SLH/metadata visual category)",
      evidence: {
        pass: imageRoleMatchPass,
        unresolvedRoleMismatchCount: imageRoleMatch?.unresolvedRoleMismatchCount ?? null,
        ambiguousCount: imageRoleMatch?.ambiguousCount ?? null,
      },
      requiredRemediation:
        "npm run brand-explorer-image-role-match-audit -- --brands <slug> --dry-run then role-match remediation",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "golden_content_quality",
      pass: goldenQuality.pass === true,
      sourceOfTruth: "golden-content-quality test (depth, non-generic, brand-specific)",
      evidence: {
        pass: goldenQuality.pass === true,
        failures: (goldenQuality.failures || []).slice(0, 20),
      },
      requiredRemediation: "Run live golden-quality rebuild / deepen brand-specific copy",
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "founder_visual_review_passed",
      pass: founderPass === true,
      sourceOfTruth: "Brand Basics Founder Visual Review Pass",
      evidence: { founderVisualReviewPass: founderPass === true },
      requiredRemediation: "Founder visual review decision",
      founderJudgmentNeeded: true,
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "active_release_approval_set",
      pass: activeApproved === true,
      sourceOfTruth: "Brand Basics Active Profile Approved / readyForActiveProfile",
      evidence: { activeProfileApproved: activeApproved === true },
      requiredRemediation: "Set active release approval only after founder pass",
      founderJudgmentNeeded: true,
      airtablePatchNeeded: true,
    }),
    osGate({
      name: "company_validated_untouched_or_true",
      pass: true,
      sourceOfTruth: "Brand Basics Company Validated (must not be claimed by OS)",
      evidence: { companyValidated: companyValidated === true, osWillNotWrite: true },
      requiredRemediation: null,
    }),
  ];

  const failedGates = gates.filter((g) => !g.pass).map((g) => g.name);

  // True blockers: must be fixed before founder/active path (exclude expected later gates)
  const laterGates = new Set(["founder_visual_review_passed", "active_release_approval_set"]);
  const trueBlockers = failedGates.filter((name) => !laterGates.has(name));

  const falseBlockerNames = [];
  if (reportReady && (failedGates.includes("external_quality_lock") || !activeApproved)) {
    falseBlockerNames.push("report_ready_but_active_approval_or_external_lock");
  }
  if (brandApi?.readyForActiveProfile === true && !activeApproved) {
    falseBlockerNames.push("api_readyForActiveProfile_without_approval_field");
  }

  const metrics = {
    galleryCount,
    openingsCount,
    galleryReady: galleryCount >= GALLERY_MIN,
    propertyExamplesReady: openingsCount >= PROPERTY_EXAMPLE_MIN,
    brandExists,
    factoryConfigExists,
    sourceCoverageReady,
    visualPackReady,
    liveInternalPreviewClean: liveInternalHits.length === 0,
    residualPresentationDirty: residualPlan.summary.patchCount > 0,
    externalQualityLockPass:
      externalQl.profileInPreparationRendered === true || externalQl.externalQualityLockPass === true,
    externalFullProfileRendered: (externalQl.tabsRenderedExternally || []).length >= 5,
    founderVisualReviewPassed: founderPass === true,
    activeReleaseApproved: activeApproved === true,
    companyValidated: companyValidated === true,
    reportReady: Boolean(reportReady),
    reportReadyButLiveBlocked: false,
    brandSpecificSourceValidationPass: sourceValidation.pass === true,
    renderedFieldCompletenessPass: completeness.auditPass === true,
    renderedFieldCompletenessPatchPlanComplete: completeness.patchPlanComplete === true,
    renderedFieldCompletenessFailFindings: completeness.failFindings || 0,
    goldenContentQualityPass: goldenQuality.pass === true,
    tabFactoryAuditPass: tabFactory.auditPass === true,
    sourceProvenanceByTabPass: provenance.pass === true,
    noEmptyRenderedComponentsPass: emptyScan.pass === true,
    imageDistinctivenessPass,
    imageRoleMatchPass,
  };
  // Report-ready vs missing live visuals is a true conflict.
  // Dirty Presentation / internal preview with a stale "ready" report is remediation, not conflict.
  metrics.reportReadyButLiveBlocked =
    Boolean(metrics.reportReady) && (!metrics.galleryReady || !metrics.propertyExamplesReady);

  return {
    brandSlug,
    gates,
    failedGates,
    trueBlockers,
    falseBlockers: falseBlockerNames,
    releaseGateInventory: releaseGates,
    residualPlan,
    liveInternalHits,
    presentationForbidden,
    externalQl,
    sourceValidation,
    renderedFieldCompleteness: {
      auditComplete: completeness.auditComplete,
      patchPlanComplete: completeness.patchPlanComplete,
      auditPass: completeness.auditPass,
      failFindings: completeness.failFindings,
      releaseQualityDecision: completeness.releaseQualityDecision,
    },
    tabFactory: {
      auditComplete: tabFactory.auditComplete,
      patchPlanComplete: tabFactory.patchPlanComplete,
      auditPass: tabFactory.auditPass,
      failFindings: tabFactory.failFindings,
      emptyRenderFailFindings: tabFactory.emptyRenderFailFindings,
      gates: tabFactory.gates,
    },
    provenance,
    emptyScan,
    goldenQuality: {
      pass: goldenQuality.pass === true,
      failures: goldenQuality.failures || [],
    },
    metrics,
  };
}
