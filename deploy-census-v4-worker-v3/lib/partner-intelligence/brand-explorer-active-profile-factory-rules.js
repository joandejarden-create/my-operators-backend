/**
 * Brand Explorer Active Profile Factory Rules v34.
 *
 * Shared factory gates consolidated from Everhome (v32*) and WoodSpring (v33*).
 * Used by preflight, asset-pack, founder-review, and apply stages.
 */
import {
  ACTIVE_PROFILE_GALLERY_MINIMUM,
  countVisibleGalleryBlocksWithImageUrl,
  detectBrandAssetImageGovernanceDefects,
  findRegistryAssetForPresentationRow,
  getDiscoveryBrandConfig,
  isGalleryImageSlot,
  isRegistryAssetApprovedForExplorer,
  isVisualImageSlot,
} from "./brand-explorer-brand-asset-image-governance.js";

export { ACTIVE_PROFILE_GALLERY_MINIMUM };
import {
  classifyPropertyExampleImage,
  isGenericBrandOrLifestyleImageUrl,
  isLogoImageUrl,
  isOfficialLifestylePropertyImageUrl,
  isPropertyExampleTitle,
} from "./brand-explorer-footprint-opening-image-governance.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { propertyExampleTitlePassesGovernance } from "./brand-explorer-cala-property-example-rules.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { evaluateBrandSpecificSourceValidation } from "./brand-explorer-brand-specific-source-validation.js";
import { evaluateRenderedFieldCompletenessFromPayload } from "./brand-explorer-rendered-field-completeness-evaluate.js";
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateAiAssistedProfileFootnoteGate } from "./brand-explorer-ai-assisted-footnote.js";

export const FACTORY_VERSION = "v34D";

export const FACTORY_SUPPORTED_SLUGS = Object.freeze([
  "woodspring-suites",
  "everhome-suites",
  "suburban-studios",
  "design-hotels",
  "small-luxury-hotels-of-the-world",
  "autograph-collection",
  "tribute-portfolio",
  "vignette-collection",
  "mgallery-collection",
  "hotel-indigo",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
]);

export const ATELIER_SCENARIO_FALLBACK_TITLES = Object.freeze([
  "Urban Repositioning",
  "Leisure-Forward Conversions",
  "Boutique Resort Adjacency",
]);

export const ATELIER_PROOF_FALLBACK_HEADS = Object.freeze([
  "Global Open Footprint",
  "Pipeline Depth",
  "Conversion-Led Growth",
  "Multi-Region Relevance",
  "Loyalty Program Scale",
  "Operator-Enabled Execution",
]);

export const FACTORY_GUARD_FLAGS = Object.freeze({
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noSummaryUrl: "--confirm-no-summary-url-field",
  brandOnly: "--confirm-brand-only",
  founderVisualReview: "--founder-approved-active-profile-visual-review",
  officialImagesOnly: "--confirm-official-source-images-only",
  minimumSixGallery: "--confirm-minimum-six-visible-gallery-images",
  propertyExamplesHaveHotelImages: "--confirm-property-examples-have-hotel-images",
  noLogoLifestylePropertyImages: "--confirm-no-logo-lifestyle-property-images",
  standardDetailGovernance: "--confirm-standard-detail-governance-reviewed",
  approveActiveProfile: "--approve-brand-explorer-active-profile",
  approveDraft: "--approve-brand-explorer-active-profile-draft",
  approveCopyGovernance: "--approve-brand-explorer-active-profile-copy-governance",
  confirmFounderVisualReviewPassed: "--confirm-founder-visual-review-passed",
});

export const COPY_SAFETY_PATTERNS = Object.freeze([
  { id: "fdd", re: /\bfdd\b|\bfranchise disclosure\b/i, severity: "high" },
  { id: "item_19", re: /\bitem\s*19\b/i, severity: "high" },
  { id: "confirm_fees", re: /\bconfirm fees?\b|\bconfirm flag\b/i, severity: "medium" },
  { id: "adr", re: /\badr\b/i, severity: "medium" },
  { id: "net_contribution", re: /\bnet contribution\b/i, severity: "high" },
  { id: "performance_rep", re: /\bperformance representation\b|\bguaranteed returns?\b/i, severity: "high" },
  { id: "pipeline_depth", re: /\bpipeline depth\b/i, severity: "medium" },
  { id: "rooms_from_loyalty", re: /\brooms from loyalty\b/i, severity: "medium" },
  { id: "source_metadata", re: /\bconsumer site\b|\bactive property page\b|\bmetadata\b|\bsource capture\b/i, severity: "medium" },
  { id: "revpar", re: /\brevpar\b/i, severity: "medium" },
  { id: "fee_stack", re: /\bfee stack\b/i, severity: "medium" },
  { id: "estimated_contribution", re: /\bestimated contribution\b/i, severity: "high" },
  { id: "booking_path", re: /\bbooking path\b/i, severity: "medium" },
  { id: "franchise_disclosure_long", re: /\bfranchise disclosure document\b/i, severity: "high" },
]);

const HIDE_DISPLAY = /^(do not display|internal only)$/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  return nz(v).length > 0;
}

function apiBlockForSlot(brandApi, slotKey) {
  const blocks = brandApi?.brandExplorer?.blocks || [];
  return blocks.find((b) => nz(b?.slotKey) === slotKey) || null;
}

function apiBlocksMatching(brandApi, pattern) {
  const blocks = brandApi?.brandExplorer?.blocks || [];
  return blocks.filter((b) => pattern.test(nz(b?.slotKey)));
}

export function resolveFactoryBrand(brandArg, brandConfig = null) {
  const slug = nz(brandArg).toLowerCase();
  const config = brandConfig || getDiscoveryBrandConfig(slug);
  if (!config) {
    throw new Error(`No discovery brand config for factory brand: ${brandArg}`);
  }
  return {
    slug: config.slug,
    recordId: config.recordId,
    name: config.name,
    parentCompany: config.parentCompany || "Choice Hotels International",
    consumerUrl: config.consumerUrl || "",
  };
}

export function isHiddenPresentationRow(row) {
  return HIDE_DISPLAY.test(nz(row?.externalDisplayStatus));
}

export function evaluateGalleryRule(brandApi, { minimum = ACTIVE_PROFILE_GALLERY_MINIMUM } = {}) {
  const blocks = brandApi?.brandExplorer?.blocks || [];
  const galleryBlocks = blocks.filter((b) => isGalleryImageSlot(b?.slotKey));
  const withImageUrl = galleryBlocks.filter((b) => hasVal(b?.imageUrl));
  const missingImageUrl = galleryBlocks.filter((b) => !hasVal(b?.imageUrl));
  const logoOrGeneric = withImageUrl.filter((b) => {
    const cls = classifyPropertyExampleImage(b.imageUrl);
    return cls.isLogo || cls.isGenericBrand || cls.isLifestyle;
  });

  const pass =
    withImageUrl.length >= minimum &&
    missingImageUrl.length === 0 &&
    logoOrGeneric.length === 0;

  return {
    ruleId: "gallery_minimum_six_visible",
    pass,
    minimum,
    visibleInApi: galleryBlocks.length,
    withImageUrl: withImageUrl.length,
    missingImageUrl: missingImageUrl.map((b) => b.slotKey),
    logoOrGenericSlots: logoOrGeneric.map((b) => b.slotKey),
    blockers: [
      ...(withImageUrl.length < minimum
        ? [`need_${minimum}_visible_gallery_imageUrl_got_${withImageUrl.length}`]
        : []),
      ...missingImageUrl.map((b) => `missing_imageUrl:${b.slotKey}`),
      ...logoOrGeneric.map((b) => `logo_or_generic:${b.slotKey}`),
    ],
  };
}

export function evaluatePropertyExampleRule(brandApi, registryAssets = [], brandConfig = null) {
  const openingBlocks = apiBlocksMatching(brandApi, /^footprint\.openings$/);
  const defects = [];
  const minimum = brandConfig?.propertyExampleMinimum || 3;
  const maximum = brandConfig?.propertyExampleMaximum || minimum;

  if (openingBlocks.length > maximum) {
    defects.push({
      slotKey: "footprint.openings",
      issue: "too_many_visible_openings",
      count: openingBlocks.length,
    });
  }
  if (openingBlocks.length < minimum) {
    defects.push({
      slotKey: "footprint.openings",
      issue: "too_few_visible_openings",
      count: openingBlocks.length,
    });
  }

  for (const block of openingBlocks) {
    const registry = findRegistryAssetForPresentationRow(registryAssets, block);
    const cls = classifyPropertyExampleImage(block.imageUrl, {
      registrySourceUrl: registry?.sourceUrl || "",
      registryNotes: [registry?.sourceNotes, registry?.reviewNotes].filter(Boolean).join("\n"),
    });
    const isPropertyExample = isPropertyExampleTitle(block.title) || /property example/i.test(block.title || "");
    if (!hasVal(block.imageUrl)) {
      defects.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "missing_imageUrl" });
    } else if (cls.isLogo) {
      defects.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "logo_image" });
    } else if (cls.isGenericBrand || cls.isLifestyle) {
      defects.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "generic_or_lifestyle" });
    } else if (!cls.isHotelPhotography && isPropertyExample) {
      const officialRegistrySource =
        registry &&
        (isOfficialLifestylePropertyImageUrl(registry.sourceUrl) ||
          isOfficialLifestylePropertyImageUrl(registry.sourcePageUrl));
      if (!officialRegistrySource) {
        defects.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "non_hotel_photography" });
      }
    }
    if (isPropertyExample && !propertyExampleTitlePassesGovernance(block.title, brandConfig)) {
      defects.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "property_label_unclear" });
    }
  }

  return {
    ruleId: "property_example_real_hotel_images",
    pass: openingBlocks.length >= minimum && openingBlocks.length <= maximum && defects.length === 0,
    visibleOpeningCards: openingBlocks.length,
    expectedOpeningCards: minimum,
    defects,
    blockers: defects.map((d) => `${d.issue}:${d.recordId || d.slotKey}`),
  };
}

export function evaluateScenarioImageRule(brandApi) {
  const scenarioBlocks = [1, 2, 3]
    .map((i) => apiBlockForSlot(brandApi, `overview.scenario.${i}`))
    .filter(Boolean);

  const placeholderCards = scenarioBlocks.filter((b) => !hasVal(b.imageUrl));
  const staleFallbackRisk = scenarioBlocks.some((b) =>
    ATELIER_SCENARIO_FALLBACK_TITLES.some((t) => nz(b.title).toLowerCase() === t.toLowerCase())
  );

  return {
    ruleId: "scenario_no_image_placeholder",
    pass: placeholderCards.length === 0 && scenarioBlocks.length > 0,
    visibleScenarioCards: scenarioBlocks.length,
    placeholderSlots: placeholderCards.map((b) => b.slotKey),
    staleFallbackTitles: staleFallbackRisk,
    blockers: [
      ...placeholderCards.map((b) => `scenario_image_placeholder:${b.slotKey}`),
      ...(scenarioBlocks.length === 0 ? ["no_visible_scenario_cards_in_api"] : []),
    ],
  };
}

export function evaluateRegistryTraceabilityRule(brandApi, registryAssets = []) {
  const visualBlocks = (brandApi?.brandExplorer?.blocks || []).filter((b) =>
    isVisualImageSlot(b?.slotKey)
  );
  const gaps = [];

  for (const block of visualBlocks) {
    if (!hasVal(block.imageUrl)) {
      gaps.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "missing_api_imageUrl" });
      continue;
    }
    const registry = findRegistryAssetForPresentationRow(registryAssets, block);
    if (!registry) {
      gaps.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "missing_registry_row" });
      continue;
    }
    if (!isRegistryAssetApprovedForExplorer(registry)) {
      gaps.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "registry_not_approved" });
    }
    if (!hasVal(registry.sourceUrl) || isTemporaryAirtableUrl(registry.sourceUrl)) {
      gaps.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "missing_durable_source_url" });
    }
    if (!hasVal(registry.sourcePageUrl)) {
      gaps.push({ slotKey: block.slotKey, recordId: block.recordId, issue: "missing_source_page_url" });
    }
  }

  return {
    ruleId: "registry_traceability",
    pass: gaps.length === 0,
    visualSlotsChecked: visualBlocks.length,
    gaps,
    blockers: gaps.map((g) => `${g.issue}:${g.slotKey}`),
  };
}

export function evaluateUiFallbackRisk(brandApi) {
  const risks = [];
  for (let i = 1; i <= 3; i += 1) {
    const block = apiBlockForSlot(brandApi, `overview.scenario.${i}`);
    if (!block) {
      risks.push({
        surface: `overview.scenario.${i}`,
        issue: "atelier_hardcoded_scenario_fallback_risk",
        detail: "No API block — atelier may render Urban/Boutique defaults",
      });
    } else if (
      ATELIER_SCENARIO_FALLBACK_TITLES.some((t) => nz(block.title).toLowerCase() === t.toLowerCase())
    ) {
      risks.push({
        surface: block.slotKey,
        issue: "stale_scenario_fallback_title",
        detail: block.title,
      });
    }
  }

  const proofBlocks = apiBlocksMatching(brandApi, /^overview\.proof\.\d+$/);
  if (proofBlocks.length > 0) {
    for (const block of proofBlocks) {
      if (
        ATELIER_PROOF_FALLBACK_HEADS.some((h) => nz(block.title).toLowerCase() === h.toLowerCase())
      ) {
        risks.push({
          surface: block.slotKey,
          issue: "stale_proof_fallback_title",
          detail: block.title,
        });
      }
    }
  }

  return {
    ruleId: "ui_fallback_preference",
    pass: risks.length === 0,
    risks,
    blockers: risks.map((r) => `${r.issue}:${r.surface}`),
  };
}

export function evaluateCopySafetyRule(presentationRows = [], brandApi = null) {
  const findings = [];
  const rows = presentationRows.filter((r) => r.visible !== false && !isHiddenPresentationRow(r));

  for (const row of rows) {
    const text = `${row.title}\n${row.body}`;
    const safetyIds = scanCopySafety(text);
    for (const id of safetyIds) {
      findings.push({
        slotKey: row.slotKey,
        recordId: row.recordId,
        patternId: id,
        excerpt: text.slice(0, 120),
        severity: id === "fdd" || id === "item_19" ? "high" : "medium",
      });
    }
    for (const pat of COPY_SAFETY_PATTERNS) {
      if (pat.re.test(text)) {
        findings.push({
          slotKey: row.slotKey,
          recordId: row.recordId,
          patternId: pat.id,
          excerpt: text.slice(0, 120),
          severity: pat.severity,
        });
      }
    }
  }

  const high = findings.filter((f) => f.severity === "high" || f.severity === "critical");
  return {
    ruleId: "copy_safety",
    pass: high.length === 0,
    findings,
    highCount: high.length,
    blockers: high.map((f) => `risky_copy:${f.patternId}:${f.slotKey}`),
  };
}

export function evaluateStandardDetailGovernanceRule(contractReport = null, completeBuildReport = null) {
  const section = (contractReport?.sections || []).find(
    (s) => /standard detail/i.test(s.section || s.name || "")
  );
  const needsFounder =
    section?.status === "blocked" ||
    section?.needsFounderReview ||
    (completeBuildReport?.blockers || []).some((b) =>
      /standard detail/i.test(typeof b === "string" ? b : b.message || b.section || "")
    );

  return {
    ruleId: "standard_detail_governance",
    pass: !needsFounder,
    sectionStatus: section?.status || "unknown",
    needsFounderReview: Boolean(needsFounder),
    blockers: needsFounder ? ["standard_detail_governance_not_cleared"] : [],
  };
}

export function evaluateAllFactoryRules({
  brandApi,
  presentationRows = [],
  registryAssets = [],
  brandConfig = null,
  brandTarget = null,
  contractReport = null,
  completeBuildReport = null,
}) {
  const imageGovDefects = brandConfig
    ? detectBrandAssetImageGovernanceDefects(brandApi, registryAssets, brandConfig, brandTarget)
    : [];

  const brandSlug = brandConfig?.slug || brandApi?.slug || brandTarget?.slug || "";
  const html = renderBrandExplorerHtmlForTest(brandApi || {}, {
    allPanels: true,
    internalPreview: false,
  });
  const sourceValidation = evaluateBrandSpecificSourceValidation({
    brandSlug,
    brandConfig,
    registryAssets,
    presentationRows,
    brandApi,
  });
  const completeness = evaluateRenderedFieldCompletenessFromPayload(
    brandApi || {},
    presentationRows,
    html,
    brandSlug
  );
  const goldenQuality = evaluateGoldenContentQuality(brandApi || {}, presentationRows, html, {
    brandSlug,
  });
  const footnoteGate = evaluateAiAssistedProfileFootnoteGate(brandApi || {}, "");

  const rules = {
    gallery: evaluateGalleryRule(brandApi),
    propertyExamples: evaluatePropertyExampleRule(brandApi, registryAssets, brandConfig),
    scenarioImages: evaluateScenarioImageRule(brandApi),
    registryTraceability: evaluateRegistryTraceabilityRule(brandApi, registryAssets),
    uiFallback: evaluateUiFallbackRisk(brandApi),
    copySafety: evaluateCopySafetyRule(presentationRows, brandApi),
    externalOwnerReadiness: evaluateExternalOwnerReadinessRule(presentationRows),
    standardDetail: evaluateStandardDetailGovernanceRule(contractReport, completeBuildReport),
    aiAssistedProfileFootnoteVisible: {
      pass: footnoteGate.pass === true,
      blockers: footnoteGate.pass
        ? []
        : (footnoteGate.failures || []).map((f) => `ai_assisted_profile_footnote_visible:${f}`),
      evidence: {
        displayLabel: footnoteGate.displayLabel,
        displaySubtitle: footnoteGate.displaySubtitle,
        failures: footnoteGate.failures,
      },
    },
    brandSpecificSourceValidation: {
      pass: sourceValidation.pass === true,
      blockers: sourceValidation.pass ? [] : sourceValidation.failures.map((f) => `source_validation:${f}`),
      evidence: {
        missingRequiredBrandDomains: sourceValidation.missingRequiredBrandDomains,
        classificationCounts: sourceValidation.classificationCounts,
      },
    },
    renderedFieldCompleteness: {
      pass: completeness.auditPass === true,
      blockers: completeness.auditPass
        ? []
        : [
            `rendered_field_completeness:failFindings=${completeness.failFindings}`,
            ...(completeness.patchPlanComplete
              ? ["rendered_field_completeness:remediation_required"]
              : ["rendered_field_completeness:patch_plan_incomplete"]),
          ],
      evidence: {
        auditComplete: completeness.auditComplete,
        patchPlanComplete: completeness.patchPlanComplete,
        auditPass: completeness.auditPass,
        failFindings: completeness.failFindings,
        releaseQualityDecision: completeness.releaseQualityDecision,
      },
    },
    goldenContentQuality: {
      pass: goldenQuality.pass === true,
      blockers: goldenQuality.pass
        ? []
        : (goldenQuality.failures || []).map((f) => `golden_content_quality:${f}`),
      evidence: { failures: goldenQuality.failures || [] },
    },
  };

  const blockers = [
    ...Object.values(rules).flatMap((r) => r.blockers || []),
    ...imageGovDefects
      .filter((d) => d.severity === "critical" || d.severity === "high")
      .map((d) => `image_governance:${d.type}:${d.slotKey}`),
  ];

  const pass = blockers.length === 0;

  return {
    factoryVersion: FACTORY_VERSION,
    pass,
    rules,
    imageGovernanceDefects: imageGovDefects,
    blockers: [...new Set(blockers)],
    galleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
    visibleGalleryCount: countVisibleGalleryBlocksWithImageUrl(brandApi),
    mandatoryReleaseGates: {
      brandSpecificSourceValidation: sourceValidation.pass === true,
      renderedFieldCompleteness: completeness.auditPass === true,
      goldenContentQuality: goldenQuality.pass === true,
      aiAssistedProfileFootnoteVisible: footnoteGate.pass === true,
      companyValidatedUntouched: true,
    },
  };
}

export function factoryGuardrailsSummary() {
  return {
    companyValidatedNeverAuto: true,
    imageApprovalNotCompanyValidation: true,
    noSummaryUrlWrites: true,
    registryOnlyNotVisibleReadiness: true,
    founderVisualReviewRequiredForApply: true,
    stagedApplyWorkflow: "apply-draft → founder-review → apply-approved",
    galleryMinimum: ACTIVE_PROFILE_GALLERY_MINIMUM,
    mandatoryBeforeFounderOrActive: [
      "brand-specific-source-validation",
      "rendered-field-completeness-audit",
      "rendered-field-completeness-remediation",
      "golden-content-quality",
      "ai_assisted_profile_footnote_visible",
    ],
    auditPassRequiresZeroFailFindings: true,
  };
}

export function isUnsafeImageUrl(url) {
  const u = nz(url);
  if (!u) return true;
  if (isLogoImageUrl(u)) return true;
  if (isGenericBrandOrLifestyleImageUrl(u)) return true;
  return false;
}
