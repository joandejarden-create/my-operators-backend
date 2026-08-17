/**
 * Sync Tab Factory evaluation (no factory / Airtable loaders).
 * Safe for OS gate evaluator imports.
 */
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { scanNoEmptyRenderedComponents } from "./brand-explorer-no-empty-rendered-components.js";
import { evaluateRenderedFieldCompletenessFromPayload } from "./brand-explorer-rendered-field-completeness-evaluate.js";
import { evaluateSourceProvenanceByTab } from "./brand-explorer-source-provenance-by-tab.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { evaluateSectionPatternParity } from "./brand-explorer-section-pattern-parity.js";
import { evaluateRecentMomentumEvidenceQuality } from "./brand-explorer-recent-momentum-evidence-quality.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "./brand-explorer-lane2-property-catalog.js";
import { CALA_AVAILABLE_BY_SLUG } from "./brand-explorer-27-recent-momentum-evidence-fix-content.js";

/**
 * Sync tab-factory evaluation from loaded payload.
 */
export function evaluateTabFactoryFromPayload({
  brand,
  rows = [],
  html = "",
  brandSlug = null,
  brandConfig = null,
  registryAssets = [],
} = {}) {
  const slug = brandSlug || brand?.slug || "";
  const completeness = evaluateRenderedFieldCompletenessFromPayload(brand, rows, html, slug);
  const emptyScan = scanNoEmptyRenderedComponents(html, { brandSlug: slug });
  const provenance = evaluateSourceProvenanceByTab({
    brandSlug: slug,
    brandConfig,
    registryAssets,
    presentationRows: rows,
    brandApi: brand,
  });
  const golden = evaluateGoldenContentQuality(brand, rows, html, { brandSlug: slug });
  const imageUniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: rows,
    brandSlug: slug,
  });
  const imageRoleMatch = evaluateBrandImageRoleMatch({
    presentationRows: rows,
    brandSlug: slug,
  });

  const fieldFails = (completeness.findings || []).filter((f) => f.status !== "pass");
  const imageDistinctivenessPass =
    imageUniqueness.pass === true &&
    !(golden.failures || []).some((f) => /duplicate_scenario/i.test(f)) &&
    !(completeness.findings || []).some((f) => f.status === "duplicate");
  const imageRoleMatchPass = imageRoleMatch.pass === true;
  const sectionPatternParity = evaluateSectionPatternParity({
    brandSlug: slug,
    brandName: brand?.name || slug,
    presentationRows: rows,
    html,
  });
  const sectionPatternParityPass = sectionPatternParity.pass === true;

  const momentumEvidence = evaluateRecentMomentumEvidenceQuality({
    brandSlug: slug,
    brandName: brand?.name || slug,
    presentationRows: rows,
    html,
    propertyCatalog: LANE2_PROPERTY_CATALOG_BY_SLUG[slug] || [],
    calaAvailableOverride:
      CALA_AVAILABLE_BY_SLUG[slug] != null ? CALA_AVAILABLE_BY_SLUG[slug] : null,
  });
  // Full evidence-quality gate is binding for 27-wave brands; other brands keep section_pattern_parity.
  const evidenceGateBinding = Object.prototype.hasOwnProperty.call(CALA_AVAILABLE_BY_SLUG, slug);
  const momentumEvidencePass = !evidenceGateBinding || momentumEvidence.pass === true;

  const auditPass =
    completeness.auditPass === true &&
    emptyScan.pass === true &&
    provenance.pass === true &&
    golden.pass === true &&
    imageDistinctivenessPass === true &&
    imageRoleMatchPass === true &&
    sectionPatternParityPass === true &&
    momentumEvidencePass === true;

  const patchPlanComplete =
    completeness.patchPlanComplete === true &&
    fieldFails.every((f) => f.proposedPatch || f.recommendedAction === "suppress_component");

  return {
    brandSlug: slug,
    brandName: brand?.name || slug,
    liveState: completeness.liveState,
    auditComplete: true,
    patchPlanComplete,
    auditPass,
    failFindings: fieldFails.length,
    emptyRenderFailFindings: emptyScan.failFindings,
    releaseQualityDecision: auditPass
      ? "field_complete"
      : patchPlanComplete
        ? "field_complete_after_patch"
        : "not_field_complete",
    completeness,
    emptyScan,
    provenance,
    golden: { pass: golden.pass === true, failures: golden.failures || [] },
    imageUniqueness,
    imageRoleMatch,
    sectionPatternParity,
    momentumEvidence,
    findings: completeness.findings,
    patchPlan: completeness.patchPlan,
    gates: {
      rendered_field_completeness: completeness.auditPass === true,
      no_empty_rendered_components: emptyScan.pass === true,
      source_provenance_by_tab: provenance.pass === true,
      golden_content_quality: golden.pass === true,
      image_distinctiveness: imageDistinctivenessPass,
      image_role_match: imageRoleMatchPass,
      section_pattern_parity: sectionPatternParityPass,
      recent_momentum_pattern_pass:
        sectionPatternParity.gates?.recent_momentum_pattern_pass === true,
      recent_momentum_evidence_quality: momentumEvidencePass,
      geographic_footprint_pattern_pass:
        sectionPatternParity.gates?.geographic_footprint_pattern_pass === true,
      portfolio_context_pattern_pass:
        sectionPatternParity.gates?.portfolio_context_pattern_pass === true,
      growth_priorities_pattern_pass:
        sectionPatternParity.gates?.growth_priorities_pattern_pass === true,
      gallery_distinct_images: imageUniqueness.galleryDistinctCount >= 6,
      scenario_distinct_images: imageUniqueness.scenarioDistinctCount >= 3,
      property_distinct_images: imageUniqueness.propertyExampleDistinctCount >= 3,
    },
  };
}

export function evaluateTabFactoryForTest(brandResult) {
  const failures = [];
  if (!brandResult.auditPass) {
    for (const f of brandResult.findings || []) {
      if (f.status === "pass" || f.status === "should_suppress") continue;
      if (f.recommendedAction === "suppress_component") continue;
      failures.push(`${f.fieldName}:${f.status}`);
    }
    for (const e of brandResult.emptyScan?.findings || []) {
      failures.push(`empty:${e.id}`);
    }
    for (const p of brandResult.provenance?.failures || []) {
      failures.push(`provenance:${p}`);
    }
    for (const g of brandResult.golden?.failures || []) {
      failures.push(`golden:${g}`);
    }
    for (const img of brandResult.imageUniqueness?.findings || []) {
      if (img.status === "fail") failures.push(`image:${img.id}`);
    }
    for (const s of brandResult.sectionPatternParity?.findings || []) {
      failures.push(`section_pattern:${s.section}:${s.status}`);
    }
  }
  return { pass: brandResult.auditPass === true && failures.length === 0, failures };
}
