/**
 * Brand Explorer Final QA Auditor — full-brand readiness audit.
 *
 * Aggregates live data, API shape, required-section contract, visual defects,
 * carryover/copy leakage, source governance, and active-brand parity.
 * Read-only by default.
 *
 * @see docs/data-intelligence/brand-explorer-final-qa-auditor.md
 */
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import {
  buildBrandExplorerRequiredSectionPopulationContractReport,
} from "./brand-explorer-required-section-population-contract.js";
import {
  buildBrandExplorerVisualDisplayDefectAuditReport,
} from "./brand-explorer-visual-display-defect-audit.js";
import {
  resolveBrandTarget as resolveBrandTargetV28C,
  getBrandTargetResolverContext,
} from "./brand-explorer-brand-target-resolver.js";
import {
  assessPresentationRowImageGovernance,
  detectBrandAssetImageGovernanceDefects,
  findRegistryAssetForPresentationRow,
  getDiscoveryBrandConfig,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  assessOpeningsRowQuarantine,
  detectOpeningsUiQuarantineDefects,
  isOpeningsEvidenceSlot,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import {
  CARRYOVER_LOGIC_VERSION,
  collectBrandTextSurfaces,
  detectBlockingCarryoverFindings,
  resolveParentFamily,
} from "./brand-explorer-parent-aware-carryover.js";

export const AUDIT_VERSION = "1";
export const CARRYOVER_AUDIT_VERSION = CARRYOVER_LOGIC_VERSION;
export const REPORT_JSON_NAME = "brand-explorer-final-qa-auditor.json";
export const REPORT_MD_NAME = "brand-explorer-final-qa-auditor.md";
export const DOC_MD_NAME = "brand-explorer-final-qa-auditor.md";

const REFERENCE_BRAND_IDS = ACTIVE_BRAND_AUDIT_TARGETS.filter(
  (b) => b.slug !== "tribute-portfolio"
).map((b) => b.recordId);

const FILES_READ = [
  "AGENTS.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-visual-display-defect-audit.js",
  "lib/partner-intelligence/brand-explorer-parent-aware-carryover.js",
  "lib/partner-intelligence/brand-explorer-portfolio-mix-context-normalization-writer.js",
  "lib/partner-intelligence/tribute-portfolio-package-pipeline.js",
  "live Brand Explorer Presentation rows",
  "live Partner Facts",
  "live Source Library records",
];

const CARRYOVER_PATTERNS = [
  { id: "curio_phrase", re: /exactly like nothing else/i, severity: "critical", family: "hilton_curio" },
  { id: "curio_brand", re: /\bcurio collection\b/i, severity: "critical", family: "hilton_curio" },
  { id: "hilton_honors", re: /\bhilton honors\b/i, severity: "critical", family: "hilton" },
  { id: "choice_privileges", re: /\bchoice privileges\b/i, severity: "critical", family: "choice" },
  { id: "radisson_rewards", re: /\bradisson rewards\b/i, severity: "critical", family: "choice" },
  { id: "marsha_code", re: /\bmarsha code\b/i, severity: "high", family: "internal" },
  { id: "census_property_url", re: /\bcensus property url\b|\bdealality census\b|\bcensus url extract\b/i, severity: "high", family: "internal" },
  { id: "item_19_ui", re: /\bitem\s*19\b/i, severity: "high", family: "internal" },
  { id: "confirm_fees_fdd_ui", re: /\bconfirm flag, fees\b|\bin your loi and fdd\b/i, severity: "high", family: "internal" },
  { id: "consumer_site", re: /\bconsumer site\b/i, severity: "medium", family: "source_capture" },
  { id: "brand_site_label", re: /\bbrand site\b/i, severity: "medium", family: "source_capture" },
  { id: "metadata_label", re: /\bmetadata\b/i, severity: "medium", family: "internal" },
  { id: "source_data_label", re: /\bsource data\b/i, severity: "medium", family: "internal" },
  { id: "dated_listing", re: /\bdated listing\b/i, severity: "medium", family: "source_capture" },
  { id: "official_materials", re: /\bofficial materials\b/i, severity: "low", family: "source_capture" },
  { id: "unsupported_percent", re: /(?:~|≈|about|roughly|approximately)?\s*\d+\s*%/i, severity: "high", family: "copy" },
  { id: "governance_phrase", re: /\bsource-backed\b|\bapproved facts\b|\bno performance guarantee\b|\bconfirm scale claims\b/i, severity: "medium", family: "governance" },
  { id: "generic_earn_redeem", re: /earn and redeem points that take you everywhere you want to go/i, severity: "medium", family: "loyalty" },
  { id: "placeholder_dash", re: /(?:^|\n)\s*[-–—]\s*(?:$|\n)/, severity: "low", family: "placeholder" },
];

const GENERIC_FALLBACK_RE =
  /lorem ipsum|placeholder|tbd|coming soon|to be determined|template filler|brand footprint setup shows/i;

const UI_QUALITY_CHECKS = [
  { id: "title_only_card", test: (row) => hasVal(row?.title) && !hasVal(row?.body), severity: "high" },
  { id: "empty_card", test: (row) => !hasVal(row?.title) && !hasVal(row?.body), severity: "high" },
  { id: "overlong_copy", test: (row) => nz(row?.body).length > 900, severity: "low" },
  { id: "generic_fallback", test: (row) => GENERIC_FALLBACK_RE.test(nz(row?.body)), severity: "medium" },
];

const HERO_SECTION_SLOTS = ["overview.hero"];
const POSITIONING_BASICS = ["brandTaglineMotto", "brandPositioning", "brandCustomerPromise", "brandFamily", "parentCompany"];
const OPENING_SLOTS = [
  "economics.opening.step.1",
  "economics.opening.step.2",
  "economics.opening.step.3",
  "economics.opening.step.4",
  "economics.opening.step.5",
  "economics.opening.process",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function resolveActiveRegistryBrandTarget(brandArg) {
  const normalized = nz(brandArg).toLowerCase();
  const bySlug = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === normalized);
  if (bySlug) return bySlug;
  const byId = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === brandArg);
  if (byId) return byId;
  const byName = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.name.toLowerCase() === normalized);
  if (byName) return byName;
  return null;
}

export function isExpansionBacklogBrandTarget(target) {
  if (nz(target?.resolution?.resolutionSource) === "expansion_backlog") return true;
  const slug = nz(target?.slug || target?.resolution?.resolvedSlug).toLowerCase();
  return Boolean(slug && getDiscoveryBrandConfig(slug));
}

export async function resolveFinalQaBrandTarget(brandArg) {
  const active = resolveActiveRegistryBrandTarget(brandArg);
  if (active) {
    return {
      slug: active.slug,
      recordId: active.recordId,
      name: active.name,
      resolution: {
        inputTarget: brandArg,
        resolvedBrandName: active.name,
        resolvedRecordId: active.recordId,
        resolvedSlug: active.slug,
        resolutionSource: "active_registry",
      },
    };
  }
  const ctx = await getBrandTargetResolverContext();
  const resolved = await resolveBrandTargetV28C(brandArg, ctx);
  const slug = nz(resolved.slug || resolved.resolution?.resolvedSlug).toLowerCase();
  const resolution = { ...(resolved.resolution || resolved) };
  if (slug && getDiscoveryBrandConfig(slug)) {
    resolution.resolutionSource = "expansion_backlog";
  }
  return {
    slug: resolved.slug || resolved.resolution?.resolvedSlug,
    recordId: resolved.recordId || resolved.resolution?.resolvedRecordId,
    name: resolved.name || resolved.resolution?.resolvedBrandName,
    resolution,
  };
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function allPresentationRows(brand) {
  return Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
}

function collectTextSurfaces(brand) {
  const surfaces = [];
  const basics = brand || {};
  for (const key of POSITIONING_BASICS) {
    if (hasVal(basics[key])) {
      surfaces.push({ surface: `basics.${key}`, text: nz(basics[key]), recordId: brand.id, slotKey: null });
    }
  }
  for (const block of allPresentationRows(brand)) {
    const parts = [nz(block.title), nz(block.body)].filter(Boolean).join("\n");
    if (!parts) continue;
    surfaces.push({
      surface: `presentation.${block.slotKey}`,
      text: parts,
      recordId: block.recordId || null,
      slotKey: block.slotKey,
    });
  }
  return surfaces;
}

function detectCarryoverDefects(brand, brandTarget) {
  const defects = [];
  const targetFamily = resolveParentFamily(brand, brandTarget);

  for (const finding of detectBlockingCarryoverFindings(brand, brandTarget)) {
    defects.push({
      type: "brand_carryover",
      severity: finding.severity || "critical",
      category: "copy",
      patternId: finding.markerId,
      surface: finding.surface,
      recordId: finding.recordId,
      slotKey: finding.slotKey,
      excerpt: finding.excerpt,
      carryoverClassification: finding.classification,
      targetFamily,
      message: finding.message,
      recommendedFixBatch: "v26A_copy_carryover_cleanup",
    });
  }

  for (const surface of collectBrandTextSurfaces(brand)) {
    for (const pattern of CARRYOVER_PATTERNS) {
      if (!pattern.re.test(surface.text)) continue;
      if (!["internal", "source_capture", "governance"].includes(pattern.family)) continue;
      defects.push({
        type: "ui_quality",
        severity: pattern.severity,
        category: "copy",
        patternId: pattern.id,
        surface: surface.surface,
        recordId: surface.recordId,
        slotKey: surface.slotKey,
        excerpt: surface.text.slice(0, 160),
        carryoverClassification: "internal_or_governance_language",
        message: `UI-facing internal/source-capture language: ${pattern.id}`,
        recommendedFixBatch: "v26A_copy_carryover_cleanup",
      });
    }
  }

  return defects;
}

function detectUiQualityDefects(brand) {
  const defects = [];
  for (const block of allPresentationRows(brand)) {
    for (const check of UI_QUALITY_CHECKS) {
      if (!check.test(block)) continue;
      defects.push({
        type: "ui_quality",
        severity: check.severity,
        category: check.id.includes("image") ? "data" : "copy",
        surface: `presentation.${block.slotKey}`,
        recordId: block.recordId || null,
        slotKey: block.slotKey,
        title: nz(block.title),
        message: `${check.id} on slot ${block.slotKey}`,
        recommendedFixBatch: check.id === "title_only_card" ? "v25C-4E_opening_loyalty_quality_repair" : "v24A_copy_cleanup_writer",
      });
    }
    if (hasVal(block.title) && /opening|momentum|footprint\.openings|footprint\.momentum/.test(block.slotKey)) {
      const needsImage = /footprint\.openings/.test(block.slotKey);
      if (needsImage && !hasVal(block.imageUrl)) {
        defects.push({
          type: "missing_image",
          severity: "high",
          category: "data",
          surface: `presentation.${block.slotKey}`,
          recordId: block.recordId || null,
          slotKey: block.slotKey,
          title: nz(block.title),
          message: "Opening row missing imageUrl/attachment",
          recommendedFixBatch: "v24B_media_asset_fix",
        });
      }
    }
  }
  return defects;
}

function detectOpeningPathDefects(brand) {
  const defects = [];
  for (const slotKey of OPENING_SLOTS) {
    const rows = blocksForSlot(brand, slotKey);
    if (!rows.length) {
      defects.push({
        type: "missing_section_data",
        severity: "high",
        category: "data",
        slotKey,
        message: `Missing opening path row for ${slotKey}`,
        recommendedFixBatch: "v25C-4E_opening_loyalty_quality_repair",
      });
      continue;
    }
    for (const row of rows) {
      if (!hasVal(row.body)) {
        defects.push({
          type: "title_only_card",
          severity: "high",
          category: "data",
          slotKey,
          recordId: row.recordId || null,
          title: nz(row.title),
          message: `Opening tile has title but no body: ${slotKey}`,
          recommendedFixBatch: "v25C-4E_opening_loyalty_quality_repair",
        });
      }
    }
  }
  return defects;
}

function detectLoyaltyDensityDefects(brand) {
  const proofCount = blocksForSlot(brand, "loyalty.proof").length;
  const defects = [];
  if (proofCount < 6) {
    defects.push({
      type: "thin_loyalty_proof",
      severity: "high",
      category: "data",
      slotKey: "loyalty.proof",
      message: `Key Benefits has ${proofCount} cards; active reference brands target 6`,
      recommendedFixBatch: "v25C-4E_opening_loyalty_quality_repair",
    });
  }
  return defects;
}

function detectGovernanceDefects(liveState, brand) {
  const defects = [];
  const facts = liveState?.facts || [];
  const sources = liveState?.sources || [];
  const explorerFacts = facts.filter((f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be."));
  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const fddFacts = explorerFacts.filter((f) => /fdd|item\s*19|franchise disclosure/i.test(`${nz(f.fieldName)} ${nz(f.sourceType)} ${nz(f.notes)}`));
  const internalFacts = explorerFacts.filter((f) => /internal|draft only|not for explorer/i.test(`${nz(f.notes)} ${nz(f.validationStatus)}`));

  const surfaces = collectTextSurfaces(brand).map((s) => s.text.toLowerCase()).join("\n");
  const hasAffirmativeValidationClaim = surfaces
    .split("\n")
    .some((line) => {
      const l = nz(line).toLowerCase();
      if (!l) return false;
      if (
        /not company validated|no company sign-off|not marriott-validated|not marriott validated|no marriott sign-off/i.test(
          l
        )
      ) {
        return false;
      }
      return /company validated|marriott validated/i.test(l);
    });
  if (hasAffirmativeValidationClaim && !hasVal(brand.companyValidated)) {
    defects.push({
      type: "false_validation_implication",
      severity: "critical",
      category: "copy",
      message: "UI copy implies company validation but Company Validated is not set",
      recommendedFixBatch: "v26A_copy_carryover_cleanup",
    });
  }

  if (hasVal(brand.companyValidated) || hasVal(brand.companyValidationDate)) {
    defects.push({
      type: "company_validated_present",
      severity: "low",
      category: "governance",
      message: `Company Validated=${nz(brand.companyValidated)} date=${nz(brand.companyValidationDate)} — verify intentional`,
      recommendedFixBatch: "manual_review",
    });
  }

  const unapprovedSources = sources.filter((s) => nz(s.approvedForExplorerUse) !== "Yes");
  if (unapprovedSources.length > 0) {
    defects.push({
      type: "source_stewardship_needed",
      severity: "medium",
      category: "source",
      message: `${unapprovedSources.length} sources not approved for Explorer use`,
      recommendedFixBatch: "source_stewardship",
    });
  }

  if (pendingFacts.length > 0) {
    defects.push({
      type: "pending_facts_present",
      severity: "medium",
      category: "source",
      message: `${pendingFacts.length} pending Explorer facts — approval needed before external copy expansion`,
      recommendedFixBatch: "fact_approval_package",
    });
  }

  if (fddFacts.length > 0) {
    defects.push({
      type: "fdd_facts_present",
      severity: "medium",
      category: "source",
      message: `${fddFacts.length} FDD-tagged facts in inventory — must not surface in external UI`,
      recommendedFixBatch: "fact_stewardship",
    });
  }

  if (internalFacts.length > 0) {
    defects.push({
      type: "internal_facts_present",
      severity: "medium",
      category: "source",
      message: `${internalFacts.length} internal-only facts in inventory`,
      recommendedFixBatch: "fact_stewardship",
    });
  }

  return {
    defects,
    summary: {
      pendingFactsCount: pendingFacts.length,
      unapprovedSourcesCount: unapprovedSources.length,
      fddFactsCount: fddFacts.length,
      internalFactsCount: internalFacts.length,
      companyValidated: nz(brand.companyValidated) || null,
      companyValidationDate: nz(brand.companyValidationDate) || null,
    },
  };
}

function compareParityMetrics(targetBrand, referenceBrands) {
  const targetBlocks = allPresentationRows(targetBrand);
  const metrics = {
    targetBlockCount: targetBlocks.length,
    referenceBlockCounts: referenceBrands.map((b) => ({
      name: b.name || b.id,
      recordId: b.id || b.recordId,
      blockCount: allPresentationRows(b).length,
    })),
    loyaltyProofTarget: blocksForSlot(targetBrand, "loyalty.proof").length,
    loyaltyProofReferences: referenceBrands.map((b) => ({
      name: b.name,
      count: blocksForSlot(b, "loyalty.proof").length,
    })),
    openingStepsTarget: OPENING_SLOTS.filter((s) => blocksForSlot(targetBrand, s).some((r) => hasVal(r.body))).length,
    openingStepsReferences: referenceBrands.map((b) => ({
      name: b.name,
      count: OPENING_SLOTS.filter((s) => blocksForSlot(b, s).some((r) => hasVal(r.body))).length,
    })),
    mixRowsTarget: blocksForSlot(targetBrand, "footprint.portfolio_mix").length,
    mixRowsReferences: referenceBrands.map((b) => ({
      name: b.name,
      count: blocksForSlot(b, "footprint.portfolio_mix").length,
    })),
  };
  const refAvgBlocks =
    metrics.referenceBlockCounts.reduce((a, b) => a + b.blockCount, 0) /
    Math.max(1, metrics.referenceBlockCounts.length);
  const blockRatio = metrics.targetBlockCount / Math.max(1, refAvgBlocks);
  return { metrics, blockRatio, comparableQuality: blockRatio >= 0.75 };
}

function mapContractSections(contractReport) {
  return (contractReport?.sectionBySectionReadiness || []).map((s) => ({
    section: s.section,
    status:
      String(s.classification).startsWith("ready")
        ? "ready"
        : /founder_review/.test(s.classification)
          ? "founder_legal_review_needed"
          : /fact_approval/.test(s.classification)
            ? "source_approval_needed"
            : /frontend_mapping/.test(s.classification)
              ? "data_present_but_not_rendering"
              : s.rendersToday && s.currentCount < s.requiredMinimum
                ? "rendering_but_weak"
                : s.currentCount > 0
                  ? "incomplete"
                  : "missing",
    classification: s.classification,
    currentCount: s.currentCount,
    requiredMinimum: s.requiredMinimum,
    rendersToday: s.rendersToday,
    recommendedNextAction: s.recommendedNextAction || "",
    blocker: s.blockerIfNotSafe || "",
  }));
}

function computeScores({
  contractReport,
  visualReport,
  defects,
  parity,
  governanceSummary,
}) {
  const requiredSectionReadinessScore = contractReport?.readinessScore ?? 0;
  const visualScore = visualReport?.visualComparability?.score ?? 0;
  const presentationQualityScore = Math.max(
    0,
    Math.round(visualScore - defects.filter((d) => d.category === "copy").length * 2)
  );
  const carryoverRiskScore = Math.max(
    0,
    100 -
      defects.filter((d) => d.type === "brand_carryover").length * 25 -
      defects.filter((d) => d.patternId?.includes("governance")).length * 5
  );
  const sourceGovernanceScore = Math.max(
    0,
    100 -
      (governanceSummary.pendingFactsCount || 0) * 3 -
      (governanceSummary.unapprovedSourcesCount || 0) * 2 -
      (governanceSummary.fddFactsCount || 0) * 5
  );
  const visualCompletenessScore =
    visualReport?.visualComparability?.score != null
      ? visualReport.visualComparability.score
      : Math.max(
          0,
          100 -
            defects.filter((d) => d.severity === "critical").length * 20 -
            defects.filter((d) => d.severity === "high").length * 8 -
            defects.filter((d) => d.type === "missing_image").length * 6
        );
  const overallNumeric = Math.round(
    requiredSectionReadinessScore * 0.25 +
      presentationQualityScore * 0.2 +
      carryoverRiskScore * 0.15 +
      sourceGovernanceScore * 0.15 +
      visualCompletenessScore * 0.25
  );
  const readinessDefects = defects.filter((d) => {
    if (d.cosmeticNonBlocking && !d.activeProfileBlocker) return false;
    if (d.severity === "low" && (d.type === "visual_defect" || d.category === "frontend")) {
      return false;
    }
    return true;
  });
  const criticalCount = readinessDefects.filter((d) => d.severity === "critical").length;
  const highCount = readinessDefects.filter((d) => d.severity === "high").length;
  const activeProfileBlockerCount = defects.filter((d) => d.activeProfileBlocker).length;
  let overallActiveProfileReadiness = "not_ready";
  if (
    criticalCount === 0 &&
    highCount === 0 &&
    activeProfileBlockerCount === 0 &&
    overallNumeric >= 85 &&
    requiredSectionReadinessScore >= 85
  ) {
    overallActiveProfileReadiness = "ready";
  } else if (criticalCount === 0 && overallNumeric >= 70) {
    overallActiveProfileReadiness = "almost_ready";
  } else if (criticalCount > 0 || requiredSectionReadinessScore < 50) {
    overallActiveProfileReadiness = "blocked";
  }
  return {
    requiredSectionReadinessScore,
    presentationQualityScore,
    brandCarryoverRiskScore: carryoverRiskScore,
    sourceGovernanceScore,
    visualCompletenessScore,
    overallNumeric,
    overallActiveProfileReadiness,
  };
}

function groupDefectsBySeverity(defects) {
  const grouped = { critical: [], high: [], medium: [], low: [] };
  for (const d of defects) {
    const sev = grouped[d.severity] ? d.severity : "medium";
    grouped[sev].push(d);
  }
  return grouped;
}

function buildRecommendedFixBatches(defects, contractReport) {
  const batches = new Set();
  for (const d of defects) {
    if (d.recommendedFixBatch) batches.add(d.recommendedFixBatch);
  }
  for (const step of contractReport?.exactNextWriterSequence || []) batches.add(step);
  return [...batches];
}

function buildHeroSectionStatus(brand) {
  const hero = blocksForSlot(brand, "overview.hero");
  const hasHero = hero.some((r) => hasVal(r.body) || hasVal(r.imageUrl));
  return {
    section: "Hero",
    status: hasHero ? "ready" : "missing",
    slotKeys: HERO_SECTION_SLOTS,
    rowCount: hero.length,
  };
}

function buildPositioningSectionStatus(brand) {
  const fields = POSITIONING_BASICS.filter((k) => hasVal(brand[k]));
  return {
    section: "Brand positioning",
    status: fields.length >= 3 ? "ready" : fields.length > 0 ? "incomplete" : "missing",
    fieldsPresent: fields,
  };
}

export async function buildBrandExplorerFinalQaAuditorReport(options = {}) {
  const allActive = Boolean(options.allActive);
  const brandArg = nz(options.brandIdOrName || "tribute-portfolio");
  const targets = allActive
    ? ACTIVE_BRAND_AUDIT_TARGETS.map((b) => ({
        slug: b.slug,
        recordId: b.recordId,
        name: b.name,
        resolution: {
          inputTarget: b.slug,
          resolvedBrandName: b.name,
          resolvedRecordId: b.recordId,
          resolvedSlug: b.slug,
          resolutionSource: "active_registry",
        },
      }))
    : [await resolveFinalQaBrandTarget(brandArg)];

  const brandReports = [];
  for (const target of targets) {
    const brand = await fetchBrandApiShape(target.recordId);
    if (!brand) {
      brandReports.push({
        brand: target,
        error: `Could not load brand ${target.recordId}`,
      });
      continue;
    }

    const liveState = await fetchLiveState(target.recordId).catch(() => ({
      recordId: target.recordId,
      sources: [],
      facts: [],
      brandBasics: null,
    }));

    const contractReport = await buildBrandExplorerRequiredSectionPopulationContractReport({
      brandIdOrName: target.recordId,
    }).catch(() => null);

    const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
      brandIdOrName: target.recordId || target.slug,
    }).catch(() => null);

    const referenceBrands = [];
    for (const refId of REFERENCE_BRAND_IDS) {
      const ref = await fetchBrandApiShape(refId);
      const meta = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === refId);
      if (ref) referenceBrands.push({ ...meta, brand: ref });
    }

    const isExpansion = isExpansionBacklogBrandTarget(target);
    const carryoverDefects = detectCarryoverDefects(brand, target);
    const uiDefects = isExpansion ? [] : detectUiQualityDefects(brand);
    const openingDefects = isExpansion ? [] : detectOpeningPathDefects(brand);
    const loyaltyDefects = isExpansion ? [] : detectLoyaltyDensityDefects(brand);
    const { defects: governanceDefects, summary: governanceSummary } = detectGovernanceDefects(liveState, brand);
    let imageGovernanceDefects = [];
    let openingsQuarantineDefects = [];
    if (isExpansion) {
      const discoveryConfig = getDiscoveryBrandConfig(target.slug);
      if (discoveryConfig) {
        const registryAssets = await listRegistryAssetsForBrand(target.recordId).catch(() => []);
        imageGovernanceDefects = detectBrandAssetImageGovernanceDefects(
          brand,
          registryAssets,
          discoveryConfig,
          target
        ).map((d) => ({
          type: d.type,
          severity: d.severity,
          category: d.category,
          surface: d.surface,
          recordId: d.recordId,
          slotKey: d.slotKey,
          message: d.message,
          recommendedFixBatch: d.recommendedFixBatch,
          cosmeticNonBlocking: Boolean(d.cosmeticNonBlocking),
          activeProfileBlocker: Boolean(d.activeProfileBlocker),
        }));
        const quarantineAssessments = [];
        for (const block of brand?.brandExplorer?.blocks || []) {
          if (!isOpeningsEvidenceSlot(block?.slotKey)) continue;
          const registryMatch = findRegistryAssetForPresentationRow(registryAssets, block);
          const imageAssessment = assessPresentationRowImageGovernance(
            block,
            discoveryConfig,
            registryAssets
          );
          const q = assessOpeningsRowQuarantine(block, imageAssessment, registryMatch);
          if (q) quarantineAssessments.push(q);
        }
        openingsQuarantineDefects = detectOpeningsUiQuarantineDefects(
          [],
          quarantineAssessments,
          target
        );
      }
    }
    const visualDefects = (visualReport?.defects || []).map((d) => ({
      type: d.defectType || d.type || "visual_defect",
      severity: d.severity || "medium",
      category: d.category || "frontend",
      surface: d.section || d.surface || "",
      slotKey: d.slotKey || null,
      recordId: d.recordId || null,
      message: d.proposedCorrection || d.description || d.message || d.referencePattern || "",
      carryoverClassification: d.carryoverClassification || null,
      cosmeticNonBlocking: Boolean(d.cosmeticNonBlocking),
      recommendedFixBatch: d.remediationBatch || visualReport?.recommendedNextBatch || "visual_repair",
    }));

    const allDefects = [
      ...carryoverDefects,
      ...uiDefects,
      ...openingDefects,
      ...loyaltyDefects,
      ...governanceDefects,
      ...imageGovernanceDefects,
      ...openingsQuarantineDefects,
      ...visualDefects,
    ];
    const parity = compareParityMetrics(
      brand,
      referenceBrands.map((r) => r.brand)
    );
    const scores = computeScores({
      contractReport,
      visualReport,
      defects: allDefects,
      parity,
      governanceSummary,
    });

    const presentationRecordIds = allPresentationRows(brand)
      .map((b) => b.recordId)
      .filter(Boolean);

    brandReports.push({
      brand: {
        slug: target.slug,
        name: brand.name || target.name,
        recordId: target.recordId,
      },
      liveRecordsInspected: {
        presentationRowCount: allPresentationRows(brand).length,
        presentationRecordIds: presentationRecordIds.slice(0, 50),
        factCount: (liveState.facts || []).length,
        sourceCount: (liveState.sources || []).length,
      },
      referenceBrandsUsed: referenceBrands.map((r) => ({ name: r.name, recordId: r.recordId })),
      heroSection: buildHeroSectionStatus(brand),
      positioningSection: buildPositioningSectionStatus(brand),
      requiredSections: mapContractSections(contractReport),
      defects: allDefects,
      defectsBySeverity: groupDefectsBySeverity(allDefects),
      defectCounts: {
        total: allDefects.length,
        critical: allDefects.filter((d) => d.severity === "critical").length,
        high: allDefects.filter((d) => d.severity === "high").length,
        medium: allDefects.filter((d) => d.severity === "medium").length,
        low: allDefects.filter((d) => d.severity === "low").length,
      },
      carryoverFindings: carryoverDefects,
      sourceGovernanceFindings: governanceDefects,
      visualRenderingFindings: [...uiDefects, ...visualDefects, ...openingDefects, ...loyaltyDefects],
      parity,
      scores,
      recommendedFixBatches: buildRecommendedFixBatches(allDefects, contractReport),
      contractReadinessScore: contractReport?.readinessScore ?? null,
      visualComparability: visualReport?.visualComparability || null,
      companyValidatedUntouched: true,
      companyValidated: governanceSummary.companyValidated,
      companyValidationDate: governanceSummary.companyValidationDate,
    });
  }

  const primary = brandReports.find((b) => !b.error) || brandReports[0];
  return {
    auditVersion: AUDIT_VERSION,
    auditorExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    allActive,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
      "scripts/brand-explorer-final-qa-auditor.mjs",
      "docs/data-intelligence/brand-explorer-final-qa-auditor.md",
      "reports/brand-explorer-final-qa-auditor.md",
      "reports/brand-explorer-final-qa-auditor.json",
      "package.json",
    ],
    activeReferenceBrands: ACTIVE_BRAND_AUDIT_TARGETS.filter((b) => b.slug !== "tribute-portfolio").map((b) => ({
      slug: b.slug,
      name: b.name,
      recordId: b.recordId,
    })),
    brandReports,
    primaryBrand: primary?.brand || null,
    scores: primary?.scores || null,
    defectsBySeverity: primary?.defectsBySeverity || null,
    defectCounts: primary?.defectCounts || null,
    recommendedFixBatches: primary?.recommendedFixBatches || [],
    exactNextCommand: allActive
      ? "npm run brand-explorer-final-qa-auditor -- --all-active --dry-run"
      : `npm run brand-explorer-final-qa-auditor -- --brand ${brandArg} --dry-run`,
    exactOrchestratorCommand: `npm run brand-explorer-complete-build -- --brand ${brandArg} --dry-run --target-quality active-profile`,
  };
}

export function buildBrandExplorerFinalQaAuditorMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Final QA Auditor");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  for (const br of report.brandReports || []) {
    if (br.error) {
      lines.push(`## ${br.brand?.name || "Unknown"} — ERROR`);
      lines.push(br.error);
      continue;
    }
    lines.push(`## ${br.brand.name} (\`${br.brand.recordId}\`)`);
    lines.push("");
    lines.push("### Scores");
    lines.push(`- Required Section Readiness: **${br.scores.requiredSectionReadinessScore}**`);
    lines.push(`- Presentation Quality: **${br.scores.presentationQualityScore}**`);
    lines.push(`- Brand Carryover Risk: **${br.scores.brandCarryoverRiskScore}** (higher is better)`);
    lines.push(`- Source Governance: **${br.scores.sourceGovernanceScore}**`);
    lines.push(`- Visual Completeness: **${br.scores.visualCompletenessScore}**`);
    lines.push(`- Overall Active Profile Readiness: **${br.scores.overallActiveProfileReadiness}** (${br.scores.overallNumeric})`);
    lines.push("");
    lines.push("### Defects by severity");
    lines.push(`- Critical: ${br.defectCounts.critical}`);
    lines.push(`- High: ${br.defectCounts.high}`);
    lines.push(`- Medium: ${br.defectCounts.medium}`);
    lines.push(`- Low: ${br.defectCounts.low}`);
    lines.push("");
    lines.push("### Required sections");
    for (const s of br.requiredSections || []) {
      lines.push(`- **${s.section}**: ${s.status} (${s.currentCount}/${s.requiredMinimum})`);
    }
    lines.push("");
    lines.push("### Recommended fix batches");
    for (const batch of br.recommendedFixBatches || []) lines.push(`- ${batch}`);
    lines.push("");
    lines.push("### Carryover findings");
    if (!(br.carryoverFindings || []).length) lines.push("- none");
    for (const f of br.carryoverFindings || []) {
      lines.push(`- [${f.severity}] ${f.message} · ${f.slotKey || f.surface}`);
    }
    lines.push("");
  }
  lines.push("## Next command");
  lines.push("```bash");
  lines.push(report.exactOrchestratorCommand || report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
