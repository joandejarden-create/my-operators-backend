/**
 * Brand Explorer Lifestyle / Affiliation Config Pack v35B.
 *
 * Registers 7 lifestyle brands in v34D factory config, runs dry-runs,
 * and produces source-capture + benchmark reports. No Airtable writes.
 */
import {
  BRAND_CONFIG_VERSION,
  getActiveProfileBrandConfig,
} from "./brand-explorer-active-profile-brand-config.js";
import {
  LIFESTYLE_ACTIVE_PROFILE_BRAND_CONFIGS,
  LIFESTYLE_BRAND_CONFIG_VERSION,
} from "./brand-explorer-lifestyle-affiliation-brand-config.js";
import {
  AFFILIATION_COPY_MODES,
  LIFESTYLE_COPY_GOVERNANCE_VERSION,
} from "./brand-explorer-lifestyle-affiliation-copy-governance.js";
import {
  buildSourceCapturePlan,
  buildSourceCapturePlanMarkdown,
  SOURCE_CAPTURE_PLAN_VERSION,
} from "./brand-explorer-lifestyle-affiliation-source-capture.js";
import { getCopyGovernanceConfig, COPY_GOVERNANCE_VERSION } from "./brand-explorer-active-profile-copy-governance-config.js";
import { buildBrandExplorerActiveProfileFactoryReport } from "./brand-explorer-active-profile-factory.js";
import { FACTORY_VERSION, FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { fetchAndResolveApprovedBrandSources, resolveApprovedBrandSources } from "./brand-source-auto-resolver.js";
import {
  buildDraftApplyCommand,
  buildActiveApprovalCommand,
} from "./brand-explorer-active-profile-staged-apply.js";

export const V35B_PACK_VERSION = "v35B";

const PRIORITY_BRANDS = Object.freeze([
  { priority: 1, slug: "design-hotels", recordId: "rec02zPClpWUTCyXM", name: "Design Hotels" },
  { priority: 2, slug: "small-luxury-hotels-of-the-world", recordId: "recjjSnY2opb8P4DG", name: "Small Luxury Hotels of the World" },
  { priority: 3, slug: "autograph-collection", recordId: "recEJCTDj1zrsjPM6", name: "Autograph Collection" },
  { priority: 4, slug: "tribute-portfolio", recordId: "recCvV0PuZOi8c3hC", name: "Tribute Portfolio" },
  { priority: 5, slug: "vignette-collection", recordId: "recDwzv86TWnz2gGB", name: "Vignette Collection" },
  { priority: 6, slug: "mgallery-collection", recordId: "recrWCD1LMqu864oU", name: "MGallery Collection" },
  { priority: 7, slug: "hotel-indigo", recordId: "recegXrqaPiSLGCIe", name: "Hotel Indigo" },
  { priority: 8, slug: "handwritten-collection", recordId: "rec7hTXwMRC81EPqz", name: "Handwritten Collection" },
]);

const DRY_RUN_BRANDS = Object.freeze(["design-hotels", "small-luxury-hotels-of-the-world", "tribute-portfolio"]);

const REQUIRED_CONFIG_FIELDS = Object.freeze([
  "slug",
  "recordId",
  "name",
  "parentCompany",
  "brandModelType",
  "copyGovernanceMode",
  "officialSourceDomains",
  "allowedSiblingMentions",
  "disallowedCopyTerms",
  "galleryMinimum",
  "propertyExampleMinimum",
  "propertyImagesRequired",
  "standardDetailGovernanceRequired",
  "companyValidatedProtection",
]);

function validateBrandConfig(slug, config) {
  const missing = REQUIRED_CONFIG_FIELDS.filter((f) => config?.[f] == null || config[f] === "");
  const factorySupported = FACTORY_SUPPORTED_SLUGS.includes(slug);
  const copyGovernance = getCopyGovernanceConfig(slug);
  return {
    slug,
    registered: Boolean(config),
    factorySupported,
    copyGovernanceMode: config?.copyGovernanceMode || null,
    copyGovernanceReady: Boolean(copyGovernance),
    brandModelType: config?.brandModelType || null,
    recordId: config?.recordId || null,
    parentCompany: config?.parentCompany || null,
    galleryMinimum: config?.galleryMinimum ?? null,
    propertyExampleMinimum: config?.propertyExampleMinimum ?? null,
    propertyImagesRequired: config?.propertyImagesRequired === true,
    standardDetailGovernanceRequired: config?.standardDetailGovernanceRequired === true,
    companyValidatedProtection: config?.companyValidatedProtection === true,
    missingFields: missing,
    pass: Boolean(config) && missing.length === 0 && factorySupported && Boolean(copyGovernance),
  };
}

async function verifyBrandRecord(recordId, expectedSlug) {
  try {
    const basics = await fetchBrandBasics(recordId);
    return {
      recordId,
      expectedSlug,
      found: Boolean(basics),
      brandName: basics?.["Brand Name"] || null,
      companyValidated: Boolean(basics?.["Company Validated"]),
      companyValidatedTouched: false,
    };
  } catch (err) {
    return { recordId, expectedSlug, found: false, error: err.message };
  }
}

function summarizeFactoryDryRun(preflightReport, assetPackReport) {
  const rules = preflightReport?.factoryRules || {};
  const assetPack = assetPackReport?.assetPack || null;
  return {
    preflightPass: rules.pass === true,
    preflightBlockers: rules.blockers || [],
    preflightRuleSummary: {
      gallery: rules.rules?.gallery?.pass,
      propertyExamples: rules.rules?.propertyExamples?.pass,
      scenarioImages: rules.rules?.scenarioImages?.pass,
      copySafety: rules.rules?.copySafety?.pass,
      standardDetail: rules.rules?.standardDetail?.pass,
      registryTraceability: rules.rules?.registryTraceability?.pass,
    },
    assetPackReady: assetPack?.ready === true,
    assetPackBlockers: assetPack?.blockers || [],
    assetPackSummary: assetPack
      ? {
          gallerySlots: assetPack.gallery?.slots?.length || 0,
          propertyExamples: assetPack.propertyExamples?.length || 0,
          scenarioCards: assetPack.scenarioCards?.length || 0,
          registryAssetsUsed: assetPack.registryAssetsUsed || 0,
        }
      : null,
    approvedSourcesCount: preflightReport?.approvedSourcesCount ?? assetPackReport?.approvedSourcesCount ?? 0,
    finalQa: preflightReport?.finalQa || assetPackReport?.finalQa || null,
  };
}

function buildTributeBenchmarkValidation(preflight, assetPack, liveSnapshot) {
  const rules = preflight?.factoryRules || {};
  const galleryPass = rules.rules?.gallery?.pass === true;
  const propertyPass = rules.rules?.propertyExamples?.pass === true;
  const copySafetyPass = rules.rules?.copySafety?.pass === true;
  const registryPass = rules.rules?.registryTraceability?.pass === true;
  const standardDetailPass = rules.rules?.standardDetail?.pass === true;

  const copyGovernanceNeeded = !copySafetyPass || (rules.rules?.copySafety?.blockers?.length || 0) > 0;
  const registryTraceabilityMissing = !registryPass;

  return {
    role: "technical_benchmark",
    brandSlug: "tribute-portfolio",
    passesV34DFactoryRules: rules.pass === true,
    passes: {
      gallerySixImage: galleryPass,
      propertyExamplesRealHotelImages: propertyPass,
      copySafety: copySafetyPass,
      standardDetailGovernance: standardDetailPass,
      registryTraceability: registryPass,
      assetPackReady: assetPack?.assetPack?.ready === true,
    },
    fails: (rules.blockers || []).map((b) => ({ id: b.id || b, message: b.message || String(b) })),
    copyGovernanceNeeded,
    registryTraceabilityMissing,
    gallerySixPasses: galleryPass,
    propertyExamplesUseRealHotelImages: propertyPass,
    stagedApplyRecommendation:
      rules.pass && assetPack?.assetPack?.ready
        ? "staged_apply_draft_then_founder_visual_review"
        : copyGovernanceNeeded || registryTraceabilityMissing
          ? "copy_governance_and_registry_traceability_first"
          : "asset_pack_completion_then_staged_apply",
    activeProfileApproval: "blocked_until_founder_visual_review_pass",
    companyValidated: liveSnapshot?.companyValidated ?? null,
    liveSnapshot,
    commands: {
      applyDraft: buildDraftApplyCommand("tribute-portfolio"),
      founderReview: "npm run brand-explorer-active-profile-founder-review -- --brand tribute-portfolio --dry-run",
      applyApproved: buildActiveApprovalCommand("tribute-portfolio"),
    },
  };
}

function buildDesignHotelsRecommendation(dryRun, sourcePlan) {
  const blockedCategories = (sourcePlan?.categories || []).filter((c) => c.status === "blocked" || c.status === "needed");
  return {
    brandSlug: "design-hotels",
    strategicPriority: 1,
    modelType: "affiliation_curation_platform",
    proceedVia: [
      "source_capture_first",
      "affiliation_specific_copy_governance_setup",
      "manual_source_library_seeding_after_official_page_capture",
      "asset_pack_dry_run_after_sources_approved",
    ],
    notYet: [
      "active_profile_approval",
      "company_validated_changes",
      "franchise_style_copy",
    ],
    customHandlingRequired: false,
    rationale:
      "Zero approved sources and no live gallery/property examples — generic factory config is registered but asset-pack cannot complete until official Design Hotels pages, property directory, and hotel photography are source-captured.",
    dryRun,
    blockedSourceCategories: blockedCategories.map((c) => c.id),
    buildRecommendation: sourcePlan?.buildRecommendation || "source_capture_first",
  };
}

function buildSlhRecommendation(dryRun, sourcePlan) {
  const blockedCategories = (sourcePlan?.categories || []).filter((c) => c.status === "blocked" || c.status === "needed");
  return {
    brandSlug: "small-luxury-hotels-of-the-world",
    strategicPriority: 2,
    modelType: "independent_luxury_consortium",
    proceedVia: [
      "source_capture_first",
      "legal_review_consortium_sensitivity",
      "affiliation_specific_copy_governance_setup",
      "manual_source_library_seeding",
      "asset_pack_dry_run_after_sources_approved",
    ],
    notYet: [
      "active_profile_approval",
      "company_validated_changes",
      "parent_brand_or_franchise_language",
    ],
    customHandlingRequired: false,
    rationale:
      "SLH requires consortium-appropriate copy governance and official source library before asset-pack. Generic factory represents the brand model; source capture is the gating step.",
    dryRun,
    blockedSourceCategories: blockedCategories.map((c) => c.id),
    buildRecommendation: sourcePlan?.buildRecommendation || "source_capture_first",
  };
}

export async function buildLifestyleAffiliationConfigV35BReport() {
  const brandConfigs = [];
  for (const seed of PRIORITY_BRANDS) {
    const config = getActiveProfileBrandConfig(seed.slug);
    const validation = validateBrandConfig(seed.slug, config);
    const recordCheck = await verifyBrandRecord(seed.recordId, seed.slug);
    const copyGovernance = getCopyGovernanceConfig(seed.slug);
    brandConfigs.push({
      ...seed,
      validation,
      recordCheck,
      copyGovernance: copyGovernance
        ? {
            mode: copyGovernance.copyGovernanceMode,
            positioningPillars: copyGovernance.positioningPillars,
            franchiseLanguageBlocked: copyGovernance.franchiseLanguageBlocked === true,
            slotRewriteCount: Object.keys(copyGovernance.slotRewrites || {}).length,
          }
        : null,
    });
  }

  const factoryDryRuns = {};
  for (const slug of DRY_RUN_BRANDS) {
    const preflight = await buildBrandExplorerActiveProfileFactoryReport({
      brandArg: slug,
      stage: "preflight",
      apply: false,
    });
    const assetPack = await buildBrandExplorerActiveProfileFactoryReport({
      brandArg: slug,
      stage: "asset-pack",
      apply: false,
    });
    factoryDryRuns[slug] = {
      preflight,
      assetPack,
      summary: summarizeFactoryDryRun(preflight, assetPack),
    };
  }

  const tributePreflight = factoryDryRuns["tribute-portfolio"]?.preflight;
  const tributeAssetPack = factoryDryRuns["tribute-portfolio"]?.assetPack;
  const tributeRules = tributePreflight?.factoryRules || {};
  const tributeConfig = getActiveProfileBrandConfig("tribute-portfolio");
  const tributeSourceResolution = await fetchAndResolveApprovedBrandSources({
    recordId: tributeConfig?.recordId,
    companyDomains: tributeConfig?.officialSourceDomains || [],
  }).catch(() => ({ sources: [] }));
  const tributeApprovedSources = resolveApprovedBrandSources(tributeSourceResolution.sources || [], {
    recordId: tributeConfig?.recordId,
    companyDomains: tributeConfig?.officialSourceDomains || [],
  });
  const tributeLiveSnapshot = {
    approvedSources: tributeApprovedSources.length,
    galleryApiWithImageUrl: tributeRules.rules?.gallery?.withImageUrl ?? 0,
    propertyExamplesWithImage: tributeRules.rules?.propertyExamples?.visibleOpeningCards ?? 0,
    galleryPass: tributeRules.rules?.gallery?.pass === true,
    propertyPass: tributeRules.rules?.propertyExamples?.pass === true,
    visibleRows: tributePreflight?.presentationRowCount || null,
    companyValidated: tributePreflight?.brandBasics?.["Company Validated"] ?? null,
  };

  const sourceCapturePlans = {
    "design-hotels": buildSourceCapturePlan("design-hotels"),
    "small-luxury-hotels-of-the-world": buildSourceCapturePlan("small-luxury-hotels-of-the-world"),
    "tribute-portfolio": buildSourceCapturePlan("tribute-portfolio", tributeLiveSnapshot),
  };

  const tributeBenchmark = buildTributeBenchmarkValidation(
    tributePreflight,
    tributeAssetPack,
    tributeLiveSnapshot
  );

  const buildRecommendations = {
    designHotels: buildDesignHotelsRecommendation(
      factoryDryRuns["design-hotels"]?.summary,
      sourceCapturePlans["design-hotels"]
    ),
    slh: buildSlhRecommendation(
      factoryDryRuns["small-luxury-hotels-of-the-world"]?.summary,
      sourceCapturePlans["small-luxury-hotels-of-the-world"]
    ),
    tributePortfolio: {
      brandSlug: "tribute-portfolio",
      role: "technical_benchmark",
      proceedVia: tributeBenchmark.stagedApplyRecommendation === "staged_apply_draft_then_founder_visual_review"
        ? ["copy_governance_pass_if_needed", "staged_apply_draft", "founder_visual_review", "apply_approved"]
        : ["copy_governance", "registry_traceability", "staged_apply_draft", "founder_visual_review"],
      buildRecommendation: tributeBenchmark.stagedApplyRecommendation,
      benchmark: tributeBenchmark,
    },
  };

  const allConfigsPass = brandConfigs.every((b) => b.validation.pass);
  const configsRegistered = brandConfigs.filter((b) => b.validation.registered).length;

  return {
    version: V35B_PACK_VERSION,
    generatedAt: new Date().toISOString(),
    guardrails: {
      noAirtableWrites: true,
      noActiveProfileApproval: true,
      noCompanyValidatedChanges: true,
      dryRunOnly: true,
    },
    versions: {
      brandConfig: BRAND_CONFIG_VERSION,
      lifestyleBrandConfig: LIFESTYLE_BRAND_CONFIG_VERSION,
      copyGovernance: COPY_GOVERNANCE_VERSION,
      lifestyleCopyGovernance: LIFESTYLE_COPY_GOVERNANCE_VERSION,
      sourceCapturePlan: SOURCE_CAPTURE_PLAN_VERSION,
      factory: FACTORY_VERSION,
    },
    copyGovernanceModes: Object.fromEntries(
      Object.entries(AFFILIATION_COPY_MODES).map(([key, mode]) => [
        key,
        {
          ...mode,
          blockedLanguage: (mode.blockedLanguage || []).map((re) => re.source),
        },
      ])
    ),
    brandConfigs,
    summary: {
      brandsRegistered: configsRegistered,
      brandsTotal: PRIORITY_BRANDS.length,
      allConfigsPass,
      factoryDryRunBrands: DRY_RUN_BRANDS,
      strategicPriority: ["design-hotels", "small-luxury-hotels-of-the-world"],
      technicalBenchmark: "tribute-portfolio",
    },
    factoryDryRuns: Object.fromEntries(
      Object.entries(factoryDryRuns).map(([slug, data]) => [slug, data.summary])
    ),
    sourceCapturePlans,
    tributeBenchmark,
    buildRecommendations,
  };
}

export function buildLifestyleAffiliationConfigV35BMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Lifestyle / Affiliation Config ${V35B_PACK_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push("- No Airtable writes");
  lines.push("- No active-profile approval");
  lines.push("- No Company Validated changes");
  lines.push("- Dry-run only for factory stages");
  lines.push("");
  lines.push("## Brand config registration");
  lines.push(`| Priority | Brand | Slug | Config pass | Copy mode |`);
  lines.push("| --- | --- | --- | --- | --- |");
  for (const b of report.brandConfigs) {
    lines.push(
      `| ${b.priority} | ${b.name} | \`${b.slug}\` | ${b.validation.pass ? "PASS" : "FAIL"} | ${b.copyGovernance?.mode || "—"} |`
    );
  }
  lines.push("");
  lines.push(`**Registered:** ${report.summary.brandsRegistered}/${report.summary.brandsTotal}`);
  lines.push("");
  lines.push("## Copy governance modes");
  for (const [mode, cfg] of Object.entries(report.copyGovernanceModes || {})) {
    lines.push(`### ${cfg.label} (\`${mode}\`)`);
    for (const pillar of cfg.positioningPillars || []) lines.push(`- ${pillar}`);
    lines.push("");
  }
  lines.push("## Factory dry-runs");
  for (const [slug, summary] of Object.entries(report.factoryDryRuns || {})) {
    lines.push(`### ${slug}`);
    lines.push(`- Preflight: **${summary.preflightPass ? "PASS" : "FAIL"}**`);
    lines.push(`- Asset pack ready: **${summary.assetPackReady ? "YES" : "NO"}**`);
    lines.push(`- Approved sources: ${summary.approvedSourcesCount}`);
    if (summary.preflightBlockers?.length) {
      lines.push(`- Blockers (${summary.preflightBlockers.length}):`);
      for (const blocker of summary.preflightBlockers.slice(0, 8)) {
        lines.push(`  - ${blocker.id || blocker}: ${blocker.message || ""}`);
      }
    }
    lines.push("");
  }
  lines.push("## Tribute Portfolio benchmark");
  const tb = report.tributeBenchmark;
  lines.push(`- Passes v34D factory rules: **${tb.passesV34DFactoryRules ? "YES" : "NO"}**`);
  lines.push(`- Gallery 6-image: **${tb.gallerySixPasses ? "PASS" : "FAIL"}**`);
  lines.push(`- Property examples (real hotel images): **${tb.propertyExamplesUseRealHotelImages ? "PASS" : "FAIL"}**`);
  lines.push(`- Copy governance needed: **${tb.copyGovernanceNeeded ? "YES" : "NO"}**`);
  lines.push(`- Registry traceability missing: **${tb.registryTraceabilityMissing ? "YES" : "NO"}**`);
  lines.push(`- Staged apply recommendation: **${tb.stagedApplyRecommendation}**`);
  lines.push("");
  lines.push("## Build recommendations");
  lines.push("### Design Hotels");
  lines.push(`- Proceed via: ${report.buildRecommendations.designHotels.proceedVia.join(" → ")}`);
  lines.push(`- Rationale: ${report.buildRecommendations.designHotels.rationale}`);
  lines.push("");
  lines.push("### Small Luxury Hotels of the World");
  lines.push(`- Proceed via: ${report.buildRecommendations.slh.proceedVia.join(" → ")}`);
  lines.push(`- Rationale: ${report.buildRecommendations.slh.rationale}`);
  lines.push("");
  lines.push("### Tribute Portfolio (benchmark)");
  lines.push(`- Proceed via: ${report.buildRecommendations.tributePortfolio.proceedVia.join(" → ")}`);
  lines.push("");
  lines.push("## Source capture plans");
  for (const slug of ["design-hotels", "small-luxury-hotels-of-the-world", "tribute-portfolio"]) {
    lines.push(buildSourceCapturePlanMarkdown(report.sourceCapturePlans[slug]));
    lines.push("");
  }
  return lines.join("\n");
}
