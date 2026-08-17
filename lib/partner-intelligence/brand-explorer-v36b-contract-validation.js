/**
 * Brand Explorer v36B — contract validation orchestrator (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { buildActiveProfileDraftPlan } from "./brand-explorer-active-profile-draft-builder.js";
import { buildCopyGovernancePlan } from "./brand-explorer-active-profile-copy-governance-builder.js";
import {
  fetchAndResolveApprovedBrandSources,
  resolveApprovedBrandSources,
} from "./brand-source-auto-resolver.js";
import { buildBrandKnowledgePack, KNOWLEDGE_PACK_VERSION } from "./brand-explorer-brand-knowledge-pack.js";
import { buildFullTabContentContract, auditPresentationRowsAgainstContract } from "./brand-explorer-full-tab-content-contract.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import {
  buildPresentationPlanFromDraftPlan,
  validateExistingPresentationRowsAsPlan,
} from "./brand-explorer-presentation-plan-contract.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { auditPresentationRowExternalOwner } from "./brand-explorer-external-owner-content-governance.js";
import { ATELIER_SCENARIO_FALLBACK_TITLES, ATELIER_PROOF_FALLBACK_HEADS } from "./brand-explorer-active-profile-factory-rules.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";

export const V36B_VERSION = "v36B";
export const REPORT_JSON = "brand-explorer-v36b-contract-validation.json";
export const REPORT_MD = "brand-explorer-v36b-contract-validation.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const DEFAULT_TEST_BRANDS = Object.freeze([
  "design-hotels",
  "small-luxury-hotels-of-the-world",
  "tribute-portfolio",
  "woodspring-suites",
  "everhome-suites",
]);

const KNOWLEDGE_PACK_FILE_KEYS = Object.freeze({
  "design-hotels": "design-hotels",
  "small-luxury-hotels-of-the-world": "slh",
  "tribute-portfolio": "tribute-portfolio",
  "woodspring-suites": "woodspring",
  "everhome-suites": "everhome",
});

const EXTERNAL_OWNER_MD_BRANDS = new Set([
  "design-hotels",
  "small-luxury-hotels-of-the-world",
  "tribute-portfolio",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function scoreExternalOwnerReadiness(ctx) {
  const { presentationRows = [], brandApi, assetPack, brandConfig, factoryRules, renderContract } = ctx;
  const categories = [];
  const blockers = [];
  const rule = evaluateExternalOwnerReadinessRule(presentationRows);

  if (rule.pass) categories.push("external_owner_ready");
  else {
    for (const b of rule.blockers) {
      if (b.includes("visible_source_urls")) {
        categories.push("blocked_by_internal_language");
        blockers.push(b);
      } else if (b.includes("modal_placeholders")) {
        categories.push("founder_review_required");
        blockers.push(b);
      } else if (b.includes("empty_visible_cards")) {
        categories.push("content_shell_only");
        blockers.push(b);
      } else if (b.includes("governance_language")) {
        categories.push("blocked_by_copy");
        blockers.push(b);
      } else {
        categories.push("founder_review_required");
        blockers.push(b);
      }
    }
  }

  const galleryMin = brandConfig?.galleryMinimum || 6;
  const galleryReady = renderContract?.summary?.galleryRenderReady ?? 0;
  if (galleryReady < galleryMin) {
    categories.push("blocked_by_images");
    blockers.push(`gallery_render_ready:${galleryReady}/${galleryMin}`);
  }

  if ((ctx.approvedSourcesCount || 0) < 3) {
    categories.push("blocked_by_sources");
    blockers.push("insufficient_approved_sources");
  }

  const uiFallback = factoryRules?.rules?.uiFallback;
  if (uiFallback && !uiFallback.pass) {
    categories.push("blocked_by_renderer_mismatch");
    blockers.push("ui_fallback_active");
  }

  const modelType = brandConfig?.brandModelType || "";
  if (brandConfig?.franchiseLanguageBlocked) {
    const franchiseHits = presentationRows.filter((r) =>
      /\b(franchise flag|fdd|item\s*19)\b/i.test(`${r.title}\n${r.body}`)
    );
    if (franchiseHits.length) {
      categories.push("blocked_by_model_fit");
      blockers.push(`franchise_language_rows:${franchiseHits.length}`);
    }
  }

  const blocks = brandApi?.brandExplorer?.blocks || [];
  const emptyBullets = presentationRows.filter(
    (r) => r.visible !== false && nz(r.title) && !nz(r.body) && !r.slotKey.startsWith("materials.gallery.")
  );
  if (emptyBullets.length > 2) {
    categories.push("content_shell_only");
    blockers.push(`empty_cards:${emptyBullets.length}`);
  }

  const underpopulatedTabs = (ctx.knowledgePack?.tabCoverage || []).filter((t) => t.status === "empty");
  if (underpopulatedTabs.length > 2) {
    categories.push("content_shell_only");
    blockers.push(`underpopulated_tabs:${underpopulatedTabs.map((t) => t.tab).join(",")}`);
  }

  if (renderContract?.summary?.registryOnlyCount > 0) {
    categories.push("blocked_by_images");
    blockers.push(`registry_only_assets:${renderContract.summary.registryOnlyCount}`);
  }

  const uniqueCategories = [...new Set(categories)];
  const pass = uniqueCategories.includes("external_owner_ready") && blockers.length === 0;

  let numericScore = 100;
  numericScore -= blockers.length * 8;
  if (galleryReady < galleryMin) numericScore -= 15;
  if (underpopulatedTabs.length) numericScore -= underpopulatedTabs.length * 5;
  numericScore = Math.max(0, Math.min(100, numericScore));

  return {
    version: V36B_VERSION,
    pass,
    numericScore,
    categories: uniqueCategories.length ? uniqueCategories : ["content_shell_only"],
    blockers: [...new Set(blockers)],
    ruleDetail: rule,
  };
}

function buildBatchQueueStatus(brandResult) {
  const {
    brandSlug,
    approvedSourcesCount = 0,
    knowledgePack,
    renderContract,
    presentationPlan,
    existingPlan,
    externalOwnerScore,
    factoryReport,
  } = brandResult;

  const flags = {
    source_ready: approvedSourcesCount >= 3,
    knowledge_pack_ready: Boolean(knowledgePack),
    visual_asset_pack_ready: Boolean(factoryReport?.assetPack),
    render_ready: renderContract?.pass === true,
    presentation_plan_ready: presentationPlan?.pass === true,
    copy_ready: (factoryReport?.copyGovernancePlan?.founderReviewQueue?.length || 0) === 0,
    external_owner_ready: externalOwnerScore?.pass === true,
    draft_ready: (factoryReport?.draftPlan?.presentationPatches?.length || 0) > 0,
    founder_review_required: externalOwnerScore?.categories?.includes("founder_review_required") || false,
    blocked_by_sources: !approvedSourcesCount || approvedSourcesCount < 3,
    blocked_by_images: externalOwnerScore?.categories?.includes("blocked_by_images") || false,
    blocked_by_copy: externalOwnerScore?.categories?.includes("blocked_by_copy") || false,
    blocked_by_model_ambiguity:
      externalOwnerScore?.categories?.includes("blocked_by_model_fit") || false,
    blocked_by_renderer_mismatch:
      externalOwnerScore?.categories?.includes("blocked_by_renderer_mismatch") || false,
    ready_for_apply_draft:
      (factoryReport?.draftPlan?.presentationPatches?.length || 0) > 0 &&
      !externalOwnerScore?.categories?.includes("blocked_by_copy") &&
      approvedSourcesCount >= 3,
    ready_for_active_approval:
      renderContract?.pass &&
      externalOwnerScore?.pass &&
      existingPlan?.summary?.externalOwnerReady === existingPlan?.summary?.total &&
      factoryReport?.factoryRules?.pass === true,
  };

  return { brandSlug, flags, summary: Object.entries(flags).filter(([, v]) => v).map(([k]) => k) };
}

function buildExceptionReviewReport(brandResult) {
  const { brandSlug, brandConfig, presentationRows = [], factoryReport, knowledgePack } = brandResult;
  const automated = [];
  const founderExceptions = [];

  for (const row of presentationRows) {
    const audit = auditPresentationRowExternalOwner(row);
    if (audit.hits.some((h) => h.patternId === "http_url" && row.slotKey !== "footprint.openings")) {
      automated.push({ slotKey: row.slotKey, issue: "visible_url", action: "auto_block" });
    }
    if (audit.hits.some((h) => h.patternId === "sources_block")) {
      automated.push({ slotKey: row.slotKey, issue: "sources_block", action: "auto_block" });
    }
  }

  if (factoryReport?.factoryRules?.rules?.gallery && !factoryReport.factoryRules.rules.gallery.pass) {
    automated.push({ issue: "gallery_not_render_ready", action: "auto_block" });
  }

  for (const gate of factoryReport?.draftPlan?.pendingGovernanceGates || []) {
    if (gate === "standard_detail_governance_review") {
      founderExceptions.push({
        type: "strategic_positioning",
        detail: "Standard detail governance sign-off required before active approval",
      });
    }
  }

  if (brandConfig?.brandModelType?.includes("affiliation")) {
    founderExceptions.push({
      type: "ambiguous_brand_model_framing",
      detail: "Confirm affiliation vs franchise language for owner-facing economics and standards sections",
    });
  }

  const weakEvidence = (knowledgePack?.knownUnknowns || []).filter((u) => u.includes("registry"));
  for (const w of weakEvidence) {
    founderExceptions.push({ type: "weak_source_evidence", detail: w });
  }

  return {
    brandSlug,
    automatedCatchList: automated,
    founderReviewExceptions: founderExceptions,
    doNotAskFounderToCatch: [
      "empty bullets",
      "visible URLs in non-openings slots",
      "IMAGE placeholders",
      "generic fallback cards (COMM_STATIC, LOY_DEMAND, proof fallbacks)",
      "FDD language when copy-governance sanitizer applies",
      "wrong-brand images when factory image rules pass",
    ],
  };
}

function diagnoseDesignHotels(brandResult) {
  const {
    presentationRows = [],
    renderContract,
    externalOwnerScore,
    factoryReport = {},
    knowledgePack,
    brandApi,
  } = brandResult;
  const failures = {
    tabContentContract: [],
    sourceUrlInternalLanguage: [],
    renderReadiness: [],
    modelFit: [],
    modalPlaceholders: [],
    fallbackRenderer: [],
    beforeActiveApproval: [],
  };

  const tabAudit = auditPresentationRowsAgainstContract(
    presentationRows,
    brandApi?.brandExplorer?.blocks || factoryReport?.brandApi?.brandExplorer?.blocks
  );
  failures.tabContentContract = [
    ...(tabAudit.contract?.renderedButUnderdocumented || []),
    ...(tabAudit.documentedButNotRenderedRows || []),
    ...(tabAudit.contract?.hardcodedFallbackSurfaces?.map((f) => f.id) || []),
  ];
  if (tabAudit.documentedButNotRenderedRows?.length) {
    failures.tabContentContract.push("materials.caseStudy_unwired");
  }

  for (const row of presentationRows) {
    const audit = auditPresentationRowExternalOwner(row);
    if (audit.hits.some((h) => ["sources_block", "http_url", "brand_verified"].includes(h.patternId))) {
      failures.sourceUrlInternalLanguage.push(row.slotKey);
    }
    if (row.slotKey === "footprint.openings") {
      const modalEmpty = [
        row.caseSummaryOverview,
        row.caseSummaryBrandRelevance,
        row.caseSummaryOwnerObjective,
      ].filter((v) => !nz(v) || v === "—").length;
      if (modalEmpty >= 2) failures.modalPlaceholders.push(row.recordId);
    }
  }

  if (renderContract && !renderContract.pass) {
    failures.renderReadiness.push(
      `gallery ${renderContract.summary.galleryRenderReady}/${renderContract.summary.galleryMinimum}`,
      `property examples ${renderContract.summary.propertyExamplesRenderReady}/${renderContract.summary.propertyExampleMinimum}`,
      `scenarios ${renderContract.summary.scenariosRenderReady}/${renderContract.summary.scenarioMinimum}`
    );
  }

  const standardsRows = presentationRows.filter((r) => r.slotKey.startsWith("standards."));
  if (standardsRows.length < 2) {
    failures.modelFit.push("standards table underpopulated for affiliation model");
  }
  const loyaltyRows = presentationRows.filter((r) => r.slotKey.startsWith("loyalty."));
  if (loyaltyRows.filter((r) => nz(r.body)).length < 3) {
    failures.modelFit.push("loyalty mechanics/proof coverage incomplete");
  }
  const economicsRows = presentationRows.filter((r) => r.slotKey.startsWith("economics."));
  if (economicsRows.some((r) => /\bfdd\b|\bitem\s*19\b/i.test(`${r.title}\n${r.body}`))) {
    failures.modelFit.push("economics still contains FDD-oriented language");
  }

  if (factoryReport?.factoryRules?.rules?.uiFallback && !factoryReport.factoryRules.rules.uiFallback.pass) {
    failures.fallbackRenderer.push(
      ...(factoryReport.factoryRules.rules.uiFallback.risks || []).map((r) => r.surface || r.issue)
    );
  }
  failures.fallbackRenderer.push(...ATELIER_SCENARIO_FALLBACK_TITLES.map((t) => `scenario_fallback:${t}`));
  failures.fallbackRenderer.push(...ATELIER_PROOF_FALLBACK_HEADS.slice(0, 3).map((t) => `proof_fallback:${t}`));

  failures.beforeActiveApproval = [
    "Materialize gallery Image attachments (6 visible imageUrl)",
    "Complete standards owner table with affiliation-safe governance language",
    "Fix loyalty KPI/proof/watchouts coverage",
    "Resolve footprint openings modal placeholders (Case Summary columns or 5+ paragraph Body)",
    "Remove any visible source URLs / Sources: blocks from owner copy",
    "Pass external owner readiness score without UI fallback risks",
    "Founder visual review after draft apply — do not conflate with active approval",
  ];

  return {
    brand: "Design Hotels",
    slug: "design-hotels",
    externalOwnerScore: externalOwnerScore?.numericScore,
    completeBuildHalted: knowledgePack?.completeBuildStatus?.halted,
    failureCategories: failures,
    summary:
      "Design Hotels fails active-profile readiness primarily on render-readiness (gallery/property/scenario images), standards/loyalty model-fit gaps, modal placeholders on footprint.openings, and UI fallback surfaces masking empty slots.",
  };
}

function diagnoseSlh(brandResult) {
  const { renderContract, externalOwnerScore, factoryReport, presentationPlan } = brandResult;
  const draftPatches = factoryReport?.draftPlan?.presentationPatches?.length || 0;
  const canProceedDraft =
    draftPatches > 0 &&
    (factoryReport?.approvedSourcesCount || 0) >= 3 &&
    !externalOwnerScore?.categories?.includes("blocked_by_copy");

  return {
    brand: "Small Luxury Hotels of the World",
    slug: "small-luxury-hotels-of-the-world",
    canProceedToApplyDraftUnderV36B: canProceedDraft && presentationPlan?.summary?.total > 0,
    blockedBeforeDraftApply: [
      ...(renderContract?.pass ? [] : ["render_not_ready_in_live_api — draft apply would materialize images"]),
      ...(factoryReport?.factoryRules?.pass ? [] : factoryReport?.factoryRules?.blockers || []),
      ...(presentationPlan?.pass ? [] : ["presentation_plan_validation_failed"]),
    ],
    recommendation: canProceedDraft
      ? "SLH may proceed to apply-draft under v34D/v36B IF founder accepts materialization of 12 presentation patches + 9 registry creates; live API is 0/6 gallery until draft apply completes."
      : "SLH blocked before draft apply — resolve factory rules and presentation plan contract failures first.",
    draftPatchCount: draftPatches,
    galleryRenderReady: renderContract?.summary?.galleryRenderReady ?? 0,
    registryOnlyCount: renderContract?.summary?.registryOnlyCount ?? 0,
  };
}

function diagnoseTribute(brandResult) {
  const { renderContract, externalOwnerScore, factoryReport, knowledgePack } = brandResult;
  const passes = [];
  const fails = [];
  const gaps = [];

  if ((knowledgePack?.tabCoverage || []).filter((t) => t.status === "populated").length >= 7) {
    passes.push("major tabs populated");
  } else {
    fails.push("tab underpopulation");
  }
  if (renderContract?.pass) passes.push("render readiness");
  else fails.push(`gallery ${renderContract?.summary?.galleryRenderReady}/${renderContract?.summary?.galleryMinimum}`);
  if (externalOwnerScore?.numericScore >= 70) passes.push("external owner score acceptable");
  else fails.push(`external owner score ${externalOwnerScore?.numericScore}`);

  if (!factoryReport?.factoryRules?.pass) {
    gaps.push(...(factoryReport?.factoryRules?.blockers || []).slice(0, 5));
  }
  gaps.push("Founder visual review required before active approval even when required-section score is 100%");

  return {
    brand: "Tribute Portfolio",
    slug: "tribute-portfolio",
    benchmarkRole: "closest lifestyle collection benchmark for Design Hotels / SLH",
    passes,
    fails,
    gapsBeforeActiveApproval: gaps,
    fasterThanDesignSlh:
      renderContract?.pass &&
      externalOwnerScore?.numericScore >= 70 &&
      (factoryReport?.factoryRules?.blockers?.length || 0) < 3,
    readyForActiveProfile: knowledgePack?.completeBuildStatus?.readyForActiveProfile ?? false,
  };
}

function buildExternalOwnerReadinessMarkdown(brandResult) {
  const { brandSlug, brandConfig, externalOwnerScore, renderContract, presentationPlan } = brandResult;
  const lines = [];
  lines.push(`# External Owner Readiness — ${brandConfig?.name || brandSlug} (v36B)`);
  lines.push("");
  lines.push(`- Numeric score: **${externalOwnerScore?.numericScore ?? "—"}/100**`);
  lines.push(`- Pass: **${externalOwnerScore?.pass ? "yes" : "no"}**`);
  lines.push(`- Categories: ${(externalOwnerScore?.categories || []).join(", ")}`);
  lines.push("");
  if (externalOwnerScore?.blockers?.length) {
    lines.push("## Blockers");
    for (const b of externalOwnerScore.blockers) lines.push(`- ${b}`);
    lines.push("");
  }
  lines.push("## Render readiness");
  lines.push(
    `- Gallery: ${renderContract?.summary?.galleryRenderReady ?? 0}/${renderContract?.summary?.galleryMinimum ?? 6}`
  );
  lines.push(
    `- Property examples: ${renderContract?.summary?.propertyExamplesRenderReady ?? 0}/${renderContract?.summary?.propertyExampleMinimum ?? 3}`
  );
  lines.push("");
  lines.push("## Presentation plan");
  lines.push(
    `- Planned patches: ${presentationPlan?.summary?.total ?? 0}; external-owner ready: ${presentationPlan?.summary?.externalOwnerReady ?? 0}`
  );
  return lines.join("\n");
}

export async function validateBrandV36BContracts(brandSlug, { dryRun = true } = {}) {
  const loadCtx = await loadBrandFactoryContext(brandSlug);
  const brandConfig = getActiveProfileBrandConfig(brandSlug) || loadCtx.activeProfileConfig;

  const assetPack = await buildActiveProfileAssetPack({
    brandSlug,
    presentationRows: loadCtx.presentationRows,
    registryAssets: loadCtx.registryAssets,
    brandApi: loadCtx.brandApi,
  });

  const draftPlan = buildActiveProfileDraftPlan({
    brandSlug,
    assetPack,
    presentationRows: loadCtx.presentationRows,
    brandBasics: loadCtx.brandBasics,
    brandApi: loadCtx.brandApi,
  });

  const sourceResolution = await fetchAndResolveApprovedBrandSources({
    recordId: loadCtx.brand.recordId,
    companyDomains: brandConfig?.officialSourceDomains || [],
  }).catch(() => ({ sources: [] }));

  const approvedSources = resolveApprovedBrandSources(sourceResolution.sources || [], {
    recordId: loadCtx.brand.recordId,
    companyDomains: brandConfig?.officialSourceDomains || [],
  });

  const copyGovernancePlan = buildCopyGovernancePlan({
    brandSlug,
    presentationRows: loadCtx.presentationRows,
    brandConfig,
    approvedSources,
    assetPack,
  });

  const presentationRows = loadCtx.presentationRows || [];
  const ctx = {
    brandSlug,
    brandConfig,
    presentationRows,
    brandApi: loadCtx.brandApi,
    assetPack,
    registryAssets: loadCtx.registryAssets || [],
    approvedSources,
    approvedSourcesCount: approvedSources.length,
    copyGovernancePlan,
    factoryRules: loadCtx.factoryRules,
    completeBuildReport: loadCtx.completeBuildReport,
    finalQa: null,
    visualReport: null,
    draftPlan,
  };

  const factoryReport = {
    assetPack,
    draftPlan,
    copyGovernancePlan,
    factoryRules: loadCtx.factoryRules,
    brandApi: loadCtx.brandApi,
    approvedSourcesCount: approvedSources.length,
  };

  const knowledgePack = buildBrandKnowledgePack({
    ...ctx,
    brandSlug,
  });

  const renderContract = assetPack
    ? extendAssetPackWithRenderReadiness(assetPack, {
        presentationRows,
        brandApi: loadCtx.brandApi,
        registryAssets: loadCtx.registryAssets || [],
      })
    : null;

  const presentationPlan = buildPresentationPlanFromDraftPlan(draftPlan, ctx);
  const existingPlan = validateExistingPresentationRowsAsPlan(presentationRows, ctx);

  const externalOwnerScore = scoreExternalOwnerReadiness({
    ...ctx,
    knowledgePack,
    renderContract,
  });

  const batchQueue = buildBatchQueueStatus({
    brandSlug,
    approvedSourcesCount: approvedSources.length,
    knowledgePack,
    renderContract,
    presentationPlan,
    existingPlan,
    externalOwnerScore,
    factoryReport,
  });

  const exceptionReport = buildExceptionReviewReport({
    brandSlug,
    brandConfig,
    presentationRows,
    factoryReport,
    knowledgePack,
  });

  return {
    brandSlug,
    dryRun,
    brandConfig,
    brandApi: loadCtx.brandApi,
    presentationRows,
    knowledgePack,
    fullTabContentContract: buildFullTabContentContract(),
    tabContractAudit: auditPresentationRowsAgainstContract(
      presentationRows,
      loadCtx.brandApi?.brandExplorer?.blocks
    ),
    renderContract,
    presentationPlan,
    existingPresentationPlan: existingPlan,
    externalOwnerScore,
    batchQueue,
    exceptionReport,
    factoryReportSummary: {
      stage: "copy-governance",
      factoryPass: loadCtx.factoryRules?.pass,
      blockers: loadCtx.factoryRules?.blockers?.slice(0, 10),
      draftPatchCount: draftPlan?.presentationPatches?.length || 0,
    },
    factoryReport: {
      assetPack,
      draftPlan,
      copyGovernancePlan,
      factoryRules: loadCtx.factoryRules,
      brandApi: loadCtx.brandApi,
      approvedSourcesCount: approvedSources.length,
    },
  };
}

export async function runV36BContractValidation({ brands = DEFAULT_TEST_BRANDS, dryRun = true } = {}) {
  const brandResults = [];
  for (const slug of brands) {
    brandResults.push(await validateBrandV36BContracts(slug, { dryRun }));
  }

  const report = {
    version: V36B_VERSION,
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "live",
    airtableModified: false,
    brands: brandResults.map((b) => b.brandSlug),
    fullTabContentContract: buildFullTabContentContract(),
    brandResults,
    diagnoses: {
      designHotels: diagnoseDesignHotels(brandResults.find((b) => b.brandSlug === "design-hotels") || {}),
      slh: diagnoseSlh(brandResults.find((b) => b.brandSlug === "small-luxury-hotels-of-the-world") || {}),
      tribute: diagnoseTribute(brandResults.find((b) => b.brandSlug === "tribute-portfolio") || {}),
    },
    batchQueueAggregate: brandResults.map((b) => b.batchQueue),
  };

  return report;
}

export function writeV36BReports(report, rootDir = ROOT) {
  const reportsDir = path.join(rootDir, "reports");
  const docsDir = path.join(rootDir, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [
    "# Brand Explorer v36B Contract Validation",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** — no Airtable writes`,
    "",
    "## Batch queue summary",
    "",
  ];
  for (const b of report.batchQueueAggregate) {
    mdLines.push(`### ${b.brandSlug}`);
    mdLines.push(`- Ready flags: ${b.summary.join(", ") || "none"}`);
    mdLines.push("");
  }
  mdLines.push("## Design Hotels diagnosis");
  mdLines.push("");
  mdLines.push(report.diagnoses.designHotels.summary);
  mdLines.push("");
  mdLines.push("## SLH readiness");
  mdLines.push("");
  mdLines.push(report.diagnoses.slh.recommendation);
  mdLines.push("");
  mdLines.push("## Tribute benchmark");
  mdLines.push("");
  mdLines.push(`Faster than Design/SLH: ${report.diagnoses.tribute.fasterThanDesignSlh}`);
  mdLines.push("");

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const docPath = path.join(docsDir, "brand-explorer-v36b-contract-validation.md");
  fs.writeFileSync(
    docPath,
    `# Brand Explorer v36B Contract Validation\n\nRead-only contract modules.\n\nSee \`reports/${REPORT_MD}\`.\n`
  );

  for (const brandResult of report.brandResults) {
    const fileKey = KNOWLEDGE_PACK_FILE_KEYS[brandResult.brandSlug] || brandResult.brandSlug;
    const kpPath = path.join(reportsDir, `brand-knowledge-pack-${fileKey}-v36b.json`);
    fs.writeFileSync(kpPath, JSON.stringify(brandResult.knowledgePack, null, 2));

    if (EXTERNAL_OWNER_MD_BRANDS.has(brandResult.brandSlug)) {
      const eoPath = path.join(reportsDir, `external-owner-readiness-${fileKey}-v36b.md`);
      fs.writeFileSync(eoPath, buildExternalOwnerReadinessMarkdown(brandResult));
    }
  }

  return { jsonPath, mdPath, docPath };
}

export { KNOWLEDGE_PACK_VERSION };
