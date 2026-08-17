/**
 * Brand Explorer Design Hotels + SLH Asset Pack / Draft Validation v35D.
 *
 * Read-only factory validation after v35C Source Library seeding.
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getCopyGovernanceConfig } from "./brand-explorer-active-profile-copy-governance-config.js";
import { buildBrandExplorerActiveProfileFactoryReport } from "./brand-explorer-active-profile-factory.js";
import { FACTORY_VERSION } from "./brand-explorer-active-profile-factory-rules.js";
import { BRAND_CONFIG_VERSION } from "./brand-explorer-active-profile-brand-config.js";
import { buildDraftApplyCommand } from "./brand-explorer-active-profile-staged-apply.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";

export const V35D_VERSION = "v35D";

const TARGET_BRANDS = Object.freeze([
  {
    slug: "design-hotels",
    recordId: "rec02zPClpWUTCyXM",
    name: "Design Hotels",
    reportKey: "design-hotels",
    modelType: "affiliation_curation_platform",
    copyModel: "Affiliation / curation platform — no franchise-flag language.",
  },
  {
    slug: "small-luxury-hotels-of-the-world",
    recordId: "recjjSnY2opb8P4DG",
    name: "Small Luxury Hotels of the World",
    reportKey: "slh",
    modelType: "independent_luxury_consortium",
    copyModel: "Independent luxury consortium — no parent-brand or franchise language.",
  },
]);

const FACTORY_STAGES = Object.freeze([
  "preflight",
  "asset-pack",
  "build-draft",
  "copy-governance",
  "founder-review",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function fetchAllBrandSources(brandRecordId) {
  const all = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);
  return all;
}

function summarizeSourceIngestion(brand, sources, brandBasics) {
  const fields = brandBasics?.fields || brandBasics || {};
  const approved = sources.filter((s) => isApprovedExplorerSource(s));
  const v35cSources = sources.filter((s) => /v35C evidenceUseCase/i.test(nz(s.notes)));
  return {
    brandSlug: brand.slug,
    recordId: brand.recordId,
    totalSources: sources.length,
    approvedForExplorer: approved.length,
    v35cTaggedSources: v35cSources.length,
    sourcesReadableByFactory: approved.length >= 3,
    companyValidated: Boolean(fields["Company Validated"]),
    companyValidationDate: fields["Company Validation Date"] || null,
    companyValidatedTouched: false,
    approvedSources: approved.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      sourceUrl: s.sourceUrl,
      sourceType: s.sourceType,
      status: s.status,
      approvedForExplorerUse: s.approvedForExplorerUse,
      sourceQuality: s.sourceQuality,
      notesPreview: nz(s.notes).slice(0, 180),
    })),
  };
}

function parseReadyFraction(value) {
  const match = nz(value).match(/^(\d+)\/(\d+)$/);
  if (!match) return { ready: 0, total: 0 };
  return { ready: Number(match[1]), total: Number(match[2]) };
}

function summarizeAssetPack(assetPack, factoryRules) {
  if (!assetPack) {
    return {
      ready: false,
      readinessBand: "missing",
      gallerySlots: 0,
      galleryReady: 0,
      propertyExamples: 0,
      propertyExamplesReady: 0,
      scenarioCards: 0,
      scenariosWithImage: 0,
      registryAssetsUsed: 0,
      woodspringContamination: false,
      blockers: factoryRules?.blockers || [],
    };
  }

  const galleryItems = Array.isArray(assetPack.gallery) ? assetPack.gallery : [];
  const propertyItems = Array.isArray(assetPack.propertyExamples) ? assetPack.propertyExamples : [];
  const scenarioItems = Array.isArray(assetPack.scenarios) ? assetPack.scenarios : [];
  const galleryFraction = parseReadyFraction(assetPack.summary?.galleryReady);
  const propertyFraction = parseReadyFraction(assetPack.summary?.propertyExamplesReady);
  const scenarioFraction = parseReadyFraction(assetPack.summary?.scenariosWithImage);
  const galleryImageUrls = galleryItems.map((s) => s.imageUrl).filter(Boolean);
  const woodspringContamination = [...galleryImageUrls, ...propertyItems.map((p) => p.imageUrl)]
    .filter(Boolean)
    .some((url) => /choicehotels\.com|woodspring/i.test(url));

  const galleryReadyCount =
    galleryFraction.ready ||
    galleryItems.filter((s) => s.passesGalleryRule && s.imageUrl).length;
  const propertyReadyCount =
    propertyFraction.ready ||
    propertyItems.filter((s) => s.passesPropertyExampleRule && s.imageUrl).length;
  const scenarioReadyCount =
    scenarioFraction.ready || scenarioItems.filter((s) => s.imageUrl).length;

  return {
    ready: assetPack.summary?.readinessBand === "full" && !woodspringContamination,
    readinessBand: assetPack.summary?.readinessBand || "unknown",
    partialReadiness: assetPack.summary?.partialReadiness === true,
    gallerySlots: galleryFraction.total || galleryItems.length,
    galleryReady: galleryReadyCount,
    propertyExamples: propertyFraction.total || propertyItems.length,
    propertyExamplesReady: propertyReadyCount,
    scenarioCards: scenarioFraction.total || scenarioItems.length,
    scenariosWithImage: scenarioReadyCount,
    registryAssetsUsed: assetPack.registryCandidateMapping?.length || 0,
    woodspringContamination,
    galleryImageUrls,
    propertyExampleDetails: propertyItems.map((p) => ({
      propertyName: p.propertyName,
      sourcePageUrl: p.sourcePageUrl,
      imageUrl: p.imageUrl,
      passesRule: p.passesPropertyExampleRule,
    })),
    scenarioDetails: scenarioItems.map((s) => ({
      slotKey: s.slotKey,
      title: s.intendedSlot || s.slotKey,
      imageUrl: s.imageUrl,
    })),
    blockers: assetPack.summary?.partialReadiness
      ? ["asset_pack_partial_readiness"]
      : woodspringContamination
        ? ["woodspring_or_choice_gallery_contamination"]
        : [],
  };
}

function summarizeDraftPlan(draftPlan) {
  if (!draftPlan) return { patchCount: 0, sections: [], slotKeys: [] };
  const patches = draftPlan.presentationPatches || draftPlan.patches || [];
  const slotKeys = patches.map((p) => p.slotKey || p.fields?.["Slot Key"]).filter(Boolean);
  const sections = [...new Set(slotKeys.map((k) => k.split(".")[0]))];
  return {
    patchCount: patches.length,
    registryCreates: draftPlan.registryCreates?.length || 0,
    sections,
    slotKeys,
    draftReady: patches.length > 0,
  };
}

function summarizeCopyGovernance(copyGovernancePlan, brand) {
  if (!copyGovernancePlan) {
    return {
      unsafeRows: 0,
      proposedRewrites: 0,
      founderQueue: 0,
      copyModel: brand.copyModel,
    };
  }
  const repairs = copyGovernancePlan.repairs || copyGovernancePlan.copyRepairs || [];
  const founderQueue = copyGovernancePlan.founderQueue || copyGovernancePlan.founderQueueItems || [];
  return {
    unsafeRows: copyGovernancePlan.unsafeRowCount || repairs.length,
    proposedRewrites: repairs.filter((r) => r.strategy === "rewrite").length,
    founderQueue: founderQueue.length,
    genericBoilerplateBlocked: true,
    copyModel: brand.copyModel,
    governanceConfigMode: getCopyGovernanceConfig(brand.slug)?.copyGovernanceMode || null,
    repairs: repairs.slice(0, 20).map((r) => ({
      slotKey: r.slotKey,
      strategy: r.strategy,
      reason: r.reason,
    })),
    founderQueueItems: founderQueue.slice(0, 10),
  };
}

function summarizeFounderReview(report) {
  const rules = report.factoryRules || {};
  const draft = summarizeDraftPlan(report.draftPlan);
  const asset = summarizeAssetPack(report.assetPack, rules);
  const copy = summarizeCopyGovernance(report.copyGovernancePlan, {
    copyModel: "",
  });
  return {
    preflightPass: rules.pass === true,
    galleryPass: rules.rules?.gallery?.pass === true,
    propertyExamplesPass: rules.rules?.propertyExamples?.pass === true,
    scenarioImagesPass: rules.rules?.scenarioImages?.pass === true,
    copySafetyPass: rules.rules?.copySafety?.pass === true,
    registryTraceabilityPass: rules.rules?.registryTraceability?.pass === true,
    blockers: rules.blockers || [],
    stage1DraftApplyCommand: buildDraftApplyCommand(report.brand?.slug),
    wouldApplyPresentationPatches: draft.patchCount,
    wouldApplyRegistryCreates: draft.registryCreates,
    galleryPreview: asset.galleryImageUrls,
    propertyExamplePreview: asset.propertyExampleDetails,
    copyRisks: copy.repairs?.length || 0,
  };
}

function classifyReadiness(sourceIngestion, assetPack, draft, copyGov, founder) {
  if (!sourceIngestion.sourcesReadableByFactory) return "blocked_by_sources";
  if (assetPack.woodspringContamination) return "blocked_by_wrong_gallery_pool";
  if (!assetPack.ready) {
    if ((assetPack.galleryReady || 0) < 6) return "blocked_by_gallery_images";
    if ((assetPack.propertyExamplesReady || 0) < 3) return "blocked_by_property_images";
    if (assetPack.readinessBand === "partial") return "blocked_by_asset_pack";
    return "blocked_by_asset_pack";
  }
  if ((copyGov.founderQueue || 0) > 0) return "copy_governance_founder_review";
  if (!founder.preflightPass) return "blocked_by_factory_rules";
  if (draft.patchCount > 0) return "draft_apply_ready";
  return "source_partial";
}

function buildRecommendation(brandResults) {
  const ranked = [...brandResults].sort((a, b) => {
    const score = (r) => {
      let s = 0;
      if (r.readiness === "draft_apply_ready") s += 100;
      else if (r.readiness === "copy_governance_founder_review") s += 70;
      else if (r.readiness === "source_partial") s += 40;
      if (r.sourceIngestion.approvedForExplorer >= 8) s += 20;
      if (r.assetPackSummary.ready) s += 30;
      if ((r.assetPackSummary.galleryReady || 0) >= 6) s += 15;
      if ((r.assetPackSummary.propertyExamplesReady || 0) >= 3) s += 15;
      if (r.assetPackSummary.woodspringContamination) s -= 50;
      if (r.founderReview.preflightPass) s += 25;
      return s;
    };
    return score(b) - score(a);
  });
  const first = ranked[0];
  const second = ranked[1];
  return {
    draftApplyFirst: first?.brandSlug || null,
    second: second?.brandSlug || null,
    rationale: first
      ? `${first.name} readiness=${first.readiness}; approved sources=${first.sourceIngestion.approvedForExplorer}; asset-pack ready=${first.assetPackSummary.ready}; preflight pass=${first.founderReview.preflightPass}`
      : "No brands validated",
    copyGovernanceBeforeApply:
      (first?.copyGovernanceSummary?.founderQueue || 0) > 0 ||
      (second?.copyGovernanceSummary?.founderQueue || 0) > 0,
    blockedBySourceOrImage: brandResults
      .filter((r) => /blocked_by_/.test(r.readiness))
      .map((r) => ({ slug: r.brandSlug, readiness: r.readiness })),
  };
}

async function validateBrand(brand) {
  const brandBasics = await fetchBrandBasics(brand.recordId);
  const sources = await fetchAllBrandSources(brand.recordId);
  const sourceIngestion = summarizeSourceIngestion(brand, sources, brandBasics);
  const brandConfig = getActiveProfileBrandConfig(brand.slug);

  const stages = {};
  for (const stage of FACTORY_STAGES) {
    stages[stage] = await buildBrandExplorerActiveProfileFactoryReport({
      brandArg: brand.slug,
      stage,
      apply: false,
    });
  }

  const founderReport = stages["founder-review"];
  const assetPackSummary = summarizeAssetPack(
    stages["asset-pack"]?.assetPack,
    stages["preflight"]?.factoryRules
  );
  const draftSummary = summarizeDraftPlan(stages["build-draft"]?.draftPlan);
  const copyGovernanceSummary = summarizeCopyGovernance(
    stages["copy-governance"]?.copyGovernancePlan,
    brand
  );
  const founderReview = summarizeFounderReview(founderReport);
  founderReview.copyRisks = copyGovernanceSummary.proposedRewrites;

  const readiness = classifyReadiness(
    sourceIngestion,
    assetPackSummary,
    draftSummary,
    copyGovernanceSummary,
    founderReview
  );

  return {
    brandSlug: brand.slug,
    reportKey: brand.reportKey,
    name: brand.name,
    recordId: brand.recordId,
    modelType: brand.modelType,
    copyModel: brand.copyModel,
    brandConfigPresent: Boolean(brandConfig),
    sourceIngestion,
    stages: Object.fromEntries(
      Object.entries(stages).map(([k, v]) => [
        k,
        {
          stage: k,
          factoryPass: v.factoryRules?.pass,
          blockerCount: v.factoryRules?.blockers?.length || 0,
          approvedSourcesCount: v.approvedSourcesCount,
        },
      ])
    ),
    factoryReports: stages,
    assetPackSummary,
    draftSummary,
    copyGovernanceSummary,
    founderReview,
    readiness,
  };
}

export function buildV35DMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Design Hotels + SLH Asset Pack / Draft Validation ${V35D_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **dry-run only** (no Airtable writes)`);
  lines.push("");
  lines.push("## Recommendation");
  lines.push(`- Draft apply first: **${report.recommendation.draftApplyFirst}**`);
  lines.push(`- Rationale: ${report.recommendation.rationale}`);
  lines.push(
    `- Copy governance before apply-draft: **${report.recommendation.copyGovernanceBeforeApply ? "yes" : "no"}**`
  );
  lines.push("");
  for (const brand of report.brands) {
    lines.push(`## ${brand.name} (\`${brand.brandSlug}\`)`);
    lines.push(`- Readiness: **${brand.readiness}**`);
    lines.push(`- Sources (approved): ${brand.sourceIngestion.approvedForExplorer}/${brand.sourceIngestion.totalSources}`);
    lines.push(`- Company Validated: ${brand.sourceIngestion.companyValidated} (untouched)`);
    lines.push(`- Asset pack ready: **${brand.assetPackSummary.ready ? "yes" : "no"}** (${brand.assetPackSummary.readinessBand})`);
    lines.push(`- Gallery slots: ${brand.assetPackSummary.galleryReady}/${brand.assetPackSummary.gallerySlots || 6}`);
    lines.push(`- Property examples: ${brand.assetPackSummary.propertyExamplesReady}/${brand.assetPackSummary.propertyExamples || 3}`);
    if (brand.assetPackSummary.woodspringContamination) {
      lines.push("- **Warning:** WoodSpring/Choice gallery contamination detected");
    }
    lines.push(`- Draft patches: ${brand.draftSummary.patchCount}`);
    lines.push(`- Copy rewrites: ${brand.copyGovernanceSummary.proposedRewrites}`);
    lines.push(`- Founder queue: ${brand.copyGovernanceSummary.founderQueue}`);
    lines.push(`- Preflight pass: **${brand.founderReview.preflightPass ? "yes" : "no"}**`);
    if (brand.founderReview.blockers?.length) {
      lines.push(`- Blockers: ${brand.founderReview.blockers.slice(0, 8).join(", ")}`);
    }
    lines.push("");
    lines.push("### Stage 1 apply command");
    lines.push("```bash");
    lines.push(brand.founderReview.stage1DraftApplyCommand);
    lines.push("```");
    lines.push("");
  }
  return lines.join("\n");
}

function buildBrandMarkdown(brand) {
  const lines = [];
  lines.push(`# ${brand.name} — Factory Validation ${V35D_VERSION}`);
  lines.push("");
  lines.push(`- Slug: \`${brand.brandSlug}\``);
  lines.push(`- Readiness: **${brand.readiness}**`);
  lines.push(`- Copy model: ${brand.copyModel}`);
  lines.push("");
  lines.push("## Source Library ingestion");
  lines.push(`- Total sources: ${brand.sourceIngestion.totalSources}`);
  lines.push(`- Approved for Explorer: ${brand.sourceIngestion.approvedForExplorer}`);
  lines.push(`- v35C tagged: ${brand.sourceIngestion.v35cTaggedSources}`);
  lines.push(`- Company Validated: ${brand.sourceIngestion.companyValidated}`);
  lines.push("");
  lines.push("## Asset pack");
  lines.push(`- Ready: ${brand.assetPackSummary.ready}`);
  lines.push(`- Band: ${brand.assetPackSummary.readinessBand}`);
  lines.push(`- Gallery: ${brand.assetPackSummary.galleryReady}/${brand.assetPackSummary.gallerySlots || 6}`);
  lines.push(`- Property examples: ${brand.assetPackSummary.propertyExamplesReady}/${brand.assetPackSummary.propertyExamples || 3}`);
  lines.push(`- Scenarios: ${brand.assetPackSummary.scenariosWithImage}/${brand.assetPackSummary.scenarioCards || 3}`);
  if (brand.assetPackSummary.woodspringContamination) {
    lines.push("- WoodSpring/Choice contamination: **yes**");
  }
  for (const p of brand.assetPackSummary.propertyExampleDetails || []) {
    lines.push(`- Property: ${p.propertyName} — ${p.imageUrl || "no image"}`);
  }
  lines.push("");
  lines.push("## Draft build");
  lines.push(`- Patches: ${brand.draftSummary.patchCount}`);
  lines.push(`- Registry creates (plan): ${brand.draftSummary.registryCreates}`);
  lines.push(`- Sections: ${(brand.draftSummary.sections || []).join(", ")}`);
  lines.push("");
  lines.push("## Copy governance");
  lines.push(`- Proposed rewrites: ${brand.copyGovernanceSummary.proposedRewrites}`);
  lines.push(`- Founder queue: ${brand.copyGovernanceSummary.founderQueue}`);
  lines.push("");
  lines.push("## Founder review preview");
  lines.push(`- Preflight pass: ${brand.founderReview.preflightPass}`);
  lines.push(`- Gallery pass: ${brand.founderReview.galleryPass}`);
  lines.push(`- Property examples pass: ${brand.founderReview.propertyExamplesPass}`);
  lines.push(`- Would apply patches: ${brand.founderReview.wouldApplyPresentationPatches}`);
  return lines.join("\n");
}

export async function buildDesignSlhAssetPackDraftV35DReport() {
  const brands = [];
  for (const brand of TARGET_BRANDS) {
    brands.push(await validateBrand(brand));
  }
  const recommendation = buildRecommendation(brands);
  return {
    version: V35D_VERSION,
    generatedAt: new Date().toISOString(),
    guardrails: {
      noAirtableWrites: true,
      noSourceLibraryChanges: true,
      noPresentationChanges: true,
      noRegistryChanges: true,
      noImageFieldChanges: true,
      noCompanyValidatedChanges: true,
    },
    factoryVersion: FACTORY_VERSION,
    brandConfigVersion: BRAND_CONFIG_VERSION,
    brands,
    recommendation,
    markdown: null,
  };
}

export { buildBrandMarkdown, TARGET_BRANDS as V35D_TARGET_BRANDS };
