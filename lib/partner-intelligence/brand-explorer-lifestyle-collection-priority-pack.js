/**
 * Brand Explorer Lifestyle / Independent Collection Priority Pack v35.
 *
 * Read-only audit for affiliation-style and soft-collection brands.
 */
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { normalizeRegistryRecordExtended } from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";
import {
  ACTIVE_PROFILE_GALLERY_MINIMUM,
  evaluateAllFactoryRules,
  isHiddenPresentationRow,
} from "./brand-explorer-active-profile-factory-rules.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { resolveBrandTarget } from "./brand-explorer-brand-target-resolver.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { fetchAndResolveApprovedBrandSources, resolveApprovedBrandSources } from "./brand-source-auto-resolver.js";
import {
  buildDraftApplyCommand,
  buildActiveApprovalCommand,
} from "./brand-explorer-active-profile-staged-apply.js";
import { isGalleryImageSlot, isVisualImageSlot } from "./brand-explorer-brand-asset-image-governance.js";
import { isPropertyExampleTitle } from "./brand-explorer-footprint-opening-image-governance.js";

export const PRIORITY_PACK_VERSION = "v35";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

export const LIFESTYLE_PRIORITY_BRANDS = Object.freeze([
  {
    priority: 1,
    inputSlug: "design-hotels",
    displayName: "Design Hotels",
    modelType: "affiliation_collection",
    parentHint: "Marriott affiliate / curation platform",
  },
  {
    priority: 2,
    inputSlug: "small-luxury-hotels-of-the-world",
    altInputs: ["small-luxury-hotels", "slh"],
    displayName: "Small Luxury Hotels of the World",
    modelType: "affiliation_collection",
    parentHint: "SLH independent luxury consortium",
  },
  {
    priority: 3,
    inputSlug: "autograph-collection",
    displayName: "Autograph Collection",
    modelType: "soft_brand",
    parentHint: "Marriott soft collection",
  },
  {
    priority: 4,
    inputSlug: "tribute-portfolio",
    altInputs: ["tribute-collection"],
    displayName: "Tribute Portfolio",
    modelType: "soft_brand",
    parentHint: "Marriott Tribute Portfolio (not a separate Tribute Collection entity)",
    namingNote:
      "“Tribute Collection” in owner-facing copy usually means Marriott Tribute Portfolio. No distinct Brand Setup row should be created for a separate “Tribute Collection”.",
  },
  {
    priority: 5,
    inputSlug: "vignette-collection",
    displayName: "Vignette Collection",
    modelType: "soft_brand",
    parentHint: "IHG soft collection",
  },
  {
    priority: 6,
    inputSlug: "mgallery",
    altInputs: ["mgallery-collection"],
    displayName: "MGallery",
    modelType: "soft_brand",
    parentHint: "Accor soft collection",
  },
  {
    priority: 7,
    inputSlug: "handwritten-collection",
    displayName: "Handwritten Collection",
    modelType: "soft_brand",
    parentHint: "IHG soft collection",
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function fetchBrandApiShape(brandRecordId) {
  const req = { query: { brandId: brandRecordId, refresh: "1" }, headers: {} };
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
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error?.message || `Airtable list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);

  return rows.map((rec) => {
    const f = rec.fields || {};
    const imageAtt = Array.isArray(f.Image) ? f.Image[0] : null;
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"] || f.Slot),
      title: nz(f.Title),
      body: nz(f.Body),
      externalDisplayStatus: nz(f["External Display Status"]),
      visible: !/do not display|internal only/i.test(nz(f["External Display Status"])),
      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",
      registryLinkIds: Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"] : [],
    };
  });
}

function summarizePresentationCoverage(presentationRows = [], brandApi = null) {
  const visible = presentationRows.filter((r) => r.visible !== false && !isHiddenPresentationRow(r));
  const galleryRows = visible.filter((r) => isGalleryImageSlot(r.slotKey));
  const galleryWithImage = galleryRows.filter((r) => nz(r.imageUrl));
  const propertyRows = visible.filter(
    (r) => r.slotKey === "footprint.openings" || isPropertyExampleTitle(r.title)
  );
  const propertyWithImage = propertyRows.filter((r) => nz(r.imageUrl));
  const scenarioRows = visible.filter((r) => /^overview\.scenario\.\d+$/.test(r.slotKey));
  const scenarioWithImage = scenarioRows.filter((r) => nz(r.imageUrl));
  const proofRows = visible.filter((r) => /^overview\.proof/.test(r.slotKey));
  const standardRows = visible.filter((r) => /standard/i.test(r.slotKey) || /standards\./i.test(r.slotKey));

  const apiGallery = (brandApi?.brandExplorer?.blocks || []).filter((b) => isGalleryImageSlot(b?.slotKey));
  const apiGalleryWithUrl = apiGallery.filter((b) => nz(b.imageUrl));

  return {
    totalRows: presentationRows.length,
    visibleRows: visible.length,
    galleryPresentationWithImage: galleryWithImage.length,
    galleryApiWithImageUrl: apiGalleryWithUrl.length,
    propertyExamplesVisible: propertyRows.length,
    propertyExamplesWithImage: propertyWithImage.length,
    scenarioCardsVisible: scenarioRows.length,
    scenarioCardsWithImage: scenarioWithImage.length,
    proofCardsVisible: proofRows.length,
    standardDetailRows: standardRows.length,
  };
}

function summarizeRegistryCoverage(registryAssets = []) {
  const approved = registryAssets.filter((a) =>
    /approved|yes/i.test(nz(a.approvedForExplorerUse || a.explorerApprovalStatus))
  );
  const withSourceUrl = registryAssets.filter((a) => nz(a.sourceUrl));
  const visualLinked = registryAssets.filter((a) => /gallery|property|scenario|image/i.test(nz(a.assetType)));
  return {
    total: registryAssets.length,
    approvedForExplorer: approved.length,
    withDurableSourceUrl: withSourceUrl.length,
    visualCandidates: visualLinked.length,
  };
}

function assessAssetPackFeasibility({
  brandSlug,
  activeProfileConfig,
  presentationCoverage,
  registryCoverage,
  sourceCoverage,
  factoryRules,
}) {
  const hasFactoryConfig = Boolean(activeProfileConfig);
  const galleryReady =
    presentationCoverage.galleryApiWithImageUrl >= ACTIVE_PROFILE_GALLERY_MINIMUM ||
    presentationCoverage.galleryPresentationWithImage >= ACTIVE_PROFILE_GALLERY_MINIMUM;
  const propertyReady = presentationCoverage.propertyExamplesWithImage >= 3;
  const scenarioReady = presentationCoverage.scenarioCardsWithImage >= 3;
  const sourceReady = sourceCoverage.approved >= 3;
  const registryReady = registryCoverage.approvedForExplorer >= 6;

  let readinessBand = "blocked_no_factory_config";
  if (hasFactoryConfig) {
    if (galleryReady && propertyReady && scenarioReady && sourceReady) readinessBand = "full";
    else if (galleryReady || propertyReady || sourceReady) readinessBand = "partial";
    else readinessBand = "blocked_no_assets";
  } else if (galleryReady && propertyReady && sourceReady) {
    readinessBand = "config_required_but_assets_present";
  } else if (sourceReady || presentationCoverage.visibleRows > 20) {
    readinessBand = "source_or_presentation_present_config_required";
  } else {
    readinessBand = "early_stage_needs_source_capture";
  }

  return {
    hasActiveProfileFactoryConfig: hasFactoryConfig,
    readinessBand,
    canRunFactoryPreflight: hasFactoryConfig,
    canRunFactoryAssetPack: hasFactoryConfig,
    gallerySixFeasible: galleryReady || sourceCoverage.approved >= 1,
    propertyExamplesFeasible: propertyReady || sourceCoverage.approved >= 2,
    scenarioImagesFeasible: scenarioReady,
    officialSourceUrls: sourceCoverage.approved,
    propertyImageUrls: presentationCoverage.propertyExamplesWithImage,
    momentumSourceCandidates: sourceCoverage.extractable,
    standardDetailEvidence: presentationCoverage.standardDetailRows > 0,
    proofCardEvidence: presentationCoverage.proofCardsVisible > 0,
    factoryPreflightPass: factoryRules?.pass ?? false,
    notes: hasFactoryConfig
      ? []
      : ["Brand not yet registered in brand-explorer-active-profile-brand-config.js — factory preflight/asset-pack require v35+ config registration."],
  };
}

function scoreStrategicRelevance(brandAudit) {
  const m = brandAudit.modelMeta || {};
  const asset = brandAudit.assetPackFeasibility || {};
  const pre = brandAudit.factoryPreflight || {};
  const cov = brandAudit.presentationCoverage || {};
  const src = brandAudit.sourceCoverage || {};
  const complete = brandAudit.completeBuild || {};

  let speedScore = 0;
  if (asset.readinessBand === "full") speedScore += 30;
  else if (asset.readinessBand === "config_required_but_assets_present") speedScore += 26;
  else if (asset.readinessBand === "source_or_presentation_present_config_required") speedScore += 18;
  else if (asset.readinessBand === "partial") speedScore += 12;
  else speedScore += 4;

  if (complete.readyForActiveProfile === true) speedScore += 20;
  else if (complete.readinessBand === "ready") speedScore += 16;
  else if (complete.readinessBand === "almost_ready") speedScore += 12;
  else if (complete.readinessBand === "blocked") speedScore += 2;
  else speedScore += 6;

  if (src.approved >= 6) speedScore += 14;
  else if (src.approved >= 3) speedScore += 9;
  else if (src.approved >= 1) speedScore += 4;

  if (cov.galleryApiWithImageUrl >= 6) speedScore += 12;
  else if (cov.galleryPresentationWithImage >= 3) speedScore += 6;

  if (pre.pass) speedScore += 10;
  speedScore -= Math.min(24, Math.floor((pre.blockerCount || 0) / 3));

  const ownerFitScore = Math.round(
    (m.independentIdentity || 0) * 0.28 +
      (m.boutiqueCredibility || 0) * 0.22 +
      (m.ownerControl || 0) * 0.22 +
      (m.affiliationFit || 0) * 0.18 +
      (m.storytellingExpectations || 0) * 0.1
  );

  return {
    speedToOwnerReady: Math.max(0, Math.min(100, speedScore)),
    ownerProjectFit: Math.max(0, Math.min(100, ownerFitScore)),
    combined: Math.max(0, Math.min(100, Math.round(speedScore * 0.4 + ownerFitScore * 0.6))),
  };
}

function modelMetaForBrand(target, modelType) {
  const name = nz(target?.name).toLowerCase();
  const isAffiliation = modelType === "affiliation_collection";
  const isMarriottSoft = /marriott|tribute|autograph|design/.test(name);
  const isIhg = /vignette|handwritten/.test(name);
  const isAccor = /mgallery/.test(name);
  const isSlh = /small luxury|slh/.test(name);

  return {
    modelType,
    independentIdentity: isAffiliation ? 95 : isMarriottSoft ? 78 : isIhg ? 72 : 70,
    boutiqueCredibility: isAffiliation ? 92 : isMarriottSoft ? 85 : 80,
    affiliationFit: isAffiliation ? 95 : isMarriottSoft || isIhg || isAccor ? 70 : 65,
    ownerControl: isAffiliation ? 88 : isSlh ? 90 : isMarriottSoft ? 68 : 70,
    conversionFit: isMarriottSoft ? 82 : isIhg ? 78 : isAccor ? 75 : 72,
    distributionPlatform: isMarriottSoft ? 90 : isIhg ? 82 : isAccor ? 80 : isSlh ? 78 : 85,
    standardsIntensity: isAffiliation ? 45 : isMarriottSoft ? 62 : 60,
    storytellingExpectations: isAffiliation ? 95 : 85,
    franchiseLanguageRisk: isAffiliation ? "high_if_forced_into_franchise_copy" : "medium_soft_brand_framing",
    copyGuidance:
      isAffiliation
        ? "Use affiliation / collection / curation / distribution language — not franchise-flag or FDD-style copy."
        : "Use soft-collection / lifestyle affiliation framing within parent platform context.",
  };
}

function buildBrandLens(brandAudit) {
  const slug = brandAudit.slug;
  const model = brandAudit.modelMeta || {};
  if (slug === "design-hotels") {
    return {
      fitSummary:
        "Design Hotels fits independent, design-led, culturally distinctive hotels seeking curated global distribution without a standardized franchise prototype.",
      affiliationVsFranchise:
        "Marriott affiliate / curation platform — not a traditional franchise flag. Owners retain independent identity; Marriott provides Bonvoy distribution and collection credibility.",
      ownerControl: "Higher than soft-brand conversion paths; narrative must preserve property-level design story.",
      storytelling: "High design/storytelling bar — over-branding or generic Marriott copy is a material risk.",
      operationalReadiness: "Property curation and design authenticity matter more than prototype compliance.",
      propertyExamples: brandAudit.presentationCoverage?.propertyExamplesWithImage ?? 0,
      imageAvailability: brandAudit.presentationCoverage?.galleryApiWithImageUrl ?? 0,
      risks: [
        "Zero approved sources in last complete-build snapshot",
        "Critical visual defects halted prior complete build",
        "Factory config not registered — cannot run v34D asset-pack yet",
        "Franchise-language contamination risk if copy uses Marriott flag framing",
      ],
    };
  }
  if (/small-luxury|slh/.test(slug)) {
    return {
      fitSummary:
        "SLH fits independent luxury/boutique hotels seeking global luxury distribution and consortium credibility without a chain flag.",
      affiliationValue: "Collection membership + SLH distribution/loyalty context — not a conversion franchise.",
      luxuryCredibility: "High when property quality and independent character are source-evidenced.",
      ownerControl: "Strong — legal/consortium sensitivity requires careful copy governance.",
      guestExperience: "Boutique/luxury service and property distinctiveness are central.",
      propertyExamples: brandAudit.presentationCoverage?.propertyExamplesWithImage ?? 0,
      imageAvailability: brandAudit.presentationCoverage?.galleryApiWithImageUrl ?? 0,
      risks: [
        "Expansion backlog wave 8 — highest source/image governance complexity",
        "Likely limited Source Library coverage",
        "Factory config not registered",
        "Independent consortium — avoid parent-franchise language",
      ],
    };
  }
  if (["autograph-collection", "tribute-portfolio", "vignette-collection", "mgallery", "handwritten-collection"].includes(slug)) {
    return {
      parentPlatform: model.parentHint || brandAudit.parentCompany,
      softBrandPositioning: "Lifestyle soft collection within parent platform — independent character with parent distribution/loyalty.",
      conversionFit: slug === "tribute-portfolio" ? "Strong for conversions/repositioning with local character" : "Moderate to strong by market",
      ownerFlexibility: "More than rigid flags; less than pure affiliation platforms like Design Hotels / SLH.",
      propertyExamples: brandAudit.presentationCoverage?.propertyExamplesWithImage ?? 0,
      diligenceQuestions: [
        "How much local identity must remain visible post-affiliation?",
        "What standards/PIP obligations apply vs collection guidelines?",
        "How does parent loyalty/distribution offset operating complexity?",
      ],
    };
  }
  return null;
}

function buildFactoryNextSteps(slug) {
  return {
    preflight: `npm run brand-explorer-active-profile-preflight -- --brand ${slug} --dry-run`,
    assetPack: `npm run brand-explorer-active-profile-asset-pack -- --brand ${slug} --dry-run`,
    buildDraft: `npm run brand-explorer-active-profile-build-draft -- --brand ${slug} --dry-run`,
    copyGovernance: `npm run brand-explorer-active-profile-copy-governance -- --brand ${slug} --dry-run`,
    applyDraft: buildDraftApplyCommand(slug),
    founderReview: `npm run brand-explorer-active-profile-founder-review -- --brand ${slug} --dry-run`,
    applyApproved: buildActiveApprovalCommand(slug),
    prerequisite: "Register brand in brand-explorer-active-profile-brand-config.js with affiliation-appropriate copy governance before factory stages.",
  };
}

export async function auditLifestylePriorityBrand(seed, { baseId, apiKey } = {}) {
  const resolvedBaseId = baseId || process.env.AIRTABLE_BASE_ID;
  const resolvedApiKey = apiKey || process.env.AIRTABLE_API_KEY;
  if (!resolvedBaseId || !resolvedApiKey) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  }

  let target;
  let resolutionError = null;
  let tributeCollectionAmbiguity = null;

  try {
    target = await resolveBrandTarget(seed.inputSlug);
  } catch (err) {
    resolutionError = err.message;
    if (seed.altInputs?.length) {
      for (const alt of seed.altInputs) {
        try {
          const altTarget = await resolveBrandTarget(alt);
          if (seed.inputSlug === "tribute-portfolio" && alt === "tribute-collection") {
            tributeCollectionAmbiguity = {
              input: alt,
              resolvedTo: altTarget.name,
              resolvedSlug: altTarget.slug,
              note: seed.namingNote,
            };
          }
          target = altTarget;
          resolutionError = null;
          break;
        } catch {
          /* try next */
        }
      }
    }
  }

  if (!target?.recordId) {
    return {
      priority: seed.priority,
      requestedSlug: seed.inputSlug,
      displayName: seed.displayName,
      found: false,
      resolutionError: resolutionError || "brand_not_found",
      namingNote: seed.namingNote || null,
    };
  }

  const [brandBasics, presentationRows, registryAssetsRaw, brandApi, contractReport, completeBuildReport] =
    await Promise.all([
      fetchBrandBasics(target.recordId),
      listPresentationRows(resolvedBaseId, resolvedApiKey, target.recordId, target.name),
      listRegistryAssetsForBrand(target.recordId).catch(() => []),
      fetchBrandApiShape(target.recordId),
      buildBrandExplorerRequiredSectionPopulationContractReport({ brandIdOrName: target.slug }).catch(() => null),
      buildBrandExplorerCompleteBuildOrchestratorReport({
        brandIdOrName: target.slug,
        targetQuality: "active-profile",
      }).catch(() => null),
    ]);

  const registryAssets = registryAssetsRaw.map(normalizeRegistryRecordExtended);
  const sourceResolution = await fetchAndResolveApprovedBrandSources({
    recordId: target.recordId,
    companyDomains: [],
  }).catch(() => ({ sources: [] }));
  const approvedSources = resolveApprovedBrandSources(sourceResolution.sources || [], {
    recordId: target.recordId,
    companyDomains: [],
  });

  const factoryRules = evaluateAllFactoryRules({
    brandApi,
    presentationRows,
    registryAssets,
    brandConfig: null,
    brandTarget: target,
    contractReport,
    completeBuildReport,
  });

  const presentationCoverage = summarizePresentationCoverage(presentationRows, brandApi);
  const registryCoverage = summarizeRegistryCoverage(registryAssets);
  const sourceCoverage = {
    total: (sourceResolution.sources || []).length,
    approved: approvedSources.length,
    extractable: approvedSources.filter((s) => nz(s.sourceUrl)).length,
  };

  const activeProfileConfig = getActiveProfileBrandConfig(target.slug);
  const assetPackFeasibility = assessAssetPackFeasibility({
    brandSlug: target.slug,
    activeProfileConfig,
    presentationCoverage,
    registryCoverage,
    sourceCoverage,
    factoryRules,
  });

  const completeBrand = completeBuildReport?.brandResults?.[0] || completeBuildReport || null;
  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: target.slug }).catch(() => null);
  const finalQaBrand = finalQaReport?.brandReports?.find((b) => b.brand?.slug === target.slug) || null;

  const modelMeta = {
    ...modelMetaForBrand(target, seed.modelType),
    parentHint: seed.parentHint,
  };

  const audit = {
    priority: seed.priority,
    found: true,
    exactBrandName: target.name,
    slug: target.slug,
    recordId: target.recordId,
    parentCompany: target.resolution?.parentCompany || nz(brandBasics?.["Parent Company"]),
    parentAffiliation: seed.parentHint,
    resolutionSource: target.resolution?.resolutionSource || null,
    namingNote: seed.namingNote || null,
    tributeCollectionAmbiguity,
    companyValidated: Boolean(brandBasics?.["Company Validated"]),
    companyValidationDate: brandBasics?.["Company Validation Date"] || null,
    brandExplorerStatus: {
      presentationRowCount: presentationRows.length,
      visiblePresentationRows: presentationCoverage.visibleRows,
      hasBrandApiShape: Boolean(brandApi),
    },
    finalQa: finalQaBrand
      ? {
          readiness: finalQaBrand.scores?.overallActiveProfileReadiness,
          numeric: finalQaBrand.scores?.overallNumeric,
          defectCount: finalQaBrand.defects?.length || 0,
          critical: finalQaBrand.defects?.filter((d) => d.severity === "critical").length || 0,
          high: finalQaBrand.defects?.filter((d) => d.severity === "high").length || 0,
        }
      : null,
    completeBuild: completeBrand
      ? {
          readinessBand: completeBrand.readinessBand,
          readyForActiveProfile: completeBrand.readyForActiveProfile,
          halted: completeBuildReport?.halted,
          haltReason: completeBuildReport?.haltReason || null,
          sourceCount: completeBuildReport?.stageResults?.find((s) => s.stage === "source_inventory")?.summary
            ?.sourceCount,
          approvedExplorerSources: completeBuildReport?.stageResults?.find((s) => s.stage === "source_inventory")
            ?.summary?.approvedForExplorer,
        }
      : null,
    sourceCoverage,
    presentationCoverage,
    registryCoverage,
    standardDetailGovernance: {
      pass: factoryRules.rules?.standardDetail?.pass ?? null,
      sectionStatus: factoryRules.rules?.standardDetail?.sectionStatus || "unknown",
      needsFounderReview: factoryRules.rules?.standardDetail?.needsFounderReview ?? null,
    },
    factoryPreflight: {
      pass: factoryRules.pass,
      blockerCount: factoryRules.blockers.length,
      galleryBlockers: factoryRules.rules?.gallery?.blockers || [],
      propertyExampleBlockers: factoryRules.rules?.propertyExamples?.blockers || [],
      scenarioBlockers: factoryRules.rules?.scenarioImages?.blockers || [],
      copySafetyBlockers: factoryRules.rules?.copySafety?.blockers || [],
      standardDetailBlockers: factoryRules.rules?.standardDetail?.blockers || [],
      registryTraceabilityBlockers: factoryRules.rules?.registryTraceability?.blockers || [],
      uiFallbackBlockers: factoryRules.rules?.uiFallback?.blockers || [],
      factoryConfigRegistered: Boolean(activeProfileConfig),
      factoryPreflightRunnable: Boolean(activeProfileConfig),
    },
    assetPackFeasibility,
    stagedApplyReadiness: {
      canApplyDraft: Boolean(activeProfileConfig) && assetPackFeasibility.readinessBand !== "early_stage_needs_source_capture",
      founderVisualReviewNeeded: true,
      activeApprovalBlockedUntilVisualPass: true,
    },
    modelMeta,
    brandLens: null,
    strategicScores: null,
    factoryNextSteps: buildFactoryNextSteps(target.slug),
  };

  audit.brandLens = buildBrandLens(audit);
  audit.strategicScores = scoreStrategicRelevance(audit);
  return audit;
}

export async function buildLifestyleCollectionPriorityPackReport() {
  const brands = [];
  for (const seed of LIFESTYLE_PRIORITY_BRANDS) {
    brands.push(await auditLifestylePriorityBrand(seed));
  }

  const ranked = [...brands]
    .filter((b) => b.found)
    .sort((a, b) => (b.strategicScores?.combined || 0) - (a.strategicScores?.combined || 0));

  const speedRanked = [...brands]
    .filter((b) => b.found)
    .sort((a, b) => (b.strategicScores?.speedToOwnerReady || 0) - (a.strategicScores?.speedToOwnerReady || 0));

  const fitRanked = [...brands]
    .filter((b) => b.found)
    .sort((a, b) => (b.strategicScores?.ownerProjectFit || 0) - (a.strategicScores?.ownerProjectFit || 0));

  const slh = brands.find((b) => /small luxury/i.test(b.exactBrandName || ""));
  const designHotels = brands.find((b) => b.slug === "design-hotels");
  const severeBlockers = (b) =>
    (b?.sourceCoverage?.approved || 0) === 0 &&
    (b?.presentationCoverage?.visibleRows || 0) === 0 &&
    !b?.factoryPreflight?.factoryConfigRegistered;

  const activation = {
    firstTwoForLiveOwnerProject: ["design-hotels", slh?.slug || "small-luxury-hotels-of-the-world"],
    firstTwoSevereBlockers: severeBlockers(designHotels) && severeBlockers(slh),
    firstTwoNote: severeBlockers(designHotels) && severeBlockers(slh)
      ? "Strategic priority remains Design Hotels + SLH for independent/lifestyle owner relevance, but both need Source Library capture, presentation buildout, and v35 factory config registration before v34D staged apply."
      : null,
    fastestTechnicalPrepare: speedRanked[0]?.slug || null,
    nextThreeBenchmarks: speedRanked
      .filter((b) => !["design-hotels", slh?.slug].includes(b.slug))
      .slice(0, 3)
      .map((b) => b.slug),
    defer: brands
      .filter(
        (b) =>
          b.found &&
          (b.strategicScores?.speedToOwnerReady || 0) < 12 &&
          (b.strategicScores?.ownerProjectFit || 0) < 80
      )
      .map((b) => b.slug),
  };

  return {
    priorityPackVersion: PRIORITY_PACK_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    guardrails: {
      noAirtableWrites: true,
      noActiveProfileApproval: true,
      companyValidatedUntouched: true,
      noFranchiseLanguageForAffiliationBrands: true,
    },
    namingAmbiguity: {
      tributeCollection: {
        likelyMeans: "Marriott Tribute Portfolio",
        resolvedSlug: "tribute-portfolio",
        createDuplicateBrandRecord: false,
        note: "No distinct Brand Setup entity found for “Tribute Collection”; use tribute-portfolio.",
      },
    },
    brands,
    strategicRanking: ranked.map((b, i) => ({
      rank: i + 1,
      slug: b.slug,
      name: b.exactBrandName,
      combinedScore: b.strategicScores?.combined,
      speedToOwnerReady: b.strategicScores?.speedToOwnerReady,
      ownerProjectFit: b.strategicScores?.ownerProjectFit,
      factoryPreflightPass: b.factoryPreflight?.pass,
      assetPackBand: b.assetPackFeasibility?.readinessBand,
    })),
    speedRanking: speedRanked.map((b, i) => ({
      rank: i + 1,
      slug: b.slug,
      name: b.exactBrandName,
      speedToOwnerReady: b.strategicScores?.speedToOwnerReady,
    })),
    ownerFitRanking: fitRanked.map((b, i) => ({
      rank: i + 1,
      slug: b.slug,
      name: b.exactBrandName,
      ownerProjectFit: b.strategicScores?.ownerProjectFit,
    })),
    recommendedActivationSequence: activation,
    topTwoFactoryNextSteps: Object.fromEntries(
      activation.firstTwoForLiveOwnerProject.map((slug) => [
        slug,
        brands.find((b) => b.slug === slug)?.factoryNextSteps || null,
      ])
    ),
  };
}

export function buildLifestyleCollectionPriorityPackMarkdown(report) {
  const lines = [];
  lines.push(`# Lifestyle / Independent Collection Priority Pack ${PRIORITY_PACK_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** (no Airtable writes)`);
  lines.push("");

  lines.push("## Naming ambiguity");
  lines.push(`- **Tribute Collection** → use **Marriott Tribute Portfolio** (\`tribute-portfolio\`). Do not create a duplicate brand record.`);
  lines.push("");

  lines.push("## Strategic ranking");
  for (const row of report.strategicRanking) {
    lines.push(
      `${row.rank}. **${row.name}** (\`${row.slug}\`) — combined ${row.combinedScore}, speed ${row.speedToOwnerReady}, fit ${row.ownerProjectFit}, asset pack \`${row.assetPackBand}\``
    );
  }
  lines.push("");

  lines.push("## Recommended activation sequence");
  lines.push(`- **First 2 (live owner/project):** ${report.recommendedActivationSequence.firstTwoForLiveOwnerProject.join(", ")}`);
  if (report.recommendedActivationSequence.firstTwoNote) {
    lines.push(`- **Note:** ${report.recommendedActivationSequence.firstTwoNote}`);
  }
  lines.push(`- **Fastest technical prepare:** ${report.recommendedActivationSequence.fastestTechnicalPrepare || "n/a"}`);
  lines.push(`- **Next 3 (benchmarks):** ${report.recommendedActivationSequence.nextThreeBenchmarks.join(", ")}`);
  lines.push(`- **Defer:** ${report.recommendedActivationSequence.defer.join(", ") || "none"}`);
  lines.push("");

  for (const brand of report.brands) {
    lines.push(`## ${brand.displayName || brand.exactBrandName || brand.requestedSlug}`);
    if (!brand.found) {
      lines.push(`- **Not found** — ${brand.resolutionError}`);
      lines.push("");
      continue;
    }
    lines.push(`- Slug: \`${brand.slug}\` · Record: \`${brand.recordId}\``);
    lines.push(`- Parent: ${brand.parentCompany || brand.parentAffiliation || "unknown"}`);
    lines.push(`- Company Validated: **${brand.companyValidated ? "yes" : "no"}**`);
    lines.push(`- Final QA: ${brand.finalQa?.readiness || "n/a"} (${brand.finalQa?.numeric ?? "?"}/100, ${brand.finalQa?.defectCount ?? 0} defects)`);
    lines.push(`- Complete Build: ${brand.completeBuild?.readinessBand || "n/a"} · readyForActiveProfile: ${brand.completeBuild?.readyForActiveProfile ?? "n/a"}`);
    lines.push(`- Sources: ${brand.sourceCoverage.approved} approved / ${brand.sourceCoverage.total} total`);
    lines.push(`- Presentation: ${brand.presentationCoverage.visibleRows} visible rows · gallery API imageUrl ${brand.presentationCoverage.galleryApiWithImageUrl}/6 · property examples w/ image ${brand.presentationCoverage.propertyExamplesWithImage}/3`);
    lines.push(`- Registry: ${brand.registryCoverage.total} rows (${brand.registryCoverage.approvedForExplorer} approved)`);
    lines.push(`- Factory preflight: **${brand.factoryPreflight.pass ? "PASS" : "FAIL"}** (${brand.factoryPreflight.blockerCount} blockers) · config registered: ${brand.factoryPreflight.factoryConfigRegistered ? "yes" : "no"}`);
    lines.push(`- Asset pack feasibility: **${brand.assetPackFeasibility.readinessBand}**`);
    if (brand.brandLens?.fitSummary) lines.push(`- Lens: ${brand.brandLens.fitSummary}`);
    if (brand.brandLens?.risks?.length) {
      lines.push("- Risks:");
      for (const r of brand.brandLens.risks) lines.push(`  - ${r}`);
    }
    lines.push("");
  }

  lines.push("## Factory next steps (top 2)");
  for (const [slug, steps] of Object.entries(report.topTwoFactoryNextSteps || {})) {
    lines.push(`### ${slug}`);
    lines.push("```bash");
    lines.push(steps?.preflight || "");
    lines.push(steps?.assetPack || "");
    lines.push("```");
    lines.push("");
  }

  return lines.join("\n");
}
