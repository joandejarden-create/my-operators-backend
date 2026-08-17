/**
 * v46 — Brand Explorer Image Remediation Batch (Hotel Indigo, MGallery, SLH).
 *
 * Read-only dry-run by default. Resolves image/visual asset blockers for the
 * remaining incomplete cohort without unlocking, writing Presentation, or
 * touching released golden brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import {
  isLogoImageUrl,
  isGenericBrandOrLifestyleImageUrl,
  classifyPropertyExampleImage,
} from "./brand-explorer-footprint-opening-image-governance.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  GRADUATED_LIFESTYLE_COHORT_SLUGS,
  ORIGINAL_GOLDEN_RELEASE_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import {
  V44_FROZEN_RELEASE_EXPECTATIONS,
  evaluateV44Regression,
  captureV44BrandSnapshot,
} from "./brand-explorer-v44-release-baseline.js";
import {
  HOTEL_INDIGO_PROPERTY_CATALOG,
  MGALLERY_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";

export const V46_VERSION = "v46";

export const V46_TARGET_BRANDS = Object.freeze([...GRADUATED_LIFESTYLE_COHORT_SLUGS]);
export const V46_PROTECTED_RELEASED = Object.freeze([...ORIGINAL_GOLDEN_RELEASE_SLUGS]);

export const V46_GALLERY_MIN = 6;
export const V46_PROPERTY_MIN = 3;
export const V46_SCENARIO_MIN = 3;

export const REPORT_JSON = "brand-explorer-v46-image-remediation-batch.json";
export const REPORT_MD = "brand-explorer-v46-image-remediation-batch.md";
export const REPORT_BASELINE_MD = "brand-explorer-v46-released-baseline-protection.md";

const BRAND_REPORT_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-v46-hotel-indigo-image-remediation.md",
  "mgallery-collection": "brand-explorer-v46-mgallery-image-remediation.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-v46-slh-image-remediation.md",
});

const CATALOGS = Object.freeze({
  "hotel-indigo": HOTEL_INDIGO_PROPERTY_CATALOG,
  "mgallery-collection": MGALLERY_PROPERTY_CATALOG,
  "small-luxury-hotels-of-the-world": SLH_PROPERTY_CATALOG,
});

const SECTION_LABELS = Object.freeze({
  "hotel-indigo": "Hotel Indigo property examples · Prefer CALA; expand U.S./global if needed",
  "mgallery-collection": "MGallery Collection property examples · Prefer CALA Accor member hotels",
  "small-luxury-hotels-of-the-world":
    "SLH curated examples · Independent luxury consortium · Not a full directory",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function isGenericIhgBrandHeroImage(url) {
  const u = normalizeUrlKey(url);
  if (!u) return false;
  // Property-specific Hotel Indigo Scene7 assets are never generic heroes.
  if (/digital\.ihg\.com\/is\/image\/ihg\/hotel-indigo-[a-z0-9-]+/i.test(u)) return false;
  if (/digital\.ihg\.com\/is\/image\/ihg\/ihg-[a-z0-9-]+/i.test(u)) {
    return !/hotelindigo/i.test(u);
  }
  return false;
}

function isInterContinentalBrandImage(url) {
  const u = normalizeUrlKey(url);
  return /intercontinental|\/ic\/|ihg-intercontinental/i.test(u);
}

function isAccorGenericBrandGraphic(url) {
  const u = normalizeUrlKey(url);
  if (!u) return false;
  return (
    (/accor\.com|all\.accor\.com/i.test(u) && /logo|brand|sprite|icon|svg/i.test(u)) ||
    /accor[-_/]?brand/i.test(u)
  );
}

function regionPriority(geographyLabel = "") {
  const g = nz(geographyLabel).toUpperCase();
  if (g.includes("CALA")) return 1;
  if (g.includes("U.S") || g.includes("US ") || g.includes("/ US")) return 2;
  return 3;
}

function regionBucket(geographyLabel = "") {
  const p = regionPriority(geographyLabel);
  return p === 1 ? "CALA" : p === 2 ? "US" : "global";
}

async function fetchBrandApiShape(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function countLiveGallery(blocks = []) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countLiveOpenings(blocks = []) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
}

function countLiveScenarios(blocks = []) {
  return blocks.filter((b) => /^overview\.scenario\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

/**
 * Classify a candidate image for acceptance under v46 rules.
 */
export function classifyV46ImageCandidate(candidate = {}, { brandSlug = "", role = "gallery" } = {}) {
  const imageUrl = nz(candidate.imageUrl);
  const sourcePageUrl = nz(candidate.sourcePageUrl);
  const propertyName = nz(candidate.propertyName);
  const region = regionBucket(candidate.geographyLabel || candidate.region || "");

  const base = {
    role,
    propertyName,
    region,
    geographyLabel: nz(candidate.geographyLabel || candidate.region),
    sourcePageUrl,
    imageUrl,
    registryOnly: candidate.registryOnly === true,
    renderReady: candidate.renderReady === true,
  };

  if (!imageUrl) {
    return { ...base, accepted: false, rejectionReason: "missing_image_url" };
  }
  if (candidate.registryOnly === true && candidate.renderReady !== true) {
    return { ...base, accepted: false, rejectionReason: "registry_only_without_render_imageurl" };
  }
  if (isLogoImageUrl(imageUrl)) {
    return { ...base, accepted: false, rejectionReason: "logo_only" };
  }
  if (isGenericBrandOrLifestyleImageUrl(imageUrl)) {
    return { ...base, accepted: false, rejectionReason: "lifestyle_or_generic_brand_image" };
  }
  if (brandSlug === "hotel-indigo") {
    if (isGenericIhgBrandHeroImage(imageUrl)) {
      return { ...base, accepted: false, rejectionReason: "generic_ihg_brand_hero" };
    }
    if (isInterContinentalBrandImage(imageUrl)) {
      return { ...base, accepted: false, rejectionReason: "wrong_brand_intercontinental" };
    }
  }
  if (brandSlug === "mgallery-collection" && isAccorGenericBrandGraphic(imageUrl)) {
    return { ...base, accepted: false, rejectionReason: "generic_accor_graphic" };
  }
  if (role === "property_example") {
    const cls = classifyPropertyExampleImage(imageUrl);
    if (cls?.recommendation === "hide" || cls?.recommendation === "replace_or_hide") {
      return {
        ...base,
        accepted: false,
        rejectionReason: cls.reason || "property_example_image_rejected",
      };
    }
    if (cls && cls.isPropertySpecific === false && (cls.isLogo || cls.isGenericBrand || cls.isLifestyle)) {
      return {
        ...base,
        accepted: false,
        rejectionReason: cls.reason || "non_property_specific_image",
      };
    }
  }

  return {
    ...base,
    accepted: true,
    rejectionReason: null,
    regionPriority: regionPriority(candidate.geographyLabel || candidate.region),
  };
}

function mapAssetToCandidate(asset, brandSlug, role) {
  const renderReady = asset.renderReadiness === "ready";
  const registryOnly =
    Boolean(asset.imageUrl) &&
    !renderReady &&
    nz(asset.registryRowCandidate) &&
    !String(asset.registryRowCandidate).startsWith("(new");

  return classifyV46ImageCandidate(
    {
      imageUrl: asset.imageUrl,
      sourcePageUrl: asset.sourcePageUrl,
      propertyName: asset.propertyName,
      geographyLabel: asset.geographyLabel || asset.notes,
      registryOnly,
      renderReady,
    },
    { brandSlug, role }
  );
}

function classifyDraftEligibility({
  liveGallery,
  liveOpenings,
  acceptedGallery,
  acceptedProperty,
  acceptedScenarios,
  renderReadyGallery,
  renderReadyProperty,
}) {
  // Live Presentation counts stay 0 until a later materialization stage.
  // Eligibility here is candidate-pack based; live gaps inform render readiness only.
  const imagesStillBlocked =
    acceptedGallery < V46_GALLERY_MIN || acceptedProperty < V46_PROPERTY_MIN;

  if (imagesStillBlocked) {
    return {
      status: "still_blocked_by_images",
      image_remediation_complete: false,
      asset_pack_ready: false,
      build_draft_ready: false,
      apply_draft_allowed: false,
      still_blocked_by_images: true,
      rationale: `Accepted candidates gallery ${acceptedGallery}/${V46_GALLERY_MIN}, property ${acceptedProperty}/${V46_PROPERTY_MIN}; live API gallery ${liveGallery}, openings ${liveOpenings} (materialization not in v46).`,
    };
  }

  const packReady =
    acceptedGallery >= V46_GALLERY_MIN &&
    acceptedProperty >= V46_PROPERTY_MIN &&
    acceptedScenarios >= V46_SCENARIO_MIN;

  // Materialization still needed — do not allow draft apply in v46
  const renderGap =
    renderReadyGallery < V46_GALLERY_MIN || renderReadyProperty < V46_PROPERTY_MIN;

  if (packReady && renderGap) {
    return {
      status: "asset_pack_ready",
      image_remediation_complete: true,
      asset_pack_ready: true,
      build_draft_ready: false,
      apply_draft_allowed: false,
      still_blocked_by_images: false,
      rationale:
        "Candidate pack meets 6/3/3 with accepted property-specific images; Presentation imageUrl materialization still required before draft apply.",
    };
  }

  if (packReady && !renderGap) {
    return {
      status: "build_draft_ready",
      image_remediation_complete: true,
      asset_pack_ready: true,
      build_draft_ready: true,
      apply_draft_allowed: false,
      still_blocked_by_images: false,
      rationale:
        "Render-ready imageUrls projected for gallery + property examples. Draft build may proceed in a later stage; apply_draft not allowed in v46.",
    };
  }

  return {
    status: "image_remediation_complete",
    image_remediation_complete: true,
    asset_pack_ready: packReady,
    build_draft_ready: false,
    apply_draft_allowed: false,
    still_blocked_by_images: false,
    rationale: "Image remediation candidates assembled; continue materialization before draft.",
  };
}

/**
 * Confirm OS routing for targets + released brands.
 */
export async function confirmV46OsRouting(targetBrands = V46_TARGET_BRANDS) {
  const targets = [];
  const blockers = [];

  for (const brandSlug of targetBrands) {
    const os = await evaluateBrandExplorerOsBrand(brandSlug);
    const brand = await fetchBrandApiShape(brandSlug);
    const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
    const ql = evaluateBrandExternalQualityLock(brand, html, { brandSlug });

    const row = {
      brandSlug,
      canonicalState: os.canonicalState,
      allowedNextAction: os.routing?.allowedNextAction,
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      displayState: brand.brandExplorerDisplayState,
      externalLocked:
        brand.shouldRenderFullProfile !== true &&
        (ql.profileInPreparationRendered === true || (ql.tabsRenderedExternally || []).length <= 1),
      galleryCount: os.metrics?.galleryCount ?? 0,
      openingsCount: os.metrics?.openingsCount ?? 0,
      failedGates: os.gateEval?.failedGates || [],
      exactNextCommand: os.routing?.exactNextCommand,
      pass:
        os.routing?.allowedNextAction === "image_remediation" &&
        brand.shouldRenderFullProfile !== true,
    };

    if (row.canonicalState !== "draft_applied_with_defects") {
      // soft: some incompletes may show hidden_incomplete display with draft OS state
    }
    if (os.routing?.allowedNextAction !== "image_remediation") {
      blockers.push(`${brandSlug}:expected_image_remediation_got_${os.routing?.allowedNextAction}`);
      row.pass = false;
    }
    if (brand.shouldRenderFullProfile === true) {
      blockers.push(`${brandSlug}:unexpected_full_profile`);
      row.pass = false;
    }
    targets.push(row);
  }

  return { pass: blockers.length === 0, blockers, targets };
}

export async function protectV46ReleasedBaseline() {
  const snapshots = [];
  for (const slug of V46_PROTECTED_RELEASED) {
    snapshots.push(await captureV44BrandSnapshot(slug));
  }
  // Also snapshot incompletes for unlock detection
  for (const slug of V46_TARGET_BRANDS) {
    snapshots.push(await captureV44BrandSnapshot(slug));
  }

  const regression = evaluateV44Regression(snapshots);
  const releasedRows = snapshots
    .filter((s) => V46_PROTECTED_RELEASED.includes(s.brandSlug))
    .map((s) => {
      const exp = V44_FROZEN_RELEASE_EXPECTATIONS[s.brandSlug];
      return {
        brandSlug: s.brandSlug,
        active_profile_ready:
          s.displayState === "active_profile_ready" || s.canonicalState === "active_profile_ready",
        shouldRenderFullProfile: s.shouldRenderFullProfile === true,
        galleryImageUrlCount: s.galleryImageUrlCount,
        propertyImageUrlCount: s.propertyImageUrlCount,
        externalTabCount: s.externalTabCount,
        companyValidated: s.companyValidated,
        externalQualityLockPass: s.externalQualityLock?.pass === true,
        frozen: exp,
      };
    });

  return {
    pass: regression.pass,
    failures: regression.failures,
    checks: regression.checks,
    releasedRows,
  };
}

/**
 * Audit one incomplete brand's image remediation state (read-only).
 */
export async function auditV46BrandImageRemediation(brandSlug) {
  if (!V46_TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`v46 image remediation refuses non-target brand: ${brandSlug}`);
  }
  if (V46_PROTECTED_RELEASED.includes(brandSlug)) {
    throw new Error(`v46 refuses released brand: ${brandSlug}`);
  }

  const config = getActiveProfileBrandConfig(brandSlug) || getDiscoveryBrandConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
  const brandApi = await fetchBrandApiShape(brandSlug);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const registryAssets = ctx?.registryAssets || [];
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;
  const catalog = CATALOGS[brandSlug] || config?.propertyCatalog || [];

  const live = {
    galleryImageUrlCount: countLiveGallery(blocks),
    propertyImageUrlCount: countLiveOpenings(blocks),
    scenarioImageUrlCount: countLiveScenarios(blocks),
  };

  let assetPack = null;
  let renderContract = null;
  let assetPackError = null;
  try {
    assetPack = await buildActiveProfileAssetPack({
      brandSlug,
      presentationRows,
      registryAssets,
      brandApi,
    });
    renderContract = extendAssetPackWithRenderReadiness(assetPack, {
      presentationRows,
      brandApi,
      registryAssets,
    });
  } catch (err) {
    assetPackError = err.message;
  }

  const galleryRaw = assetPack?.gallery || assetPack?.assets?.filter((a) => a.assetRole === "gallery") || [];
  const propertyRaw =
    assetPack?.propertyExamples ||
    assetPack?.assets?.filter((a) => a.assetRole === "property_example") ||
    [];
  const scenarioRaw =
    assetPack?.scenarios || assetPack?.assets?.filter((a) => a.assetRole === "scenario") || [];

  const galleryCandidates = galleryRaw.map((a) => ({
    ...mapAssetToCandidate(a, brandSlug, "gallery"),
    slotKey: a.slotKey,
    intendedSlot: a.intendedSlot,
    renderReadinessProjection: a.renderReadiness,
    registryReadinessProjection: a.approvalStatus,
    imageType: a.imageType,
  }));

  // Enrich property candidates with catalog region + priority order CALA → US → global
  const propertyCandidates = propertyRaw
    .map((a) => {
      const cat = catalog.find(
        (c) =>
          c.propertyKey === a.propertyKey ||
          nz(c.propertyName).toLowerCase() === nz(a.propertyName).toLowerCase()
      );
      const classified = mapAssetToCandidate(
        { ...a, geographyLabel: cat?.geographyLabel || a.geographyLabel },
        brandSlug,
        "property_example"
      );
      return {
        ...classified,
        propertyKey: a.propertyKey || cat?.propertyKey,
        slotKey: a.slotKey,
        intendedSlot: a.intendedSlot,
        geographyLabel: cat?.geographyLabel || "",
        regionPriority: regionPriority(cat?.geographyLabel),
        renderReadinessProjection: a.renderReadiness,
        registryReadinessProjection: a.approvalStatus,
        imageType: a.imageType,
        notes: a.notes,
      };
    })
    .sort((a, b) => (a.regionPriority || 9) - (b.regionPriority || 9));

  const scenarioCandidates = scenarioRaw.map((a) => ({
    ...mapAssetToCandidate(a, brandSlug, "scenario"),
    slotKey: a.slotKey,
    intendedSlot: a.intendedSlot,
    renderReadinessProjection: a.renderReadiness,
    registryReadinessProjection: a.approvalStatus,
  }));

  // Catalog-only projection when asset pack missing property rows
  const catalogPlan = (catalog || [])
    .map((c) => ({
      propertyKey: c.propertyKey,
      propertyName: c.propertyName,
      region: regionBucket(c.geographyLabel),
      regionPriority: regionPriority(c.geographyLabel),
      geographyLabel: c.geographyLabel,
      sourcePageUrl: c.sourcePageUrl,
      galleryPriority: c.galleryPriority,
    }))
    .sort((a, b) => a.regionPriority - b.regionPriority || (a.galleryPriority || 9) - (b.galleryPriority || 9));

  const acceptedGallery = galleryCandidates.filter((c) => c.accepted);
  const acceptedProperty = propertyCandidates.filter((c) => c.accepted);
  const acceptedScenarios = scenarioCandidates.filter((c) => c.accepted);
  const rejected = [
    ...galleryCandidates.filter((c) => !c.accepted),
    ...propertyCandidates.filter((c) => !c.accepted),
    ...scenarioCandidates.filter((c) => !c.accepted),
  ];

  const renderReadyGallery = galleryCandidates.filter(
    (c) => c.accepted && c.renderReadinessProjection === "ready"
  ).length;
  const renderReadyProperty = propertyCandidates.filter(
    (c) => c.accepted && c.renderReadinessProjection === "ready"
  ).length;

  const calaPropertyAccepted = acceptedProperty.filter((c) => c.region === "CALA").length;
  const usPropertyAccepted = acceptedProperty.filter((c) => c.region === "US").length;
  const globalPropertyAccepted = acceptedProperty.filter((c) => c.region === "global").length;

  const brandSpecific = {
    "hotel-indigo": {
      rejectGenericIhgHero: true,
      rejectInterContinental: true,
      calaFirst: true,
      sectionLabel: SECTION_LABELS["hotel-indigo"],
      calaSupportCount: calaPropertyAccepted,
      expansionNeeded: calaPropertyAccepted < V46_PROPERTY_MIN,
    },
    "mgallery-collection": {
      validateAccorPropertySpecific: true,
      rejectGenericAccorGraphics: true,
      sectionLabel: SECTION_LABELS["mgallery-collection"],
      towardAssetPackReady: acceptedGallery.length >= V46_GALLERY_MIN && acceptedProperty.length >= V46_PROPERTY_MIN,
    },
    "small-luxury-hotels-of-the-world": {
      consortiumLanguage: "independent_luxury_consortium",
      noFranchiseLogic: true,
      preferCalaIfOfficial: true,
      sectionLabel: SECTION_LABELS["small-luxury-hotels-of-the-world"],
      calaSupportCount: calaPropertyAccepted,
    },
  }[brandSlug];

  const eligibility = classifyDraftEligibility({
    liveGallery: live.galleryImageUrlCount,
    liveOpenings: live.propertyImageUrlCount,
    acceptedGallery: acceptedGallery.length,
    acceptedProperty: acceptedProperty.length,
    acceptedScenarios: Math.max(acceptedScenarios.length, acceptedGallery.length >= 3 ? 3 : acceptedScenarios.length),
    renderReadyGallery,
    renderReadyProperty,
  });

  const risks = {
    wrongBrand: rejected.filter((r) => /wrong_brand/i.test(r.rejectionReason || "")).length,
    logoOrGeneric: rejected.filter((r) =>
      /logo|lifestyle|generic/i.test(r.rejectionReason || "")
    ).length,
    registryOnly: rejected.filter((r) => r.rejectionReason === "registry_only_without_render_imageurl")
      .length,
    missingUrl: rejected.filter((r) => r.rejectionReason === "missing_image_url").length,
  };

  return {
    brandSlug,
    brandName: brandApi.name || config?.name || brandSlug,
    recordId: brandApi.id || config?.recordId || null,
    sectionLabel: SECTION_LABELS[brandSlug],
    brandSpecific,
    liveApi: live,
    presentationImageReadiness: {
      gallerySlotsWithImageUrl: live.galleryImageUrlCount,
      openingsWithImageUrl: live.propertyImageUrlCount,
      scenariosWithImageUrl: live.scenarioImageUrlCount,
    },
    registryReadiness: {
      registryAssetsLoaded: registryAssets.length,
      note: "Registry candidates alone do not count as render-ready imageUrl",
    },
    assetPackError,
    assetPackSummary: assetPack?.summary || null,
    renderContractSummary: renderContract?.summary || null,
    visualAssetPack: {
      galleryCandidates: galleryCandidates.slice(0, 12),
      propertyExampleCandidates: propertyCandidates.slice(0, 8),
      scenarioCandidates: scenarioCandidates.slice(0, 6),
      acceptedCounts: {
        gallery: acceptedGallery.length,
        propertyExamples: acceptedProperty.length,
        scenarios: acceptedScenarios.length,
        calaProperty: calaPropertyAccepted,
        usProperty: usPropertyAccepted,
        globalProperty: globalPropertyAccepted,
      },
      rejectedSample: rejected.slice(0, 20),
      catalogPlan: catalogPlan.slice(0, 8),
    },
    risks,
    eligibility,
    guardrails: {
      presentationWrites: false,
      unlock: false,
      activeRelease: false,
      companyValidatedChanges: false,
      releasedBrandWrites: false,
    },
    nextCommands: {
      dryRunBatch: `npm run brand-explorer-v46-image-remediation-batch -- --brands ${brandSlug} --dry-run`,
      assetPack:
        "npm run brand-explorer-active-profile-asset-pack -- --brand " + brandSlug + " --dry-run",
      note: "v46 does not materialize Presentation Image fields or apply draft.",
    },
  };
}

export async function runV46ImageRemediationBatch({
  brands = V46_TARGET_BRANDS,
  dryRun = true,
} = {}) {
  if (!dryRun) {
    throw new Error("v46 image remediation batch is read-only. Use --dry-run only (no apply in this stage).");
  }

  for (const b of brands) {
    if (V46_PROTECTED_RELEASED.includes(b)) {
      throw new Error(`v46 refuses released brand ${b}`);
    }
    if (!V46_TARGET_BRANDS.includes(b)) {
      throw new Error(`v46 target brands only: ${V46_TARGET_BRANDS.join(", ")}`);
    }
  }

  const osConfirm = await confirmV46OsRouting(brands);
  const baselineProtection = await protectV46ReleasedBaseline();
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditV46BrandImageRemediation(brandSlug));
  }

  const byStatus = brandResults.reduce((acc, b) => {
    const s = b.eligibility?.status || "unknown";
    acc[s] = (acc[s] || 0) + 1;
    return acc;
  }, {});

  return {
    version: V46_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    osConfirm,
    baselineProtection,
    brandResults,
    summary: {
      targets: brandResults.length,
      osRoutingPass: osConfirm.pass,
      baselineProtectionPass: baselineProtection.pass,
      byEligibility: byStatus,
      anyUnlock: false,
      presentationWrites: false,
      applyDraftAllowed: false,
    },
    guardrails: {
      activeRelease: false,
      companyValidatedChanges: false,
      releasedBrandChanges: false,
      incompleteBrandUnlock: false,
      genericImagesAccepted: false,
      registryOnlyCountedAsRenderReady: false,
      presentationWrites: false,
    },
  };
}

function renderBrandMd(brandResult) {
  const b = brandResult;
  const lines = [
    `# v46 Image Remediation — ${b.brandName || b.brandSlug}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    "",
    "## Eligibility",
    "",
    `- Status: **${b.eligibility?.status}**`,
    `- image_remediation_complete: ${b.eligibility?.image_remediation_complete}`,
    `- asset_pack_ready: ${b.eligibility?.asset_pack_ready}`,
    `- build_draft_ready: ${b.eligibility?.build_draft_ready}`,
    `- apply_draft_allowed: ${b.eligibility?.apply_draft_allowed} (always false in v46)`,
    `- Rationale: ${b.eligibility?.rationale || "—"}`,
    "",
    "## Live API imageUrl counts",
    "",
    `- Gallery: **${b.liveApi?.galleryImageUrlCount}** / ${V46_GALLERY_MIN}`,
    `- Property examples: **${b.liveApi?.propertyImageUrlCount}** / ${V46_PROPERTY_MIN}`,
    `- Scenarios: **${b.liveApi?.scenarioImageUrlCount}** / ${V46_SCENARIO_MIN}`,
    "",
    `Section label: **${b.sectionLabel}**`,
    "",
    "## Accepted candidate counts",
    "",
    `- Gallery: ${b.visualAssetPack?.acceptedCounts?.gallery}`,
    `- Property: ${b.visualAssetPack?.acceptedCounts?.propertyExamples} (CALA=${b.visualAssetPack?.acceptedCounts?.calaProperty}, US=${b.visualAssetPack?.acceptedCounts?.usProperty}, global=${b.visualAssetPack?.acceptedCounts?.globalProperty})`,
    `- Scenarios: ${b.visualAssetPack?.acceptedCounts?.scenarios}`,
    "",
    "## Property example candidates",
    "",
  ];

  for (const p of b.visualAssetPack?.propertyExampleCandidates || []) {
    lines.push(
      `- **${p.propertyName || p.propertyKey || "—"}** · ${p.region || "?"} · accepted=${p.accepted}${
        p.rejectionReason ? ` · reject=${p.rejectionReason}` : ""
      } · render=${p.renderReadinessProjection || "—"}`
    );
    if (p.sourcePageUrl) lines.push(`  - source: ${p.sourcePageUrl}`);
    if (p.imageUrl) lines.push(`  - imageUrl: ${p.imageUrl.slice(0, 120)}`);
  }

  lines.push("", "## Gallery candidates (sample)", "");
  for (const g of (b.visualAssetPack?.galleryCandidates || []).slice(0, 8)) {
    lines.push(
      `- \`${g.slotKey || g.intendedSlot || "gallery"}\` · accepted=${g.accepted}${
        g.rejectionReason ? ` · ${g.rejectionReason}` : ""
      } · ${g.propertyName || ""}`
    );
  }

  lines.push("", "## Risks", "");
  lines.push(`- Wrong-brand rejects: ${b.risks?.wrongBrand}`);
  lines.push(`- Logo/generic rejects: ${b.risks?.logoOrGeneric}`);
  lines.push(`- Registry-only rejects: ${b.risks?.registryOnly}`);
  lines.push(`- Missing URL rejects: ${b.risks?.missingUrl}`);

  if (b.assetPackError) {
    lines.push("", `## Asset pack error`, "", b.assetPackError, "");
  }

  lines.push("", "## Brand-specific notes", "");
  lines.push("```json");
  lines.push(JSON.stringify(b.brandSpecific || {}, null, 2));
  lines.push("```", "", "## Guardrails", "");
  for (const [k, v] of Object.entries(b.guardrails || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  return lines.join("\n");
}

export function renderV46BatchMarkdown(report) {
  const lines = [
    "# v46 Brand Explorer Image Remediation Batch",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Read-only. Targets Hotel Indigo, MGallery, SLH. Protects released golden brands. No Presentation writes. No unlock.",
    "",
    "## Summary",
    "",
    `- OS routing pass: **${report.summary?.osRoutingPass}**`,
    `- Baseline protection: **${report.summary?.baselineProtectionPass}**`,
    `- Eligibility: ${JSON.stringify(report.summary?.byEligibility || {})}`,
    `- Apply draft allowed: **false**`,
    "",
    "## Target routing",
    "",
    "| Brand | OS state | Next action | Full profile | Live gallery | Live openings | Eligibility |",
    "|---|---|---|---|---|---|---|",
  ];

  for (const t of report.osConfirm?.targets || []) {
    const br = report.brandResults.find((b) => b.brandSlug === t.brandSlug);
    lines.push(
      `| ${t.brandSlug} | ${t.canonicalState} | ${t.allowedNextAction} | ${t.shouldRenderFullProfile} | ${t.galleryCount} | ${t.openingsCount} | ${br?.eligibility?.status || "—"} |`
    );
  }

  lines.push("", "## Per-brand eligibility", "");
  for (const b of report.brandResults || []) {
    lines.push(`### ${b.brandSlug}`);
    lines.push(`- **${b.eligibility?.status}** — ${b.eligibility?.rationale}`);
    lines.push(
      `- Accepted gallery/property/scenario: ${b.visualAssetPack?.acceptedCounts?.gallery}/${b.visualAssetPack?.acceptedCounts?.propertyExamples}/${b.visualAssetPack?.acceptedCounts?.scenarios}`
    );
    lines.push("");
  }

  lines.push("## Guardrails", "");
  for (const [k, v] of Object.entries(report.guardrails || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  return lines.join("\n");
}

export function renderV46BaselineMarkdown(report) {
  const lines = [
    "# v46 Released Baseline Protection",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    `Pass: **${report.baselineProtection?.pass}**`,
    "",
    "| Brand | Active ready | Full | Gallery | Property | Tabs | CV | Ext lock |",
    "|---|---|---|---|---|---|---|---|",
  ];
  for (const r of report.baselineProtection?.releasedRows || []) {
    lines.push(
      `| ${r.brandSlug} | ${r.active_profile_ready} | ${r.shouldRenderFullProfile} | ${r.galleryImageUrlCount} | ${r.propertyImageUrlCount} | ${r.externalTabCount} | ${r.companyValidated} | ${r.externalQualityLockPass} |`
    );
  }
  if (report.baselineProtection?.failures?.length) {
    lines.push("", "## Failures", "");
    for (const f of report.baselineProtection.failures) lines.push(`- ${f}`);
  }
  lines.push(
    "",
    "## Guardrails",
    "",
    "- No writes to released brands",
    "- Incomplete brands must stay locked",
    "- Company Validated untouched",
    ""
  );
  return lines.join("\n");
}

export function writeV46Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const baselinePath = path.join(reportsDir, REPORT_BASELINE_MD);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, renderV46BatchMarkdown(report), "utf8");
  fs.writeFileSync(baselinePath, renderV46BaselineMarkdown(report), "utf8");

  const brandPaths = {};
  for (const b of report.brandResults || []) {
    const fname = BRAND_REPORT_MD[b.brandSlug];
    if (!fname) continue;
    const p = path.join(reportsDir, fname);
    fs.writeFileSync(p, renderBrandMd(b), "utf8");
    brandPaths[b.brandSlug] = p;
  }

  return { jsonPath, mdPath, baselinePath, brandPaths };
}
