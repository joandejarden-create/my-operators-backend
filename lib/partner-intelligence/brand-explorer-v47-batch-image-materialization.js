/**
 * v47 — Batch Image Materialization + Draft Readiness
 * (Hotel Indigo, MGallery Collection, SLH)
 *
 * Converts v46 accepted visual candidates into Presentation Image
 * materialization plans. Dry-run by default. Apply only with explicit gates.
 *
 * Guardrails: no unlock, no Company Validated, no released-brand writes,
 * no Source Library changes, external profiles remain locked.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  buildCalaPropertyOpeningCopy,
} from "./brand-explorer-cala-property-example-rules.js";
import {
  applyRegistryRecords,
} from "./brand-asset-registry-workflow.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";
import {
  isLogoImageUrl,
  isGenericBrandOrLifestyleImageUrl,
  isOfficialLifestylePropertyImageUrl,
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
import { pickDistinctImageAssets, evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";

export const V47_VERSION = "v47";

export const V47_TARGET_BRANDS = Object.freeze([...GRADUATED_LIFESTYLE_COHORT_SLUGS]);
export const V47_PROTECTED_RELEASED = Object.freeze([...ORIGINAL_GOLDEN_RELEASE_SLUGS]);

export const V47_GALLERY_MIN = 6;
export const V47_PROPERTY_MIN = 3;
export const V47_SCENARIO_MIN = 3;

export const REPORT_JSON = "brand-explorer-v47-batch-image-materialization.json";
export const REPORT_MD = "brand-explorer-v47-batch-image-materialization.md";
export const REPORT_BASELINE_MD = "brand-explorer-v47-released-baseline-protection.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v47-batch-image-materialization";
export const APPLY_FLAG_NO_CV = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_ACTIVE = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_NO_SOURCE = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_RELEASED = "--confirm-no-released-brand-changes";
export const APPLY_FLAG_LOCKED = "--confirm-external-profiles-remain-locked";
export const APPLY_FLAG_OFFICIAL = "--confirm-official-source-images-only";
export const APPLY_FLAG_SIX = "--confirm-six-gallery-imageurls-projected";
export const APPLY_FLAG_THREE = "--confirm-three-property-example-imageurls-projected";
export const APPLY_FLAG_BRAND_ONLY = "--confirm-brand-only";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_CV,
  APPLY_FLAG_NO_ACTIVE,
  APPLY_FLAG_NO_SOURCE,
  APPLY_FLAG_NO_RELEASED,
  APPLY_FLAG_LOCKED,
  APPLY_FLAG_OFFICIAL,
  APPLY_FLAG_SIX,
  APPLY_FLAG_THREE,
  APPLY_FLAG_BRAND_ONLY,
]);

const BRAND_REPORT_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-v47-hotel-indigo-materialization.md",
  "mgallery-collection": "brand-explorer-v47-mgallery-materialization.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-v47-slh-materialization.md",
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

const GALLERY_TITLES = Object.freeze([
  "Exterior / Arrival",
  "Guest Room / Suite",
  "Public Space",
  "F&B or Local Experience",
  "Design Detail",
  "Property Setting",
]);

const OPENINGS_SLOT = "footprint.openings";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const V46_REPORT = "brand-explorer-v46-image-remediation-batch.json";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function stripOwnerFacingUrls(text) {
  return nz(text)
    .replace(/https?:\/\/\S+/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function isGenericIhgBrandHeroImage(url) {
  const u = normalizeUrlKey(url);
  if (!u) return false;
  if (/digital\.ihg\.com\/is\/image\/ihg\/hotel-indigo-[a-z0-9-]+/i.test(u)) return false;
  if (/digital\.ihg\.com\/is\/image\/ihg\/ihg-[a-z0-9-]+/i.test(u)) return !/hotelindigo/i.test(u);
  return false;
}

function isInterContinentalBrandImage(url) {
  return /intercontinental|\/ic\/|ihg-intercontinental/i.test(normalizeUrlKey(url));
}

function isAccorGenericBrandGraphic(url) {
  const u = normalizeUrlKey(url);
  if (!u) return false;
  if (/ahstatic\.com\/photos\/[a-z0-9]+_(?:ho|ro)/i.test(u)) return false;
  return (
    (/accor\.com|all\.accor\.com/i.test(u) && /logo|brand|sprite|icon|svg/i.test(u)) ||
    /accor-brand|group\.accor\.com.*logo/i.test(u)
  );
}

function regionBucket(label) {
  const s = nz(label).toLowerCase();
  if (/cala|caribbean|latin|mexico|peru|argentina|brazil|uruguay|chile|colombia/i.test(s)) {
    return "CALA";
  }
  if (/\bu\.?s\.?\b|united states|usa\b/i.test(s)) return "US";
  return "global";
}

export function loadV46AcceptedPack(reportsDir = path.join(ROOT, "reports")) {
  const p = path.join(reportsDir, V46_REPORT);
  if (!fs.existsSync(p)) {
    throw new Error(`Missing v46 report: ${p}. Run brand-explorer-v46-image-remediation-batch first.`);
  }
  const report = JSON.parse(fs.readFileSync(p, "utf8"));
  const bySlug = {};
  for (const row of report.brandResults || []) {
    bySlug[row.brandSlug] = row;
  }
  return { report, bySlug, path: p };
}

/**
 * Validate a single accepted v46 asset for materialization.
 */
export function validateV47MaterializationAsset(asset = {}, { brandSlug = "", role = "gallery" } = {}) {
  const imageUrl = nz(asset.imageUrl);
  const sourcePageUrl = nz(asset.sourcePageUrl);
  const blockers = [];

  if (asset.accepted !== true) blockers.push("not_accepted");
  if (nz(asset.rejectionReason)) blockers.push(`rejection_reason:${asset.rejectionReason}`);
  if (!imageUrl) blockers.push("missing_image_url");
  if (!sourcePageUrl) blockers.push("missing_source_page_url");
  if (isLogoImageUrl(imageUrl)) blockers.push("logo_only");
  if (role === "property_example" && isGenericBrandOrLifestyleImageUrl(imageUrl)) {
    blockers.push("lifestyle_only_property_example");
  }
  if (isGenericIhgBrandHeroImage(imageUrl)) blockers.push("generic_ihg_hero");
  if (brandSlug === "hotel-indigo") {
    if (!/digital\.ihg\.com\/is\/image\/ihg\/hotel-indigo-/i.test(imageUrl)) {
      blockers.push("hotel_indigo_requires_scene7_property_photo");
    }
    if (isInterContinentalBrandImage(imageUrl)) blockers.push("wrong_brand_intercontinental");
    if (!/bjxgd|gdlal|limmd|bnaus|hotel-indigo-(guanajuato|guadalajara|lima|nashville)/i.test(
      `${imageUrl} ${sourcePageUrl}`
    )) {
      blockers.push("hotel_indigo_marsha_or_city_mismatch");
    }
  }
  if (brandSlug === "mgallery-collection") {
    if (isAccorGenericBrandGraphic(imageUrl)) blockers.push("generic_accor_graphic");
    if (!/ahstatic\.com\/photos\//i.test(imageUrl) && !isOfficialLifestylePropertyImageUrl(imageUrl)) {
      blockers.push("mgallery_requires_property_photography");
    }
  }
  if (brandSlug === "small-luxury-hotels-of-the-world") {
    if (!isOfficialLifestylePropertyImageUrl(imageUrl) && !/slh\.com\/-\/media\/slh\/hotels/i.test(imageUrl)) {
      blockers.push("slh_requires_property_photography");
    }
  }

  const wrongBrandRisk = blockers.some((b) => /wrong_brand/i.test(b));
  const genericHeroRisk = blockers.some((b) => /generic_ihg|generic_accor|generic/i.test(b));
  const logoOnly = blockers.includes("logo_only");
  const lifestyleOnly = blockers.includes("lifestyle_only_property_example");

  return {
    ok: blockers.length === 0,
    blockers,
    wrongBrandRisk,
    genericHeroRisk,
    logoOnly,
    lifestyleOnly,
    renderReadinessProjectionPass: blockers.length === 0,
  };
}

function ingestAcceptedAssets(v46Brand) {
  const pack = v46Brand?.visualAssetPack || {};
  const gallery = (pack.galleryCandidates || []).filter((c) => c.accepted);
  const property = (pack.propertyExampleCandidates || []).filter((c) => c.accepted);
  const scenarios = (pack.scenarioCandidates || []).filter((c) => c.accepted);
  return { gallery, property, scenarios };
}

function findCatalogEntry(brandSlug, asset) {
  const catalog = CATALOGS[brandSlug] || [];
  return (
    catalog.find(
      (c) =>
        c.propertyKey === asset.propertyKey ||
        nz(c.propertyName).toLowerCase() === nz(asset.propertyName).toLowerCase()
    ) || null
  );
}

function matchOpeningsRow(presentationRows, catalog, asset) {
  return (
    presentationRows.find(
      (r) =>
        r.slotKey === OPENINGS_SLOT &&
        nz(r.title).toLowerCase().includes(nz(catalog?.propertyName || asset.propertyName).toLowerCase())
    ) ||
    presentationRows.find(
      (r) =>
        r.slotKey === OPENINGS_SLOT &&
        catalog?.marketCity &&
        nz(r.title).toLowerCase().includes(nz(catalog.marketCity).toLowerCase())
    ) ||
    null
  );
}

function buildGalleryPlanRow({ brandSlug, brandConfig, asset, index, presentationRows }) {
  const slotKey = asset.slotKey || `materials.gallery.${index + 1}`;
  const spaceLabel = GALLERY_TITLES[index] || `Gallery ${index + 1}`;
  const propertyName = nz(asset.propertyName);
  const title =
    propertyName && !spaceLabel.includes(propertyName)
      ? `${spaceLabel} - ${propertyName}`
      : spaceLabel;
  const row = presentationRows.find((r) => r.slotKey === slotKey) || null;
  const validation = validateV47MaterializationAsset(asset, { brandSlug, role: "gallery" });
  return {
    planSlotKey: slotKey,
    slotKey,
    brandSlug,
    brandRecordId: brandConfig.recordId,
    recordId: row?.recordId || null,
    createIfMissing: !row?.recordId,
    title,
    externalBody: "",
    caseSummary: null,
    imageUrl: nz(asset.imageUrl),
    sourcePageUrl: nz(asset.sourcePageUrl),
    propertyName,
    region: regionBucket(asset.geographyLabel || asset.region),
    imageClassification: asset.imageType || "hotel_property_photography",
    sortOrder: index + 1,
    externalDisplayStatus: null,
    fields: {
      Title: title,
      Image: [{ url: nz(asset.imageUrl) }],
      "External Display Status": null,
    },
    validation,
    registryCandidate: {
      assetName: `${brandConfig.name} — ${slotKey}`,
      sourceUrl: nz(asset.imageUrl),
      sourcePageUrl: nz(asset.sourcePageUrl),
      recommendedExplorerSlot: slotKey,
    },
    ownerFacingHasUrl: false,
  };
}

function buildPropertyPlanRow({ brandSlug, brandConfig, asset, index, presentationRows }) {
  const catalog = findCatalogEntry(brandSlug, asset);
  const copy = catalog
    ? buildCalaPropertyOpeningCopy(catalog, { sectionLabel: SECTION_LABELS[brandSlug] })
    : {
        title: `${nz(asset.propertyName)}${asset.marketCity ? ` — ${nz(asset.marketCity)}` : ""}`,
        body: "",
        meta: "",
        chips: "",
        scenario: "PROPERTY EXAMPLE",
        sectionLabel: SECTION_LABELS[brandSlug],
      };
  const body = stripOwnerFacingUrls(copy.body);
  const row = matchOpeningsRow(presentationRows, catalog, asset);
  const planSlotKey = `footprint.openings.${index + 1}`;
  const validation = validateV47MaterializationAsset(asset, {
    brandSlug,
    role: "property_example",
  });
  return {
    planSlotKey,
    slotKey: OPENINGS_SLOT,
    brandSlug,
    brandRecordId: brandConfig.recordId,
    recordId: row?.recordId || null,
    createIfMissing: !row?.recordId,
    title: copy.title,
    externalBody: body,
    caseSummary: null,
    imageUrl: nz(asset.imageUrl),
    sourcePageUrl: nz(asset.sourcePageUrl || catalog?.sourcePageUrl),
    propertyName: nz(asset.propertyName || catalog?.propertyName),
    propertyKey: catalog?.propertyKey || asset.propertyKey || "",
    region: regionBucket(catalog?.geographyLabel || asset.geographyLabel || asset.region),
    geographyLabel: catalog?.geographyLabel || "",
    sectionLabel: SECTION_LABELS[brandSlug],
    imageClassification: asset.imageType || "hotel_property_photography",
    sortOrder: 10 + index,
    externalDisplayStatus: null,
    fields: {
      Title: copy.title,
      Body: body,
      Image: [{ url: nz(asset.imageUrl) }],
      "External Display Status": null,
    },
    validation,
    registryCandidate: {
      assetName: `${brandConfig.name} — ${copy.title}`,
      sourceUrl: nz(asset.imageUrl),
      sourcePageUrl: nz(asset.sourcePageUrl || catalog?.sourcePageUrl),
      recommendedExplorerSlot: OPENINGS_SLOT,
    },
    ownerFacingHasUrl: /https?:\/\//i.test(body),
  };
}

function buildScenarioPlanRow({ brandSlug, brandConfig, asset, index, presentationRows }) {
  const slotKey = asset.slotKey || `overview.scenario.${index + 1}`;
  const copy = brandConfig.overviewScenarioCopy?.[slotKey] || {};
  const title = stripOwnerFacingUrls(copy.title || `Scenario ${index + 1}`);
  const body = stripOwnerFacingUrls(copy.body || "");
  const row = presentationRows.find((r) => r.slotKey === slotKey) || null;
  const validation = validateV47MaterializationAsset(asset, { brandSlug, role: "scenario" });
  return {
    planSlotKey: slotKey,
    slotKey,
    brandSlug,
    brandRecordId: brandConfig.recordId,
    recordId: row?.recordId || null,
    createIfMissing: !row?.recordId,
    title,
    externalBody: body,
    caseSummary: null,
    imageUrl: nz(asset.imageUrl),
    sourcePageUrl: nz(asset.sourcePageUrl),
    propertyName: nz(asset.propertyName),
    region: regionBucket(asset.geographyLabel || asset.region),
    imageClassification: "scenario_card",
    sortOrder: index + 1,
    externalDisplayStatus: null,
    fields: {
      Title: title,
      Body: body,
      Image: [{ url: nz(asset.imageUrl) }],
      "External Display Status": null,
    },
    validation,
    registryCandidate: {
      assetName: `${brandConfig.name} — ${slotKey}`,
      sourceUrl: nz(asset.imageUrl),
      sourcePageUrl: nz(asset.sourcePageUrl),
      recommendedExplorerSlot: slotKey,
    },
    ownerFacingHasUrl: /https?:\/\//i.test(`${title}\n${body}`),
  };
}

export async function confirmV47OsRouting(targetBrands = V47_TARGET_BRANDS) {
  const targets = [];
  const blockers = [];

  for (const brandSlug of targetBrands) {
    const os = await evaluateBrandExplorerOsBrand(brandSlug);
    const ctx = await loadBrandFactoryContext(brandSlug);
    const brand = ctx.brandApi;
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
      pass: true,
    };

    const okState =
      os.canonicalState === "draft_applied_with_defects" ||
      os.routing?.allowedNextAction === "image_remediation";
    if (!okState) {
      blockers.push(`${brandSlug}:expected_image_remediation_or_draft_defects`);
      row.pass = false;
    }
    if (brand.shouldRenderFullProfile === true) {
      blockers.push(`${brandSlug}:unexpected_full_profile`);
      row.pass = false;
    }
    if (os.canonicalState === "active_profile_ready") {
      blockers.push(`${brandSlug}:unexpected_active_profile_ready`);
      row.pass = false;
    }
    targets.push(row);
  }

  return { pass: blockers.length === 0, blockers, targets };
}

export async function protectV47ReleasedBaseline() {
  const snapshots = [];
  for (const slug of V47_PROTECTED_RELEASED) {
    snapshots.push(await captureV44BrandSnapshot(slug));
  }
  for (const slug of V47_TARGET_BRANDS) {
    snapshots.push(await captureV44BrandSnapshot(slug));
  }

  const regression = evaluateV44Regression(snapshots);
  const releasedRows = snapshots
    .filter((s) => V47_PROTECTED_RELEASED.includes(s.brandSlug))
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

function projectRenderReadiness(planRows) {
  const gallery = planRows.filter((r) => r.slotKey.startsWith("materials.gallery."));
  const property = planRows.filter((r) => r.planSlotKey.startsWith("footprint.openings."));
  const scenarios = planRows.filter((r) => r.slotKey.startsWith("overview.scenario."));
  const galleryReady = gallery.filter((r) => r.validation?.ok && r.imageUrl).length;
  const propertyReady = property.filter((r) => r.validation?.ok && r.imageUrl).length;
  const scenarioReady = scenarios.filter((r) => r.validation?.ok && r.imageUrl).length;
  const uniqueness = evaluateImageUniqueness({
    brandSlug: "projection",
    presentationRows: planRows.map((r) => ({
      slotKey: r.slotKey,
      imageUrl: r.imageUrl,
      title: r.title,
      recordId: r.recordId || r.planSlotKey,
    })),
  });
  return {
    galleryProjected: `${galleryReady}/${V47_GALLERY_MIN}`,
    propertyProjected: `${propertyReady}/${V47_PROPERTY_MIN}`,
    scenarioProjected: `${scenarioReady}/${V47_SCENARIO_MIN}`,
    galleryReady,
    propertyReady,
    scenarioReady,
    galleryDistinctCount: uniqueness.galleryDistinctCount,
    scenarioDistinctCount: uniqueness.scenarioDistinctCount,
    propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
    uniquenessPass: uniqueness.pass === true,
    noImagePlaceholders: galleryReady + propertyReady + scenarioReady === planRows.length,
    registryOnlyCountedAsRenderReady: false,
    externalStillLocked: true,
    pass:
      galleryReady >= V47_GALLERY_MIN &&
      propertyReady >= V47_PROPERTY_MIN &&
      scenarioReady >= V47_SCENARIO_MIN &&
      uniqueness.pass === true,
  };
}

function classifyPostMaterializationEligibility(projection) {
  if (!projection.pass) {
    return {
      status: "still_blocked_by_images",
      image_remediation_complete: false,
      materialization_plan_ready: false,
      build_draft_ready: false,
      apply_draft_allowed: false,
      rationale: `Projected gallery ${projection.galleryProjected}, property ${projection.propertyProjected}, scenarios ${projection.scenarioProjected}.`,
    };
  }
  return {
    status: "build_draft_ready",
    image_remediation_complete: true,
    materialization_plan_ready: true,
    build_draft_ready: true,
    apply_draft_allowed: true,
    rationale:
      "Materialization plan projects 6/6 gallery + 3/3 property + 3/3 scenario imageUrls. External profile remains locked until founder/active release. apply_draft may proceed in a later gated stage after Presentation Image writes.",
  };
}

export async function planV47BrandMaterialization(brandSlug, { v46BySlug } = {}) {
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);

  const v46Brand = v46BySlug?.[brandSlug];
  if (!v46Brand) throw new Error(`No v46 brand result for ${brandSlug}`);
  if (v46Brand.eligibility?.asset_pack_ready !== true && v46Brand.eligibility?.status !== "asset_pack_ready") {
    throw new Error(`${brandSlug}: v46 eligibility is not asset_pack_ready`);
  }

  const ingested = ingestAcceptedAssets(v46Brand);
  const blockers = [];
  const galleryDistinct = pickDistinctImageAssets(ingested.gallery, V47_GALLERY_MIN);
  const propertyDistinct = pickDistinctImageAssets(ingested.property, V47_PROPERTY_MIN);
  const galleryGroupIds = galleryDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean);
  const scenarioPool = [...ingested.scenarios, ...ingested.gallery, ...ingested.property];
  let scenarioDistinct = pickDistinctImageAssets(scenarioPool, V47_SCENARIO_MIN, {
    excludeGroupIds: galleryGroupIds,
  });
  if (scenarioDistinct.length < V47_SCENARIO_MIN) {
    scenarioDistinct = pickDistinctImageAssets(scenarioPool, V47_SCENARIO_MIN);
  }

  if (galleryDistinct.length < V47_GALLERY_MIN) {
    blockers.push(`gallery_distinct_${galleryDistinct.length}_lt_${V47_GALLERY_MIN}`);
  }
  if (propertyDistinct.length < V47_PROPERTY_MIN) {
    blockers.push(`property_distinct_${propertyDistinct.length}_lt_${V47_PROPERTY_MIN}`);
  }
  if (scenarioDistinct.length < V47_SCENARIO_MIN) {
    blockers.push(`scenario_distinct_${scenarioDistinct.length}_lt_${V47_SCENARIO_MIN}`);
  }

  const ctx = await loadBrandFactoryContext(brandSlug);
  const presentationRows = ctx.presentationRows || [];

  const galleryPlan = galleryDistinct.slice(0, V47_GALLERY_MIN).map((asset, i) =>
    buildGalleryPlanRow({ brandSlug, brandConfig, asset, index: i, presentationRows })
  );
  const propertyPlan = propertyDistinct.slice(0, V47_PROPERTY_MIN).map((asset, i) =>
    buildPropertyPlanRow({ brandSlug, brandConfig, asset, index: i, presentationRows })
  );
  const scenarioPlan = scenarioDistinct.slice(0, V47_SCENARIO_MIN).map((asset, i) =>
    buildScenarioPlanRow({ brandSlug, brandConfig, asset, index: i, presentationRows })
  );

  const planRows = [...galleryPlan, ...propertyPlan, ...scenarioPlan];
  for (const row of planRows) {
    if (!row.validation.ok) {
      blockers.push(`${row.planSlotKey}:${(row.validation.blockers || []).join(",")}`);
    }
    if (row.ownerFacingHasUrl) blockers.push(`${row.planSlotKey}:owner_facing_url`);
  }

  const projection = projectRenderReadiness(planRows);
  if (!projection.pass) blockers.push("render_readiness_projection_failed");

  const eligibility = classifyPostMaterializationEligibility(projection);
  if (blockers.length) {
    eligibility.status = "still_blocked_by_images";
    eligibility.materialization_plan_ready = false;
    eligibility.build_draft_ready = false;
    eligibility.apply_draft_allowed = false;
    eligibility.rationale = `Blocked: ${blockers.join("; ")}`;
  }

  const presentationPatches = planRows.map((r) => ({
    recordId: r.recordId,
    slotKey: r.slotKey,
    planSlotKey: r.planSlotKey,
    fields: r.fields,
    reason: "v47_image_materialization",
    createIfMissing: r.createIfMissing,
    imageSourcePageUrl: r.sourcePageUrl,
  }));

  const registryCreates = planRows.map((r) => ({
    assetName: r.registryCandidate.assetName,
    brandRecordId: brandConfig.recordId,
    parentCompany: brandConfig.parentCompany,
    sourcePageUrl: r.registryCandidate.sourcePageUrl,
    sourceUrl: r.registryCandidate.sourceUrl,
    recommendedExplorerSlot: r.registryCandidate.recommendedExplorerSlot,
    explorerUsePermission: "Candidate Only",
    usageReviewStatus: "Pending Review",
    notes: `v47 materialization candidate — ${r.planSlotKey}`,
  }));

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    sectionLabel: SECTION_LABELS[brandSlug],
    liveApi: {
      galleryImageUrlCount: v46Brand.liveApi?.galleryImageUrlCount ?? 0,
      propertyImageUrlCount: v46Brand.liveApi?.propertyImageUrlCount ?? 0,
      scenarioImageUrlCount: v46Brand.liveApi?.scenarioImageUrlCount ?? 0,
    },
    ingestedCounts: {
      gallery: ingested.gallery.length,
      property: ingested.property.length,
      scenarios: ingested.scenarios.length,
    },
    planRows,
    presentationPatches,
    registryCreates,
    projection,
    eligibility,
    blockers,
    materializationBlocked: blockers.length > 0,
    brandSpecific: {
      "hotel-indigo": {
        scene7Only: true,
        marshaCodes: ["BJXGD", "GDLAL", "LIMMD", "BNAUS"],
        calaPropertyCount: propertyPlan.filter((p) => p.region === "CALA").length,
        sectionLabel: SECTION_LABELS["hotel-indigo"],
      },
      "mgallery-collection": {
        ahstaticPropertyOnly: true,
        sectionLabel: SECTION_LABELS["mgallery-collection"],
      },
      "small-luxury-hotels-of-the-world": {
        consortiumLanguage: "independent_luxury_consortium",
        noFranchiseLogic: true,
        sectionLabel: SECTION_LABELS["small-luxury-hotels-of-the-world"],
      },
    }[brandSlug],
    guardrails: {
      presentationImageWritesPlanned: true,
      sourceLibraryWrites: false,
      unlock: false,
      activeRelease: false,
      companyValidatedChanges: false,
      releasedBrandWrites: false,
      rawUrlsInOwnerFacingCopy: planRows.some((r) => r.ownerFacingHasUrl),
    },
    nextCommands: {
      dryRun: `npm run brand-explorer-v47-batch-image-materialization -- --brands ${brandSlug} --dry-run`,
      apply:
        `npm run brand-explorer-v47-batch-image-materialization -- --brands ${brandSlug} --apply ` +
        REQUIRED_APPLY_FLAGS.join(" "),
      buildDraft: `npm run brand-explorer-active-profile-build-draft -- --brand ${brandSlug} --dry-run`,
    },
  };
}

async function airtablePresentationWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const body = method === "POST" ? { fields } : { fields };
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} failed: ${res.status}`);
  return json;
}

function buildPresentationWriteFields(patch, brandConfig) {
  return {
    "Slot Key": patch.slotKey,
    "Brand Name": brandConfig.name,
    Brand: [brandConfig.recordId],
    Active: true,
    "Sort Order":
      patch.slotKey === OPENINGS_SLOT
        ? 10
        : Number(String(patch.planSlotKey || patch.slotKey).match(/(\d+)$/)?.[1] || 0) || 0,
    ...patch.fields,
  };
}

function registryStubToStaged(stub) {
  return {
    assetName: stub.assetName,
    assetType: ASSET_TYPE.PR_IMAGE,
    assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    sourceBasis: SOURCE_BASIS.RENDERED_OFFICIAL,
    sourceUrl: stub.sourceUrl,
    sourcePageUrl: stub.sourcePageUrl,
    usageReviewStatus: "Pending Review",
    explorerUsePermission: "Candidate Only",
    recommendedExplorerSlot: stub.recommendedExplorerSlot,
    notes: stub.notes,
  };
}

export function parseV47ApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export async function applyV47MaterializationPlans({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseV47ApplyFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flagCheck };
  }
  if (!flagCheck.ok) {
    return {
      applied: false,
      reason: "missing_apply_flags",
      missing: flagCheck.missing,
    };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (brand.materializationBlocked) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "materialization_blocked",
        blockers: brand.blockers,
      };
      continue;
    }
    if (V47_PROTECTED_RELEASED.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to released brand ${brand.brandSlug}`);
    }

    const brandConfig = getActiveProfileBrandConfig(brand.brandSlug);
    const results = {
      presentationCreated: [],
      presentationUpdated: [],
      registryCreated: [],
      registrySkipped: [],
      errors: [],
    };

    for (const patch of brand.presentationPatches || []) {
      const fields = buildPresentationWriteFields(patch, brandConfig);
      try {
        if (patch.recordId) {
          await airtablePresentationWrite({
            baseId,
            apiKey,
            table: PRESENTATION_TABLE,
            recordId: patch.recordId,
            fields,
            method: "PATCH",
          });
          results.presentationUpdated.push({
            recordId: patch.recordId,
            slotKey: patch.slotKey,
            planSlotKey: patch.planSlotKey,
          });
        } else {
          const json = await airtablePresentationWrite({
            baseId,
            apiKey,
            table: PRESENTATION_TABLE,
            recordId: "",
            fields,
            method: "POST",
          });
          results.presentationCreated.push({
            recordId: json.id,
            slotKey: patch.slotKey,
            planSlotKey: patch.planSlotKey,
          });
        }
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        results.errors.push({
          recordId: patch.recordId || null,
          slotKey: patch.planSlotKey || patch.slotKey,
          message: err.message,
        });
      }
    }

    const staged = (brand.registryCreates || []).map(registryStubToStaged);
    if (staged.length) {
      try {
        const registryApply = await applyRegistryRecords({
          brandRecordId: brandConfig.recordId,
          parentCompany: brandConfig.parentCompany || "",
          stagedAssets: staged,
          stagingRunId: "v47-batch-image-materialization",
        });
        results.registryCreated = (registryApply.created || []).map((r) => r.recordId);
        results.registrySkipped = registryApply.recordsSkippedDuplicates || [];
        if (registryApply.validationFailed?.length) {
          for (const fail of registryApply.validationFailed) {
            results.errors.push({
              slotKey: fail.assetName,
              message: (fail.errors || []).join("; "),
            });
          }
        }
      } catch (err) {
        results.errors.push({ stage: "registry", message: err.message });
      }
    }

    resultsByBrand[brand.brandSlug] = {
      applied:
        results.errors.length === 0 &&
        results.presentationCreated.length + results.presentationUpdated.length > 0,
      results,
    };
  }

  return { applied: true, resultsByBrand, flagCheck };
}

export async function runV47BatchImageMaterialization({
  brands = V47_TARGET_BRANDS,
  dryRun = true,
  argv = [],
} = {}) {
  for (const b of brands) {
    if (V47_PROTECTED_RELEASED.includes(b)) {
      throw new Error(`v47 refuses released brand ${b}`);
    }
    if (!V47_TARGET_BRANDS.includes(b)) {
      throw new Error(`v47 target brands only: ${V47_TARGET_BRANDS.join(", ")}`);
    }
  }

  const v46 = loadV46AcceptedPack();
  const osConfirm = await confirmV47OsRouting(brands);
  const baselineProtection = await protectV47ReleasedBaseline();

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await planV47BrandMaterialization(brandSlug, { v46BySlug: v46.bySlug }));
  }

  const applyResult = dryRun
    ? { applied: false, reason: "dry_run_only" }
    : await applyV47MaterializationPlans({ brandResults, apply: true, argv });

  const byStatus = brandResults.reduce((acc, b) => {
    const s = b.eligibility?.status || "unknown";
    acc[s] = (acc[s] || 0) + 1;
    return acc;
  }, {});

  return {
    version: V47_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    v46ReportPath: v46.path,
    osConfirm,
    baselineProtection,
    brandResults,
    applyResult,
    summary: {
      targets: brandResults.length,
      osRoutingPass: osConfirm.pass,
      baselineProtectionPass: baselineProtection.pass,
      byEligibility: byStatus,
      materializationBlocked: brandResults.filter((b) => b.materializationBlocked).length,
      anyUnlock: false,
      presentationWrites: dryRun ? false : applyResult.applied === true,
      sourceLibraryWrites: false,
      applyDraftAllowedProjected: brandResults.every((b) => b.eligibility?.apply_draft_allowed),
    },
    guardrails: {
      activeRelease: false,
      companyValidatedChanges: false,
      releasedBrandChanges: false,
      incompleteBrandUnlock: false,
      rawUrlsInOwnerFacingCopy: brandResults.some((b) => b.guardrails?.rawUrlsInOwnerFacingCopy),
      genericImagesAccepted: false,
      registryOnlyCountedAsRenderReady: false,
    },
  };
}

function brandMd(b) {
  const lines = [
    `# v47 Image Materialization — ${b.brandName || b.brandSlug}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    "",
    "## Eligibility",
    "",
    `- Status: **${b.eligibility?.status}**`,
    `- image_remediation_complete: ${b.eligibility?.image_remediation_complete}`,
    `- materialization_plan_ready: ${b.eligibility?.materialization_plan_ready}`,
    `- build_draft_ready: ${b.eligibility?.build_draft_ready}`,
    `- apply_draft_allowed (projected): ${b.eligibility?.apply_draft_allowed}`,
    `- Rationale: ${b.eligibility?.rationale}`,
    "",
    "## Live API (pre-materialization)",
    "",
    `- Gallery: **${b.liveApi.galleryImageUrlCount}** / 6`,
    `- Property examples: **${b.liveApi.propertyImageUrlCount}** / 3`,
    `- Scenarios: **${b.liveApi.scenarioImageUrlCount}** / 3`,
    "",
    `Section label: **${b.sectionLabel}**`,
    "",
    "## Render readiness projection",
    "",
    `- Gallery: ${b.projection.galleryProjected}`,
    `- Property: ${b.projection.propertyProjected}`,
    `- Scenarios: ${b.projection.scenarioProjected}`,
    `- Pass: **${b.projection.pass}**`,
    `- External remains locked: **true**`,
    "",
    "## Planned rows",
    "",
  ];
  for (const row of b.planRows || []) {
    lines.push(
      `- **${row.planSlotKey}** · ${row.title} · imageOk=${row.validation?.ok} · record=${row.recordId || "CREATE"}`
    );
    lines.push(`  - imageUrl: ${row.imageUrl}`);
    lines.push(`  - sourcePageUrl (internal): ${row.sourcePageUrl}`);
  }
  lines.push("", "## Brand-specific notes", "", "```json", JSON.stringify(b.brandSpecific, null, 2), "```");
  if (b.blockers?.length) {
    lines.push("", "## Blockers", "");
    for (const x of b.blockers) lines.push(`- ${x}`);
  }
  lines.push(
    "",
    "## Guardrails",
    "",
    "- presentationImageWritesPlanned: true (apply-gated)",
    "- sourceLibraryWrites: false",
    "- unlock: false",
    "- activeRelease: false",
    "- companyValidatedChanges: false",
    "- releasedBrandWrites: false",
    `- rawUrlsInOwnerFacingCopy: ${b.guardrails?.rawUrlsInOwnerFacingCopy}`,
    ""
  );
  return lines.join("\n");
}

export function writeV47Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const baselinePath = path.join(reportsDir, REPORT_BASELINE_MD);

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# v47 Brand Explorer Batch Image Materialization`,
    "",
    `Generated: ${report.generatedAt}`,
    "",
    report.dryRun
      ? "Dry-run / read-only. Plans Presentation Image materialization from v46 accepted packs."
      : "Apply mode — Presentation Image + registry candidate writes only when gates passed.",
    "",
    "## Summary",
    "",
    `- OS routing pass: **${report.summary.osRoutingPass}**`,
    `- Baseline protection: **${report.summary.baselineProtectionPass}**`,
    `- Eligibility: ${JSON.stringify(report.summary.byEligibility)}`,
    `- Materialization blocked brands: **${report.summary.materializationBlocked}**`,
    `- Presentation writes: **${report.summary.presentationWrites}**`,
    `- apply_draft_allowed projected: **${report.summary.applyDraftAllowedProjected}**`,
    "",
    "## Target routing",
    "",
    "| Brand | OS state | Next action | Full profile | Eligibility | Projection |",
    "|---|---|---|---|---|---|",
  ];
  for (const t of report.osConfirm.targets || []) {
    const b = report.brandResults.find((x) => x.brandSlug === t.brandSlug);
    md.push(
      `| ${t.brandSlug} | ${t.canonicalState} | ${t.allowedNextAction} | ${t.shouldRenderFullProfile} | ${b?.eligibility?.status} | ${b?.projection?.galleryProjected} g / ${b?.projection?.propertyProjected} p |`
    );
  }
  md.push("", "## Guardrails", "");
  for (const [k, v] of Object.entries(report.guardrails || {})) {
    md.push(`- ${k}: ${v}`);
  }
  md.push("");
  fs.writeFileSync(mdPath, md.join("\n"));

  const baselineMd = [
    `# v47 Released Baseline Protection`,
    "",
    `Generated: ${report.generatedAt}`,
    "",
    `Pass: **${report.baselineProtection.pass}**`,
    "",
    "| Brand | Active ready | Full | Gallery | Property | Tabs | CV | Ext lock |",
    "|---|---|---|---|---|---|---|---|",
  ];
  for (const s of report.baselineProtection.releasedRows || []) {
    baselineMd.push(
      `| ${s.brandSlug} | ${s.active_profile_ready} | ${s.shouldRenderFullProfile} | ${s.galleryImageUrlCount} | ${s.propertyImageUrlCount} | ${s.externalTabCount} | ${s.companyValidated} | ${s.externalQualityLockPass} |`
    );
  }
  baselineMd.push(
    "",
    "## Guardrails",
    "",
    "- No writes to released brands",
    "- Incomplete brands must stay locked",
    "- Company Validated untouched",
    ""
  );
  fs.writeFileSync(baselinePath, baselineMd.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    const name = BRAND_REPORT_MD[b.brandSlug];
    if (!name) continue;
    const p = path.join(reportsDir, name);
    fs.writeFileSync(p, brandMd(b));
    brandPaths[b.brandSlug] = p;
  }

  return { jsonPath, mdPath, baselinePath, brandPaths };
}
