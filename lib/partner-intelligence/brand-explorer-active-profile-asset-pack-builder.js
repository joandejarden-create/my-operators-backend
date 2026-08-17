/**
 * Brand Explorer Active Profile Asset Pack Builder v34B.
 *
 * Generic asset discovery for gallery, property examples, scenarios, momentum,
 * standard detail, and proof-card source support.
 */
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import {
  assignBrandGalleryImagesFromPool,
  classifyPropertyExampleImage,
  isGenericBrandOrLifestyleImageUrl,
  isLogoImageUrl,
  resolvePropertySpecificHotelImage,
} from "./brand-explorer-footprint-opening-image-governance.js";
import { findRegistryAssetForPresentationRow } from "./brand-explorer-brand-asset-image-governance.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  buildGalleryCandidates,
  probePropertyPage,
} from "./brand-explorer-lifestyle-affiliation-source-capture-v35C.js";

export const ASSET_PACK_VERSION = "v34B";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

const GALLERY_KINDS = Object.freeze([
  "exterior",
  "guest_room",
  "kitchen_suite",
  "suite_work",
  "kitchen_detail",
  "room_detail",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function isGenericIhgBrandHeroImage(url) {
  const u = normalizeUrlKey(url);
  if (!u) return false;
  if (/digital\.ihg\.com\/is\/image\/ihg\/hotel-indigo-[a-z0-9-]+/i.test(u)) return false;
  if (/digital\.ihg\.com\/is\/image\/ihg\/ihg-[a-z0-9-]+/i.test(u)) {
    return !/hotelindigo/i.test(u);
  }
  return false;
}

function loadVisualPackReport(brandSlug) {
  const p = path.join(REPORTS_DIR, `visual-asset-pack-${brandSlug}-v37a.json`);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function classifyVisualCandidateValidity(candidate, brandSlug) {
  const imageUrl = nz(candidate.imageUrl);
  if (!imageUrl) return { accepted: false, rejectionReason: "missing_image_url" };
  if (isLogoImageUrl(imageUrl)) return { accepted: false, rejectionReason: "logo_image" };
  if (isGenericBrandOrLifestyleImageUrl(imageUrl)) {
    return { accepted: false, rejectionReason: "generic_or_lifestyle_image" };
  }
  if (brandSlug === "hotel-indigo" && isGenericIhgBrandHeroImage(imageUrl)) {
    return { accepted: false, rejectionReason: "generic_ihg_brand_hero" };
  }
  return { accepted: true, rejectionReason: null };
}

function buildReportCandidatePack(brandSlug) {
  const report = loadVisualPackReport(brandSlug);
  if (!report) return null;
  const gallery = Array.isArray(report.gallery) ? report.gallery : [];
  const openings = Array.isArray(report.propertyExamples) ? report.propertyExamples : [];

  const normalizedGallery = gallery
    .map((g) => {
      const validity = classifyVisualCandidateValidity(g, brandSlug);
      return {
        slotKey: nz(g.intendedSlot),
        propertyName: nz(g.propertyName),
        sourcePageUrl: nz(g.sourcePageUrl),
        imageUrl: nz(g.imageUrl),
        accepted: validity.accepted,
        rejectionReason: validity.rejectionReason,
      };
    })
    .filter((g) => g.accepted && g.imageUrl);

  const normalizedOpenings = openings
    .map((o) => {
      const validity = classifyVisualCandidateValidity(o, brandSlug);
      return {
        propertyName: nz(o.propertyName),
        sourcePageUrl: nz(o.sourcePageUrl),
        imageUrl: nz(o.imageUrl),
        accepted: validity.accepted,
        rejectionReason: validity.rejectionReason,
      };
    })
    .filter((o) => o.accepted && o.imageUrl);

  return {
    source: "v37a_visual_asset_pack_report",
    gallery: normalizedGallery,
    propertyExamples: normalizedOpenings,
  };
}

function classifyImageType(imageUrl, { slotType = "gallery" } = {}) {
  if (!nz(imageUrl)) {
    return {
      imageType: "missing",
      isHotelPhotography: false,
      isLogo: false,
      isLifestyle: false,
      isGenericBrand: false,
    };
  }
  if (isLogoImageUrl(imageUrl)) {
    return {
      imageType: "logo",
      isHotelPhotography: false,
      isLogo: true,
      isLifestyle: false,
      isGenericBrand: false,
    };
  }
  if (isGenericBrandOrLifestyleImageUrl(imageUrl)) {
    return {
      imageType: "generic_or_lifestyle",
      isHotelPhotography: false,
      isLogo: false,
      isLifestyle: true,
      isGenericBrand: true,
    };
  }
  const cls = classifyPropertyExampleImage(imageUrl);
  return {
    imageType: cls.isHotelPhotography ? "hotel_property_photography" : slotType,
    isHotelPhotography: cls.isHotelPhotography,
    isLogo: cls.isLogo,
    isLifestyle: cls.isLifestyle,
    isGenericBrand: cls.isGenericBrand,
  };
}

function buildGallerySlotTargets(brandConfig, presentationRows) {
  const titles = brandConfig.gallerySlotTitles || [];
  return titles.map((title, i) => {
    const slotKey = `materials.gallery.${i + 1}`;
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    const catalogEntry = brandConfig.propertyCatalog[i % brandConfig.propertyCatalog.length] || null;
    return {
      slotKey,
      title,
      kind: GALLERY_KINDS[i] || "property",
      propertyKey: catalogEntry?.propertyKey || "",
      recordId: row?.recordId || null,
    };
  });
}

async function assignLifestyleGalleryImagesFromPool(slotTargets, propertyCatalog, brandSlug) {
  const reportPack = buildReportCandidatePack(brandSlug);
  if (reportPack?.gallery?.length) {
    const imagePool = reportPack.gallery.map((candidate, index) => ({
      propertyId: nz(candidate.propertyName).toLowerCase().replace(/\s+/g, "-"),
      sourcePageUrl: candidate.sourcePageUrl,
      imageUrl: candidate.imageUrl,
      label: GALLERY_KINDS[index] || "property",
      imageSource: reportPack.source,
    }));
    const assignments = slotTargets.map((target, index) => {
      const slotHit = reportPack.gallery.find(
        (c) => normalizeUrlKey(c.slotKey) === normalizeUrlKey(target.slotKey)
      );
      const candidate = slotHit || reportPack.gallery[index] || null;
      return {
        slotKey: target.slotKey,
        ok: Boolean(candidate?.imageUrl),
        imageUrl: candidate?.imageUrl || "",
        sourcePageUrl: candidate?.sourcePageUrl || "",
        propertyId: target.propertyKey || "",
        propertyName: candidate?.propertyName || "",
        error: candidate?.imageUrl ? "" : "no_report_gallery_image",
        fallbackFrom: candidate ? reportPack.source : "",
      };
    });
    return { assignments, imagePool, poolSize: imagePool.length };
  }

  const propertyProbes = [];
  for (const property of propertyCatalog) {
    propertyProbes.push(await probePropertyPage(property, brandSlug));
    await new Promise((r) => setTimeout(r, 120));
  }

  const galleryCandidates = buildGalleryCandidates(propertyProbes);
  const imagePool = galleryCandidates.map((candidate, index) => ({
    propertyId: nz(candidate.propertyName).toLowerCase().replace(/\s+/g, "-"),
    sourcePageUrl: candidate.sourcePageUrl,
    imageUrl: candidate.imageUrl,
    label: GALLERY_KINDS[index] || "property",
    imageSource: "lifestyle_property_page_probe",
  }));

  const assignments = slotTargets.map((target, index) => {
    const candidate = galleryCandidates[index] || null;
    return {
      slotKey: target.slotKey,
      ok: Boolean(candidate?.imageUrl),
      imageUrl: candidate?.imageUrl || "",
      sourcePageUrl: candidate?.sourcePageUrl || "",
      propertyId: target.propertyKey || "",
      propertyName: candidate?.propertyName || "",
      error: candidate?.imageUrl ? "" : "no_lifestyle_gallery_image",
      fallbackFrom: candidate ? "lifestyle_property_page_probe" : "",
    };
  });

  return { assignments, imagePool, poolSize: imagePool.length };
}

async function resolveLifestylePropertyExampleImage(catalog, brandSlug) {
  const reportPack = buildReportCandidatePack(brandSlug);
  if (reportPack?.propertyExamples?.length) {
    const byName = reportPack.propertyExamples.find((entry) =>
      normalizeUrlKey(entry.sourcePageUrl) === normalizeUrlKey(catalog.sourcePageUrl) ||
      nz(entry.propertyName).toLowerCase() === nz(catalog.propertyName).toLowerCase()
    );
    if (byName?.imageUrl) {
      return {
        ok: true,
        imageUrl: byName.imageUrl,
        imageKind: "hotel_property_photography",
        imageSource: reportPack.source,
        imageSourcePageUrl: byName.sourcePageUrl || catalog.sourcePageUrl,
        propertyId: catalog.propertyKey,
      };
    }
  }

  const probe = await probePropertyPage(catalog, brandSlug);
  if (probe.primaryImage?.imageUrl) {
    return {
      ok: true,
      imageUrl: probe.primaryImage.imageUrl,
      imageKind: "hotel_property_photography",
      imageSource: "lifestyle_property_page_probe",
      imageSourcePageUrl: catalog.sourcePageUrl,
      propertyId: catalog.propertyKey,
    };
  }
  return {
    ok: false,
    error: probe.error || "no_property_specific_hotel_image",
    propertyId: catalog.propertyKey,
    imageSourcePageUrl: catalog.sourcePageUrl,
  };
}

function assetRecord({
  slotKey,
  intendedSlot,
  sourcePageUrl,
  imageUrl,
  registryRowCandidate = null,
  sourceConfidence = "medium",
  imageType = "hotel_property_photography",
  approvalStatus = "pending_founder_review",
  renderReadiness = "needs_materialization",
  assetRole,
  propertyKey = "",
  propertyName = "",
  notes = "",
}) {
  const classification = classifyImageType(imageUrl, {
    slotType: assetRole === "property_example" ? "property_example" : "gallery",
  });
  const blockedAsPropertyExample =
    assetRole === "property_example" &&
    (classification.isLogo || classification.isGenericBrand || classification.isLifestyle);

  return {
    slotKey,
    intendedSlot,
    sourcePageUrl: nz(sourcePageUrl),
    imageUrl: nz(imageUrl),
    registryRowCandidate,
    sourceConfidence,
    imageTypeClassification: classification.imageType,
    isHotelPhotography: classification.isHotelPhotography,
    approvalStatus,
    renderReadiness: !imageUrl
      ? "missing_image"
      : blockedAsPropertyExample
        ? "blocked_logo_or_lifestyle"
        : renderReadiness,
    assetRole,
    propertyKey,
    propertyName,
    notes,
    passesPropertyExampleRule: assetRole !== "property_example" || !blockedAsPropertyExample,
    passesGalleryRule:
      assetRole !== "gallery" ||
      (Boolean(imageUrl) &&
        !classification.isLogo &&
        !classification.isGenericBrand &&
        !classification.isLifestyle),
  };
}

export async function buildActiveProfileAssetPack({
  brandSlug,
  presentationRows = [],
  registryAssets = [],
  brandApi = null,
} = {}) {
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) {
    throw new Error(`No active profile brand config for: ${brandSlug}`);
  }

  const galleryTargets = buildGallerySlotTargets(brandConfig, presentationRows);
  const catalogForProbe = brandConfig.propertyCatalog.map((c) => ({
    sourcePageUrl: c.sourcePageUrl,
    propertyName: c.propertyName,
  }));

  const useLifestyleProbe = brandConfig.galleryPoolStrategy === "lifestyle_property_page_probe";
  const galleryAssignment = useLifestyleProbe
    ? await assignLifestyleGalleryImagesFromPool(
        galleryTargets,
        brandConfig.propertyCatalog,
        brandSlug
      )
    : await assignBrandGalleryImagesFromPool(galleryTargets, catalogForProbe, {
        fixturePath: brandConfig.galleryPoolFixture
          ? path.join(ROOT, brandConfig.galleryPoolFixture)
          : undefined,
      });

  const galleryAssets = galleryAssignment.assignments.map((a, i) => {
    const target = galleryTargets[i];
    const row = presentationRows.find((r) => r.slotKey === a.slotKey);
    const registry = row ? findRegistryAssetForPresentationRow(registryAssets, row) : null;
    const apiBlock = (brandApi?.brandExplorer?.blocks || []).find((b) => b.slotKey === a.slotKey);
    const hasMaterialized = Boolean(row?.imageUrl && apiBlock?.imageUrl);
    return assetRecord({
      slotKey: a.slotKey,
      intendedSlot: a.slotKey,
      sourcePageUrl: a.sourcePageUrl,
      imageUrl: a.ok ? a.imageUrl : "",
      registryRowCandidate: registry?.recordId || `(new registry for ${a.slotKey})`,
      sourceConfidence: a.ok ? "high" : "none",
      imageType: a.ok ? "hotel_property_photography" : "missing",
      approvalStatus: registry?.explorerUsePermission === "Approved For Explorer"
        ? "approved"
        : "pending_founder_review",
      renderReadiness: hasMaterialized ? "ready" : a.ok ? "needs_materialization" : "blocked_no_source",
      assetRole: "gallery",
      propertyKey: a.propertyId,
      propertyName: a.propertyName || "",
      notes: a.error || a.fallbackFrom || "",
    });
  });

  const propertyExampleAssets = [];
  const propertyExampleCatalog =
    brandConfig.propertyExampleCatalog || brandConfig.propertyCatalog.slice(0, brandConfig.propertyExampleMinimum);
  for (const catalog of propertyExampleCatalog) {
    const discovery =
      brandConfig.propertyExampleStrategy === "lifestyle_property_page_probe"
        ? await resolveLifestylePropertyExampleImage(catalog, brandSlug)
        : await resolvePropertySpecificHotelImage(catalog.sourcePageUrl, {
            fixturePool: galleryAssignment.imagePool,
          });
    const row =
      presentationRows.find((r) => r.recordId === catalog.presentationRecordId) ||
      presentationRows.find(
        (r) =>
          r.slotKey === "footprint.openings" &&
          nz(r.title).toLowerCase().includes(nz(catalog.marketCity).toLowerCase())
      ) ||
      null;
    const registry = row ? findRegistryAssetForPresentationRow(registryAssets, row) : null;

    propertyExampleAssets.push(
      assetRecord({
        slotKey: row?.slotKey || "footprint.openings",
        intendedSlot: `footprint.openings — ${catalog.propertyName}`,
        sourcePageUrl: catalog.sourcePageUrl,
        imageUrl: discovery.ok ? discovery.imageUrl : "",
        registryRowCandidate: registry?.recordId || `(new registry for ${catalog.propertyKey})`,
        sourceConfidence: discovery.ok ? "high" : "none",
        imageType: discovery.ok ? discovery.imageKind : "missing",
        approvalStatus: registry ? "pending_founder_review" : "pending_founder_review",
        renderReadiness: discovery.ok ? "needs_materialization" : "blocked_no_source",
        assetRole: "property_example",
        propertyKey: catalog.propertyKey,
        propertyName: catalog.propertyName,
        notes: discovery.ok
          ? "Official property page hotel/property image"
          : discovery.error || "no_property_specific_hotel_image",
      })
    );
  }

  const scenarioAssets = [];
  const scenarioCopy = brandConfig.overviewScenarioCopy || {};
  for (let i = 1; i <= brandConfig.scenarioMinimum; i += 1) {
    const slotKey = `overview.scenario.${i}`;
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    const copy = scenarioCopy[slotKey] || {};
    const poolImage = galleryAssignment.imagePool[i - 1] || null;
    const resolvedUrl =
      nz(poolImage?.imageUrl) || propertyExampleAssets[i - 1]?.imageUrl || "";
    scenarioAssets.push(
      assetRecord({
        slotKey,
        intendedSlot: slotKey,
        sourcePageUrl: poolImage?.sourcePageUrl || brandConfig.consumerUrl,
        imageUrl: resolvedUrl,
        registryRowCandidate: row
          ? findRegistryAssetForPresentationRow(registryAssets, row)?.recordId ||
            `(new registry for ${slotKey})`
          : `(new row for ${slotKey})`,
        sourceConfidence: resolvedUrl ? "medium" : "none",
        imageType: "scenario_card",
        approvalStatus: "pending_founder_review",
        renderReadiness: resolvedUrl ? "needs_materialization" : "hide_or_partial",
        assetRole: "scenario",
        propertyKey: poolImage?.propertyId || "",
        propertyName: copy.title || row?.title || "",
        notes: copy.title ? "copy package available" : "needs copy package",
      })
    );
  }

  const momentumSources = (brandConfig.momentumSourceUrls || []).map((url, i) => ({
    slotKey: "footprint.momentum",
    intendedSlot: `momentum_source_${i + 1}`,
    sourcePageUrl: url,
    imageUrl: "",
    registryRowCandidate: null,
    sourceConfidence: "medium",
    imageTypeClassification: "text_source",
    approvalStatus: "source_candidate",
    renderReadiness: "copy_only",
    assetRole: "momentum",
    notes: "Momentum narrative source — no image required",
  }));

  const standardDetailSources = [
    {
      slotKey: "standard_detail",
      intendedSlot: "standard_detail / where_available",
      sourcePageUrl: brandConfig.consumerUrl,
      imageUrl: "",
      sourceConfidence: "medium",
      approvalStatus: brandConfig.standardDetailGovernanceRequired
        ? "pending_governance_review"
        : "not_required",
      renderReadiness: "governance_gate",
      assetRole: "standard_detail",
      notes: "Requires founder governance review before apply",
    },
  ];

  const proofSupport = [1, 2, 3, 4, 5, 6].map((i) => {
    const slotKey = `overview.proof.${i}`;
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    return {
      slotKey,
      intendedSlot: slotKey,
      sourcePageUrl: brandConfig.consumerUrl,
      imageUrl: row?.imageUrl || "",
      registryRowCandidate: row
        ? findRegistryAssetForPresentationRow(registryAssets, row)?.recordId || null
        : null,
      sourceConfidence: row?.body ? "medium" : "low",
      approvalStatus: row ? "existing_row" : "needs_copy",
      renderReadiness: row?.visible ? "existing" : "needs_review",
      assetRole: "proof_card",
    };
  });

  const galleryReady = galleryAssets.filter((a) => a.passesGalleryRule).length;
  const propertyReady = propertyExampleAssets.filter((a) => a.passesPropertyExampleRule && a.imageUrl).length;
  const scenarioReady = scenarioAssets.filter((a) => a.imageUrl).length;

  const partialReadiness =
    galleryReady < brandConfig.galleryMinimum ||
    propertyReady < brandConfig.propertyExampleMinimum;

  let readinessBand = "full";
  if (!brandConfig.propertyCatalog.length) readinessBand = "blocked_missing_catalog";
  else if (galleryReady === 0 && propertyReady === 0) readinessBand = "blocked_no_assets";
  else if (partialReadiness) readinessBand = "partial";

  return {
    assetPackVersion: ASSET_PACK_VERSION,
    brandSlug,
    brandConfig: {
      slug: brandConfig.slug,
      recordId: brandConfig.recordId,
      name: brandConfig.name,
      brandFamily: brandConfig.brandFamily,
      geographicFallbackRule: brandConfig.geographicFallbackRule,
      propertyExampleStrategy: brandConfig.propertyExampleStrategy,
      galleryMinimum: brandConfig.galleryMinimum,
      propertyExampleMinimum: brandConfig.propertyExampleMinimum,
      scenarioMinimum: brandConfig.scenarioMinimum,
    },
    gallery: galleryAssets,
    propertyExamples: propertyExampleAssets,
    scenarios: scenarioAssets,
    momentumSources,
    standardDetailSources,
    proofSupport,
    registryCandidateMapping: [
      ...galleryAssets,
      ...propertyExampleAssets,
      ...scenarioAssets,
    ].map((a) => ({
      slotKey: a.slotKey,
      intendedSlot: a.intendedSlot,
      registryRowCandidate: a.registryRowCandidate,
      sourcePageUrl: a.sourcePageUrl,
      imageUrl: a.imageUrl,
    })),
    summary: {
      galleryReady: `${galleryReady}/${brandConfig.galleryMinimum}`,
      propertyExamplesReady: `${propertyReady}/${brandConfig.propertyExampleMinimum}`,
      scenariosWithImage: `${scenarioReady}/${brandConfig.scenarioMinimum}`,
      galleryPoolSize: galleryAssignment.poolSize,
      partialReadiness,
      readinessBand,
      canProceedWithConfigOnly: false,
      canProceedWithConfigAndAssetPack: readinessBand === "full" || readinessBand === "partial",
      customCodeRequired: readinessBand === "blocked_missing_catalog",
    },
    enforcement: {
      galleryMinimum: brandConfig.galleryMinimum,
      propertyExampleMinimum: brandConfig.propertyExampleMinimum,
      propertyExamplesPartialAllowed: true,
      noLogoLifestyleAsPropertyExamples: true,
      usFallbackLabeled: brandConfig.geographicFallbackRule.includes("us_"),
    },
  };
}
