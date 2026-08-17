/**
 * Brand Explorer v36B — Visual Render Readiness Contract (read-only).
 *
 * Distinguishes registryReadiness from renderReadiness on asset-pack output.
 */
import { countVisibleGalleryBlocksWithImageUrl } from "./brand-explorer-brand-asset-image-governance.js";
import { ATELIER_SCENARIO_FALLBACK_TITLES } from "./brand-explorer-active-profile-factory-rules.js";

export const RENDER_READINESS_CONTRACT_VERSION = "v36B";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isImagePlaceholder(url) {
  const u = nz(url).toLowerCase();
  return !u || u.includes("placeholder") || u.includes("via.placeholder");
}

export function evaluateRegistryReadiness(asset, { registryAssets = [] } = {}) {
  const hasRegistryCandidate = Boolean(
    asset.registryRowCandidate && !String(asset.registryRowCandidate).startsWith("(new")
  );
  const registryMatch = registryAssets.find(
    (r) => r.recordId === asset.registryRowCandidate || r.id === asset.registryRowCandidate
  );
  const approved =
    asset.approvalStatus === "approved" ||
    registryMatch?.explorerUsePermission === "Approved For Explorer" ||
    registryMatch?.fields?.["Approved For Explorer Use"] === "Yes";

  return {
    registryCandidateExists: hasRegistryCandidate || Boolean(asset.imageUrl),
    approvedStatus: approved ? "approved" : asset.approvalStatus || "pending",
    sourceSupport: Boolean(asset.sourcePageUrl || asset.imageUrl),
    brandMatch: true,
    intendedSlot: asset.intendedSlot || asset.slotKey,
    pass: Boolean(asset.imageUrl || asset.sourcePageUrl),
    notes: asset.notes || "",
  };
}

export function evaluateRenderReadiness(asset, { presentationRow = null, apiBlock = null } = {}) {
  const row = presentationRow || {};
  const api = apiBlock || {};
  const presentationImageExists = Boolean(row.imageUrl);
  const apiImageUrl = Boolean(api.imageUrl);
  const imageUrlLoads = apiImageUrl && !isImagePlaceholder(api.imageUrl);
  const rowVisible = row.visible !== false && row.active !== false;
  const noPlaceholder = !isImagePlaceholder(row.imageUrl || api.imageUrl);

  const isVisualSlot =
    asset.assetRole === "gallery" ||
    asset.assetRole === "property_example" ||
    asset.assetRole === "scenario";

  let pass = true;
  const blockers = [];

  if (isVisualSlot) {
    if (!presentationImageExists) blockers.push("missing_presentation_image_field");
    if (!apiImageUrl) blockers.push("api_missing_imageUrl");
    if (!imageUrlLoads) blockers.push("imageUrl_not_loadable");
    if (!rowVisible && asset.assetRole !== "scenario") blockers.push("row_not_visible");
    if (!noPlaceholder) blockers.push("image_placeholder");
    pass = blockers.length === 0;
  } else if (asset.assetRole === "momentum") {
    pass = Boolean(asset.sourcePageUrl);
  } else {
    pass = rowVisible ? Boolean(nz(row.title) || nz(row.body)) : true;
  }

  return {
    presentationImageFieldExists: presentationImageExists,
    apiReturnsImageUrl: apiImageUrl,
    imageUrlLoads,
    rowVisible,
    noImagePlaceholder: noPlaceholder,
    pass,
    blockers,
    status: pass ? "render_ready" : asset.renderReadiness || "needs_materialization",
  };
}

export function extendAssetPackWithRenderReadiness(
  assetPack,
  { presentationRows = [], brandApi = null, registryAssets = [] } = {}
) {
  if (!assetPack) return null;

  const blocks = brandApi?.brandExplorer?.blocks || [];
  const enrich = (asset) => {
    let row = presentationRows.find((r) => r.slotKey === asset.slotKey) || null;
    if (asset.assetRole === "property_example" && asset.propertyName) {
      row =
        presentationRows.find(
          (r) =>
            r.slotKey === "footprint.openings" &&
            nz(r.title).toLowerCase().includes(nz(asset.propertyName).toLowerCase())
        ) || row;
    }
    const apiBlock =
      blocks.find((b) => b.slotKey === asset.slotKey && (!asset.propertyName || nz(b.title).includes(asset.propertyName))) ||
      blocks.find((b) => b.recordId && b.recordId === row?.recordId) ||
      blocks.find((b) => b.slotKey === asset.slotKey) ||
      null;
    const registryReadiness = evaluateRegistryReadiness(asset, { registryAssets });
    const renderReadiness = evaluateRenderReadiness(asset, { presentationRow: row, apiBlock: apiBlock });
    return {
      ...asset,
      registryReadiness,
      renderReadiness,
      renderReady: renderReadiness.pass,
      registryReady: registryReadiness.pass,
      readinessGap: registryReadiness.pass && !renderReadiness.pass ? "registry_only_not_render" : null,
    };
  };

  const gallery = (assetPack.gallery || []).map(enrich);
  const propertyExamples = (assetPack.propertyExamples || []).map(enrich);
  const scenarios = (assetPack.scenarios || []).map(enrich);

  const visibleGallery = countVisibleGalleryBlocksWithImageUrl(brandApi);
  const galleryRenderReady = gallery.filter((a) => a.renderReady).length;
  const propertyRenderReady = propertyExamples.filter((a) => a.renderReady).length;
  const scenarioRenderReady = scenarios.filter((a) => a.renderReady).length;

  const scenarioFallbackRisk = [1, 2, 3].filter((i) => {
    const slotKey = `overview.scenario.${i}`;
    const hasBlock = blocks.some((b) => b.slotKey === slotKey && b.imageUrl);
    return !hasBlock;
  }).map((i) => `overview.scenario.${i}`);

  return {
    contractVersion: RENDER_READINESS_CONTRACT_VERSION,
    brandSlug: assetPack.brandSlug,
    summary: {
      galleryRegistryReady: gallery.filter((a) => a.registryReady).length,
      galleryRenderReady,
      galleryMinimum: assetPack.brandConfig?.galleryMinimum || 6,
      visibleGalleryInApi: visibleGallery,
      propertyExamplesRenderReady: propertyRenderReady,
      propertyExampleMinimum: assetPack.brandConfig?.propertyExampleMinimum || 3,
      scenariosRenderReady: scenarioRenderReady,
      scenarioMinimum: assetPack.brandConfig?.scenarioMinimum || 3,
      registryOnlyCount: [...gallery, ...propertyExamples, ...scenarios].filter(
        (a) => a.registryReady && !a.renderReady
      ).length,
      scenarioFallbackRisk,
      atelierScenarioFallbackTitles: ATELIER_SCENARIO_FALLBACK_TITLES,
    },
    gallery,
    propertyExamples,
    scenarios,
    momentumSources: assetPack.momentumSources || [],
    proofSupport: assetPack.proofSupport || [],
    rules: {
      registryReadyNotEqualRenderReady: true,
      activeProfileRequiresRenderReadyImages: true,
      galleryMinimumVisibleRenderReady: assetPack.brandConfig?.galleryMinimum || 6,
      propertyExamplesRequireHotelImages: true,
      scenarioCardsNoPlaceholders: true,
    },
    pass:
      galleryRenderReady >= (assetPack.brandConfig?.galleryMinimum || 6) &&
      propertyRenderReady >= (assetPack.brandConfig?.propertyExampleMinimum || 3) &&
      scenarioRenderReady >= (assetPack.brandConfig?.scenarioMinimum || 3) &&
      scenarioFallbackRisk.length === 0,
  };
}
