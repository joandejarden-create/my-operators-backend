/**
 * Brand Explorer Brand Asset Registry image governance v31B + v33C-R2.
 *
 * Shared helpers for discovery writer, Final QA, and visual defect audit.
 * Expansion brands require approved registry assets or explicit pending review;
 * wrong-brand materialized imagery blocks active-profile readiness.
 * Property Example cards require hotel/property photography (not logo/lifestyle).
 */
import {
  detectGalleryNonHotelImageDefects,
  detectPropertyExampleImageDefects,
  isOfficialLifestylePropertyImageUrl,
  isPropertyExampleTitle,
} from "./brand-explorer-footprint-opening-image-governance.js";

export const IMAGE_GOVERNANCE_VERSION = "33C-R2";

const VISUAL_IMAGE_SLOT_RE =
  /^(footprint\.openings|materials\.gallery\.\d|overview\.hero|overview\.scenario\.\d)$/;

export const WRONG_BRAND_SIGNAGE_MARKERS = [
  { id: "quality_inn", re: /\bquality inn\b/i, severity: "critical" },
  { id: "comfort_inn", re: /\bcomfort inn\b/i, severity: "high" },
  { id: "clarion", re: /\bclarion\b/i, severity: "high" },
  { id: "sleep_inn", re: /\bsleep inn\b/i, severity: "high" },
  { id: "mainstay", re: /\bmainstay suites\b/i, severity: "high" },
  { id: "woodspring", re: /\bwoodspring\b/i, severity: "high" },
  { id: "everhome", re: /\beverhome\b/i, severity: "high" },
  { id: "marriott", re: /\bmarriott\b/i, severity: "critical" },
  { id: "hilton", re: /\bhilton\b/i, severity: "critical" },
  { id: "kimpton", re: /\bkimpton\b/i, severity: "critical" },
  { id: "curio", re: /\bcurio collection\b/i, severity: "critical" },
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

export function isVisualImageSlot(slotKey) {
  return VISUAL_IMAGE_SLOT_RE.test(nz(slotKey));
}

export function isGalleryImageSlot(slotKey) {
  return /^materials\.gallery\.\d+$/.test(nz(slotKey));
}

/** Active-profile gallery minimum — visible API cards with imageUrl (v33H). */
export const ACTIVE_PROFILE_GALLERY_MINIMUM = 6;

export function countVisibleGalleryBlocksWithImageUrl(brand) {
  const blocks = brand?.brandExplorer?.blocks || [];
  return blocks.filter(
    (b) => isGalleryImageSlot(b?.slotKey) && hasVal(b?.imageUrl)
  ).length;
}

export function galleryVisibleMinimumBlocksActiveProfile(brand, minimum = ACTIVE_PROFILE_GALLERY_MINIMUM) {
  return countVisibleGalleryBlocksWithImageUrl(brand) < minimum;
}

export function normalizeUrlKey(url) {
  return nz(url).replace(/\?.*$/, "").toLowerCase();
}

export function isRegistryAssetApprovedForExplorer(asset) {
  if (!asset) return false;
  if (nz(asset.explorerUsePermission) === "Do Not Use") return false;
  if (nz(asset.assetStatus) === "Do Not Use") return false;
  return (
    nz(asset.explorerUsePermission) === "Approved For Explorer" &&
    nz(asset.usageReviewStatus) === "Usage Review Complete"
  );
}

export function findRegistryAssetForPresentationRow(registryAssets, row) {
  const imageKey = normalizeUrlKey(row?.imageUrl);
  const sourceKey = normalizeUrlKey(row?.summaryUrl);
  const bodyUrlRaw = nz(row?.body).match(/https?:\/{1,2}[^\s<>"')]+/i)?.[0] || "";
  const bodyUrl =
    bodyUrlRaw.startsWith("https:/") && !bodyUrlRaw.startsWith("https://")
      ? bodyUrlRaw.replace(/^https:\//i, "https://")
      : bodyUrlRaw.startsWith("http:/") && !bodyUrlRaw.startsWith("http://")
        ? bodyUrlRaw.replace(/^http:\//i, "http://")
        : bodyUrlRaw;
  const bodyKey = normalizeUrlKey(bodyUrl);
  const slot = nz(row?.slotKey);
  const titleLead = nz(row?.title).split("—")[0].trim().toLowerCase();
  return (registryAssets || []).find((asset) => {
    if (nz(asset.recommendedExplorerSlot) === slot) {
      if (imageKey && normalizeUrlKey(asset.sourceUrl) === imageKey) return true;
      if (sourceKey && normalizeUrlKey(asset.sourcePageUrl) === sourceKey) return true;
      if (bodyKey && normalizeUrlKey(asset.sourcePageUrl) === bodyKey) return true;
      if (
        titleLead &&
        nz(asset.assetName).toLowerCase().includes(titleLead)
      ) {
        return true;
      }
      if (
        hasVal(row?.imageUrl) &&
        (isOfficialLifestylePropertyImageUrl(asset.sourceUrl) ||
          isOfficialLifestylePropertyImageUrl(asset.sourcePageUrl))
      ) {
        return true;
      }
      if (isRegistryAssetApprovedForExplorer(asset)) return true;
    }
    if (imageKey && normalizeUrlKey(asset.sourceUrl) === imageKey) return true;
    return false;
  });
}

export function detectWrongBrandSignageRisk(text, brandConfig = {}) {
  const haystack = nz(text);
  if (!haystack) return null;
  const hayLower = haystack.toLowerCase();
  const allowed = (brandConfig.allowedSiblingMentions || []).map((s) => s.toLowerCase());
  const brandName = nz(brandConfig.name).toLowerCase();
  const brandSlug = nz(brandConfig.slug).toLowerCase();
  const brandTokens = new Set(
    [
      brandName,
      brandSlug.replace(/-/g, " "),
      ...brandName.split(/\s+/).filter((t) => t.length > 2),
      ...allowed,
    ].filter(Boolean)
  );

  for (const marker of [...WRONG_BRAND_SIGNAGE_MARKERS, ...(brandConfig.extraWrongBrandMarkers || [])]) {
    if (!marker.re.test(haystack)) continue;
    const matchText = haystack.match(marker.re)?.[0] || marker.id;
    const matchLower = matchText.toLowerCase();
    const markerToken = marker.id.replace(/_/g, " ");

    if (brandName && (brandName.includes(matchLower) || matchLower.includes(brandName.split(/\s+/)[0]))) {
      continue;
    }
    if (
      [...brandTokens].some(
        (t) =>
          t &&
          (matchLower.includes(t) ||
            t.includes(matchLower) ||
            markerToken.includes(t) ||
            t.includes(markerToken))
      )
    ) {
      continue;
    }
    if (allowed.some((a) => hayLower.includes(a) && (markerToken.includes(a) || a.includes(markerToken)))) {
      continue;
    }

    return {
      markerId: marker.id,
      severity: marker.severity || "high",
      excerpt: haystack.slice(0, 160),
      reason: `Visible non-target brand reference: ${marker.id}`,
    };
  }
  return null;
}

export function assessPresentationRowImageGovernance(row, brandConfig, registryAssets) {
  const slotKey = nz(row?.slotKey);
  if (!isVisualImageSlot(slotKey)) return null;
  const combined = [row?.title, row?.body, row?.summaryUrl, row?.imageUrl].filter(Boolean).join("\n");
  const registryMatch = findRegistryAssetForPresentationRow(registryAssets, row);
  const hasImage = hasVal(row?.imageUrl) || hasVal(registryMatch?.sourceUrl);
  const approved = isRegistryAssetApprovedForExplorer(registryMatch);
  const wrongBrand = detectWrongBrandSignageRisk(combined, brandConfig);
  const humanSignageReviewRequired =
    hasImage && /footprint\.openings/.test(slotKey) && !approved && !registryMatch;
  const effectiveWrongBrand =
    wrongBrand ||
    (humanSignageReviewRequired
      ? {
          markerId: "human_visual_signage_review",
          severity: "high",
          reason:
            "Opening image lacks approved registry — verify signage is not wrong-brand (e.g. Quality Inn).",
        }
      : null);
  let recommendation = "remain_active";
  if (effectiveWrongBrand && hasImage) recommendation = "replace_and_queue_review";
  else if (wrongBrand) recommendation = "queue_review_wrong_brand_copy";
  else if (!hasImage) recommendation = "pending_image_review";
  else if (!registryMatch) recommendation = "queue_registry_review";
  else if (!approved) recommendation = "pending_image_review";
  else recommendation = "remain_active";

  return {
    presentationRowId: row?.recordId || null,
    slotKey,
    title: nz(row?.title),
    imageUrl: nz(row?.imageUrl) || null,
    sourceUrl: nz(row?.summaryUrl) || null,
    registryRecordId: registryMatch?.id || null,
    registryApproved: approved,
    brandMatched: !effectiveWrongBrand,
    wrongBrandRisk: effectiveWrongBrand,
    humanSignageReviewRequired,
    hasImage,
    recommendation,
    pendingImageReview: recommendation.includes("pending") || recommendation.includes("queue"),
  };
}

export function listVisualPresentationRows(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => isVisualImageSlot(b?.slotKey));
}

export const DISCOVERY_BRAND_CONFIG = Object.freeze({
  "radisson-individuals-by-choice": {
    slug: "radisson-individuals-by-choice",
    recordId: "recRyvM8OmLlDj9G7",
    name: "Radisson Individuals by Choice",
    parentCompany: "Choice Hotels International",
    consumerUrl: "https://www.radissonhotels.com/en-us/brand/radisson-individuals",
    pressKitUrl: "https://media.choicehotels.com/Radisson-Individuals-press-kit",
    officialDomains: ["radissonhotels.com", "choicehotels.com", "radisson.com", "media.choicehotels.com"],
    allowedSiblingMentions: [
      "radisson individuals",
      "radisson by choice",
      "choice privileges",
      "choice hotels",
      "faranda",
    ],
    extraWrongBrandMarkers: [{ id: "radisson_blu_signage", re: /\bradisson blu\b/i, severity: "high" }],
  },
  "suburban-studios": {
    slug: "suburban-studios",
    recordId: "reclcjg5Foa9Vs5TC",
    name: "Suburban Studios",
    parentCompany: "Choice Hotels International",
    consumerUrl: "https://www.choicehotels.com/suburban-studios",
    pressKitUrl: "https://www.choicehotels.com/",
    officialDomains: ["choicehotels.com", "suburbanstudios.com"],
    allowedSiblingMentions: ["suburban studios", "choice privileges", "choice hotels"],
    extraWrongBrandMarkers: [],
  },
  "woodspring-suites": {
    slug: "woodspring-suites",
    recordId: "recsOd51NzRPYsMko",
    name: "WoodSpring Suites",
    parentCompany: "Choice Hotels International",
    consumerUrl: "https://www.woodspring.com/",
    pressKitUrl: "https://www.choicehotels.com/",
    officialDomains: ["woodspring.com", "choicehotels.com"],
    allowedSiblingMentions: ["woodspring", "choice hotels"],
    extraWrongBrandMarkers: [],
  },
  "everhome-suites": {
    slug: "everhome-suites",
    recordId: null,
    name: "Everhome Suites",
    parentCompany: "Choice Hotels International",
    consumerUrl: "https://www.choicehotels.com/everhome-suites",
    pressKitUrl: "https://www.choicehotels.com/",
    officialDomains: ["choicehotels.com"],
    allowedSiblingMentions: ["everhome", "choice hotels"],
    extraWrongBrandMarkers: [],
  },
  kimpton: {
    slug: "kimpton",
    recordId: "recCKuXCmGvxHPfb3",
    name: "Kimpton Hotels",
    parentCompany: "InterContinental Hotels Group",
    consumerUrl: "https://kimptonhotels.com",
    officialDomains: ["kimptonhotels.com", "ihg.com"],
    allowedSiblingMentions: ["kimpton", "ihg", "ihg one rewards", "hotel indigo"],
    extraWrongBrandMarkers: [],
  },
});

export function getDiscoveryBrandConfig(slug) {
  return DISCOVERY_BRAND_CONFIG[nz(slug).toLowerCase()] || null;
}

function isExpansionBacklogBrandTarget(target) {
  return nz(target?.resolution?.resolutionSource) === "expansion_backlog";
}

export function detectBrandAssetImageGovernanceDefects(
  brand,
  registryAssets,
  brandConfig,
  brandTarget
) {
  if (!isExpansionBacklogBrandTarget(brandTarget)) return [];
  const defects = [];
  for (const row of listVisualPresentationRows(brand)) {
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryAssets);
    if (!assessment) continue;
    const registryMatch = findRegistryAssetForPresentationRow(registryAssets, row);

    for (const propertyDefect of detectPropertyExampleImageDefects(row, registryMatch, brandConfig)) {
      defects.push(propertyDefect);
    }
    for (const galleryDefect of detectGalleryNonHotelImageDefects(row, registryMatch)) {
      defects.push(galleryDefect);
    }

    if (assessment.wrongBrandRisk && assessment.hasImage) {
      defects.push({
        type: "wrong_brand_image",
        severity: assessment.wrongBrandRisk.severity || "critical",
        category: "data",
        surface: `presentation.${assessment.slotKey}`,
        recordId: assessment.presentationRowId,
        slotKey: assessment.slotKey,
        message: assessment.wrongBrandRisk.reason,
        recommendedFixBatch: "v31B_brand_asset_registry_discovery",
      });
      continue;
    }
    if (assessment.hasImage && !assessment.registryApproved) {
      if (isGalleryImageSlot(assessment.slotKey) && !assessment.wrongBrandRisk) {
        defects.push({
          type: "pending_gallery_image_review",
          severity: "low",
          category: "data",
          surface: `presentation.${assessment.slotKey}`,
          recordId: assessment.presentationRowId,
          slotKey: assessment.slotKey,
          message:
            "Gallery image attached pending Brand Asset Registry approval — visible in draft/internal views; does not count as approved visual evidence.",
          recommendedFixBatch: "v31D-R1_gallery_image_restore",
          cosmeticNonBlocking: true,
          activeProfileBlocker: true,
        });
        continue;
      }
      defects.push({
        type: "unapproved_image_materialized",
        severity: /footprint\.openings/.test(assessment.slotKey) ? "high" : "medium",
        category: "data",
        surface: `presentation.${assessment.slotKey}`,
        recordId: assessment.presentationRowId,
        slotKey: assessment.slotKey,
        message:
          "Image materialized on presentation row without approved Brand Asset Registry asset — queue pending_image_review or remove from active-profile evidence.",
        recommendedFixBatch: "v31B_brand_asset_registry_discovery",
      });
    }
    if (
      !assessment.hasImage &&
      /footprint\.openings/.test(assessment.slotKey) &&
      !isPropertyExampleTitle(row?.title)
    ) {
      defects.push({
        type: "pending_image_review",
        severity: "medium",
        category: "data",
        surface: `presentation.${assessment.slotKey}`,
        recordId: assessment.presentationRowId,
        slotKey: assessment.slotKey,
        message: "Opening example missing image — pending_image_review until approved registry asset is linked.",
        recommendedFixBatch: "v31B_brand_asset_registry_discovery",
        cosmeticNonBlocking: true,
      });
    }
  }
  return defects;
}

export function openingsFailActiveProfileImageGate(brand, registryAssets, brandConfig, brandTarget) {
  const defects = detectBrandAssetImageGovernanceDefects(
    brand,
    registryAssets,
    brandConfig,
    brandTarget
  );
  return defects.some(
    (d) =>
      d.slotKey === "footprint.openings" ||
      (d.type === "wrong_brand_image" && /footprint\.openings/.test(d.slotKey || ""))
  ) && defects.some((d) => d.severity === "critical" || d.severity === "high" || d.type === "wrong_brand_image");
}

/** Pending gallery images may render in draft/internal views but block active-profile readiness. */
export function galleryPendingReviewBlocksActiveProfile(
  brand,
  registryAssets,
  brandConfig,
  brandTarget
) {
  if (!isExpansionBacklogBrandTarget(brandTarget)) return false;
  for (const row of listVisualPresentationRows(brand)) {
    if (!isGalleryImageSlot(row?.slotKey)) continue;
    const assessment = assessPresentationRowImageGovernance(row, brandConfig, registryAssets);
    if (!assessment) continue;
    if (assessment.hasImage && !assessment.registryApproved && !assessment.wrongBrandRisk) {
      return true;
    }
  }
  return false;
}

export function unapprovedGalleryBlocksActiveProfile(
  brand,
  registryAssets,
  brandConfig,
  brandTarget
) {
  return galleryPendingReviewBlocksActiveProfile(brand, registryAssets, brandConfig, brandTarget);
}
