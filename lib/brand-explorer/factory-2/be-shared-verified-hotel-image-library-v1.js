/**
 * Shared Verified Hotel Image Library V1
 *
 * Durable, reusable Dealality intelligence store for hotel photographs.
 * Reuses Brand Basics / Hotel Property Census / Brand Asset Registry IDs.
 * Does NOT create parallel property or brand records.
 *
 * Airtable schema changes remain PROPOSED only (see docs).
 * On-disk ledger: data/brand-explorer/verified-hotel-image-library/
 *
 * Contract: BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1
 */
import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
  evaluateExactBrandImageIdentity,
} from "./be-factory-permanent-exact-brand-image-identity-v1.js";

export const BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1 =
  "BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
export const LIBRARY_DATA_DIR = path.join(
  ROOT,
  "data/brand-explorer/verified-hotel-image-library"
);
export const LIBRARY_INDEX_FILE = path.join(LIBRARY_DATA_DIR, "index-v1.json");
export const LIBRARY_ASSETS_DIR = path.join(LIBRARY_DATA_DIR, "assets");

/** Product permission flags (independent per product). */
export const PRODUCT_PERMISSION = Object.freeze({
  INTERNAL_RESEARCH: "internalResearch",
  BRAND_EXPLORER_DISPLAY: "brandExplorerDisplay",
  HOTEL_EXPLORER_DISPLAY: "hotelExplorerDisplay",
  CUSTOMER_FACING: "customerFacing",
  EXTERNAL_DISTRIBUTION: "externalDistribution",
});

export const SCENE_CATEGORY = Object.freeze({
  EXTERIOR: "exterior",
  LOBBY: "lobby",
  GUEST_ROOM: "guest_room",
  RESTAURANT_BAR: "restaurant_bar",
  MEETING_SPACE: "meeting_space",
  POOL: "pool",
  SPA: "spa",
  RESORT_GROUNDS: "resort_grounds",
  OTHER: "other",
});

export function ensureLibraryDirs() {
  fs.mkdirSync(LIBRARY_ASSETS_DIR, { recursive: true });
  if (!fs.existsSync(LIBRARY_INDEX_FILE)) {
    const seed = emptyLibraryIndex();
    seed.updatedAt = new Date().toISOString();
    fs.writeFileSync(LIBRARY_INDEX_FILE, JSON.stringify(seed, null, 2) + "\n");
  }
}

export function emptyLibraryIndex() {
  return {
    contract: BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1,
    exactBrandContract: BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1,
    updatedAt: null,
    assetCount: 0,
    assetsById: {},
    byContentHash: {},
    byBrandId: {},
    byCensusId: {},
    byRegistryId: {},
  };
}

export function readLibraryIndex() {
  ensureLibraryDirs();
  try {
    return JSON.parse(fs.readFileSync(LIBRARY_INDEX_FILE, "utf8"));
  } catch {
    return emptyLibraryIndex();
  }
}

export function writeLibraryIndex(index) {
  ensureLibraryDirs();
  const next = {
    ...index,
    updatedAt: new Date().toISOString(),
    assetCount: Object.keys(index.assetsById || {}).length,
  };
  const body = JSON.stringify(next, null, 2) + "\n";
  const tmp = `${LIBRARY_INDEX_FILE}.${process.pid}.${Date.now()}.tmp`;
  // Atomic-ish write: Windows AV occasionally returns UNKNOWN on in-place overwrite.
  let lastErr = null;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      fs.writeFileSync(tmp, body);
      try {
        fs.renameSync(tmp, LIBRARY_INDEX_FILE);
      } catch {
        fs.copyFileSync(tmp, LIBRARY_INDEX_FILE);
        try {
          fs.unlinkSync(tmp);
        } catch {
          /* ignore */
        }
      }
      return next;
    } catch (e) {
      lastErr = e;
      try {
        fs.unlinkSync(tmp);
      } catch {
        /* ignore */
      }
      // brief backoff without Atomics dependency
      const start = Date.now();
      while (Date.now() - start < 40 * (attempt + 1)) {
        /* spin */
      }
    }
  }
  throw lastErr || new Error("writeLibraryIndex_failed");
}

/** Rebuild index from per-asset JSON files (recovery path). */
export function rebuildLibraryIndexFromAssets() {
  ensureLibraryDirs();
  const index = emptyLibraryIndex();
  index.updatedAt = new Date().toISOString();
  for (const name of fs.readdirSync(LIBRARY_ASSETS_DIR)) {
    if (!name.endsWith(".json")) continue;
    try {
      const asset = JSON.parse(
        fs.readFileSync(path.join(LIBRARY_ASSETS_DIR, name), "utf8")
      );
      if (!asset?.id || !asset?.contentHash) continue;
      index.assetsById[asset.id] = asset;
      index.byContentHash[asset.contentHash] = asset.id;
      if (asset.brandBasicsId) {
        if (!index.byBrandId[asset.brandBasicsId]) index.byBrandId[asset.brandBasicsId] = [];
        if (!index.byBrandId[asset.brandBasicsId].includes(asset.id)) {
          index.byBrandId[asset.brandBasicsId].push(asset.id);
        }
      }
      if (asset.censusPropertyId) {
        if (!index.byCensusId[asset.censusPropertyId]) index.byCensusId[asset.censusPropertyId] = [];
        if (!index.byCensusId[asset.censusPropertyId].includes(asset.id)) {
          index.byCensusId[asset.censusPropertyId].push(asset.id);
        }
      }
      if (asset.brandAssetRegistryId) {
        index.byRegistryId[asset.brandAssetRegistryId] = asset.id;
      }
    } catch {
      /* skip corrupt asset */
    }
  }
  index.assetCount = Object.keys(index.assetsById).length;
  return writeLibraryIndex(index);
}

export function sha256Hex(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

export function makeAssetId(contentHash) {
  return `svhil_${String(contentHash || "").slice(0, 24)}`;
}

/**
 * Build a canonical asset record (not yet persisted).
 */
export function buildVerifiedImageAsset(input = {}) {
  const contentHash = String(input.contentHash || "").trim();
  if (!contentHash || contentHash.length < 16) {
    throw new Error("contentHash required (sha-256 hex)");
  }
  const id = input.id || makeAssetId(contentHash);
  const permissions = {
    [PRODUCT_PERMISSION.INTERNAL_RESEARCH]: input.permissions?.internalResearch !== false,
    [PRODUCT_PERMISSION.BRAND_EXPLORER_DISPLAY]: !!input.permissions?.brandExplorerDisplay,
    [PRODUCT_PERMISSION.HOTEL_EXPLORER_DISPLAY]: !!input.permissions?.hotelExplorerDisplay,
    [PRODUCT_PERMISSION.CUSTOMER_FACING]: !!input.permissions?.customerFacing,
    [PRODUCT_PERMISSION.EXTERNAL_DISTRIBUTION]: !!input.permissions?.externalDistribution,
  };
  return {
    id,
    contract: BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1,
    // Asset identity
    contentHash,
    perceptualHash: input.perceptualHash || null,
    filename: input.filename || null,
    mimeType: input.mimeType || null,
    width: input.width || null,
    height: input.height || null,
    byteSize: input.byteSize || null,
    durableStorageRef: input.durableStorageRef || null,
    discoveredAt: input.discoveredAt || new Date().toISOString(),
    verifiedAt: input.verifiedAt || null,
    // Property identity (reuse canonical IDs — never invent parallel hotels)
    censusPropertyId: input.censusPropertyId || null,
    propertyName: input.propertyName || null,
    propertyWebsite: input.propertyWebsite || null,
    city: input.city || null,
    region: input.region || null,
    country: input.country || null,
    brandBasicsId: input.brandBasicsId || null,
    brandSlug: input.brandSlug || null,
    brandName: input.brandName || null,
    parentCompany: input.parentCompany || null,
    brandVerificationDate: input.brandVerificationDate || null,
    historicalAffiliations: input.historicalAffiliations || [],
    // Provenance
    originalImageUrl: input.originalImageUrl || null,
    sourcePageUrl: input.sourcePageUrl || null,
    publisher: input.publisher || null,
    sourceType: input.sourceType || null,
    retrievalMethod: input.retrievalMethod || null,
    confidence: input.confidence || null,
    verificationNotes: input.verificationNotes || null,
    // Visual
    sceneCategory: input.sceneCategory || SCENE_CATEGORY.OTHER,
    // Permissions / rights
    copyrightOwner: input.copyrightOwner || null,
    licenseBasis: input.licenseBasis || null,
    permissions,
    permissionRestrictions: input.permissionRestrictions || null,
    permissionReviewDate: input.permissionReviewDate || null,
    // Cross-links
    brandAssetRegistryId: input.brandAssetRegistryId || null,
    presentationRecordIds: input.presentationRecordIds || [],
    // Certification
    exactBrandDecision: input.exactBrandDecision || null,
    technicalValid: input.technicalValid === true,
    browserValidated: input.browserValidated === true,
    certificationStatus: input.certificationStatus || "STAGED",
  };
}

/**
 * Upsert asset into the on-disk library index (idempotent by contentHash).
 * Pass `{ index, deferIndexWrite: true }` for batch apply, then call writeLibraryIndex(index).
 */
export function upsertVerifiedImageAsset(
  assetInput,
  { dryRun = true, index: sharedIndex = null, deferIndexWrite = false } = {}
) {
  const asset = buildVerifiedImageAsset(assetInput);
  const index = sharedIndex || readLibraryIndex();
  const existingId = index.byContentHash[asset.contentHash];
  if (existingId && index.assetsById[existingId]) {
    const prev = index.assetsById[existingId];
    const merged = {
      ...prev,
      ...asset,
      id: prev.id,
      presentationRecordIds: [
        ...new Set([...(prev.presentationRecordIds || []), ...(asset.presentationRecordIds || [])]),
      ],
      discoveredAt: prev.discoveredAt || asset.discoveredAt,
    };
    if (!dryRun) {
      index.assetsById[prev.id] = merged;
      if (!deferIndexWrite) writeLibraryIndex(index);
      fs.writeFileSync(
        path.join(LIBRARY_ASSETS_DIR, `${prev.id}.json`),
        JSON.stringify(merged, null, 2) + "\n"
      );
    }
    return { ok: true, action: "updated", dryRun, asset: merged, index };
  }
  if (!dryRun) {
    index.assetsById[asset.id] = asset;
    index.byContentHash[asset.contentHash] = asset.id;
    if (asset.brandBasicsId) {
      const list = index.byBrandId[asset.brandBasicsId] || [];
      if (!list.includes(asset.id)) list.push(asset.id);
      index.byBrandId[asset.brandBasicsId] = list;
    }
    if (asset.censusPropertyId) {
      const list = index.byCensusId[asset.censusPropertyId] || [];
      if (!list.includes(asset.id)) list.push(asset.id);
      index.byCensusId[asset.censusPropertyId] = list;
    }
    if (asset.brandAssetRegistryId) {
      index.byRegistryId[asset.brandAssetRegistryId] = asset.id;
    }
    if (!deferIndexWrite) writeLibraryIndex(index);
    fs.writeFileSync(
      path.join(LIBRARY_ASSETS_DIR, `${asset.id}.json`),
      JSON.stringify(asset, null, 2) + "\n"
    );
  }
  return { ok: true, action: "created", dryRun, asset, index };
}

/**
 * Query library before discovering new images.
 */
export function findLibraryAssetsForBrand({
  brandBasicsId = null,
  brandSlug = null,
  sceneCategory = null,
  requireCustomerFacing = false,
  requireBrandExplorerDisplay = false,
} = {}) {
  const index = readLibraryIndex();
  let ids = [];
  if (brandBasicsId && index.byBrandId[brandBasicsId]) {
    ids = index.byBrandId[brandBasicsId].slice();
  } else {
    ids = Object.keys(index.assetsById);
  }
  return ids
    .map((id) => index.assetsById[id])
    .filter(Boolean)
    .filter((a) => {
      if (brandSlug && a.brandSlug && a.brandSlug !== brandSlug) return false;
      if (sceneCategory && a.sceneCategory !== sceneCategory) return false;
      if (requireCustomerFacing && !a.permissions?.[PRODUCT_PERMISSION.CUSTOMER_FACING]) {
        return false;
      }
      if (
        requireBrandExplorerDisplay &&
        !a.permissions?.[PRODUCT_PERMISSION.BRAND_EXPLORER_DISPLAY]
      ) {
        return false;
      }
      return true;
    });
}

/**
 * Eligibility for a target Brand Explorer profile (exact brand + product permission).
 */
export function libraryAssetEligibleForBrandExplorer(asset, target = {}) {
  const exact = evaluateExactBrandImageIdentity({
    targetBrandId: target.brandBasicsId || target.targetBrandId,
    targetSlug: target.brandSlug || target.targetSlug,
    propertyBrandId: asset.brandBasicsId,
    propertyBrandSlug: asset.brandSlug,
    propertyCensusId: asset.censusPropertyId,
    propertyVerified: !!asset.censusPropertyId || !!asset.verifiedAt,
    imageUrl: asset.originalImageUrl,
    imageMeta: { propertyName: asset.propertyName, caption: asset.sceneCategory },
    rightsOk: !!asset.permissions?.[PRODUCT_PERMISSION.BRAND_EXPLORER_DISPLAY],
  });
  return {
    ok: exact.ok === true && !!asset.permissions?.[PRODUCT_PERMISSION.BRAND_EXPLORER_DISPLAY],
    exact,
    assetId: asset.id,
  };
}

export function summarizeLibrary(index = null) {
  const idx = index || readLibraryIndex();
  const assets = Object.values(idx.assetsById || {});
  const brands = new Set(assets.map((a) => a.brandBasicsId).filter(Boolean));
  const hotels = new Set(assets.map((a) => a.censusPropertyId).filter(Boolean));
  const countries = new Set(assets.map((a) => a.country).filter(Boolean));
  return {
    contract: BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1,
    totalAssets: assets.length,
    uniqueContentHashes: Object.keys(idx.byContentHash || {}).length,
    brandsRepresented: brands.size,
    hotelsRepresented: hotels.size,
    countriesRepresented: countries.size,
    exactBrandEligible: assets.filter((a) => a.exactBrandDecision?.ok === true).length,
    rightsBrandExplorer: assets.filter(
      (a) => a.permissions?.[PRODUCT_PERMISSION.BRAND_EXPLORER_DISPLAY]
    ).length,
    rightsHotelExplorer: assets.filter(
      (a) => a.permissions?.[PRODUCT_PERMISSION.HOTEL_EXPLORER_DISPLAY]
    ).length,
    rightsCustomerFacing: assets.filter(
      (a) => a.permissions?.[PRODUCT_PERMISSION.CUSTOMER_FACING]
    ).length,
    awaitingReview: assets.filter((a) => a.certificationStatus === "STAGED").length,
  };
}

export default {
  BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1,
  LIBRARY_DATA_DIR,
  PRODUCT_PERMISSION,
  SCENE_CATEGORY,
  ensureLibraryDirs,
  readLibraryIndex,
  writeLibraryIndex,
  buildVerifiedImageAsset,
  upsertVerifiedImageAsset,
  findLibraryAssetsForBrand,
  libraryAssetEligibleForBrandExplorer,
  summarizeLibrary,
  sha256Hex,
};
