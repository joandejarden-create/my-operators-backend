/**
 * Cross-product reuse: Hotel Explorer may consume Shared Verified Hotel Image Library
 * assets only when property + product permissions match.
 *
 * Contract: BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1
 *
 * Does NOT create duplicate hotel records. Does NOT inherit Brand Explorer
 * publication approvals automatically.
 */
import {
  BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1,
  findLibraryAssetsForBrand,
  libraryAssetEligibleForBrandExplorer,
  PRODUCT_PERMISSION,
  readLibraryIndex,
} from "./be-shared-verified-hotel-image-library-v1.js";
import {
  evaluateExactBrandImageIdentity,
  EXACT_BRAND_FAILURE_CODES,
} from "./be-factory-permanent-exact-brand-image-identity-v1.js";

export const BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1 =
  "BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1";

/**
 * Select library assets eligible for Hotel Explorer for a census property.
 */
export function findHotelExplorerImagesForProperty({
  censusPropertyId,
  brandBasicsId = null,
  brandSlug = null,
  sceneCategory = null,
  requireCustomerFacing = false,
} = {}) {
  const index = readLibraryIndex();
  const ids = censusPropertyId
    ? index.byCensusId[censusPropertyId] || []
    : Object.keys(index.assetsById);
  return ids
    .map((id) => index.assetsById[id])
    .filter(Boolean)
    .filter((a) => {
      if (censusPropertyId && a.censusPropertyId !== censusPropertyId) return false;
      if (brandBasicsId && a.brandBasicsId && a.brandBasicsId !== brandBasicsId) return false;
      if (brandSlug && a.brandSlug && a.brandSlug !== brandSlug) return false;
      if (sceneCategory && a.sceneCategory !== sceneCategory) return false;
      if (!a.permissions?.[PRODUCT_PERMISSION.HOTEL_EXPLORER_DISPLAY]) return false;
      if (requireCustomerFacing && !a.permissions?.[PRODUCT_PERMISSION.CUSTOMER_FACING]) {
        return false;
      }
      if (!a.technicalValid) return false;
      const exact = evaluateExactBrandImageIdentity({
        targetBrandId: brandBasicsId || a.brandBasicsId,
        targetSlug: brandSlug || a.brandSlug,
        propertyBrandId: a.brandBasicsId,
        propertyBrandSlug: a.brandSlug,
        propertyCensusId: a.censusPropertyId,
        propertyVerified: !!a.censusPropertyId,
        imageUrl: a.originalImageUrl,
        imageMeta: { propertyName: a.propertyName },
        rightsOk: true,
      });
      if (!exact.ok) return false;
      return true;
    })
    .map((a) => ({
      contract: BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1,
      libraryContract: BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1,
      assetId: a.id,
      contentHash: a.contentHash,
      censusPropertyId: a.censusPropertyId,
      brandBasicsId: a.brandBasicsId,
      sceneCategory: a.sceneCategory,
      note: "BE publication approval is independent — HE permission required separately.",
    }));
}

export function assertNoSilentBrandSubstitution(targetSlug, asset) {
  const exact = evaluateExactBrandImageIdentity({
    targetSlug,
    propertyBrandSlug: asset?.brandSlug,
    propertyBrandId: asset?.brandBasicsId,
    propertyCensusId: asset?.censusPropertyId,
    propertyVerified: !!asset?.censusPropertyId,
    imageUrl: asset?.originalImageUrl,
    imageMeta: { propertyName: asset?.propertyName },
  });
  return {
    ok: exact.ok === true,
    exact,
    failureCode: exact.failureCode || EXACT_BRAND_FAILURE_CODES.IMAGE_EXACT_BRAND_UNVERIFIED,
  };
}

export {
  findLibraryAssetsForBrand,
  libraryAssetEligibleForBrandExplorer,
};

export default {
  BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1,
  findHotelExplorerImagesForProperty,
  assertNoSilentBrandSubstitution,
};
