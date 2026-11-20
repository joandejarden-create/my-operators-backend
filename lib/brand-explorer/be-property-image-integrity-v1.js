/**
 * Brand Explorer — property image integrity + research-source separation.
 *
 * Article / publication thumbnails (e.g. Travel Weekly mastheads) are evidence
 * sources, never automatic property photographs.
 *
 * Contract: BE_PROPERTY_IMAGE_INTEGRITY_V1
 */

import {
  isBrandLogoGalleryAsset,
  isGenericGalleryCaption,
  captionFromPropertyImageUrl,
} from "./be-permanent-six-image-gallery-standard-v1.js";

export const BE_PROPERTY_IMAGE_INTEGRITY_V1 = "BE_PROPERTY_IMAGE_INTEGRITY_V1";

export const PROPERTY_IMAGE_FAIL = Object.freeze({
  PUBLICATION_THUMBNAIL: "publication_thumbnail_as_property_photo",
  LOGO: "logo_as_property_photo",
  GENERIC_CAPTION: "generic_or_misleading_caption",
  UNRELATED_PROPERTY: "unrelated_property_association",
  DUPLICATE: "duplicate_image_masquerading_as_distinct",
  UNVERIFIED_RENDERING: "unverified_rendering",
  RIGHTS_MISSING: "missing_rights_information",
  BROKEN_OR_MISSING: "broken_or_missing_image",
  TRADE_PRESS_HOST: "trade_press_or_news_host_image",
  FILENAME_PUBLICATION_MARKER: "filename_marks_publication_graphic",
});

/** Trade / news hosts that may be research citations but not property photos. */
export const PUBLICATION_OR_TRADE_HOST_RE =
  /travelweekly\.com|traveldailynews\.com|hotel-online\.com|prnewswire\.com|globenewswire\.com|businesswire\.com|hotelnewsresource\.com|breakingtravelnews\.com|travelprnews\.com|newsismybusiness\.com|hotelmanagement-network\.com|journaldespalaces\.com|ladevi\.info|semana\.com|ithic\.it/i;

/** Filename markers that encode publication graphics (e.g. …-tw.jpg = Travel Weekly). */
export const PUBLICATION_FILENAME_MARKER_RE =
  /(?:^|[-_])tw\.(?:jpe?g|png|webp|gif)$|(?:^|[-_])(travel[-_]?weekly|masthead|article[-_]?thumb|og[-_]?image|social[-_]?share)(?:[-_.]|$)/i;

export const PROPERTY_FACING_SLOT_RE =
  /^(?:footprint\.(?:openings|momentum)|materials\.caseStudy|materials\.gallery\.\d+|overview\.scenario\.\d+|valueOwners\.scenario\.\d+)$/i;

export function isPropertyFacingSlot(slotKey) {
  return PROPERTY_FACING_SLOT_RE.test(nz(slotKey));
}

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function imageBasename(urlOrName) {
  const s = nz(urlOrName).split("?")[0];
  const parts = s.split("/");
  return decodeURIComponent(parts[parts.length - 1] || "");
}

export function isPublicationOrTradePressHost(url) {
  try {
    const host = new URL(nz(url)).hostname.replace(/^www\./, "");
    return PUBLICATION_OR_TRADE_HOST_RE.test(host);
  } catch {
    return PUBLICATION_OR_TRADE_HOST_RE.test(nz(url));
  }
}

export function filenameMarksPublicationGraphic(filenameOrUrl) {
  const base = imageBasename(filenameOrUrl);
  if (!base) return false;
  if (PUBLICATION_FILENAME_MARKER_RE.test(base)) return true;
  // Explicit Travel Weekly suffix convention used in Curio openings attachment
  if (/-tw\.(jpe?g|png|webp|gif)$/i.test(base)) return true;
  if (/travel[-_]?weekly/i.test(base)) return true;
  if (/masthead|wordmark|logo[-_]?lockup/i.test(base)) return true;
  return false;
}

/**
 * True when an asset must not be used as a featured property photograph.
 */
export function isUnsuitablePropertyDisplayImage(url, meta = {}) {
  const u = nz(url);
  const filename = nz(meta.imageFilename || meta.filename || imageBasename(u));
  // Empty is not an "unsuitable substitution" — callers decide whether missing is allowed.
  if (!u && !filename) {
    return { unsuitable: false, code: PROPERTY_IMAGE_FAIL.BROKEN_OR_MISSING, detail: "missing_url", missing: true };
  }
  if (filenameMarksPublicationGraphic(filename) || filenameMarksPublicationGraphic(u)) {
    return {
      unsuitable: true,
      code: PROPERTY_IMAGE_FAIL.FILENAME_PUBLICATION_MARKER,
      detail: filename || imageBasename(u),
    };
  }
  if (u && isPublicationOrTradePressHost(u)) {
    return { unsuitable: true, code: PROPERTY_IMAGE_FAIL.TRADE_PRESS_HOST, detail: u };
  }
  if (u && isBrandLogoGalleryAsset(u, meta)) {
    return { unsuitable: true, code: PROPERTY_IMAGE_FAIL.LOGO, detail: imageBasename(u) };
  }
  if (meta.imageType === "publication_thumbnail" || meta.imageType === "article_screenshot") {
    return { unsuitable: true, code: PROPERTY_IMAGE_FAIL.PUBLICATION_THUMBNAIL, detail: meta.imageType };
  }
  return { unsuitable: false, code: null, detail: null };
}

/**
 * Structured separation: research citation vs display property image.
 */
export function separateResearchSourceFromPropertyImage(block = {}) {
  const body = nz(block.body);
  const urls = body.match(/https?:\/\/[^\s)]+/gi) || [];
  const researchUrls = urls.filter((u) => isPublicationOrTradePressHost(u) || /stories\.hilton\.com\/releases|newsroom|press/i.test(u));
  const officialPropertyUrls = urls.filter((u) => /\/hotels\/|hilton\.com\/en\/hotels|choicehotels\.com\//i.test(u));
  const imageUrl = nz(block.imageUrl || block.image_url);
  const filename = nz(block.imageFilename);
  const check = isUnsuitablePropertyDisplayImage(imageUrl, {
    imageFilename: filename,
    imageType: block.imageType,
  });

  return {
    propertyIdentity: nz(block.title) || nz(block.propertyName) || null,
    brandAffiliation: nz(block.brandAffiliation) || null,
    location: nz(block.location) || null,
    researchSourceUrl: researchUrls[0] || null,
    researchSourceUrls: researchUrls,
    officialPropertyPageUrl: officialPropertyUrls[0] || null,
    propertyImageUrl: check.unsuitable ? null : imageUrl || null,
    rejectedDisplayImageUrl: check.unsuitable ? imageUrl || null : null,
    rejection: check.unsuitable ? check : null,
    imageFilename: filename || null,
    imageType: check.unsuitable ? "rejected_non_property_visual" : nz(block.imageType) || "operating_photograph",
    caption: nz(block.title) || null,
    rightsUseStatus: nz(block.rightsUseStatus || block.rights) || null,
    verificationStatus: check.unsuitable ? "REJECTED_NON_PROPERTY_VISUAL" : nz(block.verificationStatus) || "UNVERIFIED",
  };
}

/**
 * Verified replacements for known founder failures (official Hilton stories photography).
 * Used only on RIGHT proposed profile — never mutates LEFT/CURRENT Airtable.
 */
export const VERIFIED_PROPERTY_IMAGE_REPLACEMENTS = Object.freeze([
  {
    matchTitleRe: /royal\s*palm\s*gal[aá]pagos/i,
    propertyIdentity: "Royal Palm Galapagos, Curio Collection by Hilton",
    location: "Santa Cruz Island, Galápagos, Ecuador",
    imageUrl:
      "https://stories-editor.hilton.com/wp-content/uploads/2022/07/Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg",
    sourceUrl: "https://stories.hilton.com/releases/royal-palm-galapagos-curio-collection-by-hilton-opens",
    imageType: "operating_photograph",
    sceneType: "aerial_estate",
    caption: "Royal Palm Galápagos | Hotel & Guest Villas",
    rightsUseAuthorization: "Hilton Stories / courtesy Royal Palm Galapagos (official release imagery)",
    verificationStatus: "VERIFIED_EXACT_PROPERTY",
    rejectFilenameRe: /-tw\.(jpe?g|png|webp)$/i,
  },
]);

export function findVerifiedPropertyImageReplacement(block) {
  const title = nz(block?.title);
  for (const row of VERIFIED_PROPERTY_IMAGE_REPLACEMENTS) {
    if (!row.matchTitleRe.test(title)) continue;
    /*
     * Known founder identity failures always map to the verified property photograph.
     * Do not require `-tw.jpg` filename markers: Airtable CDN URLs strip original
     * filenames, which previously left Travel Weekly mastheads in the product popup.
     * rejectFilenameRe stays on the row as diagnostic metadata only.
     */
    return row;
  }
  return null;
}

/**
 * Apply integrity repair to property-facing blocks on the proposed profile.
 * - Rejects publication thumbnails / logos as display images
 * - Applies verified replacements when available
 * - Preserves research URLs inside body text
 * - Sets explicit research-required when no suitable image exists
 */
export function applyPropertyImageIntegrityToBlocks(blocks, opts = {}) {
  const list = Array.isArray(blocks) ? blocks : [];
  const changed = [];
  const out = list.map((b) => {
    const slotKey = nz(b.slotKey || b.slot);
    if (!isPropertyFacingSlot(slotKey)) return b;
    const hasDisplayAsset = !!(nz(b.imageUrl) || nz(b.imageFilename));
    const check = isUnsuitablePropertyDisplayImage(b.imageUrl, {
      imageFilename: b.imageFilename,
      imageType: b.imageType,
    });
    const replacement = findVerifiedPropertyImageReplacement(b);

    // Empty slots: only fill when a verified exact-property replacement matches the card identity.
    if (!hasDisplayAsset) {
      if (!replacement) return b;
      const next = {
        ...b,
        imageUrl: replacement.imageUrl,
        imageFilename: imageBasename(replacement.imageUrl),
        imageType: replacement.imageType,
        __propertyImageIntegrity: {
          action: "filled_verified_property_photo",
          rejectedUrl: null,
          rejectedFilename: null,
          rejectionCode: null,
          replacementUrl: replacement.imageUrl,
          sourceUrl: replacement.sourceUrl,
          caption: replacement.caption,
          verificationStatus: replacement.verificationStatus,
          rightsUseAuthorization: replacement.rightsUseAuthorization,
        },
      };
      changed.push({
        slotKey,
        from: "",
        to: replacement.imageUrl,
        reason: "verified_property_photo_fill",
        caption: replacement.caption,
      });
      return next;
    }

    if (!check.unsuitable && !replacement) return b;

    // Verified identity replacements always win when the display URL differs.
    if (replacement && nz(b.imageUrl) !== replacement.imageUrl) {
      const next = {
        ...b,
        imageUrl: replacement.imageUrl,
        imageFilename: imageBasename(replacement.imageUrl),
        imageType: replacement.imageType,
        title: nz(b.title) || replacement.propertyIdentity,
        // Keep research citations in body; do not invent new body prose
        __propertyImageIntegrity: {
          action: "replaced_with_verified_property_photo",
          rejectedUrl: nz(b.imageUrl) || null,
          rejectedFilename: nz(b.imageFilename) || null,
          rejectionCode: check.code || PROPERTY_IMAGE_FAIL.PUBLICATION_THUMBNAIL,
          replacementUrl: replacement.imageUrl,
          sourceUrl: replacement.sourceUrl,
          caption: replacement.caption,
          verificationStatus: replacement.verificationStatus,
          rightsUseAuthorization: replacement.rightsUseAuthorization,
        },
      };
      changed.push({
        slotKey,
        from: nz(b.imageUrl),
        to: replacement.imageUrl,
        reason: "verified_property_photo_replacement",
        caption: replacement.caption,
      });
      return next;
    }

    if (check.unsuitable) {
      const next = {
        ...b,
        imageUrl: null,
        imageFilename: "",
        __propertyImageIntegrity: {
          action: "stripped_unsuitable_display_image",
          rejectedUrl: nz(b.imageUrl) || null,
          rejectedFilename: nz(b.imageFilename) || null,
          rejectionCode: check.code,
          researchRequired: true,
          note: "Research citation retained in body when present; property photograph RESEARCH_REQUIRED",
        },
      };
      changed.push({
        slotKey,
        from: nz(b.imageUrl),
        to: null,
        reason: check.code,
      });
      return next;
    }
    return b;
  });

  return {
    contract: BE_PROPERTY_IMAGE_INTEGRITY_V1,
    blocks: out,
    changed,
    changedCount: changed.length,
  };
}

export function validatePropertyFacingImages(blocks) {
  const failures = [];
  for (const b of blocks || []) {
    const slotKey = nz(b.slotKey || b.slot);
    if (!PROPERTY_FACING_SLOT_RE.test(slotKey)) continue;
    if (!nz(b.imageUrl) && !nz(b.imageFilename)) continue;
    const check = isUnsuitablePropertyDisplayImage(b.imageUrl, {
      imageFilename: b.imageFilename,
      imageType: b.imageType,
    });
    if (check.unsuitable) {
      failures.push({
        slotKey,
        title: nz(b.title),
        imageUrl: nz(b.imageUrl),
        imageFilename: nz(b.imageFilename),
        code: check.code,
        detail: check.detail,
      });
    }
    if (isGenericGalleryCaption(b.title) && /^materials\.gallery\./.test(slotKey)) {
      failures.push({
        slotKey,
        title: nz(b.title),
        code: PROPERTY_IMAGE_FAIL.GENERIC_CAPTION,
        detail: b.title,
      });
    }
  }
  return {
    contract: BE_PROPERTY_IMAGE_INTEGRITY_V1,
    ok: failures.length === 0,
    pass: failures.length === 0,
    failures,
    failureCount: failures.length,
  };
}

export function buildPropertyImageAssetRecord(block, extras = {}) {
  const sep = separateResearchSourceFromPropertyImage(block);
  return {
    brandId: extras.brandId || null,
    propertyIdentity: sep.propertyIdentity,
    location: sep.location,
    researchSourceUrl: sep.researchSourceUrl,
    propertyImageUrl: sep.propertyImageUrl,
    imageFilename: sep.imageFilename,
    imageType: sep.imageType,
    caption: sep.caption || extras.caption || null,
    rightsUseStatus: sep.rightsUseStatus,
    verificationStatus: sep.verificationStatus,
    photographVsRendering: extras.isRendering ? "rendering" : "photograph",
    slotKey: nz(block.slotKey),
    rejection: sep.rejection,
  };
}
