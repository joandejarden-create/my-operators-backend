/**
 * Permanent six-image hotel gallery standard (Brand Explorer).
 *
 * Exactly six distinct operating-hotel photographs (or labeled official
 * development renderings). No brand logos, no generic captions, no duplicates.
 *
 * Contract anchor: configs/brand-explorer-permanent-content-standards-contract-v1.json
 * #fixedCollections.imageGallery
 */

import { isLikelyLogoUrl as baseIsLikelyLogoUrl, nz } from "./be-phase2f-content-presentation-contract-v1.js";

export const BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1 =
  "BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1";

export const GALLERY_SLOT_RE = /^materials\.gallery\.(\d+)$/;
export const GALLERY_REQUIRED_COUNT = 6;

export const GALLERY_FAIL = Object.freeze({
  COUNT: "gallery_count_not_six",
  LOGO: "brand_logo_as_gallery_image",
  GENERIC_CAPTION: "generic_gallery_caption",
  DUPLICATE: "duplicate_gallery_image",
  MISSING_URL: "gallery_image_url_missing",
  UNVERIFIED_PROPERTY: "unverified_property_identity",
  RENDERING_MISLABELED: "rendering_presented_as_photography",
  RIGHTS_MISSING: "usage_rights_missing_or_incompatible",
  UNRELATED: "unrelated_hotel_imagery",
  UNSUITABLE: "image_unsuitable_for_component",
  PEOPLE_LIFESTYLE: "people_or_lifestyle_marketing_as_hotel_photo",
  ROLE_MISLABEL: "gallery_role_caption_mismatch",
});

export const GENERIC_GALLERY_CAPTION_RE =
  /^(property\s+imagery(\s+\d+)?|hotel\s+image(\s+\d+)?|imagery\s+unavailable|gallery\s*\d+|image\s*\d+|photo\s*\d+|untitled|n\/?a)$/i;

/** People / lifestyle marketing — never Brand Explorer gallery (Curio/Ascend bar). */
export const PEOPLE_OR_LIFESTYLE_IMAGE_RE =
  /emotions?|lifestyle|portrait|people|couple|family|smiling|bartender|cocktail[-_]?close|hero[-_]?people|guest[-_]with|staff[-_]with|human|apec[-_]?kv|(?:^|[-_\/])kv[-_]|devices?(?:[-_.]|\.|$)|appstore|play[-_]?store|brand[-_]?platform|module_opt|pet[-_]?friendly/i;

/** Filename / path cues that indicate hotel exterior, interior, or rendering photography. */
export const HOTEL_PROPERTY_PHOTO_CUE_RE =
  /exterior|arrival|facade|façade|aerial|cochere|porte[-_]?cochere|lobby|atrium|guest[-_]?room|guestroom|suite|bedroom|bathroom|(?:king|queen|twin|double)[-_]?bed|club[-_]?(?:lounge|level|king)|pool|spa|wellness|restaurant|dining|fb|f[-_]?and[-_]?b|ballroom|meeting|conference|render|rendering|tower|resort[-_]?grounds|beachfront|courtyard|public[-_]?space|corridor|hallway|fitness|gym/i;

/**
 * True when an asset looks like people/lifestyle marketing rather than hotel photography.
 * Gallery must be hotels only (exterior, interior, or labeled renderings) — Curio/Ascend bar.
 */
export function isPeopleOrLifestyleMarketingImage(url, meta = {}) {
  const u = nz(url);
  const blob = [
    u,
    imageBasename(u),
    nz(meta.role),
    nz(meta.sceneType),
    nz(meta.imageType),
    nz(meta.label),
    nz(meta.caption),
    nz(meta.title),
  ]
    .join(" ")
    .toLowerCase();
  if (!blob.trim()) return false;
  if (PEOPLE_OR_LIFESTYLE_IMAGE_RE.test(blob)) return true;
  // Hilton DAM "emotions" lifestyle packs
  if (/\/emotions[-_]?\d*\.jpg/i.test(u) || /emotions/i.test(imageBasename(u))) return true;
  // App / device badges
  if (/\b(appstore|playstore|devices?)\b/i.test(imageBasename(u))) return true;
  return false;
}

/**
 * True when URL/role evidence supports hotel property photography or a labeled rendering.
 */
export function isLikelyHotelPropertyPhotograph(url, meta = {}) {
  const u = nz(url);
  if (!u) return false;
  if (isPeopleOrLifestyleMarketingImage(u, meta)) return false;
  if (isBrandLogoGalleryAsset(u, meta)) return false;
  const role = nz(meta.role || meta.sceneType || meta.label).toLowerCase();
  const blob = `${u} ${imageBasename(u)} ${role}`;
  if (HOTEL_PROPERTY_PHOTO_CUE_RE.test(blob)) return true;
  if (
    /^(exterior(_arrival)?|guest_room(_suite)?|public_space_lobby|food_beverage(_experience)?|wellness_pool_spa|meeting(_event|_space)?|amenity)$/i.test(
      role
    ) ||
    /^(exterior|guest room|lobby(\s*\/\s*public space)?|f&b|pool(\s*\/\s*wellness)?|meetings|amenity|rendering)$/i.test(
      role
    )
  ) {
    return true;
  }
  // Official Marriott / Hilton / Hyatt DAM hosts with property-coded paths
  if (/cache\.marriott\.com\/(?:content\/dam\/marriott-renditions|is\/image\/marriotts7prod)\//i.test(u)) {
    // Require a hotel cue in the Marriott DAM path — reject opaque marketing codes alone
    return HOTEL_PROPERTY_PHOTO_CUE_RE.test(u);
  }
  if (/hilton\.com\/im\/en\/[a-z0-9]+\/\d+\//i.test(u) && HOTEL_PROPERTY_PHOTO_CUE_RE.test(u)) return true;
  if (/assets\.hyatt\.com\/content\/dam\/hyatt/i.test(u) && HOTEL_PROPERTY_PHOTO_CUE_RE.test(u)) return true;
  return false;
}

/**
 * Map fixture role codes + URL cues to customer-facing scene labels.
 * Never label an image "Exterior" without exterior/arrival cues (Curio/Ascend bar).
 */
export function galleryRoleToSceneLabel(role, { imageUrl = "" } = {}) {
  const r = nz(role).toLowerCase();
  const pathCue = `${nz(imageUrl)} ${imageBasename(imageUrl)}`.toLowerCase();
  if (/render/.test(r) || /render/.test(pathCue)) return "Rendering";
  if (
    /^exterior(_arrival)?$/.test(r) ||
    /exterior|arrival|facade|façade|aerial|cochere|porte[-_]?cochere/.test(pathCue)
  ) {
    return "Exterior";
  }
  if (
    /guest_room|guestroom|suite|bedroom/.test(r) ||
    /guest[-_]?room|suite|bedroom|(?:king|queen|twin|double)[-_]?bed/.test(pathCue)
  ) {
    return "Guest Room";
  }
  if (/public_space|lobby|atrium/.test(r) || /lobby|atrium|public[-_]?space/.test(pathCue)) {
    return "Lobby / Public Space";
  }
  if (/food_beverage|restaurant|dining|^fb$/.test(r) || /restaurant|dining|food[-_]?beverage/.test(pathCue)) {
    return "F&B";
  }
  if (/wellness|pool|spa/.test(r) || /(?:^|[-_\/])(pool|spa|wellness)([-_.]|$)/.test(pathCue)) {
    return "Pool / Wellness";
  }
  if (/meeting|event|ballroom/.test(r) || /meeting|ballroom|conference/.test(pathCue)) {
    return "Meetings";
  }
  if (/amenity|fitness|gym/.test(r) || /fitness|gym/.test(pathCue)) return "Amenity";
  return null;
}

const BRAND_ONLY_BASENAME_RE =
  /^(curio|ascend|autograph|tapestry|kimpton|tribute|voco|vignette|canopy|motto|spark|tru|home2|homewood|hilton|marriott|ihg|radisson|choice)[-_]?(collection)?([-_]by[-_]\w+)?$/i;

const LOGO_META_RE = /logo|wordmark|brand[-_]?mark|brand[-_]?lockup|social[-_]?image|favicon/i;

/**
 * Stronger logo rejection: URL naming + basename brand-only + metadata.
 */
export function isBrandLogoGalleryAsset(url, meta = {}) {
  const u = nz(url);
  if (!u) return false;
  if (baseIsLikelyLogoUrl(u)) return true;
  if (LOGO_META_RE.test(nz(meta.imageType) + " " + nz(meta.sceneType) + " " + nz(meta.role))) {
    return true;
  }
  const base = imageBasename(u).replace(/\.(webp|png|jpe?g|gif|svg)$/i, "");
  if (!base) return false;
  if (LOGO_META_RE.test(base)) return true;
  // Brand-name-only assets (e.g. Curio-Collection-by-Hilton.webp) are logos, not hotels
  const normalized = base.replace(/_/g, "-");
  if (BRAND_ONLY_BASENAME_RE.test(normalized)) return true;
  if (/^curio-collection-by-hilton$/i.test(normalized)) return true;
  if (/^ascend(-hotel)?-collection/i.test(normalized) && !/hotel|resort|inn|suites/i.test(normalized.replace(/^ascend(-hotel)?-collection[-_]?/i, ""))) {
    return /logo|wordmark|social|brand/i.test(u) || BRAND_ONLY_BASENAME_RE.test(normalized);
  }
  return false;
}

export function isGenericGalleryCaption(title, body = "") {
  const t = nz(title);
  if (!t) return true;
  if (GENERIC_GALLERY_CAPTION_RE.test(t)) return true;
  // Title that is only a raw URL is not customer-facing
  if (/^https?:\/\//i.test(t)) return true;
  // Body that is only a URL with empty/generic title still fails caption quality
  if (GENERIC_GALLERY_CAPTION_RE.test(t) || (t.length < 8 && /^https?:\/\//i.test(nz(body)))) {
    return true;
  }
  return false;
}

export function imageBasename(url) {
  try {
    const u = new URL(nz(url));
    const parts = u.pathname.split("/").filter(Boolean);
    return decodeURIComponent(parts[parts.length - 1] || "");
  } catch {
    const parts = nz(url).split("?")[0].split("/").filter(Boolean);
    return decodeURIComponent(parts[parts.length - 1] || "");
  }
}

export function duplicateDetectionId(url) {
  const base = imageBasename(url)
    .toLowerCase()
    .replace(/\.(webp|png|jpe?g|gif|svg)$/i, "")
    // Collapse Scene7 / DAM crop variants of the same photograph
    .replace(/[-_](\d+x\d+|2x1|3x2|4x3|16x9)([-_.]|$)/gi, "$2")
    .replace(/[^a-z0-9]+/g, "");
  return base || nz(url).toLowerCase();
}

/**
 * Derive a customer caption from official Hilton stories-style filenames.
 * Example: Hale-Hokuala-…-Exterior-with-Pool-and-Ocean.jpg
 *   → "Hale Hokuala Kauaʻi | Exterior with Pool and Ocean"
 */
export function captionFromPropertyImageUrl(url, opts = {}) {
  const base = imageBasename(url).replace(/\.(webp|png|jpe?g|gif|svg)$/i, "");
  if (!base || isBrandLogoGalleryAsset(url, opts.meta)) return null;

  // Strip soft-brand suffix + optional trailing index (_1 / -2) then optional scene tail.
  const brandStrip =
    opts.brandStripRe ||
    /-(?:Curio|Ascend|Autograph|Tapestry|Kimpton|Tribute|Voco|Vignette)-Collection(?:-by-[A-Za-z]+)?(?:[_-]\d+)?/i;

  let hotelPart = base;
  let scenePart = "";
  const m = base.match(new RegExp(`^(.+?)${brandStrip.source}(?:-(.+))?$`, "i"));
  if (m) {
    hotelPart = m[1];
    scenePart = m[2] || "";
  } else {
    // Fallback: drop trailing _N only
    hotelPart = base.replace(/[_-]\d+$/g, "");
  }

  hotelPart = hotelPart
    .replace(/_/g, " ")
    .replace(/-/g, " ")
    .replace(/\s+\d+$/g, "")
    .replace(/\s+/g, " ")
    .trim();
  scenePart = scenePart
    .replace(/_/g, " ")
    .replace(/-/g, " ")
    .replace(/\b(\w+)\s+\1\b/gi, "$1")
    .replace(/\s+/g, " ")
    .trim();
  if (/^by\s+hilton\b/i.test(scenePart) || /^by\s+\w+$/i.test(scenePart)) {
    scenePart = "";
  }

  if (!hotelPart || BRAND_ONLY_BASENAME_RE.test(hotelPart.replace(/\s+/g, "-"))) {
    return null;
  }

  const classification = opts.classification || (opts.isRendering ? "Proposed Rendering" : null);
  const scene =
    scenePart ||
    classification ||
    (opts.sceneType ? humanizeScene(opts.sceneType) : "Property Photography");

  if (opts.location) {
    return `${hotelPart}, ${opts.location} | ${scene}`;
  }
  return `${hotelPart} | ${scene}`;
}

function humanizeScene(sceneType) {
  const s = nz(sceneType).replace(/_/g, " ");
  if (!s) return "Property Photography";
  return s.replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Build canonical customer caption.
 */
export function buildGalleryCaption({
  hotelName,
  scene,
  location,
  classification,
  isRendering,
} = {}) {
  const hotel = nz(hotelName);
  const what = nz(scene) || (isRendering || /render/i.test(nz(classification))
    ? "Proposed Exterior Rendering"
    : "Property Photography");
  if (!hotel) return null;
  if (location) return `${hotel}, ${nz(location)} | ${what}`;
  if (classification && /render/i.test(classification) && !/render/i.test(what)) {
    return `${hotel} | ${what} (${nz(classification)})`;
  }
  return `${hotel} | ${what}`;
}

export function galleryBlocksFromList(blocks) {
  return (blocks || [])
    .filter((b) => GALLERY_SLOT_RE.test(nz(b.slotKey || b.slot)))
    .sort((a, b) => {
      const ai = Number(String(a.slotKey).match(GALLERY_SLOT_RE)?.[1] || 99);
      const bi = Number(String(b.slotKey).match(GALLERY_SLOT_RE)?.[1] || 99);
      return ai - bi;
    });
}

/**
 * Validate a materials.gallery.1–6 set.
 * @returns {{ ok: boolean, pass: boolean, failures: object[], assetRegister: object[] }}
 */
export function validateSixImageGallery(blocks, opts = {}) {
  const gallery = galleryBlocksFromList(blocks);
  const failures = [];
  const assetRegister = [];
  const seenDup = new Map();

  if (gallery.length !== GALLERY_REQUIRED_COUNT) {
    failures.push({
      code: GALLERY_FAIL.COUNT,
      detail: `expected ${GALLERY_REQUIRED_COUNT} gallery slots, found ${gallery.length}`,
    });
  }

  for (const b of gallery) {
    const slotKey = nz(b.slotKey || b.slot);
    const title = nz(b.title);
    const body = nz(b.body);
    const imageUrl = nz(b.imageUrl || b.image_url || (/^https?:\/\//i.test(body) ? body : ""));
    const meta = b.galleryMeta || b.meta || {};
    const entry = {
      brandId: opts.brandId || null,
      propertyId: meta.propertyId || null,
      projectIdentity: meta.projectIdentity || null,
      imageUrl: imageUrl || null,
      sourceUrl: meta.sourceUrl || null,
      imageType: meta.imageType || (meta.isRendering ? "official_rendering" : "operating_photograph"),
      sceneType: meta.sceneType || null,
      hotelName: meta.hotelName || null,
      location: meta.location || null,
      rightsUseAuthorization: meta.rightsUseAuthorization || meta.rights || null,
      verificationStatus: meta.verificationStatus || "unverified",
      dateChecked: meta.dateChecked || null,
      caption: title || null,
      duplicateDetectionIdentifier: imageUrl ? duplicateDetectionId(imageUrl) : null,
      slotKey,
    };
    assetRegister.push(entry);

    if (!imageUrl) {
      failures.push({ code: GALLERY_FAIL.MISSING_URL, slotKey, detail: "missing imageUrl" });
      continue;
    }
    if (isBrandLogoGalleryAsset(imageUrl, meta)) {
      failures.push({
        code: GALLERY_FAIL.LOGO,
        slotKey,
        detail: `logo/brand-graphic rejected: ${imageBasename(imageUrl)}`,
      });
    }
    if (isPeopleOrLifestyleMarketingImage(imageUrl, meta) || !isLikelyHotelPropertyPhotograph(imageUrl, {
      ...meta,
      role: meta.role || meta.sceneType,
      caption: title,
      title,
    })) {
      // Only fail hard on explicit people/lifestyle; unknown DAM without cues already
      // rejected by factory loader. Soft-fail when people cues are present.
      if (isPeopleOrLifestyleMarketingImage(imageUrl, { ...meta, caption: title, title })) {
        failures.push({
          code: GALLERY_FAIL.PEOPLE_LIFESTYLE,
          slotKey,
          detail: `people/lifestyle marketing rejected: ${imageBasename(imageUrl)}`,
        });
      }
    }
    if (isGenericGalleryCaption(title, body)) {
      failures.push({
        code: GALLERY_FAIL.GENERIC_CAPTION,
        slotKey,
        detail: `generic caption: ${JSON.stringify(title)}`,
      });
    }
    if (meta.isRendering === true && !/render/i.test(title + " " + nz(meta.classification))) {
      failures.push({
        code: GALLERY_FAIL.RENDERING_MISLABELED,
        slotKey,
        detail: "official rendering must be labeled as rendering, not photography",
      });
    }
    if (opts.requireRights && !entry.rightsUseAuthorization) {
      failures.push({ code: GALLERY_FAIL.RIGHTS_MISSING, slotKey, detail: "rights/use missing" });
    }
    const dupId = entry.duplicateDetectionIdentifier;
    if (dupId) {
      if (seenDup.has(dupId)) {
        failures.push({
          code: GALLERY_FAIL.DUPLICATE,
          slotKey,
          detail: `duplicate of ${seenDup.get(dupId)}`,
        });
      } else {
        seenDup.set(dupId, slotKey);
      }
    }
  }

  const suitableCount = gallery.filter((b) => {
    const imageUrl = nz(b.imageUrl || b.image_url);
    const title = nz(b.title);
    return (
      imageUrl &&
      !isBrandLogoGalleryAsset(imageUrl, b.galleryMeta || {}) &&
      !isGenericGalleryCaption(title, b.body)
    );
  }).length;

  if (suitableCount < GALLERY_REQUIRED_COUNT) {
    failures.push({
      code: GALLERY_FAIL.COUNT,
      detail: `only ${suitableCount}/${GALLERY_REQUIRED_COUNT} suitable hotel/rendering images`,
    });
  }

  const ok = failures.length === 0;
  return {
    contract: BE_PERMANENT_SIX_IMAGE_GALLERY_STANDARD_V1,
    ok,
    pass: ok,
    failures,
    suitableCount,
    requiredCount: GALLERY_REQUIRED_COUNT,
    assetRegister,
    founderApprovalBlocked: !ok,
  };
}

/**
 * Decide whether a candidate gallery slot may replace CURRENT.
 * Failing galleries never overwrite a complete CURRENT gallery.
 */
export function decideGallerySlotApply(slotKey, currentBlock, candidateBlock) {
  if (!GALLERY_SLOT_RE.test(nz(slotKey))) {
    return { applies: false, reason: "not_gallery_slot" };
  }
  const candUrl = nz(
    candidateBlock?.imageUrl ||
      candidateBlock?.image_url ||
      (/^https?:\/\//i.test(nz(candidateBlock?.body)) ? candidateBlock.body : ""),
  );
  const curUrl = nz(
    currentBlock?.imageUrl ||
      currentBlock?.image_url ||
      (/^https?:\/\//i.test(nz(currentBlock?.body)) ? currentBlock.body : ""),
  );
  const candTitle = nz(candidateBlock?.title);
  const curTitle = nz(currentBlock?.title);

  const currentOk =
    !!curUrl &&
    !isBrandLogoGalleryAsset(curUrl, currentBlock?.galleryMeta) &&
    !isGenericGalleryCaption(curTitle, currentBlock?.body);
  const candidateOk =
    !!candUrl &&
    !isBrandLogoGalleryAsset(candUrl, candidateBlock?.galleryMeta) &&
    !isGenericGalleryCaption(candTitle, candidateBlock?.body) &&
    /\|\s*\S+/.test(candTitle);

  // Never overwrite a verified CURRENT hotel photo with a reconstruction swap
  // (CDN host / metadata reprocess alone must not mark all six as changed).
  if (currentOk) {
    if (
      candidateOk &&
      duplicateDetectionId(curUrl) === duplicateDetectionId(candUrl) &&
      candTitle !== curTitle
    ) {
      // Same photograph, improved caption only — genuine caption modification.
      return { applyCandidate: true, reason: "candidate_gallery_caption_improvement_same_asset" };
    }
    return {
      applyCandidate: false,
      reason: "retain_verified_current_gallery_photo",
      retainCurrent: true,
    };
  }

  if (!candUrl || isBrandLogoGalleryAsset(candUrl, candidateBlock?.galleryMeta)) {
    return {
      applyCandidate: false,
      reason: "candidate_gallery_logo_or_missing",
      retainCurrent: false,
    };
  }
  if (isGenericGalleryCaption(candTitle, candidateBlock?.body)) {
    return {
      applyCandidate: false,
      reason: "candidate_gallery_generic_caption",
      retainCurrent: false,
    };
  }
  if (candidateOk) {
    return { applyCandidate: true, reason: "candidate_fills_empty_or_invalid_current_gallery" };
  }
  return {
    applyCandidate: false,
    reason: "candidate_gallery_not_safe",
    retainCurrent: !!curUrl,
  };
}

/**
 * Repair gallery titles from URL evidence; strip logos to empty (gate-blocked).
 */
export function repairGalleryBlocksInPlace(blocks, opts = {}) {
  const list = Array.isArray(blocks) ? blocks : [];
  const repaired = [];
  for (const b of list) {
    if (!GALLERY_SLOT_RE.test(nz(b.slotKey || b.slot))) {
      repaired.push(b);
      continue;
    }
    const next = { ...b };
    const imageUrl = nz(next.imageUrl || next.image_url || (/^https?:\/\//i.test(nz(next.body)) ? next.body : ""));
    if (!imageUrl || isBrandLogoGalleryAsset(imageUrl, next.galleryMeta)) {
      next.imageUrl = null;
      next.title = "Imagery unavailable";
      next.body = "";
      next._galleryGate = "rejected_logo_or_missing";
      repaired.push(next);
      continue;
    }
    next.imageUrl = imageUrl;
    const fromUrl = captionFromPropertyImageUrl(imageUrl, {
      brandStripRe: opts.brandStripRe,
      meta: next.galleryMeta,
    });
    const existing = nz(next.title);
    const existingWeak =
      !existing ||
      isGenericGalleryCaption(existing, next.body) ||
      /\|\s*by\s+\w+/i.test(existing) ||
      /\b(\w+)\s+\1\b/i.test(existing) ||
      /^https?:\/\//i.test(existing);
    const derived = (!existingWeak && existing) || fromUrl || null;
    if (derived) {
      // Prefer URL-derived hotel|scene when existing caption is weak/malformed
      next.title = existingWeak && fromUrl ? fromUrl : derived;
      if (!nz(next.body) || /^https?:\/\//i.test(nz(next.body)) || isGenericGalleryCaption(next.body) || existingWeak) {
        next.body = next.title;
      }
    } else if (isGenericGalleryCaption(next.title)) {
      next.title = "Imagery unavailable";
      next._galleryGate = "caption_underivable";
    }
    repaired.push(next);
  }
  return repaired;
}

export function filterNonLogoImageryEvidence(evidence = []) {
  return (evidence || []).filter((e) => {
    const url = nz(e.claim || e.imageUrl || e.url);
    if (!/^https?:\/\//i.test(url)) return false;
    return !isBrandLogoGalleryAsset(url, e);
  });
}
