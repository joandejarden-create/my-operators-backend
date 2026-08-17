/**
 * Read-only image integrity checker — metadata/source/entity consistency.
 * Never downloads, rehosts, or replaces images.
 */

import { IMAGE_CLASSIFICATIONS, IMAGE_ACTIONS, RESEARCH_MODES } from "./research-modes.js";

/**
 * @param {object} image
 * @param {object} entity - hotel or brand context
 */
export function classifyImageIntegrity(image, entity = {}) {
  const url = String(image.url || image.src || image.imageUrl || "").trim();
  const sourceDomain = domainOf(url);
  const brand = String(entity.currentBrand || entity.brand || entity.brandName || "").toLowerCase();
  const status = String(entity.currentStatus || entity.status || "").toLowerCase();
  const officialUrl = String(entity.officialUrl || entity.website || "").toLowerCase();
  const role = String(image.role || image.slot || "").toLowerCase();
  const caption = String(image.caption || image.alt || "").toLowerCase();
  const isRendering = /render|rendering|artist.?impression|cgi|3d.?visual/i.test(
    `${caption} ${url} ${image.assetType || ""}`
  );

  /** @type {string} */
  let classification = "Needs Review";
  /** @type {string} */
  let action = "Needs Manual Verification";
  /** @type {string[]} */
  const reasons = [];

  if (!url) {
    return result("Missing", "Add Candidate", ["No image URL"], image, entity);
  }

  // Wrong brand domain heuristics
  if (brand && sourceDomain) {
    if (/indigo|kimpton|ihg/.test(brand) && /marriott\.com|choicehotels\.com/.test(sourceDomain)) {
      classification = "Wrong Brand";
      action = "Replace Candidate";
      reasons.push("Image host domain conflicts with brand family");
    }
    if (/tribute|autograph|marriott/.test(brand) && /ihg\.com|hilton\.com/.test(sourceDomain) && !/cache\.marriott/.test(sourceDomain)) {
      classification = "Wrong Brand";
      action = "Replace Candidate";
      reasons.push("Image host domain conflicts with Marriott soft brand");
    }
  }

  // Reflag stale: old brand in URL path vs current
  if (entity.priorBrand && brand) {
    const prior = String(entity.priorBrand).toLowerCase();
    if (prior && prior !== brand && url.toLowerCase().includes(prior.replace(/\s+/g, ""))) {
      classification = "Stale";
      action = "Replace Candidate";
      reasons.push("URL/path references prior brand after reflag");
    }
  }

  // Pipeline + rendering + now open
  if (/pipeline/.test(String(entity.dealalityStatus || "")) && /open/.test(status) && isRendering) {
    classification = "Rendering Only";
    action = "Review";
    reasons.push("Dealality was pipeline with rendering; observed operating — prefer current photography");
  } else if (/open/.test(status) && isRendering && officialUrl) {
    classification = "Rendering Only";
    action = "Review";
    reasons.push("Operating hotel still using rendering asset");
  }

  // Duplicate marker
  if (image.duplicateOf || image.isDuplicate) {
    classification = "Duplicate";
    action = "Remove Candidate";
    reasons.push("Marked duplicate reuse across unrelated properties");
  }

  // Missing with official imagery available (hint only)
  if (!url && entity.officialPhotographyAvailable) {
    return result("Missing", "Add Candidate", ["Official photography identifiable"], image, entity);
  }

  if (!reasons.length) {
    if (sourceDomain && (officialUrl.includes(sourceDomain) || isKnownOfficialCdn(sourceDomain, brand))) {
      classification = "Current";
      action = "Keep";
      reasons.push("Source domain consistent with brand/official CDN");
    } else if (sourceDomain) {
      classification = "Low Confidence";
      action = "Review";
      reasons.push("Source domain not confirmed as official brand CDN");
    }
  }

  return result(classification, action, reasons, image, entity, { sourceDomain, isRendering, role });
}

function result(classification, action, reasons, image, entity, extra = {}) {
  return {
    researchMode: RESEARCH_MODES.IMAGE_INTEGRITY,
    hotelId: entity.hotelId || entity.recordId || null,
    brand: entity.currentBrand || entity.brandName || null,
    imageUrl: image.url || image.src || image.imageUrl || null,
    classification,
    recommended_action: action,
    reasons,
    sourceDomain: extra.sourceDomain || domainOf(image.url || image.src || ""),
    isRendering: Boolean(extra.isRendering),
    role: extra.role || null,
    note: "READ-ONLY — no download/rehost/replace",
  };
}

function domainOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return String(url || "")
      .replace(/^https?:\/\//, "")
      .split("/")[0]
      .replace(/^www\./, "");
  }
}

function isKnownOfficialCdn(domain, brand) {
  const d = String(domain || "").toLowerCase();
  if (/digital\.ihg\.com|ihg\.com/.test(d) && /indigo|kimpton|ihg|holiday|crowne/.test(brand)) return true;
  if (/cache\.marriott\.com|marriott\.com/.test(d) && /marriott|tribute|autograph|westin|sheraton/.test(brand))
    return true;
  if (/media\.choicehotels\.com|choicehotels\.com/.test(d)) return true;
  if (/hilton\.com|www\.hilton\.com/.test(d) && /hilton|hampton|curio|tapestry|spark/.test(brand)) return true;
  if (/minorhotels\.com|avanihotels\.com/.test(d) && /avani|minor/.test(brand)) return true;
  return false;
}

/**
 * @param {object[]} images
 * @param {object} entity
 */
export function auditImagesForEntity(images, entity) {
  const results = (images || []).map((img) => classifyImageIntegrity(img, entity));
  if (!(images || []).length) {
    results.push(
      classifyImageIntegrity({}, { ...entity, officialPhotographyAvailable: Boolean(entity.officialUrl) })
    );
  }
  return {
    entityId: entity.hotelId || entity.slug || entity.brandName,
    entityName: entity.name || entity.brandName,
    results,
    summary: {
      current: results.filter((r) => r.classification === "Current").length,
      missing: results.filter((r) => r.classification === "Missing").length,
      stale: results.filter((r) => r.classification === "Stale").length,
      wrongBrand: results.filter((r) => r.classification === "Wrong Brand").length,
      renderingOnly: results.filter((r) => r.classification === "Rendering Only").length,
      needsReview: results.filter((r) => r.classification === "Needs Review" || r.classification === "Low Confidence")
        .length,
    },
  };
}

export { IMAGE_CLASSIFICATIONS, IMAGE_ACTIONS };
