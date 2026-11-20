/**
 * Brand Explorer Factory — Permanent Image Integrity Contract V2
 *
 * Extends V1 (URL ≠ complete) with Airtable attachment lifecycle statuses,
 * CURRENT vs PROPOSED separation, and staged lifecycle vocab for remediations.
 *
 * Contract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2
 */
import {
  BE_FACTORY_IMAGE_INTEGRITY_V1,
  REQUIRED_IMAGE_SLOTS,
  GALLERY_SLOTS,
  SCENARIO_SLOTS,
  IMAGE_INTEGRITY_STATUS as V1_STATUS,
  isRequiredImageSlot,
  extractImageUrl,
  urlFingerprint,
  decodeImageDimensions,
} from "./be-factory-image-integrity-v1.js";

export const BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2 =
  "BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2";

export {
  REQUIRED_IMAGE_SLOTS,
  GALLERY_SLOTS,
  SCENARIO_SLOTS,
  isRequiredImageSlot,
  extractImageUrl,
  urlFingerprint,
  decodeImageDimensions,
  BE_FACTORY_IMAGE_INTEGRITY_V1,
  V1_STATUS as IMAGE_INTEGRITY_STATUS_V1,
};

/** Slot-level classification (founder directive). */
export const IMAGE_SLOT_CLASS = Object.freeze({
  MISSING: "MISSING",
  SOURCE_URL_INVALID: "SOURCE_URL_INVALID",
  SOURCE_FETCH_FAILED: "SOURCE_FETCH_FAILED",
  AIRTABLE_IMPORT_FAILED: "AIRTABLE_IMPORT_FAILED",
  ATTACHMENT_MISSING: "ATTACHMENT_MISSING",
  ATTACHMENT_PRESENT_UNVERIFIED: "ATTACHMENT_PRESENT_UNVERIFIED",
  ATTACHMENT_RENDER_FAILED: "ATTACHMENT_RENDER_FAILED",
  SIGNED_URL_EXPIRED: "SIGNED_URL_EXPIRED",
  FORMAT_UNSUPPORTED: "FORMAT_UNSUPPORTED",
  IMAGE_CORRUPT: "IMAGE_CORRUPT",
  WRONG_BRAND: "WRONG_BRAND",
  WRONG_PROPERTY: "WRONG_PROPERTY",
  WRONG_SCENE: "WRONG_SCENE",
  DUPLICATE_IMAGE: "DUPLICATE_IMAGE",
  RIGHTS_UNVERIFIED: "RIGHTS_UNVERIFIED",
  VALIDATED: "VALIDATED",
  CERTIFIED: "CERTIFIED",
});

/** Remediation lifecycle (do not collapse). */
export const IMAGE_LIFECYCLE = Object.freeze({
  IMAGE_DISCOVERED: "IMAGE_DISCOVERED",
  IMAGE_STAGED: "IMAGE_STAGED",
  IMAGE_ATTACHMENT_IMPORTED: "IMAGE_ATTACHMENT_IMPORTED",
  IMAGE_READBACK_VERIFIED: "IMAGE_READBACK_VERIFIED",
  IMAGE_BROWSER_VALIDATED: "IMAGE_BROWSER_VALIDATED",
  IMAGE_CERTIFIED: "IMAGE_CERTIFIED",
});

export const IMAGE_COVERAGE_LAYER = Object.freeze({
  AIRTABLE_CURRENT: "AIRTABLE_CURRENT",
  FACTORY_PROPOSED: "FACTORY_PROPOSED",
});

const AIRTABLE_CDN_HOST_RE = /(^|\.)airtableusercontent\.com$/i;

export function isAirtableSignedCdnUrl(url) {
  try {
    return AIRTABLE_CDN_HOST_RE.test(new URL(String(url || "")).hostname);
  } catch {
    return false;
  }
}

/**
 * Classify a live Presentation Image attachment after optional HTTP probe.
 * @param {{ attachment?: object|null, probe?: object|null }} input
 */
export function classifyLiveAttachment({ attachment = null, probe = null } = {}) {
  if (!attachment || !attachment.url) {
    return IMAGE_SLOT_CLASS.ATTACHMENT_MISSING;
  }
  if (!probe) return IMAGE_SLOT_CLASS.ATTACHMENT_PRESENT_UNVERIFIED;
  if (probe.httpStatus === 410 || probe.reason === "http_410") {
    return IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED;
  }
  if (probe.ok === false) {
    if (/403|404|410/.test(String(probe.httpStatus || ""))) {
      return IMAGE_SLOT_CLASS.ATTACHMENT_RENDER_FAILED;
    }
    return IMAGE_SLOT_CLASS.ATTACHMENT_RENDER_FAILED;
  }
  const fmt = String(probe.format || "").toLowerCase();
  if (fmt.includes("avif") || fmt.includes("heif")) {
    return IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED;
  }
  if (fmt === "html" || fmt === "unknown") {
    return IMAGE_SLOT_CLASS.IMAGE_CORRUPT;
  }
  if (probe.decodeOk === false) return IMAGE_SLOT_CLASS.IMAGE_CORRUPT;
  if (probe.browserOk === true && probe.visualOk === true && probe.rightsOk === true) {
    return IMAGE_SLOT_CLASS.CERTIFIED;
  }
  if (probe.browserOk === true && probe.decodeOk !== false) {
    return IMAGE_SLOT_CLASS.VALIDATED;
  }
  return IMAGE_SLOT_CLASS.ATTACHMENT_PRESENT_UNVERIFIED;
}

/**
 * Map V1 technical status → V2 slot class for PROPOSED/frozen URLs.
 */
export function classifyProposedUrlStatus(v1Status, reason = "") {
  const s = String(v1Status || "");
  if (s === V1_STATUS.MISSING || !s) return IMAGE_SLOT_CLASS.MISSING;
  if (s === V1_STATUS.FETCH_FAILED) {
    if (String(reason).includes("410") || /expired/i.test(reason)) {
      return IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED;
    }
    return IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED;
  }
  if (s === V1_STATUS.DECODE_FAILED) {
    if (/avif|webp|unsupported/i.test(reason)) return IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED;
    return IMAGE_SLOT_CLASS.IMAGE_CORRUPT;
  }
  if (s === V1_STATUS.BROWSER_RENDER_FAILED) return IMAGE_SLOT_CLASS.ATTACHMENT_RENDER_FAILED;
  if (s === V1_STATUS.VISUAL_REVIEW_REQUIRED) return IMAGE_SLOT_CLASS.RIGHTS_UNVERIFIED;
  if (s === V1_STATUS.CERTIFIED) return IMAGE_SLOT_CLASS.CERTIFIED;
  if (s === V1_STATUS.RENDER_VALID) return IMAGE_SLOT_CLASS.VALIDATED;
  return IMAGE_SLOT_CLASS.ATTACHMENT_PRESENT_UNVERIFIED;
}

/**
 * Image identity for writeback comparisons — never use signed CDN URL equality alone.
 */
export function attachmentIdentity(attOrBlock = {}) {
  const id = String(attOrBlock.attachmentId || attOrBlock.id || "").trim();
  const filename = String(attOrBlock.filename || attOrBlock.imageFilename || "").trim();
  const size = Number(attOrBlock.size || attOrBlock.bytes || 0) || 0;
  const type = String(attOrBlock.type || attOrBlock.contentType || "").trim();
  const byteHash = String(attOrBlock.byteHash || "").trim();
  const sourceUrl = String(attOrBlock.sourceUrl || attOrBlock.durableSourceUrl || "").trim();
  return {
    attachmentId: id || null,
    filename: filename || null,
    size: size || null,
    type: type || null,
    byteHash: byteHash || null,
    sourceUrl: sourceUrl || null,
    fingerprint: [id, filename, size, type, byteHash, sourceUrl].filter(Boolean).join("|") || null,
  };
}

/**
 * Decide whether writeback may replace CURRENT Image.
 * Default: preserve valid CURRENT; never overwrite with unverified proposed URL.
 */
export function shouldWriteImageOnWriteback({
  currentAttachment = null,
  proposedImageUrl = "",
  proposedCertified = false,
  proposedTechnicalOk = false,
  forceReplaceBrokenCurrent = false,
} = {}) {
  const hasCurrent = Boolean(currentAttachment?.url || currentAttachment?.attachmentId);
  const proposed = String(proposedImageUrl || "").trim();
  const proposedOk = /^https?:\/\//i.test(proposed);

  if (!proposedOk) {
    return { writeImage: false, reason: "proposed_missing_or_invalid" };
  }
  if (isAirtableSignedCdnUrl(proposed) && !proposed.startsWith("data:")) {
    // Signed CDN URLs are not durable sources for writeback.
    return { writeImage: false, reason: "proposed_is_ephemeral_airtable_cdn" };
  }
  if (hasCurrent) {
    if (forceReplaceBrokenCurrent) {
      return { writeImage: true, reason: "replace_broken_current_authorized" };
    }
    if (!proposedCertified && !proposedTechnicalOk) {
      return { writeImage: false, reason: "preserve_current_unverified_proposed" };
    }
    if (!proposedCertified) {
      return { writeImage: false, reason: "preserve_current_until_proposed_certified" };
    }
  }
  if (!proposedCertified && !proposedTechnicalOk) {
    return { writeImage: false, reason: "proposed_not_technically_ok" };
  }
  return { writeImage: true, reason: hasCurrent ? "replace_with_certified_proposed" : "fill_missing_current" };
}

export function emptyCoverageTotals() {
  return {
    requiredSlots: 0,
    attachmentMissing: 0,
    attachmentPresentUnverified: 0,
    signedUrlExpired: 0,
    renderFailed: 0,
    formatUnsupported: 0,
    validated: 0,
    certified: 0,
    other: 0,
  };
}

export function bumpCoverage(totals, classification) {
  const t = { ...emptyCoverageTotals(), ...(totals || {}) };
  t.requiredSlots += 1;
  switch (classification) {
    case IMAGE_SLOT_CLASS.ATTACHMENT_MISSING:
    case IMAGE_SLOT_CLASS.MISSING:
      t.attachmentMissing += 1;
      break;
    case IMAGE_SLOT_CLASS.SIGNED_URL_EXPIRED:
      t.signedUrlExpired += 1;
      break;
    case IMAGE_SLOT_CLASS.ATTACHMENT_RENDER_FAILED:
    case IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED:
    case IMAGE_SLOT_CLASS.AIRTABLE_IMPORT_FAILED:
      t.renderFailed += 1;
      break;
    case IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED:
    case IMAGE_SLOT_CLASS.IMAGE_CORRUPT:
      t.formatUnsupported += 1;
      break;
    case IMAGE_SLOT_CLASS.VALIDATED:
      t.validated += 1;
      break;
    case IMAGE_SLOT_CLASS.CERTIFIED:
      t.certified += 1;
      break;
    case IMAGE_SLOT_CLASS.ATTACHMENT_PRESENT_UNVERIFIED:
      t.attachmentPresentUnverified += 1;
      break;
    default:
      t.other += 1;
  }
  return t;
}
