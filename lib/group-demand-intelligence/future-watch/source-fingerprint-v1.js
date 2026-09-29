/**
 * Source fingerprint / material-change detection for watch URLs.
 */

import crypto from "node:crypto";

const MATERIAL_RE =
  /accommodation|alojamiento|housing|room block|host hotel|registration|inscripci|hotel|overflow|venue|fecha|dates?/i;

/**
 * Extract material-relevant text slice for hashing (ignore chrome noise).
 */
export function materialTextSlice(htmlOrText = "") {
  const t = String(htmlOrText || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  const hits = [];
  const sentences = t.split(/[.!?|;]/).map((s) => s.trim()).filter(Boolean);
  for (const s of sentences) {
    if (MATERIAL_RE.test(s)) hits.push(s.slice(0, 200));
  }
  if (!hits.length) return t.slice(0, 800);
  return hits.slice(0, 40).join(" | ");
}

export function hashMaterialContent(text = "") {
  const slice = materialTextSlice(text);
  return crypto.createHash("sha256").update(slice).digest("hex").slice(0, 32);
}

/**
 * Compare fingerprints. Trivial template-only changes → not material.
 */
export function detectSourceMaterialChange(prev = null, next = {}) {
  if (!prev) {
    return { changed: false, reason: "NO_PRIOR_FINGERPRINT", fingerprint: next };
  }
  if (prev.contentHash && next.contentHash && prev.contentHash === next.contentHash) {
    return { changed: false, reason: "HASH_UNCHANGED", fingerprint: next };
  }
  if (prev.etag && next.etag && prev.etag === next.etag) {
    return { changed: false, reason: "ETAG_UNCHANGED", fingerprint: next };
  }
  if (
    prev.lastModified &&
    next.lastModified &&
    prev.lastModified === next.lastModified &&
    prev.contentHash === next.contentHash
  ) {
    return { changed: false, reason: "LAST_MODIFIED_UNCHANGED", fingerprint: next };
  }
  // Material only if material-text hash changed
  if (prev.materialHash && next.materialHash && prev.materialHash !== next.materialHash) {
    return { changed: true, reason: "MATERIAL_HASH_CHANGED", fingerprint: next };
  }
  if (prev.contentHash && next.contentHash && prev.contentHash !== next.contentHash) {
    // Full hash changed but material hash same → trivial
    if (prev.materialHash && next.materialHash && prev.materialHash === next.materialHash) {
      return { changed: false, reason: "TRIVIAL_HTML_CHANGE", fingerprint: next };
    }
    return { changed: true, reason: "CONTENT_HASH_CHANGED", fingerprint: next };
  }
  return { changed: false, reason: "INSUFFICIENT_SIGNAL", fingerprint: next };
}

export function buildSourceFingerprint({
  url,
  text = "",
  etag = null,
  lastModified = null,
} = {}) {
  const materialHash = hashMaterialContent(text);
  const contentHash = crypto
    .createHash("sha256")
    .update(String(text || "").slice(0, 50000))
    .digest("hex")
    .slice(0, 32);
  return {
    url: url || null,
    etag,
    lastModified,
    materialHash,
    contentHash,
    capturedAt: new Date().toISOString(),
  };
}
