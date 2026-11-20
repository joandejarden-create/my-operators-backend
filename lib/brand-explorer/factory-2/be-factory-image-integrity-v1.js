/**
 * Brand Explorer Factory — Image Integrity Contract V1
 *
 * A populated image URL is NOT a successful image. Factory completion requires
 * technical fetch+decode, browser render proof (or bounded freshness evidence),
 * visual suitability, and correct slot mapping.
 *
 * Pipeline: Discover → Select → Fetch → Decode → Browser Render → Visual Validate → Certify → Score → Approval
 *
 * Contract: BE_FACTORY_IMAGE_INTEGRITY_V1
 */
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { isUnsuitablePropertyDisplayImage } from "../be-property-image-integrity-v1.js";
import {
  isPeopleOrLifestyleMarketingImage,
  isBrandLogoGalleryAsset,
  isLikelyHotelPropertyPhotograph,
} from "../be-permanent-six-image-gallery-standard-v1.js";
import { isSameBrandImage } from "../be-permanent-same-brand-image-affinity-v1.js";

export const BE_FACTORY_IMAGE_INTEGRITY_V1 = "BE_FACTORY_IMAGE_INTEGRITY_V1";

/** Machine-readable per-image validation statuses (founder vocabulary). */
export const IMAGE_INTEGRITY_STATUS = Object.freeze({
  MISSING: "MISSING",
  URL_PRESENT_UNVERIFIED: "URL_PRESENT_UNVERIFIED",
  FETCH_FAILED: "FETCH_FAILED",
  DECODE_FAILED: "DECODE_FAILED",
  BROWSER_RENDER_FAILED: "BROWSER_RENDER_FAILED",
  RENDER_VALID: "RENDER_VALID",
  VISUAL_REVIEW_REQUIRED: "VISUAL_REVIEW_REQUIRED",
  CERTIFIED: "CERTIFIED",
});

/** Required slots that must be CERTIFIED before factory image-complete. */
export const REQUIRED_IMAGE_SLOTS = Object.freeze([
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
]);

export const GALLERY_SLOTS = REQUIRED_IMAGE_SLOTS.filter((s) => s.startsWith("materials.gallery."));
export const SCENARIO_SLOTS = REQUIRED_IMAGE_SLOTS.filter((s) => s.startsWith("overview.scenario."));

/** Factory approval requires 100% CERTIFIED on required slots. */
export const FACTORY_IMAGE_APPROVAL_THRESHOLD = 1;

/** Previously CERTIFIED evidence may be reused within this window (ms) if URL unchanged. */
export const CERTIFICATION_FRESHNESS_MS = 14 * 24 * 60 * 60 * 1000;

const UA =
  "DealalityBrandExplorerBot/1.0 (+https://dealality.com; factory image integrity; $0)";
const FETCH_TIMEOUT_MS = 12000;
const MAX_BYTES = 8 * 1024 * 1024;

function nz(s) {
  return s == null ? "" : String(s).trim();
}

export function isRequiredImageSlot(slotKey) {
  return REQUIRED_IMAGE_SLOTS.includes(nz(slotKey));
}

export function extractImageUrl(block = {}) {
  const direct = nz(block.imageUrl || block.image_url || block.url || block.src);
  if (/^https?:\/\//i.test(direct)) return direct;
  const body = nz(block.body);
  const m = body.match(/https?:\/\/[^\s)"']+\.(?:jpe?g|png|webp|gif)(?:\?[^\s)"']*)?/i);
  if (m) return m[0];
  const any = body.match(/https?:\/\/[^\s)"']+/i);
  return any ? any[0] : "";
}

export function urlFingerprint(url) {
  return createHash("sha256").update(nz(url)).digest("hex");
}

/** Minimal dimension decode from image magic bytes (no native deps). */
export function decodeImageDimensions(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 24) {
    return { ok: false, reason: "buffer_too_small" };
  }
  // PNG
  if (buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) {
    const width = buf.readUInt32BE(16);
    const height = buf.readUInt32BE(20);
    if (width > 0 && height > 0) return { ok: true, format: "png", width, height };
    return { ok: false, reason: "png_zero_dimension", width, height };
  }
  // JPEG
  if (buf[0] === 0xff && buf[1] === 0xd8) {
    let i = 2;
    while (i < buf.length - 8) {
      if (buf[i] !== 0xff) {
        i += 1;
        continue;
      }
      const marker = buf[i + 1];
      if (marker === 0xd9 || marker === 0xda) break;
      const len = buf.readUInt16BE(i + 2);
      if (len < 2) break;
      // SOF0..SOF3 / SOF5..SOF7 / SOF9..SOF11 / SOF13..SOF15
      if (
        (marker >= 0xc0 && marker <= 0xc3) ||
        (marker >= 0xc5 && marker <= 0xc7) ||
        (marker >= 0xc9 && marker <= 0xcb) ||
        (marker >= 0xcd && marker <= 0xcf)
      ) {
        const height = buf.readUInt16BE(i + 5);
        const width = buf.readUInt16BE(i + 7);
        if (width > 0 && height > 0) return { ok: true, format: "jpeg", width, height };
        return { ok: false, reason: "jpeg_zero_dimension", width, height };
      }
      i += 2 + len;
    }
    return { ok: false, reason: "jpeg_sof_missing" };
  }
  // GIF
  if (buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46) {
    const width = buf.readUInt16LE(6);
    const height = buf.readUInt16LE(8);
    if (width > 0 && height > 0) return { ok: true, format: "gif", width, height };
    return { ok: false, reason: "gif_zero_dimension", width, height };
  }
  // WEBP (RIFF....WEBP)
  if (
    buf[0] === 0x52 &&
    buf[1] === 0x49 &&
    buf[2] === 0x46 &&
    buf[3] === 0x46 &&
    buf[8] === 0x57 &&
    buf[9] === 0x45 &&
    buf[10] === 0x42 &&
    buf[11] === 0x50
  ) {
    // VP8X
    if (buf[12] === 0x56 && buf[13] === 0x50 && buf[14] === 0x38 && buf[15] === 0x58) {
      const width = 1 + buf[24] + (buf[25] << 8) + (buf[26] << 16);
      const height = 1 + buf[27] + (buf[28] << 8) + (buf[29] << 16);
      if (width > 0 && height > 0) return { ok: true, format: "webp", width, height };
    }
    // VP8 lossy
    if (buf[12] === 0x56 && buf[13] === 0x50 && buf[14] === 0x38 && buf[15] === 0x20) {
      const width = buf.readUInt16LE(26) & 0x3fff;
      const height = buf.readUInt16LE(28) & 0x3fff;
      if (width > 0 && height > 0) return { ok: true, format: "webp", width, height };
    }
    // VP8L
    if (buf[12] === 0x56 && buf[13] === 0x50 && buf[14] === 0x38 && buf[15] === 0x4c) {
      const bits = buf.readUInt32LE(21);
      const width = (bits & 0x3fff) + 1;
      const height = ((bits >> 14) & 0x3fff) + 1;
      if (width > 0 && height > 0) return { ok: true, format: "webp", width, height };
    }
    return { ok: false, reason: "webp_unsupported_variant" };
  }
  return { ok: false, reason: "unsupported_or_corrupt_format" };
}

export function contentTypeLooksLikeImage(ct) {
  const c = nz(ct).toLowerCase();
  if (!c) return null;
  if (c.startsWith("image/")) return true;
  if (/octet-stream|binary/.test(c)) return null;
  return false;
}

/**
 * Fetch image bytes (bounded) and decode dimensions.
 * Status progression: MISSING → URL_PRESENT_UNVERIFIED → FETCH_FAILED | DECODE_FAILED | RENDER_VALID (technical).
 */
export async function fetchAndDecodeImage(url, opts = {}) {
  const u = nz(url);
  if (!u) {
    return {
      status: IMAGE_INTEGRITY_STATUS.MISSING,
      ok: false,
      reason: "missing_url",
    };
  }
  if (!/^https?:\/\//i.test(u)) {
    return {
      status: IMAGE_INTEGRITY_STATUS.FETCH_FAILED,
      ok: false,
      reason: "non_http_url",
      url: u,
    };
  }

  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs || FETCH_TIMEOUT_MS;
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(u, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": UA,
        Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
      },
    });
    if (!res.ok) {
      return {
        status: IMAGE_INTEGRITY_STATUS.FETCH_FAILED,
        ok: false,
        reason: `http_${res.status}`,
        httpStatus: res.status,
        url: u,
        finalUrl: res.url || u,
      };
    }
    const ct = res.headers.get("content-type") || "";
    const ctOk = contentTypeLooksLikeImage(ct);
    if (ctOk === false) {
      return {
        status: IMAGE_INTEGRITY_STATUS.FETCH_FAILED,
        ok: false,
        reason: "invalid_content_type",
        contentType: ct,
        url: u,
      };
    }
    const ab = await res.arrayBuffer();
    const buf = Buffer.from(ab);
    if (!buf.length) {
      return {
        status: IMAGE_INTEGRITY_STATUS.FETCH_FAILED,
        ok: false,
        reason: "empty_body",
        url: u,
      };
    }
    if (buf.length > (opts.maxBytes || MAX_BYTES)) {
      return {
        status: IMAGE_INTEGRITY_STATUS.DECODE_FAILED,
        ok: false,
        reason: "payload_too_large",
        bytes: buf.length,
        url: u,
      };
    }
    const dims = decodeImageDimensions(buf);
    if (!dims.ok) {
      return {
        status: IMAGE_INTEGRITY_STATUS.DECODE_FAILED,
        ok: false,
        reason: dims.reason,
        contentType: ct,
        bytes: buf.length,
        url: u,
        finalUrl: res.url || u,
      };
    }
    const byteHash = createHash("sha256").update(buf).digest("hex");
    return {
      status: IMAGE_INTEGRITY_STATUS.RENDER_VALID, // technical render-capable; browser still required for CERTIFIED
      ok: true,
      technicalValid: true,
      format: dims.format,
      width: dims.width,
      height: dims.height,
      bytes: buf.length,
      contentType: ct,
      byteHash,
      url: u,
      finalUrl: res.url || u,
      fetchedAt: new Date().toISOString(),
    };
  } catch (err) {
    return {
      status: IMAGE_INTEGRITY_STATUS.FETCH_FAILED,
      ok: false,
      reason: String(err?.message || err),
      url: u,
    };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Visual / provenance suitability (content-quality, not fetch).
 */
export function evaluateVisualSuitability(url, meta = {}) {
  const u = nz(url);
  const unsuitable = isUnsuitablePropertyDisplayImage(u, meta);
  if (unsuitable.missing) {
    return { ok: false, code: "missing", detail: "missing_url", failureClass: "technical" };
  }
  if (unsuitable.unsuitable) {
    return {
      ok: false,
      code: unsuitable.code,
      detail: unsuitable.detail,
      failureClass: "content_quality",
    };
  }
  if (isPeopleOrLifestyleMarketingImage(u, meta)) {
    return {
      ok: false,
      code: "people_lifestyle",
      detail: "people_or_lifestyle",
      failureClass: "content_quality",
    };
  }
  if (isBrandLogoGalleryAsset(u, meta)) {
    return {
      ok: false,
      code: "logo",
      detail: "brand_logo",
      failureClass: "content_quality",
    };
  }
  if (/placeholder|1x1|pixel\.gif|spacer|blank\.png/i.test(u)) {
    return {
      ok: false,
      code: "placeholder",
      detail: "placeholder_asset",
      failureClass: "content_quality",
    };
  }
  const hotelish =
    isLikelyHotelPropertyPhotograph(u, meta) ||
    /entrance|lobby|guestroom|guest-room|facade|exterior|suite|restaurant|pool|spa/i.test(
      u
    ) ||
    /airtableusercontent|cloudinary|scene7|akamai|imgix|hilton\.com\/im\/|cache\.marriott\.com|marriotts7prod|hyatt\.com\/.*\/media|images\.marriott/i.test(
      u
    );
  if (!hotelish) {
    return {
      ok: false,
      code: "not_hotel_photograph",
      detail: "weak_hotel_cues",
      failureClass: "content_quality",
      reviewRequired: true,
    };
  }
  const slug = nz(meta.slug);
  if (slug && !isSameBrandImage(slug, u, meta)) {
    return {
      ok: false,
      code: "wrong_brand_affinity",
      detail: "same_brand_check_failed",
      failureClass: "content_quality",
      reviewRequired: true,
    };
  }
  return { ok: true, failureClass: null };
}

/**
 * Evaluate one image slot through the integrity pipeline (without browser by default).
 * Browser evidence may be injected via opts.browserEvidence.
 */
export async function evaluateImageSlot(block, ctx = {}, opts = {}) {
  const slotKey = nz(block?.slotKey || block?.slot || ctx.slotKey);
  const url = extractImageUrl(block || {});
  const brandName = nz(ctx.brandName);
  const slug = nz(ctx.slug);
  const meta = {
    slug,
    brandName,
    role: block?.role || block?.sceneType,
    sceneType: block?.sceneType,
    imageType: block?.imageType,
    imageFilename: block?.imageFilename,
    title: block?.title || brandName,
    caption: block?.title,
  };

  const base = {
    contract: BE_FACTORY_IMAGE_INTEGRITY_V1,
    slotKey,
    url: url || null,
    urlFingerprint: url ? urlFingerprint(url) : null,
    brandId: ctx.brandId || null,
    brandName: brandName || null,
    slug: slug || null,
    evaluatedAt: new Date().toISOString(),
  };

  if (!url) {
    return {
      ...base,
      status: IMAGE_INTEGRITY_STATUS.MISSING,
      certified: false,
      failureClass: "technical",
      reason: "missing_url",
      countsAsPopulated: false,
      countsAsFetchValid: false,
      countsAsRenderValid: false,
      countsAsCertified: false,
    };
  }

  // Freshness reuse for previously CERTIFIED unchanged URLs
  const prior = opts.priorEvidence || null;
  if (
    prior &&
    prior.status === IMAGE_INTEGRITY_STATUS.CERTIFIED &&
    prior.urlFingerprint === urlFingerprint(url) &&
    prior.certifiedAt &&
    Date.now() - Date.parse(prior.certifiedAt) < (opts.freshnessMs || CERTIFICATION_FRESHNESS_MS) &&
    !opts.forceRevalidate
  ) {
    return {
      ...base,
      status: IMAGE_INTEGRITY_STATUS.CERTIFIED,
      certified: true,
      reusedPriorEvidence: true,
      priorCertifiedAt: prior.certifiedAt,
      technical: prior.technical || null,
      visual: prior.visual || null,
      browser: prior.browser || null,
      countsAsPopulated: true,
      countsAsFetchValid: true,
      countsAsRenderValid: true,
      countsAsCertified: true,
    };
  }

  if (opts.skipNetwork) {
    const visual = evaluateVisualSuitability(url, meta);
    return {
      ...base,
      status: IMAGE_INTEGRITY_STATUS.URL_PRESENT_UNVERIFIED,
      certified: false,
      reason: "network_skipped",
      visual,
      countsAsPopulated: true,
      countsAsFetchValid: false,
      countsAsRenderValid: false,
      countsAsCertified: false,
    };
  }

  const technical = await fetchAndDecodeImage(url, opts);
  if (!technical.ok) {
    return {
      ...base,
      status: technical.status,
      certified: false,
      reason: technical.reason,
      technical,
      failureClass: "technical",
      countsAsPopulated: true,
      countsAsFetchValid: false,
      countsAsRenderValid: false,
      countsAsCertified: false,
    };
  }

  const visual = evaluateVisualSuitability(url, meta);
  const browser = opts.browserEvidence || null;

  if (browser && browser.ok === false) {
    return {
      ...base,
      status: IMAGE_INTEGRITY_STATUS.BROWSER_RENDER_FAILED,
      certified: false,
      reason: browser.reason || "browser_render_failed",
      technical,
      visual,
      browser,
      failureClass: "technical",
      countsAsPopulated: true,
      countsAsFetchValid: true,
      countsAsRenderValid: false,
      countsAsCertified: false,
    };
  }

  if (!visual.ok) {
    if (visual.reviewRequired) {
      return {
        ...base,
        status: IMAGE_INTEGRITY_STATUS.VISUAL_REVIEW_REQUIRED,
        certified: false,
        reason: visual.code,
        technical,
        visual,
        browser,
        failureClass: "content_quality",
        countsAsPopulated: true,
        countsAsFetchValid: true,
        countsAsRenderValid: Boolean(browser?.ok) || true, // technical decode implies renderable bytes
        countsAsCertified: false,
      };
    }
    return {
      ...base,
      status: IMAGE_INTEGRITY_STATUS.VISUAL_REVIEW_REQUIRED,
      certified: false,
      reason: visual.code,
      technical,
      visual,
      browser,
      failureClass: "content_quality",
      countsAsPopulated: true,
      countsAsFetchValid: true,
      countsAsRenderValid: true,
      countsAsCertified: false,
    };
  }

  // CERTIFIED requires browser render proof (or explicit allow for technical-only dry audits)
  if (browser?.ok === true || opts.allowTechnicalCertify === true) {
    return {
      ...base,
      status: IMAGE_INTEGRITY_STATUS.CERTIFIED,
      certified: true,
      certifiedAt: new Date().toISOString(),
      technical,
      visual,
      browser: browser || { ok: true, mode: "technical_only_authorized" },
      countsAsPopulated: true,
      countsAsFetchValid: true,
      countsAsRenderValid: true,
      countsAsCertified: true,
    };
  }

  return {
    ...base,
    status: IMAGE_INTEGRITY_STATUS.RENDER_VALID,
    certified: false,
    reason: "awaiting_browser_render_certification",
    technical,
    visual,
    browser: null,
    countsAsPopulated: true,
    countsAsFetchValid: true,
    countsAsRenderValid: true,
    countsAsCertified: false,
  };
}

/**
 * Score a brand's required image slots.
 * Certified Image Completion = certified / required (URL population does NOT raise this).
 */
export function scoreBrandImageIntegrity(slotResults = [], opts = {}) {
  const required = opts.requiredSlots || REQUIRED_IMAGE_SLOTS;
  const bySlot = new Map((slotResults || []).map((r) => [r.slotKey, r]));
  const rows = required.map((slotKey) => {
    const r = bySlot.get(slotKey);
    if (r) return r;
    return {
      slotKey,
      status: IMAGE_INTEGRITY_STATUS.MISSING,
      certified: false,
      countsAsPopulated: false,
      countsAsFetchValid: false,
      countsAsRenderValid: false,
      countsAsCertified: false,
    };
  });

  const totalRequired = rows.length;
  const populated = rows.filter((r) => r.countsAsPopulated).length;
  const fetchValid = rows.filter((r) => r.countsAsFetchValid).length;
  const renderValid = rows.filter((r) => r.countsAsRenderValid).length;
  const certified = rows.filter((r) => r.countsAsCertified || r.status === IMAGE_INTEGRITY_STATUS.CERTIFIED)
    .length;
  const missing = rows.filter((r) => r.status === IMAGE_INTEGRITY_STATUS.MISSING).length;
  const broken = rows.filter((r) =>
    [
      IMAGE_INTEGRITY_STATUS.FETCH_FAILED,
      IMAGE_INTEGRITY_STATUS.DECODE_FAILED,
      IMAGE_INTEGRITY_STATUS.BROWSER_RENDER_FAILED,
    ].includes(r.status)
  ).length;
  const visualReview = rows.filter(
    (r) => r.status === IMAGE_INTEGRITY_STATUS.VISUAL_REVIEW_REQUIRED
  ).length;
  const unverified = rows.filter(
    (r) => r.status === IMAGE_INTEGRITY_STATUS.URL_PRESENT_UNVERIFIED
  ).length;

  const certifiedCompletion = totalRequired ? certified / totalRequired : 0;
  const legacyUrlCompletion = totalRequired ? populated / totalRequired : 0;
  const threshold = opts.threshold ?? FACTORY_IMAGE_APPROVAL_THRESHOLD;
  const meetsApprovalThreshold = certifiedCompletion >= threshold - 1e-9;

  return {
    contract: BE_FACTORY_IMAGE_INTEGRITY_V1,
    totalRequiredImageSlots: totalRequired,
    populatedImageReferences: populated,
    fetchValidAssets: fetchValid,
    browserRenderValidAssets: renderValid,
    certifiedImages: certified,
    invalidBrokenImages: broken,
    visuallyUnsuitableOrReview: visualReview,
    missingImages: missing,
    urlPresentUnverified: unverified,
    certifiedImageCompletion: Number(certifiedCompletion.toFixed(4)),
    /** Legacy metric — disclosed for before/after; MUST NOT gate approval. */
    legacyUrlPopulationCompletion: Number(legacyUrlCompletion.toFixed(4)),
    legacyOverstatedCompletion:
      legacyUrlCompletion > certifiedCompletion + 1e-9
        ? Number((legacyUrlCompletion - certifiedCompletion).toFixed(4))
        : 0,
    meetsFactoryApprovalThreshold: meetsApprovalThreshold,
    approvalThreshold: threshold,
    slots: rows,
  };
}

/**
 * Factory gate: block approval when certified image coverage is below threshold.
 * Does not block unrelated brands.
 */
export function evaluateFactoryImageApprovalGate(score) {
  if (!score) {
    return {
      ok: false,
      blockApproval: true,
      reason: "missing_image_integrity_score",
    };
  }
  if (!score.meetsFactoryApprovalThreshold) {
    return {
      ok: false,
      blockApproval: true,
      reason: "certified_image_completion_below_threshold",
      certifiedImageCompletion: score.certifiedImageCompletion,
      threshold: score.approvalThreshold,
      certifiedImages: score.certifiedImages,
      totalRequiredImageSlots: score.totalRequiredImageSlots,
    };
  }
  return {
    ok: true,
    blockApproval: false,
    certifiedImageCompletion: score.certifiedImageCompletion,
  };
}

/** Assess gaps for orchestrator — only CERTIFIED slots count as complete. */
export function assessImageIntegrityGaps(score) {
  const gaps = [];
  if (!score) {
    gaps.push("image_integrity_score_missing");
    return gaps;
  }
  if (score.certifiedImages < score.totalRequiredImageSlots) {
    gaps.push(
      `certified_images_${score.certifiedImages}/${score.totalRequiredImageSlots}`
    );
  }
  if (score.invalidBrokenImages > 0) {
    gaps.push(`broken_images_${score.invalidBrokenImages}`);
  }
  if (score.missingImages > 0) {
    gaps.push(`missing_images_${score.missingImages}`);
  }
  if (score.visuallyUnsuitableOrReview > 0) {
    gaps.push(`visual_review_${score.visuallyUnsuitableOrReview}`);
  }
  if (score.legacyOverstatedCompletion > 0) {
    gaps.push(
      `legacy_url_overstated_by_${score.legacyOverstatedCompletion}`
    );
  }
  return gaps;
}

export function rejectionLedgerPath(repoRoot) {
  return join(
    repoRoot,
    "data/brand-explorer/factory-2-0/image-integrity/rejection-ledger-v1.json"
  );
}

export function loadRejectionLedger(repoRoot) {
  const abs = rejectionLedgerPath(repoRoot);
  if (!existsSync(abs)) {
    return {
      contract: BE_FACTORY_IMAGE_INTEGRITY_V1,
      version: 1,
      entries: {},
    };
  }
  try {
    return JSON.parse(readFileSync(abs, "utf8"));
  } catch {
    return {
      contract: BE_FACTORY_IMAGE_INTEGRITY_V1,
      version: 1,
      entries: {},
    };
  }
}

export function recordRejection(repoRoot, { url, reason, slotKey, brandId, slug }) {
  const ledger = loadRejectionLedger(repoRoot);
  const fp = urlFingerprint(url);
  const prev = ledger.entries[fp] || {
    url,
    urlFingerprint: fp,
    reasons: [],
    hits: 0,
  };
  prev.hits = (prev.hits || 0) + 1;
  prev.lastReason = reason;
  prev.lastSlotKey = slotKey || null;
  prev.lastBrandId = brandId || null;
  prev.lastSlug = slug || null;
  prev.lastAt = new Date().toISOString();
  if (reason && !prev.reasons.includes(reason)) prev.reasons.push(reason);
  ledger.entries[fp] = prev;
  ledger.updatedAt = new Date().toISOString();
  const abs = rejectionLedgerPath(repoRoot);
  mkdirSync(join(repoRoot, "data/brand-explorer/factory-2-0/image-integrity"), {
    recursive: true,
  });
  writeFileSync(abs, JSON.stringify(ledger, null, 2) + "\n");
  return prev;
}

export function isRejectedUrl(repoRoot, url) {
  const ledger = loadRejectionLedger(repoRoot);
  return Boolean(ledger.entries[urlFingerprint(url)]);
}

/**
 * Evaluate all required slots on a block list for one brand.
 */
export async function evaluateBrandBlocksImageIntegrity(blocks, ctx = {}, opts = {}) {
  const list = Array.isArray(blocks) ? blocks : [];
  const bySlot = new Map();
  for (const b of list) {
    const k = nz(b.slotKey || b.slot);
    if (!k) continue;
    if (!bySlot.has(k)) bySlot.set(k, b);
  }
  const slotResults = [];
  for (const slotKey of opts.requiredSlots || REQUIRED_IMAGE_SLOTS) {
    const block = bySlot.get(slotKey) || { slotKey };
    const browser = opts.browserBySlot?.[slotKey] || null;
    const prior = opts.priorBySlot?.[slotKey] || null;
    const result = await evaluateImageSlot(
      { ...block, slotKey },
      { ...ctx, slotKey },
      { ...opts, browserEvidence: browser, priorEvidence: prior }
    );
    slotResults.push(result);
    if (
      result.status === IMAGE_INTEGRITY_STATUS.FETCH_FAILED ||
      result.status === IMAGE_INTEGRITY_STATUS.DECODE_FAILED ||
      result.status === IMAGE_INTEGRITY_STATUS.BROWSER_RENDER_FAILED ||
      (result.status === IMAGE_INTEGRITY_STATUS.VISUAL_REVIEW_REQUIRED && result.url)
    ) {
      if (opts.recordRejections && result.url && ctx.repoRoot) {
        recordRejection(ctx.repoRoot, {
          url: result.url,
          reason: result.reason || result.status,
          slotKey,
          brandId: ctx.brandId,
          slug: ctx.slug,
        });
      }
    }
  }
  const score = scoreBrandImageIntegrity(slotResults, opts);
  const gate = evaluateFactoryImageApprovalGate(score);
  const gaps = assessImageIntegrityGaps(score);
  return { slotResults, score, gate, gaps };
}

/**
 * Aggregate portfolio dashboard section.
 */
export function buildImageIntegrityPortfolioSection(brandRows = []) {
  const rows = Array.isArray(brandRows) ? brandRows : [];
  const totalBrands = rows.length;
  const fullyCertified = rows.filter(
    (r) => r.score?.meetsFactoryApprovalThreshold === true
  ).length;
  const totalRequired = rows.reduce(
    (n, r) => n + (r.score?.totalRequiredImageSlots || REQUIRED_IMAGE_SLOTS.length),
    0
  );
  const certified = rows.reduce((n, r) => n + (r.score?.certifiedImages || 0), 0);
  const broken = rows.reduce((n, r) => n + (r.score?.invalidBrokenImages || 0), 0);
  const missing = rows.reduce((n, r) => n + (r.score?.missingImages || 0), 0);
  const visualReview = rows.reduce(
    (n, r) => n + (r.score?.visuallyUnsuitableOrReview || 0),
    0
  );
  const repairAttempts = rows.reduce((n, r) => n + (r.repair?.attempts || 0), 0);
  const repairSuccesses = rows.reduce((n, r) => n + (r.repair?.successes || 0), 0);
  const legacyPopulated = rows.reduce(
    (n, r) => n + (r.score?.populatedImageReferences || 0),
    0
  );

  return {
    contract: BE_FACTORY_IMAGE_INTEGRITY_V1,
    generatedAt: new Date().toISOString(),
    totalBrandsProcessed: totalBrands,
    brandsWithFullyCertifiedRequiredImagery: fullyCertified,
    totalRequiredImageSlots: totalRequired,
    certifiedImageCompletionPercentage: totalRequired
      ? Number(((100 * certified) / totalRequired).toFixed(2))
      : 0,
    /** Disclosed correction — URL population is NOT certified completion. */
    legacyUrlPopulationPercentage: totalRequired
      ? Number(((100 * legacyPopulated) / totalRequired).toFixed(2))
      : 0,
    brokenImageCount: broken,
    missingImageCount: missing,
    imagesRequiringVisualReview: visualReview,
    technicalRecoveryAttempts: repairAttempts,
    technicalRecoverySuccesses: repairSuccesses,
    technicalRecoverySuccessRate: repairAttempts
      ? Number((repairSuccesses / repairAttempts).toFixed(4))
      : null,
    remainingReplacementRequirements: Math.max(0, totalRequired - certified),
    note: "Only CERTIFIED images raise certifiedImageCompletionPercentage. URL presence alone does not.",
  };
}
