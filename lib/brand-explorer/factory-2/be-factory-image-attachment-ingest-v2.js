/**
 * Durable Brand Explorer Presentation Image ingest V2.
 *
 * Never treat PATCH Image:[{url}] alone as success. Preferred path:
 * validate source → download bytes → decode → content-API upload → independent read-back.
 *
 * Contract: BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2
 */
import Airtable from "airtable";
import { createHash } from "crypto";
import { uploadFileBytesToAirtable } from "../../dealality/airtable-upload-attachment.js";
import {
  BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
  IMAGE_SLOT_CLASS,
  IMAGE_LIFECYCLE,
  decodeImageDimensions,
  isAirtableSignedCdnUrl,
  classifyLiveAttachment,
} from "./be-factory-permanent-image-integrity-v2.js";

export const BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2 =
  "BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2";

const UA =
  "DealalityBrandExplorerBot/1.0 (+https://dealality.com; image ingest v2; $0)";
const FETCH_TIMEOUT_MS = 15000;
const MAX_BYTES = 5 * 1024 * 1024;
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const IMAGE_FIELD = "Image";

const PERMANENT_HTTP = new Set([403, 404, 410]);

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function airtableBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
  return { key, baseId, base: new Airtable({ apiKey: key }).base(baseId) };
}

export async function fetchImageBytes(sourceUrl, { timeoutMs = FETCH_TIMEOUT_MS } = {}) {
  const url = nz(sourceUrl);
  if (!/^https?:\/\//i.test(url)) {
    return {
      ok: false,
      classification: IMAGE_SLOT_CLASS.SOURCE_URL_INVALID,
      reason: "invalid_url",
    };
  }
  if (isAirtableSignedCdnUrl(url)) {
    return {
      ok: false,
      classification: IMAGE_SLOT_CLASS.SOURCE_URL_INVALID,
      reason: "ephemeral_airtable_cdn_not_durable_source",
    };
  }
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: { "User-Agent": UA, Accept: "image/jpeg,image/png,image/*,*/*" },
    });
    clearTimeout(t);
    if (!res.ok) {
      return {
        ok: false,
        classification: PERMANENT_HTTP.has(res.status)
          ? IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED
          : IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED,
        reason: `http_${res.status}`,
        httpStatus: res.status,
        permanent: PERMANENT_HTTP.has(res.status),
        finalUrl: res.url,
      };
    }
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > MAX_BYTES) {
      return {
        ok: false,
        classification: IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED,
        reason: "file_too_large_for_airtable_upload",
        bytes: buf.length,
      };
    }
    const ct = String(res.headers.get("content-type") || "");
    const decoded = decodeImageDimensions(buf);
    let format = decoded.format || "unknown";
    if (buf[0] === 0xff && buf[1] === 0xd8) format = "jpeg";
    else if (buf[0] === 0x89 && buf[1] === 0x50) format = "png";
    else if (buf.slice(4, 8).toString() === "ftyp") format = "avif/heif";
    else if (/^<!DOCTYPE|^<html/i.test(buf.slice(0, 32).toString())) format = "html";

    if (format === "avif/heif" || format === "html" || format === "unknown" || !decoded.ok) {
      return {
        ok: false,
        classification:
          format === "avif/heif"
            ? IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED
            : IMAGE_SLOT_CLASS.IMAGE_CORRUPT,
        reason: decoded.reason || `unsupported_format_${format}`,
        contentType: ct,
        format,
        bytes: buf.length,
        httpStatus: res.status,
        finalUrl: res.url,
      };
    }
    if (!/^image\/(jpeg|jpg|png)$/i.test(ct) && format !== "jpeg" && format !== "png") {
      return {
        ok: false,
        classification: IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED,
        reason: `content_type_${ct || "missing"}`,
        contentType: ct,
        format,
        bytes: buf.length,
      };
    }
    return {
      ok: true,
      buffer: buf,
      contentType: format === "png" ? "image/png" : "image/jpeg",
      format,
      width: decoded.width,
      height: decoded.height,
      bytes: buf.length,
      byteHash: createHash("sha256").update(buf).digest("hex"),
      finalUrl: res.url,
      httpStatus: res.status,
      lifecycle: IMAGE_LIFECYCLE.IMAGE_DISCOVERED,
    };
  } catch (e) {
    return {
      ok: false,
      classification: IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED,
      reason: String(e.message || e).slice(0, 200),
    };
  }
}

function filenameForSlot(slotKey, format) {
  const ext = format === "png" ? "png" : "jpg";
  return `${String(slotKey || "image").replace(/\./g, "-")}-${Date.now().toString(36)}.${ext}`.slice(
    0,
    100
  );
}

export async function readPresentationImage(recordId) {
  const { base } = airtableBase();
  const rec = await base(PRESENTATION_TABLE).find(recordId);
  const img = rec.get(IMAGE_FIELD);
  const att = Array.isArray(img) && img[0] ? img[0] : null;
  return {
    recordId,
    slotKey: nz(rec.get("Slot Key")),
    title: nz(rec.get("Title")),
    attachmentCount: Array.isArray(img) ? img.length : 0,
    attachment: att
      ? {
          id: att.id || null,
          url: att.url || null,
          filename: att.filename || null,
          type: att.type || null,
          size: att.size || null,
          width: att.width || null,
          height: att.height || null,
        }
      : null,
  };
}

export async function probeAttachmentUrl(url) {
  if (!url) return { ok: false, reason: "no_url" };
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: { "User-Agent": UA, Accept: "image/*,*/*" },
    });
    clearTimeout(t);
    const buf = Buffer.from(await res.arrayBuffer());
    const decoded = decodeImageDimensions(buf);
    let format = decoded.format || "unknown";
    if (buf[0] === 0xff && buf[1] === 0xd8) format = "jpeg";
    else if (buf[0] === 0x89 && buf[1] === 0x50) format = "png";
    else if (buf.slice(4, 8).toString() === "ftyp") format = "avif/heif";
    else if (/^<!DOCTYPE|^<html/i.test(buf.slice(0, 32).toString())) format = "html";
    return {
      ok: res.ok && decoded.ok && (format === "jpeg" || format === "png"),
      httpStatus: res.status,
      contentType: res.headers.get("content-type"),
      bytes: buf.length,
      format,
      decodeOk: decoded.ok,
      width: decoded.width,
      height: decoded.height,
      finalUrl: res.url,
    };
  } catch (e) {
    return { ok: false, reason: String(e.message || e).slice(0, 200) };
  }
}

/**
 * Decode + validate a local image buffer (JPEG/PNG only).
 */
export function validateImageBuffer(buffer, { sourceUrl = null } = {}) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 24) {
    return { ok: false, classification: IMAGE_SLOT_CLASS.IMAGE_CORRUPT, reason: "empty_buffer" };
  }
  if (buffer.length > MAX_BYTES) {
    return {
      ok: false,
      classification: IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED,
      reason: "file_too_large_for_airtable_upload",
      bytes: buffer.length,
    };
  }
  const decoded = decodeImageDimensions(buffer);
  let format = decoded.format || "unknown";
  if (buffer[0] === 0xff && buffer[1] === 0xd8) format = "jpeg";
  else if (buffer[0] === 0x89 && buffer[1] === 0x50) format = "png";
  else if (buffer.slice(4, 8).toString() === "ftyp") format = "avif/heif";
  else if (/^<!DOCTYPE|^<html/i.test(buffer.slice(0, 32).toString())) format = "html";
  else if (buffer[0] === 0x52 && buffer[1] === 0x49 && buffer[8] === 0x57) format = "webp";

  if (format === "webp" || format === "avif/heif" || format === "html" || format === "unknown" || !decoded.ok) {
    return {
      ok: false,
      classification:
        format === "webp" || format === "avif/heif"
          ? IMAGE_SLOT_CLASS.FORMAT_UNSUPPORTED
          : IMAGE_SLOT_CLASS.IMAGE_CORRUPT,
      reason: decoded.reason || `unsupported_format_${format}`,
      format,
      bytes: buffer.length,
    };
  }
  return {
    ok: true,
    buffer,
    contentType: format === "png" ? "image/png" : "image/jpeg",
    format,
    width: decoded.width,
    height: decoded.height,
    bytes: buffer.length,
    byteHash: createHash("sha256").update(buffer).digest("hex"),
    finalUrl: sourceUrl,
    httpStatus: sourceUrl ? 200 : null,
    lifecycle: IMAGE_LIFECYCLE.IMAGE_DISCOVERED,
  };
}

/**
 * Ingest already-validated image bytes via content API + independent read-back.
 * Prefer this for local verified JPEG/PNG thumbs (e.g. Bunkhouse wave12 thumbs).
 */
export async function ingestPresentationImageFromBytes({
  recordId,
  buffer,
  slotKey = "",
  sourceUrl = null,
  dryRun = true,
  maxReadbackAttempts = 4,
  readbackDelayMs = 700,
  replaceExisting = true,
} = {}) {
  const contract = BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2;
  const fetched = validateImageBuffer(buffer, { sourceUrl });
  if (!fetched.ok) {
    return {
      contract,
      ok: false,
      dryRun,
      recordId,
      sourceUrl,
      lifecycle: IMAGE_LIFECYCLE.IMAGE_DISCOVERED,
      classification: fetched.classification || IMAGE_SLOT_CLASS.IMAGE_CORRUPT,
      fetch: fetched,
    };
  }

  if (dryRun) {
    return {
      contract,
      ok: true,
      dryRun: true,
      recordId,
      sourceUrl,
      lifecycle: IMAGE_LIFECYCLE.IMAGE_DISCOVERED,
      classification: IMAGE_SLOT_CLASS.RIGHTS_UNVERIFIED,
      fetch: {
        ok: true,
        bytes: fetched.bytes,
        format: fetched.format,
        width: fetched.width,
        height: fetched.height,
        byteHash: fetched.byteHash,
        finalUrl: fetched.finalUrl,
      },
      note: "Dry-run only — bytes validated; no Airtable upload",
    };
  }

  const { base, baseId, key } = airtableBase();
  const filename = filenameForSlot(slotKey || recordId, fetched.format);

  try {
    // content.airtable.com uploadAttachment APPENDs. Clear first so read-back
    // sees the newly imported JPEG/PNG (not a prior webp/PDF at index 0).
    if (replaceExisting) {
      await base(PRESENTATION_TABLE).update(recordId, { [IMAGE_FIELD]: [] });
      await new Promise((r) => setTimeout(r, 400));
    }
    await uploadFileBytesToAirtable({
      baseId,
      recordId,
      fieldName: IMAGE_FIELD,
      buffer: fetched.buffer,
      contentType: fetched.contentType,
      filename,
      apiKey: key,
    });
  } catch (e) {
    return {
      contract,
      ok: false,
      dryRun: false,
      recordId,
      sourceUrl,
      lifecycle: IMAGE_LIFECYCLE.IMAGE_STAGED,
      classification: IMAGE_SLOT_CLASS.AIRTABLE_IMPORT_FAILED,
      reason: String(e.message || e).slice(0, 300),
      fetch: { ok: true, bytes: fetched.bytes, byteHash: fetched.byteHash },
    };
  }

  let lastRead = null;
  let lastProbe = null;
  for (let attempt = 1; attempt <= maxReadbackAttempts; attempt++) {
    await new Promise((r) => setTimeout(r, readbackDelayMs * attempt));
    lastRead = await readPresentationImage(recordId);
    if (!lastRead.attachment?.url) continue;
    lastProbe = await probeAttachmentUrl(lastRead.attachment.url);
    if (lastProbe.ok) {
      const classification = classifyLiveAttachment({
        attachment: lastRead.attachment,
        probe: { ...lastProbe, browserOk: false },
      });
      return {
        contract,
        ok: true,
        dryRun: false,
        recordId,
        sourceUrl,
        lifecycle: IMAGE_LIFECYCLE.IMAGE_READBACK_VERIFIED,
        classification:
          classification === IMAGE_SLOT_CLASS.ATTACHMENT_PRESENT_UNVERIFIED
            ? IMAGE_SLOT_CLASS.VALIDATED
            : classification,
        status: IMAGE_LIFECYCLE.IMAGE_READBACK_VERIFIED,
        attachment: lastRead.attachment,
        probe: lastProbe,
        fetch: {
          bytes: fetched.bytes,
          byteHash: fetched.byteHash,
          format: fetched.format,
          width: fetched.width,
          height: fetched.height,
        },
        readbackAttempts: attempt,
        parentContract: BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2,
      };
    }
  }

  return {
    contract,
    ok: false,
    dryRun: false,
    recordId,
    sourceUrl,
    lifecycle: IMAGE_LIFECYCLE.IMAGE_ATTACHMENT_IMPORTED,
    classification: lastRead?.attachment
      ? IMAGE_SLOT_CLASS.ATTACHMENT_RENDER_FAILED
      : IMAGE_SLOT_CLASS.AIRTABLE_IMPORT_FAILED,
    reason: "readback_failed_after_upload",
    attachment: lastRead?.attachment || null,
    probe: lastProbe,
    fetch: { bytes: fetched.bytes, byteHash: fetched.byteHash },
    readbackAttempts: maxReadbackAttempts,
  };
}

/**
 * Ingest durable source into Presentation.Image via content API + independent read-back.
 */
export async function ingestPresentationImageFromSource({
  recordId,
  sourceUrl,
  slotKey = "",
  dryRun = true,
  maxReadbackAttempts = 4,
  readbackDelayMs = 700,
  replaceExisting = true,
} = {}) {
  const contract = BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2;
  const fetched = await fetchImageBytes(sourceUrl);
  if (!fetched.ok) {
    return {
      contract,
      ok: false,
      dryRun,
      recordId,
      sourceUrl,
      lifecycle: IMAGE_LIFECYCLE.IMAGE_DISCOVERED,
      classification: fetched.classification || IMAGE_SLOT_CLASS.SOURCE_FETCH_FAILED,
      fetch: fetched,
    };
  }

  return ingestPresentationImageFromBytes({
    recordId,
    buffer: fetched.buffer,
    slotKey,
    sourceUrl: fetched.finalUrl || sourceUrl,
    dryRun,
    maxReadbackAttempts,
    readbackDelayMs,
    replaceExisting,
  });
}

/** Regression helper: URL-only PATCH often "succeeds" then evaporates. */
export async function demonstrateUrlOnlyPatchFailure({
  recordId,
  sourceUrl,
  dryRun = true,
} = {}) {
  if (dryRun) {
    return {
      contract: BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2,
      mode: "dry-run",
      note: "Would PATCH Image:[{url}] then read back — use --apply for live proof on a sandbox/pilot record",
      recordId,
      sourceUrl,
    };
  }
  const { base } = airtableBase();
  await base(PRESENTATION_TABLE).update(recordId, {
    [IMAGE_FIELD]: [{ url: sourceUrl }],
  });
  await new Promise((r) => setTimeout(r, 1500));
  const immediate = await readPresentationImage(recordId);
  await new Promise((r) => setTimeout(r, 4000));
  const delayed = await readPresentationImage(recordId);
  return {
    contract: BE_FACTORY_IMAGE_ATTACHMENT_INGEST_V2,
    mode: "apply",
    recordId,
    sourceUrl,
    immediateHasAttachment: Boolean(immediate.attachment?.url),
    delayedHasAttachment: Boolean(delayed.attachment?.url),
    evaporated: Boolean(immediate.attachment?.url) && !delayed.attachment?.url,
    immediate,
    delayed,
  };
}
