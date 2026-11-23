/**
 * Fresh Brand Basics Logo delivery for DealSignal / Brand Library clients.
 *
 * Airtable attachment URLs are temporary signed CDN links. Clients should prefer
 * this allowlisted endpoint (by Brand Basics record id) over caching signed URLs.
 *
 * Contract: BRAND_LIBRARY_LOGO_DELIVERY_V1
 * No arbitrary-URL proxy — Brand Basics record id only; Logo field only.
 */
import Airtable from "airtable";

export const BRAND_LIBRARY_LOGO_DELIVERY_V1 = "BRAND_LIBRARY_LOGO_DELIVERY_V1";

const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const LOGO_FIELD = "Logo";
const MAX_BYTES = 4 * 1024 * 1024;
const UA = "DealalityBrandLibraryBot/1.0 (+https://dealality.com; brand-logo delivery)";

function airtableEnv() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
    err.status = 500;
    throw err;
  }
  return { apiKey, baseId };
}

function attachmentUrl(att) {
  if (!att || typeof att !== "object") return "";
  const url =
    att.url ||
    (att.thumbnails && att.thumbnails.large && att.thumbnails.large.url) ||
    (att.thumbnails && att.thumbnails.small && att.thumbnails.small.url) ||
    "";
  const u = String(url || "").trim();
  if (!u) return "";
  return u.startsWith("//") ? `https:${u}` : u;
}

/**
 * GET /api/brand-library/logo/:recordId
 * Streams the first Logo attachment bytes after a fresh Airtable metadata read.
 */
export async function getBrandLibraryLogo(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!/^rec[a-zA-Z0-9]{10,}$/.test(recordId)) {
      return res.status(400).json({ error: "invalid_record_id" });
    }

    const { apiKey, baseId } = airtableEnv();
    const base = new Airtable({ apiKey }).base(baseId);
    const rec = await base(BRAND_BASICS_TABLE).find(recordId);
    const logo = rec.get(LOGO_FIELD);
    const att = Array.isArray(logo) && logo[0] ? logo[0] : null;
    const url = attachmentUrl(att);
    if (!url) {
      return res.status(404).json({ error: "logo_attachment_missing" });
    }

    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 15000);
    const upstream = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: { "User-Agent": UA, Accept: "image/*,*/*" },
    });
    clearTimeout(timer);

    if (!upstream.ok) {
      return res.status(502).json({
        error: "upstream_fetch_failed",
        httpStatus: upstream.status,
        hint: upstream.status === 410 ? "signed_url_expired_retry" : undefined,
      });
    }

    const buf = Buffer.from(await upstream.arrayBuffer());
    if (buf.length > MAX_BYTES) {
      return res.status(413).json({ error: "image_too_large" });
    }

    const ct =
      att.type ||
      upstream.headers.get("content-type") ||
      (buf[0] === 0xff && buf[1] === 0xd8
        ? "image/jpeg"
        : buf[0] === 0x89 && buf[1] === 0x50
          ? "image/png"
          : "application/octet-stream");

    res.setHeader("Content-Type", ct);
    res.setHeader("Cache-Control", "private, max-age=300, must-revalidate");
    res.setHeader("X-Dealality-Image-Contract", BRAND_LIBRARY_LOGO_DELIVERY_V1);
    res.setHeader("X-Dealality-Brand-Record", recordId);
    if (att.id) res.setHeader("X-Dealality-Attachment-Id", String(att.id));
    if (att.filename) {
      res.setHeader(
        "Content-Disposition",
        `inline; filename="${String(att.filename).replace(/"/g, "")}"`,
      );
    }
    return res.status(200).send(buf);
  } catch (e) {
    const msg = String(e.message || e);
    if (/NOT_FOUND|Could not find/i.test(msg)) {
      return res.status(404).json({ error: "brand_record_not_found" });
    }
    const status = e.status || 500;
    return res.status(status).json({
      error: "brand_library_logo_delivery_failed",
      message: msg.slice(0, 300),
    });
  }
}
