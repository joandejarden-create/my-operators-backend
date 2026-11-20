/**
 * Fresh Brand Explorer Presentation Image delivery.
 *
 * Airtable attachment URLs are temporary signed CDN links. Clients should prefer
 * this allowlisted endpoint (by Presentation record id) over caching signed URLs.
 *
 * Contract: BE_PRESENTATION_IMAGE_DELIVERY_V1
 * No arbitrary-URL proxy — record id only; Image field only.
 */
import Airtable from "airtable";

export const BE_PRESENTATION_IMAGE_DELIVERY_V1 = "BE_PRESENTATION_IMAGE_DELIVERY_V1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MAX_BYTES = 8 * 1024 * 1024;
const UA = "DealalityBrandExplorerBot/1.0 (+https://dealality.com; presentation-image delivery)";

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

/**
 * GET /api/brand-explorer/presentation-image/:recordId
 * Streams the first Image attachment bytes after a fresh Airtable metadata read.
 */
export async function getBrandExplorerPresentationImage(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!/^rec[a-zA-Z0-9]{10,}$/.test(recordId)) {
      return res.status(400).json({ error: "invalid_record_id" });
    }

    const { apiKey, baseId } = airtableEnv();
    const base = new Airtable({ apiKey }).base(baseId);
    const rec = await base(PRESENTATION_TABLE).find(recordId);
    const active = rec.get("Active");
    if (active === false) {
      return res.status(404).json({ error: "inactive_presentation_row" });
    }
    const img = rec.get("Image");
    const att = Array.isArray(img) && img[0] ? img[0] : null;
    if (!att?.url) {
      return res.status(404).json({ error: "attachment_missing" });
    }

    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 15000);
    const upstream = await fetch(att.url, {
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
    res.setHeader("X-Dealality-Image-Contract", BE_PRESENTATION_IMAGE_DELIVERY_V1);
    res.setHeader("X-Dealality-Presentation-Record", recordId);
    if (att.id) res.setHeader("X-Dealality-Attachment-Id", String(att.id));
    if (att.filename) {
      res.setHeader(
        "Content-Disposition",
        `inline; filename="${String(att.filename).replace(/"/g, "")}"`
      );
    }
    return res.status(200).send(buf);
  } catch (e) {
    const status = e.status || 500;
    return res.status(status).json({
      error: "presentation_image_delivery_failed",
      message: String(e.message || e).slice(0, 300),
    });
  }
}
