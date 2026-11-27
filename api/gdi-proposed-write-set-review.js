/**
 * Admin/founder read-only review of locked proposed write-sets.
 * GET /api/admin/group-demand-intelligence/hotels/:hotelId/proposed-write-set-review
 *
 * NO_STAGED_ACTIVATE · NO_CURRENT_PUBLISH · NO_AIRTABLE_WRITES
 */

import {
  buildWyndhamProposedWriteSetFounderReview,
  GDI_PROPOSED_WRITE_SET_FOUNDER_REVIEW_V1,
} from "../lib/group-demand-intelligence/publication-package/gdi-proposed-write-set-founder-review-v1.js";

const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";

export async function getGdiProposedWriteSetReview(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotelId_required" });
    }

    if (hotelId !== WYNDHAM_HOTEL_ID) {
      return res.status(404).json({
        ok: false,
        error: "no_locked_proposed_write_set",
        message:
          "No locked proposed write-set review artifact is registered for this hotel. Wyndham San Diego Bayside is currently supported.",
        hotelId,
        contract: GDI_PROPOSED_WRITE_SET_FOUNDER_REVIEW_V1,
      });
    }

    const review = buildWyndhamProposedWriteSetFounderReview({});
    if (!review.ok) {
      return res.status(404).json(review);
    }

    res.setHeader("X-GDI-Surface", "PROPOSED_WRITE_SET_FOUNDER_REVIEW_V1");
    res.setHeader("X-Robots-Tag", "noindex, nofollow, noarchive");
    return res.json({
      ...review,
      experimentalLabel: "EXPERIMENTAL / PILOT — FOUNDER REVIEW ONLY",
    });
  } catch (err) {
    console.error("[gdi-proposed-write-set-review]", err);
    return res.status(500).json({
      ok: false,
      error: "proposed_write_set_review_failed",
      message: err?.message || String(err),
    });
  }
}
