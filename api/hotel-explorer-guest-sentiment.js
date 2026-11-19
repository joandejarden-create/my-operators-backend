/**
 * GET /api/hotel-explorer/hotels/:hotelId/guest-sentiment
 * Read-only. Never writes Airtable. Never calls paid review APIs.
 */
import {
  resolveGuestSentimentForHotel,
} from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-staged-store-v1.js";
import { GUEST_SENTIMENT_MODEL_V1 } from "../lib/hotel-intelligence/guest-sentiment/guest-sentiment-model-v1.js";

/**
 * Express handler — GET only, no provider calls.
 */
export async function getHotelExplorerGuestSentiment(req, res) {
  const hotelId = String(req.params.hotelId || "").trim();
  if (!/^rec[a-zA-Z0-9]{14}$/.test(hotelId)) {
    return res.status(400).json({
      success: false,
      error: "invalid_hotel_id",
      version: GUEST_SENTIMENT_MODEL_V1,
    });
  }

  const hotelName = String(req.query.name || req.query.hotelName || "").trim() || null;
  const modeRaw = String(req.query.mode || "live").trim().toLowerCase();
  const founderPreview =
    modeRaw === "proposed" ||
    modeRaw === "phase3" ||
    String(req.query.founderPreview || "") === "1";

  res.setHeader("X-Dealality-Write", "forbidden");
  res.setHeader("X-Dealality-Paid-Providers", "forbidden");
  res.setHeader("Cache-Control", "private, max-age=60");

  try {
    const view = resolveGuestSentimentForHotel(
      { id: hotelId, name: hotelName },
      { founderPreview }
    );

    return res.json({
      success: true,
      available: view.visible === true,
      version: GUEST_SENTIMENT_MODEL_V1,
      hotelId,
      hotelName: view.hotelName,
      visible: view.visible === true,
      proposedPreview: view.proposedPreview === true,
      platforms: view.platforms || [],
      themes: view.themes || [],
      insights: view.insights || null,
      trends: view.trends || { available: false, reason: "insufficient_history" },
      limitations: view.limitations || [],
      rightsChip: view.rightsChip || null,
      canonicalSource: view.canonicalSource || null,
      observedAt: view.observedAt || null,
      stagedPacketPath: view.stagedPacketPath || null,
      applyAuthorized: false,
    });
  } catch (err) {
    console.warn("[he-guest-sentiment] handler error", err?.message || err);
    res.setHeader("X-Dealality-Write", "forbidden");
    return res.status(200).json({
      success: true,
      available: false,
      reason: "handler_error",
      version: GUEST_SENTIMENT_MODEL_V1,
      visible: false,
      platforms: [],
      themes: [],
      insights: null,
      trends: { available: false, reason: "handler_error" },
      limitations: [],
      applyAuthorized: false,
    });
  }
}
