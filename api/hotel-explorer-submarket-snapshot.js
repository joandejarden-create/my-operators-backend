/**
 * GET /api/hotel-explorer/hotels/:hotelId/submarket-snapshot
 * Read-only HPC Submarket Snapshot for Hotel Explorer.
 * Never writes. Scout/Hotel Census are not required for selected-hotel inclusion.
 */

import { buildHpcSubmarketSnapshot } from "../lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js";

export async function getHotelExplorerSubmarketSnapshot(req, res) {
  res.setHeader("X-Dealality-Write", "forbidden");
  res.setHeader("Cache-Control", "private, max-age=30, stale-while-revalidate=30");

  const hotelId = String(req.params?.hotelId || "").trim();
  if (!/^rec[A-Za-z0-9]{14}$/.test(hotelId)) {
    return res.status(400).json({
      success: false,
      ok: false,
      error: "invalid_hotel_id",
      writeOnGet: false,
    });
  }

  try {
    const payload = await buildHpcSubmarketSnapshot({ hotelId });
    if (!payload.ok) {
      const status =
        payload.error === "hpc_selected_hotel_not_found"
          ? 404
          : payload.error === "airtable_not_configured"
            ? 503
            : 502;
      return res.status(status).json({
        success: false,
        ...payload,
      });
    }
    return res.json({
      success: true,
      ...payload,
    });
  } catch (err) {
    console.error("[he-submarket-snapshot]", err);
    return res.status(500).json({
      success: false,
      ok: false,
      error: "submarket_snapshot_failed",
      message: String(err?.message || err).slice(0, 240),
      writeOnGet: false,
      applyAuthorized: false,
    });
  }
}
