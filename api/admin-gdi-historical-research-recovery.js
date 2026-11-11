/**
 * Admin-only Bethesda historical research recovery visibility.
 * Read-only. Never writes opportunities.json or share registries.
 */

import {
  BETHESDA_CANONICAL_HOTEL_ID,
  buildBethesdaHistoricalResearchVisibility,
  isBethesdaHistoricalVisibilityHotel,
} from "../lib/group-demand-intelligence/research/bethesda-historical-research-visibility-v1.js";

export async function getAdminGdiHistoricalResearchRecovery(req, res) {
  try {
    const hotelId = String(req.params?.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({
        ok: false,
        success: false,
        error: "hotel_id_required",
      });
    }
    if (!isBethesdaHistoricalVisibilityHotel(hotelId)) {
      return res.status(404).json({
        ok: false,
        success: false,
        error: "historical_research_recovery_not_available",
        message:
          "Historical research recovery visibility is currently available for Bethesda Marriott only.",
        hotelId,
        supportedHotelId: BETHESDA_CANONICAL_HOTEL_ID,
      });
    }

    const payload = buildBethesdaHistoricalResearchVisibility();
    return res.status(200).json({
      ok: true,
      success: true,
      hotelId: payload.hotelId,
      hotelName: payload.hotelName,
      version: payload.version,
      counts: payload.counts,
      expected: payload.expected,
      labels: payload.labels,
      safety: payload.safety,
      opportunityStore: payload.opportunityStore,
      groups: payload.groups,
      attestations: payload.attestations,
      researchContinuation: payload.researchContinuation,
      attestedIds: payload.attestedIds,
      generatedAt: payload.generatedAt,
      hotelStoreUntouched: true,
      customerPublication: false,
    });
  } catch (err) {
    console.error(
      "[admin-gdi-historical-research-recovery]",
      err?.message || err
    );
    return res.status(500).json({
      ok: false,
      success: false,
      error: "historical_research_recovery_failed",
      message: "Unable to load historical research recovery view.",
    });
  }
}
