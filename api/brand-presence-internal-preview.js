/**
 * GET /api/brand-presence/internal-preview-hotels
 *
 * Authorized internal Radar only — admin / local Dealality admin.
 * Returns allowlisted Hotel Property Census hotels (e.g. Bethesda) for map merge.
 * Fail-closed: unauthenticated / non-admin → 401/403. Query flags alone never authorize.
 */

import {
  assertInternalPreviewQueryDoesNotAuthorize,
  isAuthorizedForRadarInternalPreviewHotels,
  loadAuthorizedInternalPreviewHotels,
  RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

export async function getBrandPresenceInternalPreviewHotels(req, res) {
  const queryGate = assertInternalPreviewQueryDoesNotAuthorize(req.query || {});
  if (
    queryGate.queryRequestsInternalPreview &&
    !isAuthorizedForRadarInternalPreviewHotels(req.dealalityUser, req)
  ) {
    return res.status(403).json({
      success: false,
      ok: false,
      error: "forbidden_admin",
      message:
        "internalPreview query flags do not authorize restricted hotel inclusion.",
      authorizedByQueryAlone: false,
    });
  }

  if (!isAuthorizedForRadarInternalPreviewHotels(req.dealalityUser, req)) {
    return res.status(403).json({
      success: false,
      ok: false,
      error: "forbidden_admin",
      message: "Admin or local Dealality admin access required for internal-preview hotels.",
    });
  }

  const loaded = await loadAuthorizedInternalPreviewHotels({
    dealalityUser: req.dealalityUser,
    req,
  });

  if (loaded.unauthorized) {
    return res.status(403).json({
      success: false,
      ok: false,
      error: "forbidden_admin",
    });
  }

  return res.json({
    success: true,
    ok: true,
    version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
    internalPreviewOnly: true,
    customerVisible: false,
    shareVisible: false,
    sourceTable: loaded.sourceTable,
    hotels: loaded.hotels || [],
    errors: loaded.errors || [],
    note: "Merge into Radar map for authorized internal preview only. Not a customer share pack.",
  });
}
