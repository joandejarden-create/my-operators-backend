/**
 * GET /api/brand-presence/internal-preview-hotels
 *
 * Authorized internal Radar only — authenticated Radar app principals
 * (Owner / Operator / Brand / Admin workspace, plus admin allowlists).
 * Returns Hotel Property Census hotels with Radar Status Open|Pipeline for map merge
 * (dynamic formula query — not a hotel allowlist).
 * Fail-closed: unauthenticated / no Radar WA → 401/403. Query flags alone never authorize.
 * Diagnostics are included only for authorized callers; never for customer/share surfaces.
 */

import {
  assertInternalPreviewQueryDoesNotAuthorize,
  isAuthorizedForRadarInternalPreviewHotels,
  loadAuthorizedInternalPreviewHotels,
  RADAR_HPC_STATUS_CACHE_POLICY,
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
      error: "forbidden_radar_workspace",
      message:
        "internalPreview query flags do not authorize restricted hotel inclusion.",
      authorizedByQueryAlone: false,
    });
  }

  if (!isAuthorizedForRadarInternalPreviewHotels(req.dealalityUser, req)) {
    return res.status(403).json({
      success: false,
      ok: false,
      error: "forbidden_radar_workspace",
      message:
        "Authenticated Radar workspace access (Owner, Operator, Brand, or Admin) is required for HPC Open/Pipeline inventory.",
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
      error: "forbidden_radar_workspace",
    });
  }

  const ttlSec = Math.max(
    0,
    Math.floor((loaded.diagnostics?.cacheTtlMs || RADAR_HPC_STATUS_CACHE_POLICY.ttlMsDefault) / 1000)
  );
  res.setHeader(
    "Cache-Control",
    `private, max-age=${ttlSec}, stale-while-revalidate=${Math.min(30, ttlSec || 0)}`
  );

  const ok = Boolean(loaded.ok) && !(loaded.errors && loaded.errors.length);
  // Fail closed: retrieval errors must not look like a successful empty inventory.
  if (!ok) {
    return res.status(502).json({
      success: false,
      ok: false,
      error: "hpc_retrieval_failed",
      version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
      internalPreviewOnly: true,
      customerVisible: false,
      shareVisible: false,
      sourceTable: loaded.sourceTable || null,
      hotels: [],
      errors: loaded.errors || [{ reason: "hpc_retrieval_failed" }],
      diagnostics: loaded.diagnostics || null,
      cachePolicy: loaded.cachePolicy || RADAR_HPC_STATUS_CACHE_POLICY,
      note: "HPC Radar Status retrieval failed. Do not treat empty hotels as an authoritative inventory.",
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
    hotelsMapEligible: loaded.hotelsMapEligible || [],
    errors: [],
    diagnostics: loaded.diagnostics || null,
    cachePolicy: loaded.cachePolicy || RADAR_HPC_STATUS_CACHE_POLICY,
    note: "Merge into Radar for authorized internal preview only. hotels includes Open|Pipeline search/Explorer rows (coords optional); hotelsMapEligible is marker-ready. Not a customer share pack.",
  });
}
