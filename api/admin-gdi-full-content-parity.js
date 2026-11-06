/**
 * Admin GDI Full Content Parity API — inventory + on-demand verify.
 * Does not publish, rotate shares, or write Airtable.
 */
import {
  buildGdiParityHotelInventory,
} from "../lib/group-demand-intelligence/presentation/gdi-portfolio-full-content-parity-v1.js";
import { verifyGdiHotelFullContentParity } from "../lib/group-demand-intelligence/presentation/gdi-hotel-full-content-parity-verify-v1.js";
import {
  getHotelParityStatus,
  recordHotelParityStatus,
  loadGdiParityStatusStore,
} from "../lib/group-demand-intelligence/presentation/gdi-full-content-parity-status-store-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/index.js";
import { GDI_FULL_CONTENT_PARITY_STATUS } from "../lib/group-demand-intelligence/contracts/gdi-canonical-customer-presentation-contract-v1.js";

function mergeParityOntoCatalogRow(hotelId, base = {}) {
  const stored = getHotelParityStatus(hotelId) || {};
  const inventoryHint = base.parityInventory || {};
  const status =
    stored.status ||
    inventoryHint.fullContentParityStatus ||
    GDI_FULL_CONTENT_PARITY_STATUS.VERIFICATION_PENDING;
  return {
    fullContentParityStatus: status,
    publishedOpportunityCount:
      stored.publishedOpportunityCount ??
      inventoryHint.publishedOpportunityCount ??
      null,
    publicationVersion:
      stored.publicationVersion || inventoryHint.publicationVersion || null,
    publicationId: stored.publicationId || inventoryHint.publicationId || null,
    lastParityVerifiedAt: stored.verifiedAt || null,
    parityCriticalDifferenceCount: stored.criticalDifferenceCount || 0,
    parityDifferences: stored.differences || [],
    parityMigrationCandidate: inventoryHint.migrationCandidate || null,
  };
}

/**
 * GET inventory summary for all discovered GDI hotels (read-only).
 */
export async function getAdminGdiFullContentParityInventory(req, res) {
  try {
    const hotels = listGdiSelectableHotels?.() || [];
    const inventory = buildGdiParityHotelInventory({ hotels });
    const store = loadGdiParityStatusStore();
    const rows = inventory.map((row) => {
      const stored = store.hotels?.[row.hotelId];
      return {
        ...row,
        fullContentParityStatus:
          stored?.status || row.fullContentParityStatus,
        lastParityVerifiedAt: stored?.verifiedAt || null,
        parityCriticalDifferenceCount: stored?.criticalDifferenceCount || 0,
        parityDifferences: stored?.differences || [],
      };
    });
    res.json({
      ok: true,
      contract: "gdi-portfolio-full-content-parity-v1",
      hotels: rows,
      storeUpdatedAt: store.updatedAt || null,
    });
  } catch (err) {
    console.error("[admin-gdi-parity] inventory", err);
    res.status(500).json({ ok: false, error: "parity_inventory_failed" });
  }
}

/**
 * POST verify one hotel (offline snapshot→DTO; optional live share if body.shareToken).
 * Admin-only. Never rotates tokens.
 */
export async function postAdminGdiFullContentParityVerify(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotelId_required" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const liveShare = body.liveShare === true && Boolean(body.shareToken);
    const verify = await verifyGdiHotelFullContentParity(hotelId, {
      hotelName: body.hotelName || null,
      liveShare,
      shareToken: liveShare ? String(body.shareToken) : undefined,
      prodBase: body.prodBase || process.env.GDI_PROD_BASE || undefined,
      requireLiveShare: body.requireLiveShare === true,
    });
    const recorded = recordHotelParityStatus(hotelId, verify);
    res.json({
      ok: verify.ok === true,
      hotelId,
      status: verify.status,
      verify,
      recorded: recorded.ok === true,
    });
  } catch (err) {
    console.error("[admin-gdi-parity] verify", err);
    res.status(500).json({
      ok: false,
      error: "parity_verify_failed",
      message: String(err?.message || err).slice(0, 240),
    });
  }
}

export { mergeParityOntoCatalogRow };
