/**
 * Market Demand Intelligence API — deal-scoped demand centers, supply, and snapshots.
 *
 * GET  /api/deals/:dealId/demand-centers
 * GET  /api/deals/:dealId/nearby-hotel-supply
 * GET  /api/deals/:dealId/market-demand-snapshot
 * POST /api/deals/:dealId/generate-market-demand-snapshot
 * POST /api/deals/:dealId/import-demand-centers
 */

import { DEALS_MARKET_DEMAND_FIELDS } from "../lib/market-demand/airtable-market-demand-fields.js";
import {
  fetchDemandCentersForDeal,
  fetchNearbyHotelSupplyForDeal,
  fetchLatestSnapshotForDeal,
  upsertMarketDemandSnapshot,
  patchDealsMarketDemandFields,
  createDemandCenterRecords,
} from "../lib/market-demand/airtable-market-demand-io.js";
import {
  buildSnapshotAirtableFields,
  summarizeDemandCenters,
  summarizeNearbyHotelSupply,
} from "../lib/market-demand/scoring.js";
import {
  buildImportPreview,
  filterSelectedImportItems,
  validateImportItem,
} from "../lib/market-demand/import-validation.js";

function strDealId(v) {
  const s = typeof v === "string" ? v.trim() : "";
  return s && s.startsWith("rec") ? s : "";
}

function jsonError(res, status, error, message) {
  return res.status(status).json({ ok: false, error, message });
}

function handleIoError(res, result) {
  if (!result?.error) return false;
  if (result.error === "airtable_config_missing") {
    jsonError(
      res,
      503,
      "airtable_config_missing",
      "Airtable configuration is missing or incomplete."
    );
    return true;
  }
  if (result.error === "market_demand_base_missing") {
    jsonError(
      res,
      503,
      "market_demand_base_missing",
      "Market Demand Platform base (AIRTABLE_BASE_ID_ALT) is not configured."
    );
    return true;
  }
  if (result.error === "market_demand_tables_missing") {
    jsonError(
      res,
      503,
      "market_demand_tables_missing",
      "Market Demand tables or fields are not configured yet."
    );
    return true;
  }
  if (result.error === "deal_not_found") {
    jsonError(res, 404, "deal_not_found", "No deal was found for the provided ID.");
    return true;
  }
  return false;
}

/**
 * GET /api/deals/:dealId/demand-centers
 */
export async function getDealDemandCenters(req, res) {
  try {
    const dealId = strDealId(req.params.dealId);
    if (!dealId) {
      return jsonError(res, 400, "missing_deal_id", "A deal ID is required.");
    }

    const result = await fetchDemandCentersForDeal(dealId);
    if (handleIoError(res, result)) return;

    const demandCenters = result.demandCenters || [];
    const summary = summarizeDemandCenters(demandCenters);

    return res.json({
      ok: true,
      dealId,
      demandCenters,
      summary,
    });
  } catch (err) {
    console.error("[market-demand GET demand-centers]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to load demand centers.");
  }
}

/**
 * GET /api/deals/:dealId/nearby-hotel-supply
 */
export async function getDealNearbyHotelSupply(req, res) {
  try {
    const dealId = strDealId(req.params.dealId);
    if (!dealId) {
      return jsonError(res, 400, "missing_deal_id", "A deal ID is required.");
    }

    const result = await fetchNearbyHotelSupplyForDeal(dealId);
    if (handleIoError(res, result)) return;

    const nearbyHotelSupply = result.nearbyHotelSupply || [];
    const summary = summarizeNearbyHotelSupply(nearbyHotelSupply);

    return res.json({
      ok: true,
      dealId,
      nearbyHotelSupply,
      summary,
    });
  } catch (err) {
    console.error("[market-demand GET nearby-hotel-supply]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to load nearby hotel supply.");
  }
}

/**
 * GET /api/deals/:dealId/market-demand-snapshot
 */
export async function getDealMarketDemandSnapshot(req, res) {
  try {
    const dealId = strDealId(req.params.dealId);
    if (!dealId) {
      return jsonError(res, 400, "missing_deal_id", "A deal ID is required.");
    }

    const result = await fetchLatestSnapshotForDeal(dealId);
    if (handleIoError(res, result)) return;

    return res.json({
      ok: true,
      dealId,
      snapshot: result.snapshot,
      hasSnapshot: Boolean(result.hasSnapshot),
    });
  } catch (err) {
    console.error("[market-demand GET snapshot]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to load market demand snapshot.");
  }
}

/**
 * POST /api/deals/:dealId/generate-market-demand-snapshot
 */
export async function postGenerateMarketDemandSnapshot(req, res) {
  try {
    const dealId = strDealId(req.params.dealId);
    if (!dealId) {
      return jsonError(res, 400, "missing_deal_id", "A deal ID is required.");
    }

    const [dcResult, supplyResult, snapResult] = await Promise.all([
      fetchDemandCentersForDeal(dealId),
      fetchNearbyHotelSupplyForDeal(dealId),
      fetchLatestSnapshotForDeal(dealId),
    ]);

    if (handleIoError(res, dcResult)) return;
    if (handleIoError(res, supplyResult)) return;
    if (handleIoError(res, snapResult)) return;

    const demandCenters = dcResult.demandCenters || [];
    const nearbyHotels = supplyResult.nearbyHotelSupply || [];

    const marketIds = new Set();
    for (const dc of demandCenters) {
      for (const id of dc.linkedMarketIds || []) marketIds.add(id);
    }
    const primaryMarketId = [...marketIds][0] || "";

    const bodyName =
      req.body && typeof req.body.snapshotName === "string" ? req.body.snapshotName.trim() : "";
    const built = buildSnapshotAirtableFields({
      dealId,
      demandCenters,
      nearbyHotels,
      snapshotName: bodyName,
      linkedMarketIds: [...marketIds],
    });

    const saved = await upsertMarketDemandSnapshot(
      dealId,
      built.fields,
      snapResult.recordId
    );
    if (handleIoError(res, saved)) return;

    await patchDealsMarketDemandFields(dealId, {
      ...(primaryMarketId
        ? { [DEALS_MARKET_DEMAND_FIELDS.linkedMarketRecordId]: primaryMarketId }
        : {}),
      [DEALS_MARKET_DEMAND_FIELDS.demandCenterCount]: demandCenters.length,
      [DEALS_MARKET_DEMAND_FIELDS.primaryDemandDrivers]: built.normalized.primaryDemandProfile,
      [DEALS_MARKET_DEMAND_FIELDS.demandStrengthScore]: built.normalized.overallDemandStrength,
      [DEALS_MARKET_DEMAND_FIELDS.demandConfidence]: built.normalized.dataConfidence,
      [DEALS_MARKET_DEMAND_FIELDS.demandSummary]: built.normalized.demandSummary,
      [DEALS_MARKET_DEMAND_FIELDS.demandGapsQuestions]: built.normalized.demandGaps,
    });

    return res.json({
      ok: true,
      dealId,
      snapshot: saved.snapshot,
      hasSnapshot: true,
      generated: true,
    });
  } catch (err) {
    console.error("[market-demand POST generate-snapshot]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to generate market demand snapshot.");
  }
}

function parseImportBody(body) {
  const b = body && typeof body === "object" ? body : {};
  const items = Array.isArray(b.demandCenters) ? b.demandCenters : null;
  const selectedIndices = Array.isArray(b.selectedIndices)
    ? b.selectedIndices.map((n) => Number(n)).filter((n) => Number.isInteger(n) && n >= 0)
    : null;
  return { items, selectedIndices };
}

/**
 * POST /api/deals/:dealId/preview-demand-center-import
 * Validate and preview import rows without writing to Airtable.
 */
export async function postPreviewDemandCenterImport(req, res) {
  try {
    const dealId = strDealId(req.params.dealId);
    if (!dealId) {
      return jsonError(res, 400, "missing_deal_id", "A deal ID is required.");
    }

    const { items } = parseImportBody(req.body);
    if (!items || !items.length) {
      return jsonError(
        res,
        400,
        "validation_failed",
        "Request body must include a non-empty demandCenters array."
      );
    }

    const existingResult = await fetchDemandCentersForDeal(dealId);
    if (handleIoError(res, existingResult)) return;

    const preview = buildImportPreview(items, existingResult.demandCenters || []);

    return res.json({
      ok: true,
      dealId,
      preview: true,
      previewRows: preview.previewRows,
      accepted: preview.accepted,
      rejected: preview.rejected,
      warnings: preview.warnings,
      duplicateCandidates: preview.duplicateCandidates,
      summary: preview.summary,
    });
  } catch (err) {
    console.error("[market-demand POST preview-import]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to preview demand center import.");
  }
}

/**
 * POST /api/deals/:dealId/import-demand-centers
 * Save confirmed demand centers only (manual JSON / import preview confirm).
 */
export async function postImportDemandCenters(req, res) {
  try {
    const dealId = strDealId(req.params.dealId);
    if (!dealId) {
      return jsonError(res, 400, "missing_deal_id", "A deal ID is required.");
    }

    const { items, selectedIndices } = parseImportBody(req.body);
    if (!items || !items.length) {
      return jsonError(
        res,
        400,
        "validation_failed",
        "Request body must include a non-empty demandCenters array."
      );
    }

    const toProcess =
      selectedIndices && selectedIndices.length
        ? filterSelectedImportItems(items, selectedIndices)
        : items;

    if (!toProcess.length) {
      return jsonError(
        res,
        400,
        "validation_failed",
        "No demand centers selected for import."
      );
    }

    const existingResult = await fetchDemandCentersForDeal(dealId);
    if (handleIoError(res, existingResult)) return;

    const preview = buildImportPreview(toProcess, existingResult.demandCenters || []);
    const saveable = preview.accepted
      .map((a) => a.item)
      .filter(Boolean);

    if (!saveable.length) {
      return res.status(400).json({
        ok: false,
        error: "validation_failed",
        message: "No valid demand centers to import after validation.",
        rejected: preview.rejected,
        warnings: preview.warnings,
        duplicateCandidates: preview.duplicateCandidates,
      });
    }

    const created = await createDemandCenterRecords(dealId, saveable);
    if (handleIoError(res, created)) return;

    return res.status(201).json({
      ok: true,
      dealId,
      created: created.demandCenters,
      count: created.demandCenters.length,
      accepted: preview.accepted,
      rejected: preview.rejected,
      warnings: preview.warnings,
      duplicateCandidates: preview.duplicateCandidates,
      skippedInvalidCount: toProcess.length - saveable.length,
    });
  } catch (err) {
    console.error("[market-demand POST import-demand-centers]", err);
    return jsonError(res, 500, "server_error", err.message || "Failed to import demand centers.");
  }
}

/** @internal test helper */
export { validateImportItem };
