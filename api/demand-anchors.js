/**
 * Demand Anchors / Radar Map Points API.
 *
 * GET /api/demand-anchors
 * GET /api/radar-map-points/demand-anchors
 */

import {
  fetchDemandAnchorRecords,
  verifyDemandAnchorsTable,
} from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import {
  groupDemandAnchorsLayers,
  calculateDemandAnchorsStatistics,
} from "../lib/demand-anchors/radar-map-layers.js";
import { getDemandAnchorsAirtableConfig } from "../lib/demand-anchors/demand-anchors-base.js";
import { DEMAND_ANCHORS_LAYER_FILTERS } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import {
  previewDemandAnchorsImport,
  commitDemandAnchorsImport,
} from "../lib/demand-anchors/import-commit.js";

function parseBool(v) {
  return v === "1" || v === "true" || v === "yes";
}

function emptyPayload(extra = {}) {
  return {
    success: true,
    setupNeeded: true,
    tableName: "Demand Anchors",
    points: [],
    anchors: [],
    layers: {},
    layerFilters: DEMAND_ANCHORS_LAYER_FILTERS,
    statistics: {
      totalDemandAnchors: 0,
      typeCounts: {},
      mapIconCounts: {},
    },
    totalCount: 0,
    ...extra,
  };
}

function buildRadarResponse(result, query) {
  const pointTypeFilter = query.pointTypeFilter || query.layerFilter || "";
  const grouped = groupDemandAnchorsLayers(result.points, pointTypeFilter);
  const stats = calculateDemandAnchorsStatistics(result.allPoints || result.points);

  return {
    success: true,
    setupNeeded: false,
    tableName: result.tableName,
    anchors: result.anchors,
    points: grouped.points,
    layers: grouped.layers,
    layerFilters: grouped.layerFilters,
    statistics: stats,
    totalCount: stats.totalDemandAnchors,
  };
}

async function handleDemandAnchorsRequest(req, res) {
  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg) {
    return res.status(500).json({
      success: false,
      setupNeeded: true,
      error: "airtable_config_missing",
      message: "Missing Airtable API key or Platform base id for demand anchors.",
      ...emptyPayload(),
    });
  }

  const verified = await verifyDemandAnchorsTable(cfg.baseId, cfg.apiKey);
  if (!verified.ok) {
    return res.status(200).json(
      emptyPayload({
        message: "Demand Anchors table is not configured yet.",
      })
    );
  }

  try {
    const query = {
      pointType: req.query.pointType,
      pointTypeFilter: req.query.pointTypeFilter || req.query.layerFilter,
      country: req.query.country,
      region: req.query.region,
      market: req.query.market,
      dealId: req.query.dealId,
      dealRecordId: req.query.dealRecordId,
      includeHidden: parseBool(req.query.includeHidden),
    };

    const result = await fetchDemandAnchorRecords(query);
    if (result.error === "airtable_config_missing") {
      return res.status(500).json({
        success: false,
        setupNeeded: true,
        error: result.error,
        message: "Missing Airtable configuration.",
        points: [],
      });
    }
    if (result.error === "demand_anchors_table_missing") {
      return res.status(200).json(
        emptyPayload({
          message: "Demand Anchors table is not configured yet.",
          tableName: result.tableName,
        })
      );
    }

    return res.json(buildRadarResponse(result, query));
  } catch (error) {
    console.error("[demand-anchors] API error:", error);
    return res.status(500).json({
      success: false,
      error: "server_error",
      message: error.message || "Failed to load demand anchors.",
      details: error.message,
    });
  }
}

/** @deprecated alias */
export async function getDemandAnchors(req, res) {
  return handleDemandAnchorsRequest(req, res);
}

export async function getRadarMapDemandAnchorsPoints(req, res) {
  return handleDemandAnchorsRequest(req, res);
}

/**
 * POST /api/radar-map-points/demand-anchors/import-preview
 */
export async function postDemandAnchorsImportPreview(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const result = await previewDemandAnchorsImport(body);
    if (!result.ok) {
      return res.status(400).json(result);
    }
    return res.json(result);
  } catch (err) {
    console.error("[demand-anchors] import-preview", err);
    return res.status(500).json({
      ok: false,
      error: "server_error",
      message: err.message || "Import preview failed",
    });
  }
}

/**
 * POST /api/radar-map-points/demand-anchors/import-commit
 */
export async function postDemandAnchorsImportCommit(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const records = Array.isArray(body.records) ? body.records : [];
    if (!records.length) {
      return res.status(400).json({
        ok: false,
        error: "validation_failed",
        message: "records array is required",
      });
    }
    const result = await commitDemandAnchorsImport(records, {
      skipDuplicates: body.skipDuplicates !== false,
      market: body.market,
      country: body.country,
      region: body.region,
      dealRecordId: body.dealRecordId,
      linkedMarketId: body.linkedMarketId,
    });
    const status = result.ok ? 201 : 400;
    return res.status(status).json(result);
  } catch (err) {
    console.error("[demand-anchors] import-commit", err);
    return res.status(500).json({
      ok: false,
      error: "server_error",
      message: err.message || "Import commit failed",
    });
  }
}
