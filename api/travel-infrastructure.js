/**
 * Travel Infrastructure / Radar Map Points API.
 *
 * GET /api/travel-infrastructure
 * GET /api/radar-map-points/travel-infrastructure
 */

import {
  fetchTravelInfrastructureRecords,
  verifyTravelInfrastructureTable,
} from "../lib/travel-infrastructure/airtable-travel-infrastructure-io.js";
import {
  groupTravelInfrastructureLayers,
  calculateTravelInfrastructureStatistics,
} from "../lib/travel-infrastructure/radar-map-layers.js";
import { getTravelInfrastructureAirtableConfig } from "../lib/travel-infrastructure/travel-infrastructure-base.js";
import { TRAVEL_INFRA_LAYER_FILTERS } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";
import {
  previewTravelInfrastructureImport,
  commitTravelInfrastructureImport,
} from "../lib/travel-infrastructure/import-commit.js";

function parseBool(v) {
  return v === "1" || v === "true" || v === "yes";
}

function buildRadarResponse(result, query) {
  const pointTypeFilter = query.pointTypeFilter || query.layerFilter || "";
  const grouped = groupTravelInfrastructureLayers(result.points, pointTypeFilter);
  const stats = calculateTravelInfrastructureStatistics(result.points);

  return {
    success: true,
    setupNeeded: false,
    tableName: result.tableName,
    infrastructure: result.infrastructure,
    points: grouped.points,
    layers: grouped.layers,
    layerFilters: TRAVEL_INFRA_LAYER_FILTERS,
    statistics: stats,
    totalCount: result.infrastructure.length,
  };
}

async function handleTravelInfrastructureRequest(req, res) {
  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg) {
    return res.status(500).json({
      success: false,
      setupNeeded: true,
      error: "airtable_config_missing",
      message: "Missing Airtable API key or Platform base id for travel infrastructure.",
      infrastructure: [],
      points: [],
      layers: {},
      layerFilters: TRAVEL_INFRA_LAYER_FILTERS,
      statistics: {
        totalInfrastructure: 0,
        typeCounts: {},
        subtypeCounts: {},
        countryCounts: {},
        regionCounts: {},
        mapIconCounts: {},
      },
      totalCount: 0,
    });
  }

  const verified = await verifyTravelInfrastructureTable(cfg.baseId, cfg.apiKey);
  if (!verified.ok) {
    return res.status(200).json({
      success: true,
      setupNeeded: true,
      message: "Travel Infrastructure Data table is not configured yet.",
      infrastructure: [],
      points: [],
      layers: {},
      layerFilters: TRAVEL_INFRA_LAYER_FILTERS,
      statistics: {
        totalInfrastructure: 0,
        typeCounts: {},
        subtypeCounts: {},
        countryCounts: {},
        regionCounts: {},
        mapIconCounts: {},
      },
      totalCount: 0,
    });
  }

  try {
    const query = {
      type: req.query.type,
      pointType: req.query.pointType,
      country: req.query.country,
      region: req.query.region,
      pointTypeFilter: req.query.pointTypeFilter || req.query.layerFilter,
      includeHidden: parseBool(req.query.includeHidden),
    };

    const result = await fetchTravelInfrastructureRecords(query);
    if (result.error === "airtable_config_missing") {
      return res.status(500).json({
        success: false,
        setupNeeded: true,
        error: result.error,
        message: "Missing Airtable configuration.",
        infrastructure: [],
        points: [],
      });
    }
    if (result.error === "travel_infrastructure_table_missing") {
      return res.status(200).json({
        success: true,
        setupNeeded: true,
        message: "Travel Infrastructure Data table is not configured yet.",
        tableName: result.tableName,
        infrastructure: [],
        points: [],
        layers: {},
        layerFilters: TRAVEL_INFRA_LAYER_FILTERS,
        statistics: {
          totalInfrastructure: 0,
          typeCounts: {},
          subtypeCounts: {},
          countryCounts: {},
          regionCounts: {},
          mapIconCounts: {},
        },
        totalCount: 0,
      });
    }

    return res.json(buildRadarResponse(result, query));
  } catch (error) {
    console.error("[travel-infrastructure] API error:", error);
    return res.status(500).json({
      success: false,
      error: "server_error",
      message: error.message || "Failed to load travel infrastructure.",
      details: error.message,
    });
  }
}

/** @deprecated alias — same handler */
export async function getTravelInfrastructure(req, res) {
  return handleTravelInfrastructureRequest(req, res);
}

export async function getRadarMapTravelInfrastructurePoints(req, res) {
  return handleTravelInfrastructureRequest(req, res);
}

export async function postTravelInfrastructureImportPreview(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const result = await previewTravelInfrastructureImport(body);
    if (!result.ok) return res.status(400).json(result);
    return res.json(result);
  } catch (err) {
    console.error("[travel-infrastructure] import-preview", err);
    return res.status(500).json({ ok: false, error: "server_error", message: err.message });
  }
}

export async function postTravelInfrastructureImportCommit(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const records = Array.isArray(body.records) ? body.records : [];
    if (!records.length) {
      return res.status(400).json({ ok: false, error: "validation_failed", message: "records required" });
    }
    const result = await commitTravelInfrastructureImport(records, {
      skipDuplicates: body.skipDuplicates !== false,
      market: body.market,
      country: body.country,
      region: body.region,
    });
    return res.status(result.ok ? 201 : 400).json(result);
  } catch (err) {
    console.error("[travel-infrastructure] import-commit", err);
    return res.status(500).json({ ok: false, error: "server_error", message: err.message });
  }
}
