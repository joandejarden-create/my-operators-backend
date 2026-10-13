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
import {
  loadMexicoTravelInfrastructureFixturePoints,
  shouldUseMexicoRadarFixtureFallback,
} from "../lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js";

function parseBool(v) {
  return v === "1" || v === "true" || v === "yes";
}

function mexicoFixtureTravelResponse(query) {
  const points = loadMexicoTravelInfrastructureFixturePoints({
    country: query.country,
    market: query.market,
  });
  if (!points.length) return null;
  return {
    ...buildRadarResponse(
      {
        points,
        infrastructure: points,
        tableName: "Travel Infrastructure (fixture)",
      },
      query
    ),
    source: "golden_demo_radar_fixture",
    fixtureFallback: true,
  };
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
  const query = {
    type: req.query.type,
    pointType: req.query.pointType,
    country: req.query.country,
    region: req.query.region,
    market: req.query.market,
    pointTypeFilter: req.query.pointTypeFilter || req.query.layerFilter,
    includeHidden: parseBool(req.query.includeHidden),
  };

  const emptyBody = {
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
  };

  const tryMexicoFixture = () => {
    if (!shouldUseMexicoRadarFixtureFallback(query)) return null;
    return mexicoFixtureTravelResponse(query);
  };

  const cfg = getTravelInfrastructureAirtableConfig();
  if (!cfg) {
    const fixture = tryMexicoFixture();
    if (fixture) return res.json(fixture);
    return res.status(500).json({
      success: false,
      setupNeeded: true,
      error: "airtable_config_missing",
      message: "Missing Airtable API key or Platform base id for travel infrastructure.",
      infrastructure: [],
      points: [],
      layers: {},
      layerFilters: TRAVEL_INFRA_LAYER_FILTERS,
      statistics: emptyBody.statistics,
      totalCount: 0,
    });
  }

  const verified = await verifyTravelInfrastructureTable(cfg.baseId, cfg.apiKey);
  if (!verified.ok) {
    const fixture = tryMexicoFixture();
    if (fixture) return res.json(fixture);
    return res.status(200).json(emptyBody);
  }

  try {
    const result = await fetchTravelInfrastructureRecords(query);
    if (result.error === "airtable_config_missing") {
      const fixture = tryMexicoFixture();
      if (fixture) return res.json(fixture);
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
      const fixture = tryMexicoFixture();
      if (fixture) return res.json(fixture);
      return res.status(200).json({
        ...emptyBody,
        tableName: result.tableName,
      });
    }

    const live = buildRadarResponse(result, query);
    if ((!live.points || !live.points.length) && shouldUseMexicoRadarFixtureFallback(query)) {
      const fixture = mexicoFixtureTravelResponse(query);
      if (fixture) return res.json(fixture);
    }
    return res.json(live);
  } catch (error) {
    console.error("[travel-infrastructure] API error:", error);
    const fixture = tryMexicoFixture();
    if (fixture) return res.json(fixture);
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
