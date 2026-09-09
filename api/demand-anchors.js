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
import {
  loadMexicoDemandAnchorFixturePoints,
  shouldUseMexicoRadarFixtureFallback,
} from "../lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js";

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

function mexicoFixtureDemandResponse(query) {
  const points = loadMexicoDemandAnchorFixturePoints({
    country: query.country,
    market: query.market,
  });
  if (!points.length) return null;
  return {
    ...buildRadarResponse({ points, anchors: points, tableName: "Demand Anchors (fixture)" }, query),
    source: "golden_demo_radar_fixture",
    fixtureFallback: true,
  };
}

function slimMapPoint(point) {
  if (!point || typeof point !== "object") return point;
  const lat = point.latitude != null ? point.latitude : point.lat;
  const lng = point.longitude != null ? point.longitude : point.lng;
  const out = {
    id: point.id,
    name: point.name,
    pointType: point.pointType || point.type,
    pointSubtype: point.pointSubtype,
    type: point.type || point.pointType,
    latitude: lat,
    longitude: lng,
    lat,
    lng,
    city: point.city,
    country: point.country,
    region: point.region,
    mapIconType: point.mapIconType,
    includeOnRadarMap: point.includeOnRadarMap,
    dataConfidence: point.dataConfidence,
    demandSegment: point.demandSegment,
    demandRelevance: point.demandRelevance,
  };
  for (const key of Object.keys(out)) {
    if (out[key] == null || out[key] === "") delete out[key];
  }
  return out;
}

function buildRadarResponse(result, query) {
  const pointTypeFilter = query.pointTypeFilter || query.layerFilter || "";
  const grouped = groupDemandAnchorsLayers(result.points, pointTypeFilter);
  const stats = calculateDemandAnchorsStatistics(result.allPoints || result.points);
  const countsOnly = query.countsOnly === true;
  const mapView = query.view === "map";

  if (countsOnly) {
    return {
      success: true,
      setupNeeded: false,
      tableName: result.tableName,
      anchors: [],
      points: [],
      layers: {},
      layerFilters: grouped.layerFilters,
      statistics: stats,
      totalCount: stats.totalDemandAnchors,
      view: "counts",
    };
  }

  const points = mapView ? grouped.points.map(slimMapPoint) : grouped.points;

  return {
    success: true,
    setupNeeded: false,
    tableName: result.tableName,
    anchors: mapView ? points : result.anchors,
    points,
    layers: mapView ? undefined : grouped.layers,
    layerFilters: grouped.layerFilters,
    statistics: stats,
    totalCount: stats.totalDemandAnchors,
    view: mapView ? "map" : "full",
  };
}

async function handleDemandAnchorsRequest(req, res) {
  const query = {
    pointType: req.query.pointType,
    pointTypeFilter: req.query.pointTypeFilter || req.query.layerFilter,
    country: req.query.country,
    region: req.query.region,
    market: req.query.market,
    dealId: req.query.dealId,
    dealRecordId: req.query.dealRecordId,
    includeHidden: parseBool(req.query.includeHidden),
    countsOnly: parseBool(req.query.countsOnly),
    view: String(req.query.view || "").trim().toLowerCase() === "map" ? "map" : "full",
  };

  const tryMexicoFixture = () => {
    if (!shouldUseMexicoRadarFixtureFallback(query)) return null;
    return mexicoFixtureDemandResponse(query);
  };

  const cfg = getDemandAnchorsAirtableConfig();
  if (!cfg) {
    const fixture = tryMexicoFixture();
    if (fixture) return res.json(fixture);
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
    const fixture = tryMexicoFixture();
    if (fixture) return res.json(fixture);
    return res.status(200).json(
      emptyPayload({
        message: "Demand Anchors table is not configured yet.",
      })
    );
  }

  try {
    const result = await fetchDemandAnchorRecords(query);
    if (result.error === "airtable_config_missing") {
      const fixture = tryMexicoFixture();
      if (fixture) return res.json(fixture);
      return res.status(500).json({
        success: false,
        setupNeeded: true,
        error: result.error,
        message: "Missing Airtable configuration.",
        points: [],
      });
    }
    if (result.error === "demand_anchors_table_missing") {
      const fixture = tryMexicoFixture();
      if (fixture) return res.json(fixture);
      return res.status(200).json(
        emptyPayload({
          message: "Demand Anchors table is not configured yet.",
          tableName: result.tableName,
        })
      );
    }

    const live = buildRadarResponse(result, query);
    if ((!live.points || !live.points.length) && shouldUseMexicoRadarFixtureFallback(query)) {
      const fixture = mexicoFixtureDemandResponse(query);
      if (fixture) return res.json(fixture);
    }
    return res.json(live);
  } catch (error) {
    console.error("[demand-anchors] API error:", error);
    const fixture = tryMexicoFixture();
    if (fixture) return res.json(fixture);
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
