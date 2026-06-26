/**
 * Group normalized demand anchor points into radar layers.
 */

import {
  DEMAND_ANCHORS_LAYER_NAME,
  DEMAND_ANCHORS_LAYER_FILTERS,
} from "./airtable-demand-anchors-fields.js";
import { filterRadarVisiblePoints } from "./normalize-demand-anchor.js";

/**
 * @param {import('./normalize-demand-anchor.js').DemandAnchorPoint[]} points
 * @param {string} [pointTypeFilter]
 */
export function groupDemandAnchorsLayers(points, pointTypeFilter) {
  const visible = filterRadarVisiblePoints(points);
  const filterId = String(pointTypeFilter || "").trim();
  const filtered =
    filterId && filterId !== "all"
      ? visible.filter((p) => p.pointType === filterId || p.type === filterId)
      : visible;

  const byPointType = {};
  for (const p of filtered) {
    const key = p.pointType || p.type || "Unknown";
    byPointType[key] = (byPointType[key] || 0) + 1;
  }

  return {
    layerFilters: DEMAND_ANCHORS_LAYER_FILTERS,
    layers: {
      [DEMAND_ANCHORS_LAYER_NAME]: {
        layerName: DEMAND_ANCHORS_LAYER_NAME,
        pointCount: filtered.length,
        byPointType,
        points: filtered,
      },
    },
    points: filtered,
  };
}

/**
 * @param {import('./normalize-demand-anchor.js').DemandAnchorPoint[]} points
 */
export function calculateDemandAnchorsStatistics(points) {
  const visible = filterRadarVisiblePoints(points);
  const typeCounts = {};
  const mapIconCounts = {};

  for (const p of visible) {
    const type = p.pointType || p.type || "Unknown";
    typeCounts[type] = (typeCounts[type] || 0) + 1;
    const icon = p.mapIconType || "Unknown";
    mapIconCounts[icon] = (mapIconCounts[icon] || 0) + 1;
  }

  return {
    totalDemandAnchors: visible.length,
    typeCounts,
    mapIconCounts,
  };
}

/**
 * @param {import('./normalize-demand-anchor.js').DemandAnchorPoint[]} points
 */
export function getDemandAnchorLayerFilters(points) {
  const stats = calculateDemandAnchorsStatistics(points);
  return DEMAND_ANCHORS_LAYER_FILTERS.map((def) => ({
    ...def,
    count:
      def.id === "all"
        ? stats.totalDemandAnchors
        : stats.typeCounts[def.id] || 0,
  }));
}

/**
 * @param {import('./normalize-demand-anchor.js').DemandAnchorPoint[]} points
 */
export function summarizeDemandAnchors(points) {
  return calculateDemandAnchorsStatistics(points);
}
