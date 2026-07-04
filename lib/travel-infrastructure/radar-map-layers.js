/**
 * Group normalized radar map points into Travel Infrastructure layers.
 */

import {
  TRAVEL_INFRA_LAYER_NAME,
  TRAVEL_INFRA_LAYER_FILTERS,
} from "./airtable-travel-infrastructure-fields.js";
import { filterRadarVisiblePoints } from "./normalize-radar-map-point.js";

/**
 * @param {import('./normalize-radar-map-point.js').RadarMapPoint[]} points
 * @param {string} [pointTypeFilter]
 */
export function groupTravelInfrastructureLayers(points, pointTypeFilter) {
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
    layerFilters: TRAVEL_INFRA_LAYER_FILTERS,
    layers: {
      [TRAVEL_INFRA_LAYER_NAME]: {
        layerName: TRAVEL_INFRA_LAYER_NAME,
        pointCount: filtered.length,
        byPointType,
        points: filtered,
      },
    },
    points: filtered,
  };
}

/**
 * @param {import('./normalize-radar-map-point.js').RadarMapPoint[]} points
 */
export function calculateTravelInfrastructureStatistics(points) {
  const visible = filterRadarVisiblePoints(points);
  const typeCounts = {};
  const subtypeCounts = {};
  const countryCounts = {};
  const regionCounts = {};
  const mapIconCounts = {};

  for (const p of visible) {
    const type = p.pointType || p.type || "Unknown";
    typeCounts[type] = (typeCounts[type] || 0) + 1;
    const sub = p.pointSubtype || "Unknown";
    subtypeCounts[sub] = (subtypeCounts[sub] || 0) + 1;
    const country = p.country || "Unknown";
    countryCounts[country] = (countryCounts[country] || 0) + 1;
    const region = p.region || "Unknown";
    regionCounts[region] = (regionCounts[region] || 0) + 1;
    const icon = p.mapIconType || "Unknown";
    mapIconCounts[icon] = (mapIconCounts[icon] || 0) + 1;
  }

  return {
    totalInfrastructure: visible.length,
    typeCounts,
    subtypeCounts,
    countryCounts,
    regionCounts,
    mapIconCounts,
  };
}
