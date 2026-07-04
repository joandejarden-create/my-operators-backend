/**
 * Coverage metrics for CALA Radar Buildout plans.
 */

function numCoord(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function hasValidCoordinates(point) {
  const lat = numCoord(point.latitude ?? point.lat);
  const lng = numCoord(point.longitude ?? point.lng);
  if (lat == null || lng == null) return false;
  if (lat === 0 && lng === 0) return false;
  return lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180;
}

function hasSource(point) {
  const ref = String(point.sourceReference || point.sourceUrl || "").trim();
  if (ref) return true;
  const src = point.source;
  if (Array.isArray(src) && src.length) return true;
  if (typeof src === "string" && src.trim()) return true;
  return false;
}

function isRadarVisible(point) {
  return point.includeOnRadarMap !== false;
}

/**
 * @param {object[]} points
 */
export function calculateSourceCoveragePct(points) {
  const list = points || [];
  if (!list.length) return 0;
  const backed = list.filter((p) => hasSource(p));
  return Math.round((backed.length / list.length) * 100);
}

/**
 * @param {object[]} points
 */
export function calculateCoordinateCoveragePct(points) {
  const list = points || [];
  if (!list.length) return 0;
  const withCoords = list.filter((p) => hasValidCoordinates(p));
  return Math.round((withCoords.length / list.length) * 100);
}

/**
 * @param {object[]} points
 */
export function calculateDataConfidenceMix(points) {
  const mix = { High: 0, Medium: 0, Low: 0, Unknown: 0 };
  for (const p of points || []) {
    const key = String(p.dataConfidence || "").trim() || "Unknown";
    if (mix[key] == null) mix[key] = 0;
    mix[key] += 1;
  }
  return mix;
}

/**
 * @param {object[]} demandAnchors
 * @param {object[]} travelInfra
 */
export function summarizeCountryRadarPoints(demandAnchors, travelInfra) {
  const da = demandAnchors || [];
  const ti = travelInfra || [];
  const combined = [...da, ...ti];
  const visible = combined.filter(isRadarVisible);
  return {
    demandAnchors: da.length,
    travelInfrastructure: ti.length,
    totalRadarPoints: combined.length,
    visibleRadarPoints: visible.length,
    sourceCoveragePct: calculateSourceCoveragePct(visible),
    coordinateCoveragePct: calculateCoordinateCoveragePct(visible),
    dataConfidenceMix: calculateDataConfidenceMix(visible),
    pointTypes: countBy(da, (p) => p.pointType || p.type),
    infraTypes: countBy(ti, (p) => p.pointType || p.type),
    submarkets: countBy(combined, (p) => p.submarket || ""),
  };
}

function countBy(items, keyFn) {
  const out = {};
  for (const item of items || []) {
    const k = keyFn(item) || "(none)";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

/**
 * @param {object[]} points
 */
export function countDistinctPointTypes(points) {
  const types = new Set(
    (points || []).map((p) => String(p.pointType || p.type || "").trim()).filter(Boolean)
  );
  return types.size;
}

/**
 * @param {Record<string, number>} submarketCounts
 */
export function countCorridorsCovered(submarketCounts) {
  return Object.keys(submarketCounts || {}).filter(
    (k) => k && k !== "(none)" && k !== "Other" && submarketCounts[k] > 0
  ).length;
}
