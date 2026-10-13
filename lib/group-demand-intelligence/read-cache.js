/**
 * Short-TTL in-memory read cache for GDI hotel opportunity docs and
 * commercial-progression maps. Authorization remains on every request —
 * this only avoids repeat Airtable/FS loads within a brief window.
 *
 * Scope: process-local. Keyed by hotelId. Never cross-hotel.
 * Invalidate on writes via invalidateGdiHotelReadCache(hotelId).
 */

const DEFAULT_TTL_MS = Number(process.env.GDI_READ_CACHE_TTL_MS || 45_000);

/** @type {Map<string, { expiresAt: number, value: any }>} */
const oppDocCache = new Map();
/** @type {Map<string, { expiresAt: number, value: Map<string, object> }>} */
const progressionCache = new Map();

function now() {
  return Date.now();
}

function getEntry(map, key) {
  const hit = map.get(key);
  if (!hit) return null;
  if (hit.expiresAt <= now()) {
    map.delete(key);
    return null;
  }
  return hit.value;
}

function setEntry(map, key, value, ttlMs = DEFAULT_TTL_MS) {
  map.set(key, { expiresAt: now() + Math.max(1_000, ttlMs), value });
}

export function getCachedOpportunityDoc(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) return null;
  return getEntry(oppDocCache, id);
}

export function setCachedOpportunityDoc(hotelId, doc, ttlMs) {
  const id = String(hotelId || "").trim();
  if (!id || !doc) return;
  setEntry(oppDocCache, id, doc, ttlMs);
}

export function getCachedProgressionMap(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) return null;
  return getEntry(progressionCache, id);
}

export function setCachedProgressionMap(hotelId, map, ttlMs) {
  const id = String(hotelId || "").trim();
  if (!id || !map) return;
  setEntry(progressionCache, id, map, ttlMs);
}

export function invalidateGdiHotelReadCache(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) {
    oppDocCache.clear();
    progressionCache.clear();
    return;
  }
  oppDocCache.delete(id);
  progressionCache.delete(id);
}

export function gdiReadCacheStats() {
  return {
    opportunityDocs: oppDocCache.size,
    progressionMaps: progressionCache.size,
    ttlMs: DEFAULT_TTL_MS,
  };
}
