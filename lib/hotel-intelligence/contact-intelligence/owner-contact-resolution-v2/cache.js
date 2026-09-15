/**
 * Owner-level resolution cache (file-backed via Contact Intelligence store).
 */

import { CACHE_TTL } from "./vocabulary.js";

export function isCacheFresh(entry, ttlMs = CACHE_TTL.resolution_default_ms) {
  if (!entry?.resolved_at && !entry?.cached_at) return false;
  const t = Date.parse(entry.resolved_at || entry.cached_at);
  if (!Number.isFinite(t)) return false;
  return Date.now() - t < ttlMs;
}

export function readCachedResolution(store, ownerEntityId, { force_refresh = false } = {}) {
  if (!store || force_refresh) return null;
  const route = store.getOwnerRoute?.(ownerEntityId);
  if (!route?.contact_resolution_v2) return null;
  if (!isCacheFresh(route.contact_resolution_v2)) return null;
  return {
    ...route.contact_resolution_v2,
    resolution_status: "CACHED",
    from_cache: true,
  };
}

export function writeCachedResolution(store, ownerEntityId, resolution) {
  if (!store?.putOwnerRoute) return { ok: false, reason: "no_store" };
  const existing = store.getOwnerRoute(ownerEntityId) || { owner_entity_id: ownerEntityId };
  const next = {
    ...existing,
    owner_entity_id: ownerEntityId,
    contact_resolution_v2: {
      ...resolution,
      cached_at: new Date().toISOString(),
      resolved_at: resolution.resolved_at || new Date().toISOString(),
    },
    updated_at: new Date().toISOString(),
    evaluation_staging: true,
    airtable_write: false,
  };
  store.putOwnerRoute(next);
  return { ok: true };
}
