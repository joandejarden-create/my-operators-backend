/**
 * ADP + GDI → Hotel Property Census canonical identity resolver.
 * Durable alias map: fixtures/hotel-census/adp-gdi-hotel-alias-map-v1.json
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getCensusRecordIdForAdpProperty } from "../ai-demand-positioning/census-link-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../..");
const ALIAS_MAP_PATH = path.join(
  REPO_ROOT,
  "fixtures/hotel-census/adp-gdi-hotel-alias-map-v1.json"
);

let _cache = null;

export function getAliasMapPath() {
  return ALIAS_MAP_PATH;
}

export function loadAdpGdiAliasMap({ force = false } = {}) {
  if (_cache && !force) return _cache;
  const raw = JSON.parse(fs.readFileSync(ALIAS_MAP_PATH, "utf8"));
  _cache = raw;
  return raw;
}

export function saveAdpGdiAliasMap(map) {
  fs.writeFileSync(ALIAS_MAP_PATH, JSON.stringify(map, null, 2) + "\n", "utf8");
  _cache = map;
  return map;
}

export function isAirtableRecordId(id) {
  return /^rec[a-zA-Z0-9]{14}$/.test(String(id || "").trim());
}

export function isProvisionalGdiHotelId(id) {
  return String(id || "").trim().startsWith("gdi_hotel_");
}

export function isAdpPropertyId(id) {
  return String(id || "").trim().startsWith("adp_");
}

/**
 * Resolve any known hotel key to canonical HPC record id.
 * Returns null when unresolved.
 */
export function resolveCanonicalHotelId(hotelKey, opts = {}) {
  const key = String(hotelKey || "").trim();
  if (!key) return null;
  if (isAirtableRecordId(key) && !opts.forceAliasLookup) {
    const map = loadAdpGdiAliasMap();
    const entry = map.aliases?.[key];
    if (entry?.canonicalHotelId) return entry.canonicalHotelId;
    // Already looks like HPC id
    return key;
  }

  const map = loadAdpGdiAliasMap();
  const entry = map.aliases?.[key];
  if (entry?.canonicalHotelId) return entry.canonicalHotelId;
  if (entry?.aliasOf) {
    const parent = map.aliases?.[entry.aliasOf];
    if (parent?.canonicalHotelId) return parent.canonicalHotelId;
    if (isAirtableRecordId(entry.aliasOf)) return entry.aliasOf;
  }

  if (isAdpPropertyId(key)) {
    const fromRegistry = getCensusRecordIdForAdpProperty(key);
    if (fromRegistry) return fromRegistry;
  }

  return null;
}

/**
 * Unique ADP/GDI production hotels from alias map (one row per canonical or pending root).
 */
export function listAdpGdiHotelUniverse() {
  const map = loadAdpGdiAliasMap();
  const roots = [];
  const seen = new Set();
  for (const [key, entry] of Object.entries(map.aliases || {})) {
    if (entry.aliasOf && !entry.adp && !entry.gdi) continue;
    if (entry.aliasOf && isAdpPropertyId(key) && !entry.displayName) continue;
    const rootKey =
      entry.canonicalHotelId ||
      (entry.provisionalGdiHotelId && !entry.canonicalHotelId
        ? entry.provisionalGdiHotelId
        : null) ||
      (isAdpPropertyId(key) ? key : null) ||
      key;
    if (!rootKey || seen.has(rootKey)) continue;
    // Prefer root entries that carry display / flags
    if (
      entry.displayName ||
      entry.adp ||
      entry.gdi ||
      isAirtableRecordId(key) ||
      isProvisionalGdiHotelId(key)
    ) {
      seen.add(rootKey);
      roots.push({
        key: rootKey,
        displayName: entry.displayName || key,
        canonicalHotelId: entry.canonicalHotelId || null,
        provisionalGdiHotelId: entry.provisionalGdiHotelId || null,
        adpPropertyId: entry.adpPropertyId || (isAdpPropertyId(key) ? key : null),
        adp: Boolean(entry.adp),
        gdi: Boolean(entry.gdi),
        linkStatus: entry.linkStatus || "unknown",
        identityKey: entry.identityKey || null,
      });
    }
  }
  // Also pull ADP-only linked keys that were skipped as pure aliases
  for (const [key, entry] of Object.entries(map.aliases || {})) {
    if (!isAdpPropertyId(key)) continue;
    const canon = entry.canonicalHotelId || resolveCanonicalHotelId(key);
    const rootKey = canon || key;
    if (seen.has(rootKey)) {
      const row = roots.find((r) => r.key === rootKey || r.canonicalHotelId === canon);
      if (row) {
        row.adp = true;
        row.adpPropertyId = row.adpPropertyId || key;
      }
      continue;
    }
    seen.add(rootKey);
    roots.push({
      key: rootKey,
      displayName: entry.displayName || key,
      canonicalHotelId: canon,
      provisionalGdiHotelId: null,
      adpPropertyId: key,
      adp: true,
      gdi: false,
      linkStatus: entry.linkStatus || (canon ? "canonical" : "unresolved"),
      identityKey: entry.identityKey || null,
    });
  }
  return roots.sort((a, b) =>
    String(a.displayName).localeCompare(String(b.displayName))
  );
}

export function identityStatusForHotel(row) {
  if (row.canonicalHotelId && isAirtableRecordId(row.canonicalHotelId)) {
    if (row.provisionalGdiHotelId) return "CANONICAL_HPC"; // provisional retained as alias only
    return "CANONICAL_HPC";
  }
  if (row.provisionalGdiHotelId || isProvisionalGdiHotelId(row.key)) {
    return "PROVISIONAL";
  }
  return "UNRESOLVED";
}
