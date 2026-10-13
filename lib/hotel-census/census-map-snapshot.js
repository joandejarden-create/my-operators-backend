/**
 * Census map snapshot read model — Airtable remains SoT; this is a derived Tier-C cache.
 *
 * Layout:
 *   data/census-map/CURRENT.json          pointer (atomic)
 *   data/census-map/LAST_GOOD.json         previous CURRENT (atomic)
 *   data/census-map/snapshots/<file>.json  versioned hotel payloads
 */

import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
  unlinkSync,
  statSync,
} from "fs";
import { join, dirname } from "path";
import { performance } from "perf_hooks";
import Airtable from "airtable";
import {
  MAP_DTO_SCHEMA_VERSION,
  CENSUS_MAP_TABLE,
  fetchCanonicalMapHotelsFromAirtable,
} from "./map-hotel-dto.js";

export const CENSUS_MAP_SNAPSHOT_DIR = join(process.cwd(), "data/census-map");
export const CENSUS_MAP_SNAPSHOTS_DIR = join(CENSUS_MAP_SNAPSHOT_DIR, "snapshots");
export const CENSUS_MAP_CURRENT_PATH = join(CENSUS_MAP_SNAPSHOT_DIR, "CURRENT.json");
export const CENSUS_MAP_LAST_GOOD_PATH = join(CENSUS_MAP_SNAPSHOT_DIR, "LAST_GOOD.json");

/** Tier C default: 60 minutes. */
export const DEFAULT_MAX_AGE_MS = 60 * 60 * 1000;

let memorySnapshot = null;
let buildInflight = null;
const metrics = {
  snapshotHit: 0,
  snapshotUnavailable: 0,
  snapshotStale: 0,
  fallbackUsed: 0,
  bootPreloadMs: null,
  bootPreloadBytes: null,
  lastBuildMs: null,
  lastBuildAt: null,
};

function envFlag(name, defaultTrue = false) {
  const v = process.env[name];
  if (v == null || v === "") return defaultTrue;
  const s = String(v).trim().toLowerCase();
  if (["0", "false", "off", "no"].includes(s)) return false;
  if (["1", "true", "on", "yes"].includes(s)) return true;
  return defaultTrue;
}

export function censusMapSnapshotEnabled() {
  // Default ON — set CENSUS_MAP_SNAPSHOT=0 to force legacy Airtable path only.
  return envFlag("CENSUS_MAP_SNAPSHOT", true);
}

export function censusMapSnapshotFallbackAirtableEnabled() {
  return envFlag("CENSUS_MAP_SNAPSHOT_FALLBACK_AIRTABLE", true);
}

export function censusMapSnapshotMaxAgeMs() {
  const n = parseInt(process.env.CENSUS_MAP_SNAPSHOT_MAX_AGE_MS || "", 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MAX_AGE_MS;
}

function atomicWriteJson(path, data) {
  mkdirSync(dirname(path), { recursive: true });
  const payload = typeof data === "string" ? data : JSON.stringify(data);
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, payload);
  try {
    renameSync(tmp, path);
  } catch {
    let lastErr = null;
    for (let i = 0; i < 8; i++) {
      try {
        writeFileSync(path, payload);
        lastErr = null;
        break;
      } catch (err) {
        lastErr = err;
        const until = Date.now() + 250 * (i + 1);
        while (Date.now() < until) {
          /* spin */
        }
      }
    }
    try {
      unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
    if (lastErr) throw lastErr;
  }
}

function readJson(path) {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) return null;
  return new Airtable({ apiKey }).base(baseId);
}

/**
 * Build hotels from Airtable using canonical map DTO (no second transform).
 */
export async function buildCensusMapHotelsFromAirtable(opts = {}) {
  const base = getAirtableBase();
  if (!base) {
    throw new Error("AIRTABLE_API_KEY / AIRTABLE_BASE_ID_ALT required to build census map snapshot");
  }
  return fetchCanonicalMapHotelsFromAirtable(base, opts);
}

/**
 * Publish a versioned snapshot atomically + update CURRENT / LAST_GOOD.
 */
export async function publishCensusMapSnapshot(opts = {}) {
  const t0 = performance.now();
  const universe = await buildCensusMapHotelsFromAirtable(opts);
  const generatedAt = new Date().toISOString();
  const stamp = generatedAt.replace(/[:.]/g, "-");
  const fileName = `census-map-${MAP_DTO_SCHEMA_VERSION}-${stamp}.json`;
  const snapshotPath = join(CENSUS_MAP_SNAPSHOTS_DIR, fileName);
  const buildDurationMs = Math.round(performance.now() - t0);

  const document = {
    schemaVersion: MAP_DTO_SCHEMA_VERSION,
    generatedAt,
    recordCount: universe.totalCount,
    totalWithCoordinates: universe.totalWithCoordinates,
    skippedNoCoordinates: universe.skippedNoCoordinates,
    source: `airtable:${CENSUS_MAP_TABLE}`,
    buildDurationMs,
    hotels: universe.hotels,
  };

  mkdirSync(CENSUS_MAP_SNAPSHOTS_DIR, { recursive: true });
  // Large payload — write without pretty-print for size/speed
  atomicWriteJson(snapshotPath, JSON.stringify(document));

  const pointer = {
    schemaVersion: MAP_DTO_SCHEMA_VERSION,
    generatedAt,
    recordCount: universe.totalCount,
    totalWithCoordinates: universe.totalWithCoordinates,
    skippedNoCoordinates: universe.skippedNoCoordinates,
    source: document.source,
    buildDurationMs,
    snapshotFile: `snapshots/${fileName}`,
    snapshotPath: fileName,
  };

  if (existsSync(CENSUS_MAP_CURRENT_PATH)) {
    try {
      const prev = readFileSync(CENSUS_MAP_CURRENT_PATH);
      atomicWriteJson(CENSUS_MAP_LAST_GOOD_PATH, prev.toString("utf8"));
    } catch (err) {
      console.warn("[census-map-snapshot] LAST_GOOD retention failed:", err?.message || err);
    }
  }
  atomicWriteJson(CENSUS_MAP_CURRENT_PATH, JSON.stringify(pointer, null, 2));

  memorySnapshot = {
    pointer,
    hotels: universe.hotels,
    loadedAt: Date.now(),
    ageMsAtLoad: 0,
    bytes: Buffer.byteLength(JSON.stringify(document)),
  };

  metrics.lastBuildMs = buildDurationMs;
  metrics.lastBuildAt = generatedAt;

  return {
    ok: true,
    pointer,
    buildDurationMs,
    recordCount: universe.totalCount,
    snapshotFile: pointer.snapshotFile,
  };
}

export function getCensusMapSnapshotMetrics() {
  const mem = memorySnapshot;
  const ageMs = mem?.pointer?.generatedAt
    ? Date.now() - new Date(mem.pointer.generatedAt).getTime()
    : null;
  return {
    ...metrics,
    inMemory: Boolean(mem),
    recordCount: mem?.pointer?.recordCount ?? null,
    generatedAt: mem?.pointer?.generatedAt ?? null,
    schemaVersion: mem?.pointer?.schemaVersion ?? null,
    ageMs,
    maxAgeMs: censusMapSnapshotMaxAgeMs(),
    stale: ageMs != null ? ageMs > censusMapSnapshotMaxAgeMs() : null,
    enabled: censusMapSnapshotEnabled(),
    fallbackAirtable: censusMapSnapshotFallbackAirtableEnabled(),
  };
}

function loadPointerAndHotelsFromDisk(pointerPath) {
  const pointer = readJson(pointerPath);
  if (!pointer?.snapshotFile && !pointer?.snapshotPath) return null;
  const rel = pointer.snapshotFile || `snapshots/${pointer.snapshotPath}`;
  const full = join(CENSUS_MAP_SNAPSHOT_DIR, rel.replace(/^\//, ""));
  if (!existsSync(full)) return null;
  const t0 = performance.now();
  const raw = readFileSync(full, "utf8");
  const doc = JSON.parse(raw);
  const loadMs = Math.round(performance.now() - t0);
  if (!Array.isArray(doc.hotels)) return null;
  return {
    pointer: {
      schemaVersion: doc.schemaVersion || pointer.schemaVersion,
      generatedAt: doc.generatedAt || pointer.generatedAt,
      recordCount: doc.recordCount ?? doc.hotels.length,
      totalWithCoordinates: doc.totalWithCoordinates ?? pointer.totalWithCoordinates,
      skippedNoCoordinates: doc.skippedNoCoordinates ?? pointer.skippedNoCoordinates,
      source: doc.source || pointer.source,
      buildDurationMs: doc.buildDurationMs ?? pointer.buildDurationMs,
      snapshotFile: rel,
    },
    hotels: doc.hotels,
    bytes: Buffer.byteLength(raw),
    loadMs,
  };
}

/**
 * Load CURRENT (or LAST_GOOD) into process memory.
 */
export function preloadCensusMapSnapshot({ allowLastGood = true } = {}) {
  const t0 = performance.now();
  let loaded = loadPointerAndHotelsFromDisk(CENSUS_MAP_CURRENT_PATH);
  let from = "CURRENT";
  if (!loaded && allowLastGood) {
    loaded = loadPointerAndHotelsFromDisk(CENSUS_MAP_LAST_GOOD_PATH);
    from = "LAST_GOOD";
  }
  const bootMs = Math.round(performance.now() - t0);
  if (!loaded) {
    memorySnapshot = null;
    metrics.bootPreloadMs = bootMs;
    metrics.bootPreloadBytes = 0;
    return { ok: false, bootMs, reason: "snapshot_missing" };
  }
  memorySnapshot = {
    pointer: loaded.pointer,
    hotels: loaded.hotels,
    loadedAt: Date.now(),
    ageMsAtLoad: Date.now() - new Date(loaded.pointer.generatedAt).getTime(),
    bytes: loaded.bytes,
    from,
  };
  metrics.bootPreloadMs = bootMs;
  metrics.bootPreloadBytes = loaded.bytes;
  return {
    ok: true,
    bootMs,
    loadMs: loaded.loadMs,
    bytes: loaded.bytes,
    recordCount: loaded.pointer.recordCount,
    generatedAt: loaded.pointer.generatedAt,
    from,
    ageMs: memorySnapshot.ageMsAtLoad,
  };
}

export function getMemoryCensusMapSnapshot() {
  return memorySnapshot;
}

/**
 * Resolve map hotels for Express — snapshot preferred.
 * @returns {{ ok: true, hotels, meta } | { ok: false, reason, allowFallback: boolean }}
 */
export function resolveCensusMapSnapshotForRead() {
  if (!censusMapSnapshotEnabled()) {
    return { ok: false, reason: "snapshot_disabled", allowFallback: true };
  }

  if (!memorySnapshot) {
    const disk = preloadCensusMapSnapshot({ allowLastGood: true });
    if (!disk.ok) {
      metrics.snapshotUnavailable += 1;
      return {
        ok: false,
        reason: "snapshot_unavailable",
        allowFallback: censusMapSnapshotFallbackAirtableEnabled(),
      };
    }
  }

  const generatedAt = memorySnapshot.pointer.generatedAt;
  const ageMs = Date.now() - new Date(generatedAt).getTime();
  const stale = ageMs > censusMapSnapshotMaxAgeMs();
  if (stale) metrics.snapshotStale += 1;
  metrics.snapshotHit += 1;

  return {
    ok: true,
    hotels: memorySnapshot.hotels,
    meta: {
      source: "census_map_snapshot",
      snapshot: "HIT",
      schemaVersion: memorySnapshot.pointer.schemaVersion,
      generatedAt,
      recordCount: memorySnapshot.pointer.recordCount,
      ageMs,
      stale,
      buildDurationMs: memorySnapshot.pointer.buildDurationMs,
      from: memorySnapshot.from || "memory",
    },
  };
}

/**
 * Parse optional bounding-box query (west/south/east/north or bbox=w,s,e,n).
 * @returns {{ west:number,south:number,east:number,north:number } | null}
 */
export function parseMapBoundsQuery(query = {}) {
  const q = query || {};
  let west = parseFloat(q.west);
  let south = parseFloat(q.south);
  let east = parseFloat(q.east);
  let north = parseFloat(q.north);
  if (
    !(
      Number.isFinite(west) &&
      Number.isFinite(south) &&
      Number.isFinite(east) &&
      Number.isFinite(north)
    ) &&
    q.bbox
  ) {
    const parts = String(q.bbox)
      .split(",")
      .map((s) => parseFloat(String(s).trim()));
    if (parts.length === 4 && parts.every(Number.isFinite)) {
      [west, south, east, north] = parts;
    }
  }
  if (
    !(
      Number.isFinite(west) &&
      Number.isFinite(south) &&
      Number.isFinite(east) &&
      Number.isFinite(north)
    )
  ) {
    return null;
  }
  // Normalize inverted / antimeridian-naive boxes.
  if (south > north) {
    const t = south;
    south = north;
    north = t;
  }
  return { west, south, east, north };
}

export function filterMapHotelsByBounds(hotels, bounds) {
  if (!bounds || !Array.isArray(hotels)) return hotels || [];
  const { west, south, east, north } = bounds;
  const crossesAntimeridian = west > east;
  return hotels.filter((h) => {
    const lat = Number(h?.lat);
    const lng = Number(h?.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
    if (lat < south || lat > north) return false;
    if (crossesAntimeridian) {
      return lng >= west || lng <= east;
    }
    return lng >= west && lng <= east;
  });
}

/**
 * City/region aggregates for low-zoom map (derived from snapshot hotels).
 */
export function aggregateMapHotelsByCity(hotels) {
  const cityGroups = Object.create(null);
  for (const hotel of hotels || []) {
    const city = String(hotel.city || "").trim() || "Unknown";
    const country = String(hotel.country || "").trim() || "";
    const cityKey = (city + "|" + country).toLowerCase();
    if (!cityGroups[cityKey]) {
      cityGroups[cityKey] = {
        city,
        country,
        totalHotels: 0,
        totalRooms: 0,
        withCoordinates: 0,
        withoutCoordinates: 0,
        openHotels: 0,
        pipelineHotels: 0,
        candidateHotels: 0,
        lat: null,
        lng: null,
      };
    }
    const row = cityGroups[cityKey];
    row.totalHotels += 1;
    row.totalRooms += Number(hotel.rooms) || 0;
    const status = String(hotel.status || "").toLowerCase();
    if (status === "open") row.openHotels += 1;
    else if (status === "pipeline") row.pipelineHotels += 1;
    else if (status === "candidate") row.candidateHotels += 1;
    const lat = Number(hotel.lat);
    const lng = Number(hotel.lng);
    const hasCoords =
      Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);
    if (hasCoords) {
      row.withCoordinates += 1;
      if (!Number.isFinite(row.lat) || !Number.isFinite(row.lng)) {
        row.lat = lat;
        row.lng = lng;
      }
    } else {
      row.withoutCoordinates += 1;
    }
  }
  return Object.values(cityGroups).filter(
    (r) => Number.isFinite(r.lat) && Number.isFinite(r.lng)
  );
}

export function noteCensusMapFallbackUsed() {
  metrics.fallbackUsed += 1;
}

/**
 * Single-flight publish (for boot / periodic / post-apply).
 */
export async function rebuildCensusMapSnapshotSingleFlight(opts = {}) {
  if (buildInflight) return buildInflight;
  buildInflight = (async () => {
    try {
      return await publishCensusMapSnapshot(opts);
    } finally {
      buildInflight = null;
    }
  })();
  return buildInflight;
}

/**
 * Boot helper: preload disk; optionally rebuild if missing/stale.
 */
export async function bootCensusMapSnapshot({
  rebuildIfMissing = true,
  rebuildIfStale = false,
} = {}) {
  if (!censusMapSnapshotEnabled()) {
    return { ok: true, skipped: true, reason: "snapshot_disabled" };
  }
  const pre = preloadCensusMapSnapshot({ allowLastGood: true });
  if (pre.ok) {
    const stale = (pre.ageMs || 0) > censusMapSnapshotMaxAgeMs();
    if (stale && rebuildIfStale) {
      console.log("[census-map-snapshot] boot: stale — rebuilding in background");
      rebuildCensusMapSnapshotSingleFlight().catch((err) => {
        console.error("[census-map-snapshot] background rebuild failed:", err?.message || err);
      });
    }
    console.log(
      `[census-map-snapshot] boot preload ok from=${pre.from} records=${pre.recordCount} bytes=${pre.bytes} bootMs=${pre.bootMs} ageMs=${pre.ageMs}`
    );
    return { ok: true, ...pre };
  }
  if (!rebuildIfMissing) {
    console.warn("[census-map-snapshot] boot: no snapshot on disk");
    return { ok: false, ...pre };
  }
  console.log("[census-map-snapshot] boot: missing — building from Airtable (blocking once)");
  try {
    const built = await rebuildCensusMapSnapshotSingleFlight();
    console.log(
      `[census-map-snapshot] boot build ok records=${built.recordCount} buildMs=${built.buildDurationMs}`
    );
    return { ok: true, built, bootMs: metrics.bootPreloadMs };
  } catch (err) {
    console.error("[census-map-snapshot] boot build failed:", err?.message || err);
    return { ok: false, reason: "build_failed", error: err?.message || String(err) };
  }
}

export function startCensusMapSnapshotPeriodicRebuild() {
  if (!censusMapSnapshotEnabled()) return null;
  const ms = parseInt(process.env.CENSUS_MAP_SNAPSHOT_PERIODIC_MS || "", 10);
  // Default 30 min safety rebuild; 0 disables.
  const interval = Number.isFinite(ms) ? ms : 30 * 60 * 1000;
  if (interval <= 0) return null;
  const timer = setInterval(() => {
    rebuildCensusMapSnapshotSingleFlight().catch((err) => {
      console.error("[census-map-snapshot] periodic rebuild failed:", err?.message || err);
    });
  }, interval);
  if (typeof timer.unref === "function") timer.unref();
  console.log(`[census-map-snapshot] periodic rebuild every ${interval}ms`);
  return timer;
}

/** Hook for census apply scripts — fire-and-forget rebuild. */
export function scheduleCensusMapSnapshotRebuildAfterApply(reason = "census_apply") {
  if (!censusMapSnapshotEnabled()) return;
  if (!envFlag("CENSUS_MAP_SNAPSHOT_REBUILD_ON_APPLY", true)) return;
  console.log(`[census-map-snapshot] scheduling rebuild after ${reason}`);
  setTimeout(() => {
    rebuildCensusMapSnapshotSingleFlight().catch((err) => {
      console.error("[census-map-snapshot] post-apply rebuild failed:", err?.message || err);
    });
  }, 0);
}

export function snapshotFileStats() {
  const out = { current: null, lastGood: null };
  for (const [key, path] of [
    ["current", CENSUS_MAP_CURRENT_PATH],
    ["lastGood", CENSUS_MAP_LAST_GOOD_PATH],
  ]) {
    if (!existsSync(path)) continue;
    const pointer = readJson(path);
    const rel = pointer?.snapshotFile;
    const full = rel ? join(CENSUS_MAP_SNAPSHOT_DIR, rel) : null;
    out[key] = {
      pointer,
      snapshotBytes: full && existsSync(full) ? statSync(full).size : null,
    };
  }
  return out;
}
