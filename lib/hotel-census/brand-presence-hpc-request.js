/**
 * P8.5 — HE-only request gate for Brand Presence HPC V2.
 *
 * BRAND_PRESENCE_HPC_V2=1 enables capability.
 * HPC is used only when the request opts in (Hotel Explorer):
 *   ?censusSource=hpc | ?product=hotel-explorer
 *   headers: X-Dealality-Census-Source: hpc | X-Dealality-Product: hotel-explorer
 *
 * Scout / Radar omit the opt-in → remain on Legacy Hotel Census.
 * No silent Legacy table reads on the opted-in HPC path.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isBrandPresenceHpcV2Enabled, BRAND_PRESENCE_HPC_V2_FLAG } from "./brand-presence-hpc-adapter.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_MAP_PATH = path.resolve(
  __dirname,
  "../../data/hotel-census-v2/p85-he-cutover/legacy-to-hpc-id-map.json"
);

/** @type {{ hpc: number, legacy: number, legacyIdResolvedViaMap: number, hpcMiss: number }} */
export const brandPresenceReadCounters = {
  hpc: 0,
  legacy: 0,
  legacyIdResolvedViaMap: 0,
  hpcMiss: 0,
};

export function resetBrandPresenceReadCounters() {
  brandPresenceReadCounters.hpc = 0;
  brandPresenceReadCounters.legacy = 0;
  brandPresenceReadCounters.legacyIdResolvedViaMap = 0;
  brandPresenceReadCounters.hpcMiss = 0;
}

export function snapshotBrandPresenceReadCounters() {
  return { ...brandPresenceReadCounters, at: new Date().toISOString() };
}

let _legacyToHpcMap = null;

export function loadLegacyToHpcIdMap(mapPath = process.env.BRAND_PRESENCE_LEGACY_HPC_MAP || DEFAULT_MAP_PATH) {
  if (_legacyToHpcMap) return _legacyToHpcMap;
  try {
    if (!fs.existsSync(mapPath)) {
      _legacyToHpcMap = Object.freeze({});
      return _legacyToHpcMap;
    }
    const raw = JSON.parse(fs.readFileSync(mapPath, "utf8"));
    const map = raw && typeof raw.map === "object" ? raw.map : raw;
    _legacyToHpcMap = Object.freeze(map && typeof map === "object" ? map : {});
    return _legacyToHpcMap;
  } catch (err) {
    console.error("[brand-presence-hpc] failed to load legacy→HPC map:", err?.message || err);
    _legacyToHpcMap = Object.freeze({});
    return _legacyToHpcMap;
  }
}

/** Resolve Legacy Airtable rec → HPC rec via frozen crosswalk (not a Legacy table read). */
export function resolveLegacyIdToHpcId(legacyId) {
  const map = loadLegacyToHpcIdMap();
  const hpcId = map[String(legacyId || "").trim()];
  return hpcId && String(hpcId).startsWith("rec") ? hpcId : null;
}

function headerValue(req, name) {
  if (!req || !req.headers) return "";
  const key = String(name).toLowerCase();
  const raw = req.headers[key] ?? req.headers[name];
  return String(raw || "").trim().toLowerCase();
}

function queryValue(req, name) {
  if (!req || !req.query) return "";
  return String(req.query[name] || "").trim().toLowerCase();
}

/**
 * True when this request should use HPC (flag ON + HE opt-in).
 */
export function shouldUseHpcBrandPresence(req, env = process.env) {
  if (!isBrandPresenceHpcV2Enabled(env)) return false;
  const censusSource = queryValue(req, "censusSource") || headerValue(req, "x-dealality-census-source");
  const product = queryValue(req, "product") || headerValue(req, "x-dealality-product");
  if (censusSource === "hpc") return true;
  if (product === "hotel-explorer" || product === "he") return true;
  return false;
}

export function heHpcOptInQuery() {
  return "censusSource=hpc&product=hotel-explorer";
}

export { BRAND_PRESENCE_HPC_V2_FLAG, isBrandPresenceHpcV2Enabled };
