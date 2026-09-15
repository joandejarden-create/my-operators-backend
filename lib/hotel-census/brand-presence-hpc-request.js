/**
 * P8.5 / P84-C / P8.7 / P8.8 — product-scoped request gate for Brand Presence HPC V2.
 *
 * BRAND_PRESENCE_HPC_V2=1 enables HPC capability.
 * RADAR_HPC_V2=1 enables Radar production HPC when product=radar.
 * SCOUT_HPC_V2=1 enables Scout HPC when product=scout (shadow / future cutover).
 *
 * HPC when:
 *   - HE: product=hotel-explorer|he and/or censusSource=hpc (not product=radar|scout)
 *   - Radar: product=radar AND RADAR_HPC_V2=1 (plus BRAND_PRESENCE_HPC_V2=1)
 *   - Scout census APIs: product=scout AND SCOUT_HPC_V2=1 (see shouldUseHpcScoutCensus)
 *
 * Scout without SCOUT opt-in → Legacy. BRAND_PRESENCE alone must never switch Scout.
 * No silent Legacy table reads on the opted-in HPC path.
 * No global product switch — request-level only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isBrandPresenceHpcV2Enabled, BRAND_PRESENCE_HPC_V2_FLAG } from "./brand-presence-hpc-adapter.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_MAP_PATH = path.resolve(
  __dirname,
  "../../config/hotel-census/legacy-to-hpc-id-map.json"
);

export const RADAR_HPC_V2_FLAG = "RADAR_HPC_V2";
export const SCOUT_HPC_V2_FLAG = "SCOUT_HPC_V2";
export const BRAND_EXPLORER_HPC_V2_FLAG = "BRAND_EXPLORER_HPC_V2";
export const OPERATOR_EXPLORER_HPC_V2_FLAG = "OPERATOR_EXPLORER_HPC_V2";
export const OPERATOR_INTELLIGENCE_HPC_V2_FLAG = "OPERATOR_INTELLIGENCE_HPC_V2";

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

function envFlagOn(env, name) {
  const v = String(env?.[name] || "").trim();
  return v === "1" || /^true$/i.test(v) || /^yes$/i.test(v);
}

/** Radar-specific production HPC enablement (independent of Scout; HE uses BRAND_PRESENCE only). */
export function isRadarHpcV2Enabled(env = process.env) {
  return envFlagOn(env, RADAR_HPC_V2_FLAG);
}

/** Scout-specific HPC enablement (independent of Radar; never inherited from BRAND_PRESENCE alone). */
export function isScoutHpcV2Enabled(env = process.env) {
  return envFlagOn(env, SCOUT_HPC_V2_FLAG);
}

/** Brand Explorer census metrics HPC enablement (independent of HE/Radar/Scout). */
export function isBrandExplorerHpcV2Enabled(env = process.env) {
  return envFlagOn(env, BRAND_EXPLORER_HPC_V2_FLAG);
}

/** Operator Explorer census footprint HPC enablement (independent of BE/HE/Radar/Scout). */
export function isOperatorExplorerHpcV2Enabled(env = process.env) {
  return envFlagOn(env, OPERATOR_EXPLORER_HPC_V2_FLAG);
}

/** Operator Intelligence HPC enablement (independent of OE; default OFF). */
export function isOperatorIntelligenceHpcV2Enabled(env = process.env) {
  return envFlagOn(env, OPERATOR_INTELLIGENCE_HPC_V2_FLAG);
}

/**
 * True when this Brand Presence request should use HPC (flag ON + product/censusSource opt-in).
 * Radar requires RADAR_HPC_V2=1. Scout/BE product on brand-presence stay Legacy.
 */
export function shouldUseHpcBrandPresence(req, env = process.env) {
  if (!isBrandPresenceHpcV2Enabled(env)) return false;
  const censusSource = queryValue(req, "censusSource") || headerValue(req, "x-dealality-census-source");
  const product = queryValue(req, "product") || headerValue(req, "x-dealality-product");

  if (product === "radar") {
    return isRadarHpcV2Enabled(env);
  }
  if (product === "scout" || product === "brand-explorer" || product === "be") return false;
  if (product === "hotel-explorer" || product === "he") return true;
  if (censusSource === "hpc") return true;
  return false;
}

/**
 * Scout census APIs (market-coverage / market-map / signals / insights).
 * Requires BRAND_PRESENCE_HPC_V2 + SCOUT_HPC_V2 + product=scout.
 */
export function shouldUseHpcScoutCensus(req, env = process.env) {
  if (!isBrandPresenceHpcV2Enabled(env)) return false;
  if (!isScoutHpcV2Enabled(env)) return false;
  const product = queryValue(req, "product") || headerValue(req, "x-dealality-product");
  if (product === "scout") return true;
  return false;
}

/**
 * Brand Explorer censusSummary path (brand-library only).
 * Requires BRAND_PRESENCE_HPC_V2 + BRAND_EXPLORER_HPC_V2.
 * When both flags are on, brand-library census metrics use HPC (production cutover).
 * Explicit product=brand-explorer|be / beHpc=1 also accepted for harness clarity.
 */
export function shouldUseHpcBrandExplorerMetrics(req, env = process.env) {
  if (!isBrandPresenceHpcV2Enabled(env)) return false;
  if (!isBrandExplorerHpcV2Enabled(env)) return false;
  return true;
}

/**
 * Operator Explorer census footprint path.
 * Requires BRAND_PRESENCE_HPC_V2 + OPERATOR_EXPLORER_HPC_V2.
 * When both flags are on, OE footprint uses canonical relationships + HPC.
 * Request product=operator-explorer|oe / oeHpc=1 also accepted for harness.
 */
export function shouldUseHpcOperatorExplorerMetrics(req, env = process.env) {
  if (!isBrandPresenceHpcV2Enabled(env)) return false;
  if (!isOperatorExplorerHpcV2Enabled(env)) return false;
  const product = queryValue(req, "product") || headerValue(req, "x-dealality-product");
  const oeHpc = queryValue(req, "oeHpc") || queryValue(req, "useHpc");
  if (product === "operator-explorer" || product === "oe") return true;
  if (oeHpc === "1" || oeHpc === "true") return true;
  // Flag ON = OE footprint endpoints use HPC (production cutover posture; default OFF).
  return true;
}

export function heHpcOptInQuery() {
  return "censusSource=hpc&product=hotel-explorer";
}

export function radarHpcOptInQuery() {
  return "censusSource=hpc&product=radar";
}

export function scoutHpcOptInQuery() {
  return "censusSource=hpc&product=scout";
}

export function brandExplorerHpcOptInQuery() {
  return "censusSource=hpc&product=brand-explorer";
}

export function operatorExplorerHpcOptInQuery() {
  return "censusSource=hpc&product=operator-explorer";
}

export function operatorIntelligenceHpcOptInQuery() {
  return "censusSource=hpc&product=operator-intelligence";
}

/**
 * Operator Intelligence aggregation path.
 * Requires BRAND_PRESENCE_HPC_V2 + OPERATOR_INTELLIGENCE_HPC_V2.
 * Default OFF — production remains Legacy Management Company path.
 */
export function shouldUseHpcOperatorIntelligence(req, env = process.env) {
  if (!isBrandPresenceHpcV2Enabled(env)) return false;
  if (!isOperatorIntelligenceHpcV2Enabled(env)) return false;
  const product = queryValue(req, "product") || headerValue(req, "x-dealality-product");
  const oiHpc = queryValue(req, "oiHpc") || queryValue(req, "useHpc");
  if (product === "operator-intelligence" || product === "oi") return true;
  if (oiHpc === "1" || oiHpc === "true") return true;
  return true;
}

/** Public, non-secret runtime flags for product client defaults. */
export function getDealalityRuntimeFlags(env = process.env) {
  return {
    BRAND_PRESENCE_HPC_V2: isBrandPresenceHpcV2Enabled(env),
    RADAR_HPC_V2: isRadarHpcV2Enabled(env),
    SCOUT_HPC_V2: isScoutHpcV2Enabled(env),
    BRAND_EXPLORER_HPC_V2: isBrandExplorerHpcV2Enabled(env),
    OPERATOR_EXPLORER_HPC_V2: isOperatorExplorerHpcV2Enabled(env),
    OPERATOR_INTELLIGENCE_HPC_V2: isOperatorIntelligenceHpcV2Enabled(env),
    at: new Date().toISOString(),
  };
}

export { BRAND_PRESENCE_HPC_V2_FLAG, isBrandPresenceHpcV2Enabled };
