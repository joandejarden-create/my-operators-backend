/**
 * Census Autopilot coordinate provider gate.
 *
 * Mapbox Permanent Geocoding is the only approved provider for stored
 * Hotel Property Census Latitude / Longitude. Temporary geocoding and
 * public Nominatim are never used for production Census storage.
 */

import {
  GEOCODE_COST_PER_1K_USD,
  estimateGeocodeCostUsd,
  resolveGeocodingProvider,
} from "./production-census-geocoding-providers.js";

export const COORDINATE_PROVIDER_VERSION = "census-coordinate-provider-v1";

export const COORDINATE_COMPLETION_STATUSES = Object.freeze({
  READY_PROVIDER_NEEDED:
    "production_census_mapbox_coordinate_completion_ready_provider_needed",
  READY_FOR_PRODUCTION_CYCLE:
    "production_census_mapbox_coordinate_completion_ready_for_production_cycle",
  APPLIED_CLEAN:
    "production_census_mapbox_coordinate_completion_applied_clean",
  PARTIAL_PROVIDER_OR_STEWARD:
    "production_census_mapbox_coordinate_completion_partial_provider_or_steward_remaining",
  BLOCKED: "production_census_mapbox_coordinate_completion_blocked",
});

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function isTruthyEnvFlag(env, key) {
  return String(env?.[key] ?? "").trim() === "1";
}

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function maxGeocodeRequestsPerRun(env = process.env) {
  const hardCap = Number(env.MAX_MAPBOX_PERMANENT_REQUESTS);
  const runCap = Number(env.MAX_GEOCODE_REQUESTS_PER_RUN);
  const costCapUsd = Number(env.MAX_MAPBOX_PERMANENT_COST_USD || 55);
  const ratePer1k =
    Number(env.MAPBOX_PERMANENT_GEOCODING_COST_PER_1000) ||
    GEOCODE_COST_PER_1K_USD.mapbox_permanent ||
    5;
  const fromCost =
    Number.isFinite(costCapUsd) && costCapUsd > 0 && ratePer1k > 0
      ? Math.floor((costCapUsd * 1000) / ratePer1k)
      : Infinity;

  // Founder Mapbox Permanent wave: MAX_MAPBOX_PERMANENT_REQUESTS is authoritative
  // (do not let MAX_GEOCODE_REQUESTS_PER_RUN shrink below the approved wave cap).
  if (Number.isFinite(hardCap) && hardCap > 0) {
    let n = Math.floor(hardCap);
    if (Number.isFinite(fromCost)) n = Math.min(n, fromCost);
    return Math.max(1, n);
  }

  let n = Number.isFinite(runCap) && runCap > 0 ? Math.floor(runCap) : 250;
  if (Number.isFinite(fromCost)) n = Math.min(n, fromCost);
  return Math.max(1, Math.floor(n));
}

/**
 * Hard budget guard for Mapbox Permanent waves.
 * @param {number} requestsSoFar
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function evaluateMapboxBudgetGuard(requestsSoFar, env = process.env) {
  const maxReq = maxGeocodeRequestsPerRun(env);
  const cost = estimateMapboxPermanentCost(requestsSoFar, env);
  const costCap = Number(env.MAX_MAPBOX_PERMANENT_COST_USD || 55);
  const overRequests = requestsSoFar >= maxReq;
  const overCost =
    Number.isFinite(costCap) &&
    costCap > 0 &&
    Number(cost.estimated_usd || 0) >= costCap;
  return {
    ok: !overRequests && !overCost,
    paused: overRequests || overCost,
    status: overRequests || overCost ? "COORDINATE_LANE_PAUSED_BUDGET" : null,
    max_requests: maxReq,
    requests_so_far: requestsSoFar,
    estimated_usd: cost.estimated_usd,
    cost_cap_usd: costCap,
    reason: overRequests
      ? "max_mapbox_permanent_requests_reached"
      : overCost
        ? "max_mapbox_permanent_cost_usd_reached"
        : null,
  };
}

/**
 * Mapbox Permanent readiness for Autopilot coordinate_completion.
 * All three flags required — missing any → do not call Mapbox.
 *
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function evaluateMapboxPermanentReadiness(env = process.env) {
  const token = Boolean(
    String(env.MAPBOX_ACCESS_TOKEN || env.MAPBOX_TOKEN || "").trim()
  );
  const permanent = isTruthyEnvFlag(env, "MAPBOX_PERMANENT_GEOCODING");
  const completionEnabled = isTruthyEnvFlag(env, "CENSUS_COORDINATE_COMPLETION_ENABLED");
  const providerPref = String(env.GEOCODING_PROVIDER || "")
    .trim()
    .toLowerCase();
  // Coordinate completion is Mapbox-only. Block only when explicitly disabled.
  // GEOCODING_PROVIDER=google does not disable Mapbox Permanent for this queue
  // (Google is never used for Autopilot stored Census coordinates).
  const providerBlocksMapbox =
    providerPref === "none" ||
    providerPref === "nominatim" ||
    providerPref === "osm";

  const ready = token && permanent && completionEnabled && !providerBlocksMapbox;
  const missing = [];
  if (!token) missing.push("MAPBOX_ACCESS_TOKEN");
  if (!permanent) missing.push("MAPBOX_PERMANENT_GEOCODING=1");
  if (!completionEnabled) missing.push("CENSUS_COORDINATE_COMPLETION_ENABLED=1");
  if (providerBlocksMapbox) {
    missing.push(`GEOCODING_PROVIDER=${providerPref} blocks Mapbox Autopilot geocode`);
  }

  return {
    version: COORDINATE_PROVIDER_VERSION,
    provider: "mapbox",
    ready,
    mapbox_token_present: token,
    mapbox_permanent_geocoding: permanent,
    census_coordinate_completion_enabled: completionEnabled,
    geocoding_provider: providerPref || "auto",
    missing_flags: missing,
    temporary_geocoding_blocked: true,
    nominatim_blocked: true,
    max_geocode_requests_per_run: maxGeocodeRequestsPerRun(env),
    route: ready ? "geocode_apply" : "provider_decision_needed",
    block_reason: ready ? null : "mapbox_permanent_env_incomplete",
    note: ready
      ? "Mapbox Permanent Geocoding ready for Census coordinate completion."
      : "Mapbox Permanent flags incomplete — route eligible records to provider_decision_needed; continue other Autopilot queues.",
  };
}

/**
 * Broader Autopilot geocode gate (Mapbox Permanent + completion flag).
 * Google is not used for Autopilot stored Census coordinates.
 *
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function evaluateCensusCoordinateProviderReadiness(env = process.env) {
  const mapbox = evaluateMapboxPermanentReadiness(env);
  const info = resolveGeocodingProvider(
    String(env.GEOCODING_PROVIDER || "").trim() || undefined
  );

  return {
    ...mapbox,
    provider_info: info,
    approved_for_geocode_apply: mapbox.ready,
    preferred: "mapbox_permanent",
    google_not_used_for_autopilot_storage: true,
  };
}

/**
 * Cost estimate for Mapbox Permanent requests.
 * Uses MAPBOX_PERMANENT_GEOCODING_COST_PER_1000 when set; else documented default.
 *
 * @param {number} requestCount
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function estimateMapboxPermanentCost(requestCount, env = process.env) {
  const n = Number(requestCount) || 0;
  const custom = Number(env.MAPBOX_PERMANENT_GEOCODING_COST_PER_1000);
  if (Number.isFinite(custom) && custom >= 0) {
    return {
      requests: n,
      estimated_usd: Math.round((n * custom) / 1000 * 10000) / 10000,
      basis: "mapbox_permanent_env",
      rate_per_1k_usd: custom,
      pricing_configured: true,
    };
  }
  const fromShared = estimateGeocodeCostUsd(n, {
    provider: "mapbox",
    permanent_storage_enabled: true,
  });
  return {
    ...fromShared,
    rate_per_1k_usd: GEOCODE_COST_PER_1K_USD.mapbox_permanent,
    pricing_configured: false,
    note:
      fromShared.note ||
      "Pricing env unset — request count reported; default Mapbox Permanent order-of-magnitude used when estimating USD.",
  };
}
