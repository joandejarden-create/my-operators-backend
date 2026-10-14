/**
 * GDI Parallel provider feature flags.
 * Default OFF. Property-scoped activation — San Diego first rollout only.
 */

export const GDI_PARALLEL_ENABLED_ENV = "GDI_PARALLEL_ENABLED";
export const GDI_PARALLEL_HOTEL_IDS_ENV = "GDI_PARALLEL_HOTEL_IDS";
export const GDI_PARALLEL_KILL_SWITCH_ENV = "GDI_PARALLEL_KILL_SWITCH";
export const GDI_PARALLEL_SURFE_FALLBACK_ENV = "GDI_PARALLEL_SURFE_FALLBACK_ENABLED";
export const GDI_JEV_ADVISORY_ENV = "GDI_JEV_ADVISORY_ENABLED";

/** First controlled rollout property. */
export const GDI_PARALLEL_SAN_DIEGO_HOTEL_ID = "recnPB3Q02NO8Ma1o";

export const GDI_PARALLEL_DEFAULT_HOTEL_ALLOWLIST = Object.freeze([
  GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
]);

function truthy(v) {
  const s = String(v ?? "")
    .trim()
    .toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}

function explicitlyFalse(v) {
  const s = String(v ?? "")
    .trim()
    .toLowerCase();
  return s === "0" || s === "false" || s === "no" || s === "off";
}

export function isGdiParallelKillSwitchOn(env = process.env) {
  return truthy(env[GDI_PARALLEL_KILL_SWITCH_ENV]);
}

/**
 * Master enable for Parallel provider capability.
 * Default OFF — must set GDI_PARALLEL_ENABLED=1.
 */
export function isGdiParallelEnabled(env = process.env) {
  if (isGdiParallelKillSwitchOn(env)) return false;
  if (explicitlyFalse(env[GDI_PARALLEL_ENABLED_ENV])) return false;
  return truthy(env[GDI_PARALLEL_ENABLED_ENV]);
}

export function getGdiParallelHotelAllowlist(env = process.env) {
  const raw = String(env[GDI_PARALLEL_HOTEL_IDS_ENV] || "").trim();
  if (!raw) return [...GDI_PARALLEL_DEFAULT_HOTEL_ALLOWLIST];
  return raw
    .split(/[,;\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function isGdiParallelHotelAllowed(hotelId, env = process.env) {
  const id = String(hotelId || "").trim();
  if (!id) return false;
  if (!isGdiParallelEnabled(env)) return false;
  return getGdiParallelHotelAllowlist(env).includes(id);
}

/** Surfe fallback for Parallel contact sequence — default OFF. */
export function isGdiParallelSurfeFallbackEnabled(env = process.env) {
  if (!isGdiParallelEnabled(env)) return false;
  return truthy(env[GDI_PARALLEL_SURFE_FALLBACK_ENV]);
}

/** Jev advisory — default OFF; never a gate. */
export function isGdiJevAdvisoryEnabled(env = process.env) {
  return truthy(env[GDI_JEV_ADVISORY_ENV]);
}

export function getGdiParallelFlagState(env = process.env) {
  return {
    parallelEnabled: isGdiParallelEnabled(env),
    killSwitch: isGdiParallelKillSwitchOn(env),
    hotelAllowlist: getGdiParallelHotelAllowlist(env),
    surfeFallbackEnabled: isGdiParallelSurfeFallbackEnabled(env),
    jevAdvisoryEnabled: isGdiJevAdvisoryEnabled(env),
    defaultOff: true,
    envKeys: {
      enabled: GDI_PARALLEL_ENABLED_ENV,
      hotels: GDI_PARALLEL_HOTEL_IDS_ENV,
      killSwitch: GDI_PARALLEL_KILL_SWITCH_ENV,
      surfe: GDI_PARALLEL_SURFE_FALLBACK_ENV,
      jev: GDI_JEV_ADVISORY_ENV,
    },
  };
}
