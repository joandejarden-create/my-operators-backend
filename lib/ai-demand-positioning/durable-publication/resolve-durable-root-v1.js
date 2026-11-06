/**
 * Resolve ADP durable published-report storage root + cutover mode.
 *
 * Modes (ADP_DURABLE_PUBLISHED_MODE):
 *   off      — legacy ephemeral published/ only (default)
 *   warm     — volume writable for migration; customer reads still ephemeral
 *   prefer   — read durable if property present, else ephemeral fallback
 *   required — durable only; fail closed (no ephemeral fallback)
 *
 * Safe activation: off → (volume) → code deploy → warm+migrate → prefer → required
 * Unsafe: required while volume empty → customer outage for unmigrated hotels
 */

import { existsSync, mkdirSync, accessSync, constants } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "../../..");

export const ADP_DURABLE_PUBLICATION_STORAGE_V1 = "ADP_DURABLE_PUBLICATION_STORAGE_V1";
export const DEFAULT_DURABLE_RELATIVE_ROOT = "data/adp-durable-published";

export const ADP_DURABLE_MODE = Object.freeze({
  OFF: "off",
  WARM: "warm",
  PREFER: "prefer",
  REQUIRED: "required",
});

export function resolveAdpDurablePublishedRoot(env = process.env) {
  const override = String(env.ADP_DURABLE_PUBLISHED_ROOT || "").trim();
  if (override) return override;
  return join(REPO_ROOT, DEFAULT_DURABLE_RELATIVE_ROOT);
}

/**
 * Canonical cutover mode. Backward compatible with ENABLED/REQUIRED flags.
 */
export function resolveAdpDurablePublishedMode(env = process.env) {
  const modeRaw = String(env.ADP_DURABLE_PUBLISHED_MODE || "").trim().toLowerCase();
  if (
    modeRaw === ADP_DURABLE_MODE.OFF ||
    modeRaw === ADP_DURABLE_MODE.WARM ||
    modeRaw === ADP_DURABLE_MODE.PREFER ||
    modeRaw === ADP_DURABLE_MODE.REQUIRED
  ) {
    return modeRaw;
  }

  // Legacy flags
  const required = String(env.ADP_DURABLE_PUBLISHED_REQUIRED ?? "0").trim().toLowerCase();
  if (required === "1" || required === "true" || required === "on" || required === "yes") {
    return ADP_DURABLE_MODE.REQUIRED;
  }
  const enabled = String(env.ADP_DURABLE_PUBLISHED_ENABLED ?? "").trim().toLowerCase();
  if (enabled === "1" || enabled === "true" || enabled === "on" || enabled === "yes") {
    return ADP_DURABLE_MODE.PREFER;
  }
  if (enabled === "0" || enabled === "false" || enabled === "off" || enabled === "no") {
    return ADP_DURABLE_MODE.OFF;
  }
  // Explicit root alone does not flip customer reads — stays off until MODE set.
  return ADP_DURABLE_MODE.OFF;
}

export function isAdpDurablePublishedStoreEnabled(env = process.env) {
  const mode = resolveAdpDurablePublishedMode(env);
  return mode !== ADP_DURABLE_MODE.OFF;
}

export function isAdpDurablePublishedStoreRequired(env = process.env) {
  return resolveAdpDurablePublishedMode(env) === ADP_DURABLE_MODE.REQUIRED;
}

/** Customer reads should consult durable store (prefer or required). */
export function shouldReadFromDurableStore(env = process.env) {
  const mode = resolveAdpDurablePublishedMode(env);
  return mode === ADP_DURABLE_MODE.PREFER || mode === ADP_DURABLE_MODE.REQUIRED;
}

/** Allow ephemeral fallback when durable entry missing (prefer/warm/off). */
export function allowEphemeralPublishedFallback(env = process.env) {
  return resolveAdpDurablePublishedMode(env) !== ADP_DURABLE_MODE.REQUIRED;
}

/** Writes should land in durable store (warm/prefer/required). */
export function shouldWriteToDurableStore(env = process.env) {
  const mode = resolveAdpDurablePublishedMode(env);
  return (
    mode === ADP_DURABLE_MODE.WARM ||
    mode === ADP_DURABLE_MODE.PREFER ||
    mode === ADP_DURABLE_MODE.REQUIRED
  );
}

export function ensureAdpDurablePublishedRoot(env = process.env) {
  const root = resolveAdpDurablePublishedRoot(env);
  const mode = resolveAdpDurablePublishedMode(env);
  const required = mode === ADP_DURABLE_MODE.REQUIRED;
  const enabled = mode !== ADP_DURABLE_MODE.OFF;

  if (!enabled && !required) {
    return { ok: true, enabled: false, required: false, mode, root, writable: false };
  }

  try {
    mkdirSync(root, { recursive: true });
    mkdirSync(join(root, "snapshots"), { recursive: true });
    mkdirSync(join(root, "registry"), { recursive: true });
    mkdirSync(join(root, "registry", "history"), { recursive: true });
    mkdirSync(join(root, "locks"), { recursive: true });
    mkdirSync(join(root, "staging"), { recursive: true });
    mkdirSync(join(root, "current"), { recursive: true });
    accessSync(root, constants.R_OK | constants.W_OK);
    return { ok: true, enabled: true, required, mode, root, writable: true };
  } catch (err) {
    if (required || enabled) {
      const e = new Error(
        `ADP durable published store unavailable at ${root}: ${err?.message || err}`
      );
      e.code = "ADP_DURABLE_STORE_UNAVAILABLE";
      e.root = root;
      e.mode = mode;
      throw e;
    }
    return {
      ok: false,
      enabled: false,
      required,
      mode,
      root,
      writable: false,
      error: String(err?.message || err),
    };
  }
}

export function assertDurableStoreReadyForProductionReads(env = process.env) {
  if (!shouldReadFromDurableStore(env) && !isAdpDurablePublishedStoreRequired(env)) {
    return { ok: true, skipped: true, mode: resolveAdpDurablePublishedMode(env) };
  }
  return ensureAdpDurablePublishedRoot(env);
}
