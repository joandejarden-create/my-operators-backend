/**
 * Resolve durable root for GDI published inventory registry.
 *
 * Priority:
 *  1. explicit options.root
 *  2. GDI_PUBLISHED_INVENTORY_ROOT (Railway volume mount or local explicit root)
 *  3. Shared local dev mirror (NODE_ENV !== production only):
 *       GDI_PUBLISHED_INVENTORY_DEV_MIRROR_ROOT
 *       or C:/Dev/dealality/_dealality-gdi-published-inventory-dev
 *  4. GDI_DATA_ROOT/published-inventory (dev default under GDI data tree)
 *  5. Repo-relative data/group-demand-intelligence/published-inventory
 *
 * Production recommendation: mount a Railway volume and set
 * GDI_PUBLISHED_INVENTORY_ROOT to that mount path (survives redeploy).
 * The shared local mirror must never be used when NODE_ENV=production.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getGdiDataRoot } from "../repository.js";

export const GDI_PUBLISHED_INVENTORY_REGISTRY_V1 =
  "gdi-published-inventory-registry-v1";

export const GDI_LOCAL_DEV_PUBLISHED_INVENTORY_MIRROR_DEFAULT =
  "C:/Dev/dealality/_dealality-gdi-published-inventory-dev";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_DEFAULT = path.resolve(
  __dirname,
  "../../../data/group-demand-intelligence/published-inventory"
);

function registryExists(root) {
  try {
    return fs.existsSync(path.join(root, "registry", "current-registry-v1.json"));
  } catch (_e) {
    return false;
  }
}

export function resolveGdiPublishedInventoryRoot(env = process.env, opts = {}) {
  if (opts.root) return path.resolve(String(opts.root));
  const explicit = String(env.GDI_PUBLISHED_INVENTORY_ROOT || "").trim();
  if (explicit) return path.resolve(explicit);

  const isProd = String(env.NODE_ENV || "").trim() === "production";
  if (!isProd) {
    const mirrorEnv = String(env.GDI_PUBLISHED_INVENTORY_DEV_MIRROR_ROOT || "").trim();
    const mirror = path.resolve(
      mirrorEnv || GDI_LOCAL_DEV_PUBLISHED_INVENTORY_MIRROR_DEFAULT
    );
    if (registryExists(mirror)) return mirror;
  }

  const dataRoot = getGdiDataRoot(env);
  if (dataRoot) {
    return path.join(path.resolve(dataRoot), "published-inventory");
  }
  return REPO_DEFAULT;
}

export function isGdiPublishedInventoryRegistryEnabled(env = process.env) {
  const v = String(env.GDI_PUBLISHED_INVENTORY_REGISTRY_V1 || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

/**
 * Parse hotel allowlist for customer-facing registry reads.
 * Empty / unset allowlist ⇒ no hotels (safe default; Spice-only requires explicit ID).
 */
export function parseGdiPublishedInventoryHotelAllowlist(env = process.env) {
  return String(env.GDI_PUBLISHED_INVENTORY_HOTEL_IDS || "")
    .split(/[,;\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * Customer-facing registry merge is enabled only when the global flag is on
 * AND the hotelId is explicitly listed in GDI_PUBLISHED_INVENTORY_HOTEL_IDS.
 */
export function isGdiPublishedInventoryRegistryEnabledForHotel(
  hotelId,
  env = process.env
) {
  if (!isGdiPublishedInventoryRegistryEnabled(env)) return false;
  const id = String(hotelId || "").trim();
  if (!id) return false;
  const allow = parseGdiPublishedInventoryHotelAllowlist(env);
  if (!allow.length) return false;
  return allow.includes(id);
}

/**
 * Diagnose why a hotel may appear empty when registry reads are expected.
 * Never mutates data. Safe for API responses (no secrets / absolute prod paths in prod).
 */
export function diagnoseGdiPublishedInventoryRead(hotelId, env = process.env) {
  const id = String(hotelId || "").trim();
  const root = resolveGdiPublishedInventoryRoot(env);
  const enabled = isGdiPublishedInventoryRegistryEnabled(env);
  const forHotel = isGdiPublishedInventoryRegistryEnabledForHotel(id, env);
  const registryPath = path.join(root, "registry", "current-registry-v1.json");
  const registryPresent = registryExists(root);
  let hotelEntryCount = null;
  let registryParseError = null;
  if (registryPresent) {
    try {
      const doc = JSON.parse(fs.readFileSync(registryPath, "utf8"));
      const hotel = doc?.hotels?.[id];
      const opps = hotel?.opportunities || {};
      hotelEntryCount = Object.keys(opps).length;
    } catch (e) {
      registryParseError = String(e?.message || e);
    }
  }
  let code = "OK";
  if (!enabled) code = "REGISTRY_FLAG_OFF";
  else if (!forHotel) code = "HOTEL_NOT_IN_ALLOWLIST";
  else if (!registryPresent) code = "REGISTRY_UNAVAILABLE";
  else if (registryParseError) code = "REGISTRY_PARSE_ERROR";
  else if (hotelEntryCount === 0) code = "MISSING_PUBLICATION_POINTER";
  else if (hotelEntryCount == null) code = "REGISTRY_UNAVAILABLE";

  return {
    ok: code === "OK",
    code,
    hotelId: id || null,
    registryEnabled: enabled,
    hotelAllowlisted: forHotel,
    registryPresent,
    hotelPublishedOpportunityCount: hotelEntryCount,
    registryParseError,
    // Basename only in non-prod diagnostics — avoid leaking full paths in production.
    inventoryRootBasename:
      String(env.NODE_ENV || "").trim() === "production"
        ? null
        : path.basename(root),
    usingDevMirror:
      String(env.NODE_ENV || "").trim() !== "production" &&
      path.resolve(root) ===
        path.resolve(
          String(env.GDI_PUBLISHED_INVENTORY_DEV_MIRROR_ROOT || "").trim() ||
            GDI_LOCAL_DEV_PUBLISHED_INVENTORY_MIRROR_DEFAULT
        ),
  };
}
