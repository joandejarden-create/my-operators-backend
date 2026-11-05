/**
 * Resolve durable root for GDI published inventory registry.
 *
 * Priority:
 *  1. explicit options.root
 *  2. GDI_PUBLISHED_INVENTORY_ROOT (Railway volume mount or local test root)
 *  3. GDI_DATA_ROOT/published-inventory (dev default under GDI data tree)
 *
 * Production recommendation: mount a Railway volume and set
 * GDI_PUBLISHED_INVENTORY_ROOT to that mount path (survives redeploy).
 * Airtable remains the preferred multi-hotel SoT for weekly writes; this FS
 * root is the local/simulation + volume mirror adapter.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import { getGdiDataRoot } from "../repository.js";

export const GDI_PUBLISHED_INVENTORY_REGISTRY_V1 =
  "gdi-published-inventory-registry-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_DEFAULT = path.resolve(
  __dirname,
  "../../../data/group-demand-intelligence/published-inventory"
);

export function resolveGdiPublishedInventoryRoot(env = process.env, opts = {}) {
  if (opts.root) return path.resolve(String(opts.root));
  const explicit = String(env.GDI_PUBLISHED_INVENTORY_ROOT || "").trim();
  if (explicit) return path.resolve(explicit);
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
