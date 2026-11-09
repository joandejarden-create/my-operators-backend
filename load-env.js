/**
 * Load .env and .env.local BEFORE any Airtable-dependent modules.
 * Must be imported first in server.js so process.env is populated before intake-deal.js loads.
 *
 * Also loads the shared Dealality local GDI publication mirror config when present:
 *   C:/Dev/dealality/_dealality-local-env/gdi-published-inventory-dev.env
 * This keeps approved inventory available across worktrees without copying registries.
 * Never loaded when NODE_ENV=production.
 */
import fs from "node:fs";
import path from "node:path";
import dotenv from "dotenv";

dotenv.config({ path: ".env", override: false });
dotenv.config({ path: ".env.local", override: true });

const isProd = String(process.env.NODE_ENV || "").trim() === "production";
if (!isProd) {
  // Shared Exa credential across GDI research worktrees (gitignored local store).
  // Does not override an already-set process EXA_API_KEY / EXA_KEY.
  const sharedExaEnv = path.resolve(
    "C:/Dev/dealality/_dealality-local-env/exa-dev.env"
  );
  if (fs.existsSync(sharedExaEnv)) {
    dotenv.config({ path: sharedExaEnv, override: false });
  }
  const sharedGdiEnv = path.resolve(
    "C:/Dev/dealality/_dealality-local-env/gdi-published-inventory-dev.env"
  );
  if (fs.existsSync(sharedGdiEnv)) {
    dotenv.config({ path: sharedGdiEnv, override: false });
  }
  // Well-known Windows/dev default when unset: shared mirror outside any worktree.
  if (!String(process.env.GDI_PUBLISHED_INVENTORY_ROOT || "").trim()) {
    const wellKnown = path.resolve(
      "C:/Dev/dealality/_dealality-gdi-published-inventory-dev"
    );
    if (fs.existsSync(path.join(wellKnown, "registry", "current-registry-v1.json"))) {
      process.env.GDI_PUBLISHED_INVENTORY_ROOT = wellKnown;
      if (!String(process.env.GDI_PUBLISHED_INVENTORY_REGISTRY_V1 || "").trim()) {
        process.env.GDI_PUBLISHED_INVENTORY_REGISTRY_V1 = "1";
      }
      if (!String(process.env.GDI_PUBLISHED_INVENTORY_HOTEL_IDS || "").trim()) {
        process.env.GDI_PUBLISHED_INVENTORY_HOTEL_IDS = "recKRJjcPnb4tVDDS";
      }
      process.env.GDI_PUBLISHED_INVENTORY_DEV_MIRROR = "1";
    }
  }
}
