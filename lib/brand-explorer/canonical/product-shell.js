/**
 * Canonical Brand Explorer product shell.
 * Tab ids and labels come from configs/brand-explorer-product-shell.json,
 * which matches public/js/brand-explorer-atelier-from-api.js ATELIER_TAB_DEFS.
 */
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "../../..");

export function repoRoot() {
  return ROOT;
}

export function loadJson(name) {
  return JSON.parse(readFileSync(join(ROOT, "configs", name), "utf8"));
}

export const PRODUCT_SHELL = loadJson("brand-explorer-product-shell.json");
export const PRESENTATION_CONTRACT = loadJson("brand-explorer-presentation-contract.json");
export const COLLECTION_CONTRACT = loadJson("brand-explorer-collection-cardinality-contract.json");
export const GEOMETRY_CONTRACT = loadJson("brand-explorer-visual-geometry-contract.json");
export const CONTENT_CONTRACT = loadJson("brand-explorer-content-item-contract.json");
export const LESSON_REGISTRY = loadJson("brand-explorer-quality-lessons.json");

export const CANONICAL_TABS = Object.freeze(PRODUCT_SHELL.tabs);
export const CANONICAL_TAB_IDS = Object.freeze(CANONICAL_TABS.map((t) => t.tabId));

export function assertCanonicalTabParity(tabIds = []) {
  const expected = CANONICAL_TAB_IDS.join("|");
  const actual = tabIds.join("|");
  return {
    ok: actual === expected && tabIds.length === 10,
    expected: [...CANONICAL_TAB_IDS],
    actual: [...tabIds],
  };
}

export function canonicalRegionLabels() {
  return PRESENTATION_CONTRACT.geography.regions.map((r) => r.label);
}

export function collectionById(id) {
  return COLLECTION_CONTRACT.collections.find((c) => c.id === id) || null;
}
