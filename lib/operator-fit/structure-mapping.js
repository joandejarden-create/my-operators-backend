/**
 * Operating-structure canonicalization (mapping only — no Airtable renames).
 */

import { OPERATING_STRUCTURE_CANONICAL, PRESERVED_OPERATING_STRUCTURE_VALUES } from "./config.js";

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * @param {string} raw
 * @returns {{ canonicalKey: string|null, preservedLabel: string|null, raw: string }}
 */
export function mapOperatingStructureValue(raw) {
  const rawStr = String(raw || "").trim();
  if (!rawStr) return { canonicalKey: null, preservedLabel: null, raw: rawStr };
  const n = norm(rawStr);
  for (const [key, def] of Object.entries(OPERATING_STRUCTURE_CANONICAL)) {
    for (const alias of def.aliases || []) {
      const a = norm(alias);
      if (n === a || n.includes(a) || a.includes(n)) {
        return {
          canonicalKey: key,
          preservedLabel: def.preserved || def.display || rawStr,
          raw: rawStr,
        };
      }
    }
  }
  return { canonicalKey: null, preservedLabel: rawStr, raw: rawStr };
}

/**
 * @param {string[]} values
 * @returns {string[]} unique canonical keys
 */
export function mapOperatingStructureList(values) {
  const keys = [];
  const seen = new Set();
  for (const v of values || []) {
    const m = mapOperatingStructureValue(v);
    if (m.canonicalKey && !seen.has(m.canonicalKey)) {
      seen.add(m.canonicalKey);
      keys.push(m.canonicalKey);
    }
  }
  return keys;
}

/**
 * Build owner preferred structure keys from deal SI / legacy labels.
 */
export function ownerStructureKeysFromProject(project) {
  const raw = [];
  const prefs = project?.operatingStructurePreferences;
  if (prefs && Array.isArray(prefs.value)) raw.push(...prefs.value);
  else if (prefs && typeof prefs.value === "string") raw.push(prefs.value);
  return mapOperatingStructureList(raw);
}

export function preservedStructureCatalog() {
  return [...PRESERVED_OPERATING_STRUCTURE_VALUES];
}
