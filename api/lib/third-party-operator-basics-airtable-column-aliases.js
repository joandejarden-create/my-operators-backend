/**
 * Some Airtable bases use form-style column titles instead of the shorter names in
 * api/third-party-operator-intake.js. Map canonical → alternate so writes land in the right column.
 *
 * Pairs: [canonicalNameUsedByIntake, alternateNameSeenInBases]
 */

export const BASICS_COLUMN_CANONICAL_TO_ALTERNATE = [
  ["Avg Experience Years", "Average Years of Industry Experience"],
  ["Regional Teams", "Regional Management Teams"],
  ["Property Types", "Property Types Managed"],
  ["Additional Experience Types", "Additional Experience / Location Contexts"],
  ["Emergency Response", "Emergency Response Plan"],
  ["Business Continuity", "Business Continuity Planning"],
  ["24/7 Support", "24/7 Support Availability"],
  ["Crisis Experience", "Crisis Management Experience"],
  ["Certifications", "Certifications Held"],
  ["Energy Efficiency", "Energy Efficiency Initiatives"],
  ["Waste Reduction", "Waste Reduction Programs"],
  ["Carbon Tracking", "Carbon Footprint Tracking"],
];

/** @type {Map<string, { names: Set<string>; fetchedAt: number }>} */
const tableFieldNameCache = new Map();

const CACHE_TTL_MS = 5 * 60 * 1000;

function tableFieldCacheKey(baseId, tableName) {
  return `${baseId}\0${tableName}`;
}

/**
 * Field names for any table in the base (Metadata API). Cached per base + table.
 * @param {string} baseId
 * @param {string} apiKey
 * @param {string} tableName
 * @returns {Promise<Set<string> | null>}
 */
export async function fetchAirtableTableFieldNameSet(baseId, apiKey, tableName) {
  if (!baseId || !apiKey || !tableName) return null;
  const key = tableFieldCacheKey(baseId, tableName);
  const now = Date.now();
  const hit = tableFieldNameCache.get(key);
  if (hit && now - hit.fetchedAt < CACHE_TTL_MS) return hit.names;
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return null;
    const data = await res.json();
    const table = (data.tables || []).find((t) => t.name === tableName);
    if (!table || !table.fields) return null;
    const names = new Set((table.fields || []).map((f) => f.name).filter(Boolean));
    tableFieldNameCache.set(key, { names, fetchedAt: now });
    return names;
  } catch {
    return null;
  }
}

/**
 * @param {string} baseId
 * @param {string} apiKey
 * @param {string} tableName
 * @returns {Promise<Set<string> | null>}
 */
export async function fetchOperatorBasicsFieldNameSet(baseId, apiKey, tableName) {
  return fetchAirtableTableFieldNameSet(baseId, apiKey, tableName);
}

/**
 * When the base only defines the alternate column, move values off canonical keys
 * so Airtable accepts the payload (unknown field names error) and data is visible
 * under the columns you use in the UI.
 *
 * @param {Record<string, unknown>} fields
 * @param {Set<string>} schemaNameSet
 * @returns {Record<string, unknown>}
 */
function isEmptySchemaValue(v) {
  if (v == null) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

/**
 * Drop keys Airtable would reject on create/update for this table.
 * @param {Record<string, unknown>} fields
 * @param {Set<string>} schemaNameSet
 * @returns {Record<string, unknown>}
 */
export function filterFieldsToAirtableSchema(fields, schemaNameSet) {
  if (!schemaNameSet || schemaNameSet.size === 0) return { ...fields };
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (schemaNameSet.has(k)) out[k] = v;
  }
  return out;
}

export function remapBasicsFieldsForAirtableSchema(fields, schemaNameSet) {
  if (!schemaNameSet || schemaNameSet.size === 0) return { ...fields };
  const f = { ...fields };

  for (const [canonical, alternate] of BASICS_COLUMN_CANONICAL_TO_ALTERNATE) {
    if (!Object.prototype.hasOwnProperty.call(f, canonical)) continue;
    const v = f[canonical];
    if (isEmptySchemaValue(v)) continue;
    const hasCanonical = schemaNameSet.has(canonical);
    const hasAlternate = schemaNameSet.has(alternate);
    if (hasAlternate && !hasCanonical) {
      f[alternate] = v;
      delete f[canonical];
    }
  }
  return f;
}
