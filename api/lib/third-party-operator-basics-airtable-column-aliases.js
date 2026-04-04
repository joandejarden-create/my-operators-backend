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
  ["Business Continuity Planning", "Business Continuity"],
  ["Support 24/7 Availability", "24/7 Support"],
  ["Crisis Management Experience", "Crisis Experience"],
  ["Certifications", "Certifications Held"],
  ["Energy Efficiency Initiatives", "Energy Efficiency"],
  ["Waste Reduction Programs", "Waste Reduction"],
  ["Carbon Footprint Tracking", "Carbon Tracking"],
  /** After duplicate-column cleanup; older bases may still use the left-hand titles only. */
  ["Primary Contact Email", "Contact Email"],
  ["Primary Contact Phone", "Contact Phone"],
  ["Headquarters Location", "Headquarters"],
  ["Average Contract Renewal Rate", "Renewal Rate"],
  /** Canonical (post–field-corrections) → older duplicate labels still seen in some bases */
  ["EU Existing Rooms", "Geo EU Existing Rooms"],
  ["EU Pipeline Rooms", "Geo EU Pipeline Rooms"],
  ["Luxury Avg Staff", "Luxury Avg On-Site Staff Per Property"],
  ["Average Occupancy Improvement", "Occupancy Improvement"],
  ["Markets To Avoid", "Markets to Avoid"],
  ["Milestone Operator Selection Min Months", "Milestone Min Months - First Discussion to Operator Selection"],
  ["Milestone Construction Start Min Months", "Milestone Min Months - Operator Selection to Construction Start"],
  ["Milestone Soft Opening Min Months", "Milestone Min Months - Pre-Opening Ramp to Soft Opening"],
  ["Milestone Grand Opening Min Months", "Milestone Min Months - Soft Opening to Grand Opening"],
  ["Pre-opening Experience", "Pre-Opening Experience"],
  ["Stabilized / Ongoing-Operations Experience", "Stabilized Experience"],
  ["Renovation/Rebrand Experience", "Renovation Experience"],
  ["Typical Response Time for Owner Inquiries", "Typical Owner Response Time"],
  ["# of Exits / Deflaggings (Units) in Past 24 Months", "Exits/Deflaggings (Past 24 Months)"],
  ["Average NOI Improvement", "NOI Improvement"],
  ["Certifications", "Certifications Held"],
  ["Red Flag Items That Typically Make You Decline or Proceed With Caution", "Known Red Flag Items"],
  ["Key Differentiators", "Featured Differentiators"],
  ["Specific Markets/Cities", "Specific Markets"],
  ["Major Lenders Worked With", "Major Lenders"],
  ["Mixed-Use Development Allowed", "Mixed-Use Allowed"],
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

/**
 * Intake historically used duplicate / long geo & location column names. Maps those keys to the
 * surviving Basics column names (see docs/operator-setup-mapping/airtable-field-corrections.csv).
 * @param {Record<string, unknown>} fields
 * @returns {Record<string, unknown>}
 */
export function remapLegacyBasicsFieldKeysToCanonical(fields) {
  const f = { ...fields };
  for (const [from, to] of BASICS_LEGACY_FIELD_KEY_TO_CANONICAL) {
    if (!Object.prototype.hasOwnProperty.call(f, from)) continue;
    if (from === to) continue;
    const v = f[from];
    if (Object.prototype.hasOwnProperty.call(f, to)) {
      if (isEmptySchemaValue(f[to]) && !isEmptySchemaValue(v)) f[to] = v;
    } else {
      f[to] = v;
    }
    delete f[from];
  }
  return f;
}

/** @type {readonly [string, string][]} Legacy intake / duplicate column name → canonical Basics name */
const BASICS_LEGACY_FIELD_KEY_TO_CANONICAL = [
  ["Headquarters", "Headquarters Location"],
  ["Contact Email", "Primary Contact Email"],
  ["Contact Phone", "Primary Contact Phone"],
  ["Geo CALA Existing Hotels", "CALA Existing Hotels"],
  ["Geo CALA Existing Rooms", "CALA Existing Rooms"],
  ["Geo CALA Pipeline Hotels", "CALA Pipeline Hotels"],
  ["Geo CALA Pipeline Rooms", "CALA Pipeline Rooms"],
  ["Geo NA Existing Rooms", "NA Existing Rooms"],
  ["Geo NA Pipeline Rooms", "NA Pipeline Rooms"],
  ["Geo MEA Existing Rooms", "MEA Existing Rooms"],
  ["Geo MEA Pipeline Rooms", "MEA Pipeline Rooms"],
  ["Geo APAC Existing Rooms", "APAC Existing Rooms"],
  ["Geo APAC Pipeline Rooms", "APAC Pipeline Rooms"],
  ["Location Type % Urban", "Location Type Urban"],
  ["Location Type % Suburban", "Location Type Suburban"],
  // Matches airtable-field-corrections.csv: duplicate % columns mapped to surviving short names.
  ["Location Type % Resort", "Location Type Airport"],
  ["Location Type % Airport", "Location Type Resort"],
  ["Location Type % Small Metro/Town", "Location Type Highway"],
  ["Location Type % Interstate", "Location Type Other"],
  ["Location Type % Total", "Location Type Total"],
  ["Upper Upscale Avg On-Site Staff Per Property", "Upper Upscale Avg Staff"],
  ["Upper Midscale Avg On-Site Staff Per Property", "Upper Midscale Avg Staff"],
  ["Midscale Avg On-Site Staff Per Property", "Midscale Avg Staff"],
  ["Renewal Rate", "Average Contract Renewal Rate"],
  ["Geo EU Existing Rooms", "EU Existing Rooms"],
  ["Geo EU Pipeline Rooms", "EU Pipeline Rooms"],
  ["Luxury Avg On-Site Staff Per Property", "Luxury Avg Staff"],
  ["Occupancy Improvement", "Average Occupancy Improvement"],
  ["Markets to Avoid", "Markets To Avoid"],
  ["Milestone Min Months - First Discussion to Operator Selection", "Milestone Operator Selection Min Months"],
  ["Milestone Min Months - Operator Selection to Construction Start", "Milestone Construction Start Min Months"],
  ["Milestone Min Months - Pre-Opening Ramp to Soft Opening", "Milestone Soft Opening Min Months"],
  ["Milestone Min Months - Soft Opening to Grand Opening", "Milestone Grand Opening Min Months"],
  ["Pre-Opening Experience", "Pre-opening Experience"],
  ["Stabilized Experience", "Stabilized / Ongoing-Operations Experience"],
  ["Renovation Experience", "Renovation/Rebrand Experience"],
  ["Typical Owner Response Time", "Typical Response Time for Owner Inquiries"],
  ["Exits/Deflaggings (Past 24 Months)", "# of Exits / Deflaggings (Units) in Past 24 Months"],
  ["Emergency Response Plan", "Emergency Response"],
  ["Lender References", "Lender References Available"],
  ["Report Types", "Report Types Provided"],
  ["Owner Non-Negotiable Types", "Owner Non-Negotiables (Types)"],
  ["Reporting Frequency", "Financial Reporting Frequency"],
  ["Mobile Check-in", "Mobile Check-in Capability"],
  ["Analytics Platform", "Data Analytics Platform"],
  ["Decision Making Process", "Decision-Making Process"],
  ["Owner Education Programs", "Owner Education/Training Provided"],
  ["Upscale Avg On-Site Staff Per Property", "Upscale Avg Staff"],
  ["Figures As Of", "Figures as of"],
  ["Economy Avg On-Site Staff Per Property", "Economy Avg Staff"],
  ["Primary PMS", "Primary PMS System"],
  ["Business Continuity", "Business Continuity Planning"],
  ["24/7 Support", "Support 24/7 Availability"],
  ["Carbon Tracking", "Carbon Footprint Tracking"],
  ["Crisis Experience", "Crisis Management Experience"],
  ["Energy Efficiency", "Energy Efficiency Initiatives"],
  ["Waste Reduction", "Waste Reduction Programs"],
  ["Min Property Size", "Minimum Property Size"],
  ["Max Property Size", "Maximum Property Size"],
  ["Dispute Resolution", "Dispute Resolution Approach"],
  ["ESG / Sustainability Expectations", "ESG / Sustainability Expectations You Prefer Projects to Meet"],
  ["Market Expansion Ramp Lead Time (Months)", "Pre-opening Ramp Lead Time (Months)"],
  ["Portfolio Value", "Total Portfolio Value"],
  ["Annual Revenue Managed", "Average Annual Revenue Managed"],
  ["NOI Improvement", "Average NOI Improvement"],
  ["Certifications Held", "Certifications"],
  ["Known Red Flag Items", "Red Flag Items That Typically Make You Decline or Proceed With Caution"],
  ["Featured Differentiators", "Key Differentiators"],
  ["Specific Markets", "Specific Markets/Cities"],
  ["Major Lenders", "Major Lenders Worked With"],
  ["Mixed-Use Allowed", "Mixed-Use Development Allowed"],
];

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
