/**
 * Shared table/field inventory helpers for Scout demand overlay sources.
 * Read-only — uses Airtable Metadata API.
 */

export const SCOUT_OVERLAY_TABLE_TARGETS = [
  {
    label: "Travel Infrastructure",
    match: (name) => /travel infrastructure/i.test(name),
    preferredNames: ["Travel Infrastructure Data", "Travel Infrastructure", "Travel Infrastructure data"],
  },
  {
    label: "Demand Anchors",
    match: (name) => /^demand anchors$/i.test(name),
    preferredNames: ["Demand Anchors"],
  },
  {
    label: "Market Development Projects",
    match: (name) => /market development projects/i.test(name),
    preferredNames: ["Market Development Projects"],
  },
  {
    label: "Tourism Corridors",
    match: (name) => /tourism corridors/i.test(name),
    preferredNames: ["Tourism Corridors"],
  },
  {
    label: "Mixed-Use Zones",
    match: (name) => /mixed-use zones/i.test(name),
    preferredNames: ["Mixed-Use Zones"],
  },
];

const FIELD_ROLE_PATTERNS = [
  { role: "name", patterns: [/^name$/i, /demand anchor name/i, /^title$/i] },
  { role: "type", patterns: [/^type$/i, /^point type$/i, /^category$/i, /radar category/i] },
  { role: "country", patterns: [/^country$/i] },
  { role: "city", patterns: [/^city$/i] },
  { role: "market", patterns: [/^market$/i, /linked market/i, /^region$/i, /str market/i] },
  { role: "submarket", patterns: [/^submarket$/i, /str submarket/i] },
  { role: "latitude", patterns: [/^latitude$/i, /^lat$/i] },
  { role: "longitude", patterns: [/^longitude$/i, /^lng$/i, /^lon$/i] },
  { role: "notes", patterns: [/^notes$/i, /rationale/i, /description/i] },
  { role: "source", patterns: [/^source$/i, /source url/i, /source reference/i] },
  { role: "confidence", patterns: [/confidence/i, /data confidence/i] },
  { role: "lastVerified", patterns: [/last verified/i, /verified at/i] },
  { role: "status", patterns: [/^status$/i, /^visibility$/i, /include on radar/i] },
];

/**
 * @param {string} baseId
 * @param {string} apiKey
 */
export async function fetchBaseTablesMeta(baseId, apiKey) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Metadata API failed (${res.status}): ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  return data.tables || [];
}

/**
 * @param {string} fieldName
 */
export function classifyOverlayFieldRole(fieldName) {
  const name = String(fieldName || "");
  for (const entry of FIELD_ROLE_PATTERNS) {
    if (entry.patterns.some((p) => p.test(name))) return entry.role;
  }
  return "other";
}

/**
 * @param {Array<{ name: string, fields?: Array<{ name: string, type: string }> }>} tables
 */
export function inventoryOverlaySources(tables) {
  const tableNames = tables.map((t) => t.name);
  const found = [];
  const missing = [];

  for (const target of SCOUT_OVERLAY_TABLE_TARGETS) {
    const hit = tables.find((t) => target.match(t.name));
    if (!hit) {
      missing.push(target.label);
      continue;
    }

    const fields = (hit.fields || []).map((f) => ({
      name: f.name,
      type: f.type,
      role: classifyOverlayFieldRole(f.name),
    }));

    const rolesPresent = new Set(fields.map((f) => f.role));
    const coordinateFields = fields
      .filter((f) => f.role === "latitude" || f.role === "longitude")
      .map((f) => f.name);
    const geographyFields = fields
      .filter((f) => ["country", "city", "market", "submarket"].includes(f.role))
      .map((f) => `${f.role}:${f.name}`);

    found.push({
      label: target.label,
      tableName: hit.name,
      tableId: hit.id,
      fieldCount: fields.length,
      fields,
      coordinateFields,
      geographyFields,
      hasLatitude: rolesPresent.has("latitude"),
      hasLongitude: rolesPresent.has("longitude"),
      hasName: rolesPresent.has("name"),
      hasType: rolesPresent.has("type"),
      recommendedMapping: buildRecommendedMapping(target.label, fields),
    });
  }

  return {
    tablesFound: found.map((f) => f.label),
    tablesMissing: missing,
    tables: found,
    allTableNames: tableNames,
  };
}

/**
 * @param {string} tableLabel
 * @param {Array<{ name: string, role: string }>} fields
 */
function buildRecommendedMapping(tableLabel, fields) {
  const byRole = {};
  for (const f of fields) {
    if (f.role === "other") continue;
    if (!byRole[f.role]) byRole[f.role] = f.name;
  }

  const overlayType =
    tableLabel === "Travel Infrastructure"
      ? "travel_infrastructure"
      : tableLabel === "Demand Anchors"
        ? "demand_anchor"
        : tableLabel.toLowerCase().replace(/\s+/g, "_");

  return {
    overlayType,
    enabledInScoutPhase5A: ["Travel Infrastructure", "Demand Anchors"].includes(tableLabel),
    name: byRole.name || null,
    category: byRole.type || null,
    country: byRole.country || null,
    city: byRole.city || null,
    market: byRole.market || null,
    submarket: byRole.submarket || null,
    latitude: byRole.latitude || null,
    longitude: byRole.longitude || null,
    notes: byRole.notes || null,
    source: byRole.source || null,
    confidence: byRole.confidence || null,
    lastVerified: byRole.lastVerified || null,
    status: byRole.status || null,
  };
}
