/**
 * Map Hotel Property Census records into Scout/legacy-census field shapes.
 *
 * Keeps Scout aggregations (market-coverage, opportunity-signals, market-map)
 * working against HPC without maintaining Hotel Census as a second SoT.
 * Never writes Airtable.
 */

export const HPC_SCOUT_RECORD_ADAPTER_V1 = "HPC_SCOUT_RECORD_ADAPTER_V1";
export const HPC_RUNTIME_TABLE_NAME = "Hotel Property Census";
export const HPC_RUNTIME_TABLE_ID = "tbl9aY5ijiuIzzWam";

/** HPC fields safe to request in Scout bulk loads. */
export const SCOUT_HPC_SELECT_FIELDS = Object.freeze([
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Parent Company",
  "Radar Status",
  "Rooms / Keys",
  "Country",
  "City",
  "Market",
  "Submarket",
  "Hotel Class / Segment",
  "Asset Context",
  "Operation Type",
  "Operator / Management Company",
  "Latitude",
  "Longitude",
  "Continent",
  "State / Region",
  "Official Property URL",
  "Phone",
  "Address",
]);

function firstText(fields, keys) {
  for (const key of keys) {
    const raw = fields?.[key];
    if (raw == null || raw === "") continue;
    if (typeof raw === "string") {
      const t = raw.trim();
      if (t) return t;
      continue;
    }
    if (Array.isArray(raw)) {
      const joined = raw
        .map((item) => (typeof item === "string" ? item.trim() : item?.name || ""))
        .filter(Boolean)
        .join("; ");
      if (joined) return joined;
    }
    if (typeof raw === "object" && raw.name) {
      const t = String(raw.name).trim();
      if (t) return t;
    }
  }
  return null;
}

function firstNumber(fields, keys) {
  for (const key of keys) {
    const raw = fields?.[key];
    if (raw == null || raw === "") continue;
    const n = Number(raw);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

/**
 * Normalize Radar Status / status into Scout Open|Pipeline (or passthrough).
 */
export function normalizeHpcStatusForScout(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  const lower = s.toLowerCase();
  if (lower === "open") return "Open";
  if (lower === "pipeline") return "Pipeline";
  if (lower === "hidden") return "Hidden";
  return s;
}

/**
 * @param {object} record Airtable HPC record
 * @returns {{ id: string, fields: object, _sourceTable: string, _sourceTableId: string }}
 */
export function mapHpcRecordToScoutCensusShape(record) {
  const f = record?.fields || {};
  const brand =
    firstText(f, ["Current Brand", "Affiliation", "Brand Family"]) || null;
  const status = normalizeHpcStatusForScout(
    firstText(f, ["Radar Status", "status", "Status"])
  );
  const rooms = firstNumber(f, ["Rooms / Keys", "rooms", "Rooms"]);

  return {
    id: record.id,
    fields: {
      name:
        firstText(f, ["Property Name", "Canonical Property Name", "name"]) ||
        null,
      Affiliation: brand,
      "Parent Company":
        firstText(f, ["Parent Company", "Brand Family"]) || null,
      status: status || null,
      rooms: rooms,
      country: firstText(f, ["Country", "country"]) || null,
      city: firstText(f, ["City", "city"]) || null,
      Market: firstText(f, ["Market", "Market / Submarket"]) || null,
      Submarket: firstText(f, ["Submarket"]) || null,
      "Chain Scale":
        firstText(f, ["Hotel Class / Segment", "Chain Scale"]) || null,
      Location: firstText(f, ["Asset Context", "Location", "Location Type"]) || null,
      "Operation Type": firstText(f, ["Operation Type", "Management Model"]) || null,
      "Management Company":
        firstText(f, ["Operator / Management Company", "Management Company"]) ||
        null,
      project_phase: firstText(f, ["project_phase", "Project Phase"]) || null,
      Latitude: firstNumber(f, ["Latitude", "latitude"]),
      Longitude: firstNumber(f, ["Longitude", "longitude"]),
      Region:
        firstText(f, ["Continent", "Region", "State / Region"]) || null,
      Website: firstText(f, ["Official Property URL", "Website"]) || null,
      Telephone: firstText(f, ["Phone", "Telephone"]) || null,
      "Address 1": firstText(f, ["Address", "Address 1"]) || null,
      "Property Type": firstText(f, ["Property Type"]) || null,
    },
    _sourceTable: HPC_RUNTIME_TABLE_NAME,
    _sourceTableId: HPC_RUNTIME_TABLE_ID,
  };
}

export function escapeAirtableFormulaString(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Geography filter against HPC field names (Country / Market).
 */
export function buildHpcGeographyFormula(options = {}) {
  const parts = [];
  const country = String(options.country || "").trim();
  const market = String(options.market || "").trim();
  if (country) {
    parts.push(`{Country}='${escapeAirtableFormulaString(country)}'`);
  }
  if (market) {
    parts.push(`{Market}='${escapeAirtableFormulaString(market)}'`);
  }
  if (!parts.length) return null;
  return parts.length === 1 ? parts[0] : `AND(${parts.join(",")})`;
}
