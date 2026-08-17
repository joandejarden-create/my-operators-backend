/**
 * CoStar Excel row → Airtable Properties field payload (1:1 header names).
 */
const NUMBER_FIELDS = new Set([
  "Star Rating",
  "RBA/GLA",
  "Land(AC)",
  "Year Built",
  "Year Renov",
  "Property ID",
  "Parking Ratio",
  "Rooms",
  "Expansion Rooms",
  "Mtg Rooms",
  "Total Mtg Space SF",
  "Max Contig Mtg Space SF",
  "Stories",
  "Parking Spaces",
  "Parking Spaces/Room",
  "Hotel Grade",
]);

const CHECKBOX_FIELDS = new Set(["All Inclusive Rate", "All-Suites"]);

function parseNumber(value) {
  if (value == null || value === "") return null;
  const n = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : null;
}

function parseCheckbox(value) {
  const s = String(value ?? "").trim().toLowerCase();
  if (!s) return null;
  if (s === "yes" || s === "true" || s === "1" || s === "y") return true;
  if (s === "no" || s === "false" || s === "0" || s === "n") return false;
  return null;
}

function coerceCostarCell(fieldName, raw) {
  if (raw == null || raw === "") return null;

  if (CHECKBOX_FIELDS.has(fieldName)) {
    return parseCheckbox(raw);
  }
  if (NUMBER_FIELDS.has(fieldName)) {
    return parseNumber(raw);
  }

  const text = String(raw).trim();
  return text || null;
}

/**
 * @param {string[]} headerRow
 * @param {unknown[]} line
 */
export function rowToAirtableFields(headerRow, line) {
  /** @type {Record<string, unknown>} */
  const fields = {};
  for (let i = 0; i < headerRow.length; i++) {
    const fieldName = String(headerRow[i] ?? "").trim();
    if (!fieldName) continue;
    const coerced = coerceCostarCell(fieldName, line[i]);
    if (coerced == null) continue;
    fields[fieldName] = coerced;
  }
  return fields;
}

/**
 * @param {Record<string, unknown>} fields
 */
export function propertyDedupeKey(fields) {
  const propertyId = fields["Property ID"];
  if (propertyId != null && String(propertyId).trim() !== "") {
    return `pid:${String(propertyId).trim()}`;
  }
  const name = String(fields["Building Name"] || "").trim().toLowerCase();
  const city = String(fields["City"] || "").trim().toLowerCase();
  const owner = String(fields["True Owner"] || "").trim().toLowerCase();
  if (!name) return "";
  return `ncc:${name}|${city}|${owner}`;
}

/**
 * Dedupe rows; later rows win (last file / last row order).
 * @param {{ key: string, fields: Record<string, unknown>, sourceFile: string, sourceRow: number }[]} rows
 */
export function dedupeCostarRows(rows) {
  const byKey = new Map();
  let skippedNoKey = 0;

  for (const row of rows) {
    if (!row.key) {
      skippedNoKey++;
      continue;
    }
    byKey.set(row.key, row);
  }

  return {
    rows: [...byKey.values()],
    skippedNoKey,
    duplicateRowsRemoved: rows.length - byKey.size - skippedNoKey,
  };
}
