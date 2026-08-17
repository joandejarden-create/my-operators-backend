const NUMBER_FIELDS = new Set([
  "Employees",
  "Locations",
  "Managed Properties",
  "Owned Properties",
  "Operated Properties",
  "Lease Transactions (3Y)",
  "Lease Transactions SF (3Y)",
  "Lease Listings",
  "Lease Listings Portfolio SF",
  "Lease Listings Available SF",
  "Sale Transactions (3Y)",
  "Sale Transactions SF (3Y)",
  "Sale Transactions Volume (3Y)",
  "Sale Listings",
  "Sale Listings SF",
]);

function parseNumber(value) {
  if (value == null || value === "") return null;
  const n = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : null;
}

function normalizeUrl(value) {
  const s = String(value || "").trim();
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) return s;
  return `https://${s}`;
}

function coerceCompanyCell(fieldName, raw) {
  if (raw == null || raw === "") return null;
  if (fieldName === "Website") return normalizeUrl(raw);
  if (NUMBER_FIELDS.has(fieldName)) return parseNumber(raw);
  const text = String(raw).trim();
  return text || null;
}

/**
 * @param {string[]} headerRow
 * @param {unknown[]} line
 */
export function rowToCompanyFields(headerRow, line) {
  /** @type {Record<string, unknown>} */
  const fields = {};
  for (let i = 0; i < headerRow.length; i++) {
    const fieldName = String(headerRow[i] ?? "").trim();
    if (!fieldName) continue;
    const coerced = coerceCompanyCell(fieldName, line[i]);
    if (coerced == null) continue;
    fields[fieldName] = coerced;
  }
  return fields;
}

/**
 * @param {Record<string, unknown>} fields
 */
export function companyDedupeKey(fields) {
  const company = String(fields.Company || "")
    .trim()
    .toLowerCase();
  const city = String(fields["HQ City"] || "")
    .trim()
    .toLowerCase();
  const country = String(fields["HQ Country"] || "")
    .trim()
    .toLowerCase();
  if (!company) return "";
  return `co:${company}|${city}|${country}`;
}

/**
 * @param {{ key: string, fields: Record<string, unknown> }[]} rows
 */
export function dedupeCompanyRows(rows) {
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
