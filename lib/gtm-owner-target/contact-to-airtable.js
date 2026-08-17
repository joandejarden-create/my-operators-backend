const NUMBER_FIELDS = new Set([
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

const URL_FIELDS = new Set(["LinkedIn", "Website"]);

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

function coerceContactCell(fieldName, raw) {
  if (raw == null || raw === "") return null;
  if (NUMBER_FIELDS.has(fieldName)) return parseNumber(raw);
  if (URL_FIELDS.has(fieldName)) return normalizeUrl(raw);
  const text = String(raw).trim();
  return text || null;
}

/**
 * @param {string[]} headerRow
 * @param {unknown[]} line
 */
export function rowToContactFields(headerRow, line) {
  /** @type {Record<string, unknown>} */
  const fields = {};
  for (let i = 0; i < headerRow.length; i++) {
    const fieldName = String(headerRow[i] ?? "").trim();
    if (!fieldName) continue;
    const coerced = coerceContactCell(fieldName, line[i]);
    if (coerced == null) continue;
    fields[fieldName] = coerced;
  }
  return fields;
}

/**
 * @param {Record<string, unknown>} fields
 */
export function contactDedupeKey(fields) {
  const email = String(fields.Email || "")
    .trim()
    .toLowerCase();
  if (email) return `email:${email}`;

  const name = String(fields.Name || "")
    .trim()
    .toLowerCase();
  const company = String(fields.Company || "")
    .trim()
    .toLowerCase();
  const phone = String(fields.Phone || "")
    .trim()
    .toLowerCase();
  if (name || company) return `ncc:${name}|${company}|${phone}`;
  return "";
}

/**
 * @param {{ key: string, fields: Record<string, unknown> }[]} rows
 */
export function dedupeContactRows(rows) {
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
