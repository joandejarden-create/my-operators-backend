/**
 * Normalization helpers for STR Excel ↔ Hotel Census matching (read-only / dry-run).
 */

export function csvEscape(val) {
  if (val == null) return "";
  const s = String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Fix Excel numeric STR IDs exported as 12345.0 */
export function normalizeStrId(raw) {
  if (raw == null || raw === "") return "";
  if (typeof raw === "number" && Number.isFinite(raw)) return String(Math.trunc(raw));
  let s = String(raw).trim();
  if (/^\d+\.0+$/.test(s)) s = s.replace(/\.0+$/, "");
  return s;
}

export function normalizeText(raw) {
  return String(raw ?? "")
    .trim()
    .replace(/\s+/g, " ");
}

export function normalizeKey(raw) {
  return normalizeText(raw)
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
}

export function nameCityCountryKey(name, city, country) {
  return `${normalizeKey(name)}|${normalizeKey(city)}|${normalizeKey(country)}`;
}

/** Normalize Excel header → canonical key */
export function normalizeExcelHeader(header) {
  return normalizeKey(header)
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

const EXCEL_CANONICAL = {
  "str id": "strId",
  "str number": "strId",
  "str #": "strId",
  "str identifier": "strId",
  "property id": "strId",
  city: "city",
  "hotel name": "hotelName",
  name: "hotelName",
  country: "country",
  "str market": "strMarket",
  market: "strMarket",
  "str market name": "strMarket",
  "str submarket": "strSubmarket",
  submarket: "strSubmarket",
  "str submarket name": "strSubmarket",
};

export function mapExcelHeaderToCanonical(header) {
  const k = normalizeExcelHeader(header);
  return EXCEL_CANONICAL[k] || null;
}

export const EXPECTED_EXCEL_CANONICAL = [
  "strId",
  "city",
  "hotelName",
  "country",
  "strMarket",
  "strSubmarket",
];
