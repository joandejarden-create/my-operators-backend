/**
 * LinkedIn Connections CSV parser.
 *
 * Does NOT assume row 1 is the header — detects the official LinkedIn column names
 * after optional Notes metadata rows.
 */

import {
  LINKEDIN_CONNECTIONS_COLUMNS as COL,
  LINKEDIN_CONNECTIONS_REQUIRED_HEADERS,
  LINKEDIN_CONNECTIONS_PREFERRED_HEADERS,
} from "./field-map.js";
import {
  buildPersonIdentityKey,
  formatPersonDisplayName,
  normalizeLinkedInProfileUrl,
} from "./linkedin-identity.js";

/**
 * Parse a single CSV line respecting quoted fields.
 * @param {string} line
 * @returns {string[]}
 */
export function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  const s = String(line ?? "");
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inQuotes) {
      if (ch === '"') {
        if (s[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map((c) => c.trim());
}

/**
 * @param {string} text
 * @returns {string[][]}
 */
export function csvTextToMatrix(text) {
  const raw = String(text || "").replace(/^\uFEFF/, "");
  if (!raw.trim()) return [];

  const rows = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      cur += ch;
      continue;
    }
    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && raw[i + 1] === "\n") i++;
      rows.push(parseCsvLine(cur));
      cur = "";
      continue;
    }
    cur += ch;
  }
  if (cur.length || rows.length === 0) rows.push(parseCsvLine(cur));
  return rows;
}

function normalizeHeaderCell(value) {
  return String(value || "")
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase();
}

const EXPECTED_HEADER_LOOKUP = new Map(
  [
    ...LINKEDIN_CONNECTIONS_REQUIRED_HEADERS,
    ...LINKEDIN_CONNECTIONS_PREFERRED_HEADERS,
  ].map((h) => [normalizeHeaderCell(h), h])
);

/**
 * Find the header row that contains LinkedIn Connections columns.
 * @param {string[][]} matrix
 * @returns {{ headerRowIndex: number, headerMap: Record<string, number>, headers: string[], matchedRequired: string[], matchedPreferred: string[] } | null}
 */
export function detectLinkedInConnectionsHeader(matrix) {
  const scanLimit = Math.min(40, matrix.length);
  let best = null;

  for (let i = 0; i < scanLimit; i++) {
    const row = matrix[i] || [];
    /** @type {Record<string, number>} */
    const headerMap = {};
    const headers = [];
    const matchedRequired = [];
    const matchedPreferred = [];

    row.forEach((cell, idx) => {
      const canon = EXPECTED_HEADER_LOOKUP.get(normalizeHeaderCell(cell));
      if (!canon) return;
      if (headerMap[canon] == null) headerMap[canon] = idx;
      headers.push(canon);
      if (LINKEDIN_CONNECTIONS_REQUIRED_HEADERS.includes(canon)) {
        matchedRequired.push(canon);
      } else if (LINKEDIN_CONNECTIONS_PREFERRED_HEADERS.includes(canon)) {
        matchedPreferred.push(canon);
      }
    });

    const hasAllRequired = LINKEDIN_CONNECTIONS_REQUIRED_HEADERS.every(
      (h) => headerMap[h] != null
    );
    if (!hasAllRequired) continue;

    const score = matchedRequired.length * 10 + matchedPreferred.length;
    if (!best || score > best.score) {
      best = {
        headerRowIndex: i,
        headerMap,
        headers: Object.keys(headerMap),
        matchedRequired: [...new Set(matchedRequired)],
        matchedPreferred: [...new Set(matchedPreferred)],
        score,
      };
    }
  }

  if (!best) return null;
  const { score, ...rest } = best;
  return rest;
}

/**
 * Parse LinkedIn "Connected On" values (e.g. 12 Jan 2020, 01/12/2020).
 * @param {string} raw
 * @returns {{ iso: string | null, invalid: boolean, raw: string }}
 */
export function parseConnectedOn(raw) {
  const s = String(raw || "").trim();
  if (!s) return { iso: null, invalid: false, raw: "" };

  const months = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11,
  };

  // 12 Jan 2020 / 12 January 2020
  let m = s.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (m) {
    const day = Number(m[1]);
    const mon = months[m[2].toLowerCase()];
    const year = Number(m[3]);
    if (mon != null && day >= 1 && day <= 31) {
      const d = new Date(Date.UTC(year, mon, day));
      if (!Number.isNaN(d.getTime())) {
        return { iso: d.toISOString().slice(0, 10), invalid: false, raw: s };
      }
    }
    return { iso: null, invalid: true, raw: s };
  }

  // ISO-ish
  m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) {
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    if (!Number.isNaN(d.getTime())) {
      return { iso: d.toISOString().slice(0, 10), invalid: false, raw: s };
    }
    return { iso: null, invalid: true, raw: s };
  }

  // US/EU numeric — ambiguous; accept if Date parses and year looks sane
  m = s.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$/);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    const year = Number(m[3]);
    // Prefer day-first when day > 12; else month-first (LinkedIn US often MM/DD/YYYY)
    let month;
    let day;
    if (a > 12) {
      day = a;
      month = b - 1;
    } else if (b > 12) {
      month = a - 1;
      day = b;
    } else {
      month = a - 1;
      day = b;
    }
    const d = new Date(Date.UTC(year, month, day));
    if (!Number.isNaN(d.getTime()) && year >= 2000 && year <= 2100) {
      return { iso: d.toISOString().slice(0, 10), invalid: false, raw: s };
    }
    return { iso: null, invalid: true, raw: s };
  }

  return { iso: null, invalid: true, raw: s };
}

function cellAt(line, headerMap, columnName) {
  const idx = headerMap[columnName];
  if (idx == null) return "";
  return String(line[idx] ?? "").trim();
}

/**
 * @param {string} csvText
 * @param {{ fileName?: string }} [opts]
 */
export function parseLinkedInConnectionsCsv(csvText, opts = {}) {
  const fileName = String(opts.fileName || "").trim() || "Connections.csv";
  const matrix = csvTextToMatrix(csvText);

  if (!matrix.length) {
    return {
      ok: false,
      error: "empty_file",
      message: "CSV file is empty.",
      fileName,
      headerRowIndex: -1,
      rows: [],
      invalidRows: [],
      warnings: [],
    };
  }

  const detected = detectLinkedInConnectionsHeader(matrix);
  if (!detected) {
    return {
      ok: false,
      error: "not_linkedin_connections_export",
      message:
        "File does not look like a LinkedIn Connections export. Expected columns include First Name and Last Name.",
      fileName,
      headerRowIndex: -1,
      rows: [],
      invalidRows: [],
      warnings: ["header_not_detected"],
    };
  }

  const { headerRowIndex, headerMap, headers, matchedPreferred } = detected;
  const warnings = [];
  for (const pref of LINKEDIN_CONNECTIONS_PREFERRED_HEADERS) {
    if (headerMap[pref] == null) {
      warnings.push(`missing_optional_column:${pref}`);
    }
  }

  const rows = [];
  const invalidRows = [];
  const seenIdentity = new Map();

  for (let r = headerRowIndex + 1; r < matrix.length; r++) {
    const line = matrix[r] || [];
    if (!line.length || line.every((c) => String(c ?? "").trim() === "")) {
      continue;
    }

    const firstName = cellAt(line, headerMap, COL.firstName);
    const lastName = cellAt(line, headerMap, COL.lastName);
    const urlRaw = cellAt(line, headerMap, COL.url);
    const email = cellAt(line, headerMap, COL.email);
    const company = cellAt(line, headerMap, COL.company);
    const position = cellAt(line, headerMap, COL.position);
    const connectedOnRaw = cellAt(line, headerMap, COL.connectedOn);
    const linkedInUrl = normalizeLinkedInProfileUrl(urlRaw);
    const connected = parseConnectedOn(connectedOnRaw);

    const displayName = formatPersonDisplayName(firstName, lastName);
    if (!displayName) {
      invalidRows.push({
        rowNumber: r + 1,
        reason: "missing_name",
        raw: { firstName, lastName, urlRaw, company, position },
      });
      continue;
    }

    if (urlRaw && !linkedInUrl) {
      warnings.push(`invalid_linkedin_url_row_${r + 1}`);
    }

    const parsed = {
      rowNumber: r + 1,
      firstName,
      lastName,
      displayName,
      linkedInUrl,
      linkedInUrlRaw: urlRaw,
      email,
      company,
      position,
      connectedOnRaw,
      connectedOn: connected.iso,
      connectedOnInvalid: connected.invalid,
      identityKey: buildPersonIdentityKey({
        linkedInUrl,
        firstName,
        lastName,
        company,
      }),
    };

    if (!parsed.identityKey) {
      invalidRows.push({
        rowNumber: r + 1,
        reason: "missing_identity",
        raw: parsed,
      });
      continue;
    }

    if (seenIdentity.has(parsed.identityKey)) {
      const first = seenIdentity.get(parsed.identityKey);
      parsed.duplicateOfRow = first.rowNumber;
      first.duplicateCount = (first.duplicateCount || 0) + 1;
    } else {
      seenIdentity.set(parsed.identityKey, parsed);
    }

    rows.push(parsed);
  }

  return {
    ok: true,
    fileName,
    headerRowIndex,
    headers,
    matchedPreferred,
    headerMap,
    rows,
    invalidRows,
    warnings,
    metadataRowCount: headerRowIndex,
  };
}
