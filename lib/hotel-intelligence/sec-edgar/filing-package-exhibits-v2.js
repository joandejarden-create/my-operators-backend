/**
 * Enumerate exhibits inside a SEC filing package (parent accession ≠ exhibit).
 * Uses Archives .../index.json — does not treat EX-10 as a standalone submissions form.
 */

import crypto from "node:crypto";

export const FILING_PACKAGE_EXHIBITS_V2 = "sec-edgar-filing-package-exhibits-v2";

/**
 * Issuer filenames vary: ex-10.1.htm, exhibit10.htm, drh_exhibit211x….htm (21.1).
 * Match exhibit series 10* and 21* only — never exhibit31/32 SOX certs.
 */
export function isRelevantExhibitFileName(name) {
  const n = String(name || "").toLowerCase();
  if (!n) return false;
  if (/(^|[^a-z0-9])ex[-_]?10([.\-_]\d+)?([^a-z0-9]|$)/.test(n)) return true;
  if (/(^|[^a-z0-9])ex[-_]?21([.\-_]\d+)?([^a-z0-9]|$)/.test(n)) return true;
  // exhibit10… / exhibit21… (optional digits after 10/21, e.g. exhibit211)
  if (/exhibit[-_]?10\d*/.test(n)) return true;
  if (/exhibit[-_]?21\d*/.test(n)) return true;
  return false;
}

const RELEVANT_EXHIBIT_RE = {
  test(name) {
    return isRelevantExhibitFileName(name);
  },
};

const RELEVANT_DESC_RE =
  /management agreement|franchise agreement|hotel management|amended and restated|termination|renewal|extension|purchase and sale|disposition|subsidiary|list of subsidiaries|property|hotel/i;

/**
 * @param {object} indexBody — parsed index.json
 * @param {object} parent — { cik, accessionNumber, form, filingDate, companyName }
 * @param {object} client — edgar client (for URL builder)
 */
export function enumerateFilingPackageExhibits(indexBody, parent = {}, client = null) {
  const items = normalizeIndexItems(indexBody);
  const exhibits = [];
  const seenNames = new Set();

  for (const item of items) {
    const name = String(item.name || item.Name || "").trim();
    if (!name || seenNames.has(name.toLowerCase())) continue;
    const type = String(item.type || item.Type || item["content-type"] || "");
    const desc = String(item.description || item.Description || item.title || "");
    const size = Number(item.size || item.Size || 0) || null;

    const isExhibitByName = RELEVANT_EXHIBIT_RE.test(name) || /^ex[-_]?\d+/i.test(name);
    const isRelevant =
      RELEVANT_EXHIBIT_RE.test(name) ||
      RELEVANT_DESC_RE.test(desc) ||
      /EX-10|EX-21/i.test(type);

    if (!isExhibitByName && !isRelevant) continue;
    // Skip the primary filing html/xml if it slipped through without exhibit marker
    if (!isExhibitByName && !/ex[-_]?\d+/i.test(name) && !RELEVANT_DESC_RE.test(desc)) continue;

    seenNames.add(name.toLowerCase());
    const exhibitType = classifyExhibit(name, type, desc);
    const url = client?.filingDocumentUrl
      ? client.filingDocumentUrl(parent.cik, parent.accessionNumber, name)
      : null;

    exhibits.push({
      exhibitId: exhibitId(parent.cik, parent.accessionNumber, name),
      fileName: name,
      exhibitType,
      description: desc || null,
      contentType: type || null,
      size,
      parentAccession: parent.accessionNumber || null,
      parentForm: parent.form || null,
      parentFilingDate: parent.filingDate || null,
      cik: parent.cik || null,
      companyName: parent.companyName || null,
      sourceUrl: url,
      entryKind: "EXHIBIT",
      incorporatedByReference: /incorporated by reference/i.test(desc),
    });
  }

  return {
    ok: true,
    version: FILING_PACKAGE_EXHIBITS_V2,
    parentAccession: parent.accessionNumber || null,
    packageItemCount: items.length,
    exhibitCount: exhibits.length,
    exhibits,
    note: "Parent filing accession is distinct from each exhibit document URL.",
  };
}

function normalizeIndexItems(indexBody) {
  if (!indexBody) return [];
  const dir = indexBody.directory || indexBody;
  const raw = dir.item || dir.items || indexBody.item || [];
  return Array.isArray(raw) ? raw : raw ? [raw] : [];
}

function classifyExhibit(name, type, desc) {
  const blob = `${name} ${type} ${desc}`.toLowerCase();
  if (/ex[-_]?21|exhibit[-_]?21\d*|subsidiar/.test(blob)) return "EX-21_SUBSIDIARIES";
  if (/franchise/.test(blob)) return "EX-10_FRANCHISE";
  if (/management agreement|\bhma\b|hotel management/.test(blob)) return "EX-10_MANAGEMENT";
  if (/terminat/.test(blob)) return "EX-10_TERMINATION";
  if (/amend|restated|extension|renewal/.test(blob)) return "EX-10_AMENDMENT";
  if (/purchase|sale|disposition|acquisition|psa/.test(blob)) return "EX-10_TRANSACTION";
  if (/ex[-_]?10|exhibit[-_]?10\d*/.test(blob)) return "EX-10_OTHER";
  return "EXHIBIT_OTHER";
}

function exhibitId(cik, accession, fileName) {
  const raw = `${cik}|${accession}|${fileName}`.toLowerCase();
  return `exh_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 20)}`;
}

/**
 * Deduplicate exhibits across filings (same file hash / same exhibit id / IBR).
 */
export function dedupeExhibits(exhibits = [], opts = {}) {
  const byId = new Map();
  const skipped = [];
  for (const ex of exhibits) {
    const key = ex.exhibitId || `${ex.parentAccession}|${ex.fileName}`;
    if (opts.processedIds?.has?.(key)) {
      skipped.push({ ...ex, skipReason: "already_processed" });
      continue;
    }
    if (ex.incorporatedByReference && opts.skipIncorporatedByReference !== false) {
      skipped.push({ ...ex, skipReason: "incorporated_by_reference" });
      continue;
    }
    if (byId.has(key)) {
      skipped.push({ ...ex, skipReason: "duplicate_exhibit_id" });
      continue;
    }
    byId.set(key, ex);
  }
  return {
    exhibits: [...byId.values()],
    skipped,
    uniqueCount: byId.size,
    skippedCount: skipped.length,
  };
}

/**
 * Live: fetch package index for one accession and enumerate exhibits.
 */
export async function discoverExhibitsForFiling(client, filing) {
  const res = await client.getFilingPackageIndex(filing.cik, filing.accessionNumber);
  if (!res.ok || !res.body) {
    return {
      ok: false,
      error: res.error || `HTTP_${res.status}`,
      status: res.status,
      parentAccession: filing.accessionNumber,
      exhibits: [],
    };
  }
  const enumerated = enumerateFilingPackageExhibits(res.body, filing, client);
  return {
    ok: true,
    fromCache: Boolean(res.fromCache),
    indexUrl: client.filingPackageIndexUrl(filing.cik, filing.accessionNumber),
    ...enumerated,
  };
}
