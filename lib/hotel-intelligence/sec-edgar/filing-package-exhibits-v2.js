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

/** Non-hotel EX-10 noise (employment, credit, compensation). */
const NON_HOTEL_EX10_RE =
  /\b(severance|employment|compensat|offer letter|change in control|credit agreement|revolving credit|indenture|note purchase|guaranty of|pledge and security|registration rights|underwriting|tax matters|indemnification agreement with|director|officer|ceo|cfo|executive)\b/i;

const HOTEL_CONTRACT_SIGNAL_RE =
  /\b(hotel management agreement|management agreement|franchise agreement|license agreement|franchisor|franchisee|operator agreement|hotel operating|property management|ground lease|purchase and sale agreement|hotel acquisition|hotel disposition|marriott|hilton|hyatt|intercontinental|holiday inn|courtyard|westin|sheraton)\b/i;

/**
 * Score / filter EX-10 (and related) exhibits for hotel-specific contractual content.
 * Uses filename + description + optional document text preview.
 */
export function scoreHotelSpecificExhibit(exhibit = {}, textPreview = "") {
  const blob = `${exhibit.fileName || ""} ${exhibit.description || ""} ${exhibit.exhibitType || ""} ${textPreview || ""}`;
  const type = String(exhibit.exhibitType || "");
  if (/EX-21/i.test(type) || /subsidiar/i.test(blob)) {
    return {
      hotelRelevant: true,
      priority: "HIGH",
      reason: "subsidiary_schedule",
      skip: false,
    };
  }
  if (!/EX-10/i.test(type) && !/exhibit[-_]?10|ex[-_]?10/i.test(exhibit.fileName || "")) {
    return { hotelRelevant: false, priority: "LOW", reason: "not_ex10_or_ex21", skip: true };
  }
  if (NON_HOTEL_EX10_RE.test(blob) && !HOTEL_CONTRACT_SIGNAL_RE.test(blob)) {
    return {
      hotelRelevant: false,
      priority: "SKIP",
      reason: "non_hotel_employment_or_credit",
      skip: true,
    };
  }
  if (HOTEL_CONTRACT_SIGNAL_RE.test(blob)) {
    return {
      hotelRelevant: true,
      priority: "HIGH",
      reason: "hotel_contract_signal",
      skip: false,
    };
  }
  // Unknown EX-10 — keep for bounded review but deprioritize
  return {
    hotelRelevant: null,
    priority: "MEDIUM",
    reason: "ex10_unknown_needs_preview",
    skip: false,
  };
}

/**
 * Parse primary filing text for Exhibit Index rows that describe hotel-specific
 * management / franchise / license / PSA agreements (often incorporated by reference).
 * Employment and credit exhibits are excluded.
 */
export function parseHotelAgreementsFromExhibitIndex(text = "", meta = {}) {
  const plain = String(text || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#\d+;/g, " ")
    .replace(/\s+/g, " ");
  const out = [];
  const re =
    /(?:Exhibit\s+)?(10\.\d+[A-Za-z]?)\s*[-–—:]?\s*([^.]{10,240}?(?:Hotel\s+Management\s+Agreement|Form of Hotel Management Agreement|Management\s+Agreement|Franchise\s+Agreement|License\s+Agreement|Operating\s+Agreement|Purchase\s+and\s+Sale\s+Agreement)[^.]{0,160})/gi;
  let m;
  while ((m = re.exec(plain)) && out.length < 60) {
    const description = m[2].replace(/\s+/g, " ").trim();
    if (
      /\b(employment|severance|credit agreement|revolving|indenture|compensation|incentive|stock option|restricted stock)\b/i.test(
        description,
      )
    ) {
      continue;
    }
    // Skip narrative fee paragraphs falsely matched as "10.0% … management agreements"
    if (/^%/.test(description) || /net profit thresholds/i.test(description)) continue;
    const priorAccession = description.match(/(\d{10}-\d{2}-\d{6})/)?.[1] || null;
    const expiresMatch = description.match(/expires?\s+(?:on\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{4})/i);
    out.push({
      exhibitNumber: m[1],
      description: description.slice(0, 280),
      incorporatedByReference: /incorporated by reference/i.test(description),
      priorAccession,
      expirationMention: expiresMatch?.[1] || null,
      parentAccession: meta.accessionNumber || null,
      cik: meta.cik || null,
      companyName: meta.companyName || null,
      filingDate: meta.filingDate || null,
      sourceUrl: meta.documentUrl || null,
      hotelRelevant: true,
      note: "Exhibit-index disclosure — follow IBR accession for full agreement text when attached elsewhere",
    });
  }
  return out;
}

/**
 * Filter enumerated exhibits to hotel-relevant EX-10/EX-21 set.
 */
export function filterHotelRelevantExhibits(exhibits = [], opts = {}) {
  const kept = [];
  const skipped = [];
  for (const ex of exhibits) {
    const score = scoreHotelSpecificExhibit(ex, opts.textPreviewByFile?.[ex.fileName] || "");
    if (score.skip && opts.keepUnknown !== true) {
      skipped.push({ ...ex, filterReason: score.reason, hotelRelevant: score.hotelRelevant });
      continue;
    }
    if (score.priority === "SKIP") {
      skipped.push({ ...ex, filterReason: score.reason, hotelRelevant: false });
      continue;
    }
    kept.push({ ...ex, hotelRelevance: score });
  }
  kept.sort((a, b) => {
    const rank = { HIGH: 0, MEDIUM: 1, LOW: 2, SKIP: 3 };
    return (rank[a.hotelRelevance?.priority] ?? 9) - (rank[b.hotelRelevance?.priority] ?? 9);
  });
  return { exhibits: kept, skipped, keptCount: kept.length, skippedCount: skipped.length };
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
