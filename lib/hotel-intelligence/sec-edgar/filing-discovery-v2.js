/**
 * Historical SEC filing discovery — 1990s onward, additional submissions shards.
 * Extends V1; does not replace the connector.
 */

import { DEFAULT_FORM_TYPES, dedupeFilings } from "./filing-discovery-v1.js";

export const FILING_DISCOVERY_V2 = "sec-edgar-filing-discovery-v2";

export const HISTORICAL_FORM_TYPES = Object.freeze([
  ...DEFAULT_FORM_TYPES,
  "S-11",
  "S-11/A",
  "S-1",
  "S-1/A",
  "S-3",
  "S-3/A",
  "424B1",
  "424B2",
  "424B3",
  "424B4",
  "424B5",
  "424B7",
  "F-1",
  "F-1/A",
  "ARS",
  "DEF 14A",
]);

function formMatches(form, forms) {
  const formUp = String(form || "").toUpperCase();
  if (forms.has(formUp)) return true;
  if (/^EX-10/i.test(formUp)) return true;
  if (/^424B/i.test(formUp) && [...forms].some((f) => f.startsWith("424B"))) return true;
  return [...forms].some(
    (f) => formUp === f || formUp.startsWith(`${f} `) || formUp.startsWith(`${f}/`),
  );
}

/**
 * Flatten recent + optional additional shards into a uniform filing list.
 */
export function flattenSubmissionFilings(submissionsBody, additionalBodies = []) {
  const chunks = [];
  const recent = submissionsBody?.filings?.recent;
  if (recent?.form) chunks.push(recent);
  for (const body of additionalBodies) {
    // Additional files are shaped like { form: [], filingDate: [], ... } at top level
    if (body?.form) chunks.push(body);
    else if (body?.filings?.recent?.form) chunks.push(body.filings.recent);
  }
  const out = [];
  for (const recentBlock of chunks) {
    const n = recentBlock.form?.length || 0;
    for (let i = 0; i < n; i++) {
      out.push({
        form: String(recentBlock.form?.[i] || ""),
        filingDate: String(recentBlock.filingDate?.[i] || ""),
        accessionNumber: recentBlock.accessionNumber?.[i] || null,
        primaryDocument: recentBlock.primaryDocument?.[i] || null,
        primaryDocDescription: recentBlock.primaryDocDescription?.[i] || null,
        reportDate: recentBlock.reportDate?.[i] || null,
        size: recentBlock.size?.[i] || null,
      });
    }
  }
  return out;
}

/**
 * List historical filings with expanded form set and year floor (default 1994).
 */
export function listHistoricalFilings(submissions, opts = {}) {
  const forms = new Set(
    (opts.formTypes || HISTORICAL_FORM_TYPES).map((f) => String(f).toUpperCase()),
  );
  const sinceYear = opts.sinceYear != null ? Number(opts.sinceYear) : 1994;
  const untilYear = opts.untilYear != null ? Number(opts.untilYear) : null;
  const limit = opts.limit != null ? Math.max(1, Number(opts.limit)) : null;
  const prioritize = opts.prioritizeHighYield !== false;

  const additional = opts.additionalBodies || [];
  const all = flattenSubmissionFilings(submissions, additional);
  const eligible = [];
  for (const f of all) {
    if (!formMatches(f.form, forms)) continue;
    const year = Number(String(f.filingDate || "").slice(0, 4));
    if (Number.isFinite(sinceYear) && Number.isFinite(year) && year < sinceYear) continue;
    if (untilYear != null && Number.isFinite(year) && year > untilYear) continue;
    if (!f.accessionNumber || !f.primaryDocument) continue;
    eligible.push({
      ...f,
      yieldScore: scoreFilingYield(f),
    });
  }

  if (prioritize) {
    eligible.sort((a, b) => {
      if (b.yieldScore !== a.yieldScore) return b.yieldScore - a.yieldScore;
      return String(b.filingDate).localeCompare(String(a.filingDate));
    });
  } else {
    eligible.sort((a, b) => String(b.filingDate).localeCompare(String(a.filingDate)));
  }

  const filings = limit != null ? eligible.slice(0, limit) : eligible;
  return {
    ok: true,
    version: FILING_DISCOVERY_V2,
    cik: submissions?.cik != null ? String(submissions.cik).padStart(10, "0") : null,
    name: submissions?.name || null,
    tickers: submissions?.tickers || [],
    eligibleCount: eligible.length,
    filings,
    additionalFilesListed: (submissions?.filings?.files || []).map((x) => x.name || x),
    coverage: {
      sinceYear,
      untilYear,
      formTypes: [...forms],
      eligibleFound: eligible.length,
      returned: filings.length,
      remainingInQueue: Math.max(0, eligible.length - filings.length),
    },
  };
}

export function scoreFilingYield(filing) {
  const form = String(filing.form || "").toUpperCase();
  const desc = String(filing.primaryDocDescription || "").toLowerCase();
  let score = 0;
  if (form === "10-K" || form === "10-K/A") score += 8;
  if (form === "S-11" || form.startsWith("S-11")) score += 10;
  if (form.startsWith("424B")) score += 9;
  if (form.startsWith("EX-10")) score += 12;
  if (form === "8-K" || form === "8-K/A") score += 4;
  if (form === "10-Q" || form === "10-Q/A") score += 3;
  if (/franchise|management agreement|hotel|portfolio|property schedule|exhibit 10/.test(desc))
    score += 6;
  if (/expiration|lease|hma/.test(desc)) score += 4;
  // Prefer larger docs slightly (schedules tend to be bigger)
  const size = Number(filing.size) || 0;
  if (size > 500000) score += 2;
  if (size > 2000000) score += 2;
  return score;
}

/**
 * Load all additional historical shards for a company via the EDGAR client.
 */
export async function loadAllSubmissionBodies(client, cik) {
  const primary = await client.getSubmissions(cik);
  if (!primary.ok || !primary.body) {
    return { ok: false, error: primary.error || "SUBMISSIONS_FAILED", primary: null, additional: [] };
  }
  const files = primary.body?.filings?.files || [];
  const additional = [];
  for (const f of files) {
    const name = f.name || f;
    if (!name) continue;
    const res = await client.getSubmissionsAdditionalFile(name);
    if (res.ok && res.body) additional.push(res.body);
  }
  return {
    ok: true,
    primary: primary.body,
    additional,
    additionalFileCount: files.length,
    loadedAdditional: additional.length,
  };
}

export { dedupeFilings, HISTORICAL_FORM_TYPES as DEFAULT_HISTORICAL_FORMS };
