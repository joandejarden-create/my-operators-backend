/**
 * Discover relevant SEC filings for contract intelligence (bounded).
 */

export const FILING_DISCOVERY_VERSION = "sec-edgar-filing-discovery-v1";

export const DEFAULT_FORM_TYPES = Object.freeze([
  "10-K",
  "10-K/A",
  "10-Q",
  "10-Q/A",
  "8-K",
  "8-K/A",
  "20-F",
  "20-F/A",
  "6-K",
  "6-K/A",
  "EX-10",
]);

/**
 * @param {object} submissions — SEC submissions JSON body
 * @param {object} [opts]
 */
export function listRecentFilings(submissions, opts = {}) {
  const forms = new Set((opts.formTypes || DEFAULT_FORM_TYPES).map((f) => String(f).toUpperCase()));
  const limit = Math.max(1, Number(opts.limit || 12) || 12);
  const sinceYear = opts.sinceYear != null ? Number(opts.sinceYear) : new Date().getFullYear() - 3;
  const recent = submissions?.filings?.recent;
  if (!recent?.form || !Array.isArray(recent.form)) {
    return { ok: false, error: "SUBMISSIONS_SHAPE", filings: [] };
  }

  const out = [];
  for (let i = 0; i < recent.form.length; i++) {
    const form = String(recent.form[i] || "");
    const formUp = form.toUpperCase();
    const formMatch =
      forms.has(formUp) ||
      [...forms].some((f) => formUp === f || formUp.startsWith(`${f} `) || formUp.startsWith("EX-10"));
    if (!formMatch && !/^EX-10/i.test(form)) continue;
    const filingDate = String(recent.filingDate?.[i] || "");
    const year = Number(filingDate.slice(0, 4));
    if (Number.isFinite(sinceYear) && Number.isFinite(year) && year < sinceYear) continue;
    out.push({
      form,
      filingDate,
      accessionNumber: recent.accessionNumber?.[i] || null,
      primaryDocument: recent.primaryDocument?.[i] || null,
      primaryDocDescription: recent.primaryDocDescription?.[i] || null,
      reportDate: recent.reportDate?.[i] || null,
      size: recent.size?.[i] || null,
    });
    if (out.length >= limit) break;
  }

  return {
    ok: true,
    version: FILING_DISCOVERY_VERSION,
    cik: submissions?.cik != null ? String(submissions.cik).padStart(10, "0") : null,
    name: submissions?.name || null,
    tickers: submissions?.tickers || [],
    filings: out,
  };
}

export function dedupeFilings(filings = []) {
  const seen = new Set();
  const out = [];
  for (const f of filings) {
    const key = `${f.accessionNumber}|${f.primaryDocument}|${f.form}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(f);
  }
  return out;
}
