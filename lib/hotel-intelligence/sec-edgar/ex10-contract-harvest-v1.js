/**
 * V5 EX-10 / exhibit harvest — brand-neutral candidate selection + term extraction glue.
 * Distinguishes form/draft vs executed; hotel-specific vs portfolio-level.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { extractContractTermsFromText } from "./contract-terms-extractor-v1.js";
import { VERIFICATION_GATE } from "./contract-intelligence-identity-layers-v1.js";
import { readCacheFile } from "./sec-edgar-cache-store-v1.js";

export const EX10_CONTRACT_HARVEST_V1 = "ex10-contract-harvest-v1";

export const AGREEMENT_INSTRUMENT_CLASS = Object.freeze({
  EXECUTED_AGREEMENT: "EXECUTED_AGREEMENT",
  FORM_OF_AGREEMENT: "FORM_OF_AGREEMENT",
  DRAFT_OR_PROPOSED: "DRAFT_OR_PROPOSED",
  AMENDMENT: "AMENDMENT",
  RENEWAL_OR_EXTENSION_INSTRUMENT: "RENEWAL_OR_EXTENSION_INSTRUMENT",
  TERMINATION_INSTRUMENT: "TERMINATION_INSTRUMENT",
  UNKNOWN: "UNKNOWN",
});

export const SCOPE_CLASS = Object.freeze({
  NAMED_HOTEL: "NAMED_HOTEL",
  PORTFOLIO_LEVEL: "PORTFOLIO_LEVEL",
  FORM_WITHOUT_PROPERTY: "FORM_WITHOUT_PROPERTY",
  UNRESOLVED: "UNRESOLVED",
});

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function classifyInstrument(text) {
  const t = String(text || "").slice(0, 20000);
  if (/\bform\s+of\s+(?:hotel\s+)?(?:management|franchise|license|lease)\s+agreement\b/i.test(t)) {
    return AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT;
  }
  if (/\bdraft\b|\bproposed\s+form\b/i.test(t) && /agreement/i.test(t)) {
    return AGREEMENT_INSTRUMENT_CLASS.DRAFT_OR_PROPOSED;
  }
  if (/\bamendment\b.*\bagreement\b|\bagreement\b.*\bamendment\b/i.test(t)) {
    return AGREEMENT_INSTRUMENT_CLASS.AMENDMENT;
  }
  if (/\brenewal\b|\bextension\s+agreement\b/i.test(t) && /executed|effective/i.test(t)) {
    return AGREEMENT_INSTRUMENT_CLASS.RENEWAL_OR_EXTENSION_INSTRUMENT;
  }
  if (/\btermination\s+agreement\b|\bnotice\s+of\s+termination\b/i.test(t)) {
    return AGREEMENT_INSTRUMENT_CLASS.TERMINATION_INSTRUMENT;
  }
  if (
    /\bthis\s+agreement\s+(?:is\s+)?(?:made|entered\s+into)\b/i.test(t) ||
    /\bentered\s+into\s+(?:as\s+of|effective)\b/i.test(t) ||
    /\bby\s+and\s+between\b/i.test(t)
  ) {
    return AGREEMENT_INSTRUMENT_CLASS.EXECUTED_AGREEMENT;
  }
  return AGREEMENT_INSTRUMENT_CLASS.UNKNOWN;
}

export function classifyScope(text) {
  const t = String(text || "").slice(0, 40000);
  const hotelHit =
    /\b(?:courtyard|residence inn|hampton|hilton|marriott|hyatt|holiday inn|homewood|embassy|springhill|fairfield|crowne|sheraton|westin)\b[^.]{0,80}\b(?:hotel|inn|suites|resort)\b/i.test(
      t,
    ) || /\blocated\s+at\b[^.]{0,120}\b(street|avenue|blvd|road|drive)\b/i.test(t);
  if (/\bform\s+of\b/i.test(t) && !hotelHit) return SCOPE_CLASS.FORM_WITHOUT_PROPERTY;
  if (/\bportfolio\b|\beach\s+of\s+the\s+hotels\b|\bthe\s+hotels\b/i.test(t) && !hotelHit) {
    return SCOPE_CLASS.PORTFOLIO_LEVEL;
  }
  if (hotelHit) return SCOPE_CLASS.NAMED_HOTEL;
  if (/\bthe\s+hotel\b|\bthe\s+property\b/i.test(t)) return SCOPE_CLASS.PORTFOLIO_LEVEL;
  return SCOPE_CLASS.UNRESOLVED;
}

export function detectAgreementType(text) {
  const t = String(text || "").slice(0, 15000).toLowerCase();
  if (/hotel management agreement|\bmanagement agreement\b|\boperating agreement\b/.test(t)) {
    return "MANAGEMENT";
  }
  if (/franchise agreement|\bfranchisee\b|\bfranchisor\b/.test(t)) return "FRANCHISE";
  if (/license agreement|\bbrand license\b/.test(t)) return "LICENSE";
  if (/ground lease|\blease agreement\b|\bhotel lease\b/.test(t)) return "LEASE";
  return "OTHER";
}

export function harvestExhibitDocument(doc, meta = {}) {
  const raw = doc?.text || doc?.html || "";
  const text = stripHtml(raw);
  const instrumentClass = classifyInstrument(text);
  const scopeClass = classifyScope(text);
  const agreementType = detectAgreementType(text);
  const extraction = extractContractTermsFromText(text.slice(0, 80000), {
    sourceUrl: meta.sourceUrl || null,
    accession: meta.accession || null,
    confidence: "MEDIUM",
  });

  const existenceVerified =
    Boolean(meta.sourceUrl || meta.accession) &&
    (instrumentClass === AGREEMENT_INSTRUMENT_CLASS.EXECUTED_AGREEMENT ||
      instrumentClass === AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT ||
      instrumentClass === AGREEMENT_INSTRUMENT_CLASS.AMENDMENT ||
      instrumentClass === AGREEMENT_INSTRUMENT_CLASS.RENEWAL_OR_EXTENSION_INSTRUMENT ||
      instrumentClass === AGREEMENT_INSTRUMENT_CLASS.TERMINATION_INSTRUMENT ||
      extraction.disclosedFieldCount > 0);

  const termsVerified =
    extraction.disclosedFieldCount >= 2 &&
    scopeClass !== SCOPE_CLASS.UNRESOLVED &&
    instrumentClass !== AGREEMENT_INSTRUMENT_CLASS.UNKNOWN;

  const gates = {
    [VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED]: existenceVerified,
    [VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED]: termsVerified,
    [VERIFICATION_GATE.CURRENT_STATUS_VERIFIED]: false, // requires later-document search
  };

  const id = `exh_${crypto
    .createHash("sha1")
    .update([meta.accession || "", meta.sourceUrl || "", agreementType].join("|"))
    .digest("hex")
    .slice(0, 16)}`;

  return {
    version: EX10_CONTRACT_HARVEST_V1,
    exhibitId: id,
    accession: meta.accession || null,
    sourceUrl: meta.sourceUrl || null,
    cik: meta.cik || null,
    issuerName: meta.issuerName || null,
    agreementType,
    instrumentClass,
    scopeClass,
    hotelAttributionAllowed: scopeClass === SCOPE_CLASS.NAMED_HOTEL,
    extracted: extraction.terms,
    evidence: extraction.evidence,
    disclosedFieldCount: extraction.disclosedFieldCount,
    gates,
    persistResearchEligible:
      gates[VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED] &&
      (gates[VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED] ||
        instrumentClass === AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT),
    publicationState: "CUSTOMER_BLOCKED",
  };
}

/** Build SEC-API / Exa discovery queries that are brand-neutral. */
export function buildEx10DiscoveryQueries() {
  return [
    {
      id: "hma_ex10",
      provider: "SEC_API_IO",
      query: '"Hotel Management Agreement" OR "Management Agreement" hotel',
      formTypes: ['"EX-10"', '"EX-10.1"', '"EX-10.2"'],
    },
    {
      id: "franchise_ex10",
      provider: "SEC_API_IO",
      query: '"Franchise Agreement" hotel (franchisor OR franchisee)',
      formTypes: ['"EX-10"', '"EX-10.1"'],
    },
    {
      id: "lease_license_ex10",
      provider: "SEC_API_IO",
      query: '("Hotel Lease" OR "License Agreement") (hotel OR lodging)',
      formTypes: ['"EX-10"'],
    },
    {
      id: "amendment_renewal_ex10",
      provider: "SEC_API_IO",
      query: '(amendment OR renewal OR extension) ("management agreement" OR "franchise agreement") hotel',
      formTypes: ['"EX-10"', '"EX-10.1"', '"8-K"'],
    },
    {
      id: "termination_ex10",
      provider: "SEC_API_IO",
      query: '("termination agreement" OR "notice of termination") (hotel OR lodging) (management OR franchise)',
      formTypes: ['"EX-10"', '"8-K"'],
    },
    {
      id: "exa_hma_complement",
      provider: "EXA",
      query: "hotel management agreement SEC exhibit EX-10 renewal term",
      numResults: 10,
    },
    {
      id: "exa_franchise_ex10",
      provider: "EXA",
      query: "hotel franchise agreement site:sec.gov EX-10 franchisor",
      numResults: 10,
    },
    {
      id: "exa_amendment_ex10",
      provider: "EXA",
      query: "amendment to hotel management agreement site:sec.gov EX-10",
      numResults: 10,
    },
    {
      id: "exa_license_lease",
      provider: "EXA",
      query: '"hotel lease" OR "license agreement" lodging site:sec.gov Archives/edgar',
      numResults: 8,
    },
    {
      id: "exa_renewal_extension",
      provider: "EXA",
      query: "hotel management agreement renewal OR extension executed site:sec.gov",
      numResults: 8,
    },
    {
      id: "exa_termination",
      provider: "EXA",
      query: "termination of hotel management agreement OR franchise agreement site:sec.gov EX-10",
      numResults: 8,
    },
  ];
}

const EX10_URL_RE = /ex[-_]?10|dex10|exv10|exhibit[-_]?10|hotelmanagemen|franchise.?agreem|management.?agreem/i;
const HOTEL_CONTRACT_CACHE_RE =
  /hotel management|management agreement|franchise agreement|license agreement|hotel lease|operating agreement|renewal|amendment.*agreement/i;

/**
 * Mine previously paid SEC-API full-text caches for EX-10 hotel-contract URLs.
 * Free incremental discovery — does not call SEC-API again.
 */
export function mineSecApiCacheForEx10Candidates(cacheDir, opts = {}) {
  const nodeFs = opts.fs || fs;
  const limit = Math.max(1, Number(opts.limit || 200) || 200);
  if (!nodeFs.existsSync(cacheDir)) return [];
  const byUrl = new Map();
  for (const name of nodeFs.readdirSync(cacheDir)) {
    const isJson = name.startsWith("full_text") && name.endsWith(".json") && !name.endsWith(".json.gz");
    const isGzip = name.startsWith("full_text") && name.endsWith(".json.gz");
    if (!isJson && !isGzip) continue;
    const loaded = readCacheFile(path.join(cacheDir, name), { throwOnError: false });
    if (!loaded.ok) continue;
    const raw = loaded.record;
    const filings = raw?.result?.filings || raw?.result?.body?.filings || [];
    for (const x of filings) {
      const url = String(x.filingUrl || x.linkToHtml || x.url || "").trim();
      if (!url || !/^https?:\/\/www\.sec\.gov\//i.test(url)) continue;
      const blob = `${url} ${x.description || ""} ${x.type || ""} ${x.formType || ""} ${x.companyName || ""} ${x.companyNameLong || ""}`;
      if (!EX10_URL_RE.test(url) && !HOTEL_CONTRACT_CACHE_RE.test(blob)) continue;
      // Prefer exhibit document URLs over prospectus supplements
      if (/\b424b|\.pdf$/i.test(url) && !EX10_URL_RE.test(url)) continue;
      if (byUrl.has(url)) continue;
      const cikFromUrl = url.match(/\/data\/(\d+)\//)?.[1] || null;
      const cik =
        x.cik != null
          ? String(x.cik).padStart(10, "0")
          : cikFromUrl
            ? String(cikFromUrl).padStart(10, "0")
            : null;
      const accession =
        x.accessionNo ||
        x.accessionNumber ||
        url.match(/\/(\d{10}-\d{2}-\d{6})\//)?.[1] ||
        null;
      byUrl.set(url, {
        channel: "SEC_API_IO_CACHE",
        title: x.companyNameLong || x.companyName || null,
        cik,
        accession,
        form: x.formType || x.form || "EX-10",
        filedAt: x.filedAt || x.filingDate || null,
        url,
      });
      if (byUrl.size >= limit) break;
    }
    if (byUrl.size >= limit) break;
  }
  return [...byUrl.values()];
}

/** Extract CIK + accession from a classic Archives URL when metadata is missing. */
export function enrichCandidateFromEdgarUrl(candidate = {}) {
  const url = String(candidate.url || "");
  const cikRaw = url.match(/\/data\/(\d+)\//)?.[1] || null;
  const accFromPath = url.match(/\/(\d{10})\/(\d{2})\/(\d{6})\//);
  let accession = candidate.accession || null;
  if (!accession && accFromPath) {
    accession = `${accFromPath[1]}-${accFromPath[2]}-${accFromPath[3]}`;
  } else if (!accession) {
    // Compact path: /data/{cik}/{accessionNoDashes}/file.htm
    const compact = url.match(/\/data\/\d+\/(\d{18})\//);
    if (compact) {
      const d = compact[1];
      accession = `${d.slice(0, 10)}-${d.slice(10, 12)}-${d.slice(12)}`;
    }
  }
  const cik = candidate.cik
    ? String(candidate.cik).padStart(10, "0")
    : cikRaw
      ? String(cikRaw).padStart(10, "0")
      : null;
  return { ...candidate, cik, accession };
}

export default {
  harvestExhibitDocument,
  classifyInstrument,
  classifyScope,
  buildEx10DiscoveryQueries,
  mineSecApiCacheForEx10Candidates,
  enrichCandidateFromEdgarUrl,
  AGREEMENT_INSTRUMENT_CLASS,
  SCOPE_CLASS,
};
