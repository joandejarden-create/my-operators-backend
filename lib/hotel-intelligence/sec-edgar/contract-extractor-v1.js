/**
 * Deterministic SEC filing contract extraction (no AI by default).
 * Keyword/context windows only — does not invent hotel attribution.
 */

import {
  AGREEMENT_TYPE,
  VERIFICATION_CLASS,
  EVIDENCE_FIELD_NAMES,
} from "./contract.js";

export const CONTRACT_EXTRACTOR_VERSION = "sec-edgar-contract-extractor-v1";

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/\s+/g, " ")
    .trim();
}

const DATE_RE =
  /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b|\b\d{1,2}\/\d{1,2}\/\d{2,4}\b|\b\d{4}-\d{2}-\d{2}\b/gi;

const HOTEL_NAME_HINT =
  /\b((?:The\s+)?(?:\d+\s+)?[A-Z][A-Za-z0-9&.'\-]{1,40}(?:\s+[A-Z][A-Za-z0-9&.'\-]{1,30}){0,6}\s+(?:Hotel|Inn|Resort|Suites))\b|\b((?:Courtyard|Residence Inn|SpringHill Suites|Fairfield|Aloft|Moxy|Westin|Sheraton|Renaissance|Ritz[- ]Carlton|St\.?\s*Regis|W Hotel|Edition|Autograph Collection|Tribute Portfolio|Design Hotels)[A-Za-z0-9&.'\- ]{0,50})\b/g;

const HOTEL_HINT_BLOCKLIST =
  /\b(agreement|amended|restated|bylaws|llc|inc\.?|corp\.?|partnership|purchaser|director|award|stock|shareholder|indemnification|limited liability)\b/i;

function classifyAgreement(snippet) {
  const s = snippet.toLowerCase();
  if (/franchise agreement|franchisee|franchisor/.test(s)) return AGREEMENT_TYPE.FRANCHISE;
  if (/management agreement|hotel management agreement|\bhma\b|managed by/.test(s))
    return AGREEMENT_TYPE.MANAGEMENT;
  if (/ground lease|hotel lease|lease agreement/.test(s) && !/management agreement/.test(s))
    return AGREEMENT_TYPE.LEASE;
  if (/soft.?brand|affiliation agreement|brand license/.test(s)) return AGREEMENT_TYPE.AFFILIATION;
  return AGREEMENT_TYPE.UNKNOWN;
}

function classifyMilestone(snippet, agreementType) {
  const s = snippet.toLowerCase();
  if (/terminat(ed|ion|es|ing)\b/.test(s) && /agreement|management|franchise/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_TERMINATION;
  }
  if (/amend(ed|ment)|extension|extended the term|extended through/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_AMENDMENT;
  }
  if (/renewal option|option to renew|may renew|automatic renewal/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_RENEWAL_OPTION;
  }
  if (/notice.{0,40}renew|renewal notice|notice period/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_RENEWAL_NOTICE_DEADLINE;
  }
  if (/expir(e|es|ed|ation)|term (ends|ending|expires)|through \d{4}|until \d{4}/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_EXPIRATION;
  }
  if (/effective (as of|date)|commenced|commencement/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_EFFECTIVE_DATE;
  }
  if (/signed|executed|entered into|dated as of/.test(s)) {
    return VERIFICATION_CLASS.VERIFIED_SIGNING_DATE;
  }
  if (agreementType !== AGREEMENT_TYPE.UNKNOWN) {
    return VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;
  }
  return VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;
}

function fieldNameFor(cls, agreementType) {
  if (agreementType === AGREEMENT_TYPE.FRANCHISE) {
    if (cls === VERIFICATION_CLASS.VERIFIED_SIGNING_DATE) return EVIDENCE_FIELD_NAMES.FRANCHISE_SIGNING;
    if (cls === VERIFICATION_CLASS.VERIFIED_EFFECTIVE_DATE) return EVIDENCE_FIELD_NAMES.FRANCHISE_EFFECTIVE;
    if (cls === VERIFICATION_CLASS.VERIFIED_EXPIRATION) return EVIDENCE_FIELD_NAMES.FRANCHISE_EXPIRATION;
    if (cls === VERIFICATION_CLASS.VERIFIED_RENEWAL_OPTION) return EVIDENCE_FIELD_NAMES.FRANCHISE_RENEWAL;
  }
  if (agreementType === AGREEMENT_TYPE.LEASE) {
    if (cls === VERIFICATION_CLASS.VERIFIED_EFFECTIVE_DATE) return EVIDENCE_FIELD_NAMES.LEASE_COMMENCEMENT;
    if (cls === VERIFICATION_CLASS.VERIFIED_EXPIRATION) return EVIDENCE_FIELD_NAMES.LEASE_EXPIRATION;
  }
  if (cls === VERIFICATION_CLASS.VERIFIED_SIGNING_DATE) return EVIDENCE_FIELD_NAMES.MGMT_SIGNING;
  if (cls === VERIFICATION_CLASS.VERIFIED_EFFECTIVE_DATE) return EVIDENCE_FIELD_NAMES.MGMT_EFFECTIVE;
  if (cls === VERIFICATION_CLASS.VERIFIED_EXPIRATION) return EVIDENCE_FIELD_NAMES.MGMT_EXPIRATION;
  if (cls === VERIFICATION_CLASS.VERIFIED_RENEWAL_OPTION) return EVIDENCE_FIELD_NAMES.MGMT_RENEWAL_OPTION;
  if (cls === VERIFICATION_CLASS.VERIFIED_RENEWAL_NOTICE_DEADLINE)
    return EVIDENCE_FIELD_NAMES.MGMT_RENEWAL_NOTICE;
  if (cls === VERIFICATION_CLASS.VERIFIED_TERMINATION) return EVIDENCE_FIELD_NAMES.MGMT_TERMINATION;
  if (cls === VERIFICATION_CLASS.VERIFIED_AMENDMENT) return EVIDENCE_FIELD_NAMES.AGREEMENT_AMENDMENT;
  return "Contractual Disclosure";
}

function windowAround(text, index, radius = 220) {
  const start = Math.max(0, index - radius);
  const end = Math.min(text.length, index + radius);
  return text.slice(start, end).trim();
}

/**
 * Extract candidate contractual findings from filing HTML/text.
 * Portfolio-wide mentions without a hotel name are kept but flagged unattributed.
 */
export function extractContractFindingsFromDocument(doc, meta = {}) {
  const raw = doc?.text || doc?.html || "";
  const text = stripHtml(raw);
  const findings = [];
  const keywordRes = [
    /management agreement/gi,
    /franchise agreement/gi,
    /hotel management agreement/gi,
    /\bhma\b/gi,
    /expiration (?:date|of the|of our)/gi,
    /renewal option/gi,
    /terminat(?:e|ed|ion|es)/gi,
    /amended and restated/gi,
  ];

  const seen = new Set();
  for (const re of keywordRes) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text)) !== null) {
      const snippet = windowAround(text, m.index);
      const agreementType = classifyAgreement(snippet);
      const verificationClass = classifyMilestone(snippet, agreementType);
      const dates = [...snippet.matchAll(DATE_RE)].map((d) => d[0]);
      const hotelHints = [...snippet.matchAll(HOTEL_NAME_HINT)]
        .map((h) => String(h[1] || h[2] || "").trim())
        .filter((h) => h && h.length >= 8 && h.length <= 80 && !HOTEL_HINT_BLOCKLIST.test(h));
      const key = `${verificationClass}|${dates[0] || ""}|${hotelHints[0] || ""}|${snippet.slice(0, 80)}`;
      if (seen.has(key)) continue;
      seen.add(key);

      const hotelAttributed = hotelHints.length > 0;
      // Portfolio-wide corporate disclosure must not auto-attribute to every hotel.
      let finalClass = verificationClass;
      if (!hotelAttributed && verificationClass.startsWith("VERIFIED_")) {
        // Keep as candidate but mark insufficient for property persist until attributed.
        finalClass = VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE;
      }

      findings.push({
        extractorVersion: CONTRACT_EXTRACTOR_VERSION,
        agreementType,
        verificationClass: finalClass,
        verificationClassRaw: verificationClass,
        evidenceFieldName: fieldNameFor(verificationClass, agreementType),
        datesMentioned: dates.slice(0, 4),
        primaryDate: dates[0] || null,
        hotelNameHints: hotelHints.slice(0, 5),
        hotelAttributed,
        portfolioWide: !hotelAttributed,
        snippet: snippet.slice(0, 600),
        keyword: m[0],
        source: {
          cik: meta.cik || null,
          accessionNumber: meta.accessionNumber || null,
          formType: meta.form || null,
          filingDate: meta.filingDate || null,
          documentUrl: meta.documentUrl || null,
          primaryDocument: meta.primaryDocument || null,
          companyName: meta.companyName || null,
        },
        extractedAt: new Date().toISOString(),
      });
      if (findings.length >= (meta.maxFindings || 80)) break;
    }
    if (findings.length >= (meta.maxFindings || 80)) break;
  }

  return {
    ok: true,
    version: CONTRACT_EXTRACTOR_VERSION,
    characterCount: text.length,
    findingCount: findings.length,
    keywordHitSummary: summarizeKeywords(text),
    findings,
  };
}

function summarizeKeywords(text) {
  const lower = text.toLowerCase();
  const count = (re) => (lower.match(re) || []).length;
  return {
    franchise_agreement: count(/franchise agreement/g),
    management_agreement: count(/management agreement/g),
    expiration: count(/expiration/g),
    termination: count(/terminat(?:e|ed|ion|es)/g),
    renewal: count(/renewal/g),
  };
}

/** Reject estimated / speculative language as verified. */
export function isUnsupportedExpirationClaim(snippet) {
  const s = String(snippet || "").toLowerCase();
  return (
    /approximately|expected to expire|we estimate|likely expire|may expire around|projected expiration/.test(
      s,
    ) || /could be converted|potential conversion|considering rebrand/.test(s)
  );
}
