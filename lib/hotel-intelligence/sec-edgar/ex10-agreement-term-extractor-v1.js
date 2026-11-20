/**
 * Extract hotel-level or property-named agreement term / expiration language
 * from EX-10 management / franchise agreements (prose, not schedule tables).
 *
 * Prefer property-named dates. Portfolio-wide "Initial Term of twenty years"
 * without hotel identity is retained as portfolioWide and rejected by verifier.
 */

import crypto from "node:crypto";
import { DATE_MEANING, SOURCE_FAMILY, isHotelContractDateMeaning } from "./contract-timing-types-v1.js";
import { AGREEMENT_TYPE, HISTORICAL_APPLICABILITY, VERIFICATION_CLASS } from "./contract.js";
import { detectBrandFamily } from "./brand-families-v2.js";

export const EX10_AGREEMENT_TERM_EXTRACTOR_V1 = "ex10-agreement-term-extractor-v1";

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<\/(p|div|br|li|h\d|tr)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function parseDateToken(token) {
  const t = String(token || "").trim();
  if (!t) return null;
  if (/^\d{4}$/.test(t)) return { raw: t, iso: `${t}-12-31`, year: Number(t), precision: "YEAR" };
  const mdy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (mdy) {
    const y = mdy[3].length === 2 ? 2000 + Number(mdy[3]) : Number(mdy[3]);
    const iso = `${y}-${String(Number(mdy[1])).padStart(2, "0")}-${String(Number(mdy[2])).padStart(2, "0")}`;
    return { raw: t, iso, year: y, precision: "DAY" };
  }
  const named = t.match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})$/i,
  );
  if (named) {
    const months = {
      january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
      july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
    };
    const m = months[named[1].toLowerCase()];
    const iso = `${named[3]}-${String(m).padStart(2, "0")}-${String(Number(named[2])).padStart(2, "0")}`;
    return { raw: t, iso, year: Number(named[3]), precision: "DAY" };
  }
  return null;
}

function findingId(parts) {
  return `ctim_${crypto.createHash("sha1").update(parts.filter(Boolean).join("|").toLowerCase()).digest("hex").slice(0, 20)}`;
}

function agreementTypeFromText(blob) {
  if (/franchise/i.test(blob)) return AGREEMENT_TYPE.FRANCHISE;
  if (/license/i.test(blob)) return AGREEMENT_TYPE.LICENSE;
  if (/management|hma|operator/i.test(blob)) return AGREEMENT_TYPE.MANAGEMENT;
  return AGREEMENT_TYPE.UNKNOWN;
}

function meaningFromContext(blob, agreementType) {
  const b = blob.toLowerCase();
  if (/termination right|right to terminat|early termin/.test(b)) return DATE_MEANING.TERMINATION_RIGHT_DATE;
  if (/renewal option/.test(b)) return DATE_MEANING.RENEWAL_OPTION_EXPIRATION;
  if (/loan matur|mortgage matur/.test(b)) return DATE_MEANING.LOAN_MATURITY_DATE;
  if (agreementType === AGREEMENT_TYPE.FRANCHISE) return DATE_MEANING.FRANCHISE_EXPIRATION;
  if (agreementType === AGREEMENT_TYPE.LICENSE) return DATE_MEANING.LICENSE_EXPIRATION;
  if (agreementType === AGREEMENT_TYPE.MANAGEMENT) return DATE_MEANING.MANAGEMENT_EXPIRATION;
  if (/expir/.test(b)) return DATE_MEANING.AMBIGUOUS_DATE;
  return DATE_MEANING.AGREEMENT_EFFECTIVE_DATE;
}

const HOTEL_RE =
  /\b((?:Courtyard|Residence Inn|SpringHill Suites|Fairfield Inn(?:\s+&?\s*Suites)?|Hampton Inn(?:\s+&?\s*Suites)?|Embassy Suites|Hilton(?:\s+Garden\s+Inn)?|Homewood Suites|Holiday Inn(?:\s+Express)?|Crowne Plaza|Sheraton|Westin|Hyatt(?:\s+Place|\s+House|\s+Regency)?|Marriott|DoubleTree|Renaissance|Autograph|Curio|Tapestry|W\s+Hotel|Ritz[- ]Carlton)[^,.\n]{0,60})/i;

/**
 * @param {{text?:string,html?:string}} doc
 * @param {object} meta
 */
export function extractEx10AgreementTerms(doc, meta = {}) {
  const raw = doc?.text || doc?.html || "";
  const text = stripHtml(raw);
  const findings = [];
  const rejected = [];
  const agreementType = agreementTypeFromText(text.slice(0, 4000) + " " + (meta.form || ""));
  const dateAlt =
    "(\\d{1,2}/\\d{1,2}/\\d{2,4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},?\\s+\\d{4}|\\d{4})";

  const patterns = [
    new RegExp(
      `(?:expires?|expiration(?:\\s+date)?|terminat(?:es|ion)?(?:\\s+date)?|initial\\s+term\\s+(?:ends?|expir(?:es|ation)?))\\s+(?:on\\s+|as\\s+of\\s+)?${dateAlt}`,
      "gi",
    ),
    new RegExp(
      `(?:term\\s+(?:of\\s+)?(?:this\\s+)?(?:agreement|franchise|management)\\s+(?:shall\\s+)?(?:expire|end|terminate)[s]?\\s+(?:on\\s+)?)${dateAlt}`,
      "gi",
    ),
  ];

  // Property-attributed expiration sentences
  const propRe = new RegExp(
    `${HOTEL_RE.source}[^.]{0,120}(?:expires?|expiration|terminat(?:es|ion))[^.]{0,40}${dateAlt}`,
    "gi",
  );

  for (const re of patterns) {
    let m;
    while ((m = re.exec(text)) && findings.length < 40) {
      const parsed = parseDateToken(m[1]);
      if (!parsed || parsed.year < 1995) continue;
      const start = Math.max(0, m.index - 100);
      const snippet = text.slice(start, m.index + m[0].length + 40).replace(/\s+/g, " ").trim();
      const hotelMatch = snippet.match(HOTEL_RE);
      const hotelName = hotelMatch ? hotelMatch[1].trim() : null;
      const meaning = meaningFromContext(snippet, agreementType);
      if (!isHotelContractDateMeaning(meaning) && meaning !== DATE_MEANING.LOAN_MATURITY_DATE) {
        rejected.push({ reason: "ambiguous_or_non_contract", snippet: snippet.slice(0, 160) });
        continue;
      }
      const brandHit = detectBrandFamily(hotelName || snippet);
      const row = {
        findingId: findingId([meta.cik, meta.accessionNumber, hotelName, meaning, parsed.iso]),
        hotelName: hotelName || meta.propertyNameHint || null,
        city: null,
        stateRegion: null,
        brandFamily: brandHit.family,
        brandLabel: brandHit.brandLabel,
        dateMeaning: meaning,
        agreementType,
        primaryDate: parsed.iso,
        datePrecision: parsed.precision,
        dateRaw: parsed.raw,
        isHotelContractDate: isHotelContractDateMeaning(meaning),
        isLoanMaturity: meaning === DATE_MEANING.LOAN_MATURITY_DATE,
        verificationClass: hotelName
          ? VERIFICATION_CLASS.VERIFIED_EXPIRATION
          : VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE,
        historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
        hotelAttributed: Boolean(hotelName),
        portfolioWide: !hotelName,
        snippet: snippet.slice(0, 280),
        discoveryMethod: "EX10_PROSE_TERM",
        source: {
          cik: meta.cik || null,
          companyName: meta.companyName || null,
          accessionNumber: meta.accessionNumber || null,
          formType: meta.form || null,
          filingDate: meta.filingDate || null,
          documentUrl: meta.documentUrl || null,
          sourceFamily: meta.sourceFamily || SOURCE_FAMILY.MANAGEMENT_EXHIBIT_EX10,
        },
        notes: hotelName ? ["EX10_PROPERTY_ATTRIBUTED"] : ["EX10_PORTFOLIO_OR_UNATTRIBUTED"],
      };
      if (row.isLoanMaturity) continue;
      if (!row.isHotelContractDate) continue;
      findings.push(row);
    }
  }

  let pm;
  while ((pm = propRe.exec(text)) && findings.length < 60) {
    const hotelName = String(pm[1] || "").trim();
    const parsed = parseDateToken(pm[2]);
    if (!parsed || !hotelName) continue;
    const meaning = meaningFromContext(pm[0], agreementType);
    if (!isHotelContractDateMeaning(meaning)) continue;
    const brandHit = detectBrandFamily(hotelName);
    findings.push({
      findingId: findingId([meta.cik, meta.accessionNumber, hotelName, meaning, parsed.iso]),
      hotelName,
      city: null,
      stateRegion: null,
      brandFamily: brandHit.family,
      brandLabel: brandHit.brandLabel,
      dateMeaning: meaning,
      agreementType,
      primaryDate: parsed.iso,
      datePrecision: parsed.precision,
      dateRaw: parsed.raw,
      isHotelContractDate: true,
      isLoanMaturity: false,
      verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
      hotelAttributed: true,
      portfolioWide: false,
      snippet: pm[0].replace(/\s+/g, " ").slice(0, 280),
      discoveryMethod: "EX10_PROSE_PROPERTY",
      source: {
        cik: meta.cik || null,
        companyName: meta.companyName || null,
        accessionNumber: meta.accessionNumber || null,
        formType: meta.form || null,
        filingDate: meta.filingDate || null,
        documentUrl: meta.documentUrl || null,
        sourceFamily: meta.sourceFamily || SOURCE_FAMILY.MANAGEMENT_EXHIBIT_EX10,
      },
      notes: ["EX10_PROPERTY_ATTRIBUTED"],
    });
  }

  const byId = new Map();
  for (const f of findings) {
    if (!byId.has(f.findingId)) byId.set(f.findingId, f);
  }

  return {
    ok: true,
    version: EX10_AGREEMENT_TERM_EXTRACTOR_V1,
    hotelContractFindings: [...byId.values()].filter((f) => f.hotelAttributed && !f.portfolioWide),
    portfolioFindings: [...byId.values()].filter((f) => f.portfolioWide),
    rejected,
    counts: {
      hotelContractDates: [...byId.values()].filter((f) => f.hotelAttributed && !f.portfolioWide).length,
      portfolioWide: [...byId.values()].filter((f) => f.portfolioWide).length,
      rejected: rejected.length,
    },
  };
}

export default { extractEx10AgreementTerms, EX10_AGREEMENT_TERM_EXTRACTOR_V1 };
