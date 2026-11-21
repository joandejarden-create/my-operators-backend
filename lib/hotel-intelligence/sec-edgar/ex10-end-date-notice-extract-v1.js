/**
 * Focused EX-10 end-date + notice extraction (commercial priority).
 * Calculates scheduled end from effective date + fixed term only when language supports it.
 */

import { END_DATE_STATUS } from "./ex10-document-execution-class-v1.js";
import { extractContractTermsFromText } from "./contract-terms-extractor-v1.js";

export const EX10_END_DATE_NOTICE_EXTRACT_V1 = "ex10-end-date-notice-extract-v1";

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toIso(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function addYearsIso(iso, years) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime()) || !Number.isFinite(years)) return null;
  d.setUTCFullYear(d.getUTCFullYear() + Number(years));
  return d.toISOString().slice(0, 10);
}

function addDaysIso(iso, days) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime()) || !Number.isFinite(days)) return null;
  d.setUTCDate(d.getUTCDate() + Number(days));
  return d.toISOString().slice(0, 10);
}

/** Brand-led property names only — avoid TOC / policy boilerplate. */
const BRAND_HOTEL_RE =
  /\b((?:Courtyard|Residence Inn|SpringHill Suites|Fairfield Inn(?:\s+&\s+Suites)?|TownePlace Suites|AC Hotel|Aloft|Moxy|Element|Hampton Inn(?:\s+&\s+Suites)?|Hilton Garden Inn|Homewood Suites|Home2 Suites(?:\s+by\s+Hilton)?|Embassy Suites|DoubleTree|Hilton|Conrad|Waldorf Astoria|Holiday Inn(?:\s+Express)?|Crowne Plaza|Staybridge Suites|Candlewood Suites|Hotel Indigo|InterContinental|Even Hotels|Hyatt(?:\s+(?:Place|House|Regency|Centric))?|Marriott|Sheraton|Westin|W Hotel|Renaissance|Autograph Collection|Tribute Portfolio|Design Hotels|Radisson(?:\s+Blu)?|Park Plaza|Best Western|Comfort Inn(?:\s+&\s+Suites)?|Quality Inn|Sleep Inn|Clarion|Baymont|Days Inn|Super 8|La Quinta|Wyndham|Ramada|Travelodge|Ascend)(?:\s+[A-Z][A-Za-z0-9.'-]{1,24}){0,5}(?:\s+(?:Hotel|Inn|Suites|Resort|Lodge))?)/g;

const REJECT_HOTEL_NAME =
  /\b(table of contents|section|article|agreement|amendment|termination|expenses|reimbursement|payments|solicitation|employees|operating term|policies|determine|renovation|sourcing|design approvals|qualifying|qualified lodging|lessee|code for)\b|&(?:amp|rdquo|ldquo|nbsp);/i;

/** True when candidate looks like a specific branded property name. */
export function isQualityHotelName(name) {
  const n = String(name || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/&amp;/gi, "&");
  if (n.length < 14 || n.length > 72) return false;
  if (REJECT_HOTEL_NAME.test(n)) return false;
  if (/^\d/.test(n)) return false;
  if (!/^[A-Z]/.test(n)) return false;
  if (
    /\b(Franchising|International|Agreements?|TERMS|BRAND SPECIFIC|Conference Centers?|Inc\.?|LLC|Corp\.?)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  // Must start with a known brand family token
  if (
    !/^(Courtyard|Residence Inn|SpringHill|Fairfield|TownePlace|AC Hotel|Aloft|Moxy|Element|Hampton|Hilton|Homewood|Home2|Embassy|DoubleTree|Conrad|Waldorf|Holiday Inn|Crowne Plaza|Staybridge|Candlewood|Hotel Indigo|InterContinental|Even Hotels|Hyatt|Marriott|Sheraton|Westin|W Hotel|Renaissance|Autograph|Tribute|Design Hotels|Radisson|Park Plaza|Best Western|Comfort|Quality|Sleep|Clarion|Baymont|Days Inn|Super 8|La Quinta|Wyndham|Ramada|Travelodge|Ascend)/i.test(
      n,
    )
  ) {
    return false;
  }
  // Require a location / property qualifier beyond bare brand (e.g. "Hampton Inn Austin")
  const bareBrand =
    /^(Courtyard|Residence Inn|SpringHill Suites|Fairfield Inn(?:\s+&\s+Suites)?|Hampton Inn(?:\s+&\s+Suites)?|Hilton Garden Inn|Homewood Suites|Home2 Suites|Embassy Suites|Holiday Inn(?:\s+Express)?|Hyatt Place(?:\s+Hotel)?|Hyatt House|Marriott(?:\s+(?:Hotels|Marquis(?:\s+Hotels)?))?|Hilton(?:\s+Hotels)?)$/i;
  if (bareBrand.test(n)) return false;
  // Require a place-like qualifier (city / airport / downtown / etc.)
  if (
    !/\b(Airport|Downtown|Financial|Center|Centre|Plaza|Park|Beach|Bay|Valley|Hills|Heights|Station|Square|Island|Resort|University|Medical|Convention)\b|[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?/i.test(
      n.replace(/^(Courtyard|Residence Inn|SpringHill Suites|Fairfield Inn(?:\s+&\s+Suites)?|Hampton Inn(?:\s+&\s+Suites)?|Hilton Garden Inn|Homewood Suites|Home2 Suites(?:\s+by\s+Hilton)?|Embassy Suites|Holiday Inn(?:\s+Express)?|Hyatt Place|Hyatt House|Marriott|Hilton|Sheraton|Westin)\s*/i, ""),
    )
  ) {
    return false;
  }
  return true;
}

/**
 * Extract hotel name candidates from exhibit text (named-hotel prioritization).
 */
export function extractNamedHotelCandidates(text) {
  const t = String(text || "").slice(0, 120000);
  const names = new Set();
  let m;
  BRAND_HOTEL_RE.lastIndex = 0;
  while ((m = BRAND_HOTEL_RE.exec(t)) && names.size < 20) {
    const n = m[1].replace(/\s+/g, " ").trim().replace(/&amp;/gi, "&");
    if (isQualityHotelName(n)) names.add(n);
  }
  const alt = [
    ...t.matchAll(/\b(?:hotel\s+known\s+as|property\s+known\s+as)\s+["']([^"']{6,70})["']/gi),
  ];
  for (const a of alt.slice(0, 5)) {
    const n = String(a[1] || "").replace(/\s+/g, " ").trim();
    if (isQualityHotelName(n) || (/Hotel|Inn|Suites|Resort/i.test(n) && !REJECT_HOTEL_NAME.test(n) && n.length <= 70)) {
      names.add(n);
    }
  }
  return [...names];
}

/**
 * Primary commercial extraction: end dates + notice.
 */
export function extractEndDateAndNotice(doc, meta = {}) {
  const text = stripHtml(doc?.text || doc?.html || "").slice(0, 200000);
  const terms = extractContractTermsFromText(text, meta);
  const hotels = extractNamedHotelCandidates(text);

  let explicitEnd = null;
  let explicitEndExcerpt = null;
  let endStatus = END_DATE_STATUS.NOT_DISCLOSED;
  const assumptions = [];

  // Explicit expiration / termination-effective
  let m = text.match(
    /\b(?:shall\s+)?(?:expir(?:e|es|ation)|terminat(?:e|es|ion)\s+(?:date|effective))\s*(?:date|on|as\s+of)?[:\s]+(?:on\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}-\d{2}-\d{2})/i,
  );
  if (!m) {
    m = text.match(
      /\b(?:initial\s+)?term\s+(?:shall\s+)?(?:expir|end|continue\s+until)\s+(?:on\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i,
    );
  }
  if (m) {
    const iso = toIso(m[1]);
    const y = iso ? Number(iso.slice(0, 4)) : null;
    if (iso && y >= 1990 && y <= 2055) {
      explicitEnd = iso;
      explicitEndExcerpt = m[0].slice(0, 280);
      endStatus = END_DATE_STATUS.EXPLICIT_CONTRACT_END_DATE;
    }
  }

  // Effective / commencement + fixed term years → calculated end
  let calculatedEnd = null;
  let calcExcerpt = null;
  const effective =
    toIso(terms.terms?.commencementRaw) ||
    (() => {
      const em = text.match(
        /\b(?:effective|commencement)\s+date[:\s]+(?:as\s+of\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i,
      );
      return em ? toIso(em[1]) : null;
    })();
  const termYears = terms.terms?.originalTermYears;
  if (
    !explicitEnd &&
    effective &&
    Number.isFinite(termYears) &&
    termYears >= 1 &&
    termYears <= 40
  ) {
    // Only calculate when language explicitly links term length near commencement/effective date
    const linked = new RegExp(
      String.raw`(?:initial\s+)?term\s+of\s+${termYears}\s+years?.{0,160}(?:commenc|effective)|(?:commenc|effective).{0,160}(?:initial\s+)?term\s+(?:of\s+)?${termYears}\s+years?`,
      "i",
    );
    if (linked.test(text)) {
      const calc = addYearsIso(effective, termYears);
      const y = calc ? Number(calc.slice(0, 4)) : null;
      // Fail closed on implausible far-future calculated ends
      if (calc && y && y >= 1990 && y <= 2055) {
        calculatedEnd = calc;
        endStatus = END_DATE_STATUS.CALCULATED_FROM_VERIFIED_TERMS;
        assumptions.push(`scheduledEnd = effective(${effective}) + ${termYears} years`);
        calcExcerpt = `effective ${effective}; term ${termYears} years`;
      }
    }
  }

  const scheduledEnd = explicitEnd || calculatedEnd;
  if (scheduledEnd && !meta.currentStatusVerified) {
    if (endStatus === END_DATE_STATUS.EXPLICIT_CONTRACT_END_DATE || endStatus === END_DATE_STATUS.CALCULATED_FROM_VERIFIED_TERMS) {
      endStatus = END_DATE_STATUS.HISTORICAL_DATE_STATUS_UNCONFIRMED;
    }
  }
  if (meta.currentStatusVerified && scheduledEnd) {
    endStatus = END_DATE_STATUS.CURRENTLY_CORROBORATED_END_DATE;
  }
  if (meta.terminationExecutedVerified) {
    endStatus = END_DATE_STATUS.TERMINATION_EXECUTED_VERIFIED;
  }

  const noticeDays = terms.terms?.renewalNoticeDays ?? null;
  let noticeDeadline = null;
  if (scheduledEnd && Number.isFinite(noticeDays) && noticeDays > 0) {
    noticeDeadline = addDaysIso(scheduledEnd, -noticeDays);
    assumptions.push(`noticeDeadline = end(${scheduledEnd}) − ${noticeDays} days`);
  }

  // Early termination right ≠ executed termination
  const earlyTerminationRight = Boolean(
    terms.terms?.terminationWithoutCause ||
      /\bright\s+to\s+terminat|\bearly\s+terminat/i.test(text.slice(0, 50000)),
  );

  return {
    version: EX10_END_DATE_NOTICE_EXTRACT_V1,
    namedHotelCandidates: hotels,
    primaryHotelName: hotels[0] || null,
    scheduledEndDate: scheduledEnd,
    endDateStatus: endStatus,
    explicitEndDate: explicitEnd,
    calculatedEndDate: calculatedEnd,
    explicitEndExcerpt,
    calculationProvenance: calcExcerpt,
    assumptions,
    commencementOrEffectiveDate: effective,
    originalTermYears: termYears ?? null,
    noticePeriodDays: noticeDays,
    noticeDeadline,
    noticeStatus: noticeDays != null ? "VERIFIED" : "NOTICE_NOT_DISCLOSED",
    earlyTerminationRight,
    earlyTerminationIsNotExecutedTermination: true,
    renewalOptionsCount: terms.terms?.renewalOptionsCount ?? null,
    secondaryTerms: terms.terms,
    evidence: terms.evidence,
  };
}

export default { extractEndDateAndNotice, extractNamedHotelCandidates };
