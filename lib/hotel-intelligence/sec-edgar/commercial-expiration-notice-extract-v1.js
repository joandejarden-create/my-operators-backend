/**
 * Focused extraction for commercial targeting: expiration + notice (+ secondary fields).
 * High recall with strict verification via date-meaning audit — not full legal-term depth.
 */

import { extractContractTermsFromText } from "./contract-terms-extractor-v1.js";
import { extractCmbsPropertySchedule } from "./cmbs-property-schedule-extractor-v1.js";
import { auditContractDateObservation, AUDITED_DATE_MEANING } from "./contract-date-meaning-audit-v1.js";
import { DATE_MEANING } from "./contract-timing-types-v1.js";

export const COMMERCIAL_EXPIRATION_NOTICE_EXTRACT_V1 = "commercial-expiration-notice-extract-v1";

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const EXPLICIT_EXP_DATE =
  /\b(?:franchise|management|license|operating)\s+(?:agreement\s+)?(?:expir(?:es|ation|y)|terminat(?:es|ion))\s*(?:date|on)?[:\s]+(?:on\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}-\d{2}-\d{2})/gi;

const HOTEL_NAME_NEAR =
  /\b([A-Z][A-Za-z0-9 &''.-]{3,60}(?:Hotel|Inn|Suites|Resort|Lodge|Courtyard|Residence Inn|Hampton|Hilton|Marriott|Hyatt|Holiday Inn)[A-Za-z0-9 &''.-]{0,40})/g;

function toIsoDate(raw) {
  const s = String(raw || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{4}$/.test(s)) return `${s}-12-31`; // year-only — audit must still validate meaning
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

/**
 * Extract commercial timing observations from a document.
 * Returns candidates that still require classifyCommercialOpportunity + date gate.
 */
export function extractCommercialTimingObservations(doc, meta = {}) {
  const raw = doc?.text || stripHtml(doc?.html || "");
  const text = raw.slice(0, 500000);
  const observations = [];
  const rejected = [];

  // 1) CMBS / delimited schedules — prefer explicit franchise/management expiration columns
  try {
    const schedule = extractCmbsPropertySchedule(
      { text },
      {
        documentUrl: meta.sourceUrl,
        accessionNumber: meta.accession,
        cik: meta.cik,
        companyName: meta.issuerName,
        sourceFamily: meta.sourceFamily,
      },
    );
    const list = schedule?.hotelContractFindings || schedule?.findings || [];
    for (const f of list) {
      const primaryDate = f.primaryDate || f.date || f.expirationDate || null;
      const meaning = f.dateMeaning || f.meaning || null;
      const hotelName = f.hotelName || f.propertyName || null;
      if (!hotelName || !primaryDate) continue;

      // Reject loan-only / ambiguous before promoting
      if (
        meaning === DATE_MEANING.LOAN_MATURITY_DATE ||
        meaning === DATE_MEANING.AMBIGUOUS_DATE
      ) {
        rejected.push({
          hotelName,
          primaryDate,
          reason: `schedule_meaning_${meaning}`,
          sourceUrl: meta.sourceUrl,
        });
        continue;
      }

      const rawSnippet =
        f.snippet || f.evidenceSnippet || f.rowText || `${hotelName} ${meaning} ${primaryDate}`;
      // Ensure column-header provenance reaches the date-meaning audit
      const snippet = /franchise expiration|management agreement expiration/i.test(rawSnippet)
        ? rawSnippet
        : meaning === DATE_MEANING.FRANCHISE_EXPIRATION
          ? `Franchise Expiration Date | ${rawSnippet}`
          : meaning === DATE_MEANING.MANAGEMENT_EXPIRATION
            ? `Management Agreement Expiration | ${rawSnippet}`
            : rawSnippet;
      const audit = auditContractDateObservation({
        primaryDate,
        dateMeaning: meaning,
        evidenceSnippet: snippet,
        hotelName,
        sourceUrls: meta.sourceUrl ? [meta.sourceUrl] : [],
      });

      if (
        audit.auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT ||
        audit.auditedMeaning === AUDITED_DATE_MEANING.LOAN_MATURITY
      ) {
        rejected.push({
          hotelName,
          primaryDate,
          reason: `audited_${audit.auditedMeaning}`,
          sourceUrl: meta.sourceUrl,
        });
        continue;
      }

      observations.push({
        hotelName,
        hotelVerified: true,
        hotelAttributed: true,
        location:
          [f.city, f.stateRegion || f.state].filter(Boolean).join(", ") || f.location || null,
        brand: f.brandLabel || f.brand || f.franchise || null,
        owner: f.borrower || f.owner || null,
        managementCompany: f.manager || f.operator || null,
        agreementType:
          meaning === DATE_MEANING.MANAGEMENT_EXPIRATION
            ? "MANAGEMENT"
            : meaning === DATE_MEANING.LICENSE_EXPIRATION
              ? "LICENSE"
              : "FRANCHISE",
        primaryDate: toIsoDate(primaryDate) || primaryDate,
        dateMeaning: meaning,
        dateAudit: audit,
        evidenceSnippet: String(snippet).slice(0, 400),
        sourceUrl: meta.sourceUrl || null,
        accession: meta.accession || null,
        cik: meta.cik || null,
        issuerName: meta.issuerName || null,
        sourceFamily: "CMBS_OR_SCHEDULE",
        renewalNoticeDays: f.renewalNoticeDays || null,
      });
    }
    for (const r of schedule?.rejected || []) {
      rejected.push({
        hotelName: r.hotelName || null,
        reason: r.reason || "schedule_rejected",
        sourceUrl: meta.sourceUrl,
      });
    }
  } catch {
    // schedule parser optional
  }

  // 2) Narrative EX-10 / agreement prose — hotel-attributed expiration language
  const terms = extractContractTermsFromText(text.slice(0, 120000), meta);
  let m;
  EXPLICIT_EXP_DATE.lastIndex = 0;
  while ((m = EXPLICIT_EXP_DATE.exec(text)) && observations.length < 400) {
    const dateRaw = m[1];
    const iso = toIsoDate(dateRaw);
    if (!iso) continue;
    const windowStart = Math.max(0, m.index - 220);
    const window = text.slice(windowStart, m.index + m[0].length + 80);
    const hotelMatch = window.match(HOTEL_NAME_NEAR);
    const hotelName = hotelMatch?.[hotelMatch.length - 1] || meta.hotelName || null;
    const isFranchise = /franchise/i.test(m[0]);
    const isMgmt = /management|operating/i.test(m[0]);
    const meaning = isMgmt
      ? DATE_MEANING.MANAGEMENT_EXPIRATION
      : isFranchise
        ? DATE_MEANING.FRANCHISE_EXPIRATION
        : DATE_MEANING.LICENSE_EXPIRATION;

    const audit = auditContractDateObservation({
      primaryDate: iso,
      dateMeaning: meaning,
      evidenceSnippet: window,
      hotelName,
      sourceUrls: meta.sourceUrl ? [meta.sourceUrl] : [],
    });

    if (
      !hotelName ||
      audit.auditedMeaning === AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT ||
      audit.auditedMeaning === AUDITED_DATE_MEANING.LOAN_MATURITY ||
      audit.auditedMeaning === AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING
    ) {
      rejected.push({
        hotelName,
        primaryDate: iso,
        reason: `narrative_${audit.auditedMeaning || "no_hotel"}`,
        sourceUrl: meta.sourceUrl,
      });
      continue;
    }

    observations.push({
      hotelName,
      hotelVerified: true,
      hotelAttributed: true,
      agreementType: isMgmt ? "MANAGEMENT" : isFranchise ? "FRANCHISE" : "LICENSE",
      primaryDate: iso,
      dateMeaning: meaning,
      dateAudit: audit,
      renewalNoticeDays: terms.terms?.renewalNoticeDays ?? null,
      renewalOptionsCount: terms.terms?.renewalOptionsCount ?? null,
      terminationWithoutCause: terms.terms?.terminationWithoutCause ?? null,
      evidenceSnippet: window.slice(0, 400),
      sourceUrl: meta.sourceUrl || null,
      accession: meta.accession || null,
      cik: meta.cik || null,
      issuerName: meta.issuerName || null,
      sourceFamily: "NARRATIVE_EXHIBIT",
      brand: meta.brand || null,
      owner: meta.owner || null,
      managementCompany: meta.managementCompany || null,
    });
  }

  // 3) If named hotel scope from EX-10 harvest metadata but no date → Tier C seed
  if (meta.namedHotel && meta.hotelName && observations.length === 0) {
    observations.push({
      hotelName: meta.hotelName,
      hotelVerified: true,
      hotelAttributed: true,
      agreementType: meta.agreementType || "UNKNOWN",
      primaryDate: null,
      renewalNoticeDays: terms.terms?.renewalNoticeDays ?? null,
      evidenceSnippet: meta.evidenceSnippet || null,
      sourceUrl: meta.sourceUrl || null,
      accession: meta.accession || null,
      cik: meta.cik || null,
      issuerName: meta.issuerName || null,
      sourceFamily: "EXHIBIT_WITHOUT_DATE",
      brand: meta.brand || null,
      owner: meta.owner || null,
      managementCompany: meta.managementCompany || null,
      conversionSignals: {
        brand: /FRANCHISE|LICENSE/i.test(meta.agreementType || ""),
        management: /MANAGEMENT/i.test(meta.agreementType || ""),
      },
    });
  }

  return {
    version: COMMERCIAL_EXPIRATION_NOTICE_EXTRACT_V1,
    observations,
    rejected,
    secondaryTerms: {
      originalTermYears: terms.terms?.originalTermYears ?? null,
      renewalOptionsCount: terms.terms?.renewalOptionsCount ?? null,
      renewalNoticeDays: terms.terms?.renewalNoticeDays ?? null,
      note: "Secondary — not required for commercial targeting acceptance",
    },
  };
}

export default { extractCommercialTimingObservations };
