/**
 * Extract hotel-level franchise / management / license dates from CMBS and
 * REIT property schedules. Separates loan maturity from hotel contract timing.
 */

import crypto from "node:crypto";
import {
  DATE_MEANING,
  SOURCE_FAMILY,
  isHotelContractDateMeaning,
} from "./contract-timing-types-v1.js";
import { AGREEMENT_TYPE, HISTORICAL_APPLICABILITY, VERIFICATION_CLASS } from "./contract.js";
import { detectBrandFamily } from "./brand-families-v2.js";

export const CMBS_PROPERTY_SCHEDULE_EXTRACTOR_V1 = "cmbs-property-schedule-extractor-v1";

const STATE_ALT =
  "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY|DC";

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<\/(td|th|tr|p|div|li|br|h\d)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#\d+;/g, " ")
    .replace(/&amp;/gi, "&")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function parseDateToken(token) {
  const t = String(token || "").trim();
  if (!t || /^(n\/?a|none|-|—|tbd)$/i.test(t)) return null;
  if (/^\d{4}$/.test(t)) {
    return { raw: t, iso: `${t}-12-31`, year: Number(t), precision: "YEAR" };
  }
  const mdy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (mdy) {
    const y = mdy[3].length === 2 ? 2000 + Number(mdy[3]) : Number(mdy[3]);
    const m = Number(mdy[1]);
    const d = Number(mdy[2]);
    const iso = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    return { raw: t, iso, year: y, precision: "DAY" };
  }
  const named = t.match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})$/i,
  );
  if (named) {
    const months = {
      january: 1,
      february: 2,
      march: 3,
      april: 4,
      may: 5,
      june: 6,
      july: 7,
      august: 8,
      september: 9,
      october: 10,
      november: 11,
      december: 12,
    };
    const m = months[named[1].toLowerCase()];
    const d = Number(named[2]);
    const y = Number(named[3]);
    const iso = `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    return { raw: t, iso, year: y, precision: "DAY" };
  }
  return null;
}

function findingId(parts) {
  const raw = parts.filter(Boolean).join("|").toLowerCase();
  return `ctim_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 20)}`;
}

function classifyHeader(headerBlob) {
  const h = String(headerBlob || "").toLowerCase();
  return {
    hasProperty: /property|mortgaged|hotel|collateral/.test(h),
    hasFranchise: /franchise|flag|brand affiliation/.test(h),
    hasManagement: /management agreement|\bhma\b|operator agreement|managed by/.test(h),
    hasLicense: /license agreement/.test(h),
    hasExpiration: /expir|maturity|terminat|renewal/.test(h),
    hasLoanMaturity: /loan matur|mortgage matur|maturity date|anticipated repayment/.test(h),
    hasTerminationRight: /termination right|early termin|cancel/.test(h),
    hasBrand: /\bbrand\b|\bflag\b/.test(h),
    hasBorrower: /borrower|sponsor/.test(h),
    hasOwner: /property owner|fee owner|owner of record/.test(h),
  };
}

/**
 * Infer date meaning from column header + nearby row text.
 */
export function inferDateMeaning(columnHeader = "", cellContext = "") {
  const blob = `${columnHeader} ${cellContext}`.toLowerCase();
  if (/loan matur|mortgage matur|anticipated repayment|ard\b|maturity date \(loan\)/.test(blob)) {
    return DATE_MEANING.LOAN_MATURITY_DATE;
  }
  if (/termination right|early termin|right to terminat/.test(blob)) {
    return DATE_MEANING.TERMINATION_RIGHT_DATE;
  }
  if (/renewal option|extension option/.test(blob)) {
    return DATE_MEANING.RENEWAL_OPTION_EXPIRATION;
  }
  if (/franchise.*expir|expir.*franchise|franchise expiration/.test(blob)) {
    return DATE_MEANING.FRANCHISE_EXPIRATION;
  }
  if (/management.*expir|expir.*management|hma expir|management agreement expiration/.test(blob)) {
    return DATE_MEANING.MANAGEMENT_EXPIRATION;
  }
  if (/license.*expir|expir.*license/.test(blob)) {
    return DATE_MEANING.LICENSE_EXPIRATION;
  }
  if (/franchise/.test(blob) && /expir/.test(blob)) return DATE_MEANING.FRANCHISE_EXPIRATION;
  if (/management|hma|operator/.test(blob) && /expir/.test(blob)) {
    return DATE_MEANING.MANAGEMENT_EXPIRATION;
  }
  if (/expir/.test(blob) && !/loan|mortgage|maturity/.test(blob)) {
    return DATE_MEANING.AMBIGUOUS_DATE;
  }
  if (/matur/.test(blob)) return DATE_MEANING.LOAN_MATURITY_DATE;
  return DATE_MEANING.AMBIGUOUS_DATE;
}

function agreementTypeFromMeaning(meaning) {
  if (meaning === DATE_MEANING.FRANCHISE_EXPIRATION) return AGREEMENT_TYPE.FRANCHISE;
  if (meaning === DATE_MEANING.MANAGEMENT_EXPIRATION) return AGREEMENT_TYPE.MANAGEMENT;
  if (meaning === DATE_MEANING.LICENSE_EXPIRATION) return AGREEMENT_TYPE.LICENSE;
  return AGREEMENT_TYPE.UNKNOWN;
}

function classifyColumnHeader(headerCell) {
  const h = String(headerCell || "").toLowerCase();
  if (/loan matur|mortgage matur|anticipated repayment/.test(h)) return DATE_MEANING.LOAN_MATURITY_DATE;
  if (/termination right|early termin/.test(h)) return DATE_MEANING.TERMINATION_RIGHT_DATE;
  if (/franchise.*expir|expir.*franchise|franchise expiration/.test(h)) return DATE_MEANING.FRANCHISE_EXPIRATION;
  if (/management.*expir|hma expir|management agreement/.test(h)) return DATE_MEANING.MANAGEMENT_EXPIRATION;
  if (/license.*expir/.test(h)) return DATE_MEANING.LICENSE_EXPIRATION;
  if (/renewal option|extension option/.test(h)) return DATE_MEANING.RENEWAL_OPTION_EXPIRATION;
  if (/^franchise$|^flag$/.test(h.trim())) return DATE_MEANING.FRANCHISE_EXPIRATION;
  if (/^management$|^hma$/.test(h.trim())) return DATE_MEANING.MANAGEMENT_EXPIRATION;
  if (/^loan maturity$|^maturity$/.test(h.trim())) return DATE_MEANING.LOAN_MATURITY_DATE;
  return null;
}

/**
 * Parse pipe- or tab-like schedule lines with header-column memory:
 * "Property Name | City | State | Brand | Franchise Expiration | Loan Maturity | Borrower"
 */
function parseDelimitedScheduleLines(text, meta = {}) {
  const findings = [];
  const loanOnly = [];
  const rejected = [];
  const lines = text.split(/\n/).map((l) => l.trim()).filter(Boolean);
  /** @type {(string|null)[]} */
  let columnMeanings = [];

  for (const line of lines) {
    if (!/[|]/.test(line) && !/\t/.test(line)) continue;
    const cells = line.split(/[|\t]/).map((c) => c.trim()).filter(Boolean);
    if (cells.length < 4) continue;

    const headerJoin = cells.join(" ").toLowerCase();
    const isHeader =
      /property name|mortgaged property|franchise expiration|loan maturity/.test(headerJoin) &&
      !/\b(hotel|inn|resort|suites|courtyard|hilton|marriott|hyatt)\b/i.test(cells[0]);
    if (isHeader) {
      columnMeanings = cells.map((c) => classifyColumnHeader(c));
      continue;
    }

    const hotelName = cells[0];
    if (!hotelName || hotelName.length < 4) continue;
    if (/^(total|aggregate|portfolio|property name|mortgaged)/i.test(hotelName)) {
      rejected.push({ reason: "portfolio_or_header_row", line: line.slice(0, 160) });
      continue;
    }
    if (/\band\b/i.test(hotelName) && /hotel|inn|resort/i.test(hotelName.split(/\band\b/i)[1] || "")) {
      rejected.push({ reason: "multi_hotel_row", hotelName });
      continue;
    }

    let city = null;
    let state = null;
    let brand = null;
    let borrower = null;
    let owner = null;

    for (const c of cells.slice(1, 6)) {
      if (!city && /^[A-Z][a-zA-Z .'-]{2,40}$/.test(c) && !new RegExp(`^(${STATE_ALT})$`).test(c)) {
        if (!/franchise|management|license|loan|maturity|expir/i.test(c)) city = c;
      }
      if (!state && new RegExp(`^(${STATE_ALT})$`).test(c)) state = c;
    }

    for (let i = 0; i < cells.length; i++) {
      const cell = cells[i];
      let parsed = parseDateToken(cell);
      if (!parsed) {
        const embedded = cell.match(
          /(\d{1,2}\/\d{1,2}\/\d{2,4}|\d{4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})/i,
        );
        if (embedded) parsed = parseDateToken(embedded[1]);
      }
      if (!parsed) continue;

      const colMeaning = columnMeanings[i] || null;
      const prev = cells[i - 1] || "";
      const label = colMeaning
        ? String(colMeaning)
        : /expir|matur|termin|franchise|management|license|loan|renewal/i.test(prev)
          ? prev
          : /expir|matur|termin|franchise|management|license|loan|renewal/i.test(cell)
            ? cell
            : `${prev} ${cell}`;
      let meaning = colMeaning || inferDateMeaning(label, line);
      // Mixed cell overrides when it explicitly says termination right
      if (/termination right/i.test(cell)) meaning = DATE_MEANING.TERMINATION_RIGHT_DATE;

      const brandHit = detectBrandFamily(hotelName);
      const row = {
        findingId: findingId([
          meta.cik,
          meta.accessionNumber,
          hotelName,
          city,
          state,
          meaning,
          parsed.iso,
        ]),
        hotelName,
        city,
        stateRegion: state,
        brandFamily: brandHit.family,
        brandLabel: brandHit.brandLabel || brand,
        borrower,
        propertyOwner: owner,
        dateMeaning: meaning,
        agreementType: agreementTypeFromMeaning(meaning),
        primaryDate: parsed.iso,
        datePrecision: parsed.precision,
        dateRaw: parsed.raw,
        isHotelContractDate: isHotelContractDateMeaning(meaning),
        isLoanMaturity: meaning === DATE_MEANING.LOAN_MATURITY_DATE,
        verificationClass:
          meaning === DATE_MEANING.AMBIGUOUS_DATE
            ? VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE
            : meaning === DATE_MEANING.LOAN_MATURITY_DATE
              ? VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE
              : VERIFICATION_CLASS.VERIFIED_EXPIRATION,
        historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
        hotelAttributed: Boolean(hotelName && (city || state)),
        portfolioWide: false,
        snippet: line.slice(0, 280),
        source: {
          cik: meta.cik || null,
          companyName: meta.companyName || null,
          accessionNumber: meta.accessionNumber || null,
          formType: meta.form || null,
          filingDate: meta.filingDate || null,
          documentUrl: meta.documentUrl || null,
          sourceFamily: meta.sourceFamily || SOURCE_FAMILY.CMBS_424B,
        },
        notes: colMeaning ? ["COLUMN_HEADER_MAPPED"] : [],
      };

      if (row.isLoanMaturity) {
        row.notes.push(
          "LOAN_MATURITY_NOT_HOTEL_CONTRACT — retained separately; never use as franchise/management expiration",
        );
        loanOnly.push(row);
        continue;
      }
      if (!row.isHotelContractDate) {
        rejected.push({ reason: "ambiguous_date_meaning", hotelName, date: parsed.raw, meaning });
        continue;
      }
      if (!row.hotelAttributed) {
        rejected.push({ reason: "insufficient_hotel_geo", hotelName });
        continue;
      }
      findings.push(row);
    }
  }

  return { findings, loanOnly, rejected };
}

/**
 * Regex fallback for prose / single-line REIT schedule style:
 * "Courtyard Denver Downtown Denver CO Franchise 2028"
 */
function parseProseScheduleLines(text, meta = {}) {
  const findings = [];
  const rejected = [];
  const dateAlt =
    "(\\d{4}|\\d{1,2}/\\d{1,2}/\\d{2,4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},?\\s+\\d{4})";
  const city =
    "((?:San|Los|Las|New|Fort|St\\.?|Saint|Des|El|Santa)\\s+[A-Z][a-zA-Z.'-]{2,30}|[A-Z][a-zA-Z.'-]{2,40})";
  const re = new RegExp(
    `^(.+?)\\s+${city},?\\s+(${STATE_ALT})\\s+(Franchise|Management|License)\\s+(?:Agreement\\s+)?(?:expires?\\s+)?${dateAlt}\\b`,
    "i",
  );

  for (const line of text.split(/\n/).map((l) => l.trim()).filter(Boolean)) {
    const m = line.match(re);
    if (!m) continue;
    const hotelName = m[1].trim();
    if (/^(total|portfolio)/i.test(hotelName)) {
      rejected.push({ reason: "portfolio_row", hotelName });
      continue;
    }
    const agreementWord = m[4];
    const parsed = parseDateToken(m[5]);
    if (!parsed) continue;
    const meaning =
      /franchise/i.test(agreementWord)
        ? DATE_MEANING.FRANCHISE_EXPIRATION
        : /license/i.test(agreementWord)
          ? DATE_MEANING.LICENSE_EXPIRATION
          : DATE_MEANING.MANAGEMENT_EXPIRATION;
    const brandHit = detectBrandFamily(hotelName);
    findings.push({
      findingId: findingId([meta.cik, meta.accessionNumber, hotelName, m[2], m[3], meaning, parsed.iso]),
      hotelName,
      city: m[2],
      stateRegion: m[3],
      brandFamily: brandHit.family,
      brandLabel: brandHit.brandLabel,
      dateMeaning: meaning,
      agreementType: agreementTypeFromMeaning(meaning),
      primaryDate: parsed.iso,
      datePrecision: parsed.precision,
      dateRaw: parsed.raw,
      isHotelContractDate: true,
      isLoanMaturity: false,
      verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
      hotelAttributed: true,
      portfolioWide: false,
      snippet: line.slice(0, 280),
      source: {
        cik: meta.cik || null,
        companyName: meta.companyName || null,
        accessionNumber: meta.accessionNumber || null,
        formType: meta.form || null,
        filingDate: meta.filingDate || null,
        documentUrl: meta.documentUrl || null,
        sourceFamily: meta.sourceFamily || SOURCE_FAMILY.HOTEL_REIT_SCHEDULE,
      },
      notes: [],
    });
  }
  return { findings, rejected };
}

const HOTEL_NAME_RE =
  /\b(Hotel|Inn|Resort|Suites|Marriott|Courtyard|Westin|Sheraton|Hilton|Hyatt|Embassy|Hampton|Residence|SpringHill|Fairfield|Autograph|Curio|Tapestry|DoubleTree|Holiday\s+Inn|Crowne\s+Plaza|Wyndham|Radisson|Aloft|Element|AC\s+Hotel|Four\s+Points|Ritz|St\.?\s*Regis|W\s+Hotel|Motel\s+6|Super\s+8|Days\s+Inn|La\s+Quinta|Best\s+Western|Comfort\s+Inn|Quality\s+Inn|Sleep\s+Inn)\b/i;

/**
 * Plain-text extractor for the Wave-2 gold CMBS column:
 * "Expiration/Termination of Related License/ Franchise Agreement, Operating Agreement or Management Agreement"
 *
 * HTML often splits this header across <BR>/<B> tags and nests tables so
 * <table>…</table> regex misses the schedule. After stripHtml the rows appear as:
 *   "{Portfolio}––{Hotel Name} $ 15,745,967 1.4 % 6/30/2032 7/6/2027"
 * where the first MDY is franchise/license/management expiration and the second is loan maturity.
 */
function parseRelatedLicensePlainSchedule(text, meta = {}) {
  const findings = [];
  const loanOnly = [];
  const rejected = [];
  // Collapse whitespace — stripHtml inserts newlines between every TD/BR
  const decoded = String(text || "")
    .replace(/&ndash;/gi, "–")
    .replace(/&mdash;/gi, "—")
    .replace(/&#8211;/g, "–")
    .replace(/&#8212;/g, "—")
    .replace(/&amp;/gi, "&")
    .replace(/[\t\r\n]+/g, " ")
    .replace(/ {2,}/g, " ");

  const headerRe =
    /expiration\s*\/\s*termination\s+of\s+related\s+license\s*\/\s*franchise\s+agreement/i;
  const hasGoldHeader =
    headerRe.test(decoded) || /related\s+license\s*\/\s*franchise\s+agreement/i.test(decoded);
  if (!hasGoldHeader) {
    return { findings, loanOnly, rejected };
  }

  // Prefer segments after the gold header to reduce narrative false positives
  const headerMatch = decoded.match(headerRe);
  const windowStart = headerMatch?.index != null ? headerMatch.index : 0;
  const window = decoded.slice(windowStart, windowStart + 400_000);

  const rowRe =
    /((?:[A-Z][A-Za-z0-9&.'’\- ]{2,80}(?:Portfolio|Pool)\s*[IVX0-9]*\s*[–—\-]+)?\s*(?:[A-Z][A-Za-z0-9&.'’\- ]{0,40})?(?:Hotel|Inn|Resort|Suites|Marriott|Courtyard|Westin|Sheraton|Hilton|Hyatt|Embassy|Hampton|Residence|SpringHill|Fairfield|DoubleTree|Holiday Inn|Crowne Plaza|Wyndham|Radisson|Aloft|Element|AC Hotel|Four Points|Comfort Inn|Quality Inn|La Quinta|Best Western|Homewood Suites|Home2 Suites)[A-Za-z0-9&.'’\- /,]{2,90}?)\s*\$\s*[\d,]+\s+\d+(?:\.\d+)?\s*%\s+(\d{1,2}\/\d{1,2}\/\d{2,4})\s+(\d{1,2}\/\d{1,2}\/\d{2,4})/gi;

  let m;
  const seen = new Set();
  while ((m = rowRe.exec(window)) && findings.length < 200) {
    let hotelName = String(m[1] || "")
      .replace(/\s+/g, " ")
      .replace(/^.*?(?:Portfolio|Pool)\s*[IVX0-9]*\s*[–—\-]{1,2}\s*/i, "") // drop portfolio prefix
      .replace(/^[-–—\s]+/, "")
      .trim();
    if (hotelName.length < 8 || !HOTEL_NAME_RE.test(hotelName)) {
      rejected.push({ reason: "plain_schedule_non_hotel", hotelName });
      continue;
    }
    if (/^(total|aggregate|property name)/i.test(hotelName)) continue;

    const franchiseDate = parseDateToken(m[2]);
    const loanDate = parseDateToken(m[3]);
    if (!franchiseDate) continue;

    const key = `${hotelName}|${franchiseDate.iso}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const brandHit = detectBrandFamily(hotelName);
    findings.push({
      findingId: findingId([
        meta.cik,
        meta.accessionNumber,
        hotelName,
        DATE_MEANING.FRANCHISE_EXPIRATION,
        franchiseDate.iso,
      ]),
      hotelName,
      city: null,
      stateRegion: null,
      brandFamily: brandHit.family,
      brandLabel: brandHit.brandLabel,
      dateMeaning: DATE_MEANING.FRANCHISE_EXPIRATION,
      agreementType: AGREEMENT_TYPE.FRANCHISE,
      primaryDate: franchiseDate.iso,
      datePrecision: franchiseDate.precision,
      dateRaw: franchiseDate.raw,
      isHotelContractDate: true,
      isLoanMaturity: false,
      verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
      hotelAttributed: true,
      portfolioWide: false,
      snippet: m[0].replace(/\s+/g, " ").slice(0, 320),
      source: {
        cik: meta.cik || null,
        companyName: meta.companyName || null,
        accessionNumber: meta.accessionNumber || null,
        formType: meta.form || null,
        filingDate: meta.filingDate || null,
        documentUrl: meta.documentUrl || meta.sourceUrl || null,
        sourceFamily: meta.sourceFamily || SOURCE_FAMILY.CMBS_424B,
      },
      notes: ["PLAINTEXT_RELATED_LICENSE_FRANCHISE_SCHEDULE"],
      discoveryMethod: "PLAINTEXT_RELATED_LICENSE_SCHEDULE",
    });

    if (loanDate) {
      loanOnly.push({
        findingId: findingId([
          meta.cik,
          meta.accessionNumber,
          hotelName,
          DATE_MEANING.LOAN_MATURITY_DATE,
          loanDate.iso,
        ]),
        hotelName,
        dateMeaning: DATE_MEANING.LOAN_MATURITY_DATE,
        primaryDate: loanDate.iso,
        isLoanMaturity: true,
        isHotelContractDate: false,
        notes: ["LOAN_MATURITY_NOT_HOTEL_CONTRACT", "PLAINTEXT_RELATED_LICENSE_SCHEDULE"],
        discoveryMethod: "PLAINTEXT_RELATED_LICENSE_SCHEDULE",
      });
    }
  }

  return { findings, loanOnly, rejected };
}

function cellText(html) {
  return String(html || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#\d+;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * HTML tables with an explicit Franchise / Management Expiration Date column.
 * Primary high-yield path for CMBS 424B annex schedules.
 */
function parseHtmlFranchiseExpirationTables(html, meta = {}) {
  const findings = [];
  const loanOnly = [];
  const rejected = [];
  const tables = String(html || "").match(/<table[\s\S]*?<\/table>/gi) || [];

  for (const table of tables) {
    // Skip cash-management "Franchise Expiration Reserve/Trigger" tables that are not date schedules
    if (
      /franchise\s+expiration\s+(reserve|trigger)/i.test(table) &&
      !/franchise\s+expiration\s+date|franchise\s+agreement\s+expiration|management\s+agreement\s+expiration/i.test(
        table,
      )
    ) {
      continue;
    }
    if (
      !/franchise\s+expiration\s+date|franchise\s+agreement\s+expiration|management\s+agreement\s+expir|hma\s+expir|license\s+expir/i.test(
        table,
      )
    ) {
      // Allow legacy "Franchise Expiration" column only when paired with Property Name + date-like cells
      if (
        !(/franchise\s+expir/i.test(table) && /property\s+name|mortgaged\s+property/i.test(table))
      ) {
        continue;
      }
    }
    const rows = table.match(/<tr[\s\S]*?<\/tr>/gi) || [];
    let colMap = null; // { franchiseIdx, managementIdx, loanIdx, propertyIdx, cityIdx, stateIdx }

    for (const tr of rows) {
      const cells = [...tr.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((m) => cellText(m[1]));
      if (!cells.length) continue;

      // Header detection
      const headerJoin = cells.join(" | ").toLowerCase();
      if (
        /franchise\s+expir|management\s+agreement\s+expir|property name|mortgaged property/.test(headerJoin) &&
        !HOTEL_NAME_RE.test(cells.find((c) => HOTEL_NAME_RE.test(c)) || "")
      ) {
        colMap = {
          propertyIdx: cells.findIndex((c) => /property name|mortgaged property|collateral/i.test(c)),
          cityIdx: cells.findIndex((c) => /^city$/i.test(c.trim())),
          stateIdx: cells.findIndex((c) => /^(state|st)$/i.test(c.trim())),
          // Wave-2 gold header: "Expiration/Termination of Related License/ Franchise Agreement..."
          franchiseIdx: cells.findIndex(
            (c) =>
              /franchise\s+expir/i.test(c) ||
              /expiration\/termination of related license/i.test(c) ||
              /related license\/\s*franchise/i.test(c) ||
              (/expiration\/termination/i.test(c) && /franchise|license|management|operating/i.test(c)),
          ),
          managementIdx: cells.findIndex((c) => /management\s+agreement\s+expir|hma\s+expir/i.test(c)),
          licenseIdx: cells.findIndex((c) => /license\s+expir/i.test(c) && !/franchise/i.test(c)),
          loanIdx: cells.findIndex(
            (c) =>
              /loan\s+matur|mortgage\s+matur|anticipated repayment|maturity date of the related mortgage/i.test(
                c,
              ),
          ),
          terminationIdx: cells.findIndex((c) => /termination\s+right/i.test(c)),
        };
        if (colMap.propertyIdx < 0) {
          // Common CMBS layout: Loan # | Seller | Property Name | ...
          const sellerish = cells.findIndex((c) => /seller|loan\s*#/i.test(c));
          if (sellerish >= 0 && cells.length > sellerish + 1) colMap.propertyIdx = sellerish + 1;
        }
        continue;
      }
      if (!colMap) continue;

      const propIdx = colMap.propertyIdx >= 0 ? colMap.propertyIdx : 2;
      const hotelName = cells[propIdx] || "";
      if (!hotelName || hotelName.length < 6) continue;
      if (/^(total|aggregate|portfolio|property name)/i.test(hotelName)) {
        rejected.push({ reason: "portfolio_or_header_row", hotelName });
        continue;
      }
      if (!HOTEL_NAME_RE.test(hotelName)) continue; // office/retail collateral — skip
      if (/\band\b/i.test(hotelName) && HOTEL_NAME_RE.test(hotelName.split(/\band\b/i)[1] || "")) {
        rejected.push({ reason: "multi_hotel_row", hotelName });
        continue;
      }

      let city = colMap.cityIdx >= 0 ? cells[colMap.cityIdx] || null : null;
      let state = colMap.stateIdx >= 0 ? cells[colMap.stateIdx] || null : null;
      // "Joplin, MO" packed into a single cell
      if (city && /,?\s*([A-Z]{2})\s*$/.test(city) && !state) {
        const packed = city.match(/^(.*?),?\s*([A-Z]{2})\s*$/);
        if (packed) {
          city = packed[1].trim();
          state = packed[2];
        }
      }
      // Sometimes property row embeds City, ST in a later cell
      if (!state) {
        for (const c of cells) {
          const packed = String(c || "").match(/^([A-Z][a-zA-Z .'-]{2,40}),?\s*([A-Z]{2})$/);
          if (packed && new RegExp(`^(${STATE_ALT})$`).test(packed[2])) {
            if (!city) city = packed[1];
            state = packed[2];
            break;
          }
        }
      }
      // Infer city-ish place from property name when City column absent (common in CMBS annex)
      if (!city) {
        const stripped = hotelName
          .replace(/^(Courtyard|Residence Inn|SpringHill Suites|Fairfield Inn(?:\s+&?\s*Suites)?|Hampton Inn(?:\s+&?\s*Suites)?|Embassy Suites|Hilton Garden Inn|Homewood Suites|Home2 Suites|Holiday Inn(?:\s+Express)?|Crowne Plaza|Four Points|AC Hotel|Aloft|Element|Sheraton|Westin|Hyatt Place|Hyatt House|DoubleTree(?:\s+by\s+Hilton)?)\s+/i, "")
          .trim();
        if (stripped.length >= 3 && stripped.length <= 60) city = stripped.split(/\s+/).slice(0, 4).join(" ");
      }
      // Decode common HTML entities in names
      city = city ? city.replace(/&ndash;/g, "–").replace(/&amp;/g, "&") : city;

      const dateSlots = [
        { idx: colMap.franchiseIdx, meaning: DATE_MEANING.FRANCHISE_EXPIRATION },
        { idx: colMap.managementIdx, meaning: DATE_MEANING.MANAGEMENT_EXPIRATION },
        { idx: colMap.licenseIdx, meaning: DATE_MEANING.LICENSE_EXPIRATION },
        { idx: colMap.terminationIdx, meaning: DATE_MEANING.TERMINATION_RIGHT_DATE },
        { idx: colMap.loanIdx, meaning: DATE_MEANING.LOAN_MATURITY_DATE },
      ];

      for (const slot of dateSlots) {
        if (slot.idx == null || slot.idx < 0) continue;
        const rawDate = cells[slot.idx];
        if (!rawDate || /^(&nbsp;|-|—|n\/?a)$/i.test(rawDate)) continue;
        const parsed = parseDateToken(rawDate);
        if (!parsed) continue;
        const year = parsed.year;
        if (slot.meaning !== DATE_MEANING.LOAN_MATURITY_DATE && year < 1995) continue;

        const brandHit = detectBrandFamily(hotelName);
        const row = {
          findingId: findingId([
            meta.cik,
            meta.accessionNumber,
            hotelName,
            city,
            state,
            slot.meaning,
            parsed.iso,
          ]),
          hotelName: String(hotelName).replace(/&ndash;/g, "–").replace(/&amp;/g, "&"),
          city,
          stateRegion: state,
          brandFamily: brandHit.family,
          brandLabel: brandHit.brandLabel,
          dateMeaning: slot.meaning,
          agreementType: agreementTypeFromMeaning(slot.meaning),
          primaryDate: parsed.iso,
          datePrecision: parsed.precision,
          dateRaw: parsed.raw,
          isHotelContractDate: isHotelContractDateMeaning(slot.meaning),
          isLoanMaturity: slot.meaning === DATE_MEANING.LOAN_MATURITY_DATE,
          verificationClass:
            slot.meaning === DATE_MEANING.LOAN_MATURITY_DATE
              ? VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE
              : VERIFICATION_CLASS.VERIFIED_EXPIRATION,
          historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
          hotelAttributed: Boolean(hotelName),
          portfolioWide: false,
          snippet: [
            slot.meaning === DATE_MEANING.FRANCHISE_EXPIRATION
              ? "Franchise Expiration Date"
              : slot.meaning === DATE_MEANING.MANAGEMENT_EXPIRATION
                ? "Management Agreement Expiration"
                : slot.meaning === DATE_MEANING.LOAN_MATURITY_DATE
                  ? "Loan Maturity"
                  : "Schedule Date",
            cells.filter(Boolean).slice(0, 12).join(" | "),
          ]
            .join(" | ")
            .slice(0, 320),
          source: {
            cik: meta.cik || null,
            companyName: meta.companyName || null,
            accessionNumber: meta.accessionNumber || null,
            formType: meta.form || null,
            filingDate: meta.filingDate || null,
            documentUrl: meta.documentUrl || null,
            sourceFamily: meta.sourceFamily || SOURCE_FAMILY.CMBS_424B,
          },
          notes: ["HTML_TABLE_FRANCHISE_EXPIRATION_COLUMN"],
          discoveryMethod: "HTML_PROPERTY_TABLE",
        };

        if (row.isLoanMaturity) {
          row.notes.push("LOAN_MATURITY_NOT_HOTEL_CONTRACT");
          loanOnly.push(row);
        } else if (row.isHotelContractDate) {
          // Prefer city+state when available; distinctive brand+place still allowed
          findings.push(row);
        } else {
          rejected.push({ reason: "ambiguous_date_meaning", hotelName, date: parsed.raw });
        }
      }
    }
  }

  return { findings, loanOnly, rejected };
}

/**
 * Main entry: extract hotel contract timing + separately retained loan maturities.
 */
export function extractCmbsPropertySchedule(doc, meta = {}) {
  const raw = doc?.text || doc?.html || "";
  const text = stripHtml(raw);
  const header = classifyHeader(text.slice(0, 4000));

  const htmlTables = parseHtmlFranchiseExpirationTables(raw, meta);
  const delimited = parseDelimitedScheduleLines(text, meta);
  const prose = parseProseScheduleLines(text, {
    ...meta,
    sourceFamily: meta.sourceFamily || SOURCE_FAMILY.HOTEL_REIT_SCHEDULE,
  });
  const relatedLicensePlain = parseRelatedLicensePlainSchedule(text, {
    ...meta,
    sourceUrl: meta.sourceUrl || meta.documentUrl || doc?.url || null,
  });

  const byId = new Map();
  for (const f of [
    ...htmlTables.findings,
    ...delimited.findings,
    ...prose.findings,
    ...relatedLicensePlain.findings,
  ]) {
    if (!byId.has(f.findingId)) byId.set(f.findingId, f);
  }
  const loanById = new Map();
  for (const f of [
    ...htmlTables.loanOnly,
    ...delimited.loanOnly,
    ...relatedLicensePlain.loanOnly,
  ]) {
    if (!loanById.has(f.findingId)) loanById.set(f.findingId, f);
  }

  return {
    ok: true,
    version: CMBS_PROPERTY_SCHEDULE_EXTRACTOR_V1,
    headerSignals: header,
    hotelContractFindings: [...byId.values()],
    loanMaturityFindings: [...loanById.values()],
    rejected: [
      ...htmlTables.rejected,
      ...delimited.rejected,
      ...prose.rejected,
      ...relatedLicensePlain.rejected,
    ],
    counts: {
      hotelContractDates: byId.size,
      loanMaturities: loanById.size,
      rejected:
        htmlTables.rejected.length +
        delimited.rejected.length +
        prose.rejected.length +
        relatedLicensePlain.rejected.length,
      relatedLicensePlainRows: relatedLicensePlain.findings.length,
    },
    guarantees: [
      "Loan maturity findings are isolated from hotelContractFindings",
      "Ambiguous expiration columns are rejected, not promoted",
      "HTML Franchise Expiration Date columns are primary CMBS path",
      "Plaintext Related License/Franchise schedule recovers BR-split headers missed by nested table regex",
    ],
  };
}

export default {
  extractCmbsPropertySchedule,
  inferDateMeaning,
  CMBS_PROPERTY_SCHEDULE_EXTRACTOR_V1,
};
