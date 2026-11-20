/**
 * Deterministic extraction of hotel-level franchise/management expiration schedules.
 * Test case: DiamondRock-style property franchise expiration tables.
 */

import crypto from "node:crypto";
import { AGREEMENT_TYPE, VERIFICATION_CLASS, EVIDENCE_FIELD_NAMES, HISTORICAL_APPLICABILITY } from "./contract.js";
import { detectBrandFamily } from "./brand-families-v2.js";

export const FRANCHISE_SCHEDULE_EXTRACTOR_V2 = "sec-edgar-franchise-schedule-extractor-v2";

const STATE_ALT =
  "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY|DC";
const US_STATE = new RegExp(`\\b(?:${STATE_ALT})\\b`);

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<\/(td|th|tr|p|div|li|br|h\d)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function discoveryId(name, city, state, _cik) {
  const raw = [name, city, state]
    .map((s) =>
      String(s || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim(),
    )
    .join("|");
  return `disc_sec_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 20)}`;
}

function parseYearOrDate(token) {
  const t = String(token || "").trim();
  if (/^\d{4}$/.test(t)) return { raw: t, year: Number(t), kind: "YEAR" };
  const mdy = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (mdy) {
    const y = mdy[3].length === 2 ? 2000 + Number(mdy[3]) : Number(mdy[3]);
    return { raw: t, year: y, kind: "DATE" };
  }
  const named = t.match(
    /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(\d{4})$/i,
  );
  if (named) return { raw: t, year: Number(named[2]), kind: "DATE" };
  return null;
}

/**
 * Match trailing "City, ST AgreementType Date" so hotel names can contain place words.
 * Lines resembling:
 * "Courtyard Denver Downtown Denver CO Franchise 2028"
 * "Westin San Diego San Diego, CA  Management  December 31, 2030"
 * "Hilton Garden Inn Boston Downtown | Boston | MA | Franchise | 2029"
 */
const CITY =
  "((?:San|Los|Las|New|Fort|St\\.?|Saint|Des|El|Santa)\\s+[A-Z][a-zA-Z.'-]{2,30}|[A-Z][a-zA-Z.'-]{2,40})";

function matchScheduleLine(line, scheduleHint) {
  const dateAlt =
    "(\\d{4}|\\d{1,2}/\\d{1,2}/\\d{2,4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},?\\s+\\d{4})";

  // Pipe-delimited first (unambiguous)
  let m = line.match(
    new RegExp(
      `^(.+?)\\s*[|]\\s*${CITY}\\s*[|]\\s*(${STATE_ALT})\\s*[|]\\s*(Franchise|Management|Lease)\\s*[|]\\s*${dateAlt}\\b`,
      "i",
    ),
  );
  if (m) return m;

  // Greedy hotel name + final City ST Agreement Date (state must be real US code)
  m = line.match(
    new RegExp(
      `^(.+)\\s+${CITY},?\\s+(${STATE_ALT})\\s+(Franchise|Management|Lease)\\s+(?:Agreement\\s+)?(?:expires?\\s+)?${dateAlt}\\b`,
      "i",
    ),
  );
  if (m) return m;

  if (scheduleHint) {
    m = line.match(
      new RegExp(`^(.+)\\s+${CITY},?\\s+(${STATE_ALT})\\s+${dateAlt}\\b`, "i"),
    );
    if (m) return [m[0], m[1], m[2], m[3], "Franchise", m[4]];
  }
  return null;
}

function extractScheduleRows(text, meta = {}) {
  const rows = [];
  const lines = text.split(/\n/).map((l) => l.trim()).filter(Boolean);
  const scheduleHint =
    /franchise\s+expir|expiration\s+(year|date)|management\s+agreement\s+expir|property\s+schedule|hotel\s+portfolio/i.test(
      text.slice(0, 8000),
    );

  for (const line of lines) {
    if (line.length < 12 || line.length > 220) continue;
    if (!/(Franchise|Management|Lease|Agreement|Hotel|Inn|Resort|Suites|Courtyard|Hilton|Marriott|Westin|Sheraton|Embassy|Hampton|DoubleTree|Residence|SpringHill|Fairfield|Autograph|Curio|Tapestry)/i.test(line))
      continue;
    if (/\b(total|aggregate|weighted average|approximately|table of contents)\b/i.test(line)) continue;

    const m = matchScheduleLine(line, scheduleHint);
    if (!m) continue;

    const hotelName = String(m[1]).replace(/\s+/g, " ").trim();
    const city = String(m[2]).trim();
    const state = String(m[3]).toUpperCase();
    if (!US_STATE.test(state)) continue;
    if (hotelName.length < 6 || hotelName.length > 90) continue;
    if (/\b(agreement|expir|schedule|portfolio)\b/i.test(hotelName) && !/Hotel|Inn|Resort|Suites|Courtyard|Hilton|Marriott/i.test(hotelName))
      continue;

    const agreementWord = String(m[4] || "Franchise");
    const dateToken = parseYearOrDate(m[5]);
    if (!dateToken) continue;
    const brand = detectBrandFamily(hotelName);
    const agreementType =
      /franchise/i.test(agreementWord)
        ? AGREEMENT_TYPE.FRANCHISE
        : /lease/i.test(agreementWord)
          ? AGREEMENT_TYPE.LEASE
          : AGREEMENT_TYPE.MANAGEMENT;

    const id = discoveryId(hotelName, city, state, meta.cik);
    rows.push({
      discoveryId: id,
      hotelName,
      city,
      stateRegion: state,
      country: "United States",
      brandFamily: brand.family,
      historicalBrand: brand.brandLabel,
      brandKey: brand.brandKey,
      agreementType,
      initialExpiration: dateToken.raw,
      initialExpirationYear: dateToken.year,
      verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
      evidenceFieldName:
        agreementType === AGREEMENT_TYPE.FRANCHISE
          ? EVIDENCE_FIELD_NAMES.FRANCHISE_EXPIRATION
          : agreementType === AGREEMENT_TYPE.LEASE
            ? EVIDENCE_FIELD_NAMES.LEASE_EXPIRATION
            : EVIDENCE_FIELD_NAMES.MGMT_EXPIRATION,
      snippet: line.slice(0, 240),
      discoveryMethod: "FRANCHISE_EXPIRATION_SCHEDULE",
      source: {
        cik: meta.cik || null,
        companyName: meta.companyName || null,
        accessionNumber: meta.accessionNumber || null,
        formType: meta.form || null,
        filingDate: meta.filingDate || null,
        documentUrl: meta.documentUrl || null,
      },
      hotelAttributed: true,
      hotelNameHints: [hotelName],
      portfolioWide: false,
      primaryDate: dateToken.raw,
    });
  }

  return rows;
}

/**
 * Extract schedule rows + wrap as hotels + contract findings.
 */
export function extractFranchiseExpirationSchedule(doc, meta = {}) {
  const text = stripHtml(doc?.text || doc?.html || "");
  const rows = extractScheduleRows(text, meta);
  const byId = new Map();
  for (const r of rows) {
    const prev = byId.get(r.discoveryId);
    if (!prev) {
      byId.set(r.discoveryId, {
        discoveryId: r.discoveryId,
        hotelName: r.hotelName,
        city: r.city,
        stateRegion: r.stateRegion,
        country: r.country,
        brandFamily: r.brandFamily,
        historicalBrand: r.historicalBrand,
        brandKey: r.brandKey,
        registrant: meta.companyName || null,
        cik: meta.cik || null,
        sourceFiling: meta.form || null,
        sourceUrl: meta.documentUrl || null,
        discoveryMethod: r.discoveryMethod,
        contractFindings: [toFinding(r)],
        mentionCount: 1,
      });
    } else {
      prev.mentionCount += 1;
      prev.contractFindings.push(toFinding(r));
    }
  }
  return {
    ok: true,
    version: FRANCHISE_SCHEDULE_EXTRACTOR_V2,
    scheduleRowCount: rows.length,
    hotelCount: byId.size,
    hotels: [...byId.values()],
    findings: rows.map(toFinding),
    note: "Schedule expirations are HISTORICALLY_VERIFIED from the filing date — not CURRENTLY_VERIFIED unless later evidence confirms.",
  };
}

function toFinding(r) {
  return {
    verificationClass: r.verificationClass,
    historicalApplicability: r.historicalApplicability,
    agreementType: r.agreementType,
    evidenceFieldName: r.evidenceFieldName,
    primaryDate: r.primaryDate,
    initialExpiration: r.initialExpiration,
    initialExpirationYear: r.initialExpirationYear,
    snippet: r.snippet,
    source: r.source,
    hotelAttributed: true,
    hotelNameHints: r.hotelNameHints,
    attributedHotelName: r.hotelName,
    portfolioWide: false,
    brandFamily: r.brandFamily,
    historicalBrand: r.historicalBrand,
  };
}
