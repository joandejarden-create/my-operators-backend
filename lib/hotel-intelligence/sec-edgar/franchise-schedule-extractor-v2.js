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
 * HTML table rows: cells often encode Hotel | City | State | Manager | Year
 * (DiamondRock-style portfolio / property tables).
 */
function extractFromHtmlTables(html, meta = {}) {
  const rows = [];
  const trs = String(html || "").match(/<tr[\s\S]*?<\/tr>/gi) || [];
  for (const tr of trs) {
    const cells = [...tr.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)]
      .map((m) =>
        String(m[1] || "")
          .replace(/<[^>]+>/g, " ")
          .replace(/&nbsp;/gi, " ")
          .replace(/\s+/g, " ")
          .trim(),
      )
      .filter(Boolean);
    if (cells.length < 3) continue;

    // Find a cell that looks like a hotel name
    let hotelIdx = -1;
    for (let i = 0; i < cells.length; i++) {
      const c = cells[i];
      if (c.length < 8 || c.length > 90) continue;
      if (
        /\b(Hotel|Inn|Resort|Suites|Marriott|Courtyard|Westin|Sheraton|Hilton|Hyatt|Embassy|Hampton|Residence)\b/i.test(
          c,
        ) &&
        !/\b(total|aggregate|manager|franchise|expiration|property name)\b/i.test(c)
      ) {
        hotelIdx = i;
        break;
      }
    }
    if (hotelIdx < 0) continue;

    const hotelName = cells[hotelIdx];
    // City / state often follow the hotel cell
    let city = null;
    let state = null;
    for (let i = hotelIdx + 1; i < Math.min(cells.length, hotelIdx + 4); i++) {
      const c = cells[i];
      if (!city && /^[A-Z][a-zA-Z .'-]{2,40}$/.test(c) && !STATE_ALT.split("|").includes(c.toUpperCase())) {
        city = c;
        continue;
      }
      if (!state) {
        if (new RegExp(`^(${STATE_ALT})$`, "i").test(c)) {
          state = c.toUpperCase();
          break;
        }
        // Full state name
        const full = {
          alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR", california: "CA",
          colorado: "CO", connecticut: "CT", delaware: "DE", florida: "FL", georgia: "GA",
          hawaii: "HI", idaho: "ID", illinois: "IL", indiana: "IN", iowa: "IA", kansas: "KS",
          kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD", massachusetts: "MA",
          michigan: "MI", minnesota: "MN", mississippi: "MS", missouri: "MO", montana: "MT",
          nebraska: "NE", nevada: "NV", "new hampshire": "NH", "new jersey": "NJ",
          "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND",
          ohio: "OH", oklahoma: "OK", oregon: "OR", pennsylvania: "PA", "rhode island": "RI",
          "south carolina": "SC", "south dakota": "SD", tennessee: "TN", texas: "TX",
          utah: "UT", vermont: "VT", virginia: "VA", washington: "WA", "west virginia": "WV",
          wisconsin: "WI", wyoming: "WY", "district of columbia": "DC",
        };
        const st = full[c.toLowerCase()];
        if (st) {
          state = st;
          break;
        }
      }
    }

    // Optional expiration year in row
    let year = null;
    for (const c of cells) {
      if (/^(20\d{2}|19\d{2})$/.test(c)) {
        year = c;
        break;
      }
    }

    // Manager / operator often last cell (DiamondRock: Hotel|City|State|…|Rooms|Manager)
    let disclosedManager = null;
    const last = cells[cells.length - 1];
    if (
      last &&
      last !== hotelName &&
      last !== city &&
      !/^(upper|upscale|luxury|full service|select service|rooms?|\d)/i.test(last) &&
      /^[A-Z][A-Za-z0-9&.'\- ]{2,60}$/.test(last) &&
      last.length <= 60
    ) {
      disclosedManager = last.replace(/&#\d+;/g, " ").replace(/\s+/g, " ").trim();
    }

    if (!city || !state) continue;
    const brand = detectBrandFamily(hotelName);
    const id = discoveryId(hotelName, city, state, meta.cik);
    const agreementType = year ? AGREEMENT_TYPE.FRANCHISE : AGREEMENT_TYPE.UNKNOWN;
    rows.push({
      discoveryId: id,
      hotelName,
      city,
      stateRegion: state,
      country: "United States",
      brandFamily: brand.family,
      historicalBrand: brand.brandLabel,
      brandKey: brand.brandKey,
      disclosedManager,
      agreementType,
      initialExpiration: year,
      initialExpirationYear: year ? Number(year) : null,
      verificationClass: year ? VERIFICATION_CLASS.VERIFIED_EXPIRATION : VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE,
      historicalApplicability: year
        ? HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED
        : HISTORICAL_APPLICABILITY.REQUIRES_FURTHER_RESEARCH,
      evidenceFieldName: year ? EVIDENCE_FIELD_NAMES.FRANCHISE_EXPIRATION : "Property Schedule Disclosure",
      snippet: cells.join(" | ").slice(0, 240),
      discoveryMethod: "HTML_PROPERTY_TABLE",
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
      primaryDate: year,
      // Portfolio schedule discloses manager — NOT legal ownership
      ownershipInferredFromPortfolio: false,
    });
  }
  return rows;
}

/**
 * Extract schedule rows + wrap as hotels + contract findings.
 */
export function extractFranchiseExpirationSchedule(doc, meta = {}) {
  const raw = doc?.text || doc?.html || "";
  const text = stripHtml(raw);
  const fromText = extractScheduleRows(text, meta);
  const fromHtml = extractFromHtmlTables(raw, meta);
  const rows = [...fromText, ...fromHtml];
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
  const managerRelationships = [];
  for (const r of rows) {
    if (!r.disclosedManager) continue;
    managerRelationships.push({
      role: "MANAGEMENT_COMPANY",
      entityName: r.disclosedManager,
      relationshipStatus: "HISTORICALLY_VERIFIED",
      historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
      hotelNameHints: [r.hotelName],
      attributedHotelName: r.hotelName,
      city: r.city,
      stateRegion: r.stateRegion,
      explicitEvidence: true,
      inferredFromPortfolioListing: false,
      evidenceBasis: "Property schedule Manager column — brand/manager disclosure, not fee-simple ownership",
      snippet: r.snippet,
      source: r.source,
      discoveryMethod: "HTML_PROPERTY_TABLE_MANAGER",
    });
  }

  return {
    ok: true,
    version: FRANCHISE_SCHEDULE_EXTRACTOR_V2,
    scheduleRowCount: rows.length,
    hotelCount: byId.size,
    hotels: [...byId.values()],
    findings: rows.map(toFinding),
    managerRelationships,
    note: "Schedule expirations are HISTORICALLY_VERIFIED from the filing date — not CURRENTLY_VERIFIED unless later evidence confirms. Manager column ≠ legal owner.",
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
