#!/usr/bin/env node
/**
 * Track B Historical Intelligence V2.
 * Builds verified dossiers from the Sonesta EX-10.3 schedule and named
 * Mexico/Brazil current-status sources. Does not write Airtable.
 * CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { classifyCurrentStatus } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import {
  executedInstrumentsInClause,
  timingBucket,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-intelligence-v2");
const AS_OF = "2026-11-21";
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const SONESTA_EXHIBIT =
  "https://www.sec.gov/Archives/edgar/data/945394/000094539426000061/svc-q32026xex103xsonestaho.htm";
const SONESTA_10Q =
  "https://www.sec.gov/Archives/edgar/data/945394/000094539426000061/svc-20260930.htm";
const FIBRA_Q2 = "https://www.bmv.com.mx/docs-pub/eventfid/eventfid_1574632_1596_1.pdf";
const OSCAR_URL = "https://www.letsatlantica.com.br/hotel/quality-suites-oscar-freire";
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

const EXISTING_DISCOVERY = [
  [/yorkville royal sonesta/i, "disc_tb_yorkville_royal_sonesta_toronto_2025"],
  [/toronto thornhill/i, "disc_tb_sonesta_es_toronto_thornhill_2025"],
];

function slug(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 42);
}

function decode(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, "\n")
    .replace(/&#8216;|&#8217;|&rsquo;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;|&#160;/g, " ")
    .replace(/&#91;/g, "[")
    .replace(/&#93;/g, "]");
}

function countryFromState(state) {
  const s = String(state || "").toLowerCase();
  if (s.includes("ontario")) return "CA";
  if (s.includes("puerto rico")) return "PR";
  if (s.includes("united kingdom") || s === "england" || s === "scotland" || s === "wales") return "GB";
  return "US";
}

function parseSonesta(text) {
  const lines = text
    .split(/\n/)
    .map((s) => s.replace(/\s+/g, " ").trim())
    .filter((s) => s.startsWith("Hotel Management Agreement for "));
  const hotels = [];
  const re =
    /^Hotel Management Agreement for (.+?), dated as of August 1, 2025, between (.+?), as operator, and (.+?), as owner\. \[(.+)\]$/;
  for (const line of lines) {
    const m = line.match(re);
    if (!m) continue;
    const [, hotelName, operator, owner, addressBlob] = m;
    const parts = addressBlob.split(",").map((s) => s.trim());
    const state = parts[parts.length - 1] || "";
    const city = parts[parts.length - 2] || "";
    const street = parts.slice(1, -2).join(", ");
    const country = countryFromState(state);
    const idHit = EXISTING_DISCOVERY.find(([rx]) => rx.test(hotelName));
    const key = slug(hotelName);
    hotels.push({
      wave: country === "US" ? "B10" : "B13",
      discoveryPropertyId: idHit ? idHit[1] : `disc_tb_v2_sonesta_${key}`,
      findingId: `find_tb_v2_sonesta_${key}_amend_20430630`,
      contractId: `ctr_tb_v2_sonesta_${key}_hma_20430630`,
      milestoneId: `ms_tb_v2_sonesta_${key}_exp_20430630`,
      hotelName,
      hotelNameNormalized: hotelName.toLowerCase(),
      city,
      stateProvince: state,
      address: street,
      physicalCountry: country,
      currentBrand: "Sonesta",
      currentOperator: operator,
      counterpartyName: operator,
      owner,
      agreementType: "MANAGEMENT",
      agreementRole: "RENEWAL_EXTENSION",
      contractStatus: "CURRENTLY_CORROBORATED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commencementDate: "2025-08-01",
      expirationDate: "2043-06-30",
      milestoneDate: "2043-06-30",
      milestoneType: "EXTENSION",
      commercialCategory: "MANAGEMENT",
      sourceUrl: SONESTA_EXHIBIT,
      sourcePublicationDate: "2026-11-04",
      sourceVintage: "2026",
      evidenceSnippet:
        `${hotelName} hotel management agreement dated August 1, 2025, between ${operator} as operator and ${owner} as owner. ` +
        `SEC EX-10.3 omnibus amendment dated September 25, 2026 replaces the expiration with June 30, 2043 and ratifies the remaining terms. ` +
        `The SVC 10-Q for the quarter ended September 30, 2026, in a section denominated in thousands of dollars, states the extension was in lieu of a termination fee of 11,035 on a sold hotel, ` +
        `and that the August 2025 retained agreements had been written to expire July 31, 2040 with two 10-year options at Sonesta's option. ` +
        `This row stores the amended expiration. It does not treat option exercise as having occurred and it is not a near-term commercial opportunity.`,
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        agreementExtendedOrAmended: true,
        currentStatusCorroborated: true,
      }),
      supersessionGap:
        "Same September 25, 2026 amendment previously cited from a secondary copy for the two Toronto hotels. This cycle cites the SEC exhibit. No later amendment was found.",
      hpcQuery: hotelName,
      renewalSummary:
        "Section 3.1(b) remains the extension mechanism. The 10-Q describes two 10-year Sonesta options on the August 2025 agreements. Exercise is not recorded.",
    });
  }
  return hotels;
}

function recoveryDossiers(oscarConfirmed) {
  const fibraSnippetBase = "FibraHotel Q2 2026 report, Mexico City, 15 July 2026, portfolio as of 30 June 2026.";
  const villahermosaClause =
    "El hotel de Servicio Limitado Villahermosa terminó su contrato de operación y franquicia el 1 de julio de 2026 e iniciará operaciones bajo un esquema de arrendamiento con la marca InHouse.";
  const executed = executedInstrumentsInClause(villahermosaClause);
  const rows = [];

  if (executed.includes("FRANCHISE") && executed.includes("OPERATING") && !executed.includes("LEASE")) {
    rows.push({
      wave: "B11",
      discoveryPropertyId: "disc_tb_v2_fairfield_villahermosa_2026",
      findingId: "find_tb_v2_fairfield_villahermosa_franchise_term_20260701",
      contractId: "ctr_tb_v2_fairfield_villahermosa_franchise_term_20260701",
      milestoneId: "ms_tb_v2_fairfield_villahermosa_franchise_term_20260701",
      hotelName: "Fairfield Inn & Suites Villahermosa",
      hotelNameNormalized: "fairfield inn & suites villahermosa",
      city: "Villahermosa",
      stateProvince: "Tabasco",
      physicalCountry: "MX",
      currentBrand: "Fairfield Inn",
      agreementType: "FRANCHISE",
      agreementRole: "TERMINATION_INSTRUMENT",
      contractStatus: "TERMINATED_VERIFIED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      expirationDate: null,
      milestoneDate: "2026-07-01",
      milestoneType: "TERMINATION",
      commercialCategory: "FRANCHISE",
      sourceUrl: FIBRA_Q2,
      sourcePublicationDate: "2026-07-15",
      sourceVintage: "2026",
      evidenceSnippet:
        `${fibraSnippetBase} ${villahermosaClause} The same report's limited-service table lists one Villahermosa hotel: Fairfield Inn & Suites Villahermosa, 134 rooms, operador Marriott International. ` +
        `The franchise termination is stored on its own row. The operating contract is a separate row. The InHouse lease is prospective ("iniciará") and is not stored as an executed lease.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true, superseded: true }),
      supersessionGap: "Successor lease with InHouse is announced, not stored as commenced.",
      hpcQuery: "Fairfield Inn Villahermosa",
      trackA: "NOT_QUALIFIED_SUCCESSOR_NAMED",
    });
    rows.push({
      wave: "B11",
      discoveryPropertyId: "disc_tb_v2_fairfield_villahermosa_2026",
      findingId: "find_tb_v2_fairfield_villahermosa_operating_term_20260701",
      contractId: "ctr_tb_v2_fairfield_villahermosa_operating_term_20260701",
      milestoneId: "ms_tb_v2_fairfield_villahermosa_operating_term_20260701",
      hotelName: "Fairfield Inn & Suites Villahermosa",
      hotelNameNormalized: "fairfield inn & suites villahermosa",
      city: "Villahermosa",
      stateProvince: "Tabasco",
      physicalCountry: "MX",
      currentBrand: "Fairfield Inn",
      agreementType: "OTHER",
      agreementRole: "TERMINATION_INSTRUMENT",
      contractStatus: "TERMINATED_VERIFIED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      milestoneDate: "2026-07-01",
      milestoneType: "TERMINATION",
      commercialCategory: "CONTRACT_MILESTONE",
      sourceUrl: FIBRA_Q2,
      sourcePublicationDate: "2026-07-15",
      sourceVintage: "2026",
      evidenceSnippet:
        `Instrument stored as OTHER because the sentence says "contrato de operación" and this schema has no OPERATING agreement type. It is not classified as MANAGEMENT. ${villahermosaClause}`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true, superseded: true }),
      supersessionGap: "Operating termination kept separate from the franchise termination and from the uncommenced InHouse lease.",
      hpcQuery: "Fairfield Inn Villahermosa",
      trackA: "NOT_QUALIFIED_SUCCESSOR_NAMED",
    });
  }

  rows.push(
    {
      wave: "B14",
      discoveryPropertyId: "disc_tb_hgi_tijuana_aeropuerto_2026",
      findingId: "find_tb_v2_hgi_tijuana_posadas_20260630",
      contractId: "ctr_tb_v2_hgi_tijuana_posadas_managed",
      hotelName: "Hilton Garden Inn Tijuana Aeropuerto",
      hotelNameNormalized: "hilton garden inn tijuana aeropuerto",
      city: "Tijuana",
      stateProvince: "BCN",
      physicalCountry: "MX",
      historicalBrand: "Gamma",
      currentBrand: "Hilton Garden Inn",
      currentOperator: "Grupo Posadas",
      counterpartyName: "Grupo Posadas",
      agreementType: "MANAGEMENT",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commercialCategory: "MANAGEMENT",
      sourceUrl: FIBRA_Q2,
      sourcePublicationDate: "2026-07-15",
      sourceVintage: "2026",
      evidenceSnippet:
        `${fibraSnippetBase} Hilton Garden Inn Tijuana Aeropuerto, 140 rooms, is listed under Hoteles Servicio Selecto - Administrados with operador Grupo Posadas. ` +
        `The same report says the hotel was incorporated on 5 March 2026. That date is a reopening, not a proven contract execution date, so it is not stored as Commencement Date. No expiration is stated. ` +
        `The December 31, 2025 portfolio had listed the operator as Pendiente while the hotel was closed.`,
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        currentStatusCorroborated: true,
      }),
      supersessionGap: "Prior Gamma brand finding is unchanged. This row adds the named managed-operator relationship as of 30 June 2026.",
      hpcQuery: "Hilton Garden Inn Tijuana",
      trackA: "NOT_QUALIFIED_OPERATOR_NAMED",
    },
    {
      wave: "B14",
      discoveryPropertyId: "disc_tb_v2_real_inn_mexicali_holiday_inn_2026",
      findingId: "find_tb_v2_real_inn_mexicali_aimbridge_20260531",
      hotelName: "Real Inn Mexicali / Holiday Inn Mexicali",
      hotelNameNormalized: "holiday inn mexicali",
      city: "Mexicali",
      stateProvince: "BCN",
      physicalCountry: "MX",
      historicalBrand: "Real Inn",
      currentBrand: "Holiday Inn",
      currentOperator: "Aimbridge International",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commercialCategory: "BRAND_CONVERSION",
      sourceUrl: FIBRA_Q2,
      sourcePublicationDate: "2026-07-15",
      sourceVintage: "2026",
      evidenceSnippet:
        `${fibraSnippetBase} Real Inn Mexicali suspended operations on 31 May 2026 for a brand and operator transition. ` +
        `It remains temporarily closed. Reopening is scheduled for the third quarter of 2026 as Holiday Inn operated by Aimbridge International. ` +
        `The quarter is not stored as a day. The portfolio table lists Holiday Inn Mexicali, 162 rooms, Aimbridge International, with a closed marker. ` +
        `Fiesta Inn Mexicali remains a separate Posadas hotel in the same table and is not this property. No signed management agreement date is stated.`,
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        brandChanged: true,
        operatorChanged: true,
      }),
      supersessionGap: "Temporary closure. Planned operator is named. Not an open third-party opportunity and not a permanent closure.",
      hpcQuery: "Holiday Inn Mexicali",
      trackA: "NOT_QUALIFIED_OPERATOR_NAMED",
    },
    {
      wave: "B14",
      discoveryPropertyId: "disc_tb_yucatan_playa_del_carmen_tapestry_2025",
      findingId: "find_tb_v2_yucatan_pdc_suspended_20260430",
      hotelName: "The Yucatán Resort Playa del Carmen",
      hotelNameNormalized: "the yucatan resort playa del carmen",
      city: "Playa del Carmen",
      stateProvince: "Quintana Roo",
      physicalCountry: "MX",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commercialCategory: "CONTRACT_MILESTONE",
      sourceUrl: FIBRA_Q2,
      sourcePublicationDate: "2026-07-15",
      sourceVintage: "2026",
      evidenceSnippet:
        `${fibraSnippetBase} Section title "Cierre temporal del hotel The Yucatán Resort Playa del Carmen": the hotel suspended operations on 30 April 2026. ` +
        `A footnote says it concluded operations on the same day. The heading controls the classification: temporary suspension, not a recorded permanent closure. ` +
        `This does not identify a successor manager. It is not Hyatt Vivid Playa del Carmen, which Hyatt's 17 June 2025 release describes as the conversion of the owned Hilton Playa del Carmen. ` +
        `A December 2024 trade article's February 5, 2025 effective date is not stored as an exact milestone over the Playa 10-K's February 2025 month precision.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
      supersessionGap: "February 2025 management termination finding is unchanged. Successor brand and operator remain unknown. Operations were suspended on 2026-04-30.",
      hpcQuery: "Yucatan Playa del Carmen",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
    },
    {
      wave: "B14",
      discoveryPropertyId: "disc_tb_hyatt_ziva_riviera_cancun_2023",
      findingId: "find_tb_v2_hyatt_ziva_rc_no_successor_hma",
      hotelName: "Hyatt Ziva Riviera Cancún",
      hotelNameNormalized: "hyatt ziva riviera cancun",
      city: "Cancún",
      stateProvince: "Quintana Roo",
      physicalCountry: "MX",
      currentBrand: "Hyatt Ziva",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "MEDIUM",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commercialCategory: "MANAGEMENT",
      sourceUrl: "https://www.hotelinvestmenttoday.com/Deals/Mergers-and-Acquistions/Hyatt-sells-15-Playa-assets-for-2-billion",
      sourcePublicationDate: "2025-06-30",
      sourceVintage: "2025",
      evidenceSnippet:
        "Hotel Investment Today, 30 June 2025, reports Tortuga's existing platform lists Hyatt Ziva Riviera Cancún under the Hyatt flag. " +
        "That is a brand observation. It is not a named management agreement and it does not place this hotel in the 13-of-15 fifty-year Hyatt-Tortuga management agreements, which cover formerly Playa-owned resorts. " +
        "Playa's termination of the third-party management agreement effective 15 December 2023 remains the management fact. Successor manager is not stated in a primary instrument.",
      reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
      supersessionGap: "Do not attach the June 2025 fifty-year management agreements to this hotel.",
      hpcQuery: "Hyatt Ziva Riviera Cancun",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
    },
    {
      wave: "B14",
      discoveryPropertyId: "disc_tb_hyatt_zilara_riviera_maya_2023",
      findingId: "find_tb_v2_hyatt_zilara_rm_no_successor_hma",
      hotelName: "Hyatt Zilara Riviera Maya",
      hotelNameNormalized: "hyatt zilara riviera maya",
      city: "Riviera Maya",
      stateProvince: "Quintana Roo",
      physicalCountry: "MX",
      currentBrand: "Hyatt Zilara",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "MEDIUM",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commercialCategory: "MANAGEMENT",
      sourceUrl: "https://www.hotelinvestmenttoday.com/Deals/Mergers-and-Acquistions/Hyatt-sells-15-Playa-assets-for-2-billion",
      sourcePublicationDate: "2025-06-30",
      sourceVintage: "2025",
      evidenceSnippet:
        "Hotel Investment Today, 30 June 2025, reports Tortuga's existing platform lists Hyatt Zilara Riviera Maya under the Hyatt flag. " +
        "Brand continuity is not a new management agreement and is not membership in the 13-property fifty-year set. Successor manager after the 15 December 2023 termination is not stated.",
      reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
      supersessionGap: "Do not attach the June 2025 fifty-year management agreements to this hotel.",
      hpcQuery: "Hyatt Zilara Riviera Maya",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
    },
  );

  if (oscarConfirmed) {
    rows.push({
      wave: "B12",
      discoveryPropertyId: "disc_tb_quality_suites_oscar_freire_2021",
      findingId: "find_tb_v2_oscar_freire_current_quality_20261121",
      hotelName: "Quality Suites Oscar Freire",
      hotelNameNormalized: "quality suites oscar freire",
      city: "São Paulo",
      stateProvince: "SP",
      address: "Rua Oscar Freire, 1948",
      physicalCountry: "BR",
      historicalBrand: "Comfort Suites",
      currentBrand: "Quality Suites",
      currentOperator: "Atlantica",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "MEDIUM",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V2",
      commercialCategory: "BRAND_AFFILIATION",
      sourceUrl: OSCAR_URL,
      sourceVintage: "2026",
      evidenceSnippet:
        "Retrieved 21 November 2026. Atlantica's hotel page still presents Quality Suites Oscar Freire at Rua Oscar Freire 1948 and identifies Atlantica as the operator. " +
        "That agrees with the 2021 Comfort Suites to Quality Suites conversion. No contradiction and no expiration were found. The 2021 conversion finding is not overwritten.",
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        currentStatusCorroborated: true,
      }),
      supersessionGap: "Current brand and operator confirmed. Page retrieval date is not a contract publication date.",
      hpcQuery: "Quality Suites Oscar Freire",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
    });
  }

  return rows;
}

async function hpcMeta() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`HPC_META_${res.status}`);
  const table = (json.tables || []).find((t) => t.id === HPC_TABLE_ID);
  const names = new Set((table?.fields || []).map((f) => f.name));
  const nameField = ["Property Name", "Canonical Property Name"].find((n) => names.has(n));
  const countryField = ["Country"].find((n) => names.has(n));
  const cityField = ["City"].find((n) => names.has(n));
  if (!nameField) throw new Error("HPC_NAME_FIELD_MISSING");
  return { nameField, countryField, cityField };
}

function countryMatches(recordCountry, expected) {
  const c = String(recordCountry || "").toLowerCase();
  const map = {
    MX: ["mexico", "méxico", "mx"],
    BR: ["brazil", "brasil", "br"],
    US: ["united states", "usa", "us"],
    CA: ["canada", "ca"],
    PR: ["puerto rico", "pr"],
    GB: ["united kingdom", "uk", "england"],
  };
  return (map[expected] || [String(expected || "").toLowerCase()]).some((x) => c.includes(x));
}

async function searchHpc(fields, query, country) {
  const q = String(query || "").replace(/"/g, "").slice(0, 40);
  if (!q) return { hpcMatchStatus: "NOT_SEARCHED", hpcRecordId: null };
  const formula = `FIND("${q.toLowerCase()}", LOWER({${fields.nameField}}&""))`;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "8", pageSize: "8" });
  for (const f of [fields.countryField, fields.cityField, fields.nameField]) if (f) qs.append("fields[]", f);
  let res;
  try {
    res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
  } catch {
    return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
  }
  const json = await res.json();
  if (!res.ok) return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
  const hits = (json.records || []).filter((r) =>
    fields.countryField ? countryMatches(r.fields?.[fields.countryField], country) : true,
  );
  if (hits.length === 1) {
    return {
      hpcMatchStatus: "MATCHED_UNIQUE_NAME_AND_COUNTRY",
      hpcRecordId: hits[0].id,
      hpcName: hits[0].fields?.[fields.nameField] || null,
    };
  }
  if (hits.length > 1) return { hpcMatchStatus: "AMBIGUOUS_MULTIPLE", hpcRecordId: null, hpcHitCount: hits.length };
  return { hpcMatchStatus: "DISCOVERED_NOT_IN_HPC", hpcRecordId: null };
}

function matrix(dossiers) {
  const countries = {};
  const hotels = new Set();
  for (const d of dossiers) {
    hotels.add(d.discoveryPropertyId);
    const c = d.physicalCountry || "UNKNOWN";
    if (!countries[c]) countries[c] = { hotels: new Set(), events: 0, byAgreementType: {}, byTiming: {}, byStatus: {} };
    const row = countries[c];
    row.hotels.add(d.discoveryPropertyId);
    row.events += 1;
    const t = d.agreementType || "UNRESOLVED";
    row.byAgreementType[t] = (row.byAgreementType[t] || 0) + 1;
    const bucket = timingBucket(d.milestoneDate || d.expirationDate, AS_OF).bucket;
    row.byTiming[bucket] = (row.byTiming[bucket] || 0) + 1;
    const st = d.reconciliation?.currentStatusClass || "RESEARCH_REQUIRED";
    row.byStatus[st] = (row.byStatus[st] || 0) + 1;
  }
  return {
    distinctHotels: hotels.size,
    contractualEvents: dossiers.length,
    byCountry: Object.fromEntries(
      Object.entries(countries).map(([k, v]) => [
        k,
        { ...v, hotels: v.hotels.size },
      ]),
    ),
  };
}

async function fetchText(url) {
  let lastErr = null;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const res = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html" } });
      const text = await res.text();
      return { ok: res.ok, status: res.status, text };
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 1000 * attempt));
    }
  }
  throw lastErr;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const exhibitRes = await fetchText(SONESTA_EXHIBIT);
  if (!exhibitRes.ok) throw new Error(`SONESTA_EXHIBIT_${exhibitRes.status}`);
  const sonesta = parseSonesta(decode(exhibitRes.text));
  if (sonesta.length < 40) throw new Error(`SONESTA_PARSE_TOO_SMALL_${sonesta.length}`);

  let oscarConfirmed = false;
  try {
    const oscarRes = await fetchText(OSCAR_URL);
    const oscarText = decode(oscarRes.text);
    oscarConfirmed =
      oscarRes.ok && /quality suites/i.test(oscarText) && /oscar freire/i.test(oscarText) && /atl[aâ]ntica/i.test(oscarText);
  } catch {
    oscarConfirmed = false;
  }

  const dossiers = [...sonesta, ...recoveryDossiers(oscarConfirmed)];
  const fields = token ? await hpcMeta() : null;
  const seenQuery = new Map();
  for (const d of dossiers) {
    const key = `${d.hpcQuery}|${d.physicalCountry}`;
    if (!fields) {
      d.hpcMatchStatus = "NOT_SEARCHED";
      continue;
    }
    if (seenQuery.has(key)) {
      Object.assign(d, seenQuery.get(key));
      continue;
    }
    const hit = await searchHpc(fields, d.hpcQuery, d.physicalCountry);
    seenQuery.set(key, hit);
    Object.assign(d, hit);
    await new Promise((r) => setTimeout(r, 120));
  }

  const handoff = dossiers
    .filter((d) => d.trackA)
    .map((d) => ({
      discoveryPropertyId: d.discoveryPropertyId,
      hotelName: d.hotelName,
      physicalCountry: d.physicalCountry,
      classification: d.trackA,
      reason: "Named successor or no prospective third-party decision. Not handed to Track A as a qualified opportunity.",
    }));

  const summary = {
    asOf: AS_OF,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    sonestaExhibit: SONESTA_EXHIBIT,
    sonesta10q: SONESTA_10Q,
    sonestaHotelsParsed: sonesta.length,
    oscarFreirePageConfirmed: oscarConfirmed,
    sec424b2Continuation: {
      query: "related_operating_mgmt_424b2",
      startPage: 2,
      documents: 10,
      netNew: 0,
      exhaustedSignal: true,
      note: "Did not rerun the exhausted 424H query.",
    },
    matrix: matrix(dossiers),
    qualifiedTrackAHandoffs: 0,
  };

  fs.writeFileSync(path.join(OUT, "WAVE_DOSSIERS.json"), JSON.stringify({ dossiers, summary }, null, 2));
  fs.writeFileSync(path.join(OUT, "MATRIX_COUNTRY_TIMING_TYPE.json"), JSON.stringify(summary.matrix, null, 2));
  fs.writeFileSync(path.join(OUT, "TRACK_A_HANDOFF.json"), JSON.stringify({ qualified: [], notQualified: handoff }, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
