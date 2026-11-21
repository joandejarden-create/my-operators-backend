#!/usr/bin/env node
/**
 * Track B Historical Intelligence V3 — high-yield contractual source expansion.
 * Builds verified dossiers. Does not write Airtable.
 * CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { classifyCurrentStatus } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import { timingBucket } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-intelligence-v3");
const AS_OF = "2026-11-21";
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const SONESTA_EXHIBIT =
  "https://www.sec.gov/Archives/edgar/data/945394/000094539426000061/svc-q32026xex103xsonestaho.htm";
const SOTHERLY_EXHIBIT =
  "https://www.sec.gov/Archives/edgar/data/1313536/000095017024123250/soho-ex10_22.htm";
const HR_FIBRA = "https://www.hrratings.com/pdf/Reporte_FIHO_NRSRO__2026.pdf";
const CHOICE_OSCAR = "https://www.choicehotels.com/brazil/sao-paulo/quality-inn-hotels/br009";
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

const SOTHERLY_HOTELS = [
  { hotelName: "DoubleTree by Hilton Philadelphia Airport", city: "Philadelphia", stateProvince: "Pennsylvania" },
  { hotelName: "Hotel Ballast Wilmington Riverside", city: "Wilmington", stateProvince: "North Carolina" },
  { hotelName: "The DeSoto", city: "Savannah", stateProvince: "Georgia" },
  { hotelName: "DoubleTree by Hilton Jacksonville", city: "Jacksonville", stateProvince: "Florida" },
  { hotelName: "DoubleTree by Hilton Laurel West", city: "Laurel", stateProvince: "Maryland" },
  { hotelName: "Georgian Terrace Hotel", city: "Atlanta", stateProvince: "Georgia" },
  { hotelName: "Hotel Alba Tampa", city: "Tampa", stateProvince: "Florida" },
  { hotelName: "The Whitehall", city: "Houston", stateProvince: "Texas" },
  { hotelName: "Hyatt Centric Arlington", city: "Arlington", stateProvince: "Virginia" },
  { hotelName: "DoubleTree Resort by Hilton Hollywood Beach", city: "Hollywood", stateProvince: "Florida" },
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

async function fetchText(url) {
  let lastErr = null;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const res = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html,*/*" } });
      return { ok: res.ok, status: res.status, text: await res.text() };
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 800 * attempt));
    }
  }
  throw lastErr;
}

function recoverAllegro(text) {
  const collapsed = text.replace(/\s+/g, " ");
  const m = collapsed.match(
    /Hotel Management Agreement for The Allegro Royal Sonesta Hotel, dated as of August 1, 2025, between (.+?), as operator, and (.+?), as owner\.\s*\[The Allegro Royal Sonesta Hotel,\s*(.+?)\]/i,
  );
  if (!m) return null;
  return {
    wave: "B18",
    discoveryPropertyId: "disc_tb_v3_allegro_royal_sonesta_chicago",
    findingId: "find_tb_v3_allegro_royal_sonesta_amend_20430630",
    contractId: "ctr_tb_v3_allegro_royal_sonesta_hma_20430630",
    milestoneId: "ms_tb_v3_allegro_royal_sonesta_exp_20430630",
    hotelName: "The Allegro Royal Sonesta Hotel",
    hotelNameNormalized: "the allegro royal sonesta hotel",
    city: "Chicago",
    stateProvince: "Illinois",
    address: "171 West Randolph Street",
    physicalCountry: "US",
    currentBrand: "Sonesta",
    currentOperator: m[1].trim(),
    counterpartyName: m[1].trim(),
    owner: m[2].trim(),
    agreementType: "MANAGEMENT",
    agreementRole: "RENEWAL_EXTENSION",
    contractStatus: "CURRENTLY_CORROBORATED",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    persistEligible: true,
    contractEligible: true,
    discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
    commencementDate: "2025-08-01",
    expirationDate: "2043-06-30",
    milestoneDate: "2043-06-30",
    milestoneType: "EXTENSION",
    commercialCategory: "MANAGEMENT",
    sourceUrl: SONESTA_EXHIBIT,
    sourcePublicationDate: "2026-11-04",
    sourceVintage: "2026",
    evidenceSnippet:
      `Recovered from the same SVC EX-10.3 schedule that produced the V2 Sonesta set. The HTML split "SVC" from "Randolph Street TRS LLC" across page breaks. ` +
      `Operator ${m[1].trim()}; owner ${m[2].trim()}; address ${m[3].trim()}. Amended expiration June 30, 2043. Not a near-term commercial opportunity.`,
    reconciliation: classifyCurrentStatus({ historicallyVerified: true, agreementExtendedOrAmended: true }),
    supersessionGap: "Same September 25, 2026 omnibus amendment as the other Sonesta retained hotels.",
    hpcQuery: "Allegro Royal Sonesta",
    trackA: "NOT_QUALIFIED_DISTANT_EXPIRATION",
  };
}

function sotherlyDossiers() {
  return SOTHERLY_HOTELS.map((h) => {
    const key = slug(h.hotelName);
    return {
      wave: "B15",
      discoveryPropertyId: `disc_tb_v3_sotherly_${key}`,
      findingId: `find_tb_v3_sotherly_${key}_hma_20350331`,
      contractId: `ctr_tb_v3_sotherly_${key}_hma_20350331`,
      milestoneId: `ms_tb_v3_sotherly_${key}_exp_20350331`,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      city: h.city,
      stateProvince: h.stateProvince,
      physicalCountry: "US",
      currentOperator: "Our Town Hospitality LLC",
      counterpartyName: "Our Town Hospitality LLC",
      owner: "Sotherly Hotels / MHI Hospitality TRS, LLC",
      agreementType: "MANAGEMENT",
      agreementRole: "RENEWAL_EXTENSION",
      contractStatus: "CURRENTLY_CORROBORATED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commencementDate: "2024-11-06",
      expirationDate: "2035-03-31",
      milestoneDate: "2035-03-31",
      milestoneType: "EXTENSION",
      commercialCategory: "MANAGEMENT",
      sourceUrl: SOTHERLY_EXHIBIT,
      sourcePublicationDate: "2024-11-07",
      sourceVintage: "2024",
      evidenceSnippet:
        `Sotherly Hotels EX-10.22 Amended and Restated Master Agreement dated November 6, 2024 with Our Town Hospitality LLC. ` +
        `Exhibit A lists ${h.hotelName} among Current Hotels. Each Current Hotel management agreement has maturity March 31, 2035. ` +
        `Two five-year renewals require mutual agreement. Manager must give not less than ninety days' notice before term end to request renewal; Lessee has thirty days to respond. ` +
        `Base management fee for Current Hotels is 2.5% of gross revenues per Exhibit C. This stores the hotel-level management term. It is not a near-term commercial opportunity.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true, agreementExtendedOrAmended: true }),
      supersessionGap: "Restates the 2019 master and subsequent amendments. Individual hotel HMAs share the March 31, 2035 maturity.",
      hpcQuery: h.hotelName,
      renewalSummary: "Two optional five-year renewals by mutual agreement. Manager notice not less than 90 days before expiration.",
      trackA: "NOT_QUALIFIED_DISTANT_EXPIRATION",
    };
  });
}

function mexicoBrazilRecovery() {
  return [
    {
      wave: "B18",
      discoveryPropertyId: "disc_tb_v2_fairfield_villahermosa_2026",
      findingId: "find_tb_v3_fairfield_villahermosa_lease_start_20260701",
      contractId: "ctr_tb_v3_fairfield_villahermosa_inhouse_lease",
      milestoneId: "ms_tb_v3_fairfield_villahermosa_lease_start_20260701",
      hotelName: "Fairfield Inn & Suites Villahermosa / InHouse Villahermosa",
      hotelNameNormalized: "inhouse villahermosa",
      city: "Villahermosa",
      stateProvince: "Tabasco",
      address: "Av. Carlos Pellicer Camara 5202",
      physicalCountry: "MX",
      historicalBrand: "Fairfield Inn",
      currentBrand: "InHouse",
      currentOperator: "InHouse Hotels",
      counterpartyName: "InHouse Hotels",
      agreementType: "LEASE",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commencementDate: "2026-07-01",
      milestoneDate: "2026-07-01",
      milestoneType: "COMMENCEMENT",
      commercialCategory: "CONTRACT_MILESTONE",
      sourceUrl: HR_FIBRA,
      sourcePublicationDate: "2026-08-27",
      sourceVintage: "2026",
      evidenceSnippet:
        `HR Ratings Fibra Hotel report dated 27 August 2026: the limited-service Villahermosa hotel that operated under Marriott International terminated its operation and franchise contracts. ` +
        `As of 1 July 2026 the hotel began operations under a fixed-rent lease scheme with the InHouse brand and has 134 rooms. ` +
        `This supersedes the V2 prospective-lease treatment. Prior franchise and operating termination rows remain. InHouse Hotels' public page lists InHouse Hotel Villahermosa at Av. Carlos Pellicer Camara 5202.`,
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        brandChanged: true,
        operatorChanged: true,
        currentStatusCorroborated: true,
      }),
      supersessionGap: "Lease commencement confirmed. Franchise and operating terminations already stored separately.",
      hpcQuery: "InHouse Villahermosa",
      trackA: "NOT_QUALIFIED_SUCCESSOR_NAMED",
    },
    {
      wave: "B16",
      discoveryPropertyId: "disc_tb_v3_inhouse_ciudad_del_carmen_2026",
      findingId: "find_tb_v3_cdc_inhouse_lease_20260401",
      contractId: "ctr_tb_v3_cdc_inhouse_lease_20260401",
      milestoneId: "ms_tb_v3_cdc_inhouse_lease_20260401",
      hotelName: "InHouse Ciudad del Carmen (formerly Fiesta Inn Ciudad del Carmen)",
      hotelNameNormalized: "inhouse ciudad del carmen",
      city: "Ciudad del Carmen",
      stateProvince: "Campeche",
      physicalCountry: "MX",
      historicalBrand: "Fiesta Inn",
      currentBrand: "InHouse",
      currentOperator: "InHouse Hotels",
      counterpartyName: "InHouse Hotels",
      agreementType: "LEASE",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      contractEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commencementDate: "2026-04-01",
      milestoneDate: "2026-04-01",
      milestoneType: "COMMENCEMENT",
      commercialCategory: "CONTRACT_MILESTONE",
      sourceUrl: HR_FIBRA,
      sourcePublicationDate: "2026-08-27",
      sourceVintage: "2026",
      evidenceSnippet:
        `HR Ratings Fibra Hotel report dated 27 August 2026: on 4 March 2026 FibraHotel announced that the select-service hotel in Ciudad del Carmen, which operated under the Fiesta Inn brand, would move to a fixed-rent lease. ` +
        `The hotel began operations under the InHouse brand on 1 April 2026. FibraHotel's Q2 2026 portfolio table lists InHouse Ciudad del Carmen among leased hotels.`,
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        brandChanged: true,
        operatorChanged: true,
        currentStatusCorroborated: true,
      }),
      supersessionGap: "Fiesta Inn managed relationship replaced by InHouse lease.",
      hpcQuery: "InHouse Ciudad del Carmen",
      trackA: "NOT_QUALIFIED_SUCCESSOR_NAMED",
    },
    {
      wave: "B18",
      discoveryPropertyId: "disc_tb_v2_real_inn_mexicali_holiday_inn_2026",
      findingId: "find_tb_v3_mexicali_holiday_inn_aimbridge_status",
      hotelName: "Holiday Inn Mexicali (formerly Real Inn Mexicali)",
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
      evidenceConfidence: "MEDIUM",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commercialCategory: "BRAND_CONVERSION",
      sourceUrl: HR_FIBRA,
      sourcePublicationDate: "2026-08-27",
      sourceVintage: "2026",
      evidenceSnippet:
        `HR Ratings Fibra Hotel report dated 27 August 2026 restates that Real Inn Mexicali closed on 31 May 2026 and was scheduled to reopen in the third quarter of 2026 as Holiday Inn operated by Aimbridge Hospitality (162 rooms). ` +
        `A secondary summary of FibraHotel's 21 October 2026 Q3 results states the hotel reopened on 6 August 2026. That exact day is not stored from the secondary summary alone. No signed management-agreement commencement date is in the rating report.`,
      reconciliation: classifyCurrentStatus({
        historicallyVerified: true,
        brandChanged: true,
        operatorChanged: true,
      }),
      supersessionGap: "Planned operator Aimbridge remains the corroborated commercial relationship. Exact reopen day awaits a primary issuer sentence.",
      hpcQuery: "Holiday Inn Mexicali",
      trackA: "NOT_QUALIFIED_OPERATOR_NAMED",
    },
    {
      wave: "B18",
      discoveryPropertyId: "disc_tb_yucatan_playa_del_carmen_tapestry_2025",
      findingId: "find_tb_v3_yucatan_lease_portfolio_intent_20260430",
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
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commercialCategory: "CONTRACT_MILESTONE",
      sourceUrl: HR_FIBRA,
      sourcePublicationDate: "2026-08-27",
      sourceVintage: "2026",
      evidenceSnippet:
        `HR Ratings Fibra Hotel report dated 27 August 2026: on 30 April 2026 FibraHotel announced that The Yucatán Resort Playa del Carmen suspended operations with the objective of including it in its lease portfolio. ` +
        `Successor lessee or manager is not named. This is not Hyatt Vivid Playa del Carmen. A secondary travel site claims a renovation reopening around April 2028; that day is not stored from the secondary source. ` +
        `Track A research candidate: owner-stated intent to convert to lease without a named counterparty.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
      supersessionGap: "Temporary suspension plus lease-portfolio intent. No executed successor lease stored.",
      hpcQuery: "Yucatan Playa del Carmen",
      trackA: "RESEARCH_CANDIDATE_LEASE_CONVERSION_INTENT",
      trackAPayload: {
        hotelIdentity: "The Yucatán Resort Playa del Carmen",
        physicalCountry: "MX",
        agreementType: "LEASE_CONVERSION_INTENT",
        commercialTrigger: "Owner suspended operations to place the hotel into its lease portfolio",
        evidenceSource: HR_FIBRA,
        timing: "Suspension 2026-04-30; lessee not named",
        currentStatus: "Closed; lease intent disclosed; no named successor",
        potentialOpportunityClassification: "RESEARCH_CANDIDATE",
      },
    },
    {
      wave: "B18",
      discoveryPropertyId: "disc_tb_quality_suites_oscar_freire_2021",
      findingId: "find_tb_v3_oscar_freire_choice_current_20261121",
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
      evidenceConfidence: "HIGH",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commercialCategory: "BRAND_AFFILIATION",
      sourceUrl: CHOICE_OSCAR,
      sourceVintage: "2026",
      evidenceSnippet:
        `Retrieved 21 November 2026 from Choice Hotels property page BR009: Quality Suites Oscar Freire at Rua Oscar Freire 1948, Pinheiros, São Paulo. ` +
        `That agrees with Atlantica's operator presentation and the 2021 Comfort Suites to Quality Suites conversion. No contradiction found. No franchise expiration disclosed.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true, currentStatusCorroborated: true }),
      supersessionGap: "Current brand confirmed on the franchisor site. 2021 conversion finding unchanged.",
      hpcQuery: "Quality Suites Oscar Freire",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
    },
    {
      wave: "B18",
      discoveryPropertyId: "disc_tb_hyatt_ziva_riviera_cancun_2023",
      findingId: "find_tb_v3_hyatt_ziva_rc_not_in_tortuga_13",
      hotelName: "Hyatt Ziva Riviera Cancún",
      hotelNameNormalized: "hyatt ziva riviera cancun",
      city: "Cancún",
      stateProvince: "Quintana Roo",
      physicalCountry: "MX",
      currentBrand: "Hyatt Ziva",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commercialCategory: "MANAGEMENT",
      sourceUrl: "https://newsroom.hyatt.com/May-28-2026-Investor-Day-Release",
      sourcePublicationDate: "2026-05-28",
      sourceVintage: "2026",
      evidenceSnippet:
        `Hyatt Investor Day release dated 28 May 2026 restates that Hyatt and Tortuga entered 50-year management agreements for 13 of 14 formerly Playa-owned properties sold to Tortuga. ` +
        `Hyatt Ziva Riviera Cancún was third-party managed by Playa and terminated effective 15 December 2023. It is not one of the owned Playa assets sold to Tortuga. Successor manager remains unstated in a primary instrument.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
      supersessionGap: "Brand may continue under Hyatt. Management successor still unknown.",
      hpcQuery: "Hyatt Ziva Riviera Cancun",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
      trackC: "IDENTITY_OK_NO_HPC_MATCH",
    },
    {
      wave: "B18",
      discoveryPropertyId: "disc_tb_hyatt_zilara_riviera_maya_2023",
      findingId: "find_tb_v3_hyatt_zilara_rm_not_in_tortuga_13",
      hotelName: "Hyatt Zilara Riviera Maya",
      hotelNameNormalized: "hyatt zilara riviera maya",
      city: "Riviera Maya",
      stateProvince: "Quintana Roo",
      physicalCountry: "MX",
      currentBrand: "Hyatt Zilara",
      agreementType: null,
      contractEligible: false,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      persistEligible: true,
      discoveryMethod: "TRACK_B_HISTORICAL_INTELLIGENCE_V3",
      commercialCategory: "MANAGEMENT",
      sourceUrl: "https://newsroom.hyatt.com/May-28-2026-Investor-Day-Release",
      sourcePublicationDate: "2026-05-28",
      sourceVintage: "2026",
      evidenceSnippet:
        `Hyatt Investor Day release dated 28 May 2026 describes the Tortuga 13-of-14 fifty-year management set for formerly owned Playa assets. ` +
        `Hyatt Zilara Riviera Maya is outside that owned set. The 15 December 2023 third-party management termination stands. HPC remains ambiguous for Track C.`,
      reconciliation: classifyCurrentStatus({ historicallyVerified: true }),
      supersessionGap: "Ambiguous HPC identity flagged for Track C. No successor manager.",
      hpcQuery: "Hyatt Zilara Riviera Maya",
      trackA: "NOT_QUALIFIED_NO_PROSPECTIVE_DECISION",
      trackC: "AMBIGUOUS_HPC_IDENTITY",
    },
  ];
}

const STAGED = [
  {
    discoveryPropertyId: "disc_tb_v3_staged_ashford_master_hma_amend_20240911",
    persistEligible: false,
    persistReason: "PORTFOLIO_MASTER_NO_NAMED_HOTEL_SCHEDULE",
    physicalCountry: "US",
    wave: "B19",
    evidenceSnippet:
      "Ashford First Amendment to Second Consolidated Amended and Restated Hotel Master Management Agreement dated 11 September 2024. Amends Group Services fee cap. Does not name hotels.",
    sourceUrl: "https://www.sec.gov/Archives/edgar/data/1232582/000123258224000114/ahtfirstamendmenttohma.htm",
  },
  {
    discoveryPropertyId: "disc_tb_v3_staged_sotherly_condo_rental_programs",
    persistEligible: false,
    persistReason: "CONDO_RENTAL_PROGRAM_NOT_STANDARD_HOTEL_ROW",
    physicalCountry: "US",
    wave: "B19",
    evidenceSnippet:
      "Same Sotherly EX-10.22 Exhibit A lists Hyde Beach House Resort & Residences and Lyfe Resort & Residences as Current Hotels under the Our Town master, but they are condominium rental programs. Staged separately from the ten wholly-owned hotels.",
    sourceUrl: SOTHERLY_EXHIBIT,
  },
  {
    discoveryPropertyId: "disc_tb_v3_staged_sheraton_grand_sacramento_hma_ref",
    persistEligible: false,
    persistReason: "HMA_REFERENCED_IN_LOAN_INSTRUMENT_ONLY",
    physicalCountry: "US",
    wave: "B15",
    evidenceSnippet:
      "CMCT loan agreement dated 6 December 2024 for Sheraton Grand Sacramento (1230 J Street) references an Amended and Restated Management Agreement effective 1 January 2025. Terms are not taken from the loan. Original HMA exhibit not retrieved this cycle.",
    sourceUrl: "https://www.sec.gov/Archives/edgar/data/908311/000090831124000069/exhibit101-dbxsheratongran.htm",
  },
  {
    discoveryPropertyId: "disc_tb_v3_staged_park_hotels_portfolio_terms",
    persistEligible: false,
    persistReason: "PORTFOLIO_RANGE_WITHOUT_HOTEL_LEVEL_SCHEDULE",
    physicalCountry: "US",
    wave: "B19",
    evidenceSnippet:
      "Park Hotels 10-K for year ended 31 December 2025: management agreements initial terms 5 to 30 years; franchise agreements 10 to 20 years; five hotels under franchise. No hotel-level expiration schedule.",
    sourceUrl: "https://www.sec.gov/Archives/edgar/data/1617406/000161740626000006/pk-20251231.htm",
  },
  {
    discoveryPropertyId: "disc_tb_v3_staged_ihg_uk_unnamed_managed",
    persistEligible: false,
    persistReason: "UK_PORTFOLIO_WITHOUT_NAMED_HOTELS",
    physicalCountry: "GB",
    wave: "B16",
    evidenceSnippet:
      "IHG Hotels Limited 2023 accounts and IHG 2024/2025 annual reports confirm UK managed hotels and franchise/management fee models without naming individual UK hotels. Unnamed portfolio retained as staged research.",
    sourceUrl: "https://www.ihgplc.com/~/media/Files/I/Ihg-Plc/investors/corporate-governance/uk-subsidiary-statements/2023/UKSUBS/IHG-Hotels-Ltd-2023.pdf",
  },
];

async function hpcMeta() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`HPC_META_${res.status}`);
  const table = (json.tables || []).find((t) => t.id === HPC_TABLE_ID);
  const names = new Set((table?.fields || []).map((f) => f.name));
  return {
    nameField: ["Property Name", "Canonical Property Name"].find((n) => names.has(n)),
    countryField: "Country",
    cityField: "City",
  };
}

function countryMatches(recordCountry, expected) {
  const c = String(recordCountry || "").toLowerCase();
  const map = {
    MX: ["mexico", "méxico", "mx"],
    BR: ["brazil", "brasil", "br"],
    US: ["united states", "usa", "us"],
    CA: ["canada", "ca"],
    GB: ["united kingdom", "uk", "england"],
  };
  return (map[expected] || [String(expected || "").toLowerCase()]).some((x) => c.includes(x));
}

async function searchHpc(fields, query, country) {
  const q = String(query || "").replace(/"/g, "").slice(0, 40);
  if (!q || !fields?.nameField) return { hpcMatchStatus: "NOT_SEARCHED", hpcRecordId: null };
  const formula = `FIND("${q.toLowerCase()}", LOWER({${fields.nameField}}&""))`;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "8", pageSize: "8" });
  for (const f of [fields.countryField, fields.cityField, fields.nameField]) if (f) qs.append("fields[]", f);
  try {
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
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
  } catch {
    return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
  }
}

function matrix(dossiers) {
  const hotels = new Set();
  const countries = {};
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
    byCountry: Object.fromEntries(Object.entries(countries).map(([k, v]) => [k, { ...v, hotels: v.hotels.size }])),
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const exhibit = await fetchText(SONESTA_EXHIBIT);
  if (!exhibit.ok) throw new Error(`SONESTA_${exhibit.status}`);
  const allegro = recoverAllegro(decode(exhibit.text));
  if (!allegro) throw new Error("ALLEGRO_NOT_RECOVERED");

  const sotherly = await fetchText(SOTHERLY_EXHIBIT);
  if (!sotherly.ok || !/DoubleTree by Hilton Philadelphia Airport/i.test(sotherly.text)) {
    throw new Error("SOTHERLY_EXHIBIT_VERIFY_FAIL");
  }

  const dossiers = [allegro, ...sotherlyDossiers(), ...mexicoBrazilRecovery()];
  const fields = token ? await hpcMeta() : null;
  const cache = new Map();
  for (const d of dossiers) {
    const key = `${d.hpcQuery}|${d.physicalCountry}`;
    if (!fields) {
      d.hpcMatchStatus = "NOT_SEARCHED";
      continue;
    }
    if (cache.has(key)) Object.assign(d, cache.get(key));
    else {
      const hit = await searchHpc(fields, d.hpcQuery, d.physicalCountry);
      cache.set(key, hit);
      Object.assign(d, hit);
      await new Promise((r) => setTimeout(r, 110));
    }
  }

  const trackA = dossiers
    .filter((d) => d.trackA === "RESEARCH_CANDIDATE_LEASE_CONVERSION_INTENT")
    .map((d) => d.trackAPayload);
  const trackC = dossiers
    .filter((d) => d.trackC)
    .map((d) => ({
      discoveryPropertyId: d.discoveryPropertyId,
      hotelName: d.hotelName,
      physicalCountry: d.physicalCountry,
      issue: d.trackC,
      hpcMatchStatus: d.hpcMatchStatus,
    }));

  const mgmt = JSON.parse(
    fs.readFileSync(path.join(ROOT, "reports/sec-edgar-hif-v6-management-discovery/METRICS.json"), "utf8"),
  );

  const summary = {
    asOf: AS_OF,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    documentsInvestigatedEstimate: {
      managementDiscoveryPass: mgmt.documentsAttempted,
      curatedExhibitsAndReports: 12,
      ukIhGPortfolioDocs: 3,
      parkAndLoanInstrumentsReviewed: 3,
      eftsHitListingsReviewed: 24,
      totalApprox: mgmt.documentsAttempted + 12 + 3 + 3 + 24,
      note: "Document-research objective. Automated 8-K extractor rows were not auto-persisted because hotel-name quality was low.",
    },
    sourceProductivity: [
      {
        family: "SVC_EX10_SONESTA_SCHEDULE",
        documents: 1,
        yielding: 1,
        namedHotels: 1,
        note: "Allegro recovery from split HTML line",
      },
      {
        family: "SOTHERLY_EX10_OUR_TOWN_MASTER",
        documents: 1,
        yielding: 1,
        namedHotels: 10,
        agreements: 10,
        expiration: "2035-03-31",
        noticePeriods: "90-day manager renewal notice; 30-day lessee response",
      },
      {
        family: "SEC_MGMT_DISCOVERY_PASS_EX10_8K_10K",
        documents: mgmt.documentsAttempted,
        yieldingDocs: mgmt.documentsOk,
        extractedRows: mgmt.extractedRows,
        incrementalRaw: mgmt.incrementalEvents,
        duplicates: mgmt.duplicates,
        persistEligibleAfterQa: 0,
        note: "High duplicate/noise rate. Not used for Airtable creates.",
      },
      {
        family: "HR_RATINGS_FIBRAHOTEL",
        documents: 1,
        yielding: 1,
        namedHotels: 4,
        leasesCommenced: 2,
        nearTermSignals: 1,
      },
      {
        family: "CHOICE_FRANCHISOR_PAGE",
        documents: 1,
        yielding: 1,
        namedHotels: 1,
        currentStatusConfirmations: 1,
      },
      {
        family: "IHG_UK_CORPORATE",
        documents: 3,
        yielding: 0,
        namedHotels: 0,
        note: "Portfolio-only; staged",
      },
      {
        family: "ASHFORD_CMCT_PARK_LOAN_OR_PORTFOLIO",
        documents: 3,
        yielding: 0,
        staged: 3,
      },
    ],
    matrix: matrix(dossiers),
    qualifiedTrackAHandoffs: 0,
    trackAResearchCandidates: trackA.length,
    pr546State: "OPEN_AWAITING_INTEGRATION",
  };

  fs.writeFileSync(path.join(OUT, "WAVE_DOSSIERS.json"), JSON.stringify({ dossiers, staged: STAGED, summary }, null, 2));
  fs.writeFileSync(path.join(OUT, "MATRIX_COUNTRY_TIMING_TYPE.json"), JSON.stringify(summary.matrix, null, 2));
  fs.writeFileSync(
    path.join(OUT, "TRACK_A_HANDOFF.json"),
    JSON.stringify({ qualified: [], researchCandidates: trackA, notQualified: dossiers.filter((d) => d.trackA && !String(d.trackA).startsWith("RESEARCH")).map((d) => ({ id: d.discoveryPropertyId, classification: d.trackA })) }, null, 2),
  );
  fs.writeFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "SOURCE_PRODUCTIVITY.json"), JSON.stringify(summary.sourceProductivity, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
