/**
 * Track B International Contract & Capital Source Expansion V1 dossiers.
 * Sources: CLDT YE2025 10-K franchise+IHM schedules; PEB YE2025 10-K + IR Jul 2026 + Q3 2026 10-Q;
 * Fibra Inn Spanish press (Nov 2025 / Jan 2026) + governance Aimbridge authorization.
 * CUSTOMER_BLOCKED. Debt maturity ≠ conversion. Year/month precision preserved.
 */
import { classifyCurrentStatus } from "./hif-track-b-historical-foundation-v1.js";
import {
  EVIDENCE_CLASS,
  yearOnlyExpirationPolicy,
} from "./hif-track-b-international-source-registry-v1.js";

export const CLDT_10K =
  "https://www.sec.gov/Archives/edgar/data/1476045/000143774926006103/cldt20251231_10k.htm";
export const PEB_10K =
  "https://www.sec.gov/Archives/edgar/data/1474098/000147409826000015/peb-20251231.htm";
export const PEB_10Q_Q3 =
  "https://www.sec.gov/Archives/edgar/data/1474098/000147409826000072/peb-20260930.htm";
export const PEB_IR_JULY_2026 =
  "https://uk.marketscreener.com/news/pebblebrook-hotel-trust-investor-presentation-july-2026-low-resolution-ce7f51d3dc8ef126";
export const PEB_DEBT_PRESS =
  "https://www.sec.gov/Archives/edgar/data/1474098/000147409826000011/a021126debtamendmentprex991.htm";
export const FIBRA_INN_START =
  "https://fibrainn.mx/storage/docs/fibra-inn-internaliza-la-operacion-en-28-de-sus-hoteles.pdf";
export const FIBRA_INN_DONE =
  "https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf";
export const FIBRA_GOV_2025 =
  "https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf";

const IHM_NOTICE =
  "IHM management agreements: initial 5-year term; two automatic successive 5-year renewals unless IHM gives written notice no later than 90 days before then-current term expiration of intent not to renew. Company may terminate early on sale with six months' advance notice and no termination fee. Related-party: IHM is 100% owned by CLDT CEO Jeffrey H. Fisher. Per-hotel current term end dates are not stated in the franchise/management fee tables.";

function slug(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function hotel(row) {
  return {
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    persistEligible: true,
    contractEligible: row.contractEligible !== false,
    discoveryMethod: "TRACK_B_INTERNATIONAL_SOURCE_EXPANSION_V1",
    hpcMatchStatus: "PENDING_HPC_SEARCH",
    wave: row.wave || "B20",
    reconciliation: row.reconciliation || classifyCurrentStatus({ historicallyVerified: true }),
    ...row,
  };
}

/** CLDT YE2025 franchise schedule — expiration year only. */
export const CLDT_FRANCHISE_ROWS = [
  { hotelName: "Homewood Suites by Hilton Hartford-Farmington", city: "Farmington", stateProvince: "Connecticut", franchisor: "Promus Hotels, Inc", royalty: "4.0%", program: "4.0%", year: 2039 },
  { hotelName: "Residence Inn Long Island Holtsville", city: "Holtsville", stateProvince: "New York", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2026 },
  { hotelName: "Residence Inn White Plains", city: "White Plains", stateProvince: "New York", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2030 },
  { hotelName: "Residence Inn New Rochelle", city: "New Rochelle", stateProvince: "New York", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2030 },
  { hotelName: "Residence Inn Garden Grove", city: "Garden Grove", stateProvince: "California", franchisor: "Marriott International, Inc.", royalty: "5.0%", program: "2.5%", year: 2031 },
  { hotelName: "Homewood Suites by Hilton San Antonio River Walk", city: "San Antonio", stateProvince: "Texas", franchisor: "Hilton Franchise Holding LLC", royalty: "4.0%", program: "3.5%", year: 2041 },
  { hotelName: "Residence Inn Washington DC", city: "Washington", stateProvince: "District of Columbia", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2033 },
  { hotelName: "Residence Inn Tysons Corner", city: "Tysons Corner", stateProvince: "Virginia", franchisor: "Marriott International, Inc.", royalty: "5.0%", program: "2.5%", year: 2031 },
  { hotelName: "Hampton Inn Portland Downtown", city: "Portland", stateProvince: "Oregon", franchisor: "Hampton Inns Franchise LLC", royalty: "6.0%", program: "4.0%", year: 2032 },
  { hotelName: "Hyatt Place Pittsburgh North Shore", city: "Pittsburgh", stateProvince: "Pennsylvania", franchisor: "Hyatt Hotels, LLC", royalty: "5.0%", program: "3.5%", year: 2030 },
  { hotelName: "Hampton Inn Exeter", city: "Exeter", stateProvince: "New Hampshire", franchisor: "Hampton Inns Franchise LLC", royalty: "6.0%", program: "4.0%", year: 2031 },
  { hotelName: "Residence Inn Bellevue", city: "Bellevue", stateProvince: "Washington", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2033 },
  { hotelName: "SpringHill Suites Savannah", city: "Savannah", stateProvince: "Georgia", franchisor: "Marriott International, Inc.", royalty: "5.0%", program: "2.5%", year: 2033 },
  { hotelName: "Residence Inn Silicon Valley I", city: "Sunnyvale", stateProvince: "California", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2029 },
  { hotelName: "Residence Inn Silicon Valley II", city: "Sunnyvale", stateProvince: "California", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2029 },
  { hotelName: "Residence Inn San Mateo", city: "San Mateo", stateProvince: "California", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2029 },
  { hotelName: "Residence Inn Mountain View", city: "Mountain View", stateProvince: "California", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.5%", year: 2029 },
  { hotelName: "Hyatt Place Cherry Creek", city: "Denver", stateProvince: "Colorado", franchisor: "Hyatt Hotels, LLC", royalty: "5.0%", program: "3.5%", year: 2034 },
  { hotelName: "Courtyard Addison", city: "Addison", stateProvince: "Texas", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.0%", year: 2029 },
  { hotelName: "Residence Inn San Diego Gaslamp", city: "San Diego", stateProvince: "California", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2035 },
  { hotelName: "Hilton Garden Inn Marina del Rey", city: "Marina del Rey", stateProvince: "California", franchisor: "Hilton Franchise Holding LLC", royalty: "5.5%", program: "4.3%", year: 2030 },
  { hotelName: "Residence Inn Dedham", city: "Dedham", stateProvince: "Massachusetts", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2030 },
  { hotelName: "Residence Inn Il Lugano", city: "Fort Lauderdale", stateProvince: "Florida", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2045 },
  { hotelName: "Hilton Garden Inn Portsmouth", city: "Portsmouth", stateProvince: "New Hampshire", franchisor: "Hilton Garden Inns Franchise LLC", royalty: "5.5%", program: "4.0%", year: 2037 },
  { hotelName: "Courtyard Summerville", city: "Summerville", stateProvince: "South Carolina", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2037 },
  { hotelName: "Embassy Suites Springfield", city: "Springfield", stateProvince: "Virginia", franchisor: "Hilton Franchise Holding LLC", royalty: "5.5%", program: "4.0%", year: 2037 },
  { hotelName: "Residence Inn Summerville", city: "Summerville", stateProvince: "South Carolina", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2038 },
  { hotelName: "Courtyard Dallas", city: "Dallas", stateProvince: "Texas", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.0%", year: 2038 },
  { hotelName: "Residence Inn Austin Northwest/The Domain Area", city: "Austin", stateProvince: "Texas", franchisor: "Marriott International, Inc.", royalty: "6.0%", program: "2.5%", year: 2036 },
  { hotelName: "TownePlace Suites Austin Northwest/The Domain Area", city: "Austin", stateProvince: "Texas", franchisor: "Marriott International, Inc.", royalty: "5.5%", program: "2.0%", year: 2041 },
  { hotelName: "Home2 Suites Woodland Hills", city: "Woodland Hills", stateProvince: "California", franchisor: "Hilton Franchise Holding LLC", royalty: "5.0%", program: "3.0%", year: 2040 },
  { hotelName: "Hilton Garden Inn Destin Miramar Beach", city: "Destin", stateProvince: "Florida", franchisor: "Hilton Franchise Holding LLC", royalty: "5.5%", program: "4.0%", year: 2042 },
  { hotelName: "Home2 Suites Phoenix Downtown", city: "Phoenix", stateProvince: "Arizona", franchisor: "Hilton Franchise Holding LLC", royalty: "4.0%", program: "3.5%", year: 2044 },
];

function cldtFranchiseDossiers() {
  return CLDT_FRANCHISE_ROWS.map((r) => {
    const key = slug(r.hotelName);
    const yearPolicy = yearOnlyExpirationPolicy(r.year);
    const near = r.year <= 2029;
    return hotel({
      wave: "B20",
      discoveryPropertyId: `disc_tb_ise_cldt_${key}`,
      findingId: `find_tb_ise_cldt_${key}_franchise_${r.year}`,
      contractId: `ctr_tb_ise_cldt_${key}_franchise_${r.year}`,
      hotelName: r.hotelName,
      hotelNameNormalized: r.hotelName.toLowerCase(),
      city: r.city,
      stateProvince: r.stateProvince,
      physicalCountry: "US",
      currentBrand: r.hotelName.split(" ").slice(0, 3).join(" "),
      currentOperator: "Island Hospitality Management, LLC",
      counterpartyName: r.franchisor,
      owner: "Chatham Lodging Trust",
      agreementType: "FRANCHISE",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "BRAND_AFFILIATION",
      findingType: "BRAND_AFFILIATION",
      expirationDate: null,
      milestoneDate: null,
      expirationYear: r.year,
      datePrecision: "YEAR_ONLY",
      sourceUrl: CLDT_10K,
      sourcePublicationDate: "2026-02-27",
      sourceVintage: "2025",
      sourceFamily: "OWNER_FRANCHISE_HMA_SCHEDULE",
      evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
      evidenceSnippet:
        `CLDT Form 10-K for the year ended 2025-12-31, Hotel Franchise Agreements table as of 2025-12-31: ${r.hotelName}; Franchise Company ${r.franchisor}; Royalty ${r.royalty}; Marketing/Program ${r.program}; Expiration ${r.year}. ` +
        `${yearPolicy.notes} Do not invent a day or month. IHM related-party management applies separately. ` +
        `Franchise expiration alone is not a verified conversion opportunity.`,
      renewalSummary: IHM_NOTICE,
      unresolvedTerms: [
        "Exact franchise expiration month/day not disclosed",
        "Franchise renewal / early-termination notice windows not stated in the table",
        "Per-hotel IHM current term end date not stated",
      ],
      hpcQuery: r.hotelName.replace(/ by Hilton| by Marriott/gi, "").slice(0, 40),
      trackA: near
        ? "DECISION_WINDOW_RESEARCH_CANDIDATE_YEAR_ONLY"
        : "NOT_QUALIFIED_DISTANT_OR_YEAR_ONLY",
      trackAPayload: near
        ? {
            hotelName: r.hotelName,
            owner: "Chatham Lodging Trust",
            signal: "FRANCHISE_EXPIRATION_YEAR",
            year: r.year,
            limitation: "YEAR_ONLY_NOT_VERIFIED_CONVERSION; related-party IHM manager",
            sourceUrl: CLDT_10K,
          }
        : undefined,
    });
  });
}

/** Holtsville IHM management notice terms (hotel-level attachment of portfolio IHM rules). */
function cldtHoltsvilleManagement() {
  return hotel({
    wave: "B20",
    discoveryPropertyId: "disc_tb_ise_cldt_residence_inn_long_island_holtsville",
    findingId: "find_tb_ise_cldt_holtsville_ihm_mgmt_notice",
    contractId: "ctr_tb_ise_cldt_holtsville_ihm_mgmt",
    hotelName: "Residence Inn Long Island Holtsville",
    hotelNameNormalized: "residence inn long island holtsville",
    city: "Holtsville",
    stateProvince: "New York",
    physicalCountry: "US",
    currentBrand: "Residence Inn",
    currentOperator: "Island Hospitality Management, LLC",
    counterpartyName: "Island Hospitality Management, LLC",
    owner: "Chatham Lodging Trust",
    agreementType: "MANAGEMENT",
    agreementRole: "ORIGINAL",
    contractStatus: "CURRENTLY_CORROBORATED",
    commercialCategory: "MANAGEMENT",
    findingType: "MANAGEMENT",
    sourceUrl: CLDT_10K,
    sourcePublicationDate: "2026-02-27",
    sourceVintage: "2025",
    sourceFamily: "OWNER_FRANCHISE_HMA_SCHEDULE",
    evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
    evidenceSnippet:
      `CLDT YE2025 10-K: Island Hospitality Management, LLC managed all 33 CLDT hotels as of 2025-12-31, including Residence Inn Long Island Holtsville (base fee 3.0%). ${IHM_NOTICE} ` +
      `This stores management notice/renewal mechanics. It does not state Holtsville's current HMA term-end day. Related-party operator limits third-party MCT pitches without cause.`,
    terminationSummary:
      "Early termination at Company option upon sale for no termination fee with six months' advance notice. IHM non-renewal requires written notice no later than 90 days before term end. Cause termination includes failure to meet specified performance levels.",
    renewalSummary: IHM_NOTICE,
    noticeDeadlineSummary: "IHM non-renewal notice: not later than 90 days before then-current term expiration. Sale exit: six months' advance notice.",
    unresolvedTerms: ["Exact current HMA term-end date for Holtsville", "Incentive threshold dollar amounts beyond table fees"],
    hpcQuery: "Residence Inn Holtsville",
    trackA: "LIMITATION_RELATED_PARTY_OPERATOR",
  });
}

function pebDossiers() {
  return [
    hotel({
      wave: "B21",
      discoveryPropertyId: "disc_tb_ise_peb_margaritaville_hollywood_beach",
      findingId: "find_tb_ise_peb_margaritaville_hma_terminable_202609",
      contractId: "ctr_tb_ise_peb_margaritaville_hma_terminable_202609",
      hotelName: "Margaritaville Hollywood Beach Resort",
      hotelNameNormalized: "margaritaville hollywood beach resort",
      city: "Hollywood",
      stateProvince: "Florida",
      physicalCountry: "US",
      currentBrand: "Margaritaville",
      owner: "Pebblebrook Hotel Trust",
      agreementType: "MANAGEMENT",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: null,
      milestoneDate: null,
      datePrecision: "MONTH",
      impreciseDates: ["management-terminable-at-will:2026-09"],
      sourceUrl: PEB_IR_JULY_2026,
      sourcePublicationDate: "2026-07-29",
      sourceVintage: "2026",
      sourceFamily: "OWNER_IR_EARNINGS",
      evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
      evidenceSnippet:
        `PEB July 2026 Investor Presentation footnote (1): Margaritaville Hollywood Beach Resort's management agreement becomes terminable at will beginning in September 2026. ` +
        `Month precision only — day not stated; not stored as ISO milestone. Corroboration: PEB YE2025 10-K non-terminable management list does not include this hotel (lists Westin Copley, Mondrian LA, Westin SD Gaslamp, W LA). ` +
        `Do not conflate with the Margaritaville mortgage maturity/repayment (capital fact only). Termination fee amount not disclosed in the IR footnote.`,
      terminationSummary:
        "Becomes terminable at will beginning September 2026 per PEB IR. Day-of-month and any termination fee not stated in the IR footnote.",
      unresolvedTerms: [
        "Exact day in September 2026 when terminability begins",
        "Termination fee if any",
        "Manager identity for this HMA not named in the IR footnote",
      ],
      hpcQuery: "Margaritaville Hollywood Beach",
      trackA: "DECISION_WINDOW_RESEARCH_CANDIDATE_MONTH",
      trackAPayload: {
        hotelName: "Margaritaville Hollywood Beach Resort",
        owner: "Pebblebrook Hotel Trust",
        signal: "HMA_TERMINABLE_AT_WILL_WINDOW",
        month: "2026-09",
        limitation: "MONTH_ONLY; not auto-Verified conversion; fee unknown",
        sourceUrl: PEB_IR_JULY_2026,
      },
    }),
    hotel({
      wave: "B21",
      discoveryPropertyId: "disc_tb_ise_peb_margaritaville_hollywood_beach",
      findingId: "find_tb_ise_peb_margaritaville_franchise_202807",
      contractId: "ctr_tb_ise_peb_margaritaville_franchise_202807",
      hotelName: "Margaritaville Hollywood Beach Resort",
      hotelNameNormalized: "margaritaville hollywood beach resort",
      city: "Hollywood",
      stateProvince: "Florida",
      physicalCountry: "US",
      currentBrand: "Margaritaville",
      owner: "Pebblebrook Hotel Trust",
      counterpartyName: "Margaritaville franchise / brand licensor (not named in table)",
      agreementType: "FRANCHISE",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "BRAND_AFFILIATION",
      findingType: "BRAND_AFFILIATION",
      expirationDate: null,
      milestoneDate: null,
      datePrecision: "MONTH",
      impreciseDates: ["franchise-expiration:2028-07"],
      sourceUrl: PEB_10K,
      sourcePublicationDate: "2026-02-25",
      sourceVintage: "2025",
      sourceFamily: "OWNER_FRANCHISE_HMA_SCHEDULE",
      evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
      evidenceSnippet:
        `PEB YE2025 10-K Franchise and Sub-License Agreements table: Margaritaville Hollywood Beach Resort expiration July 2028. Month only — day not stored as ISO. Separate from HMA terminability (Sep 2026) and from repaid mortgage.`,
      unresolvedTerms: ["Exact franchise expiration day", "Franchise notice / renewal terms"],
      hpcQuery: "Margaritaville Hollywood Beach",
      trackA: "NOT_QUALIFIED_DISTANT_MONTH_FRANCHISE",
    }),
    hotel({
      wave: "B21",
      discoveryPropertyId: "disc_tb_ise_peb_margaritaville_hollywood_beach",
      findingId: "find_tb_ise_peb_margaritaville_mortgage_repaid_202602",
      contractId: null,
      hotelName: "Margaritaville Hollywood Beach Resort",
      hotelNameNormalized: "margaritaville hollywood beach resort",
      city: "Hollywood",
      stateProvince: "Florida",
      physicalCountry: "US",
      currentBrand: "Margaritaville",
      owner: "Pebblebrook Hotel Trust",
      agreementType: null,
      contractEligible: false,
      commercialCategory: "OTHER",
      findingType: "OTHER",
      contractStatus: "HISTORICALLY_VERIFIED",
      sourceUrl: PEB_10Q_Q3,
      sourcePublicationDate: "2026-10-29",
      sourceVintage: "2026",
      sourceFamily: "CMBS_DEBT_CAPITAL",
      evidenceClass: EVIDENCE_CLASS.CAPITAL_CONTEXT_NOT_CONVERSION,
      evidenceSnippet:
        `PEB Form 10-Q for quarter ended 2026-09-30 Note 5: Margaritaville Hollywood Beach Resort mortgage shown with maturity September 2026 and $0 balance at 2026-09-30; footnote (4) In February 2026, the Company paid down the remaining loan balance. ` +
        `Corroborating 8-K EX-99.1 debt press (2026-02-12): used $40 million cash to fully retire the mortgage originally due September 2026. ` +
        `CAPITAL CONTEXT ONLY — debt maturity/repayment is not proof of hotel conversion or HMA change.`,
      unresolvedTerms: ["Exact February 2026 repayment day"],
      hpcQuery: "Margaritaville Hollywood Beach",
      trackA: "CAPITAL_CONTEXT_NOT_CONVERSION",
      trackAPayload: {
        hotelName: "Margaritaville Hollywood Beach Resort",
        signal: "MORTGAGE_REPAID",
        month: "2026-02",
        rule: "DEBT_MATURITY_NOT_CONVERSION",
        sourceUrl: PEB_10Q_Q3,
      },
    }),
    hotel({
      wave: "B21",
      discoveryPropertyId: "disc_tb_ise_peb_westin_copley_place_boston",
      findingId: "find_tb_ise_peb_westin_copley_hma_202812",
      contractId: "ctr_tb_ise_peb_westin_copley_hma_202812",
      hotelName: "The Westin Copley Place, Boston",
      hotelNameNormalized: "the westin copley place boston",
      city: "Boston",
      stateProvince: "Massachusetts",
      physicalCountry: "US",
      currentBrand: "Westin",
      owner: "Pebblebrook Hotel Trust",
      agreementType: "MANAGEMENT",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      expirationDate: null,
      milestoneDate: null,
      datePrecision: "MONTH",
      impreciseDates: ["non-terminable-management-expiration:2028-12"],
      sourceUrl: PEB_10K,
      sourcePublicationDate: "2026-02-25",
      sourceVintage: "2025",
      sourceFamily: "OWNER_FRANCHISE_HMA_SCHEDULE",
      evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
      evidenceSnippet:
        `PEB YE2025 10-K: The Westin Copley Place, Boston is listed among hotels with non-terminable management agreements; expiration December 2028 (assuming manager unilateral extension options exercised). ` +
        `IR July 2026 footnote (2) restates management agreement expires in December 2028. Month only.`,
      terminationSummary: "Non-terminable except manager breach / performance thresholds as described generally in the 10-K, until December 2028 expiration.",
      unresolvedTerms: ["Exact expiration day", "Manager name for Copley HMA"],
      hpcQuery: "Westin Copley Place",
      trackA: "DECISION_WINDOW_RESEARCH_CANDIDATE_MONTH",
      trackAPayload: {
        hotelName: "The Westin Copley Place, Boston",
        signal: "NON_TERMINABLE_HMA_EXPIRATION",
        month: "2028-12",
        limitation: "MONTH_ONLY; not Verified conversion",
        sourceUrl: PEB_10K,
      },
    }),
    hotel({
      wave: "B21",
      discoveryPropertyId: "disc_tb_ise_peb_embassy_suites_sd_bay",
      findingId: "find_tb_ise_peb_embassy_sd_franchise_202801",
      contractId: "ctr_tb_ise_peb_embassy_sd_franchise_202801",
      hotelName: "Embassy Suites San Diego Bay - Downtown",
      hotelNameNormalized: "embassy suites san diego bay downtown",
      city: "San Diego",
      stateProvince: "California",
      physicalCountry: "US",
      currentBrand: "Embassy Suites",
      owner: "Pebblebrook Hotel Trust",
      agreementType: "FRANCHISE",
      agreementRole: "ORIGINAL",
      contractStatus: "CURRENTLY_CORROBORATED",
      commercialCategory: "BRAND_AFFILIATION",
      findingType: "BRAND_AFFILIATION",
      expirationDate: null,
      milestoneDate: null,
      datePrecision: "MONTH",
      impreciseDates: ["franchise-expiration:2028-01"],
      sourceUrl: PEB_10K,
      sourcePublicationDate: "2026-02-25",
      sourceVintage: "2025",
      sourceFamily: "OWNER_FRANCHISE_HMA_SCHEDULE",
      evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
      evidenceSnippet:
        `PEB YE2025 10-K franchise table + IR July 2026 footnote (3): Embassy Suites San Diego Bay – Downtown franchise agreement expires in January 2028. Month only. Not a verified conversion.`,
      unresolvedTerms: ["Exact franchise expiration day", "Franchise notice windows"],
      hpcQuery: "Embassy Suites San Diego Bay",
      trackA: "DECISION_WINDOW_RESEARCH_CANDIDATE_MONTH",
      trackAPayload: {
        hotelName: "Embassy Suites San Diego Bay - Downtown",
        signal: "FRANCHISE_EXPIRATION_MONTH",
        month: "2028-01",
        limitation: "MONTH_ONLY; not Verified conversion",
        sourceUrl: PEB_10K,
      },
    }),
  ];
}

/** Fibra Inn 28 hotels — Aimbridge Latam early termination / internalization to Servicios Hoteleros FINN. */
export const FIBRA_INN_28 = [
  { hotelName: "Microtel Inn & Suites by Wyndham Toluca", city: "Toluca", rooms: 129 },
  { hotelName: "Microtel Inn & Suites by Wyndham Cd. Juárez", city: "Ciudad Juárez", rooms: 113 },
  { hotelName: "Wyndham Garden Guadalajara", city: "Guadalajara", rooms: 186 },
  { hotelName: "Wyndham Garden Playa del Carmen", city: "Playa del Carmen", rooms: 196 },
  { hotelName: "Wyndham Garden Monterrey", city: "Monterrey", rooms: 85 },
  { hotelName: "Hampton Inn by Hilton Monterrey", city: "Monterrey", rooms: 223 },
  { hotelName: "Hampton Inn by Hilton Saltillo", city: "Saltillo", rooms: 227 },
  { hotelName: "Hampton Inn by Hilton Reynosa", city: "Reynosa", rooms: 145 },
  { hotelName: "Hampton Inn by Hilton Querétaro", city: "Querétaro", rooms: 178 },
  { hotelName: "Hampton Inn by Hilton Hermosillo", city: "Hermosillo", rooms: 151 },
  { hotelName: "Hampton Inn by Hilton Chihuahua", city: "Chihuahua", rooms: 190 },
  { hotelName: "Holiday Inn Express Saltillo", city: "Saltillo", rooms: 180 },
  { hotelName: "Holiday Inn Express Toluca", city: "Toluca", rooms: 268, disambiguator: "268_rooms" },
  { hotelName: "Holiday Inn Express Monterrey", city: "Monterrey", rooms: 198 },
  { hotelName: "Holiday Inn Express Toluca", city: "Toluca", rooms: 127, disambiguator: "127_rooms" },
  { hotelName: "Holiday Inn Express & Suites Cd. Juárez", city: "Ciudad Juárez", rooms: 182 },
  { hotelName: "Holiday Inn Altamira", city: "Altamira", rooms: 203 },
  { hotelName: "Holiday Inn Reynosa", city: "Reynosa", rooms: 95 },
  { hotelName: "Holiday Inn Cd. Juárez", city: "Ciudad Juárez", rooms: 196 },
  { hotelName: "Crowne Plaza Monterrey", city: "Monterrey", rooms: 219 },
  { hotelName: "Staybridge Suites Guadalajara", city: "Guadalajara", rooms: 117 },
  { hotelName: "Marriott Puebla", city: "Puebla", rooms: 296 },
  { hotelName: "Courtyard by Marriott Saltillo", city: "Saltillo", rooms: 180 },
  { hotelName: "Courtyard by Marriott Chihuahua", city: "Chihuahua", rooms: 152 },
  { hotelName: "AC Hotels by Marriott Guadalajara", city: "Guadalajara", rooms: 180 },
  { hotelName: "Exhacienda San Xavier Guanajuato", city: "Guanajuato", rooms: 155 },
  { hotelName: "Casa Grande Chihuahua", city: "Chihuahua", rooms: 115 },
  { hotelName: "Casa Grande Delicias", city: "Delicias", rooms: 88 },
];

function fibraInnDossiers() {
  return FIBRA_INN_28.map((h) => {
    const key = slug(`${h.hotelName}_${h.disambiguator || h.rooms}`);
    return hotel({
      wave: "B22",
      discoveryPropertyId: `disc_tb_ise_finn_${key}`,
      findingId: `find_tb_ise_finn_${key}_mgmt_term_20260115`,
      contractId: `ctr_tb_ise_finn_${key}_mgmt_term_20260115`,
      milestoneId: `ms_tb_ise_finn_${key}_internalization_complete_20260115`,
      milestoneType: "TERMINATION",
      milestoneDate: "2026-01-15",
      hotelName: h.hotelName,
      hotelNameNormalized: `${h.hotelName} ${h.rooms}`.toLowerCase(),
      city: h.city,
      stateProvince: null,
      physicalCountry: "MX",
      rooms: h.rooms,
      currentOperator: "Servicios Hoteleros FINN, S.C.",
      priorOperator: "Aimbridge Latam, S.A.P.I. de C.V.",
      counterpartyName: "Aimbridge Latam, S.A.P.I. de C.V.",
      owner: "Fibra Inn (Fideicomiso DB/1616 / FINN13)",
      agreementType: "MANAGEMENT",
      agreementRole: "TERMINATION_INSTRUMENT",
      contractStatus: "TERMINATED_VERIFIED",
      commercialCategory: "MANAGEMENT",
      findingType: "MANAGEMENT",
      commencementDate: null,
      expirationDate: "2026-01-15",
      sourceUrl: FIBRA_INN_DONE,
      sourcePublicationDate: "2026-01-15",
      sourceVintage: "2026",
      sourceFamily: "MX_BMV_EMISNET",
      evidenceClass: EVIDENCE_CLASS.CORPORATE_CONTRACTUAL_DISCLOSURE,
      language: "es",
      calaMarket: true,
      evidenceSnippet:
        `Fibra Inn comunicado 2025-11-03 (español): inicia transición ordenada de operación de 28 hoteles del 2025-11-01 al 2026-01-15; lista nominativa incluye ${h.hotelName} (${h.rooms} cuartos). ` +
        `Comunicado 2026-01-15: finaliza internalización el 2026-01-15 mediante adquisición de contratos de operación; contraprestación Ps. 172.3 millones al operador anterior. ` +
        `Governance 2025: autorizó terminación anticipada de acuerdos de advisory de gestión hotelera con Aimbridge Latam, S.A.P.I. de C.V. para hoteles 100% propiedad; designó Servicios Hoteleros FINN, S.C. como operadora hotelera autorizada. ` +
        `Fecha exacta de traspaso hotel-por-hotel dentro de la ventana es desconocida; se almacena la fecha de cierre del proceso 2026-01-15. No es oportunidad de conversión de marca verificada.`,
      terminationSummary:
        "Early termination / acquisition of prior third-party operating contracts completed 2026-01-15 for the 28-hotel internalization set. Individual hotel switch dates within 2025-11-01..2026-01-15 not disclosed.",
      renewalSummary: "Successor internal operator Servicios Hoteleros FINN, S.C. Franchise/license agreements with brands remain separate and are not terminated by this announcement.",
      unresolvedTerms: [
        "Per-hotel exact transition date within Nov 2025–Jan 2026 window",
        "Prior Aimbridge agreement original expiration / notice clause text",
        "Whether any of the 28 hotels had differentiated commercial terms",
      ],
      hpcQuery: h.hotelName.replace(/ by Hilton| by Marriott| by Wyndham/gi, "").slice(0, 40),
      trackA: "PORTFOLIO_OPERATOR_CHANGE_COMPLETED_NOT_CONVERSION",
      trackAPayload: {
        hotelName: h.hotelName,
        owner: "Fibra Inn",
        signal: "OPERATOR_INTERNALIZATION_COMPLETED",
        completionDate: "2026-01-15",
        priorOperator: "Aimbridge Latam",
        currentOperator: "Servicios Hoteleros FINN",
        limitation: "Completed portfolio operator change; not a brand conversion opportunity; franchise instruments separate",
        sourceUrls: [FIBRA_INN_START, FIBRA_INN_DONE, FIBRA_GOV_2025],
      },
      trackC: h.disambiguator
        ? "TWO_HOLIDAY_INN_EXPRESS_TOLUCA_NEED_ADDRESS_DISAMBIGUATION"
        : undefined,
    });
  });
}

export const STAGED_SOURCES = [
  {
    id: "BR_CVM_HOTEL_ISSUER_THIN",
    family: "BR_CVM",
    status: "STAGED",
    note: "CVM public access confirmed conceptually; no hotel-named HMA/franchise instrument persisted this wave. Hard gate: administração condominial / multipropriedade ≠ HMA.",
  },
  {
    id: "BMV_EMISNET_DIRECT_PDF_503",
    family: "MX_BMV_EMISNET",
    status: "ACCESS_RESTRICTED_INTERMITTENT",
    note: "Direct BMV eventfid PDFs intermittently 503; Fibra Inn issuer IR PDF mirrors used instead.",
  },
  {
    id: "PAID_PROPERTY_REGISTRIES",
    family: "PUBLIC_CONCESSION_AWARD",
    status: "RESTRICTED_NO_PAID",
    note: "No paid corporate/property registries accessed. Founder authorization required for paid research.",
  },
];

export function buildInternationalSourceExpansionDossiers() {
  return [...cldtFranchiseDossiers(), cldtHoltsvilleManagement(), ...pebDossiers(), ...fibraInnDossiers()];
}
