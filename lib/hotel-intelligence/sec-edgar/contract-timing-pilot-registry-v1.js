/**
 * Seed registry for Contract Timing CMBS / REIT / FDD pilot.
 * Includes hotel REITs + CMBS securitization issuers (not hotel-company-only).
 */

export const CONTRACT_TIMING_PILOT_REGISTRY_V1 = "contract-timing-pilot-registry-v1";

/** Max documents for this pilot wave. */
export const CONTRACT_TIMING_PILOT_DOC_CAP = 100;

/**
 * Initial validation cases — publicly identifiable examples.
 * Accessions validated/updated at runtime when live resolve succeeds.
 */
export const CONTRACT_TIMING_SEED_DOCUMENTS = Object.freeze([
  // Hotel REIT franchise / management schedules
  {
    id: "seed_chatham_10k_2019",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "chatham-lodging-trust",
    legalName: "Chatham Lodging Trust",
    ticker: "CLDT",
    cik: "0001476045",
    form: "10-K",
    accessionNumber: "0001476045-19-000013",
    filingDate: "2019-02-26",
    primaryDocument: null,
    note: "Chatham franchise/management expiration schedule validation case",
    priority: 1,
  },
  {
    id: "seed_chatham_10k_2016",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "chatham-lodging-trust",
    legalName: "Chatham Lodging Trust",
    ticker: "CLDT",
    cik: "0001476045",
    form: "10-K",
    accessionNumber: "0001476045-16-000046",
    filingDate: "2016-02-29",
    primaryDocument: null,
    note: "Earlier Chatham HTML schedule (higher table yield)",
    priority: 1,
  },
  {
    id: "seed_sotherly_10k_2019",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "sotherly-hotels",
    legalName: "Sotherly Hotels Inc.",
    ticker: "SOHO",
    cik: "0001166003",
    form: "10-K",
    accessionNumber: "0001166003-19-000011",
    filingDate: "2019-03-07",
    primaryDocument: null,
    note: "Sotherly franchise expiration / property schedule case",
    priority: 1,
  },
  {
    id: "seed_sotherly_10k_2015",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "sotherly-hotels",
    legalName: "Sotherly Hotels Inc.",
    ticker: "SOHO",
    cik: "0001166003",
    form: "10-K",
    accessionNumber: "0001166003-15-000015",
    filingDate: "2015-03-13",
    primaryDocument: null,
    note: "Sotherly historical HTML schedule",
    priority: 1,
  },
  {
    id: "seed_nexpoint_10k",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "nexpoint-real-estate-finance",
    legalName: "NexPoint Real Estate Finance, Inc.",
    ticker: "NREF",
    cik: "0001780312",
    form: "10-K",
    accessionNumber: null,
    filingDate: null,
    primaryDocument: null,
    resolveLatest10K: true,
    note: "NexPoint — hospitality / CMBS-adjacent disclosures",
    priority: 2,
  },
  {
    id: "seed_sunstone_10k_2012",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "sunstone-hotel-investors",
    legalName: "Sunstone Hotel Investors, Inc.",
    ticker: "SHO",
    cik: "0001295810",
    form: "10-K",
    accessionNumber: "0001104659-12-013838",
    filingDate: "2012-02-28",
    primaryDocument: "a12-1033_110k.htm",
    note: "Sunstone high-yield manager/franchise schedule (prior V2.4 evidence)",
    priority: 1,
  },
  {
    id: "seed_diamondrock_10k_2014",
    sourceFamily: "HOTEL_REIT_SCHEDULE",
    slug: "diamondrock-hospitality",
    legalName: "DiamondRock Hospitality Company",
    ticker: "DRH",
    cik: "0001298946",
    form: "10-K",
    accessionNumber: "0001298946-14-000021",
    filingDate: "2014-02-25",
    primaryDocument: "drh_10kx12312013.htm",
    note: "DiamondRock franchise expiration schedule test case",
    priority: 1,
  },

  // CMBS 424B — securitization trusts / depositors (NOT hotel companies)
  {
    id: "seed_cmbs_jpmcc_424b_courtyard_portfolio",
    sourceFamily: "CMBS_424B",
    slug: "jpmorgan-chase-commercial-mortgage",
    legalName: "J.P. Morgan Chase Commercial Mortgage Securities Corp.",
    ticker: null,
    cik: "0001013611",
    form: "424B2",
    accessionNumber: "0001539497-20-000579",
    filingDate: "2020-03-24",
    primaryDocument: "n2032_x17-424b2.htm",
    note: "Validated high-yield CMBS 424B — hotel Franchise Expiration Date column (Courtyard portfolio)",
    priority: 1,
  },
  {
    id: "seed_cmbs_jpmcc_424b",
    sourceFamily: "CMBS_424B",
    slug: "jpmorgan-chase-commercial-mortgage",
    legalName: "J.P. Morgan Chase Commercial Mortgage Securities Corp.",
    ticker: null,
    cik: "0001013611",
    form: "424B5",
    accessionNumber: null,
    resolveLatestForm: "424B5",
    note: "CMBS depositor — search hotel mortgaged-property franchise tables",
    priority: 1,
  },
  {
    id: "seed_cmbs_banc_america_424b",
    sourceFamily: "CMBS_424B",
    slug: "banc-america-commercial-mortgage",
    legalName: "Banc of America Commercial Mortgage Inc.",
    ticker: null,
    cik: "0001004153",
    form: "424B5",
    accessionNumber: null,
    resolveLatestForm: "424B5",
    note: "CMBS depositor for hotel collateral franchise expiration tables",
    priority: 2,
  },
  {
    id: "seed_cmbs_wells_424b",
    sourceFamily: "CMBS_424B",
    slug: "wells-fargo-commercial-mortgage",
    legalName: "Wells Fargo Commercial Mortgage Securities, Inc.",
    ticker: null,
    cik: "0001701606",
    form: "424B2",
    accessionNumber: null,
    resolveLatestForm: "424B2",
    note: "CMBS 424B hotel collateral schedules",
    priority: 2,
  },
]);

/** CMBS / financing registrant CIKs for expanded discovery (beyond hotel REITs). */
export const CMBS_ISSUER_REGISTRY = Object.freeze([
  { slug: "jpmorgan-chase-commercial-mortgage", cik: "0001013611", forms: ["424B2", "424B5", "424B3"] },
  { slug: "banc-america-commercial-mortgage", cik: "0001004153", forms: ["424B2", "424B5"] },
  { slug: "wells-fargo-commercial-mortgage", cik: "0001701606", forms: ["424B2", "424B5"] },
  { slug: "morgan-stanley-capital-i", cik: "0001005150", forms: ["424B2", "424B5"] },
  { slug: "gs-mortgage-securities", cik: "0001541502", forms: ["424B2", "424B5"] },
]);

export function listSeedDocuments(opts = {}) {
  let rows = [...CONTRACT_TIMING_SEED_DOCUMENTS];
  if (opts.sourceFamily) {
    rows = rows.filter((r) => r.sourceFamily === opts.sourceFamily);
  }
  return rows.sort((a, b) => (a.priority || 99) - (b.priority || 99));
}
