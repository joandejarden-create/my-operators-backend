/**
 * Pilot SEC CIK registry for hotel-owning public companies / REITs.
 * CIKs are zero-padded 10-digit strings as used by data.sec.gov.
 */

export const SEC_EDGAR_CIK_REGISTRY_V1 = "sec-edgar-cik-registry-v1";

/** @type {ReadonlyArray<{slug:string, legalName:string, ticker:string|null, cik:string, organizationType:string, priority:number, note:string}>} */
export const PILOT_SEC_COMPANIES = Object.freeze([
  {
    slug: "marriott-international",
    legalName: "Marriott International, Inc.",
    ticker: "MAR",
    cik: "0001048286",
    organizationType: "Brand / Franchisor",
    priority: 1,
    note: "Global brand/franchisor + manager; property-level exhibits less common than REIT schedules.",
  },
  {
    slug: "sunstone-hotel-investors",
    legalName: "Sunstone Hotel Investors, Inc.",
    ticker: "SHO",
    cik: "0001295810",
    organizationType: "REIT",
    priority: 1,
    note: "Lodging REIT — prioritize property schedules and management/franchise disclosures. CIK confirmed via SEC company_tickers when live.",
  },
  {
    slug: "ashford-hospitality-trust",
    legalName: "Ashford Hospitality Trust, Inc.",
    ticker: "AHT",
    cik: "0001232582",
    organizationType: "REIT",
    priority: 1,
    note: "Lodging REIT — confirm CIK via SEC ticker resolve before persist if registry drifts.",
  },
  {
    slug: "host-hotels-resorts",
    legalName: "Host Hotels & Resorts, Inc.",
    ticker: "HST",
    cik: "0001070750",
    organizationType: "REIT",
    priority: 1,
    note: "Lodging REIT — strong property-level disclosure history. CIK confirmed via SEC company_tickers (HST).",
  },
]);

/**
 * Ashford Hospitality Trust and Sunstone must not share a CIK.
 * Resolve Ashford via SEC company_tickers / submissions before research apply.
 */
export const CIK_VERIFY_REQUIRED_SLUGS = Object.freeze(["ashford-hospitality-trust"]);

export function normalizeCik(cik) {
  const digits = String(cik || "").replace(/\D/g, "");
  if (!digits) return null;
  return digits.padStart(10, "0");
}

export function getPilotCompany(slugOrCik) {
  const key = String(slugOrCik || "").trim().toLowerCase();
  if (!key) return null;
  return (
    PILOT_SEC_COMPANIES.find((c) => c.slug === key) ||
    PILOT_SEC_COMPANIES.find((c) => normalizeCik(c.cik) === normalizeCik(key)) ||
    null
  );
}

export function listPilotCompanies(opts = {}) {
  const includeUnverified = opts.includeUnverifiedCik === true;
  return PILOT_SEC_COMPANIES.filter((c) => includeUnverified || !c.cikUnverified);
}
