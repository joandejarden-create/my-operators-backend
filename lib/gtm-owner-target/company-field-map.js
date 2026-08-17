/**
 * CoStar Companies Data Export → GTM Airtable field mapping.
 */
export const GTM_COMPANY_TABLE =
  process.env.AIRTABLE_GTM_COSTAR_COMPANIES_TABLE || "Companies";

export const MAP_GTM_COMPANY = {
  company: "Company",
  companyOverview: "Company Overview",
  specialty: "Specialty",
  hqMarket: "HQ Market",
  hqCity: "HQ City",
  hqState: "HQ State",
  hqCountry: "HQ Country",
  website: "Website",
  employees: "Employees",
  locations: "Locations",
  managedProperties: "Managed Properties",
  ownedProperties: "Owned Properties",
  operatedProperties: "Operated Properties",
  leaseTransactions3Y: "Lease Transactions (3Y)",
  leaseTransactionsSf3Y: "Lease Transactions SF (3Y)",
  leaseListings: "Lease Listings",
  leaseListingsPortfolioSf: "Lease Listings Portfolio SF",
  leaseListingsAvailableSf: "Lease Listings Available SF",
  saleTransactions3Y: "Sale Transactions (3Y)",
  saleTransactionsSf3Y: "Sale Transactions SF (3Y)",
  saleTransactionsVolume3Y: "Sale Transactions Volume (3Y)",
  saleListings: "Sale Listings",
  saleListingsSf: "Sale Listings SF",
  outreachStatus: "Outreach Status",
  internalNotes: "Internal Notes",
  sourceFile: "Source File",
  companyDedupeKey: "Company Dedupe Key",
  calaHotels: "CALA Hotels",
  calaPropertyCount: "CALA Property Count",
  calaCountriesSummary: "CALA Countries",
  matchedOwnerName: "Matched Owner Name",
  calaMatchType: "CALA Match Type",
};

export const VAL_GTM_COMPANY_CALA_HOTELS = ["yes", "no", "unknown"];

export const VAL_GTM_COMPANY_OUTREACH_STATUS = [
  "not_contacted",
  "researching",
  "intro_sent",
  "responded",
  "meeting_scheduled",
  "passed",
  "parked",
];

export const COSTAR_COMPANY_HEADERS = [
  MAP_GTM_COMPANY.company,
  MAP_GTM_COMPANY.specialty,
  MAP_GTM_COMPANY.hqMarket,
  MAP_GTM_COMPANY.hqCity,
  MAP_GTM_COMPANY.hqState,
  MAP_GTM_COMPANY.hqCountry,
  MAP_GTM_COMPANY.website,
  MAP_GTM_COMPANY.employees,
  MAP_GTM_COMPANY.locations,
  MAP_GTM_COMPANY.managedProperties,
  MAP_GTM_COMPANY.ownedProperties,
  MAP_GTM_COMPANY.operatedProperties,
  MAP_GTM_COMPANY.leaseTransactions3Y,
  MAP_GTM_COMPANY.leaseTransactionsSf3Y,
  MAP_GTM_COMPANY.leaseListings,
  MAP_GTM_COMPANY.leaseListingsPortfolioSf,
  MAP_GTM_COMPANY.leaseListingsAvailableSf,
  MAP_GTM_COMPANY.saleTransactions3Y,
  MAP_GTM_COMPANY.saleTransactionsSf3Y,
  MAP_GTM_COMPANY.saleTransactionsVolume3Y,
  MAP_GTM_COMPANY.saleListings,
  MAP_GTM_COMPANY.saleListingsSf,
];
