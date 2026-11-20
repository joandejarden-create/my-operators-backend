/**
 * Expanded SEC company registry for Marriott/Hilton historical research V2.
 */

import { PILOT_SEC_COMPANIES, normalizeCik, listPilotCompanies } from "./cik-registry-v1.js";

export const SEC_EDGAR_CIK_REGISTRY_V2 = "sec-edgar-cik-registry-v2";

/** High-yield lodging REITs / owners / brand cos for Marriott & Hilton inventory. */
export const HISTORICAL_RESEARCH_COMPANIES_V2 = Object.freeze([
  ...PILOT_SEC_COMPANIES,
  {
    slug: "hilton-worldwide",
    legalName: "Hilton Worldwide Holdings Inc.",
    ticker: "HLT",
    cik: "0001585689",
    organizationType: "Brand / Franchisor",
    priority: 1,
    brandFamilies: ["HILTON"],
    note: "Global Hilton brand/franchisor; property-level yield often lower than REIT schedules.",
  },
  {
    slug: "diamondrock-hospitality",
    legalName: "DiamondRock Hospitality Company",
    ticker: "DRH",
    cik: "0001298946",
    organizationType: "REIT",
    priority: 1,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Primary V2 test case — historical hotel-level franchise expiration schedules (incl. 2022 10-K).",
  },
  {
    slug: "park-hotels-resorts",
    legalName: "Park Hotels & Resorts Inc.",
    ticker: "PK",
    cik: "0001617406",
    organizationType: "REIT",
    priority: 2,
    brandFamilies: ["HILTON", "MARRIOTT"],
    note: "Hilton spin / lodging REIT — property schedules.",
  },
  {
    slug: "rlj-lodging-trust",
    legalName: "RLJ Lodging Trust",
    ticker: "RLJ",
    cik: "0001511337",
    organizationType: "REIT",
    priority: 2,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Lodging REIT — Marriott/Hilton heavy portfolios historically.",
  },
  {
    slug: "apple-hospitality-reit",
    legalName: "Apple Hospitality REIT, Inc.",
    ticker: "APLE",
    cik: "0001418121",
    organizationType: "REIT",
    priority: 2,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Large select-service Marriott/Hilton franchise portfolio.",
  },
  {
    slug: "pebblebrook-hotel-trust",
    legalName: "Pebblebrook Hotel Trust",
    ticker: "PEB",
    cik: "0001474098",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Upper-upscale lodging REIT.",
  },
  {
    slug: "chatham-lodging-trust",
    legalName: "Chatham Lodging Trust",
    ticker: "CLDT",
    cik: "0001476045",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Select-service lodging REIT.",
  },
  {
    slug: "xenia-hotels",
    legalName: "Xenia Hotels & Resorts, Inc.",
    ticker: "XHR",
    cik: "0001616000",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Lodging REIT — Marriott/Hilton brands common.",
  },
  {
    slug: "summit-hotel-properties",
    legalName: "Summit Hotel Properties, Inc.",
    ticker: "INN",
    cik: "0001497645",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON"],
    note: "Select-service lodging REIT.",
  },
]);

export function listHistoricalResearchCompanies(opts = {}) {
  const maxPriority = opts.maxPriority != null ? Number(opts.maxPriority) : 99;
  const brand = opts.brandFamily ? String(opts.brandFamily).toUpperCase() : null;
  let rows = HISTORICAL_RESEARCH_COMPANIES_V2.filter((c) => (c.priority || 99) <= maxPriority);
  if (brand) {
    rows = rows.filter(
      (c) =>
        !c.brandFamilies ||
        c.brandFamilies.includes(brand) ||
        c.slug.includes("marriott") ||
        c.slug.includes("hilton"),
    );
  }
  if (opts.slugs?.length) {
    const set = new Set(opts.slugs.map((s) => String(s).toLowerCase()));
    rows = rows.filter((c) => set.has(c.slug));
  }
  return rows.sort((a, b) => (a.priority || 99) - (b.priority || 99));
}

export { normalizeCik, listPilotCompanies, PILOT_SEC_COMPANIES };
