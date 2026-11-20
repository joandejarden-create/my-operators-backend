/**
 * SEC EDGAR V2.4 — universal company registry (not Marriott/Hilton-restricted).
 * ~25–40 high-priority registrants for the first expanded research wave.
 * CIKs validated via live ticker resolve before research; aliases preserved.
 */

import {
  HISTORICAL_RESEARCH_COMPANIES_V2,
  listHistoricalResearchCompanies,
  normalizeCik,
} from "./cik-registry-v2.js";
import { PILOT_SEC_COMPANIES } from "./cik-registry-v1.js";

export const SEC_EDGAR_CIK_REGISTRY_V24 = "sec-edgar-cik-registry-v24";
export const UNIVERSAL_WAVE_ID = "sec-edgar-universal-wave-v24";

/**
 * Incremental V2.4 additions beyond V2 historical list.
 * brandFamilies are research hints only — never a research restriction.
 */
export const UNIVERSAL_EXPANSION_COMPANIES_V24 = Object.freeze([
  // Global brand / franchisor companies
  {
    slug: "hyatt-hotels",
    legalName: "Hyatt Hotels Corporation",
    ticker: "H",
    cik: "0001468174",
    organizationType: "Brand / Franchisor",
    priority: 1,
    brandFamilies: ["HYATT"],
    cohorts: ["UNIVERSAL_V24", "BRAND"],
    aliases: ["Hyatt Hotels Corp"],
    note: "Global Hyatt brand/franchisor + manager disclosures.",
  },
  {
    slug: "choice-hotels",
    legalName: "Choice Hotels International, Inc.",
    ticker: "CHH",
    cik: "0001046311",
    organizationType: "Brand / Franchisor",
    priority: 1,
    brandFamilies: ["CHOICE"],
    cohorts: ["UNIVERSAL_V24", "BRAND"],
    aliases: ["Choice Hotels International"],
    note: "Franchise-heavy brand company — property schedules less common; franchise exhibits priority.",
  },
  {
    slug: "wyndham-hotels-resorts",
    legalName: "Wyndham Hotels & Resorts, Inc.",
    ticker: "WH",
    cik: "0001722684",
    organizationType: "Brand / Franchisor",
    priority: 1,
    brandFamilies: ["WYNDHAM"],
    cohorts: ["UNIVERSAL_V24", "BRAND"],
    aliases: ["Wyndham Hotels and Resorts"],
    note: "Franchise brand company post-spin.",
  },
  {
    slug: "marriott-vacations",
    legalName: "Marriott Vacations Worldwide Corporation",
    ticker: "VAC",
    cik: "0001524358",
    organizationType: "Brand / Operator",
    priority: 2,
    brandFamilies: ["MARRIOTT"],
    cohorts: ["UNIVERSAL_V24", "BRAND"],
    aliases: ["Marriott Vacations Worldwide"],
    note: "Timeshare / vacation ownership — selective hotel-adjacent disclosures.",
  },

  // Lodging REITs / owners
  {
    slug: "ryman-hospitality",
    legalName: "Ryman Hospitality Properties, Inc.",
    ticker: "RHP",
    cik: "0001040829",
    organizationType: "REIT",
    priority: 1,
    brandFamilies: ["OTHER"],
    cohorts: ["UNIVERSAL_V24", "REIT"],
    aliases: ["Gaylord Entertainment", "Ryman Hospitality Properties"],
    note: "Large-group hotel REIT — property-level disclosures.",
  },
  {
    slug: "service-properties-trust",
    legalName: "Service Properties Trust",
    ticker: "SVC",
    cik: "0000945394",
    organizationType: "REIT",
    priority: 2,
    brandFamilies: ["MARRIOTT", "HILTON", "IHG", "OTHER"],
    cohorts: ["UNIVERSAL_V24", "REIT"],
    aliases: ["Hospitality Properties Trust", "HPT", "Service Properties Trust"],
    note: "Historical HPT hotel portfolio + current SVC; strong schedule history. CIK live-validated.",
  },
  {
    slug: "braemar-hotels",
    legalName: "Braemar Hotels & Resorts Inc.",
    ticker: "BHR",
    cik: "0001574085",
    organizationType: "REIT",
    priority: 2,
    brandFamilies: ["MARRIOTT", "HILTON", "HYATT"],
    cohorts: ["UNIVERSAL_V24", "REIT"],
    aliases: ["Ashford Hospitality Prime", "Braemar Hotels & Resorts"],
    note: "Luxury lodging REIT (Ashford Prime successor).",
  },
  {
    slug: "sotherly-hotels",
    legalName: "Sotherly Hotels Inc.",
    ticker: "SOHO",
    cik: null,
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["OTHER", "HILTON", "MARRIOTT"],
    cohorts: ["UNIVERSAL_V24", "REIT"],
    aliases: ["Sotherly Hotels"],
    note: "Small lodging REIT — CIK resolved live only (never share Ashford CIK).",
    cikVerifyRequired: true,
  },
  {
    slug: "condor-hospitality",
    legalName: "Condor Hospitality Trust, Inc.",
    ticker: "CDOR",
    cik: "0000923215",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["OTHER", "CHOICE", "IHG"],
    cohorts: ["UNIVERSAL_V24", "REIT"],
    aliases: ["Condor Hospitality Trust", "Supertel Hospitality"],
    note: "Historical select-service REIT (Supertel lineage).",
  },
  {
    slug: "hersha-hospitality",
    legalName: "Hersha Hospitality Trust",
    ticker: "HT",
    cik: "0001063344",
    organizationType: "REIT",
    priority: 2,
    brandFamilies: ["MARRIOTT", "HILTON", "HYATT"],
    cohorts: ["UNIVERSAL_V24", "REIT", "HISTORICAL"],
    aliases: ["Hersha Hospitality Trust"],
    note: "Historical lodging REIT — may be delisted/acquired; retain for historical exhibits.",
  },

  // Gaming / resort owners with hotel disclosures
  {
    slug: "mgm-resorts",
    legalName: "MGM Resorts International",
    ticker: "MGM",
    cik: "0000789570",
    organizationType: "Owner / Operator",
    priority: 2,
    brandFamilies: ["OTHER"],
    cohorts: ["UNIVERSAL_V24", "RESORT"],
    aliases: ["MGM Mirage", "MGM Resorts"],
    note: "Resort/casino hotel owner-operator — property schedules + EX-10.",
  },
  {
    slug: "las-vegas-sands",
    legalName: "Las Vegas Sands Corp.",
    ticker: "LVS",
    cik: "0001300514",
    organizationType: "Owner / Operator",
    priority: 2,
    brandFamilies: ["OTHER"],
    cohorts: ["UNIVERSAL_V24", "RESORT"],
    aliases: ["Las Vegas Sands"],
    note: "Integrated resort owner — hotel property disclosures.",
  },
  {
    slug: "wynn-resorts",
    legalName: "Wynn Resorts, Limited",
    ticker: "WYNN",
    cik: "0001174922",
    organizationType: "Owner / Operator",
    priority: 2,
    brandFamilies: ["OTHER"],
    cohorts: ["UNIVERSAL_V24", "RESORT"],
    aliases: ["Wynn Resorts"],
    note: "Resort hotel owner-operator.",
  },
  {
    slug: "caesars-entertainment",
    legalName: "Caesars Entertainment, Inc.",
    ticker: "CZR",
    cik: "0001590895",
    organizationType: "Owner / Operator",
    priority: 2,
    brandFamilies: ["OTHER"],
    cohorts: ["UNIVERSAL_V24", "RESORT"],
    aliases: ["Caesars Entertainment", "Eldorado Resorts"],
    note: "Gaming/hotel portfolio — management and property exhibits.",
  },
  {
    slug: "vail-resorts",
    legalName: "Vail Resorts, Inc.",
    ticker: "MTN",
    cik: "0000812011",
    organizationType: "Owner / Operator",
    priority: 3,
    brandFamilies: ["OTHER"],
    cohorts: ["UNIVERSAL_V24", "RESORT"],
    aliases: ["Vail Resorts"],
    note: "Mountain resort lodging portfolio.",
  },

  // Historical / acquired lodging companies (research aliases)
  {
    slug: "chesapeake-lodging-trust",
    legalName: "Chesapeake Lodging Trust",
    ticker: null,
    cik: "0001476641",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON", "HYATT"],
    cohorts: ["UNIVERSAL_V24", "HISTORICAL"],
    aliases: ["Chesapeake Lodging Trust"],
    note: "Acquired by Park Hotels — historical property schedules retained.",
    historicalOnly: true,
  },
  {
    slug: "lasalle-hotel-properties",
    legalName: "LaSalle Hotel Properties",
    ticker: null,
    cik: "0001043210",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON", "HYATT", "OTHER"],
    cohorts: ["UNIVERSAL_V24", "HISTORICAL"],
    aliases: ["LaSalle Hotel Properties"],
    note: "Acquired by Pebblebrook — historical franchise/management exhibits.",
    historicalOnly: true,
  },
  {
    slug: "felcor-lodging",
    legalName: "FelCor Lodging Trust Incorporated",
    ticker: null,
    cik: "0000925590",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON", "OTHER"],
    cohorts: ["UNIVERSAL_V24", "HISTORICAL"],
    aliases: ["FelCor Lodging Trust", "FelCor"],
    note: "Merged into RLJ — historical property disclosures.",
    historicalOnly: true,
  },
  {
    slug: "strategic-hotels-resorts",
    legalName: "Strategic Hotels & Resorts, Inc.",
    ticker: null,
    cik: "0001031295",
    organizationType: "REIT",
    priority: 3,
    brandFamilies: ["MARRIOTT", "HILTON", "HYATT", "OTHER"],
    cohorts: ["UNIVERSAL_V24", "HISTORICAL"],
    aliases: ["Strategic Hotels & Resorts", "Strategic Hotel Capital"],
    note: "Taken private — historical luxury hotel schedules.",
    historicalOnly: true,
  },
]);

/** Normalize V2 pebblebrook slug alias. */
const V2_ALIASES = Object.freeze({
  "pesa-hospitality": {
    slug: "pebblebrook-hotel-trust",
    aliases: ["Pebblebrook Hotel Trust", "pesa-hospitality"],
  },
});

function normalizeV2Row(c) {
  const aliasFix = V2_ALIASES[c.slug];
  const slug = aliasFix?.slug || c.slug;
  return {
    ...c,
    slug,
    cohorts: ["CORE_LEGACY", "UNIVERSAL_V24", ...(c.organizationType === "REIT" ? ["REIT"] : ["BRAND"])],
    aliases: [...new Set([c.legalName, ...(aliasFix?.aliases || []), ...(c.aliases || [])])],
    brandFamilies: c.brandFamilies || [],
  };
}

/**
 * Deduped universal registry: pilot + V2 historical + V2.4 expansion.
 */
export function buildUniversalCompanyRegistryV24() {
  const bySlug = new Map();
  const byCik = new Map();
  const collisions = [];

  function add(row) {
    const slug = String(row.slug || "").toLowerCase();
    const cik = normalizeCik(row.cik);
    if (!slug) return;
    if (bySlug.has(slug)) {
      collisions.push({ type: "duplicate_slug", slug });
      return;
    }
    if (cik && byCik.has(cik) && !row.cikVerifyRequired) {
      const prev = byCik.get(cik);
      // Allow intentional verify-required rows to defer until live resolve
      if (prev.slug !== slug) {
        collisions.push({ type: "duplicate_cik", cik, slugs: [prev.slug, slug] });
        return;
      }
    }
    const normalized = {
      ...row,
      slug,
      cik,
      priority: row.priority || 99,
      cohorts: row.cohorts || ["UNIVERSAL_V24"],
      aliases: row.aliases || [row.legalName],
    };
    bySlug.set(slug, normalized);
    if (cik && !row.cikVerifyRequired) byCik.set(cik, normalized);
  }

  for (const c of PILOT_SEC_COMPANIES) add(normalizeV2Row(c));
  for (const c of HISTORICAL_RESEARCH_COMPANIES_V2) {
    // Skip pilot duplicates already added
    if (bySlug.has(c.slug) || (V2_ALIASES[c.slug] && bySlug.has(V2_ALIASES[c.slug].slug))) {
      continue;
    }
    // Skip exact duplicates from spread of PILOT inside V2
    if (PILOT_SEC_COMPANIES.some((p) => p.slug === c.slug)) continue;
    add(normalizeV2Row(c));
  }
  for (const c of UNIVERSAL_EXPANSION_COMPANIES_V24) add(c);

  const companies = [...bySlug.values()].sort(
    (a, b) => (a.priority || 99) - (b.priority || 99) || a.slug.localeCompare(b.slug),
  );

  return {
    version: SEC_EDGAR_CIK_REGISTRY_V24,
    waveId: UNIVERSAL_WAVE_ID,
    companyCount: companies.length,
    companies,
    collisions,
    guarantees: [
      "Research is not restricted to Marriott or Hilton brand families",
      "Duplicate slug/CIK registrations are rejected at registry build",
      "Registrant is never auto-classified as legal owner or manager",
      "CIK verify-required rows must pass live ticker resolve before inventory",
    ],
  };
}

export function listUniversalCompaniesV24(opts = {}) {
  const reg = buildUniversalCompanyRegistryV24();
  let rows = reg.companies;
  if (opts.maxPriority != null) {
    rows = rows.filter((c) => (c.priority || 99) <= Number(opts.maxPriority));
  }
  if (opts.cohort) {
    rows = rows.filter((c) => (c.cohorts || []).includes(opts.cohort));
  }
  if (opts.slugs?.length) {
    const set = new Set(opts.slugs.map((s) => String(s).toLowerCase()));
    rows = rows.filter((c) => set.has(c.slug));
  }
  if (opts.excludeHistoricalOnly) {
    rows = rows.filter((c) => !c.historicalOnly);
  }
  return rows;
}

/**
 * Live-validate tickers / CIKs; drop or correct collisions.
 */
export async function validateUniversalRegistryLive(client, opts = {}) {
  const companies = listUniversalCompaniesV24(opts);
  const validated = [];
  const failed = [];
  const corrected = [];

  for (const c of companies) {
    if (!c.ticker) {
      // Historical-only: keep static CIK if present
      if (c.cik) {
        validated.push({ ...c, cikSource: "static_historical", liveValidated: false });
      } else {
        failed.push({ slug: c.slug, error: "NO_TICKER_NO_CIK" });
      }
      continue;
    }
    try {
      const live = await client.resolveCikByTicker(c.ticker);
      if (!live?.cik) {
        failed.push({ slug: c.slug, ticker: c.ticker, error: "TICKER_NOT_FOUND" });
        continue;
      }
      const liveCik = normalizeCik(live.cik);
      if (c.cik && liveCik !== normalizeCik(c.cik)) {
        corrected.push({
          slug: c.slug,
          ticker: c.ticker,
          registryCik: c.cik,
          liveCik,
          liveTitle: live.title,
        });
      }
      validated.push({
        ...c,
        cik: liveCik,
        liveTitle: live.title,
        cikSource: "company_tickers",
        liveValidated: true,
      });
    } catch (err) {
      failed.push({ slug: c.slug, ticker: c.ticker, error: String(err.message || err) });
    }
  }

  // Drop CIK collisions after live resolve (prefer lower priority number, then slug)
  const byCik = new Map();
  const deduped = [];
  const cikCollisions = [];
  const sorted = [...validated].sort(
    (a, b) => (a.priority || 99) - (b.priority || 99) || a.slug.localeCompare(b.slug),
  );
  for (const c of sorted) {
    if (!c.cik) {
      deduped.push(c);
      continue;
    }
    if (byCik.has(c.cik)) {
      cikCollisions.push({ cik: c.cik, kept: byCik.get(c.cik).slug, dropped: c.slug });
      continue;
    }
    byCik.set(c.cik, c);
    deduped.push(c);
  }

  return {
    version: SEC_EDGAR_CIK_REGISTRY_V24,
    waveId: UNIVERSAL_WAVE_ID,
    inputCount: companies.length,
    validatedCount: deduped.length,
    failed,
    corrected,
    cikCollisions,
    companies: deduped,
  };
}

export { normalizeCik, listHistoricalResearchCompanies, HISTORICAL_RESEARCH_COMPANIES_V2 };
