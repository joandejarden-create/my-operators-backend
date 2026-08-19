/**
 * Demand-based ranking for official company adapters.
 * UNRESOLVED_CENSUS_DEMAND × EXPECTED_YIELD ÷ COST
 */
import { MAP_MASTER } from "./master-census-enrichment-v1.js";
import { MAP_BRAND } from "./master-brand-portfolio-validation-v1.js";
import {
  buildCanonicalBrandDictionary,
  familyFromOfficialUrl,
  lookupCanonicalBrand,
} from "./census-brand-canonical-dictionary.js";
import { canonicalizeParentCompany } from "./census-parent-company-normalization.js";
import { hostFromUrl } from "./official-domain-crawler-v1.js";

export const COMPANY_ADAPTER_DEMAND_VERSION = "company-adapter-demand-v1";

/** Relative cost divisors (lower cost → higher score). Apify approved = cheapest harvest. */
export const COMPANY_COST_DIVISOR = Object.freeze({
  IHG: 1,
  Marriott: 4,
  Hilton: 4,
  Accor: 3,
  Wyndham: 2.5,
  Choice: 2.5,
  Hyatt: 3,
  "Best Western": 2,
  Radisson: 2.5,
  Meliá: 3,
  Barceló: 3,
  RIU: 3,
  Iberostar: 3,
  Palladium: 3,
  Minor: 3,
  "Four Seasons": 5,
  Rosewood: 5,
  "Mandarin Oriental": 5,
  Aman: 5,
  Kerzner: 4,
  "Grupo Posadas": 2.5,
  "Bahia Principe": 3,
  Gaviota: 3,
  Cubanacán: 3,
  "Gran Caribe": 3,
});

/** Expected yield when adapter succeeds (0–1). IHG Apify approved = highest. */
export const COMPANY_EXPECTED_YIELD = Object.freeze({
  IHG: 0.85,
  Marriott: 0.35,
  Hilton: 0.25,
  Accor: 0.55,
  Wyndham: 0.6,
  Choice: 0.5,
  Hyatt: 0.45,
  "Best Western": 0.4,
  Radisson: 0.45,
  Meliá: 0.5,
  Barceló: 0.45,
  RIU: 0.45,
  Iberostar: 0.45,
  Palladium: 0.4,
  Minor: 0.35,
  "Four Seasons": 0.2,
  Rosewood: 0.15,
  "Mandarin Oriental": 0.15,
  Aman: 0.1,
  Kerzner: 0.2,
  "Grupo Posadas": 0.45,
  "Bahia Principe": 0.4,
  Gaviota: 0.35,
  Cubanacán: 0.3,
  "Gran Caribe": 0.3,
});

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function companyFromCandidateBrand(candidate, dictionary) {
  const c = String(candidate || "").trim();
  if (!c) return null;
  const hit = lookupCanonicalBrand(c, dictionary);
  const parent =
    hit?.entry?.parent_company ||
    hit?.entry?.brand_family ||
    hit?.parent ||
    null;
  if (parent) return canonicalizeParentCompany(parent);
  const resolved = dictionary?.brands?.find?.(
    (b) => String(b.canonical || b.name || "").toLowerCase() === c.toLowerCase()
  );
  if (resolved?.parent) return canonicalizeParentCompany(resolved.parent);
  return null;
}

function companyFromFamilyAlias(fields) {
  const fam =
    fields[MAP_MASTER.brandFamily] ||
    fields[MAP_MASTER.familySourceFamily] ||
    fields[MAP_BRAND.brandFamily] ||
    fields[MAP_BRAND.familySourceFamily];
  if (isBlank(fam)) return null;
  return canonicalizeParentCompany(String(fam).trim());
}

/**
 * @param {object[]} censusRecords
 * @param {object} [opts]
 * @returns {Array<{ company: string, unresolved: number, website_domain: number, candidate_brand: number, family_alias: number, demand_score: number }>}
 */
export function rankCompanyAdapterDemand(censusRecords = [], opts = {}) {
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  /** @type {Map<string, { unresolved: number, website_domain: number, candidate_brand: number, family_alias: number }>} */
  const tallies = new Map();

  const bump = (company, field) => {
    const key = canonicalizeParentCompany(company) || company;
    if (!key) return;
    const row = tallies.get(key) || {
      unresolved: 0,
      website_domain: 0,
      candidate_brand: 0,
      family_alias: 0,
    };
    row[field] += 1;
    tallies.set(key, row);
  };

  for (const rec of censusRecords) {
    const f = rec.fields || {};
    if (!isBlank(f[MAP_MASTER.currentBrand])) continue;

    const website = f[MAP_MASTER.officialUrl] || f[MAP_BRAND.officialUrl];
    const domainFamily = website ? familyFromOfficialUrl(website) : null;
    const candidateCo = companyFromCandidateBrand(f[MAP_BRAND.candidateBrand], dictionary);
    const aliasCo = companyFromFamilyAlias(f);

    const companies = new Set(
      [domainFamily, candidateCo, aliasCo].filter(Boolean).map((c) => canonicalizeParentCompany(c))
    );
    if (!companies.size) {
      bump("Unknown", "unresolved");
      continue;
    }
    for (const co of companies) {
      bump(co, "unresolved");
      if (domainFamily && canonicalizeParentCompany(domainFamily) === co) {
        bump(co, "website_domain");
      }
      if (candidateCo && canonicalizeParentCompany(candidateCo) === co) {
        bump(co, "candidate_brand");
      }
      if (aliasCo && canonicalizeParentCompany(aliasCo) === co) {
        bump(co, "family_alias");
      }
    }
  }

  const ranked = [...tallies.entries()]
    .filter(([co]) => co !== "Unknown")
    .map(([company, t]) => {
      const unresolved =
        t.unresolved + t.website_domain + t.candidate_brand + t.family_alias;
      const yieldFactor = COMPANY_EXPECTED_YIELD[company] ?? 0.35;
      const costDiv = COMPANY_COST_DIVISOR[company] ?? 3;
      const demand_score = Number(
        ((unresolved * yieldFactor) / costDiv).toFixed(4)
      );
      return {
        company,
        unresolved: t.unresolved,
        website_domain: t.website_domain,
        candidate_brand: t.candidate_brand,
        family_alias: t.family_alias,
        UNRESOLVED_CENSUS_DEMAND: unresolved,
        EXPECTED_YIELD: yieldFactor,
        COST_DIVISOR: costDiv,
        demand_score,
      };
    })
    .sort((a, b) => b.demand_score - a.demand_score);

  return ranked;
}

export function topUnresolvedBrandCompanyDemand(censusRecords, limit = 20, opts = {}) {
  return rankCompanyAdapterDemand(censusRecords, opts).slice(0, limit);
}

export function companyNamesFromDemandRank(ranked = [], max = 3) {
  return ranked.slice(0, max).map((r) => r.company);
}

export function filterCompaniesByDemand(allCompanies, rankedNames) {
  const order = new Map(rankedNames.map((n, i) => [canonicalizeParentCompany(n), i]));
  return [...allCompanies].sort((a, b) => {
    const ai = order.get(canonicalizeParentCompany(a.company)) ?? 999;
    const bi = order.get(canonicalizeParentCompany(b.company)) ?? 999;
    return ai - bi;
  });
}

void hostFromUrl;
