/**
 * Live official brand-directory acquisition (Mode B).
 * Reuses existing CALA discovery adapters + generic official-domain crawler.
 * Validation still goes through scorePortfolioToCensus / mapping repair.
 */
import {
  ensureMarriottCalaCountrySitemapCache,
  iterateMarriottDirectoryRows,
} from "./census-autopilot-marriott-discovery-adapter.js";
import {
  ensureHiltonCalaDirectoryCache,
  iterateHiltonDirectoryRows,
} from "./census-autopilot-hilton-cala-discovery-adapter.js";
import {
  ensureIhgCalaDestinationCache,
  iterateIhgDirectoryRows,
} from "./census-autopilot-ihg-cala-discovery-adapter.js";
import {
  ensureAccorCalaDirectoryCache,
  iterateAccorDirectoryRows,
} from "./census-autopilot-accor-cala-discovery-adapter.js";
import {
  ensureWyndhamCalaDirectoryCache,
  iterateWyndhamDirectoryRows,
} from "./census-autopilot-wyndham-cala-discovery-adapter.js";
import {
  ensureChoiceCalaRegionalCache,
  iterateChoiceDirectoryRows,
} from "./census-autopilot-choice-cala-discovery-adapter.js";
import {
  scorePortfolioToCensus,
  buildPortfolioBrandPatch,
  MAP_BRAND,
} from "./master-brand-portfolio-validation-v1.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import {
  resolveBrandMappingAlias,
  buildBrandMappingRepairPatch,
} from "./brand-mapping-gap-repair-v1.js";
import {
  LIVE_OFFICIAL_COMPANIES,
  crawlOfficialDomain,
  isForbiddenHost,
} from "./official-domain-crawler-v1.js";
import {
  loadSourceRegistry,
  saveSourceRegistry,
  upsertSource,
  bumpSourceStats,
  markSourceState,
  sourceIsRunnable,
  SOURCE_DISCOVERY_STATE,
} from "./source-acquisition-registry-v1.js";
import { CALA_DISCOVERY_PRIORITY_COUNTRIES } from "./census-autopilot-cala-discovery-shared.js";
import { normalizeCountry,
  normalizeText,
  websiteHost,
} from "../independent-census/match-current-census.js";
import {
  rankCompanyAdapterDemand,
  filterCompaniesByDemand,
  companyNamesFromDemandRank,
} from "./company-adapter-demand-v1.js";

export const LIVE_DIRECTORY_ACQUISITION_VERSION =
  "live-official-directory-acquisition-v1";

function normKey(s) {
  return normalizeText(String(s || ""))
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function rowToPortfolio(row, company) {
  const url = row.propertyUrl || row.website || row.url || null;
  const city = row.city || null;
  const country = row.country || null;
  const name = row.name || row.inferredHotelName || row.canonicalName || null;
  return {
    company,
    brand: row.brand || row.affiliation || row.brandName || company,
    name,
    url,
    city,
    country,
    state: row.state || row.stateRegion || null,
    address: row.addressLine1 || row.address || row.addressText || null,
    postal: row.postalCode || row.postal || null,
    phone: row.phone || row.telephone || null,
    property_code:
      row.marshaCode ||
      row.ctyhocn ||
      row.propertyId ||
      row.mnemonic ||
      null,
    host: websiteHost(url),
    name_key: normKey(name),
    city_key: normKey(city),
    country_norm: normalizeCountry(country),
  };
}

async function loadAdapterRows(cfg, opts = {}) {
  const countries = opts.countries || CALA_DISCOVERY_PRIORITY_COUNTRIES;
  const delayMs = opts.delayMs ?? 200;
  if (cfg.adapter === "marriott_sitemap") {
    const cache = await ensureMarriottCalaCountrySitemapCache({ countries, delayMs });
    return iterateMarriottDirectoryRows(cache).map((r) => rowToPortfolio(r, "Marriott"));
  }
  if (cfg.adapter === "hilton_locations") {
    const cache = await ensureHiltonCalaDirectoryCache({ countries, delayMs });
    return iterateHiltonDirectoryRows(cache).map((r) => rowToPortfolio(r, "Hilton"));
  }
  if (cfg.adapter === "ihg_destination") {
    const cache = await ensureIhgCalaDestinationCache({ countries, delayMs });
    return iterateIhgDirectoryRows(cache).map((r) => rowToPortfolio(r, "IHG"));
  }
  if (cfg.adapter === "accor_catalog") {
    const cache = await ensureAccorCalaDirectoryCache({ countries, delayMs });
    return iterateAccorDirectoryRows(cache).map((r) => rowToPortfolio(r, "Accor"));
  }
  if (cfg.adapter === "wyndham_sitemap") {
    const cache = await ensureWyndhamCalaDirectoryCache({
      countries,
      delayMs,
      forceLiveExtract: opts.forceLiveExtract === true,
    });
    return iterateWyndhamDirectoryRows(cache).map((r) => rowToPortfolio(r, "Wyndham"));
  }
  if (cfg.adapter === "choice_regional") {
    const cache = await ensureChoiceCalaRegionalCache({ countries, delayMs });
    return iterateChoiceDirectoryRows(cache).map((r) => rowToPortfolio(r, "Choice"));
  }
  if (cfg.adapter === "generic_sitemap") {
    const crawled = await crawlOfficialDomain(cfg, {
      maxSitemapFiles: opts.maxSitemapFiles || 8,
      maxPropertyUrls: opts.maxPropertyUrls || 250,
      maxPageFetches: opts.maxPageFetches || 24,
      delayMs: opts.delayMs ?? 400,
      fetchFn: opts.fetchFn,
      sleepFn: opts.sleepFn,
    });
    return {
      rows: (crawled.properties || []).filter((p) => p.url && !isForbiddenHost(p.url)),
      crawl: crawled,
    };
  }
  return [];
}

function normalizeLoaded(loaded) {
  if (Array.isArray(loaded)) return { rows: loaded, crawl: null };
  return { rows: loaded.rows || [], crawl: loaded.crawl || null };
}

/**
 * Match live directory rows onto Census and build HIGH brand patches.
 * Candidate Brand Text is a research hint only.
 */
export function matchLiveDirectoryToCensus(rows, censusRecords, opts = {}) {
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  const proposals = [];
  const mappingGaps = [];
  let high = 0;
  let attempted = 0;

  const missingBrand = (censusRecords || []).filter((r) => {
    const v = r.fields?.[MAP_BRAND.currentBrand];
    return v == null || String(v).trim() === "";
  });

  for (const rec of missingBrand) {
    const fields = rec.fields || {};
    let best = null;
    for (const row of rows) {
      if (!row?.name && !row?.url) continue;
      const scored = scorePortfolioToCensus(row, fields);
      if (!best || scored.score > best.scored.score) {
        best = { row, scored };
      }
    }
    if (!best) continue;
    attempted += 1;
    if (best.scored.confidence !== "high") continue;

    const mapped = resolveBrandMappingAlias(best.row.brand, {
      dictionary,
      propertyName: best.row.name || fields[MAP_BRAND.propertyName],
      sourceUrl: best.row.url,
    });
    const portfolio = { ...best.row, brand: mapped.ok ? mapped.canonical : best.row.brand };
    let built = buildPortfolioBrandPatch(fields, portfolio, dictionary);
    if (!built.ok && built.reason === "BRAND_MAPPING_GAP" && mapped.ok) {
      built = buildBrandMappingRepairPatch(fields, {
        dictionary,
        portfolioBrand: mapped.canonical,
        allowWriteCurrentBrand: true,
        identityHigh: true,
      });
      if (built.ok) {
        built.patch = {
          ...built.patch,
          [MAP_BRAND.currentBrand]: mapped.canonical,
        };
      }
    }
    if (!built.ok && built.reason === "BRAND_MAPPING_GAP") {
      mappingGaps.push({ id: rec.id, brand: best.row.brand, company: best.row.company });
      continue;
    }
    if (!built.ok) continue;
    high += 1;
    proposals.push({
      id: rec.id,
      fields: built.patch,
      company: best.row.company,
      source_url: best.row.url,
    });
  }

  return { proposals, mappingGaps, high, attempted };
}

export async function runLiveOfficialDirectoryAcquisition(opts = {}) {
  const log = opts.log || (() => {});
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  const records = opts.censusRecords || [];
  const registry = opts.registry || loadSourceRegistry();
  const maxCompanies = Number(opts.maxCompanies || 3);
  const demandRanked = opts.demandRanked !== false;
  const ranked = demandRanked
    ? rankCompanyAdapterDemand(records, { dictionary })
    : [];
  const demandCompanies = companyNamesFromDemandRank(ranked, maxCompanies * 2);
  let companies = (opts.companies || LIVE_OFFICIAL_COMPANIES).filter((c) => {
    const entry = registry.sources[c.id];
    return sourceIsRunnable(entry);
  });
  if (demandRanked && demandCompanies.length) {
    companies = filterCompaniesByDemand(companies, demandCompanies);
  }

  const proposals = [];
  const mappingGaps = [];
  const crawled = [];
  let requests = 0;
  let domainsDiscovered = 0;
  let domainsCrawled = 0;

  for (const cfg of companies.slice(0, maxCompanies)) {
    const entry = upsertSource(registry, {
      SOURCE_ID: cfg.id,
      DOMAIN: cfg.domain,
      COMPANY: cfg.company,
      SOURCE_TYPE: cfg.adapter,
      DISCOVERY_METHOD: cfg.adapter,
    });
    markSourceState(entry, SOURCE_DISCOVERY_STATE.DISCOVERING);
    log(`[live-dir] ${cfg.company} via ${cfg.adapter}…`);
    try {
      const loaded = normalizeLoaded(await loadAdapterRows(cfg, opts));
      domainsCrawled += 1;
      if (loaded.crawl) {
        requests += Number(loaded.crawl.requests || 0);
        domainsDiscovered += 1;
        bumpSourceStats(entry, {
          REQUESTS: loaded.crawl.requests || 0,
          PAGES_DISCOVERED: loaded.crawl.pages_discovered || 0,
          PROPERTIES_EXTRACTED: loaded.rows.length,
          ERRORS: (loaded.crawl.errors || []).length,
        });
        if (loaded.crawl.state === SOURCE_DISCOVERY_STATE.TEMP_BLOCKED) {
          markSourceState(entry, SOURCE_DISCOVERY_STATE.TEMP_BLOCKED);
        }
      } else {
        bumpSourceStats(entry, {
          REQUESTS: 1,
          PROPERTIES_EXTRACTED: loaded.rows.length,
        });
      }
      crawled.push({
        company: cfg.company,
        adapter: cfg.adapter,
        rows: loaded.rows.length,
      });
      const matched = matchLiveDirectoryToCensus(loaded.rows, records, { dictionary });
      proposals.push(...matched.proposals);
      mappingGaps.push(...matched.mappingGaps);
      bumpSourceStats(entry, {
        HIGH_MATCHES: matched.high,
        FIELDS_WRITTEN: matched.proposals.length,
      });
      if (matched.high === 0 && loaded.rows.length > 0) {
        markSourceState(entry, SOURCE_DISCOVERY_STATE.ACTIVE_LOW_YIELD, {
          PLATEAU_STATUS: "no_new_high_matches",
        });
      } else if (matched.high === 0 && loaded.rows.length === 0) {
        markSourceState(entry, SOURCE_DISCOVERY_STATE.RETRY_LATER);
      } else {
        markSourceState(entry, SOURCE_DISCOVERY_STATE.ACTIVE_HIGH_YIELD);
      }
    } catch (err) {
      bumpSourceStats(entry, { ERRORS: 1, REQUESTS: 1 });
      markSourceState(entry, SOURCE_DISCOVERY_STATE.TEMP_BLOCKED, {
        LAST_CRAWLED: new Date().toISOString(),
      });
      log(`[live-dir] ${cfg.company} error: ${String(err?.message || err).slice(0, 160)}`);
    }
  }

  saveSourceRegistry(registry);
  const remaining = companies.length > maxCompanies;
  return {
    ok: true,
    version: LIVE_DIRECTORY_ACQUISITION_VERSION,
    LIVE_OFFICIAL_DOMAINS_DISCOVERED: domainsDiscovered,
    LIVE_OFFICIAL_DOMAINS_CRAWLED: domainsCrawled,
    NEW_SOURCE_REGISTRY_ENTRIES: Object.keys(registry.sources).length,
    proposals,
    mapping_gaps: mappingGaps,
    crawled,
    requests,
    exhausted: !remaining && companies.every((c) => !sourceIsRunnable(registry.sources[c.id])),
    registry,
  };
}
