/**
 * Merge Brand Matrix / entitlements website with Discoverability pilot fixture.
 * Never fabricates URLs — only governed Brand Basics or explicit fixture governance.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  resolveOwnedDomainsFromBrandRow,
  normalizeOwnedDomain,
} from "./owned-domain-resolution.js";
import { loadLatestPhase3c2Report } from "./phase3c2-orchestrator.js";
import { DATA_STATE } from "./discoverability-data-states.js";
import { PUBLIC_CONTENT_STATE } from "./discoverability-phase3c2.js";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
} from "./brand-ai-showcase-companies.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

let _pilotCache = null;
let _companyOwnedCache = null;

function loadPilotWebsiteMap() {
  if (_pilotCache) return _pilotCache;
  try {
    const p = path.join(
      __dirname,
      "..",
      "..",
      "fixtures",
      "ai-visibility",
      "discoverability-pilot-brands-v1.json"
    );
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    const map = {};
    for (const b of raw.brands || []) {
      if (b.brandId && b.brandWebsite) {
        map[b.brandId] = {
          brandId: b.brandId,
          brandName: b.brandName,
          companyKey: b.companyKey || null,
          brandWebsite: b.brandWebsite,
          parentCompanyWebsite: b.parentCompanyWebsite || null,
          brandDevelopmentUrl: b.brandDevelopmentUrl || null,
          franchiseDevelopmentUrl: b.franchiseDevelopmentUrl || null,
          brandedResidencesSourceUrl: b.brandedResidencesSourceUrl || null,
          regionalOfficialUrl: b.regionalOfficialUrl || null,
          urlSource: b.urlSource || "fixture_governed_brand_website",
        };
      }
    }
    _pilotCache = map;
  } catch {
    _pilotCache = {};
  }
  return _pilotCache;
}

function loadCompanyOwnedDomainsConfig() {
  if (_companyOwnedCache) return _companyOwnedCache;
  try {
    const p = path.join(
      __dirname,
      "..",
      "..",
      "fixtures",
      "ai-visibility",
      "showcase-company-owned-domains-v1.json"
    );
    _companyOwnedCache = JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    _companyOwnedCache = { companies: [] };
  }
  return _companyOwnedCache;
}

/**
 * Resolve governed brand row for owned-domain + discoverability.
 */
export function resolveGovernedBrandBasicsRow(brandId, opts = {}) {
  const basics = opts.brandBasicsById?.[brandId];
  const websiteFromNames = opts.brandNamesById?.[`website:${brandId}`] || null;
  const pilot = loadPilotWebsiteMap()[brandId];

  const brandWebsite =
    basics?.brandWebsite || websiteFromNames || pilot?.brandWebsite || null;

  return {
    brandId,
    brandName:
      basics?.brandName ||
      opts.brandNamesById?.[brandId] ||
      pilot?.brandName ||
      null,
    companyKey: basics?.companyKey || pilot?.companyKey || null,
    brandWebsite,
    parentCompanyWebsite:
      basics?.parentCompanyWebsite || pilot?.parentCompanyWebsite || null,
    brandedResidencesSourceUrl:
      basics?.brandedResidencesSourceUrl ||
      pilot?.brandedResidencesSourceUrl ||
      null,
    brandDevelopmentUrl:
      basics?.brandDevelopmentUrl || pilot?.brandDevelopmentUrl || null,
    franchiseDevelopmentUrl:
      basics?.franchiseDevelopmentUrl || pilot?.franchiseDevelopmentUrl || null,
    regionalOfficialUrl:
      basics?.regionalOfficialUrl || pilot?.regionalOfficialUrl || null,
    source:
      basics?.brandWebsite || websiteFromNames
        ? "brand_matrix"
        : pilot?.brandWebsite
          ? "fixture_governed"
          : "missing",
  };
}

export function resolveOwnedDomainsForBrand(brandId, opts = {}) {
  const row = resolveGovernedBrandBasicsRow(brandId, opts);
  return {
    brandRow: row,
    owned: resolveOwnedDomainsFromBrandRow(row),
  };
}

/**
 * Infer showcase companyKey when entitled brand IDs sit inside one showcase portfolio.
 */
export function inferShowcaseCompanyKeyFromBrandIds(brandIds = [], config) {
  const ids = [...new Set((brandIds || []).filter(Boolean))];
  if (!ids.length) return null;
  const cfg = config || loadShowcaseCompaniesConfig();
  const set = new Set(ids);
  // Prefer exact portfolio match
  for (const c of cfg.companies || []) {
    const portfolio = c.brandIds || [];
    if (
      portfolio.length &&
      portfolio.length === set.size &&
      portfolio.every((id) => set.has(id))
    ) {
      return c.companyKey;
    }
  }
  // Entitled brands are a subset of exactly one showcase portfolio
  const containers = (cfg.companies || []).filter((c) => {
    const portfolio = new Set(c.brandIds || []);
    return ids.every((id) => portfolio.has(id));
  });
  if (containers.length === 1) return containers[0].companyKey;
  return null;
}

/**
 * Governed company/portfolio owned domains (fixture), never suffix-inferred.
 */
export function resolveCompanyOwnedDomainEntries(companyKey) {
  if (!companyKey) return [];
  const cfg = loadCompanyOwnedDomainsConfig();
  const company = (cfg.companies || []).find(
    (c) => String(c.companyKey || "").toLowerCase() === String(companyKey).toLowerCase()
  );
  if (!company) return [];
  return (company.ownedDomains || [])
    .map((entry) => {
      const n = normalizeOwnedDomain(entry.domain || entry.url);
      if (!n) return null;
      return {
        domain: n.hostname,
        hostname: n.hostname,
        url: entry.url || n.original,
        tier: entry.tier || null,
        field: entry.field || null,
        inheritSubdomains: entry.inheritSubdomains === true,
        GOVERNED: true,
        companyKey: company.companyKey,
        scope: "PORTFOLIO_COMPANY",
      };
    })
    .filter(Boolean);
}

/**
 * Portfolio executive owned-domain union:
 * brand-official domains for entitled brands + governed company portfolio domains.
 */
export function resolvePortfolioOwnedDomains(opts = {}) {
  const brandIds = opts.brandIds || [];
  const companyKey =
    opts.companyKey || inferShowcaseCompanyKeyFromBrandIds(brandIds);
  const byDomain = new Map();
  const usedFields = new Set();
  const mapping = [];

  for (const brandId of brandIds) {
    const { owned, brandRow } = resolveOwnedDomainsForBrand(brandId, opts);
    for (const entry of owned.domains || []) {
      if (!byDomain.has(entry.domain)) {
        byDomain.set(entry.domain, {
          ...entry,
          scope: "BRAND",
          brandId,
        });
        usedFields.add(entry.field);
        mapping.push({
          domain: entry.domain,
          field: entry.field,
          tier: entry.tier,
          scope: "BRAND",
          brandId,
        });
      }
    }
    void brandRow;
  }

  for (const entry of resolveCompanyOwnedDomainEntries(companyKey)) {
    if (!byDomain.has(entry.domain)) {
      byDomain.set(entry.domain, entry);
      if (entry.field) usedFields.add(entry.field);
      mapping.push({
        domain: entry.domain,
        field: entry.field,
        tier: entry.tier,
        scope: "PORTFOLIO_COMPANY",
        companyKey,
      });
    }
  }

  const domains = [...byDomain.values()];
  return {
    companyKey: companyKey || null,
    company: companyKey ? getShowcaseCompany(companyKey) : null,
    domains,
    ownedDomainList: domains.map((d) => d.domain),
    ownedDomainEntries: domains,
    DOMAIN_SOURCE_MAPPING: mapping,
    USED_FIELDS: [...usedFields],
    OWNED_DOMAIN_STATUS: domains.length
      ? "CONFIGURED"
      : "MISSING_GOVERNED_SOURCE",
    FABRICATED_URLS: 0,
    INFERRED_FROM_NAME_SIMILARITY: false,
    SUFFIX_ONLY_INFERENCE: false,
  };
}

/**
 * Attach Phase 3C.2 discoverability UI payload for a brand (from latest baseline report).
 */
export function buildDiscoverabilityProductPayload(brandId, opts = {}) {
  const { brandRow, owned } = resolveOwnedDomainsForBrand(brandId, opts);
  const report = loadLatestPhase3c2Report();
  const fromReport = report?.byBrandId?.[brandId] || null;

  if (owned.OWNED_DOMAIN_STATUS === "MISSING_GOVERNED_SOURCE") {
    return {
      status: DATA_STATE.CONNECTION_REQUIRED,
      DISCOVERABILITY: PUBLIC_CONTENT_STATE.SOURCE_NOT_CONFIGURED,
      OFFICIAL_SOURCES_CONFIGURED: false,
      PUBLIC_SOURCES_ACCESSIBLE: false,
      OWNER_DEVELOPMENT_CONTENT_FOUND: false,
      OWNER_INTENT_CONTENT_GAPS: [],
      OWNED_SOURCES_CITED_IN_AI_RESPONSES: null,
      message: "No official brand website has been configured.",
      ARBITRARY_DISCOVERABILITY_SCORE: false,
      brandRow,
      ownedDomainStatus: owned.OWNED_DOMAIN_STATUS,
      LIVE_BASELINE: false,
    };
  }

  if (fromReport) {
    return {
      ...fromReport,
      status: fromReport.status || DATA_STATE.MEASURED,
      DISCOVERABILITY: "BASELINE_MEASURED",
      ARBITRARY_DISCOVERABILITY_SCORE: false,
      brandRow,
      ownedDomainStatus: owned.OWNED_DOMAIN_STATUS,
      LIVE_BASELINE: true,
      MODE: report?.MODE || null,
      LAST_CHECKED_AT: fromReport.LAST_CHECKED_AT || report?.completedAt || null,
    };
  }

  return {
    status: DATA_STATE.MEASURABLE_PUBLICLY,
    DISCOVERABILITY: "CHECK_NOT_RUN",
    OFFICIAL_SOURCES_CONFIGURED: true,
    PUBLIC_SOURCES_ACCESSIBLE: null,
    OWNER_DEVELOPMENT_CONTENT_FOUND: null,
    OWNER_INTENT_CONTENT_GAPS: [],
    OWNED_SOURCES_CITED_IN_AI_RESPONSES: null,
    message: "Discoverability baseline has not yet been measured.",
    ARBITRARY_DISCOVERABILITY_SCORE: false,
    brandRow,
    ownedDomainStatus: owned.OWNED_DOMAIN_STATUS,
    LIVE_BASELINE: false,
  };
}
