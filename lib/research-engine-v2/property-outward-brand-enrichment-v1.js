/**
 * Property-outward brand + fundamentals enrichment.
 * Resolve affiliation from existing Census identity (Website, address, candidate brand)
 * rather than treating blank Current Brand as unidentified.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_MASTER } from "./master-census-enrichment-v1.js";
import {
  MAP_BRAND,
  scorePortfolioToCensus,
  buildPortfolioBrandPatch,
} from "./master-brand-portfolio-validation-v1.js";
import {
  buildCanonicalBrandDictionary,
  familyFromOfficialUrl,
  lookupCanonicalBrand,
} from "./census-brand-canonical-dictionary.js";
import {
  affiliationStatusForValidatedBrand,
  buildBrandMappingRepairPatch,
  candidateBrandCannotSelfValidate,
  resolveBrandMappingAlias,
} from "./brand-mapping-gap-repair-v1.js";
import {
  fetchOfficialText,
  extractJsonLdHotels,
  hostFromUrl,
  isForbiddenHost,
} from "./official-domain-crawler-v1.js";
import {
  researchPropertyPage,
  buildPropertyFundamentalsPatch,
  classifyNullFill,
} from "./property-fundamentals-enrichment-v1.js";
import { loadPacksFromSampleCache } from "./apify-first-party-acquisition-v1.js";
import {
  topUnresolvedBrandCompanyDemand,
  companyNamesFromDemandRank,
} from "./company-adapter-demand-v1.js";
import {
  computeBrandResolutionMetrics,
  countBlankCurrentBrand,
} from "./brand-resolution-metrics-v1.js";
import { evaluateCoordinateCompletionEligibility } from "./census-coordinate-completion.js";
import { normalizeText, normalizeCountry } from "../independent-census/match-current-census.js";

function normKey(s) {
  return normalizeText(String(s || ""))
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizePortfolioRow(row) {
  return {
    ...row,
    country_norm: normalizeCountry(row.country),
    city_key: normKey(row.city),
    name_key: normKey(row.name),
  };
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const PROPERTY_OUTWARD_VERSION = "property-outward-brand-enrichment-v1";

export const CHECKPOINT_FP = path.join(
  ROOT,
  "data/research-engine-v2/property-outward-brand/checkpoint.json"
);

const BRAND_CLAIM_PATTERNS = [
  {
    re: /\b(curio collection by hilton|curio collection)\b/i,
    brand: "Curio Collection",
    soft: true,
  },
  {
    re: /\b(autograph collection)\b/i,
    brand: "Autograph Collection",
    soft: true,
  },
  {
    re: /\b(tapestry collection by hilton|tapestry collection)\b/i,
    brand: "Tapestry Collection by Hilton",
    soft: true,
  },
  {
    re: /\b(tribute portfolio)\b/i,
    brand: "Tribute Portfolio",
    soft: true,
  },
  {
    re: /\b(unbound collection)\b/i,
    brand: "Unbound Collection",
    soft: true,
  },
  {
    re: /\b(member of|part of|a)\s+design hotels\b/i,
    brand: "Design Hotels",
    soft: true,
  },
  {
    re: /\b(small luxury hotels|slh member)\b/i,
    brand: "Small Luxury Hotels",
    soft: true,
  },
  {
    re: /\b(preferred hotels(?:\s*&\s*resorts)?|member of preferred)\b/i,
    brand: "Preferred Hotels & Resorts",
    soft: true,
  },
  {
    re: /\b(leading hotels of the world|lhw member)\b/i,
    brand: "Leading Hotels of the World",
    soft: true,
  },
];

const INDEPENDENT_SIGNAL_RE =
  /\b(independently owned|independent hotel|family owned|boutique hotel(?!\s+by)|no chain affiliation)\b/i;

const GROUP_DOMAIN_HINTS =
  /(?:marriott|hilton|ihg|holidayinn|hyatt|accor|wyndham|choicehotels|radisson|melia|barcelo|fourseasons)\./i;

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

export function loadPropertyOutwardCheckpoint() {
  try {
    if (fs.existsSync(CHECKPOINT_FP)) {
      return JSON.parse(fs.readFileSync(CHECKPOINT_FP, "utf8"));
    }
  } catch {
    // ignore
  }
  return {
    version: PROPERTY_OUTWARD_VERSION,
    domain_routes: {},
    pages_cursor: 0,
    discovery_cursor: 0,
    aggregates: {},
  };
}

export function savePropertyOutwardCheckpoint(checkpoint) {
  writeJson(CHECKPOINT_FP, { ...checkpoint, updated_at: new Date().toISOString() });
}

/**
 * Parse Website into hostname / root domain / path + official family routing.
 */
export function parseWebsiteDomainIntelligence(url) {
  const raw = String(url || "").trim();
  if (!raw) {
    return {
      ok: false,
      reason: "blank_url",
      hostname: null,
      root_domain: null,
      path: null,
      official_family: null,
      is_group_domain: false,
    };
  }
  let parsed;
  try {
    parsed = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
  } catch {
    return {
      ok: false,
      reason: "invalid_url",
      hostname: null,
      root_domain: null,
      path: null,
      official_family: null,
      is_group_domain: false,
    };
  }
  const hostname = parsed.hostname.replace(/^www\./i, "").toLowerCase();
  const parts = hostname.split(".");
  const root_domain =
    parts.length >= 2 ? parts.slice(-2).join(".") : hostname;
  const official_family = familyFromOfficialUrl(parsed.toString());
  const is_group_domain =
    Boolean(official_family) || GROUP_DOMAIN_HINTS.test(hostname);
  return {
    ok: true,
    url: parsed.toString(),
    hostname,
    root_domain,
    path: parsed.pathname || "/",
    official_family: official_family || null,
    is_group_domain,
    host: hostname,
  };
}

export function isIdentityHigh(fields = {}) {
  if (isBlank(fields[MAP_MASTER.propertyName]) && isBlank(fields[MAP_MASTER.canonicalName])) {
    return false;
  }
  if (isBlank(fields[MAP_MASTER.country])) return false;
  if (!isBlank(fields[MAP_MASTER.city]) || !isBlank(fields[MAP_MASTER.address])) {
    return true;
  }
  return false;
}

/**
 * Explicit affiliation claims from property HTML (Phase 3).
 */
export function extractBrandClaimsFromHtml(html) {
  const text = String(html || "");
  const claims = [];
  for (const pat of BRAND_CLAIM_PATTERNS) {
    if (pat.re.test(text)) {
      claims.push({
        brand: pat.brand,
        soft: pat.soft === true,
        confidence: "high",
        method: "explicit_affiliation_text",
      });
    }
  }
  return claims;
}

export function evaluateIndependentEvidence(rec, html, domainInfo) {
  const f = rec.fields || {};
  if (!domainInfo?.ok) return { ok: false, reason: "no_domain" };
  if (domainInfo.is_group_domain) return { ok: false, reason: "group_domain" };
  if (isForbiddenHost(domainInfo.url)) return { ok: false, reason: "forbidden_host" };
  if (!isIdentityHigh(f)) return { ok: false, reason: "identity_not_high" };

  const claims = extractBrandClaimsFromHtml(html);
  if (claims.length) return { ok: false, reason: "brand_claims_present" };

  const hotels = extractJsonLdHotels(html);
  for (const h of hotels) {
    if (h.brand) return { ok: false, reason: "json_ld_brand_present" };
  }

  const name = normalizeText(f[MAP_MASTER.propertyName] || f[MAP_MASTER.canonicalName]);
  const titleMatch =
    name &&
    normalizeText(html.slice(0, 8000)).includes(name.slice(0, Math.min(name.length, 24)));

  const explicitIndependent = INDEPENDENT_SIGNAL_RE.test(html);
  if (explicitIndependent && titleMatch) {
    return { ok: true, confidence: "high", method: "explicit_independent_text" };
  }
  if (titleMatch && !domainInfo.is_group_domain && hotels.length <= 1) {
    return { ok: true, confidence: "medium", method: "owned_domain_no_affiliation" };
  }
  return { ok: false, reason: "insufficient_independent_evidence" };
}

function mergePatches(...patches) {
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const p of patches) {
    if (!p) continue;
    Object.assign(out, p);
  }
  return out;
}

function tallyFieldWrites(patch, counts) {
  if (!patch) return counts;
  const c = { ...counts };
  if (patch[MAP_MASTER.currentBrand] != null) c.CURRENT_BRAND_WRITES += 1;
  if (patch[MAP_MASTER.brandFamily] != null || patch[MAP_MASTER.familySourceFamily] != null) {
    c.BRAND_FAMILY_DERIVATIONS += 1;
  }
  if (patch[MAP_MASTER.roomsKeys] != null) c.ROOMS_WRITES += 1;
  if (patch[MAP_MASTER.address] != null) c.ADDRESS_WRITES += 1;
  if (patch[MAP_MASTER.postalCode] != null) c.POSTAL_WRITES += 1;
  if (patch[MAP_MASTER.stateRegion] != null) c.STATE_WRITES += 1;
  if (patch[MAP_MASTER.city] != null) c.CITY_WRITES += 1;
  if (patch[MAP_MASTER.latitude] != null || patch[MAP_MASTER.longitude] != null) {
    c.COORDINATE_WRITES += 1;
  }
  if (patch[MAP_MASTER.officialUrl] != null) c.WEBSITE_WRITES += 1;
  if (patch[MAP_MASTER.phone] != null) c.PHONE_WRITES += 1;
  if (patch["Affiliation Status"] === "Independent") c.INDEPENDENT_VALIDATED += 1;
  return c;
}

function buildBrandPatchFromResolved(fields, resolved, opts = {}) {
  if (!resolved?.ok) return { ok: false, reason: resolved?.reason || "unresolved" };
  const dictionary = opts.dictionary;
  const built = buildBrandMappingRepairPatch(fields, {
    dictionary,
    portfolioBrand: resolved.canonical,
    allowWriteCurrentBrand: opts.identityHigh === true,
    identityHigh: opts.identityHigh === true,
  });
  if (built.ok) return built;

  if (opts.identityHigh && resolved.canonical) {
    /** @type {Record<string, unknown>} */
    const patch = {};
    if (isBlank(fields[MAP_MASTER.currentBrand])) {
      patch[MAP_MASTER.currentBrand] = resolved.canonical;
    }
    if (isBlank(fields[MAP_MASTER.brandFamily]) && resolved.parent) {
      patch[MAP_MASTER.brandFamily] = resolved.parent;
    }
    if (isBlank(fields[MAP_MASTER.familySourceFamily]) && resolved.parent) {
      patch[MAP_MASTER.familySourceFamily] = resolved.parent;
    }
    const aff = affiliationStatusForValidatedBrand(resolved);
    if (aff && isBlank(fields["Affiliation Status"])) patch["Affiliation Status"] = aff;
    if (Object.keys(patch).length) {
      patch[MAP_MASTER.lastReviewed] = todayIsoDate();
      patch[MAP_MASTER.enrichmentStatus] = "Partial";
      return { ok: true, patch, class: "BRAND_VALIDATED_HIGH", canonical: resolved.canonical };
    }
  }
  return built;
}

function portfolioRowFromJsonLd(hotel, url, company) {
  return {
    company: company || familyFromOfficialUrl(url) || null,
    brand: hotel.brand || null,
    name: hotel.name,
    url: hotel.url || url,
    city: hotel.city,
    country: hotel.country,
    state: hotel.state,
    address: hotel.address,
    postal: hotel.postal,
    phone: hotel.phone,
  };
}

function buildMultiFieldPatch(rec, extract, dictionary, opts = {}) {
  const fields = rec.fields || {};
  const proposals = [];
  /** @type {Record<string, unknown>} */
  let brandPatch = {};

  if (extract?.brandResolved?.ok && opts.identityHigh) {
    const bp = buildBrandPatchFromResolved(fields, extract.brandResolved, {
      dictionary,
      identityHigh: true,
    });
    if (bp.ok) brandPatch = bp.patch || {};
  }

  let fundamentalsPatch = {};
  if (extract?.fundamentals) {
    const built = buildPropertyFundamentalsPatch(rec, extract.fundamentals, {
      allowRoomsHigh: extract.fundamentals.property_level === true,
    });
    fundamentalsPatch = built.patch || {};
  }

  const patch = mergePatches(brandPatch, fundamentalsPatch);
  if (!Object.keys(patch).length) return null;

  for (const [k, v] of Object.entries({ ...patch })) {
    if (k === MAP_MASTER.lastReviewed || k === MAP_MASTER.enrichmentStatus) continue;
    if (!isBlank(fields[k])) delete patch[k];
  }
  if (!Object.keys(patch).length) return null;

  if (!patch[MAP_MASTER.lastReviewed]) {
    patch[MAP_MASTER.lastReviewed] = todayIsoDate();
    patch[MAP_MASTER.enrichmentStatus] = "Partial";
  }
  return { id: rec.id, fields: patch };
}

function rankPropertyOutwardQueue(records, opts = {}) {
  const requireWebsite = opts.requireWebsite !== false;
  const requireBlankBrand = opts.requireBlankBrand !== false;
  const candidateBoost = opts.candidateBoost === true;

  return records
    .map((rec) => {
      const f = rec.fields || {};
      if (requireBlankBrand && !isBlank(f[MAP_MASTER.currentBrand])) {
        return { rec, score: 0, skip: true };
      }
      const website = f[MAP_MASTER.officialUrl] || f[MAP_BRAND.officialUrl];
      if (requireWebsite && (isBlank(website) || isForbiddenHost(website))) {
        return { rec, score: 0, skip: true };
      }
      const domain = website ? parseWebsiteDomainIntelligence(website) : null;
      let score = 0;
      if (domain?.official_family) score += 3;
      if (domain?.is_group_domain) score += 2;
      if (isIdentityHigh(f)) score += 2;
      if (!isBlank(f[MAP_MASTER.address])) score += 1;
      if (!isBlank(f[MAP_BRAND.candidateBrand])) score += candidateBoost ? 4 : 1.5;
      return { rec, score, skip: false, domain };
    })
    .filter((x) => !x.skip)
    .sort((a, b) => b.score - a.score);
}

/**
 * Phase 1 — domain intelligence (no fetch).
 */
export function runPropertyOutwardDomainIntelligence(opts = {}) {
  const records = opts.censusRecords || [];
  let analyzed = 0;
  let groupDomains = 0;
  const routes = {};
  const demand = topUnresolvedBrandCompanyDemand(records, 20, opts);

  for (const rec of records) {
    const f = rec.fields || {};
    if (!isBlank(f[MAP_MASTER.currentBrand])) continue;
    const website = f[MAP_MASTER.officialUrl] || f[MAP_BRAND.officialUrl];
    if (isBlank(website)) continue;
    const domain = parseWebsiteDomainIntelligence(website);
    if (!domain.ok) continue;
    analyzed += 1;
    if (domain.official_family) {
      groupDomains += 1;
      routes[rec.id] = {
        company: domain.official_family,
        hostname: domain.hostname,
        root_domain: domain.root_domain,
      };
    }
  }

  const checkpoint = loadPropertyOutwardCheckpoint();
  checkpoint.domain_routes = { ...checkpoint.domain_routes, ...routes };
  checkpoint.last_domain_run = new Date().toISOString();
  savePropertyOutwardCheckpoint(checkpoint);

  return {
    ok: true,
    phase: "property_outward_domain",
    proposals: [],
    exhausted: analyzed === 0,
    WEBSITE_DOMAIN_PROPERTIES_ANALYZED: analyzed,
    OFFICIAL_GROUP_DOMAINS_IDENTIFIED: groupDomains,
    TOP_20_UNRESOLVED_BRAND_COMPANY_DEMAND: demand,
    domain_routes: routes,
  };
}

/**
 * Phase 2+3 — fetch official property pages for brand blank + Website populated.
 */
export async function runPropertyOutwardOfficialPages(opts = {}) {
  const log = opts.log || (() => {});
  const records = opts.censusRecords || [];
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  const fetchFn = opts.fetchFn || fetchOfficialText;
  const maxProperties = Number(opts.maxProperties || 24);
  const ranked = rankPropertyOutwardQueue(records, {
    requireWebsite: true,
    candidateBoost: opts.candidateBoost === true,
  }).slice(0, maxProperties);

  const proposals = [];
  let analyzed = 0;
  let counts = emptyWriteCounts();

  for (const item of ranked) {
    const rec = item.rec;
    const f = rec.fields || {};
    const website = f[MAP_MASTER.officialUrl] || f[MAP_BRAND.officialUrl];
    if (isBlank(website) || isForbiddenHost(website)) continue;

    candidateBrandCannotSelfValidate(f);

    let page;
    let html = "";
    let url = website;
    try {
      if (opts.researchFn) {
        page = await opts.researchFn(rec);
        html = page?.html || page?.text || "";
        url = page?.url || website;
      } else {
        const fetched = await fetchFn(website, opts);
        if (!fetched.ok || !fetched.text) continue;
        html = fetched.text;
        url = fetched.url || website;
        page = await researchPropertyPage(rec, {
          fetchFn: async () => ({
            ok: true,
            html,
            finalUrl: url,
            status: fetched.status || 200,
          }),
        });
      }
    } catch (err) {
      log(`[property-outward] page ${rec.id}: ${String(err?.message || err).slice(0, 100)}`);
      continue;
    }
    if (!page?.ok && !page?.extract && !html) continue;
    analyzed += 1;

    url = page?.url || url;
    const domain = parseWebsiteDomainIntelligence(url);
    const identityHigh = isIdentityHigh(f);

    let brandResolved = null;
    const claims = extractBrandClaimsFromHtml(html);
    if (claims.length && identityHigh) {
      const claim = claims[0];
      brandResolved = resolveBrandMappingAlias(claim.brand, {
        dictionary,
        propertyName: f[MAP_MASTER.propertyName],
        sourceUrl: url,
      });
      if (brandResolved.ok) brandResolved.soft = claim.soft;
    }

    const hotels = extractJsonLdHotels(html);
    if (!brandResolved?.ok && hotels.length && identityHigh) {
      let best = null;
      for (const h of hotels) {
        const row = portfolioRowFromJsonLd(h, url, domain.official_family);
        const scored = scorePortfolioToCensus(row, f);
        if (!best || scored.score > best.scored.score) best = { row, scored };
      }
      if (best?.scored?.confidence === "high" && best.row.brand) {
        brandResolved = resolveBrandMappingAlias(best.row.brand, {
          dictionary,
          propertyName: best.row.name || f[MAP_MASTER.propertyName],
          sourceUrl: url,
        });
      } else if (best?.scored?.confidence === "high" && domain.official_family) {
        const built = buildPortfolioBrandPatch(f, best.row, dictionary);
        if (built.ok) {
          const proposal = { id: rec.id, fields: built.patch };
          proposals.push(proposal);
          counts = tallyFieldWrites(built.patch, counts);
          counts.TOTAL_PROPERTIES_RESEARCHED += 1;
          continue;
        }
      }
    }

    const extract = {
      brandResolved,
      fundamentals: page.extract || page,
      property_level: page.property_level !== false,
    };
    const built = buildMultiFieldPatch(rec, extract, dictionary, { identityHigh });
    if (built) {
      proposals.push(built);
      counts = tallyFieldWrites(built.fields, counts);
      counts.TOTAL_PROPERTIES_RESEARCHED += 1;
      counts.TOTAL_PROPERTIES_PATCHED += 1;
      counts.TOTAL_FIELDS_WRITTEN += Object.keys(built.fields).length;
    }
  }

  return {
    ok: true,
    phase: "property_outward_pages",
    proposals,
    exhausted: ranked.length === 0 || proposals.length === 0,
    PROPERTY_WEBSITES_ANALYZED: analyzed,
    ...counts,
  };
}

/**
 * Phase 4 — conservative validated independent (Affiliation Status only).
 */
export async function runPropertyOutwardIndependent(opts = {}) {
  const records = opts.censusRecords || [];
  const fetchFn = opts.fetchFn || fetchOfficialText;
  const maxProperties = Number(opts.maxProperties || 12);
  const proposals = [];
  let counts = emptyWriteCounts();

  const queue = rankPropertyOutwardQueue(records, { requireWebsite: true })
    .filter((x) => x.domain && !x.domain.is_group_domain)
    .slice(0, maxProperties);

  for (const item of queue) {
    const rec = item.rec;
    const f = rec.fields || {};
    if (!isBlank(f["Affiliation Status"])) continue;
    const website = f[MAP_MASTER.officialUrl] || f[MAP_BRAND.officialUrl];
    const domain = parseWebsiteDomainIntelligence(website);
    const page = await fetchFn(domain.url || website, opts);
    if (!page.ok || !page.text) continue;
    const ev = evaluateIndependentEvidence(rec, page.text, domain);
    if (!ev.ok || ev.confidence !== "high") continue;

    const patch = {
      "Affiliation Status": "Independent",
      [MAP_MASTER.lastReviewed]: todayIsoDate(),
      [MAP_MASTER.enrichmentStatus]: "Partial",
    };
    proposals.push({ id: rec.id, fields: patch, class: "INDEPENDENT_VALIDATED" });
    counts.INDEPENDENT_VALIDATED += 1;
    counts.TOTAL_PROPERTIES_RESEARCHED += 1;
    counts.TOTAL_PROPERTIES_PATCHED += 1;
    counts.TOTAL_FIELDS_WRITTEN += 1;
  }

  return {
    ok: true,
    phase: "property_outward_independent",
    proposals,
    exhausted: queue.length === 0 || proposals.length === 0,
    ...counts,
  };
}

/**
 * Phase 5 — candidate brand routing (never self-validates).
 */
export async function runPropertyOutwardCandidateRouting(opts = {}) {
  const records = (opts.censusRecords || []).filter((rec) => {
    const f = rec.fields || {};
    return isBlank(f[MAP_MASTER.currentBrand]) && !isBlank(f[MAP_BRAND.candidateBrand]);
  });
  return runPropertyOutwardOfficialPages({
    ...opts,
    censusRecords: records,
    candidateBoost: true,
    maxProperties: opts.maxProperties || 20,
  });
}

function cachedPortfolioRowsFromApify() {
  const rows = [];
  for (const pack of loadPacksFromSampleCache()) {
    const company = pack.actor?.COMPANY || pack.actor?.company || null;
    for (const row of pack.rows || []) {
      rows.push({
        company,
        brand: row.brand || row.brandName || company,
        name: row.name || row.hotelName || row.propertyName,
        url: row.url || row.website || row.propertyUrl,
        city: row.city,
        country: row.country,
        state: row.state || row.stateRegion,
        address: row.address || row.addressLine1,
        postal: row.postalCode || row.postal,
        phone: row.phone,
      });
    }
  }
  return rows.filter((r) => r.url && r.name);
}

/**
 * Phase 6 — Website discovery for brand blank + Website blank (cached portfolios only).
 */
export function runPropertyOutwardWebsiteDiscovery(opts = {}) {
  const records = opts.censusRecords || [];
  const maxProperties = Number(opts.maxProperties || 30);
  const portfolioRows = (opts.portfolioRows || cachedPortfolioRowsFromApify()).map(
    normalizePortfolioRow
  );
  const proposals = [];
  let counts = emptyWriteCounts();

  const targets = records
    .filter((rec) => {
      const f = rec.fields || {};
      return (
        isBlank(f[MAP_MASTER.currentBrand]) &&
        isBlank(f[MAP_MASTER.officialUrl]) &&
        isIdentityHigh(f)
      );
    })
    .slice(0, maxProperties);

  for (const rec of targets) {
    const f = rec.fields || {};
    let best = null;
    for (const row of portfolioRows) {
      if (!row.url || isForbiddenHost(row.url)) continue;
      const scored = scorePortfolioToCensus(row, f);
      if (!best || scored.score > best.scored.score) best = { row, scored };
    }
    const identityStrong =
      best.scored.confidence === "high" ||
      ((best.scored.nameSim || 0) >= 0.88 &&
        best.scored.countryOk === true &&
        best.scored.cityOk === true &&
        best.scored.score >= 55);
    if (!identityStrong) continue;

    const patch = {
      [MAP_MASTER.officialUrl]: best.row.url,
      [MAP_MASTER.lastReviewed]: todayIsoDate(),
      [MAP_MASTER.enrichmentStatus]: "Partial",
    };
    proposals.push({ id: rec.id, fields: patch });
    counts.WEBSITE_WRITES += 1;
    counts.TOTAL_PROPERTIES_RESEARCHED += 1;
    counts.TOTAL_PROPERTIES_PATCHED += 1;
    counts.TOTAL_FIELDS_WRITTEN += 1;
  }

  return {
    ok: true,
    phase: "property_outward_website_discovery",
    proposals,
    exhausted: targets.length === 0 || proposals.length === 0,
    ...counts,
  };
}

function emptyWriteCounts() {
  return {
    CURRENT_BRAND_WRITES: 0,
    BRAND_FAMILY_DERIVATIONS: 0,
    ROOMS_WRITES: 0,
    ADDRESS_WRITES: 0,
    POSTAL_WRITES: 0,
    STATE_WRITES: 0,
    CITY_WRITES: 0,
    COORDINATE_WRITES: 0,
    WEBSITE_WRITES: 0,
    PHONE_WRITES: 0,
    INDEPENDENT_VALIDATED: 0,
    TOTAL_PROPERTIES_RESEARCHED: 0,
    TOTAL_PROPERTIES_PATCHED: 0,
    TOTAL_FIELDS_WRITTEN: 0,
  };
}

export function buildPropertyOutwardEnrichmentStatus(state = {}, records = [], extras = {}) {
  const metrics = computeBrandResolutionMetrics(records);
  const demand = topUnresolvedBrandCompanyDemand(records, 20, extras);
  return {
    PROPERTY_OUTWARD_ENRICHMENT_STATUS: "property_outward_lane_active",
    CURRENT_BRAND_BEFORE: extras.currentBrandBefore ?? null,
    CURRENT_BRAND_AFTER: extras.currentBrandAfter ?? null,
    CURRENT_BRAND_WRITES: state.current_brand_writes || extras.CURRENT_BRAND_WRITES || 0,
    ...metrics,
    WEBSITE_DOMAIN_PROPERTIES_ANALYZED:
      state.website_domain_analyzed || extras.WEBSITE_DOMAIN_PROPERTIES_ANALYZED || 0,
    OFFICIAL_GROUP_DOMAINS_IDENTIFIED:
      state.official_group_domains || extras.OFFICIAL_GROUP_DOMAINS_IDENTIFIED || 0,
    PROPERTY_WEBSITES_ANALYZED:
      state.property_websites_analyzed || extras.PROPERTY_WEBSITES_ANALYZED || 0,
    BRAND_MAPPING_GAPS_BEFORE: extras.BRAND_MAPPING_GAPS_BEFORE ?? null,
    BRAND_MAPPING_GAPS_FIXED: extras.BRAND_MAPPING_GAPS_FIXED ?? null,
    BRAND_MAPPING_GAPS_TRUE_NEW_BRANDS: extras.BRAND_MAPPING_GAPS_TRUE_NEW_BRANDS ?? null,
    BRAND_MAPPING_GAPS_AFTER: extras.BRAND_MAPPING_GAPS_AFTER ?? null,
    TOP_20_UNRESOLVED_BRAND_COMPANY_DEMAND: demand,
    ROOM_CANDIDATES_BEFORE: extras.ROOM_CANDIDATES_BEFORE ?? null,
    ROOM_CANDIDATES_RESEARCHED: extras.ROOM_CANDIDATES_RESEARCHED ?? null,
    ROOM_CANDIDATES_CORROBORATED: extras.ROOM_CANDIDATES_CORROBORATED ?? null,
    ROOM_CANDIDATES_REMAINING: extras.ROOM_CANDIDATES_REMAINING ?? null,
    ROOMS_WRITES: state.rooms_writes || extras.ROOMS_WRITES || 0,
    WEBSITE_WRITES: state.website_writes || extras.WEBSITE_WRITES || 0,
    ADDRESS_WRITES: state.address_writes || extras.ADDRESS_WRITES || 0,
    POSTAL_WRITES: state.postal_writes || extras.POSTAL_WRITES || 0,
    STATE_WRITES: state.state_writes || extras.STATE_WRITES || 0,
    CITY_WRITES: state.city_writes || extras.CITY_WRITES || 0,
    COORDINATE_WRITES: state.coordinates_written || extras.COORDINATE_WRITES || 0,
    PHONE_WRITES: state.phone_writes || extras.PHONE_WRITES || 0,
    TOTAL_PROPERTIES_RESEARCHED: state.properties_researched || 0,
    TOTAL_PROPERTIES_PATCHED: state.properties_patched || 0,
    TOTAL_FIELDS_WRITTEN: state.fields_written || 0,
    TOP_20_SOURCE_YIELDS: extras.TOP_20_SOURCE_YIELDS || [],
    FOUNDER_DECISION_REQUIRED: extras.FOUNDER_DECISION_REQUIRED || "NO",
    STOP_REASON: extras.STOP_REASON || null,
    CHECKPOINT_PATH: CHECKPOINT_FP,
  };
}

void classifyNullFill;
void evaluateCoordinateCompletionEligibility;
void countBlankCurrentBrand;
void companyNamesFromDemandRank;
