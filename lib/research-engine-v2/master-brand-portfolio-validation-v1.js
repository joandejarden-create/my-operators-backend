/**
 * Master Brand Portfolio Validation — official directory → Census Current Brand HIGH.
 *
 * Prefer official brand/company portfolios over one-off web searches.
 * Candidate Brand Text prioritizes matching; never auto-promotes alone.
 * NULL_FILL Current Brand / Brand Family / Family / Source Family only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildCanonicalBrandDictionary,
  lookupCanonicalBrand,
  familyFromOfficialUrl,
} from "./census-brand-canonical-dictionary.js";
import { canonicalizeParentCompany } from "./census-parent-company-normalization.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { POSTAL_CODE_FIELD } from "./census-postal-code-v1.js";
import {
  nameSimilarity,
  normalizeCountry,
  normalizeText,
  websiteHost,
} from "../independent-census/match-current-census.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MASTER_BRAND_PORTFOLIO_VERSION =
  "master-brand-portfolio-validation-v1";

export const MAP_BRAND = Object.freeze({
  propertyName: MAP_FIRST_PASS.propertyName,
  canonicalName: MAP_FIRST_PASS.canonicalPropertyName,
  country: MAP_FIRST_PASS.country,
  stateRegion: MAP_FIRST_PASS.stateRegion,
  city: MAP_FIRST_PASS.city,
  address: MAP_FIRST_PASS.address,
  postalCode: POSTAL_CODE_FIELD,
  officialUrl: MAP_FIRST_PASS.officialUrl,
  phone: "Phone",
  currentBrand: MAP_FIRST_PASS.currentBrand,
  brandFamily: MAP_FIRST_PASS.brandFamily,
  familySourceFamily: MAP_FIRST_PASS.family,
  candidateBrand: "Candidate Brand Text",
  candidateBrandFamily: "Candidate Brand Family",
  lastReviewed: MAP_FIRST_PASS.lastReviewed,
  enrichmentStatus: MAP_FIRST_PASS.enrichmentStatus,
});

/** Offline official portfolio extracts (VIC wave artifacts). */
export const OFFICIAL_PORTFOLIO_SOURCES = Object.freeze([
  {
    company: "Hilton",
    path: "data/research-engine-v2/verified-independent-census-wave1b-hilton/02-hilton-full-records.json",
  },
  {
    company: "Marriott",
    path: "data/research-engine-v2/verified-independent-census-wave1d-marriott/02-marriott-full-records.json",
  },
  {
    company: "Choice",
    path: "data/research-engine-v2/verified-independent-census-wave1c-choice/02-choice-full-records.json",
  },
]);

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function normKey(s) {
  return normalizeText(String(s || ""))
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function claimValue(rec, fieldNames) {
  const claims = rec.field_claims || [];
  for (const name of fieldNames) {
    const hit = claims.find(
      (c) => String(c.field || "").toLowerCase() === String(name).toLowerCase()
    );
    if (hit && !isBlank(hit.value)) return String(hit.value).trim();
  }
  return null;
}

/**
 * Normalize a VIC / portfolio record into a matchable row.
 */
export function normalizePortfolioRecord(rec, company) {
  const brand =
    String(rec.brand || "").trim() ||
    claimValue(rec, ["Affiliation", "affiliation", "brand"]) ||
    null;
  const name =
    String(rec.canonical_hotel_name || rec.name || "").trim() ||
    claimValue(rec, ["name", "Property Name"]) ||
    null;
  const url =
    String(rec.official_property_url || rec.website || "").trim() ||
    claimValue(rec, ["Website", "website", "Official Property URL"]) ||
    null;
  const city =
    String(rec.normalized_city || rec.city || "").trim() ||
    claimValue(rec, ["city", "City"]) ||
    null;
  const country =
    String(rec.country || "").trim() ||
    claimValue(rec, ["country", "Country"]) ||
    null;
  const state =
    claimValue(rec, ["State", "state", "State / Region"]) ||
    String(rec.state || rec.state_region || "").trim() ||
    null;
  const address =
    claimValue(rec, ["Address 1", "Address", "address"]) ||
    String(rec.address || "").trim() ||
    null;
  const postal =
    claimValue(rec, ["Postal Code", "postal_code"]) ||
    String(rec.postal_code || "").trim() ||
    null;
  const phone =
    claimValue(rec, ["Telephone", "Phone", "phone"]) ||
    String(rec.phone || "").trim() ||
    null;
  const propertyIds = Array.isArray(rec.official_property_ids)
    ? rec.official_property_ids.map((x) => String(x).toUpperCase())
    : [];
  const code =
    propertyIds[0] ||
    claimValue(rec, ["Brand Property Code", "Property ID"]) ||
    null;

  return {
    company,
    brand,
    name,
    url,
    city,
    country,
    state,
    address,
    postal,
    phone,
    property_code: code ? String(code).toUpperCase() : null,
    host: websiteHost(url),
    name_key: normKey(name),
    city_key: normKey(city),
    country_norm: normalizeCountry(country),
  };
}

export function loadOfficialPortfolioRows(opts = {}) {
  const sources = opts.sources || OFFICIAL_PORTFOLIO_SOURCES;
  const rows = [];
  const loaded = [];
  for (const src of sources) {
    const fp = path.isAbsolute(src.path)
      ? src.path
      : path.join(ROOT, src.path);
    if (!fs.existsSync(fp)) {
      loaded.push({
        company: src.company,
        path: src.path,
        loaded: false,
        count: 0,
        reason: "file_missing",
      });
      continue;
    }
    const json = JSON.parse(fs.readFileSync(fp, "utf8"));
    const records = Array.isArray(json)
      ? json
      : json.records || json.properties || [];
    let n = 0;
    for (const rec of records) {
      const row = normalizePortfolioRecord(rec, src.company);
      if (!row.name && !row.url) continue;
      rows.push(row);
      n += 1;
    }
    loaded.push({
      company: src.company,
      path: src.path,
      loaded: true,
      count: n,
    });
  }
  return { rows, loaded };
}

function urlPathContainsCode(url, code) {
  if (!url || !code) return false;
  return String(url).toLowerCase().includes(String(code).toLowerCase());
}

/**
 * Score portfolio row vs Census record. HIGH only when identity is strong.
 */
export function scorePortfolioToCensus(portfolio, fields) {
  const reasons = [];
  let score = 0;
  const censusUrl = String(fields[MAP_BRAND.officialUrl] || "").trim();
  const censusName = String(
    fields[MAP_BRAND.propertyName] || fields[MAP_BRAND.canonicalName] || ""
  );
  const censusCity = String(fields[MAP_BRAND.city] || "");
  const censusCountry = String(fields[MAP_BRAND.country] || "");

  if (portfolio.url && censusUrl) {
    const ph = websiteHost(portfolio.url);
    const ch = websiteHost(censusUrl);
    if (ph && ch && ph === ch) {
      score += 40;
      reasons.push("official_url_host_match");
      // Same property path / code in URL
      if (
        portfolio.property_code &&
        urlPathContainsCode(censusUrl, portfolio.property_code)
      ) {
        score += 50;
        reasons.push("property_code_in_census_url");
      } else {
        const pPath = String(portfolio.url).split("?")[0].toLowerCase();
        const cPath = String(censusUrl).split("?")[0].toLowerCase();
        if (pPath && cPath && (pPath === cPath || cPath.includes(pPath.split("/").slice(-2).join("/")))) {
          score += 45;
          reasons.push("official_url_path_match");
        }
      }
    }
  }

  if (
    portfolio.property_code &&
    censusUrl &&
    urlPathContainsCode(censusUrl, portfolio.property_code)
  ) {
    score = Math.max(score, 95);
    reasons.push("property_code_url_identity");
  }

  const nameSim = nameSimilarity(portfolio.name, censusName);
  score += Math.round(nameSim * 35);
  if (nameSim >= 0.88) reasons.push("name_high");
  else if (nameSim >= 0.72) reasons.push("name_medium");

  const countryOk =
    portfolio.country_norm &&
    normalizeCountry(censusCountry) &&
    portfolio.country_norm === normalizeCountry(censusCountry);
  if (countryOk) {
    score += 15;
    reasons.push("country_match");
  } else if (portfolio.country_norm && normalizeCountry(censusCountry)) {
    return { score: 0, confidence: "none", reasons: ["country_mismatch"], nameSim };
  }

  const cityOk =
    portfolio.city_key &&
    normKey(censusCity) &&
    (portfolio.city_key === normKey(censusCity) ||
      portfolio.city_key.includes(normKey(censusCity)) ||
      normKey(censusCity).includes(portfolio.city_key));
  if (cityOk) {
    score += 10;
    reasons.push("city_match");
  }

  let confidence = "none";
  if (score >= 90 && countryOk && (nameSim >= 0.72 || reasons.includes("property_code_url_identity") || reasons.includes("official_url_path_match"))) {
    confidence = "high";
  } else if (score >= 75) {
    confidence = "medium";
  } else if (score >= 55) {
    confidence = "low";
  }

  return { score, confidence, reasons, nameSim, countryOk, cityOk };
}

function parentFromLookup(entry) {
  return (
    canonicalizeParentCompany(entry.parent_company || entry.brand_family) ||
    entry.parent_company ||
    entry.brand_family ||
    null
  );
}

/**
 * Build NULL_FILL patch for a HIGH portfolio match.
 */
export function buildPortfolioBrandPatch(fields, portfolio, dictionary) {
  if (!isBlank(fields[MAP_BRAND.currentBrand])) {
    const existing = String(fields[MAP_BRAND.currentBrand]).trim();
    const incoming = String(portfolio.brand || "").trim();
    if (
      incoming &&
      normKey(existing) !== normKey(incoming) &&
      !normKey(existing).includes(normKey(incoming)) &&
      !normKey(incoming).includes(normKey(existing))
    ) {
      return {
        ok: false,
        class: "BRAND_CONFLICT_REVIEW",
        reason: "existing_current_brand_conflicts",
        existing,
        incoming,
      };
    }
    return { ok: false, reason: "current_brand_already_populated" };
  }

  const brandRaw = portfolio.brand;
  if (!brandRaw) {
    return { ok: false, reason: "portfolio_brand_blank" };
  }

  const lookup = lookupCanonicalBrand(brandRaw, dictionary, {
    propertyName: portfolio.name || fields[MAP_BRAND.propertyName],
    sourceUrl: portfolio.url,
  });
  if (!lookup.ok || !lookup.entry) {
    return {
      ok: false,
      class: "BRAND_MAPPING_GAP",
      reason: "BRAND_MAPPING_GAP",
      brand: brandRaw,
    };
  }

  const parent = parentFromLookup(lookup.entry);
  /** @type {Record<string, unknown>} */
  const patch = {
    [MAP_BRAND.currentBrand]: lookup.canonical || lookup.entry.canonical_brand_name,
    [MAP_BRAND.lastReviewed]: todayIsoDate(),
    [MAP_BRAND.enrichmentStatus]: "Partial",
  };
  if (isBlank(fields[MAP_BRAND.brandFamily]) && parent) {
    patch[MAP_BRAND.brandFamily] = parent;
  }
  if (isBlank(fields[MAP_BRAND.familySourceFamily]) && parent) {
    patch[MAP_BRAND.familySourceFamily] = parent;
  }

  // Opportunistic fundamentals NULL_FILL from official portfolio
  const geoFilled = {};
  if (isBlank(fields[MAP_BRAND.officialUrl]) && portfolio.url) {
    patch[MAP_BRAND.officialUrl] = portfolio.url;
  }
  if (isBlank(fields[MAP_BRAND.address]) && portfolio.address) {
    patch[MAP_BRAND.address] = portfolio.address;
    geoFilled.address = true;
  }
  if (isBlank(fields[MAP_BRAND.postalCode]) && portfolio.postal) {
    patch[MAP_BRAND.postalCode] = portfolio.postal;
    geoFilled.postal = true;
  }
  if (isBlank(fields[MAP_BRAND.city]) && portfolio.city) {
    patch[MAP_BRAND.city] = portfolio.city;
  }
  if (isBlank(fields[MAP_BRAND.stateRegion]) && portfolio.state) {
    patch[MAP_BRAND.stateRegion] = portfolio.state;
  }
  if (isBlank(fields[MAP_BRAND.phone]) && portfolio.phone) {
    patch[MAP_BRAND.phone] = portfolio.phone;
  }

  return {
    ok: true,
    class: "BRAND_VALIDATED_HIGH",
    patch,
    geo_filled: geoFilled,
    evidence: {
      method: "official_brand_portfolio_match",
      company: portfolio.company,
      portfolio_url: portfolio.url,
      brand: lookup.canonical,
    },
  };
}

/**
 * Candidate Brand Text + Official URL corroboration (never Candidate alone).
 */
export function evaluateCandidateBrandWithOfficialUrl(fields = {}, dictionary) {
  if (!isBlank(fields[MAP_BRAND.currentBrand])) {
    return { ok: false, reason: "already_populated" };
  }
  const candidate = String(fields[MAP_BRAND.candidateBrand] || "").trim();
  if (!candidate) return { ok: false, reason: "no_candidate_brand" };

  const url = String(fields[MAP_BRAND.officialUrl] || "").trim();
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, reason: "no_official_url", class: "BRAND_UNRESOLVED" };
  }
  if (/booking\.com|expedia\.|tripadvisor\.|facebook\.com|maps\.google/i.test(url)) {
    return { ok: false, reason: "forbidden_website_host" };
  }

  const urlFamily = familyFromOfficialUrl(url);
  if (!urlFamily) {
    return { ok: false, reason: "url_not_official_brand_host", class: "BRAND_CANDIDATE" };
  }

  const lookup = lookupCanonicalBrand(candidate, dictionary, {
    propertyName: fields[MAP_BRAND.propertyName],
    sourceUrl: url,
  });
  if (!lookup.ok || !lookup.entry) {
    return {
      ok: false,
      class: "BRAND_MAPPING_GAP",
      reason: "BRAND_MAPPING_GAP",
      brand: candidate,
    };
  }

  const entryFam = String(
    lookup.entry.parent_company || lookup.entry.brand_family || ""
  );
  const famOk =
    entryFam.toLowerCase().includes(String(urlFamily).toLowerCase()) ||
    String(urlFamily).toLowerCase().includes(entryFam.toLowerCase().split(/\s+/)[0] || "");
  if (!famOk) {
    return {
      ok: false,
      class: "BRAND_CONFLICT_REVIEW",
      reason: "candidate_family_vs_url_family_mismatch",
      candidate,
      url_family: urlFamily,
      entry_family: entryFam,
    };
  }

  const parent = parentFromLookup(lookup.entry);
  /** @type {Record<string, unknown>} */
  const patch = {
    [MAP_BRAND.currentBrand]: lookup.canonical || lookup.entry.canonical_brand_name,
    [MAP_BRAND.lastReviewed]: todayIsoDate(),
    [MAP_BRAND.enrichmentStatus]: "Partial",
  };
  if (isBlank(fields[MAP_BRAND.brandFamily]) && parent) {
    patch[MAP_BRAND.brandFamily] = parent;
  }
  if (isBlank(fields[MAP_BRAND.familySourceFamily]) && parent) {
    patch[MAP_BRAND.familySourceFamily] = parent;
  }

  return {
    ok: true,
    class: "BRAND_VALIDATED_HIGH",
    patch,
    evidence: {
      method: "candidate_brand_plus_official_url_family",
      candidate,
      url,
      url_family: urlFamily,
    },
  };
}

function emptyCompanyYield(company) {
  return {
    company,
    portfolio_rows: 0,
    attempted: 0,
    high_matches: 0,
    current_brand_writes: 0,
    brand_family_derivations: 0,
    family_derivations: 0,
    conflicts: 0,
    mapping_gaps: 0,
    yield_pct: 0,
  };
}

/**
 * Run portfolio + candidate corroboration brand validation against in-memory Census.
 *
 * @param {{
 *   censusRecords: object[],
 *   dictionary?: object,
 *   log?: Function,
 * }} opts
 */
export function runMasterBrandPortfolioValidation(opts = {}) {
  const log = opts.log || (() => {});
  const dictionary =
    opts.dictionary || buildCanonicalBrandDictionary({});
  const { rows: portfolioRows, loaded } = loadOfficialPortfolioRows();
  log(
    `[brand-portfolio] loaded ${portfolioRows.length} official portfolio rows (${loaded.map((l) => `${l.company}:${l.count}`).join(", ")})`
  );

  /** @type {Map<string, ReturnType<typeof emptyCompanyYield>>} */
  const byCompany = new Map();
  for (const l of loaded) {
    byCompany.set(l.company, {
      ...emptyCompanyYield(l.company),
      portfolio_rows: l.count,
    });
  }

  const tallies = {
    portfolio_rows: portfolioRows.length,
    current_brand_writes: 0,
    brand_family_derivations: 0,
    family_derivations: 0,
    brand_validations_high: 0,
    brand_conflicts: 0,
    brand_mapping_gaps: 0,
    candidate_corroborations: 0,
    address_fills: 0,
    postal_fills: 0,
    website_fills: 0,
    phone_fills: 0,
    newly_address_improved_ids: [],
  };

  /** @type {Map<string, { id: string, fields: Record<string, unknown>, evidence: object }>} */
  const patches = new Map();

  // Index census needing Current Brand
  const needBrand = [];
  for (const rec of opts.censusRecords || []) {
    if (!isBlank(rec.fields?.[MAP_BRAND.currentBrand])) continue;
    needBrand.push(rec);
  }

  // —— Path A: Official portfolio match ——
  for (const port of portfolioRows) {
    const yieldRow =
      byCompany.get(port.company) || emptyCompanyYield(port.company);
    byCompany.set(port.company, yieldRow);

    let best = null;
    for (const rec of needBrand) {
      if (patches.has(rec.id)) continue;
      const scored = scorePortfolioToCensus(port, rec.fields || {});
      if (scored.confidence !== "high") continue;
      if (!best || scored.score > best.scored.score) {
        best = { rec, scored };
      }
    }
    if (!best) continue;

    yieldRow.attempted += 1;
    yieldRow.high_matches += 1;
    const built = buildPortfolioBrandPatch(
      best.rec.fields || {},
      port,
      dictionary
    );
    if (built.class === "BRAND_CONFLICT_REVIEW") {
      tallies.brand_conflicts += 1;
      yieldRow.conflicts += 1;
      continue;
    }
    if (built.class === "BRAND_MAPPING_GAP") {
      tallies.brand_mapping_gaps += 1;
      yieldRow.mapping_gaps += 1;
      continue;
    }
    if (!built.ok) continue;

    patches.set(best.rec.id, {
      id: best.rec.id,
      fields: built.patch,
      evidence: built.evidence,
    });
    tallies.brand_validations_high += 1;
    tallies.current_brand_writes += 1;
    yieldRow.current_brand_writes += 1;
    if (built.patch[MAP_BRAND.brandFamily]) {
      tallies.brand_family_derivations += 1;
      yieldRow.brand_family_derivations += 1;
    }
    if (built.patch[MAP_BRAND.familySourceFamily]) {
      tallies.family_derivations += 1;
      yieldRow.family_derivations += 1;
    }
    if (built.patch[MAP_BRAND.officialUrl]) tallies.website_fills += 1;
    if (built.patch[MAP_BRAND.phone]) tallies.phone_fills += 1;
    if (built.geo_filled?.address) {
      tallies.address_fills += 1;
      tallies.newly_address_improved_ids.push(best.rec.id);
    }
    if (built.geo_filled?.postal) tallies.postal_fills += 1;
  }

  // —— Path B: Candidate Brand + Official URL ——
  for (const rec of needBrand) {
    if (patches.has(rec.id)) continue;
    const ev = evaluateCandidateBrandWithOfficialUrl(
      rec.fields || {},
      dictionary
    );
    if (ev.class === "BRAND_CONFLICT_REVIEW") {
      tallies.brand_conflicts += 1;
      continue;
    }
    if (ev.class === "BRAND_MAPPING_GAP") {
      tallies.brand_mapping_gaps += 1;
      continue;
    }
    if (!ev.ok) continue;
    patches.set(rec.id, {
      id: rec.id,
      fields: ev.patch,
      evidence: ev.evidence,
    });
    tallies.brand_validations_high += 1;
    tallies.current_brand_writes += 1;
    tallies.candidate_corroborations += 1;
    if (ev.patch[MAP_BRAND.brandFamily]) tallies.brand_family_derivations += 1;
    if (ev.patch[MAP_BRAND.familySourceFamily]) {
      tallies.family_derivations += 1;
    }
  }

  for (const y of byCompany.values()) {
    y.yield_pct =
      y.portfolio_rows > 0
        ? Math.round((1000 * y.current_brand_writes) / y.portfolio_rows) / 10
        : 0;
  }

  const topYields = [...byCompany.values()].sort(
    (a, b) => b.current_brand_writes - a.current_brand_writes
  );

  return {
    ok: true,
    version: MASTER_BRAND_PORTFOLIO_VERSION,
    loaded,
    tallies,
    TOP_BRAND_SOURCE_YIELDS: topYields,
    proposals: [...patches.values()],
    newly_address_improved_ids: tallies.newly_address_improved_ids,
  };
}
