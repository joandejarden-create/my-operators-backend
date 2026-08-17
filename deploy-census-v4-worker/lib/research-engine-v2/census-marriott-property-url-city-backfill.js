/**
 * Marriott Property-Level URL + Unknown City Backfill.
 *
 * Matches Census MARSHA → official country hotel-sitemap property URL.
 * Resolves City from property URL slug / High IATA fallback only.
 * Never hotel-name-only inference, Mapbox, Google, OTA, or Webhound SoT.
 * Write target: Hotel Property Census only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  CALA_CITY_CANONICAL,
  canonicalCalaCity,
  normalizePlaceKey,
} from "./census-city-state-normalizer.js";
import {
  buildCanonicalDuplicateIndex,
  assessCanonicalDuplicateRisk,
  proposeCanonicalPropertyNameWrite,
  CANONICAL_PROPERTY_NAME_FIELD,
} from "./census-canonical-property-name.js";
import { isPropertyLevelUrl } from "./production-census-description-extraction.js";
import { ensureMarriottCalaCountrySitemapCache } from "./census-autopilot-marriott-discovery-adapter.js";
import { marshaFromMarriottWebsite } from "../marriott-brand-directory-extract.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";
import { auditAllCoreIdentityIssues } from "./census-clean-core-identity-repair.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MARRIOTT_URL_CITY_BACKFILL_VERSION =
  "census-marriott-property-url-city-backfill-v1";

export const MARRIOTT_URL_CITY_BACKFILL_STATUS = Object.freeze({
  COMPLETE: "production_census_marriott_property_url_city_backfill_complete",
  PARTIAL: "production_census_marriott_property_url_city_backfill_partial_remaining",
  BLOCKED: "production_census_marriott_property_url_city_backfill_blocked",
});

export const MARRIOTT_URL_CITY_QUEUE_ID = "core_identity_source_lookup";

/** High-confidence MARSHA IATA prefix → city (only when slug has no city token). */
export const MARRIOTT_IATA_CITY_HIGH = Object.freeze({
  CUN: { city: "Cancún", countries: ["mexico"] },
  MEX: { city: "Mexico City", countries: ["mexico"] },
  PTY: { city: "Panama City", countries: ["panama"] },
  SJO: { city: "San José", countries: ["costa rica"] },
  SMR: { city: "Santa Marta", countries: ["colombia"] },
  CUU: { city: "Chihuahua", countries: ["mexico"] },
  MID: { city: "Mérida", countries: ["mexico"] },
  PXM: { city: "Puerto Escondido", countries: ["mexico"] },
});

/** Brand / marketing tokens stripped from Marriott property URL slugs before city match. */
const MARRIOTT_SLUG_BRAND_TOKENS = Object.freeze([
  "city-express-junior",
  "city-express-plus",
  "city-express",
  "fairfield-inn-and-suites",
  "fairfield",
  "courtyard",
  "ac-hotel",
  "ac-hotels",
  "residence-inn",
  "springhill-suites",
  "towneplace-suites",
  "marriott-hotel",
  "marriott-resort",
  "marriott",
  "by-marriott",
  "the-st-regis",
  "st-regis",
  "a-member-of-design-hotels",
  "a-tribute-portfolio-resort",
  "a-tribute-portfolio",
  "tribute-portfolio",
  "autograph-collection",
  "by-royalton",
  "mystique",
  "hotel",
  "resort",
  "spa",
  "inn-and-suites",
  "inn",
  "suites",
  "plus",
  "junior",
  "aeropuerto",
  "airport",
  "zona-industrial",
  "parque",
  "galerias",
  "galerías",
  "multiplaza-mall",
  "metromall",
  "los-lagos",
  "playa-dormida",
  "hacienda",
  "costa-rica",
  "and",
  "the",
  "by",
  "a",
  "of",
  "member",
  "design",
  "hotels",
]);

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

/**
 * @param {Record<string, unknown>} fields
 */
export function isMarriottCensusRecord(fields = {}) {
  const id = String(fields[MAP_FIRST_PASS.identityKey] || fields["Property Identity Key"] || "");
  const brandFamily = String(fields["Brand Family"] || "");
  const family = String(fields[MAP_FIRST_PASS.family] || fields["Family / Source Family"] || "");
  const parent = String(fields["Parent Company"] || "");
  if (/ind_marriott_/i.test(id)) return true;
  if (/marriott/i.test(brandFamily) || /marriott/i.test(family) || /marriott/i.test(parent)) {
    return true;
  }
  return false;
}

/**
 * @param {string} url
 */
export function isMarriottSitemapOrDirectoryUrl(url) {
  const s = String(url || "").toLowerCase();
  if (!s) return true;
  if (/hotel-sitemap|\/locations\/|sitemap\.xml/i.test(s)) return true;
  if (isPropertyLevelUrl(s)) return false;
  return !/marriott\.com\/(?:en-us\/)?hotels\/[a-z0-9]+-/i.test(s);
}

/**
 * Extract MARSHA from Property Identity Key.
 * @param {string} identityKey
 */
export function marshaFromCensusIdentityKey(identityKey) {
  const m = String(identityKey || "").match(/ind_marriott_[a-z]+_([a-z0-9]+)/i);
  return m ? m[1].toUpperCase() : "";
}

/**
 * Strip brand tokens from Marriott property slug (after MARSHA).
 * @param {string} slugWithMarsha e.g. cenxo-city-express-ciudad-obregon
 */
export function stripMarriottSlugBrandTokens(slugWithMarsha) {
  let s = String(slugWithMarsha || "")
    .toLowerCase()
    .replace(/^\/+/, "")
    .replace(/\/.*$/, "");
  // Drop MARSHA prefix (5 alnum)
  s = s.replace(/^[a-z0-9]{5}-/, "");
  let prev = null;
  while (prev !== s) {
    prev = s;
    for (const tok of MARRIOTT_SLUG_BRAND_TOKENS) {
      const re = new RegExp(`(?:^|-)${tok.replace(/-/g, "[-]?")}(?=-|$)`, "gi");
      s = s.replace(re, "-");
    }
    s = s.replace(/-+/g, "-").replace(/^-|-$/g, "");
  }
  return s;
}

/**
 * Match longest known CALA city token inside a hyphen/space place string.
 * @param {string} placeSlug
 */
export function matchKnownCityFromPlaceSlug(placeSlug) {
  const raw = String(placeSlug || "")
    .toLowerCase()
    .replace(/_/g, "-")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  if (!raw) return null;

  const keys = Object.keys(CALA_CITY_CANONICAL).sort((a, b) => b.length - a.length);
  const spaced = raw.replace(/-/g, " ");
  for (const key of keys) {
    if (key.length < 4 && key !== "lima" && key !== "cali" && key !== "tula") continue;
    const keyHyphen = key.replace(/\s+/g, "-");
    const keySpace = key;
    if (
      spaced === keySpace ||
      spaced.startsWith(`${keySpace} `) ||
      spaced.endsWith(` ${keySpace}`) ||
      spaced.includes(` ${keySpace} `) ||
      raw === keyHyphen ||
      raw.startsWith(`${keyHyphen}-`) ||
      raw.endsWith(`-${keyHyphen}`) ||
      raw.includes(`-${keyHyphen}-`)
    ) {
      return CALA_CITY_CANONICAL[key];
    }
  }
  return null;
}

/**
 * High city from official Marriott property URL slug (or IATA fallback).
 * Never hotel-name-only inference.
 * @param {string} propertyUrl
 * @param {string} [country]
 */
export function tryCityFromMarriottPropertyUrl(propertyUrl, country = "") {
  const url = String(propertyUrl || "").trim();
  if (!/^https?:\/\//i.test(url) || !/marriott\.com/i.test(url)) {
    return { ok: false, reason: "not_marriott_property_url" };
  }
  if (!isPropertyLevelUrl(url) && !/\/hotels\/[a-z0-9]+-/i.test(url)) {
    return { ok: false, reason: "not_property_level" };
  }

  const marsha = (marshaFromMarriottWebsite(url) || "").toUpperCase();
  const slugMatch = url.match(/\/hotels\/([a-z0-9]+(?:-[a-z0-9-]+)*)/i);
  const fullSlug = slugMatch ? slugMatch[1].toLowerCase() : "";
  const placeSlug = stripMarriottSlugBrandTokens(fullSlug);
  const countryNorm = normalizePlaceKey(country);

  // Panama country + panama token → Panama City
  if (/panama/.test(countryNorm) && (placeSlug.includes("panama") || /panama/.test(fullSlug))) {
    return {
      ok: true,
      confidence: "High",
      city: "Panama City",
      reason: "marriott_property_url_slug_panama_city",
      place_slug: placeSlug,
      marsha,
    };
  }

  // downtown-mexico / mexico token when country Mexico → Mexico City
  if (
    /mexico/.test(countryNorm) &&
    (placeSlug === "mexico" ||
      placeSlug.includes("downtown-mexico") ||
      placeSlug === "santa-fe" ||
      /downtown-mexico/.test(fullSlug))
  ) {
    return {
      ok: true,
      confidence: "High",
      city: "Mexico City",
      reason: "marriott_property_url_slug_mexico_city",
      place_slug: placeSlug,
      marsha,
    };
  }

  const fromSlug = matchKnownCityFromPlaceSlug(placeSlug);
  if (fromSlug) {
    const canon = canonicalCalaCity(fromSlug) || fromSlug;
    return {
      ok: true,
      confidence: "High",
      city: canon,
      reason: "marriott_property_url_slug_known_city",
      place_slug: placeSlug,
      marsha,
    };
  }

  // IATA fallback when slug has no known city (Design Hotels / unique property names)
  if (marsha.length >= 3) {
    const iata = marsha.slice(0, 3);
    const hit = MARRIOTT_IATA_CITY_HIGH[iata];
    if (hit && hit.countries.some((c) => countryNorm.includes(normalizePlaceKey(c)))) {
      const hotelNameLike =
        !placeSlug ||
        /^(casa|hotel|nest|vetta|mystique|terrestre|elena|matilda|humano|sevilla|downtown|nizuc)(-|$)/i.test(
          placeSlug
        );
      if (hotelNameLike) {
        return {
          ok: true,
          confidence: "High",
          city: hit.city,
          reason: "marriott_marsha_iata_high_fallback",
          place_slug: placeSlug,
          marsha,
          iata,
        };
      }
    }
  }

  return {
    ok: false,
    reason: placeSlug ? "slug_city_not_in_known_map" : "no_slug_city_or_iata",
    place_slug: placeSlug,
    marsha,
  };
}

/**
 * Select Marriott Unknown / blank-city targets with non-property Source URL.
 * @param {object[]} censusRecords
 */
export function selectMarriottUnknownCityTargets(censusRecords = []) {
  const out = [];
  for (const rec of censusRecords) {
    const fields = rec.fields || {};
    if (!isMarriottCensusRecord(fields)) continue;
    const city = String(fields[MAP_FIRST_PASS.city] || fields.City || "").trim();
    if (city && !/^unknown$/i.test(city) && !/^n\/?a$/i.test(city)) continue;
    const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
    const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
    const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
    if (!propertyName || !brand || !country) continue;
    if (fields["Human Review Required"] === true) continue;

    const sourceUrl = String(
      fields[MAP_FIRST_PASS.sourceUrl] || fields["Source URL"] || ""
    ).trim();
    const officialUrl = String(
      fields[MAP_FIRST_PASS.officialUrl] || fields["Official Property URL"] || ""
    ).trim();
    const hasPropertyLevel =
      isPropertyLevelUrl(officialUrl) ||
      isPropertyLevelUrl(sourceUrl) ||
      /marriott\.com\/(?:en-us\/)?hotels\/[a-z0-9]+-/i.test(officialUrl || sourceUrl);

    const identityKey = String(fields[MAP_FIRST_PASS.identityKey] || "").trim();
    const marsha = marshaFromCensusIdentityKey(identityKey);
    if (!marsha) continue;

    out.push({
      record: rec,
      record_id: rec.id,
      marsha,
      identity_key: identityKey,
      property_name: propertyName,
      brand,
      country,
      city_raw: city || "Unknown",
      source_url: sourceUrl,
      official_url: officialUrl,
      needs_property_url: !hasPropertyLevel || isMarriottSitemapOrDirectoryUrl(sourceUrl),
      brand_family: String(fields["Brand Family"] || "").trim(),
    });
  }
  return out;
}

/**
 * Build High proposals for Marriott URL + city backfill.
 * @param {object} opts
 */
export async function buildMarriottPropertyUrlCityProposals(opts = {}) {
  const censusRecords = opts.censusRecords || [];
  const targets = selectMarriottUnknownCityTargets(censusRecords);
  const cache =
    opts.marriottCache ||
    (await ensureMarriottCalaCountrySitemapCache({
      countries: opts.marriottCountries || null,
      delayMs: opts.delayMs ?? 120,
      cache: opts.injectCache || null,
      force: Boolean(opts.forceRefresh),
    }));

  const dupIndex = buildCanonicalDuplicateIndex(censusRecords, { isPropertyLevelUrl });
  /** @type {object[]} */
  const proposals = [];
  /** @type {object[]} */
  const steward = [];
  /** @type {object[]} */
  const examples = [];

  const counters = {
    targets: targets.length,
    property_urls_found: 0,
    source_urls_replaced: 0,
    cities_resolved: 0,
    cities_written: 0,
    state_written: 0,
    canonical_written: 0,
    stewarded: 0,
    blocked_no_sitemap_match: 0,
    blocked_slug_unresolved: 0,
    blocked_duplicate_url: 0,
  };

  for (const t of targets) {
    const row =
      cache.get(`${t.country}|${t.marsha}`) ||
      cache.get(t.marsha) ||
      null;
    const propertyUrl = String(row?.propertyUrl || row?.website || "").trim();
    if (!propertyUrl || !/\/hotels\//i.test(propertyUrl)) {
      counters.blocked_no_sitemap_match += 1;
      steward.push({
        record_id: t.record_id,
        marsha: t.marsha,
        reason: "no_official_sitemap_property_url",
        property_name: t.property_name,
      });
      counters.stewarded += 1;
      continue;
    }

    counters.property_urls_found += 1;

    // Duplicate risk on property-level URL
    const urlRisk = assessCanonicalDuplicateRisk(
      {
        id: t.record_id,
        fields: {
          ...(t.record.fields || {}),
          "Source URL": propertyUrl,
          "Official Property URL": propertyUrl,
        },
      },
      t.property_name,
      dupIndex,
      { isPropertyLevelUrl }
    );
    const otherUrlHits = (urlRisk.hits || []).filter(
      (h) => h.type === "source_url" || h.type === "official_property_url"
    );
    if (otherUrlHits.length) {
      counters.blocked_duplicate_url += 1;
      steward.push({
        record_id: t.record_id,
        marsha: t.marsha,
        reason: "property_url_duplicate_risk",
        property_url: propertyUrl,
        hits: otherUrlHits,
      });
      counters.stewarded += 1;
      continue;
    }

    const cityTry = tryCityFromMarriottPropertyUrl(propertyUrl, t.country);
    /** @type {Record<string, unknown>} */
    const patch = {};

    if (t.needs_property_url || isMarriottSitemapOrDirectoryUrl(t.source_url)) {
      patch["Source URL"] = propertyUrl;
      counters.source_urls_replaced += 1;
    }
    if (
      isBlank(t.official_url) ||
      isMarriottSitemapOrDirectoryUrl(t.official_url) ||
      !isPropertyLevelUrl(t.official_url)
    ) {
      patch["Official Property URL"] = propertyUrl;
    }

    if (cityTry.ok && cityTry.confidence === "High") {
      counters.cities_resolved += 1;
      patch.City = cityTry.city;
      counters.cities_written += 1;
    } else {
      counters.blocked_slug_unresolved += 1;
      // Still write URL even if city unresolved — improves future source lookup
      if (!Object.keys(patch).length) {
        steward.push({
          record_id: t.record_id,
          marsha: t.marsha,
          reason: cityTry.reason || "city_unresolved",
          property_url: propertyUrl,
          place_slug: cityTry.place_slug || null,
        });
        counters.stewarded += 1;
        continue;
      }
    }

    // Canonical blank autofill when safe
    if (isBlank(t.record.fields?.[CANONICAL_PROPERTY_NAME_FIELD])) {
      const canonProp = proposeCanonicalPropertyNameWrite(t.record, dupIndex, {
        isPropertyLevelUrl,
      });
      if (canonProp.action === "autofill" && canonProp.patch) {
        Object.assign(patch, canonProp.patch);
        counters.canonical_written += 1;
      }
    }

    if (!Object.keys(patch).length) {
      steward.push({
        record_id: t.record_id,
        marsha: t.marsha,
        reason: "empty_patch",
        property_url: propertyUrl,
      });
      counters.stewarded += 1;
      continue;
    }

    const proposal = {
      record_id: t.record_id,
      queue: MARRIOTT_URL_CITY_QUEUE_ID,
      action: "propose_high_write",
      confidence: "High",
      write_allowed_now: true,
      allow_normalization_overwrite: true,
      patch,
      current_fields: Object.fromEntries(
        Object.keys(patch).map((k) => [k, t.record.fields?.[k] ?? null])
      ),
      method: cityTry.ok
        ? cityTry.reason
        : "marriott_official_sitemap_property_url_only",
      marsha: t.marsha,
      property_url: propertyUrl,
      city_resolution: cityTry,
      notes:
        "Marriott sitemap MARSHA match; City from property URL slug/IATA High only; no hotel-name inference",
    };
    proposals.push(proposal);

    if (examples.length < 25) {
      examples.push({
        record_id: t.record_id,
        property_name: t.property_name,
        marsha: t.marsha,
        before: {
          City: t.city_raw,
          "Source URL": t.source_url,
        },
        after: patch,
        method: proposal.method,
      });
    }

    if (!cityTry.ok) {
      steward.push({
        record_id: t.record_id,
        marsha: t.marsha,
        reason: cityTry.reason || "city_still_unknown_after_url",
        property_url: propertyUrl,
        place_slug: cityTry.place_slug || null,
        url_written: true,
      });
      counters.stewarded += 1;
    }
  }

  return {
    ok: true,
    version: MARRIOTT_URL_CITY_BACKFILL_VERSION,
    queue: MARRIOTT_URL_CITY_QUEUE_ID,
    airtable_writes: false,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    counters,
    proposals,
    steward_review: steward,
    examples,
    marriott_cache_meta: cache?._meta || null,
  };
}

/**
 * Filter proposals to Marriott parent when --parent-company Marriott.
 * @param {object[]} proposals
 * @param {object[]} censusRecords
 * @param {string|null} parentCompany
 */
export function filterProposalsToMarriottParent(proposals, censusRecords, parentCompany) {
  if (!parentCompany || !/marriott/i.test(String(parentCompany))) {
    return proposals;
  }
  const byId = new Map((censusRecords || []).map((r) => [r.id, r]));
  return (proposals || []).filter((p) => {
    const rec = byId.get(p.record_id);
    return rec ? isMarriottCensusRecord(rec.fields || {}) : false;
  });
}

/**
 * Build before/after report for Marriott backfill sprint.
 */
export function buildMarriottUrlCityBackfillReport(opts = {}) {
  const beforeRows = opts.beforeRows || opts.censusRecordsBefore || [];
  const afterRows = opts.afterRows || opts.censusRecordsAfter || beforeRows;
  const beforeAudit = auditAllCoreIdentityIssues(beforeRows);
  const afterAudit = auditAllCoreIdentityIssues(afterRows);

  const marriottBefore = beforeRows.filter((r) => isMarriottCensusRecord(r.fields || {}));
  const marriottAfter = afterRows.filter((r) => isMarriottCensusRecord(r.fields || {}));
  const unknownBefore = marriottBefore.filter((r) =>
    /^unknown$/i.test(String(r.fields?.City || "").trim())
  ).length;
  const unknownAfter = marriottAfter.filter((r) =>
    /^unknown$/i.test(String(r.fields?.City || "").trim())
  ).length;

  const applied = opts.applied || {
    records_fixed: 0,
    fields_written: [],
    examples: [],
  };

  let status = MARRIOTT_URL_CITY_BACKFILL_STATUS.PARTIAL;
  if (opts.blocked) status = MARRIOTT_URL_CITY_BACKFILL_STATUS.BLOCKED;
  else if (unknownAfter === 0) status = MARRIOTT_URL_CITY_BACKFILL_STATUS.COMPLETE;

  const coordBefore = beforeAudit.counters.coordinate_blocked_dirty_identity;
  const coordAfter = afterAudit.counters.coordinate_blocked_dirty_identity;

  const report = {
    ok: status !== MARRIOTT_URL_CITY_BACKFILL_STATUS.BLOCKED,
    version: MARRIOTT_URL_CITY_BACKFILL_VERSION,
    generated_at: new Date().toISOString(),
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    airtable_writes: Boolean(opts.airtable_writes),
    brand_setup_writes: false,
    brand_explorer_writes: false,
    inserts_applied: 0,
    parent_scope: "Marriott",
    paused_queues: [
      "source_discovery_inserts",
      "address_confirmation",
      "coordinate_completion",
      "phone_number_enrichment",
      "rooms_keys",
    ],
    before: {
      clean_core: beforeAudit.counters.clean_core,
      below_clean_core: beforeAudit.counters.below_clean_core,
      marriott_unknown_city: unknownBefore,
      coordinate_blocked_dirty_identity: coordBefore,
      marriott_records: marriottBefore.length,
    },
    after: {
      clean_core: afterAudit.counters.clean_core,
      below_clean_core: afterAudit.counters.below_clean_core,
      marriott_unknown_city: unknownAfter,
      coordinate_blocked_dirty_identity: coordAfter,
      marriott_records: marriottAfter.length,
    },
    applied,
    counters: opts.counters || null,
    steward_remaining: opts.steward_remaining || [],
    blocked_source_patterns: [
      "marriott_country_hotel_sitemap_as_source_url",
      "akamai_blocked_property_page_json_ld",
      "hotel_name_only_city_inference_forbidden",
    ],
    examples_before_after: applied.examples || opts.examples || [],
    next_recommended_action:
      unknownAfter === 0
        ? "Marriott Unknown city cleared — resume Clean Core gate; keep address/Mapbox paused until overall Below Clean Core shrinks further."
        : "Steward remaining Marriott Unknown cities without slug/IATA High city; optionally harvest property-page locality when Akamai allows. Keep address/Mapbox paused.",
  };

  return report;
}

export function writeMarriottUrlCityBackfillReports(report, opts = {}) {
  const reportsDir =
    opts.reportsDir || path.join(ROOT, "reports/research-engine-v2");
  const docsDir = opts.docsDir || path.join(ROOT, "docs/data-intelligence");
  const jsonPath = path.join(
    reportsDir,
    "production-census-marriott-property-url-city-backfill.json"
  );
  const mdPath = path.join(
    reportsDir,
    "production-census-marriott-property-url-city-backfill.md"
  );
  const docsPath = path.join(
    docsDir,
    "production-census-marriott-property-url-city-backfill.md"
  );

  const md = `# Production Census — Marriott Property URL + Unknown City Backfill

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Write target:** Deal Capture Platform → Hotel Property Census (\`tbl9aY5ijiuIzzWam\`)  
**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}  
**Inserts:** 0  

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Clean Core (all Census) | ${report.before.clean_core} | ${report.after.clean_core} |
| Below Clean Core | ${report.before.below_clean_core} | ${report.after.below_clean_core} |
| Marriott Unknown city | ${report.before.marriott_unknown_city} | ${report.after.marriott_unknown_city} |
| Coordinate blocked (dirty identity) | ${report.before.coordinate_blocked_dirty_identity} | ${report.after.coordinate_blocked_dirty_identity} |

## Applied

- Records fixed: ${report.applied?.records_fixed ?? 0}
- Fields written: ${(report.applied?.fields_written || []).join(", ") || "(none)"}
- Property URLs found: ${report.counters?.property_urls_found ?? "—"}
- Source URLs replaced: ${report.counters?.source_urls_replaced ?? "—"}
- Cities written: ${report.counters?.cities_written ?? "—"}
- Canonical written: ${report.counters?.canonical_written ?? "—"}
- Stewarded: ${report.counters?.stewarded ?? "—"}

## Blocked source patterns

${(report.blocked_source_patterns || []).map((p) => `- ${p}`).join("\n")}

## Examples

${(report.examples_before_after || [])
  .slice(0, 12)
  .map(
    (e) =>
      `- \`${e.record_id}\` ${e.property_name || ""}: City \`${e.before?.City}\` → \`${e.after?.City || "(url only)"}\`; Source → property URL`
  )
  .join("\n") || "- (none)"}

## Next recommended action

${report.next_recommended_action}
`;

  writeJson(jsonPath, report);
  writeText(mdPath, md);
  writeText(docsPath, md);
  if (opts.runDir) {
    writeJson(path.join(opts.runDir, "marriott-property-url-city-backfill.json"), report);
    writeText(path.join(opts.runDir, "marriott-property-url-city-backfill.md"), md);
  }
  return { jsonPath, mdPath, docsPath };
}
