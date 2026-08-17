/**
 * Wikidata source for independent hotel census (Phase 2E).
 * DRY-RUN ONLY — SPARQL via Wikidata Query Service. License: CC0.
 */

import { SOURCE_TYPES } from "../fields.js";
import {
  REVIEW_STATUS,
  MATCH_CONFIDENCE,
  RECOMMENDED_ACTION,
} from "../fields.js";
import { buildIndependentCandidate, normalizeKey, normalizeText } from "../normalize-candidate.js";
import { getSourceProfile } from "../source-registry.js";
import { normalizeCountry } from "../match-current-census.js";

export const WIKIDATA_SOURCE_NAME = "Wikidata";
export const WIKIDATA_SOURCE_LICENSE = "CC0";
export const WIKIDATA_ENTITY_BASE = "https://www.wikidata.org/wiki/";

const WDQS_URL =
  process.env.WIKIDATA_QUERY_URL || "https://query.wikidata.org/sparql";

/** Lodging-related instance types (hotel and subclasses). */
export const WIKIDATA_LODGING_TYPES = [
  "Q27686", // hotel
  "Q654772", // hostel
  "Q216212", // motel
  "Q367914", // inn
  "Q26986606", // resort hotel
  "Q93342462", // tourist accommodation
];

/** Country label → Wikidata QID (expand as needed). */
export const COUNTRY_WIKIDATA_QID = {
  "dominican republic": "Q786",
  mexico: "Q96",
  jamaica: "Q766",
  "costa rica": "Q800",
  "puerto rico": "Q1183",
  "united states": "Q30",
  "united states of america": "Q30",
  canada: "Q16",
  brazil: "Q155",
  colombia: "Q739",
  spain: "Q29",
  france: "Q142",
};

const USER_AGENT =
  "DealalityIndependentCensus/1.0 (dry-run; contact: ops@dealality.local)";

function countryQid(countryName) {
  const k = normalizeCountry(countryName);
  return COUNTRY_WIKIDATA_QID[k] || null;
}

/**
 * @param {{ country: string, city?: string, limit?: number }} opts
 */
export function buildWikidataSparqlQuery(opts) {
  const qid = countryQid(opts.country);
  if (!qid) {
    throw new Error(
      `No Wikidata country QID mapped for "${opts.country}". Add to COUNTRY_WIKIDATA_QID.`
    );
  }

  const limit = Math.min(Math.max(opts.limit || 500, 1), 5000);
  const typeValues = WIKIDATA_LODGING_TYPES.map((t) => `wd:${t}`).join(" ");

  let cityFilter = "";
  if (opts.city) {
    const cityEsc = opts.city.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
    cityFilter = `
    OPTIONAL { ?item wdt:P131 ?loc .
      ?loc rdfs:label ?locLabel .
      FILTER(LANG(?locLabel) = "en" || LANG(?locLabel) = "es") }
    FILTER(!BOUND(?locLabel) || CONTAINS(LCASE(STR(?locLabel)), LCASE("${cityEsc}")))
    `;
  }

  return `
SELECT ?item ?itemLabel ?itemDescription ?coord ?locLabel ?website ?operatorLabel ?ownerLabel ?inception ?wikipedia WHERE {
  VALUES ?hotelType { ${typeValues} }
  ?item wdt:P31/wdt:P279* ?hotelType .
  ?item wdt:P17 wd:${qid} .
  OPTIONAL { ?item wdt:P625 ?coord }
  OPTIONAL { ?item wdt:P131 ?loc . ?loc rdfs:label ?locLabel . FILTER(LANG(?locLabel) = "en" || LANG(?locLabel) = "es") }
  OPTIONAL { ?item wdt:P856 ?website }
  OPTIONAL { ?item wdt:P137 ?op . ?op rdfs:label ?operatorLabel . FILTER(LANG(?operatorLabel) = "en" || LANG(?operatorLabel) = "es") }
  OPTIONAL { ?item wdt:P127 ?ow . ?ow rdfs:label ?ownerLabel . FILTER(LANG(?ownerLabel) = "en" || LANG(?ownerLabel) = "es") }
  OPTIONAL { ?item wdt:P571 ?inception }
  OPTIONAL {
    ?wikipedia schema:about ?item ;
             schema:isPartOf <https://en.wikipedia.org/> .
  }
  ${cityFilter}
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,es". }
}
LIMIT ${limit}
`.trim();
}

export function qidFromUri(uri) {
  const m = String(uri).match(/(Q\d+)\s*$/i);
  return m ? m[1].toUpperCase() : "";
}

export function parseWikidataCoord(coord) {
  if (!coord) return { lat: null, lng: null };
  const s = String(coord);
  const point = s.match(/Point\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)/i);
  if (point) {
    return { lat: Number(point[2]), lng: Number(point[1]) };
  }
  const parts = s.split(/\s+/).map(Number).filter(Number.isFinite);
  if (parts.length >= 2) {
    return { lat: parts[1], lng: parts[0] };
  }
  return { lat: null, lng: null };
}

function bindingValue(row, key) {
  const b = row[key];
  if (!b) return "";
  return b.value ?? "";
}

/**
 * @param {Array<object>} bindings SPARQL results.bindings
 */
export function normalizeWikidataBindings(bindings, ctx) {
  const profile = getSourceProfile(SOURCE_TYPES.WIKIDATA);
  const seenQids = new Set();
  const candidates = [];

  for (const row of bindings) {
    const itemUri = bindingValue(row, "item");
    const qid = qidFromUri(itemUri);
    if (!qid || seenQids.has(qid)) continue;
    seenQids.add(qid);

    const { lat, lng } = parseWikidataCoord(bindingValue(row, "coord"));
    const label = normalizeText(bindingValue(row, "itemLabel"));
    const description = normalizeText(bindingValue(row, "itemDescription"));
    const locLabel = normalizeText(bindingValue(row, "locLabel"));
    const website = normalizeText(bindingValue(row, "website"));
    const operator = normalizeText(bindingValue(row, "operatorLabel"));
    const owner = normalizeText(bindingValue(row, "ownerLabel"));
    const inception = bindingValue(row, "inception");
    const wikipedia = bindingValue(row, "wikipedia");

    const rawPayload = {
      qid,
      label,
      description,
      locatedInAdmin: locLabel,
      website,
      operator,
      owner,
      inception: inception || null,
      wikipediaUrl: wikipedia || null,
      entityUrl: `${WIKIDATA_ENTITY_BASE}${qid}`,
      instanceTypes: WIKIDATA_LODGING_TYPES,
      countryQuery: ctx.country,
      cityQuery: ctx.city || null,
    };

    const missingFieldFlags = {
      missingName: !normalizeKey(label),
      missingCity: !normalizeKey(locLabel),
      missingCountry: !normalizeKey(ctx.defaultCountry),
      missingCoordinates: !Number.isFinite(lat) || !Number.isFinite(lng),
      missingWebsite: !normalizeKey(website),
      missingOperator: !normalizeKey(operator),
      missingOwner: !normalizeKey(owner),
    };
    const missingFields = Object.entries(missingFieldFlags)
      .filter(([, v]) => v)
      .map(([k]) => k);

    const candidate = buildIndependentCandidate({
      sourceName: profile?.sourceName || WIKIDATA_SOURCE_NAME,
      sourceType: SOURCE_TYPES.WIKIDATA,
      sourceLicense: profile?.sourceLicense || WIKIDATA_SOURCE_LICENSE,
      sourceUrl: `${WIKIDATA_ENTITY_BASE}${qid}`,
      sourceRecordId: qid,
      rawHotelName: label,
      rawAddress: "",
      rawCity: locLabel,
      rawCountry: ctx.defaultCountry || "",
      rawLatitude: lat,
      rawLongitude: lng,
      rawWebsite: website,
      rawPhone: "",
      rawBrand: operator || "",
      rawPayload,
      importBatchId: ctx.batchId,
      importedAt: ctx.importedAt,
      reviewStatus: REVIEW_STATUS.PENDING,
      possibleMatchConfidence: MATCH_CONFIDENCE.NONE,
      recommendedAction: RECOMMENDED_ACTION.NEEDS_RESEARCH,
      internalMeta: {
        _wikidataQid: qid,
        _wikidataDescription: description,
        _wikidataOperator: operator,
        _wikidataOwner: owner,
        _wikidataWikipediaUrl: wikipedia,
        _hasWikipediaUrl: !!normalizeKey(wikipedia),
        _hasOperator: !missingFieldFlags.missingOperator,
        _hasOwner: !missingFieldFlags.missingOwner,
        missingFieldFlags,
        missingFields,
      },
    });

    candidates.push({
      ...candidate,
      missingFieldFlags: {
        ...candidate.missingFieldFlags,
        ...missingFieldFlags,
      },
      missingFields: [
        ...new Set([...(candidate.missingFields || []), ...missingFields]),
      ],
    });
  }

  return candidates;
}

export async function fetchWikidataSparql(query, options = {}) {
  const fetchFn = options.fetchFn || globalThis.fetch;
  if (!fetchFn) throw new Error("fetch is not available");

  const maxAttempts = options.maxAttempts ?? 3;
  const headers = {
    Accept: "application/sparql-results+json",
    "User-Agent": USER_AGENT,
    "Content-Type": "application/x-www-form-urlencoded",
  };
  const body = `format=json&query=${encodeURIComponent(query)}`;

  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetchFn(WDQS_URL, { method: "POST", headers, body });
      const text = await res.text();
      if (!res.ok) {
        lastErr = new Error(
          `Wikidata Query Service HTTP ${res.status}: ${text.slice(0, 500)}`
        );
        if (res.status >= 500 && attempt < maxAttempts) {
          await sleep(2000 * attempt);
          continue;
        }
        throw lastErr;
      }

      let json;
      try {
        json = JSON.parse(text);
      } catch {
        throw new Error(`Wikidata returned non-JSON: ${text.slice(0, 300)}`);
      }

      return json.results?.bindings || [];
    } catch (e) {
      lastErr = e;
      if (attempt < maxAttempts) {
        await sleep(2000 * attempt);
        continue;
      }
      throw e;
    }
  }
  throw lastErr;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {{ country: string, city?: string, limit?: number, batchId: string }} opts
 */
export async function fetchWikidataHotelCandidates(opts) {
  const query = buildWikidataSparqlQuery(opts);
  const bindings = await fetchWikidataSparql(query, opts);
  const importedAt = new Date().toISOString();
  const candidates = normalizeWikidataBindings(bindings, {
    country: opts.country,
    city: opts.city,
    batchId: opts.batchId,
    defaultCountry: opts.country,
    importedAt,
  });

  return { query, bindings, candidates, rawCount: bindings.length };
}

function incrementCount(map, key) {
  const k = key || "(blank)";
  map.set(k, (map.get(k) || 0) + 1);
}

function countPresent(candidates, pred) {
  return candidates.filter(pred).length;
}

/**
 * Phase 2E summary for dry-run reports.
 */
export function summarizeWikidataCandidates(candidates, extras = {}) {
  const byCountry = new Map();
  const byCity = new Map();
  const byQualityTier = new Map();

  for (const c of candidates) {
    incrementCount(byCountry, c.rawCountry);
    incrementCount(byCity, c.rawCity);
    incrementCount(byQualityTier, c.qualityTier || "unknown");
  }

  const toSortedObj = (map) =>
    Object.fromEntries([...map.entries()].sort((a, b) => b[1] - a[1]));

  const total = candidates.length;

  return {
    total,
    withCoordinates: countPresent(
      candidates,
      (c) => Number.isFinite(c.rawLatitude) && Number.isFinite(c.rawLongitude)
    ),
    withWebsite: countPresent(candidates, (c) => normalizeKey(c.rawWebsite)),
    withOperator: countPresent(candidates, (c) => c._hasOperator),
    withOwner: countPresent(candidates, (c) => c._hasOwner),
    withWikipediaUrl: countPresent(candidates, (c) => c._hasWikipediaUrl),
    missingName: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || c._missingFieldFlags)?.missingName
    ),
    missingCity: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || {})?.missingCity
    ),
    missingCountry: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || {})?.missingCountry
    ),
    missingCoordinates: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || {})?.missingCoordinates
    ),
    missingWebsite: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || {})?.missingWebsite
    ),
    missingOperator: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || {})?.missingOperator
    ),
    missingOwner: countPresent(
      candidates,
      (c) => (c.missingFieldFlags || {})?.missingOwner
    ),
    byCountry: toSortedObj(byCountry),
    byCity: toSortedObj(byCity),
    byQualityTier: toSortedObj(byQualityTier),
    sparqlBindings: extras.rawCount ?? null,
    geography: extras.geography || null,
    sourcePolicy: extras.sourcePolicy || null,
  };
}
