/**
 * Residual multi-field property research (Mode B).
 * Rank by missing-field value × identity confidence × web-resolution likelihood.
 */
import { MAP_MASTER } from "./master-census-enrichment-v1.js";
import { MAP_BRAND } from "./master-brand-portfolio-validation-v1.js";
import {
  researchPropertyPage,
  buildPropertyFundamentalsPatch,
  buildDeterministicGeoPatch,
  classifyNullFill,
} from "./property-fundamentals-enrichment-v1.js";
import { evaluateCoordinateCompletionEligibility } from "./census-coordinate-completion.js";
import { isForbiddenHost, extractFactSheetLinks, fetchOfficialText } from "./official-domain-crawler-v1.js";
import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
} from "./production-census-rooms-keys-extractor.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";

export const RESIDUAL_RESEARCH_VERSION = "residual-property-research-v1";

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

const VALUE_WEIGHTS = Object.freeze({
  currentBrand: 1.4,
  rooms: 1.35,
  address: 1.0,
  postal: 0.95,
  coords: 0.7,
  website: 0.7,
  state: 0.5,
  phone: 0.25,
  city: 0.15,
});

export function missingCoreFields(fields = {}) {
  return {
    currentBrand: isBlank(fields[MAP_MASTER.currentBrand]),
    rooms: isBlank(fields[MAP_MASTER.roomsKeys]),
    address: isBlank(fields[MAP_MASTER.address]),
    postal: isBlank(fields[MAP_MASTER.postalCode]),
    coords:
      isBlank(fields[MAP_MASTER.latitude]) || isBlank(fields[MAP_MASTER.longitude]),
    website: isBlank(fields[MAP_MASTER.officialUrl]),
    state: isBlank(fields[MAP_MASTER.stateRegion]),
    phone: isBlank(fields[MAP_MASTER.phone]),
    city: isBlank(fields[MAP_MASTER.city]),
  };
}

export function scoreResidualProperty(rec) {
  const f = rec.fields || {};
  const miss = missingCoreFields(f);
  const missingCount = Object.values(miss).filter(Boolean).length;
  if (missingCount === 0) return { score: 0, missingCount, miss, skip: true };
  // Phone-only residuals are cheap/low priority
  if (missingCount === 1 && miss.phone) {
    return { score: 0.1, missingCount, miss, skip: false, phone_only: true };
  }

  let value = 0;
  for (const [k, w] of Object.entries(VALUE_WEIGHTS)) {
    if (miss[k]) value += w;
  }

  let identity = 0.4;
  if (!isBlank(f[MAP_MASTER.propertyName]) && !isBlank(f[MAP_MASTER.country])) {
    identity += 0.3;
  }
  if (!isBlank(f[MAP_MASTER.city])) identity += 0.2;
  if (!isBlank(f[MAP_MASTER.address])) identity += 0.1;

  let likelihood = 0.2;
  const url = f[MAP_MASTER.officialUrl] || f[MAP_BRAND.officialUrl];
  if (url && !isForbiddenHost(url)) likelihood += 0.5;
  if (!isBlank(f[MAP_BRAND.candidateBrand])) likelihood += 0.25;

  return {
    score: value * identity * likelihood * missingCount,
    missingCount,
    miss,
    skip: false,
  };
}

export function rankResidualQueue(censusRecords, opts = {}) {
  const max = Number(opts.max || 80);
  const ranked = [];
  for (const rec of censusRecords || []) {
    const s = scoreResidualProperty(rec);
    if (s.skip || s.phone_only) continue;
    ranked.push({ rec, ...s });
  }
  ranked.sort((a, b) => b.score - a.score);
  return ranked.slice(0, max);
}

function countFilledFromPatch(patch) {
  const keys = [
    MAP_MASTER.currentBrand,
    MAP_MASTER.roomsKeys,
    MAP_MASTER.address,
    MAP_MASTER.postalCode,
    MAP_MASTER.officialUrl,
    MAP_MASTER.phone,
    MAP_MASTER.city,
    MAP_MASTER.stateRegion,
  ];
  return keys.filter((k) => patch[k] != null).length;
}

/**
 * Residual research: harvest every independently supported NULL field
 * from one property lookup.
 */
export async function runResidualPropertyResearch(opts = {}) {
  const log = opts.log || (() => {});
  const records = opts.censusRecords || [];
  const max = Number(opts.maxProperties || 30);
  const researchFn = opts.researchFn || researchPropertyPage;
  const queue = rankResidualQueue(records, { max });
  const proposals = [];
  const newlyEligibleCoords = [];
  let researched = 0;
  let multiField = 0;
  let websiteWrites = 0;
  let addressWrites = 0;

  for (const item of queue) {
    researched += 1;
    try {
      const result = await researchFn(item.rec, opts);
      if (!result?.ok && !result?.extract) continue;
      const built = buildPropertyFundamentalsPatch(
        item.rec,
        result.extract || result
      );
      const geo = buildDeterministicGeoPatch({
        ...item.rec,
        fields: { ...(item.rec.fields || {}), ...(built.patch || {}) },
      });
      const patch = { ...(built.patch || {}), ...(geo.patch || {}) };
      const filled = countFilledFromPatch(patch);
      if (filled >= 2) multiField += 1;
      if (patch[MAP_MASTER.officialUrl]) websiteWrites += 1;
      if (patch[MAP_MASTER.address]) addressWrites += 1;
      if (Object.keys(patch).length) {
        proposals.push({ id: item.rec.id, fields: patch });
        const mergedFields = { ...(item.rec.fields || {}), ...patch };
        const elig = evaluateCoordinateCompletionEligibility(
          { id: item.rec.id, fields: mergedFields },
          { masterFounderApprovedPathway: true }
        );
        const before = evaluateCoordinateCompletionEligibility(item.rec, {
          masterFounderApprovedPathway: true,
        });
        if (elig.eligible && !before.eligible && patch[MAP_MASTER.address]) {
          newlyEligibleCoords.push(item.rec.id);
        }
      }
    } catch (err) {
      log(
        `[residual] ${item.rec.id} ${String(err?.message || err).slice(0, 120)}`
      );
    }
  }

  return {
    ok: true,
    version: RESIDUAL_RESEARCH_VERSION,
    RESIDUAL_PROPERTIES_RESEARCHED: researched,
    RESIDUAL_PROPERTIES_WITH_2_PLUS_FIELDS_FILLED: multiField,
    WEBSITE_PATCHES: websiteWrites,
    ADDRESS_PATCHES: addressWrites,
    NEWLY_ELIGIBLE_COORDINATES: newlyEligibleCoords.length,
    newly_eligible_coordinate_ids: newlyEligibleCoords,
    proposals,
    exhausted: queue.length < max && researched > 0 && proposals.length === 0,
  };
}

/**
 * Official PDF / fact-sheet discovery for properties that already have an official URL.
 */
export async function runOfficialFactSheetDiscovery(opts = {}) {
  const log = opts.log || (() => {});
  const records = opts.censusRecords || [];
  const max = Number(opts.maxProperties || 20);
  const fetchFn = opts.fetchFn || fetchOfficialText;
  const proposals = [];
  let researched = 0;

  const candidates = records.filter((r) => {
    const f = r.fields || {};
    const url = f[MAP_MASTER.officialUrl];
    return url && !isForbiddenHost(url) && isBlank(f[MAP_ROOMS.roomsKeys]);
  }).slice(0, max);

  for (const rec of candidates) {
    researched += 1;
    const url = rec.fields[MAP_MASTER.officialUrl];
    try {
      const page = await fetchFn(url);
      if (!page.ok) continue;
      const links = extractFactSheetLinks(page.text, page.url || url);
      const htmlHits = extractRoomsKeysFromOfficialHtml(page.text, {
        url,
        propertyName: rec.fields[MAP_MASTER.propertyName],
      });
      const best = selectBestRoomsHit(
        Array.isArray(htmlHits) ? htmlHits : htmlHits?.hits || []
      );
      /** @type {Record<string, unknown>} */
      const patch = {};
      if (best?.confidence === "High") {
        const fill = classifyNullFill(rec.fields[MAP_ROOMS.roomsKeys], best.count);
        if (fill.write) patch[MAP_ROOMS.roomsKeys] = best.count;
      }
      if (Object.keys(patch).length) {
        proposals.push({ id: rec.id, fields: patch });
      }
    } catch (err) {
      log(`[factsheet] ${rec.id} ${String(err?.message || err).slice(0, 100)}`);
    }
  }

  return {
    ok: true,
    proposals,
    researched,
    exhausted: researched === 0 || (researched > 0 && proposals.length === 0),
  };
}

void MAP_BRAND;
