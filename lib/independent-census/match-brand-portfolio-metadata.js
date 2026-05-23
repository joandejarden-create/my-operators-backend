/**
 * Match official brand/portfolio metadata to Verified, Candidates, Choice targets (read-only).
 */

import { readFileSync } from "fs";
import { normalizePropertyUrl } from "./choice-property-id-reconciliation.js";
import { loadChoiceTargetList } from "./match-choice-targets-to-osm.js";
import { loadCandidateRetentionReport } from "./match-brand-directory-properties.js";
import { mapRetentionRowToOsmCandidate } from "./match-choice-targets-to-osm.js";
import { loadVerifiedIndexWithPolicy } from "./verified-dedupe-index.js";
import { getIndependentCensusBase } from "./platform-base.js";
import {
  nameSimilarity,
  normalizeCountry,
  normalizeKey,
  normalizeText,
  parseCoords,
  distanceMeters,
  websiteHost,
} from "./match-current-census.js";

export const MATCH_BUCKET = {
  READY_CANDIDATE: "ready_for_candidate_or_evidence",
  READY_VERIFIED: "ready_for_verified_review",
  NEEDS_ENRICHMENT: "needs_osm_or_google_enrichment",
  DUPLICATE: "duplicate_or_collision_review",
  HOLD_LOW: "hold_low_confidence",
};

export function loadMetadataExtractReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const results = (data.results || []).filter(
    (r) => r.fetchStatus === "ok" && r.extractionConfidence !== "none"
  );
  return { data, results };
}

function indexByPropertyUrl(rows, urlField = "officialPropertyUrl") {
  const byUrl = new Map();
  for (const r of rows) {
    const norm = normalizePropertyUrl(r[urlField] || r.propertyUrl);
    if (norm) byUrl.set(norm, r);
  }
  return byUrl;
}

function indexChoiceTargets(targets) {
  const byUrl = new Map();
  const byId = new Map();
  for (const t of targets) {
    if (t.website) {
      const h = websiteHost(t.website);
      if (h) byUrl.set(h, t);
    }
    byId.set(t.recordId, t);
    const key = normalizeKey(`${t.name}|${t.targetCountry}|${t.city}`);
    byId.set(`key:${key}`, t);
  }
  return { byUrl, byId };
}

function indexCandidatesFromRetention(retentionPath) {
  if (!retentionPath) return { byUrl: new Map(), bySourceId: new Map() };
  const retention = loadCandidateRetentionReport(retentionPath);
  const byUrl = new Map();
  const bySourceId = new Map();
  for (const r of retention.rows) {
    const c = mapRetentionRowToOsmCandidate(r);
    const norm = normalizePropertyUrl(c.rawWebsite);
    if (norm) byUrl.set(norm, c);
    if (c.sourceRecordId) bySourceId.set(c.sourceRecordId, c);
    if (c.airtableRecordId) bySourceId.set(`rec:${c.airtableRecordId}`, c);
  }
  return { byUrl, bySourceId };
}

function scoreMetadataToTarget(meta, target) {
  const nameSim = nameSimilarity(meta.officialHotelName, target.name);
  const countryOk =
    normalizeCountry(meta.country) === normalizeCountry(target.country) ||
    normalizeCountry(meta.country) === target.targetCountry;
  const cityOk =
    !meta.city ||
    !target.city ||
    normalizeKey(meta.city).includes(normalizeKey(target.city)) ||
    normalizeKey(target.city).includes(normalizeKey(meta.city));

  const tCoords = parseCoords(target.lat, target.lng);
  const mCoords = parseCoords(meta.latitude, meta.longitude);
  const distM = tCoords && mCoords ? distanceMeters(tCoords, mCoords) : null;

  const urlMatch =
    meta.officialPropertyUrl &&
    target.website &&
    websiteHost(meta.officialPropertyUrl) === websiteHost(target.website);

  let score = Math.round(nameSim * 50);
  if (countryOk) score += 15;
  if (cityOk) score += 10;
  if (distM != null && distM <= 500) score += 20;
  if (urlMatch) score += 15;

  return { nameSim, countryOk, cityOk, distM, score, urlMatch };
}

export function classifyMetadataMatch(meta, ctx) {
  const conf = meta.extractionConfidence || "none";
  const { verifiedIndex, candidateHit, targetHit, collisionCount } = ctx;

  if (collisionCount > 1) {
    return {
      bucket: MATCH_BUCKET.DUPLICATE,
      reason: "Multiple registry rows share this official property URL",
    };
  }

  if (conf === "low") {
    return {
      bucket: MATCH_BUCKET.HOLD_LOW,
      reason: "Low extraction confidence from official page",
    };
  }

  const hasCore =
    !!normalizeKey(meta.officialHotelName) &&
    !!normalizeKey(meta.country) &&
    meta.latitude != null &&
    meta.longitude != null;

  if (!hasCore) {
    return {
      bucket: MATCH_BUCKET.NEEDS_ENRICHMENT,
      reason: "Missing name, country, or coordinates on official page",
    };
  }

  if (verifiedIndex?.candidateLinks?.size && candidateHit?.airtableRecordId) {
    if (verifiedIndex.candidateLinks.has(candidateHit.airtableRecordId)) {
      return {
        bucket: MATCH_BUCKET.DUPLICATE,
        reason: "OSM candidate already linked in Verified census",
      };
    }
  }

  if (conf === "high" && targetHit) {
    return {
      bucket: MATCH_BUCKET.READY_VERIFIED,
      reason: "High-confidence official metadata aligned to Choice legacy target",
    };
  }

  if (conf === "high" || conf === "medium") {
    return {
      bucket: MATCH_BUCKET.READY_CANDIDATE,
      reason: "Official metadata sufficient for candidate/evidence workflow",
    };
  }

  return {
    bucket: MATCH_BUCKET.NEEDS_ENRICHMENT,
    reason: "Needs OSM or later Google enrichment pass",
  };
}

/**
 * @param {object} opts
 */
export async function runBrandPortfolioMetadataMatch(opts) {
  const { results: extracted } = loadMetadataExtractReport(
    opts.metadataExtractReportPath
  );

  let targets = [];
  if (opts.choiceTargetListPath) {
    const { targets: t } = loadChoiceTargetList(opts.choiceTargetListPath);
    targets = t;
  }

  const targetIdx = indexChoiceTargets(targets);
  const metaByUrl = indexByPropertyUrl(extracted);
  const candidateIdx = indexCandidatesFromRetention(opts.retentionReportPath);

  let verifiedIndex = { candidateLinks: new Set(), dedupeKeys: new Set() };
  const base = getIndependentCensusBase();
  if (base) {
    const loaded = await loadVerifiedIndexWithPolicy(base, {
      apply: false,
      allowMissingVerifiedIndex: true,
    });
    verifiedIndex = loaded.index;
  }

  const urlCollision = new Map();
  for (const m of extracted) {
    const norm = normalizePropertyUrl(m.officialPropertyUrl);
    if (!norm) continue;
    urlCollision.set(norm, (urlCollision.get(norm) || 0) + 1);
  }

  const matchRows = [];

  for (const meta of extracted) {
    const normUrl = normalizePropertyUrl(meta.officialPropertyUrl);
    const candidateHit = normUrl ? candidateIdx.byUrl.get(normUrl) : null;

    let targetHit = null;
    if (normUrl) {
      for (const t of targets) {
        const scored = scoreMetadataToTarget(meta, t);
        if (scored.score >= 65) {
          targetHit = t;
          break;
        }
      }
    }

    const classified = classifyMetadataMatch(meta, {
      verifiedIndex,
      candidateHit,
      targetHit,
      collisionCount: urlCollision.get(normUrl) || 0,
    });

    const targetScore = targetHit
      ? scoreMetadataToTarget(meta, targetHit)
      : null;

    matchRows.push({
      propertyId: meta.propertyId,
      officialPropertyUrl: meta.officialPropertyUrl,
      officialHotelName: meta.officialHotelName,
      brand: meta.brand,
      country: meta.country,
      city: meta.city,
      extractionConfidence: meta.extractionConfidence,
      matchBucket: classified.bucket,
      matchReason: classified.reason,
      legacyTargetRecordId: targetHit?.recordId || "",
      legacyTargetName: targetHit?.name || "",
      osmCandidateRecordId: candidateHit?.airtableRecordId || "",
      targetMatchScore: targetScore?.score ?? "",
      nameSimilarity: targetScore?.nameSim ?? "",
      distanceMeters: targetScore?.distM ?? meta.distanceMeters ?? "",
    });
  }

  const bucketCounts = {};
  for (const r of matchRows) {
    bucketCounts[r.matchBucket] = (bucketCounts[r.matchBucket] || 0) + 1;
  }

  return {
    batchId: opts.batchId,
    metadataExtractReportPath: opts.metadataExtractReportPath,
    extractedRowsMatched: extracted.length,
    choiceTargetCount: targets.length,
    matchRows,
    bucketCounts,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
  };
}

export const MATCH_CSV_COLUMNS = [
  "propertyId",
  "officialHotelName",
  "officialPropertyUrl",
  "country",
  "city",
  "extractionConfidence",
  "matchBucket",
  "matchReason",
  "legacyTargetRecordId",
  "osmCandidateRecordId",
  "targetMatchScore",
];
