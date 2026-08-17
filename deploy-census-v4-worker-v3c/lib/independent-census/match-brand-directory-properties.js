/**
 * Phase 4K — Read-only match: brand-directory property URLs ↔ staging candidates + verified.
 */

import { readFileSync, existsSync } from "fs";
import {
  CANDIDATES_TABLE,
  VERIFIED_TABLE,
  CANDIDATE_FIELDS,
  VERIFIED_FIELDS,
  SOURCE_TYPES,
} from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { mapCandidateRecord } from "./promotion-review.js";
import { parseCoords } from "./match-current-census.js";
import {
  nameSimilarity,
  websiteHost,
  countriesMatch,
  citiesMatch,
  normalizeText,
  normalizeKey,
  normalizeCountry,
} from "./match-current-census.js";

export const MATCH_CONFIDENCE = ["high", "medium", "low", "none"];

export const PROPERTY_MATCH_ACTIONS = {
  LINK_CANDIDATE: "link_to_existing_candidate_review",
  LINK_VERIFIED: "link_to_existing_verified_review",
  CREATE_NEW: "create_new_candidate_after_policy_review",
  MANUAL: "needs_manual_review",
  UNMATCHED_BRAND: "hold_unmatched_brand_review",
  EXCLUDE_NON_CALA: "exclude_non_cala",
};

const CHOICE_HOST = "choicehotels.com";

export function normalizeCitySlug(slug) {
  return normalizeKey(String(slug || "").replace(/-/g, " "));
}

export function deriveInferredHotelName(choice) {
  const brand =
    choice.matchedBrandSetupBrand ||
    choice.inferredBrandName ||
    titleFromSlug(choice.brandSlug);
  const city = titleFromSlug(choice.citySlug);
  if (brand && city) return `${brand} ${city}`;
  if (brand) return brand;
  return city || choice.propertyId || "";
}

function titleFromSlug(slug) {
  return normalizeText(String(slug || "").replace(/-/g, " "));
}

export function loadPropertyUrlExtractReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const rows = Array.isArray(data.propertyRows) ? data.propertyRows : [];
  return { data, rows };
}

/**
 * Rows eligible for Phase 4K matching.
 */
export function filterChoicePropertiesForMatch(rows) {
  return rows.filter((r) => {
    const calaOk =
      r.calaFilterStatus === "included" || r.calaFilterStatus === "likely";
    const actionOk = r.recommendedAction === "ready_for_candidate_review";
    return calaOk && actionOk;
  });
}

function mapVerifiedRecord(record) {
  const f = record.fields || {};
  const lat = f[VERIFIED_FIELDS.verifiedLatitude];
  const lng = f[VERIFIED_FIELDS.verifiedLongitude];
  return {
    airtableRecordId: record.id,
    verifiedHotelName: normalizeText(f[VERIFIED_FIELDS.verifiedHotelName]),
    verifiedCity: normalizeText(f[VERIFIED_FIELDS.verifiedCity]),
    verifiedCountry: normalizeText(f[VERIFIED_FIELDS.verifiedCountry]),
    verifiedWebsite: normalizeText(f[VERIFIED_FIELDS.verifiedWebsite]),
    verifiedBrandLabel: normalizeText(f[VERIFIED_FIELDS.verifiedBrandLabel]),
    websiteHost: websiteHost(f[VERIFIED_FIELDS.verifiedWebsite]),
    countryNorm: normalizeCountry(f[VERIFIED_FIELDS.verifiedCountry]),
    verifiedLatitude: Number.isFinite(lat) ? lat : null,
    verifiedLongitude: Number.isFinite(lng) ? lng : null,
  };
}

/**
 * @param {{ countryFilter?: string }} [opts]
 */
export async function loadVerifiedReadOnly(opts = {}) {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const fields = [
    VERIFIED_FIELDS.verifiedHotelName,
    VERIFIED_FIELDS.verifiedCity,
    VERIFIED_FIELDS.verifiedCountry,
    VERIFIED_FIELDS.verifiedWebsite,
    VERIFIED_FIELDS.verifiedBrandLabel,
    VERIFIED_FIELDS.verifiedLatitude,
    VERIFIED_FIELDS.verifiedLongitude,
    VERIFIED_FIELDS.active,
  ];

  const records = await base(VERIFIED_TABLE).select({ fields, pageSize: 100 }).all();
  let rows = records.map(mapVerifiedRecord);

  if (opts.countryFilter) {
    const want = normalizeCountry(opts.countryFilter);
    rows = rows.filter((r) => r.countryNorm === want);
  }

  return { totalLoaded: records.length, rows };
}

/**
 * Optional: all candidates in batch (not only OSM type) for broader pool.
 */
export async function loadCandidateBatchReadOnly(importBatchId) {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const batchEsc = String(importBatchId).replace(/'/g, "\\'");
  const formula = `{${CANDIDATE_FIELDS.importBatchId}} = '${batchEsc}'`;

  const fields = [
    CANDIDATE_FIELDS.sourceRecordId,
    CANDIDATE_FIELDS.sourceType,
    CANDIDATE_FIELDS.rawHotelName,
    CANDIDATE_FIELDS.rawCity,
    CANDIDATE_FIELDS.rawCountry,
    CANDIDATE_FIELDS.rawWebsite,
    CANDIDATE_FIELDS.rawBrand,
    CANDIDATE_FIELDS.importBatchId,
  ];

  const rows = [];
  await new Promise((resolve, reject) => {
    base(CANDIDATES_TABLE)
      .select({ filterByFormula: formula, fields })
      .eachPage(
        (records, fetchNextPage) => {
          for (const rec of records) {
            const mapped = mapCandidateRecord(rec);
            rows.push({
              airtableRecordId: rec.id,
              sourceRecordId: mapped.sourceRecordId,
              sourceType: mapped.sourceType,
              rawHotelName: mapped.rawHotelName,
              rawCity: mapped.rawCity,
              rawCountry: mapped.rawCountry,
              rawWebsite: mapped.rawWebsite,
              rawBrand: mapped.rawBrand,
              websiteHost: websiteHost(mapped.rawWebsite),
              countryNorm: normalizeCountry(mapped.rawCountry),
              importBatchId: mapped.importBatchId,
            });
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return { totalLoaded: rows.length, rows, importBatchId };
}

/** Phase 4M CALA OSM expansion batches include this suffix in Import Batch ID. */
export const CALA_OSM_EXPANSION_BATCH_SUFFIX = "choice-cala-2026-05-20";

const STAGING_CANDIDATE_FIELDS = [
  CANDIDATE_FIELDS.sourceRecordId,
  CANDIDATE_FIELDS.sourceName,
  CANDIDATE_FIELDS.sourceType,
  CANDIDATE_FIELDS.rawHotelName,
  CANDIDATE_FIELDS.rawCity,
  CANDIDATE_FIELDS.rawCountry,
  CANDIDATE_FIELDS.rawLatitude,
  CANDIDATE_FIELDS.rawLongitude,
  CANDIDATE_FIELDS.rawWebsite,
  CANDIDATE_FIELDS.rawBrand,
  CANDIDATE_FIELDS.importBatchId,
];

function escapeFormulaString(s) {
  return String(s).replace(/'/g, "\\'");
}

function mapToStagingCandidateRow(rec) {
  const mapped = mapCandidateRecord(rec);
  const coords = parseCoords(mapped.rawLatitude, mapped.rawLongitude);
  return {
    airtableRecordId: rec.id,
    sourceRecordId: mapped.sourceRecordId,
    sourceType: mapped.sourceType,
    sourceName: mapped.sourceName,
    rawHotelName: mapped.rawHotelName,
    rawCity: mapped.rawCity,
    rawCountry: mapped.rawCountry,
    rawWebsite: mapped.rawWebsite,
    rawBrand: mapped.rawBrand,
    websiteHost: websiteHost(mapped.rawWebsite),
    countryNorm: normalizeCountry(mapped.rawCountry),
    importBatchId: mapped.importBatchId,
    coords,
  };
}

export function parseCountryFilterList(countryFilterStr) {
  return String(countryFilterStr || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function parseBatchIdList(batchIdsStr) {
  return String(batchIdsStr || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function parseRetentionIncludeList(includeStr) {
  const defaults = [
    "keep_high_priority",
    "enrich_next",
    "keep_for_matching",
  ];
  const raw = String(includeStr || "").trim();
  const list = raw
    ? raw.split(",").map((s) => normalizeKey(s.trim())).filter(Boolean)
    : defaults.map(normalizeKey);
  return new Set(list);
}

export const DEFAULT_EXCLUDED_RETENTION = new Set(
  [
    "low_priority_hold",
    "duplicate_review",
    "possible_archive_later",
  ].map(normalizeKey)
);

export function loadCandidateRetentionReport(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const rows = Array.isArray(data.candidateRows) ? data.candidateRows : [];
  return { data, rows, summary: data.summary || {} };
}

/**
 * Build Airtable record IDs to include from Phase 4O retention rows.
 */
export function buildIncludedCandidateIds(retentionRows, includeSet, excludeSet) {
  const ids = new Set();
  const countsByRetention = {};

  for (const r of retentionRows) {
    const key = normalizeKey(r.retentionRecommendation);
    if (!key) continue;
    countsByRetention[key] = (countsByRetention[key] || 0) + 1;
    if (!includeSet.has(key)) continue;
    if (excludeSet.has(key)) continue;
    ids.add(r.airtableRecordId);
  }

  const includedCounts = {};
  for (const key of includeSet) {
    if (countsByRetention[key]) includedCounts[key] = countsByRetention[key];
  }

  return { ids, includedCounts, countsByRetention };
}

/**
 * Phase 4P — Load full CALA OSM pool, filter to retention subset (read-only).
 */
export async function loadPrioritizedCandidatePool(opts) {
  const {
    retentionReportPath,
    includeRetention = "",
    excludeRetention = null,
    allOsmCandidates = true,
    sourceTypeFilter = SOURCE_TYPES.OSM,
  } = opts;

  const retention = loadCandidateRetentionReport(retentionReportPath);
  const includeSet = parseRetentionIncludeList(includeRetention);
  const excludeSet = excludeRetention
    ? new Set(
        String(excludeRetention)
          .split(",")
          .map((s) => normalizeKey(s.trim()))
          .filter(Boolean)
      )
    : DEFAULT_EXCLUDED_RETENTION;

  const { ids, includedCounts, countsByRetention } = buildIncludedCandidateIds(
    retention.rows,
    includeSet,
    excludeSet
  );

  const full = await loadOsmCandidatesReadOnly({
    allOsmCandidates,
    sourceTypeFilter,
  });

  const beforeCount = full.totalLoaded;
  const filtered = full.rows.filter((r) => ids.has(r.airtableRecordId));
  const pool = buildCandidatePoolIndex(filtered);

  return {
    totalLoadedBeforeFilter: beforeCount,
    totalLoadedAfterFilter: filtered.length,
    poolReduction: beforeCount - filtered.length,
    poolReductionPct: beforeCount
      ? Number((((beforeCount - filtered.length) / beforeCount) * 100).toFixed(1))
      : 0,
    pool,
    rows: pool.rows,
    includeRetention: [...includeSet],
    excludeRetention: [...excludeSet],
    includedCounts,
    countsByRetention,
    retentionReportTotal: retention.rows.length,
    filterByFormula: full.filterByFormula,
    importBatchIds: pool.batchIds,
  };
}

const PHASE4N_BASELINE_PATH =
  "reports/independent-census-choice-property-match-cala-expanded-osm-2026-05-20.json";

/**
 * Compare Phase 4P match summary to Phase 4N expanded OSM run.
 */
export function compareToPhase4N(phase4nReportPath, phase4pSummary, poolMeta) {
  const path = phase4nReportPath || PHASE4N_BASELINE_PATH;
  if (!existsSync(path)) return null;

  const phase4n = JSON.parse(readFileSync(path, "utf8"));
  const base = phase4n.summary || {};
  const cur = phase4pSummary;

  const num = (k) => ({
    phase4N: base[k] ?? 0,
    phase4P: cur[k] ?? 0,
    delta: (cur[k] ?? 0) - (base[k] ?? 0),
  });

  return {
    phase4nReportPath: path,
    phase4nBatchId: phase4n.batchId,
    candidatePoolLoaded: {
      phase4N: phase4n.candidatesLoaded ?? 0,
      phase4P: poolMeta.totalLoadedAfterFilter,
      beforeRetentionFilter: poolMeta.totalLoadedBeforeFilter,
      reduction: poolMeta.poolReduction,
      reductionPct: poolMeta.poolReductionPct,
    },
    matchConfidence: {
      candidateHigh: num("candidateHigh"),
      candidateMedium: num("candidateMedium"),
      candidateLow: num("candidateLow"),
      candidateNone: num("candidateNone"),
    },
    outcomes: {
      linkToCandidateReview: num("linkToCandidateReview"),
      needsManualReview: num("needsManualReview"),
      likelyNewOfficialSourceOpportunities: num("likelyNewOfficialSourceOpportunities"),
      linkToVerifiedReview: num("linkToVerifiedReview"),
      createNewAfterPolicy: num("createNewAfterPolicy"),
    },
    verifiedConfidence: {
      verifiedHigh: num("verifiedHigh"),
      verifiedMedium: num("verifiedMedium"),
      verifiedLow: num("verifiedLow"),
      verifiedNone: num("verifiedNone"),
    },
  };
}

/**
 * Index candidates by normalized country for faster per-property matching.
 */
export function buildCandidatePoolIndex(rows) {
  const byCountry = new Map();
  const batchIds = new Set();
  for (const r of rows) {
    const ck = r.countryNorm || normalizeCountry(r.rawCountry) || "(unknown)";
    if (!byCountry.has(ck)) byCountry.set(ck, []);
    byCountry.get(ck).push(r);
    if (r.importBatchId) batchIds.add(r.importBatchId);
  }
  return { rows, byCountry, batchIds: [...batchIds].sort() };
}

export function candidatesForChoiceProperty(choice, pool) {
  const target =
    normalizeCountry(choice.inferredCountry) ||
    normalizeCountry(choice.countryOrRegionSegment);
  if (!target) return pool.rows;
  const bucket = pool.byCountry?.get(target);
  if (bucket?.length) return bucket;
  return pool.rows.filter((c) => countriesMatch(target, c.rawCountry));
}

function rowPassesCountryFilters(row, countryNorms) {
  if (!countryNorms?.length) return true;
  const ck = row.countryNorm || normalizeCountry(row.rawCountry);
  return countryNorms.some((want) => want === ck || countriesMatch(want, row.rawCountry));
}

/**
 * Read-only load of OSM (or other) staging candidates across batches / expansion runs.
 *
 * @param {{
 *   importBatchIds?: string[],
 *   allOsmCandidates?: boolean,
 *   sourceTypeFilter?: string,
 *   countryFilters?: string[],
 *   calaExpansionBatchSuffix?: string,
 * }} opts
 */
export async function loadOsmCandidatesReadOnly(opts = {}) {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const sourceTypeFilter = opts.sourceTypeFilter || SOURCE_TYPES.OSM;
  const importBatchIds = opts.importBatchIds || [];
  const allOsmCandidates = !!opts.allOsmCandidates;
  const calaSuffix = opts.calaExpansionBatchSuffix || CALA_OSM_EXPANSION_BATCH_SUFFIX;
  const countryNorms = (opts.countryFilters || []).map((c) => normalizeCountry(c));

  let filterByFormula;
  if (importBatchIds.length > 0) {
    const parts = importBatchIds.map(
      (id) => `{${CANDIDATE_FIELDS.importBatchId}} = '${escapeFormulaString(id)}'`
    );
    filterByFormula = parts.length === 1 ? parts[0] : `OR(${parts.join(",")})`;
  } else if (allOsmCandidates) {
    const st = escapeFormulaString(sourceTypeFilter);
    filterByFormula = `AND({${CANDIDATE_FIELDS.sourceType}} = '${st}', FIND('${escapeFormulaString(calaSuffix)}', {${CANDIDATE_FIELDS.importBatchId}}))`;
  } else {
    throw new Error(
      "Specify --all-osm-candidates, --candidate-batch-ids, or --candidate-batch-id"
    );
  }

  const rows = [];
  let totalRecordsScanned = 0;

  await new Promise((resolve, reject) => {
    base(CANDIDATES_TABLE)
      .select({ filterByFormula, fields: STAGING_CANDIDATE_FIELDS })
      .eachPage(
        (records, fetchNextPage) => {
          totalRecordsScanned += records.length;
          for (const rec of records) {
            const row = mapToStagingCandidateRow(rec);
            if (normalizeKey(row.sourceType) !== normalizeKey(sourceTypeFilter)) {
              continue;
            }
            if (!rowPassesCountryFilters(row, countryNorms)) continue;
            rows.push(row);
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  const pool = buildCandidatePoolIndex(rows);
  return {
    totalLoaded: rows.length,
    totalRecordsScanned,
    matchingPoolSize: rows.length,
    filterByFormula,
    sourceTypeFilter,
    importBatchIds: importBatchIds.length ? importBatchIds : pool.batchIds,
    countryFilters: opts.countryFilters || [],
    pool,
    rows: pool.rows,
  };
}

function brandTokens(choice) {
  const tokens = new Set();
  const add = (s) => {
    for (const t of normalizeKey(s).split(/\s+/)) {
      if (t.length > 2) tokens.add(t);
    }
  };
  add(choice.matchedBrandSetupBrand);
  add(choice.inferredBrandName);
  add(choice.brandSlug?.replace(/-/g, " "));
  return tokens;
}

function brandMatchesRecord(choice, recordName, recordBrand) {
  const tokens = brandTokens(choice);
  const hay = normalizeKey(`${recordName} ${recordBrand}`);
  if (!tokens.size) return false;
  let hits = 0;
  for (const t of tokens) {
    if (hay.includes(t)) hits++;
  }
  return hits >= Math.min(2, tokens.size) || (tokens.size === 1 && hits === 1);
}

function citySlugMatches(choice, recordCity) {
  const slugCity = normalizeCitySlug(choice.citySlug);
  if (!slugCity || !recordCity) return null;
  return citiesMatch(slugCity, recordCity);
}

function choiceUrlSignals(choice, recordWebsite, recordName) {
  const reasons = [];
  let score = 0;
  const propId = normalizeKey(choice.propertyId);
  const wh = websiteHost(recordWebsite);
  const webNorm = normalizeKey(recordWebsite);
  const nameNorm = normalizeKey(recordName);

  if (wh === CHOICE_HOST || webNorm.includes(CHOICE_HOST)) {
    score += 50;
    reasons.push("website=choicehotels.com");
  }
  if (propId && (webNorm.includes(propId) || nameNorm.includes(propId))) {
    score += 35;
    reasons.push(`propertyId=${choice.propertyId}`);
  }
  if (choice.propertyUrl && webNorm.includes(normalizeKey(choice.propertyUrl))) {
    score += 40;
    reasons.push("full_url");
  }
  return { score, reasons };
}

function scoreToConfidence(score) {
  if (score >= 70) return "high";
  if (score >= 45) return "medium";
  if (score >= 25) return "low";
  return "none";
}

function scorePropertyToCandidate(choice, candidate) {
  const reasons = [];
  let score = 0;

  const inferredName = deriveInferredHotelName(choice);
  const countryTarget =
    normalizeCountry(choice.inferredCountry) ||
    normalizeCountry(choice.countryOrRegionSegment);

  if (!countriesMatch(countryTarget, candidate.rawCountry)) {
    return {
      score: 0,
      confidence: "none",
      reason: "Country mismatch",
      inferredName,
    };
  }
  score += 8;
  reasons.push("country");

  const cityMatch = citySlugMatches(choice, candidate.rawCity);
  if (cityMatch === true) {
    score += 18;
    reasons.push("city");
  } else if (cityMatch === false) {
    score -= 8;
  }

  const ns = nameSimilarity(inferredName, candidate.rawHotelName);
  if (ns >= 0.85) {
    score += 32;
    reasons.push(`name=${ns.toFixed(2)}`);
  } else if (ns >= 0.6) {
    score += 18;
    reasons.push(`name=${ns.toFixed(2)}`);
  } else if (ns >= 0.4) {
    score += 8;
    reasons.push(`name=${ns.toFixed(2)}`);
  }

  if (brandMatchesRecord(choice, candidate.rawHotelName, candidate.rawBrand)) {
    score += 15;
    reasons.push("brand");
  }

  const urlSig = choiceUrlSignals(choice, candidate.rawWebsite, candidate.rawHotelName);
  score += urlSig.score;
  reasons.push(...urlSig.reasons);

  const confidence = scoreToConfidence(score);
  return {
    score,
    confidence,
    reason: reasons.length ? reasons.join("; ") : "Weak or no match signals",
    inferredName,
  };
}

function scorePropertyToVerified(choice, verified) {
  const reasons = [];
  let score = 0;
  const inferredName = deriveInferredHotelName(choice);
  const countryTarget =
    normalizeCountry(choice.inferredCountry) ||
    normalizeCountry(choice.countryOrRegionSegment);

  if (!countriesMatch(countryTarget, verified.verifiedCountry)) {
    return {
      score: 0,
      confidence: "none",
      reason: "Country mismatch",
      inferredName,
    };
  }
  score += 8;
  reasons.push("country");

  const cityMatch = citySlugMatches(choice, verified.verifiedCity);
  if (cityMatch === true) {
    score += 18;
    reasons.push("city");
  } else if (cityMatch === false) {
    score -= 8;
  }

  const ns = nameSimilarity(inferredName, verified.verifiedHotelName);
  if (ns >= 0.85) {
    score += 32;
    reasons.push(`name=${ns.toFixed(2)}`);
  } else if (ns >= 0.6) {
    score += 18;
    reasons.push(`name=${ns.toFixed(2)}`);
  } else if (ns >= 0.4) {
    score += 8;
    reasons.push(`name=${ns.toFixed(2)}`);
  }

  if (
    brandMatchesRecord(
      choice,
      verified.verifiedHotelName,
      verified.verifiedBrandLabel
    )
  ) {
    score += 15;
    reasons.push("brand");
  }

  const urlSig = choiceUrlSignals(
    choice,
    verified.verifiedWebsite,
    verified.verifiedHotelName
  );
  score += urlSig.score;
  reasons.push(...urlSig.reasons);

  const confidence = scoreToConfidence(score);
  return {
    score,
    confidence,
    reason: reasons.length ? reasons.join("; ") : "Weak or no match signals",
    inferredName,
  };
}

function resolveRecommendedAction(choice, candidateConf, verifiedConf) {
  if (!choice.matchedBrandSetupBrand) {
    return PROPERTY_MATCH_ACTIONS.UNMATCHED_BRAND;
  }
  if (verifiedConf === "high") return PROPERTY_MATCH_ACTIONS.LINK_VERIFIED;
  if (candidateConf === "high") return PROPERTY_MATCH_ACTIONS.LINK_CANDIDATE;
  if (verifiedConf === "medium") return PROPERTY_MATCH_ACTIONS.LINK_VERIFIED;
  if (candidateConf === "medium") return PROPERTY_MATCH_ACTIONS.LINK_CANDIDATE;
  if (verifiedConf === "low" || candidateConf === "low") {
    return PROPERTY_MATCH_ACTIONS.MANUAL;
  }
  return PROPERTY_MATCH_ACTIONS.CREATE_NEW;
}

function buildNotes(row, phase = "4N") {
  return [
    `Phase ${phase} read-only match; no Airtable writes.`,
    `Candidate: ${row.candidateMatchConfidence || "none"}.`,
    `Verified: ${row.verifiedMatchConfidence || "none"}.`,
    row.sourcePolicyStatus === "review_required"
      ? "Source policy sign-off required before candidate ingest."
      : "",
  ]
    .filter(Boolean)
    .join(" ");
}

/**
 * @param {Array<object>} choiceProperties
 * @param {{ rows: Array<object>, byCountry?: Map }} candidatePool
 * @param {{ rows: Array<object> }} verifiedPool
 * @param {{ useCountryIndex?: boolean }} [matchOpts]
 */
export function matchChoicePropertiesToStaging(
  choiceProperties,
  candidatePool,
  verifiedPool,
  matchOpts = {}
) {
  const useCountryIndex = matchOpts.useCountryIndex !== false;
  const summary = {
    choicePropertyUrlsReviewed: choiceProperties.length,
    candidateHigh: 0,
    candidateMedium: 0,
    candidateLow: 0,
    candidateNone: 0,
    verifiedHigh: 0,
    verifiedMedium: 0,
    verifiedLow: 0,
    verifiedNone: 0,
    likelyNewOfficialSourceOpportunities: 0,
    needsManualReview: 0,
    unmatchedBrandCount: 0,
    linkToCandidateReview: 0,
    linkToVerifiedReview: 0,
    createNewAfterPolicy: 0,
    byCountry: {},
    byBrand: {},
  };

  const matchRows = [];

  for (const choice of choiceProperties) {
    let bestCand = null;
    let bestCandRec = null;
    const candRows = useCountryIndex
      ? candidatesForChoiceProperty(choice, candidatePool)
      : candidatePool.rows;
    for (const c of candRows) {
      const m = scorePropertyToCandidate(choice, c);
      if (!bestCand || m.score > bestCand.score) {
        bestCand = m;
        bestCandRec = c;
      }
    }

    let bestVer = null;
    let bestVerRec = null;
    for (const v of verifiedPool.rows) {
      const m = scorePropertyToVerified(choice, v);
      if (!bestVer || m.score > bestVer.score) {
        bestVer = m;
        bestVerRec = v;
      }
    }

    const candidateConf = bestCand?.confidence || "none";
    const verifiedConf = bestVer?.confidence || "none";
    const recommendedAction = resolveRecommendedAction(choice, candidateConf, verifiedConf);

    if (candidateConf === "high") summary.candidateHigh++;
    else if (candidateConf === "medium") summary.candidateMedium++;
    else if (candidateConf === "low") summary.candidateLow++;
    else summary.candidateNone++;

    if (verifiedConf === "high") summary.verifiedHigh++;
    else if (verifiedConf === "medium") summary.verifiedMedium++;
    else if (verifiedConf === "low") summary.verifiedLow++;
    else summary.verifiedNone++;

    if (recommendedAction === PROPERTY_MATCH_ACTIONS.CREATE_NEW) {
      summary.likelyNewOfficialSourceOpportunities++;
      summary.createNewAfterPolicy++;
    } else if (recommendedAction === PROPERTY_MATCH_ACTIONS.MANUAL) {
      summary.needsManualReview++;
    } else if (recommendedAction === PROPERTY_MATCH_ACTIONS.UNMATCHED_BRAND) {
      summary.unmatchedBrandCount++;
    } else if (recommendedAction === PROPERTY_MATCH_ACTIONS.LINK_CANDIDATE) {
      summary.linkToCandidateReview++;
    } else if (recommendedAction === PROPERTY_MATCH_ACTIONS.LINK_VERIFIED) {
      summary.linkToVerifiedReview++;
    }

    const countryKey = choice.inferredCountry || choice.countryOrRegionSegment || "(unknown)";
    summary.byCountry[countryKey] = (summary.byCountry[countryKey] || 0) + 1;
    const brandKey = choice.matchedBrandSetupBrand || "(unmatched)";
    summary.byBrand[brandKey] = (summary.byBrand[brandKey] || 0) + 1;

    matchRows.push({
      parentCompany: choice.parentCompany,
      brandSetupBrand: choice.matchedBrandSetupBrand,
      propertyUrl: choice.propertyUrl,
      propertyId: choice.propertyId,
      inferredCountry: choice.inferredCountry,
      citySlug: choice.citySlug,
      inferredHotelSlug: choice.brandSlug,
      inferredHotelName: deriveInferredHotelName(choice),
      matchedCandidateRecordId: bestCandRec?.airtableRecordId || "",
      matchedCandidateName: bestCandRec?.rawHotelName || "",
      candidateMatchConfidence: candidateConf,
      candidateMatchReason: bestCand?.reason || "",
      candidateMatchScore: bestCand?.score ?? 0,
      matchedVerifiedRecordId: bestVerRec?.airtableRecordId || "",
      matchedVerifiedName: bestVerRec?.verifiedHotelName || "",
      verifiedMatchConfidence: verifiedConf,
      verifiedMatchReason: bestVer?.reason || "",
      verifiedMatchScore: bestVer?.score ?? 0,
      recommendedAction,
      sourcePolicyStatus: choice.sourcePolicy || "review_required",
      notes: "",
    });
  }

  for (const row of matchRows) {
    row.notes = buildNotes(row, matchOpts.phase || "4N");
  }

  return { rows: matchRows, summary };
}

export const MATCH_CSV_COLUMNS = [
  "Parent Company",
  "Brand Setup Brand",
  "Property URL",
  "Property ID",
  "Inferred Country",
  "City Slug",
  "Matched Candidate Record ID",
  "Matched Candidate Name",
  "Candidate Match Confidence",
  "Candidate Match Reason",
  "Matched Verified Record ID",
  "Matched Verified Name",
  "Verified Match Confidence",
  "Verified Match Reason",
  "Recommended Action",
  "Source Policy Status",
  "Notes",
];

export function matchRowToCsv(r) {
  return {
    "Parent Company": r.parentCompany,
    "Brand Setup Brand": r.brandSetupBrand,
    "Property URL": r.propertyUrl,
    "Property ID": r.propertyId,
    "Inferred Country": r.inferredCountry,
    "City Slug": r.citySlug,
    "Matched Candidate Record ID": r.matchedCandidateRecordId,
    "Matched Candidate Name": r.matchedCandidateName,
    "Candidate Match Confidence": r.candidateMatchConfidence,
    "Candidate Match Reason": r.candidateMatchReason,
    "Matched Verified Record ID": r.matchedVerifiedRecordId,
    "Matched Verified Name": r.matchedVerifiedName,
    "Verified Match Confidence": r.verifiedMatchConfidence,
    "Verified Match Reason": r.verifiedMatchReason,
    "Recommended Action": r.recommendedAction,
    "Source Policy Status": r.sourcePolicyStatus,
    Notes: r.notes,
  };
}
