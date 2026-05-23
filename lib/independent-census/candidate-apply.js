/**
 * Phase 3A — Map normalized candidates to Airtable staging fields and apply filters.
 * Writes only to Independent Hotel Source Candidates (via caller).
 */

import {
  CANDIDATE_FIELDS,
  REVIEW_STATUS,
  MATCH_CONFIDENCE,
  RECOMMENDED_ACTION,
  SOURCE_TYPES,
} from "./fields.js";
import { MATCH_RECOMMENDED_ACTIONS } from "./match-current-census.js";
import { normalizeKey } from "./normalize-candidate.js";

const QUALITY_TIER_ORDER = ["minimal", "low", "medium", "high"];

/** Match-report actions → Airtable Recommended Action single-select. */
const MATCH_ACTION_TO_STAGING = {
  [MATCH_RECOMMENDED_ACTIONS.LIKELY_EXISTING]: RECOMMENDED_ACTION.MERGE_WITH_CENSUS,
  [MATCH_RECOMMENDED_ACTIONS.POSSIBLE_DUPLICATE_REVIEW]: RECOMMENDED_ACTION.NEEDS_RESEARCH,
  [MATCH_RECOMMENDED_ACTIONS.LIKELY_NEW_CANDIDATE]: RECOMMENDED_ACTION.NEEDS_RESEARCH,
  [MATCH_RECOMMENDED_ACTIONS.NEEDS_RESEARCH]: RECOMMENDED_ACTION.NEEDS_RESEARCH,
  [MATCH_RECOMMENDED_ACTIONS.SKIP_MISSING_NAME]: RECOMMENDED_ACTION.SKIP,
};

export function candidateDuplicateKey(sourceType, sourceRecordId, importBatchId) {
  return `${normalizeKey(sourceType)}|${String(sourceRecordId).trim()}|${String(importBatchId).trim()}`;
}

export function mapMatchActionToStagingAction(matchRecommendedAction) {
  if (!matchRecommendedAction) return RECOMMENDED_ACTION.NEEDS_RESEARCH;
  return (
    MATCH_ACTION_TO_STAGING[matchRecommendedAction] || RECOMMENDED_ACTION.NEEDS_RESEARCH
  );
}

export function parseMinQualityTier(minQuality) {
  const q = normalizeKey(minQuality);
  if (!q) return null;
  const idx = QUALITY_TIER_ORDER.indexOf(q);
  if (idx < 0) throw new Error(`Invalid --min-quality "${minQuality}". Use: minimal, low, medium, high`);
  return q;
}

export function meetsMinQuality(candidate, minQualityTier) {
  if (!minQualityTier) return true;
  const tier = normalizeKey(candidate.qualityTier) || "minimal";
  const idx = QUALITY_TIER_ORDER.indexOf(tier);
  const minIdx = QUALITY_TIER_ORDER.indexOf(minQualityTier);
  return idx >= minIdx;
}

export function parseExcludeActions(excludeActionsStr) {
  if (!excludeActionsStr) return new Set();
  return new Set(
    excludeActionsStr
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean)
  );
}

/**
 * @param {object} matchReport
 * @returns {Map<string, object>}
 */
export function indexMatchReport(matchReport) {
  const map = new Map();
  if (!matchReport?.matches) return map;
  for (const m of matchReport.matches) {
    if (m.sourceRecordId) map.set(String(m.sourceRecordId), m);
  }
  return map;
}

function safeUrl(value) {
  const v = String(value ?? "").trim();
  if (!v) return null;
  if (/^https?:\/\//i.test(v)) return v;
  return null;
}

function parsePayloadJson(candidate) {
  try {
    return JSON.parse(candidate.rawPayloadJson || "{}");
  } catch {
    return {};
  }
}

/**
 * Merge match metadata into payload (no Hotel Census link fields).
 */
export function enrichPayloadWithMatch(candidate, matchRow) {
  const base = parsePayloadJson(candidate);
  if (!matchRow) {
    return { ...base, stagingApply: { appliedAt: new Date().toISOString() } };
  }
  return {
    ...base,
    stagingApply: { appliedAt: new Date().toISOString() },
    censusMatchReadOnly: {
      matchConfidence: matchRow.matchConfidence || MATCH_CONFIDENCE.NONE,
      matchScore: matchRow.matchScore ?? null,
      matchReason: matchRow.matchReason || "",
      matchRecommendedAction: matchRow.recommendedAction || "",
      matchedCensusRecordId: matchRow.matchedCensusRecordId || "",
      matchedCensusName: matchRow.matchedCensusName || "",
      matchedCensusCity: matchRow.matchedCensusCity || "",
      distanceMeters: matchRow.distanceMeters ?? null,
      note: "Read-only comparison metadata. No link to Hotel Census created.",
    },
  };
}

/**
 * @param {object} candidate
 * @param {object|null} matchRow
 */
export function candidateToAirtableFields(candidate, matchRow = null) {
  const matchConfidence =
    matchRow?.matchConfidence ||
    candidate.possibleMatchConfidence ||
    MATCH_CONFIDENCE.NONE;

  const stagingRecommendedAction = matchRow
    ? mapMatchActionToStagingAction(matchRow.recommendedAction)
    : candidate.recommendedAction || RECOMMENDED_ACTION.NEEDS_RESEARCH;

  const payload = enrichPayloadWithMatch(candidate, matchRow);

  const fields = {
    [CANDIDATE_FIELDS.sourceName]: candidate.sourceName || "",
    [CANDIDATE_FIELDS.sourceType]: candidate.sourceType || SOURCE_TYPES.OSM,
    [CANDIDATE_FIELDS.sourceLicense]: candidate.sourceLicense || "",
    [CANDIDATE_FIELDS.sourceRecordId]: candidate.sourceRecordId || "",
    [CANDIDATE_FIELDS.rawHotelName]: candidate.rawHotelName || "",
    [CANDIDATE_FIELDS.rawAddress]: candidate.rawAddress || "",
    [CANDIDATE_FIELDS.rawCity]: candidate.rawCity || "",
    [CANDIDATE_FIELDS.rawCountry]: candidate.rawCountry || "",
    [CANDIDATE_FIELDS.rawPhone]: candidate.rawPhone || "",
    [CANDIDATE_FIELDS.rawBrand]: candidate.rawBrand || "",
    [CANDIDATE_FIELDS.rawPayloadJson]: JSON.stringify(payload),
    [CANDIDATE_FIELDS.importBatchId]: candidate.importBatchId || "",
    [CANDIDATE_FIELDS.importedAt]: candidate.importedAt || new Date().toISOString(),
    [CANDIDATE_FIELDS.reviewStatus]: candidate.reviewStatus || REVIEW_STATUS.PENDING,
    [CANDIDATE_FIELDS.possibleMatchConfidence]: matchConfidence,
    [CANDIDATE_FIELDS.recommendedAction]: stagingRecommendedAction,
    [CANDIDATE_FIELDS.candidateDedupeKey]: candidate.candidateDedupeKey || "",
  };

  const sourceUrl = safeUrl(candidate.sourceUrl);
  if (sourceUrl) fields[CANDIDATE_FIELDS.sourceUrl] = sourceUrl;

  const rawWebsite = safeUrl(candidate.rawWebsite);
  if (rawWebsite) fields[CANDIDATE_FIELDS.rawWebsite] = rawWebsite;

  if (Number.isFinite(candidate.rawLatitude)) {
    fields[CANDIDATE_FIELDS.rawLatitude] = candidate.rawLatitude;
  }
  if (Number.isFinite(candidate.rawLongitude)) {
    fields[CANDIDATE_FIELDS.rawLongitude] = candidate.rawLongitude;
  }

  return fields;
}

/**
 * @param {Array<object>} candidates
 * @param {object} options
 */
export function selectCandidatesForApply(candidates, options = {}) {
  const {
    minQualityTier = null,
    excludeActions = new Set(),
    sourceType = SOURCE_TYPES.OSM,
    matchBySourceId = new Map(),
  } = options;

  const selected = [];
  const skippedByQuality = [];
  const skippedByAction = [];
  const skippedBySourceType = [];

  for (const c of candidates) {
    if (sourceType && normalizeKey(c.sourceType) !== normalizeKey(sourceType)) {
      skippedBySourceType.push(c);
      continue;
    }
    if (!meetsMinQuality(c, minQualityTier)) {
      skippedByQuality.push(c);
      continue;
    }
    const matchRow = matchBySourceId.get(c.sourceRecordId);
    const matchAction = normalizeKey(matchRow?.recommendedAction || "");
    if (matchAction && excludeActions.has(matchAction)) {
      skippedByAction.push({ candidate: c, matchAction: matchRow?.recommendedAction });
      continue;
    }
    selected.push({ candidate: c, matchRow: matchRow || null });
  }

  return {
    selected,
    skippedByQuality,
    skippedByAction,
    skippedBySourceType,
  };
}

/**
 * Load existing candidate keys for batch from Airtable.
 * @param {import('airtable').Base} base
 * @param {string} batchId
 */
export async function loadExistingCandidateKeys(base, batchId, tableName) {
  const keys = new Set();
  const formula = `{${CANDIDATE_FIELDS.importBatchId}} = '${String(batchId).replace(/'/g, "\\'")}'`;

  await new Promise((resolve, reject) => {
    base(tableName)
      .select({
        filterByFormula: formula,
        fields: [
          CANDIDATE_FIELDS.sourceType,
          CANDIDATE_FIELDS.sourceRecordId,
          CANDIDATE_FIELDS.importBatchId,
        ],
      })
      .eachPage(
        (records, fetchNextPage) => {
          for (const rec of records) {
            const f = rec.fields;
            keys.add(
              candidateDuplicateKey(
                f[CANDIDATE_FIELDS.sourceType],
                f[CANDIDATE_FIELDS.sourceRecordId],
                f[CANDIDATE_FIELDS.importBatchId]
              )
            );
          }
          fetchNextPage();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return keys;
}

const AIRTABLE_CREATE_CHUNK = 10;

/**
 * @param {import('airtable').Base} base
 * @param {string} tableName
 * @param {Array<{ candidate: object, matchRow: object|null }>} rows
 * @param {Set<string>} existingKeys
 */
export async function createCandidateRecords(base, tableName, rows, existingKeys) {
  const toWrite = [];
  const skippedDuplicate = [];

  for (const row of rows) {
    const { candidate } = row;
    const key = candidateDuplicateKey(
      candidate.sourceType,
      candidate.sourceRecordId,
      candidate.importBatchId
    );
    if (existingKeys.has(key)) {
      skippedDuplicate.push({ candidate, key });
      continue;
    }
    toWrite.push(row);
  }

  const created = [];
  for (let i = 0; i < toWrite.length; i += AIRTABLE_CREATE_CHUNK) {
    const chunk = toWrite.slice(i, i + AIRTABLE_CREATE_CHUNK);
    const payload = chunk.map(({ candidate, matchRow }) => ({
      fields: candidateToAirtableFields(candidate, matchRow),
    }));
    const records = await base(tableName).create(payload, { typecast: true });
    for (const rec of records) {
      created.push({
        airtableRecordId: rec.id,
        sourceRecordId: rec.fields[CANDIDATE_FIELDS.sourceRecordId],
      });
      const c = chunk.find((x) => x.candidate.sourceRecordId === rec.fields[CANDIDATE_FIELDS.sourceRecordId]);
      if (c) {
        existingKeys.add(
          candidateDuplicateKey(
            c.candidate.sourceType,
            c.candidate.sourceRecordId,
            c.candidate.importBatchId
          )
        );
      }
    }
  }

  return { created, skippedDuplicate, writtenCount: created.length };
}
