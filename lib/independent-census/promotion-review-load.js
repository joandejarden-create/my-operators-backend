/**
 * Phase 4B — Read-only Airtable loaders for promotion review.
 */

import {
  CANDIDATES_TABLE,
  EVIDENCE_TABLE,
  CANDIDATE_FIELDS,
  EVIDENCE_FIELDS,
} from "./fields.js";
import { getIndependentCensusBase } from "./platform-base.js";
import { normalizeKey } from "./match-current-census.js";
import { mapCandidateRecord, mapEvidenceRecord } from "./promotion-review.js";

/**
 * @param {string} evidenceBatchId
 * @param {{ dedupePrefix?: string }} [opts] — "4A" (Wikidata) or "4Q" (Choice brand-directory)
 */
export async function loadEvidenceByBatch(evidenceBatchId, opts = {}) {
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const prefix = opts.dedupePrefix || "4A";
  const batchEsc = String(evidenceBatchId).replace(/'/g, "\\'");
  const prefixEsc = String(prefix).replace(/'/g, "\\'");
  const formula = `FIND("${prefixEsc}|${batchEsc}|", {Name}) > 0`;

  const records = [];
  await new Promise((resolve, reject) => {
    base(EVIDENCE_TABLE)
      .select({
        filterByFormula: formula,
        fields: [
          "Name",
          EVIDENCE_FIELDS.candidate,
          EVIDENCE_FIELDS.evidenceType,
          EVIDENCE_FIELDS.evidenceUrl,
          EVIDENCE_FIELDS.evidenceText,
          EVIDENCE_FIELDS.capturedAt,
          EVIDENCE_FIELDS.capturedBy,
          EVIDENCE_FIELDS.matchScore,
          EVIDENCE_FIELDS.matchReason,
        ],
      })
      .eachPage(
        (page, next) => {
          records.push(...page);
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  return records.map(mapEvidenceRecord);
}

/**
 * @param {string[]} recordIds
 * @param {{ candidateBatchId?: string, candidateSourceType?: string }} [opts]
 */
export async function loadCandidatesByIds(recordIds, opts = {}) {
  const candidateBatchId =
    typeof opts === "string" ? opts : opts.candidateBatchId || "";
  const candidateSourceType =
    typeof opts === "object" && opts.candidateSourceType
      ? opts.candidateSourceType
      : "";
  const base = getIndependentCensusBase();
  if (!base) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const unique = [...new Set(recordIds.filter(Boolean))];
  const map = new Map();

  const fields = [
    CANDIDATE_FIELDS.sourceName,
    CANDIDATE_FIELDS.sourceType,
    CANDIDATE_FIELDS.sourceLicense,
    CANDIDATE_FIELDS.sourceUrl,
    CANDIDATE_FIELDS.sourceRecordId,
    CANDIDATE_FIELDS.rawHotelName,
    CANDIDATE_FIELDS.rawAddress,
    CANDIDATE_FIELDS.rawCity,
    CANDIDATE_FIELDS.rawCountry,
    CANDIDATE_FIELDS.rawLatitude,
    CANDIDATE_FIELDS.rawLongitude,
    CANDIDATE_FIELDS.rawWebsite,
    CANDIDATE_FIELDS.rawPhone,
    CANDIDATE_FIELDS.rawBrand,
    CANDIDATE_FIELDS.rawPayloadJson,
    CANDIDATE_FIELDS.importBatchId,
    CANDIDATE_FIELDS.reviewStatus,
    CANDIDATE_FIELDS.possibleMatchConfidence,
    CANDIDATE_FIELDS.recommendedAction,
    CANDIDATE_FIELDS.candidateDedupeKey,
  ];

  for (let i = 0; i < unique.length; i += 20) {
    const chunk = unique.slice(i, i + 20);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    await new Promise((resolve, reject) => {
      base(CANDIDATES_TABLE)
        .select({ filterByFormula: formula, fields })
        .eachPage(
          (page, next) => {
            for (const rec of page) {
              const c = mapCandidateRecord(rec);
              if (
                candidateBatchId &&
                c.importBatchId &&
                c.importBatchId !== candidateBatchId
              ) {
                continue;
              }
              if (
                candidateSourceType &&
                normalizeKey(c.sourceType) !== normalizeKey(candidateSourceType)
              ) {
                continue;
              }
              map.set(rec.id, c);
            }
            next();
          },
          (err) => (err ? reject(err) : resolve())
        );
    });
  }

  return map;
}
