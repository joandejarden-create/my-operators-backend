/**
 * Evidence access filtering by access depth.
 * Deep: subject-specific evidence. Comparative: limited benchmark substantiation only.
 */

import { ACCESS_DEPTH, isDeepAccess, isComparativeAccess } from "./access-depth.js";

export const EVIDENCE_ACCESS_VERSION = "ai_visibility_evidence_access_v1";

/** Max evidence records returned for comparative (benchmark) depth. */
export const COMPARATIVE_EVIDENCE_LIMIT = 3;

/**
 * Fields retained on comparative evidence traces.
 */
export const COMPARATIVE_EVIDENCE_FIELDS = Object.freeze([
  "evidenceId",
  "entityId",
  "entityName",
  "promptId",
  "provider",
  "model",
  "geographyScope",
  "commercialRegion",
  "batchId",
  "mentionRole",
  "presenceObserved",
  "citationCount",
]);

/**
 * @param {object[]} evidenceRecords
 * @param {{ accessDepth: string, subjectEntityId?: string|null, entitledEntityIds?: string[] }} opts
 */
export function filterEvidenceByAccessDepth(evidenceRecords, opts = {}) {
  const depth = opts.accessDepth || ACCESS_DEPTH.NONE;
  const rows = Array.isArray(evidenceRecords) ? evidenceRecords : [];

  if (depth === ACCESS_DEPTH.NONE) {
    return { ok: false, accessDepth: depth, evidence: [], reason: "UNAUTHORIZED_EVIDENCE" };
  }

  const subjectId = opts.subjectEntityId || null;
  const entitled = new Set(opts.entitledEntityIds || []);

  if (isDeepAccess(depth)) {
    const evidence = rows.filter((e) => {
      if (!subjectId) return true;
      return e.entityId === subjectId || entitled.has(e.entityId);
    });
    return {
      ok: true,
      accessDepth: depth,
      evidence,
      reason: "OWN_SUBJECT_DEEP_EVIDENCE",
      evidenceAccessVersion: EVIDENCE_ACCESS_VERSION,
    };
  }

  if (isComparativeAccess(depth)) {
    const limited = rows
      .filter((e) => !subjectId || e.entityId === subjectId)
      .slice(0, COMPARATIVE_EVIDENCE_LIMIT)
      .map((e) => pickComparativeEvidence(e));
    return {
      ok: true,
      accessDepth: depth,
      evidence: limited,
      reason: "COMPETITOR_LIMITED_EVIDENCE",
      evidenceAccessVersion: EVIDENCE_ACCESS_VERSION,
      limit: COMPARATIVE_EVIDENCE_LIMIT,
    };
  }

  return { ok: false, accessDepth: depth, evidence: [], reason: "UNAUTHORIZED_EVIDENCE" };
}

function pickComparativeEvidence(row) {
  const out = {};
  for (const key of COMPARATIVE_EVIDENCE_FIELDS) {
    if (row[key] !== undefined) out[key] = row[key];
  }
  return out;
}
