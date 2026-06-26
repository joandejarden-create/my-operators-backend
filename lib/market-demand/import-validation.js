/**
 * Market Demand — import preview validation and duplicate detection.
 */

import { DEMAND_CATEGORY_LABELS } from "./airtable-market-demand-fields.js";

/** Categories allowed on import (matches Demand Categories reference + Other). */
export const VALID_DEMAND_CATEGORIES = [
  ...Object.values(DEMAND_CATEGORY_LABELS),
  "Other",
];

const VALID_CATEGORY_SET = new Set(
  VALID_DEMAND_CATEGORIES.map((c) => c.toLowerCase())
);

/**
 * @param {string} name
 */
export function normalizeDemandCenterName(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Simple similarity: ratio of matching chars in shorter vs longer (Dice-style bigrams).
 * @param {string} a
 * @param {string} b
 */
export function nameSimilarity(a, b) {
  const x = normalizeDemandCenterName(a);
  const y = normalizeDemandCenterName(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  if (x.length >= 5 && y.length >= 5 && (x.includes(y) || y.includes(x))) return 0.92;

  const bigrams = (s) => {
    const out = new Map();
    for (let i = 0; i < s.length - 1; i += 1) {
      const bg = s.slice(i, i + 2);
      out.set(bg, (out.get(bg) || 0) + 1);
    }
    return out;
  };
  const bx = bigrams(x);
  const by = bigrams(y);
  let overlap = 0;
  for (const [bg, count] of bx) {
    overlap += Math.min(count, by.get(bg) || 0);
  }
  const total = [...bx.values()].reduce((a, c) => a + c, 0) + [...by.values()].reduce((a, c) => a + c, 0);
  return total ? (2 * overlap) / total : 0;
}

/**
 * @param {object} candidate
 * @param {object} existing — normalized demand center record
 */
export function isDuplicateCandidate(candidate, existing) {
  const candName = normalizeDemandCenterName(candidate.name);
  const existName = normalizeDemandCenterName(existing.name);
  const candPlace = String(candidate.sourcePlaceId || "").trim();
  const existPlace = String(existing.sourcePlaceId || "").trim();

  if (candName && existName && candName === existName) {
    return { duplicate: true, reason: "same_normalized_name" };
  }
  if (candPlace && existPlace && candPlace === existPlace) {
    return { duplicate: true, reason: "same_source_place_id" };
  }

  const candCat = String(candidate.category || "").trim().toLowerCase();
  const existCat = String(existing.category || "").trim().toLowerCase();
  if (candCat && existCat && candCat === existCat && candName && existName) {
    const sim = nameSimilarity(candidate.name, existing.name);
    if (sim >= 0.85) {
      return { duplicate: true, reason: "similar_name_same_category", similarity: sim };
    }
  }

  return { duplicate: false };
}

/**
 * @param {object} item
 * @param {number} index
 */
export function validateImportItem(item, index) {
  const errors = [];
  const warnings = [];

  if (!item || typeof item !== "object") {
    return { index, valid: false, errors: ["must be an object"], warnings: [] };
  }

  const name = String(item.name || "").trim();
  const category = String(item.category || "").trim();

  if (!name) errors.push("name is required");
  if (!category) errors.push("category is required");
  else if (!VALID_CATEGORY_SET.has(category.toLowerCase())) {
    errors.push(`category "${category}" is not in the allowed reference list`);
  }

  if (item.relevanceScore != null && item.relevanceScore !== "") {
    const n = Number(item.relevanceScore);
    if (!Number.isFinite(n) || n < 0 || n > 100) {
      warnings.push("relevanceScore should be between 0 and 100");
    }
  }

  return {
    index,
    valid: errors.length === 0,
    errors,
    warnings,
    normalized: errors.length
      ? null
      : {
          name,
          category,
          subcategory: String(item.subcategory || "").trim() || undefined,
          distanceFromDeal: item.distanceFromDeal,
          estimatedDriveTime: item.estimatedDriveTime,
          demandStrength: item.demandStrength,
          relevanceToHotelDemand: item.relevanceToHotelDemand,
          demandPattern: item.demandPattern,
          relevantHotelTypes: item.relevantHotelTypes,
          source: item.source,
          sourceReference: item.sourceReference,
          sourcePlaceId: item.sourcePlaceId,
          dataConfidence: item.dataConfidence,
          relevanceScore: item.relevanceScore,
          notes: item.notes,
          address: item.address,
          latitude: item.latitude,
          longitude: item.longitude,
        },
  };
}

/**
 * Build preview rows for import UI.
 * @param {object[]} items — raw import payload items
 * @param {object[]} existingCenters — normalized existing records for deal
 */
export function buildImportPreview(items, existingCenters = []) {
  const existing = existingCenters || [];
  const accepted = [];
  const rejected = [];
  const warnings = [];
  const duplicateCandidates = [];
  const previewRows = [];

  (items || []).forEach((item, index) => {
    const validation = validateImportItem(item, index);
    const row = {
      index,
      item: validation.normalized || item,
      errors: validation.errors,
      warnings: [...validation.warnings],
      importStatus: "rejected",
      duplicateOf: null,
      selected: validation.valid,
    };

    if (!validation.valid) {
      rejected.push({ index, errors: validation.errors });
      previewRows.push(row);
      return;
    }

    for (const ex of existing) {
      const dup = isDuplicateCandidate(validation.normalized, ex);
      if (dup.duplicate) {
        row.warnings.push(`Possible duplicate of existing record "${ex.name}" (${dup.reason})`);
        row.duplicateOf = { id: ex.id, name: ex.name, reason: dup.reason };
        duplicateCandidates.push({
          index,
          name: validation.normalized.name,
          existingId: ex.id,
          existingName: ex.name,
          reason: dup.reason,
        });
        break;
      }
    }

    for (let j = 0; j < index; j += 1) {
      const prior = previewRows[j];
      if (!prior || prior.importStatus === "rejected" || !prior.item) continue;
      const dup = isDuplicateCandidate(validation.normalized, prior.item);
      if (dup.duplicate) {
        row.warnings.push(`Possible duplicate of import row ${j + 1} (${dup.reason})`);
        if (!row.duplicateOf) {
          row.duplicateOf = { index: j, name: prior.item.name, reason: dup.reason };
        }
        break;
      }
    }

    if (row.warnings.length) {
      row.importStatus = "warning";
      warnings.push({ index, warnings: row.warnings });
      accepted.push({ index, item: validation.normalized, warnings: row.warnings });
    } else {
      row.importStatus = "accepted";
      accepted.push({ index, item: validation.normalized });
    }

    previewRows.push(row);
  });

  return {
    previewRows,
    accepted,
    rejected,
    warnings,
    duplicateCandidates,
    summary: {
      total: items?.length || 0,
      acceptedCount: accepted.length,
      rejectedCount: rejected.length,
      warningCount: warnings.length,
      duplicateCandidateCount: duplicateCandidates.length,
    },
  };
}

/**
 * Filter items to selected indices for save.
 * @param {object[]} items
 * @param {number[]} selectedIndices
 */
export function filterSelectedImportItems(items, selectedIndices) {
  const set = new Set((selectedIndices || []).map((n) => Number(n)));
  return (items || []).filter((_, i) => set.has(i));
}
