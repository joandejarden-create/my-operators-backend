/**
 * Parent company conflict audit — Brand Basics normalization (P0D-A.1).
 */

import { normalizeMatchKey } from "../normalize-entities.js";
import { normalizeParent, parentKeys } from "./brand-basics-truth.js";

const LEGAL_SUFFIX_RE = /,?\s*(Inc\.?|LLC|Ltd\.?|Corporation|Hotels Group)$/i;

/**
 * Classify a parent conflict row from Brand Basics audit fixture.
 */
export function classifyParentConflict(row) {
  const raw = row.CURRENT_PARENT || row.parentCompanyRaw || "";
  const canonical = row.CANONICAL_PARENT || row.parentCompany || "";
  if (!raw || !canonical) {
    return { conflictType: "NONE", classification: "NO_CONFLICT" };
  }

  const rawNorm = normalizeMatchKey(normalizeParent(raw));
  const canonNorm = normalizeMatchKey(normalizeParent(canonical));

  if (rawNorm === canonNorm) {
    return {
      conflictType: "DISPLAY_ALIAS",
      classification: "NORMALIZATION_VARIATION",
      rawParentValue: raw,
      canonicalParentValue: canonical,
      conflictSource: "Brand Basics fixture vs canonical parent",
    };
  }

  const rawStripped = normalizeMatchKey(String(raw).replace(LEGAL_SUFFIX_RE, "").trim());
  const canonStripped = normalizeMatchKey(String(canonical).replace(LEGAL_SUFFIX_RE, "").trim());
  if (rawStripped === canonStripped || rawNorm.includes(canonNorm) || canonNorm.includes(rawNorm)) {
    return {
      conflictType: "LEGAL_ENTITY_SUFFIX",
      classification: "NORMALIZATION_VARIATION",
      rawParentValue: raw,
      canonicalParentValue: canonical,
      conflictSource: "Brand Basics legal suffix / display form",
    };
  }

  const aliasPairs = [
    ["ihg", "intercontinental hotels group"],
    ["hilton", "hilton worldwide"],
    ["accor", "accorhotels"],
    ["choice hotels", "choice hotels international"],
    ["marriott international", "marriott"],
  ];
  for (const [a, b] of aliasPairs) {
    if (
      (rawNorm.includes(a) && canonNorm.includes(b)) ||
      (rawNorm.includes(b) && canonNorm.includes(a))
    ) {
      return {
        conflictType: "PARENT_ALIAS",
        classification: "NORMALIZATION_VARIATION",
        rawParentValue: raw,
        canonicalParentValue: canonical,
        conflictSource: "Known parent alias pair",
      };
    }
  }

  return {
    conflictType: "SOURCE_MISMATCH",
    classification: "GENUINE_CONFLICT",
    rawParentValue: raw,
    canonicalParentValue: canonical,
    conflictSource: "Unresolved parent canonical mismatch",
  };
}

/**
 * Audit all parent conflicts from basics index.
 */
export function auditParentCompanyConflicts(basicsIndex) {
  const rows = [];
  let normalizationVariation = 0;
  let genuineConflict = 0;
  let unresolved = 0;

  for (const conflict of basicsIndex.conflicts || []) {
    const row = basicsIndex.byId.get(conflict.brandId);
    const classified = classifyParentConflict({
      CURRENT_PARENT: conflict.raw || conflict.fixture,
      CANONICAL_PARENT: conflict.canonical || conflict.live,
      brandId: conflict.brandId,
      brandName: row?.brandName,
    });
    rows.push({
      brand: row?.brandName || conflict.brandId,
      brandId: conflict.brandId,
      rawParentValue: classified.rawParentValue,
      canonicalParentValue: classified.canonicalParentValue,
      conflictSource: classified.conflictSource,
      conflictType: classified.conflictType,
      classification: classified.classification,
    });
    if (classified.classification === "NORMALIZATION_VARIATION") normalizationVariation += 1;
    else if (classified.classification === "GENUINE_CONFLICT") {
      genuineConflict += 1;
      unresolved += 1;
    }
  }

  // Also scan fixture rows with NORMALIZATION_REQUIRED flag
  for (const [, row] of basicsIndex.byId) {
    if (row.parentCompanyRaw && row.parentCompany) {
      const rawNorm = normalizeMatchKey(normalizeParent(row.parentCompanyRaw));
      const canonNorm = normalizeMatchKey(normalizeParent(row.parentCompany));
      if (rawNorm !== canonNorm) {
        const exists = rows.some((r) => r.brandId === row.brandId);
        if (!exists) {
          const classified = classifyParentConflict({
            CURRENT_PARENT: row.parentCompanyRaw,
            CANONICAL_PARENT: row.parentCompany,
            brandId: row.brandId,
            brandName: row.brandName,
          });
          rows.push({
            brand: row.brandName,
            brandId: row.brandId,
            rawParentValue: classified.rawParentValue,
            canonicalParentValue: classified.canonicalParentValue,
            conflictSource: classified.conflictSource,
            conflictType: classified.conflictType,
            classification: classified.classification,
          });
          if (classified.classification === "NORMALIZATION_VARIATION") normalizationVariation += 1;
          else {
            genuineConflict += 1;
            unresolved += 1;
          }
        }
      }
    }
  }

  return {
    totalReported: rows.length,
    normalizationVariation,
    genuineConflict,
    unresolved,
    rows,
    safeForTruthLayer: unresolved === 0,
  };
}
