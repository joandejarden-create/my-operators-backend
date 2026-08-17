/**
 * Operator Intelligence — conflict detection (pure / deterministic).
 */

export const CONFLICT_SEVERITY = Object.freeze({
  HIGH: "High",
  MEDIUM: "Medium",
  LOW: "Low",
});

/**
 * Detect conflicts between an existing value and a new claim (or among claims).
 * @param {object} input
 */
export function detectClaimConflict(input = {}) {
  const {
    operatorId,
    operatorName,
    claimCategory,
    existingValue,
    newClaim,
    sources = [],
    potentialScoreImpact = "Medium",
  } = input;

  const existing = normalize(existingValue);
  const proposed = normalize(newClaim?.normalizedValue ?? newClaim?.claimValue ?? newClaim?.value);
  const presenceExisting = String(input.existingPresenceType || "");
  const presenceNew = String(newClaim?.presenceType || newClaim?.marketPresenceType || "");

  if (!existing && !proposed) {
    return nullConflict(operatorId, operatorName, claimCategory);
  }

  // Historical vs current
  if (
    (/historical/i.test(presenceExisting) && /current/i.test(presenceNew)) ||
    (/historical/i.test(String(existing)) && /current|active/i.test(String(proposed)))
  ) {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory,
      existingValue: existing,
      newClaim: proposed,
      sources,
      conflictType: "current_versus_historical",
      conflictSeverity: CONFLICT_SEVERITY.HIGH,
      potentialScoreImpact,
      recommendedDisposition: "Keep historical labeled; publish current only with primary evidence",
      humanReviewRequired: true,
    });
  }

  // Active development ≠ operating presence
  if (/active.?development|announced/i.test(presenceNew) && /current.?managed|operating.?portfolio/i.test(presenceExisting)) {
    // not necessarily conflict if both can coexist
  }
  if (
    /strategic.?interest|claimed.?capability/i.test(presenceNew) &&
    /current.?managed|operating.?portfolio/i.test(String(proposed))
  ) {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory: claimCategory || "geography",
      existingValue: existing,
      newClaim: proposed,
      sources,
      conflictType: "presence_overclaim",
      conflictSeverity: CONFLICT_SEVERITY.HIGH,
      potentialScoreImpact: "High",
      recommendedDisposition: "Downgrade to Strategic Interest / Claimed Capability",
      humanReviewRequired: true,
    });
  }

  // Country presence conflict
  if (claimCategory === "geography" && existing && proposed && existing !== proposed) {
    const exSet = toSet(existing);
    const prSet = toSet(proposed);
    const removed = [...exSet].filter((x) => !prSet.has(x));
    const added = [...prSet].filter((x) => !exSet.has(x));
    if (removed.length || (added.length && input.narrowing === false)) {
      return conflictResult({
        operatorId,
        operatorName,
        claimCategory,
        existingValue: existing,
        newClaim: proposed,
        sources,
        conflictType: "country_presence_conflict",
        conflictSeverity: removed.length ? CONFLICT_SEVERITY.HIGH : CONFLICT_SEVERITY.MEDIUM,
        potentialScoreImpact,
        recommendedDisposition: "Exception queue — reconcile Active Countries",
        humanReviewRequired: true,
        meta: { removed, added },
      });
    }
  }

  // Brand relationship — one property ≠ global approval
  if (
    claimCategory === "brand" &&
    /global.?approval|regional.?approval|approved.?operator/i.test(String(proposed)) &&
    /one.?property|single.?hotel|property.?scoped/i.test(String(newClaim?.limitations || input.limitations || ""))
  ) {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory,
      existingValue: existing,
      newClaim: proposed,
      sources,
      conflictType: "brand_relationship_overclaim",
      conflictSeverity: CONFLICT_SEVERITY.HIGH,
      potentialScoreImpact: "High",
      recommendedDisposition: "Reject global approval; keep property-scoped relationship",
      humanReviewRequired: true,
    });
  }

  // Key count conflict
  if (claimCategory === "comparable" || claimCategory === "key_count") {
    const a = Number(existing);
    const b = Number(proposed);
    if (Number.isFinite(a) && Number.isFinite(b) && a > 0 && b > 0) {
      const delta = Math.abs(a - b) / Math.max(a, b);
      if (delta > 0.15) {
        return conflictResult({
          operatorId,
          operatorName,
          claimCategory,
          existingValue: existing,
          newClaim: proposed,
          sources,
          conflictType: "key_count_conflict",
          conflictSeverity: CONFLICT_SEVERITY.MEDIUM,
          potentialScoreImpact,
          recommendedDisposition: "Prefer primary property page; note range",
          humanReviewRequired: delta > 0.35,
        });
      }
    }
  }

  // Parent company conflict
  if (claimCategory === "identity" && /parent/i.test(String(newClaim?.claimSubject || "")) && existing && proposed && existing !== proposed) {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory,
      existingValue: existing,
      newClaim: proposed,
      sources,
      conflictType: "parent_company_conflict",
      conflictSeverity: CONFLICT_SEVERITY.HIGH,
      potentialScoreImpact: "High",
      recommendedDisposition: "Human review — authoritative corp filings preferred",
      humanReviewRequired: true,
    });
  }

  // Unsupported current profile value
  if (input.currentProfileUnsupported === true) {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory,
      existingValue: existing,
      newClaim: proposed || "unsupported",
      sources,
      conflictType: "unsupported_current_value",
      conflictSeverity: CONFLICT_SEVERITY.HIGH,
      potentialScoreImpact: "High",
      recommendedDisposition: "Demote to Claimed Capability / Unknown until sourced",
      humanReviewRequired: true,
    });
  }

  // Broader claim than evidence
  if (input.evidenceScope === "property" && /portfolio|global|all.?markets/i.test(String(proposed))) {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory,
      existingValue: existing,
      newClaim: proposed,
      sources,
      conflictType: "evidence_scope_overclaim",
      conflictSeverity: CONFLICT_SEVERITY.HIGH,
      potentialScoreImpact: "High",
      recommendedDisposition: "Narrow claim to property/brand scope",
      humanReviewRequired: true,
    });
  }

  if (existing && proposed && existing !== proposed && claimCategory === "structure") {
    return conflictResult({
      operatorId,
      operatorName,
      claimCategory,
      existingValue: existing,
      newClaim: proposed,
      sources,
      conflictType: "operating_structure_conflict",
      conflictSeverity: CONFLICT_SEVERITY.MEDIUM,
      potentialScoreImpact,
      recommendedDisposition: "Union supported structures with conditions notes",
      humanReviewRequired: potentialScoreImpact === "High",
    });
  }

  return nullConflict(operatorId, operatorName, claimCategory);
}

/**
 * Scan a list of claims for pairwise / profile conflicts.
 */
export function detectConflictsForOperator({ operatorId, operatorName, claims = [], profile = {} }) {
  const out = [];
  for (const claim of claims) {
    const cat = claim.claimCategory || claim.category;
    let existingValue = null;
    if (cat === "geography") existingValue = profile.activeCountries || profile.geography;
    if (cat === "structure") existingValue = profile.managementStructuresSupported || profile.structures;
    if (cat === "identity" && /parent/i.test(String(claim.claimSubject || ""))) {
      existingValue = profile.parentCompany;
    }
    const c = detectClaimConflict({
      operatorId,
      operatorName,
      claimCategory: cat,
      existingValue,
      newClaim: claim,
      sources: claim.sources || claim.sourceIds || [],
      potentialScoreImpact: claim.potentialScoreImpact || "Medium",
      currentProfileUnsupported: claim.flags?.unsupportedCurrent === true,
      evidenceScope: claim.propertyScope ? "property" : claim.evidenceScope,
      limitations: claim.limitations,
      existingPresenceType: profile.presenceType,
    });
    if (c && c.conflictType !== "none") out.push(c);
  }
  // Deterministic sort
  out.sort((a, b) => String(a.conflictType).localeCompare(String(b.conflictType)) || String(a.newClaim).localeCompare(String(b.newClaim)));
  return out;
}

function normalize(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean).sort().join("|");
  return String(v).trim();
}

function toSet(norm) {
  return new Set(String(norm).split("|").map((x) => x.trim()).filter(Boolean));
}

function nullConflict(operatorId, operatorName, claimCategory) {
  return {
    operatorId,
    operatorName,
    claimCategory,
    existingValue: null,
    newClaim: null,
    sources: [],
    conflictType: "none",
    conflictSeverity: null,
    potentialScoreImpact: "None",
    recommendedDisposition: "No conflict",
    humanReviewRequired: false,
  };
}

function conflictResult(row) {
  return {
    ...row,
    humanReviewRequired: row.humanReviewRequired !== false,
  };
}
