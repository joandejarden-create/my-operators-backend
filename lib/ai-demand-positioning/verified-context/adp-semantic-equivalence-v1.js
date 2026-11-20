/**
 * Governed semantic-equivalence model for ADP attribute concepts.
 * Versioned rules shared by coverage diagnostics and measurement shortlists.
 * Does not silently merge or delete selections.
 */

export const ADP_SEMANTIC_EQUIVALENCE_V1 = "ADP_SEMANTIC_EQUIVALENCE_V1";

/** Version pin for inventory hashes / Airtable configuration fields. */
export const ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION =
  "ADP_SEMANTIC_EQUIVALENCE_RULES_V1_1";

export const SEMANTIC_RELATION = Object.freeze({
  TRUE_DUPLICATE: "TRUE_DUPLICATE",
  BROADER_NARROWER: "BROADER_NARROWER",
  RELATED_BUT_DISTINCT: "RELATED_BUT_DISTINCT",
  DISTINCT: "DISTINCT",
});

/**
 * Governed pairs/groups.
 * Broader/narrower do NOT automatically count separately (founder 2026-11-19).
 * RELATED_BUT_DISTINCT count separately only when independentlyMeasurable=true.
 */
export const SEMANTIC_CONCEPT_REGISTRY = Object.freeze([
  {
    conceptId: "beach_frontage",
    members: [
      { key: "beachfront", role: "canonical" },
      {
        key: "oceanfront",
        role: "near_synonym",
        relationToCanonical: SEMANTIC_RELATION.TRUE_DUPLICATE,
        independentlyMeasurable: false,
        note: "Same beach-frontage claim for Spice/Grand Anse — count once",
      },
      {
        key: "beach_access",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
        note: "Access vs frontage can be tested separately when both selected",
      },
      {
        key: "private_beach",
        role: "narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
      },
      {
        key: "walking_distance_beach",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
      },
    ],
  },
  {
    conceptId: "spa_wellness",
    members: [
      { key: "spa", role: "canonical" },
      {
        key: "full_service_spa",
        role: "narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
        note: "Full-service spa is a refinement of spa presence — conservative single count unless steward documents independent claim",
      },
      {
        key: "heavenly_spa",
        role: "brand_narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
      },
      {
        key: "wellness",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
        note: "Wellness positioning can differ from on-site spa inventory",
      },
    ],
  },
  {
    conceptId: "meeting_events",
    members: [
      { key: "meeting_space", role: "canonical" },
      {
        key: "ballroom",
        role: "narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
      },
      {
        key: "large_ballroom",
        role: "narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
      },
      {
        key: "event_space_outdoor",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
        note: "Outdoor event venue is independently testable from indoor meeting inventory",
      },
      {
        key: "business_center",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
      },
    ],
  },
  {
    conceptId: "luxury_positioning",
    members: [
      { key: "luxury", role: "canonical" },
      {
        key: "five_diamond",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
        note: "AAA Five Diamond award claim is independently testable from luxury positioning language",
      },
      {
        key: "urban_luxury",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
      },
      {
        key: "upper_upscale",
        role: "related",
        relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT,
        independentlyMeasurable: true,
      },
    ],
  },
  {
    conceptId: "fitness",
    members: [
      { key: "fitness_center", role: "canonical" },
      {
        key: "fitness_center_24hr",
        role: "narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
      },
      {
        key: "peloton",
        role: "narrower",
        relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER,
        independentlyMeasurable: false,
      },
    ],
  },
]);

/** Phase 1B diagnostic groups (aggressive: any co-membership collapsed to 1). Historical reference only. */
export const PHASE1B_SEMANTIC_EQUIVALENCE_GROUPS = Object.freeze([
  ["beachfront", "oceanfront", "beach_access", "private_beach", "walking_distance_beach"],
  ["spa", "full_service_spa", "heavenly_spa", "wellness"],
  ["fitness_center", "fitness_center_24hr", "peloton"],
  ["meeting_space", "ballroom", "large_ballroom", "event_space_outdoor", "business_center"],
  ["luxury", "urban_luxury", "five_diamond", "upper_upscale"],
]);

function memberMap() {
  const map = new Map();
  for (const concept of SEMANTIC_CONCEPT_REGISTRY) {
    for (const m of concept.members) {
      map.set(m.key, { conceptId: concept.conceptId, ...m, concept });
    }
  }
  return map;
}

const MEMBER_INDEX = memberMap();

export function getSemanticConceptId(attributeKey) {
  return MEMBER_INDEX.get(attributeKey)?.conceptId || `concept:${attributeKey}`;
}

export function getSemanticMember(attributeKey) {
  return MEMBER_INDEX.get(attributeKey) || null;
}

export function classifySemanticPair(a, b) {
  if (!a || !b || a === b) return SEMANTIC_RELATION.DISTINCT;
  const ma = MEMBER_INDEX.get(a);
  const mb = MEMBER_INDEX.get(b);
  if (!ma || !mb || ma.conceptId !== mb.conceptId) return SEMANTIC_RELATION.DISTINCT;

  if (
    ma.relationToCanonical === SEMANTIC_RELATION.TRUE_DUPLICATE ||
    mb.relationToCanonical === SEMANTIC_RELATION.TRUE_DUPLICATE ||
    ma.role === "near_synonym" ||
    mb.role === "near_synonym"
  ) {
    return SEMANTIC_RELATION.TRUE_DUPLICATE;
  }
  if (
    ma.relationToCanonical === SEMANTIC_RELATION.BROADER_NARROWER ||
    mb.relationToCanonical === SEMANTIC_RELATION.BROADER_NARROWER ||
    ma.role === "narrower" ||
    mb.role === "narrower" ||
    ma.role === "brand_narrower" ||
    mb.role === "brand_narrower"
  ) {
    return SEMANTIC_RELATION.BROADER_NARROWER;
  }
  if (
    ma.relationToCanonical === SEMANTIC_RELATION.RELATED_BUT_DISTINCT ||
    mb.relationToCanonical === SEMANTIC_RELATION.RELATED_BUT_DISTINCT ||
    ma.role === "related" ||
    mb.role === "related"
  ) {
    return SEMANTIC_RELATION.RELATED_BUT_DISTINCT;
  }
  return SEMANTIC_RELATION.RELATED_BUT_DISTINCT;
}

/**
 * Collapse keys to unique measurable concepts under RULES_V1_1.
 * - TRUE_DUPLICATE → count once
 * - BROADER_NARROWER → count once unless independentlyMeasurable override (default false)
 * - RELATED_BUT_DISTINCT → count separately when independentlyMeasurable !== false (default true for related)
 */
export function collapseToUniqueMeasurableConcepts(attributeKeys = [], opts = {}) {
  const keys = [...new Set((attributeKeys || []).map(String))];
  const kept = [];
  const collapsed = [];
  const seenConcepts = new Map(); // conceptId -> array of kept keys under that family when related-distinct

  for (const key of keys) {
    const meta = MEMBER_INDEX.get(key);
    if (!meta) {
      kept.push(key);
      continue;
    }

    const priorKeys = seenConcepts.get(meta.conceptId) || [];
    if (priorKeys.length === 0) {
      seenConcepts.set(meta.conceptId, [key]);
      kept.push(key);
      continue;
    }

    let absorbed = false;
    for (const prior of priorKeys) {
      const relation = classifySemanticPair(prior, key);
      if (relation === SEMANTIC_RELATION.TRUE_DUPLICATE) {
        collapsed.push({
          kept: prior,
          removedFromCount: key,
          relation,
          reason: "TRUE_DUPLICATE — near-synonym / same claim",
        });
        absorbed = true;
        break;
      }
      if (relation === SEMANTIC_RELATION.BROADER_NARROWER) {
        const allowSeparate =
          opts.countBroaderNarrowerSeparately === true ||
          meta.independentlyMeasurable === true;
        if (!allowSeparate) {
          collapsed.push({
            kept: prior,
            removedFromCount: key,
            relation,
            reason:
              "BROADER_NARROWER — conservative single count (founder: do not auto-count separately)",
          });
          absorbed = true;
          break;
        }
      }
      if (relation === SEMANTIC_RELATION.RELATED_BUT_DISTINCT) {
        const allowSeparate = meta.independentlyMeasurable !== false;
        if (!allowSeparate) {
          collapsed.push({
            kept: prior,
            removedFromCount: key,
            relation,
            reason: "RELATED but not independently measurable — conservative collapse",
          });
          absorbed = true;
          break;
        }
        // independently measurable related — keep both
      }
    }

    if (!absorbed) {
      priorKeys.push(key);
      seenConcepts.set(meta.conceptId, priorKeys);
      kept.push(key);
    }
  }

  return {
    contractVersion: ADP_SEMANTIC_EQUIVALENCE_V1,
    rulesVersion: ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
    uniqueConceptKeys: kept,
    uniqueConceptCount: kept.length,
    collapsed,
  };
}

/** Phase 1B aggressive group collapse (historical comparison only). */
export function collapsePhase1bAggressiveGroups(attributeKeys = []) {
  const remaining = [...new Set(attributeKeys.map(String))];
  const kept = [];
  const collapsed = [];
  const consumed = new Set();
  for (const key of remaining) {
    if (consumed.has(key)) continue;
    const group = PHASE1B_SEMANTIC_EQUIVALENCE_GROUPS.find((g) => g.includes(key));
    if (!group) {
      kept.push(key);
      consumed.add(key);
      continue;
    }
    const hits = remaining.filter((k) => group.includes(k) && !consumed.has(k));
    kept.push(hits[0]);
    for (const h of hits) consumed.add(h);
    if (hits.length > 1) {
      collapsed.push({
        kept: hits[0],
        removedFromCount: hits.slice(1),
        relation: "PHASE1B_GROUP_COLLAPSE",
        reason: "Phase 1B collapsed entire SEMANTIC_EQUIVALENCE_GROUPS membership to one concept",
      });
    }
  }
  return {
    rulesVersion: "PHASE1B_AGGRESSIVE_GROUP_COLLAPSE",
    uniqueConceptKeys: kept,
    uniqueConceptCount: kept.length,
    collapsed,
  };
}

/**
 * Key-by-key reconciliation: Phase 1B aggressive count vs RULES_V1_1.
 */
export function reconcileSemanticCounting(attributeKeys = []) {
  const keys = [...new Set((attributeKeys || []).map(String))];
  const phase1b = collapsePhase1bAggressiveGroups(keys);
  const current = collapseToUniqueMeasurableConcepts(keys);
  const pairs = analyzeSelectedSemanticOverlaps(keys);

  const keyRows = keys.map((key) => {
    const meta = MEMBER_INDEX.get(key);
    const inPhase1b = phase1b.uniqueConceptKeys.includes(key);
    const inCurrent = current.uniqueConceptKeys.includes(key);
    const collapsedBy = current.collapsed.find((c) => c.removedFromCount === key);
    return {
      attribute: key,
      conceptId: getSemanticConceptId(key),
      registryRelation: meta?.relationToCanonical || SEMANTIC_RELATION.DISTINCT,
      independentlyMeasurable: meta?.independentlyMeasurable ?? null,
      countedInPhase1bUnique: inPhase1b,
      countedInRulesV1_1Unique: inCurrent,
      collapsedInto: collapsedBy?.kept || null,
      collapseRelation: collapsedBy?.relation || null,
      collapseReason: collapsedBy?.reason || null,
      classification:
        !meta
          ? SEMANTIC_RELATION.DISTINCT
          : meta.relationToCanonical ||
            (meta.role === "canonical" ? SEMANTIC_RELATION.DISTINCT : SEMANTIC_RELATION.RELATED_BUT_DISTINCT),
    };
  });

  const deltaKeys = keyRows.filter(
    (r) => r.countedInPhase1bUnique !== r.countedInRulesV1_1Unique
  );

  return {
    rulesVersion: ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
    selectedRawCount: keys.length,
    phase1bUniqueConceptCount: phase1b.uniqueConceptCount,
    rulesV1_1UniqueConceptCount: current.uniqueConceptCount,
    delta: current.uniqueConceptCount - phase1b.uniqueConceptCount,
    explanation:
      "Phase 1B collapsed every co-member of SEMANTIC_EQUIVALENCE_GROUPS (including RELATED pairs) into one concept. RULES_V1_1 collapses TRUE_DUPLICATE and BROADER_NARROWER conservatively, but keeps RELATED_BUT_DISTINCT when independentlyMeasurable (e.g. meeting_space vs event_space_outdoor; luxury vs five_diamond).",
    pairs: pairs.pairs,
    keyRows,
    deltaKeys,
    phase1b,
    current,
  };
}

export function analyzeSelectedSemanticOverlaps(attributeKeys = []) {
  const keys = [...new Set(attributeKeys || [])];
  const pairs = [];
  for (let i = 0; i < keys.length; i++) {
    for (let j = i + 1; j < keys.length; j++) {
      const relation = classifySemanticPair(keys[i], keys[j]);
      if (relation !== SEMANTIC_RELATION.DISTINCT) {
        const ma = MEMBER_INDEX.get(keys[i]);
        pairs.push({
          a: keys[i],
          b: keys[j],
          relation,
          conceptId: getSemanticConceptId(keys[i]),
          independentlyMeasurableA: ma?.independentlyMeasurable ?? null,
          independentlyMeasurableB: MEMBER_INDEX.get(keys[j])?.independentlyMeasurable ?? null,
        });
      }
    }
  }
  return {
    contractVersion: ADP_SEMANTIC_EQUIVALENCE_V1,
    rulesVersion: ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
    pairs,
  };
}

/** Compatibility alias used by Phase 1B eligibility classifier. */
export const SEMANTIC_EQUIVALENCE_GROUPS = PHASE1B_SEMANTIC_EQUIVALENCE_GROUPS;

export function collapseEligibleToUniqueConcepts(eligibleKeys = []) {
  return collapseToUniqueMeasurableConcepts(eligibleKeys);
}
