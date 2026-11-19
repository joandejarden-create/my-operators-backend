/**
 * Governed semantic-equivalence model for ADP attribute concepts.
 * Does not silently merge or delete selections.
 */

export const ADP_SEMANTIC_EQUIVALENCE_V1 = "ADP_SEMANTIC_EQUIVALENCE_V1";

export const SEMANTIC_RELATION = Object.freeze({
  TRUE_DUPLICATE: "TRUE_DUPLICATE",
  BROADER_NARROWER: "BROADER_NARROWER",
  RELATED_BUT_DISTINCT: "RELATED_BUT_DISTINCT",
  DISTINCT: "DISTINCT",
});

/**
 * Governed pairs/groups. relation is explicit — not every overlap is a true duplicate.
 */
export const SEMANTIC_CONCEPT_REGISTRY = Object.freeze([
  {
    conceptId: "beach_frontage",
    members: [
      { key: "beachfront", role: "canonical" },
      { key: "oceanfront", role: "near_synonym", relationToCanonical: SEMANTIC_RELATION.TRUE_DUPLICATE },
      { key: "beach_access", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
      { key: "private_beach", role: "narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
      { key: "walking_distance_beach", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
    ],
  },
  {
    conceptId: "spa_wellness",
    members: [
      { key: "spa", role: "canonical" },
      { key: "full_service_spa", role: "narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
      { key: "heavenly_spa", role: "brand_narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
      { key: "wellness", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
    ],
  },
  {
    conceptId: "meeting_events",
    members: [
      { key: "meeting_space", role: "canonical" },
      { key: "ballroom", role: "narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
      { key: "large_ballroom", role: "narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
      { key: "event_space_outdoor", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
      { key: "business_center", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
    ],
  },
  {
    conceptId: "luxury_positioning",
    members: [
      { key: "luxury", role: "canonical" },
      { key: "five_diamond", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
      { key: "urban_luxury", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
      { key: "upper_upscale", role: "related", relationToCanonical: SEMANTIC_RELATION.RELATED_BUT_DISTINCT },
    ],
  },
  {
    conceptId: "fitness",
    members: [
      { key: "fitness_center", role: "canonical" },
      { key: "fitness_center_24hr", role: "narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
      { key: "peloton", role: "narrower", relationToCanonical: SEMANTIC_RELATION.BROADER_NARROWER },
    ],
  },
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

export function classifySemanticPair(a, b) {
  if (!a || !b || a === b) return SEMANTIC_RELATION.DISTINCT;
  const ma = MEMBER_INDEX.get(a);
  const mb = MEMBER_INDEX.get(b);
  if (!ma || !mb || ma.conceptId !== mb.conceptId) return SEMANTIC_RELATION.DISTINCT;
  if (
    ma.relationToCanonical === SEMANTIC_RELATION.TRUE_DUPLICATE ||
    mb.relationToCanonical === SEMANTIC_RELATION.TRUE_DUPLICATE ||
    (ma.role === "canonical" && mb.relationToCanonical === SEMANTIC_RELATION.TRUE_DUPLICATE) ||
    (mb.role === "canonical" && ma.relationToCanonical === SEMANTIC_RELATION.TRUE_DUPLICATE)
  ) {
    // beachfront/oceanfront style
    if (
      [ma.relationToCanonical, mb.relationToCanonical].includes(SEMANTIC_RELATION.TRUE_DUPLICATE) ||
      (ma.role === "near_synonym" || mb.role === "near_synonym")
    ) {
      return SEMANTIC_RELATION.TRUE_DUPLICATE;
    }
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
 * Collapse keys to unique measurable concepts.
 * TRUE_DUPLICATE → count once (keep first / preferred).
 * BROADER_NARROWER → may count separately when independentlyMeaningful=true (default: count once for min/max unless steward marks both).
 * RELATED_BUT_DISTINCT → count separately.
 */
export function collapseToUniqueMeasurableConcepts(
  attributeKeys = [],
  { countBroaderNarrowerSeparately = false } = {}
) {
  const keys = [...new Set((attributeKeys || []).map(String))];
  const kept = [];
  const collapsed = [];
  const seenConcepts = new Map(); // conceptId -> kept key

  for (const key of keys) {
    const meta = MEMBER_INDEX.get(key);
    if (!meta) {
      kept.push(key);
      continue;
    }
    const prior = seenConcepts.get(meta.conceptId);
    if (!prior) {
      seenConcepts.set(meta.conceptId, key);
      kept.push(key);
      continue;
    }
    const relation = classifySemanticPair(prior, key);
    if (relation === SEMANTIC_RELATION.TRUE_DUPLICATE) {
      collapsed.push({ kept: prior, removedFromCount: key, relation });
      continue;
    }
    if (relation === SEMANTIC_RELATION.BROADER_NARROWER && !countBroaderNarrowerSeparately) {
      collapsed.push({ kept: prior, removedFromCount: key, relation });
      continue;
    }
    // RELATED_BUT_DISTINCT or steward-allowed broader/narrower
    kept.push(key);
  }

  return {
    contractVersion: ADP_SEMANTIC_EQUIVALENCE_V1,
    uniqueConceptKeys: kept,
    uniqueConceptCount: kept.length,
    collapsed,
  };
}

export function analyzeSelectedSemanticOverlaps(attributeKeys = []) {
  const keys = [...new Set(attributeKeys || [])];
  const pairs = [];
  for (let i = 0; i < keys.length; i++) {
    for (let j = i + 1; j < keys.length; j++) {
      const relation = classifySemanticPair(keys[i], keys[j]);
      if (relation !== SEMANTIC_RELATION.DISTINCT) {
        pairs.push({
          a: keys[i],
          b: keys[j],
          relation,
          conceptId: getSemanticConceptId(keys[i]),
        });
      }
    }
  }
  return { contractVersion: ADP_SEMANTIC_EQUIVALENCE_V1, pairs };
}
