/**
 * ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1
 *
 * One canonical hotel capability → one Reality Gap measurement row → one customer card.
 * Equivalence is semantic (governed registry), not string similarity.
 *
 * Aggregation policy for alias recognition:
 *   Count SUBJECT_MENTIONED observations where ANY alias appears in attributesRecognized
 *   (observation union). Do not sum per-alias counts (would double-count the same response).
 *   Classification follows the union rate. Do not auto-prefer the more favorable alias rate.
 */

export const ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1 =
  "ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1";

/**
 * @typedef {{
 *   canonicalId: string,
 *   preferredLabel: string,
 *   aliases: string[],
 *   relatedDistinct?: string[],
 *   displayPriority: Record<string, number>,
 *   evidenceNote?: string,
 * }} CanonicalEquivalenceGroup
 */

/** @type {readonly CanonicalEquivalenceGroup[]} */
export const REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1 = Object.freeze([
  Object.freeze({
    canonicalId: "on_site_restaurant",
    preferredLabel: "On-Site Restaurant",
    aliases: Object.freeze(["on_site_restaurant", "restaurant"]),
    relatedDistinct: Object.freeze([
      "fine_dining",
      "rooftop_dining",
      "rooftop_bar",
      "private_dining",
      "nobu_restaurant",
    ]),
    displayPriority: Object.freeze({ on_site_restaurant: 20, restaurant: 10 }),
    evidenceNote:
      "restaurant and on_site_restaurant are the same hotel dining capability; identical customer labels must not double-count.",
  }),
  Object.freeze({
    canonicalId: "meeting_space",
    preferredLabel: "Meeting & Event Space",
    aliases: Object.freeze(["meeting_space", "meetings", "meeting_rooms"]),
    relatedDistinct: Object.freeze([
      "meeting_space_small",
      "ballroom",
      "large_ballroom",
      "event_space_outdoor",
      "wedding_venue",
      "ballroom_capacity",
    ]),
    displayPriority: Object.freeze({
      meeting_space: 30,
      meeting_rooms: 20,
      meetings: 10,
    }),
    evidenceNote:
      "meetings / meeting_rooms are inventory aliases of meeting_space. Intimate meeting space remains distinct.",
  }),
  Object.freeze({
    canonicalId: "ballroom",
    preferredLabel: "Ballroom",
    aliases: Object.freeze(["ballroom", "large_ballroom"]),
    relatedDistinct: Object.freeze(["meeting_space", "ballroom_capacity", "event_space_outdoor"]),
    displayPriority: Object.freeze({ large_ballroom: 30, ballroom: 20 }),
    evidenceNote:
      "large_ballroom is a specificity upgrade of ballroom capacity claim — count once; prefer large_ballroom label when present.",
  }),
  Object.freeze({
    canonicalId: "spa",
    preferredLabel: "On-Site Spa",
    aliases: Object.freeze(["spa", "full_service_spa"]),
    relatedDistinct: Object.freeze(["heavenly_spa"]),
    displayPriority: Object.freeze({ full_service_spa: 30, spa: 20 }),
    evidenceNote:
      "spa and full_service_spa describe the same on-property spa amenity for Reality Gap. Brand-named Heavenly Spa stays distinct.",
  }),
  Object.freeze({
    canonicalId: "fitness_center",
    preferredLabel: "Fitness Center",
    aliases: Object.freeze([
      "fitness_center",
      "fitness_center_24hr",
      "wellness_fitness",
      "wellness",
    ]),
    relatedDistinct: Object.freeze(["spa", "full_service_spa", "heavenly_spa"]),
    displayPriority: Object.freeze({
      fitness_center_24hr: 40,
      fitness_center: 30,
      wellness_fitness: 20,
      wellness: 10,
    }),
  }),
  Object.freeze({
    canonicalId: "beachfront",
    preferredLabel: "Beachfront",
    aliases: Object.freeze(["beachfront", "beach_access", "oceanfront", "private_beach"]),
    relatedDistinct: Object.freeze(["private_beaches", "five_private_coves", "walking_distance_beach"]),
    displayPriority: Object.freeze({
      private_beach: 40,
      beachfront: 30,
      oceanfront: 20,
      beach_access: 10,
    }),
    evidenceNote:
      "Broad beach adjacency aliases collapse once. Multi-cove / private-beaches inventory claims remain distinct when labeled differently.",
  }),
  Object.freeze({
    canonicalId: "luxury",
    preferredLabel: "Luxury Positioning",
    aliases: Object.freeze(["luxury", "urban_luxury", "five_diamond"]),
    relatedDistinct: Object.freeze(["upscale", "upper_upscale"]),
    displayPriority: Object.freeze({ five_diamond: 40, urban_luxury: 30, luxury: 20 }),
  }),
  Object.freeze({
    canonicalId: "full_service",
    preferredLabel: "Full-Service Hotel",
    aliases: Object.freeze(["full_service", "full_service_hotel"]),
    relatedDistinct: Object.freeze([]),
    displayPriority: Object.freeze({ full_service: 20, full_service_hotel: 10 }),
  }),
  Object.freeze({
    canonicalId: "upscale",
    preferredLabel: "Upscale Positioning",
    aliases: Object.freeze(["upscale", "upper_upscale"]),
    relatedDistinct: Object.freeze(["luxury", "urban_luxury"]),
    displayPriority: Object.freeze({ upper_upscale: 30, upscale: 20 }),
  }),
  Object.freeze({
    canonicalId: "historic_property",
    preferredLabel: "Historic Property",
    aliases: Object.freeze(["historic_property", "historic_building"]),
    relatedDistinct: Object.freeze(["historic_art_deco", "historic_city_access"]),
    displayPriority: Object.freeze({ historic_property: 30, historic_building: 20 }),
    evidenceNote:
      "historic_property and historic_building describe the same heritage-building capability for Reality Gap; art-deco specificity and historic-city access remain distinct.",
  }),
]);

/** Related but must NOT merge into each other for Reality Gap cards. */
export const REALITY_GAP_RELATED_DISTINCT_FAMILIES_V1 = Object.freeze({
  pool: Object.freeze([
    "outdoor_pool",
    "heated_outdoor_pool",
    "infinity_pool",
    "heated_infinity_pool",
  ]),
  dining_premium: Object.freeze([
    "fine_dining",
    "rooftop_dining",
    "rooftop_bar",
    "private_dining",
    "nobu_restaurant",
  ]),
  meetings_special: Object.freeze([
    "meeting_space_small",
    "event_space_outdoor",
    "wedding_venue",
    "ballroom_capacity",
  ]),
  spa_branded: Object.freeze(["heavenly_spa"]),
});

const ALIAS_TO_GROUP = (() => {
  /** @type {Map<string, CanonicalEquivalenceGroup>} */
  const m = new Map();
  for (const g of REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1) {
    for (const a of g.aliases) m.set(a, g);
  }
  return m;
})();

export function getEquivalenceGroupForAttribute(attributeId) {
  return ALIAS_TO_GROUP.get(String(attributeId || "").trim()) || null;
}

/**
 * Pick the display member for a group given which aliases are present on the property.
 */
export function selectDisplayAttributeId(group, presentAliases) {
  const present = (presentAliases || []).filter((a) => group.aliases.includes(a));
  if (!present.length) return group.canonicalId;
  present.sort(
    (a, b) => (group.displayPriority[b] || 0) - (group.displayPriority[a] || 0)
  );
  return present[0];
}

/**
 * Collapse property attributes to unique Reality Gap measurement identities.
 * Preserves related-distinct attributes outside equivalence groups.
 * @param {string[]} attributeIds
 * @param {{ labelFor?: (id: string) => string }} [opts]
 */
export function resolveCanonicalAttributeSet(attributeIds, opts = {}) {
  const labelFor = opts.labelFor || ((id) => id);
  const input = [...new Set((attributeIds || []).map((a) => String(a).trim()).filter(Boolean))];
  const consumed = new Set();
  const resolutions = [];
  const canonicalIds = [];

  for (const g of REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1) {
    const present = g.aliases.filter((a) => input.includes(a));
    if (!present.length) continue;
    for (const a of present) consumed.add(a);
    const displayId = selectDisplayAttributeId(g, present);
    canonicalIds.push(displayId);
    resolutions.push({
      canonicalId: g.canonicalId,
      displayAttributeId: displayId,
      preferredLabel: labelFor(displayId) || g.preferredLabel,
      aliasesPresent: present,
      aliasesCollapsed: present.filter((a) => a !== displayId),
      group: g.canonicalId,
    });
  }

  for (const id of input) {
    if (consumed.has(id)) continue;
    canonicalIds.push(id);
    resolutions.push({
      canonicalId: id,
      displayAttributeId: id,
      preferredLabel: labelFor(id) || id,
      aliasesPresent: [id],
      aliasesCollapsed: [],
      group: null,
    });
  }

  return {
    contractVersion: ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
    inputCount: input.length,
    canonicalCount: canonicalIds.length,
    collapsedCount: input.length - canonicalIds.length,
    canonicalIds,
    resolutions,
  };
}

/**
 * Coverage helper — synonym collapse via Reality Gap registry (supersedes ad-hoc lists).
 */
export function collapseSynonymAttributesV2(attributeIds) {
  return resolveCanonicalAttributeSet(attributeIds).canonicalIds;
}

/**
 * Observation-union recognition count for a set of aliases.
 * Never sums per-alias counts across the same observation.
 */
export function countAliasUnionRecognition(relevantObservations, aliases) {
  const aliasSet = new Set(aliases || []);
  let count = 0;
  const contributingObsIds = [];
  for (const obs of relevantObservations || []) {
    const recognized = obs.attributesRecognized || [];
    if (!recognized.length) continue;
    let hit = false;
    for (const a of recognized) {
      if (aliasSet.has(a)) {
        hit = true;
        break;
      }
    }
    if (hit) {
      count += 1;
      if (obs.observationId || obs.id) {
        contributingObsIds.push(obs.observationId || obs.id);
      }
    }
  }
  return { count, contributingObsIds };
}

/**
 * Per-alias rates for reconciliation reporting (not used for customer score).
 */
export function perAliasRecognitionRates(relevantObservations, aliases, eligibleDenom) {
  const denom = Number(eligibleDenom) || 0;
  const out = [];
  for (const alias of aliases || []) {
    let c = 0;
    for (const obs of relevantObservations || []) {
      if ((obs.attributesRecognized || []).includes(alias)) c += 1;
    }
    out.push({
      alias,
      count: c,
      rate: denom > 0 ? c / denom : 0,
    });
  }
  return out;
}

/**
 * Detect duplicate customer-facing labels among classified cards.
 */
export function findDuplicateCustomerFacingLabels(cards) {
  const seen = new Map();
  const dups = [];
  for (const card of cards || []) {
    const label = String(card.label || "")
      .toLowerCase()
      .trim();
    if (!label) continue;
    const attr = card.attribute || card.id || "";
    if (seen.has(label)) {
      dups.push({
        label: card.label,
        attributes: [seen.get(label), attr],
      });
    } else {
      seen.set(label, attr);
    }
  }
  return dups;
}

/**
 * Detect same canonical capability appearing in both recognized and missed lists.
 */
export function findRecognizeMissConflicts(recognized, gaps) {
  const rec = new Set((recognized || []).map((r) => r.attribute));
  const conflicts = [];
  for (const g of gaps || []) {
    if (rec.has(g.attribute)) {
      conflicts.push({ attribute: g.attribute, label: g.label });
      continue;
    }
    const group = getEquivalenceGroupForAttribute(g.attribute);
    if (!group) continue;
    for (const a of group.aliases) {
      if (a !== g.attribute && rec.has(a)) {
        conflicts.push({
          attribute: g.attribute,
          recognizedAlias: a,
          label: g.label,
        });
      }
    }
  }
  return conflicts;
}

export function registryToJson() {
  return {
    contractVersion: ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
    aggregationPolicy:
      "OBSERVATION_UNION_ANY_ALIAS — count each subject-mentioned observation at most once per canonical capability",
    classificationPolicy:
      "Union recognition rate vs REALITY_GAP_RECOGNITION_THRESHOLDS_V1; do not auto-prefer favorable alias",
    groups: REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1,
    relatedDistinctFamilies: REALITY_GAP_RELATED_DISTINCT_FAMILIES_V1,
  };
}
