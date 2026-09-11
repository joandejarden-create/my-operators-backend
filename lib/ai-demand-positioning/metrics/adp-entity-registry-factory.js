/**
 * Shared entity resolution helpers for ADP property-specific CORE governance.
 * Property-specific hotel lists live in adp-property-entity-registries.js.
 */

export const ENTITY_CLASSES = Object.freeze({
  CANONICAL_HOTEL: "CANONICAL_HOTEL",
  DUPLICATE_ALIAS: "DUPLICATE_ALIAS",
  NON_HOTEL_ENTITY: "NON_HOTEL_ENTITY",
  GENERIC_PHRASE: "GENERIC_PHRASE",
  VENUE_ONLY: "VENUE_ONLY",
  LOCATION: "LOCATION",
  BRAND_NOT_PROPERTY: "BRAND_NOT_PROPERTY",
  AMBIGUOUS: "AMBIGUOUS",
  UNRESOLVED: "UNRESOLVED",
});

const SHARED_GENERIC = [
  "best hotel",
  "top hotel",
  "luxury hotel",
  "boutique hotel",
  "this hotel",
  "the hotel",
  "best resort",
  "top resort",
  "luxury resort",
  "best meeting hotel",
  "recommended hotel",
  "social hotel",
  "mad hotel",
  "top meeting hotel",
  "times square hotel",
  "bermuda resort",
  "only resort",
  "best resort",
  "top resort",
  "friendly hotel",
  "service hotel",
  "important note hotel",
];

function norm(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/\*\*/g, "")
    .replace(/[^\w\s&'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function createEntityRegistry({ version, hotels, genericExact = [], venueOrClub = [], brandOnly = [], ambiguous = [] }) {
  const generic = new Set([...SHARED_GENERIC, ...genericExact].map(norm));
  const venues = new Set(venueOrClub.map(norm));
  const brands = new Set(brandOnly.map(norm));
  const ambiguousSet = new Set(ambiguous.map(norm));
  const byId = new Map(hotels.map((h) => [h.entityId, h]));

  function hotelById(entityId) {
    return byId.get(entityId) || null;
  }

  function classifyObservedEntity(rawName) {
    const n = norm(rawName);
    if (!n || n.length < 4) {
      return { class: ENTITY_CLASSES.GENERIC_PHRASE, canonical: null, entityId: null, identityOk: false };
    }
    if (generic.has(n)) {
      return { class: ENTITY_CLASSES.GENERIC_PHRASE, canonical: null, entityId: null, identityOk: false, raw: rawName };
    }
    if (ambiguousSet.has(n)) {
      return { class: ENTITY_CLASSES.AMBIGUOUS, canonical: null, entityId: null, identityOk: false, raw: rawName };
    }
    if (venues.has(n) || (/\bclub$/i.test(n) && !/\b(hotel|resort|inn)\b/i.test(n))) {
      return { class: ENTITY_CLASSES.VENUE_ONLY, canonical: null, entityId: null, identityOk: false, raw: rawName };
    }

    // Prefer exact, then longest contained alias/canonical. First-match-wins
    // over-collapses peers when a short alias is a substring of another hotel.
    let best = null;
    for (const hotel of hotels) {
      const canon = hotel.canonical.toLowerCase();
      const exact = hotel.aliases.includes(n) || n === canon;
      const containedAlias = hotel.aliases
        .filter((a) => a.length >= 12 && n.includes(a))
        .sort((a, b) => b.length - a.length)[0];
      const shortFormAlias = hotel.aliases
        .filter((a) => a.length >= 12 && n.length >= 12 && a.includes(n))
        .sort((a, b) => b.length - a.length)[0];
      const canonContained =
        canon.length >= 12 && (n.includes(canon) || canon.includes(n)) ? canon : null;
      if (!(exact || containedAlias || shortFormAlias || canonContained)) continue;

      const matchLen = exact
        ? 10_000 + n.length
        : Math.max(
            containedAlias?.length || 0,
            shortFormAlias?.length || 0,
            canonContained?.length || 0
          );
      if (!best || matchLen > best.matchLen) {
        best = {
          hotel,
          exact,
          matchLen,
        };
      }
    }
    if (best) {
      if (!best.exact) {
        return {
          class: ENTITY_CLASSES.DUPLICATE_ALIAS,
          canonical: best.hotel.canonical,
          entityId: best.hotel.entityId,
          identityOk: true,
          hotel: best.hotel,
          raw: rawName,
          duplicateOf: best.hotel.entityId,
        };
      }
      return {
        class: ENTITY_CLASSES.CANONICAL_HOTEL,
        canonical: best.hotel.canonical,
        entityId: best.hotel.entityId,
        identityOk: true,
        hotel: best.hotel,
        raw: rawName,
      };
    }

    if (brands.has(n)) {
      return { class: ENTITY_CLASSES.BRAND_NOT_PROPERTY, canonical: null, entityId: null, identityOk: false, raw: rawName };
    }
    if (/\b(hotel|resort|inn|suites?|lodge)\b/i.test(n)) {
      return { class: ENTITY_CLASSES.UNRESOLVED, canonical: null, entityId: null, identityOk: false, raw: rawName };
    }
    return { class: ENTITY_CLASSES.UNRESOLVED, canonical: null, entityId: null, identityOk: false, raw: rawName };
  }

  function canonicalizeToEntityId(rawName) {
    const c = classifyObservedEntity(rawName);
    return c.identityOk ? c.entityId : null;
  }

  function classifyEntityUniverse(rawNames) {
    const rows = [];
    const byClass = {};
    const canonicalIds = new Set();
    let duplicatesMerged = 0;
    for (const name of rawNames || []) {
      const c = classifyObservedEntity(name);
      rows.push({ name, ...c });
      byClass[c.class] = (byClass[c.class] || 0) + 1;
      if (c.entityId) {
        if (canonicalIds.has(c.entityId)) duplicatesMerged += 1;
        canonicalIds.add(c.entityId);
      }
    }
    const artifacts = (rawNames || []).filter((n) => {
      const c = classifyObservedEntity(n);
      return [
        ENTITY_CLASSES.GENERIC_PHRASE,
        ENTITY_CLASSES.VENUE_ONLY,
        ENTITY_CLASSES.LOCATION,
        ENTITY_CLASSES.NON_HOTEL_ENTITY,
        ENTITY_CLASSES.BRAND_NOT_PROPERTY,
      ].includes(c.class);
    });
    return {
      version,
      rawEntities: (rawNames || []).length,
      canonicalHotels: canonicalIds.size,
      duplicatesMerged,
      artifactsRemoved: artifacts.length,
      ambiguous: byClass[ENTITY_CLASSES.AMBIGUOUS] || 0,
      unresolved: byClass[ENTITY_CLASSES.UNRESOLVED] || 0,
      byClass,
      rows,
      canonicalIds: [...canonicalIds],
      artifactSamples: artifacts.slice(0, 25),
    };
  }

  return {
    version,
    hotels,
    hotelById,
    classifyObservedEntity,
    canonicalizeToEntityId,
    classifyEntityUniverse,
  };
}
