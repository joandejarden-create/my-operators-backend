/**
 * Normalize organization names for duplicate suppression.
 * Does not auto-merge ambiguous names.
 */

const SUFFIX_RE =
  /\b(inc\.?|llc\.?|ltd\.?|corp\.?|corporation|company|co\.?|group|holdings?|plc|gmbh|s\.?a\.?|bv|ag)\b/gi;

export function normalizeOrganizationName(name = "") {
  let s = String(name || "")
    .replace(/\s+/g, " ")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .trim();
  // Strip trailing ellipsis / truncation artifacts
  s = s.replace(/\.{2,}$/g, "").replace(/\s+\.{2,}.*$/, "").trim();
  const key = s
    .toLowerCase()
    .replace(SUFFIX_RE, " ")
    .replace(/[^a-z0-9\s&]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return {
    displayName: s,
    normalizeKey: key,
  };
}

/**
 * Deduplicate entities by normalizeKey + demandGeneratorId + role.
 * Ambiguous keys (very short) are not merged.
 */
export function dedupeExtractedEntities(entities = []) {
  const map = new Map();
  for (const e of entities) {
    if (!e?.entityName) continue;
    const { displayName, normalizeKey } = normalizeOrganizationName(e.entityName);
    if (!normalizeKey || normalizeKey.length < 4) {
      // keep separately under unique id
      map.set(`${e.sourceURL || ""}|${displayName}|${map.size}`, { ...e, entityName: displayName, normalizeKey });
      continue;
    }
    const seed = `${normalizeKey}|${e.demandGeneratorId || ""}|${e.participationRole || ""}|${e.eventCycleId || e.year || ""}`;
    const prev = map.get(seed);
    if (!prev) {
      map.set(seed, { ...e, entityName: displayName, normalizeKey });
      continue;
    }
    // Prefer higher confidence / richer fields
    const conf = (x) => Number(x.confidence) || 0;
    if (conf(e) > conf(prev)) {
      map.set(seed, {
        ...prev,
        ...e,
        entityName: displayName,
        normalizeKey,
        evidenceSnippet: e.evidenceSnippet || prev.evidenceSnippet,
      });
    } else {
      map.set(seed, {
        ...e,
        ...prev,
        entityName: prev.entityName || displayName,
        normalizeKey,
      });
    }
  }
  return [...map.values()];
}
