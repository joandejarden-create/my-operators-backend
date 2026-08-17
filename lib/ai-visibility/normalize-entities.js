/**
 * Entity normalization for AI Visibility.
 * Reuses Brand Basics / Operator Master identity conceptually.
 * Unresolved mentions stay Unresolved — never guessed.
 *
 * Resolver precedence (v2.1):
 * 1. Exact canonical full-name match
 * 2. Approved canonical alias (Airtable SSOT aliases on entity)
 * 3. Approved runtime alias overlay (already merged onto entity.aliases)
 * 4. Decorated-name normalization (region suffix strip)
 * 5. Parent/company guard (bare parents never invent brand matches)
 * 6. Longest specific match (index sort)
 * 7. Contextual short-brand aliases (require parent/family context; never global bare)
 * 8. Unresolved
 */

export const RESOLVER_VERSION = "ai_visibility_entity_resolver_v2_1_contextual";

/**
 * Narrow contextual short-name rules. These are NOT global aliases.
 * Bare surface forms resolve only when required parent context is present
 * and ordinary-language reject patterns do not match.
 */
export const CONTEXTUAL_ALIAS_RULES = Object.freeze([
  Object.freeze({
    id: "canopy_by_hilton_contextual_v1",
    canonicalEntityName: "Canopy by Hilton",
    surface: "Canopy",
    requiredParentContext: "Hilton",
    contextWindowChars: 220,
    brandFamilyPeers: Object.freeze([
      "Curio",
      "Signia",
      "DoubleTree",
      "Embassy Suites",
      "Tapestry",
      "Waldorf",
      "Conrad",
      "LXR",
      "Hampton",
      "Hilton Garden",
      "Homewood",
      "Home2",
      "Tempo",
      "Motto",
    ]),
    rejectPatterns: Object.freeze([
      /\b(?:tree|forest|leaf|jungle|dense)\s+canopy\b/i,
      /\bcanopy\s+(?:over|above|bed|structure|cover|awning|tent|roof|of\s+trees|of\s+leaves)\b/i,
      /\b(?:under|beneath)\s+(?:the\s+)?canopy\b/i,
      /\bcanopy\s+walkway\b/i,
      /\bglass\s+canopy\b/i,
    ]),
  }),
]);

const REGION_SUFFIXES = [
  "cala",
  "latam",
  "latin america",
  "caribbean",
  "mexico",
  "ghl holding",
  "usa",
  "us",
  "uk",
  "emea",
  "apac",
];

const BARE_PARENT_BLOCKLIST = new Set(
  [
    "hilton",
    "marriott",
    "hyatt",
    "ihg",
    "accor",
    "wyndham",
    "choice",
    "choice hotels",
    "marriott international",
    "hilton worldwide",
    "hilton hotels",
    "ihg hotels",
    "ihg hotels and resorts",
  ].map((s) => s)
);

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Strip markdown emphasis / code markers so brand names inside **bold** still match.
 * Does not strip letters from brand identity. Positions may shift — use for matching only.
 * @param {string} value
 */
/**
 * Strip markdown emphasis/links for entity matching while mapping each
 * stripped character back to its original index. Required so recommendation
 * rank/context classification uses correct offsets into the raw response.
 * @returns {{ text: string, toOriginal: number[] }}
 */
export function stripMarkdownNoiseForEntityMatchWithMap(value) {
  const original = String(value || "");
  let out = "";
  const toOriginal = [];
  let i = 0;

  while (i < original.length) {
    // **bold**
    if (original.startsWith("**", i)) {
      const end = original.indexOf("**", i + 2);
      if (end !== -1) {
        for (let j = i + 2; j < end; j++) {
          out += original[j];
          toOriginal.push(j);
        }
        i = end + 2;
        continue;
      }
    }
    // __bold__
    if (original.startsWith("__", i)) {
      const end = original.indexOf("__", i + 2);
      if (end !== -1) {
        for (let j = i + 2; j < end; j++) {
          out += original[j];
          toOriginal.push(j);
        }
        i = end + 2;
        continue;
      }
    }
    // *italic* (single asterisk; not part of **)
    if (original[i] === "*" && !original.startsWith("**", i)) {
      const end = original.indexOf("*", i + 1);
      if (end !== -1 && end > i + 1) {
        for (let j = i + 1; j < end; j++) {
          out += original[j];
          toOriginal.push(j);
        }
        i = end + 1;
        continue;
      }
    }
    // _italic_
    if (original[i] === "_" && !original.startsWith("__", i)) {
      const end = original.indexOf("_", i + 1);
      if (end !== -1 && end > i + 1) {
        for (let j = i + 1; j < end; j++) {
          out += original[j];
          toOriginal.push(j);
        }
        i = end + 1;
        continue;
      }
    }
    // `code`
    if (original[i] === "`") {
      const end = original.indexOf("`", i + 1);
      if (end !== -1 && end > i + 1) {
        for (let j = i + 1; j < end; j++) {
          out += original[j];
          toOriginal.push(j);
        }
        i = end + 1;
        continue;
      }
    }
    // [label](url)
    if (original[i] === "[") {
      const close = original.indexOf("]", i + 1);
      if (close !== -1 && original[close + 1] === "(") {
        const urlEnd = original.indexOf(")", close + 2);
        if (urlEnd !== -1) {
          for (let j = i + 1; j < close; j++) {
            out += original[j];
            toOriginal.push(j);
          }
          i = urlEnd + 1;
          continue;
        }
      }
    }

    out += original[i];
    toOriginal.push(i);
    i += 1;
  }

  return { text: out, toOriginal };
}

export function stripMarkdownNoiseForEntityMatch(value) {
  return stripMarkdownNoiseForEntityMatchWithMap(value).text;
}

/**
 * Normalize for matching: lowercase, collapse whitespace, strip most punctuation.
 */
export function normalizeMatchKey(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Decorated-name normalization: strip trailing regional suffixes / parentheses.
 * Does not strip meaningful legal/canonical distinguishing terms beyond known regions.
 * @param {string} value
 * @returns {string[]} candidate keys including original
 */
export function decoratedNameKeys(value) {
  const original = normalizeMatchKey(value);
  if (!original) return [];
  const keys = new Set([original]);

  // Remove parenthetical segments: "hotel equities (cala)" → "hotel equities"
  const noParen = original.replace(/\s*\([^)]*\)\s*/g, " ").replace(/\s+/g, " ").trim();
  if (noParen) keys.add(noParen);

  for (const suffix of REGION_SUFFIXES) {
    const re = new RegExp(`(?:\\s+|\\s)${escapeRegExp(suffix)}$`);
    if (re.test(original)) {
      keys.add(original.replace(re, "").trim());
    }
    if (noParen && re.test(noParen)) {
      keys.add(noParen.replace(re, "").trim());
    }
  }

  return [...keys].filter(Boolean);
}

export function isBlockedBareParentMention(rawMention) {
  const key = normalizeMatchKey(rawMention);
  return BARE_PARENT_BLOCKLIST.has(key);
}

/**
 * Build searchable alias index sorted longest-first for specific-match precedence.
 * Includes decorated-name variants of canonical names/aliases.
 * @param {import('./types.js').CanonicalEntity[]} entities
 */
export function buildEntityAliasIndex(entities) {
  const rows = [];
  for (const entity of entities || []) {
    if (!entity?.id || !entity?.name) continue;
    const names = new Set([entity.name, ...(entity.aliases || [])]);
    for (const raw of names) {
      const variants = decoratedNameKeys(raw);
      // Always include the display label for regex matching against original text
      const labels = new Set([String(raw).trim(), ...variants.map((k) => k)]);
      for (const label of labels) {
        const key = normalizeMatchKey(label);
        if (!key) continue;
        // Skip generating bare-parent index rows even if present as parentCompany labels
        if (entity.isParentCompanyLabel && BARE_PARENT_BLOCKLIST.has(key)) {
          // Still allow exact parent label match when the entity itself IS the parent label
        }
        rows.push({
          key,
          length: key.length,
          rawLabel: String(raw).trim(), // prefer original alias for regex
          matchLabel: label,
          entity,
          source: entity._runtimeAlias ? "runtime_overlay" : "canonical",
        });
      }
    }
  }

  // Prefer longer keys; for equal length prefer exact rawLabel === matchLabel
  rows.sort((a, b) => {
    if (b.length !== a.length) return b.length - a.length;
    return a.key.localeCompare(b.key);
  });

  // Dedupe identical key+entity keeping first (longest already sorted)
  const seen = new Set();
  const deduped = [];
  for (const row of rows) {
    const id = `${row.key}::${row.entity.id}`;
    if (seen.has(id)) continue;
    seen.add(id);
    deduped.push(row);
  }
  return deduped;
}

/**
 * Resolve a raw mention string to a canonical entity.
 * @param {string} rawMention
 * @param {ReturnType<typeof buildEntityAliasIndex>} index
 * @param {{ entityType?: "brand"|"operator" }} [opts]
 */
export function resolveEntityMention(rawMention, index, opts = {}) {
  if (isBlockedBareParentMention(rawMention)) {
    return {
      entityType: "unresolved",
      canonicalEntityId: null,
      canonicalEntityName: null,
      resolverVersion: RESOLVER_VERSION,
      reason: "blocked_bare_parent",
    };
  }

  const candidateKeys = decoratedNameKeys(rawMention);
  if (!candidateKeys.length) {
    return {
      entityType: "unresolved",
      canonicalEntityId: null,
      canonicalEntityName: null,
      resolverVersion: RESOLVER_VERSION,
    };
  }

  for (const key of candidateKeys) {
    for (const row of index) {
      if (opts.entityType && row.entity.entityType !== opts.entityType) continue;
      if (row.key === key) {
        return {
          entityType: row.entity.entityType,
          canonicalEntityId: row.entity.id,
          canonicalEntityName: row.entity.name,
          matchedAlias: row.rawLabel,
          resolverVersion: RESOLVER_VERSION,
          matchKey: key,
        };
      }
    }
  }

  return {
    entityType: "unresolved",
    canonicalEntityId: null,
    canonicalEntityName: null,
    resolverVersion: RESOLVER_VERSION,
  };
}

/**
 * Find non-overlapping matches in text with word-boundary protection
 * and longest/specific alias precedence.
 *
 * Matching uses original rawLabel against text (case-insensitive), plus
 * decorated variants where the variant appears as a contiguous phrase.
 *
 * @param {string} text
 * @param {ReturnType<typeof buildEntityAliasIndex>} index
 * @param {{ applyContextualAliases?: boolean }} [options]
 */
export function findEntitySpans(text, index, options = {}) {
  const original = String(text || "");
  const { text: source, toOriginal } = stripMarkdownNoiseForEntityMatchWithMap(original);
  const occupied = new Array(source.length).fill(false);
  const spans = [];
  const applyContextual = options.applyContextualAliases !== false;

  // Build unique search labels longest-first from index rows
  const searchRows = [];
  const seenLabelEntity = new Set();
  for (const row of index) {
    const labels = new Set([row.rawLabel]);
    // Also try matchLabel if it differs and appears as words in text
    if (row.matchLabel && normalizeMatchKey(row.matchLabel) !== normalizeMatchKey(row.rawLabel)) {
      // Reconstruct a human phrase from match key is lossy; use decorated forms of rawLabel
      for (const k of decoratedNameKeys(row.rawLabel)) {
        // Prefer scanning for known original aliases only; decorated applied via resolve
        if (k === normalizeMatchKey(row.rawLabel)) continue;
      }
    }
    const id = `${normalizeMatchKey(row.rawLabel)}::${row.entity.id}`;
    if (seenLabelEntity.has(id)) continue;
    seenLabelEntity.add(id);
    searchRows.push(row);
  }

  // Extra: add decorated surface forms that appear in live answers
  // e.g. "Hotel Equities CALA" when alias is "Hotel Equities (CALA)"
  for (const row of index) {
    for (const key of decoratedNameKeys(row.rawLabel)) {
      if (key === normalizeMatchKey(row.rawLabel)) continue;
      // Build a loose regex from key words
      const words = key.split(" ").filter(Boolean);
      if (words.length < 2) continue;
      const surface = words.join(" ");
      const id = `${key}::${row.entity.id}::decorated`;
      if (seenLabelEntity.has(id)) continue;
      seenLabelEntity.add(id);
      searchRows.push({
        ...row,
        rawLabel: surface,
        length: key.length,
        key,
      });
    }
  }

  searchRows.sort((a, b) => b.length - a.length || a.key.localeCompare(b.key));

  for (const row of searchRows) {
    if (!row.rawLabel || row.rawLabel.length < 2) continue;
    if (isBlockedBareParentMention(row.rawLabel) && !row.entity.isParentCompanyLabel) {
      continue;
    }
    const pattern = new RegExp(`\\b${escapeRegExp(row.rawLabel)}\\b`, "gi");
    let match;
    while ((match = pattern.exec(source)) !== null) {
      const start = match.index;
      const end = start + match[0].length;
      let overlaps = false;
      for (let i = start; i < end; i++) {
        if (occupied[i]) {
          overlaps = true;
          break;
        }
      }
      if (overlaps) continue;

      // Parent-company-only labels: longest-first already prefers specific brands.
      for (let i = start; i < end; i++) occupied[i] = true;
      const origStart = toOriginal[start];
      const origEndExclusive = toOriginal[end - 1] + 1;
      spans.push({
        start: origStart,
        end: origEndExclusive,
        rawMention: original.slice(origStart, origEndExclusive),
        entity: row.entity,
        matchedAlias: row.rawLabel,
        resolverVersion: RESOLVER_VERSION,
      });
    }
  }

  if (applyContextual) {
    applyContextualAliasSpans(original, source, toOriginal, occupied, spans, index);
  }

  spans.sort((a, b) => a.start - b.start);
  return spans;
}

/**
 * Apply governed CONTEXTUAL_ALIAS rules (short surface + required parent context).
 * Never adds a global bare alias to the index.
 */
export function applyContextualAliasSpans(
  original,
  source,
  toOriginal,
  occupied,
  spans,
  index
) {
  if (!index?.length) return spans;
  const entityByName = new Map();
  for (const row of index) {
    if (row?.entity?.name) entityByName.set(row.entity.name, row.entity);
  }

  for (const rule of CONTEXTUAL_ALIAS_RULES) {
    const entity = entityByName.get(rule.canonicalEntityName);
    if (!entity) continue;

    const pattern = new RegExp(`\\b${escapeRegExp(rule.surface)}\\b`, "gi");
    let match;
    while ((match = pattern.exec(source)) !== null) {
      const start = match.index;
      const end = start + match[0].length;
      let overlaps = false;
      for (let i = start; i < end; i++) {
        if (occupied[i]) {
          overlaps = true;
          break;
        }
      }
      if (overlaps) continue;

      // Skip if already matched as longer form of same entity in original offsets
      const origStart = toOriginal[start];
      const origEndExclusive = toOriginal[end - 1] + 1;
      if (
        spans.some(
          (s) =>
            s.entity?.id === entity.id &&
            !(origEndExclusive <= s.start || origStart >= s.end)
        )
      ) {
        continue;
      }

      if (!contextualAliasAccepted(original, origStart, origEndExclusive, rule)) {
        continue;
      }

      for (let i = start; i < end; i++) occupied[i] = true;
      spans.push({
        start: origStart,
        end: origEndExclusive,
        rawMention: original.slice(origStart, origEndExclusive),
        entity,
        matchedAlias: rule.surface,
        matchKind: "CONTEXTUAL_ALIAS",
        contextualRuleId: rule.id,
        resolverVersion: RESOLVER_VERSION,
      });
    }
  }
  return spans;
}

/**
 * Deterministic acceptance for a contextual short-brand match.
 */
export function contextualAliasAccepted(originalText, start, end, rule) {
  const text = String(originalText || "");
  const windowChars = rule.contextWindowChars ?? 220;
  const a = Math.max(0, start - windowChars);
  const b = Math.min(text.length, end + windowChars);
  const window = text.slice(a, b);
  const local = text.slice(Math.max(0, start - 40), Math.min(text.length, end + 40));

  for (const re of rule.rejectPatterns || []) {
    if (re.test(local) || re.test(window)) return false;
  }

  const parentRe = new RegExp(`\\b${escapeRegExp(rule.requiredParentContext)}\\b`, "i");
  if (!parentRe.test(window)) return false;

  // Prefer brand-family / structural Hilton context near the surface token
  const peerRe = new RegExp(
    `\\b(?:${(rule.brandFamilyPeers || []).map(escapeRegExp).join("|")})\\b`,
    "i"
  );
  const surface = rule.surface;
  const parent = rule.requiredParentContext;
  const structural =
    peerRe.test(window) ||
    new RegExp(
      `\\b${escapeRegExp(parent)}\\b[\\s\\S]{0,120}\\b${escapeRegExp(surface)}\\b|\\b${escapeRegExp(surface)}\\b[\\s\\S]{0,120}\\b${escapeRegExp(parent)}\\b`,
      "i"
    ).test(window) ||
    // Enumeration style: Canopy, Curio, Signia, Hilton, ...
    new RegExp(
      `\\b${escapeRegExp(surface)}\\b(?:\\s*[,/;]\\s*[A-Za-z][A-Za-z0-9 &'-]*){0,6}\\s*[,/;]?\\s*\\b${escapeRegExp(parent)}\\b|\\b${escapeRegExp(parent)}\\b(?:\\s*[,/;]\\s*[A-Za-z][A-Za-z0-9 &'-]*){0,6}\\s*[,/;]?\\s*\\b${escapeRegExp(surface)}\\b`,
      "i"
    ).test(window);

  return structural;
}

export { RESOLVER_VERSION as ENTITY_RESOLVER_VERSION };
