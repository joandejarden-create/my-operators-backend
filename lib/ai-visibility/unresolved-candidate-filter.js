/**
 * Deterministic unresolved-candidate noise filter (Phase 2B).
 * Versioned, inspectable rules — does not invent entities.
 */

export const UNRESOLVED_FILTER_VERSION = "ai_visibility_unresolved_filter_v1";

const GEOGRAPHY = new Set(
  [
    "mexico",
    "mexican",
    "caribbean",
    "latin america",
    "latam",
    "cala",
    "mexico city",
    "monterrey",
    "guadalajara",
    "puebla",
    "queretaro",
    "playa",
    "playa del carmen",
    "cancun",
    "cancún",
    "puerto rico",
    "colombia",
    "brazil",
    "argentina",
    "chile",
    "peru",
    "panama",
    "costa rica",
    "dominican republic",
    "jamaica",
    "bahamas",
    "united states",
    "usa",
    "europe",
    "asia",
  ].map((s) => s.toLowerCase())
);

const GENERIC_VOCAB = new Set(
  [
    "brand",
    "brands",
    "operator",
    "operators",
    "hotel",
    "hotels",
    "resort",
    "resorts",
    "practical",
    "recommendation",
    "recommendations",
    "conversion",
    "development",
    "developments",
    "priority",
    "best",
    "owner",
    "owners",
    "ownership",
    "recommended",
    "shortlist",
    "candidate",
    "candidates",
    "option",
    "options",
    "particularly",
    "independent",
    "lifestyle",
    "distinctive",
    "probably",
    "including",
    "example",
    "consider",
    "considered",
    "management",
    "agreement",
    "franchise",
    "distribution",
    "pipeline",
    "portfolio",
    "urban",
    "leisure",
    "historic",
    "premium",
    "full service",
    "full-service",
    "select service",
    "select-service",
    "upper upscale",
    "upper-upscale",
    "upscale",
    "luxury",
    "midscale",
    "economy",
    "chain scale",
    "soft brand",
    "soft-brand",
    "collection",
    "collections",
    "parent company",
    "international",
    "region",
    "market",
    "markets",
    "project",
    "projects",
    "asset",
    "assets",
    "value",
    "fit",
    "strong",
    "primary",
    "secondary",
    "table",
    "section",
    "summary",
    "overview",
    "conclusion",
    "note",
    "notes",
    "medium",
    "conventional",
    "relatively",
    "boutique",
    "flexible",
    "corporate",
    "highly",
    "selective",
    "watch",
    "design intensive",
    "design-intensive",
    "carmen",
    "downtown",
    "honors",
    "loyalty",
    "adr",
    "pip",
    "capex",
    "rfp",
    "hma",
  ].map((s) => s.toLowerCase())
);

const BARE_PARENT_NOISE = new Set(
  ["hilton", "marriott", "hyatt", "ihg", "accor", "wyndham", "choice"].map((s) =>
    s.toLowerCase()
  )
);

const SENTENCE_STARTERS = new Set(
  [
    "the",
    "a",
    "an",
    "for",
    "in",
    "on",
    "of",
    "and",
    "or",
    "with",
    "which",
    "when",
    "this",
    "that",
    "these",
    "those",
    "most",
    "more",
    "some",
    "any",
    "all",
    "each",
    "from",
    "into",
    "about",
    "after",
    "before",
    "under",
    "over",
    "between",
    "during",
    "while",
    "where",
    "what",
    "who",
    "how",
    "why",
  ].map((s) => s.toLowerCase())
);

/**
 * @param {string} raw
 */
export function classifyUnresolvedNoise(raw) {
  const text = String(raw || "").trim();
  if (!text) return { keep: false, reason: "empty" };
  if (/^[\d\W_]+$/.test(text)) return { keep: false, reason: "punctuation_or_numeric" };
  if (text.length < 4) return { keep: false, reason: "too_short" };

  const key = text
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!key) return { keep: false, reason: "empty_normalized" };
  if (GEOGRAPHY.has(key)) return { keep: false, reason: "geography" };
  if (GENERIC_VOCAB.has(key)) return { keep: false, reason: "generic_vocab" };
  if (BARE_PARENT_NOISE.has(key)) return { keep: false, reason: "bare_parent_company" };
  if (SENTENCE_STARTERS.has(key)) return { keep: false, reason: "sentence_starter" };

  // Adjective/adverb-only tokens (capitalized mid-sentence noise)
  if (/^(ly|ive|ing|ous|ful)$/i.test(key.slice(-3)) && !/\s/.test(key) && key.length <= 12) {
    // keep brand-like - too aggressive; skip
  }
  if (
    !/\s/.test(key) &&
    /^(medium|high|low|small|large|major|minor|local|global|regional|national)$/i.test(key)
  ) {
    return { keep: false, reason: "scalar_adjective" };
  }

  // Single-token sentence starters / adjectives already covered; multi-token
  // all-generic compounds: "Hotel Brand", "Resort Development"
  const parts = key.split(" ");
  if (parts.length >= 2 && parts.every((p) => GENERIC_VOCAB.has(p) || GEOGRAPHY.has(p))) {
    return { keep: false, reason: "generic_compound" };
  }

  // Heading-like ALL CAPS short phrases without hotel-company cues
  if (/^[A-Z\s]{4,20}$/.test(text) && !/\b(Hotel|Hotels|Collection|Hospitality|Resorts)\b/.test(text)) {
    return { keep: false, reason: "section_heading_like" };
  }

  return { keep: true, reason: "possible_entity" };
}

/**
 * @param {{ rawMention: string, position?: number }[]} candidates
 */
export function filterUnresolvedCandidates(candidates) {
  const raw = Array.isArray(candidates) ? candidates : [];
  const kept = [];
  const rejected = [];
  for (const c of raw) {
    const decision = classifyUnresolvedNoise(c?.rawMention);
    if (decision.keep) {
      kept.push({ ...c, filterDecision: decision.reason, filterVersion: UNRESOLVED_FILTER_VERSION });
    } else {
      rejected.push({ ...c, filterDecision: decision.reason, filterVersion: UNRESOLVED_FILTER_VERSION });
    }
  }
  const rawCount = raw.length;
  const filteredCount = kept.length;
  const noiseReductionPercent =
    rawCount === 0 ? 0 : Math.round(((rawCount - filteredCount) / rawCount) * 1000) / 10;
  return {
    filterVersion: UNRESOLVED_FILTER_VERSION,
    rawUnresolvedCount: rawCount,
    filteredUnresolvedCount: filteredCount,
    rejectedCount: rejected.length,
    noiseReductionPercent,
    kept,
    rejected,
  };
}
