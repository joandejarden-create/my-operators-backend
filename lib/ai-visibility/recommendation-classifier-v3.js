/**
 * AI Visibility recommendation classifier v3.3 (deterministic).
 *
 * Hardening 4 resume (DEV-only, post amended GT):
 * - Entity-aware Preferred Hotels brand-noise strip (Capital-P Preferred) so bare
 *   "preferred" positive cue does not false-fire on nearby non-Preferred mentions
 * - Consideration catalog headers exclude broad topic sections (consorcios / opción preferida)
 * - questionStatus continues to derive from final role
 * - Broader shortlist→explicit / rank / first structural paths retained from v3.2
 *   when alternate tightenings caused net DEV regressions (C11)
 */

export const RECOMMENDATION_CLASSIFIER_VERSION =
  "ai_visibility_recommendation_classifier_v3_3";

export const RECOMMENDATION_ROLE_PRECEDENCE = Object.freeze([
  "negative_or_qualified",
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "source_only",
  "no_mention",
]);

const NEGATIVE_CUES =
  /\b(not\s+recommend(?:ed)?|less\s+suitable|weaker\s+fit|may\s+be\s+difficult|not\s+ideal|unlikely|avoid|less\s+attractive|only\s+if|depends\s+heavily\s+on|poor\s+fit|weak\s+fit|would\s+not\s+consider|do\s+not\s+consider|may\s+work\s+but|would\s+require|better\s+suited\s+to|no\s+recomend(?:ad[oa]|ar)|poco\s+adecuado|evitar|no\s+ideal|menos\s+adecuado)\b/i;

const ASSOCIATION_CUES =
  /\b(commonly\s+(?:associated|considered)|associated\s+with|typically\s+(?:associated|considered)|often\s+(?:associated|considered)|frequently\s+(?:associated|considered)|market\s+association|brands?\s+associated|options?\s+include|brands?\s+typically\s+considered|can\s+be\s+considered\s+alongside|belongs?\s+in\s+the\s+consideration\s+set|consideration\s+set|se\s+consideran|suelen\s+considerar|opciones\s+incluyen)\b/i;

const POSITIVE_REC_CUES =
  /\b(recommend(?:ed)?|should\s+consider|may\s+also\s+consider|also\s+consider|i\s+would\s+(?:consider|shortlist|solicit)|strong\s+(?:fit|candidate|alternative|option|options|choice|choices)|first\s+call|best\s+fit|best\s+suited|best\s+value(?:\s+engineering)?(?:\s+option)?|good\s+(?:option|fit)|good\s+fit|preferred(?:\s+option)?|particularly\s+suitable|likely\s+fit|solicit\s+proposals?\s+from|primary\s+option|leading\s+candidate|issue\s+an\s+rfp\s+to|recomendad[oa]s?|recomiendo|opci[oó]n\s+(?:fuerte|s[oó]lida|preferida)|opciones\s+(?:fuertes|s[oó]lidas)|mejor\s+opci[oó]n|mejor\s+encaje)\b/i;

/** Strip Preferred Hotels brand mentions so bare "preferred" cue does not false-fire (C4). */
export function stripPreferredHotelsBrandNoise(text) {
  // Capital-P Preferred = brand token; keep lowercase "preferred option".
  return String(text || "").replace(
    /\*{0,2}Preferred(?:\s+Hotels(?:\s*(?:&|and)\s*Resorts)?)?\*{0,2}/g,
    " "
  );
}

export function matchesPositiveRecommendationCue(text, rawMention = null) {
  const raw = String(rawMention || "");
  // Do not strip when the span itself is Preferred Hotels (brand-name collision with cue).
  const mentionIsPreferredBrand = /^preferred\b/i.test(raw);
  const cleaned = mentionIsPreferredBrand
    ? String(text || "")
    : stripPreferredHotelsBrandNoise(text);
  return POSITIVE_REC_CUES.test(cleaned);
}

const FIRST_CALL_CUES =
  /\b(first\s+call|first\s+choice|first\s+option|top\s+choice|primary\s+recommendation|shortlist\s+first|consider\s+first|begin\s+with|start\s+with|my\s+first|primera\s+opci[oó]n|primera\s+alternativa|opci[oó]n\s+principal|prioridad\s+(?:1|uno))\b/i;

const PASSING_CUES =
  /\b(for\s+example|such\s+as|including|among\s+others|also\s+mentioned|e\.g\.|i\.e\.)\b/i;

const SOURCE_ONLY_CUES =
  /\b(according\s+to|cited\s+(?:in|by)|source:|see\s+also)\b/i;

const EXAMPLE_LIST_HEADERS =
  /\b(examples?\s+include|for\s+example|including|such\s+as|other\s+alternatives|background|context)\b/i;

const RANKED_SHORTLIST_HEADERS =
  /\b(recommended\s+shortlist|shortlist|top\s+\d+|top\s+options?|ranked\s+(?:list|options?|brands?)|priority\s+list|brands?\s+to\s+consider|operators?\s+to\s+consider|leading\s+options?|candidate\s+set|i\s+would\s+(?:consider|shortlist|solicit)|solicit\s+proposals?|primary\s+candidates?|recommended\s+brands?|recommended\s+operators?|marcas?\s+a\s+considerar|lista\s+corta|marcas?\s+recomendadas?|commonly\s+cited\s+choices?|las\s+m[aá]s\s+citadas|key\s+(?:upper[\s-]?upscale\s+)?brands?\s+to\s+consider)\b/i;

const CONSIDERATION_CATALOG_HEADERS =
  /\b(consideration\s+set|options?\s+include|brands?\s+typically\s+considered|the\s+major\s+soft\s+brand\s+collections?|marcas?\s+[\"']?soft[\"']?\s+o\s+colecciones)\b/i;

const ASSOCIATION_INTENT_TERRITORIES = new Set([
  "branded residences",
  "chain scale / positioning",
  "mixed use",
]);

const STRONG_REC_INTENT_TERRITORIES = new Set([
  "brand selection",
  "operator selection",
  "conversion",
  "hma vs franchise",
  "development strategy",
]);

export function isDocumentTopicHeading(line) {
  const t = String(line || "")
    .replace(/^#{1,6}\s*/, "")
    .replace(/^\d+[.)]\s*/, "")
    .replace(/[*_`]/g, "")
    .trim();
  if (!t) return false;
  const words = t.split(/\s+/).filter(Boolean);
  if (words.length <= 6 && !/[:(]/.test(t) && !/\b(and|y|de|del|para|with|sin|vs)\b/i.test(t)) {
    return false;
  }
  if (words.length >= 8) return true;
  if (
    /\b(consorcios?|afiliaci[oó]n|comercializaci[oó]n|overview|introducci[oó]n|contexto|methodology|conclusi[oó]n)\b/i.test(
      t
    )
  ) {
    return true;
  }
  return false;
}

export function detectRankMarker(text, start) {
  const source = String(text || "");
  const lineStart = source.lastIndexOf("\n", start - 1) + 1;
  const le = source.indexOf("\n", start);
  const line = source.slice(lineStart, le === -1 ? source.length : le);
  const prefix = source.slice(lineStart, start);

  const tableRank = prefix.match(/\|\s*\*?\*?(\d+)\*?\*?\s*\|\s*\*?\*?$/);
  if (tableRank) return parseInt(tableRank[1], 10);

  const numbered = prefix.match(/(?:^|[\s|])(?:#)?(\d+)[.)]\s*\*?\*?$/);
  if (numbered) return parseInt(numbered[1], 10);

  const headingRank = prefix.match(/^#{1,3}\s*(\d+)[.)]\s+/);
  if (headingRank) {
    if (isDocumentTopicHeading(line)) return null;
    return parseInt(headingRank[1], 10);
  }

  const priority = prefix.match(/\bpriority\s+(\d+)\s*[:|]?\s*\*?\*?$/i);
  if (priority) return parseInt(priority[1], 10);

  const prioridad = prefix.match(/\bprioridad\s+(\d+)\s*[:|]?\s*\*?\*?$/i);
  if (prioridad) return parseInt(prioridad[1], 10);

  const hash = source.slice(Math.max(0, start - 12), start).match(/#\s*(\d+)\s*$/);
  if (hash) return parseInt(hash[1], 10);

  return null;
}

export function detectBulletLine(text, start) {
  const source = String(text || "");
  const lineStart = source.lastIndexOf("\n", start - 1) + 1;
  const prefix = source.slice(lineStart, start).trim();
  return /^(?:[-*]|\u2022)\s*(?:\*\*)?$/.test(prefix) || /^(?:[-*]|\u2022)\s*$/.test(prefix);
}

export function detectOrderedListContext(text, start) {
  const source = String(text || "");
  const around = source.slice(Math.max(0, start - 280), Math.min(source.length, start + 280));
  const markers = around.match(/(?:^|\n)\s*(?:#)?\d+[.)]\s+/g);
  return Boolean(markers && markers.length >= 2);
}

export function detectNumberedBrandHeadingList(text, start) {
  const source = String(text || "");
  const around = source.slice(Math.max(0, start - 900), Math.min(source.length, start + 900));
  const headings = [...around.matchAll(/(?:^|\n)(#{1,3}\s+\d+[.)]\s+[^\n]+)/g)].map((m) => m[1]);
  return headings.filter((h) => !isDocumentTopicHeading(h)).length >= 2;
}

function inferSectionRole(title, bodySample) {
  const t = `${title}\n${bodySample}`;
  if (SOURCE_ONLY_CUES.test(t) || /\bsources?\b/i.test(title)) return "source";
  if (/\bcomparison|versus|vs\.?|alternatives?\b/i.test(title)) return "comparison";
  if (RANKED_SHORTLIST_HEADERS.test(t)) return "recommendation";
  if (CONSIDERATION_CATALOG_HEADERS.test(title)) return "consideration";
  if (EXAMPLE_LIST_HEADERS.test(t)) return "context";
  if (POSITIVE_REC_CUES.test(stripPreferredHotelsBrandNoise(title)) && RANKED_SHORTLIST_HEADERS.test(title))
    return "recommendation";
  return "unknown";
}

export function detectResponseSections(text) {
  const source = String(text || "");
  const sections = [];
  const headingRe = /(?:^|\n)(#{1,3}\s+[^\n]+|[A-Z][^\n]{0,80}:)\s*(?=\n|$)/g;
  const matches = [];
  let m;
  while ((m = headingRe.exec(source)) !== null) {
    matches.push({ index: m.index + (m[0].startsWith("\n") ? 1 : 0), title: m[1].trim() });
  }
  if (!matches.length) {
    return [
      {
        start: 0,
        end: source.length,
        title: "",
        sectionRole: inferSectionRole("", source.slice(0, 240)),
      },
    ];
  }
  for (let i = 0; i < matches.length; i++) {
    const start = matches[i].index;
    const end = i + 1 < matches.length ? matches[i + 1].index : source.length;
    const title = matches[i].title;
    const body = source.slice(start, Math.min(end, start + 280));
    sections.push({ start, end, title, sectionRole: inferSectionRole(title, body) });
  }
  if (matches[0].index > 0) {
    sections.unshift({
      start: 0,
      end: matches[0].index,
      title: "",
      sectionRole: inferSectionRole("", source.slice(0, 240)),
    });
  }
  return sections;
}

export function sectionRoleAt(text, start, sections) {
  const list = sections || detectResponseSections(text);
  for (const s of list) {
    if (start >= s.start && start < s.end) return s.sectionRole;
  }
  return "unknown";
}

export function sectionAt(text, start, sections) {
  const list = sections || detectResponseSections(text);
  for (const s of list) {
    if (start >= s.start && start < s.end) return s;
  }
  return null;
}

function lookBehindContext(text, start, chars = 350) {
  return String(text || "").slice(Math.max(0, start - chars), start);
}

export function detectShortlistItemPosition(text, start, sections) {
  const source = String(text || "");
  const sec = sectionAt(source, start, sections);
  if (!sec) return null;
  const rankedCtx =
    sec.sectionRole === "recommendation" ||
    RANKED_SHORTLIST_HEADERS.test(sec.title) ||
    RANKED_SHORTLIST_HEADERS.test(lookBehindContext(source, start, 420));
  if (!rankedCtx) return null;

  const body = source.slice(sec.start, sec.end);
  const itemRe = /(?:^|\n)\s*(?:[-*]|\u2022|\d+[.)]|#{1,3}\s*\d+[.)])\s+/g;
  const items = [];
  let m;
  while ((m = itemRe.exec(body)) !== null) {
    items.push(sec.start + m.index + (m[0].startsWith("\n") ? 1 : 0));
  }
  if (items.length < 2) return null;
  for (let i = 0; i < items.length; i++) {
    const next = i + 1 < items.length ? items[i + 1] : sec.end;
    if (start >= items[i] && start < next) return i + 1;
  }
  return null;
}

/**
 * Positive cues must be entity-linked: after the mention, or a short same-line lead-in.
 * Avoids intro-paragraph "should consider" bleed and Preferred Hotels false positives (C4/C9).
 */
export function hasEntityLinkedPositiveCue(text, start, end, rawMention = null) {
  const source = String(text || "");
  const s = Number(start) || 0;
  const e = Number(end) || s;
  const afterMention = source.slice(s, Math.min(source.length, e + 180));
  if (matchesPositiveRecommendationCue(afterMention, rawMention)) return true;
  const lineStart = source.lastIndexOf("\n", s - 1) + 1;
  const beforeOnLine = source.slice(lineStart, s);
  if (beforeOnLine.length <= 120 && matchesPositiveRecommendationCue(beforeOnLine, rawMention)) {
    return true;
  }
  return false;
}

export function classifyMentionRoleV3(mention) {
  const text = String(mention?.text || "");
  const start = Number(mention?.start ?? mention?.mentionPosition ?? 0);
  const end = Number(mention?.end ?? start + String(mention?.rawMention || "").length);
  const snip =
    mention?.contextSnippet ||
    text.slice(Math.max(0, start - 100), Math.min(text.length, end + 100));
  const window = text.slice(Math.max(0, start - 160), Math.min(text.length, end + 120));
  const behind = lookBehindContext(text, start);
  const intent = String(mention?.promptIntentTerritory || "").trim().toLowerCase();
  const sections = mention?.sections || detectResponseSections(text);
  const sectionRole = sectionRoleAt(text, start, sections);
  const sec = sectionAt(text, start, sections);

  const base = { sectionRole, classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION };

  if (NEGATIVE_CUES.test(snip) || NEGATIVE_CUES.test(window)) {
    return {
      ...base,
      role: "negative_or_qualified",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "negative_or_qualified_cue",
    };
  }

  const before = text.slice(Math.max(0, start - 80), start);
  if (
    /(?:alternative\s+to|compared\s+(?:to|with)|versus|vs\.?|unlike|similar\s+to|competitor\s+to|frente\s+a|comparad[oa]\s+con|a\s+diferencia\s+de)\s+(?:\*\*)?$/i.test(
      before
    )
  ) {
    return {
      ...base,
      role: "comparator",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "comparator_object",
    };
  }

  if (SOURCE_ONLY_CUES.test(snip) || sectionRole === "source") {
    return {
      ...base,
      role: "source_only",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "source_section_or_cue",
    };
  }

  const rank = detectRankMarker(text, start);
  const bullet = detectBulletLine(text, start);
  const orderedList = detectOrderedListContext(text, start);
  const numberedBrandList = detectNumberedBrandHeadingList(text, start);
  const exampleHeaderNear = EXAMPLE_LIST_HEADERS.test(behind);
  const lineStart = text.lastIndexOf("\n", start - 1) + 1;
  const lineEnd = text.indexOf("\n", start);
  const line = text.slice(lineStart, lineEnd === -1 ? text.length : lineEnd);
  const tableRankContext = rank != null && /\|/.test(line);
  const shortlistNear =
    RANKED_SHORTLIST_HEADERS.test(behind) ||
    sectionRole === "recommendation" ||
    (sec && RANKED_SHORTLIST_HEADERS.test(sec.title));
  const catalogNear =
    sectionRole === "consideration" || (sec && CONSIDERATION_CATALOG_HEADERS.test(sec.title));
  const associationNear = ASSOCIATION_CUES.test(behind) || ASSOCIATION_CUES.test(snip);
  // Positive cues on entity line or after mention (v3.2). Preferred Hotels no longer matches bare "preferred".
  const lineStartForPos = text.lastIndexOf("\n", start - 1) + 1;
  const lineEndForPos = text.indexOf("\n", start);
  const entityLine = text.slice(lineStartForPos, lineEndForPos === -1 ? text.length : lineEndForPos);
  const afterMention = text.slice(start, Math.min(text.length, end + 160));
  const rawMention = String(mention?.rawMention || "");
  // Preferred Hotels brand stripped for non-Preferred mentions only (C4 collision).
  const positive =
    matchesPositiveRecommendationCue(entityLine, rawMention) ||
    matchesPositiveRecommendationCue(afterMention, rawMention);
  const firstCall = FIRST_CALL_CUES.test(snip) || FIRST_CALL_CUES.test(window);
  const associationIntent = ASSOCIATION_INTENT_TERRITORIES.has(intent);
  const strongRecIntent = STRONG_REC_INTENT_TERRITORIES.has(intent);
  const shortlistPos = detectShortlistItemPosition(text, start, sections);

  const rankedContext =
    tableRankContext ||
    shortlistNear ||
    (orderedList && !exampleHeaderNear) ||
    (numberedBrandList && (shortlistNear || strongRecIntent)) ||
    /\bpriority\b|\bprioridad\b/i.test(behind.slice(0, 120));

  if (bullet && exampleHeaderNear && !shortlistNear && rank == null && !positive) {
    return {
      ...base,
      role: associationIntent || associationNear || catalogNear ? "associated_option" : "passing_mention",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "unordered_example_list",
    };
  }

  if (rank === 1 && (rankedContext || tableRankContext || orderedList || shortlistNear || strongRecIntent || positive)) {
    return {
      ...base,
      role: "first_recommendation",
      explicitRecommendation: true,
      recommendationPosition: 1,
      reason: "rank_marker_1_in_shortlist",
    };
  }

  if (
    rank != null &&
    rank > 1 &&
    (rankedContext ||
      shortlistNear ||
      strongRecIntent ||
      positive ||
      firstCall ||
      (orderedList && !exampleHeaderNear) ||
      sectionRole === "recommendation")
  ) {
    return {
      ...base,
      role: "ranked_recommendation",
      explicitRecommendation: true,
      recommendationPosition: rank,
      reason: "rank_marker_gt_1_in_shortlist",
    };
  }

  if (rank != null && numberedBrandList && !shortlistNear && !positive && !firstCall) {
    return {
      ...base,
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "numbered_brand_catalog",
    };
  }

  if (shortlistPos != null && shortlistNear) {
    return {
      ...base,
      role: shortlistPos === 1 ? "first_recommendation" : "ranked_recommendation",
      explicitRecommendation: true,
      recommendationPosition: shortlistPos,
      reason: shortlistPos === 1 ? "shortlist_position_1" : "shortlist_structural_position",
    };
  }

  if (shortlistNear && (bullet || orderedList) && !positive && !firstCall) {
    const pos = rank != null && rank > 0 ? rank : null;
    if (pos === 1) {
      return {
        ...base,
        role: "first_recommendation",
        explicitRecommendation: true,
        recommendationPosition: 1,
        reason: "shortlist_bullet_rank_1",
      };
    }
    return {
      ...base,
      role: "ranked_recommendation",
      explicitRecommendation: true,
      recommendationPosition: pos,
      reason: "shortlist_bullet_membership",
    };
  }

  const associationIntro = ASSOCIATION_CUES.test(lookBehindContext(text, start, 140));
  if (
    (associationIntro || catalogNear || (associationIntent && !positive && !firstCall && rank == null)) &&
    !shortlistNear &&
    !positive &&
    !firstCall &&
    rank == null
  ) {
    return {
      ...base,
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: catalogNear
        ? "consideration_catalog"
        : associationIntent
          ? "prompt_intent_association_territory"
          : "association_language",
    };
  }

  if (rank == null && firstCall && (shortlistNear || strongRecIntent || positive)) {
    return {
      ...base,
      role: "first_recommendation",
      explicitRecommendation: true,
      recommendationPosition: 1,
      reason: "first_call_language",
    };
  }

  if (rank != null && exampleHeaderNear && !shortlistNear && !positive) {
    return {
      ...base,
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "ranked_under_example_header",
    };
  }

  if (
    !positive &&
    !firstCall &&
    !shortlistNear &&
    rank == null &&
    bullet &&
    numberedBrandList &&
    (strongRecIntent || catalogNear || associationIntro)
  ) {
    return {
      ...base,
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "child_under_numbered_brand_catalog",
    };
  }

  // Positive recommendation cues, or recommendation-section prose (v3.2 path retained for first-rec stability)
  if (positive || (shortlistNear && (strongRecIntent || sectionRole === "recommendation") && !bullet)) {
    return {
      ...base,
      role: "explicit_recommendation",
      explicitRecommendation: true,
      recommendationPosition: null,
      reason: positive ? "positive_recommendation_cue" : "recommendation_section",
    };
  }

  if (catalogNear || (sectionRole === "consideration" && bullet)) {
    return {
      ...base,
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "consideration_section_item",
    };
  }

  if (/\bconsider(?:ed)?\b/i.test(snip) && associationIntent && !shortlistNear) {
    return {
      ...base,
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "consider_in_association_intent",
    };
  }

  if (PASSING_CUES.test(snip) || PASSING_CUES.test(window)) {
    return {
      ...base,
      role: "passing_mention",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "passing_cue",
    };
  }

  if (sectionRole === "comparison") {
    return {
      ...base,
      role: "comparator",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "comparison_section",
    };
  }

  return {
    ...base,
    role: "discussed",
    explicitRecommendation: false,
    recommendationPosition: null,
    reason: "default_discussed",
  };
}

export function assignFirstRecommendationAcrossMentionsV3(classifiedMentions, text) {
  const mentions = (classifiedMentions || []).map((m) => ({ ...m }));

  const byEntity = new Map();
  for (const m of mentions) {
    if (!m.canonicalEntityId || !m.explicitRecommendation) continue;
    const prev = byEntity.get(m.canonicalEntityId);
    if (!prev) {
      byEntity.set(m.canonicalEntityId, m);
      continue;
    }
    const prevPos = prev.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    const curPos = m.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    if (curPos < prevPos || (curPos === prevPos && m.mentionPosition < prev.mentionPosition)) {
      byEntity.set(m.canonicalEntityId, m);
    }
  }

  const ordered = [...byEntity.values()].sort((a, b) => {
    const ap = a.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    const bp = b.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
    if (ap !== bp) return ap - bp;
    return a.mentionPosition - b.mentionPosition;
  });

  if (ordered.length && !ordered.some((m) => m.role === "first_recommendation")) {
    const first = ordered[0];
    // Leading explicit/ranked entity after structure sort — not arbitrary first text mention
    if (
      first.recommendationPosition === 1 ||
      first.role === "ranked_recommendation" ||
      first.role === "explicit_recommendation" ||
      first.classificationReason === "first_call_language" ||
      first.classificationReason === "rank_marker_1_in_shortlist" ||
      first.classificationReason === "shortlist_position_1"
    ) {
      first.role = "first_recommendation";
      first.recommendationPosition = 1;
      first.explicitRecommendation = true;
    }
  }

  const firstEntityId = ordered.find((m) => m.role === "first_recommendation")?.canonicalEntityId;

  for (const m of mentions) {
    if (m.role === "first_recommendation" && m.canonicalEntityId !== firstEntityId) {
      m.role =
        m.recommendationPosition && m.recommendationPosition > 1
          ? "ranked_recommendation"
          : "explicit_recommendation";
    }
  }

  if (firstEntityId) {
    const primary = mentions
      .filter((m) => m.canonicalEntityId === firstEntityId && m.explicitRecommendation)
      .sort((a, b) => {
        const ap = a.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
        const bp = b.recommendationPosition ?? Number.MAX_SAFE_INTEGER;
        if (ap !== bp) return ap - bp;
        return a.mentionPosition - b.mentionPosition;
      })[0];
    if (primary) {
      primary.role = "first_recommendation";
      primary.recommendationPosition = 1;
      primary.explicitRecommendation = true;
    }
    for (const m of mentions) {
      if (m.canonicalEntityId !== firstEntityId || m === primary) continue;
      if (m.explicitRecommendation && m.role === "first_recommendation") {
        m.role = "ranked_recommendation";
      }
    }
  }

  let next = 2;
  const seen = new Set(firstEntityId ? [firstEntityId] : []);
  for (const m of ordered) {
    if (seen.has(m.canonicalEntityId)) continue;
    seen.add(m.canonicalEntityId);
    const structural =
      String(m.classificationReason || "").includes("rank_marker") ||
      String(m.classificationReason || "").includes("shortlist_");
    if (!structural) continue;
    if (m.recommendationPosition == null) m.recommendationPosition = next;
    if (m.role === "explicit_recommendation" && m.recommendationPosition > 1) {
      m.role = "ranked_recommendation";
    }
    for (const row of mentions) {
      if (row.canonicalEntityId === m.canonicalEntityId && row.explicitRecommendation) {
        if (row.recommendationPosition == null) row.recommendationPosition = m.recommendationPosition;
        if (row.role === "explicit_recommendation" && row.recommendationPosition > 1) {
          row.role = "ranked_recommendation";
        }
      }
    }
    next = Math.max(next, (m.recommendationPosition || next) + 1);
  }

  return mentions;
}

export function classifyMentionRoleV2(mention) {
  return classifyMentionRoleV3(mention);
}

export function assignFirstRecommendationAcrossMentions(classifiedMentions, text) {
  return assignFirstRecommendationAcrossMentionsV3(classifiedMentions, text);
}

export function classifyMentionRole(mention) {
  return classifyMentionRoleV3({
    ...mention,
    text: mention.text || mention.contextSnippet || "",
    start: mention.mentionPosition ?? 0,
  }).role;
}

export function questionStatusFromRecommendationRole(role, entityPresent) {
  if (!entityPresent) return "MISSING";
  if (role === "first_recommendation") return "FIRST_RECOMMENDED";
  if (role === "ranked_recommendation" || role === "explicit_recommendation") return "RECOMMENDED";
  if (role === "negative_or_qualified") return "NEGATIVE_OR_NOT_RECOMMENDED";
  if (role === "associated_option" || role === "passing_mention") return "PRESENT";
  if (role === "discussed" || role === "comparator" || role === "source_only") {
    return "DISCUSSION_ONLY";
  }
  if (entityPresent) return "PRESENT";
  return "NOT_APPLICABLE";
}
