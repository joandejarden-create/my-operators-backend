/**
 * AI Visibility recommendation evidence extraction v4 (deterministic).
 *
 * Stage 1: entity-local bounded context → structured evidence facts.
 * Does not assign final recommendation roles.
 *
 * Hardening 5: cues must be entity-linked via bounded scope; no document-wide
 * positive-word inheritance from other brands.
 */

import {
  isDocumentTopicHeading,
  detectRankMarker,
  detectBulletLine,
  detectOrderedListContext,
  detectResponseSections,
  sectionAt,
  stripPreferredHotelsBrandNoise,
} from "./recommendation-classifier-v3.js";

export const RECOMMENDATION_EVIDENCE_VERSION =
  "ai_visibility_recommendation_evidence_v4";

/** @typedef {'RANKED_RECOMMENDATION_HEADING'|'CONSIDERATION_SET_HEADING'|'GENERAL_RECOMMENDATION_HEADING'|'NEUTRAL_CATALOG_HEADING'|'DESCRIPTIVE_HEADING'|'OTHER'} HeadingSemanticType */

const DIRECT_POSITIVE_RE =
  /\b(recommend(?:ed|ing)?|we\s+recommend|i\s+(?:would\s+)?recommend|recomiendo|recomendad[oa]s?|strong\s+(?:fit|candidate|alternative|option|options|choice|choices)|best\s+fit|best\s+suited|best\s+value(?:\s+engineering)?(?:\s+option)?|good\s+(?:option|fit)|well\s+suited|preferred\s+(?:option|choice|fit)|opci[oó]n\s+(?:fuerte|s[oó]lida|preferida)|opciones\s+(?:fuertes|s[oó]lidas)|mejor\s+(?:opci[oó]n|encaje)|buen\s+encaje|particularly\s+suitable|likely\s+fit|primary\s+option|leading\s+candidate|issue\s+an\s+rfp\s+to)\b/i;

const DIRECT_NEGATIVE_RE =
  /\b(not\s+recommend(?:ed)?|less\s+suitable|weaker\s+fit|may\s+be\s+difficult|not\s+ideal|unlikely|avoid|less\s+attractive|poor\s+fit|weak\s+fit|would\s+not\s+consider|do\s+not\s+consider|no\s+recomend(?:ad[oa]|ar)|poco\s+adecuado|evitar|no\s+ideal|menos\s+adecuado)\b/i;

const LEAD_CUE_RE =
  /\b(first\s+call|first\s+choice|first\s+option|top\s+choice|primary\s+recommendation|primary\s+option|leading\s+candidate|shortlist\s+first|consider\s+first|begin\s+with|start\s+with|my\s+first|primera\s+opci[oó]n|primera\s+alternativa|opci[oó]n\s+principal|prioridad\s+(?:1|uno)|1st\s+(?:choice|recommendation)|first\s+recommendation|i\s+would\s+shortlist|issue\s+an\s+rfp\s+to|solicit\s+proposals?\s+from)\b/i;

const CONSIDERATION_SET_RE =
  /\b(options?\s+include|examples?\s+include|for\s+example|brands?\s+to\s+consider|brands?\s+typically\s+considered|brands?\s+commonly\s+(?:considered|cited|associated)|commonly\s+associated(?:\s+with)?|frequently\s+associated(?:\s+with)?|typically\s+associated(?:\s+with)?|associated\s+with|consideration\s+set|alternatives?\s+include|opciones\s+incluyen|marcas?\s+a\s+considerar|marcas?\s+[^\n.]{0,40}considerad|conviene\s+considerar|m[aá]s\s+conviene\s+considerar|se\s+consideran|commonly\s+cited\s+choices?|candidate\s+set|lista\s+corta|most\s+commonly\s+considered|(?:brands?|options?|alternatives?|marcas?)\s+[^\n.]{0,80}\b(?:include|incluyen|considerar))\b/i;

const CONSIDERATION_HEADING_RE =
  /\b(brands?\s+to\s+consider|operators?\s+to\s+consider|consideration\s+set|options?\s+include|examples?\s+include|brands?\s+typically\s+considered|commonly\s+cited|marcas?\s+a\s+considerar|opciones\s+incluyen|key\s+(?:upper[\s-]?upscale\s+)?brands?\s+to\s+consider|commonly\s+associated|brands?\s+associated|considerad)\b/i;

const ORDERED_REC_CONTEXT_RE =
  /\b(i\s+would\s+(?:solicit|shortlist|consider)|solicit\s+proposals?|start\s+with|begin\s+with|recommended\s+shortlist|priority\s+list|ranked|top\s+options?|shortlist)\b/i;

const COMPARATOR_BEFORE_RE =
  /(?:alternative\s+to|compared\s+(?:to|with)|versus|vs\.?|unlike|similar\s+to|competitor\s+to|frente\s+a|comparad[oa]\s+con|a\s+diferencia\s+de)\s+(?:\*\*)?$/i;

const COMPARATOR_LOCAL_RE =
  /\b(compared\s+(?:to|with)|versus|vs\.?|unlike|frente\s+a|comparad[oa]\s+con)\b/i;

const SOURCE_ONLY_RE =
  /\b(according\s+to|cited\s+(?:in|by)|source:|see\s+also)\b/i;

const PASSING_RE =
  /\b(for\s+example|such\s+as|including|among\s+others|e\.g\.|i\.e\.)\b/i;

const DESCRIPTIVE_RE =
  /\b(operates?|launched|has\s+a\s+presence|offers?|is\s+part\s+of|footprint|presence|portfolio|collection\s+of|designed\s+for|targets?|positioned|alliance|alianza|presencia|lanzad)\b/i;

const RANKED_HEADING_RE =
  /\b(recommended\s+shortlist|ranked\s+(?:list|options?|brands?)|top\s+\d+|top\s+options?|priority\s+list|marcas?\s+recomendadas?|lista\s+corta\s+recomendad|primary\s+candidates?|leading\s+options?|i\s+would\s+solicit\s+proposals?|solicit\s+proposals?\s+from|start\s+with|begin\s+with)\b/i;

const GENERAL_REC_HEADING_RE =
  /\b(recommended\s+brands?|recommended\s+operators?|recommendations?|i\s+would\s+(?:consider|shortlist|solicit))\b/i;

const NEUTRAL_CATALOG_HEADING_RE =
  /\b(relevant\s+(?:hotel\s+)?brands?|soft\s+brand(?:s|\s+collections?)|major\s+(?:soft\s+)?brand|brand\s+overview|market\s+overview|consorcios|afiliaci[oó]n|colecciones?|portfolio\s+overview)\b/i;

const DESCRIPTIVE_HEADING_RE =
  /\b(overview|introducci[oó]n|contexto|methodology|conclusi[oó]n|background|market\s+position|positioning)\b/i;

/**
 * @param {string} title
 * @returns {HeadingSemanticType}
 */
export function classifyHeadingSemanticType(title) {
  const t = String(title || "");
  if (RANKED_HEADING_RE.test(t)) return "RANKED_RECOMMENDATION_HEADING";
  if (CONSIDERATION_HEADING_RE.test(t)) return "CONSIDERATION_SET_HEADING";
  if (GENERAL_REC_HEADING_RE.test(t)) return "GENERAL_RECOMMENDATION_HEADING";
  if (NEUTRAL_CATALOG_HEADING_RE.test(t)) return "NEUTRAL_CATALOG_HEADING";
  if (DESCRIPTIVE_HEADING_RE.test(t)) return "DESCRIPTIVE_HEADING";
  return "OTHER";
}

function lineBounds(text, start) {
  const source = String(text || "");
  const lineStart = source.lastIndexOf("\n", start - 1) + 1;
  const lineEndIdx = source.indexOf("\n", start);
  const lineEnd = lineEndIdx === -1 ? source.length : lineEndIdx;
  return { lineStart, lineEnd, line: source.slice(lineStart, lineEnd) };
}

function sentenceBounds(text, start, end) {
  const source = String(text || "");
  const before = source.slice(0, start);
  const after = source.slice(end);
  const sentStartRel = Math.max(
    before.lastIndexOf(". "),
    before.lastIndexOf(".\n"),
    before.lastIndexOf("! "),
    before.lastIndexOf("? "),
    before.lastIndexOf("\n\n")
  );
  const sentStart = sentStartRel === -1 ? Math.max(0, start - 160) : sentStartRel + (before[sentStartRel] === "\n" ? 2 : 2);
  let sentEndRel = after.search(/[.!?](?:\s|$)/);
  if (sentEndRel === -1) sentEndRel = Math.min(after.length, 160);
  else sentEndRel += 1;
  const sentEnd = end + sentEndRel;
  const clippedStart = Math.max(0, Math.min(sentStart, start));
  return {
    sentenceStart: clippedStart,
    sentenceEnd: Math.min(source.length, sentEnd),
    sentence: source.slice(clippedStart, Math.min(source.length, sentEnd)),
  };
}

function paragraphBounds(text, start) {
  const source = String(text || "");
  const before = source.slice(0, start);
  const paraBreak = before.lastIndexOf("\n\n");
  const paraStart = paraBreak === -1 ? 0 : paraBreak + 2;
  const afterBreak = source.indexOf("\n\n", start);
  const paraEnd = afterBreak === -1 ? source.length : afterBreak;
  // Cap paragraph window to avoid mega-blocks
  const cappedStart = Math.max(paraStart, start - 280);
  const cappedEnd = Math.min(paraEnd, start + 280);
  return {
    paragraphStart: cappedStart,
    paragraphEnd: cappedEnd,
    paragraph: source.slice(cappedStart, cappedEnd),
  };
}

/**
 * @param {string} text
 * @param {RegExp} re
 * @param {number} from
 * @param {number} to
 */
function findCueInRange(text, re, from, to) {
  const slice = String(text || "").slice(from, to);
  const m = slice.match(re);
  if (!m || m.index == null) return null;
  return {
    cue: m[0],
    cueStart: from + m.index,
    cueEnd: from + m.index + m[0].length,
  };
}

/**
 * Strip Preferred Hotels brand noise unless the mention itself is Preferred.
 * @param {string} text
 * @param {string} rawMention
 */
export function sanitizeCueText(text, rawMention) {
  const raw = String(rawMention || "");
  if (/^preferred\b/i.test(raw)) return String(text || "");
  return stripPreferredHotelsBrandNoise(text);
}

/**
 * Build linked cue record.
 */
function makeCueLink({
  cueType,
  cue,
  cueStart,
  cueEnd,
  entityStart,
  entityEnd,
  scope,
  linkedToEntity,
  propagationAllowed = false,
}) {
  const distance = Math.min(
    Math.abs(cueStart - entityEnd),
    Math.abs(entityStart - cueEnd),
    Math.abs(cueStart - entityStart)
  );
  return {
    CUE_TYPE: cueType,
    CUE: cue,
    CUE_SPAN: { start: cueStart, end: cueEnd },
    ENTITY_SPAN: { start: entityStart, end: entityEnd },
    DISTANCE: distance,
    SCOPE: scope,
    LINKED_TO_ENTITY: linkedToEntity ? "YES" : "NO",
    PROPAGATION_ALLOWED: propagationAllowed,
  };
}

/**
 * Extract structure facts for a mention span.
 */
export function extractStructureEvidence(text, start, end, sections) {
  const source = String(text || "");
  const { line, lineStart } = lineBounds(source, start);
  const sec = sectionAt(source, start, sections);
  const headingType = classifyHeadingSemanticType(sec?.title || "");
  const numberedHeading =
    /^#{1,3}\s*\d+[.)]\s+/.test(line.trimStart()) ||
    detectRankMarker(source, start) != null && isDocumentTopicHeading(line);
  const rankMarker = detectRankMarker(source, start);
  const topicNumber = rankMarker != null && isDocumentTopicHeading(line);
  const tableRank = rankMarker != null && /\|/.test(line) ? rankMarker : null;
  const bullet = detectBulletLine(source, start);
  const orderedList = detectOrderedListContext(source, start);
  const orderedPosition =
    !topicNumber && rankMarker != null && !isDocumentTopicHeading(line) ? rankMarker : null;

  const lookBehindLocal = source.slice(Math.max(0, (sec?.start ?? start) - 80), start);
  const rankContext =
    headingType === "RANKED_RECOMMENDATION_HEADING" ||
    headingType === "GENERAL_RECOMMENDATION_HEADING" ||
    headingType === "CONSIDERATION_SET_HEADING" ||
    RANKED_HEADING_RE.test(sec?.title || "") ||
    ORDERED_REC_CONTEXT_RE.test(sec?.title || "") ||
    ORDERED_REC_CONTEXT_RE.test(lookBehindLocal);

  // Rank column / #N in table is sufficient; ordered markers need recommendation/consideration context
  const confirmedRankStructure = Boolean(
    tableRank != null ||
      (orderedPosition != null &&
        !topicNumber &&
        (rankContext ||
          headingType === "RANKED_RECOMMENDATION_HEADING" ||
          headingType === "GENERAL_RECOMMENDATION_HEADING" ||
          headingType === "CONSIDERATION_SET_HEADING" ||
          ORDERED_REC_CONTEXT_RE.test(lookBehindLocal))) ||
      (orderedList && rankContext && orderedPosition != null && !topicNumber)
  );

  // Ordered list item index under confirmed rank heading
  let orderedListPosition = null;
  if (confirmedRankStructure || (orderedList && headingType === "RANKED_RECOMMENDATION_HEADING")) {
    if (orderedPosition != null) orderedListPosition = orderedPosition;
  }

  return {
    isOrderedList: orderedList,
    orderedPosition: orderedListPosition ?? (confirmedRankStructure ? orderedPosition : null),
    isUnorderedList: bullet && !orderedList,
    tableRank,
    numberedHeading: Boolean(topicNumber || (rankMarker != null && isDocumentTopicHeading(line))),
    confirmedRankStructure,
    headingSemanticType: headingType,
    sectionTitle: sec?.title || null,
    sectionStart: sec?.start ?? null,
    sectionEnd: sec?.end ?? null,
    bullet,
    rawRankMarker: topicNumber ? null : rankMarker,
    lineStart,
  };
}

/**
 * Stage 1: extract entity-local evidence for one mention span.
 * @param {{
 *   text: string,
 *   start: number,
 *   end: number,
 *   rawMention?: string,
 *   canonicalEntityId?: string|null,
 *   canonicalEntityName?: string|null,
 *   sections?: ReturnType<typeof detectResponseSections>,
 * }} args
 */
export function extractEntityLocalEvidence(args) {
  const text = String(args?.text || "");
  const start = Number(args?.start ?? 0);
  const end = Number(args?.end ?? start + String(args?.rawMention || "").length);
  const rawMention = String(args?.rawMention || "");
  const sections = args?.sections || detectResponseSections(text);
  const sec = sectionAt(text, start, sections);

  const { line, lineStart, lineEnd } = lineBounds(text, start);
  const { sentence, sentenceStart, sentenceEnd } = sentenceBounds(text, start, end);
  const { paragraph, paragraphStart, paragraphEnd } = paragraphBounds(text, start);
  const structure = extractStructureEvidence(text, start, end, sections);

  const parentHeading = sec?.title || null;
  const headingType = structure.headingSemanticType;

  /** @type {ReturnType<typeof makeCueLink>[]} */
  const cueLinks = [];

  // --- Direct negative (same sentence / line) ---
  const negScopes = [
    { scope: "local_sentence", from: sentenceStart, to: sentenceEnd },
    { scope: "local_list_item", from: lineStart, to: lineEnd },
  ];
  let directNegativeCue = false;
  for (const s of negScopes) {
    const hit = findCueInRange(sanitizeCueText(text, rawMention), DIRECT_NEGATIVE_RE, s.from, s.to);
    if (hit) {
      directNegativeCue = true;
      cueLinks.push(
        makeCueLink({
          cueType: "direct_negative",
          ...hit,
          entityStart: start,
          entityEnd: end,
          scope: s.scope,
          linkedToEntity: true,
        })
      );
      break;
    }
  }

  // --- Comparator: entity as object of comparison ---
  const before80 = text.slice(Math.max(0, start - 80), start);
  let comparatorCue = COMPARATOR_BEFORE_RE.test(before80);
  if (comparatorCue) {
    cueLinks.push(
      makeCueLink({
        cueType: "comparator",
        cue: before80.slice(-40),
        cueStart: Math.max(0, start - 40),
        cueEnd: start,
        entityStart: start,
        entityEnd: end,
        scope: "local_sentence",
        linkedToEntity: true,
      })
    );
  } else {
    const compHit = findCueInRange(text, COMPARATOR_LOCAL_RE, sentenceStart, sentenceEnd);
    // Only if comparison language surrounds this entity closely (not multi-brand co-mention)
    if (compHit && Math.abs(compHit.cueStart - start) < 60) {
      comparatorCue = true;
      cueLinks.push(
        makeCueLink({
          cueType: "comparator",
          ...compHit,
          entityStart: start,
          entityEnd: end,
          scope: "local_sentence",
          linkedToEntity: true,
        })
      );
    }
  }

  // --- Source only ---
  let sourceOnlyCue = false;
  if (SOURCE_ONLY_RE.test(sentence) || /\bsources?\b/i.test(parentHeading || "")) {
    sourceOnlyCue = true;
    cueLinks.push(
      makeCueLink({
        cueType: "source_only",
        cue: "source",
        cueStart: sentenceStart,
        cueEnd: sentenceStart + 6,
        entityStart: start,
        entityEnd: end,
        scope: "local_sentence",
        linkedToEntity: true,
      })
    );
  }

  // --- Lead cue: same line only (do not inherit "Start with:" heading across list items) ---
  let leadCue = false;
  const leadHit = findCueInRange(
    sanitizeCueText(text, rawMention),
    LEAD_CUE_RE,
    lineStart,
    lineEnd
  );
  // Also: "I would shortlist ENTITY" on same line before entity
  if (leadHit && leadHit.cueEnd <= end + 20) {
    leadCue = true;
    cueLinks.push(
      makeCueLink({
        cueType: "lead",
        ...leadHit,
        entityStart: start,
        entityEnd: end,
        scope: "local_list_item",
        linkedToEntity: true,
      })
    );
  }

  // --- Direct positive: same sentence OR same list/table row; after-mention preferred ---
  let directPositiveCue = false;
  const afterOnLine = text.slice(start, Math.min(text.length, lineEnd));
  const afterSentence = text.slice(start, Math.min(text.length, sentenceEnd));
  const beforeOnLineShort = text.slice(lineStart, start);
  const posCandidates = [
    { scope: "local_list_item_after", text: afterOnLine, from: start, to: lineEnd },
    { scope: "local_sentence_after", text: afterSentence, from: start, to: sentenceEnd },
    {
      scope: "local_list_item_before",
      text: beforeOnLineShort.length <= 100 ? beforeOnLineShort : "",
      from: lineStart,
      to: start,
    },
  ];
  for (const c of posCandidates) {
    if (!c.text) continue;
    const cleaned = sanitizeCueText(c.text, rawMention);
    const m = cleaned.match(DIRECT_POSITIVE_RE);
    if (!m) continue;
    // Re-find in original range approximately
    const hit = findCueInRange(sanitizeCueText(text, rawMention), DIRECT_POSITIVE_RE, c.from, c.to);
    if (!hit) continue;
    directPositiveCue = true;
    cueLinks.push(
      makeCueLink({
        cueType: "direct_positive",
        ...hit,
        entityStart: start,
        entityEnd: end,
        scope: c.scope,
        linkedToEntity: true,
      })
    );
    break;
  }

  // --- Consideration set: local item under consideration heading OR local cue ---
  let considerationSetCue = false;
  const considerRanges = [
    { scope: "local_list_item", from: lineStart, to: lineEnd },
    { scope: "local_sentence", from: sentenceStart, to: sentenceEnd },
    {
      scope: "governed_section_intro",
      from: Math.max(sec?.start ?? lineStart, start - 160),
      to: start,
    },
  ];
  let localConsider = null;
  for (const r of considerRanges) {
    localConsider = findCueInRange(text, CONSIDERATION_SET_RE, r.from, r.to);
    if (localConsider) {
      localConsider._scope = r.scope;
      break;
    }
  }

  const parentIsConsideration = headingType === "CONSIDERATION_SET_HEADING";
  const parentIsRanked = headingType === "RANKED_RECOMMENDATION_HEADING";
  const parentIsGeneralRec = headingType === "GENERAL_RECOMMENDATION_HEADING";
  const parentIsNeutralCatalog = headingType === "NEUTRAL_CATALOG_HEADING";

  // Parent consideration heading may propagate ONLY to list/table items in that section
  if (
    (parentIsConsideration || /examples?\s+include|options?\s+include|brands?\s+to\s+consider|marcas?\s+a\s+considerar/i.test(parentHeading || "")) &&
    (structure.bullet || structure.tableRank != null || structure.isUnorderedList || structure.isOrderedList || structure.rawRankMarker != null)
  ) {
    considerationSetCue = true;
    cueLinks.push(
      makeCueLink({
        cueType: "consideration_set",
        cue: parentHeading || "consideration_heading",
        cueStart: sec?.start ?? start,
        cueEnd: (sec?.start ?? start) + Math.min(40, (parentHeading || "").length),
        entityStart: start,
        entityEnd: end,
        scope: "governed_parent_heading",
        linkedToEntity: true,
        propagationAllowed: true,
      })
    );
  } else if (localConsider) {
    considerationSetCue = true;
    cueLinks.push(
      makeCueLink({
        cueType: "consideration_set",
        cue: localConsider.cue,
        cueStart: localConsider.cueStart,
        cueEnd: localConsider.cueEnd,
        entityStart: start,
        entityEnd: end,
        scope: localConsider._scope || "local_sentence",
        linkedToEntity: true,
        propagationAllowed: localConsider._scope === "governed_section_intro",
      })
    );
  }

  // Ranked parent heading + list item → rank cue (not automatic first without position)
  let rankCue = Boolean(structure.confirmedRankStructure);
  if (
    parentIsRanked &&
    (structure.bullet || structure.isOrderedList || structure.tableRank != null || structure.rawRankMarker != null)
  ) {
    rankCue = true;
    if (!structure.confirmedRankStructure) {
      structure.confirmedRankStructure = true;
      if (structure.rawRankMarker != null) structure.orderedPosition = structure.rawRankMarker;
    }
  }

  // --- Descriptive / incidental ---
  let descriptiveCue = DESCRIPTIVE_RE.test(sentence) || DESCRIPTIVE_RE.test(line);
  let incidentalCue = PASSING_RE.test(sentence) && !directPositiveCue && !considerationSetCue;

  // Neutral catalog parent + bullet without decision cues → descriptive, not consideration
  if (parentIsNeutralCatalog && !directPositiveCue && !leadCue && !rankCue) {
    descriptiveCue = true;
    considerationSetCue = false;
  }

  // General recommendation heading alone does NOT create direct positive
  // (prevents recommendation_section over-promotion)
  if (parentIsGeneralRec && !directPositiveCue && !leadCue && !rankCue) {
    if (structure.bullet || structure.isOrderedList) {
      considerationSetCue = true;
    } else {
      descriptiveCue = true;
    }
  }

  const cueFromParentSection = cueLinks.some((c) => c.SCOPE === "governed_parent_heading");
  const cueLocalToEntity = cueLinks.some((c) =>
    ["local_sentence", "local_sentence_after", "local_list_item", "local_list_item_after", "local_list_item_before"].includes(
      c.SCOPE
    )
  );
  const minDistance = cueLinks.length
    ? Math.min(...cueLinks.filter((c) => c.LINKED_TO_ENTITY === "YES").map((c) => c.DISTANCE))
    : null;

  return {
    evidenceVersion: RECOMMENDATION_EVIDENCE_VERSION,
    entityId: args?.canonicalEntityId || null,
    entityName: args?.canonicalEntityName || null,
    rawMention,
    entityMentionSpan: { start, end },
    localSentence: sentence,
    localParagraph: paragraph,
    localListItem: line,
    localTableRow: /\|/.test(line) ? line : null,
    parentHeading,
    sectionHeading: parentHeading,
    structure,
    recommendationEvidence: {
      directPositiveCue,
      directNegativeCue,
      leadCue,
      rankCue,
      considerationSetCue,
      comparatorCue,
      descriptiveCue,
      incidentalCue,
      sourceOnlyCue,
    },
    evidenceScope: {
      cueLocalToEntity,
      cueFromParentSection,
      cueDistance: minDistance,
      propagationAllowed: cueLinks.some((c) => c.PROPAGATION_ALLOWED),
    },
    cueLinks,
    bounds: {
      sentenceStart,
      sentenceEnd,
      paragraphStart,
      paragraphEnd,
      lineStart,
      lineEnd,
    },
  };
}

/**
 * Aggregate evidence across all mention spans of one entity in a response.
 * Highest-severity linked cues win for flags; earliest confirmed rank position kept.
 */
export function aggregateEntityEvidence(mentionEvidenceList) {
  const list = mentionEvidenceList || [];
  if (!list.length) return null;
  const first = list[0];
  const agg = {
    evidenceVersion: RECOMMENDATION_EVIDENCE_VERSION,
    entityId: first.entityId,
    entityName: first.entityName,
    entityMentionSpans: list.map((e) => e.entityMentionSpan),
    mentionEvidence: list,
    recommendationEvidence: {
      directPositiveCue: list.some((e) => e.recommendationEvidence.directPositiveCue),
      directNegativeCue: list.some((e) => e.recommendationEvidence.directNegativeCue),
      leadCue: list.some((e) => e.recommendationEvidence.leadCue),
      rankCue: list.some((e) => e.recommendationEvidence.rankCue),
      considerationSetCue: list.some((e) => e.recommendationEvidence.considerationSetCue),
      comparatorCue:
        list.some((e) => e.recommendationEvidence.comparatorCue) &&
        !list.some((e) => e.recommendationEvidence.directPositiveCue),
      descriptiveCue: list.some((e) => e.recommendationEvidence.descriptiveCue),
      incidentalCue: list.every((e) => e.recommendationEvidence.incidentalCue),
      sourceOnlyCue:
        list.every((e) => e.recommendationEvidence.sourceOnlyCue) ||
        (list.some((e) => e.recommendationEvidence.sourceOnlyCue) &&
          !list.some(
            (e) =>
              e.recommendationEvidence.directPositiveCue ||
              e.recommendationEvidence.leadCue ||
              e.recommendationEvidence.rankCue
          )),
    },
    structure: {
      confirmedRankStructure: list.some((e) => e.structure.confirmedRankStructure),
      orderedPosition: null,
      tableRank: null,
      isOrderedList: list.some((e) => e.structure.isOrderedList),
      isUnorderedList: list.some((e) => e.structure.isUnorderedList),
      numberedHeading: list.every((e) => e.structure.numberedHeading),
      headingSemanticType: first.structure.headingSemanticType,
    },
    cueLinks: list.flatMap((e) => e.cueLinks || []),
  };

  const ranked = list
    .filter((e) => e.structure.confirmedRankStructure)
    .map((e) => e.structure.orderedPosition ?? e.structure.tableRank)
    .filter((p) => p != null);
  if (ranked.length) agg.structure.orderedPosition = Math.min(...ranked);

  const tables = list.map((e) => e.structure.tableRank).filter((p) => p != null);
  if (tables.length) agg.structure.tableRank = Math.min(...tables);

  return agg;
}
