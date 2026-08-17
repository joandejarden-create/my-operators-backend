/**
 * AI Visibility recommendation evidence extraction v4.1 (deterministic).
 *
 * Hardening 6: keep entity-local safety from v4; add bounded section propagation
 * for consideration / ranked / recommendation-set semantics.
 *
 * Does NOT allow document-wide positive inheritance or unbounded preceding text.
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
  "ai_visibility_recommendation_evidence_v4_1";

/** @typedef {
 *  'LEAD_RECOMMENDATION_SECTION'|
 *  'RANKED_RECOMMENDATION_SECTION'|
 *  'RECOMMENDATION_SET_SECTION'|
 *  'CONSIDERATION_SET_SECTION'|
 *  'COMPARISON_SECTION'|
 *  'NEGATIVE_SECTION'|
 *  'NEUTRAL_CATALOG_SECTION'|
 *  'DESCRIPTIVE_SECTION'|
 *  'UNKNOWN_SECTION'
 * } SectionType */

const DIRECT_POSITIVE_RE =
  /\b(recommend(?:ed|ing)?|we\s+recommend|i\s+(?:would\s+)?recommend|recomiendo|recomendad[oa]s?|strong\s+(?:fit|candidate|alternative|option|options|choice|choices)|best\s+fit|best\s+suited|best\s+value(?:\s+engineering)?(?:\s+option)?|good\s+(?:option|fit)|well\s+suited|preferred\s+(?:option|choice|fit)|opci[oó]n\s+(?:fuerte|s[oó]lida|preferida)|opciones\s+(?:fuertes|s[oó]lidas)|mejor\s+(?:opci[oó]n|encaje)|buen\s+encaje|particularly\s+suitable|likely\s+fit|primary\s+option|leading\s+candidate|issue\s+an\s+rfp\s+to|solicit\s+proposals?\s+from|i\s+would\s+(?:consider|shortlist|solicit)|should\s+consider|may\s+also\s+consider|also\s+consider|top\s+pick)\b/i;

const DIRECT_NEGATIVE_RE =
  /\b(not\s+recommend(?:ed)?|less\s+suitable|weaker\s+fit|may\s+be\s+difficult|not\s+ideal|unlikely|avoid|less\s+attractive|poor\s+fit|weak\s+fit|would\s+not\s+consider|do\s+not\s+consider|only\s+if|may\s+work\s+but|would\s+require|better\s+suited\s+to|no\s+recomend(?:ad[oa]|ar)|poco\s+adecuado|evitar|no\s+ideal|menos\s+adecuado)\b/i;

const LEAD_CUE_RE =
  /\b(first\s+call|first\s+choice|first\s+option|top\s+choice|primary\s+recommendation|primary\s+option|leading\s+candidate|shortlist\s+first|consider\s+first|begin\s+with|start\s+with|my\s+first|primera\s+opci[oó]n|primera\s+alternativa|opci[oó]n\s+principal|prioridad\s+(?:1|uno)|1st\s+(?:choice|recommendation)|first\s+recommendation|i\s+would\s+shortlist|recomendaci[oó]n\s+principal)\b/i;

/** Decision-set membership / consideration — NOT rank and NOT automatic explicit. */
const CONSIDERATION_SET_RE =
  /\b(options?\s+include|examples?\s+include|brands?\s+to\s+consider|operators?\s+to\s+consider|key\s+(?:upper[\s-]?upscale\s+)?brands?\s+to\s+consider|brands?\s+typically\s+considered|brands?\s+commonly\s+(?:considered|cited|associated)|commonly\s+(?:associated|considered|cited)(?:\s+with)?|frequently\s+(?:associated|considered)(?:\s+with)?|typically\s+(?:associated|considered)(?:\s+with)?|often\s+(?:associated|considered)(?:\s+with)?|consideration\s+set|alternatives?\s+include|opciones\s+incluyen|entre\s+las\s+opciones|marcas?\s+a\s+considerar|colecciones?\s+a\s+considerar|conviene\s+considerar|m[aá]s\s+conviene\s+considerar|se\s+consideran|suelen\s+considerar|commonly\s+cited\s+choices?|candidate\s+set|most\s+commonly\s+considered|options?\s+commonly\s+considered|principales\s+opciones|principales\s+marcas(?:\s+\w+){0,8}\s+considerad[oa]s?|alternativas\s+incluyen|las\s+m[aá]s\s+citadas|names?\s+that\s+appear\s+most\s+often|brands?\s+most\s+commonly\s+considered|marcas(?:\s+\w+){0,6}\s+considerad[oa]s?\s+para\s+(?:proyectos\s+con\s+)?branded\s+residences)\b/i;

/**
 * Confirmed ranking semantics only (not bare "brands to consider", not section numbers).
 * "shortlist" / "lista corta" count when they label an ordered recommendation set.
 */
const RANK_SEMANTICS_RE =
  /\b(recommended\s+shortlist|soft[\s-]?brand\s+shortlist|ranked\s+(?:list|options?|brands?)|top\s+\d+|top\s+options?|priority\s+(?:list|order)|recommended\s+in\s+order|orden\s+de\s+prioridad|primera\s+opci[oó]n|segunda\s+opci[oó]n|tercera\s+opci[oó]n|i\s+would\s+solicit\s+proposals?|solicit\s+proposals?\s+from|primary\s+candidates?|leading\s+options?|marcas?\s+recomendadas?|lista\s+corta\s+recomendad|commonly\s+shortlisted|recomendaci[oó]n\s+principal|primeras?\s+que\s+invitar|priority\s+\d+|shortlist:)\b/i;

const RECOMMENDATION_SET_RE =
  /\b(recommended\s+brands?|recommended\s+operators?|recommended\s+options?|opciones\s+recomendadas?|marcas?\s+recomendadas?|strong\s+candidates?\s+include|the\s+following\s+brands?\s+are\s+strong|best\s+soft[\s-]?brand\s+options?)\b/i;

const NEUTRAL_CATALOG_RE =
  /\b(brand\s+profiles?|market\s+participants?|brands?\s+in\s+the\s+region|background|history|soft\s+brand(?:s|\s+collections?)|major\s+(?:soft\s+)?brand|brand\s+overview|market\s+overview|consorcios|afiliaci[oó]n|portfolio\s+overview|pipeline|relevant\s+(?:hotel\s+)?brands?|understanding\s+the|market\s+context|tendencia\s+general|overview)\b/i;

const DESCRIPTIVE_RE =
  /\b(operates?|launched|has\s+a\s+presence|offers?|is\s+part\s+of|footprint|presence|portfolio|collection\s+of|designed\s+for|targets?|positioned|alliance|alianza|presencia|lanzad|forman?\s+parte)\b/i;

const COMPARATOR_BEFORE_RE =
  /(?:alternative\s+to|compared\s+(?:to|with)|versus|vs\.?|unlike|similar\s+to|competitor\s+to|frente\s+a|comparad[oa]\s+con|a\s+diferencia\s+de)\s+(?:\*\*)?$/i;

const SOURCE_ONLY_RE =
  /\b(according\s+to|cited\s+(?:in|by)|source:|see\s+also)\b/i;

const PASSING_RE =
  /\b(for\s+example|such\s+as|including|among\s+others|e\.g\.|i\.e\.)\b/i;

const ORDINAL_LEAD_RE =
  /\b(first|1st|segunda|segunda|segunda|segunda)\b/i; // unused placeholder avoided

/**
 * @param {string} title
 * @param {string} intro
 * @returns {SectionType}
 */
export function classifySectionType(title, intro = "") {
  const rawTitle = String(title || "").replace(/^#{1,6}\s*/, "").replace(/^[^\wÁÉÍÓÚáéíóúñÑ]+/, "");
  const t = `${rawTitle}\n${intro}`;
  if (/\b(comparison|versus|vs\.?|alternatives?\s+compared)\b/i.test(t)) return "COMPARISON_SECTION";
  if (DIRECT_NEGATIVE_RE.test(rawTitle) || /\b(not\s+recommended|exclude|avoid)\b/i.test(rawTitle)) {
    return "NEGATIVE_SECTION";
  }
  // Rank before consideration — "shortlist" wins over nearby "consider" prose in intro
  if (RANK_SEMANTICS_RE.test(rawTitle) || RANK_SEMANTICS_RE.test(t)) return "RANKED_RECOMMENDATION_SECTION";
  if (LEAD_CUE_RE.test(rawTitle) || /\b(start\s+with|begin\s+with)\b/i.test(rawTitle)) {
    return "LEAD_RECOMMENDATION_SECTION";
  }
  if (RECOMMENDATION_SET_RE.test(t)) return "RECOMMENDATION_SET_SECTION";
  if (CONSIDERATION_SET_RE.test(rawTitle) || CONSIDERATION_SET_RE.test(t)) {
    return "CONSIDERATION_SET_SECTION";
  }
  // Titled brand lists for branded-residences product framing (decision-set membership)
  if (
    /\b(?:marcas|brands?|options?)\b/i.test(rawTitle) &&
    /\bbranded\s+residences\b|\bresidencias\s+(?:branded|con\s+marca)\b/i.test(rawTitle)
  ) {
    return "CONSIDERATION_SET_SECTION";
  }
  if (NEUTRAL_CATALOG_RE.test(rawTitle)) return "NEUTRAL_CATALOG_SECTION";
  if (/\b(overview|introducci[oó]n|contexto|methodology|conclusi[oó]n|background)\b/i.test(t)) {
    return "DESCRIPTIVE_SECTION";
  }
  return "UNKNOWN_SECTION";
}

/** Decision-set vs neutral catalog */
export function classifyCatalogSemantics(sectionType) {
  if (
    sectionType === "CONSIDERATION_SET_SECTION" ||
    sectionType === "RECOMMENDATION_SET_SECTION" ||
    sectionType === "RANKED_RECOMMENDATION_SECTION" ||
    sectionType === "LEAD_RECOMMENDATION_SECTION"
  ) {
    return "DECISION_SET";
  }
  if (sectionType === "NEUTRAL_CATALOG_SECTION" || sectionType === "DESCRIPTIVE_SECTION") {
    return "NEUTRAL_CATALOG";
  }
  return "UNKNOWN";
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
  const sentStart =
    sentStartRel === -1 ? Math.max(0, start - 160) : sentStartRel + 2;
  let sentEndRel = after.search(/[.!?](?:\s|$)/);
  if (sentEndRel === -1) sentEndRel = Math.min(after.length, 160);
  else sentEndRel += 1;
  return {
    sentenceStart: Math.max(0, Math.min(sentStart, start)),
    sentenceEnd: Math.min(source.length, end + sentEndRel),
    sentence: source.slice(Math.max(0, Math.min(sentStart, start)), Math.min(source.length, end + sentEndRel)),
  };
}

export function sanitizeCueText(text, rawMention) {
  const raw = String(rawMention || "");
  if (/^preferred\b/i.test(raw)) return String(text || "");
  return stripPreferredHotelsBrandNoise(text);
}

function findCueInRange(text, re, from, to) {
  const slice = String(text || "").slice(from, to);
  const m = slice.match(re);
  if (!m || m.index == null) return null;
  return { cue: m[0], cueStart: from + m.index, cueEnd: from + m.index + m[0].length };
}

/**
 * Enrich raw detectResponseSections with typed section metadata + intro.
 * Also promotes bare decision-intro lines (e.g. "Soft-brand shortlist:") when
 * detectResponseSections already captured them as Title: headings.
 */
export function buildTypedSections(text) {
  const source = String(text || "");
  const raw = detectResponseSections(source);
  const typed = raw.map((sec, i) => {
    const bodyStart = sec.start + String(sec.title || "").length;
    const intro = source.slice(bodyStart, Math.min(sec.end, bodyStart + 220));
    // Untitled blocks: classify from first line only (avoid mid-prose cue promoting whole doc)
    const titleForType = sec.title || source.slice(sec.start, Math.min(sec.end, source.indexOf("\n", sec.start) === -1 ? sec.end : source.indexOf("\n", sec.start)));
    const introForType = sec.title ? intro : "";
    const sectionType = classifySectionType(titleForType || "", introForType);
    const catalogSemantics = classifyCatalogSemantics(sectionType);
    const confirmedRankStructure =
      sectionType === "RANKED_RECOMMENDATION_SECTION" ||
      (sectionType === "LEAD_RECOMMENDATION_SECTION" && RANK_SEMANTICS_RE.test(`${sec.title}\n${intro}`));
    const confirmedDecisionSet = catalogSemantics === "DECISION_SET";
    const sectionCueType =
      sectionType === "CONSIDERATION_SET_SECTION"
        ? "consideration"
        : sectionType === "RECOMMENDATION_SET_SECTION"
          ? "recommendation_set"
          : sectionType === "RANKED_RECOMMENDATION_SECTION" || sectionType === "LEAD_RECOMMENDATION_SECTION"
            ? "ranked_or_lead"
            : sectionType === "NEUTRAL_CATALOG_SECTION"
              ? "neutral_catalog"
              : "none";
    return {
      sectionId: `sec_${i}`,
      start: sec.start,
      end: sec.end,
      title: sec.title || "",
      sectionIntro: intro,
      sectionType,
      catalogSemantics,
      confirmedRankStructure,
      confirmedDecisionSet,
      sectionCueType,
      sectionRole: sec.sectionRole,
    };
  });

  // Prefatory untitled block: only retarget when opening looks like a decision heading/intro line
  if (typed.length && !typed[0].title) {
    const openLine = source.split(/\n/)[0] || "";
    const open = source.slice(0, Math.min(source.length, 180));
    const openType = classifySectionType(openLine, open);
    if (
      openType === "CONSIDERATION_SET_SECTION" ||
      openType === "RECOMMENDATION_SET_SECTION" ||
      openType === "RANKED_RECOMMENDATION_SECTION" ||
      openType === "LEAD_RECOMMENDATION_SECTION"
    ) {
      // Require intro-line shape (short line or ends with :) to avoid mid-prose bleed
      const looksLikeIntro =
        /:\s*$/.test(openLine.trim()) ||
        openLine.trim().length <= 80 ||
        /^#{1,3}\s/.test(openLine);
      if (looksLikeIntro) {
        typed[0] = {
          ...typed[0],
          sectionType: openType,
          catalogSemantics: classifyCatalogSemantics(openType),
          confirmedRankStructure: openType === "RANKED_RECOMMENDATION_SECTION",
          confirmedDecisionSet: classifyCatalogSemantics(openType) === "DECISION_SET",
          sectionCueType:
            openType === "CONSIDERATION_SET_SECTION"
              ? "consideration"
              : openType === "RECOMMENDATION_SET_SECTION"
                ? "recommendation_set"
                : openType === "RANKED_RECOMMENDATION_SECTION"
                  ? "ranked_or_lead"
                  : typed[0].sectionCueType,
          sectionIntro: open,
        };
      }
    }
  }

  // Inherit consideration/rec-set into UNKNOWN subsections until a hard reset heading.
  // Only from titled decision sections (not untitled prefatory prose).
  let activeDecisionType = null;
  for (const sec of typed) {
    if (
      (sec.sectionType === "CONSIDERATION_SET_SECTION" ||
        sec.sectionType === "RECOMMENDATION_SET_SECTION") &&
      sec.title
    ) {
      activeDecisionType = sec.sectionType;
      continue;
    }
    if (
      sec.sectionType === "NEUTRAL_CATALOG_SECTION" ||
      sec.sectionType === "DESCRIPTIVE_SECTION" ||
      sec.sectionType === "COMPARISON_SECTION" ||
      sec.sectionType === "NEGATIVE_SECTION" ||
      sec.sectionType === "RANKED_RECOMMENDATION_SECTION" ||
      sec.sectionType === "LEAD_RECOMMENDATION_SECTION"
    ) {
      activeDecisionType = null;
      continue;
    }
    if (activeDecisionType && sec.sectionType === "UNKNOWN_SECTION") {
      // Numbered parent-company / profile headings are catalogs, not decision-set children
      const titled = String(sec.title || "");
      if (/^#{1,3}\s*\d+[.)]/.test(titled) || /\b(international|marriott|hilton|ihg|hyatt|accor|grupo)\b/i.test(titled)) {
        // keep UNKNOWN — no inheritance
      } else {
        sec.sectionType = activeDecisionType;
        sec.catalogSemantics = classifyCatalogSemantics(activeDecisionType);
        sec.confirmedDecisionSet = true;
        sec.confirmedRankStructure = false;
        sec.sectionCueType =
          activeDecisionType === "CONSIDERATION_SET_SECTION" ? "consideration" : "recommendation_set";
      }
    }
  }
  return typed;
}

/**
 * Structural position among list items inside a confirmed ranked section.
 * Does not treat parent-company topic numbers as rank without list-item attachment.
 */
export function detectConfirmedListPosition(text, start, section) {
  if (!section?.confirmedRankStructure && section?.sectionType !== "RANKED_RECOMMENDATION_SECTION") {
    return null;
  }
  const source = String(text || "");
  const body = source.slice(section.start, section.end);
  const itemRe = /(?:^|\n)\s*(?:[-*]|\u2022|\d+[.)]|#{1,3}\s*\d+[.)])\s+/g;
  const items = [];
  let m;
  while ((m = itemRe.exec(body)) !== null) {
    items.push(section.start + m.index + (m[0].startsWith("\n") ? 1 : 0));
  }
  if (items.length < 2) return null;
  for (let i = 0; i < items.length; i++) {
    const next = i + 1 < items.length ? items[i + 1] : section.end;
    if (start >= items[i] && start < next) {
      // Prefer explicit numeric marker on the item line when present
      const lineStart = source.lastIndexOf("\n", start - 1) + 1;
      const prefix = source.slice(lineStart, start);
      const num = prefix.match(/(?:^|[\s|])(?:#)?(\d+)[.)]\s*\*?\*?$/);
      if (num) return parseInt(num[1], 10);
      return i + 1;
    }
  }
  return null;
}

export function typedSectionAt(sections, start) {
  for (const s of sections) {
    if (start >= s.start && start < s.end) return s;
  }
  return null;
}

/**
 * Propagation gate: entity inside section, no heading reset, compatible semantics.
 */
export function evaluateSectionPropagation({
  section,
  entityStart,
  isListOrTableChild,
  requestedCue,
}) {
  if (!section) {
    return {
      PROPAGATION_ALLOWED: false,
      REASON: "no_section",
      SECTION_ID: null,
      SECTION_TYPE: null,
    };
  }
  if (entityStart < section.start || entityStart >= section.end) {
    return {
      PROPAGATION_ALLOWED: false,
      REASON: "entity_outside_section",
      SECTION_ID: section.sectionId,
      SECTION_TYPE: section.sectionType,
    };
  }
  const distance = entityStart - section.start;
  if (distance > 2500) {
    return {
      PROPAGATION_ALLOWED: false,
      REASON: "propagation_distance_exceeded",
      SECTION_ID: section.sectionId,
      SECTION_TYPE: section.sectionType,
    };
  }
  if (section.sectionType === "NEUTRAL_CATALOG_SECTION" || section.sectionType === "DESCRIPTIVE_SECTION") {
    if (requestedCue === "consideration" || requestedCue === "recommendation_set" || requestedCue === "rank") {
      return {
        PROPAGATION_ALLOWED: false,
        REASON: "neutral_catalog_blocks_decision_propagation",
        SECTION_ID: section.sectionId,
        SECTION_TYPE: section.sectionType,
      };
    }
  }
  if (!isListOrTableChild && requestedCue !== "lead_local") {
    // prose children may inherit consideration/rec-set only from very near intro (≤180 chars into section body)
    const bodyOffset = entityStart - section.start - String(section.title || "").length;
    if (bodyOffset > 180 && (requestedCue === "consideration" || requestedCue === "recommendation_set")) {
      return {
        PROPAGATION_ALLOWED: false,
        REASON: "prose_too_far_from_section_intro",
        SECTION_ID: section.sectionId,
        SECTION_TYPE: section.sectionType,
      };
    }
  }
  return {
    PROPAGATION_ALLOWED: true,
    REASON: "bounded_section_child",
    SECTION_ID: section.sectionId,
    SECTION_TYPE: section.sectionType,
    PROPAGATION_DISTANCE: distance,
  };
}

/**
 * Stage 1 evidence for one mention span (v4.1).
 */
export function extractEntityLocalEvidence(args) {
  const text = String(args?.text || "");
  const start = Number(args?.start ?? 0);
  const end = Number(args?.end ?? start + String(args?.rawMention || "").length);
  const rawMention = String(args?.rawMention || "");
  const typedSections = args?.typedSections || buildTypedSections(text);
  const section = typedSectionAt(typedSections, start);

  const { line, lineStart, lineEnd } = lineBounds(text, start);
  const { sentence, sentenceStart, sentenceEnd } = sentenceBounds(text, start, end);
  const bullet = detectBulletLine(text, start);
  const orderedList = detectOrderedListContext(text, start);
  const rankMarker = detectRankMarker(text, start);
  const topicNumber = rankMarker != null && isDocumentTopicHeading(line);
  const tableRank = rankMarker != null && /\|/.test(line) ? rankMarker : null;
  const rawRank = topicNumber ? null : rankMarker;
  const isListOrTableChild = Boolean(bullet || tableRank != null || (rawRank != null && !topicNumber) || /^[-*•]\s/.test(line.trim()) || /^\d+[.)]\s/.test(line.trim()));

  // --- Local cues (entity-bounded) ---
  let directNegativeCue = false;
  for (const [from, to] of [
    [sentenceStart, sentenceEnd],
    [lineStart, lineEnd],
  ]) {
    if (findCueInRange(sanitizeCueText(text, rawMention), DIRECT_NEGATIVE_RE, from, to)) {
      directNegativeCue = true;
      break;
    }
  }

  const before80 = text.slice(Math.max(0, start - 80), start);
  let comparatorCue = COMPARATOR_BEFORE_RE.test(before80);

  let sourceOnlyCue = Boolean(
    SOURCE_ONLY_RE.test(sentence) || /\bsources?\b/i.test(section?.title || "")
  );

  let leadCue = false;
  const leadHit =
    findCueInRange(sanitizeCueText(text, rawMention), LEAD_CUE_RE, lineStart, lineEnd) ||
    findCueInRange(sanitizeCueText(text, rawMention), LEAD_CUE_RE, Math.max(sentenceStart, start - 100), lineEnd);
  if (leadHit && leadHit.cueEnd <= end + 40) leadCue = true;
  // Immediate "Issue an RFP to ENTITY" / "solicit proposals from ENTITY" as lead for that entity only
  if (!leadCue) {
    const beforeLocal = text.slice(Math.max(0, start - 48), start);
    if (
      /(?:issue\s+an\s+rfp\s+to|solicit\s+proposals?\s+from)\s+(?:\*\*)?$/i.test(beforeLocal)
    ) {
      leadCue = true;
    }
  }

  let directPositiveCue = false;
  // Same-line only — do not let heading words like "Recommended brands:" bleed via sentence spans
  const afterLine = text.slice(start, Math.min(text.length, lineEnd));
  const beforeShort = text.slice(lineStart, start);
  for (const chunk of [afterLine, beforeShort.length <= 120 ? beforeShort : ""]) {
    if (!chunk) continue;
    if (DIRECT_POSITIVE_RE.test(sanitizeCueText(chunk, rawMention))) {
      if (comparatorCue) break;
      directPositiveCue = true;
      break;
    }
  }
  // Same-line RFP / solicit co-mention: "Issue an RFP to A and B" → B gets positive (not lead)
  if (!directPositiveCue && !comparatorCue) {
    if (
      /(?:issue\s+an\s+rfp\s+to|solicit\s+proposals?\s+from)\b/i.test(beforeShort) &&
      /\b(?:and|y|,)\s*(?:\*\*)?$/i.test(beforeShort.slice(-12))
    ) {
      directPositiveCue = true;
    }
  }

  // Local consideration on same line, or bounded lookbehind (≤140) / same sentence when cue close
  let considerationSetCue = false;
  const lookBehind140 = text.slice(Math.max(0, start - 140), start);
  const lineCons = findCueInRange(text, CONSIDERATION_SET_RE, lineStart, lineEnd);
  const sentCons = findCueInRange(text, CONSIDERATION_SET_RE, sentenceStart, sentenceEnd);
  const behindCons = CONSIDERATION_SET_RE.test(lookBehind140)
    ? findCueInRange(text, CONSIDERATION_SET_RE, Math.max(0, start - 140), start)
    : null;
  const localCons =
    lineCons ||
    behindCons ||
    (sentCons && start - sentCons.cueEnd <= 140 && start - sentCons.cueStart <= 220 ? sentCons : null);
  if (localCons) {
    // Entity after semicolon in a different clause without its own cue → do not link
    const fromCue = text.slice(localCons.cueStart, start);
    if (/;/.test(fromCue) && !CONSIDERATION_SET_RE.test(fromCue.slice(fromCue.lastIndexOf(";")))) {
      // skip
    } else {
      considerationSetCue = true;
    }
  }
  // Cue after entity on same line: "Kimpton is often associated with ..."
  if (!considerationSetCue && CONSIDERATION_SET_RE.test(afterLine)) {
    considerationSetCue = true;
  }

  // --- Section propagation (bounded) ---
  let sectionPropagationAllowed = false;
  let propagationSource = null;
  let propagationDistance = null;
  let sectionPositiveCue = false;
  let rankCue = false;
  let confirmedRankStructure = false;
  let rankPosition = null;

  if (section) {
    const wantConsideration = section.sectionType === "CONSIDERATION_SET_SECTION";
    const wantRecSet = section.sectionType === "RECOMMENDATION_SET_SECTION";
    const wantRank =
      section.sectionType === "RANKED_RECOMMENDATION_SECTION" ||
      section.sectionType === "LEAD_RECOMMENDATION_SECTION";

    if (wantConsideration) {
      const gate = evaluateSectionPropagation({
        section,
        entityStart: start,
        isListOrTableChild,
        requestedCue: "consideration",
      });
      if (gate.PROPAGATION_ALLOWED && isListOrTableChild) {
        considerationSetCue = true;
        sectionPropagationAllowed = true;
        propagationSource = "section_consideration";
        propagationDistance = gate.PROPAGATION_DISTANCE ?? start - section.start;
      }
    }

    if (wantRecSet) {
      const gate = evaluateSectionPropagation({
        section,
        entityStart: start,
        isListOrTableChild,
        requestedCue: "recommendation_set",
      });
      // "Recommended brands:" → explicit for list children (not first/ranked)
      if (gate.PROPAGATION_ALLOWED && isListOrTableChild && !directNegativeCue) {
        sectionPositiveCue = true;
        sectionPropagationAllowed = true;
        propagationSource = "section_recommendation_set";
        propagationDistance = gate.PROPAGATION_DISTANCE ?? start - section.start;
      }
    }

    if (wantRank) {
      const gate = evaluateSectionPropagation({
        section,
        entityStart: start,
        isListOrTableChild: true,
        requestedCue: "rank",
      });
      const listPos = detectConfirmedListPosition(text, start, section);
      const position =
        tableRank != null ? tableRank : rawRank != null ? rawRank : listPos;
      if (gate.PROPAGATION_ALLOWED && position != null && !topicNumber && isListOrTableChild) {
        confirmedRankStructure = true;
        rankCue = true;
        rankPosition = position;
        sectionPropagationAllowed = true;
        propagationSource = "section_ranked";
        propagationDistance = gate.PROPAGATION_DISTANCE ?? start - section.start;
      }
    }

    // Table rank column alone (structural) under any decision-set / ranked heading
    if (tableRank != null && section.catalogSemantics === "DECISION_SET") {
      confirmedRankStructure = true;
      rankCue = true;
      rankPosition = tableRank;
    }
  }

  // Table rank with rank semantics nearby in section intro/title only
  if (tableRank != null && !confirmedRankStructure) {
    if (section && RANK_SEMANTICS_RE.test(`${section.title}\n${section.sectionIntro}`)) {
      confirmedRankStructure = true;
      rankCue = true;
      rankPosition = tableRank;
    } else if (tableRank != null && /\|/.test(line)) {
      // Bare rank column is meaningful ordering when multiple numbered rows exist nearby
      const around = text.slice(Math.max(0, start - 400), Math.min(text.length, start + 400));
      const rankRows = around.match(/\|\s*\*?\*?\d+\*?\*?\s*\|/g);
      if (rankRows && rankRows.length >= 2) {
        confirmedRankStructure = true;
        rankCue = true;
        rankPosition = tableRank;
      }
    }
  }

  // Local numbered list under a nearby ranked intro line (same untitled block / ≤200 behind)
  if (!confirmedRankStructure && rawRank != null && !topicNumber && isListOrTableChild) {
    const behind200 = text.slice(Math.max(0, start - 200), start);
    if (RANK_SEMANTICS_RE.test(behind200)) {
      confirmedRankStructure = true;
      rankCue = true;
      rankPosition = rawRank;
      propagationSource = propagationSource || "local_ranked_intro";
      sectionPropagationAllowed = true;
      propagationDistance = 200;
    }
  }

  if (
    section?.sectionType === "CONSIDERATION_SET_SECTION" &&
    isListOrTableChild &&
    !RANK_SEMANTICS_RE.test(`${section.title}\n${section.sectionIntro}`)
  ) {
    considerationSetCue = true;
    if (!section.confirmedRankStructure) {
      confirmedRankStructure = false;
      rankCue = false;
      rankPosition = null;
    }
  }
  let descriptiveCue = DESCRIPTIVE_RE.test(sentence) || DESCRIPTIVE_RE.test(line);
  let incidentalCue = PASSING_RE.test(sentence) && !directPositiveCue && !considerationSetCue && !leadCue;

  // Same-sentence decision-set membership: cue earlier in sentence, entity in catalog clause
  if (!considerationSetCue && !directNegativeCue) {
    const sentCons2 = findCueInRange(text, CONSIDERATION_SET_RE, sentenceStart, start);
    if (sentCons2 && start - sentCons2.cueEnd <= 280) {
      const between = text.slice(sentCons2.cueEnd, start);
      // Catalog shape: commas / "and" / bullets — not a new independent sentence clause
      const catalogShape = /,/.test(between) || /\b(?:and|y|e)\b/i.test(between) || /\n\s*[-*•]/.test(between);
      if (
        catalogShape &&
        !/\n#{1,3}\s|\n\n/.test(between) &&
        !/;\s+[A-ZÁÉÍÓÚ]/.test(between)
      ) {
        considerationSetCue = true;
      }
    }
  }

  if (section?.sectionType === "NEUTRAL_CATALOG_SECTION") {
    descriptiveCue = true;
    // strip decision cues unless local direct positive/negative/lead
    if (!directPositiveCue && !leadCue && !directNegativeCue) {
      considerationSetCue = false;
      sectionPositiveCue = false;
      if (!confirmedRankStructure) {
        rankCue = false;
      }
    }
  }

  // Comparator precedence: entity as comparison object
  if (comparatorCue) {
    directPositiveCue = false;
    sectionPositiveCue = false;
    leadCue = false;
  }

  const cueLinks = [];
  return {
    evidenceVersion: RECOMMENDATION_EVIDENCE_VERSION,
    entityId: args?.canonicalEntityId || null,
    entityName: args?.canonicalEntityName || null,
    rawMention,
    entityMentionSpan: { start, end },
    localSentence: sentence,
    localListItem: line,
    localTableRow: /\|/.test(line) ? line : null,
    parentHeading: section?.title || null,
    sectionHeading: section?.title || null,
    sectionId: section?.sectionId || null,
    sectionType: section?.sectionType || "UNKNOWN_SECTION",
    sectionIntro: section?.sectionIntro || null,
    sectionCueType: section?.sectionCueType || "none",
    sectionPropagationAllowed,
    confirmedDecisionSet: Boolean(section?.confirmedDecisionSet || considerationSetCue || sectionPositiveCue),
    confirmedRankStructure,
    rankPosition,
    leadPosition: leadCue ? 1 : null,
    propagationSource,
    propagationDistance,
    structure: {
      isOrderedList: orderedList,
      orderedPosition: confirmedRankStructure ? rankPosition : null,
      isUnorderedList: bullet && !orderedList,
      tableRank,
      numberedHeading: Boolean(topicNumber),
      confirmedRankStructure,
      headingSemanticType: section?.sectionType || "UNKNOWN_SECTION",
      bullet,
      rawRankMarker: rawRank,
    },
    recommendationEvidence: {
      directPositiveCue: directPositiveCue || sectionPositiveCue,
      sectionPositiveCue,
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
      cueLocalToEntity: Boolean(directPositiveCue || leadCue || directNegativeCue || comparatorCue),
      cueFromParentSection: sectionPropagationAllowed,
      cueDistance: propagationDistance,
      propagationAllowed: sectionPropagationAllowed,
    },
    cueLinks,
    bounds: { sentenceStart, sentenceEnd, lineStart, lineEnd },
  };
}

export function aggregateEntityEvidence(mentionEvidenceList) {
  const list = mentionEvidenceList || [];
  if (!list.length) return null;
  const first = list[0];
  const ranks = list
    .filter((e) => e.confirmedRankStructure && e.rankPosition != null)
    .map((e) => e.rankPosition);
  return {
    evidenceVersion: RECOMMENDATION_EVIDENCE_VERSION,
    entityId: first.entityId,
    entityName: first.entityName,
    entityMentionSpans: list.map((e) => e.entityMentionSpan),
    mentionEvidence: list,
    recommendationEvidence: {
      directPositiveCue: list.some((e) => e.recommendationEvidence.directPositiveCue),
      sectionPositiveCue: list.some((e) => e.recommendationEvidence.sectionPositiveCue),
      directNegativeCue: list.some((e) => e.recommendationEvidence.directNegativeCue),
      leadCue: list.some((e) => e.recommendationEvidence.leadCue),
      rankCue: list.some((e) => e.recommendationEvidence.rankCue),
      considerationSetCue: list.some((e) => e.recommendationEvidence.considerationSetCue),
      comparatorCue:
        list.some((e) => e.recommendationEvidence.comparatorCue) &&
        !list.some((e) => e.recommendationEvidence.directPositiveCue && !e.recommendationEvidence.comparatorCue),
      descriptiveCue: list.some((e) => e.recommendationEvidence.descriptiveCue),
      incidentalCue: list.every((e) => e.recommendationEvidence.incidentalCue),
      sourceOnlyCue:
        list.some((e) => e.recommendationEvidence.sourceOnlyCue) &&
        !list.some(
          (e) =>
            e.recommendationEvidence.directPositiveCue ||
            e.recommendationEvidence.leadCue ||
            e.recommendationEvidence.rankCue ||
            e.recommendationEvidence.considerationSetCue
        ),
    },
    structure: {
      confirmedRankStructure: list.some((e) => e.confirmedRankStructure),
      orderedPosition: ranks.length ? Math.min(...ranks) : null,
      tableRank: ranks.length ? Math.min(...ranks) : null,
      isOrderedList: list.some((e) => e.structure.isOrderedList),
      isUnorderedList: list.some((e) => e.structure.isUnorderedList),
      numberedHeading: list.every((e) => e.structure.numberedHeading),
      headingSemanticType: first.sectionType,
    },
    confirmedDecisionSet: list.some((e) => e.confirmedDecisionSet),
    sectionType: first.sectionType,
    sectionPropagationAllowed: list.some((e) => e.sectionPropagationAllowed),
    cueLinks: list.flatMap((e) => e.cueLinks || []),
  };
}

// Compat aliases used by older tests
export function classifyHeadingSemanticType(title) {
  const t = classifySectionType(title, "");
  if (t === "RANKED_RECOMMENDATION_SECTION" || t === "LEAD_RECOMMENDATION_SECTION") {
    return "RANKED_RECOMMENDATION_HEADING";
  }
  if (t === "CONSIDERATION_SET_SECTION") return "CONSIDERATION_SET_HEADING";
  if (t === "RECOMMENDATION_SET_SECTION") return "GENERAL_RECOMMENDATION_HEADING";
  if (t === "NEUTRAL_CATALOG_SECTION") return "NEUTRAL_CATALOG_HEADING";
  if (t === "DESCRIPTIVE_SECTION") return "DESCRIPTIVE_HEADING";
  return "OTHER";
}
