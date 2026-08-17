/**
 * Deterministic AI Brand Association candidate extraction (P0B research).
 * Hybrid pipeline step A — candidate generation with span requirement.
 */

import { randomUUID } from "crypto";
import {
  ASSOCIATION_ATTRIBUTES,
  ASSOCIATION_EXPLICITNESS,
  ASSOCIATION_POLARITIES,
  getAssociationAttribute,
} from "./attribute-taxonomy.js";
import {
  detectSiblingCollision,
  indexMentionsByEntityId,
  splitSentencesWithOffsets,
  validateEntityBinding,
} from "./entity-binding.js";
import {
  buildSentenceBoundedSpan,
  isTableRowSpan,
  validateSupportingSpan,
} from "./span-validation.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";
import { enrichEvidenceWithPromptMetadata } from "./prompt-metadata-lookup.js";

export const ASSOCIATION_EXTRACTOR_VERSION = "ai_visibility_association_extractor_det_v1_1";

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function newAssociationEvidenceId() {
  return `asoc_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

function compilePatterns(def) {
  const pos = [];
  const neg = [];
  const fp = [];
  for (const phrase of def.inScopeLanguage || []) {
    pos.push(new RegExp(escapeRegExp(phrase), "gi"));
  }
  for (const phrase of def.negativeExamples || []) {
    neg.push(new RegExp(escapeRegExp(phrase.split(" ").slice(0, 4).join(" ")), "gi"));
  }
  for (const phrase of def.commonFalsePositives || []) {
    fp.push(new RegExp(escapeRegExp(phrase), "gi"));
  }
  for (const phrase of def.outOfScopeLanguage || []) {
    fp.push(new RegExp(escapeRegExp(phrase), "gi"));
  }
  // Generic negative polarity cues
  neg.push(/\b(?:less|limited|lower|weak|lack(?:s|ing)?|not ideal|not suitable)\b[^.!?]{0,80}\b(?:flexibility|control|distribution|loyalty|support)\b/gi);
  return { pos, neg, fp };
}

const PATTERN_CACHE = new Map(
  ASSOCIATION_ATTRIBUTES.map((def) => [def.attributeId, compilePatterns(def)])
);

function findMatches(text, patterns) {
  const hits = [];
  for (const re of patterns) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text)) !== null) {
      hits.push({ start: m.index, end: m.index + m[0].length, text: m[0] });
    }
  }
  return hits;
}

function classifyPolarity(def, sentenceText, matchText) {
  const lower = sentenceText.toLowerCase();
  const negCues = [
    "less ",
    "limited ",
    "lower ",
    "weak ",
    "lack ",
    "lacks ",
    "not ideal",
    "not suitable",
    "not a good",
    "harder",
    "difficult",
  ];
  const posCues = ["strong ", "greater ", "more ", "better ", "well-suited", "ideal ", "excellent "];
  const hasNeg = negCues.some((c) => lower.includes(c));
  const hasPos = posCues.some((c) => lower.includes(c));
  if (hasNeg && hasPos) return "MIXED";
  if (hasNeg) return "NEGATIVE";
  if (hasPos) return "POSITIVE";
  if (/\b(?:is|are|offers|provides|emphasizes|known for|positioned as)\b/i.test(sentenceText)) {
    return "NEUTRAL_DESCRIPTIVE";
  }
  return "POSITIVE";
}

/** Reject attribute hits inside markdown table rows. */
function isHitInTableRow(text, hitStart) {
  const lineStart = text.lastIndexOf("\n", hitStart) + 1;
  const lineEnd = text.indexOf("\n", hitStart);
  const line = text.slice(lineStart, lineEnd === -1 ? text.length : lineEnd);
  return isTableRowSpan(line);
}

/**
 * @param {object} evidence evidence record from store
 * @param {object} [options]
 */
export function extractAssociationCandidatesFromEvidence(evidence, options = {}) {
  const ev = enrichEvidenceWithPromptMetadata(evidence);
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const scenarioIndex = options.scenarioIndex || buildScenarioRegistryIndex(registry);
  const peerNames = options.peerNames || [];
  const text = String(ev?.payload?.rawResponseText || "");
  const mentions = ev?.payload?.mentions || [];
  const citations = ev?.payload?.citations || [];
  const promptRow = {
    promptId: ev?.promptId,
    promptFamily: ev?.promptFamily || null,
    intentTerritory: ev?.intentTerritory || null,
    version: ev?.promptVersion || null,
  };
  const scenario = resolvePromptScenario(promptRow, scenarioIndex);

  if (!text.trim()) {
    return { evidenceId: ev?.evidenceId, candidates: [], extractionVersion: ASSOCIATION_EXTRACTOR_VERSION };
  }

  const entityIndex = indexMentionsByEntityId(mentions);
  const sentences = splitSentencesWithOffsets(text);
  const candidates = [];

  for (const [entityId, entity] of entityIndex) {
    for (const def of ASSOCIATION_ATTRIBUTES) {
      if (def.deferred && !options.includeDeferred) continue;
      const patterns = PATTERN_CACHE.get(def.attributeId);
      const hits = findMatches(text, patterns.pos);
      for (const hit of hits) {
        const fpHit = findMatches(text.slice(hit.start, hit.end + 80), patterns.fp);
        if (fpHit.length) continue;

        const binding = validateEntityBinding({
          text,
          spanStart: hit.start,
          spanEnd: hit.end,
          entity,
          mentions,
        });
        if (!binding.ok) continue;

        const sentence =
          sentences.find((s) => hit.start >= s.start && hit.start <= s.end) || null;
        if (
          sentence &&
          detectSiblingCollision({
            sentenceText: sentence.text,
            entity,
            peerNames,
          })
        ) {
          continue;
        }

        if (isHitInTableRow(text, hit.start)) continue;

        const span = sentence
          ? buildSentenceBoundedSpan(text, sentence, hit, entity)
          : { start: hit.start, end: hit.end, text: text.slice(hit.start, hit.end), exactText: text.slice(hit.start, hit.end) };

        const spanCheck = validateSupportingSpan(text, span, {
          entity,
          attributeId: def.attributeId,
          mentions,
        });
        if (!spanCheck.valid) continue;

        const polarity = classifyPolarity(def, binding.sentenceText || span.text, hit.text);
        const citationIds = (citations || [])
          .filter((c) => {
            const pos = c.start ?? c.position ?? null;
            if (pos == null) return false;
            return pos >= span.start && pos <= span.end;
          })
          .map((c) => c.citationId)
          .filter(Boolean);

        candidates.push({
          associationEvidenceId: newAssociationEvidenceId(),
          entityId,
          entityName: entity.name,
          attributeId: def.attributeId,
          polarity,
          explicitness: "EXPLICIT",
          supportingSpan: span,
          citationIds,
          hasProviderCitation: citationIds.length > 0,
          evidenceId: ev.evidenceId,
          responseId: ev.responseId,
          promptId: ev.promptId,
          scenarioId: scenario.scenarioId,
          scenarioStatus: scenario.scenarioStatus,
          geographyKey:
            ev.commercialRegion ||
            ev.regionName ||
            ev.countryName ||
            ev.geographyScope ||
            null,
          language: ev.language || ev.payload?.language || "en",
          periodKey: ev.batchId || ev.runId || null,
          provider: ev.provider || null,
          extractorVersion: ASSOCIATION_EXTRACTOR_VERSION,
          entityBinding: binding.reason,
        });
      }
    }
  }

  return {
    evidenceId: ev.evidenceId,
    candidates,
    extractionVersion: ASSOCIATION_EXTRACTOR_VERSION,
  };
}

/**
 * Research classifier = deterministic extractor with publication filter.
 * LLM adjudication hook reserved for future hybrid step (not invoked here).
 */
export function classifyAssociationsFromEvidence(evidence, options = {}) {
  const { candidates } = extractAssociationCandidatesFromEvidence(evidence, options);
  const publishable = [];
  const researchOnly = [];

  for (const c of candidates) {
    const def = getAssociationAttribute(c.attributeId);
    const passes =
      c.explicitness === "EXPLICIT" &&
      c.supportingSpan?.text &&
      c.entityBinding === "entity_bound" &&
      !def?.deferred;
    if (passes) publishable.push(c);
    else researchOnly.push({ ...c, researchOnly: true });
  }

  return {
    evidenceId: evidence.evidenceId,
    publishable,
    researchOnly,
    extractorVersion: ASSOCIATION_EXTRACTOR_VERSION,
    LLM_ADJUDICATION_USED: false,
  };
}

export { ASSOCIATION_POLARITIES, ASSOCIATION_EXPLICITNESS };
