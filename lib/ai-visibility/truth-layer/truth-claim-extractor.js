/**
 * Deterministic AI claim extraction for Truth Layer (P0D-A).
 * Constrained claim types only — no general-purpose factual extractor.
 */

import { randomUUID } from "crypto";
import { splitSentencesWithOffsets, validateEntityBinding } from "../associations/entity-binding.js";
import { enrichEvidenceWithPromptMetadata } from "../associations/prompt-metadata-lookup.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";

import { normalizeMatchKey } from "../normalize-entities.js";

export const TRUTH_CLAIM_EXTRACTOR_VERSION = "ai_visibility_truth_claim_extractor_v1_1";

/** Known parent-company surface forms for explicit parent claim validation. */
const PARENT_LABELS = new Set(
  [
    "marriott",
    "marriott international",
    "hilton",
    "hilton worldwide",
    "choice hotels",
    "ihg",
    "ihg hotels",
    "accor",
    "hyatt",
    "hyatt hotels",
    "best western",
    "radisson",
    "wyndham",
  ].map((s) => normalizeMatchKey(s))
);

/** Explicit parent-company phrasing only — no generic "X brand/collection" capture. */
const PARENT_PATTERNS = [
  /\bpart of\s+([A-Z][A-Za-z0-9&\-\s]{2,48}?)(?:['']s|\s+portfolio|\s+brand|\s+collection|\s+family|\.|,|$)/gi,
  /\b(?:owned by|operated by|managed by)\s+([A-Z][A-Za-z0-9&\-\s]{2,48}?)(?:\.|,|$)/gi,
  /\ba\s+(?:brand|collection)\s+(?:of|from)\s+([A-Z][A-Za-z0-9&\-\s]{2,48}?)(?:\.|,|$)/gi,
  /\b([A-Z][A-Za-z0-9&\-\s]{2,48}?)['']s\s+(?:portfolio|collection|brand family|soft brand|lifestyle brand)/gi,
];

const CHAIN_SCALE_MAP = Object.freeze({
  luxury: "Luxury",
  "upper upscale": "Upper Upscale",
  "upper-upscale": "Upper Upscale",
  upscale: "Upscale",
  "upper midscale": "Upper Midscale",
  midscale: "Midscale",
  economy: "Economy",
  lifestyle: "Lifestyle / Boutique",
});

const BRAND_MODEL_MAP = Object.freeze({
  "soft brand": "Soft Brand",
  "collection brand": "Collection Brand",
  "hard brand": "Hard Brand",
  "lifestyle brand": "Lifestyle Brand",
  "conversion brand": "Conversion Brand",
});

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function entityNamesNormalized(entity) {
  return [entity?.name, ...(entity?.aliases || [])]
    .filter(Boolean)
    .map((n) => normalizeMatchKey(n));
}

/**
 * Parent claim must name a plausible parent company — not the subject brand or a sibling token.
 */
export function isExplicitParentClaimValue(claimValue, entity) {
  const key = normalizeMatchKey(String(claimValue || "").trim());
  if (!key || key.length < 3) return false;
  if (/^(the|a|an|and|or|its|their|this|that|these|those)$/i.test(key)) return false;

  const entityKeys = entityNamesNormalized(entity);
  for (const ek of entityKeys) {
    if (ek === key || ek.includes(key) || key.includes(ek)) return false;
    const first = ek.split(/\s+/)[0];
    if (first && first.length >= 4 && key === first) return false;
  }

  if (PARENT_LABELS.has(key)) return true;
  for (const p of PARENT_LABELS) {
    if (key.includes(p) || p.includes(key)) return true;
  }

  return /\b(international|worldwide|hotels|group|corporation|inc|accor|ihg|hyatt|marriott|hilton|choice|wyndham|radisson)\b/i.test(
    claimValue
  );
}

function entityInSentence(sentence, entity) {
  const names = [entity?.name, ...(entity?.aliases || [])].filter(Boolean);
  return names.some((n) => new RegExp(`\\b${escapeRegExp(n)}\\b`, "i").test(sentence));
}

function buildSpan(sentence, matchStart, matchEnd) {
  const text = sentence.text.trim();
  return {
    start: sentence.start,
    end: sentence.end,
    text,
    exactText: text,
  };
}

/**
 * @param {object} evidence
 * @param {object} entity { id, name, aliases? }
 * @param {object} [options]
 */
export function extractTruthClaimsFromEvidence(evidence, entity, options = {}) {
  const ev = enrichEvidenceWithPromptMetadata(evidence);
  const text = String(ev.payload?.rawResponseText || "");
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const scenario = resolvePromptScenario(
    { promptId: ev.promptId, promptFamily: ev.promptFamily },
    scenarioIndex
  );
  const mentions = ev.payload?.mentions || [];
  const claims = [];

  if (!text.trim() || !entity?.id) return claims;

  const sentences = splitSentencesWithOffsets(text);

  for (const sentence of sentences) {
    if (!entityInSentence(sentence.text, entity)) continue;

    // PARENT_COMPANY
    for (const re of PARENT_PATTERNS) {
      re.lastIndex = 0;
      let m;
      while ((m = re.exec(sentence.text)) !== null) {
        const claimed = String(m[1] || m[0])
          .trim()
          .replace(/\s+(brand|collection|portfolio|family)$/i, "")
          .trim();
        if (!claimed || claimed.length < 3) continue;
        if (/^(the|a|an|this|that|these|those)$/i.test(claimed)) continue;
        if (!isExplicitParentClaimValue(claimed, entity)) continue;

        const binding = validateEntityBinding({
          text,
          spanStart: sentence.start + m.index,
          spanEnd: sentence.start + m.index + m[0].length,
          entity,
          mentions,
        });
        if (!binding.ok) continue;

        claims.push(makeClaim({
          entity,
          ev,
          scenario,
          claimType: "PARENT_COMPANY",
          claimValue: claimed,
          span: buildSpan(sentence, m.index, m.index + m[0].length),
        }));
      }
    }

    // CHAIN_SCALE
    const lower = sentence.text.toLowerCase();
    for (const [phrase, governed] of Object.entries(CHAIN_SCALE_MAP)) {
      if (!lower.includes(phrase)) continue;
      const idx = lower.indexOf(phrase);
      claims.push(makeClaim({
        entity,
        ev,
        scenario,
        claimType: "CHAIN_SCALE",
        claimValue: governed,
        span: buildSpan(sentence, idx, idx + phrase.length),
        claimPhrase: phrase,
      }));
    }

    // BRAND_MODEL
    for (const [phrase, governed] of Object.entries(BRAND_MODEL_MAP)) {
      if (!lower.includes(phrase)) continue;
      const idx = lower.indexOf(phrase);
      claims.push(makeClaim({
        entity,
        ev,
        scenario,
        claimType: "BRAND_MODEL",
        claimValue: governed,
        span: buildSpan(sentence, idx, idx + phrase.length),
        claimPhrase: phrase,
      }));
    }

    // SOFT_BRAND / COLLECTION
    if (/\bsoft[\s-]?brand\b/i.test(sentence.text) || /\bcollection brand\b/i.test(sentence.text)) {
      claims.push(makeClaim({
        entity,
        ev,
        scenario,
        claimType: "SOFT_BRAND_COLLECTION",
        claimValue: /\bsoft/i.test(sentence.text) ? "Soft/Collection Brand" : "Collection Brand",
        span: buildSpan(sentence, 0, sentence.text.length),
      }));
    }
  }

  return dedupeClaims(claims);
}

function makeClaim({ entity, ev, scenario, claimType, claimValue, span }) {
  return {
    claimId: `tcl_${randomUUID().replace(/-/g, "").slice(0, 16)}`,
    subjectBrandId: entity.id,
    subjectBrandName: entity.name,
    claimType,
    claimValue,
    supportingSpan: span,
    responseId: ev.responseId,
    evidenceId: ev.evidenceId,
    promptId: ev.promptId,
    scenarioId: scenario.scenarioId,
    scenarioStatus: scenario.scenarioStatus,
    provider: ev.provider,
    language: ev.language || ev.payload?.language || "en",
    geography: ev.commercialRegion || ev.countryName || ev.geographyScope || null,
    explicitness: "EXPLICIT",
    extractorVersion: TRUTH_CLAIM_EXTRACTOR_VERSION,
  };
}

function dedupeClaims(claims) {
  const seen = new Set();
  const out = [];
  for (const c of claims) {
    const key = `${c.evidenceId}:${c.claimType}:${c.claimValue}:${c.supportingSpan?.exactText?.slice(0, 40)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(c);
  }
  return out;
}

export { CHAIN_SCALE_MAP, BRAND_MODEL_MAP, PARENT_LABELS };
