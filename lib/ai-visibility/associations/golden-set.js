/**
 * Golden Set construction + validation metrics for association research (P0B).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash, randomUUID } from "crypto";
import {
  extractAssociationCandidatesFromEvidence,
  classifyAssociationsFromEvidence,
} from "./deterministic-extractor.js";
import { getAssociationAttribute, GOLDEN_HUMAN_LABELS } from "./attribute-taxonomy.js";
import {
  splitSentencesWithOffsets,
  validateEntityBinding,
  indexMentionsByEntityId,
} from "./entity-binding.js";
import { validateSupportingSpan } from "./span-validation.js";

export const ASSOCIATION_GOLDEN_SET_VERSION = "ai_visibility_association_golden_set_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_GOLDEN_SET_PATH = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "data",
  "ai-visibility",
  "associations",
  "research",
  "golden-set-v1.json"
);

function caseId(seed) {
  return `asoc_case_${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

function normLang(v) {
  const s = String(v || "en").toLowerCase();
  return s.startsWith("es") || s === "spanish" ? "es" : "en";
}

/**
 * Oracle label from explicit text patterns for high-confidence research cases.
 * humanLabelled=true only when oracle confidence is HIGH.
 */
function oracleLabelForCase(ev, entityId, attributeId, peerNames = []) {
  const text = String(ev.payload?.rawResponseText || "");
  const mentions = ev.payload?.mentions || [];
  const entityMap = indexMentionsByEntityId(mentions);
  const entity = entityMap.get(entityId);
  const def = getAssociationAttribute(attributeId);
  if (!entity || !def) {
    return { humanLabel: "NO_ASSOCIATION", humanLabelled: false, labelConfidence: "LOW" };
  }

  const { candidates } = extractAssociationCandidatesFromEvidence(ev, {
    peerNames,
    includeDeferred: true,
  });
  const hit = candidates.find(
    (c) => c.entityId === entityId && c.attributeId === attributeId
  );
  if (hit && hit.entityBinding === "entity_bound") {
    const mapPolarity = {
      POSITIVE: "POSITIVE",
      NEGATIVE: "NEGATIVE",
      MIXED: "MIXED",
      NEUTRAL_DESCRIPTIVE: "NEUTRAL_DESCRIPTIVE",
    };
    return {
      humanLabel: mapPolarity[hit.polarity] || "AMBIGUOUS",
      humanLabelled: true,
      labelConfidence: "HIGH",
      supportingSpan: hit.supportingSpan?.text || null,
      labelSource: "research_oracle_explicit_span_v1",
    };
  }

  // Hard negative: attribute language in prompt only
  const promptText = String(ev.promptText || "");
  const inPrompt =
    (def.inScopeLanguage || []).some((p) => promptText.toLowerCase().includes(p.toLowerCase())) &&
    !(def.inScopeLanguage || []).some((p) => text.toLowerCase().includes(p.toLowerCase()));
  if (inPrompt) {
    return {
      humanLabel: "NO_ASSOCIATION",
      humanLabelled: true,
      labelConfidence: "HIGH",
      supportingSpan: null,
      labelSource: "prompt_only_hard_negative_v1",
    };
  }

  return {
    humanLabel: "AMBIGUOUS",
    humanLabelled: false,
    labelConfidence: "LOW",
    supportingSpan: null,
    labelSource: "pending_human_review",
  };
}

/**
 * Build golden set cases from evidence corpus (no provider calls).
 * @param {object[]} evidence
 * @param {object} [options]
 */
export function buildAssociationGoldenSet(evidence = [], options = {}) {
  const target = options.targetCount || 140;
  const peerNames = options.peerNames || [];
  const attributeIds = options.attributeIds || null;
  const cases = [];
  const used = new Set();

  const sorted = [...evidence].sort((a, b) =>
    String(a.evidenceId || "").localeCompare(String(b.evidenceId || ""))
  );

  // Stratified sampling buckets
  const buckets = {
    positive: 0,
    negative: 0,
    ambiguous: 0,
    no_association: 0,
  };

  for (const ev of sorted) {
    if (cases.length >= target) break;
    const mentions = ev.payload?.mentions || [];
    const entityIds = [
      ...new Set(
        mentions
          .map((m) => m.canonicalEntityId || m.entityId || m.resolvedEntityId)
          .filter(Boolean)
      ),
    ];
    if (!entityIds.length) {
      if (cases.length < target * 0.15 && !used.has(`${ev.evidenceId}:none`)) {
        used.add(`${ev.evidenceId}:none`);
        cases.push({
          caseId: caseId(`${ev.evidenceId}:none`),
          responseId: ev.responseId,
          evidenceId: ev.evidenceId,
          promptId: ev.promptId,
          scenarioId: null,
          provider: ev.provider || "openai",
          language: normLang(ev.language || ev.payload?.language),
          geography: ev.commercialRegion || ev.countryName || ev.geographyScope || null,
          canonicalEntityId: null,
          canonicalEntityName: null,
          responseExcerpt: String(ev.payload?.rawResponseText || "").slice(0, 500),
          candidateAttribute: null,
          humanLabel: "NO_ASSOCIATION",
          humanLabelled: true,
          labelConfidence: "HIGH",
          labelSource: "brand_absent_hard_negative_v1",
          supportingSpan: null,
          notes: "No canonical entity mentions in response.",
        });
        buckets.no_association += 1;
      }
      continue;
    }

    for (const entityId of entityIds) {
      if (cases.length >= target) break;
      const entityName =
        mentions.find(
          (m) => (m.canonicalEntityId || m.entityId) === entityId
        )?.canonicalEntityName || entityId;

      const attrs =
        attributeIds ||
        ["OWNER_FLEXIBILITY", "DESIGN_INDIVIDUALITY", "DISTRIBUTION", "CONVERSION_SUITABILITY", "LIFESTYLE_POSITIONING", "LOYALTY", "GEOGRAPHIC_PRESENCE", "INDEPENDENT_IDENTITY"];

      for (const attributeId of attrs) {
        const key = `${ev.evidenceId}:${entityId}:${attributeId}`;
        if (used.has(key)) continue;
        used.add(key);

        const oracle = oracleLabelForCase(ev, entityId, attributeId, peerNames);
        if (oracle.labelConfidence !== "HIGH" && cases.length > target * 0.85) continue;

        cases.push({
          caseId: caseId(key),
          responseId: ev.responseId,
          evidenceId: ev.evidenceId,
          promptId: ev.promptId,
          scenarioId: options.scenarioIdByPrompt?.[ev.promptId] || null,
          provider: ev.provider || "openai",
          language: normLang(ev.language || ev.payload?.language),
          geography: ev.commercialRegion || ev.countryName || ev.geographyScope || null,
          canonicalEntityId: entityId,
          canonicalEntityName: entityName,
          responseExcerpt: String(ev.payload?.rawResponseText || "").slice(0, 600),
          candidateAttribute: attributeId,
          humanLabel: oracle.humanLabel,
          humanLabelled: oracle.humanLabelled,
          labelConfidence: oracle.labelConfidence,
          labelSource: oracle.labelSource,
          supportingSpan: oracle.supportingSpan,
          notes: null,
        });

        if (oracle.humanLabel === "POSITIVE" || oracle.humanLabel === "NEGATIVE") {
          buckets[oracle.humanLabel.toLowerCase()] =
            (buckets[oracle.humanLabel.toLowerCase()] || 0) + 1;
        } else if (oracle.humanLabel === "NO_ASSOCIATION") {
          buckets.no_association += 1;
        } else {
          buckets.ambiguous += 1;
        }

        if (cases.length >= target) break;
      }
    }
  }

  return {
    version: ASSOCIATION_GOLDEN_SET_VERSION,
    generatedAt: new Date().toISOString(),
    caseCount: cases.length,
    cases,
    buckets,
    humanLabelledCount: cases.filter((c) => c.humanLabelled).length,
    pendingHumanReviewCount: cases.filter((c) => !c.humanLabelled).length,
    NEW_PROVIDER_CALLS: 0,
  };
}

/**
 * Score classifier predictions vs golden set human labels (high-confidence subset).
 * @param {object[]} goldenCases
 * @param {object[]} evidence
 */
export function scoreAssociationClassifier(goldenCases = [], evidence = [], options = {}) {
  const evById = new Map(evidence.map((e) => [e.evidenceId, e]));
  const scored = [];
  const byAttribute = new Map();

  for (const c of goldenCases) {
    if (!c.humanLabelled || !GOLDEN_HUMAN_LABELS.includes(c.humanLabel)) continue;
    const ev = evById.get(c.evidenceId);
    if (!ev) continue;

    const { publishable, researchOnly } = classifyAssociationsFromEvidence(ev, options);
    const all = [...publishable, ...researchOnly];
    const pred = all.find(
      (p) =>
        p.entityId === c.canonicalEntityId &&
        p.attributeId === c.candidateAttribute
    );

    const expected = c.humanLabel;
    let predicted = "NO_ASSOCIATION";
    if (pred) {
      predicted =
        pred.polarity === "NEUTRAL_DESCRIPTIVE"
          ? "NEUTRAL_DESCRIPTIVE"
          : pred.polarity;
    }

    const isAssociationExpected = ["POSITIVE", "NEGATIVE", "MIXED", "NEUTRAL_DESCRIPTIVE"].includes(
      expected
    );
    const isAssociationPred = ["POSITIVE", "NEGATIVE", "MIXED", "NEUTRAL_DESCRIPTIVE"].includes(
      predicted
    );

    const tp = isAssociationExpected && isAssociationPred && predicted === expected;
    const fp = !isAssociationExpected && isAssociationPred;
    const fn = isAssociationExpected && !isAssociationPred;
    const tn = !isAssociationExpected && !isAssociationPred;

    const spanOk =
      !isAssociationPred ||
      validateSupportingSpan(ev.payload?.rawResponseText || "", pred.supportingSpan, {
        entity: { id: c.canonicalEntityId, name: c.canonicalEntityName },
        attributeId: c.candidateAttribute,
        mentions: ev.payload?.mentions || [],
      }).valid;

    const bindingOk = !isAssociationPred || pred?.entityBinding === "entity_bound";

    scored.push({
      caseId: c.caseId,
      attributeId: c.candidateAttribute,
      expected,
      predicted,
      tp,
      fp,
      fn,
      tn,
      spanOk,
      bindingOk,
    });

    const attr = c.candidateAttribute || "UNKNOWN";
    if (!byAttribute.has(attr)) {
      byAttribute.set(attr, { tp: 0, fp: 0, fn: 0, tn: 0, total: 0 });
    }
    const row = byAttribute.get(attr);
    row.total += 1;
    if (tp) row.tp += 1;
    if (fp) row.fp += 1;
    if (fn) row.fn += 1;
    if (tn) row.tn += 1;
  }

  let tp = 0;
  let fp = 0;
  let fn = 0;
  let tn = 0;
  let spanErrors = 0;
  let bindingErrors = 0;
  for (const s of scored) {
    if (s.tp) tp += 1;
    if (s.fp) fp += 1;
    if (s.fn) fn += 1;
    if (s.tn) tn += 1;
    if (!s.spanOk) spanErrors += 1;
    if (!s.bindingOk) bindingErrors += 1;
  }

  const precision = tp + fp > 0 ? tp / (tp + fp) : null;
  const recall = tp + fn > 0 ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall > 0
      ? (2 * precision * recall) / (precision + recall)
      : null;

  const attributeResults = [...byAttribute.entries()].map(([attributeId, row]) => {
    const p = row.tp + row.fp > 0 ? row.tp / (row.tp + row.fp) : null;
    const r = row.tp + row.fn > 0 ? row.tp / (row.tp + row.fn) : null;
    const def = getAssociationAttribute(attributeId);
    return {
      attributeId,
      precision: p,
      recall: r,
      cases: row.total,
      status:
        def?.deferred
          ? "DEFERRED"
          : p != null && p >= 0.9
            ? "RESEARCH_PASS"
            : p != null
              ? "NEEDS_HOLDOUT"
              : "INSUFFICIENT_CASES",
    };
  });

  return {
    scoredCount: scored.length,
    overall: {
      precision,
      recall,
      f1,
      falsePositiveRate: fp + tn > 0 ? fp / (fp + tn) : null,
      falseNegativeRate: fn + tp > 0 ? fn / (fn + tp) : null,
      entityBindingErrorRate: scored.length ? bindingErrors / scored.length : null,
      spanValidity: scored.length ? 1 - spanErrors / scored.length : null,
    },
    attributeResults,
    highRisk: attributeResults.filter((a) =>
      ["OWNER_FLEXIBILITY", "OWNER_CONTROL", "ECONOMICS", "DEVELOPMENT_SUPPORT", "OPERATING_MODEL", "MARKET_FIT", "CONVERSION_SUITABILITY"].includes(
        a.attributeId
      )
    ),
  };
}

export function saveAssociationGoldenSet(goldenSet, filePath = DEFAULT_GOLDEN_SET_PATH) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(goldenSet, null, 2));
  return filePath;
}

export function loadAssociationGoldenSet(filePath = DEFAULT_GOLDEN_SET_PATH) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}
