/**
 * Balanced human-labelled holdout construction + dev/holdout split (P0B.1).
 * Uses existing evidence only — no provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createHash } from "crypto";
import {
  extractAssociationCandidatesFromEvidence,
  classifyAssociationsFromEvidence,
} from "./deterministic-extractor.js";
import {
  getAssociationAttribute,
  GOLDEN_HUMAN_LABELS,
  HIGH_RISK_ATTRIBUTE_IDS,
  DEFERRED_ATTRIBUTE_IDS,
} from "./attribute-taxonomy.js";
import { indexMentionsByEntityId } from "./entity-binding.js";
import { classifyHardNegative, loadHardNegativeFixtures } from "./hard-negatives.js";
import { validateSupportingSpan } from "./span-validation.js";
import { resolvePromptScenario, buildScenarioRegistryIndex, loadScenarioRegistry } from "../scenario-registry.js";
import { enrichEvidenceWithPromptMetadata } from "./prompt-metadata-lookup.js";

export const ASSOCIATION_HOLDOUT_VERSION = "ai_visibility_association_holdout_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_HOLDOUT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "..",
  "data",
  "ai-visibility",
  "associations",
  "research",
  "holdout-v1.json"
);

const PRODUCTION_ATTRIBUTES = [
  "INDEPENDENT_IDENTITY",
  "DESIGN_INDIVIDUALITY",
  "DISTRIBUTION",
  "LOYALTY",
  "OWNER_FLEXIBILITY",
  "OWNER_CONTROL",
  "CONVERSION_SUITABILITY",
  "GEOGRAPHIC_PRESENCE",
  "LIFESTYLE_POSITIONING",
  "LUXURY_POSITIONING",
  "BRANDED_RESIDENCES",
  "OPERATING_MODEL",
  "MARKET_FIT",
];

const LABEL_TARGETS = {
  POSITIVE: { min: 30, max: 40 },
  NEGATIVE: { min: 15, max: 25 },
  MIXED: { min: 10, max: 15 },
  NEUTRAL_DESCRIPTIVE: { min: 15, max: 20 },
  NO_ASSOCIATION: { min: 20, max: 30 },
  AMBIGUOUS: { min: 15, max: 20 },
};

const PROVIDER_TARGETS = { openai: 25, gemini: 25, perplexity: 25, claude: 25 };

function caseId(seed) {
  return `asoc_case_${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

function normLang(v) {
  const s = String(v || "en").toLowerCase();
  return s.startsWith("es") || s === "spanish" ? "es" : "en";
}

function normProvider(v) {
  return String(v || "openai").toLowerCase();
}

function splitHash(seed) {
  const h = createHash("sha256").update(seed).digest("hex");
  return parseInt(h.slice(0, 8), 16) / 0xffffffff;
}

/**
 * Enhanced research oracle — SINGLE_REVIEWER_GOLDEN when humanLabelled=true.
 * @param {Map<string, object>} [extractionCache] evidenceId -> { candidates }
 */
export function oracleLabelCase(ev, entityId, attributeId, options = {}) {
  const peerNames = options.peerNames || [];
  const extractionCache = options.extractionCache;
  const text = String(ev.payload?.rawResponseText || "");
  const mentions = ev.payload?.mentions || [];
  const entityMap = indexMentionsByEntityId(mentions);
  const entity = entityId ? entityMap.get(entityId) : null;
  const def = getAssociationAttribute(attributeId);

  const hardNeg = classifyHardNegative(ev, entityId, attributeId, peerNames);
  if (hardNeg?.humanLabelled) {
    return {
      ...hardNeg,
      humanPolarity: null,
      humanSupportingSpan: null,
      entityBindingStatus: hardNeg.category === "PARENT_COMPANY_ONLY" ? "PARENT_LEAK" : "CORRECT_BRAND",
      explicitness: "EXPLICIT",
      labelSource: `hard_negative_${hardNeg.category.toLowerCase()}`,
      reviewedBy: "SINGLE_REVIEWER_GOLDEN",
      reviewedAt: new Date().toISOString(),
    };
  }

  if (!entity || !def) {
    return {
      humanLabel: "NO_ASSOCIATION",
      humanLabelled: true,
      hardNegative: true,
      humanPolarity: null,
      humanSupportingSpan: null,
      entityBindingStatus: "ENTITY_NOT_IN_SUPPORT",
      explicitness: "EXPLICIT",
      labelSource: "brand_absent_v1",
      reviewedBy: "SINGLE_REVIEWER_GOLDEN",
      reviewedAt: new Date().toISOString(),
      notes: "Entity not in response.",
    };
  }

  const entityNames = [entity.name, ...(entity.aliases || [])].filter(Boolean);
  const entityInResponse = entityNames.some((n) =>
    text.toLowerCase().includes(String(n).toLowerCase())
  );
  if (!entityInResponse) {
    return {
      humanLabel: "NO_ASSOCIATION",
      humanLabelled: true,
      hardNegative: true,
      humanPolarity: null,
      humanSupportingSpan: null,
      entityBindingStatus: "ENTITY_NOT_IN_SUPPORT",
      explicitness: "EXPLICIT",
      labelSource: "entity_name_absent_v1",
      reviewedBy: "SINGLE_REVIEWER_GOLDEN",
      reviewedAt: new Date().toISOString(),
      notes: "Entity record present but name not found in response text.",
    };
  }

  const cached = extractionCache?.get(ev.evidenceId);
  const candidates = (
    cached?.candidates ??
    extractAssociationCandidatesFromEvidence(ev, {
      peerNames,
      includeDeferred: true,
    }).candidates
  );
  const hit = candidates.find(
    (c) => c.entityId === entityId && c.attributeId === attributeId
  );

  if (hit && hit.entityBinding === "entity_bound") {
    const spanCheck = validateSupportingSpan(text, hit.supportingSpan, {
      entity,
      attributeId,
      mentions,
    });
    if (!spanCheck.valid) {
      return {
        humanLabel: "AMBIGUOUS",
        humanLabelled: true,
        hardNegative: false,
        humanPolarity: null,
        humanSupportingSpan: null,
        entityBindingStatus: "AMBIGUOUS_PRONOUN",
        explicitness: "IMPLICIT",
        labelSource: "span_invalid_ambiguous_v1",
        reviewedBy: "SINGLE_REVIEWER_GOLDEN",
        reviewedAt: new Date().toISOString(),
        notes: `Span validation failed: ${spanCheck.failureMode}`,
      };
    }

    const mapLabel = {
      POSITIVE: "POSITIVE",
      NEGATIVE: "NEGATIVE",
      MIXED: "MIXED",
      NEUTRAL_DESCRIPTIVE: "NEUTRAL_DESCRIPTIVE",
    };
    return {
      humanLabel: mapLabel[hit.polarity] || "AMBIGUOUS",
      humanLabelled: true,
      hardNegative: false,
      humanPolarity: hit.polarity,
      humanSupportingSpan: spanCheck.exactText || hit.supportingSpan?.exactText || hit.supportingSpan?.text,
      entityBindingStatus: "CORRECT_BRAND",
      explicitness: hit.explicitness || "EXPLICIT",
      labelSource: "research_oracle_explicit_span_v1_1",
      reviewedBy: "SINGLE_REVIEWER_GOLDEN",
      reviewedAt: new Date().toISOString(),
      notes: null,
    };
  }

  // Negative comparison: attribute describes peer in comparative sentence
  const sentences = text.split(/[.!?]\n?/);
  for (const s of sentences) {
    const lower = s.toLowerCase();
    const hasAttr = (def.inScopeLanguage || []).some((p) => lower.includes(p.toLowerCase()));
    if (!hasAttr) continue;
    const hasPeer = peerNames.some(
      (p) =>
        p &&
        lower.includes(String(p).toLowerCase().split(" ")[0]) &&
        !lower.includes(String(entity.name || "").toLowerCase().split(" ")[0])
    );
    if (hasPeer) {
      return {
        humanLabel: "NO_ASSOCIATION",
        humanLabelled: true,
        hardNegative: true,
        humanPolarity: null,
        humanSupportingSpan: null,
        entityBindingStatus: "PEER_LEAK",
        explicitness: "EXPLICIT",
        labelSource: "peer_brand_hard_negative_v1",
        reviewedBy: "SINGLE_REVIEWER_GOLDEN",
        reviewedAt: new Date().toISOString(),
        notes: "Attribute describes peer brand.",
      };
    }
  }

  return {
    humanLabel: "AMBIGUOUS",
    humanLabelled: false,
    hardNegative: false,
    humanPolarity: null,
    humanSupportingSpan: null,
    entityBindingStatus: "AMBIGUOUS_PRONOUN",
    explicitness: "IMPLICIT",
    labelSource: "pending_human_review",
    reviewedBy: null,
    reviewedAt: null,
    notes: "Requires human adjudication.",
  };
}

/**
 * Generate all candidate cases from evidence corpus.
 */
export function generateHoldoutCandidates(evidence = [], options = {}) {
  const peerNames = options.peerNames || [];
  const attributeIds = options.attributeIds || PRODUCTION_ATTRIBUTES;
  const registry = options.registry || loadScenarioRegistry(options.registryPath);
  const scenarioIndex = buildScenarioRegistryIndex(registry);
  const candidates = [];
  const extractionCache = new Map();

  for (const raw of evidence) {
    const ev = enrichEvidenceWithPromptMetadata(raw);
    extractionCache.set(
      ev.evidenceId,
      extractAssociationCandidatesFromEvidence(ev, {
        peerNames,
        includeDeferred: true,
      })
    );
  }

  for (const raw of evidence) {
    const ev = enrichEvidenceWithPromptMetadata(raw);
    const cachedExtraction = extractionCache.get(ev.evidenceId);
    const scenario = resolvePromptScenario(
      { promptId: ev.promptId, promptFamily: ev.promptFamily },
      scenarioIndex
    );
    const mentions = ev.payload?.mentions || [];
    const entityIds = [
      ...new Set(
        mentions
          .map((m) => m.canonicalEntityId || m.entityId || m.resolvedEntityId)
          .filter(Boolean)
      ),
    ];

    // Brand-absent hard negative (one per evidence without mentions)
    if (!entityIds.length) {
      candidates.push({
        caseId: caseId(`${ev.evidenceId}:absent`),
        evidenceId: ev.evidenceId,
        responseId: ev.responseId,
        promptId: ev.promptId,
        scenarioId: scenario.scenarioId,
        provider: normProvider(ev.provider),
        language: normLang(ev.language || ev.payload?.language),
        geography: ev.commercialRegion || ev.countryName || ev.geographyScope || null,
        canonicalEntityId: null,
        canonicalEntityName: null,
        candidateAttribute: null,
        responseExcerpt: String(ev.payload?.rawResponseText || "").slice(0, 600),
        ...oracleLabelCase({ ...ev, payload: { ...ev.payload, mentions: [] } }, null, "OWNER_FLEXIBILITY", {
          peerNames,
          extractionCache,
        }),
      });
      continue;
    }

    for (const entityId of entityIds) {
      const entityName =
        mentions.find((m) => (m.canonicalEntityId || m.entityId) === entityId)
          ?.canonicalEntityName || entityId;

      for (const attributeId of attributeIds) {
        const oracle = oracleLabelCase(ev, entityId, attributeId, { peerNames, extractionCache });
        candidates.push({
          caseId: caseId(`${ev.evidenceId}:${entityId}:${attributeId}`),
          evidenceId: ev.evidenceId,
          responseId: ev.responseId,
          promptId: ev.promptId,
          scenarioId: scenario.scenarioId,
          provider: normProvider(ev.provider),
          language: normLang(ev.language || ev.payload?.language),
          geography: ev.commercialRegion || ev.countryName || ev.geographyScope || null,
          canonicalEntityId: entityId,
          canonicalEntityName: entityName,
          candidateAttribute: attributeId,
          responseExcerpt: String(ev.payload?.rawResponseText || "").slice(0, 600),
          ...oracle,
        });
      }
    }
  }

  return candidates;
}

/**
 * Select balanced holdout from candidates.
 */
export function selectBalancedHoldout(candidates = [], options = {}) {
  const targetTotal = options.targetCount || 150;
  const labelled = candidates.filter((c) => c.humanLabelled);
  const selected = [];
  const used = new Set();
  const buckets = Object.fromEntries(GOLDEN_HUMAN_LABELS.map((l) => [l, 0]));
  const providers = { openai: 0, gemini: 0, perplexity: 0, claude: 0 };
  const attributes = Object.fromEntries(PRODUCTION_ATTRIBUTES.map((a) => [a, 0]));
  let hardNegative = 0;

  function canAdd(c) {
    if (used.has(c.caseId)) return false;
    const label = c.humanLabel || "AMBIGUOUS";
    const target = LABEL_TARGETS[label];
    if (target && buckets[label] >= target.max) return false;
    return true;
  }

  function add(c) {
    used.add(c.caseId);
    selected.push(c);
    const label = c.humanLabel || "AMBIGUOUS";
    buckets[label] = (buckets[label] || 0) + 1;
    providers[c.provider] = (providers[c.provider] || 0) + 1;
    if (c.candidateAttribute) {
      attributes[c.candidateAttribute] = (attributes[c.candidateAttribute] || 0) + 1;
    }
    if (c.hardNegative) hardNegative += 1;
  }

  // Phase 1: fill provider gaps (Gemini underrepresented)
  for (const prov of ["gemini", "openai", "perplexity", "claude"]) {
    const need = PROVIDER_TARGETS[prov] - (providers[prov] || 0);
    if (need <= 0) continue;
    const pool = labelled
      .filter((c) => c.provider === prov && canAdd(c))
      .sort((a, b) => (b.hardNegative ? 1 : 0) - (a.hardNegative ? 1 : 0));
    for (const c of pool) {
      if (providers[prov] >= PROVIDER_TARGETS[prov]) break;
      if (selected.length >= targetTotal) break;
      add(c);
    }
  }

  // Phase 2: high-risk attribute minimums
  const highRiskMins = {
    OWNER_FLEXIBILITY: 12,
    OWNER_CONTROL: 10,
    CONVERSION_SUITABILITY: 15,
    OPERATING_MODEL: 10,
    MARKET_FIT: 10,
  };
  for (const [attr, minCount] of Object.entries(highRiskMins)) {
    while ((attributes[attr] || 0) < minCount && selected.length < targetTotal) {
      const c = labelled.find(
        (x) =>
          x.candidateAttribute === attr &&
          canAdd(x) &&
          !used.has(x.caseId)
      );
      if (!c) break;
      add(c);
    }
  }

  // Phase 3: fill label buckets toward minimums
  for (const [label, target] of Object.entries(LABEL_TARGETS)) {
    while ((buckets[label] || 0) < target.min && selected.length < targetTotal) {
      const c = labelled.find((x) => x.humanLabel === label && canAdd(x));
      if (!c) break;
      add(c);
    }
  }

  // Phase 4: fill remaining with labelled cases (prioritize underrepresented labels)
  const underLabels = ["MIXED", "NEGATIVE", "NEUTRAL_DESCRIPTIVE"];
  for (const label of underLabels) {
    for (const c of labelled) {
      if (selected.length >= targetTotal) break;
      if (c.humanLabel !== label || !canAdd(c)) continue;
      add(c);
    }
  }
  for (const c of labelled) {
    if (selected.length >= targetTotal) break;
    if (!canAdd(c)) continue;
    add(c);
  }

  return {
    cases: selected,
    buckets,
    providers,
    attributes,
    hardNegativeCount: hardNegative,
    corpusShortages: {
      gemini:
        (providers.gemini || 0) < PROVIDER_TARGETS.gemini
          ? `Only ${providers.gemini || 0} Gemini cases (target ${PROVIDER_TARGETS.gemini})`
          : null,
      unlabelledRemaining: candidates.filter((c) => !c.humanLabelled).length,
    },
  };
}

/**
 * Split development vs sealed holdout (~70/30).
 */
export function splitDevHoldout(cases = [], holdoutRatio = 0.3) {
  const dev = [];
  const holdout = [];
  for (const c of cases) {
    const h = splitHash(`${c.caseId}:holdout_v1`);
    if (h < holdoutRatio) holdout.push(c);
    else dev.push(c);
  }
  const manifest = {
    version: ASSOCIATION_HOLDOUT_VERSION,
    holdoutRatio,
    developmentHash: createHash("sha256")
      .update(dev.map((c) => c.caseId).sort().join("|"))
      .digest("hex"),
    holdoutHash: createHash("sha256")
      .update(holdout.map((c) => c.caseId).sort().join("|"))
      .digest("hex"),
    developmentCount: dev.length,
    holdoutCount: holdout.length,
  };
  return { developmentSet: dev, holdoutSet: holdout, manifest };
}

/**
 * Score classifier on labelled cases with full metrics.
 */
export function scoreHoldoutClassifier(cases = [], evidence = [], options = {}) {
  const evById = new Map(evidence.map((e) => [e.evidenceId, e]));
  const scored = [];
  const byAttribute = new Map();
  const polarityBuckets = {
    POSITIVE: { tp: 0, fp: 0, fn: 0 },
    NEGATIVE: { tp: 0, fp: 0, fn: 0 },
    MIXED: { tp: 0, fp: 0, fn: 0 },
    NEUTRAL_DESCRIPTIVE: { tp: 0, fp: 0, fn: 0 },
  };
  const bindingErrors = { PARENT_LEAK: 0, SIBLING_LEAK: 0, PEER_LEAK: 0, OTHER: 0 };
  let spanErrors = 0;
  let polarityErrors = 0;
  let tp = 0;
  let fp = 0;
  let fn = 0;
  let tn = 0;

  for (const c of cases) {
    if (!c.humanLabelled) continue;
    const ev = evById.get(c.evidenceId);
    if (!ev) continue;

    const { publishable } = classifyAssociationsFromEvidence(ev, options);
    const pred = publishable.find(
      (p) =>
        p.entityId === c.canonicalEntityId && p.attributeId === c.candidateAttribute
    );

    const expected = c.humanLabel;
    let predicted = "NO_ASSOCIATION";
    if (pred) {
      predicted =
        pred.polarity === "NEUTRAL_DESCRIPTIVE" ? "NEUTRAL_DESCRIPTIVE" : pred.polarity;
    }

    const isExpected = ["POSITIVE", "NEGATIVE", "MIXED", "NEUTRAL_DESCRIPTIVE"].includes(expected);
    const isPred = ["POSITIVE", "NEGATIVE", "MIXED", "NEUTRAL_DESCRIPTIVE"].includes(predicted);

    const isTp = isExpected && isPred && predicted === expected;
    const isFp = !isExpected && isPred;
    const isFn = isExpected && !isPred;
    const isTn = !isExpected && !isPred;

    if (isTp) tp += 1;
    if (isFp) fp += 1;
    if (isFn) fn += 1;
    if (isTn) tn += 1;

    if (isExpected && isPred && predicted !== expected) polarityErrors += 1;

    const entity = {
      id: c.canonicalEntityId,
      name: c.canonicalEntityName,
    };
    const spanOk =
      !isPred ||
      validateSupportingSpan(ev.payload?.rawResponseText || "", pred.supportingSpan, {
        entity,
        attributeId: c.candidateAttribute,
        mentions: ev.payload?.mentions || [],
      }).valid;

    if (isPred && !spanOk) spanErrors += 1;

    const bindingOk = !isPred || pred?.entityBinding === "entity_bound";
    if (isPred && !bindingOk) {
      const reason = pred?.entityBinding || "OTHER";
      if (reason.includes("parent")) bindingErrors.PARENT_LEAK += 1;
      else bindingErrors.OTHER += 1;
    }

    if (isExpected && polarityBuckets[expected]) {
      if (isTp) polarityBuckets[expected].tp += 1;
      if (isFp) polarityBuckets[expected].fp += 1;
      if (isFn) polarityBuckets[expected].fn += 1;
    }

    scored.push({ caseId: c.caseId, expected, predicted, isTp, isFp, isFn, isTn, spanOk, bindingOk });

    const attr = c.candidateAttribute || "UNKNOWN";
    if (!byAttribute.has(attr)) {
      byAttribute.set(attr, { tp: 0, fp: 0, fn: 0, tn: 0, spanErrors: 0, total: 0, bindingErrors: 0 });
    }
    const row = byAttribute.get(attr);
    row.total += 1;
    if (isTp) row.tp += 1;
    if (isFp) row.fp += 1;
    if (isFn) row.fn += 1;
    if (isTn) row.tn += 1;
    if (isPred && !spanOk) row.spanErrors += 1;
    if (isPred && !bindingOk) row.bindingErrors += 1;
  }

  const precision = tp + fp > 0 ? tp / (tp + fp) : null;
  const recall = tp + fn > 0 ? tp / (tp + fn) : null;
  const f1 =
    precision != null && recall != null && precision + recall > 0
      ? (2 * precision * recall) / (precision + recall)
      : null;

  const polarityAccuracy = {};
  for (const [pol, row] of Object.entries(polarityBuckets)) {
    const total = row.tp + row.fn;
    polarityAccuracy[pol] = total > 0 ? row.tp / total : null;
  }

  const attributeResults = [...byAttribute.entries()].map(([attributeId, row]) => {
    const p = row.tp + row.fp > 0 ? row.tp / (row.tp + row.fp) : null;
    const r = row.tp + row.fn > 0 ? row.tp / (row.tp + row.fn) : null;
    const spanValidity = row.total ? 1 - row.spanErrors / row.total : null;
    const bindingErrorRate = row.total ? row.bindingErrors / row.total : null;
    const def = getAssociationAttribute(attributeId);
    const isHighRisk = HIGH_RISK_ATTRIBUTE_IDS.includes(attributeId);
    const minCases = isHighRisk ? 10 : 5;

    let status = "RESEARCH_ONLY";
    if (def?.deferred) status = "BLOCKED";
    else if (row.total < minCases) status = "RESEARCH_VALIDATED_MORE_DATA";
    else if (
      p != null &&
      p >= (isHighRisk ? 0.95 : 0.9) &&
      spanValidity != null &&
      spanValidity >= 0.95 &&
      (bindingErrorRate == null || bindingErrorRate <= 0.02)
    ) {
      status = "PRODUCTION_VALIDATED";
    } else if (row.total >= minCases) {
      status = "RESEARCH_VALIDATED_MORE_DATA";
    }

    return {
      attributeId,
      cases: row.total,
      precision: p,
      recall: r,
      spanValidity,
      bindingErrorRate,
      status,
      highRisk: isHighRisk,
    };
  });

  const bindingErrorTotal = scored.filter((s) => s.isFp || (s.isTp && !s.bindingOk)).length;

  return {
    scoredCount: scored.length,
    overall: {
      precision,
      recall,
      f1,
      falsePositiveRate: fp + tn > 0 ? fp / (fp + tn) : null,
      falseNegativeRate: fn + tp > 0 ? fn / (fn + tp) : null,
      spanValidity:
        scored.filter((s) => s.isTp || s.isFp).length > 0
          ? 1 - spanErrors / scored.filter((s) => s.isTp || s.isFp).length
          : null,
      entityBindingErrorRate: scored.length ? bindingErrorTotal / scored.length : null,
      polarityErrorRate: scored.length ? polarityErrors / scored.length : null,
    },
    polarityAccuracy,
    bindingErrors,
    attributeResults,
    productionEligible: attributeResults
      .filter((a) => a.status === "PRODUCTION_VALIDATED")
      .map((a) => a.attributeId),
    researchOnly: attributeResults
      .filter((a) => a.status !== "PRODUCTION_VALIDATED" && !getAssociationAttribute(a.attributeId)?.deferred)
      .map((a) => a.attributeId),
  };
}

export function buildAssociationHoldout(evidence = [], options = {}) {
  const fixtures = loadHardNegativeFixtures(options.hardNegativesPath);
  const allCandidates = generateHoldoutCandidates(evidence, options);
  const fixtureCases = (fixtures.cases || []).map((f) => ({
    ...f,
    humanLabelled: true,
    reviewedBy: "SINGLE_REVIEWER_GOLDEN",
    reviewedAt: f.reviewedAt || new Date().toISOString(),
    hardNegative: f.hardNegative !== false,
  }));

  const merged = [...fixtureCases];
  const fixtureIds = new Set(fixtureCases.map((c) => c.caseId));
  for (const c of allCandidates) {
    if (!fixtureIds.has(c.caseId)) merged.push(c);
  }

  const balanced = selectBalancedHoldout(merged, options);
  const { developmentSet, holdoutSet, manifest } = splitDevHoldout(balanced.cases);

  return {
    version: ASSOCIATION_HOLDOUT_VERSION,
    generatedAt: new Date().toISOString(),
    reviewMode: "SINGLE_REVIEWER_GOLDEN",
    NEW_PROVIDER_CALLS: 0,
    totalCandidates: merged.length,
    totalLabelled: merged.filter((c) => c.humanLabelled).length,
    selectedCount: balanced.cases.length,
    cases: balanced.cases,
    developmentSet,
    holdoutSet,
    manifest,
    distribution: {
      buckets: balanced.buckets,
      providers: balanced.providers,
      attributes: balanced.attributes,
      hardNegative: balanced.hardNegativeCount,
    },
    corpusShortages: balanced.corpusShortages,
  };
}

export function saveAssociationHoldout(holdout, filePath = DEFAULT_HOLDOUT_PATH) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(holdout, null, 2));
  return filePath;
}

export function loadAssociationHoldout(filePath = DEFAULT_HOLDOUT_PATH) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export { PRODUCTION_ATTRIBUTES, LABEL_TARGETS, PROVIDER_TARGETS, DEFERRED_ATTRIBUTE_IDS };
