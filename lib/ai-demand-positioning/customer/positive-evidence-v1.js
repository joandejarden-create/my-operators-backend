/**
 * Positive AI Evidence V1 — customer-safe representative examples.
 *
 * Layers:
 *   CUSTOMER_SUMMARY — KPIs only (no transcripts)
 *   CUSTOMER_EVIDENCE — governed excerpts via View AI evidence
 *   INTERNAL_AUDIT — full period JSON / prompts (never customer-facing)
 *
 * Principle: SHOW WHAT WE MEASURE / PROTECT EXACTLY HOW WE MEASURE IT
 */

import { createHash } from "crypto";
import { filterComparableObservations, isComparableObservation } from "../metrics/grain-governance.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { resolveCompetitiveEntityId } from "./canonical-presence-per-observation-v1.js";
import { buildVerbatimResponseFields } from "./verbatim-evidence-response-v1.js";

export const POSITIVE_EVIDENCE_VERSION = "adp_positive_evidence_v1";
export const POSITIVE_EVIDENCE_SELECTION_RULE = "PROVIDER_ROUND_ROBIN_STABLE_SCENARIO_ORDER_V1";
export const DEFAULT_EXAMPLE_LIMIT = 5;
export const MAX_EXAMPLE_LIMIT = 12;
/** @deprecated Truncation prohibited — full rawResponse is required (EVIDENCE_TEXT_MUST_EQUAL_CAPTURED_LLM_RESPONSE). */
export const MAX_EXCERPT_CHARS = Number.POSITIVE_INFINITY;

export const DEFECT_POSITIVE_EVIDENCE_CONTEXT_MISMATCH = "POSITIVE_EVIDENCE_CONTEXT_MISMATCH";
export const DEFECT_PROPRIETARY_PROMPT_LEAKAGE = "PROPRIETARY_PROMPT_LEAKAGE";
export const DEFECT_NON_DETERMINISTIC_EVIDENCE_SAMPLING = "NON_DETERMINISTIC_EVIDENCE_SAMPLING";

export const PROVIDER_ORDER = Object.freeze(["openai", "claude", "gemini", "perplexity"]);

const FRAME_DECISION_CONTEXT = Object.freeze({
  best_for: "Best-for recommendations",
  recommend: "Hotel recommendations",
  where_should: "Where-to-stay decisions",
  compare: "Hotel comparisons",
});

/**
 * Customer-safe decision context — NEVER the exact production query.
 */
export function buildCustomerSafeDecisionContext(scenario) {
  const frame = String(scenario?.frame || "").toLowerCase();
  const fromFrame = FRAME_DECISION_CONTEXT[frame];
  if (fromFrame) return fromFrame;
  if (scenario?.label && !looksLikeProductionPrompt(scenario.label)) return String(scenario.label);
  return "Monitored traveler need";
}

export function looksLikeProductionPrompt(text) {
  const t = String(text || "");
  if (t.length > 120) return true;
  if (/^(best|recommend|where should|compare)\b/i.test(t) && t.split(/\s+/).length >= 8) return true;
  return false;
}

function subjectPresent(obs) {
  if (obs?.governedInterpretation?.subjectMentioned != null) {
    return Boolean(obs.governedInterpretation.subjectMentioned);
  }
  return Boolean(obs?.mentioned);
}

function subjectRank(obs) {
  const g = obs?.governedInterpretation;
  if (g && g.subjectMentioned) {
    return {
      rank: g.subjectRank ?? obs.position ?? null,
      rankEligible: Boolean(g.rankEligible),
    };
  }
  if (obs?.mentioned) {
    return { rank: obs.position ?? null, rankEligible: obs.position != null };
  }
  return { rank: null, rankEligible: false };
}

function evidenceToken(obs) {
  return createHash("sha256")
    .update(String(obs.observationId || `${obs.scenarioId}|${obs.provider}|${obs.timestamp || ""}`))
    .digest("hex")
    .slice(0, 16);
}

/**
 * Deterministic representative selection:
 * 1) Stable sort by scenarioId, observationId
 * 2) Bucket by provider
 * 3) Round-robin across PROVIDER_ORDER until limit
 *
 * Does not cherry-pick flattering subject ranks.
 */
export function selectRepresentativeObservations(observations, limit = DEFAULT_EXAMPLE_LIMIT) {
  const cap = Math.min(MAX_EXAMPLE_LIMIT, Math.max(1, Number(limit) || DEFAULT_EXAMPLE_LIMIT));
  const sorted = [...(observations || [])].sort((a, b) => {
    const s = String(a.scenarioId || "").localeCompare(String(b.scenarioId || ""));
    if (s) return s;
    return String(a.observationId || "").localeCompare(String(b.observationId || ""));
  });

  const buckets = Object.create(null);
  for (const p of PROVIDER_ORDER) buckets[p] = [];
  for (const obs of sorted) {
    const p = obs.provider || "unknown";
    if (!buckets[p]) buckets[p] = [];
    buckets[p].push(obs);
  }

  const selected = [];
  let guard = 0;
  while (selected.length < cap && guard < 1000) {
    guard += 1;
    let progressed = false;
    for (const p of PROVIDER_ORDER) {
      if (selected.length >= cap) break;
      if (buckets[p]?.length) {
        selected.push(buckets[p].shift());
        progressed = true;
      }
    }
    // leftover unknown providers
    for (const [p, list] of Object.entries(buckets)) {
      if (PROVIDER_ORDER.includes(p)) continue;
      if (selected.length >= cap) break;
      if (list.length) {
        selected.push(list.shift());
        progressed = true;
      }
    }
    if (!progressed) break;
  }
  return selected;
}

export function toCustomerPositiveEvidenceCard(obs, scenario, propertyProfile) {
  const rankInfo = subjectRank(obs);
  const territory = territoryLabelForIntent(scenario?.intent) || scenario?.intent || "—";
  const competitors = [];
  const seen = new Set();
  for (const name of obs.competitorsMentioned || []) {
    const id = resolveCompetitiveEntityId(name, propertyProfile);
    const key = id || String(name).toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    competitors.push({
      name: typeof name === "string" ? name : String(name),
      entityId: id || null,
    });
    if (competitors.length >= 6) break;
  }

  return {
    evidenceId: evidenceToken(obs),
    // Internal linkage for assurance only — stripped from customer API responses by sanitize
    _observationId: obs.observationId || null,
    _scenarioId: obs.scenarioId || null,
    demandTerritory: territory,
    demandTerritoryIntent: scenario?.intent || null,
    decisionContext: buildCustomerSafeDecisionContext(scenario),
    provider: obs.provider,
    subjectAppeared: true,
    rank: rankInfo.rank,
    rankEligible: rankInfo.rankEligible,
    ...buildVerbatimResponseFields(obs, propertyProfile, { highlightSubject: true }),
    competitorsAlongside: competitors,
    sourcesCited: (obs.sourcesCited || []).slice(0, 3).map((s) => ({
      url: s.url || "",
      title: s.title || s.label || "",
    })),
    periodId: obs.periodId || null,
  };
}

/** Strip internal linkage fields before sending to the browser. */
export function sanitizeCustomerEvidenceCard(card) {
  if (!card) return card;
  const { _observationId, _scenarioId, ...rest } = card;
  return rest;
}

export function auditCardForPromptLeakage(card, scenariosById = {}) {
  const defects = [];
  const textBlob = JSON.stringify(card);
  if (/scenarioId|promptId|systemInstruction|canonical.?prompt/i.test(textBlob) && card.scenarioId) {
    defects.push({ code: DEFECT_PROPRIETARY_PROMPT_LEAKAGE, detail: "scenarioId exposed on customer card" });
  }
  for (const sc of Object.values(scenariosById)) {
    const q = String(sc?.query || "");
    if (q.length >= 24 && textBlob.includes(q)) {
      defects.push({
        code: DEFECT_PROPRIETARY_PROMPT_LEAKAGE,
        detail: "exact production query appears in customer evidence payload",
      });
    }
  }
  if (looksLikeProductionPrompt(card.decisionContext) && card.decisionContext?.split(/\s+/).length >= 10) {
    defects.push({
      code: DEFECT_PROPRIETARY_PROMPT_LEAKAGE,
      detail: "decisionContext looks like a production prompt",
    });
  }
  return defects;
}

/**
 * Filter observations for a positive-evidence context.
 */
export function filterPositiveEvidencePool({
  period,
  scenarios,
  propertyProfile,
  intent = null,
  provider = null,
  competitorEntityId = null,
  competitorName = null,
}) {
  const scenarioMap = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  let pool = filterComparableObservations(period?.observations || []).filter((o) => subjectPresent(o));

  if (intent && intent !== "overall") {
    const ids = new Set(
      (scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId)
    );
    pool = pool.filter((o) => ids.has(o.scenarioId));
  }
  if (provider) {
    pool = pool.filter((o) => o.provider === provider);
  }
  if (competitorEntityId || competitorName) {
    pool = pool.filter((o) => {
      for (const name of o.competitorsMentioned || []) {
        const id = resolveCompetitiveEntityId(name, propertyProfile);
        if (competitorEntityId && id === competitorEntityId) return true;
        if (
          competitorName &&
          String(name).toLowerCase().includes(String(competitorName).toLowerCase())
        ) {
          return true;
        }
      }
      return false;
    });
  }

  return { pool, scenarioMap };
}

/**
 * Build customer-safe positive evidence response for one context.
 */
export function buildPositiveEvidenceResponse({
  period,
  scenarios,
  propertyProfile,
  intent = null,
  provider = null,
  competitorEntityId = null,
  competitorName = null,
  limit = DEFAULT_EXAMPLE_LIMIT,
  offset = 0,
}) {
  const { pool, scenarioMap } = filterPositiveEvidencePool({
    period,
    scenarios,
    propertyProfile,
    intent,
    provider,
    competitorEntityId,
    competitorName,
  });

  const selected = selectRepresentativeObservations(pool, Math.min(MAX_EXAMPLE_LIMIT, (offset || 0) + (limit || DEFAULT_EXAMPLE_LIMIT)));
  const slice = selected.slice(offset || 0, (offset || 0) + (limit || DEFAULT_EXAMPLE_LIMIT));

  const cards = slice.map((obs) => {
    const sc = scenarioMap[obs.scenarioId] || { intent, scenarioId: obs.scenarioId };
    return toCustomerPositiveEvidenceCard(obs, sc, propertyProfile);
  });

  const leakage = cards.flatMap((c) => auditCardForPromptLeakage(c, scenarioMap));
  const customerCards = cards.map(sanitizeCustomerEvidenceCard);

  return {
    ok: true,
    version: POSITIVE_EVIDENCE_VERSION,
    selectionRule: POSITIVE_EVIDENCE_SELECTION_RULE,
    label: "Representative governed examples from this monitoring period",
    propertyId: period?.propertyId || null,
    periodId: period?.periodId || null,
    context: {
      intent: intent || null,
      demandTerritory: intent && intent !== "overall" ? territoryLabelForIntent(intent) : intent === "overall" ? "Overall" : null,
      provider: provider || null,
      competitorEntityId: competitorEntityId || null,
      competitorName: competitorName || null,
      polarity: "WHERE_YOU_APPEARED",
    },
    totalQualifying: pool.length,
    returned: customerCards.length,
    hasMore: pool.length > (offset || 0) + customerCards.length,
    evidence: customerCards,
    // Internal assurance payload (API must strip before customer JSON)
    _assuranceCards: cards,
    _leakageDefects: leakage,
  };
}

/**
 * Extend published evidence index with positive-presence buckets (bounded).
 */
export function buildPositiveEvidenceIndexSections(period, scenarios, propertyProfile) {
  const scenarioMap = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  const present = filterComparableObservations(period?.observations || []).filter(subjectPresent);

  const presentByIntent = {};
  const presentByProvider = {};

  for (const obs of present) {
    const sc = scenarioMap[obs.scenarioId];
    const intent = sc?.intent || "unknown";
    if (!presentByIntent[intent]) presentByIntent[intent] = [];
    presentByIntent[intent].push(obs);

    const p = obs.provider || "unknown";
    if (!presentByProvider[p]) presentByProvider[p] = [];
    presentByProvider[p].push(obs);
  }

  const toCards = (list) =>
    selectRepresentativeObservations(list, DEFAULT_EXAMPLE_LIMIT).map((obs) =>
      sanitizeCustomerEvidenceCard(
        toCustomerPositiveEvidenceCard(obs, scenarioMap[obs.scenarioId] || {}, propertyProfile)
      )
    );

  const byIntent = {};
  for (const [intent, list] of Object.entries(presentByIntent)) {
    byIntent[intent] = {
      total: list.length,
      examples: toCards(list),
    };
  }
  const byProvider = {};
  for (const [provider, list] of Object.entries(presentByProvider)) {
    byProvider[provider] = {
      total: list.length,
      examples: toCards(list),
    };
  }

  return {
    version: POSITIVE_EVIDENCE_VERSION,
    selectionRule: POSITIVE_EVIDENCE_SELECTION_RULE,
    presentByIntent: byIntent,
    presentByProvider: byProvider,
  };
}

export { isComparableObservation, subjectPresent as isGovernedSubjectPresent };
