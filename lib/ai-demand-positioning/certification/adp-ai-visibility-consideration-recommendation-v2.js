/**
 * ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2
 *
 * Shadow methodology: separates AI Visibility (mention), Positive Consideration,
 * and Explicit Recommendation. Does NOT activate customer-facing KPI replacement
 * until founder approval.
 *
 * Invariant (same eligible universe):
 *   Explicit Recommendation ⊆ Positive Consideration ⊆ AI Visibility
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import {
  adjudicateSubjectStance,
  SUBJECT_STANCE,
  ADP_SUBJECT_STANCE_ADJUDICATION_V1,
  SUBJECT_STANCE_VERSION,
} from "./adp-subject-stance-adjudication-v1.js";

export const ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2 =
  "ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2";
export const THREE_METRIC_SHADOW_VERSION = "adp_three_metric_shadow_v2";
export const ADJUDICATOR_VERSION = "adp_subject_stance_v1.2_explicit_rec_fix";
export const CUSTOMER_ACTIVATION = "SHADOW_ONLY_PENDING_FOUNDER_APPROVAL";

export const PROPOSED_CUSTOMER_LABELS = Object.freeze({
  aiVisibility: "AI Visibility",
  positiveConsideration: "Positive Consideration",
  explicitRecommendation: "AI Recommendations",
});

/** Strip markdown emphasis so "**Recommendation:**" still classifies. */
export function stripMarkdownForStance(text) {
  return String(text || "")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/__([^_]+)__/g, "$1")
    .replace(/_([^_]+)_/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/^#{1,6}\s+/gm, "");
}

function escapeRegExp(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function subjectNames(profile) {
  const names = new Set();
  const push = (n) => {
    const t = String(n || "").trim();
    if (t.length >= 3) names.add(t);
  };
  push(profile?.name);
  push(profile?.displayName);
  for (const a of profile?.identityAliases || []) push(a);
  return [...names];
}

/**
 * Direct semantic recommendation patterns (subject-anchored when possible).
 * Intentional: "clear and ideal choice is SUBJECT" qualifies even without "recommend".
 */
export function detectExplicitRecommendation(rawResponse, profile) {
  const raw = stripMarkdownForStance(rawResponse);
  const names = subjectNames(profile);
  const matched = [];

  // Explicit recommendation requires subject anchoring (no bare global "book"/"top pick").
  for (const name of names) {
    const n = escapeRegExp(name);
    const anchored = [
      new RegExp(`recommendation\\s*:\\s*${n}`, "i"),
      new RegExp(
        `(?:clear(?:\\s+and\\s+ideal)?|ideal|best|top|strong(?:est)?|short)\\s+(?:answer|choice|option|pick)\\s+is\\s+${n}`,
        "i"
      ),
      new RegExp(`(?:the\\s+)?(?:clear\\s+and\\s+ideal|ideal|clear)\\s+choice\\s+is\\s+${n}`, "i"),
      new RegExp(
        `i\\s+(?:would\\s+)?(?:strongly\\s+)?recommend\\s+${n}`,
        "i"
      ),
      new RegExp(`(?:highly\\s+)?recommend(?:ing)?\\s+${n}`, "i"),
      new RegExp(`(?:book|choose|stay\\s+at)\\s+${n}`, "i"),
      // Direct recommendation / first-choice forms only — "strong/fantastic choice" stays Positive Consideration.
      new RegExp(`${n}\\s+is\\s+one\\s+of\\s+the\\s+best\\s+choices?`, "i"),
      new RegExp(`${n}\\s+is\\s+the\\s+(?:clear|best|ideal|sole|only|top)\\s+choice`, "i"),
      new RegExp(`my\\s+(?:top\\s+)?recommendation\\s+is\\s+${n}`, "i"),
      new RegExp(`my\\s+top\\s+(?:pick|choice)\\s+is\\s+${n}`, "i"),
      new RegExp(`strongest\\s+[\\w\\s-]{0,60}choice\\s+is\\s+${n}`, "i"),
      new RegExp(`(?:short|clear)\\s+answer\\s+is\\s*:?\\s*${n}`, "i"),
    ];
    for (const re of anchored) {
      if (re.test(raw)) matched.push(re.source);
    }
  }

  // Negation / uncertainty vetoes for explicit recommendation
  const veto = [
    /\bcan(?:not|'t)\s+(?:confidently\s+)?recommend\b/i,
    /\bunable\s+to\s+recommend\b/i,
    /\bi\s+(?:would\s+)?not\s+recommend\b/i,
    /\bdo\s+not\s+recommend\b/i,
    /\bcan(?:not|'t)\s+confirm\b/i,
    /\bunable\s+to\s+(?:confirm|verify)\b/i,
  ];
  const vetoed = veto.some((re) => re.test(raw));
  const notSubject = names.some((n) =>
    new RegExp(`(?:\\bnot\\b\\s+${escapeRegExp(n)})|(?:recommend[\\s\\S]{0,80}\\bnot\\b\\s+${escapeRegExp(n)})`, "i").test(
      raw
    )
  );

  return {
    explicitRecommendation: matched.length > 0 && !vetoed && !notSubject,
    matchedPatterns: matched.slice(0, 8),
    vetoed: vetoed || notSubject,
  };
}

/**
 * Full V2 classification for one observation.
 */
export function classifyObservationThreeMetricV2(obs, propertyProfile = {}) {
  const stripped = stripMarkdownForStance(obs?.rawResponse || "");
  const base = adjudicateSubjectStance(
    { ...obs, rawResponse: stripped || obs?.rawResponse },
    propertyProfile
  );
  const explicitDetect = detectExplicitRecommendation(obs?.rawResponse || "", propertyProfile);

  let stance = base.stance;
  const reasons = [...(base.reasons || [])];
  const matchedPatterns = [...(base.matchedPatterns || [])];

  // Elevate positive consideration → explicit recommendation when semantic patterns fire
  if (
    explicitDetect.explicitRecommendation &&
    stance !== SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED &&
    stance !== SUBJECT_STANCE.MENTIONED_WITH_UNCERTAINTY &&
    stance !== SUBJECT_STANCE.ABSENT
  ) {
    stance = SUBJECT_STANCE.EXPLICITLY_RECOMMENDED;
    reasons.push("EXPLICIT_RECOMMENDATION_SEMANTIC_V2");
    matchedPatterns.push(...explicitDetect.matchedPatterns);
  } else if (
    explicitDetect.explicitRecommendation &&
    (stance === SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED ||
      stance === SUBJECT_STANCE.MENTIONED_WITH_UNCERTAINTY)
  ) {
    reasons.push("EXPLICIT_REC_PATTERN_VETOED_BY_UNCERTAINTY_OR_REFUSAL");
  }

  const aiVisibility = Boolean(base.entityMentioned);
  const explicitRecommendation = stance === SUBJECT_STANCE.EXPLICITLY_RECOMMENDED;
  const positiveConsideration =
    explicitRecommendation || stance === SUBJECT_STANCE.POSITIVELY_CONSIDERED;

  // Enforce subset mathematically from classification flags (not by forcing counts)
  if (explicitRecommendation && !positiveConsideration) {
    throw new Error("INVARIANT_VIOLATION: explicit recommendation without positive consideration");
  }
  if (positiveConsideration && !aiVisibility) {
    throw new Error("INVARIANT_VIOLATION: positive consideration without visibility");
  }

  return {
    contract: ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
    adjudicatorVersion: ADJUDICATOR_VERSION,
    stanceContract: ADP_SUBJECT_STANCE_ADJUDICATION_V1,
    stanceVersion: SUBJECT_STANCE_VERSION,
    entityMentioned: aiVisibility,
    stance,
    customerLabel: base.customerLabel,
    aiVisibility,
    positiveConsideration,
    explicitRecommendation,
    promptNamedSubject: base.promptNamedSubject,
    independentDiscoveryCredit: base.independentDiscoveryCredit,
    reasons,
    matchedPatterns: [...new Set(matchedPatterns)].slice(0, 12),
    confidence:
      stance === SUBJECT_STANCE.NEUTRALLY_MENTIONED && aiVisibility
        ? "MEDIUM"
        : explicitRecommendation || stance === SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED
          ? "HIGH"
          : "MEDIUM_HIGH",
    ambiguous:
      aiVisibility &&
      stance === SUBJECT_STANCE.NEUTRALLY_MENTIONED &&
      /suitable|option|consider|recommend|choice/i.test(String(obs?.rawResponse || "")),
  };
}

/**
 * Compute three rates over the same comparable observation denominator.
 */
export function computeThreeMetricRatesV2(observations, propertyProfile = {}) {
  const comparable = filterComparableObservations(observations || []);
  let visibilityN = 0;
  let positiveN = 0;
  let recommendN = 0;
  let uncertainN = 0;
  let notRecommendedN = 0;
  let neutralN = 0;
  let ambiguousN = 0;
  const classified = [];

  for (const obs of comparable) {
    const c = classifyObservationThreeMetricV2(obs, propertyProfile);
    classified.push({ observationId: obs.observationId, ...c });
    if (c.aiVisibility) visibilityN += 1;
    if (c.positiveConsideration) positiveN += 1;
    if (c.explicitRecommendation) recommendN += 1;
    if (c.stance === SUBJECT_STANCE.MENTIONED_WITH_UNCERTAINTY) uncertainN += 1;
    if (c.stance === SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED) notRecommendedN += 1;
    if (c.stance === SUBJECT_STANCE.NEUTRALLY_MENTIONED) neutralN += 1;
    if (c.ambiguous) ambiguousN += 1;
  }

  const den = comparable.length;
  const rate = (n) => (den > 0 ? roundAdpPercent((n / den) * 100) : null);

  const visibilityRate = rate(visibilityN);
  const positiveConsiderationRate = rate(positiveN);
  const explicitRecommendationRate = rate(recommendN);

  const invariantOk =
    recommendN <= positiveN &&
    positiveN <= visibilityN &&
    (explicitRecommendationRate == null ||
      positiveConsiderationRate == null ||
      visibilityRate == null ||
      (explicitRecommendationRate <= positiveConsiderationRate &&
        positiveConsiderationRate <= visibilityRate));

  return {
    contract: ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
    version: THREE_METRIC_SHADOW_VERSION,
    customerActivation: CUSTOMER_ACTIVATION,
    denominator: den,
    counts: {
      aiVisibility: visibilityN,
      positiveConsideration: positiveN,
      explicitRecommendation: recommendN,
      uncertainMentions: uncertainN,
      explicitlyNotRecommended: notRecommendedN,
      neutrallyMentioned: neutralN,
      ambiguous: ambiguousN,
    },
    rates: {
      aiVisibility: visibilityRate,
      positiveConsideration: positiveConsiderationRate,
      explicitRecommendation: explicitRecommendationRate,
    },
    proposedLabels: PROPOSED_CUSTOMER_LABELS,
    invariant: {
      ok: invariantOk,
      rule: "explicitRecommendationRate <= positiveConsiderationRate <= aiVisibilityRate",
    },
    classified,
  };
}

/**
 * Competitor stance diagnostic (mention windows only; does not change displacement KPI).
 */
export function classifyCompetitorStanceSnippet(snippet, competitorName) {
  const text = stripMarkdownForStance(snippet);
  const name = String(competitorName || "").trim();
  if (!name || !new RegExp(escapeRegExp(name), "i").test(text)) {
    return { mentioned: false, positivelyConsidered: false, explicitlyRecommended: false };
  }
  const profile = { name, identityAliases: [name] };
  const fakeObs = { mentioned: true, rawResponse: text };
  const c = classifyObservationThreeMetricV2(fakeObs, profile);
  return {
    mentioned: c.aiVisibility,
    positivelyConsidered: c.positiveConsideration,
    explicitlyRecommended: c.explicitRecommendation,
    stance: c.stance,
  };
}

export function getThreeMetricNegativeFixturesV2() {
  const profile = {
    name: "UMusic Hotel Madrid",
    brand: "UMusic Hotels",
    identityAliases: ["UMusic Hotel Madrid", "UMusic Madrid"],
  };
  return [
    {
      id: "GEMINI_CLEAR_IDEAL_CHOICE",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "For a corporate executive visit to Madrid, the clear and ideal choice is **UMusic Hotel Madrid**. As the flagship property.",
      },
      expect: {
        aiVisibility: true,
        positiveConsideration: true,
        explicitRecommendation: true,
        stance: SUBJECT_STANCE.EXPLICITLY_RECOMMENDED,
      },
    },
    {
      id: "PERPLEXITY_RECOMMENDATION_HEADER",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "**Recommendation: UMusic Hotel Madrid** — a strong choice for a corporate executive visit because it combines five-star service.",
      },
      expect: {
        aiVisibility: true,
        positiveConsideration: true,
        explicitRecommendation: true,
        stance: SUBJECT_STANCE.EXPLICITLY_RECOMMENDED,
      },
    },
    {
      id: "A_UNVERIFIED",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I cannot confirm that UMusic Hotel Madrid exists. Please verify before planning.",
      },
      expect: {
        aiVisibility: true,
        positiveConsideration: false,
        explicitRecommendation: false,
      },
    },
    {
      id: "B_REFUSAL",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I cannot confidently recommend UMusic Hotel Madrid because its existence is unverified.",
      },
      expect: {
        aiVisibility: true,
        positiveConsideration: false,
        explicitRecommendation: false,
      },
    },
    {
      id: "C_COMPETITOR",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I cannot verify UMusic Hotel Madrid. Consider Four Seasons Hotel Madrid instead.",
      },
      expect: {
        positiveConsideration: false,
        explicitRecommendation: false,
      },
    },
    {
      id: "D_I_RECOMMEND",
      profile,
      obs: {
        mentioned: true,
        rawResponse: "I recommend UMusic Hotel Madrid for music-forward luxury near Sol.",
      },
      expect: {
        explicitRecommendation: true,
        positiveConsideration: true,
        aiVisibility: true,
      },
    },
    {
      id: "E_PROMPT_ECHO",
      profile,
      obs: {
        mentioned: true,
        scenarioQuery: "Is UMusic Hotel Madrid a good boutique stay?",
        rawResponse:
          "Regarding UMusic Hotel Madrid: it is a hotel name that appears in your question. Here are general Madrid tips.",
      },
      expect: {
        aiVisibility: true,
        positiveConsideration: false,
        explicitRecommendation: false,
      },
    },
    {
      id: "G_NEGATION",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I recommend Four Seasons Hotel Madrid, not UMusic Hotel Madrid, for verified luxury.",
      },
      expect: {
        explicitRecommendation: false,
        positiveConsideration: false,
      },
    },
  ];
}

export function runThreeMetricNegativeFixturesV2() {
  const fixtures = getThreeMetricNegativeFixturesV2();
  const results = fixtures.map((fx) => {
    const got = classifyObservationThreeMetricV2(fx.obs, fx.profile);
    const failures = [];
    for (const [k, v] of Object.entries(fx.expect || {})) {
      if (got[k] !== v) failures.push(`${k}: expected ${v}, got ${got[k]}`);
    }
    return {
      id: fx.id,
      pass: failures.length === 0,
      failures,
      got: {
        stance: got.stance,
        aiVisibility: got.aiVisibility,
        positiveConsideration: got.positiveConsideration,
        explicitRecommendation: got.explicitRecommendation,
      },
    };
  });
  return {
    contract: ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
    ok: results.every((r) => r.pass),
    results,
  };
}
