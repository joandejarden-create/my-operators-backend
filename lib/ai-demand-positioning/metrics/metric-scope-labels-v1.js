/**
 * Governed metric-scope labels — prevent competitor Scenario Appearance
 * from being mistaken for subject AI Consideration.
 */

export const ADP_METRIC_SCOPE_LABELS_V1 = Object.freeze({
  version: "adp_metric_scope_labels_v1",
  subjectObservationGrain: {
    productLabel: "AI Consideration",
    technicalLabel: "AI Consideration Rate",
    grain: "property × scenario × provider × period (comparable observations)",
    definition:
      "Share of comparable monitored AI responses where the subject hotel appears.",
  },
  subjectScenarioGrain: {
    productLabel: "AI Scenario Presence",
    technicalLabel: "Demand Scenario Coverage",
    grain: "property × scenario (aggregates providers)",
    definition:
      "Share of monitored demand scenarios where the subject hotel appears on at least one comparable provider.",
  },
  competitorScenarioAppearance: {
    productLabel: "Scenario Appearance",
    technicalLabel: "Competitor Scenario Appearance",
    grain: "non-subject hotel × scenario (within subject hotel report)",
    definition:
      "Share of monitored scenarios in this report where a competitor hotel appears. Not the same metric as that hotel's own AI Consideration.",
    forbiddenClientLabels: Object.freeze(["Presence", "AI Presence"]),
  },
  competitorObservationAppearance: {
    productLabel: "Observation Appearance",
    technicalLabel: "Competitor Observation Appearance",
    grain: "non-subject hotel × observation (within subject hotel report)",
    definition:
      "Share of comparable monitored answers in this report where a competitor hotel appears. Not interchangeable with subject AI Consideration unless same-scope certified.",
  },
  scopeRule: "SAME_SCOPE_SAME_CANONICAL_METRIC",
  ambiguityRule: "ADP_METRIC_SCOPE_UNAMBIGUOUS",
});

export function attachMetricScopeLabels(payload) {
  if (!payload || typeof payload !== "object") return payload;
  return {
    ...payload,
    metricScopeLabels: ADP_METRIC_SCOPE_LABELS_V1,
  };
}

/** Structural UI/copy check used by client-readiness audit. */
export function auditMetricScopeUnambiguous(uiSources = []) {
  const ambiguous = [];
  for (const src of uiSources) {
    const text = String(src?.text || "");
    const context = String(src?.context || "");
    // Competitor tables must not use bare "Presence" / "AI Presence" without Scenario Appearance label
    if (
      /competitor|competitive.?overview|competitive.?set|ranking/i.test(context) &&
      /\bAI\s*Presence\b|\bPresence\b/.test(text) &&
      !/Scenario Appearance|AI Consideration|Observation Appearance/i.test(text)
    ) {
      ambiguous.push({
        file: src.file || null,
        context,
        text: text.slice(0, 120),
        reason: "Competitor surface uses ambiguous Presence label",
      });
    }
  }
  return {
    gate: "ADP_METRIC_SCOPE_UNAMBIGUOUS",
    pass: ambiguous.length === 0,
    ambiguousCount: ambiguous.length,
    samples: ambiguous.slice(0, 20),
    labels: ADP_METRIC_SCOPE_LABELS_V1,
  };
}
