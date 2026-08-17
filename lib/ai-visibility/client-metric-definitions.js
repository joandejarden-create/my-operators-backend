/**
 * Client-facing one-line metric definitions (Brand AI Visibility V1).
 * No large help center — minimum glossary only.
 */

export const CLIENT_METRIC_DEFINITIONS_VERSION =
  "ai_visibility_client_metric_definitions_v1";

export const CLIENT_METRIC_DEFINITIONS = Object.freeze({
  AI_PRESENCE: {
    id: "AI_PRESENCE",
    label: "AI Presence",
    oneLiner:
      "Share of monitored owner questions where the brand appeared in the AI answer.",
  },
  PORTFOLIO_AI_PRESENCE: {
    id: "PORTFOLIO_AI_PRESENCE",
    label: "Portfolio AI Presence",
    oneLiner:
      "Share of monitored owner questions where at least one of your linked brands appeared.",
    allProvidersOneLiner:
      "Mean of each linked brand’s cross-provider average Presence — not a single combined AI run.",
  },
  COMPETITIVE_POSITION: {
    id: "COMPETITIVE_POSITION",
    label: "Competitive Position",
    oneLiner:
      "Rank by AI Presence among peers in the comparable monitored cohort (#1 = highest Presence).",
  },
  QUESTIONS_MISSING: {
    id: "QUESTIONS_MISSING",
    label: "Questions Missing",
    oneLiner:
      "Monitored owner questions where the brand was not observed in the answer.",
    allProvidersOneLiner:
      "Comparable owner questions where the brand was missing across every monitored provider.",
    allProvidersPortfolioOneLiner:
      "Comparable owner questions where none of your linked brands appeared on any monitored provider.",
  },
  ALL_PROVIDERS: {
    id: "ALL_PROVIDERS",
    label: "All Providers",
    oneLiner:
      "Derived comparison across monitored providers — not a single combined AI run.",
  },
  CITATION_RATE: {
    id: "CITATION_RATE",
    label: "Citation Rate",
    oneLiner:
      "Share of comparable monitored responses that include at least one citation.",
  },
  OWNED_SOURCE_CITATION_RATE: {
    id: "OWNED_SOURCE_CITATION_RATE",
    label: "Owned Source Citation Rate",
    oneLiner:
      "Share of comparable monitored responses that cite at least one governed brand/company domain.",
  },
  CITED_SOURCE: {
    id: "CITED_SOURCE",
    label: "Cited Source",
    oneLiner:
      "A source explicitly returned or cited by the provider in the observed response.",
  },
  ASSOCIATED_SOURCE: {
    id: "ASSOCIATED_SOURCE",
    label: "Associated Source",
    oneLiner:
      "A source captured in response or search evidence but not necessarily cited in the final answer. Association is not influence.",
  },
  AI_DISCOVERABILITY: {
    id: "AI_DISCOVERABILITY",
    label: "Public Discoverability",
    oneLiner:
      "Whether relevant official public information exists and appears retrievable — separate from AI Presence. Not a composite score.",
  },
  PUBLIC_DISCOVERABILITY: {
    id: "PUBLIC_DISCOVERABILITY",
    label: "Public Discoverability",
    oneLiner:
      "Factual public-URL baseline: configured official sources, accessibility, and owner/development content presence.",
  },
  COMPARABLE_TREND: {
    id: "COMPARABLE_TREND",
    label: "Comparable Trend",
    oneLiner:
      "Change in Presence only when prior and current monitoring periods share compatible provider, language, geography, and methodology.",
  },
});

export const PRIMARY_PORTFOLIO_KPI_LABEL = "Portfolio AI Presence";

export const PRIMARY_PORTFOLIO_KPI_RECOMMENDATION = Object.freeze({
  clientLabel: PRIMARY_PORTFOLIO_KPI_LABEL,
  alternateAcceptable: "AI Presence",
  deprecateAsPrimaryHeadline: "Decision Visibility Coverage",
  reason:
    "First executive question should be understandable without training: How visible are our brands in AI?",
});

export function listClientMetricDefinitions() {
  return Object.values(CLIENT_METRIC_DEFINITIONS);
}
