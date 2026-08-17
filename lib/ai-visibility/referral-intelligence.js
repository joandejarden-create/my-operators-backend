/**
 * Referral Intelligence — definition, classification, metrics (Phase 3C.1).
 * No LLM guess. No mislabeling Google organic as Gemini.
 */

export const REFERRAL_INTELLIGENCE_VERSION = "ai_visibility_referral_intelligence_v1";

export const REFERRAL_DEFINITION = Object.freeze({
  question:
    "Are AI-originated or AI-associated sources sending measurable visits to Brand development content?",
  SOURCES: ["GA4", "Adobe Analytics", "Matomo", "server analytics", "other governed analytics"],
  GA4_ONLY: false,
});

export const REFERRAL_CLASSIFICATION = Object.freeze({
  DIRECT_AI_REFERRAL: "DIRECT_AI_REFERRAL",
  AI_ASSOCIATED_UNCERTAIN: "AI_ASSOCIATED_UNCERTAIN",
  NON_AI: "NON_AI",
  UNKNOWN: "UNKNOWN",
});

/** Deterministic referrer domain → provider mapping. */
export const AI_REFERRER_REGISTRY = Object.freeze([
  { referrerDomain: "chatgpt.com", provider: "openai", classification: REFERRAL_CLASSIFICATION.DIRECT_AI_REFERRAL, evidence: "documented_chatgpt_referrer" },
  { referrerDomain: "chat.openai.com", provider: "openai", classification: REFERRAL_CLASSIFICATION.DIRECT_AI_REFERRAL, evidence: "documented_openai_chat_referrer" },
  { referrerDomain: "perplexity.ai", provider: "perplexity", classification: REFERRAL_CLASSIFICATION.DIRECT_AI_REFERRAL, evidence: "documented_perplexity_referrer" },
  { referrerDomain: "claude.ai", provider: "anthropic", classification: REFERRAL_CLASSIFICATION.DIRECT_AI_REFERRAL, evidence: "documented_claude_referrer" },
  { referrerDomain: "gemini.google.com", provider: "google", classification: REFERRAL_CLASSIFICATION.DIRECT_AI_REFERRAL, evidence: "documented_gemini_surface_referrer" },
  { referrerDomain: "google.com", provider: "google", classification: REFERRAL_CLASSIFICATION.NON_AI, evidence: "standard_organic_search_not_gemini_specific" },
  { referrerDomain: "www.google.com", provider: "google", classification: REFERRAL_CLASSIFICATION.NON_AI, evidence: "standard_organic_search_not_gemini_specific" },
]);

export const REFERRAL_METRICS_V1 = Object.freeze([
  { id: "ai_referral_sessions", label: "AI Referral Sessions" },
  { id: "ai_referral_users", label: "AI Referral Users", optional: true },
  { id: "development_page_visits_from_ai", label: "Development Page Visits From AI" },
  { id: "engaged_ai_referral_sessions", label: "Engaged AI Referral Sessions" },
  { id: "ai_referral_engagement_rate", label: "AI Referral Engagement Rate", optional: true },
]);

export const PROVIDER_REFERRAL_CAPABILITY = Object.freeze({
  openai: { distinctReferrer: true, status: "DOCUMENTED", domains: ["chatgpt.com", "chat.openai.com"] },
  perplexity: { distinctReferrer: true, status: "DOCUMENTED", domains: ["perplexity.ai"] },
  gemini: { distinctReferrer: "PARTIAL", status: "PARTIAL", note: "gemini.google.com distinguishable; google.com organic is NOT Gemini" },
  claude: { distinctReferrer: true, status: "DOCUMENTED", domains: ["claude.ai"] },
});

/**
 * Classify a referral row deterministically.
 * @param {{ referrer?: string, source?: string, medium?: string }} row
 */
export function classifyAiReferrer(row = {}) {
  const ref = String(row.referrer || row.referrerDomain || row.source || "").toLowerCase();
  let domain = ref;
  try {
    if (ref.includes("://")) domain = new URL(ref).hostname.toLowerCase();
    else if (ref.includes("/")) domain = ref.split("/")[0].toLowerCase();
  } catch {
    domain = ref.replace(/^www\./, "");
  }
  domain = domain.replace(/^www\./, "");

  for (const entry of AI_REFERRER_REGISTRY) {
    if (domain === entry.referrerDomain || domain.endsWith(`.${entry.referrerDomain}`)) {
      return {
        referrerDomain: domain,
        provider: entry.provider,
        classification: entry.classification,
        source: row.source || null,
        medium: row.medium || null,
        evidence: entry.evidence,
      };
    }
  }

  if (/chatgpt|openai|perplexity|claude|gemini|anthropic/.test(domain)) {
    return {
      referrerDomain: domain,
      provider: null,
      classification: REFERRAL_CLASSIFICATION.AI_ASSOCIATED_UNCERTAIN,
      source: row.source || null,
      medium: row.medium || null,
      evidence: "partial_domain_match_requires_review",
    };
  }

  if (!domain) {
    return {
      referrerDomain: null,
      provider: null,
      classification: REFERRAL_CLASSIFICATION.UNKNOWN,
      evidence: "missing_referrer",
    };
  }

  return {
    referrerDomain: domain,
    provider: null,
    classification: REFERRAL_CLASSIFICATION.NON_AI,
    source: row.source || null,
    medium: row.medium || null,
    evidence: "no_ai_referrer_match",
  };
}
