/**
 * OPERATOR_SIGNAL_PRESENCE — deterministic, evidence-first.
 * Does not use Recommendation metrics. Brand extractMentions is not mutated.
 */

import { findOperatorSpans } from "./aliases.js";
import { isPrimaryMonitoredOperator } from "./universe.js";

export const OPERATOR_SIGNAL_PRESENCE = "OPERATOR_SIGNAL_PRESENCE";
export const OPERATOR_PRESENCE_CLASSIFIER_VERSION = "operator_signal_presence_v1";

const URL_ONLY_RE = /https?:\/\/[^\s)]+/gi;
const FOOTER_HINT_RE = /\b(subscribe|privacy policy|cookie|all rights reserved|follow us)\b/i;

function stripUrls(text) {
  return String(text || "").replace(URL_ONLY_RE, " ");
}

function isLikelyFooterOnly(text, span) {
  const window = String(text || "").slice(Math.max(0, span.start - 40), span.end + 40);
  return FOOTER_HINT_RE.test(window);
}

/**
 * @param {{ text: string, citations?: Array<{url?: string, domain?: string}> }} input
 */
export function classifyOperatorPresence(input = {}) {
  const text = String(input.text || "");
  const body = stripUrls(text);
  const spans = findOperatorSpans(body);
  const presentIds = [];
  const observedCompetitors = [];
  const rejected = [];

  for (const span of spans) {
    const id = span.entity.id;
    if (isLikelyFooterOnly(text, span)) {
      rejected.push({ canonicalEntityId: id, reason: "footer_or_nav" });
      continue;
    }
    if (!isPrimaryMonitoredOperator(id)) {
      observedCompetitors.push({
        canonicalEntityId: id,
        canonicalName: span.entity.name,
        rawMention: span.rawMention,
      });
      continue;
    }
    if (!presentIds.includes(id)) presentIds.push(id);
  }

  const citationDomains = (input.citations || [])
    .map((c) => String(c.domain || c.url || "").toLowerCase())
    .filter(Boolean);
  const sourceOnlyIds = [];
  for (const span of findOperatorSpans(text)) {
    const id = span.entity.id;
    if (presentIds.includes(id)) continue;
    const domainHit = (span.entity.firstPartyDomains || []).some((d) =>
      citationDomains.some((cd) => cd.includes(String(d).toLowerCase()))
    );
    if (domainHit) sourceOnlyIds.push(id);
  }

  return {
    signal: OPERATOR_SIGNAL_PRESENCE,
    version: OPERATOR_PRESENCE_CLASSIFIER_VERSION,
    presentOperatorIds: presentIds,
    present: presentIds.length > 0,
    observedCompetitors,
    sourceOnlyOperatorIds: [...new Set(sourceOnlyIds)],
    rejected,
    recommendationMetrics: null,
    censusReads: 0,
  };
}

export function operatorPresentInText(text, operatorId) {
  const result = classifyOperatorPresence({ text });
  return result.presentOperatorIds.includes(operatorId);
}
