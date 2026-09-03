/**
 * Missing AI Evidence V1 — full governed subject-absent observation set.
 *
 * Forensic / completeness lane (distinct from Positive Evidence).
 * Consumes the SAME canonical subject-presence path as certified metrics.
 *
 * Do NOT silently cap the underlying set. UI may page via limit/offset.
 */

import { createHash } from "crypto";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";
import {
  buildCustomerSafeDecisionContext,
  looksLikeProductionPrompt,
  sanitizeCustomerEvidenceCard,
} from "./positive-evidence-v1.js";
import { buildVerbatimResponseFields } from "./verbatim-evidence-response-v1.js";

export const MISSING_EVIDENCE_VERSION = "adp_missing_evidence_v1";
export const MISSING_EVIDENCE_GRAIN = "COMPARABLE_OBSERVATION_SUBJECT_ABSENT";

export const DEFECT_MISSING_EVIDENCE_CONTEXT_MISMATCH = "MISSING_EVIDENCE_CONTEXT_MISMATCH";
export const DEFECT_PROPRIETARY_PROMPT_LEAKAGE = "PROPRIETARY_PROMPT_LEAKAGE";

export const DEFAULT_PAGE_SIZE = 25;
export const MAX_PAGE_SIZE = 100;
/** @deprecated Truncation prohibited — full rawResponse required. */
export const MAX_EXCERPT_CHARS = Number.POSITIVE_INFINITY;

function evidenceToken(obs) {
  return createHash("sha256")
    .update(String(obs.observationId || `${obs.scenarioId}|${obs.provider}|${obs.timestamp || ""}`))
    .digest("hex")
    .slice(0, 16);
}

function subjectAbsent(obs) {
  return !getGovernedSubjectMentioned(obs);
}

/**
 * Full governed missing pool for a context (no cap).
 */
export function filterMissingEvidencePool({
  period,
  scenarios,
  intent = null,
  provider = null,
}) {
  const scenarioMap = Object.fromEntries((scenarios || []).map((s) => [s.scenarioId, s]));
  let pool = filterComparableObservations(period?.observations || []).filter(subjectAbsent);

  if (intent && intent !== "overall") {
    const ids = new Set(
      (scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId)
    );
    pool = pool.filter((o) => ids.has(o.scenarioId));
  }
  if (provider) {
    pool = pool.filter((o) => o.provider === provider);
  }

  // Stable order for pagination reproducibility
  pool = [...pool].sort((a, b) => {
    const s = String(a.scenarioId || "").localeCompare(String(b.scenarioId || ""));
    if (s) return s;
    const p = String(a.provider || "").localeCompare(String(b.provider || ""));
    if (p) return p;
    return String(a.observationId || "").localeCompare(String(b.observationId || ""));
  });

  return { pool, scenarioMap };
}

export function toCustomerMissingEvidenceCard(obs, scenario, propertyProfile = null) {
  const territory = territoryLabelForIntent(scenario?.intent) || scenario?.intent || "—";
  return {
    evidenceId: evidenceToken(obs),
    _observationId: obs.observationId || null,
    _scenarioId: obs.scenarioId || null,
    demandTerritory: territory,
    demandTerritoryIntent: scenario?.intent || null,
    decisionContext: buildCustomerSafeDecisionContext(scenario),
    provider: obs.provider,
    subjectAppeared: false,
    mentioned: false,
    rank: null,
    rankEligible: false,
    ...buildVerbatimResponseFields(obs, propertyProfile, { highlightSubject: false }),
    competitorsMentioned: (obs.competitorsMentioned || []).slice(0, 8),
    competitorsAlongside: (obs.competitorsMentioned || []).slice(0, 8).map((name) => ({
      name: typeof name === "string" ? name : String(name),
      entityId: null,
    })),
    sourcesCited: (obs.sourcesCited || []).slice(0, 3).map((s) => ({
      url: s.url || "",
      title: s.title || s.label || "",
    })),
    periodId: obs.periodId || null,
    timestamp: obs.timestamp || null,
  };
}

export function auditMissingCardForPromptLeakage(card) {
  const defects = [];
  if (looksLikeProductionPrompt(card.decisionContext) && card.decisionContext?.split(/\s+/).length >= 10) {
    defects.push({
      code: DEFECT_PROPRIETARY_PROMPT_LEAKAGE,
      detail: "decisionContext looks like a production prompt",
    });
  }
  return defects;
}

/**
 * Build customer-safe missing evidence response — full set accessible via paging.
 */
export function buildMissingEvidenceResponse({
  period,
  scenarios,
  propertyProfile,
  intent = null,
  provider = null,
  limit = DEFAULT_PAGE_SIZE,
  offset = 0,
}) {
  const { pool, scenarioMap } = filterMissingEvidencePool({
    period,
    scenarios,
    intent,
    provider,
  });

  const pageSize = Math.min(MAX_PAGE_SIZE, Math.max(1, Number(limit) || DEFAULT_PAGE_SIZE));
  const start = Math.max(0, Number(offset) || 0);
  const slice = pool.slice(start, start + pageSize);

  const cards = slice.map((obs) => {
    const sc = scenarioMap[obs.scenarioId] || { intent, scenarioId: obs.scenarioId };
    return toCustomerMissingEvidenceCard(obs, sc, propertyProfile);
  });

  const leakage = cards.flatMap((c) => auditMissingCardForPromptLeakage(c));
  const customerCards = cards.map(sanitizeCustomerEvidenceCard);

  // Scenario-grain absent count (aligns with Demand Capture "Missing" column concept)
  const scenarioIds = new Set(pool.map((o) => o.scenarioId));

  return {
    ok: true,
    version: MISSING_EVIDENCE_VERSION,
    grain: MISSING_EVIDENCE_GRAIN,
    label: "All governed missing observations for this context",
    propertyId: period?.propertyId || null,
    periodId: period?.periodId || null,
    context: {
      intent: intent || null,
      demandTerritory:
        intent && intent !== "overall"
          ? territoryLabelForIntent(intent)
          : intent === "overall"
            ? "Overall"
            : null,
      provider: provider || null,
    },
    evidence: customerCards,
    total: pool.length,
    totalQualifying: pool.length,
    scenarioAbsentCount: scenarioIds.size,
    offset: start,
    limit: pageSize,
    hasMore: start + customerCards.length < pool.length,
    completeSetAccessible: true,
    capped: false,
    _assuranceCards: cards,
    _leakageDefects: leakage,
  };
}
