/**
 * Canonical evidence resolution for Brand AI Visibility read paths.
 *
 * Historical provider baselines (Gemini / Perplexity / Claude) often omit
 * run.evidenceId while still persisting evidence keyed by responseId.
 * Treat run.responseId ↔ evidence.responseId as a first-class legacy join.
 *
 * Resolution order (exact match only — no fuzzy / prompt-text / position matching):
 *   A. run.evidenceId → getEvidence
 *   B. run.responseId → exactly one evidence.responseId in scope
 *   C. UNRESOLVED / AMBIGUOUS_EVIDENCE_LINK
 */

export const EVIDENCE_RESOLUTION_MODES = Object.freeze({
  EVIDENCE_ID: "EVIDENCE_ID",
  RESPONSE_ID_LEGACY: "RESPONSE_ID_LEGACY",
  UNRESOLVED: "UNRESOLVED",
  AMBIGUOUS_EVIDENCE_LINK: "AMBIGUOUS_EVIDENCE_LINK",
});

export const EVIDENCE_RESOLUTION_CONTRACT_VERSION =
  "ai_visibility_evidence_resolution_v1";

/**
 * Build an in-memory responseId → evidence[] index for uniqueness checks.
 * Prefer batch-scoped listing when the store supports filter.batchId.
 *
 * @param {object} store
 * @param {{ batchId?: string|null, provider?: string|null }} [scope]
 * @returns {Promise<{ byResponseId: Map<string, object[]>, listedCount: number }>}
 */
export async function buildEvidenceResolutionIndex(store, scope = {}) {
  /** @type {Map<string, object[]>} */
  const byResponseId = new Map();
  let listedCount = 0;

  if (!store || typeof store.listEvidence !== "function") {
    return { byResponseId, listedCount };
  }

  const filter = {};
  if (scope.batchId) filter.batchId = scope.batchId;
  if (scope.provider) filter.provider = scope.provider;

  let rows = (await store.listEvidence(filter)) || [];

  // Some evidence rows omit batchId; if batch filter returned nothing but we have
  // a provider filter, fall back to provider-scoped list (still uniqueness-gated).
  if (!rows.length && scope.batchId && scope.provider) {
    rows = (await store.listEvidence({ provider: scope.provider })) || [];
  }

  listedCount = rows.length;
  for (const ev of rows) {
    const responseId = ev?.responseId || ev?.payload?.responseId || null;
    if (!responseId) continue;
    const key = String(responseId);
    const list = byResponseId.get(key) || [];
    list.push(ev);
    byResponseId.set(key, list);
  }

  return { byResponseId, listedCount };
}

/**
 * Resolve governed evidence for a monitoring run.
 *
 * @param {object} store
 * @param {object} run
 * @param {{ index?: { byResponseId: Map<string, object[]> }|null, batchId?: string|null, provider?: string|null }} [opts]
 * @returns {Promise<{
 *   mode: string,
 *   evidence: object|null,
 *   evidenceId: string|null,
 *   responseId: string|null,
 *   matchCount: number,
 * }>}
 */
export async function resolveEvidenceForRun(store, run, opts = {}) {
  const responseId = run?.responseId ? String(run.responseId) : null;
  const evidenceId = run?.evidenceId ? String(run.evidenceId) : null;

  if (evidenceId && typeof store?.getEvidence === "function") {
    const evidence = await store.getEvidence(evidenceId);
    if (evidence) {
      return {
        mode: EVIDENCE_RESOLUTION_MODES.EVIDENCE_ID,
        evidence,
        evidenceId: evidence.evidenceId || evidenceId,
        responseId: evidence.responseId || responseId,
        matchCount: 1,
      };
    }
  }

  if (responseId) {
    let index = opts.index || null;
    if (!index) {
      index = await buildEvidenceResolutionIndex(store, {
        batchId: opts.batchId || run?.batchId || null,
        provider: opts.provider || run?.provider || null,
      });
    }
    const matches = index.byResponseId?.get(responseId) || [];
    if (matches.length === 1) {
      const evidence = matches[0];
      return {
        mode: EVIDENCE_RESOLUTION_MODES.RESPONSE_ID_LEGACY,
        evidence,
        evidenceId: evidence.evidenceId || null,
        responseId,
        matchCount: 1,
      };
    }
    if (matches.length > 1) {
      return {
        mode: EVIDENCE_RESOLUTION_MODES.AMBIGUOUS_EVIDENCE_LINK,
        evidence: null,
        evidenceId: null,
        responseId,
        matchCount: matches.length,
      };
    }
  }

  return {
    mode: EVIDENCE_RESOLUTION_MODES.UNRESOLVED,
    evidence: null,
    evidenceId: evidenceId || null,
    responseId,
    matchCount: 0,
  };
}
