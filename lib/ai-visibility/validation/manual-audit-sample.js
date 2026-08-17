/**
 * Deterministic human audit sampling — no reviewer workflow UI required.
 * Airtable writes: none (dry-run proposal only if schema needed later).
 */

import crypto from "crypto";

/**
 * Stable hash for sampling.
 * @param {string} s
 */
function hashMod(s, mod) {
  const h = crypto.createHash("sha256").update(String(s)).digest();
  return h.readUInt32BE(0) % mod;
}

/**
 * Deterministically sample observation rows for human review.
 * @param {object[]} candidates — { responseId, promptId, entityId, batchId, provider, geography, language, promptFamily, classificationType, systemResult }
 * @param {{ sampleSize?: number, seed?: string }} [options]
 */
export function sampleManualAuditCases(candidates, options = {}) {
  const sampleSize = Math.max(0, Number(options.sampleSize) || 25);
  const seed = options.seed || "ai_intelligence_manual_audit_v1";
  const scored = (candidates || []).map((c, i) => {
    const key = [
      seed,
      c.batchId,
      c.responseId || c.evidenceId,
      c.promptId,
      c.entityId,
      c.provider,
      c.geography,
      c.language,
      i,
    ].join("|");
    return { ...c, _rank: hashMod(key, 1_000_000_007) };
  });
  scored.sort((a, b) => a._rank - b._rank || String(a.responseId).localeCompare(String(b.responseId)));
  const picked = scored.slice(0, sampleSize).map((c, idx) => {
    const { _rank, ...rest } = c;
    return {
      reviewCaseId: `rev_${hashMod(`${seed}|${c.batchId}|${c.responseId || c.evidenceId}|${idx}`, 1e9).toString(16)}`,
      batchId: rest.batchId || null,
      responseId: rest.responseId || rest.evidenceId || null,
      promptId: rest.promptId || null,
      entityId: rest.entityId || null,
      systemResult: rest.systemResult || null,
      reviewerResult: null,
      agreement: null,
      reviewedAt: null,
      reviewer: null,
      notes: null,
      dimensions: {
        provider: rest.provider || null,
        geography: rest.geography || null,
        language: rest.language || null,
        promptFamily: rest.promptFamily || null,
        classificationType: rest.classificationType || null,
      },
    };
  });
  return {
    sampleSize: picked.length,
    requestedSize: sampleSize,
    seed,
    cases: picked,
  };
}
