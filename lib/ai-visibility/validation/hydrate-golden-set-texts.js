/**
 * Hydrate Golden Set scoring texts from monitoring file store.
 * Does not mutate Golden Set ground-truth labels.
 * LIVE_PROVIDER_CALLS: 0.
 */

import { createBrandAiVisibilityReadStore } from "../storage/index.js";

/**
 * Load full rawText for a case when batchId + responseId are present.
 * @returns {Promise<{ text: string|null, source: string, rawTextLength: number }>}
 */
export async function resolveFullResponseText(caseRow, store) {
  const excerpt = caseRow?.text || caseRow?.rawResponseExcerpt || "";
  const batchId = caseRow?.batchId || null;
  const responseId = caseRow?.responseId || caseRow?.sourceResponseId || null;
  if (!batchId || !responseId || !store?.listBatchRuns) {
    return {
      text: excerpt || null,
      source: excerpt ? "excerpt_only" : "missing",
      rawTextLength: String(excerpt || "").length,
      textLossVsExcerpt: false,
    };
  }

  try {
    const runs = (await store.listBatchRuns(batchId)) || [];
    const hit = runs.find((r) => r.responseId === responseId);
    let raw = hit?.rawText ? String(hit.rawText) : "";
    if ((!raw || raw.length < String(excerpt).length) && hit?.evidenceId && store.getEvidence) {
      const ev = await store.getEvidence(hit.evidenceId);
      raw = String(ev?.payload?.rawText || raw || "");
    }
    if (raw && raw.length >= String(excerpt).length) {
      return {
        text: raw,
        source: "monitoring_store_rawText",
        rawTextLength: raw.length,
        textLossVsExcerpt: raw.length > String(excerpt).length + 50,
        excerptLength: String(excerpt).length,
      };
    }
  } catch (err) {
    return {
      text: excerpt || null,
      source: "excerpt_fallback_error",
      rawTextLength: String(excerpt || "").length,
      error: err?.message || String(err),
    };
  }

  return {
    text: excerpt || null,
    source: "excerpt_only",
    rawTextLength: String(excerpt || "").length,
    textLossVsExcerpt: false,
  };
}

/**
 * Clone cases with hydrated text for scoring (labels untouched).
 * @param {object[]} cases
 * @param {{ store?: object }} [options]
 */
export async function hydrateGoldenSetCasesForScoring(cases, options = {}) {
  const store = options.store || createBrandAiVisibilityReadStore({});
  const out = [];
  const stats = {
    total: (cases || []).length,
    hydratedFromStore: 0,
    excerptOnly: 0,
    missing: 0,
    bytesGained: 0,
  };

  for (const c of cases || []) {
    const resolved = await resolveFullResponseText(c, store);
    if (resolved.source === "monitoring_store_rawText") {
      stats.hydratedFromStore += 1;
      stats.bytesGained += Math.max(
        0,
        resolved.rawTextLength - String(c.text || c.rawResponseExcerpt || "").length
      );
    } else if (resolved.text) {
      stats.excerptOnly += 1;
    } else {
      stats.missing += 1;
    }
    out.push({
      ...c,
      text: resolved.text,
      textHydrationSource: resolved.source,
      textHydrationRawLength: resolved.rawTextLength,
    });
  }

  return { cases: out, stats };
}
