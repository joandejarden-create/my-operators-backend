/**
 * High-value scrape failure recovery — retain snippet leads; pivot; do not re-pay same URL.
 * Never fall back to OTA/travel merely because it is accessible.
 */
import { SOURCE_CLASS } from "./constants.js";
import { classifySourceClass } from "./scrape-selection-gate.js";

/**
 * @param {{
 *   url: string,
 *   title?: string,
 *   snippet?: string,
 *   tier?: number,
 *   fail_reason?: string,
 * }} failed
 * @param {{
 *   attempted_urls?: string[],
 *   extracted_cnpj?: string|null,
 *   extracted_legal_name?: string|null,
 *   alternate_candidates?: Array<{ url?: string, title?: string, snippet?: string }>,
 * }} ctx
 */
export function planScrapeFailureRecovery(failed = {}, ctx = {}) {
  const url = String(failed.url || "");
  const tier = Number(failed.tier || 99);
  const attempted = new Set((ctx.attempted_urls || []).map(String));
  const pivots = [];

  // Low-value: do not retry; do not suggest OTA substitutes
  if (tier >= 4) {
    return {
      retain_snippet_evidence: Boolean(failed.title || failed.snippet),
      retry_same_url: false,
      pivots: [],
      ota_fallback_allowed: false,
      reason: "LOW_VALUE_NO_RETRY",
    };
  }

  const retain = Boolean(
    failed.title || failed.snippet || ctx.extracted_cnpj || ctx.extracted_legal_name
  );

  // HTTP/HTTPS variant
  try {
    const u = new URL(url);
    const alt =
      u.protocol === "https:"
        ? url.replace(/^https:/i, "http:")
        : url.replace(/^http:/i, "https:");
    if (alt !== url && !attempted.has(alt)) {
      pivots.push({ kind: "PROTOCOL_VARIANT", url: alt });
    }
    const root = `${u.protocol}//${u.host}/`;
    if (!attempted.has(root) && root !== url) {
      pivots.push({ kind: "ROOT_DOMAIN", url: root });
    }
  } catch {
    /* ignore bad URL */
  }

  if (ctx.extracted_cnpj) {
    pivots.push({
      kind: "CNPJ_REGISTRY_SEARCH",
      query: `"${ctx.extracted_cnpj}" CNPJ`,
    });
  }
  if (ctx.extracted_legal_name) {
    pivots.push({
      kind: "LEGAL_NAME_REGISTRY_SEARCH",
      query: `"${ctx.extracted_legal_name}" CNPJ`,
    });
  }

  pivots.push({
    kind: "ALTERNATE_REGISTRY_SOURCE",
    query: ctx.extracted_cnpj
      ? `${ctx.extracted_cnpj} (Receita Federal OR QSA OR "quadro de socios")`
      : null,
  });

  // Prefer next highest-value independent candidate — never OTA/travel as ownership substitute
  const alts = [];
  for (const cand of ctx.alternate_candidates || []) {
    const cUrl = String(cand.url || "");
    if (!cUrl || attempted.has(cUrl) || cUrl === url) continue;
    const sc = classifySourceClass(cand);
    if (sc === SOURCE_CLASS.LOW_VALUE_TRAVEL || sc === SOURCE_CLASS.SYNTHETIC_OR_TEST) {
      continue;
    }
    if (
      [
        SOURCE_CLASS.PRIMARY_REGULATORY,
        SOURCE_CLASS.STRONG_REGISTRY_DERIVED,
        SOURCE_CLASS.OFFICIAL_LEGAL_OR_PRIVACY,
        SOURCE_CLASS.FII_OR_INVESTOR,
      ].includes(sc)
    ) {
      alts.push({ kind: "ALTERNATE_HIGH_VALUE_URL", url: cUrl, source_class: sc });
    }
  }

  return {
    retain_snippet_evidence: retain,
    retry_same_url: false,
    pivots: [...pivots, ...alts].filter((p) => p.url || p.query),
    ota_fallback_allowed: false,
    reason: "HIGH_VALUE_PIVOT_NO_OTA_SUBSTITUTE",
  };
}
