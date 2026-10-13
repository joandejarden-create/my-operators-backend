/**
 * Generic web retrieval abstraction — swappable providers.
 * Prefers Context.dev when configured; falls back to existing research fetch.
 */

import {
  isContextDevConfigured,
  contextDevScrapeMarkdown,
} from "../../../context-dev/index.js";
import { fetchResearchPage, htmlToSearchableText } from "../../room-count-research/fetch.js";

export const WEB_RETRIEVAL_V2 = "web-retrieval-v2";

export function describeWebRetrievalProviders(env = process.env) {
  return {
    context_dev: Boolean(String(env.CONTEXT_DEV_API_KEY || "").trim()),
    internal_fetch: true,
    apify: Boolean(String(env.APIFY_TOKEN || env.APIFY_API_TOKEN || "").trim()),
    apify_used_for_contacts: false,
    note: "Apify present for Tripadvisor/rooms — not wired as contact crawler in V2.",
  };
}

/**
 * @returns {{ ok: boolean, provider: string, url: string, text: string, markdown?: string, error?: object }}
 */
export async function fetchPage(url, { prefer = "auto", ledger = null, chargeFn = null } = {}) {
  const target = String(url || "").trim();
  if (!/^https?:\/\//i.test(target)) {
    return { ok: false, provider: "none", url: target, text: "", error: { class: "INVALID_URL" } };
  }

  const useContext =
    (prefer === "context_dev" || prefer === "auto") && isContextDevConfigured();

  if (useContext) {
    if (ledger && chargeFn) {
      const gate = chargeFn();
      if (!gate?.ok) {
        return { ok: false, provider: "context_dev", url: target, text: "", error: { class: "BUDGET" } };
      }
    }
    const scraped = await contextDevScrapeMarkdown({ url: target });
    if (scraped.ok) {
      const md = String(
        typeof scraped.data === "string" ? scraped.data : scraped.data?.markdown || scraped.data?.content || ""
      );
      return { ok: true, provider: "context_dev", url: target, text: md, markdown: md };
    }
  }

  try {
    const page = await fetchResearchPage(target, { timeoutMs: 20000 });
    const html = page?.html || page?.body || "";
    const text = htmlToSearchableText(html) || String(html || "").slice(0, 50000);
    return {
      ok: Boolean(text),
      provider: "internal_fetch",
      url: target,
      text,
      error: text ? null : { class: "EMPTY_BODY" },
    };
  } catch (err) {
    return {
      ok: false,
      provider: "internal_fetch",
      url: target,
      text: "",
      error: { class: "FETCH_ERROR", message: String(err?.message || err) },
    };
  }
}

/** Bounded path crawl — does not spider entire domain. */
export async function crawlDomainPaths(domain, paths, fetchOpts = {}) {
  const host = String(domain || "")
    .replace(/^https?:\/\//i, "")
    .replace(/\/$/, "")
    .replace(/^www\./, "");
  const results = [];
  for (const p of paths) {
    const url = `https://${host}${p.startsWith("/") ? p : `/${p}`}`;
    const page = await fetchPage(url, fetchOpts);
    results.push({ path: p, ...page });
    if (results.filter((r) => r.ok).length >= (fetchOpts.maxOkPages || 6)) break;
  }
  return results;
}
