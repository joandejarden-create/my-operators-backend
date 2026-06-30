/**
 * Fetch article meta description when RSS feeds omit <description>.
 */
import axios from "axios";
import { decodeHtmlEntitiesPreserveWhitespace } from "./decode-html-entities.js";

const USER_AGENT = "DealCapture-MarketAlerts/1.0 (Hospitality news aggregator)";

const META_PATTERNS = [
  /<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']*)["'][^>]*>/i,
  /<meta[^>]+content=["']([^"']*)["'][^>]+property=["']og:description["'][^>]*>/i,
  /<meta[^>]+name=["']twitter:description["'][^>]+content=["']([^"']*)["'][^>]*>/i,
  /<meta[^>]+content=["']([^"']*)["'][^>]+name=["']twitter:description["'][^>]*>/i,
  /<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["'][^>]*>/i,
  /<meta[^>]+content=["']([^"']*)["'][^>]+name=["']description["'][^>]*>/i,
];

export function extractSummaryFromHtml(html) {
  if (!html || typeof html !== "string") return "";
  for (const re of META_PATTERNS) {
    const m = html.match(re);
    const raw = (m?.[1] || "").trim();
    if (raw.length > 20) {
      return decodeHtmlEntitiesPreserveWhitespace(raw).replace(/\s+/g, " ").trim().slice(0, 500);
    }
  }
  return "";
}

/**
 * @param {string} url
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function fetchArticleSummary(url, opts = {}) {
  const target = (url || "").trim();
  if (!target) return "";

  const timeoutMs = opts.timeoutMs ?? parseInt(process.env.MARKET_ALERTS_SUMMARY_FETCH_TIMEOUT_MS || "8000", 10);

  try {
    const { data, status } = await axios.get(target, {
      timeout: timeoutMs,
      responseType: "text",
      maxRedirects: 5,
      headers: { "User-Agent": USER_AGENT, Accept: "text/html" },
      validateStatus: (s) => s >= 200 && s < 400,
    });
    if (status !== 200 || typeof data !== "string") return "";
    return extractSummaryFromHtml(data);
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[rss-article-summary-enrich] fetch failed:", target, err.message);
    }
    return "";
  }
}

/**
 * Fill missing RSS summaries from article pages (bounded concurrency).
 * @param {Array<{ summary?: string, link?: string, title?: string, source?: string }>} items
 */
export async function enrichRssItemsSummaries(items, opts = {}) {
  const concurrency = Math.min(
    Math.max(parseInt(opts.concurrency ?? process.env.MARKET_ALERTS_SUMMARY_ENRICH_CONCURRENCY ?? "4", 10), 1),
    8
  );
  const enabled = opts.enabled ?? process.env.MARKET_ALERTS_RSS_ENRICH_SUMMARIES !== "false";
  if (!enabled || !Array.isArray(items) || !items.length) return { items, enriched: 0, attempted: 0 };

  const out = items.map((item) => ({ ...item }));
  const indices = out
    .map((item, i) => ({ i, link: (item.link || "").trim(), hasSummary: !!(item.summary || "").trim() }))
    .filter((x) => !x.hasSummary && x.link)
    .map((x) => x.i);

  let enriched = 0;
  for (let start = 0; start < indices.length; start += concurrency) {
    const batch = indices.slice(start, start + concurrency);
    const results = await Promise.all(
      batch.map(async (i) => {
        const summary = await fetchArticleSummary(out[i].link);
        return { i, summary };
      })
    );
    for (const { i, summary } of results) {
      if (summary) {
        out[i].summary = summary;
        enriched += 1;
      }
    }
  }

  return { items: out, enriched, attempted: indices.length };
}
