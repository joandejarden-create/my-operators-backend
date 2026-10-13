/**
 * On-demand article body fetch for stakeholder person extraction.
 * Never persists Surfe PII. Does not overwrite editorial summary.
 */

import axios from "axios";
import * as cheerio from "cheerio";
import { resolveGoogleNewsArticleUrl } from "../market-alerts-dedupe.js";

export const STAKEHOLDER_EXTRACTION_SOURCE = Object.freeze({
  FULL_ARTICLE: "FULL_ARTICLE",
  ENRICHED_TEXT: "ENRICHED_TEXT",
  SUMMARY_ONLY: "SUMMARY_ONLY",
  UNAVAILABLE: "UNAVAILABLE",
});

export const STAKEHOLDER_EXTRACTION_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  PARTIAL: "PARTIAL",
  NO_PERSON_FOUND: "NO_PERSON_FOUND",
  ARTICLE_BODY_UNAVAILABLE: "ARTICLE_BODY_UNAVAILABLE",
});

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const HTML_NOISE = "script, style, nav, footer, noscript, svg, iframe, form, aside";

/** In-process cache — request-scoped bodies only; never Surfe PII. */
const bodyCache = new Map();

/**
 * Prefer existing body/enriched text; otherwise fetch publisher HTML once.
 * @param {object} alert
 * @param {{ timeoutMs?: number, forceFetch?: boolean }} [opts]
 * @returns {Promise<{
 *   alert: object,
 *   stakeholderExtractionSource: string,
 *   fetched: boolean,
 *   resolvedSourceUrl: string|null,
 *   bodyChars: number,
 *   bodyFetchMethod?: 'HTTP'|'PLAYWRIGHT'|'CACHE'|'EXISTING'|null,
 * }>}
 */
export async function ensureArticleBodyForStakeholderExtraction(alert = {}, opts = {}) {
  const existing =
    String(alert.articleBody || alert.articleText || alert.body || "").trim() ||
    String(alert.enrichedArticleText || "").trim();
  if (existing.length >= 120 && !opts.forceFetch) {
    const source = alert.enrichedArticleText && !alert.articleBody
      ? STAKEHOLDER_EXTRACTION_SOURCE.ENRICHED_TEXT
      : STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE;
    return {
      alert,
      stakeholderExtractionSource: source,
      fetched: false,
      resolvedSourceUrl: alert.resolvedSourceUrl || alert.sourceUrl || null,
      bodyChars: existing.length,
      bodyFetchMethod: "EXISTING",
    };
  }

  const sourceUrl = String(alert.sourceUrl || alert.link || "").trim();
  if (!sourceUrl) {
    return {
      alert,
      stakeholderExtractionSource: hasSummaryOrIntel(alert)
        ? STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY
        : STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE,
      fetched: false,
      resolvedSourceUrl: null,
      bodyChars: 0,
      bodyFetchMethod: null,
    };
  }

  const cacheKey = sourceUrl.toLowerCase();
  if (bodyCache.has(cacheKey) && !opts.forceFetch) {
    const cached = bodyCache.get(cacheKey);
    if (cached?.articleBody) {
      return {
        alert: {
          ...alert,
          articleBody: cached.articleBody,
          resolvedSourceUrl: cached.resolvedSourceUrl || sourceUrl,
        },
        stakeholderExtractionSource: STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE,
        fetched: false,
        resolvedSourceUrl: cached.resolvedSourceUrl || sourceUrl,
        bodyChars: cached.articleBody.length,
        bodyFetchMethod: "CACHE",
      };
    }
  }

  const timeoutMs =
    opts.timeoutMs ??
    parseInt(process.env.MARKET_ALERTS_STAKEHOLDER_BODY_TIMEOUT_MS || "15000", 10);

  let resolved = sourceUrl;
  try {
    if (/news\.google\.com/i.test(sourceUrl)) {
      resolved = await resolveGoogleNewsArticleUrl(sourceUrl, { timeoutMs });
    }
    if (!resolved || /news\.google\.com/i.test(resolved)) {
      return {
        alert,
        stakeholderExtractionSource: hasSummaryOrIntel(alert)
          ? STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY
          : STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE,
        fetched: true,
        resolvedSourceUrl: resolved || sourceUrl,
        bodyChars: 0,
        bodyFetchMethod: null,
      };
    }

    const { text: articleBody, method } = await fetchPublisherArticleBody(resolved, { timeoutMs });
    if (articleBody && articleBody.length >= 120) {
      bodyCache.set(cacheKey, { articleBody, resolvedSourceUrl: resolved, method });
      return {
        alert: {
          ...alert,
          articleBody,
          resolvedSourceUrl: resolved,
          _bodyFetchMethod: method,
        },
        stakeholderExtractionSource: STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE,
        fetched: true,
        resolvedSourceUrl: resolved,
        bodyChars: articleBody.length,
        bodyFetchMethod: method === "PLAYWRIGHT" ? "PLAYWRIGHT" : "HTTP",
      };
    }
    // Resolved publisher but body blocked/empty — do not pretend research completed
    return {
      alert: {
        ...alert,
        resolvedSourceUrl: resolved,
        articleBodyFetchFailed: true,
      },
      stakeholderExtractionSource: STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE,
      fetched: true,
      resolvedSourceUrl: resolved,
      bodyChars: 0,
      bodyFetchMethod: null,
    };
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        "[market-alerts-contact] article body fetch failed:",
        String(err?.message || err).slice(0, 120)
      );
    }
    const resolvedPublisher = resolved && !/news\.google\.com/i.test(resolved);
    return {
      alert: {
        ...alert,
        resolvedSourceUrl: resolvedPublisher ? resolved : alert.resolvedSourceUrl || null,
        articleBodyFetchFailed: true,
      },
      stakeholderExtractionSource: resolvedPublisher
        ? STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE
        : hasSummaryOrIntel(alert)
          ? STAKEHOLDER_EXTRACTION_SOURCE.SUMMARY_ONLY
          : STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE,
      fetched: true,
      resolvedSourceUrl: resolvedPublisher ? resolved : sourceUrl,
      bodyChars: 0,
      bodyFetchMethod: null,
    };
  }
}

/**
 * Derive extraction status after person extract.
 */
export function deriveStakeholderExtractionStatus({
  stakeholderExtractionSource,
  people = [],
} = {}) {
  const named = (people || []).filter((p) => p?.personName);
  if (stakeholderExtractionSource === STAKEHOLDER_EXTRACTION_SOURCE.UNAVAILABLE) {
    return STAKEHOLDER_EXTRACTION_STATUS.ARTICLE_BODY_UNAVAILABLE;
  }
  if (!named.length) return STAKEHOLDER_EXTRACTION_STATUS.NO_PERSON_FOUND;
  if (
    stakeholderExtractionSource === STAKEHOLDER_EXTRACTION_SOURCE.FULL_ARTICLE ||
    stakeholderExtractionSource === STAKEHOLDER_EXTRACTION_SOURCE.ENRICHED_TEXT
  ) {
    return STAKEHOLDER_EXTRACTION_STATUS.COMPLETE;
  }
  // Summary-only (or weaker) text that still yielded people
  return STAKEHOLDER_EXTRACTION_STATUS.PARTIAL;
}

export function clearArticleBodyCache() {
  bodyCache.clear();
}

function hasSummaryOrIntel(alert) {
  const intel = alert.intelligence || {};
  return Boolean(
    String(alert.summary || intel.summary || "").trim() ||
      String(alert.whatChanged || intel.whatChanged || "").trim() ||
      String(alert.title || "").trim()
  );
}

async function fetchPublisherArticleBody(url, { timeoutMs = 12000 } = {}) {
  try {
    const { data, status } = await axios.get(url, {
      timeout: timeoutMs,
      responseType: "text",
      maxRedirects: 5,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "text/html,application/xhtml+xml",
      },
      validateStatus: (s) => s >= 200 && s < 500,
    });
    if (status >= 200 && status < 300 && typeof data === "string") {
      const text = htmlToArticleText(data);
      if (text.length >= 120 && !looksLikeCloudflareChallenge(text, data)) {
        return { text, method: "HTTP" };
      }
    }
    // 403 / challenge / empty — try headless browser fallback
    if (status === 403 || status === 503 || status === 200) {
      const browserText = await fetchPublisherArticleBodyWithPlaywright(url, { timeoutMs });
      if (browserText && browserText.length >= 120) {
        return { text: browserText, method: "PLAYWRIGHT" };
      }
    }
    return { text: "", method: null };
  } catch (err) {
    const browserText = await fetchPublisherArticleBodyWithPlaywright(url, { timeoutMs });
    if (browserText && browserText.length >= 120) {
      return { text: browserText, method: "PLAYWRIGHT" };
    }
    throw err;
  }
}

function looksLikeCloudflareChallenge(text, html) {
  const blob = `${text || ""}\n${html || ""}`.toLowerCase();
  return (
    /just a moment|cf-browser-verification|attention required|cloudflare/i.test(blob) &&
    String(text || "").length < 800
  );
}

/**
 * Playwright fallback when publishers Cloudflare-block plain HTTP clients.
 * Opt-out: MARKET_ALERTS_STAKEHOLDER_BODY_PLAYWRIGHT=0
 */
async function fetchPublisherArticleBodyWithPlaywright(url, { timeoutMs = 20000 } = {}) {
  if (String(process.env.MARKET_ALERTS_STAKEHOLDER_BODY_PLAYWRIGHT || "1") === "0") {
    return "";
  }
  let browser = null;
  try {
    const { chromium } = await import("playwright");
    browser = await chromium.launch({
      headless: true,
      args: ["--disable-blink-features=AutomationControlled"],
    });
    const page = await browser.newPage({
      userAgent: USER_AGENT,
      viewport: { width: 1280, height: 900 },
    });
    page.setDefaultTimeout(Math.min(timeoutMs, 45000));
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: Math.min(timeoutMs, 45000) });
    // Dismiss common consent banners when present
    try {
      const accept = page.getByRole("button", { name: /accept all/i });
      if (await accept.isVisible({ timeout: 1500 })) await accept.click({ timeout: 2000 });
    } catch {
      /* no consent banner */
    }
    await page.waitForTimeout(800);
    const text = await page.evaluate(() => {
      const noise = "script, style, nav, footer, noscript, svg, iframe, form, aside";
      document.querySelectorAll(noise).forEach((el) => el.remove());
      const article =
        document.querySelector("article") ||
        document.querySelector('[itemprop="articleBody"]') ||
        document.querySelector(".article-body, .story-body, .entry-content, .post-content, main") ||
        document.body;
      return (article?.innerText || "").replace(/\s+/g, " ").trim().slice(0, 50000);
    });
    return text && text.length >= 120 && !/just a moment/i.test(text) ? text : "";
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(
        "[market-alerts-contact] playwright body fetch failed:",
        String(err?.message || err).slice(0, 120)
      );
    }
    return "";
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch {
        /* ignore */
      }
    }
  }
}

function htmlToArticleText(html) {
  const $ = cheerio.load(html);
  $(HTML_NOISE).remove();
  const article =
    $("article").first().text() ||
    $('[itemprop="articleBody"]').first().text() ||
    $(".article-body, .story-body, .entry-content, .post-content, main").first().text() ||
    $("body").text();
  return String(article || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 50000);
}
