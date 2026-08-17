/**
 * Pre-publish quality gate for Market Alerts.
 * Run before every Airtable create so cleanup happens prior to publish.
 */
import { assessMarketAlertRelevance } from "./market-alerts-relevance.js";
import {
  looksLikeHtmlMarkup,
  isJunkSummaryBlurb,
  sanitizeMarketAlertPlainText,
} from "./market-alerts-plain-text.js";
import { canonicalizeSourceUrl } from "./market-alerts-dedupe.js";

/**
 * @param {{ title?: string, summary?: string, source?: string, sourceName?: string, link?: string, sourceUrl?: string }} item
 * @returns {{ ok: boolean, reasons: string[], cleaned: { title: string, summary: string, source: string, link: string } }}
 */
export function assessMarketAlertPublishReady(item) {
  const reasons = [];
  const link = String(item.link || item.sourceUrl || "").trim();
  const source = sanitizeMarketAlertPlainText(item.source || item.sourceName || "");
  const title = sanitizeMarketAlertPlainText(item.title || "");
  const summary = sanitizeMarketAlertPlainText(item.summary || "", {
    preserveWhitespace: true,
    maxLen: 10000,
  });

  if (!title) reasons.push("missing_title");
  if (looksLikeHtmlMarkup(title) || looksLikeHtmlMarkup(summary) || looksLikeHtmlMarkup(source)) {
    reasons.push("html_markup");
  }
  if (isJunkSummaryBlurb(summary)) reasons.push("junk_summary_blurb");

  const relevance = assessMarketAlertRelevance({
    title,
    summary,
    source,
    sourceName: source,
  });
  if (!relevance.keep) reasons.push(`irrelevant:${relevance.reason || "noise"}`);

  // Soft warning path: google wrapper URLs are allowed only as last resort after resolve attempts upstream.
  if (/news\.google\.com/i.test(link)) {
    reasons.push("google_news_wrapper_url");
    if (!summary.trim()) reasons.push("google_news_incomplete");
  }

  const hardFail = reasons.some(
    (r) =>
      r === "missing_title" ||
      r === "html_markup" ||
      r === "junk_summary_blurb" ||
      r === "google_news_incomplete" ||
      r.startsWith("irrelevant:")
  );

  return {
    ok: !hardFail,
    reasons,
    cleaned: {
      title,
      summary,
      source,
      link: canonicalizeSourceUrl(link) ? link : link,
    },
  };
}

/** True when an existing Airtable row should be removed by cleanup batch. */
export function shouldDeletePublishedAlert(fields) {
  const gate = assessMarketAlertPublishReady({
    title: fields.Title || fields.title,
    summary: fields.Summary || fields.summary,
    source: fields["Source Name"] || fields.sourceName,
    link: fields["Source URL"] || fields.sourceUrl,
  });
  return !gate.ok;
}
