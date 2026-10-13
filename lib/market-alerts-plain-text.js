/**
 * Plain-text cleanup for Market Alerts titles/summaries before publish.
 * Handles entity-encoded markup (decode → strip → decode) and CMS junk.
 */

import {
  decodeHtmlEntities,
  decodeHtmlEntitiesPreserveWhitespace,
} from "./decode-html-entities.js";

const TAG_RE = /<\/?[a-zA-Z][^>]*>/g;
const SCRIPT_STYLE_RE = /<(script|style|noscript|iframe)[^>]*>[\s\S]*?<\/\1>/gi;
const COMMENT_RE = /<!--[\s\S]*?-->/g;
const LEFTOVER_TAGISH_RE = /<\/?[a-zA-Z][^>]*>|&lt;\/?[a-zA-Z][^&]*&gt;/i;
const CMS_JUNK_RE =
  /\b(field--name-|field--type-|field__item|data-pm-slice|text-formatted|clearfix text-formatted)\b/i;

/**
 * Convert HTML / entity-encoded HTML into clean plain text.
 * @param {string} text
 * @param {{ preserveWhitespace?: boolean }} [opts]
 */
export function stripHtmlToPlainText(text, { preserveWhitespace = false } = {}) {
  if (text == null) return "";
  let s = String(text);

  // Drop script/style blocks before any decode.
  s = s.replace(SCRIPT_STYLE_RE, " ");
  s = s.replace(COMMENT_RE, " ");

  // Decode entities first so &lt;span&gt; becomes strip-able tags.
  s = preserveWhitespace
    ? decodeHtmlEntitiesPreserveWhitespace(s)
    : decodeHtmlEntities(s);

  // Strip tags (repeat for nested leftovers).
  for (let i = 0; i < 3; i++) {
    const next = s.replace(TAG_RE, " ");
    if (next === s) break;
    s = next;
  }

  // Decode again in case attribute text still had entities.
  s = preserveWhitespace
    ? decodeHtmlEntitiesPreserveWhitespace(s)
    : decodeHtmlEntities(s);

  // Normalize whitespace / punctuation artifacts from tag removal.
  s = s
    .replace(/\u00A0/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ");

  if (!preserveWhitespace) s = s.replace(/\s+/g, " ").trim();
  else s = s.trim();

  return s;
}

const REJECT_SUMMARY_RE =
  /comprehensive up-to-date news coverage|aggregated from sources all over the world by google news|stories from the web about/i;

const GOOGLE_NEWS_WRAPPER_URL_RE =
  /https?:\/\/news\.google\.com\/rss\/articles\/[^\s<>"']+/gi;

const ANCHOR_TAG_RE = /<a\b[^>]*>([\s\S]*?)<\/a>/gi;

/** True if text still looks like HTML/CMS markup after a naive pass. */
export function looksLikeHtmlMarkup(text) {
  if (!text || typeof text !== "string") return false;
  return LEFTOVER_TAGISH_RE.test(text) || CMS_JUNK_RE.test(text);
}

/** True if summary is a useless publisher/site boilerplate blurb. */
export function isJunkSummaryBlurb(text) {
  if (!text || typeof text !== "string") return false;
  return REJECT_SUMMARY_RE.test(text);
}

/** Remove Google News wrapper URLs that should never appear as Highlights prose. */
export function stripGoogleNewsWrapperUrls(text) {
  if (!text || typeof text !== "string") return "";
  let s = text.replace(GOOGLE_NEWS_WRAPPER_URL_RE, " ");
  s = s.replace(/\bnews\.google\.com\/rss\/articles\/[^\s<>"']+/gi, " ");
  return s.replace(/\s+/g, " ").trim();
}

/** Convert anchor tags to inner text before broader HTML stripping. */
export function stripAnchorTagsToText(text) {
  if (!text || typeof text !== "string") return "";
  return text.replace(ANCHOR_TAG_RE, (_, inner) => String(inner || " ").trim());
}

/** Drop CMS/RSS leftover link placeholders that leak into titles/summaries. */
export function stripLinkPlaceholders(text) {
  if (!text || typeof text !== "string") return "";
  let s = text;
  s = s.replace(/\bLINK\s+[A-Za-z0-9._-]{1,12}\b/gi, " ");
  s = s.replace(/(?:^|[\s\[(])LINK(?=[\s\])]|:|$)/gi, " ");
  s = s.replace(/\[\s*LINK\s*\]/gi, " ");
  return s.replace(/\s+/g, " ").trim();
}

/**
 * Compare key for title vs summary — strips publisher suffixes and punctuation.
 * Used to detect Google News / CMS cases where "summary" is just the headline again.
 */
export function summaryCompareKey(text) {
  let t = String(text || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
  t = t.replace(/\s+[-–—|]\s+[a-z0-9 .,&'()]{2,80}$/i, "");
  t = t.replace(/\s+\([^)]{2,60}\)\s*$/g, "");
  t = t
    .replace(/&[a-z#0-9]+;/gi, " ")
    .replace(/[''`´]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return t;
}

/**
 * True when summary is empty or effectively repeats the title
 * (common Google News RSS description = headline + publisher).
 */
export function isTitleDuplicateSummary(title, summary) {
  const s = String(summary || "").trim();
  if (!s) return true;
  const tKey = summaryCompareKey(title);
  let sKey = summaryCompareKey(summary);
  if (!sKey) return true;
  if (!tKey) return false;
  if (tKey === sKey) return true;

  // Summary often keeps publisher without a dash ("… Street The Urban Developer")
  if (sKey.startsWith(tKey) && sKey.length - tKey.length <= 40) return true;
  if (tKey.startsWith(sKey) && tKey.length - sKey.length <= 40) return true;

  if (tKey.length >= 24 && sKey.length >= 24) {
    if (tKey.includes(sKey) || sKey.includes(tKey)) {
      const longer = Math.max(tKey.length, sKey.length);
      const shorter = Math.min(tKey.length, sKey.length);
      if (shorter / longer >= 0.7) return true;
    }
  }
  // Very short "summaries" that are just a clipped title
  if (sKey.length < 48 && tKey.startsWith(sKey)) return true;
  return false;
}

/**
 * Sanitize Market Alert text for Airtable / UI publish.
 * Returns empty string if the result is still CMS garbage.
 */
export function sanitizeMarketAlertPlainText(text, { preserveWhitespace = false, maxLen = 10000 } = {}) {
  let pre = stripAnchorTagsToText(text);
  pre = stripGoogleNewsWrapperUrls(pre);
  let cleaned = stripHtmlToPlainText(pre, { preserveWhitespace });
  cleaned = stripGoogleNewsWrapperUrls(cleaned);
  cleaned = stripLinkPlaceholders(cleaned);
  if (looksLikeHtmlMarkup(cleaned) || CMS_JUNK_RE.test(cleaned)) {
    // Second hard pass: remove anything that still looks like a tag skeleton.
    cleaned = cleaned
      .replace(/<\/?[a-zA-Z][^>]*>/g, " ")
      .replace(/field--[a-z0-9_-]+/gi, " ")
      .replace(/data-pm-slice="[^"]*"/gi, " ")
      .replace(/\s+/g, " ")
      .trim();
    cleaned = stripLinkPlaceholders(cleaned);
  }
  if (looksLikeHtmlMarkup(cleaned) || CMS_JUNK_RE.test(cleaned) || isJunkSummaryBlurb(cleaned)) {
    return "";
  }
  if (maxLen > 0) cleaned = cleaned.slice(0, maxLen);
  return cleaned;
}
