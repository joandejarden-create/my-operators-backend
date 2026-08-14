import axios from "axios";
import {
  canonicalizeSourceUrl,
  normalizeAlertTitle,
  preferDirectArticleUrl,
} from "../lib/market-alerts-dedupe.js";
import { sanitizeMarketAlertPlainText } from "../lib/market-alerts-plain-text.js";
import { isMarketAlertRelevant } from "../lib/market-alerts-relevance.js";
import { sanitizeMarketAlertText } from "./lib/market-alerts-rss-airtable.js";

/**
 * Hotel industry RSS feeds for Market Alerts.
 * Prefer deal/supply-dense sources; avoid appointment-heavy channels.
 */
export const RSS_FEEDS = [
  { url: "https://www.hospitalitynet.org/news/global.xml", source: "Hospitality Net" },
  { url: "https://www.hospitalitynet.org/news/us.xml", source: "Hospitality Net (USA & Canada)" },
  { url: "https://www.hospitalitynet.org/news/openings.xml", source: "Hospitality Net (Openings)" },
  { url: "https://skift.com/feed/", source: "Skift" },
  { url: "https://lodgingmagazine.com/rssfeed", source: "LODGING Magazine" },
  { url: "https://www.hotelexecutive.com/rss/4", source: "Hotel Executive (Business & Finance)" },
  { url: "https://www.hotelexecutive.com/rss/24", source: "Hotel Executive (Construction & Development)" },
  { url: "https://www.hotelexecutive.com/rss/13", source: "Hotel Executive (Market & Trends)" },
  { url: "https://www.hotelnewsresource.com/xml.php", source: "Hotel News Resource" },
  { url: "https://www.hotelnewsresource.com/xml.php?region=Asia", source: "Hotel News Resource (Asia)" },
  { url: "https://www.hoteldive.com/feeds/news/", source: "Hotel Dive" },
  { url: "https://www.hotelmanagement.net/rss.xml", source: "Hotel Management" },
  {
    url: "https://hospitality.economictimes.indiatimes.com/rss/hotels",
    source: "ET HospitalityWorld",
  },
  // Google News removed: wrappers are unresolvable publisher URLs and dominate noise.
  // Prefer Hotel Dive / HNR / Hotel Management / ET for deal & opening signal.
];

const CACHE_MS = 15 * 60 * 1000; // 15 minutes
let cached = null;
let cachedAt = 0;

function extractTag(block, tag) {
  const re = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, "i");
  const m = block.match(re);
  if (!m) return "";
  const raw = m[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, "$1").trim();
  const preserve =
    tag === "description" || tag === "content" || tag === "summary" || tag === "content:encoded";
  // Decode entities THEN strip tags so &lt;span&gt; cannot reappear as visible HTML.
  return sanitizeMarketAlertPlainText(raw, { preserveWhitespace: preserve });
}

function extractLinkHref(block) {
  const m =
    block.match(/<link\s+(?:[^>]*\s+)?href=["']([^"']+)["']/i) ||
    block.match(/<link>([^<]+)<\/link>/i);
  return m ? m[1].trim() : "";
}

function parseRssXml(xml, source) {
  const items = [];
  const itemBlocks = xml.split(/<item>|<\/item>|<entry>|<\/entry>/i);
  for (let i = 1; i < itemBlocks.length; i += 2) {
    const block = (itemBlocks[i] || "").trim();
    if (!block) continue;
    const title = extractTag(block, "title");
    let link = extractTag(block, "link") || extractLinkHref(block);
    if (!link && block.includes("<link")) {
      const href = block.match(/href=["']([^"']+)["']/);
      if (href) link = href[1];
    }
    link = preferDirectArticleUrl(link, block);
    const pubDate =
      extractTag(block, "pubDate") || extractTag(block, "updated") || extractTag(block, "published");
    const description =
      extractTag(block, "description") ||
      extractTag(block, "content") ||
      extractTag(block, "summary");
    if (title || link) {
      items.push({
        title: sanitizeMarketAlertPlainText(title || "(No title)"),
        link: link || "",
        pubDate: pubDate || null,
        summary: sanitizeMarketAlertPlainText(description || "", {
          preserveWhitespace: true,
          maxLen: 500,
        }),
        source,
      });
    }
  }
  return items;
}

function itemDedupeKeys(item) {
  const urlKey = canonicalizeSourceUrl(item.link || "");
  const titleKey = normalizeAlertTitle(item.title || "");
  return { urlKey, titleKey };
}

export function parseMarketAlertsRssXml(xml, source) {
  return parseRssXml(xml, source);
}

const DEFAULT_RSS_FETCH_OPTIONS = {
  timeout: 15000,
  headers: {
    "User-Agent": "DealCapture-MarketAlerts/1.1 (Hotel industry news aggregator)",
    Accept: "application/rss+xml, application/xml, text/xml, */*",
  },
  validateStatus: () => true,
};

/**
 * Fetch one RSS URL and parse items. Does not dedupe across feeds.
 * @param {string} url
 * @param {string} source
 */
export async function fetchSingleRssFeed(url, source) {
  const { data, status } = await axios.get(url, {
    ...DEFAULT_RSS_FETCH_OPTIONS,
    responseType: "text",
  });
  if (status !== 200 || typeof data !== "string") {
    const err = new Error(`RSS status ${status}`);
    err.status = status;
    throw err;
  }
  return parseRssXml(data, source);
}

/** Fetch + dedupe hospitality RSS (used by API and CLI). */
export async function fetchMarketAlertsRssItems({ limit = 50 } = {}) {
  const allItems = [];
  const fetchOptions = {
    timeout: 15000,
    headers: {
      "User-Agent": "DealCapture-MarketAlerts/1.1 (Hotel industry news aggregator)",
      Accept: "application/rss+xml, application/xml, text/xml, */*",
    },
    validateStatus: () => true,
  };

  for (const { url, source } of RSS_FEEDS) {
    try {
      const { data, status } = await axios.get(url, { ...fetchOptions, responseType: "text" });
      if (status !== 200 || typeof data !== "string") {
        if (process.env.NODE_ENV !== "production") {
          console.warn("[market-alerts-rss] feed status", status, source);
        }
        continue;
      }
      const items = parseRssXml(data, source);
      allItems.push(...items);
    } catch (err) {
      console.warn("RSS fetch failed:", url, err.message);
    }
  }

  allItems.sort((a, b) => {
    const da = a.pubDate ? new Date(a.pubDate).getTime() : 0;
    const db = b.pubDate ? new Date(b.pubDate).getTime() : 0;
    return db - da;
  });

  const seenUrls = new Set();
  const seenTitles = new Set();
  const unique = [];
  let droppedIrrelevant = 0;
  for (const item of allItems) {
    if (!isMarketAlertRelevant(item)) {
      droppedIrrelevant += 1;
      continue;
    }
    const { urlKey, titleKey } = itemDedupeKeys(item);
    if (urlKey && seenUrls.has(urlKey)) continue;
    if (titleKey && titleKey.length >= 24 && seenTitles.has(titleKey)) continue;
    if (urlKey) seenUrls.add(urlKey);
    if (titleKey && titleKey.length >= 24) seenTitles.add(titleKey);
    unique.push(item);
  }

  if (process.env.NODE_ENV !== "production" && droppedIrrelevant) {
    console.warn(`[market-alerts-rss] dropped ${droppedIrrelevant} irrelevant items`);
  }

  const max = Math.min(Math.max(parseInt(String(limit), 10) || 50, 1), 250);
  return unique.slice(0, max);
}

export async function getMarketAlertsNews(req, res) {
  try {
    if (cached && Date.now() - cachedAt < CACHE_MS) {
      return res.json({ success: true, items: cached, cached: true });
    }

    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 100);
    const rawItems = await fetchMarketAlertsRssItems({ limit });
    const items = rawItems.map((item) => ({
      ...item,
      title: sanitizeMarketAlertText(item.title || ""),
      summary: sanitizeMarketAlertText(item.summary || "", { preserveWhitespace: true }),
      source: sanitizeMarketAlertText(item.source || ""),
    }));

    cached = items;
    cachedAt = Date.now();

    res.json({ success: true, items, cached: false });
  } catch (error) {
    console.error("Error fetching market alerts news:", error);
    if (cached) {
      return res.json({
        success: true,
        items: cached,
        cached: true,
        error: "Some feeds failed; showing cached data.",
      });
    }
    res.status(500).json({ success: false, error: "Failed to load news", items: [] });
  }
}
