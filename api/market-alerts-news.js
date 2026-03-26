import axios from "axios";

// Hotel industry RSS feeds (see HOTEL_INDUSTRY_RSS_FEEDS.md)
const RSS_FEEDS = [
  { url: "https://www.hospitalitynet.org/news/global.xml", source: "Hospitality Net" },
  { url: "https://www.hospitalitynet.org/news/us.xml", source: "Hospitality Net (USA & Canada)" },
  { url: "https://skift.com/feed/", source: "Skift" },
  { url: "https://lodgingmagazine.com/rssfeed", source: "LODGING Magazine" },
  { url: "https://www.hotelexecutive.com/rss/4", source: "Hotel Executive (Business & Finance)" },
  { url: "https://www.hotelexecutive.com/rss/24", source: "Hotel Executive (Construction & Development)" },
  { url: "https://www.hotelexecutive.com/rss/13", source: "Hotel Executive (Market & Trends)" },
  { url: "https://www.hospitalitynet.org/news/openings.xml", source: "Hospitality Net (Openings)" },
];

const CACHE_MS = 15 * 60 * 1000; // 15 minutes
let cached = null;
let cachedAt = 0;

function extractTag(block, tag) {
  const re = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, "i");
  const m = block.match(re);
  if (!m) return "";
  let s = m[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, "$1").trim();
  return s.replace(/<[^>]+>/g, "").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').trim();
}

function extractLinkHref(block) {
  const m = block.match(/<link\s+(?:[^>]*\s+)?href=["']([^"']+)["']/i) || block.match(/<link>([^<]+)<\/link>/i);
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
    const pubDate = extractTag(block, "pubDate") || extractTag(block, "updated") || extractTag(block, "published");
    const description = extractTag(block, "description") || extractTag(block, "content") || extractTag(block, "summary");
    if (title || link) {
      items.push({
        title: title || "(No title)",
        link: link || "",
        pubDate: pubDate || null,
        summary: description ? description.slice(0, 500) : "",
        source,
      });
    }
  }
  return items;
}

export async function getMarketAlertsNews(req, res) {
  try {
    if (cached && Date.now() - cachedAt < CACHE_MS) {
      return res.json({ success: true, items: cached, cached: true });
    }

    const allItems = [];
    const fetchOptions = {
      timeout: 12000,
      headers: { "User-Agent": "DealCapture-MarketAlerts/1.0 (Hotel industry news aggregator)" },
      validateStatus: () => true,
    };

    for (const { url, source } of RSS_FEEDS) {
      try {
        const { data, status } = await axios.get(url, { ...fetchOptions, responseType: "text" });
        if (status !== 200 || typeof data !== "string") continue;
        const items = parseRssXml(data, source);
        allItems.push(...items);
      } catch (err) {
        console.warn("RSS fetch failed:", url, err.message);
      }
    }

    // Sort by date first (newest first), so dedupe keeps the newest version of each article
    allItems.sort((a, b) => {
      const da = a.pubDate ? new Date(a.pubDate).getTime() : 0;
      const db = b.pubDate ? new Date(b.pubDate).getTime() : 0;
      return db - da;
    });

    // Normalize link for dedup (lowercase, strip trailing slash and query params)
    function normKey(item) {
      const link = (item.link || "").trim().toLowerCase().replace(/\/+$/, "");
      if (link) return link.replace(/\?.*$/, "");
      return (item.title || "").trim().toLowerCase().slice(0, 200);
    }

    // Deduplicate by link (or title if no link); keep first = newest after sort
    const seen = new Set();
    const unique = [];
    for (const item of allItems) {
      const key = normKey(item);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      unique.push(item);
    }

    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 100);
    const items = unique.slice(0, limit);

    cached = items;
    cachedAt = Date.now();

    res.json({ success: true, items, cached: false });
  } catch (error) {
    console.error("Error fetching market alerts news:", error);
    if (cached) {
      return res.json({ success: true, items: cached, cached: true, error: "Some feeds failed; showing cached data." });
    }
    res.status(500).json({ success: false, error: "Failed to load news", items: [] });
  }
}
