/**
 * GET /api/hotel-explorer/hotels/:hotelId/news-articles
 * Read-only Market Alerts → hotel-specific news. No writes.
 */
import Airtable from "airtable";
import {
  buildHotelNewsIdentity,
  selectHotelNewsArticles,
  HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
  HOTEL_NEWS_ALERT_FIELDS as F_ALERT,
  HOTEL_NEWS_INTEL_FIELDS as MAP_INTEL,
  MAP_HOTEL_ASSOC,
} from "../lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js";
import {
  marketAlertsArticlesToCitations,
} from "../lib/hotel-intelligence/market-alerts-evidence-citations-v1.js";

const TABLE = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
const CACHE_MS = 5 * 60 * 1000;
const cache = new Map();

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return null;
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
}

function escapeAirtableString(str) {
  return String(str || "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")
    .replace(/[\r\n]+/g, " ")
    .trim();
}

function cacheKey(hotelId, limit) {
  return `${hotelId}|${limit}`;
}

function buildSearchFormula(identity) {
  const parts = [];
  if (identity.hpcId) {
    // Canonical association via Tags `hpc:<recId>` (no new table). Optional
    // MAP_HOTEL_ASSOC field is designed but not required for read matching.
    parts.push(
      `SEARCH('hpc:${escapeAirtableString(identity.hpcId)}', ARRAYJOIN({${F_ALERT.tags}}))`
    );
  }
  for (const name of identity.nameKeys.slice(0, 6)) {
    const term = escapeAirtableString(String(name).toLowerCase());
    if (term.length < 4) continue;
    parts.push(`SEARCH('${term}', LOWER({${F_ALERT.title}}))`);
    parts.push(`SEARCH('${term}', LOWER({${F_ALERT.summary}}))`);
    parts.push(`SEARCH('${term}', LOWER({${MAP_INTEL.hotelProject}}))`);
  }
  if (!parts.length) return null;
  return `OR(${parts.join(",")})`;
}

async function fetchCandidateRecords(base, identity) {
  const formula = buildSearchFormula(identity);
  if (!formula) return [];

  const baseFields = [
    F_ALERT.title,
    F_ALERT.dedupeId,
    F_ALERT.summary,
    F_ALERT.sourceName,
    F_ALERT.sourceUrl,
    F_ALERT.publishedAt,
    F_ALERT.category,
    F_ALERT.tags,
    MAP_INTEL.hotelProject,
    MAP_INTEL.brandInvolved,
    MAP_INTEL.operatorInvolved,
    MAP_INTEL.ownerDeveloper,
  ];
  // Optional designed assoc field — include when present; never create schema here.
  const fieldsWithAssoc = [...baseFields, MAP_HOTEL_ASSOC.hpcHotelRecordIds];

  async function selectWith(fields, withSort) {
    const opts = {
      filterByFormula: formula,
      maxRecords: 80,
      fields,
    };
    if (withSort) opts.sort = [{ field: F_ALERT.publishedAt, direction: "desc" }];
    return base(TABLE).select(opts).all();
  }

  try {
    return await selectWith(fieldsWithAssoc, true);
  } catch (err) {
    const msg = String(err?.message || err);
    const missingAssoc = /unknown field name/i.test(msg) && /HPC Hotel Record IDs/i.test(msg);
    console.warn("[he-news] market alerts select failed", msg);
    try {
      return await selectWith(missingAssoc ? baseFields : fieldsWithAssoc, false);
    } catch (err2) {
      console.warn("[he-news] market alerts retry failed", err2?.message || err2);
      try {
        return await selectWith(baseFields, false);
      } catch (err3) {
        console.warn("[he-news] market alerts final retry failed", err3?.message || err3);
        return null;
      }
    }
  }
}

/**
 * Express handler — never writes.
 */
export async function getHotelExplorerNewsArticles(req, res) {
  const hotelId = String(req.params.hotelId || "").trim();
  if (!/^rec[a-zA-Z0-9]{14}$/.test(hotelId)) {
    return res.status(400).json({
      success: false,
      error: "invalid_hotel_id",
      version: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
    });
  }

  const limit = Math.min(Math.max(parseInt(String(req.query.limit || "5"), 10) || 5, 1), 25);
  const viewAll = String(req.query.view || "") === "all";
  const effectiveLimit = viewAll ? Math.min(limit * 5, 25) : limit;

  const hotelHints = {
    id: hotelId,
    name: req.query.name || req.query.hotelName || null,
    brand: req.query.brand || null,
    managementCompany: req.query.operator || null,
    economicOwner: req.query.economicOwner || null,
    city: req.query.city || null,
    aliases: String(req.query.aliases || "")
      .split("|")
      .map((s) => s.trim())
      .filter(Boolean),
  };

  // Prefer richer identity from body-less GET query; caller should pass name.
  const key = cacheKey(hotelId + ":" + (hotelHints.name || ""), effectiveLimit);
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < CACHE_MS) {
    res.setHeader("X-Dealality-Write", "forbidden");
    res.setHeader("Cache-Control", "private, max-age=60");
    return res.json({ ...hit.payload, cached: true });
  }

  const base = getBase();
  if (!base) {
    res.setHeader("X-Dealality-Write", "forbidden");
    return res.status(200).json({
      success: true,
      available: false,
      reason: "market_alerts_unavailable",
      version: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
      visible: false,
      articles: [],
      total: 0,
      hasMore: false,
      canonicalSource: "MarketAlerts",
    });
  }

  try {
    const identity = buildHotelNewsIdentity(hotelHints);
    const records = await fetchCandidateRecords(base, identity);
    if (records === null) {
      res.setHeader("X-Dealality-Write", "forbidden");
      return res.status(200).json({
        success: true,
        available: false,
        reason: "market_alerts_error",
        version: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
        visible: false,
        articles: [],
        total: 0,
        hasMore: false,
        canonicalSource: "MarketAlerts",
      });
    }

    const selected = selectHotelNewsArticles(identity, records, {
      limit: effectiveLimit,
      includeHistorical: true,
    });

    // Client cards omit internal match enums; evidence citations keep verification for Sources.
    const articles = (selected.articles || []).map((a) => ({
      id: a.id,
      headline: a.headline,
      publisher: a.publisher,
      publishedAt: a.publishedAt,
      category: a.category,
      relevanceSummary: a.relevanceSummary,
      supportedFact: a.supportedFact || a.relevanceSummary,
      url: a.url,
      verificationStatus: a.verificationStatus || null,
      canonicalSource: a.canonicalSource,
      dedupeKey: a.dedupeKey || null,
    }));

    const evidenceCitations = marketAlertsArticlesToCitations(selected.articles || [], {
      hotelName: selected.hotelName,
    });

    const payload = {
      success: true,
      available: true,
      version: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
      hotelId,
      hotelName: selected.hotelName,
      canonicalSource: "MarketAlerts",
      visible: selected.visible,
      total: selected.total,
      hasMore: selected.hasMore,
      articles,
      evidenceCitations,
      writeOnRead: false,
      cached: false,
    };
    cache.set(key, { at: Date.now(), payload });
    res.setHeader("X-Dealality-Write", "forbidden");
    res.setHeader("Cache-Control", "private, max-age=60");
    return res.json(payload);
  } catch (err) {
    console.warn("[he-news] handler error", err?.message || err);
    res.setHeader("X-Dealality-Write", "forbidden");
    return res.status(200).json({
      success: true,
      available: false,
      reason: "handler_error",
      version: HOTEL_NEWS_FROM_MARKET_ALERTS_V1,
      visible: false,
      articles: [],
      total: 0,
      hasMore: false,
      canonicalSource: "MarketAlerts",
    });
  }
}
