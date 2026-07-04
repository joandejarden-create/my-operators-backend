import { fetchMarketAlertsRssItems } from "./market-alerts-news.js";
import { syncRssItemsToAirtable } from "./lib/market-alerts-rss-airtable.js";
import { enrichRssItemsSummaries } from "../lib/rss-article-summary-enrich.js";

function parseLimit(value, fallback = 100) {
  const n = parseInt(String(value ?? fallback), 10);
  return Math.min(Math.max(Number.isFinite(n) ? n : fallback, 1), 250);
}

/**
 * Fetch hospitality RSS and upsert new rows into MarketAlerts.
 * @param {{ limit?: number, dryRun?: boolean }} [opts]
 */
export async function runMarketAlertsRssSync({ limit, dryRun = false } = {}) {
  const resolvedLimit = parseLimit(
    limit ?? process.env.RSS_TOTAL_TARGET ?? process.env.MARKET_ALERTS_RSS_SYNC_LIMIT ?? 100
  );
  const rawItems = await fetchMarketAlertsRssItems({ limit: resolvedLimit });
  const { items, enriched, attempted } = await enrichRssItemsSummaries(rawItems);
  const result = await syncRssItemsToAirtable({
    items,
    dryRun: dryRun || process.env.DRY_RUN === "true",
  });
  return { ...result, limit: resolvedLimit, summaryEnrichment: { enriched, attempted } };
}

function isAuthorizedCronRequest(req) {
  const expected =
    process.env.MARKET_ALERTS_RSS_CRON_SECRET || process.env.CRON_SECRET || "";
  if (!expected) return false;
  const provided =
    req.headers["x-cron-secret"] ||
    req.headers["authorization"]?.replace(/^Bearer\s+/i, "") ||
    req.query?.secret;
  return Boolean(provided && provided === expected);
}

/** POST/GET /api/cron/market-alerts-rss-sync — for Railway Cron, GitHub Actions, etc. */
export async function cronMarketAlertsRssSync(req, res) {
  if (!isAuthorizedCronRequest(req)) {
    return res.status(401).json({
      success: false,
      error: "Unauthorized",
      hint: "Set MARKET_ALERTS_RSS_CRON_SECRET and pass x-cron-secret header or ?secret=",
    });
  }

  try {
    const dryRun = String(req.query?.dryRun || "").toLowerCase() === "true";
    const result = await runMarketAlertsRssSync({
      limit: req.query?.limit,
      dryRun,
    });

    if (!result.ok && result.error) {
      return res.status(503).json({ success: false, ...result });
    }

    if (result.createErrors?.length) {
      return res.status(500).json({ success: false, ...result });
    }

    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[cronMarketAlertsRssSync]", err);
    return res.status(500).json({
      success: false,
      error: err.message || "RSS sync failed",
    });
  }
}
