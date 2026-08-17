import { fetchMarketAlertsRssItems } from "./market-alerts-news.js";
import { syncRssItemsToAirtable } from "./lib/market-alerts-rss-airtable.js";
import { enrichRssItemsSummaries } from "../lib/rss-article-summary-enrich.js";
import { resolveGoogleNewsArticleUrl } from "../lib/market-alerts-dedupe.js";
import { maybeRunEarlySignalProductionSync } from "../lib/market-alerts-early-signal-schedule.js";

function parseLimit(value, fallback = 100) {
  const n = parseInt(String(value ?? fallback), 10);
  return Math.min(Math.max(Number.isFinite(n) ? n : fallback, 1), 250);
}

async function resolveGoogleNewsLinks(items, { concurrency = 4 } = {}) {
  const out = items.map((item) => ({ ...item }));
  const idxs = out
    .map((item, i) => (/news\.google\.com/i.test(item.link || "") ? i : -1))
    .filter((i) => i >= 0);

  let resolved = 0;
  for (let start = 0; start < idxs.length; start += concurrency) {
    const batch = idxs.slice(start, start + concurrency);
    const results = await Promise.all(
      batch.map(async (i) => {
        const next = await resolveGoogleNewsArticleUrl(out[i].link);
        return { i, next };
      })
    );
    for (const { i, next } of results) {
      if (next && next !== out[i].link && !/news\.google\.com/i.test(next)) {
        out[i].link = next;
        resolved += 1;
      }
    }
  }
  return { items: out, resolved, attempted: idxs.length };
}

/**
 * Fetch hospitality RSS and upsert new rows into MarketAlerts.
 * Pre-publish path: fetch → resolve Google URLs → enrich summaries → quality gate → Airtable.
 * @param {{ limit?: number, dryRun?: boolean }} [opts]
 */
export async function runMarketAlertsRssSync({ limit, dryRun = false } = {}) {
  const resolvedLimit = parseLimit(
    limit ?? process.env.RSS_TOTAL_TARGET ?? process.env.MARKET_ALERTS_RSS_SYNC_LIMIT ?? 100
  );
  const rawItems = await fetchMarketAlertsRssItems({ limit: resolvedLimit });
  const { items: withDirectLinks, resolved, attempted: resolveAttempted } =
    await resolveGoogleNewsLinks(rawItems);
  const { items, enriched, attempted } = await enrichRssItemsSummaries(withDirectLinks);
  const result = await syncRssItemsToAirtable({
    items,
    dryRun: dryRun || process.env.DRY_RUN === "true",
  });

  let earlySignals = null;
  if (!dryRun && process.env.DRY_RUN !== "true") {
    try {
      earlySignals = await maybeRunEarlySignalProductionSync({ dryRun: false });
    } catch (err) {
      console.error("[market-alerts-rss-sync] early signal production failed:", err.message || err);
      earlySignals = { ok: false, error: err.message || String(err) };
    }
  }

  return {
    ...result,
    limit: resolvedLimit,
    summaryEnrichment: { enriched, attempted },
    googleNewsResolve: { resolved, attempted: resolveAttempted },
    earlySignals,
  };
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
