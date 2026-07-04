import { runMarketAlertsRssSync } from "./run-market-alerts-rss-sync.js";

let intervalId = null;
let running = false;

async function tick() {
  if (running) {
    console.warn("[market-alerts-rss-scheduler] skipped: previous run still in progress");
    return;
  }

  running = true;
  try {
    const result = await runMarketAlertsRssSync();
    console.log(
      "[market-alerts-rss-scheduler] done",
      JSON.stringify({
        fetched: result.fetched,
        created: result.created,
        skipped: result.skipped,
        invalid: result.invalid,
      })
    );
  } catch (err) {
    console.error("[market-alerts-rss-scheduler] failed:", err.message || err);
  } finally {
    running = false;
  }
}

/**
 * Opt-in in-process scheduler (single server instance).
 * Enable with MARKET_ALERTS_RSS_SYNC_ENABLED=true
 */
export function startMarketAlertsRssScheduler() {
  if (process.env.MARKET_ALERTS_RSS_SYNC_ENABLED !== "true") {
    return;
  }

  const minutes = parseInt(process.env.MARKET_ALERTS_RSS_SYNC_INTERVAL_MINUTES || "240", 10);
  const intervalMs = Math.max(minutes, 15) * 60 * 1000;

  if (intervalId) clearInterval(intervalId);
  intervalId = setInterval(tick, intervalMs);

  console.log(
    `[market-alerts-rss-scheduler] enabled — every ${Math.max(minutes, 15)} minutes (limit ${process.env.RSS_TOTAL_TARGET || process.env.MARKET_ALERTS_RSS_SYNC_LIMIT || 100})`
  );

  if (process.env.MARKET_ALERTS_RSS_SYNC_ON_STARTUP === "true") {
    tick();
  }
}

export function stopMarketAlertsRssScheduler() {
  if (intervalId) {
    clearInterval(intervalId);
    intervalId = null;
  }
}
