#!/usr/bin/env node
/**
 * Market Alerts cleanup batch:
 * 1) Delete rows that fail the pre-publish quality gate (noise / markup / junk)
 * 2) Fix Hilton shared-URL collision (keep the roundup that matches the URL)
 * 3) Resolve Google News wrapper URLs and re-enrich summaries when possible
 *
 * Usage:
 *   node scripts/cleanup-market-alerts-publish-batch.mjs --dry-run
 *   node scripts/cleanup-market-alerts-publish-batch.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MAP_ALERT,
  sanitizeMarketAlertText,
} from "../api/lib/market-alerts-rss-airtable.js";
import { assessMarketAlertPublishReady } from "../lib/market-alerts-publish-gate.js";
import {
  canonicalizeSourceUrl,
  resolveGoogleNewsArticleUrl,
} from "../lib/market-alerts-dedupe.js";
import { fetchArticleSummary } from "../lib/rss-article-summary-enrich.js";

const HILTON_SHARED_URL =
  "https://stories.hilton.com/releases/hilton-ends-2025-with-robust-luxury-and-lifestyle-growth-across-the-caribbean-and-latin-america";

const HILTON_KEEP_TITLE_RE = /70\+\s*hotel deals across caribbean and latin america/i;

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

async function destroyIds(base, table, ids, dryRun) {
  if (!ids.length) return { deleted: 0 };
  if (dryRun) return { deleted: ids.length, dryRun: true };
  let deleted = 0;
  for (let i = 0; i < ids.length; i += 10) {
    const batch = ids.slice(i, i + 10);
    await base(table).destroy(batch);
    deleted += batch.length;
  }
  return { deleted };
}

async function main() {
  const { dryRun } = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const rows = [];
  await base(table)
    .select({
      fields: [
        MAP_ALERT.title,
        MAP_ALERT.summary,
        MAP_ALERT.sourceName,
        MAP_ALERT.sourceUrl,
      ],
      pageSize: 100,
    })
    .eachPage((recs, next) => {
      recs.forEach((r) => rows.push({ id: r.id, fields: r.fields }));
      next();
    });

  const deleteIds = [];
  const deleteSamples = [];
  const hiltonCollision = [];
  const googleRows = [];

  for (const row of rows) {
    const f = row.fields;
    const title = f[MAP_ALERT.title] || "";
    const url = (f[MAP_ALERT.sourceUrl] || "").trim();

    const gate = assessMarketAlertPublishReady({
      title,
      summary: f[MAP_ALERT.summary],
      source: f[MAP_ALERT.sourceName],
      link: url,
    });
    if (!gate.ok) {
      deleteIds.push(row.id);
      if (deleteSamples.length < 12) {
        deleteSamples.push({
          id: row.id,
          title: title.slice(0, 90),
          reasons: gate.reasons,
        });
      }
      continue;
    }

    if (
      canonicalizeSourceUrl(url) === canonicalizeSourceUrl(HILTON_SHARED_URL) ||
      /hilton-ends-2025-with-robust-luxury-and-lifestyle-growth/i.test(url)
    ) {
      hiltonCollision.push(row);
    }
    if (/news\.google\.com/i.test(url)) {
      googleRows.push(row);
    }
  }

  // Hilton: keep the roundup title that matches the URL; delete sibling mis-links.
  const hiltonDelete = [];
  const hiltonKeep = [];
  for (const row of hiltonCollision) {
    const title = row.fields[MAP_ALERT.title] || "";
    if (HILTON_KEEP_TITLE_RE.test(title)) hiltonKeep.push(row.id);
    else hiltonDelete.push(row.id);
  }
  // If keep title missing, keep the oldest-looking first and delete the rest.
  if (!hiltonKeep.length && hiltonCollision.length) {
    hiltonKeep.push(hiltonCollision[0].id);
    for (const row of hiltonCollision.slice(1)) hiltonDelete.push(row.id);
  }
  for (const id of hiltonDelete) {
    if (!deleteIds.includes(id)) deleteIds.push(id);
  }

  // Resolve Google News wrappers + enrich.
  const googleUpdates = [];
  let googleResolved = 0;
  let googleEnriched = 0;
  for (const row of googleRows) {
    if (deleteIds.includes(row.id)) continue;
    const oldUrl = row.fields[MAP_ALERT.sourceUrl] || "";
    const nextUrl = dryRun ? oldUrl : await resolveGoogleNewsArticleUrl(oldUrl);
    const patch = {};
    if (nextUrl && nextUrl !== oldUrl && !/news\.google\.com/i.test(nextUrl)) {
      patch[MAP_ALERT.sourceUrl] = nextUrl.slice(0, 1000);
      googleResolved += 1;
    }
    const enrichUrl = patch[MAP_ALERT.sourceUrl] || oldUrl;
    if (!dryRun && enrichUrl && !/news\.google\.com/i.test(enrichUrl)) {
      const summary = await fetchArticleSummary(enrichUrl);
      const cleaned = sanitizeMarketAlertText(summary || "", { preserveWhitespace: true });
      if (cleaned && cleaned.length >= 40) {
        patch[MAP_ALERT.summary] = cleaned;
        googleEnriched += 1;
      }
    }
    if (Object.keys(patch).length) {
      googleUpdates.push({ id: row.id, fields: patch, title: row.fields[MAP_ALERT.title] });
    }
  }

  console.log(
    JSON.stringify(
      {
        table,
        dryRun,
        scanned: rows.length,
        wouldDelete: deleteIds.length,
        deleteSamples,
        hilton: {
          collisionRows: hiltonCollision.length,
          keep: hiltonKeep,
          delete: hiltonDelete,
        },
        googleNews: {
          wrappers: googleRows.length,
          wouldUpdate: googleUpdates.length,
          resolved: googleResolved,
          enriched: googleEnriched,
          samples: googleUpdates.slice(0, 5).map((u) => ({
            id: u.id,
            title: String(u.title || "").slice(0, 80),
            fields: u.fields,
          })),
        },
      },
      null,
      2
    )
  );

  if (dryRun) return;

  const del = await destroyIds(base, table, deleteIds, false);
  let updated = 0;
  const errors = [];
  for (let i = 0; i < googleUpdates.length; i += 10) {
    const batch = googleUpdates.slice(i, i + 10);
    try {
      await base(table).update(batch.map((r) => ({ id: r.id, fields: r.fields })));
      updated += batch.length;
    } catch (err) {
      errors.push({ batchStart: i, message: err.message || String(err) });
    }
  }

  console.log(JSON.stringify({ deleted: del.deleted, updated, googleResolved, googleEnriched, errors }, null, 2));
  if (errors.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
