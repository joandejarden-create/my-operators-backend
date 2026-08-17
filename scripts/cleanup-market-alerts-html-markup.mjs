#!/usr/bin/env node
/**
 * Strip HTML/CMS markup from MarketAlerts Title/Summary/Source Name.
 * Optionally re-enrich emptied summaries from article pages.
 *
 * Usage:
 *   node scripts/cleanup-market-alerts-html-markup.mjs --dry-run
 *   node scripts/cleanup-market-alerts-html-markup.mjs
 *   node scripts/cleanup-market-alerts-html-markup.mjs --enrich
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  MAP_ALERT,
  sanitizeMarketAlertText,
} from "../api/lib/market-alerts-rss-airtable.js";
import { looksLikeHtmlMarkup } from "../lib/market-alerts-plain-text.js";
import { fetchArticleSummary } from "../lib/rss-article-summary-enrich.js";

function parseArgs(argv) {
  return {
    dryRun: argv.includes("--dry-run"),
    enrich: argv.includes("--enrich"),
  };
}

function isWeakSummary(title, summary) {
  const t = (title || "").trim().toLowerCase();
  const s = (summary || "").trim();
  if (!s) return true;
  if (s.length < 40) return true;
  if (t && s.toLowerCase().startsWith(t.slice(0, Math.min(40, t.length))) && s.length < t.length + 40) {
    return true;
  }
  return false;
}

async function main() {
  const { dryRun, enrich } = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const candidates = [];

  await base(table)
    .select({
      fields: [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName, MAP_ALERT.sourceUrl],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const rec of records) {
        const f = rec.fields;
        const patch = {};
        for (const key of [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName]) {
          const val = f[key];
          if (typeof val !== "string" || !val) continue;
          const preserveWhitespace = key === MAP_ALERT.summary;
          const cleaned = sanitizeMarketAlertText(val, { preserveWhitespace });
          if (cleaned !== val) patch[key] = cleaned;
        }

        const nextTitle = patch[MAP_ALERT.title] ?? f[MAP_ALERT.title] ?? "";
        let nextSummary = patch[MAP_ALERT.summary] ?? f[MAP_ALERT.summary] ?? "";
        if (Object.prototype.hasOwnProperty.call(patch, MAP_ALERT.summary) && isWeakSummary(nextTitle, nextSummary)) {
          patch[MAP_ALERT.summary] = "";
          nextSummary = "";
        }

        if (Object.keys(patch).length) {
          candidates.push({
            id: rec.id,
            fields: patch,
            title: nextTitle,
            sourceUrl: f[MAP_ALERT.sourceUrl] || "",
            needsEnrich: enrich && isWeakSummary(nextTitle, nextSummary),
            summaryBefore: (f[MAP_ALERT.summary] || "").slice(0, 120),
            summaryAfter: nextSummary.slice(0, 120),
            hadMarkup: looksLikeHtmlMarkup(f[MAP_ALERT.summary] || ""),
          });
        }
      }
      next();
    });

  let enriched = 0;
  if (enrich && !dryRun) {
    for (const row of candidates) {
      if (!row.needsEnrich || !row.sourceUrl) continue;
      const summary = await fetchArticleSummary(row.sourceUrl);
      if (summary && summary.length >= 40) {
        row.fields[MAP_ALERT.summary] = summary;
        row.summaryAfter = summary.slice(0, 120);
        enriched += 1;
      }
    }
  }

  console.log(
    JSON.stringify(
      {
        table,
        dryRun,
        enrich,
        toUpdate: candidates.length,
        wouldEnrich: candidates.filter((c) => c.needsEnrich).length,
        enriched,
        samples: candidates.slice(0, 8).map((c) => ({
          id: c.id,
          title: (c.title || "").slice(0, 80),
          hadMarkup: c.hadMarkup,
          summaryBefore: c.summaryBefore,
          summaryAfter: c.summaryAfter,
        })),
      },
      null,
      2
    )
  );

  if (dryRun || !candidates.length) return;

  let updated = 0;
  const errors = [];
  for (let i = 0; i < candidates.length; i += 10) {
    const batch = candidates.slice(i, i + 10);
    try {
      await base(table).update(batch.map((r) => ({ id: r.id, fields: r.fields })));
      updated += batch.length;
    } catch (err) {
      errors.push({ batchStart: i, message: err.message || String(err) });
    }
  }
  console.log(JSON.stringify({ updated, enriched, errors }, null, 2));
  if (errors.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
