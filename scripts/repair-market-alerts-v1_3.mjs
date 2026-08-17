#!/usr/bin/env node
/**
 * V1.3 one-time quality repair — dry-run by default.
 * Usage:
 *   node scripts/repair-market-alerts-v1_3.mjs --dry-run
 *   node scripts/repair-market-alerts-v1_3.mjs --apply
 *   node scripts/repair-market-alerts-v1_3.mjs --apply --record recu9auGNA0WqrkDx
 */
import "dotenv/config";
import Airtable from "airtable";
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";
import {
  computeMarketAlertIntelligence,
  applyIntelligencePatch,
} from "../lib/market-alerts-intelligence.js";
import { patchMarketAlertTextFields } from "../api/lib/market-alerts-rss-airtable.js";
import { looksLikeHtmlMarkup } from "../lib/market-alerts-plain-text.js";
import { assessContentQuality } from "../lib/market-alerts-content-quality.js";

const TABLE = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
const apply = process.argv.includes("--apply");
const dryRun = !apply || process.argv.includes("--dry-run");
const recordArg = process.argv.find((a) => a.startsWith("--record="));
const singleRecord = recordArg ? recordArg.split("=")[1] : null;

function getBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    throw new Error("Airtable not configured");
  }
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
}

function isMalformedSummary(text) {
  const s = String(text || "");
  return (
    looksLikeHtmlMarkup(s) ||
    /<a\s/i.test(s) ||
    /news\.google\.com\/rss\/articles/i.test(s) ||
    /&lt;\/?[a-z]/i.test(s)
  );
}

async function main() {
  const base = getBase();
  const table = base(TABLE);
  const records = singleRecord
    ? [await table.find(singleRecord)]
    : await table.select({ pageSize: 100 }).all();

  const report = {
    total: records.length,
    malformedHtml: [],
    contentQualityIgnore: [],
    intelligenceRefresh: [],
    summaryRepair: [],
    errors: [],
  };

  for (const rec of records) {
    const fields = rec.fields;
    const title = fields[MAP_ALERT.title] || fields.Title || "";
    const summary = fields[MAP_ALERT.summary] || fields.Summary || "";

    const textPatch = patchMarketAlertTextFields({
      [MAP_ALERT.title]: title,
      [MAP_ALERT.summary]: summary,
      [MAP_ALERT.sourceName]: fields[MAP_ALERT.sourceName] || "",
    });

    if (isMalformedSummary(summary)) {
      report.malformedHtml.push(rec.id);
    }

    const quality = assessContentQuality({ title, summary });
    if (quality.ignore) {
      report.contentQualityIgnore.push({ id: rec.id, reason: quality.reason });
    }

    const computed = computeMarketAlertIntelligence({
      title: textPatch[MAP_ALERT.title] || title,
      summary: textPatch[MAP_ALERT.summary] || summary,
      alertId: rec.id,
    });

    const patch = { ...textPatch, ...computed.fields };
    const needsRepair =
      Object.keys(textPatch).length > 0 ||
      quality.ignore ||
      singleRecord === rec.id ||
      isMalformedSummary(summary);

    if (!needsRepair) continue;

    report.intelligenceRefresh.push(rec.id);

    if (dryRun) {
      if (Object.keys(textPatch).length) report.summaryRepair.push(rec.id);
      continue;
    }

    try {
      await applyIntelligencePatch(rec.id, patch, { dryRun: false });
    } catch (err) {
      report.errors.push({ id: rec.id, error: err.message || String(err) });
    }
  }

  console.log(JSON.stringify(report, null, 2));
  console.log(
    dryRun
      ? "Dry-run only — pass --apply to write intelligence/summary repairs."
      : "Apply complete."
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
