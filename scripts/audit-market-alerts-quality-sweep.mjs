#!/usr/bin/env node
/**
 * Broad MarketAlerts quality sweep beyond classification parity.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import Airtable from "airtable";
import {
  MAP_ALERT,
  sanitizeMarketAlertText,
  inferRegionGroupFromFields,
  inferCategoryFromFields,
} from "../api/lib/market-alerts-rss-airtable.js";
import { hasHtmlEntities } from "../lib/decode-html-entities.js";
import {
  looksLikeHtmlMarkup,
  isJunkSummaryBlurb,
} from "../lib/market-alerts-plain-text.js";
import { assessMarketAlertRelevance } from "../lib/market-alerts-relevance.js";
import { normalizeAlertTitle, canonicalizeSourceUrl } from "../lib/market-alerts-dedupe.js";

const FIELDS = [
  MAP_ALERT.title,
  MAP_ALERT.summary,
  MAP_ALERT.sourceName,
  MAP_ALERT.sourceUrl,
  MAP_ALERT.publishedAt,
  MAP_ALERT.category,
  MAP_ALERT.regionGroup,
  MAP_ALERT.dedupeId,
];

function push(bucket, item, limit = 8) {
  if (!bucket.samples) bucket.samples = [];
  bucket.count = (bucket.count || 0) + 1;
  if (bucket.samples.length < limit) bucket.samples.push(item);
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const rows = [];

  await base(table)
    .select({ fields: FIELDS, pageSize: 100 })
    .eachPage((recs, next) => {
      recs.forEach((r) => rows.push({ id: r.id, fields: r.fields }));
      next();
    });

  const issues = {
    htmlMarkup: {},
    htmlEntities: {},
    junkBlurb: {},
    sanitizeWouldChange: {},
    missingSummary: {},
    missingTitle: {},
    missingUrl: {},
    googleWrapperUrl: {},
    irrelevantNoise: {},
    regionMismatch: {},
    categoryMismatch: {},
    duplicateTitle: {},
    duplicateUrl: {},
    titleHasHtml: {},
    oddChars: {},
  };

  const titleMap = new Map();
  const urlMap = new Map();

  for (const { id, fields: f } of rows) {
    const title = f[MAP_ALERT.title] || "";
    const summary = f[MAP_ALERT.summary] || "";
    const source = f[MAP_ALERT.sourceName] || "";
    const url = f[MAP_ALERT.sourceUrl] || "";
    const sample = {
      id,
      title: title.slice(0, 90),
      source,
      category: f[MAP_ALERT.category],
      region: f[MAP_ALERT.regionGroup],
    };

    if (!title.trim()) push(issues.missingTitle, sample);
    if (!summary.trim()) push(issues.missingSummary, sample);
    if (!url.trim()) push(issues.missingUrl, sample);
    if (looksLikeHtmlMarkup(title) || looksLikeHtmlMarkup(summary)) {
      push(issues.htmlMarkup, { ...sample, snippet: summary.slice(0, 100) });
    }
    if (/<[^>]+>/.test(title)) push(issues.titleHasHtml, sample);
    if (hasHtmlEntities(title) || hasHtmlEntities(summary) || hasHtmlEntities(source)) {
      push(issues.htmlEntities, sample);
    }
    if (isJunkSummaryBlurb(summary)) push(issues.junkBlurb, { ...sample, summary: summary.slice(0, 100) });

    const patchTitle = sanitizeMarketAlertText(title);
    const patchSummary = sanitizeMarketAlertText(summary, { preserveWhitespace: true });
    const patchSource = sanitizeMarketAlertText(source);
    if (patchTitle !== title || patchSummary !== summary || patchSource !== source) {
      push(issues.sanitizeWouldChange, {
        ...sample,
        summaryBefore: summary.slice(0, 80),
        summaryAfter: patchSummary.slice(0, 80),
      });
    }

    if (/news\.google\.com/i.test(url)) push(issues.googleWrapperUrl, sample);

    const relevance = assessMarketAlertRelevance({
      title,
      summary,
      source,
      sourceName: source,
    });
    if (!relevance.keep) {
      push(issues.irrelevantNoise, { ...sample, reason: relevance.reason });
    }

    const inferredRegion = inferRegionGroupFromFields(f);
    const inferredCategory = inferCategoryFromFields(f);
    const currentRegion = f[MAP_ALERT.regionGroup] || "Global";
    const currentCategory = f[MAP_ALERT.category] || "";
    if (inferredRegion !== currentRegion) {
      push(issues.regionMismatch, {
        ...sample,
        current: currentRegion,
        inferred: inferredRegion,
      });
    }
    if (inferredCategory !== currentCategory) {
      push(issues.categoryMismatch, {
        ...sample,
        current: currentCategory,
        inferred: inferredCategory,
      });
    }

    if (/[�]|â€™|â€œ|â€|Ã©|ï¿½/.test(`${title} ${summary}`)) {
      push(issues.oddChars, { ...sample, snippet: `${title} ${summary}`.slice(0, 100) });
    }

    const tk = normalizeAlertTitle(title);
    if (tk && tk.length >= 24) {
      if (!titleMap.has(tk)) titleMap.set(tk, []);
      titleMap.get(tk).push(sample);
    }
    const uk = canonicalizeSourceUrl(url);
    if (uk) {
      if (!urlMap.has(uk)) urlMap.set(uk, []);
      urlMap.get(uk).push(sample);
    }
  }

  for (const [, group] of titleMap) {
    if (group.length > 1) {
      push(issues.duplicateTitle, { count: group.length, titles: group.map((g) => g.title) }, 10);
    }
  }
  for (const [url, group] of urlMap) {
    if (group.length > 1) {
      push(
        issues.duplicateUrl,
        { count: group.length, url: url.slice(0, 90), titles: group.map((g) => g.title) },
        10
      );
    }
  }

  const summary = {
    auditedAt: new Date().toISOString(),
    total: rows.length,
    issueCounts: Object.fromEntries(
      Object.entries(issues).map(([k, v]) => [k, v.count || 0])
    ),
    samples: Object.fromEntries(
      Object.entries(issues)
        .filter(([, v]) => (v.count || 0) > 0)
        .map(([k, v]) => [k, v.samples || []])
    ),
  };

  const outDir = path.join(process.cwd(), "reports");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    `market-alerts-quality-sweep-${new Date().toISOString().slice(0, 10)}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
  console.log(`Wrote ${outPath}`);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
