#!/usr/bin/env node
/**
 * Deep quality review beyond classification parity (audit-market-alerts.mjs).
 * Surfaces: default-Global stories, possible missed geo, duplicates, text anomalies.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import Airtable from "airtable";
import {
  MAP_ALERT,
  inferRegionGroupFromFields,
} from "../api/lib/market-alerts-rss-airtable.js";
import { REGION_GEO_REGEX } from "../lib/market-alerts-geo-keywords.js";
import { hasHtmlEntities } from "../lib/decode-html-entities.js";

const GLOBAL_SIGNAL_RE =
  /\b(global (?:hotel|hospitality|travel|tourism|market|trend|outlook|industry|revpar|demand|supply|pipeline|shift|survey|report|study|index|workforce)|world(?:'s|wide)|world-wide|around the world|across (?:the )?(?:world|regions|markets)|all regions|multi-?region|pan-?regional|cross-?border|international (?:hotel|hospitality|chain|group)|industry-?wide|world hotel|hotel industry(?: as a whole)?|regions besides|every region|no single market|world's boutique|world's hotel)\b/i;

const FIELDS = [
  MAP_ALERT.title,
  MAP_ALERT.summary,
  MAP_ALERT.sourceUrl,
  MAP_ALERT.sourceName,
  MAP_ALERT.regionGroup,
  MAP_ALERT.category,
  MAP_ALERT.publishedAt,
  MAP_ALERT.dedupeId,
];

function detectGeoRegion(text) {
  for (const { region, re } of REGION_GEO_REGEX) {
    if (re.test(text)) return region;
  }
  return null;
}

function hasWeirdChars(s) {
  return /[\uFFFD\u0000-\u0008\u000B\u000C\u000E-\u001F]|[\uD800-\uDFFF]/.test(s || "");
}

function normTitle(s) {
  return (s || "").trim().toLowerCase().replace(/\s+/g, " ");
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const base = new Airtable({ apiKey }).base(baseId);
  const rows = [];

  await base(table)
    .select({ fields: FIELDS, pageSize: 100 })
    .eachPage((records, next) => {
      records.forEach((rec) => rows.push({ id: rec.id, fields: rec.fields }));
      next();
    });

  const titleMap = new Map();
  const dedupeMap = new Map();
  const defaultGlobal = [];
  const explicitGlobal = [];
  const shortSummary = [];
  const weirdText = [];
  const brandSamples = [];

  for (const { id, fields: f } of rows) {
    const title = f[MAP_ALERT.title] || "";
    const summary = f[MAP_ALERT.summary] || "";
    const text = `${title} ${summary} ${f[MAP_ALERT.sourceUrl] || ""}`.trim();
    const region = f[MAP_ALERT.regionGroup] || "Global";
    const geoHit = detectGeoRegion(text);
    const hasGlobalSignal = GLOBAL_SIGNAL_RE.test(text);
    const inferred = inferRegionGroupFromFields(f);

    const nt = normTitle(title);
    if (nt) {
      if (!titleMap.has(nt)) titleMap.set(nt, []);
      titleMap.get(nt).push({ id, title: title.slice(0, 80) });
    }

    const dedupe = f[MAP_ALERT.dedupeId];
    if (dedupe) {
      if (!dedupeMap.has(dedupe)) dedupeMap.set(dedupe, []);
      dedupeMap.get(dedupe).push(id);
    }

    if (region === "Global") {
      if (hasGlobalSignal) {
        explicitGlobal.push({ title: title.slice(0, 90), source: f[MAP_ALERT.sourceName] });
      } else if (!geoHit) {
        defaultGlobal.push({
          title: title.slice(0, 90),
          source: f[MAP_ALERT.sourceName],
          category: f[MAP_ALERT.category],
          inferred,
        });
      }
    }

    if ((summary || "").trim().length > 0 && (summary || "").trim().length < 40) {
      shortSummary.push({ title: title.slice(0, 70), len: summary.trim().length });
    }
    if (!(summary || "").trim()) {
      shortSummary.push({ title: title.slice(0, 70), len: 0, missing: true });
    }

    for (const key of [MAP_ALERT.title, MAP_ALERT.summary]) {
      const v = f[key] || "";
      if (hasWeirdChars(v) || /â€™|â€œ|â€|Ã©|Ã¯|Ã¼|ï¿½/.test(v)) {
        weirdText.push({ field: key, title: title.slice(0, 60), snippet: v.slice(0, 80) });
      }
    }

    if (f[MAP_ALERT.category] === "Brand") {
      brandSamples.push({ title: title.slice(0, 90), region, source: f[MAP_ALERT.sourceName] });
    }
  }

  const dupTitles = [...titleMap.entries()].filter(([, v]) => v.length > 1);
  const dupDedupe = [...dedupeMap.entries()].filter(([, v]) => v.length > 1);

  const report = {
    auditedAt: new Date().toISOString(),
    total: rows.length,
    global: {
      total: rows.filter((r) => (r.fields[MAP_ALERT.regionGroup] || "Global") === "Global").length,
      explicitGlobalSignal: explicitGlobal.length,
      defaultGlobalNoGeo: defaultGlobal.length,
      defaultGlobalSamples: defaultGlobal.slice(0, 15),
      explicitGlobalSamples: explicitGlobal.slice(0, 8),
    },
    duplicates: {
      titleGroups: dupTitles.length,
      titleRecords: dupTitles.reduce((n, [, v]) => n + v.length, 0),
      titleSamples: dupTitles.slice(0, 5).map(([t, v]) => ({ title: t.slice(0, 60), count: v.length })),
      dedupeGroups: dupDedupe.length,
    },
    summaries: {
      missingOrShort: shortSummary.length,
      missing: shortSummary.filter((x) => x.missing).length,
      shortSamples: shortSummary.slice(0, 10),
    },
    text: {
      weirdEncoding: weirdText.length,
      weirdSamples: weirdText.slice(0, 8),
      htmlEntities: rows.filter((r) =>
        [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName].some((k) =>
          hasHtmlEntities(r.fields[k] || "")
        )
      ).length,
    },
    brandCategory: {
      count: brandSamples.length,
      samples: brandSamples.slice(0, 10),
    },
  };

  const outDir = path.join(process.cwd(), "reports");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `market-alerts-deep-audit-${new Date().toISOString().slice(0, 10)}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(`Wrote ${outPath}`);
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
