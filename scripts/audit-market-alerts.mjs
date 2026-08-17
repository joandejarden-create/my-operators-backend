#!/usr/bin/env node
/**
 * Audit all MarketAlerts for classification, text, and data-quality issues.
 *
 * Usage:
 *   node scripts/audit-market-alerts.mjs
 *   node scripts/audit-market-alerts.mjs --json > reports/market-alerts-audit.json
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import Airtable from "airtable";
import {
  MAP_ALERT,
  ALLOWED_CATEGORIES,
  ALLOWED_REGION_GROUPS,
  inferClassificationPatch,
  inferRegionGroupFromFields,
  inferCategoryFromFields,
  patchMarketAlertTextFields,
} from "../api/lib/market-alerts-rss-airtable.js";
import { hasHtmlEntities } from "../lib/decode-html-entities.js";
import { REGION_GEO_REGEX } from "../lib/market-alerts-geo-keywords.js";

const FIELDS = [
  MAP_ALERT.title,
  MAP_ALERT.summary,
  MAP_ALERT.sourceUrl,
  MAP_ALERT.sourceName,
  MAP_ALERT.regionGroup,
  MAP_ALERT.category,
  MAP_ALERT.publishedAt,
  MAP_ALERT.tags,
];

function detectGeoRegion(text) {
  for (const { region, re } of REGION_GEO_REGEX) {
    if (re.test(text)) return region;
  }
  return null;
}

function auditRecord(rec) {
  const f = rec.fields;
  const issues = [];
  const title = f[MAP_ALERT.title] || "";
  const summary = f[MAP_ALERT.summary] || "";
  const source = f[MAP_ALERT.sourceName] || "";
  const text = `${title} ${summary} ${f[MAP_ALERT.sourceUrl] || ""}`.trim();

  const currentRegion = f[MAP_ALERT.regionGroup] || "Global";
  const currentCategory = f[MAP_ALERT.category] || "";
  const inferredRegion = inferRegionGroupFromFields(f);
  const inferredCategory = inferCategoryFromFields(f);
  const geoHit = detectGeoRegion(text);

  if (!title.trim()) issues.push({ type: "missing_title", severity: "high" });
  if (!(summary || "").trim()) issues.push({ type: "missing_summary", severity: "medium" });
  if (!f[MAP_ALERT.publishedAt]) issues.push({ type: "missing_published_at", severity: "low" });

  for (const key of [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.sourceName]) {
    if (hasHtmlEntities(f[key] || "")) {
      issues.push({ type: "html_entities", field: key, severity: "medium" });
    }
  }

  if (currentCategory && !ALLOWED_CATEGORIES.includes(currentCategory)) {
    issues.push({ type: "invalid_category", value: currentCategory, severity: "high" });
  }
  if (currentRegion && !ALLOWED_REGION_GROUPS.includes(currentRegion)) {
    issues.push({ type: "invalid_region", value: currentRegion, severity: "high" });
  }

  if (inferredRegion !== currentRegion) {
    issues.push({
      type: "region_mismatch",
      severity: "high",
      current: currentRegion,
      inferred: inferredRegion,
      geoHint: geoHit,
    });
  }

  if (inferredCategory !== currentCategory) {
    issues.push({
      type: "category_mismatch",
      severity: "medium",
      current: currentCategory,
      inferred: inferredCategory,
    });
  }

  if (currentRegion === "Global" && geoHit && geoHit !== "Global") {
    issues.push({
      type: "suspicious_global",
      severity: "high",
      geoHint: geoHit,
      inferred: inferredRegion,
    });
  }

  if (/\bopenings?\b/i.test(source) && currentCategory !== "Supply" && inferredCategory === "Supply") {
    issues.push({ type: "openings_not_supply", severity: "high" });
  }

  const textPatch = patchMarketAlertTextFields(f);
  const classPatch = inferClassificationPatch(f).patch;
  const fixPatch = { ...textPatch, ...classPatch };

  return {
    id: rec.id,
    title: title.slice(0, 100),
    source,
    region: currentRegion,
    category: currentCategory,
    issues,
    fixPatch,
  };
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const table = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const writeJson = process.argv.includes("--json");

  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const rows = [];

  await base(table)
    .select({ fields: FIELDS, pageSize: 100 })
    .eachPage((records, next) => {
      records.forEach((rec) => rows.push(auditRecord(rec)));
      next();
    });

  const issueCounts = {};
  const byType = {};
  let withIssues = 0;
  let fixable = 0;

  for (const row of rows) {
    if (row.issues.length) withIssues += 1;
    if (Object.keys(row.fixPatch).length) fixable += 1;
    for (const issue of row.issues) {
      issueCounts[issue.type] = (issueCounts[issue.type] || 0) + 1;
      if (!byType[issue.type]) byType[issue.type] = [];
      if (byType[issue.type].length < 8) {
        byType[issue.type].push({
          title: row.title,
          region: row.region,
          category: row.category,
          ...issue,
        });
      }
    }
  }

  const regionDist = {};
  const categoryDist = {};
  for (const row of rows) {
    regionDist[row.region] = (regionDist[row.region] || 0) + 1;
    categoryDist[row.category] = (categoryDist[row.category] || 0) + 1;
  }

  const report = {
    auditedAt: new Date().toISOString(),
    table,
    total: rows.length,
    withIssues,
    fixable,
    clean: rows.length - withIssues,
    issueCounts,
    regionDist,
    categoryDist,
    samples: byType,
  };

  if (writeJson) {
    const outDir = path.join(process.cwd(), "reports");
    fs.mkdirSync(outDir, { recursive: true });
    const outPath = path.join(outDir, `market-alerts-audit-${new Date().toISOString().slice(0, 10)}.json`);
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
    console.log(`Wrote ${outPath}`);
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
