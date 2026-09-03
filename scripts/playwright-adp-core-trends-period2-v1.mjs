#!/usr/bin/env node
/**
 * Playwright: Core ADP Trends Period-2 for all five properties.
 * npm run playwright:adp-core-trends-period2-v1
 */

import { chromium } from "playwright";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";

const BASE = process.env.ADP_PLAYWRIGHT_BASE || "http://127.0.0.1:8080";
const WIDTHS = [1440, 1024, 390];
const OUT = join(process.cwd(), "reports/ai-demand-positioning/core-trends-period2-wiring");

async function auditProperty(page, propertyId) {
  await page.goto(`${BASE}/owner-ai-demand-share.html?property=${encodeURIComponent(propertyId)}`, {
    waitUntil: "networkidle",
    timeout: 60000,
  });
  await page.waitForSelector("#adpTrendSummary", { timeout: 45000 });
  // dismiss possible login dialogs
  try {
    page.once("dialog", (d) => d.dismiss());
  } catch {
    /* ignore */
  }
  const result = await page.evaluate(() => {
    const summary = document.getElementById("adpTrendSummary");
    const text = summary ? summary.innerText : "";
    const periodCard = Array.from(document.querySelectorAll(".aiv-detail-trend-stat")).find((el) =>
      /Periods/i.test(el.querySelector(".aiv-detail-trend-stat__label")?.innerText || "")
    );
    const periodCount = periodCard
      ? Number(periodCard.querySelector(".aiv-detail-trend-stat__value")?.innerText || "0")
      : 0;
    const awaiting = /Awaiting next comparable period/i.test(text);
    const chart = window.adpTrendChart || null;
    // Chart instance is module-local; inspect canvas labels via DOM + KPI text
    const dates = Array.from(document.querySelectorAll(".aiv-detail-trend-stat")).map((el) =>
      el.innerText
    );
    return {
      periodCount,
      awaiting,
      summaryText: text.slice(0, 500),
      hasChartCanvas: Boolean(document.getElementById("adpTrendChart")),
      chartHidden: document.getElementById("adpTrendChartWrap")?.hidden === true,
      dates,
      chartPresent: Boolean(chart),
    };
  });

  // Pull report JSON for metric reconciliation
  const report = await page.evaluate(async (pid) => {
    const res = await fetch(`/api/ai-demand-positioning/property/${encodeURIComponent(pid)}/report`);
    return res.json();
  }, propertyId);

  const trends = report.trends || [];
  const prior = report.executiveMetrics?.currentVsPrior;
  return {
    propertyId,
    ui: result,
    trendsLen: trends.length,
    trendDates: trends.map((t) => String(t.date || "").slice(0, 10)),
    priorReady: Boolean(prior?.priorComparablePeriodId),
    priorPeriodId: prior?.priorComparablePeriodId || null,
    deltas: prior?.deltas || null,
    pass:
      result.periodCount === 2 &&
      !result.awaiting &&
      trends.length === 2 &&
      Boolean(prior?.priorComparablePeriodId) &&
      !result.chartHidden,
  };
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const rows = [];
  for (const width of WIDTHS) {
    const context = await browser.newContext({ viewport: { width, height: 900 } });
    const page = await context.newPage();
    for (const propertyId of ADP_CERTIFIED_PROPERTY_IDS) {
      const row = await auditProperty(page, propertyId);
      rows.push({ width, ...row });
      console.log(
        JSON.stringify({
          width,
          propertyId,
          pass: row.pass,
          periodCount: row.ui.periodCount,
          trendsLen: row.trendsLen,
          awaiting: row.ui.awaiting,
        })
      );
    }
    await context.close();
  }
  await browser.close();

  const pass = rows.every((r) => r.pass);
  mkdirSync(OUT, { recursive: true });
  const outPath = join(OUT, `playwright-core-trends-period2-${Date.now()}.json`);
  writeFileSync(outPath, JSON.stringify({ pass, rows }, null, 2));
  console.log(JSON.stringify({ pass, outPath, failCount: rows.filter((r) => !r.pass).length }, null, 2));
  if (!pass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
