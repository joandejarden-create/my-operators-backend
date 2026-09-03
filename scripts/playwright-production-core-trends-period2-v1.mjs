#!/usr/bin/env node
/**
 * Production Playwright: Core Trends P2 on Railway HTTPS + existing share tokens.
 * npm run playwright:production-core-trends-period2-v1
 */

import { chromium } from "playwright";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

const inv = JSON.parse(
  readFileSync(
    "reports/client-share-links/PRODUCTION_CLIENT_SHARE_LINK_INVENTORY_2026-09-03T04-34-03.json",
    "utf8"
  )
);
const WIDTHS = [1440, 1024, 390];
const OUT = join(process.cwd(), "reports/ai-demand-positioning/core-trends-period2-wiring");

async function audit(page, link) {
  await page.goto(link.shareUrl, { waitUntil: "networkidle", timeout: 90000 });
  try {
    page.once("dialog", (d) => d.dismiss());
  } catch {
    /* ignore */
  }
  await page.waitForSelector("#adpTrendSummary", { timeout: 60000 });
  const ui = await page.evaluate(() => {
    const summary = document.getElementById("adpTrendSummary");
    const text = summary ? summary.innerText : "";
    const periodCard = Array.from(document.querySelectorAll(".aiv-detail-trend-stat")).find((el) =>
      /Periods/i.test(el.querySelector(".aiv-detail-trend-stat__label")?.innerText || "")
    );
    const periodCount = periodCard
      ? Number(periodCard.querySelector(".aiv-detail-trend-stat__value")?.innerText || "0")
      : 0;
    return {
      periodCount,
      awaiting: /Awaiting next comparable period/i.test(text),
      summaryText: text.slice(0, 400),
      chartHidden: document.getElementById("adpTrendChartWrap")?.hidden === true,
      jsSrc: document.querySelector('script[src*="ai-demand-positioning.js"]')?.src || null,
    };
  });
  const share = new URL(link.shareUrl).searchParams.get("share");
  const report = await page.evaluate(
    async ({ propertyId, share }) => {
      const res = await fetch(
        `/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report?share=${encodeURIComponent(share)}&_cb=${Date.now()}`,
        { cache: "no-store" }
      );
      return res.json();
    },
    { propertyId: link.propertyId, share }
  );
  const trends = report.trends || [];
  const prior = report.executiveMetrics?.currentVsPrior;
  return {
    propertyId: link.propertyId,
    ui,
    trendsLen: trends.length,
    trendDates: trends.map((t) => String(t.date || "").slice(0, 10)),
    periodId: report.period?.periodId || null,
    priorReady: Boolean(prior?.priorComparablePeriodId),
    pass:
      ui.periodCount === 2 &&
      !ui.awaiting &&
      trends.length === 2 &&
      Boolean(prior?.priorComparablePeriodId) &&
      !ui.chartHidden &&
      String(ui.jsSrc || "").includes("adp-v76-20260903-trends-p2"),
  };
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const rows = [];
  for (const width of WIDTHS) {
    const context = await browser.newContext({
      viewport: { width, height: 900 },
      // fresh context = no shared cache / cookies
    });
    const page = await context.newPage();
    for (const link of inv.adpLinks || []) {
      const row = await audit(page, link);
      rows.push({ width, ...row });
      console.log(
        JSON.stringify({
          width,
          propertyId: row.propertyId,
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
  const outPath = join(OUT, `playwright-production-core-trends-period2-${Date.now()}.json`);
  writeFileSync(outPath, JSON.stringify({ pass, publicBase: inv.publicBase, rows }, null, 2));
  console.log(JSON.stringify({ pass, outPath, failCount: rows.filter((r) => !r.pass).length }, null, 2));
  if (!pass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
