#!/usr/bin/env node
/**
 * Playwright QA for ADP Official Baseline Period 001 (local or PRODUCTION_BASE URL).
 *
 * Usage:
 *   node scripts/run-adp-official-baseline-period-001-playwright-qa-v1.mjs
 *   PRODUCTION_BASE=https://my-operators-backend-production.up.railway.app node scripts/...
 */

import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import {
  ADP_PROPERTY_IDS_V1,
  OFFICIAL_BASELINE_PERIOD_MARKER,
  MEASUREMENT_CONTRACT_VERSION,
} from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const BASE =
  process.env.PRODUCTION_BASE ||
  process.env.ADP_QA_BASE ||
  "http://127.0.0.1:8080";
const isProduction = /railway\.app|dealality\.com/i.test(BASE);

async function main() {
  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    const out = { STATUS: "FAIL", reason: "playwright_not_installed" };
    console.log(JSON.stringify(out, null, 2));
    process.exit(1);
  }

  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
  const tests = [];
  let uncaught = 0;
  let failedReq = 0;
  let brokenClicks = 0;
  let developingVisible = 0;
  let strongestInSnapshot = 0;
  let mobileOverflowP2 = 0;

  for (const propertyId of ADP_PROPERTY_IDS_V1) {
    const page = await browser.newPage();
    const consoleErrors = [];
    const pageErrors = [];
    const failedRequests = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });
    page.on("pageerror", (err) => pageErrors.push(String(err)));
    page.on("requestfailed", (req) => {
      const url = req.url();
      if (/\/api\/ai-demand|owner-ai-demand/i.test(url)) failedRequests.push(url);
    });

    const url = `${BASE}/owner-ai-demand-share.html?property=${propertyId}&v=baseline001`;
    const notes = [];
    let ok = true;
    try {
      await page.goto(url, { waitUntil: "networkidle", timeout: 90000 });
      await page.waitForSelector("#adpKpiRow .aiv-kpi, .adp-er-summary-box", { timeout: 60000 });

      const bodyText = await page.locator("body").innerText();
      if (/Benchmark Developing/i.test(bodyText)) {
        developingVisible += 1;
        ok = false;
        notes.push("CUSTOMER_VISIBLE_BENCHMARK_DEVELOPING");
      }
      const snap = await page.locator("#adpKpiRow").innerText().catch(() => "");
      if (/Strongest Demand Territory/i.test(snap)) {
        strongestInSnapshot += 1;
        ok = false;
        notes.push("STRONGEST_DEMAND_TERRITORY_IN_SNAPSHOT");
      }

      // Basic click targets
      for (const sel of ["#adpTabExec", ".adp-section-executive-read", "#adpKpiRow"]) {
        const el = page.locator(sel).first();
        if ((await el.count()) === 0) {
          brokenClicks += 1;
          ok = false;
          notes.push(`missing:${sel}`);
        }
      }

      await page.setViewportSize({ width: 390, height: 844 });
      const overflow = await page.evaluate(() => {
        const doc = document.documentElement;
        return doc.scrollWidth > doc.clientWidth + 2;
      });
      if (overflow) {
        mobileOverflowP2 += 1;
        notes.push("h_overflow_mobile");
      }

      if (consoleErrors.length || pageErrors.length) {
        uncaught += consoleErrors.length + pageErrors.length;
        ok = false;
        notes.push("console_or_page_errors");
      }
      if (failedRequests.length) {
        failedReq += failedRequests.length;
        ok = false;
        notes.push("failed_api");
      }
    } catch (err) {
      ok = false;
      notes.push(String(err).slice(0, 200));
    }

    tests.push({
      propertyId,
      ok,
      notes,
      consoleErrors: consoleErrors.slice(0, 5),
      pageErrors: pageErrors.slice(0, 5),
      failedRequests: failedRequests.slice(0, 5),
    });
    await page.close();
  }

  await browser.close();

  const passed = tests.filter((t) => t.ok).length;
  const out = {
    title: "ADP_OFFICIAL_BASELINE_PERIOD_001_PLAYWRIGHT_QA_V1",
    BASE,
    isProduction,
    baselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    finished: new Date().toISOString(),
    TESTS: `${passed}/${tests.length}`,
    STATUS: passed === tests.length ? "PASS" : "FAIL",
    UNCAUGHT_JS_ERRORS: uncaught,
    FAILED_CUSTOMER_API_REQUESTS: failedReq,
    BROKEN_CLICK_TARGETS: brokenClicks,
    CUSTOMER_VISIBLE_BENCHMARK_DEVELOPING: developingVisible,
    CUSTOMER_VISIBLE_STRONGEST_DEMAND_TERRITORY_IN_SNAPSHOT: strongestInSnapshot,
    P2_MOBILE_OVERFLOW: mobileOverflowP2,
    tests,
  };

  const outPath = join(
    process.cwd(),
    isProduction
      ? "reports/ai-demand-positioning/adp-official-baseline-period-001-playwright-production-qa-v1.json"
      : "reports/ai-demand-positioning/adp-official-baseline-period-001-playwright-qa-v1.json"
  );
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify({ STATUS: out.STATUS, TESTS: out.TESTS, outPath, BASE }, null, 2));
  process.exit(out.STATUS === "PASS" ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
