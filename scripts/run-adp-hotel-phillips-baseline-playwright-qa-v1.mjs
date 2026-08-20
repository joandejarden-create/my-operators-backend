#!/usr/bin/env node
/**
 * Playwright QA for Hotel Phillips first official baseline (local or PRODUCTION_BASE).
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const PROPERTY_ID = "adp_hotel_phillips_kansas_city";
const BASE =
  process.env.ADP_QA_BASE ||
  process.env.PRODUCTION_BASE ||
  "http://127.0.0.1:8080";
const expectVisible = process.env.EXPECT_DROPDOWN_VISIBLE !== "0";

async function main() {
  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    console.log(JSON.stringify({ STATUS: "FAIL", reason: "playwright_not_installed" }, null, 2));
    process.exit(1);
  }

  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
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

  const notes = [];
  let ok = true;
  const url = `${BASE}/owner-ai-demand-share.html?property=${PROPERTY_ID}&v=hpkc-baseline`;
  try {
    await page.goto(url, { waitUntil: "networkidle", timeout: 90000 });
    await page.waitForSelector("#adpKpiRow .aiv-kpi, .adp-er-summary-box, #adpTabExec", {
      timeout: 60000,
    });
    const bodyText = await page.locator("body").innerText();

    if (/Benchmark Developing/i.test(bodyText)) {
      ok = false;
      notes.push("CUSTOMER_VISIBLE_BENCHMARK_DEVELOPING");
    }
    if (/Resort Leisure/i.test(bodyText)) {
      ok = false;
      notes.push("CUSTOMER_VISIBLE_RESORT_LEISURE");
    }
    if (!/Leisure Travel/i.test(bodyText)) {
      notes.push("LEISURE_TRAVEL_NOT_VISIBLE_WARNING");
    }
    if (/Strongest Demand Territory/i.test(bodyText)) {
      ok = false;
      notes.push("STRONGEST_DEMAND_TERRITORY_IN_SNAPSHOT");
    }

    // Snapshot card count / labels
    const kpiText = await page.locator("#adpKpiRow").innerText().catch(() => "");
    const required = [
      "Property Reality Coverage",
      "Scenarios Monitored",
      "Traveler Needs Where Hotel Appeared",
      "Traveler Needs Where Hotel Was Missing",
      "Top Observed AI Alternative",
    ];
    for (const label of required) {
      if (!new RegExp(label, "i").test(kpiText) && !new RegExp(label, "i").test(bodyText)) {
        ok = false;
        notes.push(`MISSING_SNAPSHOT_CARD:${label}`);
      }
    }

    // Dropdown check on owner page if present
    const select = page.locator("select#adpPropertySelect, select[name='property'], #propertySelect");
    if ((await select.count()) > 0) {
      const options = await select.first().locator("option").allTextContents();
      const matches = options.filter((t) => /Hotel Phillips/i.test(t));
      if (expectVisible && matches.length !== 1) {
        ok = false;
        notes.push(`DROPDOWN_COUNT_${matches.length}`);
      }
      if (!expectVisible && matches.length > 0) {
        ok = false;
        notes.push("DROPDOWN_VISIBLE_BEFORE_CERTIFY");
      }
    }

    for (const width of [1366, 1440, 1920, 390]) {
      await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
      await page.waitForTimeout(200);
      const visible = await page.locator("#adpKpiRow, .adp-er-summary-box").first().isVisible();
      if (!visible) {
        ok = false;
        notes.push(`INVISIBLE_AT_${width}`);
      }
    }

    if (consoleErrors.length || pageErrors.length) {
      ok = false;
      notes.push("UNCAUGHT_JS_ERRORS");
    }
    if (failedRequests.length) {
      ok = false;
      notes.push("FAILED_CUSTOMER_API_REQUESTS");
    }
  } catch (e) {
    ok = false;
    notes.push(String(e?.message || e));
  }

  await browser.close();
  const out = {
    STATUS: ok ? "PASS" : "FAIL",
    BASE,
    PROPERTY_ID,
    UNCAUGHT_JS_ERRORS: consoleErrors.length + pageErrors.length,
    FAILED_CUSTOMER_API_REQUESTS: failedRequests.length,
    notes,
    consoleErrors: consoleErrors.slice(0, 10),
    pageErrors: pageErrors.slice(0, 10),
  };
  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(
    join(process.cwd(), "reports/ai-demand-positioning/adp-hotel-phillips-baseline-playwright.json"),
    JSON.stringify(out, null, 2) + "\n"
  );
  console.log(JSON.stringify(out, null, 2));
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
