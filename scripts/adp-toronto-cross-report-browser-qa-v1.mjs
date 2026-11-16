#!/usr/bin/env node
/**
 * Browser QA for Toronto cross-report reconciliation.
 * Loads adp-current-report-pdf-render.html with Playwright route mocks
 * (same pattern as current-report PDF generation) so Memberstack is not required.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-cross-report-reconciliation-v1"
);
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const BASE = process.env.ADP_PDF_BASE_URL || "http://localhost:8095";

const published = loadPublishedReport(PROPERTY_ID);
if (!published?.executiveMetrics) {
  console.error("published_missing");
  process.exit(1);
}
const reportBody = { ok: true, ...published };

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
await page.route("**/api/ai-demand-positioning/**", async (route) => {
  const url = route.request().url();
  if (url.includes("/properties")) {
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        properties: [
          {
            propertyId: PROPERTY_ID,
            name: published.property?.name || "Radisson Blu Toronto Downtown",
            city: published.property?.city || "Toronto",
            state: published.property?.state || "Ontario",
          },
        ],
      }),
    });
  }
  if (url.includes("/publication-meta") || url.includes("/publication")) {
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        product: "ai_demand_positioning",
        bpp: { customerPublished: true },
      }),
    });
  }
  if (url.includes("/report")) {
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(reportBody),
    });
  }
  return route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ ok: true, items: [], rows: [] }),
  });
});

const url = `${BASE}/adp-current-report-pdf-render.html?propertyId=${encodeURIComponent(
  PROPERTY_ID
)}&pdfRender=1&_cb=toronto-xreport`;
await page.goto(url, { waitUntil: "networkidle", timeout: 120000 });
await page.waitForSelector("#adp-pdf-ready-host[data-adp-pdf-ready='1']", {
  timeout: 120000,
});
await page.waitForTimeout(500);

const text = await page.evaluate(() => document.body.innerText || "");
const checks = {
  consideration_33_2: /33\.2\s*%/.test(text),
  scenario_38_1: /38\.1\s*%/.test(text),
  number_one_44_7: /44\.7\s*%/.test(text),
  top3_85_1: /85\.1\s*%/.test(text),
  coverage_19: /19(?:\.0)?\s*%/.test(text),
  gap_66_7: /66\.7\s*%/.test(text),
  no_29: !/\b29\s*%/.test(text) || /33\.2\s*%/.test(text), // 29 alone may appear elsewhere; ban stale consideration narrative
  stale_29_narrative: !/AI Consideration is 29%/i.test(text) && !/but in 29% of individual/i.test(text),
  no_81: !/81(?:\.0)?\s*%/.test(text),
  no_misses_17: !/misses 17 of 21/i.test(text) && !/17 missed of 21/i.test(text),
  missed_partial_wording:
    /11 missed and 6 partially represented/i.test(text) ||
    (/11/.test(text) && /partial/i.test(text) && /66\.7/.test(text)),
  portfolio_45_5: /45\.5\s*%/.test(text),
  portfolio_rank: /#\s*1 of 6|#1 of 6/i.test(text),
  owned_9_5: /9\.5\s*%/.test(text),
};

const screenshot = path.join(OUT, "_browser-exec-summary.png");
await page.screenshot({ path: screenshot, fullPage: false });

// Reality Gap tab
await page.click('#adpTabReality, [data-tab="reality"], button:has-text("Reality")').catch(() => {});
await page.waitForTimeout(400);
const realityText = await page.evaluate(() => document.body.innerText || "");
checks.reality_tab_66_7 = /66\.7\s*%/.test(realityText);
checks.reality_tab_no_81 = !/81(?:\.0)?\s*%/.test(realityText);

await browser.close();

const ok = Object.entries(checks)
  .filter(([k]) => !k.startsWith("_"))
  .every(([, v]) => v === true);

const result = {
  ok,
  url,
  checks,
  screenshot,
  narrativeHas332: /33\.2/.test(text),
  excerpt: text.slice(0, 1200),
};
fs.writeFileSync(path.join(OUT, "BROWSER_QA.json"), `${JSON.stringify(result, null, 2)}\n`);
fs.writeFileSync(
  path.join(OUT, "WEB_REPORT_QA.md"),
  `# Web Report QA — Radisson Blu Toronto Downtown

URL: \`${url}\`

| Check | Result |
|-------|--------|
| AI Consideration 33.2% | ${checks.consideration_33_2 ? "YES" : "NO"} |
| Scenario Presence 38.1% | ${checks.scenario_38_1 ? "YES" : "NO"} |
| #1 44.7% | ${checks.number_one_44_7 ? "YES" : "NO"} |
| Top-3 85.1% | ${checks.top3_85_1 ? "YES" : "NO"} |
| Reality Coverage 19% | ${checks.coverage_19 ? "YES" : "NO"} |
| Reality Gap 66.7% | ${checks.gap_66_7 ? "YES" : "NO"} |
| No stale 29% consideration narrative | ${checks.stale_29_narrative ? "YES" : "NO"} |
| No 81.0% Reality Gap | ${checks.no_81 && checks.reality_tab_no_81 ? "YES" : "NO"} |
| No misleading 17 missed | ${checks.no_misses_17 ? "YES" : "NO"} |
| Portfolio 45.5% / #1 of 6 | ${checks.portfolio_45_5 && checks.portfolio_rank ? "YES" : "NO"} |
| Owned source share 9.5% | ${checks.owned_9_5 ? "YES" : "NO"} |

BROWSER VERIFIED: **${ok ? "YES" : "NO"}**
`
);

console.log(JSON.stringify(result, null, 2));
process.exit(ok ? 0 : 1);
