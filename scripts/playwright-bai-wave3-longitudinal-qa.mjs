#!/usr/bin/env node
/**
 * Playwright: BAI Wave 3 internal longitudinal QA + Marriott share regression.
 * Usage:
 *   node scripts/playwright-bai-wave3-longitudinal-qa.mjs
 *   node scripts/playwright-bai-wave3-longitudinal-qa.mjs --production
 */
import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildBaiWave3LongitudinalIntelligenceV1,
  BAI_WAVE3_MARRIOTT_BRAND_IDS,
} from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(ROOT, "reports", "bai-wave3-longitudinal-qa");
fs.mkdirSync(outDir, { recursive: true });

const production = process.argv.includes("--production");
const BASE = production
  ? "https://my-operators-backend-production.up.railway.app"
  : process.env.BAI_QA_BASE || "http://127.0.0.1:3000";

const MARRIOTT_SHARE =
  process.env.BAI_MARRIOTT_SHARE ||
  "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfZDIyMTlkNzlmNjU1Mjk3MGJjNTgyZTU4IiwicGFyZW50Q29tcGFueUlkIjoibWFycmlvdHQiLCJzdXJmYWNlcyI6WyJyZXBvcnQiLCJldmlkZW5jZSIsInBvcnRmb2xpbyIsImV4ZWN1dGl2ZV9zdW1tYXJ5Il0sInJlcG9ydFNjb3BlIjoiY3VycmVudF9wdWJsaXNoZWQiLCJpYXQiOjE3ODg0MTAwNDMsImV4cCI6bnVsbH0.6iUKi6nmlhOpL0oWsx3BQyx36t747_1UHcigoIcPrTw";

const widths = [1440, 1024, 390];
const results = { internalCorpus: null, share: [], leak: null };

// Offline corpus QA (always)
const intel = buildBaiWave3LongitudinalIntelligenceV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  parentCompanyName: "Marriott",
  brandIds: BAI_WAVE3_MARRIOTT_BRAND_IDS,
});
results.internalCorpus = {
  ok: intel.ok,
  brands: (intel.brands || []).map((b) => ({
    brandId: b.brandId,
    brandName: b.brandName,
    deltaDisplay: b.deltaDisplay,
    rankDisplay: b.rankDisplay,
    absolute: b.absoluteRelative?.absolutePerformance,
    relative: b.absoluteRelative?.relativePerformance,
    membershipState: b.membershipState,
  })),
  portfolioDelta: intel.portfolio?.portfolioDeltaDisplay || null,
  narrativeSnippet: (intel.executiveLongitudinal?.narrative || "").slice(0, 240),
};
fs.writeFileSync(
  path.join(outDir, "internal-corpus-marriott.json"),
  JSON.stringify(results.internalCorpus, null, 2)
);

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();

for (const w of widths) {
  await page.setViewportSize({ width: w, height: 900 });
  const url = `${BASE}/brand-ai-visibility-share.html?share=${encodeURIComponent(MARRIOTT_SHARE)}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
  try {
    await page.waitForFunction(
      () => {
        const text = document.body?.innerText || "";
        return (
          text.includes("Portfolio Position") &&
          (text.includes("91.7%") || text.includes("Aug 14"))
        );
      },
      { timeout: 45000 }
    );
  } catch {
    /* continue with probe */
  }
  await page.waitForTimeout(500);
  const shot = path.join(outDir, `marriott-share-${w}.png`);
  await page.screenshot({ path: shot, fullPage: false });
  const probe = await page.evaluate(() => {
    const text = document.body.innerText || "";
    return {
      hasAug14: /Aug 14,\s*2026|Aug 14, 2026/.test(text) || text.includes("2026-08-14"),
      period2Leak: /20260902|Period 2|d3d713|Prior Run Position/i.test(text),
      presence: (text.match(/91\.7%/) || [])[0] || null,
      kpiInfo: document.querySelectorAll('#aivExecPosition [data-bai-kpi-info="1"]').length,
      infoIcons: document.querySelectorAll("#aivExecPosition .info-icon").length,
      parent: document.getElementById("aivParentCompanyName")?.textContent || null,
      cssHref:
        [...document.querySelectorAll('link[rel=stylesheet]')]
          .map((l) => l.href)
          .find((h) => h.includes("ai-visibility-shared.css")) || null,
    };
  });
  results.share.push({ width: w, shot, ...probe });
}

results.leak = {
  anyPeriod2: results.share.some((r) => r.period2Leak),
  allAug14: results.share.every((r) => r.hasAug14),
  kpiIconsAt1440: results.share.find((r) => r.width === 1440)?.infoIcons ?? 0,
  // Icons require deployed Wave 3 assets; corpus + leak gates are hard fail.
  iconsDeployed:
    (results.share.find((r) => r.width === 1440)?.cssHref || "").includes(
      "bai-wave3-longitudinal"
    ) || false,
};

fs.writeFileSync(path.join(outDir, "playwright-summary.json"), JSON.stringify(results, null, 2));
await browser.close();

const fail =
  !results.internalCorpus.ok ||
  results.leak.anyPeriod2 ||
  !results.leak.allAug14 ||
  (results.leak.iconsDeployed && results.leak.kpiIconsAt1440 < 5);

console.log(JSON.stringify(results, null, 2));
console.log(fail ? "PLAYWRIGHT FAIL" : "PLAYWRIGHT PASS");
process.exit(fail ? 1 : 0);
