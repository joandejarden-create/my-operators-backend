#!/usr/bin/env node
/**
 * Pre-publish smoke for BAI Period 2 customer publication.
 * Payload-level + optional local Playwright visual smoke.
 * STOP on P0/P1. No publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { buildBaiCustomerPromotionPreviewV1 } from "../lib/ai-visibility/brand-longitudinal/bai-customer-longitudinal-payload-v1.js";
import { buildBaiWave3FullCohortReconciliationV1 } from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(ROOT, "reports/bai-p2-promotion-readiness/pre-publish-smoke");
fs.mkdirSync(outDir, { recursive: true });

const parents = ["marriott", "hilton", "choice", "ihg"];
const viewports = [1600, 1440, 1024, 390];
const expectedCounts = { marriott: 5, hilton: 4, choice: 5, ihg: 5 };

let failed = 0;
function fail(msg) {
  failed += 1;
  console.error("  FAIL", msg);
}
function pass(msg) {
  console.log("  PASS", msg);
}

const fpPath = path.join(
  ROOT,
  "reports/bai-p2-promotion-readiness/hypothetical-customer-fingerprints.json"
);
const fp = JSON.parse(fs.readFileSync(fpPath, "utf8"));
const w3 = buildBaiWave3FullCohortReconciliationV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
});

if (w3.ok && w3.matrix?.length === 19 && (fp.brands19?.length === 19 || fp.brands19)) {
  pass("BAI_P2_EXPECTED_CUSTOMER_FINGERPRINT_READY freeze");
} else {
  fail("fingerprint freeze / 19-brand recon");
}

if (
  w3.periodResolve?.priorPeriodDate === "2026-08-14" &&
  w3.periodResolve?.currentPeriodDate === "2026-09-03"
) {
  pass("fingerprint dates Sep3/Aug14");
} else {
  fail("fingerprint dates");
}

for (const parent of parents) {
  const preview = buildBaiCustomerPromotionPreviewV1({
    parentCompanyName: parent,
    scope: "parent_filter",
  });
  const pv = preview.parents?.[0];
  const blob = JSON.stringify(pv || {});
  if (!preview.ok || !pv) {
    fail(`${parent} preview ok`);
    continue;
  }
  if (pv.brandMovement.rows.length !== expectedCounts[parent]) {
    fail(`${parent} brand count`);
  } else pass(`${parent} brand count ${expectedCounts[parent]}`);
  if (pv.currentDate !== "2026-09-03" || pv.priorDate !== "2026-08-14") {
    fail(`${parent} dates`);
  } else pass(`${parent} dates`);
  if (blob.includes("2026-08-18")) fail(`${parent} Aug18 leak`);
  else pass(`${parent} no Aug18`);
  if (!pv.trend || pv.trend.points?.length !== 2) fail(`${parent} trends`);
  else pass(`${parent} trends`);
  if (pv.provider?.showPriorDeltaColumns !== false) fail(`${parent} provider cols`);
  else pass(`${parent} provider disclosure`);
  if (!/Intent-level change is not yet comparable/i.test(pv.ownerIntent?.presentation || "")) {
    fail(`${parent} intent disclosure`);
  } else pass(`${parent} intent disclosure`);
  if (!pv.disclosures?.cohortChange) fail(`${parent} cohort disclosure`);
  else pass(`${parent} cohort disclosure`);
  if (/Period\s*2|candidate|internal QA/i.test(pv.executiveRead?.narrative || "")) {
    fail(`${parent} internal terminology`);
  } else pass(`${parent} customer-safe exec`);
}

async function visualSmoke() {
  const previewByParent = Object.fromEntries(
    parents.map((p) => [
      p,
      buildBaiCustomerPromotionPreviewV1({
        parentCompanyName: p,
        scope: "parent_filter",
      }),
    ])
  );

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    if (url.pathname === "/api/ai-visibility/brand/customer-promotion-preview") {
      const parent = url.searchParams.get("parent") || "marriott";
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, ...previewByParent[parent] }));
      return;
    }
    if (url.pathname === "/api/ai-visibility/brand/executive-summary") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          success: true,
          currentPosition: { portfolioAiPresence: { display: "—" } },
          portfolioOverview: { brands: [] },
          customerLongitudinal: null,
        })
      );
      return;
    }
    let filePath = path.join(ROOT, "public", url.pathname === "/" ? "ai-visibility-brand.html" : url.pathname);
    if (!fs.existsSync(filePath) && url.pathname.startsWith("/js/")) {
      filePath = path.join(ROOT, "public", url.pathname);
    }
    if (!fs.existsSync(filePath)) {
      res.writeHead(404);
      res.end("missing");
      return;
    }
    let body = fs.readFileSync(filePath);
    if (filePath.endsWith(".html")) {
      // Stub auth so local smoke can load without Memberstack
      let html = body.toString("utf8");
      html = html.replace(
        /\/js\/dealality-memberstack-auth\.js[^"]*/g,
        "/js/ai-visibility/ai-visibility-shared.js"
      );
      body = Buffer.from(html);
    }
    const ct = filePath.endsWith(".html")
      ? "text/html"
      : filePath.endsWith(".js")
        ? "application/javascript"
        : filePath.endsWith(".css")
          ? "text/css"
          : "application/octet-stream";
    res.writeHead(200, { "Content-Type": ct });
    res.end(body);
  });

  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const port = server.address().port;
  const browser = await chromium.launch({ headless: true });
  try {
    for (const parent of parents) {
      for (const width of viewports) {
        const page = await browser.newPage({ viewport: { width, height: 900 } });
        await page.goto(
          `http://127.0.0.1:${port}/ai-visibility-brand.html?baiPromotionPreview=1&parent=${parent}`,
          { waitUntil: "domcontentloaded", timeout: 60000 }
        );
        // Inject render directly if auth blocks full load
        await page.evaluate((payload) => {
          if (window.BaiCustomerLongitudinal) {
            window.BaiCustomerLongitudinal.render(payload, {
              previewMode: true,
              parentKey: payload.parents?.[0]?.parentCompanyKey,
            });
          }
        }, previewByParent[parent]);
        await page.waitForTimeout(400);
        const text = await page.locator("#aivCustomerLongitudinal").innerText().catch(() => "");
        const shot = path.join(outDir, `${parent}-${width}.png`);
        await page.screenshot({ path: shot, fullPage: false });
        if (!text.includes("2026-08-14") && !text.includes("Aug")) {
          // dates may render as ISO
          if (!text.includes("2026-09-03") && !text.includes("Prior")) {
            fail(`visual ${parent}@${width} missing prior/current cues`);
          } else pass(`visual ${parent}@${width}`);
        } else {
          if (text.includes("2026-08-18")) fail(`visual ${parent}@${width} Aug18`);
          else pass(`visual ${parent}@${width}`);
        }
        if (text.includes("2026-08-18")) fail(`visual DOM Aug18 ${parent}@${width}`);
        await page.close();
      }
    }
  } finally {
    await browser.close();
    server.close();
  }
}

await visualSmoke().catch((err) => {
  fail(`visual smoke: ${err.message}`);
});

fs.writeFileSync(
  path.join(outDir, "smoke-summary.json"),
  JSON.stringify(
    {
      failed,
      stopPublication: failed > 0,
      BAI_P2_EXPECTED_CUSTOMER_FINGERPRINT_READY: failed === 0 ? "PASS" : "FAIL",
    },
    null,
    2
  )
);

console.log(`\nPre-publish smoke: ${failed === 0 ? "CLEAR" : "BLOCKED"} (${failed} failures)`);
process.exit(failed ? 1 : 0);
