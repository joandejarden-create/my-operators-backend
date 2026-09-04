#!/usr/bin/env node
/**
 * Playwright: BAI Wave 4 internal QA visuals + 4-parent customer share regression.
 *
 * Internal QA: serve static files + mock internal-longitudinal-qa (no auth).
 * Share regression: production Marriott/Hilton/Choice/IHG fingerprints.
 */
import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { buildBaiWave4LongitudinalPresentationV1 } from "../lib/ai-visibility/brand-longitudinal/bai-wave4-longitudinal-presentation-v1.js";
import { buildBaiWave3FullCohortReconciliationV1 } from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(ROOT, "reports", "bai-wave4-longitudinal-qa");
fs.mkdirSync(outDir, { recursive: true });

const production = process.argv.includes("--production");
const PROD = "https://my-operators-backend-production.up.railway.app";

const SHARE_TOKENS = {
  marriott:
    process.env.BAI_MARRIOTT_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfZDIyMTlkNzlmNjU1Mjk3MGJjNTgyZTU4IiwicGFyZW50Q29tcGFueUlkIjoibWFycmlvdHQiLCJzdXJmYWNlcyI6WyJyZXBvcnQiLCJldmlkZW5jZSIsInBvcnRmb2xpbyIsImV4ZWN1dGl2ZV9zdW1tYXJ5Il0sInJlcG9ydFNjb3BlIjoiY3VycmVudF9wdWJsaXNoZWQiLCJpYXQiOjE3ODg0MTAwNDMsImV4cCI6bnVsbH0.6iUKi6nmlhOpL0oWsx3BQyx36t747_1UHcigoIcPrTw",
  hilton:
    process.env.BAI_HILTON_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfNWRlNDI4ZDdkMjg0MDU0Yjg3NTZjZjBhIiwicGFyZW50Q29tcGFueUlkIjoiaGlsdG9uIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4NDEwMDQzLCJleHAiOm51bGx9.Z3e_o3ODbKOrVB9Nz1zSAHU3Zj6TOR0JTzJ6q4BTnEM",
  choice:
    process.env.BAI_CHOICE_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfYmFhNGU0MDgzNzZlZjhkY2IwMzMxYzVlIiwicGFyZW50Q29tcGFueUlkIjoiY2hvaWNlIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4NDEwMDQzLCJleHAiOm51bGx9.rj370Pew8tQGhIjni_POVQWjhQYamWD6MOWKyUHiVSo",
  ihg:
    process.env.BAI_IHG_SHARE ||
    "baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfZTUwN2Y4MTY5Mzc0ZjAxMTI5ZWI5OTA4IiwicGFyZW50Q29tcGFueUlkIjoiaWhnIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzg4NDEwMDQzLCJleHAiOm51bGx9.-7pEnvoWF2LhiFadMCyDTi86qXqJ6h_OU-VlJfMaHIk",
};

const viewports = [1600, 1440, 1280, 1024, 768, 390];
const parents = ["marriott", "hilton", "choice", "ihg"];

const wave3 = buildBaiWave3FullCohortReconciliationV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
});
const wave4 = buildBaiWave4LongitudinalPresentationV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  scope: "full_cohort",
  parentCompanyName: "all",
});

const apiPayload = {
  success: true,
  ok: wave3.ok && wave4.ok,
  accessClass: "INTERNAL_LONGITUDINAL_QA",
  SHARE_CAPABILITY_FORBIDDEN: true,
  PERIOD_2_PUBLICATION_STATE: "UNPROMOTED",
  scope: "full_cohort",
  wave: 4,
  ...wave3,
  wave4,
  LIVE_PROVIDER_CALLS: 0,
  PROVIDER_CALLS: 0,
  ANALYTICAL_CONTRACT_CHANGES: "NONE",
};

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  return "application/octet-stream";
}

function startStaticServer() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    if (url.pathname === "/api/ai-visibility/brand/internal-longitudinal-qa") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(apiPayload));
      return;
    }
    let rel = url.pathname === "/" ? "/brand-ai-longitudinal-qa.html" : url.pathname;
    // Strip cache-bust query — pathname already excludes search, but be safe.
    rel = rel.split("?")[0];
    const filePath = path.join(ROOT, "public", rel.replace(/^\//, ""));
    if (!filePath.startsWith(path.join(ROOT, "public")) || !fs.existsSync(filePath)) {
      // Also allow /css from public/css
      const alt = path.join(ROOT, rel.replace(/^\//, ""));
      if (fs.existsSync(alt) && alt.startsWith(ROOT)) {
        res.writeHead(200, { "Content-Type": contentType(alt) });
        res.end(fs.readFileSync(alt));
        return;
      }
      res.writeHead(404);
      res.end("not found: " + rel);
      return;
    }
    res.writeHead(200, { "Content-Type": contentType(filePath) });
    res.end(fs.readFileSync(filePath));
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({ server, base: `http://127.0.0.1:${port}` });
    });
  });
}

const results = {
  wave4Ok: wave4.ok,
  gates: wave4.gates,
  internal: [],
  share: [],
  thirtySecond: {},
};

const { server, base } = await startStaticServer();
const browser = await chromium.launch({ headless: true });

try {
  for (const parent of parents) {
    for (const width of viewports) {
      const page = await browser.newPage({ viewport: { width, height: 900 } });
      await page.route("**/dealality-memberstack-auth.js*", (route) =>
        route.fulfill({
          status: 200,
          contentType: "application/javascript",
          body: "window.DealalityMemberstackAuth={authFetch:(u,o)=>fetch(u,o)};",
        })
      );
      await page.goto(`${base}/brand-ai-longitudinal-qa.html`, {
        waitUntil: "domcontentloaded",
        timeout: 60000,
      });
      await page.waitForFunction(
        () => {
          const shell = document.getElementById("baiW4Shell");
          const status = document.getElementById("baiQaStatus");
          return (
            (shell && !shell.hidden) ||
            (status && /Blocked|Error|failed/i.test(status.textContent || ""))
          );
        },
        { timeout: 30000 }
      );
      const statusText = await page.textContent("#baiQaStatus");
      if (/Blocked|Error|failed/i.test(statusText || "")) {
        throw new Error(`Wave4 QA load failed: ${statusText}`);
      }
      await page.selectOption("#baiW4ParentSelect", parent);
      await page.waitForTimeout(400);
      const probe = await page.evaluate((viewportWidth) => {
        const text = document.body.innerText || "";
        const docOverflow =
          document.documentElement.scrollWidth >
          document.documentElement.clientWidth + 2;
        const tableScroller = document.querySelector(
          ".deals-table-container, [data-bai-w4='brand-table']"
        )?.closest(".deals-table-container");
        const containedTableScroll =
          !!tableScroller &&
          getComputedStyle(tableScroller).overflowX !== "visible";
        // Mobile: allow horizontal scroll only inside table containers.
        const overflowX =
          viewportWidth <= 599
            ? docOverflow && !containedTableScroll
            : docOverflow;
        return {
          hasExec: !!document
            .querySelector('[data-bai-w4="executive-read"]')
            ?.textContent?.trim(),
          hasTrend: !!document.querySelector('[data-bai-w4="trend-card"]'),
          hasProvider: !!document.querySelector('[data-bai-w4="provider-card"]'),
          hasBrandTable:
            document.querySelectorAll('[data-bai-w4="brand-table"] tbody tr')
              .length > 0,
          hasCompetitive: !!document
            .querySelector('[data-bai-w4="competitive-story"]')
            ?.textContent?.trim(),
          hasIntent: /not yet comparable/i.test(
            document.querySelector('[data-bai-w4="intent-state"]')
              ?.textContent || ""
          ),
          hasPrimaryKpi: !!document.querySelector(".bai-w4-kpi--primary"),
          overflowX,
        };
      }, width);
      const shot = path.join(outDir, `internal-${parent}-${width}.png`);
      await page.screenshot({ path: shot, fullPage: false });
      results.internal.push({ parent, width, shot, ...probe });
      await page.close();
    }
    const parentRows = results.internal.filter((r) => r.parent === parent);
    results.thirtySecond[parent] =
      parentRows.every(
        (r) =>
          r.hasExec &&
          r.hasBrandTable &&
          r.hasCompetitive &&
          r.hasIntent &&
          r.hasPrimaryKpi &&
          !r.overflowX
      )
        ? "YES"
        : "NO";
  }

  // Customer share regression (production when --production or always for isolation)
  const shareBase = production ? PROD : PROD;
  for (const parent of parents) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const token = SHARE_TOKENS[parent];
    await page.goto(
      `${shareBase}/brand-ai-visibility-share.html?share=${encodeURIComponent(token)}`,
      { waitUntil: "domcontentloaded", timeout: 60000 }
    );
    try {
      await page.waitForFunction(
        () => (document.body?.innerText || "").includes("Portfolio Position"),
        { timeout: 45000 }
      );
    } catch {
      /* continue */
    }
    await page.waitForTimeout(600);
    const probe = await page.evaluate(() => {
      const text = document.body.innerText || "";
      return {
        hasAug14: /Aug 14,\s*2026|2026-08-14/.test(text),
        period2Leak: /20260902|Period 2|d3d713|Prior Run Position|Wave 4/i.test(text),
        hasWave4Controls: !!document.querySelector("#baiW4ParentSelect"),
      };
    });
    const shot = path.join(outDir, `share-${parent}-1440.png`);
    await page.screenshot({ path: shot, fullPage: false });
    results.share.push({ parent, shot, ...probe });
    await page.close();
  }
} finally {
  await browser.close();
  server.close();
}

results.summary = {
  internalPass: results.internal.every(
    (r) =>
      r.hasExec &&
      r.hasBrandTable &&
      r.hasCompetitive &&
      r.hasIntent &&
      r.hasPrimaryKpi &&
      !r.overflowX
  ),
  sharePass: results.share.every(
    (r) => r.hasAug14 && !r.period2Leak && !r.hasWave4Controls
  ),
  thirtySecond: results.thirtySecond,
};

fs.writeFileSync(
  path.join(outDir, "playwright-summary.json"),
  JSON.stringify(results, null, 2)
);
fs.writeFileSync(
  path.join(outDir, "known-good-visual-contract.json"),
  JSON.stringify(
    {
      gates: wave4.gates,
      parents: wave4.parents.map((p) => ({
        parentCompanyKey: p.parentCompanyKey,
        brandCount: p.brandMovement.rows.length,
        trendMode: p.trend.chartMode,
        intent: p.ownerIntent.comparabilityState,
        providerComparability: p.provider.comparabilityState,
      })),
      screenshots: results.internal.map((r) => ({
        parent: r.parent,
        width: r.width,
        shot: path.basename(r.shot),
      })),
    },
    null,
    2
  )
);

const pass = results.summary.internalPass && results.summary.sharePass && wave4.ok;
console.log(JSON.stringify(results.summary, null, 2));
console.log(pass ? "PLAYWRIGHT WAVE4 PASS" : "PLAYWRIGHT WAVE4 FAIL");
process.exit(pass ? 0 : 1);
