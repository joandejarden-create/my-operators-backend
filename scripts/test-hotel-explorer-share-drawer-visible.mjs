#!/usr/bin/env node
/**
 * Production smoke: Hotel Explorer share-mode Research Center drawer
 * must open on-screen (right-side fixed panel), not clipped off-viewport.
 *
 * Usage:
 *   npm run test:hotel-explorer-share-drawer-visible
 *   BASE_URL=https://... node scripts/test-hotel-explorer-share-drawer-visible.mjs
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASE_URL = (
  process.env.BASE_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

const HOTELS = Object.freeze([
  { slug: "kgpv", id: "recUNycnMwOVFX0hc" },
  { slug: "cambridge", id: "recIwaP1etgx2g9nA" },
  { slug: "sheraton", id: "recsYJb2R1jarPpK3" },
  { slug: "voco", id: "recTYaiA4S6fR6ixx" },
]);

const OUT_DIR = path.join(
  ROOT,
  "reports",
  "hotel-intelligence",
  "share-drawer-visible"
);

function hotelUrl(hotelId) {
  const q = new URLSearchParams({
    autopen: "1",
    share: "1",
    hotelExplorer: hotelId,
    hexTab: "ownership",
    _cb: "drawerVis",
  });
  return `${BASE_URL}/hotel-intelligence-golden-demo.html?${q.toString()}`;
}

function assertDrawerVisible(box, viewport) {
  assert.ok(box, "drawer bounding box missing");
  assert.ok(box.width > 200, `drawer width ${box.width} <= 200 (clipped?)`);
  assert.ok(box.height > 200, `drawer height ${box.height} <= 200 (clipped?)`);
  assert.ok(
    box.x < viewport.width,
    `drawer left ${box.x} is past viewport width ${viewport.width}`
  );
  assert.ok(
    box.x + box.width > viewport.width * 0.5,
    `drawer right ${box.x + box.width} does not extend past mid-viewport (not right-side)`
  );
  assert.ok(
    box.x > -20,
    `drawer left ${box.x} is offscreen left`
  );
  assert.ok(
    box.x + box.width > 40,
    `drawer barely intersects viewport (clipped off right)`
  );
}

async function assertDrawerCss(drawer) {
  const css = await drawer.evaluate((el) => {
    const s = getComputedStyle(el);
    return {
      position: s.position,
      zIndex: s.zIndex,
      transform: s.transform,
    };
  });
  assert.equal(css.position, "fixed", `expected position fixed, got ${css.position}`);
  const z = parseInt(css.zIndex, 10);
  assert.ok(
    Number.isFinite(z) && z >= 14000,
    `expected zIndex >= 14000, got ${css.zIndex}`
  );
  return css;
}

async function waitDrawerOpen(page) {
  const drawer = page.locator("#hiResearchDrawer");
  await drawer.waitFor({ state: "attached", timeout: 30000 });
  await page.waitForFunction(
    () => {
      const el = document.getElementById("hiResearchDrawer");
      return !!(el && el.classList.contains("is-open"));
    },
    null,
    { timeout: 30000 }
  );
  return drawer;
}

async function assertDrawerClosed(page) {
  await page.waitForFunction(
    () => {
      const el = document.getElementById("hiResearchDrawer");
      if (!el) return true;
      if (!el.classList.contains("is-open")) return true;
      const t = getComputedStyle(el).transform || "";
      // matrix(1, 0, 0, 1, tx, 0) with large positive tx ~= translateX(100%)
      if (/matrix\(/.test(t)) {
        const parts = t.slice(7, -1).split(",").map((x) => Number(x.trim()));
        if (parts.length === 6 && parts[4] > 100) return true;
      }
      if (/translateX\(100%\)/.test(t)) return true;
      return false;
    },
    null,
    { timeout: 15000 }
  );
}

async function runHotel(page, hotel) {
  const url = hotelUrl(hotel.id);
  console.log(`\n==> ${hotel.slug} (${hotel.id})`);
  console.log(`    ${url}`);
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });

  const researchBtn = page.getByRole("button", { name: /Research Reports/i });
  await researchBtn.waitFor({ state: "visible", timeout: 90000 });
  await researchBtn.click();

  const drawer = await waitDrawerOpen(page);
  const viewport = page.viewportSize() || { width: 1440, height: 900 };
  const box = await drawer.boundingBox();
  assertDrawerVisible(box, viewport);
  await assertDrawerCss(drawer);

  const bodyText = await page.locator("body").innerText();
  assert.ok(
    /Full Hotel Intelligence|Completed Reports/i.test(bodyText),
    'page text missing "Full Hotel Intelligence" or "Completed Reports"'
  );

  const viewBtn = page.getByRole("button", { name: /View Report/i }).first();
  await viewBtn.waitFor({ state: "visible", timeout: 30000 });
  const pdfBtn = page.getByRole("button", { name: /Download PDF/i }).first();
  await pdfBtn.waitFor({ state: "visible", timeout: 30000 });

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const shotPath = path.join(OUT_DIR, `${hotel.slug}-open.png`);
  await page.screenshot({ path: shotPath, fullPage: false });
  console.log(`    screenshot ${shotPath}`);

  await page.locator("#hiResearchClose").click();
  await assertDrawerClosed(page);
  console.log("    closed ok");

  await researchBtn.click();
  await waitDrawerOpen(page);
  const box2 = await drawer.boundingBox();
  assertDrawerVisible(box2, viewport);
  await assertDrawerCss(drawer);
  console.log("    reopen ok");

  return { hotel: hotel.slug, id: hotel.id, screenshot: shotPath, ok: true };
}

async function main() {
  console.log("Hotel Explorer share drawer visibility");
  console.log("BASE_URL", BASE_URL);
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 1,
  });
  const page = await context.newPage();

  const results = [];
  const failures = [];
  try {
    for (const hotel of HOTELS) {
      try {
        results.push(await runHotel(page, hotel));
      } catch (err) {
        const msg = err && err.message ? err.message : String(err);
        failures.push({ hotel: hotel.slug, id: hotel.id, error: msg });
        console.error(`  FAIL ${hotel.slug}: ${msg}`);
        try {
          fs.mkdirSync(OUT_DIR, { recursive: true });
          await page.screenshot({
            path: path.join(OUT_DIR, `${hotel.slug}-FAIL.png`),
            fullPage: false,
          });
        } catch {
          /* ignore screenshot errors */
        }
      }
    }
  } finally {
    await browser.close();
  }

  const summaryPath = path.join(OUT_DIR, "summary.json");
  fs.writeFileSync(
    summaryPath,
    JSON.stringify(
      {
        ok: failures.length === 0,
        baseUrl: BASE_URL,
        passed: results,
        failed: failures,
        generatedAt: new Date().toISOString(),
      },
      null,
      2
    )
  );
  console.log("\nsummary", summaryPath);
  console.log(
    `passed ${results.length}/${HOTELS.length}` +
      (failures.length ? ` failed ${failures.length}` : "")
  );

  if (failures.length) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
