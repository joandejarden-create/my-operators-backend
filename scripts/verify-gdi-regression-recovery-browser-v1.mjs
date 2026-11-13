#!/usr/bin/env node
/**
 * Browser verification for GDI regression recovery (served assets on :8080).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const OUT = path.join(
  ROOT,
  "reports/gdi-regression-recovery-v1/screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

const HOTELS = [
  {
    id: "recLuxvwwxID7U2B8",
    label: "bethesda-marriott",
    nameNeedle: "Bethesda Marriott",
    expectScope: /All\s*\(\s*6[0-9]\s*\)/,
  },
  {
    id: "recnPB3Q02NO8Ma1o",
    label: "wyndham-san-diego-bayside",
    nameNeedle: "Wyndham San Diego Bayside",
    expectScope: null,
  },
  {
    id: "gdi_hotel_waterstone_boca_raton",
    label: "waterstone-boca-raton",
    nameNeedle: "Waterstone",
    expectScope: null,
  },
];

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await page.route("**/api/me**", async (route) => {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      success: true,
      user: { email: "dealalitydemo@dealality.com" },
      dealality: {
        email: "dealalitydemo@dealality.com",
        isAdmin: true,
        isDemo: true,
        flags: { isAdmin: true, gdiPilot: true },
      },
    }),
  });
});

const evidence = [];

for (const hotel of HOTELS) {
  if (!hotel.id) {
    evidence.push({ label: hotel.label, skipped: "hotel_id_not_found" });
    continue;
  }
  await page.addInitScript((hotelId) => {
    try {
      localStorage.setItem("gdi.hotelId", hotelId);
      localStorage.setItem("dealality.gdi.hotelId", hotelId);
    } catch {
      /* ignore */
    }
  }, hotel.id);

  const url = `${BASE}/group-demand-intelligence.html?hotelId=${encodeURIComponent(
    hotel.id
  )}&cb=${Date.now()}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForFunction(
    (needle) => {
      const t = document.body?.innerText || "";
      return (
        t.includes(needle) &&
        !t.includes("Loading Group Demand Intelligence")
      );
    },
    hotel.nameNeedle,
    { timeout: 90000 }
  );
  await page.waitForTimeout(2000);

  // Confirm served cache-bust includes recovery assets.
  const cssHref = await page.evaluate(() => {
    const el = [...document.querySelectorAll('link[rel="stylesheet"]')].find(
      (l) => (l.href || "").includes("group-demand-intelligence.css")
    );
    return el?.href || "";
  });
  assert.match(
    cssHref,
    /gdi-regression-recovery-v1|gdi-title-sourcing-consistency-v1|gdi-opp-type-classification-v1|gdi-card-no-/
  );

  const bodyText = await page.locator("body").innerText();
  assert.equal(bodyText.includes(" updated"), false, "X updated header must be gone");
  assert.ok(
    /Group & Demand/i.test(bodyText),
    "Group & Demand Opportunities nav label missing"
  );
  assert.ok(
    bodyText.includes("All (") && bodyText.includes("Hotel (") && bodyText.includes("Competitive ("),
    "Market Scope labels missing"
  );
  if (hotel.expectScope) {
    assert.match(bodyText, hotel.expectScope);
  }

  // Market Scope height vs adjacent select
  const heights = await page.evaluate(() => {
    const scope = document.querySelector(".gdi-market-scope");
    const select = document.querySelector(
      ".gdi-property-bar__primary select, .gdi-weekly-filter select, .gdi-opportunity-type-filter select"
    );
    if (!scope || !select) return null;
    const s = scope.getBoundingClientRect();
    const c = select.getBoundingClientRect();
    return {
      scopeH: Math.round(s.height),
      controlH: Math.round(c.height),
      delta: Math.abs(Math.round(s.height) - Math.round(c.height)),
    };
  });
  assert.ok(heights, "toolbar controls not found");
  assert.ok(
    heights.delta <= 2,
    `Market Scope height mismatch: ${JSON.stringify(heights)}`
  );

  // Title cleanliness on visible title nodes
  const dirtyTitles = await page.evaluate(() => {
    const forbidden = ["(Early Watch)", "(early watch)", "— Sep ", "— December "];
    const nodes = [
      ...document.querySelectorAll(
        ".gdi-tile-title, .gdi-opp-title, .gdi-list-title, h3, h4"
      ),
    ];
    const bad = [];
    for (const el of nodes) {
      const t = (el.textContent || "").trim();
      for (const frag of forbidden) {
        if (t.includes(frag)) bad.push(t.slice(0, 120));
      }
    }
    return bad;
  });
  assert.equal(dirtyTitles.length, 0, JSON.stringify(dirtyTitles));

  const toolbarShot = path.join(OUT, `${hotel.label}-toolbar.png`);
  await page.screenshot({ path: toolbarShot, fullPage: false });

  // Open first View Details if present
  const viewBtn = page.locator('button:has-text("View Details")').first();
  let drawerShot = null;
  if ((await viewBtn.count()) > 0) {
    await viewBtn.click();
    await page.waitForTimeout(1200);
    const drawer = page.locator("#gdiDrawer, .gdi-drawer, [data-gdi-tour-target='drawer']");
    if ((await drawer.count()) > 0) {
      drawerShot = path.join(OUT, `${hotel.label}-drawer.png`);
      await drawer.first().screenshot({ path: drawerShot }).catch(async () => {
        await page.screenshot({ path: drawerShot, fullPage: false });
      });
    }
    await page.keyboard.press("Escape").catch(() => {});
  }

  evidence.push({
    hotelId: hotel.id,
    label: hotel.label,
    cssHref,
    heights,
    toolbarShot,
    drawerShot,
    showing: (bodyText.match(/Showing[^\n]+/) || [null])[0],
  });
}

await browser.close();
const report = {
  ok: true,
  generatedAt: new Date().toISOString(),
  evidence,
};
fs.writeFileSync(
  path.join(ROOT, "reports/gdi-regression-recovery-v1/02-browser-verify.json"),
  JSON.stringify(report, null, 2)
);
console.log(JSON.stringify(report, null, 2));
