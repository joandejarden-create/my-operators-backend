#!/usr/bin/env node
/**
 * Live GDI presentation screenshots (actual app routes on local server).
 * No publish. No Airtable writes. No share-token minting.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-need-dates-27-hotel-integration-certification/screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

const BASE = process.env.GDI_LOCAL_BASE || "http://localhost:3091";
const HOTELS = [
  { id: "recLuxvwwxID7U2B8", slug: "bethesda", expect: "HOTEL_CONFIRMED" },
  { id: "recgMYovrrZDJMqzX", slug: "waterstone", expect: "MARKET_ESTIMATED" },
  { id: "recKRJjcPnb4tVDDS", slug: "spice", expect: "NOT_ESTABLISHED" },
  { id: "rec2PVBDavppGpenm", slug: "ac-coruna", expect: "NOT_ESTABLISHED" },
  { id: "rec0eTEnCqlsGJJt9", slug: "mayflower-onboarded", expect: "NOT_ESTABLISHED" },
  { id: "recRkGGNKslJptFIL", slug: "falls-church", expect: "NOT_ESTABLISHED" },
  { id: "recterKmNPdntCm0v", slug: "chapultepec", expect: "NOT_ESTABLISHED" },
  { id: "recIEcSWuhSCoNLFN", slug: "condesa", expect: "NOT_ESTABLISHED" },
];

async function patchFetch(page) {
  await page.addInitScript(() => {
    const orig = window.fetch.bind(window);
    window.fetch = (input, init = {}) => {
      const headers = new Headers(init.headers || {});
      headers.set("X-Dealality-Owner-App", "1");
      return orig(input, { ...init, headers });
    };
  });
}

async function captureHotel(browser, hotel, viewport) {
  const context = await browser.newContext({
    viewport,
    deviceScaleFactor: 1,
  });
  const page = await context.newPage();
  await patchFetch(page);
  const url = `${BASE}/group-demand-intelligence.html?hotelId=${hotel.id}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(2500);

  // Force hotel selection via localStorage + reload if selector present
  await page.evaluate((hotelId) => {
    try {
      localStorage.setItem("gdi.hotelId", hotelId);
      localStorage.setItem("dealality.gdi.hotelId", hotelId);
    } catch {}
  }, hotel.id);
  await page.goto(url, { waitUntil: "networkidle", timeout: 60000 }).catch(() => {});
  await page.waitForTimeout(3000);

  const status = await page.evaluate(() => {
    const el = document.querySelector("[data-gdi-need-date-config-status]");
    const strip = document.querySelector("[data-gdi-need-date-config-visible]");
    const legendBtn = document.querySelector(
      "[data-gdi-need-date-legend-toggle], .gdi-need-date-info-btn, button[aria-label*='Need date']"
    );
    return {
      statusAttr: el?.getAttribute("data-gdi-need-date-config-status") || null,
      stripVisible: Boolean(strip),
      text: (el?.textContent || strip?.textContent || "").replace(/\s+/g, " ").trim().slice(0, 200),
      legendPresent: Boolean(legendBtn),
      title: document.title,
      bodySnippet: (document.body?.innerText || "").slice(0, 400),
    };
  });

  const fileBase = `${hotel.slug}-${viewport.width}x${viewport.height}`;
  const shotPath = path.join(OUT, `${fileBase}.png`);
  await page.screenshot({ path: shotPath, fullPage: false });

  let legendPath = null;
  if (status.legendPresent || true) {
    const clicked = await page.evaluate(() => {
      const btn =
        document.querySelector("[data-gdi-need-date-legend-toggle]") ||
        document.querySelector(".gdi-need-date-info-btn") ||
        document.querySelector("button[aria-label*='Need Dates']") ||
        document.querySelector("button[aria-label*='need date']") ||
        document.querySelector("[data-gdi-need-date-config-visible] button");
      if (btn) {
        btn.click();
        return true;
      }
      return false;
    });
    if (clicked) {
      await page.waitForTimeout(800);
      legendPath = path.join(OUT, `${fileBase}-legend.png`);
      await page.screenshot({ path: legendPath, fullPage: false });
    }
  }

  await context.close();
  return {
    hotelId: hotel.id,
    slug: hotel.slug,
    expect: hotel.expect,
    viewport,
    status,
    shotPath: path.relative(ROOT, shotPath).replace(/\\/g, "/"),
    legendPath: legendPath
      ? path.relative(ROOT, legendPath).replace(/\\/g, "/")
      : null,
  };
}

const browser = await chromium.launch({ headless: true });
const results = [];
try {
  for (const hotel of HOTELS) {
    results.push(await captureHotel(browser, hotel, { width: 1440, height: 900 }));
    if (["bethesda", "waterstone", "spice"].includes(hotel.slug)) {
      results.push(await captureHotel(browser, hotel, { width: 390, height: 844 }));
    }
  }
} finally {
  await browser.close();
}

const report = {
  ok: results.every((r) => r.status?.stripVisible !== false),
  generatedAt: new Date().toISOString(),
  base: BASE,
  results,
};
fs.writeFileSync(
  path.join(OUT, "..", "live-presentation-screenshots.json"),
  JSON.stringify(report, null, 2) + "\n"
);
console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
