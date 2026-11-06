#!/usr/bin/env node
/**
 * Browser (Playwright) full-content parity harness for published GDI hotels.
 *
 * Requires:
 *   - PLAYWRIGHT available
 *   - GDI_SHARE_TOKEN_MAP_JSON or --share-token-map { hotelId: "gdishare.v1...." }
 *   - Optional GDI_PROD_BASE
 *
 * Does NOT rotate or recreate share tokens.
 * Hotels without a share capability are recorded as NOT_VERIFIABLE.
 *
 *   node scripts/gdi-portfolio-browser-full-content-parity-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildGdiParityHotelInventory } from "../lib/group-demand-intelligence/presentation/gdi-portfolio-full-content-parity-v1.js";
import { GDI_FULL_CONTENT_PARITY_STATUS } from "../lib/group-demand-intelligence/contracts/gdi-canonical-customer-presentation-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-universal-portfolio-full-content-parity-v1",
);

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function main() {
  const prodBase =
    argValue("--prod-base") ||
    process.env.GDI_PROD_BASE ||
    "https://my-operators-backend-production.up.railway.app";
  const root =
    argValue("--root") ||
    process.env.GDI_PUBLISHED_INVENTORY_ROOT ||
    path.join(
      ROOT,
      "reports/gdi-universal-full-presentation-parity-v1/work-gdi-published-inventory",
    );

  let shareTokensByHotelId = {};
  const tokenMapPath = argValue("--share-token-map");
  if (tokenMapPath && fs.existsSync(tokenMapPath)) {
    shareTokensByHotelId = JSON.parse(fs.readFileSync(tokenMapPath, "utf8"));
  } else if (process.env.GDI_SHARE_TOKEN_MAP_JSON) {
    shareTokensByHotelId = JSON.parse(process.env.GDI_SHARE_TOKEN_MAP_JSON);
  }

  const inventory = buildGdiParityHotelInventory({ root });
  const published = inventory.filter((h) => h.publishedOpportunityCount > 0);

  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    const report = {
      ok: false,
      status: "PLAYWRIGHT_UNAVAILABLE",
      message:
        "Install playwright to run browser parity. Inventory classification still recorded.",
      hotels: published.map((h) => ({
        hotelId: h.hotelId,
        hotelName: h.hotelName,
        publishedOpportunityCount: h.publishedOpportunityCount,
        browserStatus: shareTokensByHotelId[h.hotelId]
          ? GDI_FULL_CONTENT_PARITY_STATUS.VERIFICATION_PENDING
          : GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
      })),
    };
    fs.mkdirSync(OUT_DIR, { recursive: true });
    const out = path.join(OUT_DIR, "02-browser-parity.json");
    fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
    console.log(JSON.stringify({ ...report, out }, null, 2));
    process.exitCode = 0;
    return;
  }

  const browser = await playwright.chromium.launch({ headless: true });
  const results = [];

  for (const hotel of published) {
    const share = shareTokensByHotelId[hotel.hotelId];
    if (!share) {
      results.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        browserStatus: GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
        reason: "share_token_missing",
      });
      continue;
    }
    const page = await browser.newPage();
    const shareUrl = `${prodBase}/group-demand-intelligence-share.html?share=${encodeURIComponent(share)}`;
    try {
      await page.goto(shareUrl, { waitUntil: "networkidle", timeout: 60000 });
      const tileCount = await page.locator("[data-opportunity-id], .gdi-opportunity-tile, .gdi-tile").count();
      const listCount = await page.evaluate(() => {
        const el = document.querySelector("[data-opportunity-count], .gdi-count, .gdi-opportunity-count");
        if (!el) return null;
        const n = Number(String(el.textContent || "").replace(/[^\d]/g, ""));
        return Number.isFinite(n) ? n : null;
      });
      const okCount =
        tileCount === hotel.publishedOpportunityCount ||
        listCount === hotel.publishedOpportunityCount;
      results.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        expectedCount: hotel.publishedOpportunityCount,
        tileCount,
        listCount,
        browserStatus: okCount
          ? GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED
          : GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH,
        shareUrlRedacted: shareUrl.replace(/share=[^&]+/, "share=REDACTED"),
      });
    } catch (err) {
      results.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        browserStatus: GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
        error: String(err?.message || err).slice(0, 240),
      });
    } finally {
      await page.close();
    }
  }

  await browser.close();

  const verified = results.filter(
    (r) => r.browserStatus === GDI_FULL_CONTENT_PARITY_STATUS.VERIFIED,
  ).length;
  const mismatch = results.filter(
    (r) => r.browserStatus === GDI_FULL_CONTENT_PARITY_STATUS.MISMATCH,
  ).length;
  const notVerifiable = results.filter(
    (r) => r.browserStatus === GDI_FULL_CONTENT_PARITY_STATUS.NOT_VERIFIABLE,
  ).length;

  const report = {
    ok: mismatch === 0 && verified === published.length,
    status:
      mismatch === 0 && verified === published.length
        ? "GDI_BROWSER_PORTFOLIO_FULL_CONTENT_PARITY_PASS"
        : "GDI_BROWSER_PORTFOLIO_FULL_CONTENT_PARITY_INCOMPLETE",
    generatedAt: new Date().toISOString(),
    summary: {
      PUBLISHED_HOTELS: published.length,
      VERIFIED_HOTELS: verified,
      MISMATCH_HOTELS: mismatch,
      NOT_VERIFIABLE_HOTELS: notVerifiable,
    },
    hotels: results,
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const out = path.join(OUT_DIR, "02-browser-parity.json");
  fs.writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify({ status: report.status, summary: report.summary, out }, null, 2));
  if (process.argv.includes("--fail-on-incomplete") && !report.ok) {
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
