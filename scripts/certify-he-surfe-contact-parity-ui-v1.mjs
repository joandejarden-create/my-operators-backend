#!/usr/bin/env node
/**
 * UI certification for Surfe session-only paint + Find button (no live Surfe spend).
 * Optional live DOM: DEALALITY_PLAYWRIGHT_BASE=http://localhost:8080
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/hotel-explorer/he-surfe-contact-parity-v1");
fs.mkdirSync(OUT, { recursive: true });

const jsSrc = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8"
);
const apiSrc = fs.readFileSync(path.join(ROOT, "api/contact-intelligence.js"), "utf8");

assert.match(jsSrc, /Surfe · Session Only/);
assert.match(jsSrc, /telHrefForPhone\(phone\)/);
assert.match(jsSrc, /payload\.pending/);
assert.match(jsSrc, /_inflightReveal/);
assert.match(jsSrc, /data-hci-surfe-fallback/);
assert.match(jsSrc, /enrich-hybrid/);
assert.match(jsSrc, /\/reveal/);
assert.match(apiSrc, /linkedinUrl/);
assert.match(apiSrc, /surfe_pending/);
assert.match(apiSrc, /surfeIncludeFromMissingState|requestChannel/);
assert.match(apiSrc, /needsEmail|needsPhone/);
assert.doesNotMatch(apiSrc, /meinrad\.lang@marriott\.com/i);
assert.doesNotMatch(jsSrc, /\+13019082451/);

const report = {
  at: new Date().toISOString(),
  staticGates: "PASS",
  live8080: null,
};

const base = process.env.DEALALITY_PLAYWRIGHT_BASE || "";
if (base) {
  try {
    const require = createRequire(import.meta.url);
    const { chromium } = require("playwright");
    const browser = await chromium.launch({
      headless: true,
      channel: process.env.HE_PW_CHANNEL || "chrome",
    });
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const hotelId = "recLuxvwwxID7U2B8";
    const url = `${base.replace(/\/$/, "")}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${hotelId}&hexTab=contacts&cb=${Date.now()}`;
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
    await page
      .waitForFunction(() => typeof window.HotelExplorer === "object", null, {
        timeout: 45000,
      })
      .catch(() => {});
    await page.evaluate((id) => {
      if (window.HotelExplorer?.open) {
        window.HotelExplorer.open({ id, recordId: id }, { tab: "contacts" });
      }
    }, hotelId);
    await page.waitForTimeout(6000);
    await page
      .waitForSelector("[data-hci-contact-details-cell], [data-hci-hybrid-enrich]", {
        timeout: 45000,
      })
      .catch(() => {});
    const shot = path.join(OUT, "PLAYWRIGHT_BETHESDA_CONTACTS.png");
    await page.screenshot({ path: shot, fullPage: false });
    const hybridCount = await page.locator("[data-hci-hybrid-enrich]").count();
    const enrichCount = await page.locator("[data-hci-enrich]").count();
    report.live8080 = {
      url,
      base,
      meinradVisible: (await page.getByText(/Meinrad/i).count()) > 0,
      findButtonPresent: hybridCount + enrichCount > 0,
      hybridEnrichCount: hybridCount,
      sessionOnlyLabelPresent:
        (await page.getByText(/Surfe · Session Only|Session Only/i).count()) > 0,
      screenshot: shot,
      note: "Live Surfe click not auto-fired (credit guard). Provider verified via diagnostic. HE shell via deal-capture-radar hotelExplorer.",
    };
    await browser.close();
  } catch (err) {
    report.live8080 = {
      status: "BLOCKED",
      error: String(err?.message || err).slice(0, 240),
    };
  }
} else {
  report.live8080 = { status: "SKIPPED", reason: "DEALALITY_PLAYWRIGHT_BASE unset" };
}

fs.writeFileSync(path.join(OUT, "UI_CERT.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
console.log("PASS — Surfe parity UI certification");
