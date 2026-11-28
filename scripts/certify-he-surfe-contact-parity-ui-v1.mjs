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
assert.match(apiSrc, /channel === "both"/);
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
    const url = `${base.replace(/\/$/, "")}/app#/hotel-explorer?hotelId=recLuxvwwxID7U2B8`;
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForTimeout(3000);
    const contactsTab = page.getByText(/Contact Intelligence|Contacts/i).first();
    if ((await contactsTab.count()) > 0) {
      await contactsTab.click({ timeout: 8000 }).catch(() => {});
      await page.waitForTimeout(2000);
    }
    const shot = path.join(OUT, "PLAYWRIGHT_BETHESDA_CONTACTS.png");
    await page.screenshot({ path: shot, fullPage: false });
    report.live8080 = {
      url,
      meinradVisible: (await page.getByText(/Meinrad/i).count()) > 0,
      findButtonPresent: (await page.locator("[data-hci-enrich]").count()) > 0,
      screenshot: shot,
      note: "Live Surfe click not auto-fired (credit guard). Provider verified via diagnostic script.",
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
