#!/usr/bin/env node
/**
 * CONTACT_ENRICHMENT_TOP_RIGHT_TOAST — viewport position + lifecycle regression.
 *
 * Usage:
 *   node scripts/hotel-explorer/test-he-enrich-toast-upper-right-v1.mjs
 *   node scripts/hotel-explorer/test-he-enrich-toast-upper-right-v1.mjs --browser
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = path.join(ROOT, "reports/hotel-explorer/he-enrich-toast-upper-right-v1");
const SHOTS = path.join(OUT, "SCREENSHOTS");
fs.mkdirSync(SHOTS, { recursive: true });

const BASE = (process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";
const BROWSER = process.argv.includes("--browser");

const css = fs.readFileSync(path.join(ROOT, "public/css/hotel-contact-intelligence.css"), "utf8");
const js = fs.readFileSync(path.join(ROOT, "public/js/hotel-contact-intelligence.js"), "utf8");

const results = [];
function gate(id, ok, detail) {
  results.push({ id, ok, detail });
  if (!ok) throw new Error(id + (detail ? ": " + JSON.stringify(detail) : ""));
}

gate("CONTACT_TOAST_CSS_UPPER_RIGHT", /hci-enrich-loading-toast[\s\S]{0,220}top:\s*20px[\s\S]{0,80}right:\s*20px/.test(css));
gate("CONTACT_TOAST_NOT_BOTTOM_CENTER", !/hci-enrich-loading-toast[\s\S]{0,120}bottom:\s*28px/.test(css));
gate("CONTACT_TOAST_ENTRANCE_ANIMATION", /\.hci-enrich-loading-toast\.show[\s\S]{0,120}opacity:\s*1/.test(css));
gate("CONTACT_TOAST_BODY_HOST", /document\.body\.appendChild\(host\)/.test(js));
gate("CONTACT_TOAST_NO_TABLE_INJECT", /never inside table rows|never inject into table rows/i.test(js));
gate("CONTACT_TOAST_Z_ABOVE_HE_MODAL", /hci-enrich-loading-toast[\s\S]{0,200}z-index:\s*14\d{3}/.test(css));

async function launch() {
  const require = createRequire(
    path.join(process.env.DEALALITY_NODE_MODULES_ROOT || "C:/Dev/dealality/ws-gdi", "package.json"),
  );
  const { chromium } = require("playwright");
  try {
    return await chromium.launch({ headless: true });
  } catch {
    return chromium.launch({ channel: "chrome", headless: true });
  }
}

async function runBrowser() {
  const browser = await launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

  let release;
  const gateP = new Promise((r) => {
    release = r;
  });
  await page.route("**/api/contact-intelligence/hotels/*/people/**", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    await gateP;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        mock: true,
        providerAttribution: "Surfe · Session Only",
        persistence: "NOT_WRITTEN",
        person: { email: "toast.mock@example.hotel", phone: null },
      }),
    });
  });

  await page.goto(
    `${BASE}/deal-capture-radar-with-ranked-list.html?hotelExplorer=${HOTEL}&hexTab=contacts&cb=${Date.now()}`,
    { waitUntil: "domcontentloaded", timeout: 90000 },
  );
  await page.waitForFunction(() => typeof window.HotelExplorer === "object", null, { timeout: 45000 });
  await page.evaluate((id) => {
    HotelExplorer.open({ id, recordId: id }, { tab: "contacts", requestedHotelId: id });
  }, HOTEL);
  await page.waitForSelector("[data-hci-enrich]", { timeout: 90000 });

  const btn = page.locator("[data-hci-enrich]").first();
  await btn.click();
  await page.waitForTimeout(250);

  const toast = await page.evaluate(() => {
    const el = document.getElementById("hci-enrich-loading-toast");
    if (!el) return { present: false };
    const r = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    const inTable = !!(el.closest("table") || el.closest("tr") || el.closest("[data-hci-contact-details-cell]"));
    const shown =
      style.display !== "none" &&
      style.visibility !== "hidden" &&
      Number(style.opacity || "0") > 0.2 &&
      r.width > 0;
    return {
      present: true,
      shown,
      inTable,
      parentIsBody: el.parentElement === document.body,
      upperRight: shown && r.top <= 120 && window.innerWidth - r.right <= 120,
      top: Math.round(r.top),
      rightGap: Math.round(window.innerWidth - r.right),
      hasWave: !!el.querySelector(".wave, .dealality-wave-loader, .toast-wave-container, .wave-container"),
      text: (el.innerText || "").slice(0, 160),
      zIndex: style.zIndex,
    };
  });
  await page.screenshot({ path: path.join(SHOTS, "01_toast_upper_right.png"), fullPage: false });

  release();
  await page.waitForTimeout(1200);

  const after = await page.evaluate(() => {
    const el = document.getElementById("hci-enrich-loading-toast");
    if (!el) return { dismissed: true };
    const style = getComputedStyle(el);
    const shown =
      style.display !== "none" &&
      style.visibility !== "hidden" &&
      Number(style.opacity || "0") > 0.2 &&
      el.getBoundingClientRect().width > 0;
    return {
      dismissed: !shown || el.hidden === true || !el.classList.contains("show"),
      stillInTable: !!(el.closest("table") || el.closest("tr")),
    };
  });
  await page.screenshot({ path: path.join(SHOTS, "02_toast_dismissed.png"), fullPage: false });
  await browser.close();
  return { toast, after };
}

let browserDetail = null;
if (BROWSER) {
  browserDetail = await runBrowser();
  gate("CONTACT_ENRICHMENT_TOP_RIGHT_TOAST", Boolean(browserDetail.toast.shown && browserDetail.toast.upperRight && !browserDetail.toast.inTable), browserDetail.toast);
  gate("CONTACT_TOAST_OUTSIDE_TABLE", browserDetail.toast.parentIsBody === true && browserDetail.toast.inTable === false, browserDetail.toast);
  gate("CONTACT_TOAST_WAVE_PRESENT", browserDetail.toast.hasWave === true, browserDetail.toast);
  gate("CONTACT_TOAST_DISMISSES", browserDetail.after.dismissed === true && browserDetail.after.stillInTable !== true, browserDetail.after);
}

const summary = {
  at: new Date().toISOString(),
  classification: results.every((r) => r.ok)
    ? BROWSER
      ? "HE_ENRICH_TOAST_UPPER_RIGHT_CERTIFIED"
      : "HE_ENRICH_TOAST_STATIC_PASS_BROWSER_NOT_TESTED"
    : "HE_ENRICH_TOAST_NOT_CERTIFIED",
  browser: BROWSER,
  results,
  browserDetail,
};
fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ ok: results.every((r) => r.ok), classification: summary.classification, passed: results.length }, null, 2));
if (!results.every((r) => r.ok)) process.exit(1);
