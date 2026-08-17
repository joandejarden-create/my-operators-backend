/**
 * Capture high-DPI marketing screenshots for dealality-landing-v5.html
 * Run: node scripts/capture-marketing-screenshots.mjs
 * Requires local server on PORT (default 8080).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import puppeteer from "puppeteer";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "..", "public", "marketing", "screenshots");
const BASE = (process.env.MARKETING_CAPTURE_BASE || "http://localhost:8080").replace(/\/$/, "");
const DEMO_DEAL_ID = (process.env.MARKETING_DEMO_DEAL_ID || "recqGVET08a8faagy").trim();
const VIEWPORT_W = 1200;
const DPR = 2;
const WAIT_MS = 3000;

const CAPTURES = [
  {
    file: "brand-explorer.png",
    url: `${BASE}/brand-education-atelier-north.html?embed=1`,
    waitMs: 4000,
    async beforeShot(page) {
      const clip = await page.evaluate(() => {
        const hero = document.querySelector(".brand-hero");
        const section = document.querySelector(".oe-section");
        if (!hero) return null;
        const top = hero.getBoundingClientRect().top;
        const bottom = section ? section.getBoundingClientRect().bottom : hero.getBoundingClientRect().bottom;
        return {
          x: 0,
          y: Math.max(0, top - 8),
          width: window.innerWidth,
          height: Math.min(window.innerHeight - top, bottom - top + 48),
        };
      });
      if (clip && clip.height > 120) {
        page._marketingClip = clip;
      }
    },
    useStoredClip: true,
  },
  {
    file: "matched-brands.png",
    url: `${BASE}/marketing/screenshot-matched-brands.html`,
    clipSelector: ".mkt-snapshot",
    waitMs: 2000,
  },
  {
    file: "deal-compare.png",
    url: `${BASE}/marketing/screenshot-deal-compare.html`,
    clipSelector: ".mkt-snapshot",
    waitMs: 2000,
  },
  {
    file: "market-map.png",
    url: `${BASE}/deal-capture-radar`,
    clipSelector: ".map-container, #map, .radar-map-wrap",
    waitMs: 12000,
  },
  {
    file: "fee-estimator.png",
    url: `${BASE}/franchise-fee-estimator.html`,
    clipSelector: ".calculator-container, .fee-estimator, main .container, main",
    waitMs: 4000,
  },
  {
    file: "operator-track-record.png",
    url: `${BASE}/operator-explorer-gold-mock.html?embed=1`,
    clipSelector: ".proof-grid, .section-title",
    waitMs: 3500,
    async beforeShot(page) {
      await page.evaluate(() => {
        const tabs = [...document.querySelectorAll(".explorer-tab, [data-tab], button")];
        const proof = tabs.find((el) => /proof/i.test(el.textContent || ""));
        if (proof) proof.click();
      });
      await new Promise((r) => setTimeout(r, 1200));
      const clip = await page.evaluate(() => {
        const title = [...document.querySelectorAll(".section-title")].find((el) =>
          /proof/i.test(el.textContent || "")
        );
        const grid = document.querySelector(".proof-grid");
        if (!title || !grid) return null;
        const top = title.getBoundingClientRect().top;
        const bottom = grid.getBoundingClientRect().bottom;
        return {
          x: 24,
          y: Math.max(0, top - 12),
          width: window.innerWidth - 48,
          height: bottom - top + 36,
        };
      });
      if (clip && clip.height > 120) page._marketingClip = clip;
    },
    useStoredClip: true,
  },
];

async function screenshotElement(page, el, outPath) {
  const box = await el.boundingBox();
  if (!box || box.width < 120 || box.height < 80) {
    throw new Error("clip target too small or hidden");
  }
  await el.screenshot({ path: outPath, type: "png" });
  return box;
}

async function captureOne(browser, spec) {
  const page = await browser.newPage();
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const u = req.url();
    if (/memberstack|googletagmanager|google-analytics|hotjar|segment|intercom/i.test(u)) {
      req.abort();
      return;
    }
    req.continue();
  });
  await page.setViewport({ width: VIEWPORT_W, height: 900, deviceScaleFactor: DPR });
  const waitMs = spec.waitMs ?? WAIT_MS;

  try {
    await page.goto(spec.url, { waitUntil: spec.waitUntil || "domcontentloaded", timeout: 90000 });
    if (spec.readySelector) {
      await page.waitForSelector(spec.readySelector, { timeout: waitMs }).catch(() => null);
    }
    await new Promise((r) => setTimeout(r, waitMs));
    if (spec.beforeShot) await spec.beforeShot(page);

    const outPath = path.join(OUT_DIR, spec.file);
    let captured = false;

    if (spec.clipSelector) {
      for (const sel of spec.clipSelector.split(",").map((s) => s.trim())) {
        const el = await page.$(sel);
        if (!el) continue;
        try {
          const box = await screenshotElement(page, el, outPath);
          console.log(`✓ ${spec.file} clipped ${sel} (${Math.round(box.width * DPR)}×${Math.round(box.height * DPR)}px) ← ${spec.url}`);
          captured = true;
          break;
        } catch {
          /* try next selector */
        }
      }
    }

    if (!captured && spec.useStoredClip && page._marketingClip) {
      const clip = page._marketingClip;
      await page.screenshot({ path: outPath, type: "png", clip });
      console.log(
        `✓ ${spec.file} custom clip (${Math.round(clip.width * DPR)}×${Math.round(clip.height * DPR)}px) ← ${spec.url}`
      );
      captured = true;
    }

    if (!captured) {
      await page.screenshot({ path: outPath, type: "png", fullPage: false });
      console.log(`✓ ${spec.file} viewport ← ${spec.url}`);
    }
    return true;
  } catch (err) {
    console.error(`✗ ${spec.file}: ${err.message}`);
    return false;
  } finally {
    await page.close();
  }
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const failed = [];
  try {
    for (const spec of CAPTURES) {
      const ok = await captureOne(browser, spec);
      if (ok === false) failed.push(spec.file);
    }
  } finally {
    await browser.close();
  }
  if (failed.length) {
    console.warn(`Skipped or failed: ${failed.join(", ")}`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
