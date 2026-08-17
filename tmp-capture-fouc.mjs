/**
 * Capture Old Home first-paint FOUC evidence via Playwright.
 * Saves screenshots at early paint intervals.
 */
import { chromium } from "playwright";
import fs from "fs";

const outDir = "tmp-fouc-shots";
fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 1,
});
const page = await context.newPage();

// Block nothing — we want real FOUC
const url = "https://www.dealality.com/old-home?fouc=" + Date.now();

await page.addInitScript(() => {
  window.__ohPaintLog = [];
  const log = (tag) => {
    try {
      const hero = document.getElementById("hero");
      const about = document.getElementById("about");
      const h2 = document.getElementById("about-h2") || document.querySelector("#about h2, .oh-about-h2");
      const tech = Array.from(document.querySelectorAll("h2,h1")).find((el) =>
        /Technology Built Around/i.test(el.textContent || "")
      );
      window.__ohPaintLog.push({
        tag,
        t: performance.now(),
        ohReady: document.documentElement.classList.contains("oh-ready"),
        ohBoot: document.documentElement.classList.contains("oh-boot"),
        heroH: hero ? Math.round(hero.getBoundingClientRect().height) : null,
        aboutTop: about ? Math.round(about.getBoundingClientRect().top) : null,
        techTop: tech ? Math.round(tech.getBoundingClientRect().top) : null,
        techText: tech ? (tech.textContent || "").slice(0, 60) : null,
        scrollY: window.scrollY || 0,
        cssCount: document.querySelectorAll('link[rel="stylesheet"]').length,
      });
    } catch (e) {}
  };
  log("init");
  document.addEventListener("DOMContentLoaded", () => log("dom"));
  window.addEventListener("load", () => log("load"));
});

const shots = [];
page.on("domcontentloaded", async () => {
  try {
    const p = `${outDir}/01-domcontentloaded.png`;
    await page.screenshot({ path: p, fullPage: false });
    shots.push(p);
  } catch (e) {}
});

// Navigate with commit (first response) then rapid screenshots
const nav = page.goto(url, { waitUntil: "commit", timeout: 60000 });
await page.waitForTimeout(50);
try {
  await page.screenshot({ path: `${outDir}/00-commit-50ms.png`, fullPage: false });
  shots.push(`${outDir}/00-commit-50ms.png`);
} catch (e) {}

await page.waitForTimeout(150);
try {
  await page.screenshot({ path: `${outDir}/00b-200ms.png`, fullPage: false });
  shots.push(`${outDir}/00b-200ms.png`);
} catch (e) {}

await page.waitForTimeout(300);
try {
  await page.screenshot({ path: `${outDir}/00c-500ms.png`, fullPage: false });
  shots.push(`${outDir}/00c-500ms.png`);
} catch (e) {}

await nav;
await page.waitForLoadState("domcontentloaded");
await page.waitForTimeout(100);
try {
  await page.screenshot({ path: `${outDir}/02-after-dom.png`, fullPage: false });
  shots.push(`${outDir}/02-after-dom.png`);
} catch (e) {}

await page.waitForLoadState("networkidle").catch(() => {});
await page.waitForTimeout(200);
try {
  await page.screenshot({ path: `${outDir}/03-settled.png`, fullPage: false });
  shots.push(`${outDir}/03-settled.png`);
} catch (e) {}

const paintLog = await page.evaluate(() => window.__ohPaintLog || []);
const visibleText = await page.evaluate(() => {
  const els = Array.from(document.querySelectorAll("h1,h2,.oh-hrword,#rotator > *"));
  return els.slice(0, 20).map((el) => ({
    tag: el.tagName,
    id: el.id,
    text: (el.textContent || "").trim().slice(0, 80),
    top: Math.round(el.getBoundingClientRect().top),
    visible: el.getBoundingClientRect().bottom > 0 && el.getBoundingClientRect().top < innerHeight,
  }));
});

fs.writeFileSync(
  `${outDir}/report.json`,
  JSON.stringify({ url, paintLog, visibleText, shots }, null, 2)
);
console.log(JSON.stringify({ paintLog, visibleText: visibleText.filter((v) => v.visible), shots }, null, 2));
await browser.close();
