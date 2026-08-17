import { chromium } from "playwright";
import fs from "fs";

const outDir = "tmp-fouc-shots-after";
fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 1,
});
const page = await context.newPage();
const url = "https://www.dealality.com/old-home?foucfix=" + Date.now();

await page.addInitScript(() => {
  window.__ohPaintLog = [];
  const log = (tag) => {
    try {
      const hero = document.getElementById("hero");
      const list = document.getElementById("hero-globe-list");
      const rotatorKids = Array.from(document.querySelectorAll("#rotator > *")).map((el) => ({
        text: (el.textContent || "").trim().slice(0, 40),
        top: Math.round(el.getBoundingClientRect().top),
        opacity: getComputedStyle(el).opacity,
        visible:
          el.getBoundingClientRect().bottom > 0 &&
          el.getBoundingClientRect().top < innerHeight &&
          getComputedStyle(el).opacity !== "0" &&
          getComputedStyle(el).visibility !== "hidden",
      }));
      window.__ohPaintLog.push({
        tag,
        t: Math.round(performance.now()),
        ohReady: document.documentElement.classList.contains("oh-ready"),
        ohBoot: document.documentElement.classList.contains("oh-boot"),
        pageVis: document.getElementById("dc-page")
          ? getComputedStyle(document.getElementById("dc-page")).visibility
          : null,
        heroH: hero ? Math.round(hero.getBoundingClientRect().height) : null,
        globeListDisplay: list ? getComputedStyle(list).display : null,
        visibleRotator: rotatorKids.filter((k) => k.visible),
        cssFouc: !!document.querySelector('link[data-oh-fouc],script[src*="fouc-gate"]'),
        hasGateStyle: !!document.getElementById("oh-fouc-gate"),
      });
    } catch (e) {
      window.__ohPaintLog.push({ tag, err: String(e) });
    }
  };
  document.addEventListener("DOMContentLoaded", () => log("dom"));
  window.addEventListener("load", () => log("load"));
});

await page.goto(url, { waitUntil: "commit", timeout: 60000 });
await page.waitForTimeout(80);
await page.screenshot({ path: `${outDir}/00-80ms.png`, fullPage: false }).catch(() => {});
await page.waitForTimeout(200);
await page.screenshot({ path: `${outDir}/01-280ms.png`, fullPage: false }).catch(() => {});
await page.waitForTimeout(400);
await page.screenshot({ path: `${outDir}/02-680ms.png`, fullPage: false }).catch(() => {});
await page.waitForLoadState("domcontentloaded");
await page.waitForTimeout(100);
await page.screenshot({ path: `${outDir}/03-dom.png`, fullPage: false }).catch(() => {});
await page.waitForLoadState("networkidle").catch(() => {});
await page.waitForTimeout(300);
await page.screenshot({ path: `${outDir}/04-settled.png`, fullPage: false }).catch(() => {});

const htmlHas = await page.evaluate(() => ({
  ohBoot: document.documentElement.classList.contains("oh-boot"),
  ohReady: document.documentElement.classList.contains("oh-ready"),
  gateStyle: !!document.getElementById("oh-fouc-gate"),
  foucScript: !!document.querySelector('script[src*="fouc-gate"]'),
  w21: !!document.querySelector('link[href*="freeform-head.v20260729w21"]'),
  herofit: !!document.querySelector('link[href*="hero-fit.v20260729c"]'),
}));
const paintLog = await page.evaluate(() => window.__ohPaintLog || []);
const report = { url, htmlHas, paintLog };
fs.writeFileSync(`${outDir}/report.json`, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
await browser.close();
