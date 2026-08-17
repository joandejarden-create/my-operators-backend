import { chromium } from "playwright";
import fs from "fs";
import path from "path";

const outDir = "tmp-pf-screenshots";
fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({
  headless: true,
  channel: "chrome",
});
const page = await browser.newPage();
await page.setViewportSize({ width: 1440, height: 1100 });
await page.goto("https://www.dealality.com/old-home?pf=30a", {
  waitUntil: "domcontentloaded",
  timeout: 90000,
});
await page.waitForSelector("#platform-features-grid", { timeout: 30000 });
await page.waitForTimeout(3000);

const info = await page.evaluate(() => {
  const grid = document.getElementById("platform-features-grid");
  const cards = grid ? [...grid.querySelectorAll("article")] : [];
  const rects = cards.map((c) => {
    const r = c.getBoundingClientRect();
    return { w: Math.round(r.width), h: Math.round(r.height) };
  });
  return {
    cardCount: cards.length,
    titles: cards.map((c) => (c.querySelector("h3") || {}).textContent || ""),
    lead: (document.getElementById("platform-features-lead") || {}).textContent || "",
    dataAttr: grid ? grid.getAttribute("data-oh-pf-tiles") : null,
    rects,
    cssHref: (document.querySelector('link[href*="platform-features"]') || {}).href || null,
  };
});
fs.writeFileSync(path.join(outDir, "verify.json"), JSON.stringify(info, null, 2));
console.log(JSON.stringify(info, null, 2));

const section = page.locator("#platform-features");
await section.scrollIntoViewIfNeeded();
await page.waitForTimeout(600);
await section.screenshot({ path: path.join(outDir, "features-desktop-3x3.png") });

await page.setViewportSize({ width: 390, height: 900 });
await page.waitForTimeout(700);
await section.scrollIntoViewIfNeeded();
await page.waitForTimeout(500);
await section.screenshot({ path: path.join(outDir, "features-mobile-stack.png") });

await browser.close();
console.log("screenshots ok");
