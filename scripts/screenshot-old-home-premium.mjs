import { chromium } from "playwright";
import fs from "fs";
import path from "path";

const outDir = path.resolve("tmp-screenshots-old-home-premium");
fs.mkdirSync(outDir, { recursive: true });
const browser = await chromium.launch({ headless: true });

async function shot(name, width, height) {
  const page = await browser.newPage({ viewport: { width, height } });
  await page.goto("https://www.dealality.com/old-home", {
    waitUntil: "networkidle",
    timeout: 60000,
  });
  await page.waitForTimeout(1200);
  await page.screenshot({
    path: path.join(outDir, name),
    fullPage: name.includes("full"),
  });
  const order = await page.evaluate(() => {
    const ids = ["nav", "hero", "problem", "how-it-works", "product-proof", "trust", "cta", "footer"];
    return ids.map((id) => {
      const el = document.getElementById(id);
      if (!el) return { id, missing: true };
      return { id, top: Math.round(el.getBoundingClientRect().top + window.scrollY) };
    });
  });
  const afterFooter = await page.evaluate(() => {
    const footer = document.getElementById("footer");
    if (!footer) return ["no-footer"];
    const all = [...document.querySelectorAll("section, footer, nav")];
    return all
      .filter((el) => el.compareDocumentPosition(footer) & Node.DOCUMENT_POSITION_FOLLOWING)
      .map((el) => el.id || el.className || el.tagName);
  });
  console.log(name, JSON.stringify({ order, afterFooter }));
  await page.close();
}

await shot("desktop-hero.png", 1440, 900);
await shot("desktop-full.png", 1440, 900);
await shot("mobile-hero.png", 390, 844);
await shot("mobile-full.png", 390, 844);
await browser.close();
console.log("wrote", outDir);
