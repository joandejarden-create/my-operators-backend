import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const outDir = path.resolve("tmp-screenshots-old-home-refine");
fs.mkdirSync(outDir, { recursive: true });
const url = "https://www.dealality.com/old-home?nocache=" + Date.now();

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();

async function shot(name, width, height, scrollTo) {
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  await page.waitForSelector("#hero", { timeout: 30000 });
  if (scrollTo) {
    await page.evaluate((sel) => {
      const el = document.querySelector(sel);
      if (el) el.scrollIntoView({ block: "start" });
    }, scrollTo);
    await new Promise((r) => setTimeout(r, 600));
  } else {
    await new Promise((r) => setTimeout(r, 900));
  }
  const file = path.join(outDir, name);
  await page.screenshot({ path: file, fullPage: false });
  console.log("wrote", file);
}

await shot("desktop-hero.png", 1440, 900, null);
await shot("desktop-product-proof.png", 1440, 900, "#product-proof");
await shot("desktop-trust.png", 1440, 900, "#trust");
await shot("mobile-hero.png", 390, 844, null);
await shot("mobile-product-proof.png", 390, 844, "#product-proof");

// Footer empty-space check
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 1 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
const metrics = await page.evaluate(() => {
  const footer = document.querySelector("#footer");
  const body = document.body;
  const html = document.documentElement;
  const footerBottom = footer ? footer.getBoundingClientRect().bottom + window.scrollY : null;
  return {
    bodyScrollHeight: body.scrollHeight,
    htmlScrollHeight: html.scrollHeight,
    footerBottom,
    gapAfterFooter:
      footerBottom != null ? Math.max(body.scrollHeight, html.scrollHeight) - footerBottom : null,
    wordmarkSrc: document.querySelector("#nav-logo img")?.getAttribute("src") || null,
    wordmarkAlt: document.querySelector("#nav-logo img")?.getAttribute("alt") || null,
    ppHeading: document.querySelector("#pp-h2")?.textContent || null,
    trustHeading: document.querySelector("#trust-h2")?.textContent || null,
    hasProductImgs: !!document.querySelector("#pp-a-img, #product-proof img"),
  };
});
fs.writeFileSync(path.join(outDir, "metrics.json"), JSON.stringify(metrics, null, 2));
console.log(metrics);

await browser.close();
