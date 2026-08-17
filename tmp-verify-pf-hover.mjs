import puppeteer from "puppeteer";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?cb=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 4000));

const before = await page.evaluate(() => {
  const mi = document.getElementById("pf-card-6");
  const p = document.getElementById("pf-card-6-p");
  const vis = document.getElementById("pf-card-6-visual");
  return {
    ver: window.__ohPlatformFeaturesTiles || null,
    imgH: vis ? Math.round(vis.getBoundingClientRect().height) : null,
    pOp: p ? getComputedStyle(p).opacity : null,
    pMax: p ? getComputedStyle(p).maxHeight : null,
    bodyMin: mi
      ? getComputedStyle(document.getElementById("pf-card-6-body")).minHeight
      : null,
  };
});

await page.hover("#pf-card-6");
await new Promise((r) => setTimeout(r, 400));
const after = await page.evaluate(() => {
  const p = document.getElementById("pf-card-6-p");
  return {
    pOp: p ? getComputedStyle(p).opacity : null,
    pMax: p ? getComputedStyle(p).maxHeight : null,
    pText: p ? p.textContent.slice(0, 80) : null,
  };
});

console.log(JSON.stringify({ before, after }, null, 2));
await browser.close();
