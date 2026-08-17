import puppeteer from "puppeteer";
import fs from "fs";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.setRequestInterception(true);
page.on("request", (req) => {
  const url = req.url();
  if (/ipapi\.co|api\.country\.is|ipwho\.is|googletagmanager|google-analytics|doubleclick/i.test(url)) {
    req.abort();
    return;
  }
  req.continue();
});
await page.goto("https://www.dealality.com/old-home#about", {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.waitForSelector("#about", { timeout: 20000 });
await page.waitForFunction(
  () => document.getElementById("about")?.getAttribute("data-oh-problem-v2") === "1",
  { timeout: 15000 }
).catch(() => null);
await new Promise((r) => setTimeout(r, 1500));

const state = await page.evaluate(() => {
  const h2 = document.getElementById("about-h2");
  const lead = document.getElementById("about-lead");
  const frag = document.getElementById("about-frag");
  const point1 = document.getElementById("about-point-1");
  const icon = document.getElementById("about-point-1-icon");
  const css = [...document.querySelectorAll('link[rel="stylesheet"]')]
    .map((l) => l.href)
    .filter((h) => /freeform-head|w18|problem/i.test(h));
  return {
    flagged: document.getElementById("about")?.getAttribute("data-oh-problem-v2"),
    h2: h2?.innerText || null,
    leadStart: (lead?.textContent || "").slice(0, 90),
    hasFrag: !!frag,
    fragEyebrow: document.getElementById("about-frag-eyebrow")?.textContent || null,
    point1Title: point1?.querySelector("strong")?.textContent || null,
    point1Body: point1?.querySelector("span")?.textContent || null,
    hasIconSvg: !!(icon && icon.querySelector("svg")),
    badgeAfter: getComputedStyle(document.getElementById("about-badge"), "::after").content,
    css,
    oldPath: !!document.getElementById("hv-s4"),
  };
});

await page.evaluate(() => {
  document.getElementById("about")?.scrollIntoView({ block: "start" });
});
await new Promise((r) => setTimeout(r, 400));
fs.mkdirSync("public/marketing/qa-shots", { recursive: true });
await page.screenshot({
  path: "public/marketing/qa-shots/problem-section-live.png",
  fullPage: false,
});
fs.writeFileSync("tmp-problem-live-state.json", JSON.stringify(state, null, 2));
console.log(JSON.stringify(state, null, 2));
await browser.close();
