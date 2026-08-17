import puppeteer from "puppeteer";
import fs from "fs";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.setCacheEnabled(false);
await page.setRequestInterception(true);
page.on("request", (req) => {
  const url = req.url();
  if (/ipapi\.co|api\.country\.is|ipwho\.is|googletagmanager|google-analytics|doubleclick/i.test(url)) {
    req.abort();
    return;
  }
  req.continue();
});

await page.goto(`https://www.dealality.com/old-home?cb=${Date.now()}#about`, {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await page.waitForSelector("#about", { timeout: 20000 });
await page.waitForFunction(
  () => document.getElementById("about")?.getAttribute("data-oh-problem-v2") === "1",
  { timeout: 20000 }
).catch(() => null);

// Wait for CSS pin + chip fade-in (injector retries through ~1.8s)
await new Promise((r) => setTimeout(r, 4500));

const state = await page.evaluate(() => {
  const chip = document.querySelector("#about-frag-scatter .about-frag-chip");
  const hard = document.getElementById("about-frag-hard");
  const hidden = document.getElementById("about-frag-hidden");
  const scripts = [...document.scripts].map((s) => s.src).filter(Boolean);
  const css = [...document.querySelectorAll('link[rel="stylesheet"]')]
    .map((l) => l.href)
    .filter((h) => /freeform-head|problem/i.test(h));
  return {
    flagged: document.getElementById("about")?.getAttribute("data-oh-problem-v2"),
    h2: document.getElementById("about-h2")?.innerText || null,
    leadStart: (document.getElementById("about-lead")?.textContent || "").slice(0, 100),
    badgeAfter: getComputedStyle(document.getElementById("about-badge"), "::after").content,
    point1Title: document.querySelector("#about-point-1 strong")?.textContent || null,
    hasIconSvg: !!document.querySelector("#about-point-1-icon svg"),
    hasFrag: !!document.getElementById("about-frag"),
    chipCount: document.querySelectorAll("#about-frag-scatter .about-frag-chip").length,
    chipOpacity: chip ? getComputedStyle(chip).opacity : null,
    hardOpacity: hard ? getComputedStyle(hard).opacity : null,
    hiddenOpacity: hidden ? getComputedStyle(hidden).opacity : null,
    hardText: hard?.textContent || null,
    hiddenText: hidden?.textContent || null,
    oldPath: !!document.getElementById("hv-s4"),
    scripts: scripts.filter((u) => /problem-v2|freeform-head|footer-oh/i.test(u)),
    css,
  };
});

await page.evaluate(() => {
  document.getElementById("about")?.scrollIntoView({ block: "start" });
});
await new Promise((r) => setTimeout(r, 300));
fs.mkdirSync("public/marketing/qa-shots", { recursive: true });
await page.screenshot({
  path: "public/marketing/qa-shots/problem-section-live.png",
  fullPage: false,
});
const visual = await page.$("#about-visual");
if (visual) {
  await visual.screenshot({
    path: "public/marketing/qa-shots/problem-visual-live.png",
  });
}
fs.writeFileSync("tmp-problem-live-state.json", JSON.stringify(state, null, 2));
console.log(JSON.stringify(state, null, 2));
await browser.close();

const cssJoined = JSON.stringify(state.css);
const ok =
  state.flagged === "1" &&
  state.hasFrag &&
  Number(state.chipOpacity) > 0.9 &&
  Number(state.hardOpacity) > 0.9 &&
  /v20260729d\.js/.test(JSON.stringify(state.scripts)) &&
  /w20\.css/.test(cssJoined) &&
  !/w16\.css/.test(cssJoined);
console.log("QA_OK", ok);
process.exit(ok ? 0 : 2);
