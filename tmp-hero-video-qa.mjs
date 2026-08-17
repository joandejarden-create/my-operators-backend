import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const outDir = path.resolve("tmp-hero-video-shots");
fs.mkdirSync(outDir, { recursive: true });
const url = "https://www.dealality.com/old-home?v=" + Date.now();

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

async function shot(name, width, height, interact) {
  const page = await browser.newPage();
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  await page.waitForSelector("#hero-inner", { timeout: 30000 });
  await new Promise((r) => setTimeout(r, 1500));
  if (interact) await interact(page);
  await page.screenshot({
    path: path.join(outDir, name),
    fullPage: false,
  });
  await page.close();
  console.log("wrote", name);
}

const checks = await (async () => {
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });
  await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  const html = await page.content();
  const keys = [
    "hero-video-card",
    "hero-video-poster",
    "Watch Platform Overview",
    "2-minute platform overview",
    "old-home-hero-video",
    "helping owners move faster",
    "brand-explorer",
  ];
  for (const k of keys) console.log(html.includes(k) ? "YES" : "NO", k);
  // confirm no video element until click
  const before = await page.$("#oh-video-player");
  console.log("video_before_click", !!before);
  await page.click("#hero-video-poster");
  await new Promise((r) => setTimeout(r, 800));
  const after = await page.$("#oh-video-player");
  const modalOpen = await page.$eval("#oh-video-modal", (el) =>
    el.classList.contains("is-open")
  );
  console.log("video_after_click", !!after, "modal_open", modalOpen);
  await page.close();
  return { after: !!after, modalOpen };
})();

await shot("desktop-hero.png", 1440, 900);
await shot("mobile-hero.png", 390, 844);
await shot("video-modal.png", 1280, 800, async (page) => {
  await page.click("#hero-video-poster");
  await page.waitForSelector("#oh-video-modal.is-open", { timeout: 10000 });
  await new Promise((r) => setTimeout(r, 600));
});

await browser.close();
console.log("checks", checks);
