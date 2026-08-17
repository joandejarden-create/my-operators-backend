import puppeteer from "puppeteer";
import fs from "fs";

const OUT = "tmp-hero-video-shots";
const QA = "public/marketing/qa-shots";
fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(QA, { recursive: true });

const url = `https://www.dealality.com/old-home?v=${Date.now()}`;
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });

async function shot(name, width, height, openModal = false) {
  const page = await browser.newPage();
  await page.setViewport({ width, height, deviceScaleFactor: 1 });
  await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  await page.waitForSelector("#hero-video-poster", { timeout: 30000 });

  const before = await page.evaluate(() => ({
    videoEl: !!document.getElementById("oh-video-player"),
    poster: !!document.getElementById("hero-video-poster"),
    cta: (document.getElementById("fsw-secondary") || {}).textContent?.trim(),
    primary: (document.getElementById("fsw-btn") || {}).textContent?.trim(),
  }));

  if (openModal) {
    await page.click("#hero-video-poster");
    await page.waitForFunction(
      () => {
        const m = document.getElementById("oh-video-modal");
        return m && m.classList.contains("is-open") && getComputedStyle(m).display === "flex";
      },
      { timeout: 15000 }
    );
    await new Promise((r) => setTimeout(r, 1200));
    const after = await page.evaluate(() => {
      const m = document.getElementById("oh-video-modal");
      const v = document.getElementById("oh-video-player");
      const d = document.getElementById("oh-video-dialog");
      const r = d?.getBoundingClientRect();
      return {
        open: m?.classList.contains("is-open"),
        display: m && getComputedStyle(m).display,
        videoExists: !!v,
        videoSrc: v?.currentSrc || v?.src || null,
        dialog: r && { w: Math.round(r.width), h: Math.round(r.height) },
      };
    });
    console.log(name, { before, after });
    await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: false });
  } else {
    const lazy = await page.evaluate(() => !!document.getElementById("oh-video-player"));
    console.log(name, { before, videoLoadedBeforeClick: lazy });
    await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: false });
  }

  fs.copyFileSync(`${OUT}/${name}.png`, `${QA}/${name}.png`);
  await page.close();
}

await shot("desktop-hero", 1440, 900, false);
await shot("mobile-hero", 390, 844, false);
await shot("video-modal", 1440, 900, true);

// 360px modal smoke
const page = await browser.newPage();
await page.setViewport({ width: 360, height: 740 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await page.click("#hero-video-poster");
await page.waitForFunction(
  () => {
    const m = document.getElementById("oh-video-modal");
    return m && getComputedStyle(m).display === "flex";
  },
  { timeout: 15000 }
);
const mobileModal = await page.evaluate(() => {
  const d = document.getElementById("oh-video-dialog");
  const r = d.getBoundingClientRect();
  return { w: Math.round(r.width), h: Math.round(r.height), video: !!document.getElementById("oh-video-player") };
});
console.log("mobile360-modal", mobileModal);
await page.screenshot({ path: `${OUT}/video-modal-360.png`, fullPage: false });
fs.copyFileSync(`${OUT}/video-modal-360.png`, `${QA}/video-modal-360.png`);
await page.close();

await browser.close();
console.log("done");
