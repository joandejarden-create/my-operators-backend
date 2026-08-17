import puppeteer from "puppeteer";
import fs from "fs";

fs.mkdirSync("tmp-hero-video-shots", { recursive: true });
fs.mkdirSync("public/marketing/qa-shots", { recursive: true });

const url = `https://www.dealality.com/old-home?v=${Date.now()}`;
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await new Promise((r) => setTimeout(r, 2500));

const info = await page.evaluate(() => {
  const g = document.getElementById("hero-globe");
  const c = document.getElementById("oh-globe-canvas");
  const cs = g && getComputedStyle(g);
  return {
    parent: g && g.parentElement && g.parentElement.id,
    hiddenAttr: g && g.hasAttribute("hidden"),
    display: cs && cs.display,
    opacity: cs && cs.opacity,
    position: cs && cs.position,
    canvas: !!c,
    canvasSize: c && { w: c.clientWidth, h: c.clientHeight },
    videoPoster: !!document.getElementById("hero-video-poster"),
    scripts: [...document.scripts].map((s) => s.src).filter((s) => /globe|three/i.test(s)),
    freeformCss: [...document.styleSheets]
      .map((s) => s.href)
      .filter((h) => h && /freeform-head|globe-bg/i.test(h)),
  };
});
console.log(JSON.stringify(info, null, 2));

await page.screenshot({
  path: "tmp-hero-video-shots/desktop-hero-globe-bg.png",
  fullPage: false,
});
fs.copyFileSync(
  "tmp-hero-video-shots/desktop-hero-globe-bg.png",
  "public/marketing/qa-shots/desktop-hero-globe-bg.png"
);

await page.setViewport({ width: 390, height: 844 });
await page.reload({ waitUntil: "networkidle2", timeout: 90000 });
await new Promise((r) => setTimeout(r, 2000));
await page.screenshot({
  path: "tmp-hero-video-shots/mobile-hero-globe-bg.png",
  fullPage: false,
});
fs.copyFileSync(
  "tmp-hero-video-shots/mobile-hero-globe-bg.png",
  "public/marketing/qa-shots/mobile-hero-globe-bg.png"
);

await browser.close();
