import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const outDir = path.join(
  process.cwd(),
  "docs/old-home-problem-deal-desk-snapshots-cinematic"
);
fs.mkdirSync(outDir, { recursive: true });

const base =
  "http://127.0.0.1:8788/docs/old-home-problem-deal-desk-preview.html";
const states = [
  "opportunity",
  "workstreams",
  "artifacts",
  "comparison",
  "momentum",
  "outcome",
];

const shots = [
  ...states.map((s) => ({
    name: `01-${s}`,
    url: `${base}?dealDeskState=${s}`,
    width: 1440,
    height: 1100,
  })),
  {
    name: "07-tablet-workstreams",
    url: `${base}?dealDeskState=workstreams`,
    width: 900,
    height: 1200,
  },
  {
    name: "08-mobile-opportunity",
    url: `${base}?dealDeskState=opportunity`,
    width: 390,
    height: 980,
  },
];

const browser = await puppeteer.launch({
  headless: true,
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

try {
  for (const shot of shots) {
    const page = await browser.newPage();
    await page.setViewport({
      width: shot.width,
      height: shot.height,
      deviceScaleFactor: 1,
    });
    await page.goto(shot.url, { waitUntil: "networkidle0", timeout: 60000 });
    await page.waitForSelector(".dealality-problem-desk", { timeout: 15000 });
    const desk = await page.$(".dealality-problem-desk");
    const target = desk || (await page.$("#about"));
    const file = path.join(outDir, `${shot.name}.png`);
    await target.screenshot({ path: file });
    const dims = await page.evaluate(() => {
      const root = document.querySelector(".dealality-problem-desk");
      const stage = document.querySelector(".dpd-stage");
      const desk = document.querySelector(".dpd-desk");
      const rs = (el) =>
        el ? { w: Math.round(el.getBoundingClientRect().width), h: Math.round(el.getBoundingClientRect().height) } : null;
      return {
        state: root?.getAttribute("data-story-state"),
        root: rs(root),
        desk: rs(desk),
        stage: rs(stage),
        hasHotel: !!document.querySelector(".dpd-hotel-img")?.complete,
        hotelNatural: document.querySelector(".dpd-hotel-img")?.naturalWidth || 0,
      };
    });
    console.log(JSON.stringify({ file: path.basename(file), ...dims, viewport: { w: shot.width, h: shot.height } }));
    await page.close();
  }
} finally {
  await browser.close();
}
