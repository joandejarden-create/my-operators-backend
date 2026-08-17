import puppeteer from "puppeteer";
import fs from "fs";

fs.mkdirSync("tmp-screenshots-tiles", { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
const logs = [];
page.on("console", (m) => logs.push({ type: m.type(), text: m.text() }));
page.on("pageerror", (e) => logs.push({ type: "pageerror", text: String(e) }));

await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?cb=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 5000));

const info = await page.evaluate(() => {
  const list = document.getElementById("hero-globe-list");
  const rotator = document.getElementById("rotator");
  const words = rotator
    ? [...rotator.children].map((c) => ({
        text: (c.textContent || "").trim(),
        className: c.className,
        opacity: getComputedStyle(c).opacity,
        display: getComputedStyle(c).display,
      }))
    : [];
  const canvas = document.getElementById("oh-globe-canvas");
  const cs = canvas ? getComputedStyle(canvas) : null;
  return {
    listExists: !!list,
    listChildCount: list ? list.children.length : 0,
    listHtml: list ? list.innerHTML.slice(0, 400) : null,
    hgItemCount: document.querySelectorAll("[id^='hg-item-']").length,
    rotatorExists: !!rotator,
    rotatorChildCount: rotator ? rotator.children.length : 0,
    words: words.slice(0, 12),
    activeWord: words.find((w) => /on|active/i.test(w.className)),
    canvas: canvas
      ? {
          w: canvas.width,
          h: canvas.height,
          cssW: cs.width,
          cssH: cs.height,
          display: cs.display,
          opacity: cs.opacity,
          visibility: cs.visibility,
        }
      : null,
    three: typeof window.THREE,
    globeErrors: (window.__ohGlobeDebug && window.__ohGlobeDebug) || null,
  };
});

// sample rotator change over time
await new Promise((r) => setTimeout(r, 2500));
const after = await page.evaluate(() => {
  const rotator = document.getElementById("rotator");
  if (!rotator) return null;
  return [...rotator.children]
    .filter((c) => /on|active/i.test(c.className) || getComputedStyle(c).opacity > 0.5)
    .map((c) => (c.textContent || "").trim());
});

console.log(JSON.stringify({ info, after, logs: logs.slice(0, 40) }, null, 2));
await page.screenshot({
  path: "tmp-screenshots-tiles/hero-globe-detail.png",
  fullPage: false,
});
await browser.close();
