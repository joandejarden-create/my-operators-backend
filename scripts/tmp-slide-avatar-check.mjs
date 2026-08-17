import puppeteer from "puppeteer";
import fs from "fs";

fs.mkdirSync("tmp-screenshots-tiles", { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto(`https://www.dealality.com/old-home?cb=${Date.now()}`, {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 3000));

async function dump(label) {
  const m = await page.evaluate(() => {
    const root =
      document.getElementById("testimonials") || document.getElementById("trust");
    root.scrollIntoView({ block: "center" });
    return [...root.querySelectorAll("[data-slide]")].map((s) => {
      const sc = getComputedStyle(s);
      return {
        id: s.getAttribute("data-slide"),
        active: s.classList.contains("is-active"),
        slideVis: sc.visibility,
        slideOp: sc.opacity,
        imgs: [...s.querySelectorAll("img")].map((img) => {
          const cs = getComputedStyle(img);
          const r = img.getBoundingClientRect();
          return {
            src: img.src.slice(-45),
            nat: img.naturalWidth,
            complete: img.complete,
            vis: cs.visibility,
            op: cs.opacity,
            disp: cs.display,
            w: Math.round(r.width),
            h: Math.round(r.height),
            top: Math.round(r.top),
          };
        }),
      };
    });
  });
  console.log(label, JSON.stringify(m, null, 2));
  return m;
}

await dump("initial");
await page.screenshot({ path: "tmp-screenshots-tiles/slide-check-0.png" });

const dots = await page.$$("#testimonials-dots button");
if (dots[0]) {
  await dots[0].click();
  await new Promise((r) => setTimeout(r, 1000));
  await dump("dot0");
  await page.screenshot({ path: "tmp-screenshots-tiles/slide-check-dot0.png" });
}
if (dots[1]) {
  await dots[1].click();
  await new Promise((r) => setTimeout(r, 1000));
  await dump("dot1");
  await page.screenshot({ path: "tmp-screenshots-tiles/slide-check-dot1.png" });
}

await browser.close();
