import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await page.goto("https://www.dealality.com/old-home?cb=navcheck" + Date.now(), {
  waitUntil: "domcontentloaded",
  timeout: 90000,
});
await page.waitForSelector("#oh-how-we-do-it", { timeout: 45000 });
await page.waitForTimeout(2500);

async function check(idx) {
  await page.evaluate((i) => {
    document.querySelector(`[data-dealality-process-nav="${i}"]`)?.click();
  }, idx);
  await page.waitForTimeout(1200);
  return page.evaluate(() => {
    const nav = document.querySelector("#oh-how-we-do-it .dealality-process_nav");
    const layout = document.querySelector("#oh-how-we-do-it .dealality-process_layout");
    const sec = document.getElementById("oh-how-we-do-it");
    let el = nav;
    const overflowAncestors = [];
    while (el && el !== document.body) {
      const cs = getComputedStyle(el);
      if (["auto", "scroll", "hidden", "clip"].includes(cs.overflowY) || ["auto", "scroll", "hidden", "clip"].includes(cs.overflow)) {
        overflowAncestors.push({
          tag: el.tagName,
          id: el.id,
          className: String(el.className).slice(0, 80),
          overflow: cs.overflow,
          overflowY: cs.overflowY,
        });
      }
      el = el.parentElement;
    }
    const nr = nav.getBoundingClientRect();
    return {
      layoutCols: getComputedStyle(layout).gridTemplateColumns,
      navPos: getComputedStyle(nav).position,
      navTopCss: getComputedStyle(nav).top,
      navRect: { top: Math.round(nr.top), left: Math.round(nr.left), width: Math.round(nr.width), height: Math.round(nr.height), bottom: Math.round(nr.bottom) },
      navInViewport: nr.bottom > 0 && nr.top < window.innerHeight && nr.width > 50,
      overflowAncestors,
      sectionOverflow: getComputedStyle(sec).overflow,
    };
  });
}

for (const i of [0, 2, 4]) {
  const r = await check(i);
  console.log("=== step", i + 1, "===");
  console.log(JSON.stringify(r, null, 2));
}
await browser.close();
