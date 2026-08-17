import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await page.goto("https://www.dealality.com/old-home?cb=" + Date.now(), {
  waitUntil: "domcontentloaded",
  timeout: 90000,
});
await page.waitForTimeout(4000);

const result = await page.evaluate(() => {
  const links = [...document.querySelectorAll('link[rel="stylesheet"]')].map((l) => l.href);
  const freeform = links.filter((h) => /freeform-head/i.test(h));
  const ohTt = document.getElementById("oh-tt");
  const ohTtText = ohTt ? ohTt.textContent || ohTt.innerHTML : "";
  const compact = ohTtText.replace(/\s+/g, "");
  const slide = document.querySelector("#testimonials-viewport > div[data-slide]");
  const grid = slide
    ? slide.querySelector(":scope > div, .oh-testimonial-grid")
    : document.querySelector("#testimonials-viewport .oh-testimonial-grid, #testimonials-viewport > div > div");
  const articles = grid ? [...grid.querySelectorAll(":scope > article")] : [];
  const styles = articles.map((a, i) => {
    const cs = getComputedStyle(a);
    const r = a.getBoundingClientRect();
    return {
      i,
      display: cs.display,
      visibility: cs.visibility,
      width: Math.round(r.width),
      height: Math.round(r.height),
      left: Math.round(r.left),
      top: Math.round(r.top),
    };
  });
  const gridCs = grid ? getComputedStyle(grid) : null;
  return {
    freeform,
    hasW16: freeform.some((h) => /w16/i.test(h)),
    hasW22: freeform.some((h) => /w22/i.test(h)),
    ohTtPresent: !!ohTt,
    ohTtHas2Col: /repeat\(2/.test(ohTtText),
    ohTtHides2nd: /nth-child\(n\+2\)\{display:none/.test(compact),
    ohTtShows2nd: /nth-child\(n\+2\)\{display:flex/.test(compact),
    modules30d: !!document.querySelector('script[src*="ohmodulestabfix30d"]'),
    modulesW16: !!document.querySelector('script[src*="ohmodulestabfixw16"]'),
    boot30b: !!document.querySelector('script[src*="boot-guard.v20260730b"]'),
    fouc29e: !!document.querySelector('script[src*="fouc-gate.v20260729e"]'),
    articleCount: articles.length,
    gridCols: gridCs ? gridCs.gridTemplateColumns : null,
    styles,
    twoUpVisible:
      styles.length >= 2 &&
      styles[0].display !== "none" &&
      styles[1].display !== "none" &&
      styles[1].width > 100 &&
      Math.abs(styles[0].top - styles[1].top) < 80,
  };
});

console.log(JSON.stringify(result, null, 2));
await browser.close();
