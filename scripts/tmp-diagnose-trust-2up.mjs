/**
 * Diagnose 2-up layout for #trust testimonials.
 */
import puppeteer from "puppeteer";

const url = "https://www.dealality.com/old-home?cb=" + Date.now() + "#trust";
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await page.waitForSelector("#testimonials-viewport, #trust #testimonials-viewport", { timeout: 30000 });

const info = await page.evaluate(() => {
  const root = document.getElementById("testimonials") || document.getElementById("trust");
  const slide = root.querySelector('#testimonials-viewport > div[data-slide="0"]');
  const grid = slide && slide.querySelector(":scope > div");
  const articles = slide ? [...slide.querySelectorAll(":scope > div > article")] : [];
  const ohTt = !!document.getElementById("oh-tt");
  function cs(el) {
    if (!el) return null;
    const s = getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return {
      display: s.display,
      gridTemplateColumns: s.gridTemplateColumns,
      width: Math.round(r.width),
      height: Math.round(r.height),
      top: Math.round(r.top),
      left: Math.round(r.left),
      visibility: s.visibility,
      opacity: s.opacity,
    };
  }
  return {
    ohTt,
    slideDisplay: cs(slide),
    grid: cs(grid),
    articles: articles.map((a, i) => ({ i, ...cs(a), text: (a.querySelector("blockquote")?.textContent || "").slice(0, 60) })),
    sheetOhTt: [...document.styleSheets].some((ss) => {
      try {
        return [...(ss.cssRules || [])].some((r) => String(r.cssText || "").includes("#testimonials-viewport>div[data-slide]>div>article:nth-child"));
      } catch {
        return false;
      }
    }),
  };
});

console.log(JSON.stringify(info, null, 2));
await browser.close();
