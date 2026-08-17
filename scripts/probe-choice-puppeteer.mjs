#!/usr/bin/env node
import puppeteer from "puppeteer";

const url = process.argv[2] || "https://www.choicehotels.com/puerto-rico/levittown/comfort-inn-hotels/pr006";
const browser = await puppeteer.launch({ headless: "new" });
try {
  const page = await browser.newPage();
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
  );
  await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
  const html = await page.content();
  console.log("html len", html.length);
  console.log("amenit count", (html.match(/amenit/gi) || []).length);
  const ld = await page.evaluate(() => {
    const scripts = [...document.querySelectorAll('script[type="application/ld+json"]')];
    return scripts.map((s) => s.textContent).slice(0, 2);
  });
  console.log("ld scripts", ld.length);
  for (const s of ld) {
    try {
      const j = JSON.parse(s);
      console.log(JSON.stringify(j).slice(0, 1200));
    } catch {}
  }
  const chips = await page.evaluate(() => {
    const out = [];
    document.querySelectorAll("[class*='amenit' i], [data-testid*='amenit' i], li").forEach((el) => {
      const t = (el.textContent || "").trim();
      if (t.length > 2 && t.length < 80 && /amenit/i.test(el.className + el.parentElement?.className))
        out.push(t);
    });
    return [...new Set(out)].slice(0, 30);
  });
  console.log("chips", chips);
} finally {
  await browser.close();
}
