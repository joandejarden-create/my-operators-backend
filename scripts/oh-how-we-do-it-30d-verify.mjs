/**
 * Bug evidence + post-fix verification for How We Do It 30d.
 */
import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const OUT = path.resolve("reports/oh-how-we-do-it-30d");
fs.mkdirSync(OUT, { recursive: true });
const url = process.argv[2] || "https://www.dealality.com/old-home?cb=" + Date.now();
const mode = process.argv[3] || "verify"; // bug | verify

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 1,
});
const page = await context.newPage();
await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
await page.waitForSelector("#oh-how-we-do-it", { timeout: 45000 });
await page.waitForTimeout(2500);

const audit = await page.evaluate(() => {
  const sec = document.getElementById("oh-how-we-do-it");
  if (!sec) return { error: "no section" };
  const runway = sec.querySelector(".dealality-process_runway");
  const steps = [...sec.querySelectorAll("[data-dealality-process-step]")].map((el) => {
    const cs = getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return {
      id: el.id,
      title: el.querySelector(".dealality-process_step-title")?.textContent || "",
      hiddenAttr: el.hasAttribute("hidden"),
      display: cs.display,
      opacity: cs.opacity,
      visibility: cs.visibility,
      height: Math.round(r.height),
      isActive: el.classList.contains("is-active"),
    };
  });
  return {
    version: window.__ohHowWeDoIt || null,
    dataOhHow: sec.getAttribute("data-oh-how"),
    runwayHeight: runway ? Math.round(runway.getBoundingClientRect().height) : null,
    runwayCssHeight: runway ? getComputedStyle(runway).height : null,
    steps,
    cssHref: document.querySelector('link[href*="how-we-do-it"]')?.href || null,
  };
});
fs.writeFileSync(path.join(OUT, `${mode}-audit.json`), JSON.stringify(audit, null, 2));
console.log(JSON.stringify(audit, null, 2));

await page.evaluate(() => {
  document.getElementById("oh-how-we-do-it")?.scrollIntoView({ block: "start" });
});
await page.waitForTimeout(600);
await page.screenshot({ path: path.join(OUT, `${mode}-intro.png`) });

for (let i = 0; i < 5; i++) {
  await page.evaluate((idx) => {
    const btn = document.querySelector(`[data-dealality-process-nav="${idx}"]`);
    if (btn) btn.click();
  }, i);
  await page.waitForTimeout(1100);
  const title = await page.evaluate((idx) => {
    const el = document.querySelector(`[data-dealality-process-step="${idx}"]`);
    const r = el?.getBoundingClientRect();
    return {
      title: el?.querySelector(".dealality-process_step-title")?.textContent || "",
      top: r ? Math.round(r.top) : null,
      height: r ? Math.round(r.height) : null,
      visibleText: (el?.innerText || "").slice(0, 120),
    };
  }, i);
  console.log("step", i + 1, title);
  await page.screenshot({ path: path.join(OUT, `${mode}-step-0${i + 1}.png`) });
}

await context.close();
await browser.close();
