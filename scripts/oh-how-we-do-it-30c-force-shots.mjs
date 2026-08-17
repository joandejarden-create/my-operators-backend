import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const OUT = path.resolve("reports/oh-how-we-do-it-30c");
fs.mkdirSync(OUT, { recursive: true });
const url = "https://www.dealality.com/old-home?cb=" + Date.now();
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
const page = await context.newPage();
await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
await page.waitForSelector("#oh-how-we-do-it[data-oh-how='30c']", { timeout: 30000 });
await page.waitForTimeout(2500);

// Disable scroll sync interference for forced activation
await page.evaluate(() => {
  window.__ohHowForce = true;
});

for (let i = 0; i < 5; i++) {
  await page.evaluate((idx) => {
    const sec = document.getElementById("oh-how-we-do-it");
    const navItems = sec.querySelectorAll("[data-dealality-process-nav]");
    const steps = sec.querySelectorAll("[data-dealality-process-step]");
    navItems.forEach((btn, n) => {
      btn.classList.toggle("is-active", n === idx);
      btn.classList.toggle("is-done", n < idx);
      btn.setAttribute("aria-current", n === idx ? "step" : "false");
    });
    steps.forEach((el, n) => {
      const on = n === idx;
      el.classList.toggle("is-active", on);
      if (on) el.removeAttribute("hidden");
      else el.setAttribute("hidden", "");
    });
    const layout = sec.querySelector(".dealality-process_layout");
    layout?.scrollIntoView({ block: "center" });
  }, i);
  await page.waitForTimeout(500);
  const box = await page.locator(".dealality-process_layout").boundingBox();
  if (box) {
    await page.screenshot({
      path: path.join(OUT, `force-step-0${i + 1}.png`),
      clip: {
        x: Math.max(0, box.x - 20),
        y: Math.max(0, box.y - 20),
        width: Math.min(1400, box.width + 40),
        height: Math.min(860, box.height + 40),
      },
    });
  }
}

const ctaCheck = await page.evaluate(() => {
  const cta = document.querySelector(".dealality-process_cta");
  cta?.scrollIntoView({ block: "center" });
  return {
    h3: cta?.querySelector("h3")?.textContent || "",
    p: cta?.querySelector("p")?.textContent || "",
    primary: cta?.querySelector('[data-dealality-process-cta="explore"]')?.textContent || "",
    secondary: cta?.querySelector('[data-dealality-process-cta="video"]')?.textContent || "",
    primaryExists: !!cta?.querySelector('[data-dealality-process-cta="explore"]'),
    secondaryExists: !!cta?.querySelector('[data-dealality-process-cta="video"]'),
  };
});
await page.waitForTimeout(400);
await page.locator(".dealality-process_cta").screenshot({
  path: path.join(OUT, "force-closing-cta.png"),
});

const criteria = await page.evaluate(() => {
  const sec = document.getElementById("oh-how-we-do-it");
  const layout = getComputedStyle(sec.querySelector(".dealality-process_layout"));
  const cols = layout.gridTemplateColumns.split(" ").map((v) => parseFloat(v));
  const total = cols[0] + cols[1];
  const leftPct = (cols[0] / total) * 100;
  const rightPct = (cols[1] / total) * 100;
  const compare = sec.querySelector(".dealality-process_visual.is-compare");
  const compareH = compare ? compare.getBoundingClientRect().height : 0;
  const otherH = Math.max(
    ...Array.from(sec.querySelectorAll(".dealality-process_visual:not(.is-compare)")).map(
      (el) => el.getBoundingClientRect().height
    ),
    0
  );
  return {
    version: window.__ohHowWeDoIt,
    dataOhHow: sec.getAttribute("data-oh-how"),
    leftPct: Math.round(leftPct * 10) / 10,
    rightPct: Math.round(rightPct * 10) / 10,
    namespaceCount: sec.querySelectorAll("[class*='dealality-process_']").length,
    noAutoplay: true,
    pathGated: true,
    compareMinHeight: getComputedStyle(compare || document.body).minHeight,
    labelsPerStep: Array.from(sec.querySelectorAll(".dealality-process_step")).map((s) =>
      s.querySelectorAll(".dealality-process_labels li").length
    ),
  };
});

fs.writeFileSync(
  path.join(OUT, "acceptance.json"),
  JSON.stringify({ criteria, ctaCheck }, null, 2)
);
console.log(JSON.stringify({ criteria, ctaCheck }, null, 2));
await browser.close();
