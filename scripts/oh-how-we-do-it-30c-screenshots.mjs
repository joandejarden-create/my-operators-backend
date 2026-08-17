/**
 * Viewport screenshots for How We Do It 30c (sticky runway is too tall for element shots).
 */
import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const OUT = path.resolve("reports/oh-how-we-do-it-30c");
fs.mkdirSync(OUT, { recursive: true });
const url = "https://www.dealality.com/old-home?cb=" + Date.now();

const browser = await chromium.launch({ headless: true });

async function captureDesktop() {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 1,
  });
  const page = await context.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForSelector("#oh-how-we-do-it", { timeout: 30000 });
  await page.waitForTimeout(2200);

  const audit = await page.evaluate(() => {
    const sec = document.getElementById("oh-how-we-do-it");
    return {
      version: window.__ohHowWeDoIt || null,
      dataOhHow: sec?.getAttribute("data-oh-how") || null,
      eyebrow: (sec?.querySelector(".dealality-process_eyebrow")?.innerText || "").replace(/\s+/g, " ").trim(),
      h2: sec?.querySelector(".dealality-process_h2")?.textContent || "",
      lead: sec?.querySelector(".dealality-process_lead")?.textContent || "",
      cta: sec?.querySelector(".dealality-process_cta h3")?.textContent || "",
      navLabels: Array.from(sec?.querySelectorAll(".dealality-process_nav-label") || []).map((n) => n.textContent),
      activeTitle: sec?.querySelector(".dealality-process_step.is-active .dealality-process_step-title")?.textContent || "",
      visualLarge: !!sec?.querySelector(".dealality-process_visual.is-large"),
      compareStrong: !!sec?.querySelector(".dealality-process_visual.is-compare"),
      cssHref: document.querySelector('link[href*="how-we-do-it.v20260730c"]')?.href || null,
      layoutCols: getComputedStyle(sec?.querySelector(".dealality-process_layout") || document.body).gridTemplateColumns,
    };
  });
  fs.writeFileSync(path.join(OUT, "audit-live.json"), JSON.stringify(audit, null, 2));

  // Scroll section intro into view first
  await page.evaluate(() => {
    document.getElementById("oh-how-we-do-it")?.scrollIntoView({ block: "start" });
  });
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT, "desktop-intro.png") });

  for (let i = 0; i < 5; i++) {
    await page.evaluate((idx) => {
      const btn = document.querySelector(`[data-dealality-process-nav="${idx}"]`);
      if (btn) btn.click();
    }, i);
    await page.waitForTimeout(900);
    await page.evaluate(() => {
      document.querySelector(".dealality-process_layout")?.scrollIntoView({ block: "center" });
    });
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(OUT, `desktop-step-0${i + 1}.png`) });
  }

  await page.evaluate(() => {
    document.querySelector(".dealality-process_cta")?.scrollIntoView({ block: "center" });
  });
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT, "desktop-closing-cta.png") });
  await context.close();
  return audit;
}

async function captureMobile() {
  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 2,
    isMobile: true,
  });
  const page = await context.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForSelector("#oh-how-we-do-it", { timeout: 30000 });
  await page.waitForTimeout(2000);
  await page.evaluate(() => {
    document.getElementById("oh-how-we-do-it")?.scrollIntoView({ block: "start" });
  });
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT, "mobile-stacked.png") });
  const overflow = await page.evaluate(() => {
    const sec = document.getElementById("oh-how-we-do-it");
    return {
      scrollWidth: sec?.scrollWidth,
      clientWidth: sec?.clientWidth,
      bodyScrollWidth: document.documentElement.scrollWidth,
      innerWidth: window.innerWidth,
    };
  });
  fs.writeFileSync(path.join(OUT, "mobile.json"), JSON.stringify(overflow, null, 2));
  await context.close();
}

async function captureReduced() {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    reducedMotion: "reduce",
  });
  const page = await context.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForSelector("#oh-how-we-do-it", { timeout: 30000 });
  await page.waitForTimeout(2000);
  await page.evaluate(() => {
    document.getElementById("oh-how-we-do-it")?.scrollIntoView({ block: "start" });
  });
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(OUT, "desktop-reduced-motion.png") });
  const rm = await page.evaluate(() => ({
    activeSteps: document.querySelectorAll("#oh-how-we-do-it .dealality-process_step.is-active").length,
    totalSteps: document.querySelectorAll("#oh-how-we-do-it .dealality-process_step").length,
  }));
  fs.writeFileSync(path.join(OUT, "reduced-motion.json"), JSON.stringify(rm, null, 2));
  await context.close();
}

const audit = await captureDesktop();
await captureMobile();
await captureReduced();
await browser.close();
console.log("DONE");
console.log(JSON.stringify(audit, null, 2));
