import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const url = "https://www.dealality.com/old-home?nocache=w19verify";
const out = path.resolve("tmp-hero-signals-w19-verify.json");

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1366, height: 900 } });
await page.goto(url, { waitUntil: "networkidle", timeout: 90000 });
await page.waitForTimeout(1500);

const result = await page.evaluate(() => {
  const pos = document.getElementById("hs-pos-label");
  const neg = document.getElementById("hs-neg-label");
  const signals = document.getElementById("hero-signals");
  const posBox = document.getElementById("hero-signals-pos");
  const negBox = document.getElementById("hero-signals-neg");
  function lines(el) {
    if (!el) return null;
    const cs = getComputedStyle(el);
    const h = el.getBoundingClientRect().height;
    const lh = parseFloat(cs.lineHeight) || parseFloat(cs.fontSize) * 1.2;
    return { text: el.textContent.trim(), height: h, lineHeight: lh, approxLines: Math.round(h / lh), whiteSpace: cs.whiteSpace, fontSize: cs.fontSize, nowrap: cs.whiteSpace === "nowrap" };
  }
  function box(el) {
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return { width: Math.round(r.width), height: Math.round(r.height) };
  }
  return {
    quietStyle: !!document.getElementById("oh-hero-signals-quiet-w19"),
    quietScript: [...document.scripts].some((s) => (s.src || "").includes("hero-signals-quiet.v20260729w19")),
    oldQuiet: [...document.scripts].some((s) => (s.src || "").includes("hero-signals-quiet.v20260729w18")),
    freeformW22: [...document.querySelectorAll('link[rel="stylesheet"]')].some((l) => (l.href || "").includes("freeform-head.v20260729w22")),
    freeformW21: [...document.querySelectorAll('link[rel="stylesheet"]')].some((l) => (l.href || "").includes("freeform-head.v20260729w21")),
    heroFit29d: [...document.querySelectorAll('link[rel="stylesheet"]')].some((l) => (l.href || "").includes("hero-fit.v20260729d")),
    fouc29b: [...document.scripts].some((s) => (s.src || "").includes("fouc-gate.v20260729b")),
    signals: box(signals),
    pos: box(posBox),
    neg: box(negBox),
    posLabel: lines(pos),
    negLabel: lines(neg),
    equalWidth: posBox && negBox ? Math.abs(posBox.getBoundingClientRect().width - negBox.getBoundingClientRect().width) < 2 : false,
  };
});

await page.screenshot({ path: "tmp-hero-signals-w19-desktop.png", fullPage: false });

// laptop-ish
await page.setViewportSize({ width: 1280, height: 800 });
await page.waitForTimeout(400);
const laptop = await page.evaluate(() => {
  const pos = document.getElementById("hs-pos-label");
  const neg = document.getElementById("hs-neg-label");
  const posBox = document.getElementById("hero-signals-pos");
  const negBox = document.getElementById("hero-signals-neg");
  function lines(el) {
    if (!el) return null;
    const cs = getComputedStyle(el);
    const h = el.getBoundingClientRect().height;
    const lh = parseFloat(cs.lineHeight) || parseFloat(cs.fontSize) * 1.2;
    return { text: el.textContent.trim(), approxLines: Math.round(h / lh), nowrap: cs.whiteSpace === "nowrap" };
  }
  return {
    posLabel: lines(pos),
    negLabel: lines(neg),
    equalWidth: posBox && negBox ? Math.abs(posBox.getBoundingClientRect().width - negBox.getBoundingClientRect().width) < 2 : false,
    posW: Math.round(posBox.getBoundingClientRect().width),
    negW: Math.round(negBox.getBoundingClientRect().width),
  };
});
await page.screenshot({ path: "tmp-hero-signals-w19-laptop.png", fullPage: false });

fs.writeFileSync(out, JSON.stringify({ desktop: result, laptop }, null, 2));
console.log(JSON.stringify({ desktop: result, laptop }, null, 2));
await browser.close();
