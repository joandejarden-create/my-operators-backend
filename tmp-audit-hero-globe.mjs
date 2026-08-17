import puppeteer from "puppeteer";
import fs from "fs";

fs.mkdirSync("tmp-screenshots-tiles", { recursive: true });

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?cb=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 4000));

const info = await page.evaluate(() => {
  const hero = document.getElementById("hero");
  const globe = document.getElementById("hero-globe");
  const container = document.getElementById("hero-globe-container");
  const canvas = document.querySelector("#hero-globe canvas, #hero-globe-container canvas, canvas");
  const pins = document.querySelectorAll("[id^='hg-item'], [id^='hg-pin'], .hg-pin");
  const scripts = [...document.querySelectorAll("script[src]")]
    .map((s) => s.src)
    .filter((s) => /hero|globe|rotat|signal|boot|fouc/i.test(s));
  const links = [...document.querySelectorAll("link[href]")]
    .map((l) => l.href)
    .filter((h) => /hero|globe|rotat|signal|freeform|dark/i.test(h));
  const heroText = hero ? (hero.innerText || "").slice(0, 500) : null;
  const rotating =
    document.querySelector("[data-rotate], .oh-hero-rotate, #hero-rotate, #hero-signals, #hero-copy-rotate") ||
    document.getElementById("hero-signals") ||
    document.getElementById("hero-rotator");
  const allHeroIds = hero
    ? [...hero.querySelectorAll("[id]")]
        .slice(0, 80)
        .map((el) => el.id)
    : [];
  const globeStyle = globe
    ? {
        display: getComputedStyle(globe).display,
        visibility: getComputedStyle(globe).visibility,
        opacity: getComputedStyle(globe).opacity,
        height: Math.round(globe.getBoundingClientRect().height),
        width: Math.round(globe.getBoundingClientRect().width),
        ariaHidden: globe.getAttribute("aria-hidden"),
      }
    : null;
  return {
    hasHero: !!hero,
    hasGlobe: !!globe,
    hasContainer: !!container,
    canvasCount: document.querySelectorAll("canvas").length,
    pinCount: pins.length,
    globeStyle,
    rotatingId: rotating && rotating.id,
    rotatingHtml: rotating ? rotating.outerHTML.slice(0, 300) : null,
    heroText,
    allHeroIds,
    scripts,
    links,
    windowFlags: {
      __ohBootGuard: window.__ohBootGuard,
      globe: typeof window.__ohHeroGlobe,
      signals: typeof window.__ohHeroSignals,
    },
  };
});

console.log(JSON.stringify(info, null, 2));
await page.screenshot({
  path: "tmp-screenshots-tiles/hero-live.png",
  fullPage: false,
});
await browser.close();
