import puppeteer from "puppeteer";

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
await new Promise((r) => setTimeout(r, 2500));

const info = await page.evaluate(() => {
  const h1 = document.getElementById("hero-h1");
  const h1wrap = document.getElementById("h1wrap");
  const hstatic = document.getElementById("hstatic");
  const hrwrap = document.getElementById("hrwrap");
  const rot = document.getElementById("rotator");
  function box(el) {
    if (!el) return null;
    const r = el.getBoundingClientRect();
    const s = getComputedStyle(el);
    return {
      display: s.display,
      visibility: s.visibility,
      opacity: s.opacity,
      position: s.position,
      clip: s.clip,
      w: Math.round(r.width),
      h: Math.round(r.height),
      top: Math.round(r.top),
      left: Math.round(r.left),
      text: (el.textContent || "").trim().slice(0, 80),
    };
  }
  return {
    heroH1: box(h1),
    h1wrap: box(h1wrap),
    hstatic: box(hstatic),
    hrwrap: box(hrwrap),
    rot: box(rot),
    reducedMotion: window.matchMedia("(prefers-reduced-motion: reduce)")
      .matches,
    slots: h1wrap && getComputedStyle(h1wrap).getPropertyValue("--hr-slots"),
    lh: h1wrap && getComputedStyle(h1wrap).getPropertyValue("--hr-lh"),
  };
});
console.log(JSON.stringify(info, null, 2));
await browser.close();
