import puppeteer from "puppeteer";

const url = "https://www.dealality.com/old-home?heroalign=qa=" + Date.now();
const browser = await puppeteer.launch({
  headless: true,
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1366, height: 900, deviceScaleFactor: 1 });
await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
await page.waitForSelector("#hero-inner", { timeout: 30000 });
await new Promise((r) => setTimeout(r, 1500));

const metrics = await page.evaluate(() => {
  const nav = document.querySelector("#nav, .oh-nav");
  const logo = document.querySelector("#nav-logo, .oh-nav a, #nav a");
  const hero = document.querySelector("#hero, .oh-hero");
  const inner = document.querySelector("#hero-inner, .oh-hero-inner");
  const sst = document.querySelector("#section-subtitle, .oh-sst-wrap");
  const about = document.querySelector("#about, .oh-about");
  const box = (el) => {
    if (!el) return null;
    const r = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    return {
      left: Math.round(r.left),
      width: Math.round(r.width),
      marginLeft: cs.marginLeft,
      paddingLeft: cs.paddingLeft,
      maxWidth: cs.maxWidth,
    };
  };
  return {
    hasAlignEmbed: !!document.getElementById("oh-hero-align-w17-css") || !!document.getElementById("oh-hero-align-w17"),
    nav: box(nav),
    logo: box(logo),
    hero: box(hero),
    inner: box(inner),
    sst: box(sst),
    about: box(about),
  };
});

await page.screenshot({
  path: "tmp-old-home-hero-align-1366.png",
  fullPage: false,
});
await browser.close();
console.log(JSON.stringify(metrics, null, 2));
