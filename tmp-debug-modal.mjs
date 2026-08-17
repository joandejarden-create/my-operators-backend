import puppeteer from "puppeteer";
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.setViewport({ width: 1280, height: 800 });
await page.goto("https://www.dealality.com/old-home?v=" + Date.now(), { waitUntil: "networkidle2", timeout: 90000 });
await page.click("#hero-video-poster");
await new Promise((r) => setTimeout(r, 1000));
const info = await page.evaluate(() => {
  const m = document.getElementById("oh-video-modal");
  const d = document.getElementById("oh-video-dialog");
  const v = document.getElementById("oh-video-player");
  const cs = m && getComputedStyle(m);
  const ds = d && getComputedStyle(d);
  return {
    modalExists: !!m,
    open: m && m.classList.contains("is-open"),
    hidden: m && m.hasAttribute("hidden"),
    display: cs && cs.display,
    visibility: cs && cs.visibility,
    opacity: cs && cs.opacity,
    zIndex: cs && cs.zIndex,
    dialogDisplay: ds && ds.display,
    dialogRect: d && (() => { const r = d.getBoundingClientRect(); return { w: r.width, h: r.height, y: r.y }; })(),
    videoExists: !!v,
  };
});
console.log(info);
await page.screenshot({ path: "tmp-hero-video-shots/video-modal.png", fullPage: false });
await browser.close();
