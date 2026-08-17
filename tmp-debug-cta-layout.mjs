import puppeteer from "puppeteer";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?v=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
const info = await page.evaluate(() => {
  const ids = [
    "form-subscribe-wrap",
    "fsw-shell",
    "fsw-inner",
    "fsw-btn-wrap",
    "fsw-secondary-wrap",
    "fsw-secondary",
    "h1wrap",
    "hrwrap",
  ];
  const out = {};
  for (const id of ids) {
    const el = document.getElementById(id);
    if (!el) {
      out[id] = null;
      continue;
    }
    const cs = getComputedStyle(el);
    const r = el.getBoundingClientRect();
    out[id] = {
      display: cs.display,
      position: cs.position,
      flexDirection: cs.flexDirection,
      alignItems: cs.alignItems,
      marginTop: cs.marginTop,
      top: cs.top,
      left: cs.left,
      width: Math.round(r.width),
      height: Math.round(r.height),
      y: Math.round(r.y),
      x: Math.round(r.x),
    };
  }
  return out;
});
console.log(JSON.stringify(info, null, 2));
await browser.close();
