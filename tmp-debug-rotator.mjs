import puppeteer from "puppeteer";
const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?v=" + Date.now(), { waitUntil: "networkidle2", timeout: 90000 });
await new Promise((r) => setTimeout(r, 2000));
const info = await page.evaluate(() => {
  const rot = document.getElementById("rotator");
  const wrap = document.getElementById("hrwrap");
  const h1 = document.getElementById("h1wrap");
  return {
    rotTransform: rot && getComputedStyle(rot).transform,
    rotInline: rot && rot.style.transform,
    hrH: wrap && wrap.getBoundingClientRect().height,
    hrW: wrap && wrap.style.getPropertyValue("--hr-w"),
    hrLh: h1 && h1.style.getPropertyValue("--hr-lh"),
    children: rot ? rot.children.length : 0,
    onText: rot && [...rot.children].find((c) => c.classList.contains("on") || c.classList.contains("oh-hrword-on"))?.textContent,
  };
});
console.log(info);
await browser.close();
