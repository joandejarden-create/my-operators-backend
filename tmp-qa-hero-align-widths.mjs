import puppeteer from "puppeteer";

const url = "https://www.dealality.com/old-home?heroalign=qa2=" + Date.now();
const widths = [1024, 1280, 1440];
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

const out = {};
for (const width of widths) {
  const page = await browser.newPage();
  await page.setViewport({ width, height: 900, deviceScaleFactor: 1 });
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForSelector("#hero-inner", { timeout: 30000 });
  await new Promise((r) => setTimeout(r, 800));
  out[width] = await page.evaluate(() => {
    const logo = document.querySelector("#nav-logo, .oh-nav a, #nav a");
    const inner = document.querySelector("#hero-inner");
    const sst = document.querySelector("#section-subtitle");
    const fsw = document.querySelector("#form-subscribe-wrap, #fsw-btn");
    const aboutBadge = document.querySelector("#about-badge, #about .oh-badge, #about");
    const left = (el) => (el ? Math.round(el.getBoundingClientRect().left) : null);
    return {
      logo: left(logo),
      inner: left(inner),
      sst: left(sst),
      fsw: left(fsw),
      about: left(aboutBadge),
      deltaLogoInner: left(inner) - left(logo),
      marginLeft: getComputedStyle(inner).marginLeft,
      heroPadLeft: getComputedStyle(document.querySelector("#hero")).paddingLeft,
    };
  });
  await page.close();
}
await browser.close();
console.log(JSON.stringify(out, null, 2));
