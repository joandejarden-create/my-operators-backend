import fs from "fs";
import puppeteer from "puppeteer";

const out = "tmp-screenshots-dch-ref";
fs.mkdirSync(out, { recursive: true });
const url = "https://deal-capture-home.webflow.io/";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 1 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await new Promise((r) => setTimeout(r, 1500));
await page.screenshot({ path: `${out}/desktop-top.png` });

for (const [i, y] of [
  [1, 900],
  [2, 1800],
  [3, 2700],
  [4, 3600],
].entries()) {
  await page.evaluate((yy) => window.scrollTo(0, yy), y);
  await new Promise((r) => setTimeout(r, 500));
  await page.screenshot({ path: `${out}/desktop-s${i + 1}.png` });
}

const info = await page.evaluate(() => {
  const pick = (sel) =>
    [...document.querySelectorAll(sel)].slice(0, 25).map((el) => {
      const cs = getComputedStyle(el);
      return {
        tag: el.tagName,
        class: (el.className || "").toString().slice(0, 100),
        text: (el.innerText || "").trim().slice(0, 100),
        bg: cs.backgroundColor,
        color: cs.color,
        font: cs.fontFamily,
        size: cs.fontSize,
        weight: cs.fontWeight,
        radius: cs.borderRadius,
        pad: cs.padding,
      };
    });

  return {
    title: document.title,
    body: {
      bg: getComputedStyle(document.body).backgroundColor,
      color: getComputedStyle(document.body).color,
      font: getComputedStyle(document.body).fontFamily,
    },
    headings: pick("h1,h2,h3"),
    buttons: pick("a.w-button, .button, .primary-button, [class*='Button'] a"),
    sections: [...document.querySelectorAll("section")].map((el) => ({
      class: (el.className || "").toString().slice(0, 80),
      id: el.id,
      bg: getComputedStyle(el).backgroundColor,
      heading: (el.querySelector("h1,h2,h3") || {}).innerText?.trim()?.slice(0, 90),
    })),
    text: document.body.innerText.slice(0, 3000),
  };
});

fs.writeFileSync(`${out}/structure.json`, JSON.stringify(info, null, 2));
console.log(JSON.stringify(info, null, 2).slice(0, 5000));
await browser.close();
