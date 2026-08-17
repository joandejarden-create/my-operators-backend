import fs from "fs";
import puppeteer from "puppeteer";

const out = "tmp-screenshots-dch-ref";
fs.mkdirSync(out, { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 1100 });
await page.goto("https://deal-capture-home.webflow.io/", {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 1200));

const sections = await page.evaluate(() =>
  [...document.querySelectorAll("section")].map((el, i) => {
    const r = el.getBoundingClientRect();
    return {
      i,
      class: (el.className || "").toString().slice(0, 100),
      id: el.id,
      top: Math.round(r.top + window.scrollY),
      h: Math.round(r.height),
      heading: (el.querySelector("h1,h2,h3") || {}).innerText
        ?.trim()
        ?.slice(0, 120),
    };
  })
);

fs.writeFileSync(`${out}/sections-map.json`, JSON.stringify(sections, null, 2));
console.log(JSON.stringify(sections, null, 2));

for (const s of sections) {
  await page.evaluate((y) => window.scrollTo(0, Math.max(0, y - 60)), s.top);
  await new Promise((r) => setTimeout(r, 300));
  await page.screenshot({ path: `${out}/sec-${s.i}.png` });
}

await browser.close();
