import puppeteer from "puppeteer";
import fs from "fs";

const out = "tmp-screenshots-tiles";
fs.mkdirSync(out, { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?v=quote-tiles-a", {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 3500));

const metrics = await page.evaluate(() => {
  const root =
    document.getElementById("testimonials") || document.getElementById("trust");
  if (!root) return { found: false };
  root.scrollIntoView({ block: "center" });
  const css = !!document.querySelector('link[data-oh-quote-tiles="1"]');
  const cssHref = (
    document.querySelector('link[data-oh-quote-tiles="1"]') || {}
  ).href;
  const active =
    root.querySelector("[data-slide].is-active") ||
    root.querySelector("[data-slide]");
  const arts = active
    ? [...active.querySelectorAll("article")].map((a, i) => {
        const s = getComputedStyle(a);
        const r = a.getBoundingClientRect();
        const p = a.querySelector("p");
        const pr = p ? p.getBoundingClientRect() : null;
        return {
          i,
          display: s.display,
          h: Math.round(r.height),
          cardBottom: Math.round(r.bottom),
          pBottom: pr ? Math.round(pr.bottom) : null,
          inside: pr ? pr.bottom <= r.bottom + 1 : null,
          gap: pr ? Math.round(r.bottom - pr.bottom) : null,
        };
      })
    : [];
  const visible = arts.filter((a) => a.display !== "none").length;
  return { found: true, css, cssHref, visible, arts, slides: root.querySelectorAll("[data-slide]").length };
});

await new Promise((r) => setTimeout(r, 400));
await page.screenshot({ path: `${out}/trust-after.png` });

const dots = await page.$$("#testimonials-dots button, #trust #testimonials-dots button");
if (dots[1]) {
  await dots[1].click();
  await new Promise((r) => setTimeout(r, 900));
  const afterClick = await page.evaluate(() => {
    const root =
      document.getElementById("testimonials") || document.getElementById("trust");
    const active = root.querySelector("[data-slide].is-active");
    const arts = active
      ? [...active.querySelectorAll("article")].map((a) => getComputedStyle(a).display)
      : [];
    return { visible: arts.filter((d) => d !== "none").length, displays: arts };
  });
  await page.screenshot({ path: `${out}/trust-after-click2.png` });
  console.log("afterClick", JSON.stringify(afterClick));
}

console.log(JSON.stringify(metrics, null, 2));
await browser.close();
