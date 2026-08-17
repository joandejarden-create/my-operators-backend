import puppeteer from "puppeteer";
import fs from "fs";

fs.mkdirSync("tmp-screenshots-tiles", { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
const logs = [];
page.on("console", (m) => logs.push({ type: m.type(), text: m.text() }));
page.on("pageerror", (e) => logs.push({ type: "pageerror", text: String(e) }));

await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?cb=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 2000));

async function snap(label) {
  const data = await page.evaluate(() => {
    const rot = document.getElementById("rotator");
    const wrap = document.getElementById("hrwrap");
    const on = document.querySelector("#rotator .oh-hrword-on, #rotator .on");
    const cs = rot ? getComputedStyle(rot) : null;
    const wcs = wrap ? getComputedStyle(wrap) : null;
    return {
      active: on ? (on.textContent || "").trim() : null,
      transform: cs && cs.transform,
      transition: cs && cs.transition,
      wrapOverflow: wcs && wcs.overflow,
      wrapHeight: wcs && wcs.height,
      hrLh: wrap && wrap.style.getPropertyValue("--hr-lh"),
      hrW: wrap && wrap.style.getPropertyValue("--hr-w"),
      flag: window.__ohHeroRotator,
      scripts: [...document.querySelectorAll("script[src]")]
        .map((s) => s.src)
        .filter((s) => /rotator|footer-oh|boot-guard/i.test(s)),
      wordClasses: rot
        ? [...rot.children].slice(0, 6).map((c) => ({
            t: (c.textContent || "").trim(),
            cls: c.className,
            opacity: getComputedStyle(c).opacity,
          }))
        : [],
    };
  });
  return { label, ...data };
}

const samples = [];
for (let i = 0; i < 6; i++) {
  samples.push(await snap("t" + i));
  await page.screenshot({
    path: `tmp-screenshots-tiles/rotator-t${i}.png`,
    clip: { x: 0, y: 80, width: 900, height: 420 },
  });
  await new Promise((r) => setTimeout(r, 3500));
}

fs.writeFileSync(
  "tmp-rotator-debug.json",
  JSON.stringify({ samples, logs: logs.slice(0, 30) }, null, 2)
);
console.log(JSON.stringify({ samples, logs: logs.slice(0, 20) }, null, 2));
await browser.close();
