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
await new Promise((r) => setTimeout(r, 2000));

async function sample() {
  return page.evaluate(() => {
    const on = document.querySelector("#rotator .oh-hrword-on, #rotator .on");
    const list = document.getElementById("hero-globe-list");
    const canvas = document.getElementById("oh-globe-canvas");
    return {
      active: on ? (on.textContent || "").trim() : null,
      listInDom: !!list,
      listParent: list && list.parentElement && list.parentElement.id,
      canvasOk: !!(canvas && canvas.width > 0),
      globeKids: document.getElementById("hero-globe")
        ? [...document.getElementById("hero-globe").children].map((c) => c.id)
        : [],
      containerKids: document.getElementById("hero-globe-container")
        ? [...document.getElementById("hero-globe-container").children].map(
            (c) => c.id || c.tagName
          )
        : [],
    };
  });
}

const samples = [];
for (let i = 0; i < 6; i++) {
  samples.push(await sample());
  await new Promise((r) => setTimeout(r, 1800));
}
console.log(JSON.stringify(samples, null, 2));
await browser.close();
