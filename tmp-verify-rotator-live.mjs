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
await new Promise((r) => setTimeout(r, 2500));

const samples = [];
for (let i = 0; i < 5; i++) {
  samples.push(
    await page.evaluate(() => {
      const on = document.querySelector("#rotator .oh-hrword-on, #rotator .on");
      const scripts = [...document.querySelectorAll("script[src]")]
        .map((s) => s.src)
        .filter((s) => /rotator|boot-guard|globe-bg/i.test(s));
      return {
        active: on ? (on.textContent || "").trim() : null,
        hasRotatorFlag: !!window.__ohHeroRotator,
        canvas: !!document.getElementById("oh-globe-canvas"),
        scripts,
      };
    })
  );
  await new Promise((r) => setTimeout(r, 3600));
}

console.log(JSON.stringify(samples, null, 2));
await browser.close();
