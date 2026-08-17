import puppeteer from "puppeteer";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 1100 });
await page.setRequestInterception(true);
page.on("request", (req) => {
  const url = req.url();
  if (/ipapi\.co|api\.country\.is|ipwho\.is|googletagmanager|google-analytics|doubleclick/i.test(url)) {
    req.abort();
    return;
  }
  req.continue();
});
await page.goto("https://www.dealality.com/old-home#about", {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.waitForFunction(
  () => document.getElementById("about")?.getAttribute("data-oh-problem-v2") === "1",
  { timeout: 20000 }
);
await new Promise((r) => setTimeout(r, 3500));

const diag = await page.evaluate(() => {
  const chips = [...document.querySelectorAll("#about-frag-scatter .about-frag-chip")];
  const scatter = document.getElementById("about-frag-scatter");
  const visual = document.getElementById("about-visual");
  return {
    chipCount: chips.length,
    chips: chips.map((c) => {
      const s = getComputedStyle(c);
      const r = c.getBoundingClientRect();
      return {
        text: c.textContent,
        opacity: s.opacity,
        display: s.display,
        top: s.top,
        left: s.left,
        width: r.width,
        height: r.height,
        y: r.y,
      };
    }),
    scatterH: scatter?.getBoundingClientRect().height,
    visualH: visual?.getBoundingClientRect().height,
    hard: document.getElementById("about-frag-hard")?.textContent,
    hardOpacity: document.getElementById("about-frag-hard")
      ? getComputedStyle(document.getElementById("about-frag-hard")).opacity
      : null,
  };
});
console.log(JSON.stringify(diag, null, 2));
await page.evaluate(() => document.getElementById("about-visual")?.scrollIntoView({ block: "center" }));
await new Promise((r) => setTimeout(r, 300));
await page.screenshot({ path: "public/marketing/qa-shots/problem-visual-live.png" });
await browser.close();
