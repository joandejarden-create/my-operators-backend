const puppeteer = require("puppeteer");

(async () => {
  const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });
  await page.goto("https://www.dealality.com/old-home#insights", {
    waitUntil: "networkidle2",
    timeout: 90000,
  });
  await new Promise((r) => setTimeout(r, 2000));

  const metrics = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    const cards = [...track.querySelectorAll("#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6")];
    const trackRect = track.getBoundingClientRect();
    const cardRects = cards.map((c) => {
      const r = c.getBoundingClientRect();
      return {
        id: c.id,
        width: Math.round(r.width),
        left: Math.round(r.left),
        right: Math.round(r.right),
        fullyVisible: r.left >= trackRect.left - 1 && r.right <= trackRect.right + 1,
        partiallyVisible: r.right > trackRect.left + 2 && r.left < trackRect.right - 2,
      };
    });
    const visibleFull = cardRects.filter((c) => c.fullyVisible).length;
    const visiblePartial = cardRects.filter((c) => c.partiallyVisible).length;
    return {
      trackWidth: Math.round(trackRect.width),
      scrollWidth: track.scrollWidth,
      clientWidth: track.clientWidth,
      cardWidth: cardRects[0] && cardRects[0].width,
      visibleFull,
      visiblePartial,
      cardRects: cardRects.slice(0, 4),
      hasCalcInPage: document.documentElement.outerHTML.includes("--ins-card-w"),
    };
  });

  const before = await page.evaluate(() => document.getElementById("insights-grid").scrollLeft);
  await page.click("#insights-next");
  await new Promise((r) => setTimeout(r, 700));
  const after = await page.evaluate(() => document.getElementById("insights-grid").scrollLeft);

  console.log(JSON.stringify({ metrics, before, after, stillScrolls: after > before }, null, 2));
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
