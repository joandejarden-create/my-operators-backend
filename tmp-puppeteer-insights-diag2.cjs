const puppeteer = require("puppeteer");

(async () => {
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });
  await page.goto("https://www.dealality.com/old-home", {
    waitUntil: "domcontentloaded",
    timeout: 60000,
  });
  await page.waitForSelector("#insights-grid");
  await page.evaluate(() => {
    document.getElementById("insights").scrollIntoView();
  });
  await new Promise((r) => setTimeout(r, 1000));

  const diag = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    const parents = [];
    let el = track;
    while (el && el !== document.body) {
      const cs = getComputedStyle(el);
      parents.push({
        id: el.id,
        tag: el.tagName,
        className: String(el.className).slice(0, 80),
        overflow: cs.overflow,
        overflowX: cs.overflowX,
        overflowY: cs.overflowY,
        transform: cs.transform,
        contain: cs.contain,
        touchAction: cs.touchAction,
        scrollSnapType: cs.scrollSnapType,
        width: cs.width,
        maxWidth: cs.maxWidth,
        minWidth: cs.minWidth,
        display: cs.display,
        flexDirection: cs.flexDirection,
      });
      el = el.parentElement;
    }

    // Attempt several scroll methods
    const attempts = [];
    track.style.setProperty("scroll-snap-type", "none", "important");
    track.style.setProperty("scroll-behavior", "auto", "important");

    track.scrollLeft = 500;
    attempts.push({ method: "scrollLeft=500", left: track.scrollLeft });

    track.scrollTo(500, 0);
    attempts.push({ method: "scrollTo(500,0)", left: track.scrollLeft });

    track.scrollBy(200, 0);
    attempts.push({ method: "scrollBy(200,0)", left: track.scrollLeft });

    track.style.overflowX = "scroll";
    track.scrollLeft = 500;
    attempts.push({ method: "overflow scroll + scrollLeft", left: track.scrollLeft });

    // Force width constraint
    track.style.maxWidth = "1320px";
    track.style.width = "1320px";
    track.scrollLeft = 500;
    attempts.push({
      method: "forced width + scrollLeft",
      left: track.scrollLeft,
      sw: track.scrollWidth,
      cw: track.clientWidth,
    });

    // Check if a parent is the real scroll container
    let p = track.parentElement;
    const parentScroll = [];
    while (p && p !== document.body) {
      const before = p.scrollLeft;
      p.scrollLeft = 200;
      parentScroll.push({
        id: p.id,
        before,
        after: p.scrollLeft,
        sw: p.scrollWidth,
        cw: p.clientWidth,
      });
      p.scrollLeft = before;
      p = p.parentElement;
    }

    // Listeners?
    return {
      attempts,
      parentScroll,
      parents,
      childCount: track.children.length,
      childTags: [...track.children].map((c) => ({
        id: c.id,
        tag: c.tagName,
        w: Math.round(c.getBoundingClientRect().width),
      })),
    };
  });

  console.log(JSON.stringify(diag, null, 2));
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
