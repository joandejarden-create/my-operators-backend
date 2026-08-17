const puppeteer = require("puppeteer");

(async () => {
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });
  await page.goto("https://www.dealality.com/old-home#insights", {
    waitUntil: "networkidle2",
    timeout: 60000,
  });
  await new Promise((r) => setTimeout(r, 1500));

  const before = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    const prev = document.getElementById("insights-prev");
    const next = document.getElementById("insights-next");
    const cards = track ? [...track.querySelectorAll("article, [id^=ins-]")] : [];
    const cs = track ? getComputedStyle(track) : null;
    const cardCs = cards[0] ? getComputedStyle(cards[0]) : null;
    return {
      cardCount: cards.length,
      scrollLeft: track && track.scrollLeft,
      scrollWidth: track && track.scrollWidth,
      clientWidth: track && track.clientWidth,
      max: track && Math.max(0, track.scrollWidth - track.clientWidth),
      overflowX: cs && cs.overflowX,
      display: cs && cs.display,
      flexWrap: cs && cs.flexWrap,
      minWidth: cs && cs.minWidth,
      width: cs && cs.width,
      cardFlex: cardCs && cardCs.flex,
      cardWidth: cardCs && cardCs.width,
      cardMinWidth: cardCs && cardCs.minWidth,
      cardFlexShrink: cardCs && cardCs.flexShrink,
      prevDisabled: prev && prev.getAttribute("aria-disabled"),
      nextDisabled: next && next.getAttribute("aria-disabled"),
      prevPointer: prev && getComputedStyle(prev).pointerEvents,
      nextPointer: next && getComputedStyle(next).pointerEvents,
      prevHref: prev && prev.getAttribute("href"),
      nextHref: next && next.getAttribute("href"),
      cardWidths: cards.map((c) => Math.round(c.getBoundingClientRect().width)),
    };
  });
  console.log("BEFORE", JSON.stringify(before, null, 2));

  // Click next
  await page.click("#insights-next");
  await new Promise((r) => setTimeout(r, 800));

  const after = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    return {
      scrollLeft: track.scrollLeft,
      scrollWidth: track.scrollWidth,
      clientWidth: track.clientWidth,
    };
  });
  console.log("AFTER NEXT CLICK", after);

  // Try programmatic scroll
  const prog = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    track.scrollLeft = 400;
    return { scrollLeft: track.scrollLeft, max: track.scrollWidth - track.clientWidth };
  });
  console.log("PROG SCROLL", prog);

  // Check covering elements
  const hit = await page.evaluate(() => {
    const next = document.getElementById("insights-next");
    const r = next.getBoundingClientRect();
    const el = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
    return {
      target: el && el.id,
      tag: el && el.tagName,
      className: el && el.className,
      nextRect: { x: r.x, y: r.y, w: r.width, h: r.height },
    };
  });
  console.log("HIT TEST NEXT", hit);

  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
