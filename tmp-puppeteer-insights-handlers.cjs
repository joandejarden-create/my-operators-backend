const puppeteer = require("puppeteer");

(async () => {
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox"],
  });
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e)));
  page.on("console", (m) => {
    if (m.type() === "error") errors.push("console:" + m.text());
  });

  await page.setViewport({ width: 1440, height: 900 });
  await page.goto("https://www.dealality.com/old-home", {
    waitUntil: "networkidle2",
    timeout: 90000,
  });
  await new Promise((r) => setTimeout(r, 2000));

  const info = await page.evaluate(() => {
    const prev = document.getElementById("insights-prev");
    const next = document.getElementById("insights-next");
    const track = document.getElementById("insights-grid");

    // Probe whether our click handler is present by dispatching a trusted-ish click path
    let prevented = false;
    const probe = new MouseEvent("click", { bubbles: true, cancelable: true });
    const orig = Event.prototype.preventDefault;
    let called = false;
    Event.prototype.preventDefault = function () {
      called = true;
      prevented = true;
      return orig.call(this);
    };
    next.dispatchEvent(probe);
    Event.prototype.preventDefault = orig;

    const leftBefore = track.scrollLeft;
    next.click();
    const leftAfterClick = track.scrollLeft;

    // Direct scroll still works?
    track.style.setProperty("scroll-snap-type", "none", "important");
    track.scrollLeft = 350;
    const leftDirect = track.scrollLeft;

    return {
      hrefPrev: prev.getAttribute("href"),
      hrefNext: next.getAttribute("href"),
      preventDefaultCalled: called,
      leftBefore,
      leftAfterClick,
      leftDirect,
      nextOuterHTML: next.outerHTML,
    };
  });

  console.log(JSON.stringify({ info, errors }, null, 2));
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
