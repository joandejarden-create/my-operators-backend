const puppeteer = require("puppeteer");

(async () => {
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });
  const errors = [];
  page.on("pageerror", (e) => errors.push(String(e.message || e)));
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push("console:" + msg.text());
  });
  await page.goto("https://www.dealality.com/old-home#insights", {
    waitUntil: "networkidle2",
    timeout: 90000,
  });
  await new Promise((r) => setTimeout(r, 2000));

  const before = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    const prev = document.getElementById("insights-prev");
    const next = document.getElementById("insights-next");
    const scripts = Array.from(document.scripts).map((s) => s.textContent || "").join("\n");
    return {
      cards: track ? track.children.length : 0,
      scrollLeft: track ? track.scrollLeft : null,
      scrollWidth: track ? track.scrollWidth : null,
      clientWidth: track ? track.clientWidth : null,
      prevHref: prev && prev.getAttribute("href"),
      nextHref: next && next.getAttribute("href"),
      bound: track && track.getAttribute("data-oh-ins-bound"),
      nextDisabled: next && next.getAttribute("aria-disabled"),
      hasFixedTransform: scripts.includes('translateY("+((c-ri)*h)+"px)")'),
      hasBrokenTransform: /translateY\("\+\(\(c-ri\)\*h\)\+"px\);/.test(scripts),
      hasDelegation: scripts.includes("#insights-prev,#insights-next"),
    };
  });

  await page.click("#insights-next");
  await new Promise((r) => setTimeout(r, 700));
  const after = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    return { scrollLeft: track ? track.scrollLeft : null };
  });

  await page.click("#insights-next");
  await new Promise((r) => setTimeout(r, 700));
  const after2 = await page.evaluate(() => {
    const track = document.getElementById("insights-grid");
    return { scrollLeft: track ? track.scrollLeft : null };
  });

  console.log(
    JSON.stringify(
      {
        before,
        after,
        after2,
        moved: after.scrollLeft > before.scrollLeft,
        movedAgain: after2.scrollLeft > after.scrollLeft,
        pageErrors: errors.filter((e) => /SyntaxError|Unexpected token/i.test(e)),
        errorCount: errors.length,
      },
      null,
      2
    )
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
