import puppeteer from "puppeteer";
import fs from "fs";

const out = "tmp-screenshots-tiles";
fs.mkdirSync(out, { recursive: true });

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});
try {
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });
  page.setDefaultTimeout(45000);
  await page.goto(`https://www.dealality.com/old-home?cb=${Date.now()}`, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });
  await page.waitForSelector("#testimonials-viewport article img", { timeout: 30000 });
  await new Promise((r) => setTimeout(r, 2500));

  const report = await page.evaluate(() => {
    const root =
      document.getElementById("testimonials") || document.getElementById("trust");
    root?.scrollIntoView({ block: "center" });
    const active =
      root?.querySelector("[data-slide].is-active") ||
      root?.querySelector("[data-slide]");
    const cards = [...(active?.querySelectorAll("article") || [])].map((a) => {
      const img = a.querySelector("img");
      const bq = a.querySelector("blockquote");
      const p = a.querySelector("p");
      const r = a.getBoundingClientRect();
      const pr = p?.getBoundingClientRect();
      return {
        display: getComputedStyle(a).display,
        loadedOk: !!(img && img.complete && img.naturalWidth > 0),
        naturalW: img?.naturalWidth || 0,
        src: (img?.currentSrc || img?.src || "").slice(-60),
        imgW: img ? Math.round(img.getBoundingClientRect().width) : 0,
        quoteLen: (bq?.textContent || "").trim().length,
        clipped: bq ? bq.scrollHeight > bq.clientHeight + 2 : null,
        attr: (p?.textContent || "").replace(/\s+/g, " ").trim(),
        attrInside: pr ? pr.bottom <= r.bottom + 2 : null,
      };
    });
    return {
      cssInjector: !!document.querySelector('link[data-oh-quote-tiles="1"]'),
      visible: cards.filter((c) => c.display !== "none").length,
      cards,
    };
  });

  await page.screenshot({ path: `${out}/trust-verify-final.png` });
  console.log(JSON.stringify(report, null, 2));
} finally {
  await browser.close();
}
