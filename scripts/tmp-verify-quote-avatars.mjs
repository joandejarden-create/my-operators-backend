import puppeteer from "puppeteer";
import fs from "fs";

const out = "tmp-screenshots-tiles";
fs.mkdirSync(out, { recursive: true });
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
page.setDefaultNavigationTimeout(60000);
await page.goto(`https://www.dealality.com/old-home?v=quote-av-${Date.now()}`, {
  waitUntil: "domcontentloaded",
  timeout: 60000,
});
await page.waitForSelector("#trust, #testimonials", { timeout: 30000 }).catch(() => null);
await new Promise((r) => setTimeout(r, 4500));

const metrics = await page.evaluate(() => {
  const root =
    document.getElementById("testimonials") || document.getElementById("trust");
  if (!root) return { found: false };
  root.scrollIntoView({ block: "center" });
  const cssLink = document.querySelector('link[data-oh-quote-tiles="1"]');
  const scripts = [...document.querySelectorAll("script[src]")]
    .map((s) => s.src)
    .filter((u) => /quote-tiles|testimonials|freeform-head/i.test(u));
  const active =
    root.querySelector("[data-slide].is-active") ||
    root.querySelector("[data-slide]");
  const arts = active
    ? [...active.querySelectorAll("article")].map((a, i) => {
        const s = getComputedStyle(a);
        const r = a.getBoundingClientRect();
        const img = a.querySelector("img");
        const p = a.querySelector("p");
        const bq = a.querySelector("blockquote");
        const ir = img ? img.getBoundingClientRect() : null;
        const pr = p ? p.getBoundingClientRect() : null;
        const br = bq ? bq.getBoundingClientRect() : null;
        const is = img ? getComputedStyle(img) : null;
        return {
          i,
          display: s.display,
          h: Math.round(r.height),
          visible: s.display !== "none" && r.width > 0 && r.height > 0,
          img: {
            src: img?.currentSrc || img?.src || null,
            srcset: img?.getAttribute("srcset") || null,
            naturalW: img?.naturalWidth || 0,
            naturalH: img?.naturalHeight || 0,
            complete: !!img?.complete,
            display: is?.display || null,
            w: ir ? Math.round(ir.width) : null,
            h: ir ? Math.round(ir.height) : null,
            objectFit: is?.objectFit || null,
            borderRadius: is?.borderRadius || null,
            opacity: is?.opacity || null,
            loadedOk: !!(img && img.complete && img.naturalWidth > 0),
          },
          quote: {
            text: (bq?.textContent || "").trim().slice(0, 140),
            h: br ? Math.round(br.height) : null,
            clipped: bq ? bq.scrollHeight > bq.clientHeight + 2 : null,
          },
          attr: {
            text: (p?.textContent || "").trim().slice(0, 140),
            inside: pr ? pr.bottom <= r.bottom + 2 : null,
            gap: pr ? Math.round(r.bottom - pr.bottom) : null,
          },
        };
      })
    : [];
  return {
    found: true,
    css: !!cssLink,
    cssHref: cssLink?.href || null,
    scripts,
    visible: arts.filter((a) => a.visible).length,
    arts,
    slides: root.querySelectorAll("[data-slide]").length,
  };
});

await new Promise((r) => setTimeout(r, 400));
await page.screenshot({ path: `${out}/trust-avatars.png` });
console.log(JSON.stringify(metrics, null, 2));
await browser.close();
