/**
 * Runtime verify: Joan quote exact + one-pass autoplay then stop (no infinite loop).
 */
import puppeteer from "puppeteer";

const url = "https://www.dealality.com/old-home?cb=" + Date.now() + "#trust";
const EXACT =
  "After nearly 30 years in hospitality, working across Europe, Latin America, the Caribbean, and the United States, I kept seeing the same problem: hotel owners had options, but no clear way to uncover and compare them. I built Dealality to give owners a confidential, structured process for understanding what an asset could become before committing to the brand, operator, partner, or strategy that will shape its future.";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function activeSlide(page) {
  return page.evaluate(() => {
    const root = document.getElementById("testimonials") || document.getElementById("trust");
    const slides = [...root.querySelectorAll("#testimonials-viewport > div[data-slide]")];
    return slides.findIndex((s) => s.classList.contains("is-active"));
  });
}

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });

await page.waitForFunction(
  () => document.querySelectorAll("#testimonials-dots button").length >= 2,
  { timeout: 30000 }
);

const initial = await page.evaluate((exact) => {
  const root = document.getElementById("testimonials") || document.getElementById("trust");
  const quotes = [...root.querySelectorAll("blockquote")].map((b) =>
    (b.textContent || "").replace(/\s+/g, " ").trim()
  );
  const joan = quotes.find((q) => /nearly 30 years|Joan Dejarden is the Founder/i.test(q));
  const joanNorm = (joan || "").replace(/^[\s\u201C\u201D"]+|[\s\u201C\u201D"]+$/g, "");
  const scripts = [...document.scripts].map((s) => s.src).filter((s) => /testimonial|boot-guard/i.test(s));
  const art = root.querySelectorAll("#testimonials-viewport > div[data-slide].is-active article");
  let twoUp = false;
  if (art.length >= 2) {
    const r0 = art[0].getBoundingClientRect();
    const r1 = art[1].getBoundingClientRect();
    twoUp = r0.width > 40 && r1.width > 40 && Math.abs(r0.top - r1.top) < 120;
  }
  return {
    slideCount: root.querySelectorAll("#testimonials-viewport > div[data-slide]").length,
    active: [...root.querySelectorAll("#testimonials-viewport > div[data-slide]")].findIndex((s) =>
      s.classList.contains("is-active")
    ),
    joanExact: joanNorm === exact,
    hasOld: quotes.some((q) => q.includes("working both sides") || q.includes("Joan Dejarden is the Founder")),
    scripts,
    twoUp,
    attrs: [...root.querySelectorAll("article p")].map((p) => (p.textContent || "").replace(/\s+/g, " ").trim()),
  };
}, EXACT);

const t0 = await activeSlide(page);
await sleep(7500);
const t1 = await activeSlide(page);
await sleep(7500);
const t2 = await activeSlide(page);
await sleep(7500);
const t3 = await activeSlide(page);

console.log(
  JSON.stringify(
    {
      url,
      initial,
      timeline: { t0, after7_5s: t1, after15s: t2, after22_5s: t3 },
      verdict: {
        bioOk: initial.joanExact && !initial.hasOld,
        twoUpOk: initial.twoUp,
        scriptLoaded: initial.scripts.some((s) => /testimonials\.v20260730a/.test(s)),
        bootLoaded: initial.scripts.some((s) => /boot-guard\.v20260730b/.test(s)),
        // Expect: start 0 -> after ~7s move to 1 -> stay on 1 (no loop back to 0)
        oneAdvanceThenManualOnly: t0 === 0 && t1 === 1 && t2 === 1 && t3 === 1,
        loopsForever: t0 === 0 && t1 === 1 && t2 === 0,
      },
    },
    null,
    2
  )
);

await browser.close();
