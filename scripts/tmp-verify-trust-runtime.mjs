/**
 * Live verify: Joan first-person quote + one-pass autoplay (no infinite loop).
 */
const urls = [
  "https://www.dealality.com/old-home?cb=" + Date.now(),
  "https://dealality.com/old-home?cb=" + Date.now(),
];

const EXACT =
  "After nearly 30 years in hospitality, working across Europe, Latin America, the Caribbean, and the United States, I kept seeing the same problem: hotel owners had options, but no clear way to uncover and compare them. I built Dealality to give owners a confidential, structured process for understanding what an asset could become before committing to the brand, operator, partner, or strategy that will shape its future.";

const OLD_SNIPS = [
  "Joan Dejarden is the Founder",
  "working both sides",
  "she built Dealality",
];

async function checkHtml(url) {
  const res = await fetch(url, { headers: { "cache-control": "no-cache" } });
  const html = await res.text();
  const trustIdx = Math.max(html.indexOf('id="trust"'), html.indexOf('id="testimonials"'));
  const chunk = trustIdx >= 0 ? html.slice(trustIdx, trustIdx + 14000) : "";
  const quotes = [...chunk.matchAll(/<blockquote[^>]*>([\s\S]*?)<\/blockquote>/gi)].map((m) =>
    m[1].replace(/\s+/g, " ").trim()
  );
  const attrs = [...chunk.matchAll(/<p>([\s\S]*?)<\/p>/gi)].map((m) =>
    m[1].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim()
  );
  const slides = [...chunk.matchAll(/data-slide="(\d+)"/g)].map((m) => m[1]);
  const joanQuote = quotes.find((q) => /nearly 30 years|I built Dealality|Joan Dejarden is the Founder/i.test(q));
  const joanNorm = (joanQuote || "").replace(/^[\s\u201C\u201D"]+|[\s\u201C\u201D"]+$/g, "");
  return {
    url,
    status: res.status,
    slides,
    quoteCount: quotes.length,
    quotes,
    attrs,
    joanExact: joanNorm === EXACT,
    joanNorm,
    hasOldBio: OLD_SNIPS.some((s) => chunk.includes(s) || quotes.some((q) => q.includes(s))),
    hasOhTt: html.includes('id="oh-tt"'),
    hasBoot30b: html.includes("boot-guard.v20260730b"),
    hasBoot30c: html.includes("boot-guard.v20260730c"),
    hasTt30aInHtml: html.includes("testimonials.v20260730a"),
    hasQuoteTiles: html.includes("quote-tiles"),
  };
}

const cdn =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a8739c83f9c69c9343dfe_dealality-old-home-testimonials.v20260730a.js";
const js = await (await fetch(cdn)).text();

const results = [];
for (const url of urls) results.push(await checkHtml(url));

console.log(
  JSON.stringify(
    {
      results,
      cdn: {
        ok: js.includes("startAutoplayOnce") && js.includes("allSlidesSeen") && js.includes("autoplayDone"),
        loopsForever: /setInterval[\s\S]*setSlide\(index\s*\+\s*1\)/.test(js) && !js.includes("allSlidesSeen"),
        len: js.length,
      },
    },
    null,
    2
  )
);
