#!/usr/bin/env node
const url =
  process.argv[2] ||
  "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap";
const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" } });
const html = await r.text();
const d = JSON.parse(html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);

/** @type {object|null} */
let sample = null;
function walk(obj) {
  if (!obj || sample) return;
  if (Array.isArray(obj)) return obj.forEach(walk);
  if (typeof obj !== "object") return;
  if (obj.marsha && obj.url && obj.title) {
    sample = obj;
    return;
  }
  for (const v of Object.values(obj)) walk(v);
}
walk(d?.props?.pageProps?.model);
console.log(JSON.stringify(sample, null, 2));

// Try overview with sitemap referer
const overview = sample?.url;
if (overview) {
  const r2 = await fetch(overview, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      Accept: "text/html",
      Referer: url,
    },
  });
  const h2 = await r2.text();
  console.log("\noverview", r2.status, h2.length, /access denied/i.test(h2));
  const meta = h2.match(/<meta name="description" content="([^"]*)"/i);
  console.log("meta desc", meta?.[1]?.slice(0, 120));
  const next2 = h2.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  console.log("overview __NEXT_DATA__", Boolean(next2));
}
