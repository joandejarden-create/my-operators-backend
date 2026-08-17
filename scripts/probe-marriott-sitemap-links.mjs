#!/usr/bin/env node
const url =
  process.argv[2] ||
  "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap";
const r = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html",
  },
});
const html = await r.text();
const next = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
const d = JSON.parse(next[1]);
const model = d?.props?.pageProps?.model;

/** @type {object[]} */
const links = [];
function walk(obj, depth = 0) {
  if (!obj || depth > 20) return;
  if (Array.isArray(obj)) return obj.forEach((x) => walk(x, depth + 1));
  if (typeof obj !== "object") return;
  const href = obj.href || obj.url || obj.link || obj.path;
  if (typeof href === "string" && /\/hotels\/[a-z0-9]+-/i.test(href)) {
    links.push({
      href,
      text: obj.text || obj.title || obj.label || obj.name || obj.linkText,
      keys: Object.keys(obj).slice(0, 15),
    });
  }
  for (const v of Object.values(obj)) walk(v, depth + 1);
}
walk(model);
console.log("hotel links found", links.length);
for (const l of links.slice(0, 5)) {
  console.log(JSON.stringify(l));
}
