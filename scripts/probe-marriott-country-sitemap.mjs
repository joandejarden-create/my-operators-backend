#!/usr/bin/env node
const url =
  process.argv[2] ||
  "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap";
const r = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml,*/*;q=0.8",
  },
});
const html = await r.text();
console.log("status", r.status, "len", html.length, "denied", /access denied/i.test(html));
const next = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
console.log("__NEXT_DATA__", Boolean(next));
if (next) {
  const d = JSON.parse(next[1]);
  const pp = d?.props?.pageProps || {};
  console.log("pageProps keys", Object.keys(pp));
  const str = JSON.stringify(pp);
  const hotelUrls =
    str.match(/https:\\\/\\\/www\.marriott\.com\\\/en-us\\\/hotels\\\/[a-z0-9-]+\\\/overview/gi) ||
    str.match(/\/en-us\/hotels\/[a-z0-9]+-[a-z0-9-]+\/overview/gi) ||
    [];
  console.log("hotel path matches", hotelUrls.length, hotelUrls.slice(0, 8));
}

const plainUrls = [...html.matchAll(/https:\/\/www\.marriott\.com\/en-us\/hotels\/[a-z0-9]+-[a-z0-9-]+\/overview/gi)].map(
  (m) => m[0]
);
console.log("plain hotel urls", [...new Set(plainUrls)].length);
console.log([...new Set(plainUrls)].slice(0, 10).join("\n"));
