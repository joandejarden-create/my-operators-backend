#!/usr/bin/env node
const url = process.argv[2] || "https://www.marriott.com/hotelinformation/MIDCY.mi";
const res = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
  },
  redirect: "follow",
});
console.log("status", res.status, "final", res.url);
const html = await res.text();
console.log("len", html.length);
console.log("access denied", /access denied/i.test(html));
const next = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
console.log("__NEXT_DATA__", Boolean(next));
if (next) {
  const d = JSON.parse(next[1]);
  console.log("pageProps keys", Object.keys(d?.props?.pageProps || {}));
}
for (const pat of ["shortDescription", "marshaCode", "MIDCY", "Courtyard", "hotelDescription"]) {
  console.log(pat, html.includes(pat));
}
