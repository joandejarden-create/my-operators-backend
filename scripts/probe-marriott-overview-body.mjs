#!/usr/bin/env node
const sitemap =
  "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap";
const overview =
  "https://www.marriott.com/en-us/hotels/sdqjw-jw-marriott-hotel-santo-domingo/overview/";
const r = await fetch(overview, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html",
    Referer: sitemap,
  },
  redirect: "follow",
});
const html = await r.text();
console.log("status", r.status, "final", r.url, "len", html.length);
console.log(html);
