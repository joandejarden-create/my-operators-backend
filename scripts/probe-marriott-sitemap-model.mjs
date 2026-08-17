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
console.log("model keys", model ? Object.keys(model) : null);

function findHotels(obj, depth = 0, out = []) {
  if (!obj || depth > 12) return out;
  if (Array.isArray(obj)) {
    for (const item of obj) findHotels(item, depth + 1, out);
    return out;
  }
  if (typeof obj !== "object") return out;
  const keys = Object.keys(obj);
  if (
    (obj.marshaCode || obj.propertyCode || obj.hotelName) &&
    (obj.url || obj.hotelUrl || obj.overviewUrl || obj.path)
  ) {
    out.push(obj);
  }
  for (const k of keys) findHotels(obj[k], depth + 1, out);
  return out;
}

const hotels = findHotels(model);
console.log("hotel-like objects", hotels.length);
if (hotels[0]) console.log("sample", JSON.stringify(hotels[0], null, 2).slice(0, 1500));

// Also search interceptorResponse / apollo cache
const ir = d?.props?.pageProps?.interceptorResponse;
if (ir) {
  const irStr = JSON.stringify(ir);
  console.log("\ninterceptorResponse len", irStr.length);
  console.log("has marshaCode", irStr.includes("marshaCode"));
  console.log("has description", /description/i.test(irStr));
}
