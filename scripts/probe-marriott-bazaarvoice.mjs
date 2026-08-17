#!/usr/bin/env node
const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const exp = `https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`;

const html = await fetch(exp, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html",
  },
}).then((r) => r.text());

for (const pat of [
  /passkey=([a-zA-Z0-9]+)/,
  /productId["':\s]+([A-Z0-9_-]+)/i,
  /"productId"\s*:\s*"([^"]+)"/,
  /data-bv-product-id="([^"]+)"/,
  /POPLC/,
  /bazaarvoice[^"']*products[^"']*/gi,
]) {
  const m = html.match(pat);
  console.log(String(pat), m?.[0]?.slice?.(0, 120) || m?.[1] || "no");
}

// Try direct bazaarvoice with product id POPLC
const passkey = "canCX9lvC812oa4Y6HYf4gmWK5uszkZCKThrdtYkZqcYE";
for (const pid of ["POPLC", "poplc", "POPLC-the-ocean-club"]) {
  const url = `https://api.bazaarvoice.com/data/products.json?passkey=${passkey}&apiversion=5.5&filter=id:eq:${pid}`;
  const r = await fetch(url);
  const j = await r.json();
  console.log("\npid", pid, "results", j.Results?.length, j.Results?.[0]?.Description?.slice(0, 120));
}

// Search in html for product id near bazaarvoice
const bv = [...html.matchAll(/productId[^A-Za-z0-9_]+([A-Za-z0-9_-]{3,40})/g)].slice(0, 10);
console.log("\nproductId matches", bv.map((m) => m[1]));
