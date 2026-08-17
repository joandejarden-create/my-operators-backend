#!/usr/bin/env node
const url =
  process.argv[2] ||
  "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap";
const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" } });
const d = JSON.parse((await r.text()).match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);
const str = JSON.stringify(d?.props?.pageProps?.model || {});
for (const pat of [
  "description",
  "amenit",
  "phone",
  "address",
  "latitude",
  "checkIn",
  "overview",
  "shortDescription",
]) {
  const re = new RegExp(`"${pat}[^"]*"`, "gi");
  const m = str.match(re);
  console.log(pat, m ? [...new Set(m)].slice(0, 5) : "none");
}

// Try cache content API patterns for SDQJW
for (const u of [
  "https://cache.marriott.com/content/dam/marriott-digital/mi/global/en_us/hotel-sitemap/dominican-republic-hotel-sitemap.json",
  "https://www.marriott.com/mi/query/propertyDetails",
  "https://cache.marriott.com/Content/MarriottData/Property/sdqjw.json",
]) {
  const rr = await fetch(u, { headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" } });
  console.log("\n", u, rr.status, (await rr.text()).slice(0, 150).replace(/\s+/g, " "));
}
