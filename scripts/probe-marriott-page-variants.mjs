#!/usr/bin/env node
const MARSHA = "POPLC";
const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const urls = [
  `https://www.marriott.com/hotels/travel/${MARSHA.toLowerCase()}-the-ocean-club/`,
  `https://www.marriott.com/en-us/hotels/${SLUG}/overview/`,
  `https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`,
  `https://www.marriott.com/en-us/hotels/${SLUG}/hotel-amenities/`,
  `https://www.marriott.com/en-us/hotels/${SLUG}/amenities/`,
  `https://www.marriott.com/hotels/hotelinformation/${MARSHA.toLowerCase()}`,
  `https://www.marriott.com/hotel-search/hotelInformation/${MARSHA}.json`,
];

const H = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/json,*/*",
  "Accept-Language": "en-US,en;q=0.9",
};

for (const url of urls) {
  const r = await fetch(url, { headers: H, redirect: "follow" });
  const t = await r.text();
  const next = t.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  const hasOverview = /15-minute drive from Puerto Plata/i.test(t);
  const hasAmenity = /Free high-speed internet/i.test(t) || /Fitness center/i.test(t);
  console.log("\n", url);
  console.log(" ", r.status, t.length, "next", !!next, "overviewText", hasOverview, "amenityText", hasAmenity);
  if (next) {
    const d = JSON.parse(next[1]);
    console.log("  pageProps keys", Object.keys(d?.props?.pageProps || {}).slice(0, 8));
  }
}
