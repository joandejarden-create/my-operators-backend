#!/usr/bin/env node
const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const urls = [
  `https://www.marriott.com/en-us/hotels/${SLUG}/overview/`,
  `https://m.marriott.com/hotels/travel/poplc-the-ocean-club/`,
  `https://www.marriott.com/hotels/travel/poplc-the-ocean-club/`,
  `https://www.marriott.com/en-us/hotels/${SLUG}/`,
];

const agents = [
  ["chrome", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"],
  ["googlebot", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"],
  ["bingbot", "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"],
];

for (const url of urls) {
  console.log("\nURL", url);
  for (const [name, ua] of agents) {
    const r = await fetch(url, {
      headers: { "User-Agent": ua, Accept: "text/html", "Accept-Language": "en-US,en;q=0.9" },
      redirect: "follow",
    });
    const t = await r.text();
    console.log(
      ` ${name}`,
      r.status,
      t.length,
      "denied",
      /access denied/i.test(t),
      "overview",
      /15-minute drive/i.test(t),
      "amenities",
      /Free high-speed internet/i.test(t)
    );
  }
}

// Try rooms page for embedded AEM model
const rooms = await fetch(`https://www.marriott.com/en-us/hotels/${SLUG}/rooms/`, {
  headers: { "User-Agent": agents[0][1], Accept: "text/html" },
}).then((r) => r.text());

for (const pat of [
  /window\.__APOLLO_STATE__\s*=\s*/,
  /window\.digitalData\s*=\s*/,
  /"pageModel"\s*:\s*\{/,
  /"propertyOverview"\s*:/,
  /"hotelOverview"\s*:/,
  /"featuredAmenities"\s*:/,
  /"amenitiesList"\s*:/,
]) {
  const m = rooms.match(pat);
  console.log("\nrooms pattern", pat.source, Boolean(m), m?.index);
}
