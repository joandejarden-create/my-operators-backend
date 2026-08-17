#!/usr/bin/env node
const MARSHA = "POPLC";
const patterns = [
  `https://cache.marriott.com/content/dam/marriott-digital/lc/cala/hws/p/${MARSHA.toLowerCase()}/en_us/overview.json`,
  `https://cache.marriott.com/content/dam/marriott-digital/lc/cala/hws/p/${MARSHA.toLowerCase()}/en_us/hotel-overview.json`,
  `https://cache.marriott.com/content/dam/marriott-digital/lc/cala/hws/p/${MARSHA.toLowerCase()}/en_us/data/overview.json`,
  `https://www.marriott.com/content/dam/marriott-digital/lc/cala/hws/p/${MARSHA.toLowerCase()}/en_us/overview.json`,
  `https://www.marriott.com/hws/data/${MARSHA.toLowerCase()}/overview.en-us.json`,
  `https://www.marriott.com/hws/data/${MARSHA.toLowerCase()}/property.en-us.json`,
  `https://www.marriott.com/mi/hws/property/${MARSHA}`,
  `https://www.marriott.com/mi/hws/property/${MARSHA}/overview`,
  `https://www.marriott.com/mi/hws/property/${MARSHA}/amenities`,
  `https://www.marriott.com/mi/hws/content/${MARSHA}/overview.en-us.json`,
];

const H = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/experiences/",
};

for (const url of patterns) {
  const r = await fetch(url, { headers: H });
  const t = await r.text();
  const hit =
    /15-minute|Puerto Plata|Free high-speed|Mobility accessible|overview/i.test(t) ||
    (r.status === 200 && t.startsWith("{"));
  console.log(r.status, hit ? "HIT" : "miss", url.slice(0, 90));
  if (hit && r.status === 200) console.log(" ", t.slice(0, 300).replace(/\s+/g, " "));
}

// Try listing parent path via HEAD
const parent = `https://cache.marriott.com/content/dam/marriott-digital/lc/cala/hws/p/${MARSHA.toLowerCase()}/`;
const pr = await fetch(parent, { headers: H, method: "HEAD" });
console.log("\nparent HEAD", pr.status);
