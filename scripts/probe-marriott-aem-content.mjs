#!/usr/bin/env node
const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const MARSHA = "poplc";
const base = `https://www.marriott.com/content/marriott-hws/na/en-us/hotels/p/${SLUG}`;
const paths = [
  "/overview/jcr:content.json",
  "/overview.json",
  "/overview.infinity.json",
  "/overview/jcr:content.infinity.json",
  "/amenities/jcr:content.json",
  "/hotel-amenities/jcr:content.json",
  "/.model.json",
  "/overview/.model.json",
  "/overview/_jcr_content.json",
];

const H = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  Accept: "application/json,text/html",
};

for (const p of paths) {
  const url = base + p;
  const r = await fetch(url, { headers: H });
  const t = await r.text();
  const hit =
    r.status === 200 &&
    (/15-minute|Puerto Plata|Free high-speed|overview|amenit/i.test(t) || t.startsWith("{"));
  console.log(r.status, hit ? "HIT" : "miss", p);
  if (hit) console.log(" ", t.slice(0, 350).replace(/\s+/g, " "));
}

// roomCards service
const rc = await fetch(
  "https://www.marriott.com/services/marriott-hws/roomCards/?marsha=POPLC&locale=en-US&acrsEnabled=false",
  { headers: H }
);
console.log("\nroomCards", rc.status, (await rc.text()).slice(0, 200));

// Try overview content path with marsha only
for (const p of [
  `/content/marriott-hws/na/en-us/hotels/${MARSHA}/overview/jcr:content.json`,
  `/content/marriott-hws/na/en-us/hotels/${MARSHA}/overview.infinity.json`,
]) {
  const url = `https://www.marriott.com${p}`;
  const r = await fetch(url, { headers: H });
  const t = await r.text();
  console.log("\n", p, r.status, t.slice(0, 200).replace(/\s+/g, " "));
}
