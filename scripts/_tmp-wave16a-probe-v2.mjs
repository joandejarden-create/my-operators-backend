#!/usr/bin/env node
/**
 * Wave 16A Stage 2B — corrected MARSHA DAM probe + Wayback snapshot harvest.
 */
import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
  Accept: "image/*,text/html,*/*",
};

const PROPERTIES = [
  // Fairfield — corrected codes
  {
    brand: "fairfield-by-marriott",
    marsha: "CUNFI",
    name: "Fairfield Inn & Suites Cancun Airport",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/cunfi-fairfield-inn-and-suites-cancun-airport/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "NYCMW",
    name: "Fairfield Inn & Suites New York Manhattan/Times Square South",
    market: "New York, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycmw-fairfield-inn-and-suites-new-york-manhattan-times-square-south/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "NYCFT",
    name: "Fairfield Inn & Suites New York Manhattan/Central Park",
    market: "New York, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycft-fairfield-inn-and-suites-new-york-manhattan-central-park/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "MIAFO",
    name: "Fairfield Inn & Suites Miami Airport West/Doral",
    market: "Miami, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/miafo-fairfield-inn-and-suites-miami-airport-west-doral/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "MEXFO",
    name: "Fairfield by Marriott Mexico City Vallejo",
    market: "Mexico City, Mexico",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/mexfo-fairfield-mexico-city-vallejo/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "BOGFO",
    name: "Fairfield by Marriott Bogota Airport",
    market: "Bogotá, Colombia",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/bogfo-fairfield-bogota-airport/overview/",
  },
  // Four Points
  {
    brand: "four-points-by-sheraton",
    marsha: "BOGFP",
    name: "Four Points by Sheraton Bogota",
    market: "Bogotá, Colombia",
    geographyLabel: "CALA",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/bogfp-four-points-bogota/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "MIAFA",
    name: "Four Points by Sheraton Miami Airport",
    market: "Miami, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/miafa-four-points-miami-airport/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "SDQFP",
    name: "Four Points by Sheraton Santo Domingo",
    market: "Santo Domingo, Dominican Republic",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/sdqfp-four-points-santo-domingo/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "CUNFP",
    name: "Four Points by Sheraton Cancun Centro",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/cunfp-four-points-cancun-centro/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "LAXFP",
    name: "Four Points by Sheraton Los Angeles International Airport",
    market: "Los Angeles, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/laxfp-four-points-los-angeles-international-airport/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "NYCFM",
    name: "Four Points by Sheraton Midtown New York",
    market: "New York, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycfm-four-points-midtown-new-york/overview/",
  },
  // Delta — corrected Vancouver code
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YYZDA",
    name: "Delta Hotels Toronto Airport & Conference Centre",
    market: "Toronto, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yyzda-delta-hotels-toronto-airport-and-conference-centre/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YVRDV",
    name: "Delta Hotels Vancouver Downtown Suites",
    market: "Vancouver, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yvrdv-delta-hotels-vancouver-downtown-suites/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YYCDL",
    name: "Delta Hotels Calgary Downtown",
    market: "Calgary, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yycdl-delta-hotels-calgary-downtown/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "CUNDL",
    name: "Delta Hotels Cancun Inn",
    market: "Cancún, Mexico",
    geographyLabel: "CALA",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/cundl-delta-hotels-cancun-inn/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "MEXDL",
    name: "Delta Hotels Mexico City Metropolitan",
    market: "Mexico City, Mexico",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/mexdl-delta-hotels-mexico-city-metropolitan/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YULDL",
    name: "Delta Hotels Montreal",
    market: "Montreal, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/yuldl-delta-hotels-montreal/overview/",
  },
];

const DESCS = [
  "exterior",
  "lobby",
  "guestroom",
  "suite",
  "pool",
  "fitness",
  "restaurant",
  "bar",
  "breakfast",
  "meeting",
  "ballroom",
  "dining",
  "aerial",
  "reception",
  "lounge",
  "spa",
  "bathroom",
  "king-guestroom",
  "queen-guestroom",
  "meeting-room",
  "boardroom",
  "pool-deck",
  "terrace",
];

const ROLE_BY_DESC = {
  exterior: "exterior_arrival",
  aerial: "property_setting",
  lobby: "public_space_lobby",
  reception: "public_space_lobby",
  lounge: "public_space_lobby",
  guestroom: "guest_room",
  "king-guestroom": "guest_room",
  "queen-guestroom": "guest_room",
  suite: "guest_room_suite",
  bathroom: "guest_room",
  pool: "amenity_pool",
  "pool-deck": "amenity_pool",
  fitness: "amenity_fitness",
  spa: "wellness_pool_spa",
  restaurant: "food_beverage_experience",
  bar: "food_beverage_experience",
  breakfast: "food_beverage_experience",
  dining: "food_beverage_experience",
  meeting: "meeting_space",
  ballroom: "meeting_space",
  "meeting-room": "meeting_space",
  boardroom: "meeting_space",
  terrace: "property_setting",
};

function padId(id) {
  if (id < 100) return String(id).padStart(4, "0");
  return String(id);
}

const IDS = [];
for (let i = 1; i <= 40; i++) IDS.push(i);
for (const x of [47, 50, 86, 96, 101, 259, 1000, 2000, 3000, 3986, 4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008, 4009, 4010, 5000, 6364, 7590]) {
  if (!IDS.includes(x)) IDS.push(x);
}

async function headOk(url) {
  try {
    const r = await fetch(url, { method: "HEAD", headers: HEADERS, redirect: "follow" });
    const ct = r.headers.get("content-type") || "";
    if (r.ok && /image/i.test(ct)) return true;
    if ([403, 405, 501].includes(r.status)) {
      const g = await fetch(url, {
        method: "GET",
        headers: { ...HEADERS, Range: "bytes=0-512" },
        redirect: "follow",
      });
      const gct = g.headers.get("content-type") || "";
      return (g.ok || g.status === 206) && /image/i.test(gct);
    }
    return false;
  } catch {
    return false;
  }
}

async function mapPool(items, concurrency, fn) {
  const out = [];
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx]);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return out;
}

function extractImages(html) {
  const s = String(html || "");
  const urls = [
    ...s.matchAll(/https?:\/\/cache\.marriott\.com\/(?:content\/dam\/marriott-renditions|is\/image\/marriotts7prod)\/[^\"'\\s>]+/gi),
  ].map((m) => m[0].replace(/\\/g, "").split("?")[0]);
  return [...new Set(urls)].filter((u) => !/getty|logo|icon|pixel/i.test(u));
}

async function fetchWayback(url) {
  const cdx = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(url)}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=8`;
  try {
    const r = await fetch(cdx, { headers: HEADERS });
    const text = await r.text();
    if (!text.trim().startsWith("[")) return { snapshots: [], images: [] };
    const rows = JSON.parse(text);
    const snaps = rows.slice(1).map((row) => ({ ts: row[0], original: row[1] }));
    const images = [];
    for (const snap of snaps.slice(0, 3)) {
      const wb = `https://web.archive.org/web/${snap.ts}id_/${snap.original}`;
      try {
        const hr = await fetch(wb, { headers: HEADERS, redirect: "follow" });
        if (!hr.ok) continue;
        const html = await hr.text();
        images.push(...extractImages(html));
      } catch {
        /* ignore */
      }
    }
    return { snapshots: snaps, images: [...new Set(images)] };
  } catch (e) {
    return { snapshots: [], images: [], error: e.message };
  }
}

console.log("Building DAM candidates...");
const candidates = [];
for (const p of PROPERTIES) {
  const code = p.marsha.toLowerCase();
  for (const desc of DESCS) {
    for (const id of IDS) {
      const filename = `${code}-${desc}-${padId(id)}-hor-wide.jpg`;
      const url = `https://cache.marriott.com/content/dam/marriott-renditions/${p.marsha}/${filename}`;
      candidates.push({
        ...p,
        propertyKey: code,
        propertyName: p.name,
        role: ROLE_BY_DESC[desc] || "property_setting",
        imageUrl: url,
        filename,
      });
    }
  }
}
console.log("candidates", candidates.length);

const results = await mapPool(candidates, 50, async (c) => ((await headOk(c.imageUrl)) ? c : null));
const hits = results.filter(Boolean);
const byMarsha = {};
for (const h of hits) {
  byMarsha[h.marsha] = (byMarsha[h.marsha] || 0) + 1;
  console.log("HIT", h.marsha, h.role, h.filename);
}
console.log("byMarsha", byMarsha, "TOTAL", hits.length);

console.log("\nWayback harvest...");
const wayback = {};
for (const p of PROPERTIES) {
  const res = await fetchWayback(p.sourcePageUrl);
  wayback[p.marsha] = {
    name: p.name,
    snapCount: res.snapshots.length,
    imageCount: res.images.length,
    images: res.images.slice(0, 40),
    error: res.error || null,
  };
  console.log(p.marsha, "snaps", res.snapshots.length, "imgs", res.images.length);
  for (const u of res.images.slice(0, 5)) console.log(" ", u.slice(0, 140));
}

fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-rendition-hits-v2.json",
  JSON.stringify({ hits, byMarsha, wayback }, null, 2)
);
console.log("wrote reports/_tmp-wave16a-stage2b-rendition-hits-v2.json");
