#!/usr/bin/env node
/**
 * Fast parallel Marriott DAM rendition probe for Wave 16A Stage 2B.
 * Only probes -hor-wide.jpg (canonical uniqueness base).
 */
import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
  Accept: "image/*,*/*",
};

const PROPERTIES = [
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
    marsha: "NYCTS",
    name: "Fairfield Inn & Suites New York Manhattan/Times Square South",
    market: "New York, USA",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycts-fairfield-inn-and-suites-new-york-manhattan-times-square-south/overview/",
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
    marsha: "YVRDL",
    name: "Delta Hotels Vancouver Downtown",
    market: "Vancouver, Canada",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yvrdl-delta-hotels-vancouver-downtown/overview/",
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
];

const DESCS = [
  ["exterior", "exterior_arrival"],
  ["lobby", "public_space_lobby"],
  ["guestroom", "guest_room"],
  ["suite", "guest_room_suite"],
  ["pool", "amenity_pool"],
  ["fitness", "amenity_fitness"],
  ["restaurant", "food_beverage_experience"],
  ["bar", "food_beverage_experience"],
  ["breakfast", "food_beverage_experience"],
  ["meeting", "meeting_space"],
  ["ballroom", "meeting_space"],
  ["dining", "food_beverage_experience"],
  ["aerial", "property_setting"],
];

// Marriott DAM IDs often cluster 0001–0050 or 4000–4050; sample both bands + known hits.
const IDS = [
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 30, 40, 47, 50,
  1000, 1001, 2000, 2001, 3000, 3001,
  4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008, 4009, 4010, 4011, 4012,
  5000, 5001, 7590, 7592,
];

function padId(id) {
  if (id < 100) return String(id).padStart(4, "0");
  return String(id);
}

async function headOk(url) {
  try {
    const r = await fetch(url, { method: "HEAD", headers: HEADERS, redirect: "follow" });
    const ct = r.headers.get("content-type") || "";
    if (r.ok && /image/i.test(ct)) return true;
    // Some CDNs reject HEAD — try Range GET
    if (r.status === 403 || r.status === 405 || r.status === 501) {
      const g = await fetch(url, {
        method: "GET",
        headers: { ...HEADERS, Range: "bytes=0-1023" },
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
      out[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return out;
}

const candidates = [];
for (const p of PROPERTIES) {
  const code = p.marsha.toLowerCase();
  for (const [desc, role] of DESCS) {
    for (const id of IDS) {
      const filename = `${code}-${desc}-${padId(id)}-hor-wide.jpg`;
      const url = `https://cache.marriott.com/content/dam/marriott-renditions/${p.marsha}/${filename}`;
      candidates.push({ ...p, propertyKey: code, propertyName: p.name, role, imageUrl: url, filename });
    }
  }
}

console.log("candidates", candidates.length);
const results = await mapPool(candidates, 40, async (c) => {
  const ok = await headOk(c.imageUrl);
  return ok ? c : null;
});

const hits = results.filter(Boolean);
const byMarsha = {};
for (const h of hits) {
  byMarsha[h.marsha] = (byMarsha[h.marsha] || 0) + 1;
  console.log("HIT", h.marsha, h.role, h.filename);
}
console.log("byMarsha", byMarsha);
console.log("TOTAL", hits.length);

fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-rendition-hits.json",
  JSON.stringify(hits, null, 2)
);
