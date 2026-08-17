#!/usr/bin/env node
/**
 * Recover full Marriott DAM URLs from Wayback HTML + known filename seeds,
 * then HEAD-verify and emit curated Wave 16A Stage 2B gallery pools.
 */
import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
  Accept: "text/html,image/*,*/*",
};

const PROPERTIES = [
  {
    brand: "fairfield-by-marriott",
    marsha: "CUNFI",
    name: "Fairfield Inn & Suites Cancun Airport",
    marketCity: "Cancún",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/cunfi-fairfield-inn-and-suites-cancun-airport/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "NYCMW",
    name: "Fairfield Inn & Suites New York Manhattan/Times Square South",
    marketCity: "New York",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycmw-fairfield-inn-and-suites-new-york-manhattan-times-square-south/overview/",
  },
  {
    brand: "fairfield-by-marriott",
    marsha: "NYCFT",
    name: "Fairfield Inn & Suites New York Manhattan/Central Park",
    marketCity: "New York",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycft-fairfield-inn-and-suites-new-york-manhattan-central-park/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "BOGFP",
    name: "Four Points by Sheraton Bogota",
    marketCity: "Bogotá",
    geographyLabel: "CALA",
    sourcePageUrl: "https://www.marriott.com/en-us/hotels/bogfp-four-points-bogota/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "MIAFA",
    name: "Four Points by Sheraton Miami Airport",
    marketCity: "Miami",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/miafa-four-points-miami-airport/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "CUNFP",
    name: "Four Points by Sheraton Cancun Centro",
    marketCity: "Cancún",
    geographyLabel: "CALA",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/cunfp-four-points-cancun-centro/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "LAXFP",
    name: "Four Points by Sheraton Los Angeles International Airport",
    marketCity: "Los Angeles",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/laxfp-four-points-los-angeles-international-airport/overview/",
  },
  {
    brand: "four-points-by-sheraton",
    marsha: "NYCFM",
    name: "Four Points by Sheraton Midtown New York",
    marketCity: "New York",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/nycfm-four-points-midtown-new-york/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YYZDA",
    name: "Delta Hotels Toronto Airport & Conference Centre",
    marketCity: "Toronto",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yyzda-delta-hotels-toronto-airport-and-conference-centre/overview/",
  },
  {
    brand: "delta-hotels-by-marriott",
    marsha: "YVRDV",
    name: "Delta Hotels Vancouver Downtown Suites",
    marketCity: "Vancouver",
    geographyLabel: "International Reference",
    sourcePageUrl:
      "https://www.marriott.com/en-us/hotels/yvrdv-delta-hotels-vancouver-downtown-suites/overview/",
  },
];

function roleFromFilename(fn) {
  const u = fn.toLowerCase();
  if (/exterior|front-desk|arrival|entrance|aerial/i.test(u)) return "exterior_arrival";
  if (/lobby|reception|lounge|seating/i.test(u)) return "public_space_lobby";
  if (/suite|junior/i.test(u)) return "guest_room_suite";
  if (/guest|king|queen|bedroom|bathroom|room/i.test(u)) return "guest_room";
  if (/restaurant|bar|breakfast|dining|brew|buffet|farm-table/i.test(u))
    return "food_beverage_experience";
  if (/pool|spa|wellness/i.test(u)) return "amenity_pool";
  if (/fitness|gym/i.test(u)) return "amenity_fitness";
  if (/meeting|ballroom|boardroom|conference/i.test(u)) return "meeting_space";
  if (/terrace|patio|balcony|setting/i.test(u)) return "property_setting";
  return "property_setting";
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function headOk(url) {
  try {
    const r = await fetch(url, { method: "HEAD", headers: HEADERS, redirect: "follow" });
    const ct = r.headers.get("content-type") || "";
    if (r.ok && /image/i.test(ct)) return true;
    if ([403, 405, 501].includes(r.status)) {
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

function extractDamFromHtml(html, marsha) {
  const s = String(html || "").replace(/\\\//g, "/").replace(/&amp;/g, "&");
  const re = new RegExp(
    `https?:\\/\\/cache\\.marriott\\.com\\/content\\/dam\\/marriott-renditions\\/${marsha}\\/[a-z0-9._-]+\\.(?:jpg|jpeg|png|webp)`,
    "gi"
  );
  const found = [...s.matchAll(re)].map((m) => m[0].split("?")[0]);
  // Also relative paths
  const re2 = new RegExp(
    `\\/content\\/dam\\/marriott-renditions\\/${marsha}\\/([a-z0-9._-]+\\.(?:jpg|jpeg|png|webp))`,
    "gi"
  );
  for (const m of s.matchAll(re2)) {
    found.push(
      `https://cache.marriott.com/content/dam/marriott-renditions/${marsha}/${m[1]}`
    );
  }
  return [...new Set(found)].filter((u) => /hor-wide|hor-clsc|sq\\.jpg/i.test(u));
}

async function waybackImages(p) {
  const cdx = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(p.sourcePageUrl)}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=6`;
  const r = await fetch(cdx, { headers: HEADERS });
  const text = await r.text();
  if (!text.trim().startsWith("[")) return [];
  const rows = JSON.parse(text).slice(1);
  const urls = new Set();
  for (const row of rows.slice(0, 4)) {
    const wb = `https://web.archive.org/web/${row[0]}id_/${row[1]}`;
    try {
      const hr = await fetch(wb, { headers: HEADERS, redirect: "follow" });
      if (!hr.ok) continue;
      const html = await hr.text();
      for (const u of extractDamFromHtml(html, p.marsha)) urls.add(u);
    } catch {
      /* ignore */
    }
  }
  return [...urls];
}

// Prefer hor-wide when both exist
function preferWide(urls) {
  const byStem = new Map();
  for (const u of urls) {
    const stem = u
      .replace(/-hor-wide|-hor-pano|-hor-clsc|-hor-feat|-sq/gi, "")
      .replace(/\.(jpg|jpeg|png|webp)$/i, "")
      .toLowerCase();
    const prev = byStem.get(stem);
    if (!prev) {
      byStem.set(stem, u);
      continue;
    }
    const score = (x) => (/hor-wide/i.test(x) ? 3 : /hor-clsc/i.test(x) ? 2 : /sq/i.test(x) ? 1 : 0);
    if (score(u) > score(prev)) byStem.set(stem, u);
  }
  return [...byStem.values()].filter((u) => /hor-wide|hor-clsc/i.test(u));
}

const brandSites = JSON.parse(
  fs.readFileSync("reports/_tmp-wave16a-stage2b-brand-site-harvest.json", "utf8")
);
const priorHits = JSON.parse(
  fs.readFileSync("reports/_tmp-wave16a-stage2b-rendition-hits-v2.json", "utf8")
).hits;

const seedByBrand = {
  "fairfield-by-marriott": (brandSites.fairfield?.scene7 || []).filter(
    (u) => /\/fi[-_]/i.test(u) && !/getty|pdt-/i.test(u)
  ),
  "four-points-by-sheraton": (brandSites.fp?.scene7 || []).filter(
    (u) => /\/fp[-_]/i.test(u) && !/fpx|xf-|getty|flex|bonvoy|pdt-/i.test(u)
  ),
  "delta-hotels-by-marriott": [],
};

const recovered = {};
for (const p of PROPERTIES) {
  console.log("wayback", p.marsha);
  const wb = await waybackImages(p);
  const prior = priorHits
    .filter((h) => h.marsha === p.marsha)
    .map((h) => h.imageUrl);
  const all = preferWide([...wb, ...prior]);
  const ok = [];
  for (const u of all) {
    if (await headOk(u)) {
      ok.push(u);
      console.log(" OK", u.split("/").pop());
    } else {
      console.log(" FAIL", u.split("/").pop());
    }
  }
  recovered[p.marsha] = { ...p, images: ok };
  console.log(p.marsha, "ok", ok.length);
}

// Build fixtures
const pools = {
  "fairfield-by-marriott": [],
  "four-points-by-sheraton": [],
  "delta-hotels-by-marriott": [],
};

for (const p of PROPERTIES) {
  const rec = recovered[p.marsha];
  for (const imageUrl of rec.images) {
    pools[p.brand].push({
      propertyKey: slugify(p.name),
      propertyName: p.name,
      marketCity: p.marketCity,
      geographyLabel: p.geographyLabel,
      sourcePageUrl: p.sourcePageUrl,
      imageUrl,
      label: "property",
      role: roleFromFilename(imageUrl),
      caption: `${p.name} — ${roleFromFilename(imageUrl).replace(/_/g, " ")}.`,
    });
  }
}

for (const [brand, urls] of Object.entries(seedByBrand)) {
  const brandName =
    brand === "fairfield-by-marriott"
      ? "Fairfield by Marriott"
      : brand === "four-points-by-sheraton"
        ? "Four Points by Sheraton"
        : "Delta Hotels by Marriott";
  const sourcePageUrl =
    brand === "fairfield-by-marriott"
      ? "https://fairfield.marriott.com/"
      : brand === "four-points-by-sheraton"
        ? "https://four-points.marriott.com/"
        : "https://delta-hotels.marriott.com/";
  for (const imageUrl of urls) {
    if (!(await headOk(imageUrl))) continue;
    pools[brand].push({
      propertyKey: `${slugify(brandName)}-brand-photography`,
      propertyName: `${brandName} (brand photography)`,
      marketCity: "",
      geographyLabel: "International Reference",
      sourcePageUrl,
      imageUrl,
      label: "brand_site",
      role: roleFromFilename(imageUrl),
      caption: `${brandName} — brand photography.`,
    });
  }
}

// Dedupe within brand by uniqueness stem
function stem(u) {
  return String(u || "")
    .split("?")[0]
    .replace(/:Wide-Hor|:Feature-Hor|:Square|:Classic-Hor|:Pano-Hor/gi, "")
    .replace(/-hor-wide|-hor-pano|-hor-clsc|-hor-feat|-sq/gi, "")
    .replace(/-\d{2,4}x\d{2,4}(?=\.(?:jpg|jpeg|png|webp))/i, "")
    .toLowerCase();
}

for (const brand of Object.keys(pools)) {
  const seen = new Set();
  const deduped = [];
  for (const row of pools[brand]) {
    const k = stem(row.imageUrl);
    if (!k || seen.has(k)) continue;
    // Flex contamination hard reject
    if (/flex|fpx-|\/xf-|four-points-flex|fourpointsexpress/i.test(row.imageUrl + row.propertyName + row.sourcePageUrl)) {
      continue;
    }
    seen.add(k);
    deduped.push(row);
  }
  pools[brand] = deduped;
  const path = `fixtures/wave16a-${brand}-gallery-pool.json`;
  fs.writeFileSync(path, JSON.stringify(deduped, null, 2) + "\n");
  const props = new Set(deduped.filter((r) => r.label === "property").map((r) => r.propertyKey));
  console.log(
    brand,
    "pool",
    deduped.length,
    "propertyKeys",
    props.size,
    "brand_site",
    deduped.filter((r) => r.label === "brand_site").length
  );
}

fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-recovered-pools.json",
  JSON.stringify({ recovered, poolCounts: Object.fromEntries(Object.entries(pools).map(([k, v]) => [k, v.length])) }, null, 2)
);
