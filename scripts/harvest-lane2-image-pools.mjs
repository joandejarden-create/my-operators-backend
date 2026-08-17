/**
 * Harvest Lane 2 soft-brand official image candidates into fixture pools.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");

function decode(s) {
  return String(s || "")
    .replace(/\\u002D/g, "-")
    .replace(/\\u002F/g, "/")
    .replace(/\\\//g, "/")
    .replace(/&amp;/g, "&")
    .replace(/\\x26/g, "&");
}

function cleanUrl(u) {
  return decode(u)
    .replace(/[),.;]+$/g, "")
    .trim();
}

function isLogoOrBrand(u) {
  return /logo|chiclet|primary_logo|brand-refresh-lp|open-graph-home|nav-logo|app-web/i.test(u);
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
    },
    redirect: "follow",
  });
  const html = await res.text();
  return { status: res.status, html: decode(html) };
}

function extract(html, patterns) {
  const out = [];
  for (const re of patterns) {
    for (const m of html.matchAll(re)) out.push(cleanUrl(m[1] || m[0]));
  }
  return [...new Set(out)].filter(Boolean);
}

async function harvestProperty(brand, property) {
  const pageUrl = property.galleryPageUrl || property.sourcePageUrl;
  const { status, html } = await fetchHtml(pageUrl);
  const images = [];

  if (brand === "autograph-collection") {
    images.push(
      ...extract(html, [
        /https:\/\/cache\.marriott\.com\/content\/dam\/marriott-renditions\/[A-Z0-9]+\/[^"'\\\s<>]+/gi,
        /https:\/\/cache\.marriott\.com\/is\/image\/marriotts7prod\/[^"'\\\s<>]+/gi,
      ])
    );
  } else if (brand === "handwritten-collection") {
    images.push(
      ...extract(html, [/https:\/\/www\.ahstatic\.com\/photos\/[^"'\\\s<>]+/gi])
    );
  } else if (brand === "vignette-collection") {
    images.push(
      ...extract(html, [/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/[^"'\\\s<>]+/gi]).filter(
        (u) => !isLogoOrBrand(u)
      )
    );
  } else if (brand === "tapestry-collection-by-hilton") {
    images.push(
      ...extract(html, [
        /https:\/\/www\d*\.hilton\.com\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi,
        /https:\/\/[^"'\\\s<>]*hilton[^"'\\\s<>]*\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi,
        /https:\/\/images\.hilton\.com\/[^"'\\\s<>]+/gi,
      ])
    );
  } else if (brand === "radisson-collection") {
    images.push(
      ...extract(html, [
        /https:\/\/[^"'\\\s<>]*radisson[^"'\\\s<>]*\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi,
        /https:\/\/media\.radissonhotels\.com\/[^"'\\\s<>]+/gi,
        /https:\/\/www\.choicehotels\.com\/hoteldam\/[^"'\\\s<>]+/gi,
      ])
    );
  }

  // Always capture og:image
  for (const re of [
    /property=["']og:image["'][^>]*content=["']([^"']+)["']/gi,
    /content=["']([^"']+)["'][^>]*property=["']og:image["']/gi,
  ]) {
    for (const m of html.matchAll(re)) {
      const u = cleanUrl(m[1]);
      if (u && !isLogoOrBrand(u)) images.unshift(u);
    }
  }

  const unique = [...new Set(images.map(cleanUrl))].filter((u) => u.startsWith("http"));
  return {
    ...property,
    brand,
    probeStatus: status,
    imageCandidates: unique.slice(0, 24),
  };
}

const CATALOGS = {
  "autograph-collection": [
    {
      propertyKey: "mspak-emery",
      propertyName: "Emery, Autograph Collection",
      marketCity: "Minneapolis",
      stateRegion: "USA",
      sourcePageUrl: "https://www.marriott.com/en-us/hotels/mspak-emery-autograph-collection/overview/",
      galleryPageUrl: "https://www.marriott.com/en-us/hotels/mspak-emery-autograph-collection/photos/",
      galleryPriority: 1,
    },
    {
      propertyKey: "chidx-emc2",
      propertyName: "Hotel EMC2, Autograph Collection",
      marketCity: "Chicago",
      stateRegion: "USA",
      sourcePageUrl: "https://www.marriott.com/en-us/hotels/chidx-hotel-emc2-autograph-collection/overview/",
      galleryPageUrl: "https://www.marriott.com/en-us/hotels/chidx-hotel-emc2-autograph-collection/photos/",
      galleryPriority: 1,
    },
    {
      propertyKey: "mciak-raphael",
      propertyName: "The Raphael Hotel, Autograph Collection",
      marketCity: "Kansas City",
      stateRegion: "USA",
      sourcePageUrl: "https://www.marriott.com/en-us/hotels/mciak-the-raphael-hotel-autograph-collection/overview/",
      galleryPageUrl: "https://www.marriott.com/en-us/hotels/mciak-the-raphael-hotel-autograph-collection/photos/",
      galleryPriority: 1,
    },
    {
      propertyKey: "mkedd-trade",
      propertyName: "The Trade, Autograph Collection",
      marketCity: "Milwaukee",
      stateRegion: "USA",
      sourcePageUrl: "https://www.marriott.com/en-us/hotels/mkedd-the-trade-autograph-collection/overview/",
      galleryPageUrl: "https://www.marriott.com/en-us/hotels/mkedd-the-trade-autograph-collection/photos/",
      galleryPriority: 1,
    },
    {
      propertyKey: "bossv-row",
      propertyName: "The Row Hotel at Assembly Row, Autograph Collection",
      marketCity: "Somerville",
      stateRegion: "USA",
      sourcePageUrl:
        "https://www.marriott.com/en-us/hotels/bossv-the-row-hotel-at-assembly-row-autograph-collection/overview/",
      galleryPageUrl:
        "https://www.marriott.com/en-us/hotels/bossv-the-row-hotel-at-assembly-row-autograph-collection/photos/",
      galleryPriority: 1,
    },
    {
      propertyKey: "lgaaw-envue",
      propertyName: "Envue, Autograph Collection",
      marketCity: "Weehawken",
      stateRegion: "USA",
      sourcePageUrl: "https://www.marriott.com/en-us/hotels/lgaaw-envue-autograph-collection/overview/",
      galleryPageUrl: "https://www.marriott.com/en-us/hotels/lgaaw-envue-autograph-collection/photos/",
      galleryPriority: 2,
    },
  ],
  "handwritten-collection": [
    {
      propertyKey: "c344-stratford",
      propertyName: "Hotel Stratford San Francisco - Handwritten Collection",
      marketCity: "San Francisco",
      stateRegion: "USA",
      sourcePageUrl: "https://all.accor.com/hotel/C344/index.en.shtml",
      galleryPriority: 1,
    },
  ],
  "vignette-collection": [
    {
      propertyKey: "nbofn-fairview",
      propertyName: "Fairview Hotel Nairobi, Vignette Collection",
      marketCity: "Nairobi",
      stateRegion: "Kenya",
      sourcePageUrl: "https://www.ihg.com/vignettecollection/hotels/us/en/nairobi/nbofn/hoteldetail",
      galleryPriority: 1,
    },
    {
      propertyKey: "lpldu-halyard",
      propertyName: "The Halyard Liverpool, Vignette Collection",
      marketCity: "Liverpool",
      stateRegion: "UK",
      sourcePageUrl: "https://www.ihg.com/vignettecollection/hotels/gb/en/liverpool/lpldu/hoteldetail",
      galleryPriority: 1,
    },
    {
      propertyKey: "liscs-convent",
      propertyName: "Convent Square Lisbon Hotel, Vignette Collection",
      marketCity: "Lisbon",
      stateRegion: "Portugal",
      sourcePageUrl: "https://www.ihg.com/vignettecollection/hotels/gb/en/lisbon/liscs/hoteldetail",
      galleryPriority: 1,
    },
  ],
  "tapestry-collection-by-hilton": [
    {
      propertyKey: "savvyup-cotton-sail",
      propertyName: "The Cotton Sail Hotel Savannah, Tapestry Collection by Hilton",
      marketCity: "Savannah",
      stateRegion: "USA",
      sourcePageUrl: "https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
      galleryPriority: 1,
    },
    {
      propertyKey: "ilmwrup-ballast",
      propertyName: "Hotel Ballast Wilmington, Tapestry Collection by Hilton",
      marketCity: "Wilmington",
      stateRegion: "USA",
      sourcePageUrl: "https://www.hilton.com/en/hotels/ilmwrup-hotel-ballast-wilmington/",
      galleryPriority: 1,
    },
    {
      propertyKey: "litemup-burgundy",
      propertyName: "The Burgundy Hotel, Tapestry Collection by Hilton",
      marketCity: "Little Rock",
      stateRegion: "USA",
      sourcePageUrl: "https://www.hilton.com/en/hotels/litemup-the-burgundy-hotel/",
      galleryPriority: 1,
    },
  ],
  "radisson-collection": [
    {
      propertyKey: "stockholm-strand",
      propertyName: "Radisson Collection Strand Hotel, Stockholm",
      marketCity: "Stockholm",
      stateRegion: "Sweden",
      sourcePageUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection-stockholm-strand",
      galleryPriority: 1,
    },
    {
      propertyKey: "santa-sofia-milan",
      propertyName: "Radisson Collection Santa Sofia, Milan",
      marketCity: "Milan",
      stateRegion: "Italy",
      sourcePageUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection-santa-sofia-milan",
      galleryPriority: 1,
    },
    {
      propertyKey: "edinburgh-royal-mile",
      propertyName: "Radisson Collection Hotel, Royal Mile Edinburgh",
      marketCity: "Edinburgh",
      stateRegion: "UK",
      sourcePageUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection-edinburgh-royal-mile",
      galleryPriority: 1,
    },
  ],
};

const report = { generatedAt: new Date().toISOString(), brands: {} };

for (const [brand, props] of Object.entries(CATALOGS)) {
  const harvested = [];
  for (const p of props) {
    process.stdout.write(`Harvest ${brand} ${p.propertyKey}... `);
    try {
      const row = await harvestProperty(brand, p);
      console.log(`status=${row.probeStatus} images=${row.imageCandidates.length}`);
      harvested.push(row);
    } catch (err) {
      console.log(`ERR ${err.message}`);
      harvested.push({ ...p, brand, probeStatus: 0, imageCandidates: [], error: err.message });
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  report.brands[brand] = harvested;

  // Build flat gallery pool fixture: prefer property-specific hotel photography
  const pool = [];
  for (const h of harvested) {
    for (const imageUrl of h.imageCandidates || []) {
      if (isLogoOrBrand(imageUrl)) continue;
      pool.push({
        propertyKey: h.propertyKey,
        propertyName: h.propertyName,
        marketCity: h.marketCity,
        sourcePageUrl: h.sourcePageUrl,
        imageUrl,
        label: "property",
      });
    }
  }
  const fixturePath = path.join(FIXTURES, `lane2-${brand}-gallery-pool.json`);
  fs.writeFileSync(fixturePath, `${JSON.stringify(pool, null, 2)}\n`);
  console.log(`Wrote ${fixturePath} (${pool.length} candidates)`);
}

fs.mkdirSync(path.join(ROOT, "reports"), { recursive: true });
fs.writeFileSync(
  path.join(ROOT, "reports", "brand-explorer-lane2-image-harvest.json"),
  `${JSON.stringify(report, null, 2)}\n`
);
console.log("Done harvest.");
