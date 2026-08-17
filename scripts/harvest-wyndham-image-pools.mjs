#!/usr/bin/env node
/**
 * Probe Wyndham property pages for official image CDN URLs (harvest helper).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function cleanUrl(u) {
  return String(u || "")
    .replace(/\\u002D/g, "-")
    .replace(/\\u002F/g, "/")
    .replace(/\\\//g, "/")
    .replace(/&amp;/g, "&")
    .replace(/[),.;]+$/g, "")
    .trim();
}

function isLogoOrJunk(u) {
  return /logo|favicon|sprite|icon|chiclet|nav-|app-web|open-graph-home|placeholder|pixel|1x1|blank/i.test(
    u
  );
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
  return { status: res.status, html: await res.text(), finalUrl: res.url };
}

function extractImages(html) {
  const urls = new Set();
  const patterns = [
    /https:\/\/[^"'\\\s<>]*wyndham[^"'\\\s<>]*\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi,
    /https:\/\/dynamic-media\.ace\.ndmsystems\.com\/[^"'\\\s<>]+/gi,
    /https:\/\/[^"'\\\s<>]*cloudinary[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi,
    /https:\/\/cdn\.worldota\.net\/[^"'\\\s<>]+/gi,
    /https:\/\/[^"'\\\s<>]*\/is\/image\/[^"'\\\s<>]+/gi,
    /https:\/\/images\.trvl-media\.com\/[^"'\\\s<>]+/gi,
    /https:\/\/[^"'\\\s<>]*akamai[^"'\\\s<>]*\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi,
    /https:\/\/[^"'\\\s<>]*scene7[^"'\\\s<>]+/gi,
    /property=["']og:image["'][^>]*content=["']([^"']+)["']/gi,
    /content=["']([^"']+)["'][^>]*property=["']og:image["']/gi,
  ];
  for (const re of patterns) {
    for (const m of html.matchAll(re)) {
      const u = cleanUrl(m[1] || m[0]);
      if (u.startsWith("http") && !isLogoOrJunk(u)) urls.add(u);
    }
  }
  for (const m of html.matchAll(/"(?:url|imageUrl|src|hiRes|loRes|large|medium)":"(https:[^"]+)"/gi)) {
    const u = cleanUrl(m[1]);
    if ((/\.(jpg|jpeg|png|webp)/i.test(u) || /\/is\/image\//i.test(u)) && !isLogoOrJunk(u)) {
      urls.add(u);
    }
  }
  // data-src / srcset fragments
  for (const m of html.matchAll(/(?:src|data-src|data-image)=["'](https:[^"']+)["']/gi)) {
    const u = cleanUrl(m[1]);
    if (/\.(jpg|jpeg|png|webp)/i.test(u) && !isLogoOrJunk(u)) urls.add(u);
  }
  return [...urls];
}

const PROPERTIES = [
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-palermo",
    propertyName: "Dazzler by Wyndham Buenos Aires Palermo",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-palermo-buenos-aires/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-palermo-buenos-aires/photos",
  },
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-recoleta",
    propertyName: "Dazzler by Wyndham Buenos Aires Recoleta",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-recoleta/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-recoleta/photos",
  },
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-san-martin",
    propertyName: "Dazzler by Wyndham Buenos Aires San Martin",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-san-martin/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-san-martin/photos",
  },
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-polo",
    propertyName: "Dazzler by Wyndham Buenos Aires Polo",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-polo-hotel-buenos-aires/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-polo-hotel-buenos-aires/photos",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "mb-hotel-miami",
    propertyName: "MB Hotel, Trademark Collection by Wyndham",
    marketCity: "Miami Beach",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/miami-beach-florida/mb-hotel-trademark-collection-by-wyndham/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/trademark/miami-beach-florida/mb-hotel-trademark-collection-by-wyndham/photos",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "chula-vista",
    propertyName: "Chula Vista Resort, Trademark Collection by Wyndham",
    marketCity: "Wisconsin Dells",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/wisconsin-dells-wisconsin/chula-vista-resort-trademark/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/trademark/wisconsin-dells-wisconsin/chula-vista-resort-trademark/photos",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "the-walden",
    propertyName: "The Walden, Trademark Collection by Wyndham",
    marketCity: "Pigeon Forge",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/pigeon-forge-tennessee/the-walden-trademark-collection/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/trademark/pigeon-forge-tennessee/the-walden-trademark-collection/photos",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "magic-village-views",
    propertyName: "Magic Village Views, Trademark Collection by Wyndham",
    marketCity: "Kissimmee",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/kissimmee-florida/magic-village-views-trademark-collection-by-wyndham/overview",
    galleryPageUrl:
      "https://www.wyndhamhotels.com/trademark/kissimmee-florida/magic-village-views-trademark-collection-by-wyndham/photos",
  },
];

async function harvestOne(property) {
  const pages = [property.sourcePageUrl, property.galleryPageUrl].filter(Boolean);
  const all = [];
  const probes = [];
  for (const pageUrl of pages) {
    try {
      const { status, html, finalUrl } = await fetchHtml(pageUrl);
      const images = extractImages(html);
      probes.push({ pageUrl, finalUrl, status, imageCount: images.length });
      all.push(...images);
    } catch (err) {
      probes.push({ pageUrl, error: err.message });
    }
  }
  const unique = [...new Set(all)];
  return { ...property, probes, imageCandidates: unique.slice(0, 40) };
}

async function main() {
  const results = [];
  for (const p of PROPERTIES) {
    console.log(`Harvesting ${p.propertyKey}…`);
    const row = await harvestOne(p);
    console.log(`  candidates=${row.imageCandidates.length}`);
    results.push(row);
  }
  const outDir = path.join(ROOT, "reports");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "brand-explorer-wyndham-image-harvest-probe.json");
  fs.writeFileSync(outPath, `${JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)}\n`);
  console.log(`Wrote ${outPath}`);

  // Summarize hosts
  const hosts = {};
  for (const r of results) {
    for (const u of r.imageCandidates) {
      try {
        const h = new URL(u).hostname;
        hosts[h] = (hosts[h] || 0) + 1;
      } catch {
        /* ignore */
      }
    }
  }
  console.log("Hosts:", hosts);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
