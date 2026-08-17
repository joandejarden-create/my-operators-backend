#!/usr/bin/env node
/**
 * Expand Wyndham DAM property-image pools by probing common filename patterns
 * from the property ID discovered via og:image / overview page.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const SUFFIXES = [
  "exterior_view_1",
  "exterior_view_2",
  "exterior_view_3",
  "exterior_view_4",
  "lobby_view_1",
  "lobby_view_2",
  "lobby_view_3",
  "lobby_view_4",
  "guest_room_1",
  "guest_room_2",
  "guest_room_3",
  "guestroom_1",
  "guestroom_2",
  "suite_1",
  "suite_2",
  "pool_view_1",
  "pool_1",
  "restaurant_1",
  "restaurant_2",
  "bar_1",
  "fitness_1",
  "bathroom_1",
  "meeting_1",
  "detail_1",
  "terrace_1",
  "aerial_1",
  "public_space_1",
  "public_area_1",
];

const PROPERTIES = [
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-palermo",
    propertyName: "Dazzler by Wyndham Buenos Aires Palermo",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-palermo-buenos-aires/overview",
  },
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-recoleta",
    propertyName: "Dazzler by Wyndham Buenos Aires Recoleta",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-recoleta/overview",
  },
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-san-martin",
    propertyName: "Dazzler by Wyndham Buenos Aires San Martin",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-san-martin/overview",
  },
  {
    brand: "dazzler-by-wyndham",
    propertyKey: "dazzler-polo",
    propertyName: "Dazzler by Wyndham Buenos Aires Polo",
    marketCity: "Buenos Aires",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-polo-hotel-buenos-aires/overview",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "mb-hotel-miami",
    propertyName: "MB Hotel, Trademark Collection by Wyndham",
    marketCity: "Miami Beach",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/miami-beach-florida/mb-hotel-trademark-collection-by-wyndham/overview",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "chula-vista",
    propertyName: "Chula Vista Resort, Trademark Collection by Wyndham",
    marketCity: "Wisconsin Dells",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/wisconsin-dells-wisconsin/chula-vista-resort-trademark/overview",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "the-walden",
    propertyName: "The Walden, Trademark Collection by Wyndham",
    marketCity: "Pigeon Forge",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/pigeon-forge-tennessee/the-walden-trademark-collection/overview",
  },
  {
    brand: "trademark-collection-by-wyndham",
    propertyKey: "magic-village-views",
    propertyName: "Magic Village Views, Trademark Collection by Wyndham",
    marketCity: "Kissimmee",
    sourcePageUrl:
      "https://www.wyndhamhotels.com/trademark/kissimmee-florida/magic-village-views-trademark-collection-by-wyndham/overview",
  },
];

async function headOk(url) {
  try {
    const res = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    if (res.ok) return true;
    // some CDNs reject HEAD — try GET range
    const get = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": "Mozilla/5.0", Range: "bytes=0-0" },
      redirect: "follow",
    });
    return get.ok || get.status === 206;
  } catch {
    return false;
  }
}

async function discoverBase(sourcePageUrl) {
  const res = await fetch(sourcePageUrl, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
    redirect: "follow",
  });
  const html = await res.text();
  const m = html.match(
    /https:\/\/www\.wyndhamhotels\.com\/content\/dam\/property-images\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/i
  );
  if (!m) return null;
  const imageUrl = m[0];
  const baseDir = imageUrl.replace(/\/[^/]+$/, "/");
  const file = imageUrl.split("/").pop() || "";
  const propertyId = (file.match(/^(\d+)_/) || [])[1] || null;
  return { seedImageUrl: imageUrl, baseDir, propertyId, status: res.status };
}

async function expandProperty(property) {
  console.log(`Expanding ${property.propertyKey}…`);
  const disc = await discoverBase(property.sourcePageUrl);
  if (!disc?.baseDir || !disc.propertyId) {
    console.log(`  no DAM seed found`);
    return { ...property, discovery: disc, pool: [] };
  }
  const pool = [];
  const seen = new Set();
  // keep seed
  pool.push({
    propertyKey: property.propertyKey,
    propertyName: property.propertyName,
    marketCity: property.marketCity,
    sourcePageUrl: property.sourcePageUrl,
    imageUrl: disc.seedImageUrl,
    label: "property",
  });
  seen.add(disc.seedImageUrl);

  for (const suffix of SUFFIXES) {
    const imageUrl = `${disc.baseDir}${disc.propertyId}_${suffix}.jpg`;
    if (seen.has(imageUrl)) continue;
    const ok = await headOk(imageUrl);
    if (!ok) continue;
    seen.add(imageUrl);
    pool.push({
      propertyKey: property.propertyKey,
      propertyName: property.propertyName,
      marketCity: property.marketCity,
      sourcePageUrl: property.sourcePageUrl,
      imageUrl,
      label: "property",
      probedSuffix: suffix,
    });
  }
  console.log(`  id=${disc.propertyId} pool=${pool.length}`);
  return { ...property, discovery: disc, pool };
}

async function main() {
  const byBrand = {
    "dazzler-by-wyndham": [],
    "trademark-collection-by-wyndham": [],
  };
  const results = [];
  for (const p of PROPERTIES) {
    const row = await expandProperty(p);
    results.push(row);
    byBrand[p.brand].push(...row.pool);
  }

  const fixturesDir = path.join(ROOT, "fixtures");
  fs.mkdirSync(fixturesDir, { recursive: true });
  for (const [slug, pool] of Object.entries(byBrand)) {
    const file = path.join(fixturesDir, `lane2-${slug}-gallery-pool.json`);
    fs.writeFileSync(file, `${JSON.stringify(pool, null, 2)}\n`);
    console.log(`Wrote ${file} (${pool.length} images)`);
  }

  const reportPath = path.join(ROOT, "reports", "brand-explorer-wyndham-dam-pool-expand.json");
  fs.writeFileSync(
    reportPath,
    `${JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)}\n`
  );
  console.log(`Wrote ${reportPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
