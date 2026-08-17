#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const pages = ["/experiences/", "/rooms/", "/dining/", "/events/"];
const H = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html",
};

for (const p of pages) {
  const html = await fetch(`https://www.marriott.com/en-us/hotels/${SLUG}${p}`, { headers: H }).then(
    (r) => r.text()
  );
  console.log("\n===", p, html.length);

  // JSON-LD with flexible script tag
  for (const m of html.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)) {
    try {
      const j = JSON.parse(m[1].trim());
      const type = j["@type"];
      console.log(" ld+json", type, j.description?.slice?.(0, 80), j.amenityFeature?.length);
    } catch (e) {
      console.log(" ld+json parse err", e.message);
    }
  }

  // Look for inline JSON blobs with amenity arrays
  for (const m of html.matchAll(/"amenity(?:Feature|List|Names)?"\s*:\s*(\[[\s\S]{20,8000}?\])/gi)) {
    console.log(" amenity blob", m[1].slice(0, 200));
  }

  // data-page-model or similar
  for (const m of html.matchAll(/data-page-model="([^"]+)"/gi)) {
    console.log(" data-page-model len", m[1].length);
  }

  // AEM content fragments in script tags
  for (const key of ["pageModel", "hotelData", "propertyData", "overviewText", "featuredAmenities"]) {
    const idx = html.indexOf(`"${key}"`);
    if (idx >= 0) console.log(" found key", key, "at", idx, html.slice(idx, idx + 200));
  }
}

// Parse experiences JSON-LD Hotel block fully
const exp = await fetch(`https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`, { headers: H }).then(
  (r) => r.text()
);
const ldBlocks = [...exp.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)].map(
  (m) => {
    try {
      return JSON.parse(m[1].trim());
    } catch {
      return null;
    }
  }
).filter(Boolean);

writeFileSync("reports/marriott-poplc-ld.json", JSON.stringify(ldBlocks, null, 2));
console.log("\nld blocks saved", ldBlocks.length);
for (const b of ldBlocks) {
  console.log(b["@type"], Object.keys(b));
}
