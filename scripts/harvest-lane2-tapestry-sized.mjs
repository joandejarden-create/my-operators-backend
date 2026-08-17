/**
 * Re-harvest Tapestry Hilton images using impolicy=ratio sizing.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SIZE = "impolicy=ratio&rw=1200&rh=800";

async function fetchText(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html,*/*",
    },
  });
  return { status: res.status, text: await res.text() };
}

async function imageOk(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent": "Mozilla/5.0",
      Accept: "image/*,*/*",
      Referer: "https://www.hilton.com/",
    },
  });
  const buf = Buffer.from(await res.arrayBuffer());
  const type = res.headers.get("content-type") || "";
  return res.status === 200 && /image\//i.test(type) && buf.length > 20000;
}

function extractHiltonIm(text) {
  const out = [];
  for (const m of text.matchAll(/https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi)) {
    out.push(m[0].split("?")[0]);
  }
  for (const m of text.matchAll(
    /https:\/\/web\.archive\.org\/web\/\d+(?:im_)?\/(https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp))/gi
  )) {
    out.push(m[1]);
  }
  return [...new Set(out)];
}

function withSize(url) {
  return url.includes("?") ? `${url}&${SIZE}` : `${url}?${SIZE}`;
}

const hotels = [
  {
    propertyKey: "savvyup-cotton-sail",
    propertyName: "The Cotton Sail Hotel Savannah, Tapestry Collection by Hilton",
    marketCity: "Savannah",
    sourcePageUrl: "https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
    wayback:
      "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
  },
  {
    propertyKey: "ilmwrup-ballast",
    propertyName: "Hotel Ballast Wilmington, Tapestry Collection by Hilton",
    marketCity: "Wilmington",
    sourcePageUrl: "https://www.hilton.com/en/hotels/ilmwrup-hotel-ballast-wilmington/",
    wayback:
      "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/ilmwrup-hotel-ballast-wilmington/",
  },
  {
    propertyKey: "litemup-burgundy",
    propertyName: "The Burgundy Hotel, Tapestry Collection by Hilton",
    marketCity: "Little Rock",
    sourcePageUrl: "https://www.hilton.com/en/hotels/litemup-the-burgundy-hotel/",
    wayback:
      "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/litemup-the-burgundy-hotel/",
  },
];

const pool = [];
for (const h of hotels) {
  process.stdout.write(`${h.propertyKey}... `);
  const wb = await fetchText(h.wayback);
  const urls = extractHiltonIm(wb.text);
  console.log(`candidates=${urls.length}`);
  const live = [];
  for (const u of urls) {
    const sized = withSize(u);
    if (await imageOk(sized)) live.push(sized);
    if (live.length >= 8) break;
  }
  console.log(`  ok=${live.length}`);
  for (const imageUrl of live) {
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

fs.writeFileSync(
  path.join(ROOT, "fixtures", "lane2-tapestry-collection-by-hilton-gallery-pool.json"),
  `${JSON.stringify(pool, null, 2)}\n`
);
console.log(`Wrote tapestry pool: ${pool.length}`);

// Clean handwritten titles
function decodeHtml(s) {
  return String(s || "")
    .replace(/&amp;/g, "&")
    .replace(/&#x27;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s*\|\s*.*$/i, "")
    .replace(/\s*[-–]\s*ALL\s*$/i, "")
    .replace(/\s+I\s+Handwritten Collection.*/i, " — Handwritten Collection")
    .replace(/\s+/g, " ")
    .trim();
}

const hwPath = path.join(ROOT, "fixtures", "lane2-handwritten-collection-gallery-pool.json");
const hw = JSON.parse(fs.readFileSync(hwPath, "utf8"));
for (const row of hw) {
  row.propertyName = decodeHtml(row.propertyName);
  if (!/handwritten/i.test(row.propertyName)) {
    row.propertyName = `${row.propertyName} — Handwritten Collection`;
  }
  // Prefer hotel names over "Hotel in San Francisco"
  if (/^Hotel in /i.test(row.propertyName)) {
    row.propertyName = "Hotel Stratford San Francisco — Handwritten Collection";
  }
}
fs.writeFileSync(hwPath, `${JSON.stringify(hw, null, 2)}\n`);
console.log("Handwritten titles cleaned.");
