/**
 * Harvest Hilton Tapestry + Radisson Collection pools via known CDN / Wayback.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

async function fetchText(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html,image/*,*/*",
    },
  });
  return { status: res.status, text: await res.text(), finalUrl: res.url, type: res.headers.get("content-type") };
}

async function imageOk(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        Accept: "image/*,*/*",
      },
    });
    const buf = Buffer.from(await res.arrayBuffer());
    const type = res.headers.get("content-type") || "";
    return res.status === 200 && /image\//i.test(type) && buf.length > 8000;
  } catch {
    return false;
  }
}

function extractHiltonIm(text) {
  const out = [];
  for (const m of text.matchAll(/https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi)) {
    out.push(m[0].split("?")[0]);
  }
  // also from wayback-wrapped
  for (const m of text.matchAll(
    /https:\/\/web\.archive\.org\/web\/\d+(?:im_)?\/(https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp))/gi
  )) {
    out.push(m[1]);
  }
  return [...new Set(out)];
}

function extractRadissonMedia(text) {
  const out = [];
  for (const m of text.matchAll(/https:\/\/media\.radissonhotels\.net\/image\/[^"'\\\s<>]+/gi)) {
    const u = m[0].replace(/[),.;]+$/, "");
    if (/promotional|radisson-hotels-app|doorknob|logo/i.test(u)) continue;
    out.push(u.split("?")[0]);
  }
  return [...new Set(out)];
}

const tapestryHotels = [
  {
    propertyKey: "savvyup-cotton-sail",
    propertyName: "The Cotton Sail Hotel Savannah, Tapestry Collection by Hilton",
    marketCity: "Savannah",
    ctyhocn: "SAVVYUP",
    sourcePageUrl: "https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
    wayback:
      "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
  },
  {
    propertyKey: "ilmwrup-ballast",
    propertyName: "Hotel Ballast Wilmington, Tapestry Collection by Hilton",
    marketCity: "Wilmington",
    ctyhocn: "ILMWRUP",
    sourcePageUrl: "https://www.hilton.com/en/hotels/ilmwrup-hotel-ballast-wilmington/",
    wayback:
      "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/ilmwrup-hotel-ballast-wilmington/",
  },
  {
    propertyKey: "litemup-burgundy",
    propertyName: "The Burgundy Hotel, Tapestry Collection by Hilton",
    marketCity: "Little Rock",
    ctyhocn: "LITEMUP",
    sourcePageUrl: "https://www.hilton.com/en/hotels/litemup-the-burgundy-hotel/",
    wayback:
      "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/litemup-the-burgundy-hotel/",
  },
];

const tapestryPool = [];
for (const h of tapestryHotels) {
  process.stdout.write(`Tapestry ${h.ctyhocn}... `);
  let urls = [];
  try {
    const wb = await fetchText(h.wayback);
    urls = extractHiltonIm(wb.text);
    console.log(`wayback=${wb.status} candidates=${urls.length}`);
  } catch (err) {
    console.log(`wayback ERR ${err.message}`);
  }
  // Prefer live hilton.com/im URLs
  const live = [];
  for (const u of urls.slice(0, 20)) {
    const ok = await imageOk(u);
    if (ok) live.push(u);
    if (live.length >= 8) break;
  }
  console.log(`  live-ok=${live.length}`);
  for (const imageUrl of live) {
    tapestryPool.push({
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
  `${JSON.stringify(tapestryPool, null, 2)}\n`
);
console.log(`Tapestry pool: ${tapestryPool.length}`);

const radissonHotels = [
  {
    propertyKey: "stockholm-strand",
    propertyName: "Radisson Collection Strand Hotel, Stockholm",
    marketCity: "Stockholm",
    sourcePageUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection-stockholm-strand",
    waybacks: [
      "https://web.archive.org/web/2024/https://www.radissonhotels.com/en-us/hotels/radisson-collection-stockholm-strand",
      "https://web.archive.org/web/20240101000000/https://www.radissonhotels.com/en-us/hotels/radisson-collection-stockholm-strand",
    ],
  },
  {
    propertyKey: "santa-sofia-milan",
    propertyName: "Radisson Collection Santa Sofia, Milan",
    marketCity: "Milan",
    sourcePageUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection-santa-sofia-milan",
    waybacks: [
      "https://web.archive.org/web/2024/https://www.radissonhotels.com/en-us/hotels/radisson-collection-santa-sofia-milan",
      "https://web.archive.org/web/20240101000000/https://www.radissonhotels.com/en-us/hotels/radisson-collection-santa-sofia-milan",
    ],
  },
  {
    propertyKey: "edinburgh-royal-mile",
    propertyName: "Radisson Collection Hotel, Royal Mile Edinburgh",
    marketCity: "Edinburgh",
    sourcePageUrl: "https://www.radissonhotels.com/en-us/hotels/radisson-collection-edinburgh-royal-mile",
    waybacks: [
      "https://web.archive.org/web/2024/https://www.radissonhotels.com/en-us/hotels/radisson-collection-edinburgh-royal-mile",
      "https://web.archive.org/web/20240101000000/https://www.radissonhotels.com/en-us/hotels/radisson-collection-edinburgh-royal-mile",
    ],
  },
];

const radissonPool = [];
for (const h of radissonHotels) {
  process.stdout.write(`Radisson ${h.propertyKey}... `);
  let urls = [];
  for (const wbUrl of h.waybacks) {
    try {
      const wb = await fetchText(wbUrl);
      const found = extractRadissonMedia(wb.text);
      // also any hotel jpg on page
      const jpgs = [
        ...wb.text.matchAll(/https:\/\/[^"'\\\s<>]*radisson[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi),
      ].map((m) => m[0].split("?")[0]);
      urls.push(...found, ...jpgs.filter((u) => !/logo|promotional|doorknob|app\//i.test(u)));
      if (urls.length) {
        console.log(`wayback hit status candidates=${[...new Set(urls)].length}`);
        break;
      }
    } catch {
      /* try next */
    }
  }
  urls = [...new Set(urls)];
  if (!urls.length) console.log("no media found");
  const live = [];
  for (const u of urls.slice(0, 25)) {
    // unwrap wayback prefix if present
    const liveUrl = u.includes("web.archive.org")
      ? u.replace(/^https:\/\/web\.archive\.org\/web\/\d+(?:im_)?\//, "")
      : u;
    if (await imageOk(liveUrl)) live.push(liveUrl);
    if (live.length >= 8) break;
  }
  console.log(`  live-ok=${live.length}`);
  for (const imageUrl of live) {
    radissonPool.push({
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
  path.join(ROOT, "fixtures", "lane2-radisson-collection-gallery-pool.json"),
  `${JSON.stringify(radissonPool, null, 2)}\n`
);
console.log(`Radisson pool: ${radissonPool.length}`);

// Fix handwritten property names via <title>
const hw = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures", "lane2-handwritten-collection-gallery-pool.json"), "utf8")
);
const codeTitle = {};
for (const row of hw) {
  const code = (row.sourcePageUrl.match(/hotel\/([A-Za-z0-9]+)/) || [])[1];
  if (!code || codeTitle[code]) continue;
  try {
    const { text } = await fetchText(row.sourcePageUrl);
    const title = ((text.match(/<title>([^<]+)<\/title>/i) || [])[1] || "")
      .replace(/\s*\|\s*.*$/, "")
      .replace(/\s+/g, " ")
      .trim();
    codeTitle[code] = title || row.propertyName;
    console.log(`HW title ${code}: ${codeTitle[code]}`);
  } catch (err) {
    console.log(`HW title ${code} ERR`, err.message);
  }
}
for (const row of hw) {
  const code = (row.sourcePageUrl.match(/hotel\/([A-Za-z0-9]+)/) || [])[1];
  if (code && codeTitle[code]) {
    row.propertyName = codeTitle[code];
    row.propertyKey = `${code.toLowerCase()}-${codeTitle[code]
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/-+/g, "-")
      .slice(0, 48)}`;
  }
}
fs.writeFileSync(
  path.join(ROOT, "fixtures", "lane2-handwritten-collection-gallery-pool.json"),
  `${JSON.stringify(hw, null, 2)}\n`
);
console.log("Handwritten titles updated.");
