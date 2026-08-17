#!/usr/bin/env node
/**
 * Probe IHG / Marriott / Hilton CDNs for Wave 12 property imagery.
 */
async function exists(url) {
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        Range: "bytes=0-128",
        Referer: "https://www.ihg.com/",
      },
      redirect: "follow",
    });
    return r.ok || r.status === 206;
  } catch {
    return false;
  }
}

const ihgProbes = [];
const evenCodes = ["nycep", "miaeh", "nycme", "even-hotel-new-york", "even-hotels-new-york-times-square-south"];
const evenPatterns = (code) => [
  `https://digital.ihg.com/is/image/ihg/${code}-exterior-1`,
  `https://digital.ihg.com/is/image/ihg/${code}-exterior`,
  `https://digital.ihg.com/is/image/ihg/${code}-lobby-1`,
  `https://digital.ihg.com/is/image/ihg/${code}-guestroom-1`,
  `https://digital.ihg.com/is/image/ihg/${code}-fitness-1`,
  `https://digital.ihg.com/is/image/ihg/even-hotels-${code}-exterior`,
  `https://digital.ihg.com/is/image/ihg/even-${code}-exterior`,
];

for (const code of evenCodes) {
  for (const u of evenPatterns(code)) {
    if (await exists(u)) ihgProbes.push(u);
  }
}

console.log("EVEN hits", ihgProbes.length);
for (const u of ihgProbes.slice(0, 20)) console.log(" ", u);

// Fetch actual hoteldetail and find even- property asset ids
async function extractIhg(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html",
    },
  });
  const html = await r.text();
  const all = [...html.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/([^"'\\\s<>?]+)/gi)].map(
    (m) => m[1]
  );
  const uniq = [...new Set(all)];
  const propertyish = uniq.filter(
    (id) =>
      /even|nycep|miaeh|nycme|voco|avid|ex-/i.test(id) &&
      !/getty|family|snorkeling|maldives|stays|logo|chiclet|learning|portal/i.test(id)
  );
  console.log("\n", url.slice(-40), "status", r.status, "total", uniq.length, "propertyish", propertyish.length);
  console.log(propertyish.slice(0, 25).join("\n"));
}

await extractIhg("https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail");
await extractIhg("https://www.ihg.com/voco/hotels/us/en/ciudad-de-mexico/mexvc/hoteldetail");
await extractIhg("https://www.ihg.com/avidhotels/hotels/us/en/austin/ausav/hoteldetail");

// Wayback CDX for canopy
async function cdx(url) {
  const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(url)}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=5`;
  const r = await fetch(api, {
    headers: { "User-Agent": "Mozilla/5.0 DealalityImageHarvest/1.0" },
  });
  const j = await r.json();
  console.log("\nCDX", url.slice(-50), j.slice(0, 4));
  return j.slice(1);
}

const canopy = await cdx(
  "https://www.hilton.com/en/hotels/cnypycc-canopy-by-hilton-pittsburgh-downtown/"
);
const tempo = await cdx("https://www.hilton.com/en/hotels/nycnptp-tempo-by-hilton-times-square/");
const motto = await cdx("https://www.hilton.com/en/hotels/czmtuua-motto-tulum/");

async function harvestWa(ts, original) {
  const wa = `https://web.archive.org/web/${ts}/${original}`;
  const r = await fetch(wa, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
  });
  const t = await r.text();
  const re =
    /https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi;
  const imgs = [...new Set([...t.matchAll(re)].map((m) => m[0]))];
  console.log("WA", ts, r.status, "imgs", imgs.length, imgs[0] || "");
  return imgs;
}

if (canopy[0]) await harvestWa(canopy[0][0], canopy[0][1]);
if (tempo[0]) await harvestWa(tempo[0][0], tempo[0][1]);
if (motto[0]) await harvestWa(motto[0][0], motto[0][1]);
