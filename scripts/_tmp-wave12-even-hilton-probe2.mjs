#!/usr/bin/env node
async function exists(url, referer = "https://www.ihg.com/") {
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0",
        Range: "bytes=0-64",
        Referer: referer,
      },
      redirect: "follow",
    });
    return r.ok || r.status === 206;
  } catch {
    return false;
  }
}

async function extract(url) {
  const r = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
  });
  const html = await r.text();
  const ids = [
    ...new Set(
      [...html.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/([^"'\\\s<>?]+)/gi)].map(
        (m) => m[1]
      )
    ),
  ];
  return { status: r.status, ids, htmlLen: html.length };
}

// Try photos / gallery variants + brand pages
const pages = [
  "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail",
  "https://www.ihg.com/evenhotels/content/us/en/brands/evenhotels.html",
  "https://www.ihg.com/evenhotels/us/en/reservation",
  "https://www.ihg.com/evenhotels/hotels/us/en/find-hotels/hotel-search-results",
];

for (const p of pages) {
  try {
    const { status, ids } = await extract(p);
    const even = ids.filter((id) => /even/i.test(id) && !/getty|family|snorkel|learning|portal|stays/i.test(id));
    console.log(status, p.slice(-60), "ids", ids.length, "even", even.length);
    console.log(even.slice(0, 15).join("\n") || "(none)");
    console.log("---");
  } catch (e) {
    console.log("ERR", p, e.message);
  }
}

// Probe naming patterns for EVEN NY
const stems = [
  "even-hotels-new-york",
  "even-hotel-new-york",
  "even-hotels-times-square",
  "even-hotels-new-york-times-square-south",
  "even-nycep",
  "nycep-even",
  "even-hotels-miami",
  "even-hotel-miami-airport",
  "even-hotels-midtown-east",
];
const suffixes = [
  "-exterior",
  "-lobby",
  "-guestroom",
  "-fitness",
  "-pool",
  "-restaurant",
  "-1",
  "-2",
  "-3",
  "-4x3",
  "-2x1",
  "-16x9",
];
const hits = [];
for (const stem of stems) {
  for (const s of suffixes) {
    const id = `${stem}${s}`;
    const u = `https://digital.ihg.com/is/image/ihg/${id}`;
    if (await exists(u)) hits.push(id);
  }
}
console.log("\nProbe hits", hits.length);
console.log(hits.slice(0, 40).join("\n"));

// Numeric ID style like avid
for (const n of [
  "even-hotels-new-york-times-square-south",
  "even-hotels-miami-airport",
  "even-hotels-new-york-midtown-east",
]) {
  for (let i = 5000000000; i < 5000000010; i++) {
    // skip massive probe
  }
}

// Wayback CDX even
async function cdx(url) {
  const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(url)}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=8`;
  const r = await fetch(api, { headers: { "User-Agent": "Mozilla/5.0" } });
  const j = await r.json();
  console.log("\nCDX", url.slice(-55), j.length - 1);
  return j.slice(1);
}

const evenCdx = await cdx(
  "https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail"
);
if (evenCdx[0]) {
  const wa = `https://web.archive.org/web/${evenCdx[0][0]}/${evenCdx[0][1]}`;
  const { status, ids } = await extract(wa);
  const even = ids.filter((id) => /even/i.test(id));
  console.log("WA even", status, even.slice(0, 20));
}

// Hilton canopy/tempo alternate codes from seeds
const hiltonUrls = [
  "hilton.com/en/hotels/cnypycc*",
  "hilton.com/en/hotels/*canopy*",
  "hilton.com/en/hotels/*tempo*",
  "hilton.com/en/hotels/cnycndc*",
  "hilton.com/en/hotels/phxtptp*",
];
for (const u of hiltonUrls) {
  const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(u)}&matchType=prefix&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=5`;
  const r = await fetch(api, { headers: { "User-Agent": "Mozilla/5.0" } });
  const j = await r.json();
  console.log("\nCDX prefix", u, "rows", Math.max(0, j.length - 1));
  for (const row of j.slice(1, 4)) console.log(" ", row[0], row[1].slice(0, 90));
}
