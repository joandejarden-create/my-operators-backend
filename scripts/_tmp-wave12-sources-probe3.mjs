#!/usr/bin/env node
async function fetchText(url, headers = {}) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html,application/json",
      ...headers,
    },
    redirect: "follow",
  });
  const text = await r.text();
  return { status: r.status, text, url: r.url };
}

function extractHiltonIm(text) {
  const re =
    /https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi;
  return [...new Set([...text.matchAll(re)].map((m) => m[0]))];
}

function extractIhg(text) {
  return [
    ...new Set(
      [...text.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/([^"'\\\s<>?]+)/gi)].map(
        (m) => m[1]
      )
    ),
  ];
}

function extractTrip(text) {
  const out = [];
  for (const re of [
    /https:\/\/media-cdn\.tripadvisor\.com\/media\/photo-[^"'\\\s<>]+/gi,
    /https:\/\/dynamic-media\.tacdn\.com\/[^"'\\\s<>]+/gi,
    /https:\/\/images\.trvl-media\.com\/[^"'\\\s<>]+/gi,
  ]) {
    for (const m of text.matchAll(re)) out.push(m[0].replace(/[),.;]+$/, ""));
  }
  return [...new Set(out)];
}

async function cdx(url) {
  try {
    const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
      url
    )}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=10`;
    const r = await fetch(api, {
      headers: { "User-Agent": "Mozilla/5.0 Dealality/1.0", Accept: "application/json" },
    });
    const ct = r.headers.get("content-type") || "";
    if (!ct.includes("json")) {
      console.log("CDX non-json", r.status, url.slice(-50));
      return [];
    }
    const j = await r.json();
    return j.slice(1);
  } catch (e) {
    console.log("CDX err", e.message);
    return [];
  }
}

const hiltonPages = [
  "https://www.hilton.com/en/hotels/waswhcp-canopy-washington-dc-the-wharf/",
  "https://www.hilton.com/en/hotels/rekcpcp-canopy-reykjavik-city-centre/",
  "https://www.hilton.com/en/hotels/pdxpdcp-canopy-portland-pearl-district/",
  "https://www.hilton.com/en/hotels/czmtuua-motto-tulum/",
  "https://www.hilton.com/en/hotels/cuzinua-motto-cusco/",
  "https://www.hilton.com/en/hotels/dcamtmt-motto-washington-dc-city-center/",
  "https://www.hilton.com/en/hotels/bnapopo-tempo-nashville-downtown/",
  "https://www.hilton.com/en/hotels/rdutpup-tempo-raleigh/",
  "https://www.hilton.com/en/hotels/tytspup-tempo-pigeon-forge/",
];

for (const page of hiltonPages) {
  const rows = await cdx(page);
  console.log("\n", page.split("/").filter(Boolean).pop(), "cdx", rows.length);
  if (!rows.length) continue;
  const ts = rows[Math.floor(rows.length / 2)][0];
  const wa = `https://web.archive.org/web/${ts}/${page}`;
  const { status, text } = await fetchText(wa);
  const imgs = extractHiltonIm(text);
  console.log("  WA", ts, status, "imgs", imgs.length, imgs[0] || "");
}

// stories.hilton.com (often not 403)
for (const u of [
  "https://stories.hilton.com/canopy-by-hilton-fact-sheet",
  "https://stories.hilton.com/releases/hilton-lifestyle-brand-lands-in-music-city-tempo-by-hilton-nashville-downtown-is-now-open",
  "https://stories.hilton.com/releases/motto-by-hilton-debuts-in-mexico-this-fall-with-the-opening-of-motto-by-hilton-tulum",
]) {
  const { status, text } = await fetchText(u);
  const imgs = [
    ...extractHiltonIm(text),
    ...[...text.matchAll(/https:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) => m[0]),
  ].filter((x) => /hilton|motto|canopy|tempo/i.test(x) && !/logo|icon|sprite/i.test(x));
  console.log("\nstories", status, u.slice(-55), "imgs", [...new Set(imgs)].length);
  console.log([...new Set(imgs)].slice(0, 6).join("\n"));
}

// EVEN via development + tripadvisor search
for (const u of [
  "https://development.ihg.com/hotel-brands/even-hotels",
  "https://www.ihgplc.com/en/news-and-media/news-releases/2022/ihg-hotels-and-resorts-evolves-upscale-even-hotels-brand",
]) {
  const { status, text } = await fetchText(u);
  const ids = extractIhg(text);
  const even = ids.filter((id) => /even/i.test(id));
  const imgs = [...text.matchAll(/https:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) =>
    m[0].replace(/[),.;]+$/, "")
  );
  console.log("\nEVEN src", status, u.slice(-50), "ihgIds", even.length, "imgs", imgs.length);
  console.log(even.slice(0, 10).join("\n"));
  console.log(imgs.filter((x) => /even|ihg|media/i.test(x)).slice(0, 8).join("\n"));
}

// TripAdvisor EVEN NY (known hotel page pattern — may need discover)
const tripUrls = [
  "https://www.tripadvisor.com/Hotel_Review-g60763-d9586756-Reviews-EVEN_Hotel_New_York_Times_Square_South-New_York_City_New_York.html",
  "https://www.tripadvisor.com/Hotel_Review-g34438-d12132720-Reviews-EVEN_Hotel_Miami_Airport-Miami_Florida.html",
];
for (const u of tripUrls) {
  const { status, text } = await fetchText(u);
  const imgs = extractTrip(text);
  console.log("\nTrip", status, u.slice(-40), "imgs", imgs.length);
  console.log(imgs.slice(0, 8).join("\n"));
}

// Bunkhouse property pages
for (const u of [
  "https://bunkhousehotels.com/hotels/hotel-san-cristobal/",
  "https://bunkhousehotels.com/hotels/hotel-san-fernando/",
  "https://bunkhousehotels.com/hotels/hotel-saint-cecilia/",
]) {
  const { status, text, url: finalUrl } = await fetchText(u);
  const imgs = [
    ...new Set(
      [...text.matchAll(/https:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi)].map(
        (m) => m[0].replace(/[),.;]+$/, "")
      )
    ),
  ].filter((x) => !/logo|favicon|404-image|default\.png|wp-includes/i.test(x));
  console.log("\nBunk", status, finalUrl.slice(-50), "imgs", imgs.length);
  console.log(imgs.slice(0, 8).join("\n"));
}
