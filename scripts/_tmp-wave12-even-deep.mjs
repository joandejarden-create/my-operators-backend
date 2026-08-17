#!/usr/bin/env node
async function dump(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    },
  });
  const html = await r.text();
  const ids = [
    ...new Set(
      [...html.matchAll(/https:\/\/digital\.ihg\.com\/is\/image\/ihg\/([^"'\\\s<>?]+)/gi)].map(
        (m) => m[1]
      )
    ),
  ];
  console.log(url.slice(-50), "status", r.status, "ids", ids.length);
  for (const id of ids) console.log(" ", id);
  // Also look for JSON image payloads
  const jsonImgs = [
    ...html.matchAll(/"(?:imageUrl|url|hiRes|src|path)"\s*:\s*"(https?:[^"]+)"/gi),
  ].map((m) => m[1].replace(/\\u002F/g, "/").replace(/\\\//g, "/"));
  console.log("json-ish", [...new Set(jsonImgs)].filter((u) => /image|ihg|photo/i.test(u)).slice(0, 20));
}

await dump("https://www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail");
await dump("https://www.ihg.com/evenhotels/hotels/us/en/miami/miaeh/hoteldetail");

// Try IHG media gallery API patterns used by some properties
const apis = [
  "https://apis.ihg.com/marketing/v1/destinations/hotels/NYCEP/media",
  "https://www.ihg.com/content/us/en/brands/evenhotels.image.json",
  "https://digital.ihg.com/is/image/ihg/?req=set,json,imageSet&id=nycep",
];
for (const u of apis) {
  try {
    const r = await fetch(u, {
      headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json,*/*" },
    });
    const t = await r.text();
    console.log("\nAPI", r.status, u.slice(-60), t.slice(0, 200).replace(/\s+/g, " "));
  } catch (e) {
    console.log("API ERR", e.message);
  }
}

// Search stories / press for EVEN images beyond meta
const press = [
  "https://news.google.com/rss/search?q=EVEN+Hotels+IHG+opening+when:5y",
  "https://www.prnewswire.com/search/news/?keyword=EVEN%20Hotels&page=1&pagesize=20",
];
for (const u of press) {
  try {
    const r = await fetch(u, { headers: { "User-Agent": "Mozilla/5.0" } });
    const t = await r.text();
    const imgs = [...t.matchAll(/https?:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) =>
      m[0]
    );
    console.log("\npress", r.status, u.slice(0, 50), "imgs", imgs.length);
    console.log(imgs.filter((x) => /even|ihg/i.test(x)).slice(0, 10).join("\n"));
  } catch (e) {
    console.log("press ERR", e.message);
  }
}

// Wayback CDX for EVEN meta images folder
const cdx =
  "https://web.archive.org/cdx/search/cdx?url=ihgplc.com/*EVEN*&output=json&fl=timestamp,original,statuscode,mimetype&filter=mimetype:image/jpeg&limit=40";
const r = await fetch(cdx, { headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" } });
const t = await r.text();
console.log("\nCDX ihgplc EVEN", r.status);
if (t.startsWith("[")) {
  const j = JSON.parse(t);
  for (const row of j.slice(1, 25)) console.log(row[1]);
} else console.log(t.slice(0, 300));
