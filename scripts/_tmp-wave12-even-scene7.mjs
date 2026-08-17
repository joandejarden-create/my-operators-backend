#!/usr/bin/env node
async function exists(url) {
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0",
        Range: "bytes=0-200",
        Accept: "*/*",
      },
      redirect: "follow",
    });
    const buf = Buffer.from(await r.arrayBuffer());
    const ct = r.headers.get("content-type") || "";
    // Scene7 returns XML error for missing
    const isImg = /image|jpeg|png|webp/i.test(ct) || (buf[0] === 0xff && buf[1] === 0xd8);
    const isXmlErr = /Problem parsing|does not exist|error/i.test(buf.toString("utf8"));
    return { ok: (r.ok || r.status === 206) && isImg && !isXmlErr, status: r.status, ct, bytes: buf.length };
  } catch (e) {
    return { ok: false, err: e.message };
  }
}

const candidates = [];
const stems = [
  "even-hotels-new-york-10552045640",
  "even-hotels-new-york-times-square-south",
  "even-hotels-new-york",
  "even-hotel-new-york-times-square-south",
  "even-hotels-miami",
  "even-hotel-miami",
  "even-hotels-miami-airport",
  "even-hotels-new-york-midtown-east",
  "even-hotels-omaha",
  "even-hotels-rockville",
  "even-hotels-eugene",
  "even-hotels-syracuse",
];
const ratios = ["", "-4x3", "-2x1", "-16x9", "-3x2", "-1x1", "-original"];
for (const stem of stems) {
  for (const r of ratios) {
    candidates.push(`https://digital.ihg.com/is/image/ihg/${stem}${r}`);
  }
}

// Also probe numeric neighbors around 10552045640
for (let i = 10552045630; i <= 10552045650; i++) {
  for (const r of ["-4x3", "-2x1", "-16x9"]) {
    candidates.push(`https://digital.ihg.com/is/image/ihg/even-hotels-new-york-${i}${r}`);
  }
}

const hits = [];
for (const u of candidates) {
  const res = await exists(u);
  if (res.ok) {
    hits.push(u);
    console.log("HIT", u);
  }
}
console.log("\nTotal hits", hits.length);

// Try content listing / search endpoints
for (const u of [
  "https://digital.ihg.com/is/image/ihg/even-hotels-new-york-10552045640-original",
  "https://digital.ihg.com/is/content/ihg/even-hotels-new-york-10552045640-original",
  "https://digital.ihg.com/is/image/ihg/even-wellness-stay-concession-offer",
]) {
  console.log(u.slice(-50), await exists(u));
}

// Fetch development page HTML more carefully for even image refs
const dev = await fetch("https://development.ihg.com/hotel-brands/even-hotels", {
  headers: { "User-Agent": "Mozilla/5.0" },
});
const html = await dev.text();
const refs = [
  ...html.matchAll(/even[^"'\\\s<>]{0,80}/gi),
].map((m) => m[0]);
console.log("\ndev status", dev.status, "even refs", [...new Set(refs)].slice(0, 40));

// Wayback for older EVEN hoteldetail that might embed property photos
const cdx =
  "https://web.archive.org/cdx/search/cdx?url=www.ihg.com/evenhotels/hotels/us/en/new-york/nycep/hoteldetail&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=15";
const cdxRes = await fetch(cdx, { headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" } });
const cdxText = await cdxRes.text();
console.log("\nCDX", cdxRes.status, cdxText.slice(0, 500));
if (cdxText.startsWith("[")) {
  const j = JSON.parse(cdxText);
  for (const row of j.slice(1, 4)) {
    const wa = `https://web.archive.org/web/${row[0]}id_/${row[1]}`;
    const r = await fetch(wa, { headers: { "User-Agent": "Mozilla/5.0" } });
    const t = await r.text();
    const ids = [
      ...new Set(
        [...t.matchAll(/digital\.ihg\.com\/is\/image\/ihg\/([^"'\\\s<>?]+)/gi)].map((m) => m[1])
      ),
    ].filter((id) => /even/i.test(id));
    console.log("WA", row[0], r.status, "even ids", ids.length, ids.slice(0, 15));
  }
}
