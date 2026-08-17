#!/usr/bin/env node
async function check(url, label) {
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0",
        Accept: "image/*,*/*",
        Referer: "https://www.hilton.com/",
      },
      redirect: "follow",
    });
    const buf = Buffer.from(await r.arrayBuffer());
    console.log(label, r.status, "bytes", buf.length, "ct", r.headers.get("content-type"));
  } catch (e) {
    console.log(label, "ERR", e.message);
  }
}

const hilton =
  "https://www.hilton.com/im/en/BNAPOPO/18516072/bnapo-exterior2.jpg?impolicy=ratio&rw=1200&rh=800";
const stories =
  "https://stories-editor.hilton.com/wp-content/uploads/2024/02/Tempo-by-Hilton-Nashville-Lobby.jpg";
const even =
  "https://www.ihgplc.com/~/media/Images/I/Ihg-Plc/images/news/2022/EVEN-Hotels-Exterior-meta-image.jpg";
const bunk =
  "https://login.bunkhousehotels.com/hotel-saint-cecilia/wp-content/uploads/sites/12/2023/06/Nick_Simonite_HSC_20220615_Lounge00002-3.webp";

await check(hilton, "hilton-im");
await check(`https://wsrv.nl/?url=${encodeURIComponent(hilton)}&w=800&output=jpg`, "wsrv-hilton");
await check(stories, "stories");
await check(even, "even-ihgplc");
await check(bunk, "bunk");

// CDX image search for even
const cdxApi =
  "https://web.archive.org/cdx/search/cdx?url=digital.ihg.com/is/image/ihg/*even*&output=json&fl=timestamp,original,statuscode,mimetype&filter=statuscode:200&limit=30&from=2018";
try {
  const r = await fetch(cdxApi, { headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" } });
  const t = await r.text();
  console.log("\nCDX even status", r.status, "ct", r.headers.get("content-type"));
  if (t.startsWith("[")) {
    const j = JSON.parse(t);
    console.log("rows", j.length - 1);
    for (const row of j.slice(1, 20)) console.log(row[1]);
  } else {
    console.log(t.slice(0, 200));
  }
} catch (e) {
  console.log("CDX err", e.message);
}

// List media folder on ihgplc for EVEN
const mediaDirs = [
  "https://www.ihgplc.com/~/media/Images/I/Ihg-Plc/images/news/2022/",
  "https://www.ihgplc.com/en/brands/even-hotels",
  "https://www.ihg.com/content/dam/etc/marketing/even",
];
for (const u of mediaDirs) {
  try {
    const r = await fetch(u, { headers: { "User-Agent": "Mozilla/5.0" }, redirect: "follow" });
    const text = await r.text();
    const imgs = [...text.matchAll(/EVEN[^"'\\\s<>]*\.(?:jpg|jpeg|png|webp)/gi)].map((m) => m[0]);
    const urls = [
      ...text.matchAll(/https?:\/\/[^"'\\\s<>]*EVEN[^"'\\\s<>]*\.(?:jpg|jpeg|png|webp)/gi),
    ].map((m) => m[0]);
    console.log("\nmedia", r.status, u.slice(-40), "hits", imgs.length, urls.length);
    console.log([...new Set([...imgs, ...urls])].slice(0, 15).join("\n"));
  } catch (e) {
    console.log("media ERR", e.message);
  }
}

// hotel-san-fernando bunkhouse path
for (const u of [
  "https://www.bunkhousehotels.com/hotel-san-fernando",
  "https://www.bunkhousehotels.com/austin-proper",
  "https://www.bunkhousehotels.com/hotel-figueroa",
]) {
  const r = await fetch(u, { headers: { "User-Agent": "Mozilla/5.0" }, redirect: "follow" });
  const text = await r.text();
  const imgs = [
    ...new Set(
      [...text.matchAll(/https:\/\/login\.bunkhousehotels\.com\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi)].map(
        (m) => m[0]
      )
    ),
  ].filter((x) => !/-\d{2,4}x\d{2,4}\./.test(x) && !/logo|icon|png$/i.test(x) || /Nick_|Simonite|Property|Lobby|Room|Exterior|Pool/i.test(x));
  console.log("\nbunk page", r.status, r.url.slice(-40), "fullsize-ish", imgs.length);
  console.log(imgs.slice(0, 12).join("\n"));
}
