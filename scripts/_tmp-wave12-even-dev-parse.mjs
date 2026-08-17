#!/usr/bin/env node
import fs from "node:fs";

const html = fs.readFileSync("reports/_tmp-even-dev.html", "utf8");

function around(needle, pad = 300) {
  let i = 0;
  let n = 0;
  while ((i = html.indexOf(needle, i)) >= 0 && n < 5) {
    console.log("\n---", needle, "at", i);
    console.log(html.slice(Math.max(0, i - pad), i + needle.length + pad).replace(/\s+/g, " "));
    i += needle.length;
    n++;
  }
}

for (const n of [
  "EVEN_Exterior",
  "EVEN Lobby",
  "MIASW_10947767447",
  "BZNBD_10092009639",
  "even-hotel/",
  "EVEN-Hotel-Orlando",
  "poster",
  "brightcove",
  "sources",
]) {
  around(n, 250);
}

// Extract all quoted strings containing even-hotel or EVEN_
const quoted = [
  ...html.matchAll(/"(https?:[^"]{0,200}EVEN[^"]{0,200})"/gi),
  ...html.matchAll(/"(https?:[^"]{0,200}even-hotel[^"]{0,200})"/gi),
  ...html.matchAll(/"(\/sites\/[^"]+\.(?:jpg|jpeg|png|webp))"/gi),
  ...html.matchAll(/"(even-hotel\/[^"]+)"/gi),
];
console.log("\nquoted hits", quoted.length);
for (const m of quoted.slice(0, 60)) console.log(m[1]);

// Try known Drupal media paths
const probes = [
  "https://development.ihg.com/sites/ihgplc/files/even-hotel/MIASW_10947767447.jpg",
  "https://development.ihg.com/sites/ihgplc/files/2022-06/even-hotel/MIASW_10947767447.jpg",
  "https://development.ihg.com/sites/ihgplc/files/IHG/even-hotel/MIASW_10947767447.jpg",
  "https://development.ihg.com/sites/ihgplc/files/brands/even-hotel/MIASW_10947767447.jpg",
  "https://digital.ihg.com/is/image/ihg/MIASW_10947767447",
  "https://digital.ihg.com/is/image/ihg/miasw-10947767447-4x3",
  "https://digital.ihg.com/is/image/ihg/even-hotels-miami-doral-10947767447-4x3",
  "https://digital.ihg.com/is/image/ihg/even-hotels-miami-10947767447-4x3",
];
for (const u of probes) {
  const res = await fetch(u, {
    method: "GET",
    headers: { "User-Agent": "Mozilla/5.0", Range: "bytes=0-128" },
    redirect: "follow",
  });
  const buf = Buffer.from(await res.arrayBuffer());
  const head = buf.slice(0, 20).toString("utf8");
  console.log(res.status, res.headers.get("content-type"), u.slice(-70), head.replace(/\n/g, " "));
}
