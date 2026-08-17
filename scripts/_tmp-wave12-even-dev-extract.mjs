#!/usr/bin/env node
import fs from "node:fs";

const r = await fetch("https://development.ihg.com/hotel-brands/even-hotels", {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    Accept: "text/html",
  },
});
const html = await r.text();
fs.writeFileSync("reports/_tmp-even-dev.html", html);
console.log("status", r.status, "len", html.length);

const idx = html.indexOf("even-hotel");
console.log("first even-hotel idx", idx);
console.log(html.slice(Math.max(0, idx - 200), idx + 800));

const imgs = [
  ...html.matchAll(/https?:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)[^"'\\\s<>]*/gi),
].map((m) => m[0].replace(/&amp;/g, "&").replace(/&quot;/g, ""));
const evenPaths = [...html.matchAll(/even-hotel\/[^"'\\\s<>]+/gi)].map((m) =>
  m[0].replace(/&amp;/g, "&").replace(/&quot;/g, "")
);
const media = [...html.matchAll(/\/sites\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) =>
  m[0]
);

console.log("\nabsolute", [...new Set(imgs)].slice(0, 30).join("\n"));
console.log("\nevenPaths", [...new Set(evenPaths)].slice(0, 50).join("\n"));
console.log("\nmedia", [...new Set(media)].slice(0, 30).join("\n"));

// Probe likely CDN bases
const bases = [
  "https://development.ihg.com/sites/default/files/",
  "https://development.ihg.com/sites/ihgplc/files/",
  "https://digital.ihg.com/is/image/ihg/",
  "https://www.ihg.com/",
];
const samples = [...new Set(evenPaths)].slice(0, 20);
for (const path of samples) {
  for (const base of bases) {
    const u = path.startsWith("http") ? path : base + path.replace(/^\/+/, "");
    try {
      const res = await fetch(u, {
        method: "GET",
        headers: { "User-Agent": "Mozilla/5.0", Range: "bytes=0-64" },
        redirect: "follow",
      });
      if (res.ok || res.status === 206) {
        console.log("OK", res.status, u.slice(0, 120));
        break;
      }
    } catch {
      /* continue */
    }
  }
}
