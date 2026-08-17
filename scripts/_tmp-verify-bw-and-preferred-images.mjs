#!/usr/bin/env node
import fs from "fs";

const infoRes = await fetch(
  "https://images.bestwestern.com/bwi/brochures/71034/propertyInfo_en.txt",
  { headers: { "User-Agent": "Mozilla/5.0" } }
);
const info = await infoRes.json();
const first = (info.Media || []).find((m) => m.ImagePath);
const url = `https://images.bestwestern.com/bwi/brochures/71034/${first.ImagePath}`;
const head = await fetch(url, { method: "HEAD", headers: { "User-Agent": "Mozilla/5.0" } });
console.log(JSON.stringify({
  url,
  status: head.status,
  contentType: head.headers.get("content-type"),
}));

const preferredPages = [
  {
    propertyKey: "nizuc",
    propertyName: "NIZUC Resort & Spa Preferred Hotels & Resorts",
    marketCity: "Cancun",
    sourcePageUrl: "https://preferredhotels.com/hotels/mexico/nizuc-resort-spa",
    site: "https://www.nizucresort.com/",
  },
  {
    propertyKey: "capellan",
    propertyName: "Capellan de Getsemani Preferred Hotels & Resorts",
    marketCity: "Cartagena",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/capellan-de-getsemani",
    site: "https://capellandegetsemani.com/",
  },
  {
    propertyKey: "ek",
    propertyName: "EK Hotel Preferred Hotels & Resorts",
    marketCity: "Bogota",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/ek-hotel",
    site: "https://www.ekhoteles.com/",
  },
];

function extractUrls(html, base) {
  const abs = [];
  const absRe = /https?:\/\/[^"'\\\s>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s>]*)?/gi;
  let m;
  while ((m = absRe.exec(html))) abs.push(m[0]);
  const rel = [];
  const relRe = /(?:src|data-src|content)=["']([^"']+\.(?:jpg|jpeg|png|webp)[^"']*)["']/gi;
  while ((m = relRe.exec(html))) {
    const raw = m[1];
    try {
      rel.push(new URL(raw, base).toString());
    } catch {
      /* ignore */
    }
  }
  return [...new Set([...abs, ...rel])].filter((u) => !/logo|sprite|icon|favicon|pride/i.test(u));
}

const pool = [];
for (const p of preferredPages) {
  try {
    const res = await fetch(p.site, {
      headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
      redirect: "follow",
    });
    const html = await res.text();
    const urls = extractUrls(html, p.site).slice(0, 20);
    console.log(p.propertyKey, res.status, urls.length, urls.slice(0, 3));
    for (const imageUrl of urls) {
      pool.push({
        propertyKey: p.propertyKey,
        propertyName: p.propertyName,
        marketCity: p.marketCity,
        sourcePageUrl: p.sourcePageUrl,
        imageUrl,
        label: "property",
      });
    }
  } catch (err) {
    console.log(p.propertyKey, "error", err.message);
  }
}

fs.writeFileSync(
  "fixtures/lane2-preferred-hotels-and-resorts-gallery-pool.json",
  `${JSON.stringify(pool, null, 2)}\n`
);
console.log("Wrote preferred pool", pool.length);
