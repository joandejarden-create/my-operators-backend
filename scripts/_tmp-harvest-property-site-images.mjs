#!/usr/bin/env node
import fs from "fs";

const targets = [
  {
    brand: "bw-premier-collection",
    propertyKey: "terra-nova",
    propertyName: "Terra Nova BW Premier Collection",
    marketCity: "Kingston",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-kingston/terra-nova-bw-premier-collection/propertyCode.71034.html",
    sites: ["https://www.terranovajamaica.com/gallery", "https://www.terranovajamaica.com/"],
  },
  {
    brand: "bw-premier-collection",
    propertyKey: "whitehall",
    propertyName: "The Whitehall Hotel BW Premier Collection",
    marketCity: "Chicago",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-chicago/the-whitehall-hotel-bw-premier-collection/propertyCode.14236.html",
    sites: ["https://www.thewhitehallhotel.com/", "https://www.thewhitehallhotel.com/gallery"],
  },
  {
    brand: "bw-premier-collection",
    propertyKey: "finial",
    propertyName: "Hotel Finial BW Premier Collection",
    marketCity: "Anniston",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/anniston/hotel-rooms/hotel-finial-bw-premier-collection/propertyCode.01133.html",
    sites: ["https://www.hotelfinial.com/", "https://www.hotelfinial.com/gallery"],
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "libre",
    propertyName: "Libre Hotel BW Signature Collection",
    marketCity: "Lima",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-lima/libre-hotel-bw-signature-collection/propertyCode.76413.html",
    sites: ["https://www.librehotel.com/", "https://librehotel.pe/"],
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "mayaguez",
    propertyName: "Mayaguez Plaza Hotel BW Signature Collection",
    marketCity: "Mayaguez",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-mayaguez/hotel-mayaguez-plaza-bw-signature-collection/propertyCode.55105.html",
    sites: ["https://www.hotelmayaguezplaza.com/"],
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "brion",
    propertyName: "Brion City Hotel BW Signature Collection",
    marketCity: "Willemstad",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-willemstad/brion-city-hotel-bw-signature-collection/propertyCode.71029.html",
    sites: ["https://www.brioncityhotel.com/"],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "nizuc",
    propertyName: "NIZUC Resort & Spa Preferred Hotels & Resorts",
    marketCity: "Cancun",
    sourcePageUrl: "https://preferredhotels.com/hotels/mexico/nizuc-resort-spa",
    sites: ["https://www.nizucresort.com/", "https://www.nizuccancun.com/"],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "capellan",
    propertyName: "Capellan de Getsemani Preferred Hotels & Resorts",
    marketCity: "Cartagena",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/capellan-de-getsemani",
    sites: ["https://www.capellandegetsemani.com/", "https://capellandegetsemani.com/"],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "ek",
    propertyName: "EK Hotel Preferred Hotels & Resorts",
    marketCity: "Bogota",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/ek-hotel",
    sites: ["https://www.ekhoteles.com/", "https://ekhotel.co/"],
  },
];

function extractUrls(html, base) {
  const out = [];
  const absRe = /https?:\/\/[^"'\\\s>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s>]*)?/gi;
  let m;
  while ((m = absRe.exec(html))) out.push(m[0]);
  const relRe = /(?:src|data-src|content|href)=["']([^"']+\.(?:jpg|jpeg|png|webp)[^"']*)["']/gi;
  while ((m = relRe.exec(html))) {
    try {
      out.push(new URL(m[1], base).toString());
    } catch {
      /* ignore */
    }
  }
  return [...new Set(out)].filter(
    (u) => !/logo|sprite|icon|favicon|pride|pixel|tracking|1x1|badge/i.test(u)
  );
}

const byBrand = {};
for (const t of targets) {
  const collected = [];
  for (const site of t.sites) {
    try {
      const res = await fetch(site, {
        headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
        redirect: "follow",
      });
      const html = await res.text();
      const urls = extractUrls(html, site);
      console.log(`${t.propertyKey} ${site} -> ${res.status} ${urls.length}`);
      for (const imageUrl of urls.slice(0, 16)) {
        collected.push({
          propertyKey: t.propertyKey,
          propertyName: t.propertyName,
          marketCity: t.marketCity,
          sourcePageUrl: t.sourcePageUrl,
          imageUrl,
          label: "property",
        });
      }
    } catch (err) {
      console.log(`${t.propertyKey} ${site} error ${err.message}`);
    }
  }
  byBrand[t.brand] = byBrand[t.brand] || [];
  byBrand[t.brand].push(...collected);
}

for (const [brand, rows] of Object.entries(byBrand)) {
  const dedup = [];
  const seen = new Set();
  for (const r of rows) {
    if (seen.has(r.imageUrl)) continue;
    seen.add(r.imageUrl);
    dedup.push(r);
  }
  const path = `fixtures/lane2-${brand}-gallery-pool.json`;
  fs.writeFileSync(path, `${JSON.stringify(dedup, null, 2)}\n`);
  console.log("Wrote", path, dedup.length);
}
