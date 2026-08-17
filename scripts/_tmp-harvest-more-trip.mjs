#!/usr/bin/env node
import fs from "fs";

async function extractTrip(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
  });
  const html = await res.text();
  const urls = [...html.matchAll(/https?:\/\/ak-d\.tripcdn\.com\/images\/[^"'\\\s>]+\.jpg[^"'\\\s>]*/gi)].map(
    (m) => m[0]
  );
  return { status: res.status, urls: [...new Set(urls)] };
}

const more = [
  {
    brand: "bw-signature-collection",
    propertyKey: "libre",
    propertyName: "Libre Hotel BW Signature Collection",
    marketCity: "Lima",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-lima/libre-hotel-bw-signature-collection/propertyCode.76413.html",
    pages: [
      "https://www.trip.com/hotels/lima-hotel-detail-68920500/libre-hotel/",
      "https://www.trip.com/hotels/list?city=273&keyword=Libre%20Hotel",
    ],
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "mayaguez",
    propertyName: "Mayaguez Plaza Hotel BW Signature Collection",
    marketCity: "Mayaguez",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-mayaguez/hotel-mayaguez-plaza-bw-signature-collection/propertyCode.55105.html",
    pages: [
      "https://www.trip.com/hotels/mayaguez-hotel-detail-2177166/mayaguez-resort-and-casino/",
      "https://www.trip.com/hotels/mayaguez-hotel-detail-99673468/hotel-mayaguez-plaza/",
    ],
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "brion",
    propertyName: "Brion City Hotel BW Signature Collection",
    marketCity: "Willemstad",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-willemstad/brion-city-hotel-bw-signature-collection/propertyCode.71029.html",
    pages: [
      "https://www.trip.com/hotels/willemstad-hotel-detail-99673469/brion-city-hotel/",
      "https://www.trip.com/hotels/list?keyword=Brion%20City%20Hotel%20Willemstad",
    ],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "capellan",
    propertyName: "Capellan de Getsemani Preferred Hotels & Resorts",
    marketCity: "Cartagena",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/capellan-de-getsemani",
    pages: [
      "https://www.trip.com/hotels/cartagena-hotel-detail-68920501/capellan-de-getsemani/",
      "https://www.trip.com/hotels/list?keyword=Capellan%20de%20Getsemani",
    ],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "ek",
    propertyName: "EK Hotel Preferred Hotels & Resorts",
    marketCity: "Bogota",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/ek-hotel",
    pages: [
      "https://www.trip.com/hotels/bogota-hotel-detail-68920502/ek-hotel/",
      "https://www.trip.com/hotels/list?keyword=EK%20Hotel%20Bogota",
    ],
  },
];

const byBrand = {
  "bw-signature-collection": JSON.parse(
    fs.readFileSync("fixtures/lane2-bw-signature-collection-gallery-pool.json", "utf8")
  ),
  "preferred-hotels-and-resorts": JSON.parse(
    fs.readFileSync("fixtures/lane2-preferred-hotels-and-resorts-gallery-pool.json", "utf8")
  ),
};

for (const g of more) {
  for (const page of g.pages) {
    try {
      const { status, urls } = await extractTrip(page);
      console.log(g.propertyKey, status, urls.length, page);
      for (const imageUrl of urls.slice(0, 15)) {
        byBrand[g.brand].push({
          propertyKey: g.propertyKey,
          propertyName: g.propertyName,
          marketCity: g.marketCity,
          sourcePageUrl: g.sourcePageUrl,
          imageUrl,
          label: "property",
        });
      }
    } catch (err) {
      console.log(g.propertyKey, "error", err.message, page);
    }
  }
}

for (const brand of Object.keys(byBrand)) {
  const dedup = [];
  const seen = new Set();
  for (const r of byBrand[brand]) {
    if (!r.imageUrl || seen.has(r.imageUrl)) continue;
    seen.add(r.imageUrl);
    dedup.push(r);
  }
  fs.writeFileSync(`fixtures/lane2-${brand}-gallery-pool.json`, `${JSON.stringify(dedup, null, 2)}\n`);
  const byProp = {};
  for (const r of dedup) byProp[r.propertyKey] = (byProp[r.propertyKey] || 0) + 1;
  console.log("Wrote", brand, dedup.length, byProp);
}
