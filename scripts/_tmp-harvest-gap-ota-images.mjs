#!/usr/bin/env node
import fs from "fs";

async function extractFrom(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
    redirect: "follow",
  });
  const html = await res.text();
  const out = [];
  const re = /https?:\/\/[^"'\\\s>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s>]*)?/gi;
  let m;
  while ((m = re.exec(html))) out.push(m[0]);
  return { status: res.status, urls: [...new Set(out)].filter((u) => !/logo|icon|favicon|badge/i.test(u)) };
}

const gapSources = [
  {
    brand: "bw-premier-collection",
    propertyKey: "terra-nova",
    propertyName: "Terra Nova BW Premier Collection",
    marketCity: "Kingston",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-kingston/terra-nova-bw-premier-collection/propertyCode.71034.html",
    pages: [
      "https://www.trip.com/hotels/kingston-hotel-detail-3107076/terra-nova-all-suite-hotel/",
      "https://www.expedia.com/Kingston-Hotels-Terra-Nova-All-Suite-Hotel.h6615478.Hotel-Information",
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
      "https://www.tripadvisor.com/Hotel_Review-g147321-d234870-Reviews-Mayaguez_Resort_Casino-Mayaguez_Puerto_Rico.html",
      "https://www.expedia.com/Mayaguez-Hotels-Mayaguez-Resort.h.h.Hotel-Information",
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
      "https://www.tripadvisor.com/Hotel_Review-g147278-d15530551-Reviews-Brion_City_Hotel-Willemstad_Curacao.html",
    ],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "capellan",
    propertyName: "Capellan de Getsemani Preferred Hotels & Resorts",
    marketCity: "Cartagena",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/capellan-de-getsemani",
    pages: [
      "https://www.tripadvisor.com/Hotel_Review-g297476-d17747990-Reviews-Capellan_de_Getsemani-Cartagena_District_of_Cartagena_Bolivar_Department.html",
    ],
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "ek",
    propertyName: "EK Hotel Preferred Hotels & Resorts",
    marketCity: "Bogota",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/ek-hotel",
    pages: [
      "https://www.tripadvisor.com/Hotel_Review-g294074-d15620854-Reviews-EK_Hotel-Bogota.html",
    ],
  },
];

const byBrand = {
  "bw-premier-collection": JSON.parse(
    fs.readFileSync("fixtures/lane2-bw-premier-collection-gallery-pool.json", "utf8")
  ),
  "bw-signature-collection": JSON.parse(
    fs.readFileSync("fixtures/lane2-bw-signature-collection-gallery-pool.json", "utf8")
  ),
  "preferred-hotels-and-resorts": JSON.parse(
    fs.readFileSync("fixtures/lane2-preferred-hotels-and-resorts-gallery-pool.json", "utf8")
  ),
};

for (const g of gapSources) {
  for (const page of g.pages) {
    try {
      const { status, urls } = await extractFrom(page);
      console.log(g.propertyKey, page, status, urls.length, urls.slice(0, 3));
      for (const imageUrl of urls.slice(0, 12)) {
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
      console.log(g.propertyKey, page, "error", err.message);
    }
  }
}

for (const [brand, rows] of Object.entries(byBrand)) {
  const dedup = [];
  const seen = new Set();
  for (const r of rows) {
    if (!r.imageUrl || seen.has(r.imageUrl)) continue;
    seen.add(r.imageUrl);
    dedup.push(r);
  }
  fs.writeFileSync(`fixtures/lane2-${brand}-gallery-pool.json`, `${JSON.stringify(dedup, null, 2)}\n`);
  const byProp = {};
  for (const r of dedup) byProp[r.propertyKey] = (byProp[r.propertyKey] || 0) + 1;
  console.log(brand, dedup.length, byProp);
}
