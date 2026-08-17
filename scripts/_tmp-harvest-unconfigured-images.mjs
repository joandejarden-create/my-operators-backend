#!/usr/bin/env node
/**
 * Temporary harvest helper for Lane 4 gallery pool seeding.
 * Writes reports/_tmp-unconfigured-image-harvest.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const pages = [
  {
    brand: "bw-premier-collection",
    propertyKey: "terra-nova",
    propertyName: "Terra Nova BW Premier Collection",
    marketCity: "Kingston",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-kingston/terra-nova-bw-premier-collection/propertyCode.71034.html",
  },
  {
    brand: "bw-premier-collection",
    propertyKey: "whitehall",
    propertyName: "The Whitehall Hotel BW Premier Collection",
    marketCity: "Chicago",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-chicago/the-whitehall-hotel-bw-premier-collection/propertyCode.14236.html",
  },
  {
    brand: "bw-premier-collection",
    propertyKey: "finial",
    propertyName: "Hotel Finial BW Premier Collection",
    marketCity: "Anniston",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/anniston/hotel-rooms/hotel-finial-bw-premier-collection/propertyCode.01133.html",
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "libre",
    propertyName: "Libre Hotel BW Signature Collection",
    marketCity: "Lima",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-lima/libre-hotel-bw-signature-collection/propertyCode.76413.html",
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "mayaguez",
    propertyName: "Mayaguez Plaza Hotel BW Signature Collection",
    marketCity: "Mayaguez",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-mayaguez/hotel-mayaguez-plaza-bw-signature-collection/propertyCode.55105.html",
  },
  {
    brand: "bw-signature-collection",
    propertyKey: "brion",
    propertyName: "Brion City Hotel BW Signature Collection",
    marketCity: "Willemstad",
    sourcePageUrl:
      "https://www.bestwestern.com/en_US/book/hotels-in-willemstad/brion-city-hotel-bw-signature-collection/propertyCode.71029.html",
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "capellan",
    propertyName: "Capellan de Getsemani Preferred Hotels & Resorts",
    marketCity: "Cartagena",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/capellan-de-getsemani",
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "ek",
    propertyName: "EK Hotel Preferred Hotels & Resorts",
    marketCity: "Bogota",
    sourcePageUrl: "https://preferredhotels.com/hotels/colombia/ek-hotel",
  },
  {
    brand: "preferred-hotels-and-resorts",
    propertyKey: "nizuc",
    propertyName: "NIZUC Resort & Spa Preferred Hotels & Resorts",
    marketCity: "Cancun",
    sourcePageUrl: "https://preferredhotels.com/hotels/mexico/nizuc-resort-spa",
  },
];

function extractImages(html) {
  const og = [];
  for (const re of [
    /property=["']og:image["'][^>]*content=["']([^"']+)["']/gi,
    /content=["']([^"']+)["'][^>]*property=["']og:image["']/gi,
  ]) {
    let m;
    while ((m = re.exec(html))) og.push(m[1]);
  }
  const imgs = [];
  const imgRe = /https?:\/\/[^"'\s>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\s>]*)?/gi;
  let m;
  while ((m = imgRe.exec(html))) imgs.push(m[0]);
  return {
    og: [...new Set(og)].slice(0, 8),
    imgs: [...new Set(imgs)].slice(0, 20),
  };
}

const results = [];
for (const p of pages) {
  try {
    const res = await fetch(p.sourcePageUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept: "text/html,application/xhtml+xml",
      },
      redirect: "follow",
    });
    const html = await res.text();
    const extracted = extractImages(html);
    results.push({
      ...p,
      status: res.status,
      ...extracted,
      ok: res.ok && (extracted.og.length > 0 || extracted.imgs.length > 0),
    });
    console.log(
      `${p.propertyKey}: status=${res.status} og=${extracted.og.length} imgs=${extracted.imgs.length}`
    );
  } catch (err) {
    results.push({ ...p, ok: false, error: err?.message || String(err) });
    console.log(`${p.propertyKey}: error ${err?.message || err}`);
  }
}

const out = path.join(ROOT, "reports", "_tmp-unconfigured-image-harvest.json");
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, `${JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)}\n`);
console.log("Wrote", out);
