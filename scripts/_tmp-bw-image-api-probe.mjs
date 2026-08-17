#!/usr/bin/env node
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const bwCodes = [
  { brand: "bw-premier-collection", propertyKey: "terra-nova", propertyName: "Terra Nova BW Premier Collection", marketCity: "Kingston", code: "71034", sourcePageUrl: "https://www.bestwestern.com/en_US/book/hotels-in-kingston/terra-nova-bw-premier-collection/propertyCode.71034.html" },
  { brand: "bw-premier-collection", propertyKey: "whitehall", propertyName: "The Whitehall Hotel BW Premier Collection", marketCity: "Chicago", code: "14236", sourcePageUrl: "https://www.bestwestern.com/en_US/book/hotels-in-chicago/the-whitehall-hotel-bw-premier-collection/propertyCode.14236.html" },
  { brand: "bw-premier-collection", propertyKey: "finial", propertyName: "Hotel Finial BW Premier Collection", marketCity: "Anniston", code: "01133", sourcePageUrl: "https://www.bestwestern.com/en_US/book/anniston/hotel-rooms/hotel-finial-bw-premier-collection/propertyCode.01133.html" },
  { brand: "bw-signature-collection", propertyKey: "libre", propertyName: "Libre Hotel BW Signature Collection", marketCity: "Lima", code: "76413", sourcePageUrl: "https://www.bestwestern.com/en_US/book/hotels-in-lima/libre-hotel-bw-signature-collection/propertyCode.76413.html" },
  { brand: "bw-signature-collection", propertyKey: "mayaguez", propertyName: "Mayaguez Plaza Hotel BW Signature Collection", marketCity: "Mayaguez", code: "55105", sourcePageUrl: "https://www.bestwestern.com/en_US/book/hotels-in-mayaguez/hotel-mayaguez-plaza-bw-signature-collection/propertyCode.55105.html" },
  { brand: "bw-signature-collection", propertyKey: "brion", propertyName: "Brion City Hotel BW Signature Collection", marketCity: "Willemstad", code: "71029", sourcePageUrl: "https://www.bestwestern.com/en_US/book/hotels-in-willemstad/brion-city-hotel-bw-signature-collection/propertyCode.71029.html" },
];

const endpoints = (code) => [
  `https://www.bestwestern.com/bin/bestwestern/proxy/hotelDetails?propertyCode=${code}&locale=en_US`,
  `https://www.bestwestern.com/bin/bestwestern/services/hotel/hotelDetails?propertyCode=${code}&locale=en_US`,
  `https://www.bestwestern.com/bin/bestwestern/services/hotelImage?propertyCode=${code}`,
  `https://images.bestwestern.com/bimg/propertyimages/large/${code}.jpg`,
];

function collectUrls(obj, out = []) {
  if (!obj) return out;
  if (typeof obj === "string") {
    if (/https?:\/\/.+\.(jpg|jpeg|png|webp)/i.test(obj) || /images\.bestwestern|bimg\//i.test(obj)) {
      out.push(obj);
    }
    return out;
  }
  if (Array.isArray(obj)) {
    for (const v of obj) collectUrls(v, out);
    return out;
  }
  if (typeof obj === "object") {
    for (const v of Object.values(obj)) collectUrls(v, out);
  }
  return out;
}

const results = [];
for (const p of bwCodes) {
  const found = [];
  for (const url of endpoints(p.code)) {
    try {
      const res = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          Accept: "application/json,text/html,*/*",
        },
      });
      const ct = res.headers.get("content-type") || "";
      let bodyText = "";
      let urls = [];
      if (ct.includes("json")) {
        const json = await res.json();
        urls = [...new Set(collectUrls(json))];
        bodyText = JSON.stringify(json).slice(0, 200);
      } else {
        bodyText = await res.text();
        urls = [...bodyText.matchAll(/https?:\/\/[^"'\s>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\s>]*)?/gi)].map((m) => m[0]);
        if (res.ok && /image\//i.test(ct)) urls.push(url);
      }
      found.push({ url, status: res.status, ct, sampleUrls: [...new Set(urls)].slice(0, 10), bodyPreview: bodyText.slice(0, 120) });
    } catch (err) {
      found.push({ url, error: err.message });
    }
  }
  results.push({ ...p, probes: found });
  console.log(p.propertyKey, found.map((f) => `${f.status || "err"}:${(f.sampleUrls || []).length}`).join(" "));
}

const out = path.join(ROOT, "reports", "_tmp-bw-image-api-probe.json");
fs.writeFileSync(out, `${JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)}\n`);
console.log("Wrote", out);
