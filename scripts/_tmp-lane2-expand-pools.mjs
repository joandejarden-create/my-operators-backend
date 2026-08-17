/** Expand Handwritten Accor harvest + Choice/Wayback Hilton/Radisson probes. */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function decode(s) {
  return String(s || "")
    .replace(/\\u002D/g, "-")
    .replace(/\\u002F/g, "/")
    .replace(/\\\//g, "/")
    .replace(/&amp;/g, "&");
}

async function fetchText(url) {
  const res = await fetch(url, {
    redirect: "follow",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      Accept: "text/html,application/json,*/*",
    },
  });
  return { status: res.status, text: decode(await res.text()), finalUrl: res.url };
}

function extract(text, re) {
  return [...new Set([...text.matchAll(re)].map((m) => (m[1] || m[0]).replace(/[),.;]+$/, "")))];
}

const handwrittenCodes = [
  ["C344", "Hotel Stratford San Francisco"],
  ["B9F3", "Handwritten B9F3"],
  ["C139", "Handwritten C139"],
  ["C013", "Handwritten C013"],
  ["C1U0", "Handwritten C1U0"],
  ["C150", "Handwritten C150"],
  ["B7A6", "Handwritten B7A6"],
  ["C2L2", "Handwritten C2L2"],
  ["C160", "Handwritten C160"],
];

const handwrittenPool = [];
for (const [code, fallbackName] of handwrittenCodes) {
  const url = `https://all.accor.com/hotel/${code}/index.en.shtml`;
  process.stdout.write(`HW ${code}... `);
  try {
    const { status, text } = await fetchText(url);
    const title =
      (text.match(/<h1[^>]*>([^<]+)<\/h1>/i) || [])[1]?.replace(/\s+/g, " ").trim() || fallbackName;
    const imgs = extract(text, /https:\/\/www\.ahstatic\.com\/photos\/[^"'\\\s<>]+/gi).filter(
      (u) => /_1024x768|_2048x1536/i.test(u) && !/_120x90|_346x260/i.test(u)
    );
    console.log(`status=${status} title=${title.slice(0, 50)} imgs=${imgs.length}`);
    for (const imageUrl of imgs.slice(0, 8)) {
      handwrittenPool.push({
        propertyKey: `${code.toLowerCase()}-${title
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .slice(0, 40)}`,
        propertyName: title,
        marketCity: "",
        sourcePageUrl: url,
        imageUrl,
        label: "property",
      });
    }
  } catch (err) {
    console.log(`ERR ${err.message}`);
  }
  await new Promise((r) => setTimeout(r, 250));
}

fs.writeFileSync(
  path.join(ROOT, "fixtures", "lane2-handwritten-collection-gallery-pool.json"),
  `${JSON.stringify(handwrittenPool, null, 2)}\n`
);
console.log(`Handwritten pool: ${handwrittenPool.length}`);

// Choice Hotels page — dig for hoteldam / property image JSON
process.stdout.write("Choice Radisson Collection page deep extract... ");
const choice = await fetchText("https://www.choicehotels.com/radisson-collection");
const hoteldam = extract(choice.text, /https?:\/\/[^"'\\\s<>]*hoteldam[^"'\\\s<>]+/gi);
const media = extract(choice.text, /https?:\/\/media\.radissonhotels[^"'\\\s<>]+/gi);
const damPaths = extract(choice.text, /\/hoteldam\/[a-z]{2}\/[a-z0-9/-]+/gi);
const jsonUrls = extract(choice.text, /"(https?:\\\/\\\/[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"/gi).map(
  (u) => u.replace(/\\\//g, "/")
);
console.log(
  `status=${choice.status} hoteldam=${hoteldam.length} media=${media.length} damPaths=${damPaths.length} jsonImgs=${jsonUrls.length}`
);
console.log("hoteldam sample", hoteldam.slice(0, 8));
console.log("damPaths sample", damPaths.slice(0, 12));
console.log("jsonImgs sample", jsonUrls.slice(0, 12));

// Wayback Hilton Cotton Sail
const waybackCandidates = [
  "https://web.archive.org/web/2024/https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
  "https://web.archive.org/web/20240101000000/https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/",
];
for (const url of waybackCandidates) {
  process.stdout.write(`Wayback ${url.slice(0, 60)}... `);
  try {
    const { status, text } = await fetchText(url);
    const imgs = extract(
      text,
      /https?:\/\/[^"'\\\s<>]*(?:hilton|cloudinary|akamaized)[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'\\\s<>]*)?/gi
    );
    console.log(`status=${status} len=${text.length} imgs=${imgs.length}`);
    if (imgs.length) console.log(imgs.slice(0, 10));
  } catch (err) {
    console.log(`ERR ${err.message}`);
  }
}

// Try Hilton GraphQL-ish public endpoints sometimes used by brand pages
const hiltonApis = [
  "https://www.hilton.com/graphql/customer?app=dx&operationName=hotel&variables=%7B%22ctyhocn%22%3A%22SAVVYUP%22%7D",
  "https://www.hilton.com/en/hotels/savvyup-the-cotton-sail-hotel-savannah/media.json",
];
for (const url of hiltonApis) {
  process.stdout.write(`Hilton API probe... `);
  try {
    const { status, text } = await fetchText(url);
    console.log(`status=${status} len=${text.length}`);
    console.log(text.slice(0, 300).replace(/\s+/g, " "));
  } catch (err) {
    console.log(`ERR ${err.message}`);
  }
}
