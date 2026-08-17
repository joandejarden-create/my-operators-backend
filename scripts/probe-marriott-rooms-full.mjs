#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const url = `https://www.marriott.com/en-us/hotels/${SLUG}/rooms/`;
const H = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html",
  "Accept-Language": "en-US,en;q=0.9",
};

const r = await fetch(url, { headers: H });
const html = await r.text();
writeFileSync("reports/marriott-poplc-rooms-full.html", html);
console.log("status", r.status, "len", html.length);

for (const needle of [
  "15-minute drive",
  "Free high-speed internet",
  "Mobility accessible",
  "Puerto Plata International",
  "__INITIAL_JSON__",
  "__NEXT_DATA__",
  "hotelAmenities",
  "overviewDescription",
  "propertyOverview",
]) {
  console.log(needle, html.includes(needle));
}

const scripts = [...html.matchAll(/<script[^>]*>([\s\S]{0,80})/g)].map((m) => m[0].slice(0, 100));
console.log("\nscript prefixes", scripts.filter((s) => /INITIAL|NEXT|apollo|state/i.test(s)).slice(0, 10));

const init = html.match(/window\.__INITIAL_JSON__\s*=\s*/);
console.log("\n__INITIAL_JSON__ index", init?.index);

if (init) {
  const start = init.index + init[0].length;
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let i = start; i < html.length; i++) {
    const c = html[i];
    if (inStr) {
      if (esc) esc = false;
      else if (c === "\\") esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') inStr = true;
    else if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) {
        const jsonStr = html.slice(start, i + 1);
        writeFileSync("reports/marriott-poplc-initial-json-full.json", jsonStr);
        const data = JSON.parse(jsonStr);
        console.log("parsed __INITIAL_JSON__ keys", Object.keys(data));
        break;
      }
    }
  }
}
