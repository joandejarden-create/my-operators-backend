#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const H = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  Accept: "application/json",
};

const marshaList = ["POPLC", "MIDCY", "SDQJW"];

for (const marsha of marshaList) {
  for (const svc of [
    `roomCards/?marsha=${marsha}&locale=en-US&acrsEnabled=false`,
    `propertyOverview/?marsha=${marsha}&locale=en-US`,
    `hotelOverview/?marsha=${marsha}&locale=en-US`,
    `amenities/?marsha=${marsha}&locale=en-US`,
    `propertyAmenities/?marsha=${marsha}&locale=en-US`,
    `overview/?marsha=${marsha}&locale=en-US`,
  ]) {
    const url = `https://www.marriott.com/services/marriott-hws/${svc}`;
    const r = await fetch(url, { headers: H });
    const t = await r.text();
    if (r.status === 200 && t.length > 50 && !/^<!DOCTYPE/i.test(t)) {
      console.log("OK", svc.split("?")[0], marsha, t.length);
      if (/overview|amenit|description|15-minute|high-speed/i.test(t)) {
        console.log("  content keywords found");
      }
      if (marsha === "POPLC" && svc.startsWith("roomCards")) {
        writeFileSync("reports/marriott-poplc-roomcards.json", t);
      }
    }
  }
}

// Walk roomCards JSON for POPLC
const rc = JSON.parse(await (await fetch(
  "https://www.marriott.com/services/marriott-hws/roomCards/?marsha=POPLC&locale=en-US&acrsEnabled=false",
  { headers: H }
)).text());

function walk(obj, path = "", out = []) {
  if (!obj || typeof obj !== "object") return out;
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => walk(v, `${path}[${i}]`, out));
    return out;
  }
  for (const [k, v] of Object.entries(obj)) {
    const p = path ? `${path}.${k}` : k;
    if (/amenit|overview|description/i.test(k)) {
      out.push({ path: p, preview: typeof v === "string" ? v.slice(0, 120) : Array.isArray(v) ? `array(${v.length})` : typeof v });
    }
    walk(v, p, out);
  }
  return out;
}

console.log("\nroomCards keys", walk(rc).slice(0, 30));
