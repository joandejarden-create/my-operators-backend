#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const base = `https://www.marriott.com/en-us/hotels/${SLUG}`;

const H = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: `${base}/experiences/`,
};

async function fetchPage(path) {
  const url = base + path;
  const r = await fetch(url, { headers: H, redirect: "follow" });
  return { url, status: r.status, text: await r.text() };
}

function extractJsonLd(html) {
  const blocks = [];
  for (const m of html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)) {
    try {
      blocks.push(JSON.parse(m[1]));
    } catch {
      /* skip */
    }
  }
  return blocks;
}

function findKeys(obj, keys, path = "", out = []) {
  if (!obj || typeof obj !== "object") return out;
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => findKeys(v, keys, `${path}[${i}]`, out));
    return out;
  }
  for (const [k, v] of Object.entries(obj)) {
    const p = path ? `${path}.${k}` : k;
    if (keys.some((key) => k.toLowerCase().includes(key))) out.push({ path: p, value: v });
    findKeys(v, keys, p, out);
  }
  return out;
}

for (const path of ["/rooms/", "/experiences/", "/dining/"]) {
  const { status, text } = await fetchPage(path);
  console.log("\n===", path, status, text.length);

  const ld = extractJsonLd(text);
  console.log("json-ld blocks", ld.length);
  for (const block of ld) {
    if (block["@type"] === "Hotel") {
      console.log("Hotel keys", Object.keys(block));
      if (block.description) console.log("description", block.description.slice(0, 200));
      if (block.amenityFeature) console.log("amenityFeature count", block.amenityFeature.length);
    }
  }

  const hits = findKeys(ld, ["amenit", "overview", "description", "highlights"]);
  console.log("interesting ld paths", hits.slice(0, 15).map((h) => h.path));

  // window.__INITIAL_STATE__ or similar
  for (const pat of [
    /window\.__INITIAL_STATE__\s*=\s*({[\s\S]*?});/,
    /window\.__APOLLO_STATE__\s*=\s*({[\s\S]*?});/,
    /"hotelAmenities"\s*:\s*(\[[\s\S]*?\])/,
    /"overview"\s*:\s*"([^"]{50,500})"/,
    /"description"\s*:\s*"([^"]{50,500})"/,
  ]) {
    const m = text.match(pat);
    if (m) console.log("pattern hit", pat.source.slice(0, 40), m[1]?.slice?.(0, 120) || "yes");
  }

  if (path === "/rooms/") {
    writeFileSync("reports/marriott-poplc-rooms-sample.html", text.slice(0, 200000));
  }
}

// Try overview with cookies from experiences
const exp = await fetchPage("/experiences/");
const setCookie = exp.text.match(/document\.cookie\s*=/) ? "has cookie script" : "no";
console.log("\noverview with session-like headers");
const ov = await fetch(`${base}/overview/`, {
  headers: {
    ...H,
    Cookie: "country=US; lang=en",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
  },
});
console.log("overview", ov.status, (await ov.text()).length);
