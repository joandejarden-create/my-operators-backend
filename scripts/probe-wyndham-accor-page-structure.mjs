#!/usr/bin/env node
import { load } from "cheerio";

const headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124" };

function ldJsonHotels(html) {
  const $ = load(html);
  /** @type {object[]} */
  const hotels = [];
  $("script[type='application/ld+json']").each((_, el) => {
    try {
      const json = JSON.parse($(el).html() || "");
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (!obj || typeof obj !== "object") continue;
        const types = Array.isArray(obj["@type"]) ? obj["@type"] : [obj["@type"]];
        if (types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) hotels.push(obj);
      }
    } catch {
      /* skip */
    }
  });
  return hotels;
}

for (const url of [
  "https://all.accor.com/hotel/0338/index.en.shtml",
  "https://www.wyndhamhotels.com/laquinta/panama-city-florida/la-quinta-panama-city/overview",
]) {
  const res = await fetch(url, { headers, redirect: "follow" });
  const html = await res.text();
  const hotels = ldJsonHotels(html);
  console.log("\n---", url);
  console.log("status", res.status, "len", html.length);
  console.log("hotels", hotels.map((h) => ({ name: h.name, city: h.address?.addressLocality, country: h.address?.addressCountry })));
  console.log("amenityFeature sample", hotels[0]?.amenityFeature?.slice?.(0, 3));
}
