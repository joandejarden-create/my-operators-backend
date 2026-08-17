#!/usr/bin/env node
/**
 * Probe one IHG destination page HTML for name/url/mnemonic extract patterns.
 */
import { writeFileSync } from "node:fs";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const url = process.argv[2] || "https://www.ihg.com/destinations/us/en/mexico-hotels";
const r = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html" } });
const html = await r.text();
console.log("status", r.status, "len", html.length, "final", r.url);

// Card blocks
const cardChunks = html.split(/data-component-hotelcard/i).slice(1);
console.log("card chunks", cardChunks.length);

const hotels = [];
for (const chunk of cardChunks.slice(0, 5)) {
  const mnemonic = (chunk.match(/data-hotel-mnemonic="([A-Z0-9]+)"/i) || [])[1];
  const countryCode = (chunk.match(/data-hotel-countryCode="([A-Z]{2})"/i) || [])[1];
  const aria = (chunk.match(/aria-label="Check Rates for ([^"]+)"/i) || [])[1];
  const href = (chunk.match(/href="([^"]*hoteldetail[^"]*)"/i) || [])[1];
  const nameEl = (chunk.match(/class="[^"]*cmp-card__title[^"]*"[^>]*>([^<]+)/i) || [])[1]
    || (chunk.match(/itemprop="name"[^>]*content="([^"]+)"/i) || [])[1]
    || (chunk.match(/<h[23][^>]*>([^<]+)/i) || [])[1];
  hotels.push({ mnemonic, countryCode, aria, href, nameEl: nameEl?.trim() });
}
console.log(JSON.stringify(hotels, null, 2));

// Also look for JSON embeds
const jsonHits = [...html.matchAll(/"mnemonic"\s*:\s*"([A-Z0-9]+)"/gi)].length;
const nameHits = [...html.matchAll(/"hotelName"\s*:\s*"([^"]+)"/gi)].slice(0, 5).map((m) => m[1]);
console.log("json mnemonic hits", jsonHits, "hotelName sample", nameHits);

writeFileSync("reports/ihg-destination-page-snippet.html", html.slice(0, 200000));
console.log("wrote snippet");
