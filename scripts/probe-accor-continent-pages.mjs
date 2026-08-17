#!/usr/bin/env node
import { ACCOR_FETCH_HEADERS, accorHotelCodeFromUrl } from "../lib/accor-brand-directory-extract.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";

const pages = [
  { label: "south-america", slug: "hotels-south-america-c02" },
  { label: "central-america", slug: "hotels-central-america-c10" },
  { label: "north-america", slug: "hotels-north-america-c01" },
];

function extractHotelsFromHtml(html) {
  /** @type {{ code: string, name: string, url: string }[]} */
  const hotels = [];
  for (const block of String(html).match(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi) || []) {
    try {
      const json = JSON.parse(block.replace(/<\/?script[^>]*>/gi, "").trim());
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        if (obj?.["@type"] === "ItemList" && Array.isArray(obj.itemListElement)) {
          for (const el of obj.itemListElement) {
            const item = el.item || el;
            const u = item?.url || item?.["@id"];
            if (u && /\/hotel\//i.test(String(u))) {
              hotels.push({
                code: accorHotelCodeFromUrl(u),
                name: String(item.name || ""),
                url: String(u),
              });
            }
          }
        }
      }
    } catch {
      /* skip */
    }
  }
  return hotels;
}

for (const page of pages) {
  const baseUrl = `https://all.accor.com/a/en/destination/continent/${page.slug}.html`;
  const res = await fetch(baseUrl, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
  const html = await res.text();
  const hotels = extractHotelsFromHtml(html);
  const codes = [...new Set(hotels.map((h) => h.code).filter(Boolean))];

  // probe pagination patterns from HTML
  const pageLinks = [
    ...new Set(
      [...html.matchAll(/href="([^"]*destination\/continent\/[^"]*page[^"]*)"/gi)].map((m) => m[1])
    ),
  ].slice(0, 8);

  console.log(page.label, {
    status: res.status,
    jsonLdHotels: hotels.length,
    uniqueCodes: codes.length,
    sample: hotels.slice(0, 4).map((h) => ({ code: h.code, name: h.name.slice(0, 50) })),
    pageLinks,
  });
}
