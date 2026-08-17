#!/usr/bin/env node
import { ACCOR_FETCH_HEADERS, parseAccorHotelMetadataFromHtml } from "../lib/accor-brand-directory-extract.js";
import { accorCountryCodeIsCala } from "../lib/brand-sitemap/cala-url-segments.js";

async function probe(code) {
  const url = `https://all.accor.com/hotel/${code}/index.en.shtml`;
  try {
    const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS });
    if (!res.ok) return null;
    const meta = parseAccorHotelMetadataFromHtml(await res.text());
    if (!meta) return null;
    return { code, ...meta, cala: accorCountryCodeIsCala(meta.countryCode) };
  } catch {
    return null;
  }
}

/** @type {object[]} */
const cala = [];
for (const code of ["1000", "2000", "3000", "4000", "5000", "5500", "5800", "6000", "6500", "7000", "7500", "8000", "8500", "9000", "9500", "A0B2", "B4K7"]) {
  const r = await probe(code);
  console.log(code, r?.countryCode, r?.cala, r?.name?.slice(0, 50));
  if (r?.cala) cala.push(r);
}
console.log("cala samples", cala.length);
