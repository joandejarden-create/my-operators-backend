#!/usr/bin/env node
import { ACCOR_FETCH_HEADERS, extractSitemapLocs, parseAccorHotelMetadataFromHtml } from "../lib/accor-brand-directory-extract.js";
import { accorCountryCodeIsCala } from "../lib/brand-sitemap/cala-url-segments.js";

const index = await (await fetch("https://all.accor.com/sitemap-fh.xml", { headers: ACCOR_FETCH_HEADERS })).text();
const children = extractSitemapLocs(index);
console.log("child sitemaps", children);

for (const sm of children.filter((u) => /sitemap-fh\.(es|pt|en|mx|br)/i.test(u))) {
  const locs = extractSitemapLocs(await (await fetch(sm, { headers: ACCOR_FETCH_HEADERS })).text());
  console.log("\n", sm, "locs", locs.length);
  for (const url of locs.slice(0, 3)) {
    const html = await (await fetch(url, { headers: ACCOR_FETCH_HEADERS })).text();
    const meta = parseAccorHotelMetadataFromHtml(html);
    console.log(" ", url, meta?.country, meta?.countryCode, accorCountryCodeIsCala(meta?.countryCode), meta?.name?.slice(0, 40));
  }
}
