#!/usr/bin/env node
import {
  WYNDHAM_SITEMAP_INDEX,
  WYNDHAM_FETCH_HEADERS,
  extractSitemapLocs,
  parseWyndhamPropertyUrl,
} from "../lib/wyndham-brand-directory-extract.js";

const res = await fetch(WYNDHAM_SITEMAP_INDEX, { headers: WYNDHAM_FETCH_HEADERS });
const children = extractSitemapLocs(await res.text()).filter((u) => /properties/i.test(u));
const pat =
  /mexico|jamaica|puerto-rico|dominican|colombia|brazil|costa-rica|panama|bahamas|barbados|aruba|curacao|trinidad|bermuda|cayman|peru|chile|argentina|ecuador|guatemala|honduras|el-salvador|uruguay|paraguay|bolivia|venezuela|cancun|caribbean|santo-domingo|san-juan|rio-de-janeiro|sao-paulo|buenos-aires|lima|bogota|medellin|monterrey|guadalajara/i;

/** @type {string[]} */
const hits = [];
for (const childUrl of children) {
  const cx = await (await fetch(childUrl, { headers: WYNDHAM_FETCH_HEADERS })).text();
  for (const loc of extractSitemapLocs(cx)) {
    if (!/\/overview\/?$/i.test(loc)) continue;
    if (pat.test(loc)) hits.push(loc);
  }
  if (hits.length >= 20) break;
}
console.log("hits", hits.length);
for (const u of hits.slice(0, 20)) {
  console.log(u);
  console.log(" parsed", parseWyndhamPropertyUrl(u));
}
