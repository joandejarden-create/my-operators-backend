#!/usr/bin/env node
import {
  WYNDHAM_SITEMAP_INDEX,
  WYNDHAM_FETCH_HEADERS,
  extractSitemapLocs,
} from "../lib/wyndham-brand-directory-extract.js";

const res = await fetch(WYNDHAM_SITEMAP_INDEX, { headers: WYNDHAM_FETCH_HEADERS });
const children = extractSitemapLocs(await res.text()).filter((u) => /properties/i.test(u));

const countrySegments = [
  "puerto-rico",
  "mexico",
  "jamaica",
  "dominican-republic",
  "colombia",
  "brazil",
  "costa-rica",
  "panama",
  "bahamas",
  "barbados",
  "aruba",
  "curacao",
  "trinidad-and-tobago",
  "bermuda",
  "cayman-islands",
  "peru",
  "chile",
  "argentina",
  "ecuador",
  "guatemala",
  "honduras",
  "el-salvador",
  "uruguay",
  "paraguay",
  "bolivia",
  "venezuela",
];

/** @type {string[]} */
const hits = [];
for (const childUrl of children) {
  const cx = await (await fetch(childUrl, { headers: WYNDHAM_FETCH_HEADERS })).text();
  for (const loc of extractSitemapLocs(cx)) {
    if (!/\/overview\/?$/i.test(loc)) continue;
    const lower = loc.toLowerCase();
    if (countrySegments.some((seg) => lower.includes(`/${seg}/`))) hits.push(loc);
  }
}

console.log("exact country segment hits", hits.length);
console.log(hits.slice(0, 40).join("\n"));
