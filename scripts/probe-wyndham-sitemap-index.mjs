#!/usr/bin/env node
import { WYNDHAM_SITEMAP_INDEX, WYNDHAM_FETCH_HEADERS, extractSitemapLocs } from "../lib/wyndham-brand-directory-extract.js";

const res = await fetch(WYNDHAM_SITEMAP_INDEX, { headers: WYNDHAM_FETCH_HEADERS });
const all = extractSitemapLocs(await res.text());
console.log("total sitemap index locs", all.length);
console.log("non en-us", all.filter((u) => !/en-us/i.test(u)).slice(0, 20));
console.log("locale samples", [...new Set(all.map((u) => u.match(/sitemap_([^_]+)/i)?.[1]).filter(Boolean))].slice(0, 30));
