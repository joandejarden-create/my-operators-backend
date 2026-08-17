#!/usr/bin/env node
import { load } from "cheerio";

const url =
  process.argv[2] ||
  "https://www.wyndhamhotels.com/wyndham/puerto-rico/san-juan/wyndham-grand-rio-mar-beach-resort/overview";
const html = await (await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 Chrome/124" } })).text();
const $ = load(html);
console.log("status url", url);
console.log("amenit class count", $("[class*='amenit' i]").length);
$("[class*='amenit' i]")
  .slice(0, 8)
  .each((i, el) => console.log(i, $(el).text().trim().slice(0, 80)));
console.log("data-testid", $("[data-testid*='amenit' i]").length);
