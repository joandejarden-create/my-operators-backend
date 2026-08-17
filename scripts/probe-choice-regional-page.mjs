#!/usr/bin/env node
import "../load-env.js";
import { choicePropertyIdFromUrl } from "../lib/choice-hotel-content-fetch.js";

const url =
  process.argv[2] ||
  "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0";

const res = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
  },
  redirect: "follow",
});

console.log("status:", res.status);
console.log("final:", res.url);
const html = await res.text();
console.log("html length:", html.length);
console.log("blocked:", /access denied|robot check|captcha/i.test(html));

const propertyPathRe =
  /https?:\/\/(?:www\.)?choicehotels\.com\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z]{2}\d{2,3}/gi;
const relPathRe = /\/mexico\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z]{2}\d{2,3}/gi;
const found = [...new Set([...html.matchAll(propertyPathRe)].map((m) => m[0]))];
const relFound = [...new Set([...html.matchAll(relPathRe)].map((m) => "https://www.choicehotels.com" + m[0]))];
const all = [...new Set([...found, ...relFound])];
console.log("property URLs found:", all.length);
for (const u of all.slice(0, 40)) {
  console.log(" ", u, "|", choicePropertyIdFromUrl(u));
}
if (all.length > 40) console.log(" ... +" + (all.length - 40));

const mxIds = [...new Set([...html.matchAll(/\b(mx\d{3})\b/gi)].map((m) => m[1].toUpperCase()))];
console.log("mx### tokens:", mxIds.length, mxIds.join(", "));

const nextData = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
if (nextData) {
  try {
    const data = JSON.parse(nextData[1]);
    const blob = JSON.stringify(data);
    const inNext = [...new Set([...blob.matchAll(propertyPathRe)].map((m) => m[0]))];
    console.log("__NEXT_DATA__ property URLs:", inNext.length);
    for (const u of inNext.slice(0, 20)) console.log("  next:", u);
  } catch (e) {
    console.log("__NEXT_DATA__ parse failed:", e.message);
  }
}
