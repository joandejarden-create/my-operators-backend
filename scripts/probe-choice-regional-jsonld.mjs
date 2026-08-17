#!/usr/bin/env node
import "../load-env.js";
import { parseChoiceRegionalHotelsFromHtml } from "../lib/choice-regional-directory-extract.js";

const url =
  "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0";
const res = await fetch(url, {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  },
});
const html = await res.text();
console.log("bytes", html.length);

const hotels = parseChoiceRegionalHotelsFromHtml(html);
console.log("parsed hotels", hotels.length);

for (const id of ["MX077", "MX067", "MX163"]) {
  const h = hotels.find((x) => x.propertyId === id);
  console.log(id, h || "missing");
}

// Sample raw Hotel snippets
const idx = html.indexOf("mx077");
if (idx >= 0) console.log("\nmx077 context:\n", html.slice(Math.max(0, idx - 200), idx + 200));

const hotelSnippets = [...html.matchAll(/"@type":"Hotel"/g)].length;
console.log("\n@type Hotel count:", hotelSnippets);

const ldJson = [...html.matchAll(/<script type="application\/ld\+json">/gi)].length;
console.log("ld+json script tags:", ldJson);
